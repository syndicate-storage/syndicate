#!/usr/bin/python

"""
Daemon that sits on a sliver, which:
* learns the Volumes it attached to from OpenCloud
* creates/destroyes Gateways to said Volumes
* keeps the Gateways mounted and running
"""

import os
import sys
import signal
import resource
import daemon 
import grp
import lockfile
import argparse
import cgi
import BaseHTTPServer
import base64
import json 
import errno
import requests
import threading
import psutil
import socket
import subprocess

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig

import syndicate.util.watchdog as watchdog
import syndicate.util.daemon as daemon 
import syndicate.util.config as modconf

import syndicate.syndicate as c_syndicate

#-------------------------------
# constants 
OPENCLOUD_JSON                          = "opencloud_json"
OPENCLOUD_JSON_DATA                     = "opencloud_json_data"
OPENCLOUD_JSON_SIGNATURE                = "opencloud_json_signature"

OPENCLOUD_VOLUME_NAME                   = "volume_name"
OPENCLOUD_VOLUME_OWNER_ID               = "volume_owner"
OPENCLOUD_VOLUME_PASSWORD               = "volume_password"
OPENCLOUD_SYNDICATE_URL                 = "syndicate_url"

SYNDICATE_UG_WATCHDOG_NAME              = "syndicate-ug"
SYNDICATE_RG_WATCHDOG_NAME              = "syndicate-rg"
SYNDICATE_UG_BINARY                     = "syndicatefs"

UG_PORT                                 = 32780
RG_PORT                                 = 32880

#-------------------------------
CONFIG_OPTIONS = {
   "config":            ("-c", 1, "Path to the daemon configuration file"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-l", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-i", 1, "Path to the desired PID file."),
   "public-key":        ("-p", 1, "Path to the Observer public key."),
   "observer-secret":   ("-s", 1, "Shared secret with Observer."),
   "observer-url":      ("-u", 1, "URL to the Syndicate Observer"),
   "poll-timeout":      ("-t", 1, "Interval to wait between asking OpenCloud for our Volume credentials."),
   "mountpoint-dir":    ("-m", 1, "Directory to hold Volume mountpoints."),
   "closure":           ("-C", 1, "Path to the RG closure to use."),
   "port":              ("-P", 1, "Port to listen on.")
}

DEFAULT_CONFIG = {
    "config":           "/etc/syndicate/syndicated.conf",
    "public-key":       "/etc/syndicate/observer.pub",
    "logdir":           "/var/log/syndicated",
    "pidfile":          "/var/run/syndicated.pid",
    "poll-timeout":     43200,          # ask twice a day; the Observer should poke us directly anyway
    "observer-secret":  None,  
    "observer-url":     "https://localhost:5553",
    "mountpoint-dir":   "/tmp/syndicate-mounts",
    "port":             5553,
    "closure":          "/usr/local/lib64/python2.7/site-packages/syndicate/rg/drivers/s3",
       
    # these are filled in at runtime.
    # THis is the information needed to act as a principal of the volume onwer (i.e. the slice)
    "runtime": {
        "volume_name":          None
        "volume_owner":         "judecn@gmail.com",
        "volume_password":      "nya!",
        "syndicate_url":        "http://localhost:8080",
     }
}


#-------------------------------

# global config structure 
CONFIG = None 

def get_config():
    return CONFIG
    
def update_config_runtime( runtime_info ): 
    if not CONFIG.has_key('runtime'):
        CONFIG['runtime'] = {}
        
    CONFIG['runtime'].update( runtime_info )


#-------------------------------
class ObserverPushHandler( BaseHTTPServer.BaseHTTPRequestHandler ):
    """
    Listen for new Volume information pushed out from the Syndicate Observer.
    """
    
    def response_error( self, status, message, content_type="text/plain" ):
        
        self.send_response( status )
        self.send_header( "Content-Type", content_type )
        self.send_header( "Content-Length", len(message) )
        self.end_headers()
        
        self.wfile.write( message )
        
    def response_success( self, message, content_type="text/plain" ):
        return self.response_error( 200, message, content_type )
        
        
    def do_POST( self ):
        
        # do we have the requisite runtime information?
        config = get_config()
        if config is None:
            response_error( 500, "Server is not configured" )
            return 
        
        form = cgi.FieldStorage( fp=self.rfile,
                                 headers=self.headers,
                                 environ={
                                    'REQUEST_METHOD':   'POST',
                                    'CONTENT_TYPE':     self.headers['Content-Type'],
                                 } )

        # expecting a JSON string 
        json_text = form.getfirst( OPENCLOUD_COMMAND, None )
        if json_text is None:
            # malformed request 
            self.response_error( 400, "Missing data" )
            return 
        
        # get the data 
        rc, data_text = read_opencloud_data_from_json( config, json_text )
        if rc != 0:
            # failed to read 
            self.response_error( 400, "Invalid request" )
            return 
        
        # parse the data 
        rc, data = parse_opencloud_data( config, data_text )
        if rc != 0:
            # failed to parse 
            self.response_error( 400, "Invalid request")
            return 
        
        if data is not None:
           update_config_runtime( data )
           ensure_running()
        
        # finish up
        return response_success( "OK" )
        

#-------------------------------
def load_public_key( key_path ):
    try:
      key_text = read_file( key_path )
   except Exception, e:
      log.error("Failed to read public key '%s'" % key_path )
      return None

   try:
      key = CryptoKey.importKey( key_text )
      assert not key.has_private()
         
   except Exception, e:
      log.error("Failed to load public key %s'" % key_path )
      return None
   
   return key


#-------------------------------
def verify_signature( key, data, sig ):
    h = HashAlg.new( data )
    verifier = CryptoSigner.new(key)
    ret = verifier.verify( h, sig )
    return ret


#-------------------------------
def find_missing_and_invalid_fields( required_fields, json_data ):
    """
    Look for missing or invalid fields, and return them.
    """
    
    missing = []
    invalid = []
    for req, types in required_fields.items():
        if req not in json_data.keys():
            missing.append( req )
        
        if type(json_data[req]) not in types:
            invalid.append( req )
            
    return missing, invalid


#-------------------------------
def read_opencloud_data_from_json( config, json_text ):
    """
    Parse and validate a JSON structure.
    Return 0 on success
    Return nonzero on error 
    """
    
    # verify the presence and types of our required fields 
    required_fields = {
        OPENCLOUD_COMMAND_DATA: [str, unicode],
        OPENCLOUD_COMMAND_SIGNATURE: [str, unicode]
    }
    
    # parse the json structure 
    try:
        json_data = json.loads( json_text )
    except:
        # can't parse 
        log.error("Failed to parse JSON text")
        return -errno.EINVAL
     
    # look for missing or invalid fields
    missing, invalid = find_missing_and_invalid_fields( required_fields, json_data )
    
    if len(missing) > 0:
        log.error("Missing fields: %s" % (", ".join(missing)))
        return (-errno.EINVAL, None)
    
    if len(invalid) > 0:
        log.error("Invalid fields: %s" % (", ".join(invalid)))
        return (-errno.EINVAL, None)
    
    # extract fields (they will be base64-encoded)
    opencloud_data_b64 = json_data[OPENCLOUD_JSON_DATA]
    opencloud_signature_b64 = json_data[OPENCLOUD_JSON_SIGNATURE]
    
    try:
        opencloud_data = base64.b64decode( opencloud_data_b64 )
        opencloud_signature = base64.b64decode( opencloud_signature_b64 )
    except:
        log.error("Failed to decode message")
        return (-errno.EINVAL, None)
    
    # get the public key 
    k = load_public_key( config['public-key'] )
    if k is None:
        logger.error("Failed to load public key from %s" % (config['public-key']))
        return (-errno.ENOENT, None)
    
    # verify the signature 
    rc = verify_signature( k, opencloud_data, opencloud_signature )
    if not rc:
        logger.error("Invalid signature")
        return (-errno.EINVAL, None)
    
    # we're good!
    return (0, opencloud_data)


#-------------------------------
def parse_opencloud_data( config, data_text ):
    """
    Parse a string of JSON data from OpenCloud.  It should be a JSON structure 
    with some particular fields.
    Return (0, dict) on success
    Return (nonzero, None) on error.
    """
    
    # verify the presence and types of our required fields
    required_fields = {
        OPENCLOUD_VOLUME_NAME: [str, unicode],
        OPENCLOUD_VOLUME_OWNER_ID: [str, unicode],
        OPENCLOUD_VOLUME_PASSWORD: [str, unicode],
        OPENCLOUD_SYNDICATE_URL: [str, unicode],
    }
    
    # parse the data text
    try:
        data = json.loads( data_text )
    except:
        # can't parse 
        log.error("Failed to parse JSON data")
        return -errno.EINVAL
    
    # look for missing or invalid fields
    missing, invalid = find_missing_and_invalid_fields( required_fields, data )
    
    if len(missing) > 0:
        log.error("Missing fields: %s" % (", ".join(missing)))
        return (-errno.EINVAL, None)
    
    if len(invalid) > 0:
        log.error("Invalid fields: %s" % (", ".join(invalid)))
        return (-errno.EINVAL, None)
    
    return (0, data)
    

#-------------------------------
def poll_opencloud_data( config ):
    """
    Download, verify, and parse OpenCloud data.
    Return (0, data) on success
    Return (nonzero, None) on error 
    """
    url = config['opencloud-url']
    
    req = requests.get( url )
    if req.status != 200:
        log.error("GET %s status %s" % (url, req.status))
        return None
    
    rc, opencloud_data = read_opencloud_data_from_json( config, req.text )
    if rc != 0:
        log.error("Failed to read JSON, rc = %s" % rc)
        return None 
    
    rc, data = parse_opencloud_data( config, opencloud_data )
    if rc != 0:
        log.error("Failed to read OpenCloud data, rc = %s" % rc )
        return None
    
    return data
    

#-------------------------------
def connect_syndicate( config ):
    """
    Connect to the Syndicate SMI
    """
    runtime_info = config.get('runtime', None)
    
    if runtime_info is None:
        return None
    
    volume_owner_id = runtime_info['volume_owner']
    syndicate_url = runtime_info['syndicate_url']
    volume_password = runtime_info['volume_password']
    
    client = syntool.Client( volume_owner_id, syndicate_url, password=volume_password, debug=True )

    return client


#-------------------------------
def make_gateway_name( gateway_type, volume_name, host ):
    """
    Generate a name for a gateway
    """
    return "OpenCloud-%s-%s-%s" % (volume_name, gateway_type, host)    


#-------------------------------
def make_gateway_private_key_password( gateway_name, observer_secret ):
    """
    Generate a unique gateway private key password.
    NOTE: its only as secure as the OpenCloud secret; the rest can be guessed by the adversary 
    """
    h = HashAlg.SHA256Hash()
    h.update( "%s-%s-%s" % (gateway_name, observer_secret))
    return h.hexdigest()
 

#-------------------------------
def ensure_gateway_exists( config, gateway_type, user_email, volume_name, gateway_name, host, port, key_password, closure=None ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    Returns the gateway on succes.
    Returns None if we can't connect.
    Raises an exception on error.
    We assume that the Volume (and thus user) already exist...if they don't, its an error.
    """

    client = connect_syndicate( config )
    if client is None:
        return None
    
    try:
        gateway = client.read_gateway( gateway_name )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e

    need_create_gateway = False

    # is it the right gateway?
    if gateway is not None:
        
        # the associated user and volume must exist, and they must match 
        try:
            user = client.read_user( user_email )
        except Exception, e:
            # transport error
            logger.exception(e)
            raise e

        try:
            volume = client.read_volume( volume_name )
        except Exception, e:
            # transport error 
            logger.exception(e)
            raise e

        # these had better exist...
        if user is None or volume is None:
            raise Exception("Orphaned gateway with the same name as us (%s)" % gateway_name)

        # does this gateway match the user and volume it claims to belong to?
        # NOTE: this doesn't check the closure!
        if msconfig.GATEWAY_TYPE_TO_STR[ gateway["gateway_type"] ] != gateway_type or gateway['owner_id'] != user['owner_id'] or gateway['volume_id'] != volume['volume_id']:
            raise Exception("Gateway exists under a different volume (%s) or user (%s)" % (volume['name'], user['email']))

        # gateway exists, and is owned by the given volume and user 
        return gateway

    else:
        # create the gateway 
        kw = {
           'encryption_password': key_password,
        }
        
        if closure is not None:
            kw['closure'] = closure
        
        try:
            gateway = client.create_gateway( volume_name, user_email, gateway_type, gateway_name, host, port, **kw )
        except Exception, e:
            # transport, collision, or missing Volume or user
            logger.exception(e)
            raise e

        else:
            return gateway


#-------------------------------
def ensure_gateway_absent( config, gateway_type, user_email, volume_name, host ):
    """
    Ensure that a particular gateway does not exist.
    Return True on success
    return False if we can't connect
    raise exception on error.
    """
    
    client = connect_syndicate( config )
    if client is None:
        return False

    client.delete_gateway( gateway_name )
    return True


#-------------------------------
def ensure_UG_exists( config, user_email, volume_name, gateway_name, host, port=UG_PORT ):
    """
    Ensure that a particular UG exists.
    """
    return ensure_gateway_exists( config, "UG", user_email, volume_name, gateway_name, host, port )


#-------------------------------
def ensure_RG_exists( config, user_email, volume_name, gateway_name, host, port=RG_PORT, closure=None ):
    """
    Ensure that a particular RG exists.
    """
    return ensure_gateway_exists( config, "RG", user_email, volume_name, gateway_name, host, port, closure=closure )


#-------------------------------
def ensure_UG_absent( config, user_email, volume_name, gateway_name, host ):
    """
    Ensure that a particular UG does not exist
    """
    return ensure_gateway_absent( config, "UG", user_email, volume_name, gateway_name, host )


#-------------------------------
def ensure_RG_absent( config, user_email, volume_name, gateway_name, host ):
    """
    Ensure that a particular RG does not exist
    """
    return ensure_gateway_absent( config, "RG", user_email, volume_name, gateway_name, host )


#-------------------------------
def make_UG_mountpoint_path( config, volume_name ):
    """
    Generate the path to a mountpoint.
    """
    vol_dirname = volume_name.replace("/", ".")
    vol_mountpoint = os.path.join( config['mountpoint-dir'], vol_dirname )
    return vol_mountpoint


#-------------------------------
def ensure_UG_mountpoint_exists( mountpoint ):
   """
   Make a mountpoint (i.e. a directory)
   """
   rc = 0
   try:
      os.makedirs( mountpoint )
   except OSError, oe:
      return -oe.errno
   except Exception, e:
      log.exception(e)
      return -errno.EPERM

#-------------------------------
def make_UG_command_string( binary_name, user_email, user_password, volume_name, gateway_name, key_password, mountpoint ):
   return "%s -f -u %s -p %s -v %s -g %s -K %s %s" % (binary_name, user_email, user_password, volume_name, gateway_name, key_password, mountpoint )
                           

#-------------------------------
def find_UG_by_attr( attr_name, attr_value ):
   """
   Find a running UG (watchdog) by a given attr name and value
   """
   UGs = []
   for p in psutil.process_iter():
      if p.name == SYNDICATE_UG_WATCHDOG_NAME:
         # switch on attrs 
         for attr_kv in p.cmdline[1:]:
            if attr_kv.startswith("attr:") and "=" in attr_kv:
               # parse this: attr:<name>=<value>
               kv = attr_kv[len("attr:"):]
               
               key, values = kv.split("=")
               value = "=".join(values)
               
               if attr_name == key and attr_value == value:
                  UGs.append( p )


   return UGs
            

#-------------------------------
def start_UG( config, username, volume_name, gateway_name, key_password, mountpoint ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information,
   # which should NOT become visible to other users via /proc
   password = config['runtime']['volume_password']
   syndicate_url = config['runtime']['syndicate_url']
   
   command_str = make_UG_command_string( SYNDICATE_UG_BINARY, username, password, volume_name, gateway_name, key_password, mountpoint )
   
   # start the watchdog
   p = subprocess.Popen([SYNDICATE_RG_WATCHDOG_NAME, "-v", volume_name, "-m", mountpoint], stdin=subprocess.PIPE)

   # send the command to execute 
   p.stdin.write( command_str )
   p.stdin.close()
   
   # the watchdog will now daemonize and start the UG
   return 0


#-------------------------------
def stop_UG( config, mountpoint ):
   # stop a UG, given its mountpoint.
   # this method is idempotent
   mounted_UGs = find_UG_by_attr( "mountpoint", mountpoint )
   if len(mounted_UGs) > 0:
      for proc in mounted_UGs:
         # tell the watchdog to die, so it shuts down the UG
         try:
            os.kill( proc.pid, signal.SIGTERM )
         except OSError, oe:
            if oe.errno != errno.ESRCH:
               # NOT due to the process dying after we checked for it
               log.exception(oe)
               return -1
         
         except Exception, e:
            log.exception(e)
            return -1
         
   
   return 0


#-------------------------------
def start_RG( config, volume_name, gateway_name ):
   # TODO
   return 0


#-------------------------------
def ensure_UG_running( config, user_email, volume_name, gateway_name, key_password, mountpoint, check_only=False ):
    """
    Ensure that a User Gateway is running on a particular mountpoint.
    Return 0 on success
    Return negative on error.
    """
    
    # make sure a mountpoint exists
    rc = ensure_UG_mountpoint_exists( mountpoint )
    if rc != 0:
        return rc
    
    # is there a UG running at this mountpoint?
    mounted_UGs = find_UG_by_attr( "mountpoint", mountpoint )
    if len(mounted_UGs) == 1:
       # we're good!
       return mounted_UGs[0].pid
    
    elif len(mounted_UGs) > 1:
       # too many!  probably in the middle of starting up 
       return -errno.EAGAN
    
    else: 
       if not config.has_key('runtime') or config['runtime'] == None:
          return -errno.EAGAIN
       
       if not check_only:
          return start_UG( config, user_email, volume_name, gateway_name, key_password, mountpoint )
       
       else:
          return 0


#-------------------------------
def ensure_UG_stopped( config, mountpoint ):
    """
    Ensure a UG is no longer running.
    """
    return stop_UG( config, mountpoint )


#-------------------------------
def ensure_RG_running( config, user_email, volume_name, gateway_name, key_password, check_only=False ):
    pass 

#-------------------------------
def ensure_RG_stopped( config ):
    pass


#-------------------------
def ensure_running():
   """
   Once config['runtime'] has been populated,
   ensure that the gateways to the volume exist,
   and that they are up and running.
   """
   
   config = get_config()
   
   hostname = socket.gethostname()
   
   try:
      volume_name = config['runtime']['volume_name']
      user_email = config['runtime']['volume_owner']
      volume_password = config['runtime']['volume_password']
      syndicate_url = config['runtime']['syndicate_url']
      observer_secret = config['observer_secret']
      
   except:
      return -errno.ENODATA
   
   UG_name = make_gateway_name( "UG", volume_name, hostname )
   RG_name = make_gateway_name( "RG", volume_name, hostname )
   
   UG_key_password = make_gateway_private_key_password( UG_name, observer_secret )
   RG_key_password = make_gateway_private_key_password( RG_name, observer_secret )
   
   UG_mountpoint_path = make_UG_mountpoint_path( config, volume_name )
   
   # is the UG running?
   rc = ensure_UG_running( config, user_email, volume_name, UG_name, UG_key_password, UG_mountpoint_path, check_only=True )
   
   if rc != 0:
      # ensure the UG exists
      rc = ensure_UG_exists( config, user_email, volume_name, UG_name, host, port=UG_PORT )
      if not rc:
         log.error("Failed to create UG %s on the MS" % UG_name)
         return rc
      
      # ensure our local copy is running 
      rc = ensure_UG_running( config, user_email, volume_name, UG_name, UG_key_password, UG_mountpoint_path )
      if rc != 0:
         log.error("Failed to ensure that UG %s is running" % UG_name )
         return rc
      
   # is the RG running?
   rc = ensure_RG_running( config, user_email, volume_name, RG_name, RG_key_password, check_only=True )
   if rc != 0:
      # ensure the RG exists 
      rc = ensure_RG_exists( config, user_email, volume_name, RG_name, host, port=UG_PORT )
      if not rc:
         log.error("Failed to create RG %s on the MS" % RG_name)
         return rc
      
      # ensure our local copy is running
      rc = ensure_RG_running( config, user_email, volume_name, RG_name, RG_key_password )
      if rc != 0:
         log.error("Failed to ensure that UG %s is running" % UG_name )
         return rc
      
      
   return 0

#-------------------------
def validate_config( config ):
   
   # required arguments
   required = ['observer-secret', 'observer-url', 'public-key']
   for req in required:
      if config.get( req, None ) == None:
         print >> sys.stderr, "Missing required argument: %s" % req
         return -1
   
   # required types 
   required_types = {
       'poll-timeout': int,
   }
   
   for req, reqtype in required_types.items():
       try:
           i = reqtype( config[req] )
           config[req] = i
       except:
           print >> sys.stderr, "Invalid value for '%s'" % req
           return -1
       
   return 0
      
      
#-------------------------
class PollThread( threading.Thread ):
   
   def run():
      """
      Continuously poll the OpenCloud observer for runtime data.
      """
      while True:
         config = get_config()
         
         poll_timeout = config['poll-timeout']
         
         time.sleep( poll_timeout )
         
         rc, data = poll_opencloud_data( self.config )
         
         if rc != 0:
            log.error("poll_opencloud_data rc = %s" % rc)
            
         elif data is not None:
            update_config_runtime( data )
            
            # act on the data 
            ensure_running()
            
         

#-------------------------
def main( config ):
   
   # start polling 
   th = PollThread()
   th.start()
   
   # start listening for pushes
   httpd = BaseHTTPServer.HTTPServer( ('', 5553), ObserverPushHandler )
   httpd.serve_forever()


#-------------------------    
if __name__ == "__main__":
   
   config = modconf.build_config( argv, conf_validator=validate_config )
   
   if config is None:
      sys.exit(-1)
   
   if config.get("foreground", None):
      main( config )
   else:
      logfile_path = None 
      pidfile_path = config.get("pidfile", None)
      
      if config.has_key("logdir"):
         logfile_path = os.path.join( config['logdir'], "syndicated.log" )
      
      daemonize.daemonize( lambda: main(config), logfile_path=logfile_path, pidfile_path=pidfile_path )
        

