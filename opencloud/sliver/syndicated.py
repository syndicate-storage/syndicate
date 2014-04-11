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
import shlex
import time

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig

import syndicate.util.watchdog as watchdog
import syndicate.util.daemonize as daemon 
import syndicate.util.config as modconf
import syndicate.util.storage as storage

import syndicate.syndicate as c_syndicate

#-------------------------------
# constants 
OPENCLOUD_JSON                          = "observer_message"
OPENCLOUD_JSON_DATA                     = "data"
OPENCLOUD_JSON_SIGNATURE                = "sig"

OPENCLOUD_VOLUME_NAME                   = "volume_name"
OPENCLOUD_VOLUME_OWNER_ID               = "volume_owner"
OPENCLOUD_VOLUME_PASSWORD               = "volume_password"
OPENCLOUD_SYNDICATE_URL                 = "syndicate_url"

SYNDICATE_UG_WATCHDOG_NAME              = "syndicate-ug"
SYNDICATE_RG_WATCHDOG_NAME              = "syndicate-rg"
SYNDICATE_UG_BINARY_NAME                = "syndicatefs"


SYNDICATE_UG_WATCHDOG_PATH              = "/home/jude/Desktop/research/git/syndicate/build/out/bin/UG/syndicate-ug"
SYNDICATE_RG_WATCHDOG_PATH              = "/home/jude/Desktop/research/git/syndicate/build/out/bin/RG/syndicate-rg"
SYNDICATE_UG_BINARY_PATH                = "/home/jude/Desktop/research/git/syndicate/build/out/bin/UG/syndicatefs"

UG_PORT                                 = 32780
RG_PORT                                 = 32880

#-------------------------------
CONFIG_OPTIONS = {
   "config":            ("-c", 1, "Path to the daemon configuration file"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-l", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-i", 1, "Path to the desired PID file."),
   "public_key":        ("-p", 1, "Path to the Observer public key."),
   "observer_secret":   ("-s", 1, "Shared secret with Observer."),
   "observer_url":      ("-u", 1, "URL to the Syndicate Observer"),
   "poll_timeout":      ("-t", 1, "Interval to wait between asking OpenCloud for our Volume credentials."),
   "mountpoint_dir":    ("-m", 1, "Directory to hold Volume mountpoints."),
   "closure":           ("-C", 1, "Path to the RG closure to use."),
   "port":              ("-P", 1, "Port to listen on."),
   "debug":             ("-d", 0, "Print debugging information."),
   "run_once":          ("-1", 0, "Poll once (for testing)")
}

DEFAULT_CONFIG = {
    "config":           "/etc/syndicate/syndicated.conf",
    "public_key":       "/etc/syndicate/observer.pub",
    "logdir":           "/var/log/syndicated",
    "pidfile":          "/var/run/syndicated.pid",
    "poll_timeout":     43200,          # ask twice a day; the Observer should poke us directly anyway
    "observer_secret":  None,  
    "observer_url":     "https://localhost:5553",
    "mountpoint_dir":   "/tmp/syndicate-mounts",
    "port":             5553,
    "closure":          "/usr/local/lib64/python2.7/site-packages/syndicate/rg/drivers/s3",
       
    # these are filled in at runtime.
    # THis is the information needed to act as a principal of the volume onwer (i.e. the slice)
    "runtime": {
        "volume_name":          None,
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
    global CONFIG 
    
    if not CONFIG.has_key('runtime'):
        CONFIG['runtime'] = {}
        
    CONFIG['runtime'].update( runtime_info )

#-------------------------------
class EnsureRunningThread( threading.Thread ):
   """
   Process OpenCloud data and act on it, either 
   synchronously or asynchronously
   """
   
   processing_lock = threading.Lock()
   
   def __init__(self, sealed_data):
      self.sealed_data = sealed_data
      super( EnsureRunningThread, self ).__init__()
      
   @classmethod 
   def unseal_and_process_data( cls, sealed_data ):
      """
      Unseal data, and then process it.
      Make sure no more than one instance of this request is being processed.
      """
      locked = cls.processing_lock.acquire( False )
      if not locked:
         # failed to aquire 
         return -errno.EAGAIN
   
      config = get_config()
      
      # unseal the data 
      rc, data_text = unseal_observer_data( config['observer_secret'], sealed_data )
      if rc != 0:
         # failed to read 
         cls.processing_lock.release()
         log.error("unseal_observer_data rc = %s" % rc)
         return -errno.EINVAL
      
      # parse the data 
      rc, data = parse_observer_data( data_text )
      if rc != 0:
         # failed to parse 
         cls.processing_lock.release()
         log.error("parse_observer_data rc = %s" % rc)
         return -errno.EINVAL
      
      if data is not None:
         update_config_runtime( data )
         rc = ensure_running()
         
         if rc != 0:
            errorf("ensure_running rc = %s" % rc)
      
      cls.processing_lock.release()
      return rc
   
   
   def run( self ):
      """
      Unseal and process the data given in the constructor.
      """
      return EnsureRunningThread.unseal_and_process_data( self.sealed_data )
      

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
        json_text = form.getfirst( OPENCLOUD_JSON, None )
        if json_text is None:
            # malformed request 
            self.response_error( 400, "Missing data" )
            return 
        
        # get the data 
        rc, sealed_data = read_observer_data_from_json( config['public_key'], json_text )
        if rc != 0:
            # failed to read 
            self.response_error( 400, "Invalid request" )
            return 
         
        # send the request back immediately 
        self.response_success( "OK" )
        self.wfile.flush()
        
        # data came from OpenCloud
        # asynchronously process it
        proc_thread = EnsureRunningThread( sealed_data )
        proc_thread.start()
        
        return
        
        

#-------------------------------
def load_public_key( key_path ):
   try:
      key_text = storage.read_file( key_path )
   except Exception, e:
      log.error("Failed to read public key '%s'" % key_path )
      return None

   try:
      key = CryptoKey.importKey( key_text )
      assert not key.has_private()
         
   except Exception, e:
      log.error("Failed to load public key %s" % key_path )
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
def read_observer_data_from_json( public_key_path, json_text ):
    """
    Parse and validate a JSON structure.
    Return 0 on success
    Return nonzero on error 
    """
    
    # verify the presence and types of our required fields 
    required_fields = {
        OPENCLOUD_JSON_DATA: [str, unicode],
        OPENCLOUD_JSON_SIGNATURE: [str, unicode]
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
        sealed_data = base64.b64decode( opencloud_data_b64 )
        opencloud_signature = base64.b64decode( opencloud_signature_b64 )
    except:
        log.error("Failed to decode message")
        return (-errno.EINVAL, None)
    
    # get the public key 
    k = load_public_key( public_key_path )
    if k is None:
        log.error("Failed to load public key from %s" % (public_key_path))
        return (-errno.ENOENT, None)
    
    # verify the signature 
    rc = verify_signature( k, sealed_data, opencloud_signature )
    if not rc:
        log.error("Invalid signature")
        return (-errno.EINVAL, None)
    
    # return the encrypted data 
    return (0, sealed_data)
 
 
#-------------------------------
def unseal_observer_data( shared_secret, sealed_data ):
    
    # decrypt the data, using the shared secret 
    log.info("Unsealing credentials...")
    rc, data = c_syndicate.password_unseal( sealed_data, shared_secret )
    if rc != 0:
        loggerr.error("Failed to decrypt data")
        return (-errno.EINVAL, None)
    
    # we're good!
    return (0, data)


#-------------------------------
def parse_observer_data( data_text ):
    """
    Parse a string of JSON data from the Syndicate OpenCloud Observer.  It should be a JSON structure 
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
    Download, verify, and parse Observer data.
    Return (0, data) on success
    Return (nonzero, None) on error 
    """
    url = config['observer_url']
    
    req = requests.get( url )
    if req.status_code != 200:
        log.error("GET %s status %s" % (url, req.status_code))
        return None
    
    rc, sealed_data_str = read_observer_data_from_json( config['public_key'], req.text )
    if rc != 0:
        log.error("Failed to read JSON, rc = %s" % rc)
        return None 
    
    rc, sealed_data_str = unseal_observer_data( config['observer_secret'], sealed_data_str )
    if rc != 0:
       log.error("Failed to unseal data, rc = %s" % rc)
       return None
    
    rc, data = parse_observer_data( data_str )
    if rc != 0:
        log.error("Failed to read OpenCloud data, rc = %s" % rc )
        return None
    
    return data
    

#-------------------------------
def connect_syndicate( syndicate_url, volume_owner, volume_password ):
    """
    Connect to the Syndicate SMI
    """
    client = syntool.Client( volume_owner, syndicate_url, password=volume_password, debug=True )

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
    h.update( "%s-%s" % (gateway_name, observer_secret))
    return h.hexdigest()
 

#-------------------------------
def ensure_gateway_exists( syndicate_url, gateway_type, user_email, volume_password, volume_name, gateway_name, host, port, key_password, closure=None ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    Returns the gateway on succes.
    Returns None if we can't connect.
    Raises an exception on error.
    We assume that the Volume (and thus user) already exist...if they don't, its an error.
    """

    client = connect_syndicate( syndicate_url, user_email, volume_password )
    if client is None:
        return None
    
    try:
        gateway = client.read_gateway( gateway_name )
    except Exception, e:
        # transport error 
        log.exception(e)
        raise e

    need_create_gateway = False

    # is it the right gateway?
    if gateway is not None:
        
        # the associated user and volume must exist, and they must match 
        try:
            user = client.read_user( user_email )
        except Exception, e:
            # transport error
            log.exception(e)
            raise e

        try:
            volume = client.read_volume( volume_name )
        except Exception, e:
            # transport error 
            log.exception(e)
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
        kw = {}
        if closure is not None:
            kw['closure'] = closure
        
        try:
            gateway = client.create_gateway( volume_name, user_email, gateway_type, gateway_name, host, port, encryption_password=key_password, gateway_public_key="MAKE_AND_HOST_GATEWAY_KEY", **kw )
        except Exception, e:
            # transport, collision, or missing Volume or user
            log.exception(e)
            raise e

        else:
            return gateway


#-------------------------------
def ensure_gateway_absent( syndicate_url, gateway_type, user_email, volume_password, volume_name, host ):
    """
    Ensure that a particular gateway does not exist.
    Return True on success
    return False if we can't connect
    raise exception on error.
    """
    
    client = connect_syndicate( syndicate_url, user_email, volume_password )
    if client is None:
        return False

    client.delete_gateway( gateway_name )
    return True


#-------------------------------
def ensure_UG_exists( syndicate_url, user_email, volume_password, volume_name, gateway_name, host, port, key_password ):
    """
    Ensure that a particular UG exists.
    """
    return ensure_gateway_exists( syndicate_url, "UG", user_email, volume_password, volume_name, gateway_name, host, port, key_password )


#-------------------------------
def ensure_RG_exists( syndicate_url, user_email, volume_password, volume_name, gateway_name, host, port, key_password, closure=None ):
    """
    Ensure that a particular RG exists.
    """
    return ensure_gateway_exists( syndicate_url, "RG", user_email, volume_password, volume_name, gateway_name, host, port, key_password, closure=closure )


#-------------------------------
def ensure_UG_absent( syndicate_url, user_email, volume_password, volume_name, gateway_name, host ):
    """
    Ensure that a particular UG does not exist
    """
    return ensure_gateway_absent( syndicate_url, "UG", user_email, volume_password, volume_name, gateway_name, host )


#-------------------------------
def ensure_RG_absent( syndicate_url, user_email, volume_password, volume_name, gateway_name, host ):
    """
    Ensure that a particular RG does not exist
    """
    return ensure_gateway_absent( syndicate_url, "RG", user_email, volume_password, volume_name, gateway_name, host )


#-------------------------------
def make_UG_mountpoint_path( mountpoint_dir, volume_name ):
    """
    Generate the path to a mountpoint.
    """
    vol_dirname = volume_name.replace("/", ".")
    vol_mountpoint = os.path.join( mountpoint_dir, vol_dirname )
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
      if oe.errno != errno.EEXIST:
         return -oe.errno
      else:
         return 0
   except Exception, e:
      log.exception(e)
      return -errno.EPERM

#-------------------------------
def make_UG_command_string( binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, mountpoint ):
   # NOTE: run in foreground; watchdog handles the rest
   return "%s -f -m %s -u %s -p %s -v %s -g %s -K %s %s" % (binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, mountpoint )
                           
#-------------------------------
def make_RG_command_string( binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password ):
   return "%s -m %s -u %s -p %s -v %s -g %s -K %s" % (binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password)


#-------------------------------
def make_watchdog( binary, argv, stdin_buf ):
   """
   Fork a watchdog.
   Return its pid on success
   return errno on error 
   """
   
   try:
      watchdog_pid = os.fork()
   except OSError, oe:
      log.error("os.fork() errno %s" % -oe.errno)
      return -oe.errno 
   
   if watchdog_pid == 0:
      # child 
      return daemon.exec_with_piped_stdin( binary, argv, stdin_buf )
   
   else:
      # parent 
      return watchdog_pid
   

#-------------------------------
def start_UG( syndicate_url, username, password, volume_name, gateway_name, key_password, mountpoint ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_UG_command_string( SYNDICATE_UG_BINARY_NAME, syndicate_url, username, password, volume_name, gateway_name, key_password, mountpoint )
   
   # start the watchdog
   command_list = shlex.split( command_str )   
   return make_watchdog( SYNDICATE_UG_WATCHDOG_NAME, [SYNDICATE_UG_WATCHDOG_NAME, '-v', volume_name, '-m', mountpoint], command_str )
   

#-------------------------------
def stop_UG( volume_name, mountpoint ):
   # stop a UG, given its mountpoint.
   # this method is idempotent
   mounted_UGs = watchdog.find_by_attrs( SYNDICATE_UG_WATCHDOG_NAME, {"volume": volume_name, "mountpoint": mountpoint} )
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
def start_RG( syndicate_url, username, password, volume_name, gateway_name, key_password ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_RG_command_string( SYNDICATE_RG_WATCHDOG_NAME, syndicate_url, username, password, volume_name, gateway_name, key_password )
   
   # start the watchdog
   command_list = shlex.split( command_str )
   
   return make_watchdog( command_list[0], [SYNDICATE_RG_WATCHDOG_NAME, '-R', '-v', volume_name], command_str )
   


#-------------------------------
def stop_gateway_watchdog( pid ):
   # stop a watchdog, given a PID.
   # return 0 on success, -1 on error
   
   # tell the watchdog to die, so it shuts down the UG
   try:
      os.kill( pid, signal.SIGTERM )
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
def stop_UG( volume_name, mountpoint ):
   # stop a UG, given its mountpoint and volume name
   # this method is idempotent
   mounted_UGs = watchdog.find_by_attrs( SYNDICATE_UG_WATCHDOG_NAME, {"volume": volume_name, "mountpoint": mountpoint} )
   if len(mounted_UGs) > 0:
      for proc in mounted_UGs:
         rc = stop_gateway_watchdog( proc.pid )
         if rc != 0:
            return rc
   
   return 0


#-------------------------------
def stop_RG( volume_name ):
   # stop an RG
   running_RGs = watchdog.find_by_attrs( SYNDICATE_RG_WATCHDOG_NAME, {"volume": volume_name} )
   if len(running_RGs) > 0:
      for proc in running_RGs:
         rc = stop_gateway_watchdog( proc.pid )
         if rc != 0:
            return rc
         
   return 0

         
#-------------------------------
def ensure_UG_running( syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, mountpoint, check_only=False ):
    """
    Ensure that a User Gateway is running on a particular mountpoint.
    Return 0 on success
    Return negative on error.
    """
    
    # make sure a mountpoint exists
    rc = ensure_UG_mountpoint_exists( mountpoint )
    if rc != 0:
       log.error("Failed to ensure mountpoint %s exists" % mountpoint)
       return rc
    
    # is there a UG running at this mountpoint?
    mounted_UGs = watchdog.find_by_attrs( SYNDICATE_UG_WATCHDOG_NAME, {"volume": volume_name, "mountpoint": mountpoint} )
    if len(mounted_UGs) == 1:
       # we're good!
       logging.info("UG for %s at %s already running; PID = %s" % (volume_name, mountpoint, mounted_UGs[0].pid))
       return mounted_UGs[0].pid
    
    elif len(mounted_UGs) > 1:
       # too many!  probably in the middle of starting up 
       logging.error("Multiple UGs running for %s on %s...?" % (volume_name, mountpoint))
       return -errno.EAGAN
    
    else: 
       logging.error("No UG for %s on %s" % (volume_name, mountpoint))
       if not check_only:
          pid = start_UG( syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, mountpoint )
          if pid < 0:
             log.error("Failed to start UG in %s at %s, rc = %s" % (volume_name, mountpoint, pid))
          
          return pid
       
       else:
          return 0


#-------------------------------
def ensure_UG_stopped( volume_name, mountpoint ):
    """
    Ensure a UG is no longer running.
    """
    rc = stop_UG( volume_name, mountpoint )
    if rc != 0:
       log.error("Failed to stop UG in %s at %s, rc = %s" % (volume_name, mountpoint, rc))
    
    return rc


#-------------------------------
def ensure_RG_running( syndicate_url, user_email, password, volume_name, gateway_name, key_password, check_only=False ):
    """
    Ensure an RG is running
    """
    # is there an RG running for this volume?
    running_RGs = watchdog.find_by_attrs( SYNDICATE_RG_WATCHDOG_NAME, {"volume": volume_name} )
    if len(running_RGs) == 1:
       # we're good!
       logging.info("RG for %s already running; PID = %s" % (volume_name, running_RGs[0].pid))
       return running_RGs[0].pid
    
    elif len(running_RGs) > 1:
       # too many! probably in the middle of starting up 
       logging.error("Multiple RGs running for %s...?" % (volume_name))
       return -errno.EAGAIN
    
    else:
       logging.error("No RG for %s" % (volume_name))
       if not check_only:
          pid = start_RG( syndicate_url, user_email, password, volume_name, gateway_name, key_password )
          if pid < 0:
             log.error("Failed to start RG in %s, rc = %s" % (volume_name, pid))
             
          return pid
                       
       else:
          return 0
       

#-------------------------------
def ensure_RG_stopped( volume_name ):
    """
    Ensure that the RG is stopped.
    """
    rc = stop_RG( volume_name )
    if rc != 0:
       log.error("Failed to stop RG in %s, rc = %s" % (volume_name, rc))

    return rc


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
   
   UG_mountpoint_path = make_UG_mountpoint_path( config['mountpoint_dir'], volume_name )
   
   # is the UG running?
   rc = ensure_UG_running( syndicate_url, user_email, volume_password, volume_name, UG_name, UG_key_password, UG_mountpoint_path, check_only=True )
   
   if rc <= 0:
      # ensure the UG exists
      rc = ensure_UG_exists( syndicate_url, user_email, volume_password, volume_name, UG_name, hostname, UG_PORT, UG_key_password )
      if rc is None:
         log.error("Failed to create UG %s on the MS, rc = %s" % (UG_name, rc))
         return -errno.ENOENT
      
      # ensure our local copy is running 
      rc = ensure_UG_running( syndicate_url, user_email, volume_password, volume_name, UG_name, UG_key_password, UG_mountpoint_path )
      if rc < 0:
         log.error("Failed to ensure that UG %s is running, rc = %s" % (UG_name, rc) )
         return rc
   
   if rc >= 0:
      log.info("\n\nUG for %s (mounted at %s) running on PID %s\n" % (volume_name, UG_mountpoint_path, rc))
      
   # is the RG running?
   rc = ensure_RG_running( syndicate_url, user_email, volume_password, volume_name, RG_name, RG_key_password, check_only=True )
   if rc <= 0:
      # ensure the RG exists 
      rc = ensure_RG_exists( syndicate_url, user_email, volume_password, volume_name, RG_name, hostname, RG_PORT, RG_key_password )
      if rc is None:
         log.error("Failed to create RG %s on the MS, rc = %s" % (RG_name, rc))
         return -errno.ENOENT
      
      # ensure our local copy is running
      rc = ensure_RG_running( syndicate_url, user_email, volume_password, volume_name, RG_name, RG_key_password )
      if rc < 0:
         log.error("Failed to ensure that UG %s is running, rc = %s" % (UG_name, rc) )
         return rc
      
   if rc >= 0:
      log.info("\n\nRG for %s running on PID %s\n" % (volume_name, rc))
   
   return 0

#-------------------------
def validate_config( config ):
   global DEFAULT_CONFIG
   
   # debugging info?
   if config['debug']:
      log.setLevel( logging.DEBUG )
      
   # required arguments
   required = ['observer_secret', 'observer_url', 'public_key']
   for req in required:
      if config.get( req, None ) == None:
         print >> sys.stderr, "Missing required argument: %s" % req
         return -1
   
   # required types 
   required_types = {
       'poll_timeout': int,
       'port': int,
   }
   
   for req, reqtype in required_types.items():
      if config.get( req, None ) is not None:
         try:
            i = reqtype( config[req] )
            config[req] = i
         except:
            print >> sys.stderr, "Invalid value for '%s'" % req
            return -1
       
   # fill defaults 
   for def_key, def_value in DEFAULT_CONFIG.items():
      if config.get( def_key, None ) is None:
         log.debug("Default: %s = %s" % (def_key, def_value) )
         
         config[def_key] = def_value
         
         
   return 0
      
      
#-------------------------
class PollThread( threading.Thread ):
   
   @classmethod 
   def poll_data( cls, config ):
      data = poll_opencloud_data( config )
      
      if data is None:
         log.error("Failed to poll data")
         
      else:
         update_config_runtime( data )
         
         # act on the data 
         rc = ensure_running()
         if rc != 0:
            log.error("ensure_running rc = %s" % rc)
         
         
   def run(self):
      """
      Continuously poll the OpenCloud observer for runtime data.
      """
      self.running = True
      while self.running:
         config = get_config()
         
         poll_timeout = config['poll_timeout']
         
         time.sleep( poll_timeout )
         
         PollThread.poll_data( config )
            
            
#-------------------------
class ReaperThread( threading.Thread ):
   
   
   def run(self):
      """
      Wait for all child processes to die, and reap their exit statuses.
      """
      self.running = True
      while self.running:
         try:
            pid, status = os.wait()
         except OSError, oe:
            if oe.errno != errno.ECHILD:
               log.error("os.wait() errno %s" % -oe.errno)
               
            time.sleep(1)
            continue
         
         if os.WIFSIGNALED( status ):
            log.info("Process %s caught signal %s" % (pid, os.WTERMSIG( status ) ))
         
         elif os.WIFEXITED( status ):
            log.info("Process %s exit status %s" % (pid, os.WEXITSTATUS( status ) ))


#-------------------------
def run_once( config ):
   rt = ReaperThread()
   rt.daemon = True
   rt.start()
   
   PollThread.poll_data( config )
   
   if config['debug']:
      import pprint 
      pp = pprint.PrettyPrinter()
      pp.pprint( config )
   
      print "UG key password: %s" % make_gateway_private_key_password( make_gateway_name( "UG", config['runtime']['volume_name'], socket.gethostname() ), config['observer_secret'] )
      print "RG key password: %s" % make_gateway_private_key_password( make_gateway_name( "RG", config['runtime']['volume_name'], socket.gethostname() ), config['observer_secret'] )

   rt.running = False

#-------------------------
def main( config ):
   # start reaping 
   rt = ReaperThread()
   rt.daemon = True
   rt.start()
   
   # start polling 
   th = PollThread()
   th.daemon = True
   th.start()
   
   config = get_config()
   
   # start listening for pushes
   httpd = BaseHTTPServer.HTTPServer( ('', config['port']), ObserverPushHandler )
   
   log.info("Listening on %s" % config['port'])
   try:
      httpd.serve_forever()
   except:
      log.info("shutting down")
      rt.running = False
      th.running = False
      sys.exit(0)


#-------------------------    
if __name__ == "__main__":
   
   argv = sys.argv
   config = modconf.build_config( argv, "Syndicate Observer Daemon", "syndicated", CONFIG_OPTIONS, conf_validator=validate_config )
   
   if config is None:
      sys.exit(-1)
   
   CONFIG = config 
   
   if config['run_once']:
      run_once( config )
      sys.exit(0)
      
   if config.get("foreground", None):
      main( config )
      
   else:
      logfile_path = None 
      pidfile_path = config.get("pidfile", None)
      
      if config.has_key("logdir"):
         logfile_path = os.path.join( config['logdir'], "syndicated.log" )
      
      daemonize.daemonize( lambda: main(config), logfile_path=logfile_path, pidfile_path=pidfile_path )
        

