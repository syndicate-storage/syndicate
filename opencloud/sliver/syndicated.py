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
import SimpleHTTPServer
import base64
import json 
import errno
import requests
import threading

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig
import syndicate.util.watchdog as watchdog

import syndicate.syndicate as c_syndicate

#-------------------------------
# constants 
OPENCLOUD_JSON                          = "opencloud_json"
OPENCLOUD_JSON_DATA                     = "opencloud_json_data"
OPENCLOUD_JSON_SIGNATURE                = "opencloud_json_signature"

OPENCLOUD_VOLUME_ID                     = "volume_id"
OPENCLOUD_VOLUME_PASSWORD               = "volume_password"
OPENCLOUD_SYNDICATE_URL                 = "syndicate_url"

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
   "mountpoint-dir":    ("-m", 1, "Directory to hold Volume mountpoints.")
}

DEFAULT_CONFIG = {
    "config":           "/etc/syndicate/syndicated.conf",
    "public-key":       "/etc/syndicate/observer.pub",
    "logdir":           "/var/log/syndicated",
    "pidfile":          "/var/run/syndicated.pid",
    "poll-timeout":     86400,          # ask once a day; the Observer should poke us directly anyway
    "observer-secret":  "woo",  
    "observer-url":     "https://localhost:5553",
    "mountpoint-dir":   "/tmp/syndicate-mounts",
    
    # these are filled in at runtime.
    # THis is the information needed to act as a principal of the volume onwer (i.e. the slice)
    "runtime": {
        "volume_owner":         "judecn@gmail.com",
        "volume_password":      "nya!",
        "syndicate_url":        "http://localhost:8080",
        "public-key":           None            # instantiated from CryptoKey
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
class CommandHandler( SimpleHTTPServer.SimpleHTTPRequestHanlder ):
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
        
        update_config_runtime( data )
        
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
        OPENCLOUD_VOLUME_ID: [str, unicode],
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
def ensure_gateway_exists( config, gateway_type, user_email, volume_name, host, port, closure=None ):
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
    
    gateway_name = make_gateway_name( gateway_type, volume_name, host )

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
        kw = {}
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
    
    gateway_name = make_gateway_name( gateway_type, volume_name, host )

    client.delete_gateway( gateway_name )
    return True


#-------------------------------
def ensure_UG_exists( config, user_email, volume_name, host, port=32780 ):
    """
    Ensure that a particular UG exists.
    """
    return ensure_gateway_exists( config, "UG", user_email, volume_name, host, port )


#-------------------------------
def ensure_RG_exists( config, user_email, volume_name, host, port=32800, closure=None ):
    """
    Ensure that a particular RG exists.
    """
    return ensure_gateway_exists( config, "RG", user_email, volume_name, host, port, closure=closure )


#-------------------------------
def ensure_UG_absent( config, user_email, volume_name, host ):
    """
    Ensure that a particular UG does not exist
    """
    return ensure_gateway_absent( config, "UG", user_email, volume_name, host )


#-------------------------------
def ensure_UG_absent( config, user_email, volume_name, host ):
    """
    Ensure that a particular RG does not exist
    """
    return ensure_gateway_absent( config, "RG", user_email, volume_name, host )


#-------------------------------
def make_UG_mountpoint_path( config, volume_name ):
    """
    Generate the path to a mountpoint.
    """
    vol_dirname = volume_name.replace("/", ".")
    vol_mountpoint = os.path.join( config['mountpoint-dir'], vol_dirname )
    return vol_mountpoint


#-------------------------------
def ensure_dir_exists( path ):
    """
    Create a directory, and ensure it exists
    """
    try:
        os.makedirs( path )
    except OSError, oe:
        if oe.errno == errno.EEXIST:
            # must be a dir 
            if os.path.isdir( path ):
                return 0
            else:
                return -errno.ENOTDIR
            
        else:
            return -oe.errno
    

#-------------------------------
def ensure_UG_running( config, volume_name, gateway_name, mountpoint ):
    """
    Ensure that a User Gateway is running on a particular mountpoint.
    Return the PID on success.
    Return negative on error.
    """
    
    # make sure a mountpoint exists
    rc = make_UG_mountpoint( mountpoint )
    if rc != 0:
        return rc
    
    # is there a UG running?
    
    
    pass 


#-------------------------------
def ensure_UG_stopped( config, volume_name, gateway_name, mountpoint ):
    pass 


#-------------------------------
def ensure_RG_running( config, volume_name, gateway_name ):
    pass 

#-------------------------------
def ensure_RG_stopped( config, volume_name, gateway_name ):
    pass

#-------------------------------
def daemonize( config, main_method ):
   """
   Become a daemon.  Run our main method.
   """
   signal_map = {
      signal.SIGTERM: 'terminate',
      signal.SIGHUP: None
   }
   
   daemon_gid = grp.getgrnam('daemon').gr_gid
   
   output_fd = None
   error_fd = None
   
   if config.get("logdir", None) != None:
      # create log files
      output_fd = open( os.path.join(config["logdir"], "syndicated.log"), "w+" )
      error_fd = output_fd 
      
      os.dup2( output_fd, sys.stdout )
      os.dup2( output_fd, sys.stderr )
      
   else:
      # write to stdout and stderr
      output_fd = sys.stdout 
      error_fd = sys.stderr
   
   context = daemon.DaemonContext( umask=0o002, prevent_core=True, signal_map=signal_map )
   
   # don't close these if they're open...
   files_preserve = []
   if output_fd:
      files_preserve.append(output_fd)
   if error_fd and error_fd != output_fd:
      files_preserve.append(error_fd)
   
   context.files_preserve = files_preserve
   
   # pid file?
   pidfile_path = config.get("pidfile", None) 
   if pidfile_path:
      context.pidfile = lockfile.FileLock(pidfile_path)
   
   # start up
   with context:
      main_method()


#-------------------------
def load_config( config_str, opts ):
   
   config = None 
   
   if config_str:
      config = ConfigParser.SafeConfigParser()
      config_fd = StringIO.StringIO( config_str )
      config_fd.seek( 0 )
      
      try:
         config.readfp( config_fd )
      except Exception, e:
         log.exception( e )
         return None
   
   ret = {}
   ret["_in_argv"] = []
   ret["_in_config"] = []
   
   # convert to dictionary, merging in argv opts
   for arg_opt in CONFIG_OPTIONS.keys():
      if hasattr(opts, arg_opt) and getattr(opts, arg_opt) != None:
         ret[arg_opt] = getattr(opts, arg_opt)
         
         # force singleton...
         if isinstance(ret[arg_opt], list) and len(ret[arg_opt]) == 1 and CONFIG_OPTIONS[arg_opt][1] == 1:
            ret[arg_opt] = ret[arg_opt][0]
            
         ret["_in_argv"].append( arg_opt )
      
      elif config != None and config.has_option("syndicated", arg_opt):
         ret[arg_opt] = config.get("syndicated", arg_opt)
         
         ret["_in_config"].append( arg_opt )
   
   return ret

#-------------------------
def build_parser( progname ):
   parser = argparse.ArgumentParser( prog=progname, description="Syndicate control daemon" )
   
   for (config_option, (short_option, nargs, config_help)) in CONFIG_OPTIONS.items():
      if not isinstance(nargs, int) or nargs >= 1:
         if short_option:
            # short option means 'typical' argument
            parser.add_argument( "--" + config_option, short_option, metavar=config_option, nargs=nargs, help=config_help)
         else:
            # no short option (no option in general) means accumulate
            parser.add_argument( config_option, metavar=config_option, type=str, nargs=nargs, help=config_help)
      else:
         # no argument, but mark its existence
         parser.add_argument( "--" + config_option, short_option, action="store_true", help=config_help)
   
   return parser


#-------------------------
def validate_args( config ):
   
   # required arguments
   required = ['observer-secret', 'observer-url', 'public-key']
   for req in required:
      if config.get( req, None ) == None:
         raise Exception("Missing required argument: %s" % req )
   
   # required types 
   required_types = {
       'poll-timeout': int,
   }
   
   for req, reqtype in required_types.items():
       try:
           i = reqtype( config[req] )
           config[req] = i
       except:
           raise Exception("Invalid value for '%s'" % req )
       
   return True
      
#-------------------------
def build_config( argv ):
   
   parser = build_parser( argv[0] )
   opts = parser.parse_args( argv[1:] )
   config = load_config( None, opts )
   
   if config == None:
      log.error("Failed to load configuration")
      parser.print_help()
      sys.exit(1)
   
   rc = validate_args( config )
   if not rc:
      log.error("Invalid arguments")
      parser.print_help()
      sys.exit(1)
      
   return config
      

#-------------------------
def poll_thread( config ):
    """
    Continuously poll the OpenCloud observer for runtime data.
    """
    while True:
        pass

#-------------------------
def main( config ):
   
   poll_timeout = config['poll-timeout']
   
   # start our listening server...
   
   
   return True

#-------------------------    
if __name__ == "__main__":
   config = build_config( argv )
   
   if config.get("foreground", None):
        main( config )
   else:
        daemonize( config, lambda: main(config) )
        

