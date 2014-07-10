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
import copy

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
import syndicate.client.common.api as api
import syndicate.client.common.msconfig as msconfig

import syndicate.util.watchdog as watchdog
import syndicate.util.daemonize as daemon 
import syndicate.util.config as modconf
import syndicate.util.storage as storage
import syndicate.util.crypto as crypto
import syndicate.util.provisioning as provisioning

import syndicate.syndicate as c_syndicate

#-------------------------------
# constants 
OPENCLOUD_JSON                          = "observer_message"

# message for one Volume
OPENCLOUD_VOLUME_NAME                   = "volume_name"
OPENCLOUD_VOLUME_OWNER_ID               = "volume_owner"
OPENCLOUD_VOLUME_PEER_PORT              = "volume_peer_port"
OPENCLOUD_VOLUME_USER_PKEY_PEM          = "volume_user_pkey_pem"
OPENCLOUD_SYNDICATE_URL                 = "syndicate_url"

# binary names
SYNDICATE_UG_WATCHDOG_NAME              = "syndicate-ug"
SYNDICATE_RG_WATCHDOG_NAME              = "syndicate-rg"
SYNDICATE_UG_BINARY_NAME                = "syndicatefs"

# where can we find the watchdogs?
SYNDICATE_UG_WATCHDOG_PATH              = "/home/jude/Desktop/research/git/syndicate/build/out/bin/UG/syndicate-ug"
SYNDICATE_RG_WATCHDOG_PATH              = "/home/jude/Desktop/research/git/syndicate/build/out/bin/RG/syndicate-rg"
SYNDICATE_UG_BINARY_PATH                = "/home/jude/Desktop/research/git/syndicate/build/out/bin/UG/syndicatefs"

#-------------------------------
CONFIG_OPTIONS = {
   "config":            ("-c", 1, "Path to the daemon configuration file"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-l", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-i", 1, "Path to the desired PID file."),
   "public_key":        ("-p", 1, "Path to the Observer public key."),
   "slice_secret":      ("-s", 1, "Shared secret with Observer for this slice."),
   "observer_url":      ("-u", 1, "URL to the Syndicate Observer"),
   "poll_timeout":      ("-t", 1, "Interval to wait between asking OpenCloud for our Volume credentials."),
   "mountpoint_dir":    ("-m", 1, "Directory to hold Volume mountpoints."),
   "port":              ("-P", 1, "Port to listen on."),
   "debug":             ("-d", 0, "Print debugging information."),
   "run_once":          ("-1", 0, "Poll once (for testing)"),
   "RG_only":           ("-R", 0, "Only start the RG"),
   "UG_only":           ("-U", 0, "Only start the UG"),
}

DEFAULT_CONFIG = {
    "config":           "/etc/syndicate/syndicated.conf",
    "public_key":       "/etc/syndicate/observer.pub",
    "logdir":           "/var/log/syndicated",
    "pidfile":          "/var/run/syndicated.pid",
    "poll_timeout":     43200,          # ask twice a day; the Observer should poke us directly anyway
    "slice_secret":     None,  
    "observer_url":     "https://localhost:5553",
    "mountpoint_dir":   "/tmp/syndicate-mounts",
    "port":             5553,
}

"""
    # these are filled in at runtime.
    # This is the information needed to act as a principal of the volume onwer (i.e. the slice)
    "runtime": {
        "testvolume": {
            "volume_name":          "testvolume",
            "volume_owner":         "judecn@gmail.com",
            "volume_peer_port":      32780,
            "volume_replicate_port": 32781,
            "syndicate_url":        "http://localhost:8080",
        },
        "testvolume2": {
            "volume_name":          "testvolume2",
            "volume_owner":         "padmin@vicci.org",
            "volume_peer_port":      32880,
            "volume_replicate_port": 32881,
            "syndicate_url":        "http://localhost:8080",
        }
     }
}
"""


#-------------------------------

# global config structure 
CONFIG = None 
CONFIG_lock = threading.Lock()

#-------------------------------
def get_config():
    # return a duplicate of the global config
    global CONFIG 
    
    CONFIG_lock.acquire()
    
    config = copy.deepcopy( CONFIG )
    
    CONFIG_lock.release()
    
    return config

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
      rc, data_text = unseal_observer_data( config['slice_secret'], sealed_data )
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
         rc = ensure_running( data )
         
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
    
    # get the public key 
    k = load_public_key( public_key_path )
    if k is None:
        log.error("Failed to load public key from %s" % (public_key_path))
        return (-errno.ENOENT, None)
    
    return crypto.verify_and_parse_json( k.exportKey(), json_text )
 
 
#-------------------------------
def unseal_observer_data( shared_secret, sealed_data ):
    
    # decrypt the data, using the shared secret 
    log.info("Unsealing credentials...")
    rc, data = c_syndicate.password_unseal( sealed_data, shared_secret )
    if rc != 0:
        logger.error("Failed to decrypt data")
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
        OPENCLOUD_VOLUME_PEER_PORT: [int],
        OPENCLOUD_VOLUME_USER_PKEY_PEM: [str, unicode],
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
    
    # force string 
    for req_field, types in required_fields.items():
       if type(data[req_field]) in [str, unicode]:
          log.debug("convert %s to str" % req_field)
          data[req_field] = str(data[req_field])
    
    return (0, data)
    

#-------------------------------
def parse_opencloud_volume_list( data_str ):
    """
    Parse a string representing a volume list from OpenCloud.
    """
    try:
       volume_data = json.loads( data_str )
    except Exception, e:
       log.error("Invalid Volume data")
       return None
    
    # verify it's a { "volumes": ["volume_name_1", "volume_name_2", ...] }
    try:
       
       assert volume_data.has_key( "volumes" ), "missing 'volumes' field"
       assert type(volume_data["volumes"]) == list, "'volumes' is not a list"
       
       for v in volume_data["volumes"]:
          assert type(v) == str or type(v) == unicode, "volume name must be a string"
          
    except Exception, e:
       log.error("Invalid volume data: %s" % e.message)
       return None
    
    return volume_data["volumes"]


#-------------------------------
def download_validate_unseal_data( config, url ):
    """
    Download, validate, and unseal the data, given the URL to it.
    """
    
    req = requests.get( url )
    if req.status_code != 200:
        log.error("GET %s status %s" % (url, req.status_code))
        return None
    
    rc, sealed_data_str = read_observer_data_from_json( config['public_key'], req.text )
    if rc != 0:
        log.error("Failed to read JSON, rc = %s" % rc)
        return None 
    
    rc, data_str = unseal_observer_data( config['slice_secret'], sealed_data_str )
    if rc != 0:
       log.error("Failed to unseal data, rc = %s" % rc)
       return None
    
    return data_str


#-------------------------------
def poll_opencloud_volume_list( config ):
    """
    Download, verify, and return the list of Volumes
    this sliver should attach itself to.
    """
    url = config['observer_url']
    
    data_str = download_validate_unseal_data( config, url )
    if data_str is None:
       log.error("Failed to read data from %s" % url)
       return None 
    
    volume_list = parse_opencloud_volume_list( data_str )
    if volume_list is None:
       log.error("Failed to parse volume list")
       return None
    
    return volume_list


#-------------------------------
def poll_opencloud_volume_data( config, volume_name ):
    """
    Download, verify, and parse Observer data.
    Return (0, data) on success
    Return (nonzero, None) on error 
    """
    url = config['observer_url']
    
    volume_url = os.path.join( url, volume_name )
    
    data_str = download_validate_unseal_data( config, volume_url )
    if data_str is None:
       log.error("Failed to read data from %s" % url)
       return None
    
    rc, data = parse_observer_data( data_str )
    if rc != 0:
        log.error("Failed to read OpenCloud data, rc = %s" % rc )
        return None
    
    return data 
    

#-------------------------------
def connect_syndicate( syndicate_url, user_email, user_pkey_pem ):
    """
    Connect to the Syndicate SMI
    """
    global CONFIG 
    debug = CONFIG.get('debug', False)
    
    client = syntool.Client( user_email, syndicate_url, user_pkey_pem=user_pkey_pem, debug=debug )

    return client


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
def make_UG_command_string( binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint ):
   # NOTE: run in foreground; watchdog handles the rest
   return "%s -f -m %s -u %s -p %s -v %s -g %s -K %s -P '%s' %s" % (binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint )
                           
#-------------------------------
def make_RG_command_string( binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, user_pkey_pem ):
   return "%s -m %s -u %s -p %s -v %s -g %s -K %s -P '%s'" % (binary_name, syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, user_pkey_pem)


#-------------------------------
def start_UG( syndicate_url, username, password, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_UG_command_string( SYNDICATE_UG_BINARY_NAME, syndicate_url, username, password, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint )
   
   # start the watchdog
   command_list = shlex.split( command_str ) 
   
   pid = watchdog.run( SYNDICATE_UG_WATCHDOG_NAME, [SYNDICATE_UG_WATCHDOG_NAME, '-v', volume_name, '-m', mountpoint], command_str )
   
   if pid < 0:
      log.error("Failed to make watchdog %s, rc = %s" % (SYNDICATE_UG_BINARY_NAME, pid))
   
   return pid
   

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
def start_RG( syndicate_url, username, password, volume_name, gateway_name, key_password, user_pkey_pem ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_RG_command_string( SYNDICATE_RG_WATCHDOG_NAME, syndicate_url, username, password, volume_name, gateway_name, key_password, user_pkey_pem )
   
   # start the watchdog
   command_list = shlex.split( command_str )
   
   pid = watchdog.run( command_list[0], [SYNDICATE_RG_WATCHDOG_NAME, '-R', '-v', volume_name], command_str )
   
   if pid < 0:
      log.error("Failed to make watchdog %s, rc = %s" % (command_list[0], pid))
   
   return pid
   


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
def ensure_UG_running( syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, mountpoint, user_pkey_pem, check_only=False ):
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
       logging.error("No UG running for %s on %s" % (volume_name, mountpoint))
       if not check_only:
          pid = start_UG( syndicate_url, user_email, user_password, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint )
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
def ensure_RG_running( syndicate_url, user_email, password, volume_name, gateway_name, key_password, user_pkey_pem, check_only=False ):
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
       logging.error("No RG running for %s" % (volume_name))
       if not check_only:
          pid = start_RG( syndicate_url, user_email, password, volume_name, gateway_name, key_password, user_pkey_pem )
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
def instantiate_and_run( gateway_type, gateway_name, exist_func, run_func ):
   # reserve a port
   
   try:
      # ensure the gateway exists
      rc = exist_func()
   except Exception, e:
      rc = None
      log.exception(e)
      
   if rc is None:
      log.error("Failed to create %s %s on the MS, rc = %s" % (gateway_type, gateway_name, rc))
      return rc
   
   # ensure the gateway is running
   try:
      rc = run_func()
   except Exception, e:
      rc = -errno.EAGAIN 
      log.exception(e)
      
   if rc < 0:
      log.error("Failed to ensure that %s %s is running, rc = %s" % (gateway_type, gateway_name, rc) )
      return rc
   
   return rc

#-------------------------
def ensure_running( volume_info ):
   """
   Ensure that the gateways to the volume exist,
   and that they are up and running.
   
   volume_info is a dict of {name: dict of {attr: value}}
   """
   
   config = get_config()
   
   if not config.has_key('runtime'):
      log.warning("No runtime information given!")
      return -errno.ENODATA
   
   
   slice_secret = config['slice_secret']
   mountpoint_dir = config['mountpoint_dir']
   hostname = socket.gethostname()
   
   for volume_name, volume_config in volume_info.items():
      if volume_config is None:
         continue
      
      try:
         user_email = volume_config[ OPENCLOUD_VOLUME_OWNER_ID ]
         syndicate_url = volume_config[ OPENCLOUD_SYNDICATE_URL ]
         UG_portnum = volume_config[ OPENCLOUD_VOLUME_PEER_PORT ]
         user_pkey_pem = volume_config[ OPENCLOUD_VOLUME_USER_PKEY_PEM ]
      except:
         log.error("Invalid configuration for Volume %s" % volume_name)
         continue
      
      UG_name = provisioning.make_gateway_name( "OpenCloud", "UG", volume_name, hostname )
      RG_name = provisioning.make_gateway_name( "OpenCloud", "RG", volume_name, "localhost" )
      
      UG_key_password = provisioning.make_gateway_private_key_password( UG_name, slice_secret )
      RG_key_password = provisioning.make_gateway_private_key_password( RG_name, slice_secret )
      
      UG_mountpoint_path = make_UG_mountpoint_path( mountpoint_dir, volume_name )
      
      if not config['UG_only']:
         # is the RG running?
         rc = ensure_RG_running( syndicate_url, user_email, volume_name, RG_name, RG_key_password, user_pkey_pem, check_only=True )
         if rc <= 0:
            log.error("Failed to start RG for %s, rc = %s" % (volume_name, rc))
         else:
            log.info("\n\nRG for %s running on PID %s\n\n" % (volume_name, rc))
            
         """
         if rc <= 0:
            # NOTE: in OpenCloud, the RG always runs on localhost, so the local UG can always find it.
            # There is one logical RG, but it is instantiated once per host.
            rc = instantiate_and_run( "RG", RG_name,
                                    lambda: provisioning.ensure_RG_exists( client, user_email, volume_name, RG_name, "localhost", RG_portnum, RG_key_password ),
                                    lambda: ensure_RG_running( syndicate_url, user_email, volume_name, RG_name, RG_key_password, user_pkey_pem ) )
            
            if rc < 0:
               log.error("Failed to instantiate and run RG %s" % UG_name )
               continue 
            
         if rc >= 0:
            log.info("\n\nRG for %s running on PID %s\n" % (volume_name, rc))
         """
         
      
      if not config['RG_only']:
         # is the UG running?
         rc = ensure_UG_running( syndicate_url, user_email, volume_name, UG_name, UG_key_password, UG_mountpoint_path, user_pkey_pem, check_only=True )
         
         if rc <= 0:
            client = connect_syndicate( syndicate_url, user_email, user_pkey_pem )
            
            rc = instantiate_and_run( "UG", UG_name,
                                    lambda: provisioning.ensure_UG_exists( client, user_email, volume_name, UG_name, hostname, UG_portnum, UG_key_password ),
                                    lambda: ensure_UG_running( syndicate_url, user_email, volume_name, UG_name, UG_key_password, UG_mountpoint_path, user_pkey_pem ) )
            
            if rc < 0:
               log.error("Failed to instantiate and run UG %s" % UG_name )
               continue 
         
         if rc >= 0:
            log.info("\n\nUG for %s (mounted at %s) running on PID %s\n" % (volume_name, UG_mountpoint_path, rc))
   
   return 0

#-------------------------
def validate_config( config ):
   global DEFAULT_CONFIG
   
   # debugging info?
   if config['debug']:
      log.setLevel( logging.DEBUG )
      
   # required arguments
   required = ['slice_secret', 'observer_url', 'public_key']
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
       
   #either or 
   if config['UG_only'] and config['RG_only']:
      log.error("Can have UG_only or RG_only, but not both")
      return -1
   
   # fill defaults 
   for def_key, def_value in DEFAULT_CONFIG.items():
      # skip default runtime 
      if def_key == "runtime":
         continue 
      
      if config.get( def_key, None ) is None:
         log.debug("Default: %s = %s" % (def_key, def_value) )
         
         config[def_key] = def_value
         
         
   return 0
      
      
#-------------------------
class PollThread( threading.Thread ):
   
   @classmethod 
   def poll_data( cls, config ):
      # get the list of volumes 
      volumes = poll_opencloud_volume_list( config )
      if volumes is None:
         log.error("Failed to poll volume list")
         return False 
      
      all_volume_data = {}
      
      for volume in volumes:
         volume_data = poll_opencloud_volume_data( config, volume )
         
         if volume_data is None:
            log.error("Failed to poll data for volume %s" % volume)
            continue
            
         else:
            try:
               all_volume_data[ volume_data['volume_name'] ] = volume_data
            except:
               log.error("Malformed volume data")
               continue 
      
      # act on the data 
      rc = ensure_running( all_volume_data )
      if rc != 0:
         log.error("ensure_running rc = %s" % rc)
         return False
      else:
         return True
            
         
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
      
      daemon.daemonize( lambda: main(config), logfile_path=logfile_path, pidfile_path=pidfile_path )
        

