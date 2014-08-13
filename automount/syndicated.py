#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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
import binascii
import setproctitle

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.ERROR )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.api as api
import syndicate.client.common.msconfig as msconfig

import syndicate.util.watchdog as watchdog
import syndicate.util.daemonize as daemon 
import syndicate.util.config as modconf
import syndicate.util.storage as storage
import syndicate.util.crypto as crypto

import syndicate.observer.cred as observer_cred
import syndicate.observer.startstop as observer_startstop

import syndicate.syndicate as c_syndicate

#-------------------------------
# system constants 
SYNDICATE_SLICE_SECRET_NAME             = "SYNDICATE_SLICE_SECRET"

# run gateways as "daemon", but have them run in the "fuse" group so they can access /dev/fuse and /etc/fuse.conf
GATEWAY_UID_NAME                        = "daemon"
GATEWAY_GID_NAME                        = "fuse"

#-------------------------------
CONFIG_OPTIONS = {
   "config":            ("-c", 1, "Path to the daemon configuration file"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-l", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-i", 1, "Path to the desired PID file."),
   "public_key":        ("-p", 1, "Path to the Observer public key."),
   "slice_name":        ("-S", 1, "Name of the slice."),
   "slice_secret":      ("-s", 1, "Shared secret with Observer for this slice."),
   "observer_url":      ("-u", 1, "URL to the Syndicate Observer"),
   "poll_timeout":      ("-t", 1, "Interval to wait between asking the Observer for our Volume credentials."),
   "mountpoint_dir":    ("-m", 1, "Directory to hold Volume mountpoints."),
   "port":              ("-P", 1, "Port to listen on for Observer-given commands."),
   "debug":             ("-d", 0, "Print debugging information."),
   "run_once":          ("-1", 0, "Poll once and exit, printing the polled data and taking no further action."),
   "RG_only":           ("-R", 0, "Only start the RG"),
   "UG_only":           ("-U", 0, "Only start the UG"),
   "RG_public":         ("-G", 0, "Make the local RG instance publicly available (no effect if --UG_only is given)"),
   "hostname":          ("-H", 1, "Hostname to provision and run gateways under.  Defaults to the contents of /etc/hostname."),
   "command":           (None, '*', "Control-plane requests.  'pids' will print the list of process IDs running at the given mountpoint.")
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
    "RG_public":        False
}

#-------------------------------

# global config structure 
CONFIG = None 
CONFIG_lock = threading.Lock()

# cached slice secret 
SLICE_SECRET = None

#-------------------------------
def get_config():
    # return a duplicate of the global config
    global CONFIG 
    
    CONFIG_lock.acquire()
    
    config = copy.deepcopy( CONFIG )
    
    CONFIG_lock.release()
    
    return config
 
 
#------------------------------- 
def get_cached_slice_secret( config ):
    # get the locally-cached slice secret 
    global SLICE_SECRET 
    
    return SLICE_SECRET 

#------------------------------- 
def get_slice_secret( config ):
    # obtain the slice secret 
    observer_url = config['observer_url']
    
    slice_secret = download_data( os.path.join( observer_url, config['slice_name'], SYNDICATE_SLICE_SECRET_NAME ) )
    
    return slice_secret

#-------------------------------
def cache_slice_secret( config, secret ):
    # cache the slice secret 
    global SLICE_SECRET 
    
    SLICE_SECRET = secret 


#-------------------------------
class EnsureRunningThread( threading.Thread ):
   """
   Process observer data and act on it, either 
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
      slice_secret = get_cached_slice_secret( config )
      
      if slice_secret is None:
         # no secret on file 
         cls.processing_lock.release()
         log.error("No slice secret")
         return -errno.EAGAIN
      
      # unseal the data 
      rc, data_text = unseal_observer_data( slice_secret, sealed_data )
      if rc != 0:
         # failed to read 
         cls.processing_lock.release()
         log.error("unseal_observer_data rc = %s" % rc)
         return -errno.EINVAL
      
      # parse the data 
      rc, data = observer_cred.parse_observer_data( data_text )
      if rc != 0:
         # failed to parse 
         cls.processing_lock.release()
         log.error("parse_observer_data rc = %s" % rc)
         return -errno.EINVAL
      
      if data is not None:
         
         # start all gateways for this volume
         rc = observer_startstop.start_stop_volume( config, data, slice_secret, hostname=config['hostname'], gateway_uid_name=GATEWAY_UID_NAME, gateway_gid_name=GATEWAY_GID_NAME, debug=config['debug'] )
      
         if rc != 0:
            log.error("ensure_running rc = %s" % rc)
      
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
        json_text = form.getfirst( observer_cred.OPENCLOUD_JSON, None )
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
        
        # data came from the observer
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
    rc, data = c_syndicate.symmetric_unseal( sealed_data, shared_secret )
    if rc != 0:
        log.error("Failed to decrypt data")
        return (-errno.EINVAL, None)
    
    # we're good!
    return (0, data)


#-------------------------------
def download_data( url ):
    """
    Download some data froma URL.
    Return the data on success.
    Return None on failure.
    """
    
    req = requests.get( url )
    if req.status_code != 200:
        log.error("GET %s status %s" % (url, req.status_code))
        return None
    
    # NOTE: some older versions of request use .content instead of .text
    try:
       return req.text
    except:
       return req.content
    

#-------------------------------
def download_validate_unseal_data( config, url ):
    """
    Download, validate, and unseal the data, given the URL to it.
    """
    
    # can't proceed unless we have the slice secret already 
    
    slice_secret = get_cached_slice_secret( config )
    
    if slice_secret is None:
        log.error("No slice secret")
        return None
    
    data = download_data( url )
    if data is None:
        log.error("Failed to download from %s" % url)
        return None 
     
    rc, sealed_data_str = read_observer_data_from_json( config['public_key'], data )
    if rc != 0:
        log.error("Failed to read JSON, rc = %s" % rc)
        return None 
    
    rc, data_str = unseal_observer_data( slice_secret, sealed_data_str )
    if rc != 0:
       log.error("Failed to unseal data, rc = %s" % rc)
       return None
    
    return data_str


#-------------------------------
def poll_opencloud_volume_list( config, slice_name ):
    """
    Download, verify, and return the list of Volumes
    this sliver should attach itself to.
    """
    url = os.path.join( config['observer_url'], slice_name )
    
    data_str = download_validate_unseal_data( config, url )
    if data_str is None:
       log.error("Failed to read data from %s" % url)
       return None 
    
    volume_list = observer_cred.parse_opencloud_volume_list( data_str )
    if volume_list is None:
       log.error("Failed to parse volume list")
       return None
    
    return volume_list


#-------------------------------
def poll_opencloud_volume_data( config, slice_name, volume_name ):
    """
    Download, verify, and parse Observer data.
    Return (0, data) on success
    Return (nonzero, None) on error 
    """
    url = config['observer_url']
    
    volume_url = os.path.join( url, slice_name, volume_name )
    
    data_str = download_validate_unseal_data( config, volume_url )
    if data_str is None:
       log.error("Failed to read data from %s" % url)
       return None
    
    rc, data = observer_cred.parse_observer_data( data_str )
    if rc != 0:
        log.error("Failed to read Observer data, rc = %s" % rc )
        return None
    
    return data 
    

#-------------------------------
def connect_syndicate( syndicate_url, principal_id, user_pkey_pem ):
    """
    Connect to the Syndicate SMI. 
    principal_id must be an email address
    """
    global CONFIG 
    debug = CONFIG.get('debug', False)
    
    client = syntool.Client( principal_id, syndicate_url, user_pkey_pem=user_pkey_pem, debug=debug )

    return client


#-------------------------
def validate_config( config ):
   global DEFAULT_CONFIG
   
   # debugging info?
   if config['debug']:
      log.setLevel( logging.DEBUG )
   else:
      log.setLevel( logging.ERROR )
   
   # required arguments
   required = ['observer_url', 'public_key', 'mountpoint_dir']
   
   # if we're not running a contorl-plane command, we need the slice name 
   if not config.has_key('command') or config['command'] is None or len(config['command']) == 0:
      required.append('slice_name')
   
   for req in required:
      if config.get( req, None ) is None:
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
       
   # either or 
   if config['UG_only'] and config['RG_only']:
      log.error("Can have UG_only or RG_only, but not both")
      return -1
   
   # fill defaults 
   for def_key, def_value in DEFAULT_CONFIG.items():
      
      if config.get( def_key, None ) is None:
         log.debug("Default: %s = %s" % (def_key, def_value) )
         
         config[def_key] = def_value
         
   
   # convert slice secret to binary, if given 
   if config.has_key('slice_secret') and config['slice_secret'] is not None:
      
      try:
         config['slice_secret'] = binascii.unhexlify( config['slice_secret'] )
      except:
         log.error("Invalid slice secret")
         return -1
   
   # verify that these directories exist, are directories, adn are writable and searchable 
   check_directories = [ config["mountpoint_dir"] ]
   
   # if not foreground, verify that log directory and pidfile exist and are searchable and writable 
   if not config.has_key('foreground') or not config['foreground']:
      
      # need these arguments 
      for required in ['logdir']:
         if config.get( required, None ) is None:
            print >> sys.stderr, "Missing required argument: %s" % required
            return -1
      
      pidfile_path = config.get("pidfile", None)
      pidfile_dir_path = None 
      
      if pidfile_path is not None:
         pidfile_dir_path = os.path.dirname( pidfile_path.rstrip("/") )
      
      logdir_path = config["logdir"]
      
      check_directories.append( logdir_path )
      check_directories.append( pidfile_dir_path )
   
   # check these directories
   for dirpath in check_directories:
      
      if dirpath is None:
         continue 
      
      if not os.path.exists( dirpath ):
         raise Exception("No such file or directory: %s" % dirpath )
      
      if not os.path.isdir( dirpath ):
         raise Exception("Not a directory: %s" % dirpath )
      
      if not os.access( dirpath, os.W_OK | os.X_OK ):
         raise Exception("Directory not writable/searchable: %s" % dirpath )
      
   return 0


      
#-------------------------
class PollThread( threading.Thread ):
   
   @classmethod 
   def poll_data( cls, config ):
      """
      Poll the Observer for data.
      Return (slice_secret, volume_data) on success
      Return (None, None) on error 
      """
      
      observer_url = config['observer_url']
      slice_name = config['slice_name']
      
      slice_secret = get_cached_slice_secret( config )
      if slice_secret is None:
         # obtain the secret 
         slice_secret = get_slice_secret( config )
         
         if slice_secret is None:
            log.error("Failed to get slice secret")
            return (None, None)
         
         cache_slice_secret( config, slice_secret )
      
      # get the list of volumes 
      volumes = poll_opencloud_volume_list( config, slice_name )
      if volumes is None:
         log.error("Failed to poll volume list")
         return (None, None) 
      
      all_volume_data = []
      ignored_volumes = []
      
      for volume in volumes:
         volume_data = poll_opencloud_volume_data( config, slice_name, volume )
         
         if volume_data is None:
            log.error("Failed to poll data for volume %s" % volume)
            ignored_volumes.append( volume )
            continue
            
         else:
            all_volume_data.append( volume_data )
      
      # act on the data--start all gateways for all volumes that are active, but stop the ones that aren't
      rc = observer_startstop.start_stop_all_volumes( config, all_volume_data, slice_secret, ignored=ignored_volumes, hostname=config['hostname'], gateway_uid_name=GATEWAY_UID_NAME, gateway_gid_name=GATEWAY_GID_NAME, debug=config['debug'] )
      if rc != 0:
         log.error("start_stop_all_volumes rc = %s" % rc)
         return (None, None)
      else:
         return (slice_secret, all_volume_data)
            
         
   def run(self):
      """
      Continuously poll the observer for runtime data.
      """
      self.running = True
      while self.running:
         
         # poll, then sleep, so we poll when we start up
         config = get_config()
         PollThread.poll_data( config )
         
         poll_timeout = config['poll_timeout']
         time.sleep( poll_timeout )
         
            
            
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
   
   slice_secret, all_volume_data = PollThread.poll_data( config )
   
   if config['debug']:
      import pprint 
      pp = pprint.PrettyPrinter()
      
      print "Slice secret: %s" % slice_secret
      print "Volume config:"
      
      pp.pprint( all_volume_data )
      
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
   except Exception, e:
      log.exception(e)
      log.info("Uncaught exception, shutting down")
      rt.running = False
      th.running = False
      sys.exit(0)


#-------------------------    
def load_config_file( paths ):
   """
   Load the config file from the first path given on the command-line.
   """
   if paths is None:
      return None 
   
   path = paths[0]
   log.info( "Loading config from %s" % path )
   config_txt = storage.read_file( path )
   
   if config_txt is None:
      raise Exception("Failed to read config from %s" % path)
   
   return config_txt


#-------------------------    
def get_pids_of_daemons_for_dir( mountpoint_dir ):
   """
   Get the PIDs of all automount daemons running on a mountpoint.
   """
   procs = watchdog.find_by_attrs( "syndicate-automount-daemon", {"mounts": mountpoint_dir} )
   
   ret = [ watchdog.get_proc_pid(p) for p in procs ]
   
   return ret


#-------------------------    
def ensure_running( config ):
   """
   Verify that there is an automount daemon servicing a mountpoint.
   If there isn't, start one.
   If we're configured to run in the foreground, this method never returns.
   """
   
   mountpoint_dir = config['mountpoint_dir']
   
   # is the daemon running?
   procs = watchdog.find_by_attrs( "syndicate-automount-daemon", {"mounts": mountpoint_dir} )
   if len(procs) > 0:
      # it's running
      print "Syndicate automount daemon already running for %s (PID(s): %s)" % (mountpoint_dir, ",".join( [str(watchdog.get_proc_pid(p)) for p in procs] ))
      return True
   
   if config.get("foreground", None):
      main( config )
      
   else:
      logfile_path = None 
      pidfile_path = config.get("pidfile", None)
      
      if config.has_key("logdir"):
         logfile_path = os.path.join( config['logdir'], "syndicated.log" )
      
      title = watchdog.attr_proc_title( "syndicate-automount-daemon", {"mounts" : mountpoint_dir} )
      setproctitle.setproctitle( title )
      
      daemon.daemonize( lambda: main(config), logfile_path=logfile_path, pidfile_path=pidfile_path )
      
      return True


#-------------------------    
def signal_all( procs, signum ):
   """
   Send signum to all of a list of processes.
   Return the ones where we couldn't deliver the signal.
   """
   
   failed = []
   
   for proc in procs:
      pid = watchdog.get_proc_pid( proc )
      
      log.info("Send signal %s to %s" % (signum, pid))
      try:
         os.kill( pid, signum )
      except OSError, oe:
         log.exception(oe)
         log.error("Failed to send signal %s to %s, errno = %s" % (signum, pid, oe.errno))
         
         failed.append( proc )

   return failed
   


#-------------------------    
def ensure_stopped( config ):
   """
   Stop all syndicated instances for this mountpoint.
   """
   
   mountpoint_dir = config['mountpoint_dir']
   
   # is the daemon running?
   procs = watchdog.find_by_attrs( "syndicate-automount-daemon", {"mounts": mountpoint_dir} )
   
   if len(procs) > 0:
      
      failed = signal_all( procs, signal.SIGTERM )
      
      # wait for signals to be delivered
      time.sleep(1.0)
      
      procs = watchdog.find_by_attrs( "syndicate-automount-daemon", {"mounts": mountpoint_dir} )
      
      if len(procs) > 0 or len(failed) > 0:
         
         failed = signal_all( procs, signal.SIGKILL )
         if len(failed) > 0:
            log.error("Failed to stop automount daemons %s" % (",".join( [str(watchdog.get_proc_pid( p )) for p in procs] )))
            
            return False 
         
   return True


#-------------------------    
if __name__ == "__main__":
   
   argv = sys.argv
   
   log.setLevel( logging.ERROR )
   
   # early enable debug logging 
   if "-d" in sys.argv or "--debug" in sys.argv:
      import syndicate.client.common.log as Log
      client_log = Log.get_logger()
      
      log.setLevel( logging.DEBUG )
      client_log.setLevel( "DEBUG" )
   
   opt_handlers = {
      "config": lambda arg: load_config_file( arg )
   }
   
   config = modconf.build_config( argv, "Syndicate Automount Daemon", "syndicated", CONFIG_OPTIONS, conf_validator=validate_config, opt_handlers=opt_handlers, config_opt="config" )
   
   if config is None:
      sys.exit(-1)
   
   # sanitize paths 
   for path_opt in ['logdir', 'mountpoint_dir']:
      if config.has_key(path_opt):
         config[path_opt] = config[path_opt].rstrip("/\\")        # directories do NOT end in / or \ 
   
   # sanitize hostname 
   if not config.has_key("hostname") or config['hostname'] is None or len(config['hostname']) == 0:
      config['hostname'] = socket.gethostname()
   
   # sanitize debug 
   if not config.has_key("debug") or config['debug'] is None:
      config['debug'] = False
   
   CONFIG = config 
   
   # process any control-plane commands
   if config.has_key('command') and config['command'] is not None and len(config['command']) > 0:
      command = config['command'][0]
      
      # did we just want to get the pids?
      if command == 'pids':
         pids = get_pids_of_daemons_for_dir( config['mountpoint_dir'] )
         for p in pids:
            print p
         
         sys.exit(0)
      
      elif command == 'stop':
         rc = ensure_stopped( config )
         
         if not rc:
            sys.exit(1)
         
         else:
            sys.exit(0)
      
      elif command == 'status':
         # get status of daemon on this mountpoint. Exit 0 if running, exit 1 if not
         pids = get_pids_of_daemons_for_dir( config['mountpoint_dir'] )
         
         if len(pids) > 0:
            sys.exit(0)
         
         else:
            sys.exit(1)
         
      else:
         log.error("Unrecognized command '%s'" % command)
         sys.exit(1)
   
   # obtain the slice secret, if one was not given 
   if config.has_key('slice_secret') and config['slice_secret'] is not None:
      cache_slice_secret( config, config['slice_secret'] )
   else:
      log.info("No slice secret given; obtaining...")
      slice_secret = get_slice_secret( config )
      
      if slice_secret is None:
         log.error("Could not obtain slice secret")
         sys.exit(1)
      
      else:
         cache_slice_secret( config, slice_secret )
   
   if config['run_once']:
      run_once( config )
      sys.exit(0)
   
   ensure_running( config )     

