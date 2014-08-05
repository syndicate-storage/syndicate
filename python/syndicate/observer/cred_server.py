
"""
   Copyright 2014 The Trustees of Princeton University

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
import json
import time
import traceback
import base64
import BaseHTTPServer
import setproctitle
import threading

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.util.storage as syndicate_storage_api
import syndicate.util.watchdog as syndicate_watchdog
import syndicate.util.daemonize as syndicate_daemon
import syndicate.util.config as modconf

import syndicate.observer.core as observer_core
import syndicate.observer.cred as observer_cred

CONFIG = observer_core.get_config()
observer_storage = observer_core.get_observer_storage()

#-------------------------------
class CredentialServerHandler( BaseHTTPServer.BaseHTTPRequestHandler ):
   """
   HTTP server handler that allows syndicated.py instances to poll
   for volume state.
   
   NOTE: this is a fall-back mechanism.  The observer should push new 
   volume state to the slices' slivers.  However, if that fails, the 
   slivers are configured to poll for volume state periodically.  This 
   server allows them to do just that.
   
   Responses:
      GET /<slicename>              -- Reply with the signed sealed list of volume names, encrypted by the slice secret
      GET /<slicename>/<volumename> -- Reply with the signed sealed volume access credentials, encrypted by the slice secret
      
      !!! TEMPORARY !!!
      GET /<slicename>/SYNDICATE_SLICE_SECRET    -- Reply with the slice secret (TEMPORARY)
   
   
   NOTE: We want to limit who can learn which Volumes a slice can access, so we'll seal its slivers'
   credentials with the SliceSecret secret.  The slivers (which have the slice-wide secret) can then decrypt it.
   However, sealing the listing is a time-consuming process (on the order of 10s), so we only want 
   to do it when we have to.  Since *anyone* can ask for the ciphertext of the volume list,
   we will cache the list ciphertext for each slice for a long-ish amount of time, so we don't
   accidentally DDoS this server.  This necessarily means that the sliver might see a stale
   volume listing, but that's okay, since the Observer is eventually consistent anyway.
   """
   
   cached_volumes_json = {}             # map slice_name --> (volume name, timeout)
   cached_volumes_json_lock = threading.Lock()
   
   CACHED_VOLUMES_JSON_LIFETIME = 3600          # one hour
   
   SLICE_SECRET_NAME = "SYNDICATE_SLICE_SECRET"
   
   def parse_request_path( self, path ):
      """
      Parse the URL path into a slice name and (possibly) a volume name or SLICE_SECRET_NAME
      """
      path_parts = path.strip("/").split("/")
      
      if len(path_parts) == 0:
         # invalid 
         return (None, None)
      
      if len(path_parts) > 2:
         # invalid
         return (None, None)
      
      slice_name = path_parts[0]
      if len(slice_name) == 0:
         # empty string is invalid 
         return (None, None)
      
      volume_name = None
      
      if len(path_parts) > 1:
         volume_name = path_parts[1]
         
      return slice_name, volume_name
   
   
   def reply_data( self, data, datatype="application/json" ):
      """
      Give back a 200 response with data.
      """
      self.send_response( 200 )
      self.send_header( "Content-Type", datatype )
      self.send_header( "Content-Length", len(data) )
      self.end_headers()
      
      self.wfile.write( data )
      return 
   
   
   def get_volumes_message( self, private_key_pem, observer_secret, slice_name ):
      """
      Get the json-ized list of volumes this slice is attached to.
      Check the cache, evict stale data if necessary, and on miss, 
      regenerate the slice volume list.
      """
      
      # block the cache.
      # NOTE: don't release the lock until we've generated credentials.
      # Chances are, there's a thundering herd of slivers coming online.
      # Block them all until we've generated their slice's credentials,
      # and then serve them the cached one.
      
      self.cached_volumes_json_lock.acquire()
      
      ret = None
      volume_list_json, cache_timeout = self.cached_volumes_json.get( slice_name, (None, None) )
      
      if (cache_timeout is not None) and cache_timeout < time.time():
         # expired
         volume_list_json = None
      
      if volume_list_json is None:
         # generate a new list and cache it.
         
         volume_names = observer_storage.get_volumeslice_volume_names( slice_name )
         if volume_names is None:
            # nothing to do...
            ret = None
         
         else:
            # get the slice secret 
            slice_secret = observer_storage.get_slice_secret( private_key_pem, slice_name )
            
            if slice_secret is None:
               # no such slice 
               logger.error("No slice secret for %s" % slice_name)
               ret = None
            
            else:
               # seal and sign 
               ret = observer_cred.create_volume_list_blob( private_key_pem, slice_secret, volume_names )
         
         # cache this 
         if ret is not None:
            self.cached_volumes_json[ slice_name ] = (ret, time.time() + self.CACHED_VOLUMES_JSON_LIFETIME )
      
      else:
         # hit the cache
         ret = volume_list_json
      
      self.cached_volumes_json_lock.release()
      
      return ret
      
   
   def do_GET( self ):
      """
      Handle one GET
      """
      slice_name, volume_name = self.parse_request_path( self.path )
      
      # valid request?
      if volume_name is None and slice_name is None:
         self.send_error( 400 )
      
      # slice secret request?
      elif volume_name == self.SLICE_SECRET_NAME and slice_name is not None:
         
         # get the slice secret 
         ret = observer_storage.get_slice_secret( self.server.private_key_pem, slice_name )
         
         if ret is not None:
            self.reply_data( ret )
            return 
         else:
            self.send_error( 404 )
      
      # volume list request?
      elif volume_name is None and slice_name is not None:
         
         # get the list of volumes for this slice
         ret = self.get_volumes_message( self.server.private_key_pem, self.server.observer_secret, slice_name )
         
         if ret is not None:
            self.reply_data( ret )
            return
         else:
            self.send_error( 404 )
      
      # volume credential request?
      elif volume_name is not None and slice_name is not None:
         
         # get the VolumeSlice record
         vs = observer_storage.get_volumeslice( volume_name, slice_name )
         if vs is None:
            # not found
            self.send_error( 404 )
            return
         
         else:
            ret = vs.credentials_blob 
            if ret is not None:
               self.reply_data( vs.credentials_blob )
            else:
               # not generated???
               print ""
               print vs
               print ""
               self.send_error( 503 )
            return
         
      else:
         # shouldn't get here...
         self.send_error( 500 )
         return 
   
   
#-------------------------------
class CredentialServer( BaseHTTPServer.HTTPServer ):
   
   def __init__(self, private_key_pem, observer_secret, server, req_handler ):
      self.private_key_pem = private_key_pem
      self.observer_secret = observer_secret
      BaseHTTPServer.HTTPServer.__init__( self, server, req_handler )


#-------------------------------
def credential_server_spawn( old_exit_status ):
   """
   Start our credential server (i.e. in a separate process, started by the watchdog)
   """
   
   setproctitle.setproctitle( "syndicate-credential-server" )
   
   private_key = syndicate_storage_api.read_private_key( CONFIG.SYNDICATE_OBSERVER_PRIVATE_KEY )
   if private_key is None:
      # exit code 255 will be ignored...
      logger.error("Cannot load private key.  Exiting...")
      sys.exit(255)
   
   logger.info("Starting Syndicate Observer credential server on port %s" % CONFIG.SYNDICATE_OBSERVER_HTTP_PORT)
               
   srv = CredentialServer( private_key.exportKey(), observer_core.get_syndicate_observer_secret( CONFIG.SYNDICATE_OBSERVER_SECRET ), ('', CONFIG.SYNDICATE_OBSERVER_HTTP_PORT), CredentialServerHandler)
   srv.serve_forever()


#-------------------------------
def ensure_credential_server_running( foreground=False, run_once=False ):
   """
   Instantiate our credential server and keep it running.
   """
   
   # is the watchdog running?
   pids = syndicate_watchdog.find_by_attrs( "syndicate-credential-server-watchdog", {} )
   if len(pids) > 0:
      # it's running
      return True
   
   if foreground:
      # run in foreground 
      
      if run_once:
         return credential_server_spawn( 0 )
      
      else:
         return syndicate_watchdog.main( credential_server_spawn, respawn_exit_statuses=range(1,254) )
      
   
   # not running, and not foregrounding.  fork a new one
   try:
      watchdog_pid = os.fork()
   except OSError, oe:
      logger.error("Failed to fork, errno = %s" % oe.errno)
      return False
   
   if watchdog_pid != 0:
      
      # child--become watchdog
      setproctitle.setproctitle( "syndicate-credential-server-watchdog" )
      
      if run_once:
         syndicate_daemon.daemonize( lambda: credential_server_spawn(0), logfile_path=getattr(CONFIG, "SYNDICATE_HTTP_LOGFILE", None) )
      
      else:
         syndicate_daemon.daemonize( lambda: syndicate_watchdog.main( credential_server_spawn, respawn_exit_statuses=range(1,254) ), logfile_path=getattr(CONFIG, "SYNDICATE_HTTP_LOGFILE", None) )


#-------------------------------
# Begin functional tests.
# Any method starting with ft_ is a functional test.
#-------------------------------
  
#-------------------------------
def ft_credential_server():
   """
   Functional test for the credential server
   """
   ensure_credential_server_running( run_once=True, foreground=True )
   

#-------------------------------   
if __name__ == "__main__":
   CONFIG_OPTIONS = {
      "run_once":          ("-1", 0, "Run the server once.  Do not spawn a watchdog."),
      "foreground":        ("-f", 0, "Run the foreground.  Do not daemonize."),
      "tests":             ("-t", 0, "Run functional tests and exit.")
   }
   
   config = modconf.build_config( sys.argv, "Syndicate Credential Server", "credserver", CONFIG_OPTIONS )
   
   if config is None:
      log.error("Failed to load config")
      sys.exit(1)
   
   if config['tests']:
      ft_credential_server()
      sys.exit(0)
   
   else:
      ensure_credential_server_running( foreground=config['foreground'], run_once=config['run_once'] )
      
