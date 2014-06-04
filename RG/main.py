#!/usr/bin/env python

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

import sys

import syndicate.rg.common as rg_common
import syndicate.rg.closure as rg_closure
import syndicate.rg.server as rg_server

import syndicate.util.config as modconf
import syndicate.util.storage as syndicate_storage

import socket
import os
import errno
import shlex

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

log = rg_common.get_logger()

CONFIG_OPTIONS = {
   "gateway":           ("-g", 1, "The name of this RG"),
   "volume":            ("-v", 1, "The Volume this RG runs in"),
   "config":            ("-c", 1, "Path to the Syndicate configuration file for this RG"),
   "username":          ("-u", 1, "Syndicate username of the owner of this RG"),
   "password":          ("-p", 1, "If authenticating via OpenID, the Syndicate user's OpenID password"),
   "gateway_pkey_password":      ("-K", 1, "Gateway private key decryption password"),
   "MS":                ("-m", 1, "Syndicate MS URL"),
   "user_pkey":         ("-U", 1, "Path to the PEM-encoded user private key"),
   "user_pkey_pem":     ("-P", 1, "PEM-encoded user private key, as a string"),
   "volume_pubkey":     ("-V", 1, "Path to the PEM-encoded Volume public key"),
   "gateway_pkey":      ("-G", 1, "Path to the PEM-encoded RG private key"),
   "tls_pkey":          ("-T", 1, "Path to the PEM-encoded RG TLS private key.  Use if you want TLS for data transfers (might cause breakage if your HTTP caches do not expect TLS)."),
   "tls_cert":          ("-C", 1, "Path to the PEM-encoded RG TLS certificate.  Use if you want TLS for data transfers (might cause breakage if your HTTP caches do not expect TLS)."),
   "syndicate_pubkey":  ("-S", 1, "Path to the PEM-encoded Syndicate public key"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-L", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-l", 1, "Path to the desired PID file."),
   "stdin":             ("-R", 0, "Read arguments on stdin (i.e. for security)"),
   "debug_level":       ("-d", 1, "Debug level (0: nothing (default), 1: global debug, 2: global and locking debug)"),
}

#-------------------------
def validate_args( config ):
   
   # if we're reading on stdin, then proceed to do so
   if config.get('stdin'):
      return 0
   
   # required arguments
   required = ['gateway', 'MS', 'volume']
   for req in required:
      if config.get( req, None ) == None:
         print >> sys.stderr, "Missing required argument: %s" % req
         return -1
   
   # check types...
   paths = []
   for path_type in ['volume_pubkey', 'gateway_pkey', 'tls_pkey', 'tls_cert', "syndicate_pubkey"]:
      if config.get( path_type, None ) != None:
         paths.append( config[path_type] )
   
   # check paths and readability
   invalid = False
   for file_path in paths:
      if not os.path.exists( file_path ):
         log.error("Path '%s' does not exist" % (file_path))
         invalid = True
      
      else:
         try:
            fd = open(file_path, "r")
         except OSError:
            log.error("Cannot read '%s'" % (file_path))
            invalid = True
         finally:
            fd.close()
            
   # coerce integer 
   must_be_int = ['debug_level']
   for mbi in must_be_int:
      if config.get(mbi, None) != None:
         try:
            config[mbi] = int(config[mbi])
         except:
            log.error("Invalid argument '%s' for %s" % (config[mbi], mbi))
            invalid = True
            
   if invalid:
      return -1
      
   return 0
      
#-------------------------
def setup_syndicate( config ):
   
   gateway_name = config.get('gateway', None)
   rg_username = config.get('username', None)
   rg_password = config.get('password', None)
   key_password = config.get('gateway_pkey_password', None)
   ms_url = config.get('MS', None)
   user_pkey = config.get('user_pkey', None)
   user_pkey_pem = config.get('user_pkey_pem', None)
   my_key_file = config.get('gateway_pkey', None)
   volume_name = config.get('volume', None)
   volume_pubkey = config.get('volume_pubkey', None)
   tls_pkey = config.get('tls_pkey', None)
   tls_cert = config.get('tls_cert', None)
   syndicate_pubkey = config.get('syndicate_pubkey', None)
   config_file = config.get('config_file', None)
   debug_level = config.get("debug_level", 0)
   
   if user_pkey_pem is None and user_pkey is not None:
      user_pkey = syndicate_storage.read_key( user_pkey )
      if user_pkey is None:
         log.error("Failed to read %s" % user_pkey )
         return None
      
      user_pkey_pem = user_pkey.exportKey()
   
   # start up libsyndicate
   syndicate = rg_common.syndicate_init( ms_url=ms_url,
                                         gateway_name=gateway_name,
                                         volume_name=volume_name,
                                         username=rg_username,
                                         password=rg_password,
                                         user_pkey_pem=user_pkey_pem,
                                         gateway_pkey_decryption_password=key_password,
                                         gateway_pkey_path=my_key_file,
                                         config_file=config_file,
                                         volume_pubkey_path=volume_pubkey,
                                         tls_pkey_path=tls_pkey,
                                         tls_cert_path=tls_cert,
                                         debug_level=debug_level,
                                         syndicate_pubkey_path=syndicate_pubkey )
   
   return syndicate 


#-------------------------
def run_devel( hostname, portnum ):
   """
   Start the server, using the WSGI reference server (good for development)
   """
   
   log.info("Starting development server on %s:%s" % (hostname, portnum))

   from wsgiref.simple_server import make_server 
   
   # start serving
   httpd = make_server( hostname, portnum, rg_server.wsgi_handle_request )
   
   httpd.serve_forever()
   
   return 0
   

#-------------------------
def run_bjoern( hostname, portnum ):
   """
   Start the server, using the Bjoern server (https://github.com/jonashaag/bjoern)
   """
   
   log.info("Starting Bjoern server on %s:%s" % (hostname, portnum))
   
   import bjoern
   
   bjoern.run( rg_server.wsgi_handle_request, hostname, portnum )
   

#-------------------------
def build_config( argv ):
   
   gateway_name = "RG"
   gateway_desc = "Syndicate Replica Gateway"
   
   config = modconf.build_config( argv, gateway_desc, gateway_name, CONFIG_OPTIONS, conf_validator=validate_args )
   
   if config is None:
      log.error("Failed to load config")
      return None 
   
   # reading commands from stdin?
   if config.get("stdin", None) is not None and config["stdin"]:
      
      arg_string = sys.stdin.read()
      argv = shlex.split( arg_string )
      
      config = modconf.build_config( argv, gateway_desc, gateway_name, CONFIG_OPTIONS, conf_validator=validate_args )
      if config is None:
         log.error("Failed to read config from stdin")
         return None
         
      if config.get("stdin", None) is not None and config["stdin"]:
         log.error("Invalid argument: passed --stdin on stdin")
         return None
      
   return config



#-------------------------
def oneoff_init( argv ):
   # one-off initializaiton 
   
   # load config
   config = build_config( argv )
   if config is None:
      log.error("Failed to load config")
      return (-errno.ENOENT, None, None)
   
   # load syndicate
   syndicate = setup_syndicate( config )
   if syndicate is None:
      log.error("Failed to initialize syndicate")
      return (-errno.ENOTCONN, None, None)
      
   # get our key files
   my_key_file = config.get("gateway_pkey", None )
   
   # get our configuration from the MS and start keeping it up to date 
   rc = rg_closure.init( syndicate, my_key_file )
   if rc < 0:
      log.error("Failed to initialize (rc = %s)" % rc)
      return (rc, None, None)

   return (0, config, syndicate)
   

#-------------------------    
# for testing
if __name__ == "__main__":
   rc, config, syndicate = oneoff_init( sys.argv )
   if rc != 0:
      sys.exit(1)
   
   run_devel( syndicate.hostname(), syndicate.portnum() )
