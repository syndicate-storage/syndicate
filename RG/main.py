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

import socket
import os

from wsgiref.simple_server import make_server 

log = rg_common.get_logger()

CONFIG_OPTIONS = {
   "gateway":           ("-g", 1, "The name of this RG"),
   "volume":            ("-v", 1, "The Volume this RG runs in"),
   "config":            ("-c", 1, "Path to the Syndicate configuration file for this RG"),
   "username":          ("-u", 1, "Syndicate username of the owner of this RG"),
   "password":          ("-p", 1, "If authenticating via OpenID, the Syndicate user's OpenID password"),
   "key_password":      ("-K", 1, "Gateway private key decryption password"),
   "MS":                ("-m", 1, "Syndicate MS URL"),
   "sender_pubkey":     ("-U", 1, "Path to the PEM-encoded public key to verify closures for this RG"),
   "volume_pubkey":     ("-V", 1, "Path to the PEM-encoded Volume public key"),
   "gateway_pkey":      ("-G", 1, "Path to the PEM-encoded RG private key"),
   "tls_pkey":          ("-T", 1, "Path to the PEM-encoded RG TLS private key.  Use if you want TLS for data transfers (might cause breakage if your HTTP caches do not expect TLS)."),
   "tls_cert":          ("-C", 1, "Path to the PEM-encoded RG TLS certificate.  Use if you want TLS for data transfers (might cause breakage if your HTTP caches do not expect TLS)."),
   "syndicate_pubkey":  ("-S", 1, "Path to the PEM-encoded Syndicate public key"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-L", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-l", 1, "Path to the desired PID file.")
}

#-------------------------
def validate_args( config ):
   
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
         
   if invalid:
      return -1
      
   return 0
      
#-------------------------
def setup_syndicate( config ):
   
   gateway_name = config.get('gateway', None)
   rg_username = config.get('username', None)
   rg_password = config.get('password', None)
   key_password = config.get('key_password', None)
   ms_url = config.get('MS', None)
   my_key_file = config.get('gateway_pkey', None)
   volume_name = config.get('volume', None)
   volume_pubkey = config.get('volume_pubkey', None)
   tls_pkey = config.get('tls_pkey', None)
   tls_cert = config.get('tls_cert', None)
   syndicate_pubkey = config.get('syndicate_pubkey', None)
   config_file = config.get('config_file', None)
   
   # start up libsyndicate
   syndicate = rg_common.syndicate_init( ms_url=ms_url,
                                         gateway_name=gateway_name,
                                         volume_name=volume_name,
                                         username=rg_username,
                                         password=rg_password,
                                         gateway_pkey_decryption_password=key_password,
                                         gateway_pkey_path=my_key_file,
                                         config_file=config_file,
                                         volume_pubkey_path=volume_pubkey,
                                         tls_pkey_path=tls_pkey,
                                         tls_cert_path=tls_cert,
                                         syndicate_pubkey_path=syndicate_pubkey )
   
   return syndicate 

#-------------------------
def run( config, syndicate ):

   # get our hostname
   hostname = socket.gethostname()
   
   # get our key files
   my_key_file = config.get("gateway_pkey", None )
   sender_pubkey_file = config.get("sender_pubkey", None )
   
   # get our configuration from the MS and start keeping it up to date 
   rc = rg_closure.init( syndicate, my_key_file, sender_pubkey_file )
   if rc < 0:
       log.error("Failed to initialize (rc = %s)" % rc)
       return rc

   # start serving
   httpd = make_server( hostname, syndicate.portnum(), rg_server.wsgi_application )
   
   httpd.serve_forever()
   
   return 0
   

#-------------------------
def debug():
   
   rg_common.syndicate_lib_path( "../python" )
   
   gateway_name = "RG-t510-0-690"
   rg_username = "jcnelson@cs.princeton.edu"
   rg_password = "nya!"
   ms_url = "http://localhost:8080/"
   my_key_file = "../../../replica_manager/test/replica_manager_key.pem"
   sender_pubkey_file = "../../../../ms/tests/user_test_key.pub"
   volume_name = "testvolume-jcnelson-cs.princeton.edu"
   
   # start up libsyndicate
   syndicate = rg_common.syndicate_init( ms_url=ms_url, gateway_name=gateway_name, volume_name=volume_name, username=rg_username, password=rg_password, gateway_pkey_path=my_key_file )
   
   # start up config
   rg_closure.init( syndicate, my_key_file, sender_pubkey_file )
   
   # start serving!
   httpd = make_server( "t510", syndicate.portnum(), rg_server.wsgi_application )
   
   httpd.serve_forever()
   
   return 0 

#-------------------------
def build_config( argv ):
   return modconf.build_config( argv, "Syndicate Replica Gateway", "RG", CONFIG_OPTIONS, conf_validator=validate_args )
      
#-------------------------
def main( config, syndicate=None ):
   if syndicate == None:
      syndicate = setup_syndicate( config )
      
   return run( config, syndicate )
   #debug()
   

#-------------------------    
if __name__ == "__main__":
   config = build_config( sys.argv )
   if config is None:
      sys.exit(1)
      
   main( config )
