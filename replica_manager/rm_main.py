#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import sys
import rm_common
import rm_config
import rm_server
import argparse
import socket

from wsgiref.simple_server import make_server 

log = rm_common.log

#-------------------------
def build_parser( progname ):
   parser = argparse.ArgumentParser( prog=progname, description="Syndicate Replica Gateway" )
   parser.add_argument( "--gateway",            "-g", nargs=1, help="The name of this RG" )
   parser.add_argument( "--volume",             "-v", nargs=1, help="The Volume to join" )
   parser.add_argument( "--config-file",        "-c", nargs=1, help="Path to config file", dest="config_file" )
   parser.add_argument( "--username",           "-u", nargs=1, help="Account username" )
   parser.add_argument( "--password",           "-p", nargs=1, help="Account password" )
   parser.add_argument( "--port",               "-P", nargs=1, help="Port to listen on" )
   parser.add_argument( "--MS",                 "-m", nargs=1, help="Syndicate MS URL" )
   parser.add_argument( "--volume-pubkey",      "-V", nargs=1, help="Path to the PEM-encoded Volume public key", dest="volume_pubkey" )
   parser.add_argument( "--gateway-pkey",       "-G", nargs=1, help="Path to the PEM-encoded private key for this RG", dest="gateway_pkey" )
   parser.add_argument( "--tls-pkey",           "-S", nargs=1, help="Path to the PEM-encoded TLS private key to use", dest="tls_pkey" )
   parser.add_argument( "--tls-cert",           "-C", nargs=1, help="Path to the PEM-encoded TLS certificate to use", dest="tls_cert" )
   parser.add_argument( "--foreground",         "-f", nargs=0, help="Run in the foreground" )
   
   return parser

#-------------------------
def usage( parser, exit_code=0 ):
   opts.print_help()
   sys.exit( exit_code )

#-------------------------
def validate_args( parser ):
   
   opts = parser.parse_args()
   
   # check types...
   try:
      gateway_portnum = int(opts.port)
   except:
      log.error("Invalid port '%s'" % opts.port)
      usage( parser, 1 )
   
   # check paths...
   invalid = False
   for file_path in [opts.gateway_pkey, opts.volume_pubkey, opts.tls_pkey, opts.tls_cert, opts.config_file]:
      if file_path != None:
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
               close(fd)
         
   if invalid:
      usage( parser, 1 )
      
   return opts
      
#-------------------------
def run( opts ):
   
   gateway_portnum = int(opts.port)
   gateway_name = opts.gateway
   rg_username = opts.username
   rg_password = opts.password
   ms_url = opts.MS
   my_key_file = opts.gateway_pkey
   volume_name = opts.volume
   volume_pubkey = opts.volume_pubkey
   tls_pkey = opts.tls_pkey
   tls_cert = opts.tls_cert
   config_file = opts.config_file
   
   # start up libsyndicate
   syndicate = rm_comon.syndicate_init( ms_url=opts.ms_url,
                                        gateway_name=opts.gateway,
                                        portnum=opts.port,
                                        volume_name=opts.volume,
                                        gateway_cred=opts.username,
                                        gateway_pass=opts.password,
                                        my_key_filename=opts.gateway_pkey,
                                        conf_filename=opts.config_file,
                                        volume_key_filename=opts.volume_pubkey,
                                        tls_pkey_filename=opts.tls_pkey,
                                        tls_cert_filename=opts.tls_cert )
   
   # get our hostname
   hostname = socket.gethostname()
   
   # get our configuration from the MS and start keeping it up to date 
   rm_config.init( syndicate )

   # start serving
   httpd = make_server( hostname, gateway_portnum, rm_server.wsgi_application )
   
   httpd.serve_forever()
   
   return True
   

#-------------------------
def debug():
   log = rm_common.log
   
   rm_common.syndicate_lib_path( "../python" )
   
   gateway_name = "RG-t510-0-690"
   gateway_portnum = 24160
   rg_username = "jcnelson@cs.princeton.edu"
   rg_password = "nya!"
   ms_url = "http://localhost:8080/"
   my_key_file = "../../../replica_manager/test/replica_manager_key.pem"
   volume_name = "testvolume-jcnelson-cs.princeton.edu"
   
   # start up libsyndicate
   syndicate = rm_common.syndicate_init( ms_url=ms_url, gateway_name=gateway_name, portnum=gateway_portnum, volume_name=volume_name, gateway_cred=rg_username, gateway_pass=rg_password, my_key_filename=my_key_file )
   
   # start up config
   rm_config.init( syndicate )
   
   # start serving!
   httpd = make_server( "t510", gateway_portnum, rm_server.wsgi_application )
   
   httpd.serve_forever()
   
   return True 

#-------------------------    
if __name__ == "__main__":
   debug()
