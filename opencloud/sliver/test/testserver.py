#!/usr/bin/python

"""
Test server for syndicated.py
"""


import os
import sys
import BaseHTTPServer
import json

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig

import syndicate.util.watchdog as watchdog
import syndicate.util.daemonize as daemon 
import syndicate.util.config as modconf
import syndicate.util.storage as storage

import syndicate.syndicate as c_syndicate

# for importing syndicate_observer's syndicatelib
sys.path.insert( 0, os.path.join( os.path.dirname(__file__), "../../") )

import syndicate_observer.syndicatelib as syndicatelib

#-------------------------------
CONFIG_OPTIONS = {
   "private_key":       ("-k", 1, "Path to Observer private key."),
   "observer_secret":   ("-s", 1, "Shared secret with Observer."),
   "volume":            ("-v", 1, "Name of Volume."),
   "username":          ("-u", 1, "Name of volume owner."),
   "password":          ("-p", 1, "Volume password."),
   "MS":                ("-m", 1, "Syndicate Metadata Service URL."),
   "push":              ("-P", 1, "Push a credential blob to given host(s).  Hosts must be comma-separated."),
   "server":            ("-S", 1, "Test serving on a given IP:Port combination"),
   "UG_port":           ("-U", 1, "User Gateway port number" ),
   "RG_port":           ("-R", 1, "Replica Gateway port number" ),
   "user_pkey":         ("-K", 1, "User private key"),
}


#-------------------------------
def validate_config( config ):
   required = ["private_key", "observer_secret", "volume", "username", "password", "MS", "UG_port", "RG_port", "user_pkey"]
   missing = []
   ret = 0
   
   for req in required:
      if config.get(req, None) is None:
         missing.append(req)
         ret = -1
         
   if len(missing) > 0:
      log.error("Missing arguments: %s" % (", ".join(missing)))
      return -1
      
   # must do either a push or a server 
   if config.get("push", None) is None and config.get("server",None) is None:
      log.error("Must pass either 'push' or 'server'")
      ret = -1 
      
      
   # these must be integers 
   must_be_ints = ["UG_port", "RG_port"]
   for mbi in must_be_ints:
      try:
         config[mbi] = int(config[mbi])
      except:
         log.error("%s must be an integer" % mbi)
         ret = -1
         break
      
   return ret


#-------------------------------
def load_private_key( key_path ):
   try:
      key_text = storage.read_file( key_path )
   except Exception, e:
      log.error("Failed to read private key '%s'" % key_path )
      return None

   try:
      key = CryptoKey.importKey( key_text )
      assert key.has_private()
         
   except Exception, e:
      log.error("Failed to load private key %s'" % key_path )
      return None
   
   return key


#-------------------------------
class ObserverServerHandler( BaseHTTPServer.BaseHTTPRequestHandler ):
   
   def do_GET( self ):
      volume_name = self.path.strip("/")
      data = None 
      datatype = None
      if len(volume_name) == 0:
         # request for volume name 
         data = self.server.volume_str
         datatype = "application/json"
      
      elif volume_name == "testvolume":
         # request for credential 
         data = self.server.cred_str
         datatype = "application/json"
         
      else:
         # not found 
         self.send_response( 404 )
         self.send_header( "Content-Type", "text/plain" )
         self.wfile.write("Not found")
         self.end_headers()
         return 
      
      self.send_response( 200 )
      self.send_header( "Content-Type", datatype )
      self.send_header( "Content-Length", len(data) )
      self.end_headers()
      
      self.wfile.write( data )
      return
   
   
#-------------------------------
class ObserverServer( BaseHTTPServer.HTTPServer ):
   
   def __init__(self, cred_str, volume_str, server, req_handler ):
      self.cred_str = cred_str
      self.volume_str = volume_str
      BaseHTTPServer.HTTPServer.__init__( self, server, req_handler )
      

if __name__ == "__main__":
   argv = sys.argv
   
   config = modconf.build_config( argv, "Syndicate Test Observer Server", "syndicate-test", CONFIG_OPTIONS, conf_validator=validate_config )
   
   if config is None:
      sys.exit(-1)
   
   # get private key 
   key = load_private_key( config['private_key'] )
   if key is None:
      sys.exit(-1)
   
   key_pem = key.exportKey()
   
   # get user private key
   user_key = load_private_key( config['user_pkey'] )
   if user_key is None:
      sys.exit(-1)
      
   user_pkey_pem = user_key.exportKey()
   
   # make a volume list 
   volume_str = syndicatelib.create_volume_list_blob( key_pem, config["observer_secret"], ["testvolume"] )
   
   # make a volume credential 
   cred_str = syndicatelib.create_credential_blob( key_pem, config["observer_secret"], config["MS"], config["volume"], config["username"], config["password"], config["UG_port"], config["RG_port"], user_pkey_pem )
   
   if config.get("push", None) is not None:
      
      hosts = config["push"].split(",")
      
      syndicatelib.do_push( hosts, cred_str )
      
      # done!
      sys.exit(0)
      
      
   elif config.get("server", None) is not None:
      # serve data 
      server, portnum = config['server'].split( ":" )
      portnum = int(portnum)
      
      log.info("Serving on %s port %s" % (server, portnum))
      
      httpd = ObserverServer( cred_str, volume_str, (server, portnum), ObserverServerHandler )
      httpd.serve_forever()
      