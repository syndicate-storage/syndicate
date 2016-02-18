#!/usr/bin/env python

"""
   Copyright 2015 The Trustees of Princeton University

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

import syndicate.util.config as conf
import syndicate.ms.jsonrpc as jsonrpc
import syndicate.ms.msconfig as msconfig

import os 
import urlparse

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

log = conf.log


def warn_key_change( config ):
   """
   return a warning string the user that the MS's public key has changed, and exit.
   """
   return """
SECURE VERIFICATION FAILURE!

It's possible that someone is impersonating your Syndicate, to get you to leak sensitive data!
If you are certain this is not the case, you should remove the offending public key.

Offending public key path:  %s
""" % conf.object_key_path( config, "syndicate", make_ms_url( config['syndicate_host'], config['syndicate_port'], config['no_tls'] ).strip("https://"), public=True )


def parse_url( url ):
   """
   Given a URL, find its host and port,
   and determine based on the protocol scheme 
   whether or not it uses TLS.
   Return (host, port, tls) on success 
   Raise exception if invalid.
   """
   
   if "://" not in url:
      log.warning("No scheme for %s.  Assuming https://" % url)
      url = "https://" + url
      
   parsed = urlparse.urlparse( url )
   
   scheme = parsed.scheme 
   netloc = parsed.netloc
   
   host = None
   port = None 
   no_tls = None
   
   if ":" in netloc:
      host, port = netloc.split(":")
      try:
         port = int(port)
      except:
         raise Exception("Invalid URL %s" % config['url'])
      
   else:
      host = netloc 
   
   if scheme.lower() == 'http':
      if port is None:
         port = 80
      
      no_tls = True
      
   elif scheme.lower() == 'https':
      port = 443
      no_tls = False
   
   else:
      raise Exception("Unrecognized scheme in URL %s" % config['url'] )
   
   return (host, port, no_tls)


def make_ms_url( syndicate_host, syndicate_port, no_tls, urlpath="" ):
   """
   Make a URL to the MS.
   Return the URL.
   """
   scheme = "https://"
   default_port = 80
   
   if no_tls:
      default_port = 443
      scheme = "http://"
      
   if syndicate_port != default_port:
      return scheme + os.path.join( syndicate_host.strip("/") + ":%s" % syndicate_port, urlpath )
   else:
      return scheme + os.path.join( syndicate_host.strip("/"), urlpath )
  

def api_call_signer( signing_pkey, method_name, data ):
   """
   Sign an RPC call with the user's private key
   """
   
   # sign the data
   h = HashAlg.new( data )
   signer = CryptoSigner.new( signing_pkey )
   signature = signer.sign( h )

   return signature 
   
   
def api_call_verifier( config, pubkey, method_name, data, syndicate_data, rpc_result ):
   """
   Verify an RPC call.
   """
   
   # sanity check
   if not 'signature' in syndicate_data:
      log.error("No signature in server reply")
      return False 
   
   sig = syndicate_data['signature']
   
   # verify object ID and type
   ret = False
   
   if pubkey is not None:
      
      # verify this data
      h = HashAlg.new( data )
      verifier = CryptoSigner.new(pubkey)
      ret = verifier.verify( h, sig )
   
      if not ret:
         # verification key has changed on the MS
         print warn_key_change( config )
         return False
   
      else:
         return True
   
   else:
      raise Exception("No public key given.  Unable to verify result.")


def make_rpc_client( config ):
   """
   Create an RPC client for calling MS methods.
   Requires a config dictionary with:
   * syndicate_host 
   * syndicate_port 
   * no_tls 
   * syndicate_public_key
   * user_pkey
   """
   
   ms_url = make_ms_url( config['syndicate_host'], config['syndicate_port'], config['no_tls'] ) + "/API"
   
   username = config['username']
   user_private_key = config['user_pkey']
   syndicate_public_key = config['syndicate_public_key']
   
   if not ms_url.lower().startswith("https://"):
      log.warning("MS URL %s is NOT secure!" % ms_url )
   
   signer = lambda method_name, data: api_call_signer( user_private_key, method_name, data )
   verifier = lambda method_name, args, kw, data, syndicate_data, rpc_result: api_call_verifier( config, syndicate_public_key, method_name, data, syndicate_data, rpc_result )
   
   json_client = jsonrpc.Client( ms_url, jsonrpc.VERSION, signer=signer, verifier=verifier, username=username )
      
   return json_client

