#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import os
import json

import sys
import tempfile
import base64
import stat

import syndicate.conf as conf 
import syndicate.storage as storage
import syndicate.common.jsonrpc as jsonrpc
import syndicate.log as Log
import syndicate.common.msconfig as msconfig
import syndicate.common.api as api

import traceback

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import pprint 

CONFIG = None

# the private and public keys we're using to sign and verify requests
SIGNING_PKEY = None
VERIFYING_PUBKEY = None

# the object we're processing
OBJECT_SIGNING_KEY_NAME = None
OBJECT_SIGNING_KEY_TYPE = None

FORCE_USER_KEY_NAME = None

log = Log.log


# -------------------
def verify_key_from_method_result( method_result ):
   if isinstance(method_result, dict):
      if 'verify_public_key' in method_result:
         return method_result['verify_public_key']
      
   return None

# -------------------
def warn_verification_key_change():
   
   print """
SECURE VERIFICATION FAILURE!

It's possible that of the following happened:
   * Someone is impersonating your MS, to get you to leak sensitive data.
   * Your user account has been tampered with.
   * Someone else deleted and then recreated the object you're working in.
   
If you are sure that no one is impersonating your MS, and that your account
has not been tampered with, you can remove the offending verification
key with the 'untrust' subcommand.
"""

   sys.exit(1)

# -------------------
def prompt_trust_verify_key( key_type, key_name, pubkey ):
   print """
%s ACCESSED FOR THE FIRST TIME!

To securely access this %s (%s) automatically, do you wish to remember its 
public key?  If unsure, say yes.

The public key is:
   
%s
""" % (key_type.upper(), key_type, key_name, pubkey)

   prompt = "Trust this key? (Y/n): "
   while True:
      trust = raw_input(prompt)
      if trust not in ['Y', 'y', 'N', 'n']:
         prompt = "Please enter 'Y' or 'N': "
         continue
      
      break
   
   if trust in ['Y', 'y']:
      return True 
   else:
      return False
   

# -------------------
def api_call_signer( method_name, data ):
   """
   Sign an RPC call.
   """
   global SIGNING_PKEY
   
   pkey = SIGNING_PKEY
   
   # sign the data
   h = HashAlg.new( data )
   signer = CryptoSigner.new( pkey )
   signature = signer.sign( h )
   
   return signature 


# -------------------
def ask_trust_verify_key( method_result, method_name, args, kw ):
   global CONFIG 
   
   # do we know how to get the verification key from the method?
   verify_key_type = conf.verify_key_type_from_method_name( CONFIG, method_name )
   verify_key_name = conf.verify_key_name_from_method_result( CONFIG, method_name, args, kw, method_result )
   verify_key = verify_key_from_method_result( method_result )
   
   if (verify_key_type == None or verify_key_name == None) and verify_key == None:
      log.warning("No knowledge of how to handle a given verify_key (type = %s, name = %s)" % (verify_key_type, verify_key_name))
      return False
   
   if (verify_key_type != None and verify_key_name != None) and verify_key is None:
      pp = pprint.PrettyPrinter()
      pp.pprint( method_result )
      log.error("SECURITY ERROR: expected but did not receive verification key!")
      return False
   
   if verify_key is not None and verify_key_type != None and verify_key_name != None:
      trust = prompt_trust_verify_key( verify_key_type, verify_key_name, verify_key )
      if trust:
         print "Trusting MS verification public key for %s %s" % (verify_key_type, verify_key_name)
         storage.store_keys( CONFIG, verify_key_type, verify_key_name, verify_key, None )
         return True 
      else:
         return False


# -------------------
def api_call_verifier( method_name, args, kw, data, syndicate_data, rpc_result ):
   """
   Verify an RPC call.
   It's possible that we don't have the key yet, so throw up a warning if so.
   """
   global VERIFYING_PUBKEY, OBJECT_SIGNING_KEY_NAME, OBJECT_SIGNING_KEY_TYPE, CONFIG
   
   # sanity check
   if not 'signature' in syndicate_data:
      return False 
   
   method_result = rpc_result.get("result")
   if method_result == None:
      raise Exception("No data returned from '%s'" % method_name )
   
   sig = syndicate_data['signature']
   
   # verify object ID and type
   pubkey = VERIFYING_PUBKEY
   ret = False
   
   if pubkey is not None:
      
      # verify this data
      h = HashAlg.new( data )
      verifier = CryptoSigner.new(pubkey)
      ret = verifier.verify( h, sig )
   
      if not ret:
         # verification key has changed on the MS
         warn_verification_key_change()
      
   
   else:
      # this public key is not known to us...prompt to trust it.
      
      # if we got back a list of things, ask for each key
      if isinstance( method_result, list ):
         for single_result in method_result:
            if not ask_trust_verify_key( single_result, method_name, args, kw ):
               ret = False
               break
            
      else:
         ret = ask_trust_verify_key( method_result, method_name, args, kw )

   return ret


# -------------------
def make_rpc_client( config, verify_reply=True ):
   ms_url = config["MSAPI"]
   
   if not ms_url.lower().startswith("https://"):
      log.warning("MS URL %s is NOT secure!" % ms_url )
   
   verifier = api_call_verifier
   if not verify_reply:
      verifier = None 
      
   json_client = jsonrpc.Client( ms_url, msconfig.JSON_MS_API_VERSION, signer=api_call_signer, verifier=verifier )
   
   return json_client



# -------------------
def call_method( client, user_id, method_name, *args, **kw ):
   # which key do we use?
   global SIGNING_PKEY, VERIFYING_PUBKEY, CONFIG, OBJECT_SIGNING_KEY_NAME, OBJECT_SIGNING_KEY_TYPE
   
   SIGNING_PKEY = None
   VERIFYING_PUBKEY = None
   
   key_types = conf.signing_key_types_from_method_name( CONFIG, method_name )
   key_names = conf.signing_key_names_from_method_args( CONFIG, method_name, args, kw )
   
   for key_type, key_name in zip( key_types, key_names ):
      # override user key name if desired
      if key_type == "user" and FORCE_USER_KEY_NAME != None:
         key_name = FORCE_USER_KEY_NAME 
         
      try:
         VERIFYING_PUBKEY, SIGNING_PKEY = storage.get_object_keys( CONFIG, key_type, key_name )
         
         OBJECT_SIGNING_KEY_TYPE = key_type 
         OBJECT_SIGNING_KEY_NAME = key_name 
         
         client.set_key_info( key_type, key_name )
         
      except Exception, e:
         traceback.print_exc()
         SIGNING_PKEY = None
         VERIFYING_PUBKEY = None
         log.error("Failed to load object keys")
         pass
      
      if SIGNING_PKEY is not None and VERIFYING_PUBKEY is not None:
         # got a usable key-pair
         break
      
   
   # can we proceed?
   if SIGNING_PKEY is None:
      raise Exception("Failed to load a usable signing key for %s" % key_name)
      
   method = getattr( client, method_name )
   return method( *args, **kw )
   
   
# -------------------
def serialize_param( value ):
   # try to cast value to something
   if "." in value or "e" in value:
      try:
         value = float(value)
         return value
      except:
         pass

   if value == "True" or value == "False":
      try:
         value = eval(value)
         return value 
      except:
         pass
   
   if value.strip().startswith("{") or value.strip().startswith("["):
      try:
         value = eval(value)
         return value
      except:
         pass
   
   try:
      value = int(value)
      return value
   except:
      pass
   
   
   if value.find("=") == -1:
      return value
   
   raise Exception("Could not parse '%s'" % param)
   

# -------------------
def read_args_and_kw( params ):
   args = []
   kw = {}
   for param in params:
      try:
         serialized_param = serialize_param( param )
      except:
         # is this a keyword argument?
         param_parts = param.split("=")
         if len(param_parts) > 1:
            kw[param_parts[0]] = serialize_param( "=".join( param_parts[1:] ) )
         else:
            raise Exception("Malformed parameter '%s'" % param)
      else:
         args.append( serialized_param )

   return (args, kw)


# -------------------   
def generate_key_pair( key_size ):
   log.info("Generating key pair...")
   
   rng = Random.new().read
   key = CryptoKey.generate(key_size, rng)

   private_key_pem = key.exportKey()
   public_key_pem = key.publickey().exportKey()

   return (public_key_pem, private_key_pem)


# -------------------   
def make_auth_keys():
   key_size = msconfig.OBJECT_KEY_SIZE
   
   pubkey_pem, privkey_pem = generate_key_pair( key_size )
   
   return (pubkey_pem, privkey_pem)
   

# -------------------   
def parse_auth_key_request( auth_key_request, arg ):
   if auth_key_request == "MAKE_SIGNING_KEY":
      return make_auth_keys()
   
   elif auth_key_request == "USE_SIGNING_KEY":
      if arg == None:
         raise Exception("Path required after USE_SIGNING_KEY")
      
      return (storage.read_auth_key( arg ), None)
   
   return (None, None)
   
   
# -------------------   
def insert_signing_key( args, kw ):
   new_args = []
   new_kw = {}
   
   key = None
   
   for i in xrange(0, len(args)):
      
      possible_auth_key_arg = None
      if i + 1 < len(args):
         possible_auth_key_arg = args[i+1]
      
      if key == None:
         pubkey, privkey = parse_auth_key_request( args[i], possible_auth_key_arg )
         
         if pubkey != None:
            new_args.append( pubkey )
            key = (pubkey, privkey)
         else:
            new_args.append( args[i] )
      else:
         new_args.append( args[i] )
         
      
   for param, value in kw.items():
      
      if key == None:
         pubkey, privkey = parse_auth_key_request( param, value )
         
         if pubkey != None:
            new_kw[param] = pubkey 
            key = (pubkey, privkey)
      
         else:
            new_kw[param] = value
            
      else:
         new_kw[param] = value 
   
   pubkey = None 
   privkey = None 
   
   if key:
      pubkey, privkey = key
      
   return (new_args, new_kw, pubkey, privkey)


# -------------------   
def check_signing_key( method_result, signing_public_key ):
   # verify that the API key was received by the MS
   if "signing_public_key" not in method_result.keys():
      raise Exception("SECURITY ERROR: method result does not have our public signing key!")
   
   if method_result['signing_public_key'] != signing_public_key:
      raise Exception("SECURITY ERROR: method result has a different public signing key!")
   
   return True
   

# -------------------   
if __name__ == "__main__":
   # read the config
   CONFIG = conf.load_options( sys.argv )
   pp = pprint.PrettyPrinter()
   
   for opt in CONFIG.keys():
      log.debug( "%s = %s" % (opt, CONFIG[opt] ) )
   
   if not CONFIG.has_key("user_id") or not CONFIG.has_key("params"):
      conf.usage( sys.argv[0] )
   
   # calling user...
   user_id = CONFIG["user_id"]
   
   if "user_id" in CONFIG['_in_argv']:
      # override user_id
      FORCE_USER_KEY_NAME = user_id
   
   # method parameters
   all_params = CONFIG["params"]
   
   method_name = all_params[0]
   args, kw = read_args_and_kw( all_params[1:] )
   
   # special case?
   if method_name == "help":
      try:
         method_name = all_params[1]
         method_help = conf.method_help_from_method_name( method_name )
      except:
         method_help = "General HELP goes here..."
         
      print "Help for '%s':\n%s" % (method_name, method_help)
      sys.exit(0)
   
   elif method_name == "untrust":
   
      try:
         key_type = all_params[1]
         key_name = all_params[2]
         
         if key_type not in conf.KEY_TYPES:
            raise Exception("Usage: %s untrust <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_TYPES)))
         
         
         storage.revoke_keys( CONFIG, key_type, key_name, revoke_signing_key=False )
      except:
         raise Exception("Usage: %s untrust <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_TYPES)))
   
      sys.exit(0)
         
   
   # generate any signing keys
   args, kw, signing_public_key, signing_private_key = insert_signing_key( args, kw )
   
   # verify that we won't be clobbering an existing key
   if signing_private_key is not None:
      pass
   
   # what object signing key are we working on?
   log.debug("With %s: call %s( args=%s, kw=%s )" % (conf.signing_key_types_from_method_name( CONFIG, method_name ), method_name, args, kw) )
   
   client = make_rpc_client( CONFIG )
   ret = call_method( client, user_id, method_name, *args, **kw ) 
   
   # before storing anything, do we need to revoke and add trust in a verifying key?
   if ret != None:
      # remove old keys first
      revoke_key_type = conf.revoke_key_type_from_method_name( CONFIG, method_name )
      revoke_key_name = conf.revoke_key_name_from_method_args( CONFIG, method_name, args, kw )
      
      if revoke_key_type != None and revoke_key_name != None:
         storage.revoke_keys( CONFIG, revoke_key_type, revoke_key_name )
   
      # trust new keys, if need be
      trust_key_type = conf.trust_key_type_from_method_name( CONFIG, method_name )
      trust_key_name = conf.trust_key_name_from_method_args( CONFIG, method_name, args, kw )
      
      if trust_key_type != None and trust_key_name != None:
         verify_key = verify_key_from_method_result( ret )
         if verify_key is not None:
            storage.store_keys( CONFIG, trust_key_type, trust_key_name, verify_key, None )
         else:
            raise Exception("Server error: expected verify key in response")
         
         
   if signing_public_key is not None and ret != None:
      # verify that the key returned by the MS (suggesting, but not implying, that it took)
      check_signing_key( ret, signing_public_key )
      
      verify_key_type = conf.verify_key_type_from_method_name( CONFIG, method_name )
      verify_key_name = conf.verify_key_name_from_method_result( CONFIG, method_name, args, kw, ret )
      
      # save the object's signing private key 
      if signing_private_key:
         storage.store_keys( CONFIG, verify_key_type, verify_key_name, None, signing_private_key )
      else:
         raise Exception("Unable to determine which keys to store!")
      
   pp.pprint( ret )
   
   