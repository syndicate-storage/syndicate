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

import os
import json

import sys
import tempfile
import base64
import stat

import syndicate.client.conf as conf 
import syndicate.client.storage as storage
import syndicate.client.common.jsonrpc as jsonrpc
import syndicate.client.common.log as Log
import syndicate.client.common.msconfig as msconfig
import syndicate.client.common.api as api

from syndicate.client.common.object_stub import StubObject 

import traceback

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import pprint 

log = Log.get_logger()

TRUST_VERIFY_KEY = False 

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
def prompt_revoke_signing_key( key_type, key_name ):
   print """
WARNING! REVOKING A %s PRIVATE SIGNING KEY CANNOT BE UNDONE!

Once revoked, you will no longer be able to access this %s
without administrator privileges.  This includes deleting
it, reading it, and changing its public signing key.

""" % (key_type.upper(), key_type)

   prompt = "Revoke the private signing key for %s? (Y/n): " % key_name
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
def api_call_signer( signing_pkey, method_name, data ):
   """
   Sign an RPC call.
   """
   
   # sign the data
   h = HashAlg.new( data )
   signer = CryptoSigner.new( signing_pkey )
   signature = signer.sign( h )
   
   return signature 


# -------------------
def ask_trust_verify_key( config, method_result, method_name, args, kw ):
   
   # do we know how to get the verification key from the method?
   verify_key_type = api.verify_key_type_from_method_name( method_name )
   verify_key_name = api.verify_key_name_from_method_result( method_name, args, kw, method_result, default_user_id=config['user_id'] )
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
         storage.store_object_public_key( config, verify_key_type, StubObject.VERIFYING_KEY_TYPE, verify_key_name, verify_key )
         return True 
      else:
         return False


# -------------------
def api_call_verifier( config, pubkey, method_name, args, kw, data, syndicate_data, rpc_result, trust_verify_key ):
   """
   Verify an RPC call.
   It's possible that we don't have the key yet, so throw up a warning if so.
   """
   
   # sanity check
   if not 'signature' in syndicate_data:
      return False 
   
   method_result = rpc_result.get("result")
   if method_result == None:
      raise Exception("No data returned from '%s'" % method_name )
   
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
         warn_verification_key_change()
   
   else:
      if not trust_verify_key:
         # this public key is not known to us...prompt to trust it, if we're not going to do so automatically
         # if we got back a list of things, ask for each key
         if isinstance( method_result, list ):
            ret = True
            for single_result in method_result:
               if not ask_trust_verify_key( config, single_result, method_name, args, kw ):
                  ret = False
                  break
               
         else:
            ret = ask_trust_verify_key( config, method_result, method_name, args, kw )

      else:
         # will store the verifying key automatically
         ret = True

   return ret


# -------------------
def make_rpc_client( config, verifying_pubkey, signing_pkey, signing_key_type, signing_key_name, trust_verify_key, verify_reply=True ):
   ms_url = config["MSAPI"]
   
   if not ms_url.lower().startswith("https://"):
      log.warning("MS URL %s is NOT secure!" % ms_url )
   
   signer = lambda method_name, data: api_call_signer( signing_pkey, method_name, data )
   verifier = lambda method_name, args, kw, data, syndicate_data, rpc_result: api_call_verifier( config, verifying_pubkey, method_name, args, kw, data, syndicate_data, rpc_result, trust_verify_key )
   
   if not verify_reply:
      verifier = None 
   
   json_client = jsonrpc.Client( ms_url, msconfig.JSON_MS_API_VERSION, signer=signer, verifier=verifier )
   json_client.set_key_info( signing_key_type, signing_key_name )
   
   return json_client


# -------------------
def get_signing_and_verifying_keys( config, user_id, method_name, args, kw, force_user_key_name=None ):
   
   signing_pkey = None
   verifying_pubkey = None
   if config.has_key('signing_pkey_pem') and config.has_key('signing_key_type') and config.has_key('signing_key_name'):
      try:
         signing_pkey = CryptoKey.importKey( config['signing_pkey_pem'] )
         assert signing_pkey.has_private()
      except Exception, e:
         raise Exception("Not a private key")
      
      if config.get('verifying_pubkey_pem', None) is not None:
         try:
            verifying_pubkey = CryptoKey.importKey( config.get('verifying_pubkey_pem', None) )
            assert not verifying_pubkey.has_private()
         except Exception, e:
            raise Exception("Not a public key")
            
      loaded_key_type = config['signing_key_type']
      loaded_key_name = config['signing_key_name']
      
      return (verifying_pubkey, signing_pkey, loaded_key_type, loaded_key_name)
   
      
   key_types = api.signing_key_types_from_method_name( method_name )
   key_names = api.signing_key_names_from_method_args( method_name, args, kw, default_user_id=config['user_id'] )
   
   loaded_key_type = None 
   loaded_key_name = None 
   
   for key_type, key_name in zip( key_types, key_names ):
      # override user key name if desired
      if key_type == "user" and force_user_key_name != None:
         key_name = force_user_key_name 
         
      try:
         verifying_pubkey = storage.load_object_public_key( config, key_type, StubObject.VERIFYING_KEY_TYPE, key_name )
         signing_pkey = storage.load_object_private_key( config, key_type, StubObject.SIGNING_KEY_TYPE, key_name )
         
         loaded_key_type = key_type 
         loaded_key_name = key_name 
         
      except Exception, e:
         traceback.print_exc()
         signing_pkey = None
         verifying_pubkey = None
         log.error("Failed to load object keys")
         pass
      
      if signing_pkey is not None and verifying_pubkey is not None:
         # got a usable key-pair
         break
      
   
   # can we proceed?
   if signing_pkey is None:
      raise Exception("Failed to load a usable signing key for %s" % key_name)
   
   return (verifying_pubkey, signing_pkey, loaded_key_type, loaded_key_name)


# -------------------
def need_verifying_key( config, method_name, signing_key_type ):
   verifying_key_type = api.verify_key_type_from_method_name( method_name )
   if verifying_key_type != signing_key_type:
      # won't get a verifying key in this call
      return True
   else:
      return False

# -------------------
def call_method( config, client, method_name, args, kw ):
   # which key do we use?
   # what object signing key are we working on?
   log.debug("With %s (as %s): call %s( args=%s, kw=%s )" % (api.signing_key_types_from_method_name( method_name ), config['user_id'], method_name, args, kw) )
   
   method = getattr( client, method_name )
   
   return method( *args, **kw )
   
   
# -------------------
def serialize_positional_arg( value ):
   # try to cast value to something for a positional argument (not a keyword argument)
   if "." in value or "e" in value:
      # float?
      try:
         value = float(value)
         return value
      except:
         pass

   if value == "True" or value == "False":
      # bool?
      try:
         value = eval(value)
         return value 
      except:
         pass
   
   if value.strip().startswith("{") or value.strip().startswith("["):
      # dict or list?
      try:
         value = eval(value)
         return value
      except:
         pass
   
   try:
      # integer?
      value = int(value)
      return value
   except:
      pass
   
   
   if value.find("=") == -1:
      # string?
      return value
   
   raise Exception("Could not parse '%s'" % param)
   

# -------------------
def read_params( params ):
   if len(params) == 0:
      return (None, None, None)
   
   method_name = params[0]
   params = params[1:]
   args = []
   kw = {}
   for param in params:
      try:
         serialized_arg = serialize_positional_arg( param )
      except:
         # is this a keyword argument?
         param_parts = param.split("=")
         if len(param_parts) > 1:
            kw[param_parts[0]] = serialize_positional_arg( "=".join( param_parts[1:] ) )
         else:
            raise Exception("Malformed parameter '%s'" % param)
      else:
         args.append( serialized_arg )

   return (method_name, args, kw)
   
# -------------------
def setup_key_directories( config ):
   # validate key directories
   for key_type, object_cls in api.KEY_TYPE_TO_CLS.items():
      key_dirname = conf.KEY_DIR_NAMES.get(key_type)
      if key_dirname == None:
         # forgot to add an entry in KEY_DIR_NAMES for the given key type
         raise Exception("BUG: unknown key type %s" % key_type)
      
      key_dir = config.get(key_dirname, None)
      if key_dir == None:
         # forgot to set the path to this directory in the config
         raise Exception("BUG: unknown key directory %s" % key_dirname)
      
      ret = storage.make_or_verify_internal_key_directories( key_dir, object_cls )
      
      if not ret:
         raise Exception("Failed to set up key directories")
   
   return True


# -------------------
def load_options( argv ):
   
   parser = conf.build_parser( argv[0] )
   opts = parser.parse_args( argv[1:] )
   
   # load everything into a dictionary and return it
   config = None 
   config_str = None
   config_file_path = None
   
   if hasattr( opts, "config" ) and opts.config != None:
      config_file_path = opts.config[0]
   else:
      config_file_path = conf.CONFIG_FILENAME
   
   config_str = storage.read_file( config_file_path )
   
   config = {}
   method_name, args, kw = read_params( getattr(opts, 'params', [] ) )
   
   if config_str == None:
      # possibly calling 'setup', so fill in empty information
      if method_name != "setup":
         raise Exception("Failed to load configuration from %s" % config_file_path)
      
      conf.fill_defaults( config )
      
      if not hasattr(opts, 'user_id') and method_name == "setup":
         raise Exception("--user_id option is required for 'setup'")
      
      config['user_id'] = opts.user_id
      config_file_path = conf.CONFIG_FILENAME
      config_str = "[syndicate]\n" + "\n".join( ["%s=%s" % (config_key, config_value) for (config_key, config_value) in config.items()] )
      
      config_with_opts = conf.load_config( config_file_path, config_str, opts )
      if config_with_opts != None:
         config.update( config_with_opts )
      else:
         raise Exception("Failed to parse command-line args")
      
   else:
      config = conf.load_config( config_file_path, config_str, opts )
      if config == None:
         raise Exception("Failed to parse configuration from %s" % config_file_path)
   
   config['params'] = getattr( opts, 'params', [] )
   
   # set up the key directories
   setup_key_directories( config )
   
   return config


# -------------------
def do_method_help( config, all_params ):
   try:
      method_name = all_params[1]
      method_help = api.method_help_from_method_name( method_name )
   except Exception, e:
      log.exception(e)
      method_help = "FIXME: General HELP goes here..."
      
   print "Help for '%s':\n%s" % (method_name, method_help)
   sys.exit(0)


# -------------------   
def do_untrust( config, all_params ):
   try:
      key_type = all_params[1]
      key_name = all_params[2]
   except Exception, e:
      log.exception(e)
      raise Exception("Usage: %s untrust <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_DIR_NAMES.keys() )))
   
   if key_type not in conf.KEY_DIR_NAMES.keys():
      raise Exception("Usage: %s untrust <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_DIR_NAMES.keys() )))
   
   storage.revoke_object_public_key( config, key_type, StubObject.VERIFYING_KEY_TYPE, key_name )
   
   sys.exit(0)
   

# -------------------   
def do_revoke( config, all_params ):
   
   try:
      key_type = all_params[1]
      key_name = all_params[2]
      
   except Exception, e:
      log.exception(e)
      raise Exception("Usage: %s revoke <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_DIR_NAMES.keys() ) ))

   if key_type not in conf.KEY_DIR_NAMES.keys():
      raise Exception("Usage: %s revoke <%s> <name>" % (sys.argv[0], "|".join( conf.KEY_DIR_NAMES.keys() ) ))
   
   revoke = prompt_revoke_signing_key( key_type, key_name )
   if revoke:
      storage.revoke_object_public_key( config, key_type, StubObject.VERIFYING_KEY_TYPE, key_name )
      storage.revoke_object_private_key( config, key_type, StubObject.SIGNING_KEY_TYPE, key_name )
   
   sys.exit(0)


# -------------------   
def do_setup( config, all_params ):
   print "Setting up syntool..."
   
   # if the config file already exists, then bail
   if os.path.exists( conf.CONFIG_FILENAME ):
      raise Exception("syntool is already set up (in %s)" % conf.CONFIG_DIR)
   
   # check args...
   for required_key in ['MSAPI', 'user_id', 'privkey']:
      if config.get(required_key, None) == None:
         print >> sys.stderr, "Missing argument: %s" % required_key
         sys.exit(1)
   
   key_dirs = {}
   for key_type, object_cls in api.KEY_TYPE_TO_CLS.items():
      key_dirname = conf.KEY_DIR_NAMES.get(key_type)
      key_dirs[key_type] = key_dirname + "/"
   
   config_str = """
[syndicate]
MSAPI=%s
user_id=%s
volume_keys=%s
user_keys=%s
gateway_keys=%s
""" % (config['MSAPI'], config['user_id'], key_dirs['volume'], key_dirs['user'], key_dirs['gateway'])

   config_str = config_str.strip() + "\n"

   # try to read the private key
   privkey_pem = None
   try:   
      privkey_pem = storage.read_private_key( config['privkey'] ).exportKey()
   except Exception, e:
      log.exception(e)
      print >> sys.stderr, "Failed to read private key from %s" % config['privkey']
      sys.exit(1)
   
   # we're good.  Store the key information
   try:
      storage.store_object_private_key( config, "user", StubObject.SIGNING_KEY_TYPE, config['user_id'], privkey_pem )
   except Exception, e:
      log.exception(e)
      print >> sys.stderr, "Failed to write private key"
      sys.exit(1)
   
   # store config
   try:
      storage.write_file( conf.CONFIG_FILENAME, config_str )
   except Exception, e:
      log.exception(e)
      print >> sys.stderr, "Failed to write configuration"
      sys.exit(1)
      
   sys.exit(0)
   

# -------------------   
def check_signing_key( method_result, signing_public_key ):
   # verify that the API key was received by the MS
   if "signing_public_key" not in method_result.keys():
      raise Exception("SECURITY ERROR: method result does not have our public signing key!")
   
   if method_result['signing_public_key'] != signing_public_key:
      raise Exception("SECURITY ERROR: method result has a different public signing key!")
   
   return True

# -------------------   
def check_trust_verify_key( config, method_name, args, kw ):
   # trust keys by default?
   trust_verify_key = config.get('trust_verify_key', False)
   
   if not trust_verify_key:
      # implicitly trust?
      # will we trust verifying keys automatically for this method?
      trust_key_type = api.trust_key_type_from_method_name( method_name )
      trust_key_name = api.trust_key_name_from_method_args( method_name, args, kw )
   
      if trust_key_type != None and trust_key_name != None:
         # trust by default
         trust_verify_key = True

   return trust_verify_key


# -------------------   
def make_conf( user_id, ms_api, trust_verify_key=False, setup_dirs=False, **defaults ):
   config = {}
   conf.fill_defaults( config )
   
   config['user_id'] = user_id
   config['MSAPI'] = ms_api
   config['trust_verify_key'] = trust_verify_key
   config['force_user_key_name'] = user_id
   
   for (k, v) in defaults.items():
      config[k] = v
   
   # set up the key directories
   if( setup_dirs ):
      setup_key_directories( config )
   
   return config


# -------------------   
def client_call( CONFIG, method_name, *args, **kw ):
   
   user_id = CONFIG.get('user_id', None)
   if user_id is None:
      raise Exception("Invalid config: no user_id")
   
   # parse arguments; get extra data and hold onto it for now
   lib = conf.ArgLib()
   lib.config = CONFIG
   lib.storage = storage
   
   args, kw, extras = conf.parse_args( method_name, args, kw, lib )
   
   # validate arguments
   valid = conf.validate_args( method_name, args, kw )
   if not valid:
      raise Exception("Invalid arguments for %s" % method_name)
   
   force_user_key_name = None 
   
   if "user_id" in CONFIG['_in_argv']:
      # override user_id
      force_user_key_name = user_id
   
   if CONFIG.has_key('force_user_key_name'):
      force_user_key_name = CONFIG['force_user_key_name']
      
   # load the key information
   verifying_pubkey, signing_pkey, signing_key_type, signing_key_name = get_signing_and_verifying_keys( CONFIG, user_id, method_name, args, kw, force_user_key_name = force_user_key_name )
   
   # is a verifying key required in advance?
   if need_verifying_key( CONFIG, method_name, signing_key_type ) and verifying_pubkey is None:
      raise Exception("No verifying key found")
   
   # will we trust the verify key?
   trust_verify_key = check_trust_verify_key( CONFIG, method_name, args, kw )
   
   # create the RPC client
   client = make_rpc_client( CONFIG, verifying_pubkey, signing_pkey, signing_key_type, signing_key_name, trust_verify_key )
   
   # call the method
   ret = call_method( CONFIG, client, method_name, args, kw ) 
   
   # failure? 
   if ret == None:
      raise Exception("No data returned from server")
   
   # before storing anything, do we need to revoke a verifying key?
   revoke_key_type = api.revoke_key_type_from_method_name( method_name )
   revoke_key_name = api.revoke_key_name_from_method_args( method_name, args, kw )
   
   if revoke_key_type != None and revoke_key_name != None:
      storage.revoke_object_public_key( CONFIG, revoke_key_type, StubObject.VERIFYING_KEY_TYPE, revoke_key_name )
      storage.revoke_object_private_key( CONFIG, revoke_key_type, StubObject.SIGNING_KEY_TYPE, revoke_key_name )
      
      # revoke any other keys as well
      for internal_key_type in api.KEY_TYPE_TO_CLS[revoke_key_type].internal_keys:
         if internal_key_type not in [StubObject.VERIFYING_KEY_TYPE, StubObject.SIGNING_KEY_TYPE]:
            storage.revoke_object_public_key( CONFIG, revoke_key_type, internal_key_type, revoke_key_name )
            storage.revoke_object_private_key( CONFIG, revoke_key_type, internal_key_type, revoke_key_name )
      
   
   result_verify_key = None
   
   # will we trust verifying keys automatically for this method?
   if trust_verify_key:
      do_trust = True
      
      trust_key_type = api.trust_key_type_from_method_name( method_name )
      trust_key_name = api.trust_key_name_from_method_args( method_name, args, kw )

      if trust_key_type == None or trust_key_name == None:
         log.warning("No key to trust for this method")
         do_trust = False
      
      if do_trust:
         # get the key 
         result_verify_key = verify_key_from_method_result( ret )
         if result_verify_key is not None:
            # and trust it
            storage.store_object_public_key( CONFIG, trust_key_type, StubObject.VERIFYING_KEY_TYPE, trust_key_name, result_verify_key )
            
         else:
            raise Exception("MS error: expected verify key in response")
      

   # do we need to store a signing key?
   new_signing_public_key = extras.get("signing_public_key", None )
   new_signing_private_key = extras.get("signing_private_key", None )
   
   if new_signing_public_key is not None:
      # verify that the key was returned by the MS (suggesting, but not implying, that it took)
      check_signing_key( ret, new_signing_public_key )
      
      verify_key_type = api.verify_key_type_from_method_name( method_name )
      verify_key_name = api.verify_key_name_from_method_result( method_name, args, kw, ret, default_user_id=user_id )
      
      # save the object's signing private key 
      if new_signing_private_key is not None and new_signing_private_key != result_verify_key:
         storage.store_object_private_key( CONFIG, verify_key_type, StubObject.SIGNING_KEY_TYPE, verify_key_name, new_signing_private_key )
         
      else:
         raise Exception("Unable to determine which keys to store!")
      
   
   # process object-specific extras
   for (_, object_cls) in api.KEY_TYPE_TO_CLS.items():
      object_cls.ProcessExtras( extras, CONFIG, method_name, args, kw, ret, storage )

   return ret


# -------------------   
def main( argv ):
   # read the config
   global TRUST_VERIFY_KEY
   
   CONFIG = load_options( argv )
   
   for opt in CONFIG.keys():
      log.debug( "%s = %s" % (opt, CONFIG[opt] ) )
   
   if not CONFIG.has_key("user_id") or not CONFIG.has_key("params"):
      print >> sys.stderr, "Missing user ID or method"
      conf.usage( argv[0] )
      
   # method parameters
   all_params = CONFIG["params"]
   method_name, args, kw = read_params( all_params )
   
   # special case?
   if method_name == "help":
      do_method_help( CONFIG, all_params )
   
   elif method_name == "untrust":
      do_untrust( CONFIG, all_params )
   
   elif method_name == "revoke":
      do_revoke( CONFIG, all_params )
   
   elif method_name == "setup":
      do_setup( CONFIG, all_params )
   
   return client_call( CONFIG, method_name, *args, **kw )


if __name__ == "__main__":
   ret = main( sys.argv )
   pp = pprint.PrettyPrinter()
   pp.pprint( ret )
   
   
   