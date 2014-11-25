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

import syndicate.client.common.object_stub as object_stub

import syndicate.util.config as modconf 

import traceback

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import pprint 
import urlparse
import requests 
import getpass

log = Log.get_logger()

# -------------------
def store_syndicate_public_key( config, syndicate_public_key, overwrite=True ):
   assert 'syndicate_host' in config
   assert 'syndicate_port' in config
   assert 'no_tls' in config 
   
   pubkey_name = conf.make_syndicate_pubkey_name( config['syndicate_host'], config['syndicate_port'], config['no_tls'] )
   pubkey_path = conf.object_key_path( config, "syndicate", pubkey_name, no_suffix=True )
   
   return storage.write_key( pubkey_path, syndicate_public_key )


# -------------------
def download_syndicate_public_key( config ):
   assert 'syndicate_host' in config
   assert 'no_tls' in config
   
   port = config.get("syndicate_port", None)
   if port is None:
      port = conf.default_syndicate_port( config['syndicate_host'], config['no_tls'] )

   url = conf.make_ms_url( config['syndicate_host'], port, config['no_tls'], urlpath="PUBKEY" )
   
   pubkey_req = requests.get( url )
   
   if pubkey_req.status_code != 200:
      raise Exception("Failed to get public key from %s, HTTP status %s" % (url, pubkey_req.status) )
   
   assert hasattr(pubkey_req, "content"), "Invalid response; no content given!"
   
   pubkey_pem = str(pubkey_req.content)
   pubkey_pem = pubkey_pem.strip()
   
   # validate 
   try:
      pubkey = CryptoKey.importKey( pubkey_pem )
   except Exception, e:
      log.error("Invalid key")
      raise e
   
   return pubkey.publickey().exportKey()
   

# -------------------
def load_syndicate_public_key( config ):
   if config.has_key("syndicate_public_key"):
      # already loaded
      return config['syndicate_public_key']
   
   pubkey_name = conf.make_syndicate_pubkey_name( config['syndicate_host'], config['syndicate_port'], config['no_tls'] )
   pubkey_path = conf.object_key_path( config, 'syndicate', pubkey_name, no_suffix=True )
   
   return storage.read_public_key( pubkey_path )

# -------------------
def load_user_private_key( config, user_id ):
   return storage.read_private_key( config, "user", user_id )


# -------------------
def warn_key_change( config ):
   
   print """
SECURE VERIFICATION FAILURE!

It's possible that someone is impersonating your Syndicate, to get you to leak sensitive data!
If you are certain this is not the case, you should remove the offending public key.

Offending public key path:  %s
""" % conf.make_syndicate_pubkey_name( config['syndicate_host'], config['syndicate_port'], config['no_tls'] )

   sys.exit(1)

# -------------------
def prompt_trust_public_key( name, port, pubkey ):
   print """
Syndicate at %s:%s accessed for the first time!

To securely access Syndicate at %s:%s automatically, do you wish to remember its 
public key?  If unsure, say yes.

The public key is:
   
%s
""" % (name, port, name, port, pubkey)

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
def ask_trust_public_key( config, syndicate_pubkey ):
   if syndicate_pubkey is not None:
      trust = prompt_trust_public_key( config['syndicate_host'], config['syndicate_port'], syndicate_pubkey )
      if trust:
         print "Trusting public key for %s" % (config['syndicate_host'])
         return True 
      else:
         return False

   else:
      log.error("Syndicate did not reply with a name or public key to trust!")
      return False

# -------------------
def api_call_signer( signing_pkey, method_name, data ):
   """
   Sign an RPC call with the user's private key
   """
   
   # sign the data
   h = HashAlg.new( data )
   signer = CryptoSigner.new( signing_pkey )
   signature = signer.sign( h )

   return signature 
   
   
# -------------------
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
         warn_key_change( config )
         return False
   
      else:
         return True
   
   else:
      raise Exception("No public key given.  Unable to verify result.")


# -------------------
def make_rpc_client( config, username=None, password=None, user_private_key=None, syndicate_public_key=None, no_verify_result=False ):
   ms_url = conf.make_ms_url( config['syndicate_host'], config['syndicate_port'], config['no_tls'] )
   
   if not ms_url.lower().startswith("https://"):
      log.warning("MS URL %s is NOT secure!" % ms_url )
   
   signer = None
   verifier = None
   
   if user_private_key is not None:
      log.info("Using public-key authentication")
      signer = lambda method_name, data: api_call_signer( user_private_key, method_name, data )
      
      # use public-key authentication 
      ms_url += "/pubkey"

   if syndicate_public_key is not None and not no_verify_result:
      verifier = lambda method_name, args, kw, data, syndicate_data, rpc_result: api_call_verifier( config, syndicate_public_key, method_name, data, syndicate_data, rpc_result )
   
   json_client = jsonrpc.Client( ms_url, msconfig.JSON_MS_API_VERSION, signer=signer, verifier=verifier, username=username, password=password )
      
   return json_client


# -------------------
def call_method( config, client, method_name, args, kw ):
   # which key do we use?
   # what object signing key are we working on?
   log.debug("as %s: call %s( args=%s, kw=%s )" % (config['user_id'], method_name, args, kw) )
   
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
   for key_type, key_dirname in conf.KEY_DIR_NAMES.items():
      key_dirname = conf.KEY_DIR_NAMES.get(key_type)
      if key_dirname is None:
         # forgot to add an entry in KEY_DIR_NAMES for the given key type
         raise Exception("BUG: unknown key type %s" % key_type)
      
      key_dir = config.get(key_dirname, None)
      if key_dir is None:
         # forgot to set the path to this directory in the config
         raise Exception("BUG: unknown key directory %s" % key_dirname)
      
      ret = storage.make_or_check_key_directory( key_dir )
      if not ret:
         raise Exception("Failed to set up key directories")
   
   return True


# -------------------
def parse_url( url ):
   # return (host, port, insecure)
   
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


# -------------------
def load_options( argv, setup_methods=[], builtin_methods=[] ):
   
   parser = modconf.build_parser( argv[0], conf.CONFIG_DESCRIPTION, conf.CONFIG_OPTIONS )
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
   
   if config_str is None:
      
      # possibly calling 'setup', so fill in empty information
      if method_name not in setup_methods:
         raise Exception("Failed to load configuration from %s" % config_file_path)
      
      # calling 'setup', so populate the config
      conf.fill_defaults( config )
      
      if not hasattr(opts, 'user_id') and method_name in setup_methods:
         raise Exception("--user_id option is required for this method")
      
      config['user_id'] = opts.user_id
      config_file_path = conf.CONFIG_FILENAME
      config_str = conf.make_config_str( config )
      
      config_with_opts = conf.load_config( config_file_path, config_str, opts )
      if config_with_opts != None:
         config.update( config_with_opts )
      else:
         raise Exception("Failed to parse command-line args")
      
   else:
      config = conf.load_config( config_file_path, config_str, opts )
      if config is None:
         raise Exception("Failed to parse configuration from %s" % config_file_path)
   
   config['params'] = getattr( opts, 'params', [] )
   
   # set up the key directories
   setup_key_directories( config )
   
   # generate syndicate_host and syndicate_port from URL, if needed
   if config.get('url', None) != None:
      # use https if no :// is given 
      url = config['url']
      
      host, port, no_tls = parse_url( url )
      
      config['syndicate_host'] = host 
      config['syndicate_port'] = port
      config['no_tls'] = no_tls
      
      
   # what do we need for authentication?
   auth_opts = None
   if method_name in setup_methods:
      # setup methods need password
      auth_opts = [msconfig.AUTH_METHOD_PASSWORD]
      
   elif method_name in builtin_methods:
      # builtin methods that don't do setup need no password
      auth_opts = [msconfig.AUTH_METHOD_NONE]
   
   else:
      auth_opts = api.method_auth_options( method_name )
      
   have_auth_tokens = False
   
   if msconfig.AUTH_METHOD_NONE in auth_opts:
      # no authentication needed
      config['user_pkey'] = None 
      config['password'] = None 
      have_auth_tokens = True
   
   else:
      # obtain the user's private key and/or password, if present
      if not config.has_key('user_pkey') and msconfig.AUTH_METHOD_PUBKEY in auth_opts:
         try:
            config['user_pkey'] = load_user_private_key( config, config['user_id'] )
            have_auth_tokens = True
         except Exception, e:
            log.warning("No private key found.")
            config['user_pkey'] = None
            pass
         
      if not have_auth_tokens and not config.has_key('password') and msconfig.AUTH_METHOD_PASSWORD in auth_opts:
         config['password'] = getpass.getpass("Syndicate password: ")
         have_auth_tokens = True
      
   if not have_auth_tokens:
      raise Exception("Could not satisfy authentication requirements for %s (required: %s)"  % (method_name, auth_opts) )
      
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
def do_setup( config, all_params ):
   print "Setting up syntool..."
   
   # if the config file already exists, then bail
   if os.path.exists( conf.CONFIG_FILENAME ):
      raise Exception("Syntool is already set up (in %s)" % conf.CONFIG_DIR)
   
   # check args...
   for required_key in ['syndicate_host', 'syndicate_port', 'user_id']:
      if config.get(required_key, None) is None:
         print >> sys.stderr, "Missing argument: %s" % required_key
         sys.exit(1)
   
   key_dirs = {}
   write_config = {}
   for key_type, key_dirname in conf.KEY_DIR_NAMES.items():
      key_dirs[key_type] = key_dirname + "/"
   
   # generate URL if not given already
   if not config.has_key('url'):
      config['url'] = conf.make_ms_url( config['syndicate_host'], config['syndicate_port'], config['no_tls'] )
   
   write_config.update( key_dirs )
   for attr in ['user_id', 'url']:
      write_config[attr] = config[attr]
   
   config_str = conf.make_config_str( write_config )

   log.debug("Obtaining Syndicate public key...")
   
   syndicate_public_key = download_syndicate_public_key( config )
   if syndicate_public_key is None:
      raise Exception("Failed to obtain Syndicate public key")
   
   if not config['trust_public_key']:
      do_trust = ask_trust_public_key( config, syndicate_public_key )
      if not do_trust:
         log.error("Refusing to trust key.")
         sys.exit(0)
   
   
   # store this for activation
   try:
      config['syndicate_public_key'] = CryptoKey.importKey( syndicate_public_key )
   except Exception, e:
      # shouldn't fail--download_syndicate_public_key validates
      log.error("Invalid public key")
      log.exception(e)
      sys.exit(1)
   
   log.debug("Activating Account")
   
   # prompt activation password
   activation_password = getpass.getpass("Activation password: " )
   
   rc = client_call( config, "register_account", config['user_id'], activation_password )
   
   if rc != True:
      log.error("Failed to activate account!")
      pp = pprint.PrettyPrinter()
      pp.pprint( rc )
      sys.exit(1)
   
   log.debug("Storing config...")
   
   store_syndicate_public_key( config, syndicate_public_key, overwrite=True )
   
   # store config
   try:
      storage.write_file( conf.CONFIG_FILENAME, config_str )
   except Exception, e:
      log.exception(e)
      print >> sys.stderr, "Failed to write configuration"
      sys.exit(1)
      
   print "Account for %s activated." % config['user_id']
   sys.exit(0)


# -------------------   
def make_conf( user_id, syndicate_host, **defaults ):
   config = {}
   conf.fill_defaults( config )
   extend_paths( config, CONFIG_DIR )
   
   config['user_id'] = user_id
   config['syndicate_host'] = syndicate_host
   
   for (k, v) in defaults.items():
      config[k] = v
   
   # set up the key directories
   if( setup_dirs ):
      setup_key_directories( config )
   
   return config


# -------------------   
def client_call( CONFIG, method_name, *args, **kw ):
   # call a method, with the given args and kw.
   
   called_from_main = False 
   if CONFIG.has_key('__from_main__'):
      # we were called from the main method (not from a client program).
      # This should be remembered, since it determines whether or not we use
      # the CONFIG-given user_id if no user_id can be found.
      called_from_main = CONFIG['__from_main__']
      
   verify_reply = True
   if CONFIG.has_key('verify_reply'):
      # do not verify the reply (i.e. we might not know the Syndicate public key)
      verify_reply = CONFIG['verify_reply']
   
   user_id = CONFIG.get('user_id', None)
   if user_id is None:
      raise Exception("Invalid config: no user_id")
   
   # parse arguments.
   # use lib to load and store temporary data for the argument parsers.
   lib = conf.ArgLib()
   lib.config = CONFIG
   lib.storage = storage
   lib.user_id = user_id
   
   args, kw, extras = conf.parse_args( method_name, args, kw, lib )
   
   # validate arguments
   valid = conf.validate_args( method_name, args, kw )
   if not valid:
      raise Exception("Invalid arguments for %s" % method_name)
   
   # determine the user_id, if needed
   force_user_name = None 
   
   if "user_id" in CONFIG['_in_argv']:
      # override user_id with argv
      force_user_name = user_id
   
   if CONFIG.has_key('force_user_name'):
      # override user_id with a value set in make_conf()?
      force_user_name = CONFIG['force_user_name']
      
   if called_from_main and force_user_name is None:
      # use CONFIG-given user_id if called from the command-line (main()), but we don't yet know the user_id from above.
      force_user_name = user_id
      
      
   # attempt to read the public key from disk   
   syndicate_public_key = load_syndicate_public_key( CONFIG )
   
   # will we trust a downloaded public key, if one is not known?
   trust_public_key = False
   if syndicate_public_key is None:
      trust_public_key = CONFIG.get('trust_public_key', False)
   
   # will we verify the result of our method?
   no_verify_result = CONFIG.get('no_verify_result', False)
   
   if syndicate_public_key is None:
      # get the public key from the MS
      syndicate_public_key_str = download_syndicate_public_key( CONFIG )
      if syndicate_public_key_str is not None:
         if not trust_public_key:
            do_trust = ask_trust_public_key( CONFIG, syndicate_public_key_str )
            if do_trust:
               store_syndicate_public_key( CONFIG, syndicate_public_key_str )
   
         elif not no_verify_result:
            log.error("Could not obtain Syndicate public key.  Cannot continue.")
            return None
         
         else:
            log.warning("INSECURE: will not verify result!")
            
         try:
            syndicate_public_key = CryptoKey.importKey( syndicate_public_key_str )
         except:
            log.error("Failed to parse public key")
            return None
            
   
   # create the RPC client
   client = make_rpc_client( CONFIG, username=CONFIG['user_id'], password=CONFIG.get('password',None), user_private_key=CONFIG.get('user_pkey',None),
                             syndicate_public_key=syndicate_public_key, no_verify_result=no_verify_result )
   
   # call the method
   ret = call_method( CONFIG, client, method_name, args, kw ) 
   
   # failure? 
   if ret is None:
      raise Exception("No data returned from server")
   
   # process object-specific extra information
   for object_cls in object_stub.object_classes:
      object_cls.ProcessExtras( extras, CONFIG, method_name, args, kw, ret, storage )

   return ret


# -------------------   
class RPCMethod:
   def __init__(self, config, method_name):
      self.config = config 
      self.method_name = method_name
      
      # create the RPC client
      self.client = make_rpc_client( self.config,
                                     username=self.config['user_id'],
                                     password=self.config.get('password',None),
                                     user_private_key=self.config.get('user_pkey',None),
                                     syndicate_public_key=self.config.get('syndicate_public_key',None),
                                     no_verify_result=self.config.get('no_verify_result') )
      
   def __call__(self, *args, **kw):
      # parse arguments.
      # use lib to load and store temporary data for the argument parsers.
      lib = conf.ArgLib()
      lib.config = self.config
      lib.storage = storage
      lib.user_id = self.config['user_id']
      
      args, kw, extras = conf.parse_args( self.method_name, args, kw, lib )
      
      # validate arguments
      valid = conf.validate_args( self.method_name, args, kw )
      if not valid:
         raise Exception("Invalid arguments for %s" % self.method_name)
      
      return call_method( self.config, self.client, self.method_name, args, kw )
   
   

# -------------------   
class Client:
   
   def __init__(self, user_id, ms_url,
                      password=None,
                      user_pkey_pem=None,
                      syndicate_public_key_pem=None,
                      debug=False,
                      ):
      
      self.config = {}
      self.method_singletons = {}
      
      no_verify_result = True
      
      # sanity check...
      if syndicate_public_key_pem is not None:
         try:
            self.config['syndicate_public_key'] = CryptoKey.importKey( syndicate_public_key )
            no_verify_result = False
         except:
            raise Exception("Invalid value for syndicate_public_key_pem")
      
      # sanity check...
      if user_pkey_pem is not None:
         try:
            self.config['user_pkey'] = CryptoKey.importKey( user_pkey_pem )
            assert self.config['user_pkey'].has_private(), "Not a private key"
         except:
            raise Exception("Invalid value for user_pkey_pem")
      
      try:
         host, port, no_tls = parse_url( ms_url )
      except:
         raise Exception("Invalid URL '%s'" % ms_url)
      
      # populate our config 
      self.config['syndicate_host'] = host 
      self.config['syndicate_port'] = port 
      self.config['no_tls'] = no_tls
      self.config['user_id'] = user_id
      self.config['debug'] = debug 
      self.config['password'] = password
      self.config['no_verify_result'] = no_verify_result
      
      Log.set_log_level( "DEBUG" if debug else "INFO" )

   def __getattr__(self, method_name ):
      # return a callable
      if not self.method_singletons.has_key( method_name ):
         self.method_singletons[method_name] = RPCMethod( self.config, method_name )
      
      return self.method_singletons[method_name]
      

# -------------------   
def main( argv ):
   # read the config
   CONFIG = load_options( argv, setup_methods=['setup'], builtin_methods=['setup', 'help'] )
   
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
   
   elif method_name == "setup":
      do_setup( CONFIG, all_params )
   
   # called from main 
   CONFIG["__from_main__"] = True
   return client_call( CONFIG, method_name, *args, **kw )


if __name__ == "__main__":
   ret = main( sys.argv )
   pp = pprint.PrettyPrinter()
   pp.pprint( ret )
   
   
   