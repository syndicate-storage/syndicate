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

# stubs for all Syndicate objects

import msconfig
from msconfig import *
import api
import inspect
import re 
import sys
import base64
import random
import json
import ctypes

try:
   import pickle
except:
   # only needed by clients...
   pass

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto.Protocol.KDF import PBKDF2

import scrypt
import getpass

try:
   import syndicate.client.common.log as Log
except:
   import log as Log
   
try:
   import syndicate.client.storage as storage 
except:
   import storage_stub as storage

log = Log.get_logger()

SECRETS_PAD_KEY = "__syndicate_pad__"

# RFC-822 compliant, as long as there aren't any comments in the address.
# taken from http://chrisbailey.blogs.ilrt.org/2013/08/19/validating-email-addresses-in-python/
email_regex_str = r"^(?=^.{1,256}$)(?=.{1,64}@)(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22)(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22))*\x40(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d])(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d]))*$"

email_regex = re.compile( email_regex_str )


def is_valid_binary_driver( driver_path, required_syms ):
   """
   Does a given path refer to a binary driver with the given symbols?
   """
   try:
      driver = ctypes.CDLL( driver_path )
   except Exception, e:
      log.exception(e)
      return False
   
   missing = []
   
   # check symbols
   for required_sym in required_syms:
      try:
         f = getattr( driver, required_sym )
      except:
         missing.append( required_sym )
         
   if len(missing) > 0:
      log.warning("Driver '%s' is missing symbols %s" % (driver_path, ", ".join( missing )) )
      return False 
   
   return True



def get_dict_from_closure( config_module, dict_module_name, dict_name ):
   """
   Get a named dictionary from a module.
   """
   
   # attempt to import the module with the dictionary
   dict_module = None
   try:
      dict_module = __import__( config_module.__name__ + "." + dict_module_name, {}, {}, [dict_module_name], -1 )
   except Exception, e:
      log.exception( e )
      raise Exception("No %s module found" % dict_module_name)

   # get the dictionary 
   ret_dict = None
   try:
      ret_dict = getattr(dict_module, dict_name)
      assert isinstance( ret_dict, dict )
   except Exception, e:
      log.exception( e )
      raise Exception("No %s dictionary found" % dict_name)
   
   return ret_dict 


def is_valid_closure_dir( gateway_closure_path, required_files ):
   """
   Does the given directory contain a structurally-valid closure?
   """
   try:
      if not os.path.isdir( gateway_closure_path ):
         return False
   
   except Exception, e:
      log.exception(e)
      return False
   
   files = os.listdir( gateway_closure_path )
   
   missing = []
   
   for required_file in required_files:
      try:
         test_path = os.path.join( gateway_closure_path, required_file )
         if not os.path.exists( test_path ):
            missing.append( test_path )
            
      except Exception, e:
         log.exception(e)
         return False
      
   if len(missing) > 0:
      #log.warning("%s is missing files %s" % (gateway_closure_path, ",".join(missing)) )
      return False
   
   return True


def import_closure( gateway_closure_path ):
   """
   Given the path to the closure directory, import it as a Python module.
   Return the module, i.e. so we can get at the config and secrets dictionaries.
   """
   
   # attempt to import the closure
   dirname = os.path.dirname(gateway_closure_path.rstrip("/"))
   sys.path.append( dirname )
   config_module_name = gateway_closure_path[len(dirname) + 1:].strip("/")
   
   try:
      closure_module = __import__( config_module_name, {}, {}, [], -1 )
   except Exception, e:
      log.exception(e)
      raise Exception("Failed to import %s from %s" % (config_module_name, gateway_closure_path) )
   
   return closure_module



def load_binary_driver( gateway_closure_path, storagelib ):
   """
   Load a binary driver into RAM, using the given storagelib.read_file() method.
   NOTE: we don't do any architecture checks here.
   """
   driver_path = os.path.join( gateway_closure_path, "libdriver.so" )
   return storagelib.read_file( driver_path )



def encrypt_secrets_dict( obj_type, obj_name, secrets_dict, serializer, c_syndicate=None, config=None, storagelib=None, privkey_pem=None ):
   """
   Encrypt a secrets dictionary, returning the serialized base64-encoded ciphertext.
   c_syndicate must be the binary Syndicate module (containing the encrypt_closure_secrets() method)
   storage must be a module with a load_private_key() method
   """
   
   if storagelib is None:
      storagelib = storage
   
   if serializer is None:
      serializer = pickle.dumps
      
   # pad the secrets first
   secrets_dict[ SECRETS_PAD_KEY ] = base64.b64encode( ''.join(chr(random.randint(0,255)) for i in xrange(0,256)) )
   
   try:
      secrets_dict_str = serializer( secrets_dict )
   except Exception, e:
      log.exception( e )
      raise Exception("Failed to serialize secrets")
   
   # attempt to get the private key
   privkey = None
   if privkey_pem is None:
      try:
         privkey = storagelib.load_private_key( config, obj_type, obj_name )
         privkey_pem = privkey.exportKey()
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to load private key for %s" % obj_name )
   
   else:
      try:
         privkey = CryptoKey.importKey( privkey_pem )
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to parse private key for %s" % obj_name )
      
   pubkey_pem = privkey.publickey().exportKey()
   
   # encrypt the serialized secrets dict 
   rc = 0
   encrypted_secrets_str = None
   try:
      log.info("Encrypting secrets...")
      rc, encrypted_secrets_str = c_syndicate.encrypt_closure_secrets( privkey_pem, pubkey_pem, secrets_dict_str )
   except Exception, e:
      log.exception( e )
      raise Exception("Failed to encrypt secrets")
   
   if rc != 0 or encrypted_secrets_str == None:
      raise Exception("Failed to encrypt secrets, rc = %d" % rc)
   
   return encrypted_secrets_str


def load_closure_config( closure_path, storagelib, serializer=None ):
   """
   Given the path to our closure on disk, load and serialize the config dictionary.
   storagelib must be our storage library.
   return serialized config
   """
   
   if serializer is None:
      serializer = pickle.dumps
      
   closure_module = import_closure( closure_path )
   
   config_dict = {}
   
   # get the config
   try:
      config_dict = get_dict_from_closure( closure_module, "config", "CONFIG" )
   except Exception, e:
      log.warning("No config defined")
   
   
   # serialize the config and secrets dictionaries 
   config_dict_str = ""
   
   # config...
   try:
      config_dict_str = serializer( config_dict )
   except Exception, e:
      log.exception( e )
      raise Exception("Failed to serialize")
   
   return config_dict_str 


def load_closure_secrets( closure_path, obj_type, obj_name, config, storagelib, serializer=None, privkey_pem=None ):
   """
   Given the path to our closure on disk, load and serialize the secrets dictionary, encrypting it with the public key of the given object type and name.
   storagelib must be our storage library.
   return serialized encrypted secrets
   """
   
   if serializer is None:
      serializer = pickle.dumps
      
   try:
      import syndicate.syndicate as c_syndicate
   except Exception, e:
      log.exception(e)
      raise Exception("Failed to load libsyndicate")
   
   closure_module = import_closure( closure_path )
   
   secrets_dict = {}
   
   # get the secrets
   try:
      secrets_dict = get_dict_from_closure( closure_module, "secrets", "SECRETS" )
   except Exception, e:
      log.warning("No secrets defined")
   
   # secrets...
   secrets_dict_str = encrypt_secrets_dict( obj_type, obj_name, secrets_dict, serializer, c_syndicate=c_syndicate, config=config, storagelib=storagelib, privkey_pem=privkey_pem )
   
   return secrets_dict_str
   
   
def load_config_and_secrets( closure_path, obj_type, obj_name, config, storagelib, serializer=None, privkey_pem=None ):
   """
   Load a closure's config and secrets.  Literall wraps load_closure_config and load_closure_secrets.
   """
   config_str = load_closure_config( closure_path, storagelib, serializer )
   secrets_str = load_closure_secrets( closure_path, obj_type, obj_name, config, storagelib, serializer=serializer, privkey_pem=privkey_pem )
   return (config_str, secrets_str)


def make_closure_json( closure_dict ):
   """
   Given a dict containing our closure's contents,
   convert everything to base64 and serialize it.
   """
   
   closure_b64 = {}
   
   for (key, value) in closure_dict.items():
      closure_b64[key] = base64.b64encode( value )
   
   try:
      closure_str = json.dumps( closure_b64 )
   except Exception, e:
      log.exception(e)
      raise Exception("Failed to serialize closure")
   
   return closure_str


def load_binary_closure_essentials( closure_path, obj_type, obj_name, config, storagelib, privkey_pem=None):
   """
   Load binary driver essentials: the .so file, the config dict, and the secrets dict.
   Generate a JSON string containing them all.
   NOTE: don't pickle the dicts.  Serialize them as JSON instead, so we can load them in libsyndicate
   """
   
   # get config and secrets data
   config_dict_str, secrets_dict_str = load_config_and_secrets( closure_path, obj_type, obj_name, config, storagelib, serializer=json.dumps, privkey_pem=privkey_pem )
   
   # get the driver
   driver_str = load_binary_driver( closure_path, storagelib )
   if driver_str is None:
      raise Exception("No driver found in %s" % closure_path)
   
   return {"config": config_dict_str, "secrets": secrets_dict_str, "driver": driver_str }



class StubObject( object ):
   """
   Stub object class with just enough functionality to be compatible with 
   the MS's storagetypes.Object class.  This class includes extra information 
   for parsing and validating arguments that are derived from or relate to 
   object data.
   """
   def __init__(self, *args, **kw):
      pass
   
   @classmethod
   def Authenticate( cls, *args, **kw ):
      raise Exception("Called stub Authenticate method!  Looks like you have an import error somewhere.")
   
   @classmethod
   def Sign( cls, *args, **kw ):
      raise Exception("Called stub Sign method!  Looks like you have an import error somewhere.")
   
   @classmethod
   def parse_or_generate_private_key( cls, pkey_str, pkey_generate_args, key_size ):
      """
      Check a private key (pkey_str) and verify that it has the appopriate security 
      parameters.  If pkey_str is in pkey_generate_args, then generate a public/private key pair.
      Return the key pair.
      """
      if pkey_str in pkey_generate_args:
         # generate one
         pubkey_str, pkey_str = api.generate_key_pair( key_size )
         return pubkey_str, pkey_str
      
      else:
         # validate a given one
         try:
            pkey = CryptoKey.importKey( pkey_str )
         except Exception, e:
            log.exception(e)
            raise Exception("Failed to parse private key")
         
         # is it the right size?
         if pkey.size() != key_size - 1:
            raise Exception("Private key has %s bits; expected %s bits" % (pkey.size() + 1, key_size))
         
         return pkey.publickey().exportKey(), pkey_str
   
   
   @classmethod
   def parse_gateway_caps( cls, caps_str, lib=None ):
      """
      Interpret a bitwise OR of gateway caps as a string.
      """
      ret = 0
      
      aliases = {
         "ALL": "GATEWAY_CAP_READ_DATA|GATEWAY_CAP_WRITE_DATA|GATEWAY_CAP_READ_METADATA|GATEWAY_CAP_WRITE_METADATA|GATEWAY_CAP_COORDINATE",
         "READWRITE": "GATEWAY_CAP_READ_DATA|GATEWAY_CAP_WRITE_DATA|GATEWAY_CAP_READ_METADATA|GATEWAY_CAP_WRITE_METADATA",
         "READONLY": "GATEWAY_CAP_READ_DATA|GATEWAY_CAP_READ_METADATA"
      }
      
      if aliases.has_key( caps_str ):
         caps_str = aliases[caps_str]
      
      if isinstance( caps_str, str ):
         flags = caps_str.split("|")
         ret = 0
         
         for flag in flags:
            value = getattr( msconfig, flag, 0 )
            if value == 0:
               raise Exception("Unknown Gateway capability '%s'" % flag)
            
            try:
               ret |= value
            except:
               raise Exception("Invalid value '%s'" % value)
      
      elif isinstance( caps_str, int ):
         ret = caps_str 
      
      else:
         raise Exception("Could not parse capabilities: '%s'" % caps_str )
         
      return ret, {}
   
   
   # Map an argument name to a function that parses and validates it.
   arg_parsers = {}
   
   # what type of key does this object require?
   key_type = None
   
   # which key directory stores this object's key information?
   key_dir = None
   
   # what kinds of keys are maintained by this object?
   internal_keys = []
   
   @classmethod
   def ParseArgs( cls, argspec, args, kw, lib ):
      """
      Insert arguments and keywords from commandline-given arguments.
      Return the new args and kw, as well as a dict of extra information 
      generated by the arg parser that the caller might want to know.
      This method walks the arg_parsers class method.
      """
      extras_all = {}
      
      parsed = []
      
      #log.info("argspec: args=%s, defaults=%s" % (argspec.args, argspec.defaults))
      #log.info("args = %s" % [str(s) for s in args])
      
      # parse args in order
      for i in xrange(0, len(argspec.args)):
         argname = argspec.args[i]
         
         log.debug("parse argument '%s'" % argname)
         
         arg_func = cls.arg_parsers.get( argname, None )
         if arg_func != None:
            args, kw, extras = cls.ReplaceArg( argspec, argname, arg_func, args, kw, lib )
            extras_all.update( extras )
         
         parsed.append( argname )
      
      # parse the keyword arguments, in lexigraphical order
      unparsed = list( set(cls.arg_parsers.keys()) - set(parsed) )
      unparsed.sort()
      
      for argname in unparsed:
         arg_func = cls.arg_parsers[argname]
         args, kw, extras = cls.ReplaceArg( argspec, argname, arg_func, args, kw, lib )
         extras_all.update( extras )
         
      return args, kw, extras_all
   
   
   @classmethod 
   def replace_kw( cls, arg_name, arg_value_func, value, lib ):
      """
      Replace a single keyword argument.
      """
      ret = arg_value_func( cls, value, lib )
      if ret is None:
         raise Exception("Got None for parsing %s" % (arg_name))
      
      lret = 0
      try:
         lret = len(ret)
      except:
         raise Exception("Got non-iterable for parsing %s" % (arg_name))
      
      if len(ret) != 2:
         raise Exception("Invalid value from parsing %s" % (arg_name))
      
      return ret[0], ret[1]


   @classmethod
   def ReplaceArg( cls, argspec, arg_name, arg_value_func, args, kw, lib ):
      """
      Replace a positional or keyword argument named by arg_name by feeding 
      it through with arg_value_func (which takes the argument value as its
      only argument).
      Return the positional arguments, keyword arguments, and extra information
      generated by arg_value_func.
      """
      # find positional argument?
      args = list(args)
      extras = {}
      replaced = False
      
      # replace positional args
      for i in xrange(0, min(len(args), len(argspec.args))):
         if argspec.args[i] == arg_name:
            args[i], arg_extras = arg_value_func( cls, args[i], lib )
            
            log.debug("positional argument '%s' is now '%s'" % (arg_name, args[i]) )
            
            extras.update( arg_extras )
            
            replaced = True
      
      if not replaced and argspec.defaults != None:
         # replace default args 
         for i in xrange(0, len(argspec.defaults)):
            if argspec.args[ len(argspec.args) - len(argspec.defaults) + i ] == arg_name:
               
               value = None
               if arg_name in kw.keys():
                  value = kw[arg_name]
               else:
                  value = argspec.defaults[i]
               
               ret = cls.replace_kw( arg_name, arg_value_func, value, lib )
               
               log.debug("defaulted keyword argument '%s' is now '%s'" % (arg_name, ret[0]))
            
               kw[arg_name], arg_extras = ret[0], ret[1]
               
               extras.update( arg_extras )
            
               replaced = True
      
      # find keyword argument?
      if not replaced and arg_name in kw.keys():
         ret = cls.replace_kw( arg_name, arg_value_func, kw[arg_name], lib )
         
         log.debug("keyword argument '%s' is now '%s'" % (arg_name, ret[0]) )
         
         kw[arg_name], arg_extras = ret[0], ret[1]
         
         extras.update( arg_extras )
         
         replaced = True
      
      return (args, kw, extras)      
         
   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      pass
   
   
   
   
class SyndicateUser( StubObject ):
   
   USER_KEY_UNSET = "unset"
   USER_KEY_UNUSED = "unused"
   
   @classmethod
   def parse_or_generate_signing_public_key( cls, signing_public_key, lib=None ):
      """
      Check a signing public key and verify that it has the appropriate security 
      parameters.  Interpret MAKE_SIGNING_KEY as a command to generate and return one.
      Return pubkey, extras
      """
      extra = {}
      pubkey_pem = None 
      
      if signing_public_key == "MAKE_SIGNING_KEY":
         pubkey_pem, privkey_pem = api.generate_key_pair( OBJECT_KEY_SIZE )
         extra['signing_public_key'] = pubkey_pem
         extra['signing_private_key'] = privkey_pem
         
         signing_public_key = pubkey_pem
      
      elif signing_public_key == "unset":
         return None, extra
      
      else:
         # is this a key literal?
         try:
            pubkey = CryptoKey.importKey( signing_public_key )
            assert not pubkey.has_private()
            
            return signing_public_key, extra
         
         except:
            # not a key literal
            pass
         
         # is this a path?  Try to load it from disk
         try:
            storagelib = lib.storage
         except:
            raise Exception("Missing runtime storage library")
         
         try:
            pubkey = storagelib.read_public_key( signing_public_key )
         except:
            raise Exception("Failed to load %s" % signing_public_key )
         
         pubkey_pem = pubkey.exportKey()
         
      return pubkey_pem, extra
   
   @classmethod
   def parse_user_name_or_id( cls, user_name_or_id, lib=None ):
      """
      Make sure user_name_or_id is an email address.
      """
      try:
         user_id = int(user_name_or_id)
      except:
         if not email_regex.match(user_name_or_id):
            raise Exception("Not an email address: '%s'" % user_name_or_id)
         else:
            if lib is not None:
               lib.user_name = user_name_or_id
               
            return user_name_or_id, {"username": user_name_or_id}
      
      return user_id, {}
      #raise Exception("Parse error: only user emails (not IDs) are allowed")
   
   
   @classmethod
   def generate_password_hash( cls, password, lib=None ):
      """
      Make a password hash and salt from a password.
      Do NOT pass the password; it will be ignored by the MS.
      """
      extra = {}
      pw_salt = api.password_salt()
      pw_hash = api.hash_password( password, pw_salt )
      
      if lib is not None:
         lib.password_salt = pw_salt
         lib.password_hash = pw_hash
      
      return "", extra
   
   @classmethod
   def recover_password_salt( cls, salt, lib=None ):
      """
      Get back the salt we made in generate_password_hash
      """
      if lib is not None and hasattr(lib, "password_salt"):    # got from generate_password_hash
         return lib.password_salt, {}
      else:
         return None, {}
   
   @classmethod
   def recover_password_hash( cls, salt, lib=None ):
      """
      Get back the hash we made in generate_password_hash
      """
      if lib is not None and hasattr(lib, "password_hash"):    # got from generate_password_hash
         return lib.password_hash, {}
      else:
         return None, {}
   
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "email": (lambda cls, arg, lib: cls.parse_user_name_or_id(arg, lib)),
      "activate_password": (lambda cls, arg, lib: cls.generate_password_hash(arg, lib)),
      "activate_password_hash": (lambda cls, arg, lib: cls.recover_password_hash(arg, lib)),
      "activate_password_salt": (lambda cls, arg, lib: cls.recover_password_salt(arg, lib)),
      "signing_public_key": (lambda cls, arg, lib: cls.parse_or_generate_signing_public_key(arg, lib))
   }.items() )
   
   key_type = "user"
   
   key_dir = "user_keys"
   
   
   SIGNING_KEY_TYPE = "signing"
   
   internal_keys = StubObject.internal_keys + [
      SIGNING_KEY_TYPE
   ]
   
   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      super( SyndicateUser, cls ).ProcessExtras( extras, config, method_name, args, kw, result, storage_stub )
      
      # if we deleted the user, remove the private key as well
      if method_name == "delete_user":
         if not extras.has_key('username'):
            log.error("Could not determine username.  Please delete the user's private key from your Syndicate keys directory!")
         
         else:
            log.info("Erasing private key for %s" % extras['username'] )
            storage_stub.erase_private_key( config, "user", extras['username'] )
            
   
   
            

class Volume( StubObject ):
   
   @classmethod
   def parse_volume_name_or_id( cls, volume_name_or_id, lib=None ):
      """
      Make sure volume_name_or_id is a volume name.
      """
      try:
         vid = int(volume_name_or_id)
      except:
         # needed for parse_closure
         if lib is not None:
            lib.volume_name = volume_name_or_id
         
         return volume_name_or_id, {"volume_name": volume_name_or_id}
      
      raise Exception("Parse error: Only volume names (not IDs) are allowed")


   @classmethod
   def parse_metadata_private_key( cls, metadata_private_key, lib=None ):
      """
      Load or generate a metadata private key.  Preserve the public key as
      extra data if we generate one.
      """
      pubkey, privkey = cls.parse_or_generate_private_key( metadata_private_key, ["MAKE_METADATA_KEY"], OBJECT_KEY_SIZE )
      extra = {'metadata_public_key': pubkey, "metadata_private_key": privkey}
      
      return privkey, extra

   @classmethod
   def parse_store_private_key( cls, value, lib=None ):
      """
      Store or do not store metadata private key.
      """
      extra = {}
      try:
         extra['store_private_key'] = bool(value)
      except Exception, e:
         log.error("Failed to parse storage_private_key = '%s'" % value)
         raise e
      
      return value, extra
   
   
   @classmethod 
   def parse_closure( cls, closure_dir, lib=None ):
      """
      Parse a binary closure for allowing the Volume's UGs to connect to the cache providers
      """
      # extract volume name and storage lib
      try:
         volume_name = lib.volume_name
         storagelib = lib.storage 
         config = lib.config
      except Exception, e:
         log.error("Missing parsed call information")
         raise e
      
      if not is_valid_closure_dir( closure_dir, ['__init__.py', 'config.py', 'secrets.py', 'libdriver.so'] ):
         raise Exception("Not a valid Volume closure")
      
      driver_path = os.path.join( closure_dir, "libdriver.so" )
      if not is_valid_binary_driver( driver_path, ["connect_cache"] ):
         raise Exception("%s is missing a 'connect_cache' function" % driver_path )
      
      # load config 
      config_str = load_closure_config( closure_dir, storagelib, json.dumps )
      
      # load the driver 
      driver_str = load_binary_driver( closure_dir, storagelib )
      if driver_str is None:
         raise Exception("No driver found in %s" % closure_path )
      
      essentials = {"driver": driver_str, "config": config_str}
      
      # build the JSON
      json_str = make_closure_json( essentials )
      
      return json_str, {}
      

   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "name":                   (lambda cls, arg, lib: cls.parse_volume_name_or_id(arg, lib)),
      "volume_name_or_id":      (lambda cls, arg, lib: cls.parse_volume_name_or_id(arg, lib)),
      "metadata_private_key":   (lambda cls, arg, lib: cls.parse_metadata_private_key(arg, lib)),
      "default_gateway_caps":   (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib)),
      "store_private_key":      (lambda cls, arg, lib: cls.parse_store_private_key(arg, lib)),
      "closure":                (lambda cls, arg, lib: cls.parse_closure(arg, lib))
   }.items() )
   
   key_type = "volume"
   
   key_dir = "volume_keys"
   
   METADATA_KEY_TYPE = "metadata"
   
   internal_keys = StubObject.internal_keys + [
      METADATA_KEY_TYPE
   ]
   
   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      # process signing and verifying keys 
      super( Volume, cls ).ProcessExtras( extras, config, method_name, args, kw, result, storage_stub )
      
      # process metadata public key
      if extras.get("metadata_public_key", None) != None:
         volume_name = extras.get("volume_name", None)
         if volume_name == None:
            raise Exception("Could not determine name of Volume")
         
         # store it
         storage_stub.store_public_key( config, "volume", volume_name, extras['metadata_public_key'] )

         del extras['metadata_public_key']

      # process metadata private key
      if extras.get("metadata_private_key", None) != None and extras.get("store_private_key", False):
         volume_name = extras.get("volume_name", None)
         if volume_name == None:
            raise Exception("Could not determine name of Volume")
         
         # store it
         storage_stub.store_private_key( config, "volume", volume_name, extras['metadata_private_key'] )

         del extras['metadata_private_key']
         
      # delete public key?
      if method_name == "delete_volume":
         volume_name = extras.get("volume_name", None )
         if volume_name == None:
            log.error("Could not determine name of Volume.  You will need to manaully delete its public key from your Syndicate key directory.")
         
         else:
            storage_stub.erase_public_key( config, "volume", volume_name )
         

class Gateway( StubObject ):
   
   @classmethod 
   def seal_private_key( cls, key_str, password ):
      """
      Seal data with the password
      """
      import syndicate.syndicate as c_syndicate
      
      rc, sealed_data = c_syndicate.password_seal( key_str, password )
      if rc != 0:
         raise Exception("Failed to seal data with password: rc = %s" % rc)
      
      return sealed_data

   
   @classmethod
   def parse_gateway_name_or_id( cls, gateway_name_or_id, lib=None ):
      """
      Make usre gateway_name_or_id is a gateway name.
      """
      try:
         vid = int(gateway_name_or_id)
      except:
         # needed for parse_gateway_closure
         if lib is not None:
            lib.gateway_name = gateway_name_or_id
         
         return gateway_name_or_id, {"gateway_name": gateway_name_or_id}
      
      raise Exception("Parse error: Only Gateway names (not IDs) are allowed")

      
   @classmethod
   def parse_gateway_type( cls, type_str, lib ):
      """
      Parse UG, RG, or AG into a gateway type constant.
      """
      gtype = None
      extras = {}
      if type_str == "UG":
         gtype = GATEWAY_TYPE_UG
      elif type_str == "RG":
         gtype = GATEWAY_TYPE_RG
      elif type_str == "AG":
         gtype = GATEWAY_TYPE_AG
      
      if gtype != None:
         # needed for parse_gateway_config
         if lib is not None:
            lib.gateway_type = gtype
            
         return (gtype, extras)
      raise Exception("Unknown Gateway type '%s'" % type_str)
   
   
   @classmethod
   def parse_encryption_password( cls, passwd, lib ):
      """
      Hold on to the password in lib.
      """
      lib.encryption_password = passwd
      
      return passwd, {}
   
   
   @classmethod
   def parse_gateway_public_key( cls, gateway_public_key, lib ):
      """
      Load or generate a gateway public key.  Preserve the private key 
      as extra data if we generate one.
      """
      pubkey_str, privkey_str = cls.parse_or_generate_private_key( gateway_public_key, ["MAKE_GATEWAY_KEY", "MAKE_AND_HOST_GATEWAY_KEY"], OBJECT_KEY_SIZE )
      
      host_private_key = (gateway_public_key == "MAKE_AND_HOST_GATEWAY_KEY")
      
      if host_private_key:
         # seal the key with the password
         if lib is None:
            raise Exception("No password given.  Needed for gateway_public_key == MAKE_AND_HOST_GATEWAY_KEY.")
         
         if not hasattr(lib, "encryption_password") or lib.encryption_password is None:
            password = getpass.getpass("Gateway private key password: ")
            lib.encryption_password = password
         
         encrypted_key_str = Gateway.seal_private_key( privkey_str, lib.encryption_password )
         
         # save for later, to be recovered as keyword arguments
         lib.encrypted_gateway_key_str = encrypted_key_str
         
      else:
         log.warning("MS will not host private key.  Be sure to back it up and keep it safe!")
         
      extra = {'gateway_private_key': privkey_str}
      lib.gateway_public_key_str = pubkey_str           # validate against private key, if we need to load it
      lib.gateway_private_key_str = privkey_str
      
      return pubkey_str, extra
   
   
   @classmethod
   def recover_or_load_private_key( cls, gateway_private_key_path, lib ):
      """
      Load a private key from disk, or recover one previously generated from parsing the public key argument.
      """
      if gateway_private_key_path is not None:
         # need an encryption password
         if lib is None:
            raise Exception("No password given.  Needed for host_private_key.")
         
         if not hasattr(lib, "encryption_password"):
            # prompt for it
            password = getpass.getpass("Gateway private key password: ")
            lib.encryption_password = password
               
         try:
            fd = open(gateway_private_key_path, "r")
            privkey_str = fd.read()
            fd.close()
         except OSError, oe:
            log.error("Failed to read private key from %s" % gateway_private_key_path)
            raise oe
         
         try:
            # verify it's valid
            privkey = CryptoKey.importKey( privkey_str )
            assert privkey.has_private(), "Not a private key"
         except Exception, e:
            log.error("Failed to parse private key")
            raise e
         
         if hasattr(lib, "gateway_public_key_str" ):
            try:
               # ensure it matches the public key, if given 
               pubkey_str = CryptoKey.importKey( gateway_public_key_str ).exportKey()
            except Exception, e:
               log.error("Failed to parse previously-loaded public key")
               raise e
            
            assert pubkey_str == privkey.publickey().exportKey(), "Public key does not match private key"
            
            
         # seal with password
         encrypted_key_str = Gateway.seal_private_key( privkey.exportKey(), lib.encryption_password )
         
         return base64.b64encode( encrypted_privkey_str ), {}
            
      elif lib is not None:
         if hasattr( lib, "encrypted_gateway_key_str" ):
            # recover from lib--was generated earlier
            return base64.b64encode( lib.encrypted_gateway_key_str ), {}
         else:
            raise Exception("BUG: don't know how to handle gateway private key")
   
   
   @classmethod 
   def is_valid_RG_closure_dir( cls, gateway_closure_path ):
      # does the given directory have the appropriate files to be an RG closure?
      return is_valid_closure_dir( gateway_closure_path, ['__init__.py', 'driver.py', 'replica.py', 'config.py', 'secrets.py'] );
   
   @classmethod 
   def is_valid_UG_closure_dir( cls, gateway_closure_path ):
      # does the given directory have the appropriate files to be an UG closure?
      return is_valid_closure_dir( gateway_closure_path, ['__init__.py', 'config.py', 'secrets.py', 'libdriver.so'] )
   
   @classmethod 
   def is_valid_AG_closure_dir( cls, gateway_closure_path ):
      # does the given directory have the appropriate files to be an AG closure?
      # TODO: come back and refine this
      return is_valid_closure_dir( gateway_closure_path, ['__init__.py', 'spec.xml', 'config.py', 'secrets.py', 'libdriver.so'] )
   
   
   @classmethod 
   def has_valid_UG_driver( cls, gateway_closure_path ):
      """
      Does a given path refer to a UG binary closure?
      """
      driver_path = os.path.join( gateway_closure_path, "libdriver.so" )
      return is_valid_binary_driver( driver_path, ["connect_cache", "write_block_preup", "write_manifest_preup", "read_block_postdown", "read_manifest_postdown", "chcoord_begin", "chcoord_end"] )
   
   
   @classmethod 
   def has_valid_AG_driver( cls, gateway_closure_path ):
      """
      Does a given path refer to an AG binary closure?
      """
      driver_path = os.path.join( gateway_closure_path, "libdriver.so" )
      return is_valid_binary_driver( driver_path, ["get_dataset", "cleanup_dataset", "publish_dataset", "connect_dataset", "controller"] )
   
   
   @classmethod 
   def is_wellformed_RG_closure( cls, gateway_closure_path ):
      """
      Does the given directory look like a well-formed RG closure?
      """
      if not cls.is_valid_RG_closure_dir( gateway_closure_path ):
         return False
      
      return True
   
   @classmethod 
   def is_wellformed_UG_closure( cls, gateway_closure_path ):
      """
      Does the given directory look like a well-formed UG closure?
      """
      if not cls.is_valid_UG_closure_dir( gateway_closure_path ):
         return False
      
      if not cls.has_valid_UG_driver( gateway_closure_path ):
         return False
      
      return True
   
   @classmethod 
   def is_wellformed_AG_closure( cls, gateway_closure_path ):
      """
      Does the given directory look like a well-formed AG closure?
      """
      if not cls.is_valid_AG_closure_dir( gateway_closure_path ):
         return False
      
      if not cls.has_valid_AG_driver( gateway_closure_path ):
         return False 
      
      return True
   
   
   @classmethod 
   def parse_gateway_closure( cls, gateway_closure_path, lib=None ):
      """
      Parse the gateway closure.  Figure out what kind of closure it is, and load it.
      
      NOTE: we don't check to see if the closure matches the type of gateway.
      
      NOTE: lib must have:
       * gateway name
       * storage API
      """
      
      if cls.is_wellformed_RG_closure( gateway_closure_path ):
         return cls.parse_RG_closure( gateway_closure_path, lib )
      
      elif cls.is_wellformed_UG_closure( gateway_closure_path ):
         return cls.parse_UG_closure( gateway_closure_path, lib )
      
      elif cls.is_wellformed_AG_closure( gateway_closure_path ):
         return cls.parse_AG_closure( gateway_closure_path, lib )
   
      raise Exception("Not a well-formed closure: %s" % gateway_closure_path) 
   
   
   @classmethod 
   def parse_UG_closure( cls, gateway_closure_path, lib=None ):
      """
      Load a UG binary driver, its config, and its secrets.
      Generate a JSON string containing them all.
      """
      
      try:
         gateway_name = lib.gateway_name
         storagelib = lib.storage 
         config = lib.config
      except Exception, e:
         log.error("Missing parsed call information")
         raise e
      
      # maybe generated a private key earlier?
      privkey_pem = None
      try:
         privkey_pem = lib.gateway_private_key_str
      except:
         pass
      
      essentials = load_binary_closure_essentials( gateway_closure_path, "gateway", gateway_name, config, storagelib, privkey_pem=privkey_pem )
      
      # UG needs nothing more...
      
      # build the JSON
      json_str = make_closure_json( essentials )
      
      return json_str, {}
   
      
   @classmethod 
   def parse_AG_closure( cls, gateway_closure_path, lib=None ):
      """
      Load an AG binary driver, as well as its config, secrets, and spec file.
      Generate a JSON string containing them all.
      """
      
      # get library data
      try:
         gateway_name = lib.gateway_name
         storagelib = lib.storage
         config = lib.config
      except Exception, e:
         log.error("Missing parsed call information")
         raise e
      
      # maybe generated a private key earlier?
      privkey_pem = None
      try:
         privkey_pem = lib.gateway_private_key_str
      except:
         pass
      
      driver_components = load_binary_closure_essentials( gateway_closure_path, "gateway", gateway_name, config, storagelib, privkey_pem=privkey_pem )
      
      # load the spec file 
      spec_file_path = os.path.join( gateway_closure_path, "spec.xml" )
      spec_file_str = storagelib.read_file( spec_file_path )
      if spec_file_str is None:
         raise Exception("Failed to load spec file from %s" % spec_file_path )
      
      driver_components["spec"] = spec_file_str
      
      # build the JSON
      json_str = make_closure_json( driver_components )
      
      return json_str, {}
   
   
   @classmethod
   def parse_RG_closure( cls, gateway_closure_path, lib=None ):
      """
      Load up a gateway config (replication policy and drivers) from a set of python files.
      Transform them into a JSON-ized closure to be deployed to the recipient gateway.
      
      NOTE: lib must have:
       * config
       * storage API
      """
      
      try:
         import syndicate.rg.closure as rg_closure
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to load RG closure module")
      
      
      # get library data
      try:
         gateway_name = lib.gateway_name 
         storagelib = lib.storage
         config = lib.config
      except Exception, e:
         log.error("Missing parsed call information")
         raise e
      
      # maybe generated a private key earlier?
      privkey_pem = None
      try:
         privkey_pem = lib.gateway_private_key_str
      except:
         pass
      
      # get config and secrets data
      config_dict_str, secrets_dict_str = load_config_and_secrets( gateway_closure_path, "gateway", gateway_name, config, storagelib, privkey_pem=privkey_pem )
      
      # get the replica.py and driver.py files, if they exist 
      replica_py_path = os.path.join( gateway_closure_path, "replica.py" )
      driver_py_path = os.path.join( gateway_closure_path, "driver.py" )
      
      replica_py = None 
      driver_py = None 
      
      if os.path.exists( replica_py_path ):
         replica_py = storagelib.read_file( replica_py_path )
         
      if os.path.exists( driver_py_path ):
         driver_py = storagelib.read_file( driver_py_path )
      
      # need replica, but not driver
      if replica_py == None:
         raise Exception("Missing %s" % replica_py_path)
      
      drivers = []
      if driver_py:
         drivers.append( ("builtin", driver_py) )
      
      # validate the closure by verifying that we can deserialize and load it
      try:
         closure = rg_closure.load_closure( base64.b64encode( replica_py ), base64.b64encode( config_dict_str ), None )
         assert closure != None, "closure == None"
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to load closure")
      
      # validate the driver, if needed
      if driver_py:
         try:
            driver = rg_closure.load_storage_driver( "builtin", base64.b64encode( driver_py ) )
            assert driver != None, "driver == None"
         except Exception, e:
            log.exception(e)
            raise Exception("Failed to load driver")
      
      # package up the closure
      closure_json_str = rg_closure.make_closure_json( replica_py, config_dict_str, secrets_dict_str, drivers )
      
      return closure_json_str, {}
   
   
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "gateway_name":           (lambda cls, arg, lib: cls.parse_gateway_name_or_id(arg, lib)),
      "g_name_or_id":           (lambda cls, arg, lib: cls.parse_gateway_name_or_id(arg, lib)),
      "ug_caps":                (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib)),
      "caps":                   (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib)),
      "gateway_type":           (lambda cls, arg, lib: cls.parse_gateway_type(arg, lib)),
      "gateway_public_key":     (lambda cls, arg, lib: cls.parse_gateway_public_key(arg, lib)),
      "closure":                (lambda cls, arg, lib: cls.parse_gateway_closure(arg, lib)),
      "host_gateway_key":       (lambda cls, arg, lib: cls.recover_or_load_private_key(arg, lib)),
      "encryption_password":    (lambda cls, arg, lib: cls.parse_encryption_password(arg, lib)),
   }.items() )
   
   key_type = "gateway"
   
   key_dir = "gateway_keys"
   
   RUNTIME_KEY_TYPE = "runtime"
   
   internal_keys = StubObject.internal_keys + [
      RUNTIME_KEY_TYPE
   ]

   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      # process signing and verifying keys 
      super( Gateway, cls ).ProcessExtras( extras, config, method_name, args, kw, result, storage_stub )
      
      # process runtime key
      if extras.get("gateway_private_key", None) != None:
         gateway_name = extras.get("gateway_name", None)
         if gateway_name == None:
            raise Exception("Could not determine name of Gateway")
         
         # store it
         storage_stub.store_private_key( config, "gateway", gateway_name, extras['gateway_private_key'] )
         
         del extras['gateway_private_key']
      
      # erase private key if deleted
      if method_name == "delete_gateway":
         gateway_name = extras.get("gateway_name", None)
         if gateway_name == None:
            log.error("Failed to determine the name of the gateway.  You will need to remove the gateway's private key manually")
         
         else:
            storage_stub.erase_private_key( config, "gateway", gateway_name )


class VolumeAccessRequest( StubObject ):
   
   @classmethod
   def parse_allowed_gateways( cls, allowed_gateways, lib=None ):
      """
      Parse allowed gateways (string or int)
      """
      
      if isinstance( allowed_gateways, str ):
         gtypes = allowed_gateways.split("|")
         ret = 0
         
         for gtype in gtypes:
            value = getattr( msconfig, gtype, 0 )
            if value == 0:
               raise Exception("Unknown Gateway type '%s'" % gtype)
            
            try:
               ret |= (1 << value)
            except:
               raise Exception("Invalid value '%s'" % value)
      
      elif isinstance( allowed_gateways, int ):
         ret = allowed_gateways 
      
      else:
         raise Exception("Could not parse Gateway types: '%s'" % allowed_gateways )
         
      return ret, {}
   
      
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "ug_caps":                   (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib)),
      "allowed_gateways":          (lambda cls, arg, lib: cls.parse_allowed_gateways(arg, lib)),
   }.items() )


object_classes = [SyndicateUser, Volume, Gateway, VolumeAccessRequest]
