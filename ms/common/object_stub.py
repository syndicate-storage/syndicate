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

try:
   import pickle
except:
   # only needed by clients...
   pass

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

try:
   import syndicate.client.common.log as Log
except:
   import log as Log
   
try:
   import syndicate.client.storage as storage 
except:
   import storage_stub as storage

log = Log.get_logger()

# RFC-822 compliant, as long as there aren't any comments in the address.
# taken from http://chrisbailey.blogs.ilrt.org/2013/08/19/validating-email-addresses-in-python/
email_regex_str = r"^(?=^.{1,256}$)(?=.{1,64}@)(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22)(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22))*\x40(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d])(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d]))*$"

email_regex = re.compile( email_regex_str )

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
      pass
   
   @classmethod
   def Sign( cls, *args, **kw ):
      pass

   @classmethod
   def parse_or_generate_signing_public_key( cls, signing_public_key, lib ):
      """
      Check a signing public key and verify that it has the appropriate security 
      parameters.  Interpret MAKE_SIGNING_KEY as a command to generate and return one.
      Return pubkey, extras
      """
      extra = {}
      
      if signing_public_key == "MAKE_SIGNING_KEY":
         pubkey_pem, privkey_pem = api.generate_key_pair( OBJECT_KEY_SIZE )
         extra['signing_public_key'] = pubkey_pem
         extra['signing_private_key'] = privkey_pem
         
         signing_public_key = pubkey_pem
      
      else:
         # try validating the given one
         try:
            pubkey = CryptoKey.importKey( signing_public_key )
         except Exception, e:
            log.exception(e)
            raise Exception("Failed to parse public key")
         
      return signing_public_key, extra
   
   
   @classmethod
   def parse_or_generate_private_key( cls, pkey_str, pkey_generate_arg, key_size ):
      """
      Check a private key (pkey_str) and verify that it has the appopriate security 
      parameters.  If pkey_str == pkey_generate_arg, then generate a public/private key pair.
      Return the key pair.
      """
      if pkey_str == pkey_generate_arg:
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
   def parse_gateway_caps( cls, caps_str, lib ):
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
   arg_parsers = {
      "signing_public_key": (lambda cls, arg, lib: cls.parse_or_generate_signing_public_key(arg, lib))
   }
   
   # what type of key does this object require?
   key_type = None
   
   # which key directory stores this object's key information?
   key_dir = None
   
   # what kinds of keys are maintained by this object?
   internal_keys = [
      "signing",
      "verifying"
   ]
   
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
      
      # fill default arguments
      if argspec.defaults != None:
         for i in xrange(0, len(argspec.defaults)):
            kw[ argspec.args[ len(argspec.args) - len(argspec.defaults) + i] ] = argspec.defaults[i]
      
      # parse args in order
      for i in xrange(0, len(argspec.args)):
         argname = argspec.args[i]
         arg_func = cls.arg_parsers.get( argname, None )
         if arg_func != None:
            args, kw, extras = cls.ReplaceArg( argspec, argname, arg_func, args, kw, lib )
            extras_all.update( extras )
         
         parsed.append( argname )
      
      # parse the keyword arguments
      unparsed = list( set(cls.arg_parsers.keys()) - set(parsed) )
      
      for argname in unparsed:
         arg_func = cls.arg_parsers[argname]
         args, kw, extras = cls.ReplaceArg( argspec, argname, arg_func, args, kw, lib )
         extras_all.update( extras )
         
      return args, kw, extras_all
   
   
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
      
      for i in xrange(0, min(len(args), len(argspec.args))):
         if argspec.args[i] == arg_name:
            args[i], arg_extras = arg_value_func( cls, args[i], lib )
            extras.update( arg_extras )
      
      # find keyword argument?
      if arg_name in kw.keys():
         kw[arg_name], arg_extras = arg_value_func( cls, kw[arg_name], lib )
         extras.update( arg_extras )
      
      return (args, kw, extras)
         
   
   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      pass
         
         
   
class SyndicateUser( StubObject ):
   @classmethod
   def parse_user_name_or_id( cls, user_name_or_id, lib ):
      """
      Make usre user_name_or_id is an email address.
      """
      try:
         user_id = int(user_name_or_id)
      except:
         if not email_regex.match(user_name_or_id):
            raise Exception("Not an email address: '%s'" % user_name_or_id)
         else:
            lib.user_name = user_name_or_id
            return user_name_or_id, {}
      
      raise Exception("Parse error: only user emails (not IDs) are allowed")
   
   
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "email": (lambda cls, arg, lib: cls.parse_user_name_or_id(arg, lib))
   }.items() )
   
   key_type = "user"
   
   key_dir = "user_keys"
            
            
            
            

class Volume( StubObject ):
   
   @classmethod
   def parse_volume_name_or_id( cls, volume_name_or_id, lib ):
      """
      Make sure volume_name_or_id is a volume name.
      """
      try:
         vid = int(volume_name_or_id)
      except:
         return volume_name_or_id, {"volume_name": volume_name_or_id}
      
      raise Exception("Parse error: Only volume names (not IDs) are allowed")


   @classmethod
   def parse_metadata_private_key( cls, metadata_private_key, lib ):
      """
      Load or generate a metadata private key.  Preserve the public key as
      extra data if we generate one.
      """
      pubkey, privkey = cls.parse_or_generate_private_key( metadata_private_key, "MAKE_METADATA_KEY", OBJECT_KEY_SIZE )
      extra = {'metadata_public_key': pubkey}
      
      return privkey, extra

   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "name":                   (lambda cls, arg, lib: cls.parse_volume_name_or_id(arg, lib)),
      "volume_name_or_id":      (lambda cls, arg, lib: cls.parse_volume_name_or_id(arg, lib)),
      "metadata_private_key":   (lambda cls, arg, lib: cls.parse_metadata_private_key(arg, lib)),
      "default_gateway_caps":   (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib))
   }.items() )
   
   key_type = "volume"
   
   key_dir = "volume_keys"
   
   internal_keys = StubObject.internal_keys + [
      "metadata"
   ]
   

   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      # process signing and verifying keys 
      super( Volume, cls ).ProcessExtras( extras, config, method_name, args, kw, result, storage_stub )
      
      # process metadata key
      if extras.get("metadata_public_key", None) != None:
         volume_name = extras.get("volume_name", None)
         if volume_name == None:
            raise Exception("Could not determine name of Volume")
         
         # store it
         storage_stub.store_object_public_key( config, "volume", "metadata", volume_name, extras['metadata_public_key'] )

         del extras['metadata_public_key']



class Gateway( StubObject ):
   @classmethod
   def parse_gateway_name_or_id( cls, gateway_name_or_id, lib ):
      """
      Make usre gateway_name_or_id is a gateway name.
      """
      try:
         vid = int(gateway_name_or_id)
      except:
         # needed for parse_gateway_config
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
   def parse_gateway_public_key( cls, gateway_public_key, lib ):
      """
      Load or generate a gateway public key.  Preserve the private key 
      as extra data if we generate one.
      """
      pubkey, privkey = cls.parse_or_generate_private_key( gateway_public_key, "MAKE_GATEWAY_KEY", OBJECT_KEY_SIZE )
      extra = {'gateway_private_key': privkey}
      
      return pubkey, extra
   
   
   @classmethod
   def get_dict_from_closure( cls, config_module, dict_module_name, dict_name ):
      
      # successfully imported module
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
   
   
   @classmethod
   def parse_gateway_closure( cls, gateway_closure_path, lib ):
      """
      Load up a gateway config (replication policy and drivers) from a set of python files.
      Transform them into a JSON-ized closure to be deployed to the recipient gateway.
      
      NOTE: you're limited to one driver.py file.
      
      NOTE: lib must have:
       * gateway name
       * syntool config
       * storage API
      """
      
      try:
         import syndicate.syndicate as c_syndicate
         import syndicate.rg.closure as rg_closure
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to load libsyndicate")
      
      # get library data
      try:
         gateway_name = lib.gateway_name 
         config = lib.config
         storage = lib.storage
      except Exception, e:
         log.exception(e)
         raise Exception("Missing required data for argument parsing")
      
      # attempt to import the closure
      dirname = os.path.dirname(gateway_closure_path.rstrip("/"))
      sys.path.append( dirname )
      config_module_name = gateway_closure_path[len(dirname) + 1:].strip("/")
      
      try:
         closure_module = __import__( config_module_name, {}, {}, [], -1 )
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to import %s from %s" % (config_module_name, gateway_closure_path) )
      
      
      secrets_dict = {}
      config_dict = {}
      
      # get the secrets
      try:
         secrets_dict = cls.get_dict_from_closure( closure_module, "secrets", "SECRETS" )
      except Exception, e:
         log.warning("No secrets defined")
     
      # get the config
      try:
         config_dict = cls.get_dict_from_closure( closure_module, "config", "CONFIG" )
      except Exception, e:
         log.warning("No config defined")
      
      # serialize the config and secrets dictionaries 
      secrets_dict_str = None
      config_dict_str = None
      try:
         config_dict_str = pickle.dumps( config_dict )
         secrets_dict_str = pickle.dumps( secrets_dict )
      except Exception, e:
         log.exception( e )
         raise Exception("Failed to serialize")
      
      
      if len(secrets_dict.keys()) > 0:
         # get the gateway public key 
         gateway_pubkey_pem = None
         try:
            gateway_privkey = storage.load_object_private_key( config, "gateway", "runtime", gateway_name )
            gateway_pubkey_pem = gateway_privkey.publickey().exportKey()
         except Exception, e:
            log.exception( e )
            raise Exception("Failed to load runtime private key for %s" % gateway_name )
         
         # encrypt the serialized secrets dict 
         # TODO plaintext padding
         rc = 0
         encrypted_secrets_str = None
         try:
            log.info("Encrypting secrets...")
            rc, encrypted_secrets_str = c_syndicate.encrypt_closure_secrets( gateway_pubkey_pem, secrets_dict_str )
         except Exception, e:
            log.exception( e )
            raise Exception("Failed to encrypt secrets")
         
         if rc != 0 or encrypted_secrets_str == None:
            raise Exception("Failed to encrypt secrets, rc = %d" % rc)
         
         secrets_dict_str = encrypted_secrets_str
      
      # get the replica.py and driver.py files, if they exist 
      replica_py_path = os.path.join( gateway_closure_path, "replica.py" )
      driver_py_path = os.path.join( gateway_closure_path, "driver.py" )
      
      replica_py = None 
      driver_py = None 
      
      if os.path.exists( replica_py_path ):
         replica_py = storage.read_file( replica_py_path )
         
      if os.path.exists( driver_py_path ):
         driver_py = storage.read_file( driver_py_path )
      
      # need replica, but not driver
      if replica_py == None:
         raise Exception("Missing %s" % replica_py_path)
      
      drivers = []
      if driver_py:
         drivers.append( ("builtin", driver_py) )
      
      # validate the closure
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
      "caps":                   (lambda cls, arg, lib: cls.parse_gateway_caps(arg, lib)),
      "gateway_type":           (lambda cls, arg, lib: cls.parse_gateway_type(arg, lib)),
      "gateway_public_key":     (lambda cls, arg, lib: cls.parse_gateway_public_key(arg, lib)),
      "closure":                (lambda cls, arg, lib: cls.parse_gateway_closure(arg, lib))
   }.items() )
   
   key_type = "gateway"
   
   key_dir = "gateway_keys"
   
   internal_keys = StubObject.internal_keys + [
      "runtime"
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
         storage_stub.store_object_private_key( config, "gateway", "runtime", gateway_name, extras['gateway_private_key'] )
         
         del extras['gateway_private_key']


class VolumeAccessRequest( StubObject ):
   pass

