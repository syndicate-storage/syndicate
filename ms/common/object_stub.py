#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

# stubs for all Syndicate objects

import msconfig
from msconfig import *
import api
import inspect
import re 

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

try:
   import syndicate.client.common.log as log
except:
   import log

log = log.log

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
   def parse_or_generate_signing_public_key( cls, signing_public_key ):
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
   def parse_or_generate_private_key( cls, pkey_str, pkey_arg_name, key_size ):
      """
      Check a private key (pkey_str) and verify that it has the appopriate security 
      parameters.  If pkey_str == pkey_arg_name, then generate a public/private key pair.
      Return the key pair.
      """
      if pkey_str == pkey_arg_name:
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
   def parse_gateway_caps( cls, caps_str ):
      """
      Interpret a bitwise OR of gateway caps as a string.
      """
      ret = 0
      
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
      "signing_public_key": (lambda cls, arg: cls.parse_or_generate_signing_public_key(arg))
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
   def ParseArgs( cls, argspec, args, kw ):
      """
      Insert arguments and keywords from commandline-given arguments.
      Return the new args and kw, as well as a dict of extra information 
      generated by the arg parser that the caller might want to know.
      This method walks the arg_parsers class method.
      """
      extras_all = {}
      for arg_name, arg_func in cls.arg_parsers.items():
         args, kw, extras = cls.ReplaceArg( argspec, arg_name, arg_func, args, kw )
         extras_all.update( extras )
      
      return args, kw, extras_all
   
   @classmethod
   def ReplaceArg( cls, argspec, arg_name, arg_value_func, args, kw ):
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
            args[i], arg_extras = arg_value_func( cls, args[i] )
            extras.update( arg_extras )
      
      # find keyword argument?
      if arg_name in kw.keys():
         kw[arg_name], arg_extras = arg_value_func( cls, kw[arg_name] )
         extras.update( arg_extras )
      
      return (args, kw, extras)
         
   
   @classmethod
   def ProcessExtras( cls, extras, config, method_name, args, kw, result, storage_stub ):
      pass
         
         
   
class SyndicateUser( StubObject ):
   @classmethod
   def parse_user_name_or_id( cls, user_name_or_id ):
      """
      Make usre user_name_or_id is an email address.
      """
      try:
         user_id = int(user_name_or_id)
      except:
         if not email_regex.match(user_name_or_id):
            raise Exception("Not an email address: '%s'" % user_name_or_id)
         else:
            return user_name_or_id, {}
      
      raise Exception("Parse error: only user emails (not IDs) are allowed")
   
   
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "email": (lambda cls, arg: cls.parse_user_name_or_id(arg))
   }.items() )
   
   key_type = "user"
   
   key_dir = "user_keys"
            
            
            
            

class Volume( StubObject ):
   
   @classmethod
   def parse_volume_name_or_id( cls, volume_name_or_id ):
      """
      Make sure volume_name_or_id is a volume name.
      """
      try:
         vid = int(volume_name_or_id)
      except:
         return volume_name_or_id, {"volume_name": volume_name_or_id}
      
      raise Exception("Parse error: Only volume names (not IDs) are allowed")


   @classmethod
   def parse_metadata_private_key( cls, metadata_private_key ):
      """
      Load or generate a metadata private key.  Preserve the public key as
      extra data if we generate one.
      """
      pubkey, privkey = cls.parse_or_generate_private_key( metadata_private_key, "MAKE_METADATA_KEY", OBJECT_KEY_SIZE )
      extra = {'metadata_public_key': pubkey}
      
      return privkey, extra

   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "name":                   (lambda cls, arg: cls.parse_volume_name_or_id(arg)),
      "volume_name_or_id":      (lambda cls, arg: cls.parse_volume_name_or_id(arg)),
      "metadata_private_key":   (lambda cls, arg: cls.parse_metadata_private_key(arg)),
      "default_gateway_caps":   (lambda cls, arg: cls.parse_gateway_caps(arg))
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
   def parse_gateway_name_or_id( cls, gateway_name_or_id ):
      """
      Make usre gateway_name_or_id is a gateway name.
      """
      try:
         vid = int(gateway_name_or_id)
      except:
         return gateway_name_or_id, {"gateway_name": gateway_name_or_id}
      
      raise Exception("Parse error: Only Gateway names (not IDs) are allowed")

      
   @classmethod
   def parse_gateway_type( cls, type_str ):
      """
      Parse UG, RG, or AG into a gateway type constant.
      """
      if type_str == "UG":
         return GATEWAY_TYPE_UG, {}
      elif type_str == "RG":
         return GATEWAY_TYPE_RG, {}
      elif type_str == "AG":
         return GATEWAY_TYPE_AG, {}
      
      raise Exception("Unknown Gateway type '%s'" % type_str)
   
   @classmethod
   def parse_gateway_public_key( cls, gateway_private_key ):
      """
      Load or generate a gateway public key.  Preserve the private key 
      as extra data if we generate one.
      """
      pubkey, privkey = cls.parse_or_generate_private_key( gateway_private_key, "MAKE_GATEWAY_KEY", OBJECT_KEY_SIZE )
      extra = {'gateway_private_key': privkey}
      
      return pubkey, extra
   
   """
   @classmethod
   def parse_gateway_config( cls, gateway_config_path ):
      
      # Load up a gateway config (replication policy and drivers) from a set of python files.
      # Transform them into a JSON-ized closure to be deployed to the recipient gateways.
      
      
      # first, find the __init__.py file
      init_path = os.path.join( gateway_config_path, "__init__.py" )
      try:
         init_fd = open(init_path, "r")
         init_fd.close()
      except Exception, e:
         log.exception(e)
         raise Exception("No __init__.py file can be found in %s" % gateway_config_path)
      
      # attempt to import this
      sys.path.append( os.path.dirname(gateway_config_path) )
      config_module_name = os.path.basename(gateway_config_path).strip("/")
      
      try:
         config_module = __import__( config_module_name, {}, {}, [], -1 )
      except Exception, e:
         log.exception(e)
         raise Exception("Failed to import %s" % gateway_config_path )
      
      # successfully imported module
      # attempt to import the secrets module
      secrets_module = None
      try:
         secrets_module = __import__( config_module.__name__ + ".secrets", {}, {}, ['secrets'], -1 )
      except Exception, e:
         log.error("No secrets package found")
         raise e

      if secrets_module:
         # get the secrets dictionary 
         secrets_dict = None
         try:
            secrets_dict = secrets_module.SECRETS
            assert isinstance( secrets_dict, dict )
         except Exception, e:
            log.error("No SECRETS dictionary found")
            raise e
         
         if secrets_dict != None:
            # encrypt the secrets dictionary
            
   """   
   
   arg_parsers = dict( StubObject.arg_parsers.items() + {
      "gateway_name":           (lambda cls, arg: cls.parse_gateway_name_or_id(arg)),
      "g_name_or_id":           (lambda cls, arg: cls.parse_gateway_name_or_id(arg)),
      "caps":                   (lambda cls, arg: cls.parse_gateway_caps(arg)),
      "gateway_type":           (lambda cls, arg: cls.parse_gateway_type(arg)),
      "gateway_public_key":     (lambda cls, arg: cls.parse_gateway_public_key(arg)),
      "config":                 (lambda cls, arg: cls.parse_gateway_config(arg))
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

