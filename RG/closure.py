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
import sys
import syndicate.rg.common as rg_common
import types
import inspect
import collections
import json
import base64
import threading
import errno
import pickle
import imp
import resource
import traceback

import syndicate.syndicate as c_syndicate

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner


#-------------------------
StorageDriver = collections.namedtuple("StorageDriver", ["name", "module", "read_file", "write_file", "delete_file"])
StorageClosure = collections.namedtuple("StorageClosure", ["module", "replica_read", "replica_write", "replica_delete"])
StorageConfig = collections.namedtuple("StorageConfig", ["closure", "drivers"] )
StorageContext = collections.namedtuple("StorageContext", ["config", "drivers", "secrets", "log"] )

#-------------------------
STORAGE_CONFIG = {}
storage_closure_lock = threading.Lock()

SECRETS_PAD_KEY = "__syndicate_pad__"

#-------------------------
REQUIRED_CLOSURE_FIELDS = {
   "replica_read": types.FunctionType,
   "replica_write": types.FunctionType,
   "replica_delete": types.FunctionType
}

#-------------------------
REQUIRED_DRIVER_FIELDS = {
   "read_file": types.FunctionType,
   "write_file": types.FunctionType,
   "delete_file": types.FunctionType
}

#-------------------------
REQUIRED_JSON_FIELDS = {
   "closure": types.UnicodeType,
   "secrets": types.UnicodeType,
   "config": types.UnicodeType,
   "drivers": types.ListType,
   #"user_sig": types.UnicodeType
}

REQUIRED_JSON_DRIVER_FIELDS = {
   "name": types.UnicodeType
}

OPTIONAL_JSON_DRIVER_FIELDS = {
   "code": types.UnicodeType
}

#-------------------------
CLOSURE_READ_SIGNATURE = inspect.ArgSpec( args=['context', 'request_info', 'filename', 'outfile'], varargs=None, defaults=None, keywords=None )
CLOSURE_WRITE_SIGNATURE = inspect.ArgSpec( args=['context', 'request_info', 'filename', 'infile'], varargs=None, defaults=None, keywords=None )
CLOSURE_DELETE_SIGNATURE = inspect.ArgSpec( args=['context', 'request_info', 'filename'], varargs=None, defaults=None, keywords=None )

DRIVER_READ_SIGNATURE = inspect.ArgSpec( args=['filename', 'outfile'], varargs=None, defaults=None, keywords='kw' )
DRIVER_WRITE_SIGNATURE = inspect.ArgSpec( args=['filename', 'infile'], varargs=None, defaults=None, keywords='kw' )
DRIVER_DELETE_SIGNATURE = inspect.ArgSpec( args=['filename'], varargs=None, defaults=None, keywords='kw' )

#-------------------------
GATEWAY_PRIVKEY_PEM = None
SENDER_PUBKEY_PEM = None

#-------------------------
log = rg_common.get_logger()
   
#-------------------------
def is_valid_function( func, signature ):
   '''
      Is a particular Python object a function with the correct signature?
   '''
   
   if not inspect.isfunction( func ):
      return False
   
   argspec = inspect.getargspec( func )
   
   def_len = 0
   if argspec.defaults != None:
      def_len = len(argspec.defaults)
   
   sig_def_len = 0
   if signature.defaults != None:
      sig_def_len = len(argspec.defaults)
      
   if len(argspec.args) == len(signature.args) and def_len == sig_def_len and \
      ((argspec.keywords == None and signature.keywords == None) or (argspec.keywords != None and signature.keywords != None)) and \
      ((argspec.varargs == None and signature.varargs == None) or (argspec.varargs != None and signature.varargs != None)):
      
      return True
   
   else:
      return False


#-------------------------
def validate_driver( driver_data ):
   '''
      Verify that a driver (encoded as a dictionary) is valid.
      Raise an exception if not.
   '''
   
   global DRIVER_READ_SIGNATURE, DRIVER_WRITE_SIGNATURE, DRIVER_DELETE_SIGNATURE
   
   for func_name, func_sig in zip( ['read_file', 'write_file', 'delete_file'], [DRIVER_READ_SIGNATURE, DRIVER_WRITE_SIGNATURE, DRIVER_DELETE_SIGNATURE] ):
      func = driver_data[ func_name ]
      if not is_valid_function( func, func_sig ):
         raise Exception( "Driver function '%s' does not match required signature" % (func_name) )
      
   return True
   

#-------------------------
def validate_closure( closure_data ):
   '''
      Verify that a closure (encoded as a dictionary) is valid.
      Raise an exception if not.
   '''
   
   global CLOSURE_DELETE_SIGNATURE, CLOSURE_READ_SIGNATURE, CLOSURE_WRITE_SIGNATURE
   
   for func_name, func_sig in zip( ['replica_delete', 'replica_read', 'replica_write'], [CLOSURE_DELETE_SIGNATURE, CLOSURE_READ_SIGNATURE, CLOSURE_WRITE_SIGNATURE] ):
      func = closure_data[ func_name ]
      if not is_valid_function( func, func_sig ):
         raise Exception("Closure function '%s' does not match required signature (sig is %s, expected %s)" % (func_name, inspect.getargspec(func), func_sig) )
   
   return True


#-------------------------
def decrypt_secrets( sender_pubkey_pem, receiver_privkey_pem, encrypted_secrets_str ):
   '''
      Given a secrets dictionary, decrypt it (with this gateway's private key)
   '''
   
   rc, secrets_str = c_syndicate.decrypt_closure_secrets( sender_pubkey_pem, receiver_privkey_pem, encrypted_secrets_str )
   if rc != 0:
      raise Exception("decrypt_closure_secrets rc = %d", rc )
   
   # parse secrets
   try:
      secrets = pickle.loads(secrets_str)
   except Exception, e:
      log.exception( e )
      raise Exception("Failed to parse secrets")
   
   if secrets.has_key( SECRETS_PAD_KEY ):
      del secrets[SECRETS_PAD_KEY]
      
   return secrets

   
#-------------------------
def load_closure( python_text_b64, config_text_b64, encrypted_secrets_b64, sender_pubkey_pem=None, gateway_privkey_pem=None ):
   '''
      Given a string containing Python statements defining the closure,
      load it in.
   '''
   
   global REQUIRED_CLOSURE_FIELDS
   global ALL_CLOSURE_FIELDS
   global GATEWAY_PRIVKEY_PEM
   global SENDER_PUBKEY_PEM
   
   if sender_pubkey_pem is None:
      sender_pubkey_pem = SENDER_PUBKEY_PEM
      
   if gateway_privkey_pem is None:
      gateway_privkey_pem = GATEWAY_PRIVKEY_PEM
   
   if encrypted_secrets_b64 != None and (gateway_privkey_pem is None or sender_pubkey_pem is None):
      log.error("No private key set")
      return None
   
   try:
      python_text = base64.b64decode( python_text_b64 )
   except Exception, e:
      log.exception( e )
      return None
   
   closure_module = imp.new_module("closure")
   
   try:
      exec( python_text, closure_module.__dict__ )
   except Exception, e:
      log.exception( e )
      return None
   
   # sanity check 
   rg_common.validate_fields( closure_module.__dict__, REQUIRED_CLOSURE_FIELDS )
   validate_closure( closure_module.__dict__ )
   
   config_dict = {}
   if config_text_b64 != None:
      # unmarshall the config text
      try:
         config_dict_str = base64.b64decode( config_text_b64 )
      except Exception, e:
         log.exception( e )
         return None
      
      try:
         config_dict = pickle.loads( config_dict_str )
      except Exception, e:
         log.exception( e )
         return None
   
   secrets_dict = {}
   
   if encrypted_secrets_b64 != None:
      # unmarshall the secrets
      try:
         encrypted_secrets = base64.b64decode( encrypted_secrets_b64 )
      except Exception, e:
         log.exception( e )
         return None 
   
      try:
         secrets_dict = decrypt_secrets( sender_pubkey_pem, gateway_privkey_pem, encrypted_secrets )
      except Exception, e:
         log.exception( e )
         return None
      
   
   closure_module.CONFIG = config_dict 
   closure_module.SECRETS = secrets_dict 
   closure_module.LOGGING = log
   cls = StorageClosure( module=closure_module, replica_read=closure_module.replica_read, replica_write=closure_module.replica_write, replica_delete=closure_module.replica_delete )
   
   return cls
   
   
#-------------------------
def load_storage_driver( sd_name, sd_code_b64, force_reload=False, cache=None ):
   '''
      Load a storage driver module.
   '''
   
   global REQUIRED_DRIVER_FIELDS
   
   if not force_reload and cache != None and sd_name in cache.keys():
      return cache[sd_name]
   
   if sd_code_b64 == None:
      raise Exception("No code loaded for driver '%s'" % sd_name )
   
   try:
      sd_code = base64.b64decode( sd_code_b64 )
   except Exception, e:
      log.exception( e )
      return None
   
   sd_module = imp.new_module(sd_name)
   
   try:
      exec( sd_code, sd_module.__dict__ )
   except:
      return None
   
   # sanity check the driver
   rg_common.validate_fields( sd_module.__dict__, REQUIRED_DRIVER_FIELDS )
   validate_driver( sd_module.__dict__ )
   
   sd = StorageDriver( name=sd_name, module=sd_module, read_file=sd_module.read_file, write_file=sd_module.write_file, delete_file=sd_module.delete_file )
   
   return sd


#-------------------------
def load_closure_json( json_str ):
   '''
      Given a JSON string, load and validate its closureuration data (as a dict).
      Return a StorageConfig with the closure and drivers.
   '''
   
   global REQUIRED_JSON_FIELDS
   global REQUIRED_JSON_DRIVER_FIELDS
   
   closure_dict = None 
   rg_closure = {}
   
   try:
      closure_dict = json.loads( json_str )
   except Exception, e:
      raise Exception("Invalid json string '%s'" % json_str)
      return None
   
   # sanity check the closure dict
   try:
      rg_common.validate_fields( closure_dict, REQUIRED_JSON_FIELDS )
   except Exception, e:
      log.exception( e )
      return None
   
   # load the drivers
   driver_list = closure_dict['drivers']
   
   drivers = {}
   
   for driver in driver_list:
      try:
         rg_common.validate_fields( driver, REQUIRED_JSON_DRIVER_FIELDS, OPTIONAL_JSON_DRIVER_FIELDS )
      except Exception, e:
         log.exception( e )
         continue
      
      sd_name = driver['name']
      sd_code = None
      force_reload = False
      
      if driver.has_key('code'):
         sd_code = driver['code']
         force_reload = True
      
      try:
         sd = load_storage_driver( sd_name, sd_code, force_reload )
         
         drivers[sd_name] = sd
         
      except Exception, e:
         log.exception( e )
         continue
      
   
   # load the closure
   try:
      cls = load_closure( closure_dict['closure'], closure_dict['config'], closure_dict['secrets'] )
      if cls == None:
         raise Exception("Failed to load closure")
   except Exception, e:
      log.exception( e )
      return None
   
   return StorageConfig(closure=cls, drivers=drivers)
   

#-------------------------
def load_local_drivers( storage_driver_module_names ):
   '''
      Cache locally-hosted storage drivers
   '''
   
   global REQUIRED_DRIVER_FIELDS
   
   drivers = {}
   
   for sd_name in storage_driver_module_names:
      sd_module = None
      
      try:
         sd_module = __import__(sd_name)
      except Exception, e:
         log.exception( e )
         continue
      
      # validate it
      sd_module_dict = dict( [ (attr_name, getattr(sd_module, attr_name)) for attr in dir(sd_module) ] )
      
      try:
         rg_common.validate_fields( sd_module_dict, REQUIRED_DRIVER_FIELDS )
         validate_driver( sd_module_dict )
      except Excetion, e:
         log.exception( e )
         continue
      
      drivers[ sd_name ] = sd_module
   
   return drivers


#-------------------------
def make_closure_json( closure_python_text, config_python_text, encrypted_secrets_str, driver_python_texts ):
   '''
      Given the contents of a closure python file, zero or more driver files, and encrypted secrets,
      generate a JSON string that can be loaded by load_closure_json.
      
      driver_python_texts must be in the form of [(name, python_text)]
   '''
   
   closure_dict = {}
   
   for (txt, pos_id) in zip( [closure_python_text, config_python_text, encrypted_secrets_str], ["First", "Second", "Third"] ):
      assert isinstance(txt, str) or isinstance(txt, unicode), "%s argument must be a str or unicode" % pos_id
      
   if not isinstance(driver_python_texts, list) and not isinstance(driver_python_texts, tuple):
      raise Exception("Fourth argument must be a list of (str or unicode, str or unicode)")
   
   if isinstance( driver_python_texts, tuple ):
      driver_python_texts = [driver_python_texts]
      
   for (code, name) in driver_python_texts:
      assert isinstance(name, str) or isinstance(name, unicode), "Fourth argument must be a list of (str, str)"
      assert isinstance(code, str) or isinstance(code, unicode), "Fourth argument must be a list of (str, str)"
   
   # load the closure 
   closure_dict['closure'] = base64.b64encode( closure_python_text )
   closure_dict['drivers'] = [ {'name': unicode(name), 'code': base64.b64encode(code)} for (name, code) in driver_python_texts ]
   closure_dict['secrets'] = base64.b64encode( encrypted_secrets_str )
   closure_dict['config'] = base64.b64encode( config_python_text )
   
   return json.dumps( closure_dict )
   

#-------------------------
def view_change_callback():
   '''
      Called when the Volume we're bound to changes state.
      Specifically, reload the storage closure
   '''
   
   global STORAGE_CONFIG
   global storage_closure_lock
   
   libsyndicate = rg_common.get_libsyndicate()
   
   try:
      new_storage_closure_text = libsyndicate.get_closure_text()
   except Exception, e:
      log.exception( e )
      return -errno.ENOTCONN
   
   if new_storage_closure_text == None:
      # nothing to do...
      log.info("No storage closure given")
      return
   
   try:
      new_storage_closure = load_closure_json( new_storage_closure_text )
   except Exception, e:
      log.exception( e )
      return -errno.EINVAL
   
   # reload local drivers, unless there are ones already supplied by the closure
   sd_path = libsyndicate.get_sd_path()
   
   if sd_path != None:
      # find all SDs
      sd_names = os.listdir( sd_path )
      
      # any SDs to load?
      if not "__init__.py" in sd_names:
         log.error("No __init__.py in storage driver directory '%s'" % sd_path )
      
      else:
         if sd_path not in sys.path:
            sys.path.append( sd_path )
         
         sd_module_names = map( lambda sd_name_py: sd_name_py.split(".")[0], filter( lambda sd_name: sd_name.startswith("sd_") and (sd_name.endswith(".py") or sd_name.endswith(".pyc")), sd_names ) )
         
         # only load drivers that were not supplied in the closure
         sd_modules_to_load = set(sd_module_names) - set(new_storage_closure.drivers.keys())
         
         local_drivers = load_local_drivers( sd_modules_to_load )
         
         storage_closure.drivers.update( local_drivers )
      
   
   # set the new closure
   storage_closure_lock.acquire()
   STORAGE_CONFIG = new_storage_closure
   storage_closure_lock.release()
   
   return 0

#-------------------------
def make_context_from_storage_struct( storage_config ):
   return StorageContext(config=storage_config.closure.module.CONFIG, secrets=storage_config.closure.module.SECRETS, drivers=storage_config.drivers, log=log)

#-------------------------
def make_context( config, secrets, drivers, log ):
   return StorageContext(config=config, secrets=secrets, drivers=drivers, log=log)

#-------------------------
def get_common_globals( storage_config ):
   '''
      Get common local and global variables from a given (locked) StorageConfig
   '''
   
   cls_globals = {
                  
                 }
   
   closure_globals = {}
   closure_globals.update( storage_config.closure.closure_globals )
   closure_globals.update( cls_globals )
   
   return closure_globals
                 

#-------------------------
def call_closure_read( request, filename, outfile ):
   '''
      Call the global storage closure's read_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_closure_lock
   
   storage_closure_lock.acquire()
   
   context = make_context_from_storage_struct( STORAGE_CONFIG )
   
   rc = 500
   
   try:
      rc = STORAGE_CONFIG.closure.replica_read( context, request, filename, outfile )
   except Exception, e:
      log.exception(e)
      traceback.print_exc()
   
   storage_closure_lock.release()
   
   return rc


#-------------------------
def call_closure_write( request, filename, infile ):
   '''
      Call the global storage closure's read_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_closure_lock
   
   storage_closure_lock.acquire()
   
   
   context = make_context_from_storage_struct( STORAGE_CONFIG )
   
   rc = 500
   
   try:
      rc = STORAGE_CONFIG.closure.replica_write( context, request, filename, infile )
   except Exception, e:
      log.exception(e)
      traceback.print_exc()
   
   storage_closure_lock.release()
   
   return rc


#-------------------------
def call_closure_delete( request, filename ):
   '''
      Call the global storage closure's delete_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_closure_lock
   
   storage_closure_lock.acquire()
   
   context = make_context_from_storage_struct( STORAGE_CONFIG )
   
   rc = 500
   
   try:
      rc = STORAGE_CONFIG.closure.replica_delete( context, request, filename )
   except Exception, e:
      log.exception(e)
      traceback.print_exc()
   
   storage_closure_lock.release()
   
   return rc


#-------------------------
def init( libsyndicate, gateway_key_path, sender_pubkey_path ):
   '''
      Initialize this module.
   '''
   
   global GATEWAY_PRIVKEY_PEM
   global SENDER_PUBKEY_PEM
   
   # disable core dumps (don't want our private key to get leaked)
   resource.setrlimit( resource.RLIMIT_CORE, (0, 0) )
   
   sender_pubkey_pem = None 
   
   if sender_pubkey_path is not None:
      try:
         # load keys
         fd = open( sender_pubkey_path, "r" )
         sender_pubkey_pem = fd.read()
         fd.close()
      except OSError, oe:
         log.exception(oe)
         log.error("Failed to read sender public key %s" % sender_pubkey_path)
         return -1
   
   gateway_privkey_pem = None 
   
   if gateway_key_path is not None:
      fd = open( gateway_key_path, "r" )
      gateway_privkey_pem = fd.read()
      fd.close()
      
      # verify
      try:
         gateway_privkey = CryptoKey.importKey( gateway_privkey_pem )
         assert gateway_privkey.has_private(), "Not a private key"
      except Exception, e:
         log.exception(e)
         log.error("Failed to load Gateway private key %s" % gateway_key_path )
         return -1
      
   else:
      # get the private key from libsyndicate
      rc, gateway_privkey_pem = libsyndicate.get_gateway_private_key_pem()
      if rc != 0:
         log.error("Failed to get gateway private key, rc = %s" % rc)
         return -1
      
      gateway_privkey = CryptoKey.importKey( gateway_privkey_pem )
      
   if sender_pubkey_pem is None:
      log.warning("Using Gateway public key to verify closures")
      sender_pubkey_pem = gateway_privkey.publickey().exportKey()

   SENDER_PUBKEY_PEM = sender_pubkey_pem 
   GATEWAY_PRIVKEY_PEM = gateway_privkey_pem
   
   # set up our storage
   view_change_callback()
   
   # register view change callback
   libsyndicate.set_view_change_callback( view_change_callback )
   
   return 0
   



closure_str = """
#!/usr/bin/env python 

def poop(a, b):
   return a + b

def replica_read( context, request_info, filename, outfile ):
   print "replica_read called!"
   
   CONFIG, SECRETS, DRIVERS = context.config, context.secrets, context.drivers 
   
   print "CONFIG = " + str(CONFIG)
   print "SECRETS = " + str(SECRETS)
   print "DRIVERS = " + str(DRIVERS)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "outfile = " + str(outfile)
   print ""
   
   rc = DRIVERS['sd_test'].read_file( filename, outfile, extra_param="Foo", **CONFIG )
   
   return rc
   
def replica_write( context, request_info, filename, infile ):
   print "replica_write called!"
   
   global_vars = globals().keys()
   global_vars.sort()
   print global_vars
   poop(1,2)
   
   CONFIG, SECRETS, DRIVERS = context.config, context.secrets, context.drivers 
   
   print "CONFIG = " + str(CONFIG)
   print "SECRETS = " + str(SECRETS)
   print "DRIVERS = " + str(DRIVERS)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "infile = " + str(infile)
   print ""
   
   rc = DRIVERS['sd_test'].write_file( filename, infile, extra_param="Foo", **CONFIG )
   
   return rc

def replica_delete( context, request_info, filename ):
   print "replica_delete called!"
   
   CONFIG, SECRETS, DRIVERS = context.config, context.secrets, context.drivers 
   
   print "CONFIG = " + str(CONFIG)
   print "SECRETS = " + str(SECRETS)
   print "DRIVERS = " + str(DRIVERS)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print ""
   
   rc = DRIVERS['sd_test'].delete_file( filename, extra_param="Foo", **CONFIG )
   
   return rc
"""

driver_str = """
#!/usr/bin/env python 

def read_file( filename, outfile, **kw ):
   import traceback

   print "  read_file called!"
   print "  filename = " + str(filename)
   print "  outfile = " + str(outfile)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "r" )
      outfile.write( fd.read() )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def write_file( filename, infile, **kw ):
   import traceback

   print "  write_file called!"
   print "  filename = " + str(filename)
   print "  infile = " + str(infile)
   print "  kw = " + str(kw)
   
   buf = infile.read()
   
   print "  Got data: '" + str(buf) + "'"
   
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "w" )
      fd.write( buf )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def delete_file( filename, **kw ):
   import traceback
   import os

   print "  delete_file called!"
   print "  filename = " + str(filename)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      os.unlink( STORAGE_DIR + filename )
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200
"""

secrets_str = pickle.dumps( {"xyzzy": "abbab"} )

config_str = pickle.dumps( {"foo": "bar", "STORAGE_DIR": "/tmp/"} )
      
if __name__ == "__main__":
   import syndicate.rg.request as rg_request
   
   test_request = rg_request.RequestInfo( type=rg_request.RequestInfo.BLOCK,
                                          volume_id=123,
                                          file_id=456,
                                          gateway_id=789,
                                          user_id=246,
                                          version=135,
                                          block_id=468,
                                          block_version=357,
                                          mtime_sec=2,
                                          mtime_nsec=3,
                                          data_hash="abcdef",
                                          size=4,
                                          kwargs={"asdf": "jkl;"} )
   

   # generate a public/private key pair
   from Crypto.Hash import SHA256 as HashAlg
   from Crypto.PublicKey import RSA as CryptoKey
   from Crypto import Random
   from Crypto.Signature import PKCS1_PSS as CryptoSigner
   
   def generate_key_pair( key_size ):
      rng = Random.new().read
      key = CryptoKey.generate(key_size, rng)

      private_key_pem = key.exportKey()
      public_key_pem = key.publickey().exportKey()

      return (public_key_pem, private_key_pem)
   
   print "generating fake user keys"
   user_pubkey_pem, user_privkey_pem = generate_key_pair( 4096 )
   
   print "generating fake gateway keys"
   gateway_pubkey_pem, gateway_privkey_pem = generate_key_pair( 4096 )
   
   GATEWAY_PRIVKEY_PEM = gateway_privkey_pem
   SENDER_PUBKEY_PEM = user_pubkey_pem 
   
   # encrypt the secrets
   rc, encrypted_secrets_str = c_syndicate.encrypt_closure_secrets( user_privkey_pem, gateway_pubkey_pem, secrets_str )
   
   if rc != 0:
      raise Exception("encrypt_closure_secrets rc = %d", rc )
   
   # make the json 
   json_str = make_closure_json( closure_str, config_str, encrypted_secrets_str, [("sd_test", driver_str)] )
   
   print "json = " + json_str
   
   # run tests
   STORAGE_CONFIG = load_closure_json( json_str )
   if STORAGE_CONFIG == None:
      sys.exit(1)
   
   infile_name = "/tmp/infile.test"
   outfile_name = "/tmp/outfile.test"
   
   f = open(infile_name, "w" )
   f.write("some fake data on disk")
   f.close()
   
   infile = open( infile_name, "r" )
   outfile = open( outfile_name, "w" )
   
   test_filename = "testfilename"
   
   
   write_rc = call_closure_write( test_request, test_filename, infile )
   read_rc = call_closure_read( test_request, test_filename, outfile )
   delete_rc = call_closure_delete( test_request, test_filename )
   
   print "read_rc = %s, write_rc = %s, delete_rc = %s" % (read_rc, write_rc, delete_rc)
   
      