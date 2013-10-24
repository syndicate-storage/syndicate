#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import sys
import rm_common
import types
import inspect
import collections
import json
import base64
import threading
import errno

#-------------------------
StorageDriver = collections.namedtuple("StorageDriver", ["name", "read_file", "write_file", "delete_file"])
StorageClosure = collections.namedtuple("StorageClosure", ["CONFIG", "replica_read", "replica_write", "replica_delete"])
StorageConfig = collections.namedtuple("StorageConfig", ["closure", "drivers"] )

#-------------------------
STORAGE_CONFIG = {}
storage_config_lock = threading.Lock()

#-------------------------
REQUIRED_CLOSURE_FIELDS = {
   "CONFIG" : types.DictType,
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
   "drivers": types.ListType
}

REQUIRED_JSON_DRIVER_FIELDS = {
   "name": types.UnicodeType
}

OPTIONAL_JSON_DRIVER_FIELDS = {
   "code": types.UnicodeType
}

#-------------------------
CLOSURE_READ_SIGNATURE = inspect.ArgSpec( args=['drivers', 'request_info', 'filename', 'outfile'], varargs=None, defaults=None, keywords='kw' )
CLOSURE_WRITE_SIGNATURE = inspect.ArgSpec( args=['drivers', 'request_info', 'filename', 'infile'], varargs=None, defaults=None, keywords='kw' )
CLOSURE_DELETE_SIGNATURE = inspect.ArgSpec( args=['drivers', 'request_info', 'filename'], varargs=None, defaults=None, keywords='kw' )

DRIVER_READ_SIGNATURE = inspect.ArgSpec( args=['filename', 'outfile'], varargs=None, defaults=None, keywords='kw' )
DRIVER_WRITE_SIGNATURE = inspect.ArgSpec( args=['filename', 'infile'], varargs=None, defaults=None, keywords='kw' )
DRIVER_DELETE_SIGNATURE = inspect.ArgSpec( args=['filename'], varargs=None, defaults=None, keywords='kw' )

#-------------------------
def is_valid_function( func, signature ):
   '''
      Is a particular Python object a function with the correct signature?
   '''
   
   if not inspect.isfunction( func ):
      return False
   
   argspec = inspect.getargspec( func )
   
   if len(argspec.args) == len(signature.args) and argspec.varargs == signature.varargs and argspec.defaults == signature.defaults and argspec.keywords != None:
      return True
   else:
      return False


#-------------------------
def validate_driver( driver_data ):
   '''
      Verify that a driver (encoded as a dictionary) is valid.
      Raise an exception if not.
   '''
   
   global DRIVER_READ_SIGNATURE
   global DRIVER_WRITE_SIGNATURE
   global DRIVER_DELETE_SIGNATURE
   
   for func_name, func_sig in zip( ['read_file', 'write_file', 'delete_file'], [DRIVER_READ_SIGNATURE, DRIVER_WRITE_SIGNATURE, DRIVER_DELETE_SIGNATURE] ):
      func = driver_data[ func_name ]
      if not is_valid_function( func, func_sig ):
         raise Exception( "Driver function '%s' does not match required signature" % (func_name) )
      
   return True
   

#-------------------------
def load_closure( python_text_b64 ):
   '''
      Given a string containing Python statements defining the closure,
      load it in.
   '''
   
   global REQUIRED_CLOSURE_FIELDS
   global ALL_CLOSURE_FIELDS
   
   log = rm_common.log
   
   closure_globals = {}
   closure_locals = {}
   
   try:
      python_text = base64.b64decode( python_text_b64 )
   except Exception, e:
      log.exception( e )
      return None
   
   try:
      exec( python_text, closure_globals, closure_locals )
   except Exception, e:
      log.exception( e )
      return None
   
   closure_vars = closure_locals
   closure_vars.update( closure_globals )
   
   # sanity check 
   rm_common.validate_fields( closure_vars, REQUIRED_CLOSURE_FIELDS )
   
   cls = StorageClosure( CONFIG=closure_vars['CONFIG'], replica_read=closure_vars['replica_read'], replica_write=closure_vars['replica_write'], replica_delete=closure_vars['replica_delete'] )
   
   return cls
   
   
#-------------------------
def load_storage_driver( sd_name, sd_code_b64, force_reload=False, cache=None ):
   '''
      Load a storage driver module.
   '''
   
   global REQUIRED_DRIVER_FIELDS
   
   if not force_reload and sd_name in cache.keys():
      return cache[sd_name]
   
   if sd_code_b64 == None:
      raise Exception("No code loaded for driver '%s'" % sd_name )
   
   try:
      sd_code = base64.b64decode( sd_code_b64 )
   except Exception, e:
      log.exception( e )
      return None
   
   sd_locals = {}
   sd_globals = {}
   try:
      exec( sd_code, sd_globals, sd_locals )
   except:
      return None
   
   # sanity check the driver
   rm_common.validate_fields( sd_locals, REQUIRED_DRIVER_FIELDS )
   validate_driver( sd_locals )
   
   driver_read_func = sd_locals['read_file']
   driver_write_func = sd_locals['write_file']
   driver_delete_func = sd_locals['delete_file']
   
   sd = StorageDriver( name=sd_name, read_file=driver_read_func, write_file=driver_write_func, delete_file=driver_delete_func )
   
   return sd


#-------------------------
def load_config_json( json_str ):
   '''
      Given a JSON string, load and validate its configuration data (as a dict).
      Return a StorageConfig with the closure and drivers.
   '''
   
   global REQUIRED_JSON_FIELDS
   global REQUIRED_JSON_DRIVER_FIELDS
   
   log = rm_common.log
   
   config_dict = None 
   rg_config = {}
   
   try:
      config_dict = json.loads( json_str )
   except Exception, e:
      print e
      raise Exception("Invalid json string '%s'" % json_str)
      return None
   
   # sanity check the config dict
   try:
      rm_common.validate_fields( config_dict, REQUIRED_JSON_FIELDS )
   except Exception, e:
      log.exception( e )
      return None
   
   # load the drivers
   driver_list = config_dict['drivers']
   
   drivers = {}
   
   for driver in driver_list:
      try:
         rm_common.validate_fields( driver, REQUIRED_JSON_DRIVER_FIELDS, OPTIONAL_JSON_DRIVER_FIELDS )
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
      cls = load_closure( config_dict['closure'] )
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
   
   log = rm_common.log
   
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
         rm_common.validate_fields( sd_module_dict, REQUIRED_DRIVER_FIELDS )
         validate_driver( sd_module_dict )
      except Excetion, e:
         log.exception( e )
         continue
      
      drivers[ sd_name ] = sd_module
   
   return drivers


#-------------------------
def view_change_callback():
   '''
      Called when the Volume we're bound to changes state.
      Specifically, reload the storage config
   '''
   
   global STORAGE_CONFIG
   global storage_config_lock
   
   libsyndicate = rm_common.get_libsyndicate()
   log = rm_common.log
   
   try:
      new_storage_config_text = libsyndicate.get_closure_text()
   except Exception, e:
      log.exception( e )
      return -errno.ENOTCONN
   
   if new_storage_config_text == None:
      # nothing to do...
      log.info("No storage config given")
      return
   
   try:
      new_storage_config = load_config_json( new_storage_config_text )
   except Exception, e:
      log.exception( e )
      return -errno.EINVAL
   
   # reload local drivers, unless there are ones already supplied by the config
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
         sd_modules_to_load = set(sd_module_names) - set(new_storage_config.drivers.keys())
         
         local_drivers = load_local_drivers( sd_modules_to_load )
         
         storage_config.drivers.update( local_drivers )
      
   
   # set the new config
   storage_config_lock.acquire()
   STORAGE_CONFIG = new_storage_config
   storage_config_lock.release()
   
   return 0


#-------------------------
def call_config_read( request, filename, outfile ):
   '''
      Call the global storage config's read_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_config_lock
   
   log = rm_common.log
   
   storage_config_lock.acquire()
   
   cls_locals = { "request" : request,
                  "outfile" : outfile,
                  "filename" : filename,
                  "drivers" : STORAGE_CONFIG.drivers,
                  "replica_read" : STORAGE_CONFIG.closure.replica_read,
                }
   
   cls_globals = { 
                   "CONFIG" : STORAGE_CONFIG.closure.CONFIG,
                   "LOG" : log
                 }
   
   rc = eval( "replica_read( drivers, request, filename, outfile )", cls_globals, cls_locals )
   
   storage_config_lock.release()
   
   return rc


#-------------------------
def call_config_write( request, filename, infile ):
   '''
      Call the global storage config's read_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_config_lock
   
   log = rm_common.log
   
   storage_config_lock.acquire()
   
   cls_locals = { "request" : request,
                  "infile" : infile,
                  "filename" : filename,
                  "drivers" : STORAGE_CONFIG.drivers,
                  "replica_write" : STORAGE_CONFIG.closure.replica_write,
                }
   
   cls_globals = { 
                   "CONFIG" : STORAGE_CONFIG.closure.CONFIG,
                   "LOG" : log
                 }
   
   rc = eval( "replica_write( drivers, request, filename, infile )", cls_globals, cls_locals )
   
   storage_config_lock.release()
   
   return rc


#-------------------------
def call_config_delete( request, filename ):
   '''
      Call the global storage config's delete_replica() closure function.
   '''
   
   global STORAGE_CONFIG
   global storage_config_lock
   
   log = rm_common.log
   
   storage_config_lock.acquire()
   
   cls_locals = { "request" : request,
                  "filename" : filename,
                  "drivers" : STORAGE_CONFIG.drivers,
                  "replica_delete" : STORAGE_CONFIG.closure.replica_delete,
                }
   
   cls_globals = { 
                   "CONFIG" : STORAGE_CONFIG.closure.CONFIG,
                   "LOG" : log
                 }
   
   rc = eval( "replica_delete( drivers, request, filename )", cls_globals, cls_locals )
   
   storage_config_lock.release()
   
   return rc


#-------------------------
def init( libsyndicate ):
   '''
      Initialize this module.
   '''
   log = rm_common.log
      
   # set up our storage
   view_change_callback()
   
   # register view change callback
   libsyndicate.set_view_change_callback( view_change_callback )
   
   return 0
   

      
if __name__ == "__main__":
   import rm_request
   
   test_request = rm_request.RequestInfo( type=rm_request.RequestInfo.BLOCK, volume_id=123, file_id=456, gateway_id=789, user_id=246, version=135, block_id=468, block_version=357, mtime_sec=2, mtime_nsec=3, data_hash="abcdef", size=4 )
   
   closure_str = """
#!/usr/bin/env python 

CONFIG = {'foo': 'bar'}

def replica_read( drivers, request_info, filename, outfile ):
   print "replica_read called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "outfile = " + str(outfile)
   print ""
   
   drivers['sd_test'].read_file( filename, outfile, extra_param="Foo" )
   
   return 200
   
def replica_write( drivers, request_info, filename, infile ):
   print "replica_write called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "infile = " + str(infile)
   print ""
   
   drivers['sd_test'].write_file( filename, infile, extra_param="Foo" )
   
   return 200

def replica_delete( drivers, request_info, filename ):
   print "replica_delete called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print ""
   
   drivers['sd_test'].delete_file( filename, extra_param="Foo" )
   
   return 200
"""

   driver_str = """
#!/usr/bin/env python 

def read_file( filename, outfile, **kw ):
   print "  read_file called!"
   print "  filename = " + str(filename)
   print "  outfile = " + str(outfile)
   print "  kw = " + str(kw)
   print ""
   
   outfile.write("This is some fake data from read_file")
   
   return 0

def write_file( filename, infile, **kw ):
   print "  write_file called!"
   print "  filename = " + str(filename)
   print "  infile = " + str(infile)
   print "  kw = " + str(kw)
   
   buf = infile.read()
   
   print "  Got data: '" + str(buf) + "'"
   
   print ""
   
   return 0

def delete_file( filename, **kw ):
   print "  delete_file called!"
   print "  filename = " + str(filename)
   print "  kw = " + str(kw)
   print ""
   
   return 0
"""

   json_str = '{ "closure" : "%s", "drivers" : [ { "name" : "sd_test", "code" : "%s" } ] }' % (base64.b64encode( closure_str ), base64.b64encode( driver_str ) )
   
   print "json = " + json_str
   
   STORAGE_CONFIG = load_config_json( json_str )
   
   infile_name = "/tmp/infile.test"
   outfile_name = "/tmp/outfile.test"
   
   f = open(infile_name, "w" )
   f.write("some fake data on disk")
   f.close()
   
   infile = open( infile_name, "r" )
   outfile = open( outfile_name, "w" )
   
   test_filename = "/tmp/testfilename"
   
   read_rc = call_config_read( test_request, test_filename, outfile )
   write_rc = call_config_write( test_request, test_filename, infile )
   delete_rc = call_config_delete( test_request, test_filename )
   
   print "read_rc = %s, write_rc = %s, delete_rc = %s" % (read_rc, write_rc, delete_rc)
   
      