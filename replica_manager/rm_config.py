#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import rm_common
import types
import inspect
import collections
import json
import base64

#-------------------------
StorageDriver = collections.namedtuple("StorageDriver", ["name", "read_file", "write_file"])
StorageClosure = collections.namedtuple("StorageClosure", ["CONFIG", "replica_read", "replica_write"])
StorageConfig = collections.namedtuple("StorageConfig", ["closure", "drivers"] )

#-------------------------
CACHED_STORAGE_DRIVERS = {}

#-------------------------
REQUIRED_CLOSURE_FIELDS = {
   "CONFIG" : types.DictType,
   "replica_read": types.FunctionType,
   "replica_write": types.FunctionType
}

#-------------------------
REQUIRED_DRIVER_FIELDS = {
   "read_file": types.FunctionType,
   "write_file": types.FunctionType
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

DRIVER_READ_SIGNATURE = inspect.ArgSpec( args=['filename', 'outfile'], varargs=None, defaults=None, keywords='kw' )
DRIVER_WRITE_SIGNATURE = inspect.ArgSpec( args=['filename', 'infile'], varargs=None, defaults=None, keywords='kw' )

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
def validate_fields( data, required_fields, optional_fields=None ):
   missing = []
   invalid = []
   for req_field in required_fields.keys():
      if not data.has_key( req_field ):
         missing.append( req_field )
      elif type(data[req_field]) != required_fields[req_field]:
         print "invalid type for %s: got %s, expected %s" % (req_field, type(data[req_field]), required_fields[req_field])
         invalid.append( req_field )
   
   if optional_fields != None:
      for field in data.keys():
         if field not in required_fields.keys() and field not in optional_fields.keys():
            invalid.append( field )
            
   if len(missing) != 0 or len(invalid) != 0:
      missing_txt = ",".join( missing )
      if len(missing_txt) == 0:
         missing_txt = "None"
         
      invalid_txt = ",".join( invalid )
      if len(invalid_txt) == 0:
         invalid_txt = "None"
         
      raise Exception("Missing fields: %s; Invalid fields: %s" % (missing_txt, invalid_txt) )
   
   return


#-------------------------
def load_closure( python_text_b64 ):
   '''
      Given a string containing Python statements defining the closure,
      load it in.
   '''
   
   global REQUIRED_CLOSURE_FIELDS
   global ALL_CLOSURE_FIELDS
   
   log = rm_common.get_logger()
   
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
   validate_fields( closure_vars, REQUIRED_CLOSURE_FIELDS )
   
   cls = StorageClosure( CONFIG=closure_vars['CONFIG'], replica_read=closure_vars['replica_read'], replica_write=closure_vars['replica_write'] )
   
   return cls
   
   
#-------------------------
def load_storage_driver( sd_name, sd_code_b64, force_reload=False ):
   '''
      Load a storage driver module.
   '''
   
   global CACHED_STORAGE_DRIVERS
   global REQUIRED_DRIVER_FIELDS
   
   if not force_reload and sd_name in CACHED_STORAGE_DRIVERS.keys():
      return CACHED_STORAGE_DRIVERS['sd_name']
   
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
   validate_fields( sd_locals, REQUIRED_DRIVER_FIELDS )
   
   driver_read_func = sd_locals['read_file']
   driver_write_func = sd_locals['write_file']
   
   # check functions
   if not is_valid_function( driver_read_func, DRIVER_READ_SIGNATURE ):
      raise Exception("Driver read function does not match required signature")
   
   if not is_valid_function( driver_write_func, DRIVER_WRITE_SIGNATURE ):
      raise Exception("Driver write function does not match required signature")
   
   sd = StorageDriver( name=sd_name, read_file=driver_read_func, write_file=driver_write_func )
   
   return sd


#-------------------------
def load_config_json( json_str ):
   '''
      Given a JSON string, load and validate its configuration data (as a dict).
      Return a StorageConfig with the closure and drivers.
   '''
   
   global REQUIRED_JSON_FIELDS
   global REQUIRED_JSON_DRIVER_FIELDS
   
   log = rm_common.get_logger()
   
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
      validate_fields( config_dict, REQUIRED_JSON_FIELDS )
   except Exception, e:
      log.exception( e )
      return None
   
   # load the drivers
   driver_list = config_dict['drivers']
   
   drivers = {}
   
   for driver in driver_list:
      try:
         validate_fields( driver, REQUIRED_JSON_DRIVER_FIELDS, OPTIONAL_JSON_DRIVER_FIELDS )
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
def cache_local_drivers( storage_driver_module_names ):
   '''
      Cache locally-hosted storage drivers
   '''
   
   global CACHED_STORAGE_DRIVERS
   
   log = rm_common.get_logger()
   
   for sd_name in storage_driver_module_names:
      sd_module = None
      
      try:
         sd_module = __import__(sd_name)
      except Exception, e:
         log.exception( e )
         continue
      
      
      
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
   
   return 0
   
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
   
   return 0
   
"""

   driver_str = """
#!/usr/bin/env python 

def read_file( filename, outfile, **kw ):
   print "  read_file called!"
   print "  filename = " + str(filename)
   print "  outfile = " + str(outfile)
   print "  kw = " + str(kw)
   print ""
   
   return 0

def write_file( filename, infile, **kw ):
   print "  write_file called!"
   print "  filename = " + str(filename)
   print "  infile = " + str(infile)
   print "  kw = " + str(kw)
   print ""
   
   return 0

   
"""
   json_str = '{ "closure" : "%s", "drivers" : [ { "name" : "sd_test", "code" : "%s" } ] }' % (base64.b64encode( closure_str ), base64.b64encode( driver_str ) )
   
   print "json = " + json_str
   
   storage_config = load_config_json( json_str )
   
   infile_name = "/tmp/infile.test"
   outfile_name = "/tmp/outfile.test"
   
   f = open(infile_name, "w" )
   f.write("some fake data on disk")
   f.close()
   
   infile = open( infile_name, "r" )
   outfile = open( outfile_name, "w" )
   
   test_filename = "/tmp/testfilename"
   
   cls_locals = { "request" : test_request,
                  "infile" : infile,
                  "outfile" : outfile,
                  "filename" : test_filename,
                  "drivers" : storage_config.drivers,
                  "replica_read" : storage_config.closure.replica_read,
                  "replica_write" : storage_config.closure.replica_write
                }
   
   cls_globals = { 
                   "CONFIG" : storage_config.closure.CONFIG
                 }
   
   read_rc = eval( "replica_read( drivers, request, filename, outfile )", cls_globals, cls_locals )
   
   write_rc = eval( "replica_write( drivers, request, filename, infile )", cls_locals, cls_locals )
   
   print "read_rc = %s, write_rc = %s" % (read_rc, write_rc)
   
      