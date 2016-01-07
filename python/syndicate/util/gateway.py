#!/usr/bin/python

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

import os
import sys
import errno 
import cStringIO
import traceback
import signal
import json
import threading
import cPickle as pickle
import imp
from syndicate.protobufs.sg_pb2 import DriverRequest, Manifest

driver_shutdown = None 

def do_driver_shutdown():
    """
    gracefully shut down
    """
    global driver_shutdown

    print >> sys.stderr, "Worker %s exiting" % os.getpid()

    if driver_shutdown is not None:
        rc = driver_shutdown()
        if type(rc) in [int, long]:
            sys.exit(rc)
        else:
            sys.exit(0)

    else:
        sys.exit(0)


def read_string( f ):
   """
   Read a null-terminated string from file f. 
   """
   s = cStringIO.StringIO()
   while True:
      
      c = f.read(1)
      if c == '\0':
         break 
      
      s.write(c)
   
   return s.getvalue()


def read_int( f ):
   """
   Read an integer from file f, as a newline-terminated string
   Return the int on success 
   Return None on error
   """
   
   # read the int 
   i = f.readline( 100 )
   if len(i) == 0:
       # gateway exit 
       do_driver_shutdown()

   if i[-1] != '\n':
      
      # invalid line 
      print >> sys.stderr, "Integer too long: '%s'" % i 
      return None
  
   try:
      i = int(i.strip())
   except Exception, e:
      print >> sys.stderr, "Invalid integer: '%s'" % i
      return None 
   
   return i


def read_data( f, size ):
   """
   Read a newline-terminated data stream from f, up to size.
   Return the chunk on success
   Return None on error
   """
   
   chunk = f.read( size+1 )
   if len(chunk) == 0:
       # gateway exit
       do_driver_shutdown()
   
   if len(chunk) != size+1:
      
      # invalid chunk 
      print >> sys.stderr, "Data too short"
      return None 
   
   if chunk[-1] != '\n':
      
      # invalid chunk 
      print >> sys.stderr, "Data too long"
      return None 
   
   chunk = chunk[:len(chunk)-1]
   return chunk


def read_chunk( f ):
   """
   Get a chunk of data from a file descriptor.
   A chunk is encoded by its length, a newline, and data.
   """

   chunk_len = read_int( f )
   if chunk_len is None:
       do_driver_shutdown()

   chunk = read_data( f, chunk_len )
   return chunk


def read_request( f ):
   """
   Read a chunk from file f that 
   contains a DriverRequest string.
   Return the deserialized DriverRequest on success
   Shut down the driver on error.
   """
   
   req_chunk = read_chunk( f )
   try:
       driver_req = DriverRequest()
       driver_req.ParseFromString( req_chunk )
   except:
       print >> sys.stderr, "Failed to parse driver request"
       do_driver_shutdown()

   return driver_req


def request_to_storage_path( request ):
   """
   Create a storage path for a request.
   It will be prefixed by UID, then volume ID, then 
   inode, then path, version, and either block ID or version
   or manifest timestamp (depending on what kind of 
   request this is).

   Return the string on success
   """

   prefix = "%s/%s/%X/%s" % (request.user_id, request.volume_id, request.file_id, request.path)

   if request.request_type == DriverRequest.BLOCK:
       prefix = os.path.join( prefix, "%s/%s" % (request.block_id, request.block_version) )

   elif request.request_type == DriverRequest.MANIFEST:
       prefix = os.path.join( prefix, "manifest/%s.%s" % (request.manifest_mtime_sec, request.manifest_mtime_nsec))

   else:
       print >> sys.stderr, "Invalid driver request type '%s'" % request.request_type
       do_driver_shutdown()

   return prefix


def write_int( f, i ):
   """
   Send back an integer to the main Syndicate daemon.
   """
   f.write( "%s\n" % i )


def write_data( f, data ):
   """
   Send back a string of data to the main Syndicate daemon.
   """
   f.write( "%s\n" % data )


def write_chunk( f, data ):
   """
   Send back a length, followed by a newline, followed by a string
   of data to Syndicate.
   """
   f.write( "%s\n%s\n" % (len(data), data))


driver_shutdown = None
driver_shutdown_lock = threading.Semaphore(1)

# die on a signal
def sig_die( signum, frame ):

   global driver_shutdown, driver_shutdown_lock

   if driver_shutdown is not None:
       driver_shutdown_lock.acquire()   # leave this locked to prevent subsequent calls
       driver_shutdown()

   sys.exit(0)
   

# is an object callable?
def is_callable( obj ):
    return hasattr( obj, "__call__" )


# does a module have a method?
def has_method( mod, method_name ):
    return hasattr( mod, method_name ) and is_callable( getattr(mod, method_name, None) )


def driver_setup( operation_modes, expected_callback_names, default_callbacks={} ):
   """
   Set up a gateway driver:
   * verify that the operation mode is supported (i.e. the operation 
   mode is present in operation_modes)
   * install signal handlers for shutting down the driver, and calling 
   the driver_shutdown() method.
   * load configuration, secrets, and code.
   * validate configuration, secrets, and code (i.e. verify that 
   the config and secrets are well-formed, and that the code defines
   a callback for each method names in expected_callback_names).
   * run the driver_init() method.

   Return (operation mode, driver module) on success
   Signal the parent process and exit with a non-zero exit code on failure:
   * return 1 and exit 1 on misconfiguration 
   * return 1 and exit 2 on failure to initialize
   * return 2 and exit 0 if the driver does not implement the requested operation mode
   """

   # die on SIGINT 
   signal.signal( signal.SIGINT, sig_die )
   
   # usage: $0 operation_mode
   if len(sys.argv) != 2:
      
      print >> sys.stderr, "Usage: %s operation_mode" % sys.argv[0]
      
      # tell the parent that we failed
      print "1"
      sys.exit(1)
   
   usage = sys.argv[1]
   
   if usage not in operation_modes:
      
      print >> sys.stderr, "Usage: %s operation_mode" % sys.argv[0]
      
      # tell the parent that we failed
      print "1"
      sys.exit(1)
 
   method_name_idx = operation_modes.index(usage)
   method_name = expected_callback_names[method_name_idx]
       
   # on stdin: config and secrets (as two separate null-terminated strings)
   config_len = read_int( sys.stdin )
   if config_len is None:
      print "1"
      sys.exit(1)
      
   config_str = read_data( sys.stdin, config_len )
   if config_str is None:
      print "1"
      sys.exit(1)
   
   secrets_len = read_int( sys.stdin )
   if secrets_len is None:
      print "1"
      sys.exit(1)
      
   secrets_str = read_data( sys.stdin, secrets_len )
   if secrets_str is None:
      print "1"
      sys.exit(1)
   
   driver_len = read_int( sys.stdin )
   if driver_len is None:
      print "1"
      sys.exit(1)
   
   driver_str = read_data( sys.stdin, driver_len )
   if driver_str is None:
      print "1"
      sys.exit(1)
      
   CONFIG = {}
   SECRETS = {}

   # config_str should be a JSON dict 
   try:
       CONFIG = json.loads(config_str)
   except Exception, e:
      
      print >> sys.stderr, "Failed to load config"
      print >> sys.stderr, "'%s'" % config_str
      print >> sys.stderr, traceback.format_exc()
      
      # tell the parent that we failed
      print "1"
      sys.exit(2)
   
   # secrets_str should be a JSON dict
   try:
      SECRETS = json.loads(secrets_str)
   except Exception, e:
      
      print >> sys.stderr, "Failed to load secrets"
      print >> sys.stderr, traceback.format_exc()
      
      # tell the parent that we failed 
      print "1"
      sys.exit(2)

   # driver should be a set of methods 
   driver_mod = imp.new_module("driver_mod")
   try:
      exec driver_str in driver_mod.__dict__
   except Exception, e:
            
      print >> sys.stderr, "Failed to load driver"
      print >> sys.stderr, traceback.format_exc()
      
      # tell the parent that we failed 
      print "1"
      sys.exit(2)
     
   # verify that the driver method is defined 
   fail = False
   if not has_method( driver_mod, method_name ):
       if not default_callbacks.has_key( method_name ):
           fail = True 
           print >> sys.stderr, "No method '%s' defined" % method_name

       elif default_callbacks[method_name] is None:
           # no method implementation; fall back to the gateway
           print >> sys.stderr, "No implementation for '%s'" % method_name
           print "2"
           sys.exit(0)

       else:
           print >> sys.stderr, "Using default implementation for '%s'" % method_name
           setattr( driver_mod, usage, default_callbacks[method_name] )


   # remember generic shutdown so the signal handler can use it
   if has_method( driver_mod, "driver_shutdown" ):
       global driver_shutdown
       driver_shutdown = driver_mod.driver_shutdown 

   # do our one-time init, if given 
   if not fail and has_method( driver_mod, "driver_init" ):

       try:
           fail = driver_mod.driver_init( CONFIG, SECRETS )
       except:
           print >> sys.stderr, "driver_init raised an exception"
           print >> sys.stderr, traceback.format_exc()
           fail = True 

       if fail not in [True, False]:

           # indicates a bug 
           fail = True

   if fail:
      print "1"
      sys.stdout.flush()
      sys.exit(2)
 
   driver_mod.CONFIG = CONFIG 
   driver_mod.SECRETS = SECRETS
   return (usage, driver_mod)
