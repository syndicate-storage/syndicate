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


def read_path( f ):
   """
   Read a newline-deliminated string from file f, up to PATH_MAX
   Return the path on success
   Return None on error
   """
   
   # read path from stdin; output blocks to stdout 
   path = f.readline( 4096 )
   if path[-1] != '\n':
      
      # invalid line 
      print >> sys.stderr, "Path too long: '%s'" % path
      return None
   
   path = path.strip()
   return path


def read_data( f, size ):
   """
   Read a newline-terminated data stream from f, up to size.
   Return the chunk on success
   Return None on error
   """
   
   chunk = f.read( size+1 )
   
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
       return None 

   chunk = read_data( f, chunk_len )
   return chunk


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
   

def driver_setup( operation_modes, expected_callback_names ):
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

   Return a tuple with the following fields:
   * the operational mode
   * the configuration (as a dict)
   * the secrets (as a dict)
   * a dict mapping each name in expected_callback_names to the callback
   defined by the driver.

   Signal the parent process and exit with a non-zero exit code on failure.
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
   METHODS = {}

   # config_str should be a JSON dict 
   try:
       CONFIG = json.loads(config_str)
   except Exception, e:
      
      print >> sys.stderr, "Failed to load config"
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
   try:
      exec( driver_str )
   except Exception, e:
            
      print >> sys.stderr, "Failed to load driver"
      print >> sys.stderr, traceback.format_exc()
      
      # tell the parent that we failed 
      print "1"
      sys.exit(2)
      
   # verify that the driver methods are defined 
   fail = False
   for method_name in expected_callback_names + ["driver_init", "driver_shutdown"]:
       if method_name not in locals():
           fail = True
           print >> sys.stderr, "No '%s' method defined" % method_name
        
       method = locals()[method_name]
       if not hasattr(method, "__call__"):
           fail = True 
           print >> sys.stderr, "Object '%s' is not callable" % method_name 

       METHODS[ method_name ] = method

   # remember generic shutdown so the signal handler can use it
   if METHODS.has_key('driver_shutdown')
       global driver_shutdown
       driver_shutdown = METHODS['driver_shutdown']

   # do our one-time init, if given 
   if not fail and METHODS.has_key('driver_init'):

       try:
           fail = METHODS['driver_init']( CONFIG, SECRETS )
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
  
   return (usage, CONFIG, SECRETS, METHODS)
