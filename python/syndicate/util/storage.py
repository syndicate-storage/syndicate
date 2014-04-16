#!/usr/bin/python

"""
   Copyright 2014 The Trustees of Princeton University

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

"""
Higher-level storage operations, 
particulary for keys.
"""

import os
import stat
import random
import traceback

from Crypto.PublicKey import RSA as CryptoKey

import logging

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()


# -------------------   
def read_file( file_path ):
   try:
      fd = open( file_path, "r" )
   except:
      return None
   
   buf = None
   try:
      buf = fd.read()
   except:
      fd.close()
      return None
   
   try:
      fd.close()
   except:
      return None
   
   return buf

# -------------------   
def write_file( file_path, data ):
   try:
      fd = open(file_path, "w" )
      fd.write( data )
      fd.close()
      return True
   except:
      try:
         os.unlink( file_path )
      except:
         pass
      return False

# -------------------
def read_key( key_path, public=False ):
   try:
      key_text = read_file( key_path )
   except Exception, e:
      log.error("Failed to read public key '%s'" % key_path )
      return None

   try:
      key = CryptoKey.importKey( key_text )
      
      if public:
         assert not key.has_private()
      else:
         assert key.has_private()
         
   except Exception, e:
      log.error("Failed to load public key %s'" % key_path )
      return None
   
   return key 


# -------------------   
def read_public_key( key_path ):
   return read_key( key_path, public=True )

def read_private_key( key_path ):
   return read_key( key_path, public=False )


# -------------------   
def write_key( path, key_data, overwrite=False ):
   umask_original = os.umask(0)
   try:
      handle = os.fdopen( os.open( path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0600 ), 'w' )
      handle.write( key_data )
      handle.close()
      
      os.umask( umask_original )
      
   except Exception, e:
      os.umask( umask_original )
      
      # does this path exist?
      if os.path.exists( path ):
         if overwrite:
            # overwrite!
            try:
               fd = open(path, "w" )
               fd.write( key_data )
               fd.close()
            except Exception, e:
               log.error("Failed to overwrite key at %s" % path )
               raise e

         else:
            raise Exception("SECURITY ERROR: tried to overwrite key '%s' with new data!" % path )
      else:
         raise e
   
   return


# -------------------   
def secure_erase_key( key_path ):
   try:
      size = os.stat( key_path ).st_size
   except OSError, oe:
      log.error("Failed to stat %s" % key_path)
      return False
   
   try:
      fd = open(key_path, "a")
      fd.seek(0)
      
      # erase with random data
      for i in xrange(0,10):
         rnd = os.urandom(size)
         fd.write(rnd)
         fd.flush()
         
         fd.seek(0)
      
      # and unlink
      fd.close()
      os.unlink( key_path )
   except OSError, oe:
      log.error("Failed to securely erase %s" % key_path)
      return False
   
   return True
   
