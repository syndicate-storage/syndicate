#!/usr/bin/python

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
import errno
import sys
import tempfile
import base64
import stat
import random

import syndicate.client.conf as conf 
import syndicate.client.common.jsonrpc as jsonrpc
import syndicate.client.common.log as Log
import syndicate.client.common.msconfig as msconfig

import traceback

from Crypto.PublicKey import RSA as CryptoKey

import pprint 

log = Log.get_logger()

# -------------------
def make_or_check_key_directory( key_path ):
   """
   Verify that a given object's internal key storage on disk is well-formed.
   Set up the storage if it does not exist.
   """
   
   if not os.path.exists( key_path ):
      log.warn("Directory '%s' does not exist" % key_path )
      
      try:
         os.makedirs( key_path, mode=0700 )
         log.info("Created directory %s" % key_path)
      except Exception, e:
         log.error("Failed to create directory %s" % key_path)
         log.exception( e )
         
         return False
   
   if not os.path.isdir( key_path ):
      log.error("File '%s' is not a directory" % key_path )
      return False
      
   try:
      mode = os.stat( key_path ).st_mode
      if (mode & (stat.S_IRGRP | stat.S_IROTH | stat.S_IXGRP | stat.S_IXOTH)) != 0:
         log.warning("Key path %s is not privately readable.  Recommend 'chmod 0600 %s'" % (key_path, key_path))
      
      if (mode & (stat.S_IWGRP | stat.S_IWOTH)) != 0:
         log.warning("Key path %s is not privately writable.  Recommend 'chmod 0600 %s'" % (key_path, key_path))
      
   except Exception, e:
      log.error("Failed to stat directory %s" % key_path)
      log.exception( e )
      return False

   return True


# -------------------   
def read_file( file_path ):
   try:
      fd = open( file_path, "r" )
      buf = fd.read()
      fd.close()
      return buf
   except:
      return None

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
def load_public_key( config, key_type, object_id ):
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return read_public_key( key_path )

def load_private_key( config, key_type, object_id ):
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return read_private_key( key_path )

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
def store_public_key( config, key_type, object_id, key_data ):
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return write_key( key_path, key_data )

def store_private_key( config, key_type, object_id, key_data ):
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return write_key( key_path, key_data )
