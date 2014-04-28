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

import syndicate.util.storage as util_storage 

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
   return util_storage.read_file( file_path )

# -------------------   
def write_file( file_path, data ):
   return util_storage.write_file( file_path, data )

# -------------------
def read_key( key_path, public=False ):
   return util_storage.read_key( key_path, public=public )

# -------------------   
def read_public_key( key_path ):
   return read_key( key_path, public=True )

# -------------------   
def read_private_key( key_path ):
   return read_key( key_path, public=False )

# -------------------   
def load_public_key( config, key_type, object_id ):
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return read_public_key( key_path )

# -------------------   
def load_private_key( config, key_type, object_id ):
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return read_private_key( key_path )

# -------------------   
def write_key( path, key_data, overwrite=False ):
   try:
      return util_storage.write_key( path, key_data, overwrite=overwrite )
   except:
      if not overwrite:
         
         tmp_keypath = os.tmpnam()
         
         print "\n\n!!! FAILED TO WRITE KEY TO %s !!!\n!!! Saving %s to %s instead !!!\n\n" % (path, path, tmp_keypath)
         
         return write_key( tmp_keypath, key_data )

# -------------------   
def store_public_key( config, key_type, object_id, key_data ):
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return write_key( key_path, key_data )
      

# -------------------   
def store_private_key( config, key_type, object_id, key_data ):
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return write_key( key_path, key_data )

# -------------------   
def erase_key( config, key_type, object_id, public=False ):
   key_path = conf.object_key_path( config, key_type, object_id, public=public )
   return util_storage.secure_erase_key( key_path )
   
# -------------------   
def erase_public_key( config, key_type, object_id ):
   return erase_key( config, key_type, object_id, public=True )

# -------------------   
def erase_private_key( config, key_type, object_id ):
   return erase_key( config, key_type, object_id, public=False )

