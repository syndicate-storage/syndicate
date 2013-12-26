#!/usr/bin/python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
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
def make_or_verify_internal_key_directories( key_base_dir, object_cls ):
   """
   Verify that a given object's internal key storage on disk is well-formed.
   Set up the storage if it does not exist.
   """
   for key_type in object_cls.internal_keys:
      key_path = os.path.join( key_base_dir, key_type )
   
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
def load_object_public_key( config, key_type, internal_type, object_id ):
   key_path = conf.object_key_path( config, key_type, internal_type, object_id, public=True )
   return read_public_key( key_path )

def load_object_private_key( config, key_type, internal_type, object_id ):
   key_path = conf.object_key_path( config, key_type, internal_type, object_id, public=False )
   return read_private_key( key_path )

# -------------------   
def write_key( path, key_data ):
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
         try:
            data_fd = open(path, "r")
            data = data_fd.read().strip()
            data_fd.close()
         except:
            raise Exception("Cannot write to '%s'" % path )
      
         if data != key_data:
            raise Exception("SECURITY ERROR: tried to overwrite key '%s' with new data!" % path )
         
         else:
            # key existed
            return
      else:
         raise e
   
   return


# -------------------   
def store_object_key( config, key_type, internal_type, object_id, key_data, public=False ):
   # validate
   try:
      key = CryptoKey.importKey( key_data )
      
      if public:
         assert not key.has_private()
      else:
         assert key.has_private()
   except Exception, e:
      log.error("Invalid key data")
      raise e 
   
   keytype_str = None
   if public:
      keytype_str = "public"
   else:
      keytype_str = "private"
      
   # store
   key_path = conf.object_key_path( config, key_type, internal_type, object_id, public=public )
   log.info("Store %s %s key for %s at %s" % (internal_type, keytype_str, object_id, key_path))
   write_key( key_path, key_data )
   
   return 

# -------------------   
def store_object_public_key( config, key_type, internal_type, object_id, key_data ):
   return store_object_key( config, key_type, internal_type, object_id, key_data, public=True )

def store_object_private_key( config, key_type, internal_type, object_id, key_data ):
   return store_object_key( config, key_type, internal_type, object_id, key_data, public=False )

# -------------------   
def revoke_object_key( config, key_type, internal_type, object_id, public=False ):
   key_path = conf.object_key_path( config, key_type, internal_type, object_id, public=public )
   
   # *erase* this key
   try:
      size = os.stat(key_path).st_size
      fd = open(key_path, "w")
      
      for i in xrange(0,10):
         fd.seek(0)
         
         # overwrite with junk
         buf = ''.join(chr(random.randint(0,255)) for i in xrange(0,size))
         fd.write( buf )
         fd.flush()

      # now safe to unlink
      os.unlink( key_path )
   except OSError, oe:
      if oe.errno != errno.ENOENT:
         raise oe
   
# -------------------   
def revoke_object_public_key( config, key_type, internal_type, object_id ):
   log.info("Revoke %s public key for %s" % (internal_type, object_id ))
   return revoke_object_key( config, key_type, internal_type, object_id, public=True )

def revoke_object_private_key( config, key_type, internal_type, object_id ):
   log.info("Revoke %s private key for %s" % (internal_type, object_id ))
   return revoke_object_key( config, key_type, internal_type, object_id, public=False )
