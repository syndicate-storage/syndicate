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
import syndicate.util.config as conf

log = conf.log


# -------------------   
def read_file( file_path ):
   """
   Read data from file_path
   Return non-None on success
   Return None on error.
   """
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
   """
   Write data to file_path
   Return True on success
   Return False on failure
   """
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
   """
   Read a key from key_path.
   Verify that it is the kind of key Syndicate supports (i.e. RSA 4096-bit)
   Verify that it is a private key, or if public is True, a public key.
   Return the PEM-encoded key on success.
   Return None on error
   """
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
   """
   Read a PEM-encoded public key from key_path.
   Return the key on success 
   Return None on error 
   """
   return read_key( key_path, public=True )


def read_private_key( key_path ):
   """
   Read a PEM-encoded private key from key_path.
   Return the key on success
   Return None on error 
   """
   return read_key( key_path, public=False )


# -------------------   
def write_key( path, key_data, overwrite=False ):
   """
   Write a key's data to path.
   If overwrite is True, then overwrite it if it exists (otherwise bail).
   Returns True on success 
   raises an exception on error
   """

   try:
       with os.fdopen( os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0600), 'w' ) as f:
           f.write( key_data )
           f.flush()

   except (OSError, IOError), e:
       log.error("Failed to write '%s': %s" % (path, e.strerror))
       if os.path.exists( path ):
           if overwrite:
               try:
                   with open(path, "w") as f:
                       f.write( key_data )
                       f.flush()
               except (OSError, IOError), e:
                    log.error("Failed to write '%s': %s" % (path, e.strerror))
                    raise

           else:
               raise Exception("SECURITY ERROR: tried to overwrite key '%s' with new data!" % path)
       else:
           raise e

   return True


# -------------------   
def secure_erase_key( key_path ):
   """
   'Securely' erase a key at key_path.
   Fill it with random data first, flush the data,
   and then unlink it.
   Return True on success
   Return False on error.
   """
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
   
# -------------------
def make_or_check_object_directory( key_path ):
   """
   Verify that a given object's directory on disk is well-formed.
   Create it and chown it accordingly if it does not exist.
   Print a warning if it does exist, but has potentially insecure settings.
   Return True on success 
   Return False on error
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
         log.warning("Directory %s is not privately readable.  Recommend 'chmod 0600 %s'" % (key_path, key_path))
      
      if (mode & (stat.S_IWGRP | stat.S_IWOTH)) != 0:
         log.warning("Directory %s is not privately writable.  Recommend 'chmod 0600 %s'" % (key_path, key_path))
      
   except Exception, e:
      log.error("Failed to stat directory %s" % key_path)
      log.exception( e )
      return False

   return True


def load_public_key( config, key_type, object_id ):
   """
   Load a public key of the given type for the given object.
   """
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return read_public_key( key_path )


def load_private_key( config, key_type, object_id ):
   """
   Load a private key of the given type for the given object.
   """
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return read_private_key( key_path )


def store_public_key( config, key_type, object_id, key_data ):
   """
   Store a public key for a given object of a given type.
   """
   key_data_pem = key_data.exportKey()
   key_path = conf.object_key_path( config, key_type, object_id, public=True )
   return write_key( key_path, key_data_pem )
      

def store_private_key( config, key_type, object_id, key_data, overwrite=False ):
   """
   Store a private key for a given object of a given type.
   """
   key_data_pem = key_data.exportKey()
   assert key_data.has_private(), "Not a private key"
   key_path = conf.object_key_path( config, key_type, object_id, public=False )
   return write_key( key_path, key_data_pem, overwrite=overwrite )


def erase_key( config, key_type, object_id, public=False ):
   """
   Erase a key for a given object of a given type.
   """
   key_path = conf.object_key_path( config, key_type, object_id, public=public )
   return secure_erase_key( key_path )
   
   
def erase_public_key( config, key_type, object_id ):
   """
   Erase a public key for a given object of a given type.
   """
   return erase_key( config, key_type, object_id, public=True )


def erase_private_key( config, key_type, object_id ):
   """
   Erase a private for a given object of a given type.
   """
   return erase_key( config, key_type, object_id, public=False )

