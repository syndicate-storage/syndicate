#!/usr/bin/python


"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import os
import json

import sys
import tempfile
import base64
import stat

import syndicate.conf as conf 
import syndicate.common.jsonrpc as jsonrpc
import syndicate.log as Log
import syndicate.common.msconfig as msconfig

import traceback

from Crypto.PublicKey import RSA as CryptoKey

import pprint 

log = Log.log

# -------------------
def make_or_verify_key_directories( key_base_dir, object_cls ):

   for key_type in object_cls.key_types:
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
def load_key( key_path ):
   try:
      pkey_text_fd = open(key_path, "r")
      pkey_text = pkey_text_fd.read()
      pkey_text_fd.close()
   except Exception, e:
      log.error("Failed to read key '%s'" % key_path )
      return None

   try:
      pkey = CryptoKey.importKey( pkey_text )
   except Exception, e:
      log.error("Failed to load key %s'" % key_path )
      return None
   
   return pkey 

# -------------------
def get_object_keys( config, key_type, key_name ):
   
   pubkey_path = None 
   privkey_path = None
   
   pubkey_path = conf.object_verify_key_filename( config, key_type, key_name )
   privkey_path = conf.object_signing_key_filename( config, key_type, key_name )
   
   pubkey = load_key( pubkey_path )
   privkey = load_key( privkey_path )
   
   return (pubkey, privkey)

# -------------------   
def read_auth_key( file_name ):
   key = load_key( file_name )
   return key.publickey().exportKey()

# -------------------   
def read_file( file_name ):
   try:
      fd = open( file_name, "r" )
      buf = fd.read()
      fd.close()
      return buf
   except:
      return None

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
            raise Exception("SECURITY ERROR: tried to overwrite trusted key '%s' with new data!" % path )
         
         else:
            # key existed
            return
      else:
         raise e
   
   return



# -------------------
def has_signing_keys( config, key_type, key_name ):
   sign_path = conf.object_signing_key_filename( config, key_type, key_name )
   return os.path.exists( sign_path )

# -------------------
def has_verify_keys( config, key_type, key_name ):
   verify_path = conf.object_verify_key_filename( config, key_type, key_name )
   return os.path.exists( verify_path )

# -------------------   
def store_keys( config, key_type, key_name, verify_pem, sign_pem ):
   # validate
   try:
      if verify_pem:
         verify_key = CryptoKey.importKey( verify_pem )
      
      if sign_pem:
         sign_key = CryptoKey.importKey( sign_pem )
      
   except Exception, e:
      log.error("Invalid keys")
      raise e
   
   # store
   if verify_pem:
      verify_path = conf.object_verify_key_filename( config, key_type, key_name )
      log.info("Store verifying key for %s at %s" % (key_name, verify_path))
      write_key( verify_path, verify_pem )
   
   if sign_pem:
      sign_path = conf.object_signing_key_filename( config, key_type, key_name )
      log.info("Store signing key for %s at %s" % (key_name, sign_path))
      write_key( sign_path, sign_pem )
   
   return

# -------------------   
def revoke_keys( config, key_type, key_name, revoke_verifying_key=True, revoke_signing_key=True ):
   # revoke
   if revoke_verifying_key:
      try:
         verifying_key_path = conf.object_verify_key_filename( config, key_type, key_name )
         log.info("Revoke verifying key for %s" % key_name)
         os.unlink( verifying_key_path )
      except:
         pass 

   if revoke_signing_key:
      try:
         signing_key_path = conf.object_signing_key_filename( config, key_type, key_name )
         log.info("Revoke signing key for %s" % key_name)
         os.unlink( signing_key_path )
      except:
         pass
   
   return
