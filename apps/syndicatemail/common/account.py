#!/usr/bin/env python

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


import storage
import keys
import contact
import message
import singleton

import os

ALL_STORAGE_PACKAGES = [keys, contact, storage, message]

VOLUME_STORAGE_ROOT = "/apps/syndicatemail/data"
LOCAL_STORAGE_ROOT = os.path.expanduser("/tmp/syndicatemail")

import sys
import tempfile
import socket 
import collections
import uuid
import urllib

from Crypto.Hash import SHA256 as HashAlg
from Crypto.Hash import HMAC
from Crypto.PublicKey import RSA as CryptoKey
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto import Random

from urlparse import urlparse

import shutil
import syndicate.client.bin.syntool as syntool
from syndicate.client.common.object_stub import Gateway, SyndicateUser, Volume

from syndicate.volume import Volume as SyndicateVolume 

import syndicate.client.common.log as Log

log = Log.get_logger()

USER_STORAGE_DIR = "user_info"
GATEWAY_STORAGE_DIR = "gateway_info"
GATEWAY_RUNTIME_STORAGE = "gateway_runtime"
VOLUME_STORAGE_DIR = "volume_info"
LOCAL_STORAGE_DIR = os.path.expanduser("~/syndicatemail")
LOCAL_TMP_DIR = "/tmp"

DEFAULT_GATEWAY_PORT = 33334

# needed for setup
VOLUME_STORAGE_DIRS = [
   USER_STORAGE_DIR
]

LOCAL_STORAGE_DIRS = [
   GATEWAY_STORAGE_DIR,
   VOLUME_STORAGE_DIR,
   LOCAL_TMP_DIR,
   LOCAL_STORAGE_DIR
]

CREATED = 1
EXISTS = 2

# -------------------------------------
SyndicateAccountInfo = collections.namedtuple("SyndicateAccountInfo", [ "gateway_name", "gateway_port", "gateway_privkey_pem", "volume_pubkey_pem"] )

# -------------------------------------
def privkey_basename( name ):
   return name + ".pkey"

# -------------------------------------
def pubkey_basename( name ):
   return name + ".pub"

# -------------------------------------
def gateway_privkey_path( gateway_name ):
   global GATEWAY_STORAGE_DIR
   return storage.local_path( GATEWAY_STORAGE_DIR, privkey_basename( gateway_name ) )

# -------------------------------------
def volume_pubkey_path( volume_name ):
   global VOLUME_STORAGE_DIR
   return storage.local_path( VOLUME_STORAGE_DIR, pubkey_basename( volume_name ) )

# -------------------------------------
def read_gateway_privkey( password, gateway_name ):
   path = gateway_privkey_path( gateway_name )
   encrypted_privkey_json = storage.read_file( path, volume=None )

   try:
      encrypted_privkey = storage.json_to_tuple( keys.EncryptedPrivateKey, encrypted_privkey_json )
   except Exception, e:
      log.exception(e)
      log.error("Failed to unserialize encrypted private gateway key")
      return None
   
   privkey_str = keys.decrypt_private_key( encrypted_privkey, password )

   return privkey_str

# -------------------------------------
def write_gateway_privkey( password, gateway_name, gateway_privkey_str ):
   encrypted_data = keys.encrypt_private_key( gateway_privkey_str, password )
   try:
      encrypted_data_json = storage.tuple_to_json( encrypted_data )
   except Exception, e:
      log.exception(e)
      log.error("Failed to serialize encrypted private gateway key" )
      return None
   
   path = gateway_privkey_path( gateway_name )
   return storage.write_file( path, encrypted_data_json, volume=None )

# -------------------------------------
def delete_gateway_privkey( gateway_name ):
   return storage.erase_file( gateway_privkey_path( gateway_name ), volume=None )

# -------------------------------------
def read_volume_pubkey( volume_name, prefix=None ):
   # support prefix, since sometimes this can be called before storage is initialized
   if prefix is None:
      prefix = "/"
   
   pubkey_path = storage.path_join(prefix, volume_pubkey_path( volume_name ))
   return storage.read_file( pubkey_path, volume=None )

# -------------------------------------
def write_volume_pubkey( volume_name, metadata_pubkey ):
   return storage.write_file( volume_pubkey_path( volume_name ), metadata_pubkey, volume=None )

# -------------------------------------
def delete_volume_pubkey( volume_name ):
   return storage.delete_file( volume_pubkey_path( volume_name ), volume=None )

# -------------------------------------
def make_default_gateway_name():
   #return uuid.uuid4().gethex()
   return "syndicatemail-UG-%s" % socket.gethostname()

# -------------------------------------
def write_gateway_name( gateway_name ):
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_name" )
   return storage.write_file( name_path, gateway_name, volume=None )

# -------------------------------------
def read_gateway_name():
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_name" )
   return storage.read_file( name_path, volume=None )

# -------------------------------------
def delete_gateway_name():
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_name" )
   return storage.delete_file( name_path, volume=None )

# -------------------------------------
def write_gateway_port( gateway_port ):
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_port" )
   return storage.write_file( name_path, "%s" % gateway_port, volume=None )

# -------------------------------------
def read_gateway_port():
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_port" )
   rc = storage.read_file( name_path, volume=None )
   try:
      rc = int(rc)
      return rc
   except:
      return None

# -------------------------------------
def delete_gateway_port():
   name_path = storage.local_path( GATEWAY_STORAGE_DIR, "gateway_port" )
   return storage.delete_file( name_path, volume=None )

# -------------------------------------
def write_syndicate_user_id( pubkey_pem, user_id, volume ):
   uid_path = storage.volume_path( USER_STORAGE_DIR, "syndicate_user_id" )
   return storage.write_encrypted_file( pubkey_pem, uid_path, user_id, volume=volume )

# -------------------------------------
def read_syndicate_user_id( privkey_pem, volume ):
   uid_path = storage.volume_path( USER_STORAGE_DIR, "syndicate_user_id" )
   return storage.read_encrypted_file( privkey_pem, uid_path, volume=volume )

# -------------------------------------
def delete_syndicate_user_id( volume ):
   uid_path = storage.volume_path( USER_STORAGE_DIR, "syndicate_user_id" )
   return storage.delete_file( uid_path, volume=volume )

   
# -------------------------------------
def MS_url( ms_host, nossl=False ):
   if nossl:
      return "http://%s" % ms_host
   else:
      return "https://%s" % ms_host

# -------------------------------------
def setup_syntool( syndicate_user_id, ms_url_api, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, signing_key_type, signing_key_name, setup_dirs=True ):
   # store user data in a convenient place, so we can blow it away later
   tmpdir = None
   if setup_dirs:
      tmpdir = tempfile.mkdtemp( dir=storage.local_path( LOCAL_TMP_DIR ) )
   
   syntool_conf = syntool.make_conf( syndicate_user_id, ms_url_api, setup_dirs=setup_dirs, gateway_keys=tmpdir, volume_keys=tmpdir, user_keys=tmpdir,
                                     signing_pkey_pem=syndicate_user_signingkey_str, verifying_pubkey_pem=syndicate_user_verifyingkey_str, signing_key_type=signing_key_type, signing_key_name=signing_key_name, debug=True )
   
   return tmpdir, syntool_conf
   
# -------------------------------------
def cleanup_syntool( key_paths, tmpdir ):
   
   for key_path in key_paths:
      if storage.path_exists( key_path, volume=None ):
         rc = storage.erase_file( key_path, volume=None )
         if not rc:
            log.critical( "!!! FAILED TO ERASE KEY! Securely erase %s !!!" % key_path )

   try:
      shutil.rmtree( tmpdir )
   except Exception, e:
      log.exception(e)
      log.error("Failed to remove %s" % t)


# -------------------------------------
def is_signature_failure( e ):
   # FIXME: use exception subclasses
   return "-32400" in e.message.lower() and "signature verification error" in e.message.lower()


# -------------------------------------
def make_gateway( pubkey_str, mail_password, syndicate_user_id, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, ms_url, volume_name, gateway_name, gateway_port, gateway_pkey_pem ):
   global CREATED, EXISTS
   
   if gateway_name is None:
      gateway_name = make_default_gateway_name()
      
   log.info("Check gateway for %s" % volume_name )
   
   ms_url_api = os.path.join( ms_url, "api" )
   
   # store user data in a convenient place, so we can blow it away later
   tmpdir, syntool_conf = setup_syntool( syndicate_user_id, ms_url_api, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, "user", syndicate_user_id )
   
   expected_gateway_pkey_path = storage.path_join( tmpdir, Gateway.RUNTIME_KEY_TYPE, privkey_basename( gateway_name ) )
   expected_gateway_signingkey_path = storage.path_join( tmpdir, Gateway.SIGNING_KEY_TYPE, privkey_basename( gateway_name ) )
   
   # see if the gateway exists.  If so, then get its key and be done with it
   gateway_exists = False
   gateway_info = None 
   try:
      gateway_info = syntool.client_call( syntool_conf, "read_gateway", gateway_name )
      gateway_exists = True
      
      readwrite_gateway_caps, _ = Gateway.parse_gateway_caps( "READWRITE" )
      assert (gateway_info['caps'] & readwrite_gateway_caps) == readwrite_gateway_caps
      
   except Exception, e:
      # FIXME: use exception subclasses
      if is_signature_failure(e):
         raise Exception("Invalid private key: signature verification failure")
      
      if not gateway_exists:
         log.info("No gateway %s exists; will try to create one" % gateway_name)
      else:
         # assertion failed
         log.error("Gateway %s is not suitable for SyndicateMail")
         cleanup_syntool( [], tmpdir )
         return False
   
   if not gateway_exists:
      log.info("Creating gateway for %s" % volume_name )
      
      # make the gateway
      gateway_info = None
      try:
         gateway_info = syntool.client_call( syntool_conf, "create_gateway", volume_name, syndicate_user_id, "UG", gateway_name, "localhost", gateway_port )
      except Exception, e:
         # FIXME: use exception subclasses
         if is_signature_failure(e):
            raise Exception("Invalid private key: signature verification failure")
         
         log.exception(e)
         log.error("Failed to set up Volume access")
         cleanup_syntool( [], tmpdir )
         return False
      
      # give the gateway write permission 
      try:
         setcaps_result = syntool.client_call( syntool_conf, "set_gateway_caps", gateway_name, "READWRITE" )
         assert setcaps_result
      except Exception, e:
         log.exception(e)
         log.error("Failed to set up Volume access")
         cleanup_syntool( [expected_gateway_pkey_path, expected_gateway_signingkey_path], tmpdir )
         return False
   
   cleanup_files = [expected_gateway_pkey_path, expected_gateway_signingkey_path]
   
   if gateway_pkey_pem is None:
      # get the private key 
      if not storage.path_exists( expected_gateway_pkey_path, volume=None ):
         log.error("Failed to create gateway %s (no key in %s), ret = %s" % (gateway_name, expected_gateway_pkey_path, gateway_info)  )
         cleanup_syntool( cleanup_files, tmpdir )
         return False
      
      gateway_pkey_pem = storage.read_file( expected_gateway_pkey_path, volume=None )
      if gateway_pkey_pem is None:
         log.error("Failed to load private key from %s" % expected_gateway_pkey_path )
         cleanup_syntool( cleanup_files, tmpdir )
         return None
      
      # store the key 
      rc = write_gateway_privkey( mail_password, gateway_name, gateway_pkey_pem )
      if not rc:
         log.error("Failed to store gateway private key")
         cleanup_syntool( cleanup_files, tmpdir )
         return False
   
   else:
      # everything should be in place already 
      cleanup_files = []
      cleanup_syntool( cleanup_files, tmpdir )
      return EXISTS
      
   # store the gateway name
   rc = write_gateway_name( gateway_name )
   if not rc:
      log.error("Failed to store gateway name")
      cleanup_syntool( cleanup_files, tmpdir )
      return False

   # store gateway port
   rc = write_gateway_port( gateway_port )
   if not rc:
      log.error("Failed to store gateway port")
      cleanup_syntool( cleanup_files, tmpdir )
      delete_gateway_name( gateway_name )
      return False 
   
   cleanup_syntool( cleanup_files, tmpdir )
   
   if not gateway_exists:
      return CREATED
   else:
      return EXISTS


# -------------------------------------
"""
def make_volume( syndicate_user_id, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, ms_url, volume_name, volume_metadata_pubkey_pem=None ):
   global CREATED, EXISTS
   
   log.info("Checking for volume %s" % volume_name)
   
   ms_url_api = os.path.join( ms_url, "api" )
   
   # store user data in a convenient place, so we can blow it away later
   tmpdir, syntool_conf = setup_syntool( syndicate_user_id, ms_url_api, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, "user", syndicate_user_id )
   
   expected_volume_metadata_pubkey_path = storage.path_join( tmpdir, Volume.METADATA_KEY_TYPE, pubkey_basename( volume_name ) )
   expected_volume_signingkey_path = storage.path_join( tmpdir, Volume.SIGNING_KEY_TYPE, privkey_basename( volume_name ) )
   
   # does this volume exist?
   volume_info = None
   volume_exists = False
   try:
      volume_info = syntool.client_call( syntool_conf, "read_volume", volume_name )
      volume_exists = True
      assert volume_info['active']
      assert volume_info['allow_anon']
      assert not volume_info['private']
   except Exception, e:
      # FIXME: use exception subclasses
      if is_signature_failure(e):
         raise Exception("Invalid private key: signature verification failure")
      
      if not volume_exists:
         log.info("No such volume %s; will create it" % volume_name )
      else:
         log.error("Volume %s is insuitable for SyndicateMail")
         cleanup_syntool( [], tmpdir )
         return False
   
   if not volume_exists:
      volume_info = None
      try:
         volume_info = syntool.client_call( syntool_conf, "create_volume", syndicate_user_id, volume_name, "SyndicateMail Volume", DEFAULT_VOLUME_BLOCKSIZE,
                                            metadata_private_key="MAKE_METADATA_KEY", private=False, default_gateway_caps="READONLY", active=True, allow_anon=True )
      except Exception, e:
         # FIXME: use exception subclasses
         if is_signature_failure(e):
            raise Exception("Invalid private key: signature verification failure")
            
         log.exception(e)
         log.error("Failed to set up Volume access")
         cleanup_syntool( [], tmpdir )
         return False
      
   # get the metadata public key
   # we can throw the signing key away.
   
   if not storage.path_exists( expected_volume_metadata_pubkey_path, volume=None ):
      log.error("Failed to create volume %s (no key in %s), ret = %s" % (volume_name, expected_volume_metadata_pubkey_path, volume_info)  )
      cleanup_syntool( [expected_volume_metadata_pubkey_path, expected_volume_signingkey_path], tmpdir )
      return False
   
   if volume_metadata_pubkey_pem is None:
      volume_metadata_pubkey_pem = storage.read_file( expected_volume_metadata_pubkey_path, volume=None )
      if volume_metadata_pubkey_pem is None:
         log.error("Failed to load metadata public key from %s" % expected_volume_metadata_pubkey_path )
         cleanup_syntool( [expected_volume_metadata_pubkey_path, expected_volume_signingkey_path], tmpdir )
         return False
      
      # store the keys
      rc = write_volume_pubkey( volume_name, volume_metadata_pubkey_pem )
      if not rc:
         log.error("Failed to store volume metadata public key")
         cleanup_syntool( [expected_volume_metadata_pubkey_path, expected_volume_signingkey_path], tmpdir )
         return False

   cleanup_syntool( [expected_volume_metadata_pubkey_path, expected_volume_signingkey_path], tmpdir )
   
   if not volume_exists:
      return CREATED
   else:
      return EXISTS


# -------------------------------------
def delete_volume( syndicate_user_id, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, ms_url, volume_name, volume_inst ):
   
   log.info("Deleting volume contents")
   
   # delete Volume contents
   if volume_inst is not None:
      rc = storage.volume_rmtree( volume_inst, "/" )
      if not rc:
         log.error("Failed to clear Volume contents")
         return False
      
   log.info("Deleting volume")
   
   ms_url_api = os.path.join( ms_url, "api" )
   
   # store user data in a convenient place, so we can blow it away later
   tmpdir, syntool_conf = setup_syntool( syndicate_user_id, ms_url_api, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, "user", syndicate_user_id )
   
   delete_result = None
   try:
      delete_result = syntool.client_call( syntool_conf, "delete_volume", volume_name )
      assert delete_result
   except Exception, e:
      
      # FIXME: use exception subclasses
      if "-32400" in e.message.lower() and "signature verification failure" in e.message.lower():
         raise Exception("Invalid private key: signature verification failure")
      
      log.exception(e)
      log.error("Failed to delete volume")
      return False
   
   finally:
      cleanup_syntool( [], tmpdir )
   
   # delete the metadata key
   rc = delete_volume_pubkey( volume_name )
   if not rc:
      log.error("Failed to delete metadata public key for %s" % volume_name)
      # not really an error--it'll get overwritten later 
      
   return True
"""

# -------------------------------------
def delete_gateway( syndicate_user_id, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, ms_url, gateway_name ):
   
   log.info("Deleting gateway")
   
   ms_url_api = os.path.join( ms_url, "api" )
   
   # store user data in a convenient place, so we can blow it away later
   tmpdir, syntool_conf = setup_syntool( syndicate_user_id, ms_url_api, syndicate_user_signingkey_str, syndicate_user_verifyingkey_str, "user", syndicate_user_id )
   
   delete_result = None
   try:
      delete_result = syntool.client_call( syntool_conf, "delete_gateway", gateway_name )
      assert delete_result
   except Exception, e:
      if "-32400" in e.message.lower() and "signature verification failure" in e.message.lower():
         raise Exception("Invalid private key: signature verification failure")
      
      log.exception(e)
      log.error("Failed to delete gateway")
      return False
   
   finally:
      cleanup_syntool( [], tmpdir )
   
   rc = delete_gateway_name()
   if not rc:
      log.error("!!! Failed to erase gateway name for %s !!!" % gateway_name )
   
   rc = delete_gateway_port()
   if not rc:
      log.error("!!! Failed to erase gateway port for %s !!!" % gateway_name )
   
   # delete the metadata key
   rc = delete_gateway_privkey( gateway_name )
   if not rc:
      log.error("!!! Failed to erase private key for %s !!!" % gateway_name)
      return False
      
   return True


# -------------------------------------
def create_account( syndicatemail_uid, syndicatemail_password, mail_server, password, ms_url, syndicate_user_id, syndicate_user_password, syndicate_user_privkey_str, syndicate_user_verifyingkey_str,
                    existing_volume_name, existing_volume_pubkey_pem, num_downloads=1, duration=3600, existing_gateway_name=None, existing_gateway_port=None, existing_gateway_pkey_pem=None ):
   
   global DEFAULT_GATEWAY_PORT, ALL_STORAGE_PACKAGES, VOLUME_STORAGE_ROOT, LOCAL_STORAGE_ROOT
   
   if storage.LOCAL_ROOT_DIR is None:
      # FIXME: remove this kludge
      fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
      this_module = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS )
      
      rc = storage.setup_local_storage( LOCAL_STORAGE_ROOT, ALL_STORAGE_PACKAGES + [this_module] )
      
   if existing_gateway_name is None:
      existing_gateway_name = make_default_gateway_name()
      
   if existing_gateway_port is None:
      existing_gateway_port = DEFAULT_GATEWAY_PORT
      
   # generate a SyndicateMail private key
   pubkey_pem, privkey_pem = keys.generate_key_pair()
   
   # create or load the gateway
   rc = make_gateway( pubkey_pem, syndicatemail_password, syndicate_user_id, syndicate_user_privkey_str, syndicate_user_verifyingkey_str, ms_url,
                      existing_volume_name, existing_gateway_name, existing_gateway_port, existing_gateway_pkey_pem )
   if not rc:
      raise Exception("Failed to create Gateway")
   
   gateway_rc = rc
   
   # create email address
   MS_host = urlparse( ms_url ).netloc
   email = contact.make_addr_str( syndicatemail_uid, existing_volume_name, MS_host, mail_server )
   
   log.info("SyndicateMail address is %s" % email )
   
   # cleanup Syndicate function
   def cleanup_syndicate():
      if gateway_rc != EXISTS:
         rc = delete_gateway( syndicate_user_id, syndicate_user_privkey_str, syndicate_user_verifyingkey_str, ms_url, existing_gateway_name )
         if not rc:
            log.critical("!!! cleanup failure: could not delete gateway %s !!!" % existing_gateway_name )
            
   
   # get the private key
   existing_gateway_pkey_pem = read_gateway_privkey( syndicatemail_password, existing_gateway_name )
   if existing_gateway_pkey_pem is None:
      log.critical("Failed to store gateway private key!")
      cleanup_syndicate()
      return False
      
   try:
      # open the Volume
      vol = SyndicateVolume(  gateway_name=existing_gateway_name,
                              gateway_port=existing_gateway_port,
                              oid_username=syndicate_user_id,
                              oid_password=syndicate_user_password,
                              ms_url=ms_url,
                              my_key_pem=existing_gateway_pkey_pem,
                              volume_name=existing_volume_name,
                              storage_root=storage.local_path( GATEWAY_RUNTIME_STORAGE ) )
   
   except Exception, e:
      log.exception(e)
      log.critical("Failed to connect to Volume!")
      
      cleanup_syndicate()
      return False
   
   
   # set up storage on the volume
   # FIXME: this is a kludge to get our storage directories included
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   this_module = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS )
   
   rc = storage.setup_volume_storage( VOLUME_STORAGE_ROOT, ALL_STORAGE_PACKAGES + [this_module], volume=vol )
   if not rc:
      log.critical("Failed to set up Volume!")
      cleanup_syndicate()
      return False
   
   # for clean up function...
   stored_private_key = False
   stored_private_key_locally = False
   stored_public_key = False
   stored_volume_public_key = False
   stored_syndicate_account_name = False
   
   # cleanup function...
   def cleanup_on_failure():
      cleanup_syndicate()
      
      if stored_private_key:
         keys.delete_private_key_from_volume( email, volume=vol )
      
      if stored_private_key_locally:
         keys.delete_private_key( email )
         
      if stored_public_key:
         keys.delete_public_key( email )
         
      if stored_volume_public_key:
         delete_volume_pubkey( existing_volume_name )
         
      if stored_syndicate_account_name:
         delete_syndicate_user_id( vol )
                                 
   # encrypt and store the account private key on the Volume (so it can be transferred to the other endpoints)
   privkey = CryptoKey.importKey( privkey_pem )
   rc = keys.store_private_key_to_volume( email, privkey, password, num_downloads, duration, vol )

   if not rc:
      log.error("Failed to store account info")
      cleanup_on_failure()
      raise Exception("Failed to store account info")

   stored_private_key = True
   
   # save locally too
   rc = keys.store_private_key( email, privkey, password )
   if not rc:
      log.error("Failed to store account info locally")
      cleanup_on_failure()
      raise Exception("Failed to store account info")
   
   stored_private_key_locally = True
   
   # sign and store the public key to the Volume
   pubkey = CryptoKey.importKey( pubkey_pem )
   syndicate_user_privkey = CryptoKey.importKey( syndicate_user_privkey_str )
   rc = keys.store_public_key( email, pubkey, syndicate_user_privkey )
   if not rc:
      log.error("Failed to store account info")
      cleanup_on_failure()
      raise Exception("Failed to store account info")
   
   stored_public_key = True
   
   # store the metadata public key locally 
   rc = write_volume_pubkey( existing_volume_name, existing_volume_pubkey_pem )
   if not rc:
      log.error("Failed to store volume metadata public key")
      cleanup_on_failure()
      raise Exception("Failed to store Volume info")

   stored_volume_public_key = True
   
   # store our syndicate user ID for later
   rc = write_syndicate_user_id( pubkey_pem, syndicate_user_id, vol )
   if not rc:
      log.error("Failed to store Syndicate account name")
      cleanup_on_failure()
      raise Exception("Failed to store Syndicate account name")

   stored_syndicate_account_name = True
   
   return privkey_pem


# -------------------------------------
def read_account( syndicatemail_password, email_addr ):
      
   try:
      email_addr_parsed = contact.parse_addr( email_addr )
   except:
      log.error("Invalid email address %s" % email_addr)
      return None
                                             
   gateway_name = read_gateway_name()
   if gateway_name is None:
      log.error("Failed to load gateway name")
      return None
   
   gateway_port = read_gateway_port()
   if gateway_port is None:
      log.error("Failed to read gateway port")
      return None
   
   volume_pubkey_pem = read_volume_pubkey( email_addr_parsed.volume )
   if volume_pubkey_pem is None:
      log.error("Failed to read volume public key")
      return None
   
   gateway_privkey_pem = read_gateway_privkey( syndicatemail_password, gateway_name )
   if gateway_privkey_pem is None:
      log.error("Failed to read gateway private key")
      return None
   
   account = SyndicateAccountInfo( gateway_name=gateway_name, 
                                   gateway_port=gateway_port,
                                   volume_pubkey_pem=volume_pubkey_pem,
                                   gateway_privkey_pem=gateway_privkey_pem )
   
   return account
      

# -------------------------------------
def delete_account( privkey_str, email, volume, syndicate_user_id, remove_gateway=False, syndicate_user_privkey_str=None, syndicate_user_verifykey_str=None, test=False ):
   # NOTE: verify user first!
   try:
      parsed_email = contact.parse_addr( email )
   except Exception, e:
      log.exception(e)
      log.error("Invalid email %s" % email)
      return False
   
   gateway_name = read_gateway_name()
   
   delete_syndicate_user_id( volume )
   
   if remove_gateway:
      have_data = True
      
      if syndicate_user_id is None:
         log.error("Failed to load Syndicate user ID")
         have_data = False
      
      if syndicate_user_privkey_str is None:
         log.error("Need Syndicate user private (signing) key to delete gateway and volume")
         have_data = False
      
      if syndicate_user_verifykey_str is None:
         log.error("Need Syndicate user public (verifying) key to delete gateway and volume")
         have_data = False
      
      if have_data:
         gateway_name = read_gateway_name()
         
         # delete the gateway
         rc = delete_gateway( syndicate_user_id, syndicate_user_privkey_str, syndicate_user_verifykey_str, MS_url( parsed_email.MS, nossl=test ), gateway_name )
         if not rc:
            log.critical("!!! Failed to delete gateway %s !!!" % gateway_name )
   
   keys.delete_private_key_from_volume( email )
   keys.delete_private_key( email )
   keys.delete_public_key( email )
   
   delete_gateway_name()
   delete_gateway_port()
   
   return True

# -------------------------------------
def read_account_volume_pubkey( email_addr, storage_root=None ):
   try:
      email_addr_parsed = contact.parse_addr( email_addr )
   except:
      log.error("Invalid email address %s" % email_addr)
      return None
                                  
   volume_pubkey_pem = read_volume_pubkey( email_addr_parsed.volume, prefix=storage_root )
   return volume_pubkey_pem

# -------------------------------------
if __name__ == "__main__":
   import session
   
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   fake_vol = session.do_test_volume( "/tmp/storage-test/volume" )
   singleton.set_volume( fake_vol )
   
   print "------- setup --------"
   fake_mod = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS + keys.LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS + keys.VOLUME_STORAGE_DIRS )
   rc = storage.setup_local_storage( "/tmp/storage-test/local", [fake_mod] )
   assert rc, "setup_local_storage failed"
   
   pubkey_str = """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxwhi2mh+f/Uxcx6RuO42
EuVpxDHuciTMguJygvAHEuGTM/0hEW04Im1LfXldfpKv772XrCq+M6oKfUiee3tl
sVhTf+8SZfbTdR7Zz132kdP1grNafGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0R
rXyEnxpJmnLyNYHaLN8rTOig5WFbnmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsY
QPywYw8nJaax/kY5SEiUup32BeZWV9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmx
L1LRX5T2v11KLSpArSDO4At5qPPnrXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8V
WpsmzZaFExJ9Nj05sDS1YMFMvoINqaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65
A7d9Fn/B42n+dCDYx0SR6obABd89cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kw
JtgiKSCt6m7Hwx2kwHBGI8zUfNMBlfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CB
hGBRJQFWVutrVtTXlbvT2OmUkRQT9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyP
GuKX1KO5JLQjcNTnZ3h3y9LIWHsCTCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2
lPCia/UWfs9eeGgdGe+Wr4sCAwEAAQ==
-----END PUBLIC KEY-----
""".strip()
   
   privkey_str = """
-----BEGIN RSA PRIVATE KEY-----
MIIJKQIBAAKCAgEAxwhi2mh+f/Uxcx6RuO42EuVpxDHuciTMguJygvAHEuGTM/0h
EW04Im1LfXldfpKv772XrCq+M6oKfUiee3tlsVhTf+8SZfbTdR7Zz132kdP1grNa
fGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0RrXyEnxpJmnLyNYHaLN8rTOig5WFb
nmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsYQPywYw8nJaax/kY5SEiUup32BeZW
V9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmxL1LRX5T2v11KLSpArSDO4At5qPPn
rXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8VWpsmzZaFExJ9Nj05sDS1YMFMvoIN
qaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65A7d9Fn/B42n+dCDYx0SR6obABd89
cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kwJtgiKSCt6m7Hwx2kwHBGI8zUfNMB
lfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CBhGBRJQFWVutrVtTXlbvT2OmUkRQT
9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyPGuKX1KO5JLQjcNTnZ3h3y9LIWHsC
TCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2lPCia/UWfs9eeGgdGe+Wr4sCAwEA
AQKCAgEAl1fvIzkWB+LAaVMzZ7XrdE7yL/fv4ufMgzIB9ULjfh39Oykd/gxZBQSq
xIyG5XpRQjGepZIS82I3e7C+ohLg7wvE4qE+Ej6v6H0/DonatmTAaVRMWBNMLaJi
GWx/40Ml6J/NZg0MqQLbw+0iAENAz/TBO+JXWZRSTRGif0Brwp2ZyxJPApM1iNVN
nvhuZRTrjv7/Qf+SK2gMG62MgPceSDxdO9YH5H9vFXT8ldRrE8SNkUrnGPw5LMud
hp6+8bJYQUnjvW3vcaVQklp55AkpzFxjTRUO09DyWImqiHtME91l820UHDpLLldS
1PujpDD54jyjfJF8QmPrlCjjWssm5ll8AYpZFn1mp3SDY6CQhKGdLXjmPlBvEaoR
7yfNa7JRuJAM8ntrfxj3fk0B8t2e5NMylZsBICtposCkVTXpBVJt50gs7hHjiR3/
Q/P7t19ywEMlHx5edy+E394q8UL94YRf7gYEF4VFCxT1k3BhYGw8m3Ov22HS7EZy
2vFqro+RMOR7VkQZXvGecsaZ/5xhL8YIOS+9S90P0tmMVYmuMgp7L+Lm6DZi0Od6
cwKxB7LYabzrpfHXSIfqE5JUgpkV5iTVo4kbmHsrBQB1ysNFR74E1PJFy5JuFfHZ
Tpw0KDBCIXVRFFanQ19pCcbP85MucKWif/DhjOr6nE/js/8O6XECggEBAN0lhYmq
cPH9TucoGnpoRv2o+GkA0aA4HMIXQq4u89LNxOH+zBiom47AAj2onWl+Zo3Dliyy
jBSzKkKSVvBwsuxgz9xq7VNBDiaK+wj1rS6MPqa/0Iyz5Fhi0STp2Fm/elDonYJ8
Jp8MRIWDk0luMgaAh7DuKpIm9dsg45wQmm/4LAGJw6WbbbZ4TUGrT684qIRXk8Q5
1Z08hgSOKUIyDwmv4LqenV6n4XemTq3zs8R0abQiJm81YqSOXwsJppXXgZoUM8sg
L/gxX5pXxCzAfC2QpLI94VJcVtRUNGBK5rMmrANd2uITg6h/wDCy9FxRKWG8f+p4
qAcxr/oXXXebI98CggEBAOZmppx+PoRWaZM547VebUrEDKuZ/lp10hXnr3gkDAKz
2av8jy3YdtCKq547LygpBbjd1i/zFNDZ/r4XT+w/PfnNRMuJR5td29T+lWMi3Hm3
ant/o8qAyVISgkRW1YQjTAhPwYbHc2Y24n/roCutrtIBG9WMLQNEbJUXjU5uNF/0
+ezKKNFIruCX/JafupBfXl1zAEVuT0IkqlHbmSL4oxYafhPorLzjIPLiJgjAB6Wb
iIOVIUJt61O6vkmeBWOP+bj5x1be6h35MlhKT+p4rMimaUALvbGlGQBX+Bm54/cN
Ih0Kqx/gsDoD5rribQhuY0RANo1wfXdkW/ajHZihCdUCggEABO01EGAPrBRskZG/
JUL1cek1v4EZKmyVl21VOvQo0mVrIW2/tjzrWj7EzgLXnuYF+tqEmfJQVJW5N0pz
TV/1XHa7qrlnGBe27Pzjost2VDcjnitfxgKr75wj9KKRA07UtsC34ZRKd/iZ/i90
NIqT6rkqTLLBmAfuKjeNWoi0KBJrSI19Ik9YHlyHvBLI76pfdrNMw25WZ+5VPfy8
xpC+7QRSCVZHQziSOUwnLJDlTFcbk7u/B3M1A114mJJad7QZWwlgLgJFj03qR1H1
ONoA6jLyuFXQkzkjZg+KKysAALW310tb+PVeVX6jFXKnJvdX6Kl+YAbYF3Dv7q5e
kq+OGQKCAQEAngEnoYqyNO9N17mLf4YSTYPFbKle1YqXWI5at3mBAxlz3Y6GYlpg
oQN4TjsoS9JWKkF38coyLEhTeulh1hJI3lb3Jt4uTU5AxAETUblGmfI/BBK0sNtB
NRecXmFubAAI1GpdvaBqc16QVkmwvkON8FbyT7Ch7euuy1Arh+3r3SKTgt/gviWq
SDvy7Rj9SKUegdesB/FuSV37r8d5bZI1xaLFc8HNNHxOzEJq8vU+SUQwioxrErNu
/yzB8pp795t1FnW1Ts3woD2VWRcdVx8K30/APjvPC1S9oI6zhnEE9Rf8nQ4D7QiZ
0i96vA8r1uxdByFCSB0s7gPVTX7vfQxzQQKCAQAnNWvIwXR1W40wS5kgKwNd9zyO
+G9mWRvQgM3PptUXM6XV1kSPd+VofGvQ3ApYJ3I7f7VPPNTPVLI57vUkhOrKbBvh
Td3OGzhV48behsSmOEsXkNcOiogtqQsACZzgzI+46akS87m+OHhP8H3KcdsvGUNM
xwHi4nnnVSMQ+SWtSuCHgA+1gX5YlNKDjq3RLCRG//9XHIApfc9c52TJKZukLpfx
chit4EZW1ws/JPkQ+Yer91mCQaSkPnIBn2crzce4yqm2dOeHlhsfo25Wr37uJtWY
X8H/SaEdrJv+LaA61Fy4rJS/56Qg+LSy05lISwIHBu9SmhTuY1lBrr9jMa3Q
-----END RSA PRIVATE KEY-----
""".strip()

   # need to run setup.sh for this
   testuser_signing_pkey_fd = open( os.path.expanduser("~/.syndicate/user_keys/signing/testuser@gmail.com.pkey"))
   testuser_signing_pkey_str = testuser_signing_pkey_fd.read()
   testuser_signing_pkey_fd.close()
   
   testuser_verifying_pubkey_fd = open( os.path.expanduser("~/.syndicate/user_keys/verifying/testuser@gmail.com.pub" ))
   testuser_verifying_pubkey_str = testuser_verifying_pubkey_fd.read()
   testuser_verifying_pubkey_fd.close()
   
   volume_name = "mail"
   volume_pubkey_pem_fd = open( os.path.expanduser("~/.syndicate/volume_keys/metadata/mail.pub") )
   volume_pubkey_pem = volume_pubkey_pem_fd.read()
   volume_pubkey_pem_fd.close()
   
   print "------- create account --------"
   #def create_account( syndicatemail_uid, mail_server, password, ms_url, syndicate_user_id, syndicate_user_password, syndicate_user_privkey_str, syndicate_user_verifyingkey_str, num_downloads, duration,
   account_privkey_pem = create_account( "fakeuser", "poop", "t510", "yoink", "http://localhost:8080", "testuser@gmail.com", "sniff", testuser_signing_pkey_str, testuser_verifying_pubkey_str, volume_name, volume_pubkey_pem )
   assert account_privkey_pem is not None and account_privkey_pem != False, "create_account failed"
   
   print "------- read account --------"
   account_info = read_account( "poop", "fakeuser.mail.localhost%3A8080@t510" )
   assert account_info is not None, "read_account failed"
   
   import pprint 
   pp = pprint.PrettyPrinter()
   pp.pprint( account_info )
   
   print "------- load volume --------"
   
   from syndicate.volume import Volume 
   volume = Volume( volume_name=volume_name,
                    gateway_name=account_info.gateway_name,
                    gateway_port=account_info.gateway_port,
                    oid_username="testuser@gmail.com",
                    oid_password="sniff",
                    ms_url="http://localhost:8080",
                    my_key_pem=account_info.gateway_privkey_pem,
                    storage_root=storage.local_path( GATEWAY_RUNTIME_STORAGE ) )
   
   print "------- delete account --------"
   rc = delete_account( account_privkey_pem, "fakeuser.mail.localhost:8080@t510", volume, "testuser@gmail.com", remove_gateway=True,
                        syndicate_user_privkey_str = testuser_signing_pkey_str, syndicate_user_verifykey_str = testuser_verifying_pubkey_str, test=True )
   
   assert rc, "delete_account failed"