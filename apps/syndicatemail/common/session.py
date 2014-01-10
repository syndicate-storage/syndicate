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

import time
import os
import errno
import stat

import keys
import contact
import account
import singleton

import syndicate.client.common.log as Log

log = Log.get_logger()

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import syndicate.volume
from syndicate.volume import Volume

# -------------------------------------
SESSION_LENGTH = 3600 * 24 * 7      # one week

# -------------------------------------
def do_first_login( config, password, syndicate_oid_username, syndicate_oid_password, user_signing_privkey_pem, user_verifying_pubkey_pem, volume_pubkey_pem ):
   
   try:
      parsed_email = contact.parse_addr( config['email'] )
   except:
      raise Exception("Invalid email '%s'" % config['email'] )
   
   account_privkey_pem = account.create_account( config['mail_username'], password, config['mail_server'], password, config['MS'], syndicate_oid_username, syndicate_oid_password,
                                                 user_signing_privkey_pem, user_verifying_pubkey_pem, parsed_email.volume, volume_pubkey_pem )

   if account_privkey_pem is None or account_privkey_pem == False:
      raise Exception("Failed to create an account for %s" % config['email'] )
   
   return account_privkey_pem

# -------------------------------------
def do_login( config, email, password, syndicate_oid_username, syndicate_oid_password, user_signing_privkey_pem=None, user_verifying_pubkey_pem=None, volume_pubkey_pem=None, create_on_absent=False):
   # TODO: remove need for OID login/password
   
   global SESSION_LENGTH
   
   try:
      parsed_email = contact.parse_addr( email )
   except Exception, e:
      log.exception(e)
      raise Exception("Invalid email '%s'" % email)
   
   config['email'] = email
   config['session_expires'] = int(time.time()) + SESSION_LENGTH
   config['volume_name'] = parsed_email.volume
   config['mail_username'] = parsed_email.username
   config['mail_server'] = parsed_email.server
   
   privkey = None
   
   # attempt to get the volume public key 
   # FIXME: remove this kludge so storage is sanely initialized
   import storage
   storage.LOCAL_ROOT_DIR = "/"
   existing_volume_pubkey_pem = account.read_account_volume_pubkey( email, storage_root=account.LOCAL_STORAGE_ROOT )
   storage.LOCAL_ROOT_DIR = None
   
   need_storage_setup = False
   if existing_volume_pubkey_pem is None:
      # no account exists 
      log.warning("No account for %s exists" % email)
      if create_on_absent:
         log.info("Creating account for %s" % email)
         
         if user_signing_privkey_pem is None or user_verifying_pubkey_pem is None:
            raise Exception("Need to give Syndicate user signing and verifying keys for %s" % syndicate_oid_username )
         
         account_privkey_pem = None
         try:
            account_privkey_pem = do_first_login( config, password, syndicate_oid_username, syndicate_oid_password, user_signing_privkey_pem, user_verifying_pubkey_pem, volume_pubkey_pem )
         except Exception, e:
            log.error("Failed to create account for %s" % email)
            log.exception(e)
            raise e
         
         privkey = CryptoKey.importKey( account_privkey_pem )
         existing_volume_pubkey_pem = volume_pubkey_pem
         
      else:
         raise Exception("No such account %s" % email )
      
   else:
      # set up storage
      need_storage_setup = True
      
      
   config['volume_pubkey_pem'] = existing_volume_pubkey_pem
   
   # set up local storage
   if need_storage_setup:
      rc = storage.setup_local_storage( account.LOCAL_STORAGE_ROOT, [] )
      if not rc:
         do_logout( config )
         raise Exception("Failed to set up local storage")
   
   # get account info
   account_info = account.read_account( password, email )
   if account_info is None:
      do_logout( config )
      raise Exception("Failed to read account information.")
   
   gateway_name = account_info.gateway_name
   gateway_port = account_info.gateway_port
   gateway_privkey_pem = account_info.gateway_privkey_pem
   volume_pubkey_pem = account_info.volume_pubkey_pem
   
   # load the Volume
   volume = Volume( gateway_name=gateway_name,
                    gateway_port=gateway_port,
                    ms_url=parsed_email.MS,
                    my_key_pem=gateway_privkey_pem,
                    volume_key_pem=volume_pubkey_pem,   # FIXME: volume_pubkey_pem consistent naming
                    volume_name=parsed_email.volume,
                    oid_username=syndicate_oid_username,
                    oid_password=syndicate_oid_password )
                    
   
   config['gateway_privkey_pem'] = gateway_privkey_pem
   config['volume'] = volume
   singleton.set_volume( volume )
   
   # set up volume storage
   if need_storage_setup:
      rc = storage.setup_storage( account.VOLUME_STORAGE_ROOT, account.LOCAL_STORAGE_ROOT, [], volume=volume )
      if not rc:
         do_logout( config )
         raise Exception( "Failed to set up storage" )
   
   # get our account private key
   if privkey is None:
      # attempt to load the SyndicateMail private key
      privkey = keys.load_private_key( email, password )
      if privkey is None:
         
         raise Exception("Invalid username/password")
   
   privkey_str = privkey.exportKey()
   
   config['privkey'] = privkey
   config['pubkey'] = privkey.publickey()
   
   return True


# -------------------------------------
def do_logout( config ):
   for k in ['privkey', 'pubkey', 'session_expires', 'volume_name', 'volume', 'gateway_privkey_pem', 'email', 'volume_pubkey_pem']:
      if config.has_key(k):
         del config[k]
         
   singleton.set_volume( None )

# -------------------------------------
def do_delete( config, email, password, delete_gateway=False, syndicate_user_privkey_str=None, syndicate_user_pubkey_str=None ):
   try:
      do_login( config, email, password )
   except Exception, e:
      log.exception(e)
      log.error("Invalid credentials")
      return False
   
   rc = account.delete_account( config['privkey'].exportKey(), email, remove_gateway=delete_gateway, syndicate_user_privkey_str=syndicate_user_privkey_str, syndicate_user_verifykey_str=syndicate_user_pubkey_str )
   do_logout( config )
   return rc

# -------------------------------------
def is_expired( config ):
   for session_key in ['privkey', 'session_expires']:
      if session_key not in config:
         return True
   
   if config['session_expires'] < 0:
      return False
   
   if time.time() > config['session_expires']:
      return True
   
   return False

# -------------------------------------
class FakeVolume:
   def __init__(self, storage_root):
      try:
         os.makedirs(storage_root)
      except OSError, oe:
         if oe.errno != errno.EEXIST:
            raise oe 
         else:
            pass
         
      self.storage_root = storage_root
   
   def create(self, path, mode ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/"))
         fd = os.open( fp, os.O_CREAT | os.O_RDONLY )
         os.close( fd )
         os.chmod( fp, mode )
      except OSError, oe:
         log.error( "create %s rc = %d" % (fp, -oe.errno) )
         return -oe.errno
      
   def open( self, path, flags ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/"))
         fd = os.open( fp, flags )
         return fd
      except OSError, oe:
         log.error( "open %s rc = %d" % (fp, -oe.errno) )
         return -oe.errno
   
   def read( self, fd, size ):
      try:
         buf = os.read( fd, size )
         return buf
      except OSError, oe:
         return -oe.errno
   
   def write( self, fd, buf ):
      try:
         return os.write( fd, buf )
      except OSError, oe:
         return -oe.errno
   
   def close( self, fd ):
      try:
         os.close( fd )
      except OSError, oe:
         return -oe.errno
      
      return 0
   
   def mkdir( self, path, mode ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/"))
         os.mkdir( fp, mode )
         return 0
      except OSError, oe:
         return -oe.errno
   
   def rmdir( self, path ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/"))
         os.rmdir( fp, path )
         return 0
      except OSError, oe:
         return -oe.errno 
   
   def opendir( self, path ):
      return path
   
   def readdir( self, handle ):
      try:
         fp = os.path.join(self.storage_root, handle.strip("/"))
         names = os.listdir( fp )
      except OSError, oe:
         log.error( "readdir %s rc = %d" % (fp, -oe.errno) )
         return -oe.errno
      
      ret = []
      for name in names:
         fp = os.path.join( self.storage_root, handle.strip("/"), name.strip("/") )
         sb = os.stat( fp )
         
         ftype = 0
         if stat.S_ISDIR( sb.st_mode ):
            ftype = syndicate.volume.Volume.ENT_TYPE_DIR 
         elif stat.S_ISREG( sb.st_mode ):
            ftype = syndicate.volume.Volume.ENT_TYPE_FILE
            
         se = syndicate.volume.SyndicateEntry( type=ftype, name=name, file_id=sb.st_ino, ctime=(sb.st_ctime, 0), mtime=(sb.st_mtime,0), write_nonce=0, version=0,
                                               max_read_freshness=-1, max_write_freshness=-1, owner=0, coordinator=0, volume=0, mode=sb.st_mode, size=sb.st_size )
         
         ret.append( se )
      
      return ret
   
   def closedir( self, path ):
      return 0
   
   def unlink( self, path ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/"))
         os.unlink( fp )
      except OSError, oe:
         log.error( "unlink %s rc = %d" % (fp, -oe.errno) )
         return -oe.errno
      
   def fsync( self, fd ):
      try:
         os.fsync( fd )
         return 0
      except OSError, oe:
         return -oe.errno
   
   def seek( self, fd, pos, whence ):
      try:
         return os.lseek( fd, pos, whence )
      except OSError, oe:
         return -oe.errno
   
   def stat( self, path ):
      try:
         fp = os.path.join(self.storage_root, path.strip("/")) 
         return os.stat( fp )
      except OSError, oe:
         #log.error( "stat %s rc = %d" % (fp, -oe.errno) )
         return -oe.errno

# -------------------------------------
def do_test_login( config, email, password, volume=None ):
   
   privkey_str = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
""".strip()
   
   
   try:
      parsed_email = contact.parse_addr( email )
   except:
      raise Exception("Invalid email '%s'" % email)
   
   privkey = CryptoKey.importKey( privkey_str )
   config['privkey'] = privkey
   config['pubkey'] = privkey.publickey()
   config['session_expires'] = time.time() + SESSION_LENGTH
   config['volume_name'] = parsed_email.volume
   config['volume'] = volume
   return True


# -------------------------------------
def do_test_volume( storage_root ):
   fake = FakeVolume( storage_root )
   return fake

if __name__ == "__main__":
   print "put some tests here"
   