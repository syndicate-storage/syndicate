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
import contact
import account

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
def do_login( config, email, password ):
   global SESSION_LENGTH
   
   try:
      parsed_email = contact.parse_addr( email )
   except:
      raise Exception("Invalid email '%s'" % email)
   
   # attempt to load the SyndicateMail private key
   privkey = keys.load_private_key( email, password )
   if privkey is None:
      raise Exception("Invalid username/password")
   
   config['privkey'] = privkey
   config['pubkey'] = privkey.publickey()
   config['session_expires'] = int(time.time()) + SESSION_LENGTH
   config['ms_url'] = parsed_email.MS
   config['volume_name'] = parsed_email.volume
   
   
   # attempt to load the (local) gateway private key.
   # it will be encrypted with the SyndicateMail private key
   gateway_name = account.read_gateway_name()
   gateway_privkey_str = account.read_gateway_privkey( config['privkey'].exportKey(), gateway_name )
   
   # TODO: initialize volume
   return True


# -------------------------------------
def do_logout( config ):
   del config['privkey']
   del config['pubkey']
   del config['session_expires']
   del config['ms_url']
   del config['volume_name']

# -------------------------------------
def do_delete( config, email, password, delete_volume_and_gateway=False, syndicate_user_privkey=None ):
   try:
      do_login( config, email, password )
   except Exception, e:
      log.exception(e)
      log.error("Invalid credentials")
      return False
   
   rc = account.delete_account( config['privkey'].exportKey(), email, delete_volume_and_gateway=delete_volume_and_gateway, syndicate_user_privkey=syndicate_user_privkey )
   do_logout( config )
   return rc

# -------------------------------------
def is_expired( config ):
   for session_key in ['privkey', 'session_expires']:
      if session_key not in config:
         return True
   
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
def do_test_login( config, email, password ):
   
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
   config['ms_url'] = parsed_email.MS
   config['volume_name'] = parsed_email.volume
   return True


# -------------------------------------
def do_test_volume( storage_root ):
   fake = FakeVolume( storage_root )
   return fake

if __name__ == "__main__":
   print "put some tests here"
   