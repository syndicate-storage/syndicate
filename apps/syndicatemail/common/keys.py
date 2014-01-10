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

import os
import storage
import collections
from itertools import izip

import scrypt
import base64

from Crypto.Hash import SHA256 as HashAlg
from Crypto.Hash import HMAC
from Crypto.PublicKey import RSA as CryptoKey
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto import Random

import syndicate.client.common.log as Log

import singleton 

log = Log.get_logger()

PRIVATE_STORAGE_DIR = "/keys"

STORAGE_DIR = "/keys"

VOLUME_STORAGE_DIRS = [
   STORAGE_DIR
]

LOCAL_STORAGE_DIRS = [
   PRIVATE_STORAGE_DIR
]

#-------------------------
EncryptedPrivateKey = collections.namedtuple( "EncryptedPrivateKey", ["salt", "data"] )
SignedPublicKey = collections.namedtuple( "SignedPublicKey", ["pubkey_str", "signature"] )

#-------------------------
KEY_SIZE = 4096

def generate_key_pair( key_size=KEY_SIZE ):
   rng = Random.new().read
   key = CryptoKey.generate(key_size, rng)

   private_key_pem = key.exportKey()
   public_key_pem = key.publickey().exportKey()

   return (public_key_pem, private_key_pem)

#-------------------------
def make_key_local_path( name ):
   global PRIVATE_STORAGE_DIR
   
   # NOTE: do NOT store this on the Volume
   return storage.local_path( PRIVATE_STORAGE_DIR, name )


#-------------------------
def make_key_volume_path( name ):
   global STORAGE_DIR
   
   # used for loading the private key from the Volume
   return storage.volume_path( STORAGE_DIR, name )


#-------------------------
def encrypt_with_password( data, password ):
   # first, make a PBKDF2 key from the password
   salt = os.urandom(64)        # 512 bits
   
   key = PBKDF2( unicode(password), salt, dkLen=64 )  # 512 bit key
   
   # second, feed this key and the private key into scrypt.
   # NOTE: scrypt uses AES256-CTR with encrypt-then-MAC
   enc_data = scrypt.encrypt( str(data), key )
   
   return salt, enc_data


#-------------------------
def decrypt_with_password( encrypted_data, password, salt ):
   # reproduce the password for decryption...
   key = PBKDF2( unicode(password), salt, dkLen=64 )
   
   try:
      data = scrypt.decrypt( encrypted_data, key )
   except:
      log.error( "Failed to decrypt data.  Wrong password?")
      return None
   
   return data
   
#-------------------------
def encrypt_private_key( privkey_str, password ):
   salt, encrypted_key = encrypt_with_password( privkey_str, password )
   return EncryptedPrivateKey( salt=base64.b64encode(salt), data=base64.b64encode( encrypted_key ) )


#-------------------------
def decrypt_private_key( encrypted_private_key, password ):
   pkey_str = decrypt_with_password( base64.b64decode( encrypted_private_key.data ), password, base64.b64decode( encrypted_private_key.salt ) )
   if pkey_str is None:
      log.error("Failed to decrypt private key")
   
   return pkey_str

#-------------------------
def load_private_key_from_path( key_path, password, local ):
   encrypted_privkey_str = None
   
   if local:
      encrypted_privkey_str = storage.read_file( key_path, volume=None )
   
   else:
      encrypted_privkey_str = storage.read_file( key_path )
      
   if encrypted_privkey_str is None:
      log.error("Failed to load key from %s" % key_path )
      return None
   
   try:
      encrypted_private_key = storage.json_to_tuple( EncryptedPrivateKey, encrypted_privkey_str )
   except Exception, e: 
      log.error("Failed to unserialize private key")
      return None
   
   privkey_str = decrypt_private_key( encrypted_private_key, password )

   # load this into a usable form
   try:
      privkey = CryptoKey.importKey( privkey_str )
      assert privkey.has_private(), "Not a private key"
   except Exception, e:
      log.error("Failed to load private key")
      log.exception(e)
      return None
   
   return privkey


#-------------------------
def load_private_key( key_name, password, check_volume=True ):
   key_path = make_key_local_path( key_name )
   local = True
   if not storage.path_exists( key_path, volume=None ) and check_volume:
      # load it from the Volume
      key_path = make_key_volume_path( key_name )
      local = False
      
   return load_private_key_from_path( key_path, password, local )

#-------------------------
def load_private_key_from_volume( key_name, password ):
   key_path = make_key_volume_path( key_name )
   return load_private_key_from_path( key_path, password )


#-------------------------
def store_private_key_to_path( key_path, privkey, password, volume ):
   privkey_str = privkey.exportKey()
   
   encrypted_private_key = encrypt_private_key( privkey_str, password )

   try:
      encrypted_privkey_json = storage.tuple_to_json( encrypted_private_key )
   except Exception, e:
      log.error("Failed to serialize encrypted private key")
      return False
   
   rc = storage.write_file( key_path, encrypted_privkey_json, volume=volume )
   
   return rc
   
   
#-------------------------
def store_private_key( key_name, privkey, password ):
   # ensure the path exists...
   global PRIVATE_STORAGE_DIR
   key_path = make_key_local_path( key_name )
   return store_private_key_to_path( key_path, privkey, password, None )


#-------------------------
def store_private_key_to_volume( key_name, privkey, password, num_downloads, duration, volume ):
   # ensure the path exists...
   key_path = make_key_volume_path( key_name )
   
   # TODO: use num_downloads, duration to limit key lifetime on the Volume
   return store_private_key_to_path( key_path, privkey, password, volume )


#-------------------------
def delete_private_key_from_volume( key_name, volume=None ):
   key_path = make_key_volume_path( key_name )
   rc = storage.delete_file( key_path, volume=volume )
   return rc

#-------------------------
def delete_private_key( key_name ):
   key_path = make_key_local_path( key_name )
   rc = storage.delete_file( key_path, volume=None )
   return rc


#-------------------------
def sign_data( privkey_str, data ):
   privkey = CryptoKey.importKey( privkey_str )
   h = HashAlg.new( data )
   signer = CryptoSigner.new(privkey)
   signature = signer.sign( h )
   return signature

#-------------------------   
def verify_data( pubkey_str, data, sig ):
   pubkey = CryptoKey.importKey( pubkey_str )
   h = HashAlg.new( data )
   verifier = CryptoSigner.new(pubkey)
   ret = verifier.verify( h, sig )
   return ret


#-------------------------
def sign_public_key( pubkey_str, syndicate_user_privkey ):
   h = HashAlg.new( pubkey_str )
   signer = CryptoSigner.new(syndicate_user_privkey)
   signature = signer.sign( h )
   return signature


#-------------------------   
def verify_public_key( pubkey, syndicate_user_pubkey ):
   h = HashAlg.new( pubkey.pubkey_str )
   verifier = CryptoSigner.new(syndicate_user_pubkey)
   ret = verifier.verify( h, base64.b64decode(pubkey.signature) )
   return ret
   
   
#-------------------------   
def store_public_key( key_name, pubkey, syndicate_user_privkey ):
   pubkey_str = pubkey.publickey().exportKey()
   
   signature = sign_public_key( pubkey_str, syndicate_user_privkey )
   
   pubkey = SignedPublicKey( signature=base64.b64encode(signature), pubkey_str=pubkey_str )
   
   try:
      pubkey_json = storage.tuple_to_json( pubkey )
   except Exception, e:
      log.error("Failed to serialize signed public key")
      log.exception(e)
      return False
   
   key_path = make_key_local_path( key_name + ".pub" )
   return storage.write_file( key_path, pubkey_json, volume=None )


#-------------------------   
def load_public_key( key_name, syndicate_user_pubkey ):
   key_path = make_key_volume_path( key_name + ".pub" )
   
   pubkey_json = storage.read_file( key_path )
   if pubkey_json is None:
      log.error("Failed to load public key")
      return False
   
   try:
      pubkey = storage.json_to_tuple( SignedPublicKey, pubkey_json )
   except Exception, e:
      log.error("Failed to unserialize signed public key")
      log.exception(e)
      return False
   
   rc = verify_public_key( pubkey, syndicate_user_pubkey )
   if not rc:
      log.error("Failed to verify signed public key")
      return rc
   
   return CryptoKey.importKey( pubkey.pubkey_str )

#-------------------------   
def delete_public_key( key_name ):
   key_path = make_key_local_path( key_name + ".pub" )
   return storage.delete_file( key_path, volume=None )

#-------------------------   
def secure_hash_compare(s1, s2):
    # constant-time compare
    # see http://carlos.bueno.org/2011/10/timing.html
    diff = 0
    for char_a, char_b in izip(s1, s2):
        diff |= ord(char_a) ^ ord(char_b)
    return diff == 0


if __name__ == "__main__":
   import session
   
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   fake_vol = session.do_test_volume( "/tmp/storage-test/volume" )
   singleton.set_volume( fake_vol )
   
   fake_mod = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS )
   assert storage.setup_storage( "/apps/syndicatemail/data", "/tmp/storage-test/local", [fake_mod] ), "setup_storage failed"
   
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
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
-----END RSA PRIVATE KEY-----
""".strip()

   print "----- secure hash compare -----"
   
   assert secure_hash_compare("sniff", "sniff"), "secure compare failed for equality"
   assert not secure_hash_compare("sniff", "yoink"), "secure compare failed for inequality"
   
   print "----- encrypt key -----"
   encrypted_key = encrypt_private_key( privkey_str, "sniff" )
   
   print "----- decrypt key -----"
   decrypted_key = decrypt_private_key( encrypted_key, "sniff" )
   
   assert privkey_str == decrypted_key, "Decrypt(Encrypt( key )) != key\n\nexpected\n%s\n\n\ngot\n%s" % (str(privkey_str), str(decrypted_key))
   
   privkey = CryptoKey.importKey( privkey_str )
   pubkey = CryptoKey.importKey( pubkey_str )
   
   rc = store_private_key( "test.key", privkey, "sniff" )
   if not rc:
      raise Exception("store_private_key failed")
   
   privkey2 = load_private_key( "test.key", "sniff" )
   
   privkey2_str = privkey2.exportKey()
   
   assert privkey_str == privkey2_str, "load(store(key)) != key"
   
   rc = store_public_key( "test2.key", pubkey, privkey )
   if not rc:
      raise Exception("store_public_key failed")
   
   pubkey2_str = load_public_key( "test2.key", pubkey ).exportKey()
   
   assert pubkey2_str == pubkey_str, "load(store(pubkey)) != pubkey"
   
   #delete_private_key( "test.key" )
   