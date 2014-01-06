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

import keys

# -------------------------------------
def create_account( config, email, password, syndicate_user_privkey, num_downloads, duration ):
   # generate a private key, certify its public key, and store it
   pubkey_pem, privkey_pem = keys.generate_key_pair( key_size )
   
   # encrypt and store the private key to the Volume
   privkey = CryptoKey.importKey( privkey_pem )
   rc = keys.store_private_key_to_volume( email, privkey, password, num_downloads, duration )

   if not rc:
      log.error("Failed to store account info")
      raise Exception("Failed to store account info")

   # save locally too
   rc = keys.store_private_key( email, privkey, password )
   if not rc:
      log.error("Failed to store account info locally")
      keys.delete_private_key_from_volume( email )
      raise Exception("Failed to store account info")
   
   # sign and store the public key to the Volume
   pubkey = CryptoKey.importKey( pubkey_pem )
   rc = keys.store_public_key( email, pubkey, syndicate_user_privkey )
   if not rc:
      log.error("Failed to store account info")
      keys.delete_private_key_from_volume( email )
      keys.delete_private_key( email )
      raise Exception("Failed to store account info")
      
   return True


# -------------------------------------
def delete_account( config, email, password ):
   # verify the user
   try:
      do_login( config, email, password )
   except Exception, e:
      raise Exception("Invalid credentials")
   
   keys.delete_private_key_from_volume( email )
   keys.delete_private_key( email )
   return True