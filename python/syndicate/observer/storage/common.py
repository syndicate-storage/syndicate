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

import base64

from Crypto.PublicKey import RSA as CryptoKey

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.syndicate as c_syndicate

#-------------------------------
def encrypt_slice_secret( observer_pkey_pem, slice_secret ):
    """
    Encrypt and serialize the slice secret with the Observer private key
    """
    
    # get the public key
    try:
       observer_pubkey_pem = CryptoKey.importKey( observer_pkey_pem ).publickey().exportKey()
    except Exception, e:
       logger.exception(e)
       logger.error("Failed to derive public key from private key")
       return None 
    
    # encrypt the data 
    rc, sealed_slice_secret = c_syndicate.encrypt_data( observer_pkey_pem, observer_pubkey_pem, slice_secret )
    
    if rc != 0:
       logger.error("Failed to encrypt slice secret")
       return None 
    
    sealed_slice_secret_b64 = base64.b64encode( sealed_slice_secret )
    
    return sealed_slice_secret_b64
    

#-------------------------------
def decrypt_slice_secret( observer_pkey_pem, sealed_slice_secret_b64 ):
    """
    Unserialize and decrypt a slice secret
    """
        
    # get the public key
    try:
       observer_pubkey_pem = CryptoKey.importKey( observer_pkey_pem ).publickey().exportKey()
    except Exception, e:
       logger.exception(e)
       logger.error("Failed to derive public key from private key")
       return None 
    
    sealed_slice_secret = base64.b64decode( sealed_slice_secret_b64 )
    
    # decrypt it 
    rc, slice_secret = c_syndicate.decrypt_data( observer_pubkey_pem, observer_pkey_pem, sealed_slice_secret )
    
    if rc != 0:
       logger.error("Failed to decrypt '%s', rc = %d" % (sealed_slice_secret_b64, rc))
       return None
    
    return slice_secret
 