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

# high-level crypto primitives 

import json
import errno 
import logging
import base64
import traceback

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner
from Crypto.Protocol.KDF import PBKDF2


logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger()

def sign_data( privkey, data ):
   """
   Given a loaded private key and a string of data,
   generate and return a signature over it.
   """
   h = HashAlg.new( data )
   signer = CryptoSigner.new(privkey)
   signature = signer.sign( h )
   return signature 


def generate_key_pair( key_size ):
   """
   Make a key pair
   """
   rng = Random.new().read
   key = CryptoKey.generate(key_size, rng)

   private_key_pem = key.exportKey()
   public_key_pem = key.publickey().exportKey()

   return (public_key_pem, private_key_pem)


def verify_data( pubkey_str, data, signature ):
   """
   Given a public key, data, and a signature, 
   verify that the private key signed it.
   """

   key = CryptoKey.importKey( pubkey_str )
   h = HashAlg.new( data )
   verifier = CryptoSigner.new(key)
   ret = verifier.verify( h, signature )
   return ret
 
   
def hash_data( data ):
   """
   Given a string of data, calculate 
   the SHA256 over it
   """
   h = HashAlg.new()
   h.update( data )
   return h.digest()


#-------------------------------
def _find_missing_and_invalid_fields( required_fields, json_data ):
    """
    Look for missing or invalid fields, and return them.
    """
    
    missing = []
    invalid = []
    for req, types in required_fields.items():
        if req not in json_data.keys():
            missing.append( req )
        
        if type(json_data[req]) not in types:
            invalid.append( req )
            
    return missing, invalid


#-------------------------------
def verify_and_parse_json( public_key_pem, json_text ):
    """
    Parse and validate a JSON structure that has:
      data (str): base64-encoded data
      sig (str): base64-encoded signature
      
    Return (0, data) on success
    Return (nonzero, None) on error 
    """
    
    # verify the presence and types of our required fields 
    required_fields = {
        "data": [str, unicode],
        "sig": [str, unicode]
    }
    
    # parse the json structure 
    try:
        json_data = json.loads( json_text )
    except:
        # can't parse 
        log.error("Failed to parse JSON text")
        return (-errno.EINVAL, None)
     
    # look for missing or invalid fields
    missing, invalid = _find_missing_and_invalid_fields( required_fields, json_data )
    
    if len(missing) > 0:
        log.error("Missing fields: %s" % (", ".join(missing)))
        return (-errno.EINVAL, None)
    
    if len(invalid) > 0:
        log.error("Invalid fields: %s" % (", ".join(invalid)))
        return (-errno.EINVAL, None)
     
     
    # extract fields (they will be base64-encoded)
    data_b64 = json_data["data"]
    sig_b64 = json_data["sig"]
    
    try:
        data = base64.b64decode( data_b64 )
        sig = base64.b64decode( sig_b64 )
    except:
        log.error("Failed to decode message")
        return (-errno.EINVAL, None)
    
    # verify the signature 
    try:
        rc = verify_data( public_key_pem, data, sig )
        return (0, data)
    except Exception, e:
        log.exception(e)
        log.error("Failed to verify data")
        return (-errno.EINVAL, None)
        
 
 
#-------------------------------
def sign_and_serialize_json( private_key, data, toplevel_fields={} ):
    """
    Sign and serialize data.  Put it into a JSON object with:
       data (str): base64-encoded data 
       sig (str): base64-encoded signature, with the given private key.
       
    Any toplevel fields will also be added, but they will not be signed.

    Return the serialized JSON on success.
    Return None on error
    """
    
    signature = sign_data( private_key, data )
    if not signature:
       logger.error("Failed to sign data")
       return None

    # create signed credential
    msg = {
       "data":  base64.b64encode( data ),
       "sig":   base64.b64encode( signature )
    }
    msg.update( toplevel_fields )

    return json.dumps( msg )
