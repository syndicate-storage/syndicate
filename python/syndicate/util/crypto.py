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

from Crypto.PublicKey import RSA as CryptoKey

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger()


import syndicate.client.common.api as api


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
    
    # load the key 
    try:
       k = CryptoKey.importKey( public_key_pem )
    except:
       log.error("Failed to import public key")
       return (-errno.EINVAL, None)
    
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
    rc = api.verify_data( k, data, sig )
    if not rc:
        log.error("Invalid signature")
        return (-errno.EINVAL, None)
    
    return (0, data)
 
 
 #-------------------------------
 def sign_and_serialize_json( private_key_pem, data ):
    """
    Sign and serialize data.  Put it into a JSON object with:
       data (str): base64-encoded data 
       sig (str): base64-encoded signature, with the given private key.
       
    Return the serialized JSON on success.
    Return None on error
    """
    
    signature = api.sign_data( private_key_pem, data )
    if not signature:
       logger.error("Failed to sign data")
       return None

    # create signed credential
    msg = {
       "data":  base64.b64encode( data ),
       "sig":   base64.b64encode( signature )
    }

    return json.dumps( msg )
