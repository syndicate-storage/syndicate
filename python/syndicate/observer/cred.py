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

import os
import sys
import random
import json
import time
import requests
import traceback
import base64
import BaseHTTPServer
import urllib
import binascii

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.client.common.api as api
import syndicate.util.crypto as syndicate_crypto
import syndicate.syndicate as c_syndicate

#-------------------------------
# JSON constants 
OPENCLOUD_JSON                          = "observer_message"

# message for one Volume
OPENCLOUD_VOLUME_NAME                   = "volume_name"
OPENCLOUD_VOLUME_OWNER_ID               = "volume_owner"
OPENCLOUD_SLICE_NAME                    = "slice_name"
OPENCLOUD_SLICE_UG_PORT                 = "slice_UG_port"
OPENCLOUD_PRINCIPAL_PKEY_PEM            = "principal_pkey_pem"
OPENCLOUD_SYNDICATE_URL                 = "syndicate_url"

OPENCLOUD_VOLUME_LIST                   = "volumes"


#-------------------------------
def create_sealed_and_signed_blob( private_key_pem, key, data ):
    """
    Create a sealed and signed message.
    """
    
    rc, sealed_data = c_syndicate.symmetric_seal( data, key )
    if rc != 0:
       logger.error("Failed to seal data with the key, rc = %s" % rc)
       return None
    
    msg = syndicate_crypto.sign_and_serialize_json( private_key_pem, sealed_data )
    if msg is None:
       logger.error("Failed to sign credential")
       return None 
    
    return msg 


#-------------------------------
def verify_and_unseal_blob( public_key_pem, secret, blob_data ):
    """
    verify and unseal a serialized string of JSON
    """

    # verify it 
    rc, sealed_data = syndicate_crypto.verify_and_parse_json( public_key_pem, blob_data )
    if rc != 0:
        logger.error("Failed to verify and parse blob, rc = %s" % rc)
        return None

    logger.info("Unsealing credential data")

    #rc, data = c_syndicate.password_unseal( sealed_data, secret )
    rc, data = c_syndicate.symmetric_unseal( sealed_data, secret )
    if rc != 0:
        logger.error("Failed to unseal blob, rc = %s" % rc )
        return None

    return data


#-------------------------------
def create_volume_list_blob( private_key_pem, slice_secret, volume_list ):
    """
    Create a sealed volume list, signed with the private key.
    """
    list_data = {
       OPENCLOUD_VOLUME_LIST: volume_list
    }
    
    list_data_str = json.dumps( list_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, slice_secret, list_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg
 

#-------------------------------
def create_slice_credential_blob( private_key_pem, slice_name, slice_secret, syndicate_url, volume_name, volume_owner, slice_UG_port, principal_pkey_pem ):
    """
    Create a sealed, signed, encoded slice credentials blob.
    """
    
    # create and serialize the data 
    cred_data = {
       OPENCLOUD_SYNDICATE_URL:   syndicate_url,
       OPENCLOUD_VOLUME_NAME:     volume_name,
       OPENCLOUD_VOLUME_OWNER_ID: volume_owner,
       OPENCLOUD_SLICE_NAME:      slice_name,
       OPENCLOUD_SLICE_UG_PORT:   slice_UG_port,
       OPENCLOUD_PRINCIPAL_PKEY_PEM: principal_pkey_pem,
    }
    
    cred_data_str = json.dumps( cred_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, slice_secret, cred_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg 


#-------------------------------
def find_missing_and_invalid_fields( required_fields, json_data ):
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
def parse_observer_data( data_text ):
    """
    Parse a string of JSON data from the Syndicate OpenCloud Observer.  It should be a JSON structure 
    with some particular fields.
    Return (0, dict) on success
    Return (nonzero, None) on error.
    """
    
    # verify the presence and types of our required fields
    required_fields = {
        OPENCLOUD_VOLUME_NAME: [str, unicode],
        OPENCLOUD_VOLUME_OWNER_ID: [str, unicode],
        OPENCLOUD_SLICE_UG_PORT: [int],
        OPENCLOUD_PRINCIPAL_PKEY_PEM: [str, unicode],
        OPENCLOUD_SYNDICATE_URL: [str, unicode],
    }
    
    # parse the data text
    try:
        data = json.loads( data_text )
    except:
        # can't parse 
        logger.error("Failed to parse JSON data")
        return -errno.EINVAL
    
    # look for missing or invalid fields
    missing, invalid = find_missing_and_invalid_fields( required_fields, data )
    
    if len(missing) > 0:
        logger.error("Missing fields: %s" % (", ".join(missing)))
        return (-errno.EINVAL, None)
    
    if len(invalid) > 0:
        logger.error("Invalid fields: %s" % (", ".join(invalid)))
        return (-errno.EINVAL, None)
    
    # force string 
    for req_field, types in required_fields.items():
       if type(data[req_field]) in [str, unicode]:
          logger.debug("convert %s to str" % req_field)
          data[req_field] = str(data[req_field])
    
    return (0, data)
    

#-------------------------------
def parse_opencloud_volume_list( data_str ):
    """
    Parse a string representing a volume list from OpenCloud.
    """
    try:
       volume_data = json.loads( data_str )
    except Exception, e:
       logger.error("Invalid Volume data")
       return None
    
    # verify it's a { OPENCLOUD_VOLUME_LIST: ["volume_name_1", "volume_name_2", ...] }
    try:
       
       assert volume_data.has_key( OPENCLOUD_VOLUME_LIST ), "missing '%s' field" % OPENCLOUD_VOLUME_LIST
       assert type(volume_data[OPENCLOUD_VOLUME_LIST]) == list, "'%s' is not a list" % OPENCLOUD_VOLUME_LIST
       
       for v in volume_data[OPENCLOUD_VOLUME_LIST]:
          assert type(v) == str or type(v) == unicode, "volume name must be a string"
          
    except Exception, e:
       logger.error("Invalid volume data: %s" % e.message)
       return None
    
    return volume_data[OPENCLOUD_VOLUME_LIST]


#-------------------------------
def ft_seal_and_unseal():
    """
    Functional test for sealing/unsealing data
    """
    import syndicate.observer.core as observer_core
    
    c_syndicate.crypto_init()
    
    print "generating key pair"
    pubkey_pem, privkey_pem = api.generate_key_pair( 4096 )
    
    key = observer_core.generate_symmetric_secret()
    
    sealed_buf = create_sealed_and_signed_blob( privkey_pem, key, "hello world")
    print "sealed data is:\n\n%s\n\n" % sealed_buf

    buf = verify_and_unseal_blob( pubkey_pem, key, sealed_buf )
    print "unsealed data is: \n\n%s\n\n" % buf
    
    c_syndicate.crypto_shutdown()
    
    

# run functional tests
if __name__ == "__main__":
    
    argv = sys.argv[:]
    
    if len(argv) < 2:
      print "Usage: %s testname [args]" % argv[0]
    
    # call a method starting with ft_, and then pass the rest of argv as its arguments
    testname = argv[1]
    ft_testname = "ft_%s" % testname
    
    test_call = "%s(%s)" % (ft_testname, ",".join(argv[2:]))
   
    print "calling %s" % test_call
   
    rc = eval( test_call )
   
    print "result = %s" % rc
    