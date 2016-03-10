#!/usr/bin/python

"""
   Copyright 2016 The Trustees of Princeton University

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

# provisioning operations 

import os
import sys
import errno
import random
import logging
import base64
import binascii
import json

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate
import syndicate.util.certs as certs
import syndicate.util.crypto as crypto
import syndicate.util.objects as object_stub
import syndicate.util.client as rpc

from syndicate.util.objects import MissingKeyException, MissingCertException, CertExistsException 

import syndicate.syndicate as c_syndicate

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner


#-------------------------------
def make_gateway_name( gateway_type, volume_name, host ):
    """
    Generate a name for a gateway
    """
    return "%s-%s-%s" % (volume_name, gateway_type, host)    


#-------------------------------
def gateway_default_caps( type_str ):
    """
    Get default capabilities for a gateway by type.

    UG: ALL
    RG: NONE
    AG: ALL
    """
    if type_str == "UG":
        return "ALL"
    elif type_str == "AG":
        return "ALL"
    elif type_str == "RG":
        return "NONE"
    else:
        return "NONE"


#-------------------------------
def gateway_check_consistent( config, gateway, gateway_type, user_email, volume_name, **attrs ):
    """
    Ensure that an existing gateway is consistent with the given fields.
    * We must have a user certificate on-file
    * We must have a volume certificate on-file

    Return a dict with inconsistent fields (empty dict indicates consistent)
    """

    # sanity check 
    ignore = []
    for key in attrs.keys():
        if key not in gateway.keys():
            ignore.append(key)

    user_cert = certs.get_user_cert( config, user_email )
    if user_cert is None:
        raise Exception("No certificate found for user '%s'" % user_email)

    volume_cert = certs.get_volume_cert( config, volume_name )
    if volume_cert is None:
        raise Exception("No certificate found for volume '%s'" % volume_name )

    type_aliases = object_stub.load_gateway_type_aliases( config )
    type_id = type_aliases.get( gateway_type, None )

    if type_id is None:
        raise Exception("Invalid gateway type '%s'" % gateway_type )

    inconsistent = {}

    if not gateway.has_key('volume_id'):
        raise Exception("Missing volume_id:\n%s" % json.dumps(gateway,indent=4,sort_keys=True))

    if not gateway.has_key('owner_id'):
        raise Exception("Missing owner_id:\n%s" % json.dumps(gateway,indent=4,sort_keys=True))

    # validate
    if gateway['volume_id'] != volume_cert.volume_id:
        log.debug("Gateway mismatch: does not match volume")
        inconsistent['volume_id'] = volume_cert.volume_id

    if gateway['owner_id'] != user_cert.user_id:
        log.debug("Gateway mismatch: does not match user")
        inconsistent['owner_id'] = user_cert.user_id

    for key in attrs.keys():
        if key in ignore:
            continue 

        if gateway[key] != attrs[key]:
            # special case: caps
            if key == "caps":
                if object_stub.Gateway.parse_gateway_caps(attrs[key], None)[0] == gateway[key]:
                    # not inconsistent 
                    continue 
            
            inconsistent[key] = attrs[key] 

    return inconsistent


#-------------------------------
def ensure_gateway_exists( client, gateway_type, user_email, volume_name, gateway_name, host, port, **gateway_kw ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    
    This method is idempotent.

    This method can only be called by the volume owner.
    
    Returns the (created, updated, gateway) (as a dict) on success, where 'created' and 'updated' are True/False
    Raises an exception on error.
    * If the gateway cannot be updated to be consistent with the given fields, an exception is thrown.
    """
    
    created = False 
    updated = False

    try:
        gateway = client.read_gateway( gateway_name )
    except Exception, e:
        # transport error 
        log.exception(e)
        raise Exception("Failed to read '%s'" % gateway_name)

    # is it the right gateway?
    if gateway is not None:
        
        inconsistent = gateway_check_consistent( client.config, gateway, gateway_type, user_email, volume_name, gateway_name=gateway_name, host=host, port=port, **gateway_kw )
        if len(inconsistent.keys()) > 0:

            log.debug("Inconsistent: %s" % ",".join(inconsistent.keys()))

            # update the gateway to be consistent, if possible 
            if 'volume_id' in inconsistent.keys() or 'owner_id' in inconsistent.keys():
                # can't do much about this
                raise Exception("Cannot make gateway '%s' consistent" % gateway_name)

            else:
                # update it 
                try:
                    rc = client.update_gateway( gateway_name, **inconsistent )
                except Exception, e:
                    log.exception(e)
                    raise Exception("Failed to update '%s'" % gateway_name)

                gateway.update( inconsistent )
                updated = True
       
    if gateway is None:
       
        # create the gateway 
        try:
            gateway = rpc.ms_rpc( client, "create_gateway", volume=volume_name, email=user_email, type=gateway_type, name=gateway_name, host=host, port=port, **gateway_kw )
            created = True
        except Exception, e:
            # transport, collision, or missing Volume or user
            log.exception(e)
            raise e

    return (created, updated, gateway)


#-------------------------------
def ensure_gateway_absent( client, gateway_name ):
    """
    Ensure that a particular gateway does not exist.
    Return True on success
    return False if we can't connect
    raise exception on error.
    """
    
    try:
        rc = rpc.ms_rpc( client, "delete_gateway", gateway_name )
        return rc
    except MissingCertException, MissingKeyException:
        log.debug("Missing cert or key; assuming deleted")
        return True


#-------------------------------
def user_check_consistent( config, user, user_email, public_key, **attrs ):
    """
    Given an existing user, is it consistent with the data we were given?
    NOTE: public_key must be a PEM-encoded 4096-bit RSA public key.

    Return a dict of inconsistent fields.
    """

    # sanity check 
    missing = []
    for key in attrs.keys():
        if key not in user.keys():
            missing.append(key)

    if len(missing) > 0:
        raise Exception("Missing user fields: %s" % ", ".join(missing))

    user_cert = certs.get_user_cert( config, user_email )
    if user_cert is None:
        raise Exception("No certificate found for user '%s'" % user_email)

    # check consistency
    inconsistent = {}
    if user['public_key'].strip() != public_key.strip():
        log.debug("User public key mismatch")
        inconsistent['public_key'] = public_key

    match = True
    for key in attrs.keys():
        if user[key] != attrs[key]:
            inconsistent[key] = attrs[key]

    return inconsistent


#-------------------------------
def ensure_user_exists( client, user_email, private_key, **user_kw ):
    """
    Given a user email, ensure that the corresponding
    Syndicate user exists with the given fields.

    This method is idempotent.

    This method can only be called by an admin user.

    @private_key must be a PEM-encoded 4096-bit RSA private key
    
    Return the (created, updated, user), where created==True if the user 
    was created and created==False if the user was read.
    Raise an exception on error.
    """
    
    created = False
    updated = False 

    private_key = private_key.strip()
    try:
        private_key = CryptoKey.importKey( private_key ).exportKey()
        public_key = CryptoKey.importKey( private_key ).publickey().exportKey()
    except:
        log.error("Could not import private key")
        raise Exception("Could not import private key")

    try:
        user = rpc.ms_rpc( client, "read_user", user_email )
    except Exception, e:
        # transport error
        log.exception(e)
        raise Exception("Failed to read '%s'" % user_email )

    if user is not None:
        inconsistent = user_check_consistent( client.config, user, user_email, public_key, **user_kw )
        if len(inconsistent) > 0:
            # the only field we can change is the public key 
            if len(inconsistent) == 1 and inconsistent.has_key('public_key'):

                log.debug("Inconsistent: %s" % ",".join(inconsistent.keys()))

                try:
                    rc = rpc.ms_rpc( client, "reset_user", user_email, public_key )
                except Exception, e:
                    log.exception(e)
                    raise Exception("Failed to read '%s'" % user_email)

                user.update( inconsistent )
                updated = True

            else:
                raise Exception("Cannot update user fields: %s" % ", ".join( inconsistent.keys() ))
            

    if user is None:
        # the user does not exist; try to create it
        try:
            assert 'private_key' not in user_kw.keys(), "Two private keys given"
            assert 'public_key' not in user_kw.keys(), "Public and private key given"
            user = rpc.ms_rpc( client, "create_user", user_email, private_key=private_key, **user_kw )
            created = True
        except Exception, e:
            log.exception(e)
            raise Exception("Failed to create user '%s'" % user_email)
    
    return (created, updated, user)


#-------------------------------
def ensure_user_absent( client, user_email ):
    """
    Ensure that a given OpenCloud user's associated Syndicate user record
    has been deleted.
    
    This method is idempotent.

    Returns True on success
    Raises an exception on error
    """

    try:
        rc = rpc.ms_rpc( client, "delete_user", user_email )
        return rc
    except MissingCertException, MissingKeyException:
        log.debug("Missing cert or private key; assuming deleted")
        return True


#-------------------------------
def volume_check_consistent( config, volume, volume_name, description, blocksize, email, **attrs ):
    """
    Given an existing volume, is it consistent with the data we were given?
    Return a dict of inconsistent fields.
    """

    # sanity check 
    missing = []
    for key in attrs.keys():
        if key not in volume.keys():
            missing.append(key)

    if len(missing) > 0:
        raise Exception("Missing volume fields: %s\n%s" % (", ".join(missing), json.dumps(volume,indent=4,sort_keys=True)))

    volume_cert = certs.get_volume_cert( config, volume_name )
    if volume_cert is None:
        raise Exception("No certificate found for volume '%s'" % volume_name)

    user_cert = certs.get_user_cert( config, email )
    if user_cert is None:
        raise Exception("No certificate found for user '%s'" % email)

    owner_cert = certs.get_user_cert( config, volume_cert.owner_email )
    if owner_cert is None:
        raise Exception("No certificate found for volume owner '%s'" % volume_cert.owner_email )

    # check consistency
    inconsistent = {}
    if volume['name'] != volume_name:
        log.debug("Volume mismatch: name")
        inconsistent['name'] = volume_name 

    if volume['volume_id'] != volume_cert.volume_id:
        log.debug("Volume mismatch: volume_id")
        inconsistent['volume_id'] = volume_cert.volume_id

    if volume['description'] != description:
        log.debug("Volume mismatch: description")
        inconsistent['description'] = description 

    if volume['blocksize'] != blocksize:
        log.debug("Volume mismatch: blocksize (%s != %s)" % (volume['blocksize'], blocksize))
        inconsistent['blocksize'] = blocksize

    if volume['owner_id'] != user_cert.user_id:
        log.debug("Volume mismatch: owner ID (%s != %s)" % (volume['owner_id'], user_cert.user_id))
        inconsistent['owner_id'] = user_cert.user_id

    match = True
    for key in attrs.keys():
        if volume[key] != attrs[key]:
            inconsistent[key] = attrs[key]

    return inconsistent


#-------------------------------
def ensure_volume_exists( client, volume_name, description, blocksize, user_email, **attrs ):
    """
    Ensure that a volume exists and is consistent with the given attributes.

    This method is idempotent.

    Returns (created, updated, volume) on success, where created/updated are booleans
    Raises an exception on error
    * i.e. if the user doesn't exist or is over-quota
    """

    created = False
    updated = False

    try:
        volume = rpc.ms_rpc( client, "read_volume", volume_name )
    except Exception, e:
        # transport error 
        log.exception(e)
        raise Exception("Failed to read '%s'" % volume_name)

    # is it the right volume?
    if volume is not None:

        inconsistent = volume_check_consistent( client.config, volume, volume_name, description, blocksize, user_email, **attrs )
        if len(inconsistent.keys()) > 0:

            log.debug("Inconsistent: %s" % ",".join(inconsistent.keys()))

            # update the volume to be consistent, if possible
            if 'volume_id' in inconsistent.keys() or 'owner_id' in inconsistent.keys() or 'blocksize' in inconsistent.keys():
                # can't do much about this
                raise Exception("Cannot make volume '%s' consistent" % volume_name)

            else:
                # update it 
                try:
                    rc = rpc.ms_rpc( client, "update_volume", volume_name, **inconsistent )
                except Exception, e:
                    log.exception(e)
                    raise Exception("Failed to update '%s'" % volume_name)

                volume.update( inconsistent )
                updated = True
       
    if volume is None:
        
        # create the volume
        try:
            volume = rpc.ms_rpc( client, "create_volume", name=volume_name, email=user_email, description=description, blocksize=blocksize, **attrs )
            created = True
        except Exception, e:
            # transport, collision, or missing user
            log.exception(e)
            raise e

    return (created, updated, volume)


#-------------------------------
def ensure_volume_absent( client, volume_name ):
    """
    Ensure that a volume no longer exists.
    
    This method is idempotent.

    Only the volume owner can call it.
    Return True on success
    Raise exception on error
    """

    try:
        rc = rpc.ms_rpc( client, "delete_volume", volume_name )
        return rc
    except MissingCertException, MissingKeyException:
        log.debug("Missing cert or private key; assuming deleted")
        return True


#-------------------------------
if __name__ == "__main__":
    # unit tests
    # TODO

    sys.exit(0)
