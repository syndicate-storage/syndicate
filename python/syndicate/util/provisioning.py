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

# provisioning operations for gateways

import os
import sys
import errno
import logging

logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.api as api
import syndicate.client.common.msconfig as msconfig

import syndicate.syndicate as c_syndicate

from Crypto.Hash import SHA256 as HashAlg

#-------------------------------
def make_gateway_name( namespace, gateway_type, volume_name, host ):
    """
    Generate a name for a gateway
    """
    return "%s-%s-%s-%s" % (namespace, volume_name, gateway_type, host)    


#-------------------------------
def make_gateway_private_key_password( gateway_name, secret ):
    """
    Generate a unique gateway private key password.
    NOTE: its only as secure as the secret; the rest can be guessed by the adversary 
    """
    h = HashAlg.SHA256Hash()
    h.update( "%s-%s" % (gateway_name, secret))
    return h.hexdigest()
 
 
#-------------------------------
def ensure_gateway_exists( client, gateway_type, user_email, volume_name, gateway_name, host, port, key_password, closure=None ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    
    This method is idempotent.
    
    TODO: add the option to store the private key locally, instead of hosting the encrypted key remotely.
    
    Returns the gateway (as a dict) on succes.
    Returns None if we can't connect.
    Raises an exception on error.
    We assume that the Volume (and thus user) already exist...if they don't, its an error.
    """
    
    try:
        gateway = client.read_gateway( gateway_name )
    except Exception, e:
        # transport error 
        log.exception(e)
        raise e

    need_create_gateway = False

    # is it the right gateway?
    if gateway is not None:
        
        # the associated user and volume must exist, and they must match 
        try:
            user = client.read_user( user_email )
        except Exception, e:
            # transport error
            log.exception(e)
            raise e

        try:
            volume = client.read_volume( volume_name )
        except Exception, e:
            # transport error 
            log.exception(e)
            raise e

        # these had better exist...
        if user is None or volume is None:
            raise Exception("Orphaned gateway with the same name as us (%s)" % gateway_name)

        # does this gateway match the user and volume it claims to belong to?
        # NOTE: this doesn't check the closure!
        if msconfig.GATEWAY_TYPE_TO_STR[ gateway["gateway_type"] ] != gateway_type or gateway['owner_id'] != user['owner_id'] or gateway['volume_id'] != volume['volume_id']:
            raise Exception("Gateway exists under a different volume (%s) or user (%s)" % (volume['name'], user['email']))

        # gateway exists, and is owned by the given volume and user 
        return gateway

    else:
        # create the gateway 
        kw = {}
        if closure is not None:
            kw['closure'] = closure
        
        try:
            gateway = client.create_gateway( volume_name, user_email, gateway_type, gateway_name, host, port, encryption_password=key_password, gateway_public_key="MAKE_AND_HOST_GATEWAY_KEY", **kw )
        except Exception, e:
            # transport, collision, or missing Volume or user
            log.exception(e)
            raise e

        else:
            return gateway


#-------------------------------
def ensure_gateway_absent( client, gateway_name ):
    """
    Ensure that a particular gateway does not exist.
    Return True on success
    return False if we can't connect
    raise exception on error.
    """
    
    client.delete_gateway( gateway_name )
    return True


#-------------------------------
def ensure_UG_exists( client, user_email, volume_name, gateway_name, host, port, key_password ):
    """
    Ensure that a particular UG exists.
    This method is idempotent.
    """
    return ensure_gateway_exists( client, "UG", user_email, volume_name, gateway_name, host, port, key_password )


#-------------------------------
def ensure_RG_exists( client, user_email, volume_name, gateway_name, host, port, key_password, closure=None ):
    """
    Ensure that a particular RG exists.
    This method is idempotent.
    """
    return ensure_gateway_exists( client, "RG", user_email, volume_name, gateway_name, host, port, key_password, closure=closure )


#-------------------------------
def ensure_volume_access_right_exists( client, user_email, volume_name, caps, allowed_gateways ):
    """
    Ensure that a particular user has particular access to a particular volume.
    This method is idempotent.
    """
    
    try:
        # compute the allowed_gateways bitmask 
        allowed_gateways_bitmask = 0
        for allowed_gateway_type in allowed_gateways:
           allowed_gateways_bitmask |= (1 << allowed_gateway_type)
           
        rc = client.set_volume_access( user_email, volume_name, allowed_gateways_bitmask, caps )
        assert rc is True, "Failed to allow '%s' to access Volume '%s'" % (user_email, volume_name)
        
    except Exception, e:
        # transport error 
        logger.exception(e)
        return False
    
    return True


#-------------------------------
def ensure_volume_access_right_absent( client, user_email, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    This method is idempotent.
    """
    
    return client.remove_volume_access( user_email, volume_name )
 
