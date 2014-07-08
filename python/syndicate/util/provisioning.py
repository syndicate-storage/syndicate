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
def make_registration_password():
    """
    Generate a random user registration password.
    """
    return "".join( random.sample("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 32) )
 
#-------------------------------
def ensure_gateway_exists( client, gateway_type, user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    
    This method is idempotent.
    
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
        if 'encryption_key_password' not in gateway_kw:
           gateway_kw['encryption_key_password'] = key_password
        
        if 'gateway_public_key' not in gateway_kw:
           gateway_kw['gateway_public_key'] = "MAKE_AND_HOST_GATEWAY_KEY"
           
        try:
            gateway = client.create_gateway( volume_name, user_email, gateway_type, gateway_name, host, port, **gateway_kw )
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
def ensure_UG_exists( client, user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw ):
    """
    Ensure that a particular UG exists.
    This method is idempotent.
    """
    return ensure_gateway_exists( client, "UG", user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw )


#-------------------------------
def ensure_RG_exists( client, user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw ):
    """
    Ensure that a particular RG exists.
    This method is idempotent.
    """
    return ensure_gateway_exists( client, "RG", user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw )


#-------------------------------
def ensure_AG_exists( client, user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw ):
    """
    Ensure that a particular AG exists.
    This method is idempotent.
    """
    return ensure_gateway_exists( client, "AG", user_email, volume_name, gateway_name, host, port, key_password, **gateway_kw )


#-------------------------------
def _create_and_activate_user( client, user_email, user_openid_url, user_activate_pw, **user_kw ):
    """
    Create, and then activate a Syndicate user account,
    given an OpenCloud user record.
    
    Return the newly-created user, if the user did not exist previously.
    Return None if the user already exists.
    Raise an exception on error.
    """

    try:
        # NOTE: allow for lots of UGs and RGs, since we're going to create at least one for each sliver
        new_user = client.create_user( user_email, user_openid_url, user_activate_pw, **user_kw )
    except Exception, e:
        # transport error, or the user already exists (rare, but possible)
        logger.exception(e)
        if not exc_user_exists( e ):
            # not because the user didn't exist already, but due to something more serious
            raise e
        else:
            return None     # user already existed

    if new_user is None:
        # the method itself failed
        raise Exception("Creating %s failed" % user_email)

    else:
        # activate the user.
        # first, generate a keypar 
        logger.info("Generating %s-bit key pair for %s" % (msconfig.OBJECT_KEY_SIZE, user_email))
        pubkey_pem, privkey_pem = api.generate_key_pair( msconfig.OBJECT_KEY_SIZE )
        
        # then, activate the account with the keypair
        try:
            activate_rc = client.register_account( user_email, user_activate_pw, signing_public_key=pubkey_pem )
        except Exception, e:
            # transport error, or the user diesn't exist (rare, but possible)
            logger.exception(e)
            raise e
            
        else:
            # give back the keys to the caller
            new_user['signing_public_key'] = pubkey_pem
            new_user['signing_private_key'] = privkey_pem
            return new_user     # success!


#-------------------------------
def ensure_user_exists( client, user_email, user_openid_url, **user_kw ):
    """
    Given an OpenCloud user, ensure that the corresponding
    Syndicate user exists.

    This method is idempotent 
    
    Return the (created, user), where created==True if the user 
    was created and created==False if the user was read.
    Raise an exception on error.
    """
    
    try:
        user = client.read_user( user_email )    
    except Exception, e:
        # transport error
        logger.exception(e)
        raise e

    if user is None:
        # the user does not exist....try to create it
        user_activate_pw = make_registration_password()
        user = _create_and_activate_user( client, user_email, user_openid_url, user_activate_pw, **user_kw )
        return (True, user)          # user exists now 
    
    else:
        return (False, user)         # user already exists


#-------------------------------
def ensure_user_absent( client, user_email ):
    """
    Ensure that a given OpenCloud user's associated Syndicate user record
    has been deleted.
    
    This method is idempotent.

    Returns True on success
    Raises an exception on error
    """

    return client.delete_user( user_email )


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
        assert rc is True, "Failed to allow '%s' to access Volume '%s' (rc = %s)" % (user_email, volume_name, rc)
        
    except Exception, e:
        # transport error 
        log.exception(e)
        return False
    
    return True


#-------------------------------
def ensure_volume_access_right_absent( client, user_email, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    This method is idempotent.
    """
    
    return client.remove_volume_access( user_email, volume_name )
 
