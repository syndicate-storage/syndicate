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

# get config package 
if "/etc/syndicate" not in sys.path:
   sys.path.append("/etc/syndicate")
   
import syndicatelib_config.config as CONFIG 

import syndicate.ms.syntool as syntool
import syndicate.ms.msconfig as msconfig
import syndicate.ms.api as api
import syndicate.util.storage as syndicate_storage_api
import syndicate.util.crypto as syndicate_crypto
import syndicate.util.provisioning as syndicate_provisioning
import syndicate.syndicate as c_syndicate

import syndicate.observer.cred as observer_cred

#-------------------------------
class SyndicateObserverError( Exception ):
    pass

#-------------------------------
# determine which storage backend to use 
try:
   foo = CONFIG.SYNDICATE_OBSERVER_STORAGE_BACKEND
except:
   raise SyndicateObserverError( "Missing SYNDICATE_OBSERVER_STORAGE_BACKEND from config" )

if CONFIG.SYNDICATE_OBSERVER_STORAGE_BACKEND == "opencloud":
   import syndicate.observer.storage.opencloud as observer_storage
elif CONFIG.SYNDICATE_OBSERVER_STORAGE_BACKEND == "disk":
   import syndicate.observer.storage.disk as observer_storage 
else:
   raise SyndicateObserverError( "Unknown storage backend '%s'" % CONFIG.SYNDICATE_OBSERVER_STORAGE_BACKEND )
   
   
#-------------------------------
def get_config():
    """
    Return the imported config
    """
    return CONFIG
 
#-------------------------------
def get_observer_storage():
    """
    Return the imported observer storage backend.
    """
    return observer_storage

#-------------------------------
def get_syndicate_observer_secret( secret_path ):
    """
    Load the syndicate observer secret from the config 
    """
    try:
       fd = open(secret_path, "r")
       secret_str = fd.read().strip()
       fd.close()
    except IOError, eio:
       logger.error("Failed to load %s, errno = %d" % ( secret_path, eio.errno ) )
       return None 
    except OSError, eos:
       logger.error("Failed to load %s, errno = %d" % ( secret_path, eos.errno ) )
       return None 
    
    # unserialize from hex string to binary string
    try:
       secret_bin = binascii.unhexlify( secret_str )
    except Exception, e:
       logger.exception(e)
       logger.error("Invalid secret")
       return None 
    
    return secret_bin


#-------------------------------
def generate_symmetric_secret():
    """
    Generate a secret suitable for symmetric_seal and symmetric_unseal 
    """
    
    # 256 bits == 32 bytes == 64 hex characters
    secret_hexstr = "".join( map( lambda x: random.sample("0123456789abcdef", 1)[0], [0] * 64 ) )
    secret_bin = binascii.a2b_hex( secret_hexstr )
    return secret_bin 


#-------------------------------
def make_openid_url( email ):
    """
    Generate an OpenID identity URL from an email address.
    """
    return os.path.join( CONFIG.SYNDICATE_OPENID_TRUSTROOT, "id", email )


#-------------------------------
def connect_syndicate( username=CONFIG.SYNDICATE_OPENCLOUD_USER, password=CONFIG.SYNDICATE_OPENCLOUD_PASSWORD, user_pkey_path=CONFIG.SYNDICATE_OPENCLOUD_PKEY ):
    """
    Connect to the OpenCloud Syndicate SMI, using the OpenCloud user credentials.
    """
    debug = True 
    if hasattr(CONFIG, "DEBUG"):
       debug = CONFIG.DEBUG
    
    user_pkey_pem = None
    
    if user_pkey_path is not None:
       user_pkey = syndicate_storage_api.read_private_key( user_pkey_path )
       if user_pkey is None:
          raise Exception("Failed to load private key from %s" % user_pkey_path )
       else:
          user_pkey_pem = user_pkey.exportKey()
       
    client = syntool.Client( username, CONFIG.SYNDICATE_SMI_URL,
                             password=password,
                             user_pkey_pem=user_pkey_pem,
                             debug=debug )

    return client


#-------------------------------
def opencloud_caps_to_syndicate_caps( cap_read, cap_write, cap_host ):
    """
    Convert OpenCloud capability bits from the UI into Syndicate's capability bits.
    """
    syn_caps = 0
    
    if cap_read:
        syn_caps |= (msconfig.SG_CAP_READ_DATA | msconfig.SG_CAP_READ_METADATA)
    if cap_write:
        syn_caps |= (msconfig.SG_CAP_WRITE_DATA | msconfig.SG_CAP_WRITE_METADATA)
    if cap_host:
        syn_caps |= (msconfig.SG_CAP_COORDINATE)

    return syn_caps


#-------------------------------
def ensure_user_exists( user_email, **user_kw ):
    """
    Given an OpenCloud user, ensure that the corresponding
    Syndicate user exists on the MS.  This method does NOT 
    create any OpenCloud-specific data.

    Return the (created, user), where created==True if the user 
    was created and created==False if the user was read.
    Raise an exception on error.
    """
    
    client = connect_syndicate()
    user_openid_url = make_openid_url( user_email )
    
    return syndicate_provisioning.ensure_user_exists( client, user_email, user_openid_url, **user_kw )


#-------------------------------
def ensure_user_absent( user_email ):
    """
    Ensure that a given OpenCloud user's associated Syndicate user record
    has been deleted.  This method does NOT delete any OpenCloud-specific data.

    Returns True on success
    Raises an exception on error
    """

    client = connect_syndicate()

    return client.delete_user( user_email )
 

#-------------------------------
def make_volume_principal_id( principal_id, volume_name ):
    """
    Create a principal id for a Volume owner.
    """
    
    volume_name_safe = urllib.quote( volume_name )
    
    return "volume_%s.%s" % (volume_name_safe, principal_id)
 
 
#-------------------------------
def make_slice_principal_id( principal_id, slice_name ):
    """
    Create a principal id for a slice owner.
    """
    
    slice_name_safe = urllib.quote( slice_name )
    
    return "slice_%s.%s" % (slice_name, principal_id)
 

#-------------------------------
def put_sealed_principal_data( principal_id, observer_secret, public_key_pem, private_key_pem ):
    """
    Put principal data, sealed with the observer secret
    """
    sealed_private_key = observer_cred.create_sealed_and_signed_blob( private_key_pem, observer_secret, private_key_pem )
    if sealed_private_key is None:
        return False

    return observer_storage.put_principal_data( principal_id, public_key_pem, sealed_private_key )

#-------------------------------
def ensure_principal_exists( principal_id, observer_secret, **user_kw ):
    """ 
    Ensure that a Syndicate user exists, as well as its OpenCloud-specific data.
    
    Return (True, (None OR user)) on success.  Returns a user if the user was created.
    Return (False, None) on error
    """
    
    try:
         created, new_user = ensure_user_exists( principal_id, **user_kw )
    except Exception, e:
         traceback.print_exc()
         logger.error("Failed to ensure user '%s' exists" % principal_id )
         return (False, None)
      
    # if we created a new user, then save its (sealed) credentials to the Django DB
    if created:
         try:
            rc = put_sealed_principal_data( principal_id, observer_secret, new_user['public_key'], new_user['private_key'] )
            assert rc == True, "Failed to save SyndicatePrincipal"
         except Exception, e:
            traceback.print_exc()
            logger.error("Failed to save private key for principal %s" % (principal_id))
            return (False, None)

    return (True, new_user)


#-------------------------------
def ensure_principal_absent( principal_id ):
    """
    Ensure that a Syndicate user does not exists, and remove the OpenCloud-specific data.
    
    Return True on success.
    """
    
    ensure_user_absent( principal_id )
    observer_storage.delete_principal_data( principal_id )
    return True

#-------------------------------
def ensure_volume_exists( principal_id, opencloud_volume, user=None ):
    """
    Given the email address of a user, ensure that the given
    Volume exists and is owned by that user.
    Do not try to ensure that the user exists.

    Return the Volume if we created it, or return None if we did not.
    Raise an exception on error.
    """
    client = connect_syndicate()

    try:
        volume = client.read_volume( opencloud_volume.name )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e

    if volume is None:
        # the volume does not exist....try to create it 
        vol_name = opencloud_volume.name
        vol_blocksize = opencloud_volume.blocksize
        vol_description = opencloud_volume.description
        vol_private = opencloud_volume.private
        vol_archive = opencloud_volume.archive 

        try:
            vol_info = client.create_volume( principal_id, vol_name, vol_description, vol_blocksize,
                                             private=vol_private,
                                             archive=vol_archive,
                                             active=True,
                                             store_private_key=False,
                                             metadata_private_key="MAKE_METADATA_KEY" )

        except Exception, e:
            # transport error
            logger.exception(e)
            raise e

        else:
            # successfully created the volume!
            return vol_info

    else:
        
        # volume already exists.  Verify its owned by this user.
        if user is None:
           try:
               user = client.read_user( volume['owner_id'] )
               
           except Exception, e:
               # transport error, or user doesn't exist (either is unacceptable)
               logger.exception(e)
               raise e

        if user is None or not user.has_key('email') or user['email'] != principal_id:
            if not user.has_key('email'):
               raise Exception("Invalid user returned: %s" % user)
            else:
               raise Exception("Volume '%s' already exists, but is NOT owned by '%s'" % (opencloud_volume.name, principal_id) )

        # we're good!
        return None


#-------------------------------
def ensure_volume_absent( volume_name ):
    """
    Given an OpenCloud volume name, ensure that the corresponding Syndicate
    Volume does not exist.
    """

    client = connect_syndicate()
    
    # this is idempotent, and returns True even if the Volume doesn't exist
    return client.delete_volume( volume_name )
    
    
#-------------------------------
def update_volume( opencloud_volume ):
    """
    Update a Syndicate Volume from an OpenCloud Volume model.
    Fails if the Volume does not exist in Syndicate.
    """

    client = connect_syndicate()

    vol_name = opencloud_volume.name
    vol_description = opencloud_volume.description
    vol_private = opencloud_volume.private
    vol_archive = opencloud_volume.archive

    try:
        rc = client.update_volume( vol_name,
                                   description=vol_description,
                                   private=vol_private,
                                   archive=vol_archive )

        if not rc:
            raise Exception("update_volume(%s) failed!" % vol_name )

    except Exception, e:
        # transort or method error 
        logger.exception(e)
        return False

    else:
        return True


#-------------------------------
def ensure_volume_access_right_exists( principal_id, volume_name, caps, allowed_gateways=[] ):
    """
    Ensure that a particular user has particular access to a particular volume.
    Do not try to ensure that the user or volume exist, however!
    """
    client = connect_syndicate()
    return syndicate_provisioning.ensure_volume_access_right_exists( client, principal_id, volume_name, caps, allowed_gateways )

#-------------------------------
def ensure_volume_access_right_absent( principal_id, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    """
    client = connect_syndicate()
    return syndicate_provisioning.ensure_volume_access_right_absent( client, principal_id, volume_name )
    

#-------------------------------
def setup_global_RG( principal_id, volume_name, gateway_name_prefix, slice_secret, RG_port, RG_closure, global_hostname="localhost" ):
    """
    Create/read an RG that will run on each host, on a particular global hostname.
    """
    
    client = connect_syndicate()
   
    RG_name = syndicate_provisioning.make_gateway_name( gateway_name_prefix, "RG", volume_name, global_hostname )
    RG_key_password = syndicate_provisioning.make_gateway_private_key_password( RG_name, slice_secret )

    try:
       rc = syndicate_provisioning.ensure_RG_exists( client, principal_id, volume_name, RG_name, global_hostname, RG_port, RG_key_password, closure=RG_closure )
    except Exception, e:
       logger.exception(e)
       return False 
    
    return True
    

#-------------------------------
def revoke_volume_access( principal_id, volume_name ):
    """
    Revoke access to a Volume for a User.
      * remove the user's Volume Access Right
      * remove the user's gateways
    """
    client = connect_syndicate()
    
    # block the user from creating more gateways, and delete the gateways
    try:
       rc = client.remove_user_from_volume( principal_id, volume_name )
       assert rc is True, "Failed to remove access right for %s in %s" % (principal_id, volume_name)
       
    except Exception, e:
       logger.exception(e)
       return False
    
    return True
 

#-------------------------------
def get_principal_pkey( user_email, observer_secret ):
    """
    Fetch and unseal the private key of a SyndicatePrincipal.
    """
    
    sp = observer_storage.get_principal_data( user_email )
    if sp is None:
        logger.error("Failed to find private key for principal %s" % user_email )
        return None 
     
    public_key_pem = sp.public_key_pem
    sealed_private_key_pem = sp.sealed_private_key

    # unseal
    private_key_pem = observer_cred.verify_and_unseal_blob(public_key_pem, observer_secret, sealed_private_key_pem)
    if private_key_pem is None:
        logger.error("Failed to unseal private key")

    return private_key_pem


#-------------------------------
def get_observer_private_key_pem( pkey_path ):
    """
    Get a private key from storage, PEM-encoded.
    """
    
    # get the OpenCloud private key 
    observer_pkey = syndicate_storage_api.read_private_key( pkey_path )
    if observer_pkey is None:
       logger.error("Failed to load Observer private key")
       return None
    
    observer_pkey_pem = observer_pkey.exportKey()
    
    return observer_pkey_pem

#-------------------------------
def get_or_create_slice_secret( observer_pkey_pem, slice_name, slice_fk=None ):
   """
   Get a slice secret if it already exists, or generate a slice secret if one does not.
   """
   
   slice_secret = observer_storage.get_slice_secret( observer_pkey_pem, slice_name, slice_fk=slice_fk )
   if slice_secret is None or len(slice_secret) == 0:
      
      # generate a slice secret 
      # slice_secret = "".join( random.sample("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 32) )
      slice_secret = generate_symmetric_secret()
      
      # store it 
      rc = observer_storage.put_slice_secret( observer_pkey_pem, slice_name, slice_secret, slice_fk=slice_fk )
      
      if not rc:
         raise SyndicateObserverError("Failed to create slice secret for (%s, %s)" % (slice_fk, slice_name))
      
   return slice_secret


#-------------------------------
def generate_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret, 
                                instantiate_UG=False, run_UG=False, UG_port=0, UG_closure=None,
                                instantiate_RG=False, run_RG=False, RG_port=0, RG_closure=None, RG_global_hostname=None,
                                instantiate_AG=False, run_AG=False, AG_port=0, AG_closure=None, AG_global_hostname=None,
                                gateway_name_prefix="", existing_user=None, user_pkey_pem=None ):
    """
    Generate and return the set of credentials to be sent off to the slice VMs.
    exisitng_user is a Syndicate user, as a dictionary.
    
    Return None on failure
    """
    
    # get the user's private key 
    logger.info("Obtaining private key for %s" % user_email)
    
    # it might be in the existing_user...
    if existing_user is not None:
       user_pkey_pem = existing_user.get('private_key', None)
       
    # no luck?
    if user_pkey_pem is None:
      try:
         # get it from storage
         user_pkey_pem = get_principal_pkey( user_email, observer_secret )
         assert user_pkey_pem is not None, "No private key for %s" % user_email
         
      except:
         traceback.print_exc()
         logger.error("Failed to get private key; cannot generate credentials for %s in %s" % (user_email, volume_name) )
         return None
    
    # generate a credetials blob 
    logger.info("Generating credentials for %s's slice" % (user_email))
    try:
       creds = observer_cred.create_slice_credential_blob( observer_pkey_pem, slice_name, slice_secret, syndicate_url, volume_name, user_email, user_pkey_pem,
                                                           instantiate_UG=instantiate_UG, run_UG=run_UG, UG_port=UG_port, UG_closure=UG_closure,
                                                           instantiate_RG=instantiate_RG, run_RG=run_RG, RG_port=RG_port, RG_closure=RG_closure, RG_global_hostname=RG_global_hostname,
                                                           instantiate_AG=instantiate_AG, run_AG=run_AG, AG_port=AG_port, AG_closure=AG_closure, AG_global_hostname=AG_global_hostname,
                                                           gateway_name_prefix=gateway_name_prefix )
       
       assert creds is not None, "Failed to create credentials for %s" % user_email 
    
    except:
       traceback.print_exc()
       logger.error("Failed to generate credentials for %s in %s" % (user_email, volume_name))
       return None
    
    return creds


#-------------------------------
def save_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret, 
                            instantiate_UG=False, run_UG=False, UG_port=0, UG_closure=None,
                            instantiate_RG=False, run_RG=False, RG_port=0, RG_closure=None, RG_global_hostname=None,
                            instantiate_AG=False, run_AG=False, AG_port=0, AG_closure=None, AG_global_hostname=None,
                            gateway_name_prefix="", existing_user=None, user_pkey_pem=None ): 
    """
    Create and save a credentials blob to a VolumeSlice.
    It will contain directives to be sent to the automount daemons to provision and instantiate gateways.
    Return the creds on success.
    Return None on failure
    """
    
    creds = generate_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret,
                                        instantiate_UG=instantiate_UG, run_UG=run_UG, UG_port=UG_port, UG_closure=UG_closure,
                                        instantiate_RG=instantiate_RG, run_RG=run_RG, RG_port=RG_port, RG_closure=RG_closure, RG_global_hostname=RG_global_hostname,
                                        instantiate_AG=instantiate_AG, run_AG=run_AG, AG_port=AG_port, AG_closure=AG_closure, AG_global_hostname=AG_global_hostname,
                                        gateway_name_prefix=gateway_name_prefix, existing_user=existing_user, user_pkey_pem=user_pkey_pem )
    ret = None
    
    if creds is not None:
       # save it 
       rc = observer_storage.put_volumeslice_creds( volume_name, slice_name, creds )
       if rc:
          # success! 
          ret = creds 
          
       else:
          logger.error("Failed to update VolumeSlice(%s, %s)" % (volume_name, slice_name))
       
    else:
       logger.error("Failed to generate credentials for %s, %s" % (volume_name, slice_name))
       
    return ret


#-------------------------------
def revoke_slice_credentials( volume_name, slice_name ):
   """
   Remove the binding between a volume and a slice.
   """
   
   return observer_storage.delete_volumeslice( volume_name, slice_name )


#-------------------------------
def get_volume_slice_names( volume_name ):
   """
   Get the list of the names of slice on which the given volume is mounted.
   """
   return observer_storage.get_volumeslice_slice_names( volume_name )

#-------------------------------
# Begin functional tests.
# Any method starting with ft_ is a functional test.
#-------------------------------
  
#-------------------------------
def ft_syndicate_access():
    """
    Functional tests for ensuring objects exist and don't exist in Syndicate.
    """
    
    fake_user = FakeObject()
    fake_user.email = "fakeuser@opencloud.us"

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email, is_admin=False, max_UGs=1100, max_RGs=1 )

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email, is_admin=False, max_UGs=1100, max_RGs=1 )

    fake_volume = FakeObject()
    fake_volume.name = "fakevolume"
    fake_volume.description = "This is a fake volume, created for funtional testing"
    fake_volume.blocksize = 1024
    fake_volume.cap_read_data = True 
    fake_volume.cap_write_data = True 
    fake_volume.cap_host_data = False
    fake_volume.archive = False
    fake_volume.private = True
    
    # test idempotency
    print "\nensure_volume_exists(%s)\n" % fake_volume.name
    ensure_volume_exists( fake_user.email, fake_volume )

    print "\nensure_volume_exists(%s)\n" % fake_volume.name
    ensure_volume_exists( fake_user.email, fake_volume )
    
    print "\nensure_volume_access_right_exists(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nensure_volume_access_right_exists(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nensure_volume_access_right_absent(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_absent( fake_user.email, fake_volume.name )
    
    print "\nensure_volume_access_right_absent(%s,%s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_absent( fake_user.email, fake_volume.name )
 
    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume.name )

    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume.name )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )
    
    
    
    
    print "\nensure_principal_exists(%s)\n" % fake_user.email
    ensure_principal_exists( fake_user.email, "asdf", is_admin=False, max_UGs=1100, max_RGs=1 )
    
    print "\nensure_principal_exists(%s)\n" % fake_user.email
    ensure_principal_exists( fake_user.email, "asdf", is_admin=False, max_UGs=1100, max_RGs=1 )

    print "\nensure_volume_exists(%s)\n" % fake_volume.name
    ensure_volume_exists( fake_user.email, fake_volume )

    print "\nensure_volume_access_right_exists(%s, %s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nensure_volume_access_right_exists(%s, %s)\n" % (fake_user.email, fake_volume.name)
    ensure_volume_access_right_exists( fake_user.email, fake_volume.name, 31 )
    
    print "\nrevoke_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name )
    revoke_volume_access( fake_user.email, fake_volume.name )
    
    print "\nrevoke_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name )
    revoke_volume_access( fake_user.email, fake_volume.name )
    
    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume.name )

    print "\nensure_principal_absent(%s)\n" % fake_user.email
    ensure_principal_absent( fake_user.email )
    


#-------------------------------
def ft_syndicate_principal():
   """
   Functional tests for creating, reading, and deleting SyndicatePrincipals.
   """
   
   c_syndicate.crypto_init()
   
   print "generating key pair"
   pubkey_pem, privkey_pem = api.generate_key_pair( 4096 )
   
   user_email = "fakeuser@opencloud.us"
   
   print "saving principal"
   
   key = generate_symmetric_secret()
   
   put_sealed_principal_data( user_email, key, pubkey_pem, privkey_pem )
   
   print "fetching principal private key"
   saved_privkey_pem = get_principal_pkey( user_email, key )
   
   assert saved_privkey_pem is not None, "Could not fetch saved private key"
   assert saved_privkey_pem == privkey_pem, "Saved private key does not match actual private key"
   
   print "delete principal"
   
   observer_storage.delete_principal_data( user_email )
   
   print "make sure its deleted..."
   
   saved_privkey_pem = get_principal_pkey( user_email, key )
   
   assert saved_privkey_pem is None, "Principal key not deleted"
   
   c_syndicate.crypto_shutdown()
   

# run functional tests
if __name__ == "__main__":
    sys.path.append("/opt/planetstack")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

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
      
    
