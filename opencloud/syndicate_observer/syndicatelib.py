#!/usr/bin/python

"""
Define some common methods for the Syndicate observer.
"""
import os
import sys
import random
import json
import requests
import traceback 
import base64 

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner


try:
    from util.logger import Logger, logging
    logger = Logger(level=logging.INFO)

except:
    # for testing
    import logging
    from logging import Logger
    logger = logging.getLogger()
    logger.setLevel( logging.INFO )

# get config package 
import syndicatelib_config.config as CONFIG

# get the Syndicate models 
sys.path.insert(0, os.path.join( os.path.dirname( __file__), ".." ) )

try:
   import syndicate.models as models
except:
   logger.warning("Failed to import models; assming testing environment")
   pass


# get the Syndicate package

#sys.path.insert(0,"/root/syndicate/build/out/python")
#sys.path.insert(0,"/usr/local/lib64/site-packages") 
sys.path.insert(0,"/home/jude/Desktop/research/git/syndicate/build/out/python")

import syndicate
syndicate = reload(syndicate)

import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig
import syndicate.syndicate as c_syndicate

import lib.grequests.grequests as grequests

#-------------------------------
def openid_url( email ):
    """
    Generate an OpenID identity URL from an email address.
    """
    return os.path.join( CONFIG.SYNDICATE_OPENID_TRUSTROOT, "id", email )


#-------------------------------
def registration_password():
    """
    Generate a random user registration password.
    """
    return "".join( random.sample("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 32) )


#-------------------------------
def exc_user_exists( exc ):
    """
    Given an exception, is it due to the user already existing?
    """
    return "already exists" in exc.message


#-------------------------------
def connect_syndicate():
    """
    Connect to the OpenCloud Syndicate SMI
    """
    client = syntool.Client( CONFIG.SYNDICATE_OPENCLOUD_USER, CONFIG.SYNDICATE_SMI_URL,
                             password=CONFIG.SYNDICATE_OPENCLOUD_PASSWORD,
                             debug=True )

    return client


#-------------------------------
def opencloud_caps_to_syndicate_caps( caps ):
    """
    Convert OpenCloud capability bits from the UI into Syndicate's capability bits.
    """
    OPENCLOUD_CAP_READ = 1
    OPENCLOUD_CAP_WRITE = 2
    OPENCLOUD_CAP_HOST = 4

    syn_caps = 0
    
    if (caps & models.Volume.CAP_READ_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_READ_DATA | msconfig.GATEWAY_CAP_READ_METADATA)
    if (caps & models.Volume.CAP_WRITE_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_WRITE_DATA | msconfig.GATEWAY_CAP_WRITE_METADATA)
    if (caps & models.Volume.CAP_HOST_DATA) != 0:
        syn_caps |= (msconfig.GATEWAY_CAP_COORDINATE)

    return syn_caps
    
    
#-------------------------------
def create_and_activate_user( client, user_email ):
    """
    Create, and then activate a Syndicate user account,
    given an OpenCloud user record.
    
    Return the newly-created user, if the user did not exist previously.
    Return None if the user already exists.
    Raise an exception on error.
    """

    user_id = user_email
    user_openid_url = openid_url( user_id )
    user_activate_pw = registration_password()
    try:
        # NOTE: allow for lots of UGs and RGs, since we're going to create at least one for each sliver
        new_user = client.create_user( user_id, user_openid_url, user_activate_pw, is_admin=False, max_UGs=1100, max_RGs=1100 )
    except Exception, e:
        # transport error, or the user already exists (rare, but possible)
        logger.exception(e)
        if not exc_user_exists( e ):
            # not because the user didn't exist already, but due to something more serious
            raise e
        else:
            return None     # user already existed

    else:
        # activate the user
        try:
            activate_rc = client.register_account( user_id, user_activate_pw )
        except Exception, e:
            # transport error, or the user diesn't exist (rare, but possible)
            logger.exception(e)
            raise e
            
        else:
            return new_user     # success!


#-------------------------------
def ensure_user_exists( user_email ):
    """
    Given an OpenCloud user, ensure that the corresponding
    Syndicate user exists.

    Return the new user if a user was created.
    Return None if the user already exists.
    Raise an exception on error.
    """
    
    client = connect_syndicate()
    
    try:
        user = client.read_user( user_email )    
    except Exception, e:
        # transport error
        logger.exception(e)
        raise e

    if user is None:
        # the user does not exist....try to create it
        user = create_and_activate_user( client, user_email )
        return user          # user exists now 
    
    else:
        return None         # user already exists


#-------------------------------
def ensure_user_absent( user_email ):
    """
    Ensure that a given OpenCloud user's associated Syndicate user record
    has been deleted.

    Returns True on success
    Raises an exception on error
    """

    client = connect_syndicate()

    return client.delete_user( user_email )
 

#-------------------------------
def ensure_volume_exists( user_email, opencloud_volume ):
    """
    Given the email address of a user, ensure that the given
    Volume exists and is owned by that user.
    Do not try to ensure that the user exists.
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
        vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.default_gateway_caps )

        try:
            vol_info = client.create_volume( user_email, vol_name, vol_description, vol_blocksize,
                                             private=vol_private,
                                             archive=vol_archive,
                                             active=True,
                                             default_gateway_caps=vol_default_gateway_caps,
                                             store_private_key=False,
                                             metadata_private_key="MAKE_METADATA_KEY" )

        except Exception, e:
            # transport error
            logger.exception(e)
            raise e

        else:
            # successfully created the volume!
            return True

    else:
        # volume already exists.  Verify its owned by this user.

        try:
            user = client.read_user( volume['owner_id'] )
        except Exception, e:
            # transport error, or user doesn't exist (either is unacceptable)
            logger.exception(e)
            raise e

        if user is None or user['email'] != user_email:
            raise Exception("Volume '%s' already exists, but is NOT owned by '%s'" % (opencloud_volume.name, user_email) )

        # we're good!
        return True



#-------------------------------
def ensure_volume_absent( opencloud_volume ):
    """
    Given an OpenCloud volume, ensure that the corresponding Syndicate
    Volume does not exist.
    """

    client = connect_syndicate()

    # this is idempotent, and returns True even if the Volume doesn't exist
    return client.delete_volume( opencloud_volume.name )
    
    
#-------------------------------
def update_volume( opencloud_volume ):
    """
    Update a Syndicate Volume, from an OpenCloud Volume model.
    Fails if the Volume does not exist in Syndicate.
    """

    client = connect_syndicate()

    vol_name = opencloud_volume.name
    vol_description = opencloud_volume.description
    vol_private = opencloud_volume.private
    vol_archive = opencloud_volume.archive
    vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.default_gateway_caps )

    try:
        rc = client.update_volume( vol_name,
                                   description=vol_description,
                                   private=vol_private,
                                   archive=vol_archive,
                                   default_gateway_caps=vol_default_gateway_caps )

        if not rc:
            raise Exception("update_volume(%s) failed!" % vol_name )

    except Exception, e:
        # transort or method error 
        logger.exception(e)
        raise e

    else:
        return True


#-------------------------------
def ensure_volume_access_right_exists( user_email, volume_name, caps ):
    """
    Ensure that a particular user has particular access to a particular volume.
    Do not try to ensure that the user or volume exist, however!
    """
    client = connect_syndicate()
    
    try:
        rc = client.set_volume_access( user_email, volume_name, caps )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e
    
    return True


#-------------------------------
def ensure_volume_access_right_absent( user_email, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    """
    client = connect_syndicate()
    
    return client.remove_volume_access( user_email, volume_name )
    

#-------------------------------
def slice_user_email( slice_name ):
    """
    Generate a user email for a slice, i.e. in order to make the slice the principal that owns the Volume.
    """

    return slice_name + "@opencloud.us"


#-------------------------------
def sign_data( private_key_pem, data_str ):
   """
   Sign a string of data with a PEM-encoded private key.
   """
   try:
      key = CryptoKey.importKey( private_key_pem )
   except Exception, e:
      logging.error("importKey %s", traceback.format_exc() )
      return None
   
   h = HashAlg.new( data_str )
   signer = CryptoSigner.new(key)
   signature = signer.sign( h )
   return signature


#-------------------------------
def create_credential_blob( private_key_pem, shared_secret, syndicate_url, volume_name, volume_owner, volume_password ):
    """
    Create a sealed, signed, encoded credentials blob.
    """
    
    # create and serialize the data 
    cred_data = {
       "syndicate_url":   syndicate_url,
       "volume_name":     volume_name,
       "volume_owner":    volume_owner,
       "volume_password": volume_password,
    }
    
    cred_data_str = json.dumps( cred_data )
    
    # seal it with the password 
    logger.info("Sealing credential data")
    
    rc, sealed_data = c_syndicate.password_seal( cred_data_str, shared_secret )
    if rc != 0:
       logger.error("Failed to seal data with the OpenCloud secret, rc = %s" % rc)
       return None
    
    # sign it 
    signature = sign_data( private_key_pem, sealed_data )
    if signature is None:
       logger.error("Failed to sign sealed data")
       return None
    
    # create signed credential
    creds = {
       "data":  base64.b64encode( sealed_data ),
       "sig":   base64.b64encode( signature )
    }
    
    return json.dumps( creds )
    

#-------------------------------
def store_credentials( volume, sealed_credentials_b64 ):
    """
    Store the sealed Volume credentials into the database,
    so the sliver-side Syndicate daemon syndicated.py can get them later
    (in case pushing them out fails).
    """
    volume.credentials_blob = sealed_credentials_b64
    volume.save( updated_fields=['credentials_blob'] )


#-------------------------------
def get_volume( volume_name ):
    """
    Get a Volume from the datastore.
    """
    try:
        volume = models.Volume.objects.get( name=volume_name )  
    except:
        logger.error("No such volume %s" % volume_name)
        return None 
    
    return volume
 
 
#-------------------------------
def push_begin( sliver_host, payload ):
    """
    Start pushing a message to a sliver host.
    Return the socket
    """
    
    rs = grequests.post( sliver_host, data={"observer_message": payload} )
    return rs
 
 
#-------------------------------
def push_run( requests ):
    """
    Run all pushes concurrently.
    Log results.
    """
    responses = grequests.map( requests )
    
    assert len(responses) == len(requests), "grequests error: len(responses) != len(requests)"
    
    for i in xrange(0,len(requests)):
       resp = responses[i]
       req = requests[i]
       
       if resp is None:
          logger.error("Failed to connect to %s" % (req.url))
          continue 
       
       # verify they all worked 
       if resp.status_code != 200:
          logger.error("Failed to POST to %s, status code = %s" % (resp.url, resp.status_code))
          continue
          
    return 0
   

# for testing 
class FakeObject(object):
    def __init__(self):
        pass


# functional test 
if __name__ == "__main__":
    sys.path.append("/opt/planetstack")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

    fake_user = FakeObject()
    fake_user.email = "fakeuser@opencloud.us"

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email )

    print "\nensure_user_exists(%s)\n" % fake_user.email
    ensure_user_exists( fake_user.email )

    fake_volume = FakeObject()
    fake_volume.name = "fakevolume"
    fake_volume.description = "This is a fake volume, created for funtional testing"
    fake_volume.blocksize = 1024
    fake_volume.default_gateway_caps = 3
    fake_volume.archive = False
    fake_volume.private = True
    
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
    ensure_volume_absent( fake_volume )

    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )

    print "\nensure_user_absent(%s)\n" % fake_user.email
    ensure_user_absent( fake_user.email )

