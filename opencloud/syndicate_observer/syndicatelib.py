#!/usr/bin/python

"""
Define some common methods for the Syndicate observer.
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
import setproctitle
import threading
import urllib

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
import syndicatelib_config.config as CONFIG

# get the Syndicate modules
import syndicate

import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig
import syndicate.client.common.api as api
import syndicate.util.storage as syndicate_storage
import syndicate.util.watchdog as syndicate_watchdog
import syndicate.util.daemonize as syndicate_daemon
import syndicate.util.crypto as syndicate_crypto
import syndicate.util.provisioning as syndicate_provisioning
import syndicate.syndicate as c_syndicate

# for testing 
TESTING = False
class FakeObject(object):
   def __init__(self):
       pass

if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
   sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
else:
   logger.warning("No OPENCLOUD_PYTHONPATH set.  Assuming Syndicate models are in your PYTHONPATH")

try:
   os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

   # get our models
   import syndicate_storage.models as models

   # get OpenCloud models 
   from core.models import Slice,Sliver
   
   from django.core.exceptions import ObjectDoesNotExist
   from django.db import IntegrityError

except ImportError, ie:
   logger.warning("Failed to import models; some tests will not work")

   # create a fake "models" package that has exactly the members we need for testing.
   models = FakeObject()
   models.Volume = FakeObject()
   models.Volume.CAP_READ_DATA = 1
   models.Volume.CAP_WRITE_DATA = 2
   models.Volume.CAP_HOST_DATA = 4
   
   TESTING = True


#-------------------------------
class SyndicateObserverError( Exception ):
    pass

#-------------------------------
def get_config():
    """
    Return the imported config
    """
    return CONFIG


#-------------------------------
def make_openid_url( email ):
    """
    Generate an OpenID identity URL from an email address.
    """
    return os.path.join( CONFIG.SYNDICATE_OPENID_TRUSTROOT, "id", email )


#-------------------------------
def connect_syndicate( username=CONFIG.SYNDICATE_OPENCLOUD_USER, password=CONFIG.SYNDICATE_OPENCLOUD_PASSWORD, user_pkey_pem=CONFIG.SYNDICATE_OPENCLOUD_PKEY ):
    """
    Connect to the OpenCloud Syndicate SMI, using the OpenCloud user credentials.
    """
    debug = True 
    if hasattr(CONFIG, "DEBUG"):
       debug = CONFIG.DEBUG
       
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
        syn_caps |= (msconfig.GATEWAY_CAP_READ_DATA | msconfig.GATEWAY_CAP_READ_METADATA)
    if cap_write:
        syn_caps |= (msconfig.GATEWAY_CAP_WRITE_DATA | msconfig.GATEWAY_CAP_WRITE_METADATA)
    if cap_host:
        syn_caps |= (msconfig.GATEWAY_CAP_COORDINATE)

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
def make_volume_principal_id( user_email, volume_name ):
    """
    Create a principal id for a Volume owner.
    """
    
    volume_name_safe = urllib.quote( volume_name )
    
    return "volume_%s.%s" % (volume_name_safe, user_email)
 
 
#-------------------------------
def make_slice_principal_id( user_email, slice_name ):
    """
    Create a principal id for a slice owner.
    """
    
    slice_name_safe = urllib.quote( slice_name )
    
    return "slice_%s.%s" % (slice_name, user_email)
 

#-------------------------------
def ensure_principal_exists( user_email, observer_secret, **user_kw ):
    """ 
    Ensure that a Syndicate user exists, as well as its OpenCloud-specific data.
    
    Return (True, (None OR user)) on success.  Returns a user if the user was created.
    Return (False, None) on error
    """
    
    try:
         created, new_user = ensure_user_exists( user_email, **user_kw )
    except Exception, e:
         traceback.print_exc()
         logger.error("Failed to ensure user '%s' exists" % user_email )
         return (False, None)
      
    # if we created a new user, then save its (sealed) credentials to the Django DB
    if created:
         try:
            rc = put_principal_data( user_email, observer_secret, new_user['signing_public_key'], new_user['signing_private_key'] )
            assert rc == True, "Failed to save SyndicatePrincipal"
         except Exception, e:
            traceback.print_exc()
            logger.error("Failed to save private key for principal %s" % (user_email))
            return (False, None)

    return (True, new_user)



#-------------------------------
def ensure_principal_absent( user_email ):
    """
    Ensure that a Syndicate user does not exists, and remove the OpenCloud-specific data.
    
    Return True on success.
    """
    
    ensure_user_absent( user_email )
    delete_principal_data( user_email )
    return True

#-------------------------------
def ensure_volume_exists( user_email, opencloud_volume, user=None ):
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
        vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.cap_read_data, opencloud_volume.cap_write_data, opencloud_volume.cap_host_data )

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

        if user is None or user['email'] != user_email:
            raise Exception("Volume '%s' already exists, but is NOT owned by '%s'" % (opencloud_volume.name, user_email) )

        # we're good!
        return None


#-------------------------------
def ensure_volume_absent( volume_name ):
    """
    Given an OpenCloud volume, ensure that the corresponding Syndicate
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
    vol_default_gateway_caps = opencloud_caps_to_syndicate_caps( opencloud_volume.cap_read_data, opencloud_volume.cap_write_data, opencloud_volume.cap_host_data )

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
        return False

    else:
        return True


#-------------------------------
def ensure_volume_access_right_exists( user_email, volume_name, caps, allowed_gateways=[msconfig.GATEWAY_TYPE_UG] ):
    """
    Ensure that a particular user has particular access to a particular volume.
    Do not try to ensure that the user or volume exist, however!
    """
    client = connect_syndicate()
    return syndicate_provisioning.ensure_volume_access_right_exists( client, user_email, volume_name, caps, allowed_gateways )

#-------------------------------
def ensure_volume_access_right_absent( user_email, volume_name ):
    """
    Ensure that acess to a particular volume is revoked.
    """
    client = connect_syndicate()
    return syndicate_provisioning.ensure_volume_access_right_absent( client, user_email, volume_name )
    

#-------------------------------
def setup_volume_access( user_email, volume_name, caps, RG_port, slice_secret, RG_closure=None ):
    """
    Set up the Volume to allow the slice to provision UGs in it, and to fire up RGs.
       * create the Volume Access Right for the user, so (s)he can create Gateways.
       * provision a single Replica Gateway, serving on localhost.
    """
    client = connect_syndicate()
    
    try:
       rc = ensure_volume_access_right_exists( user_email, volume_name, caps )
       assert rc is True, "Failed to create access right for %s in %s" % (user_email, volume_name)
       
    except Exception, e:
       logger.exception(e)
       return False
    
    RG_name = syndicate_provisioning.make_gateway_name( "OpenCloud", "RG", volume_name, "localhost" )
    RG_key_password = syndicate_provisioning.make_gateway_private_key_password( RG_name, slice_secret )
    
    try:
       rc = syndicate_provisioning.ensure_RG_exists( client, user_email, volume_name, RG_name, "localhost", RG_port, RG_key_password, closure=RG_closure )
    except Exception, e:
       logger.exception(e)
       return False
    
    return True
       

#-------------------------------
def teardown_volume_access( user_email, volume_name ):
    """
    Revoke access to a Volume for a User.
      * remove the user's Volume Access Right
      * remove the use'rs gateways
    """
    client = connect_syndicate()
    
    # block the user from creating more gateways, and delete the gateways
    try:
       rc = client.remove_user_from_volume( user_email, volume_name )
       assert rc is True, "Failed to remove access right for %s in %s" % (user_email, volume_name)
       
    except Exception, e:
       logger.exception(e)
       return False
    
    return True
    

#-------------------------------
def create_sealed_and_signed_blob( private_key_pem, secret, data ):
    """
    Create a sealed and signed message.
    """
    
    # seal it with the password 
    logger.info("Sealing credential data")
    
    rc, sealed_data = c_syndicate.password_seal( data, secret )
    if rc != 0:
       logger.error("Failed to seal data with the secret, rc = %s" % rc)
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

    rc, data = c_syndicate.password_unseal( sealed_data, secret )
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
       "volumes": volume_list
    }
    
    list_data_str = json.dumps( list_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, slice_secret, list_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg
 

#-------------------------------
def create_slice_credential_blob( private_key_pem, slice_name, slice_secret, syndicate_url, volume_name, volume_owner, UG_port, user_pkey_pem ):
    """
    Create a sealed, signed, encoded slice credentials blob.
    """
    
    # create and serialize the data 
    cred_data = {
       "syndicate_url":   syndicate_url,
       "volume_name":     volume_name,
       "volume_owner":    volume_owner,
       "slice_name":      slice_name,
       "slice_UG_port":   UG_port,
       "principal_pkey_pem": user_pkey_pem,
    }
    
    cred_data_str = json.dumps( cred_data )
    
    msg = create_sealed_and_signed_blob( private_key_pem, slice_secret, cred_data_str )
    if msg is None:
       logger.error("Failed to seal volume list")
       return None 
    
    return msg 


#-------------------------------
def put_principal_data( user_email, observer_secret, public_key_pem, private_key_pem ):
    """
    Seal and store the principal's private key into the database, in a SyndicatePrincipal object,
    so the sliver-side Syndicate daemon syndicated.py can get them later.
    Overwrite an existing principal if one exists.
    """
    
    sealed_private_key = create_sealed_and_signed_blob( private_key_pem, observer_secret, private_key_pem )
    if sealed_private_key is None:
        return False

    try:
       sp = models.SyndicatePrincipal( sealed_private_key=sealed_private_key, public_key_pem=public_key_pem, principal_id=user_email )
       sp.save()
    except IntegrityError, e:
       logger.error("WARN: overwriting existing principal %s" % user_email)
       sp.delete()
       sp.save()
    
    return True


#-------------------------------
def delete_principal_data( user_email ):
    """
    Delete an OpenCloud SyndicatePrincipal object.
    """
    
    sp = get_principal_data( user_email )
    if sp is not None:
      sp.delete()
    
    return True


#-------------------------------
def get_principal_data( user_email ):
    """
    Get a SyndicatePrincipal record from the database 
    """
    
    try:
        sp = models.SyndicatePrincipal.objects.get( principal_id=user_email )
        return sp
    except ObjectDoesNotExist:
        logger.error("No SyndicatePrincipal record for %s" % user_email)
        return None
    


#-------------------------------
def get_principal_pkey( user_email, observer_secret ):
    """
    Fetch and unseal the private key of a SyndicatePrincipal.
    """
    
    sp = get_principal_data( user_email )
    if sp is None:
        logger.error("Failed to find private key for principal %s" % user_email )
        return None 
     
    public_key_pem = sp.public_key_pem
    sealed_private_key_pem = sp.sealed_private_key

    # unseal
    private_key_pem = verify_and_unseal_blob(public_key_pem, observer_secret, sealed_private_key_pem)
    if private_key_pem is None:
        logger.error("Failed to unseal private key")

    return private_key_pem


#-------------------------------
def get_private_key_pem( pkey_path ):
    """
    Get a private key from storage, PEM-encoded.
    """
    
    # get the OpenCloud private key 
    observer_pkey = syndicate_storage.read_private_key( pkey_path )
    if observer_pkey is None:
       logger.error("Failed to load Observer private key")
       return None
    
    observer_pkey_pem = observer_pkey.exportKey()
    
    return observer_pkey_pem


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
 

#--------------------------------
def get_slice_secret( observer_pkey_pem, slice_name, slice_fk=None ):
    """
    Get the shared secret for a slice.
    """
    
    ss = None 
    
    # get the sealed slice secret from Django
    try:
       if slice_fk is not None:
          ss = models.SliceSecret.objects.get( slice_id=slice_fk )
       else:
          ss = models.SliceSecret.objects.get( slice_id__name=slice_name )
    except ObjectDoesNotExist, e:
       logger.error("Failed to load slice secret for (%s, %s)" % (slice_fk, slice_name) )
       return None 

    return ss.secret 
 

#-------------------------------
def put_slice_secret( observer_pkey_pem, slice_name, slice_secret, slice_fk=None, opencloud_slice=None ):
    """
    Put the shared secret for a slice, encrypting it first.
    """
    
    ss = None 
    
    if opencloud_slice is None:
       # look up the slice 
       try:
          if slice_fk is None:
             opencloud_slice = models.Slice.objects.get( name=slice_name )
          else:
             opencloud_slice = models.Slice.objects.get( id=slice_fk.id )
       except Exception, e:
          logger.exception(e)
          logger.error("Failed to load slice (%s, %s)" % (slice_fk, slice_name) )
          return False 
    
    ss = models.SliceSecret( slice_id=opencloud_slice, secret=slice_secret )
    
    ss.save()
    
    return True


#-------------------------------
def get_or_create_slice_secret( observer_pkey_pem, slice_name, slice_fk=None ):
   """
   Get a slice secret if it already exists, or generate a slice secret if one does not.
   """
   
   slice_secret = get_slice_secret( observer_pkey_pem, slice_name, slice_fk=slice_fk )
   if slice_secret is None or len(slice_secret) == 0:
      
      # generate a slice secret 
      slice_secret = "".join( random.sample("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 32) )
      
      # store it 
      rc = put_slice_secret( observer_pkey_pem, slice_name, slice_secret, slice_fk=slice_fk )
      
      if not rc:
         raise SyndicateObserverError("Failed to create slice secret for (%s, %s)" % (slice_fk, slice_name))
      
   return slice_secret


#-------------------------------
def generate_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret, UG_port, existing_user=None ):
    """
    Generate and return the set of credentials to be sent off to the slice VMs.
    exisitng_user is a Syndicate user, as a dictionary.
    
    Return None on failure
    """
    
    # get the user's private key 
    logger.info("Obtaining private key for %s" % user_email)
    
    # it might be in the existing_user...
    user_pkey_pem = None
    if existing_user is not None:
       user_pkey_pem = existing_user.get('signing_private_key', None)
       
    # no luck?
    if user_pkey_pem is None:
      try:
         # get it from Django DB
         user_pkey_pem = get_principal_pkey( user_email, observer_secret )
         assert user_pkey_pem is not None, "No private key for %s" % user_email
         
      except:
         traceback.print_exc()
         logger.error("Failed to get private key; cannot generate credentials for %s in %s" % (user_email, volume_name) )
         return None
    
    # generate a credetials blob 
    logger.info("Generating credentials for %s's slice" % (user_email))
    try:
       creds = create_slice_credential_blob( observer_pkey_pem, slice_name, slice_secret, syndicate_url, volume_name, user_email, UG_port, user_pkey_pem )
       assert creds is not None, "Failed to create credentials for %s" % user_email 
    
    except:
       traceback.print_exc()
       logger.error("Failed to generate credentials for %s in %s" % (user_email, volume_name))
       return None
    
    return creds


#-------------------------------
def save_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret, UG_port, existing_user=None ): 
    """
    Create and save a credentials blob to a VolumeSlice.
    Return the creds on success.
    Return None on failure
    """
    
    creds = generate_slice_credentials( observer_pkey_pem, syndicate_url, user_email, volume_name, slice_name, observer_secret, slice_secret, UG_port, existing_user=existing_user )
    ret = None
    
    if creds is not None:
       # save it 
       vs = get_volumeslice( volume_name, slice_name )
       
       if vs is not None:
          vs.credentials_blob = creds
          vs.save()
          
          # success!
          ret = creds
       else:
          logger.error("Failed to look up VolumeSlice(%s, %s)" % (volume_name, slice_name))
       
    else:
       logger.error("Failed to generate credentials for %s, %s" % (volume_name, slice_name))
       
    return ret


#-------------------------------
def get_volumeslice_volume_names( slice_name ):
    """
    Get the list of Volume names from the datastore.
    """
    try:
        all_vs = models.VolumeSlice.objects.filter( slice_id__name = slice_name )
        volume_names = []
        for vs in all_vs:
           volume_names.append( vs.volume_id.name )
           
        return volume_names
    except Exception, e:
        logger.exception(e)
        logger.error("Failed to query datastore for volumes mounted in %s" % slice_name)
        return None 
 

#-------------------------------
def get_volumeslice( volume_name, slice_name ):
    """
    Get a volumeslice record from the datastore.
    """
    try:
        vs = models.VolumeSlice.objects.get( volume_id__name = volume_name, slice_id__name = slice_name )
        return vs
    except Exception, e:
        logger.exception(e)
        logger.error("Failed to query datastore for volumes (mounted in %s)" % (slice_name if (slice_name is not None or len(slice_name) > 0) else "UNKNOWN"))
        return None 


#-------------------------------
def do_push( sliver_hosts, portnum, payload ):
    """
    Push a payload to a list of slivers.
    NOTE: this has to be done in one go, since we can't import grequests
    into the global namespace (without wrecking havoc on the credential server),
    but it has to stick around for the push to work.
    """
    
    global TESTING, CONFIG
    
    from gevent import monkey
    
    if TESTING:
       monkey.patch_all()
    
    else:
       # make gevents runnabale from multiple threads (or Django will complain)
       monkey.patch_all(socket=True, dns=True, time=True, select=True, thread=False, os=True, ssl=True, httplib=False, aggressive=True)
    
    import grequests
    
    # fan-out 
    requests = []
    for sh in sliver_hosts:
      rs = grequests.post( "http://" + sh + ":" + str(portnum), data={"observer_message": payload}, timeout=getattr(CONFIG, "SYNDICATE_HTTP_PUSH_TIMEOUT", 60) )
      requests.append( rs )
      
    # fan-in
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
          
    return True
   

#-------------------------------
def get_slice_hostnames( slice_name ):
   """
   Query the Django DB and get the list of hosts running in a slice.
   """

   openstack_slice = Slice.objects.get( name=slice_name )
   if openstack_slice is None:
       logger.error("No such slice '%s'" % slice_name)
       return None

   hostnames = [s.node.name for s in openstack_slice.slivers.all()]

   return hostnames

   
#-------------------------------
def push_credentials_to_slice( slice_name, payload ):
   """
   Push a credentials payload to the VMs in a slice.
   """
   hostnames = get_slice_hostnames( slice_name )
   return do_push( hostnames, CONFIG.SYNDICATE_SLIVER_PORT, payload )

   
#-------------------------------
class CredentialServerHandler( BaseHTTPServer.BaseHTTPRequestHandler ):
   """
   HTTP server handler that allows syndicated.py instances to poll
   for volume state.
   
   NOTE: this is a fall-back mechanism.  The observer should push new 
   volume state to the slices' slivers.  However, if that fails, the 
   slivers are configured to poll for volume state periodically.  This 
   server allows them to do just that.
   
   Responses:
      GET /<slicename>              -- Reply with the signed sealed list of volume names, encrypted by the slice secret
      GET /<slicename>/<volumename> -- Reply with the signed sealed volume access credentials, encrypted by the slice secret
      
      !!! TEMPORARY !!!
      GET /<slicename>/SYNDICATE_SLICE_SECRET    -- Reply with the slice secret (TEMPORARY)
   
   
   NOTE: We want to limit who can learn which Volumes a slice can access, so we'll seal its slivers'
   credentials with the SliceSecret secret.  The slivers (which have the slice-wide secret) can then decrypt it.
   However, sealing the listing is a time-consuming process (on the order of 10s), so we only want 
   to do it when we have to.  Since *anyone* can ask for the ciphertext of the volume list,
   we will cache the list ciphertext for each slice for a long-ish amount of time, so we don't
   accidentally DDoS this server.  This necessarily means that the sliver might see a stale
   volume listing, but that's okay, since the Observer is eventually consistent anyway.
   """
   
   cached_volumes_json = {}             # map slice_name --> (volume name, timeout)
   cached_volumes_json_lock = threading.Lock()
   
   CACHED_VOLUMES_JSON_LIFETIME = 3600          # one hour
   
   SLICE_SECRET_NAME = "SYNDICATE_SLICE_SECRET"
   
   def parse_request_path( self, path ):
      """
      Parse the URL path into a slice name and (possibly) a volume name or SLICE_SECRET_NAME
      """
      path_parts = path.strip("/").split("/")
      
      if len(path_parts) == 0:
         # invalid 
         return (None, None)
      
      if len(path_parts) > 2:
         # invalid
         return (None, None)
      
      slice_name = path_parts[0]
      if len(slice_name) == 0:
         # empty string is invalid 
         return (None, None)
      
      volume_name = None
      
      if len(path_parts) > 1:
         volume_name = path_parts[1]
         
      return slice_name, volume_name
   
   
   def reply_data( self, data, datatype="application/json" ):
      """
      Give back a 200 response with data.
      """
      self.send_response( 200 )
      self.send_header( "Content-Type", datatype )
      self.send_header( "Content-Length", len(data) )
      self.end_headers()
      
      self.wfile.write( data )
      return 
   
   
   def get_volumes_message( self, private_key_pem, observer_secret, slice_name ):
      """
      Get the json-ized list of volumes this slice is attached to.
      Check the cache, evict stale data if necessary, and on miss, 
      regenerate the slice volume list.
      """
      
      # block the cache.
      # NOTE: don't release the lock until we've generated credentials.
      # Chances are, there's a thundering herd of slivers coming online.
      # Block them all until we've generated their slice's credentials,
      # and then serve them the cached one.
      
      self.cached_volumes_json_lock.acquire()
      
      ret = None
      volume_list_json, cache_timeout = self.cached_volumes_json.get( slice_name, (None, None) )
      
      if (cache_timeout is not None) and cache_timeout < time.time():
         # expired
         volume_list_json = None
      
      if volume_list_json is None:
         # generate a new list and cache it.
         
         volume_names = get_volumeslice_volume_names( slice_name )
         if volume_names is None:
            # nothing to do...
            ret = None
         
         else:
            # get the slice secret 
            slice_secret = get_slice_secret( private_key_pem, slice_name )
            
            if slice_secret is None:
               # no such slice 
               logger.error("No slice secret for %s" % slice_name)
               ret = None
            
            else:
               # seal and sign 
               ret = create_volume_list_blob( private_key_pem, slice_secret, volume_names )
         
         # cache this 
         if ret is not None:
            self.cached_volumes_json[ slice_name ] = (ret, time.time() + self.CACHED_VOLUMES_JSON_LIFETIME )
      
      else:
         # hit the cache
         ret = volume_list_json
      
      self.cached_volumes_json_lock.release()
      
      return ret
      
   
   def do_GET( self ):
      """
      Handle one GET
      """
      slice_name, volume_name = self.parse_request_path( self.path )
      
      # valid request?
      if volume_name is None and slice_name is None:
         self.send_error( 400 )
      
      # slice secret request?
      elif volume_name == self.SLICE_SECRET_NAME and slice_name is not None:
         
         # get the slice secret 
         ret = get_slice_secret( self.server.private_key_pem, slice_name )
         
         if ret is not None:
            self.reply_data( ret )
            return 
         else:
            self.send_error( 404 )
      
      # volume list request?
      elif volume_name is None and slice_name is not None:
         
         # get the list of volumes for this slice
         ret = self.get_volumes_message( self.server.private_key_pem, self.server.observer_secret, slice_name )
         
         if ret is not None:
            self.reply_data( ret )
            return
         else:
            self.send_error( 404 )
      
      # volume credential request?
      elif volume_name is not None and slice_name is not None:
         
         # get the VolumeSlice record
         vs = get_volumeslice( volume_name, slice_name )
         if vs is None:
            # not found
            self.send_error( 404 )
            return
         
         else:
            ret = vs.credentials_blob 
            if ret is not None:
               self.reply_data( vs.credentials_blob )
            else:
               # not generated???
               print ""
               print vs
               print ""
               self.send_error( 503 )
            return
         
      else:
         # shouldn't get here...
         self.send_error( 500 )
         return 
   
   
#-------------------------------
class CredentialServer( BaseHTTPServer.HTTPServer ):
   
   def __init__(self, private_key_pem, observer_secret, server, req_handler ):
      self.private_key_pem = private_key_pem
      self.observer_secret = observer_secret
      BaseHTTPServer.HTTPServer.__init__( self, server, req_handler )


#-------------------------------
def credential_server_spawn( old_exit_status ):
   """
   Start our credential server (i.e. in a separate process, started by the watchdog)
   """
   
   setproctitle.setproctitle( "syndicate-credential-server" )
   
   private_key = syndicate_storage.read_private_key( CONFIG.SYNDICATE_PRIVATE_KEY )
   if private_key is None:
      # exit code 255 will be ignored...
      logger.error("Cannot load private key.  Exiting...")
      sys.exit(255)
   
   logger.info("Starting Syndicate Observer credential server on port %s" % CONFIG.SYNDICATE_HTTP_PORT)
               
   srv = CredentialServer( private_key.exportKey(), CONFIG.SYNDICATE_OPENCLOUD_SECRET, ('', CONFIG.SYNDICATE_HTTP_PORT), CredentialServerHandler)
   srv.serve_forever()


#-------------------------------
def ensure_credential_server_running( foreground=False, run_once=False ):
   """
   Instantiate our credential server and keep it running.
   """
   
   # is the watchdog running?
   pids = syndicate_watchdog.find_by_attrs( "syndicate-credential-server-watchdog", {} )
   if len(pids) > 0:
      # it's running
      return True
   
   if foreground:
      # run in foreground 
      
      if run_once:
         return credential_server_spawn( 0 )
      
      else:
         return syndicate_watchdog.main( credential_server_spawn, respawn_exit_statuses=range(1,254) )
      
   
   # not running, and not foregrounding.  fork a new one
   try:
      watchdog_pid = os.fork()
   except OSError, oe:
      logger.error("Failed to fork, errno = %s" % oe.errno)
      return False
   
   if watchdog_pid != 0:
      
      # child--become watchdog
      setproctitle.setproctitle( "syndicate-credential-server-watchdog" )
      
      if run_once:
         syndicate_daemon.daemonize( lambda: credential_server_spawn(0), logfile_path=getattr(CONFIG, "SYNDICATE_HTTP_LOGFILE", None) )
      
      else:
         syndicate_daemon.daemonize( lambda: syndicate_watchdog.main( credential_server_spawn, respawn_exit_statuses=range(1,254) ), logfile_path=getattr(CONFIG, "SYNDICATE_HTTP_LOGFILE", None) )


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

    print "\nsetup_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name)
    setup_volume_access( fake_user.email, fake_volume.name, 31, 38800, "abcdef" )
    
    print "\nsetup_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name)
    setup_volume_access( fake_user.email, fake_volume.name, 31, 38800, "abcdef" )
    
    print "\nteardown_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name )
    teardown_volume_access( fake_user.email, fake_volume.name )
    
    print "\nteardown_volume_access(%s, %s)\n" % (fake_user.email, fake_volume.name )
    teardown_volume_access( fake_user.email, fake_volume.name )
    
    print "\nensure_volume_absent(%s)\n" % fake_volume.name
    ensure_volume_absent( fake_volume.name )

    print "\nensure_principal_absent(%s)\n" % fake_user.email
    ensure_principal_absent( fake_user.email )
    


#-------------------------------
def ft_volumeslice( slice_name ):
    """
    Functional tests for reading VolumeSlice information
    """
    print "slice: %s" % slice_name
    
    volumes = get_volumeslice_volume_names( slice_name )
    
    print "volumes mounted in slice %s:" % slice_name
    for v in volumes:
       print "   %s:" % v
      
       vs = get_volumeslice( v, slice_name )
       
       print "      %s" % dir(vs)
          

#-------------------------------
def ft_get_slice_hostnames( slice_name ):
   """
   Functional tests for getting slice hostnames
   """
   
   print "Get slice hostnames for %s" % slice_name
   
   hostnames = get_slice_hostnames( slice_name )
   import pprint 
   
   pp = pprint.PrettyPrinter()
   
   pp.pprint( hostnames )


#-------------------------------
def ft_syndicate_principal():
   """
   Functional tests for creating, reading, and deleting SyndicatePrincipals.
   """
   print "generating key pair"
   pubkey_pem, privkey_pem = api.generate_key_pair( 4096 )
   
   user_email = "fakeuser@opencloud.us"
   
   print "saving principal"
   put_principal_data( user_email, "asdf", pubkey_pem, privkey_pem )
   
   print "fetching principal private key"
   saved_privkey_pem = get_principal_pkey( user_email, "asdf" )
   
   assert saved_privkey_pem is not None, "Could not fetch saved private key"
   assert saved_privkey_pem == privkey_pem, "Saved private key does not match actual private key"
   
   print "delete principal"
   
   delete_principal_data( user_email )
   
   print "make sure its deleted..."
   
   saved_privkey_pem = get_principal_pkey( user_email, "asdf" )
   
   assert saved_privkey_pem is None, "Principal key not deleted"
   

#-------------------------------
def ft_credential_server():
   """
   Functional test for the credential server
   """
   ensure_credential_server_running( run_once=True, foreground=True )


#-------------------------------
def ft_seal_and_unseal():
    """
    Functional test for sealing/unsealing data
    """
    print "generating key pair"
    pubkey_pem, privkey_pem = api.generate_key_pair( 4096 )
    
    sealed_buf = create_sealed_and_signed_blob( privkey_pem, "foo", "hello world")
    print "sealed data is:\n\n%s\n\n" % sealed_buf

    buf = verify_and_unseal_blob( pubkey_pem, "foo", sealed_buf )
    print "unsealed data is: \n\n%s\n\n" % buf
    

# run functional tests
if __name__ == "__main__":
    sys.path.append("/opt/planetstack")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

    if len(sys.argv) < 2:
      print "Usage: %s testname [args]" % sys.argv[0]
      
    # call a method starting with ft_, and then pass the rest of argv as its arguments
    testname = sys.argv[1]
    ft_testname = "ft_%s" % testname
    
    test_call = "%s(%s)" % (ft_testname, ",".join(sys.argv[2:]))
   
    print "calling %s" % test_call
   
    rc = eval( test_call )
   
    print "result = %s" % rc
      
    
