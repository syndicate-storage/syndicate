#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

"""
Front-end API
"""


from msconfig import *

try:
   import logging
except:
   import log 
   logging = log

try:
   import storage.storage as storage
except Exception, e:
   logging.debug("Using storage stub")
   from storage_stub import StorageStub as storage
   
try:
   from MS.volume import Volume, VolumeAccessRequest
   from MS.user import SyndicateUser
   from MS.gateway import Gateway
except Exception, e:
   logging.debug("Using object stub")
   from object_stub import *

try:
   from MS.auth import *
except Exception, e:
   logging.debug("Using auth stub")
   from auth_stub import *


# ----------------------------------
SIGNING_KEY_DEFAULT_USER_ID = 1         # use the client's given user ID, not one derived from the argument list

KEY_TYPES = [
   "gateway",
   "user",
   "volume"
]

KEY_TYPE_TO_CLS = {
   "user": SyndicateUser,
   "volume": Volume,
   "gateway": Gateway
}


# ----------------------------------
# The User API.

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="user", verify_key_id="email", trust_key_type="user", trust_key_id="email" )
@CreateAPIGuard( SyndicateUser, admin_only=True )
def create_user( email, openid_url, signing_public_key, **attrs ):
   """
   Create a user.
   
   Requred arguments:
      email (str): 
         The email address of the new user.  It will
         serve as the user's identifier, so it must 
         be unique.
                   
      openid_url (str):
         The URL to the OpenID provider to use to 
         authenticate this user using password
         authentication.
      
      signing_public_key (str):
         The PEM-encoded public key that the MS will
         use to authenticate a client program that wishes
         to access this user.  The client program must use 
         the corresponding private key (the "signing key")
         to sign requests to the MS, in order to prove to
         the MS that the program is acting on behalf of the
         owner of this user account.  Currently, this must be 
         a 4096-bit RSA key.
         
   Optional keyword arguments:
      max_volumes=int: (default: 10)
         Maximum number of Volumes this user may own.
         -1 means infinite.
      
      max_UGs=int: (default: 10)
         Maximum number of User Gateways this user may own.
         -1 means infinite.
      
      max_RGs=int: (default: 10)
         Maximum number of Replica Gateways this user may own.
         -1 means infinite.
      
      is_admin=bool: (default: False)
         Whether or not this user will be a Syndicate admin.
      
   Returns:
      A SyndicateUser object on success, or an exception
      on error.
      
   Authorization:
      Only an administrator can create new users.
   """
   
   return storage.create_user( email, openid_url, signing_public_key, **attrs )


@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], verify_key_type="user", verify_key_id="email" )
@ReadAPIGuard( SyndicateUser )
def read_user( email ):
   """
   Read a user.
   
   Required arguments:
      email (str):
         The email address of the desired user.
   
   Returns:
      A SyndicateUser object on success, or an exception 
      if the user does not exist or if the caller is not 
      allowed to read this user's account.
   
   Authorization:
      The administrator can read any user account.
      A user can only read itself.
   """
   return storage.read_user( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"] )
@UpdateAPIGuard( SyndicateUser )
def update_user( email, **attrs ):
   """
   Update a user.
   
   Required arguments:
      email (str):
         The email address of the desired user.
      
   Optional keyword arguments:
      max_volumes=int:
         Maximum number of Volumes this user may own.
         -1 means infinite.
      
      max_UGs=int:
         Maximum number of User Gateways this user may own.
         -1 means infinite.
      
      max_RGs=int:
         Maximum number of Replica Gateways this user may own.
         -1 means infinite.
      
      is_admin=bool:
         Whether or not this user will be a Syndicate admin.
      
      openid_url=str:
         The URL of the OpenID provider to use to 
         authenticate this user using password 
         authentication.
         
   Returns:
      True on success, or an exception if the user
      does not exist or the caller is not authorized
      to write one or more of the given fields.
   
   Authorization:
      An administrator can update any user.
      A user can only change its openid_url.
   """
      
   return storage.update_user( email, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], revoke_key_type="user", revoke_key_id="email" )
@DeleteAPIGuard( SyndicateUser )
def delete_user( email ):
   """
   Delete a user.
   
   Required arguments:
      email (str):
         The email address of the desired user.
   
   Returns:
      True on success, or an exception if the 
      user does not exist or the caller is 
      not authorized to delete the user.
   
   Authorization:
      An administrator can delete any user.
      A user can only delete itself.
   """
   return storage.delete_user( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="user", verify_key_id="email" )
@ListAPIGuard( SyndicateUser, admin_only=True )
def list_users( query_attrs ):
   """
   List users.
   
   Required arguments:
      query_attrs (dict):
         The fields to query on.  Each item must be in
         the form of
         
            'SyndicateUser.${attr} ${op}': ${value}
         
         where ${attr} is a user attribute, ${op}
         is ==, !=, >, >=, <, <=, or IN, and ${value}
         is the value to compare the attribute against.
   
   Returns:
      A list of users that satisfy the query, or an 
      exception if the query was malformed or could 
      not be executed by the MS's underlying datastore.
      The exception may be specific to the datastore.
   
   Authorization:
      Only an administrator may call this method.
   """
   return storage.list_users( query_attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], revoke_key_type="user", revoke_key_id="email" )
@UpdateAPIGuard( SyndicateUser )
def set_user_public_signing_key( email, signing_public_key, **attrs ):
   """
   Set a user's public signing key.
   
   Required arguments:
      email (str):
         The email of the desired user. 
      
      signing_public_key (str):
         The PEM-encoded public key that the MS will
         use to authenticate a client program that wishes
         to access this user.  The client program must use 
         the corresponding private key (the "signing key")
         to sign requests to the MS, in order to prove to
         the MS that the program is acting on behalf of the
         owner of this user account.  Currently, this must be 
         a 4096-bit RSA key.
   
   Returns:
      True on success, or an exception if the user does 
      not exist or the caller is not authorized to set 
      the API field.
   
   Authorization:
      An administrator can set any user's public signing key.
      A user can only set its own public signing key.
   """
   return storage.set_user_public_signing_key( email, signing_public_key, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@ListAPIGuard( SyndicateUser, pass_caller_user="caller_user" )
def list_volume_user_ids( volume_name_or_id, **attrs ):
   """
   List the emails of the users that can access data
   in a given volume.  That is, which users have 
   registered User Gateways that can access 
   the volume's metadata.
   
   Required arguments:
      volume_name (str):
         The name of the volume to query.
   
   Returns:
      The list of emails of users that can run User Gateways
      within this volume.
   
   Authorization:
      An administrator can list any Volume's user IDs.
      A user must own the Volume.
   """
   return storage.list_volume_user_ids( volume_name_or_id, **attrs )
      


# ----------------------------------
# The Volume API.

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], verify_key_type="volume", verify_key_id="name", trust_key_type="volume", trust_key_id="name" )
@CreateAPIGuard( Volume, pass_caller_user="caller_user" )
def create_volume( email, name, description, blocksize, signing_public_key, **attrs ):
   """
   Create a Volume.  It will be owned by the calling user.
   
   Required arguments:
      email (str):
         The email of the user to own this Volume.
      
      name (str):
         The name of this volume.  It must be unique.
         It is recommend that it be human-readable, so other 
         users can request to join it.
      
      description (str):
         A human-readable description of the Volume's contents.
         
      blocksize (int)
         The size of a block in this Volume.  Each block will
         be cached as an HTTP object in the underlying Web 
         caches, so it is probably best to pick block sizes between 
         4KB and 1MB or so.
      
      signing_public_key (str)
         The PEM-encoded public key that the MS will
         use to authenticate a client program that wishes
         to read this volume.  The client program must use 
         the corresponding private key (the "signing key")
         to sign requests to the MS, in order to prove to
         the MS that the program is acting on behalf of the
         owner of this volume.  Currently, this must be 
         a 4096-bit RSA key.
         
   Optional keyword arguments:
      private=bool (default: True)
         If True, this volume will not be searchable, and users
         will not be able to request access to it.  This value
         is True by default.
         
      metadata_private_key=str (default: None)
         The PEM-encoded private key the Volume will use to sign 
         metadata served to User Gateways.  It must be a 4096-bit
         RSA key.  It will be automatically generated if not given.
      
      archive=bool (default: False)
         If True, only an Acquisition Gateway owned by the given 
         user may write metadata to this Volume.  It will be read-
         only to every other Gateway.
      
      default_gateway_caps=int
         Default capability bits for User Gateways when they are 
         added to this Volume.  By default, User Gateways are 
         given read-only access to data and metadata.
         
         Valid capability bits are:
            GATEWAY_CAP_READ_METADATA
            GATEWAY_CAP_WRITE_METADATA
            GATEWAY_CAP_READ_DATA
            GATEWAY_CAP_WRITE_DATA
            GATEWAY_CAP_COORDINATE
      
   Returns:
      On success, this method returns a Volume.  On failure, it
      raises an exception.
      
   Authorization:
      An administrator can create an unlimited number of volumes.
      A user can only create as many as allowed by its max_volumes value.
   """
   return storage.create_volume( email, name, description, blocksize, signing_public_key, **attrs )


@Authenticate( object_authenticator=Volume.Authenticate, object_response_signer=Volume.Sign,
               signing_key_types=["user", "volume"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID, "volume_name_or_id"],
               verify_key_type="volume", verify_key_id="name" )
@ReadAPIGuard( Volume )
def read_volume( volume_name_or_id ):
   return storage.read_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@UpdateAPIGuard( Volume )
def update_volume( volume_name_or_id, **attrs ):
   return storage.update_volume( volume_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="volume", revoke_key_id="volume_name_or_id" )
@DeleteAPIGuard( Volume )
def delete_volume( volume_name_or_id ):
   return storage.delete_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume, admin_only=True )
def list_volumes( query_attrs ):
   return storage.list_volumes( query_attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume )
def list_user_volumes( email ):   
   return storage.list_user_volumes( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume )
def list_public_volumes():
   return storage.list_public_volumes()

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume )
def list_archive_volumes():
   return storage.list_archive_volumes()

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="volume", revoke_key_id="volume_name_or_id" )
@UpdateAPIGuard( Volume, pass_caller_user="caller_user" )
def set_volume_public_signing_key( volume_name_or_id, signing_public_key, **attrs ):   
   return storage.set_volume_public_signing_key( volume_name_or_id, signing_public_key, **attrs )


# ----------------------------------
# The Volume Access Request API.

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"])
@BindAPIGuard( SyndicateUser, Volume, caller_owns_target=False )
def remove_volume_access_request( email, volume_name_or_id ):
   return storage.remove_volume_access_request( email, volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"])
@BindAPIGuard( SyndicateUser, Volume, caller_owns_target=False )
def request_volume_access( email, volume_name_or_id, caps, message ):
   return storage.request_volume_access( email, volume_name_or_id, caps, message )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID])
@ListAPIGuard( VolumeAccessRequest, pass_caller_user="caller_user" )
def list_volume_access_requests( volume_name_or_id, **attrs ):
   return storage.list_volume_access_requests( volume_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID])
@ListAPIGuard( VolumeAccessRequest )
def list_user_access_requests( email ):
   return storage.list_user_access_requests( email )


# ----------------------------------
# The Gateway API.

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], verify_key_type="gateway", verify_key_id="name", trust_key_type="gateway", trust_key_id="name" )
@CreateAPIGuard( Gateway )
def create_gateway( volume_name_or_id, email, gateway_type, signing_public_key, **attrs ):
   return storage.create_gateway( volume_name_or_id, email, gateway_type, signing_public_key, **attrs )

@Authenticate( object_authenticator=Gateway.Authenticate, object_response_signer=Gateway.Sign,
               signing_key_types=["user", "gateway"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID, "g_name_or_id"],
               verify_key_type="gateway", verify_key_id="name" )
@ReadAPIGuard( Gateway )
def read_gateway( g_name_or_id ):
   return storage.read_gateway( g_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@UpdateAPIGuard( Gateway )
def update_gateway( g_name_or_id, **attrs ):
   return storage.update_gateway( g_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@BindAPIGuard( Volume, Gateway, caller_owns_target=False )
def set_gateway_caps( volume_name_or_id, g_name_or_id, caps ):
   return storage.set_gateway_caps( g_name_or_id, caps )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="gateway", revoke_key_id="g_name_or_id" )
@DeleteAPIGuard( Gateway )
def delete_gateway( g_name_or_id ):
   return storage.delete_gateway( g_name_or_id )  

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, admin_only=True )
def list_gateways( query_attrs ):
   return storage.list_gateways( query_attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway )
def list_gateways_by_user( email ):
   return storage.list_gateways_by_user( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, admin_only=True )
def list_gateways_by_host( host ):
   return storage.list_gateways_by_host( host )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway )
def list_gateways_by_volume( volume_name_or_id ):
   return storage.list_gateways_by_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="gateway", revoke_key_id="g_name_or_id" )
@UpdateAPIGuard( Gateway )
def set_gateway_public_signing_key( g_name_or_id, signing_public_key, **attrs ):
   return storage.set_gateway_public_signing_key( g_name_or_id, signing_public_key, **attrs )
      

# ----------------------------------
class API( object ):
   
   def __init__(self):
      pass
   
   @classmethod
   def verifier( cls, method, args, kw, request_body, syndicate_data, data ):
      # NOTE: this should only be used by the MS
      if not isinstance( method, AuthMethod ):
         raise Exception("Invalid method")
      
      key_type = syndicate_data.get("key_type")
      key_name = syndicate_data.get("key_name")
      sig = syndicate_data.get("signature")
      
      if sig == None:
         logging.error("No signature")
         # no signature
         return False
      
      cls = KEY_TYPE_TO_CLS.get( key_type )
      if cls == None:
         logging.error("Invalid caller type '%s'" % key_type )
         return False
      
      authenticated_caller = cls.Authenticate( key_name, request_body, sig )
      
      if not authenticated_caller:
         # failed to authenticate
         logging.error("Failed to authenticate")
         return False 
      
      method.authenticated_caller = authenticated_caller
      return True
   
   
   @classmethod
   def error_signer( cls, data ):
      return None
   
   
   @classmethod
   def signer( cls, method, data ):
      if method == None:
         # no method means error code.  Use the "error" signer
         return cls.error_signer( data )
      
      if not isinstance( method, AuthMethod ):
         raise Exception("Invalid method")
      
      return method.sign_reply( data )
      
      
   
   def __getattr__(self, method_name):
      # return the appropriate method
      method = globals().get( method_name, None )
      
      assert_public_method( method )
      
      return AuthMethod( method, None )
   