#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

"""
Front-end API
"""


from msconfig import *

import log as Log
log = Log.get_logger()

try:
   import storage.storage as storage
except Exception, e:
   from storage_stub import StorageStub as storage
   
try:
   from MS.volume import Volume, VolumeAccessRequest
   from MS.user import SyndicateUser
   from MS.gateway import Gateway
except Exception, e:
   from object_stub import *

try:
   from MS.auth import *
except Exception, e:   
   from auth_stub import *

# ----------------------------------
from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

# ----------------------------------
def generate_key_pair( key_size ):
   rng = Random.new().read
   key = CryptoKey.generate(key_size, rng)

   private_key_pem = key.exportKey()
   public_key_pem = key.publickey().exportKey()

   return (public_key_pem, private_key_pem)

# ----------------------------------
SIGNING_KEY_DEFAULT_USER_ID = 1         # use the client's given user ID, not one derived from the argument list

# map key type to the object class
KEY_TYPE_TO_CLS = dict( [(cls.key_type, cls) for cls in [Volume, SyndicateUser, Gateway]] )


# ----------------------------------
# The User API.

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="user", verify_key_id="email", trust_key_type="user", trust_key_id="email" )
@CreateAPIGuard( SyndicateUser, admin_only=True, parse_args=SyndicateUser.ParseArgs )
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
         
         Pass MAKE_SIGNING_KEY if you want your client to 
         generate one for you, in which case the private 
         signing key will be stored to your user key directory
         on successful return of this method.
         
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
@ReadAPIGuard( SyndicateUser, parse_args=SyndicateUser.ParseArgs )
def read_user( email ):
   """
   Read a user.
   
   Positional arguments:
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
@UpdateAPIGuard( SyndicateUser, target_object_name="email", parse_args=SyndicateUser.ParseArgs )
def update_user( email, **attrs ):
   """
   Update a user.
   
   Positional arguments:
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
@DeleteAPIGuard( SyndicateUser, target_object_name="email", parse_args=SyndicateUser.ParseArgs )
def delete_user( email ):
   """
   Delete a user.
   
   Positional arguments:
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
@ListAPIGuard( SyndicateUser, admin_only=True, parse_args=SyndicateUser.ParseArgs )
def list_users( query_attrs ):
   """
   List users.
   
   Positional arguments:
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
@UpdateAPIGuard( SyndicateUser, target_object_name="email", parse_args=SyndicateUser.ParseArgs )
def set_user_public_signing_key( email, signing_public_key, **attrs ):
   """
   Set a user's public signing key.
   
   Positional arguments:
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
         
         Pass MAKE_SIGNING_KEY if you want your client to 
         generate one for you, in which case the private 
         signing key will be stored to your user key directory
         on successful return of this method.
   
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
@ListAPIGuard( SyndicateUser, pass_caller_user="caller_user", parse_args=SyndicateUser.ParseArgs )
def list_volume_user_ids( volume_name_or_id, **attrs ):
   """
   List the emails of the users that can access data
   in a given volume.  That is, which users have 
   registered User Gateways that can access 
   the volume's metadata.
   
   Positional arguments:
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
@CreateAPIGuard( Volume, pass_caller_user="caller_user", parse_args=Volume.ParseArgs )
def create_volume( email, name, description, blocksize, signing_public_key, **attrs ):
   """
   Create a Volume.  It will be owned by the calling user, or, if the caller is an admin, the user identified by the given email address.
   
   Positional arguments:
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
         
         Pass MAKE_SIGNING_KEY if you want your client to 
         generate one for you, in which case the private 
         signing key will be stored to your user key directory
         on successful return of this method.
         
   Optional keyword arguments:
      private=bool (default: True)
         If True, this volume will not be searchable, and users
         will not be able to request access to it.  This value
         is True by default.
         
      metadata_private_key=str (default: None)
         The PEM-encoded private key the Volume will use to sign 
         metadata served to User Gateways.  It must be a 4096-bit
         RSA key.  It will be automatically generated by the MS
         if not given.  If you want your client to generate the 
         private key, pass MAKE_METADATA_KEY.
      
      archive=bool (default: False)
         If True, only an Acquisition Gateway owned by the given 
         user may write metadata to this Volume.  It will be read-
         only to every other Gateway.
         
      active=bool (default: True)
         If True, this Volume will be accessible by gateways immediately.
         If False, it will not be.
      
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
      
         You can pass capability bits by name and bitwise OR them
         together (e.g. "GATEWAY_CAP_COORDINATE | GATEWAY_CAP_WRITE_DATA").
         
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
@ReadAPIGuard( Volume, parse_args=Volume.ParseArgs )
def read_volume( volume_name_or_id ):
   return storage.read_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@UpdateAPIGuard( Volume, target_object_name="volume_name_or_id", parse_args=Volume.ParseArgs )
def update_volume( volume_name_or_id, **attrs ):
   return storage.update_volume( volume_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="volume", revoke_key_id="volume_name_or_id" )
@DeleteAPIGuard( Volume, target_object_name="volume_name_or_id", parse_args=Volume.ParseArgs )
def delete_volume( volume_name_or_id ):
   return storage.delete_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume, admin_only=True, parse_args=Volume.ParseArgs )
def list_volumes( query_attrs ):
   return storage.list_volumes( query_attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs )
def list_accessible_volumes( email ):   
   return storage.list_accessible_volumes( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs )
def list_public_volumes():
   return storage.list_public_volumes()

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="volume", verify_key_id="name" )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs )
def list_archive_volumes():
   return storage.list_archive_volumes()

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="volume", revoke_key_id="volume_name_or_id" )
@UpdateAPIGuard( Volume, target_object_name="volume_name_or_id", pass_caller_user="caller_user", parse_args=Volume.ParseArgs )
def set_volume_public_signing_key( volume_name_or_id, signing_public_key, **attrs ):   
   return storage.set_volume_public_signing_key( volume_name_or_id, signing_public_key, **attrs )


# ----------------------------------
# The Volume Access Request API.

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"])
@BindAPIGuard( SyndicateUser, Volume, source_object_name="email", target_object_name="volume_name_or_id", caller_owns_target=False, parse_args=VolumeAccessRequest.ParseArgs )
def remove_volume_access_request( email, volume_name_or_id ):
   return storage.remove_volume_access_request( email, volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"])
@BindAPIGuard( SyndicateUser, Volume, source_object_name="email", target_object_name="volume_name_or_id", caller_owns_target=False, parse_args=VolumeAccessRequest.ParseArgs )
def request_volume_access( email, volume_name_or_id, caps, message ):
   return storage.request_volume_access( email, volume_name_or_id, caps, message )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID])
@ListAPIGuard( VolumeAccessRequest, pass_caller_user="caller_user", parse_args=VolumeAccessRequest.ParseArgs )
def list_volume_access_requests( volume_name_or_id, **attrs ):
   return storage.list_volume_access_requests( volume_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID])
@ListAPIGuard( VolumeAccessRequest, parse_args=VolumeAccessRequest.ParseArgs )
def list_user_access_requests( email ):
   return storage.list_user_access_requests( email )


# ----------------------------------
# The Gateway API

@Authenticate( signing_key_types=["user"], signing_key_ids=["email"], verify_key_type="gateway", verify_key_id="name", trust_key_type="gateway", trust_key_id="gateway_name" )
@CreateAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def create_gateway( volume_name_or_id, email, gateway_type, gateway_name, host, port, gateway_public_key, signing_public_key, **attrs ):
   """
   Create a Gateway.  It will be owned by the calling user, or, if the caller user is an admin, a user identified by the given email address.
   
   Positional arguments:
      volume_name_or_id (str or int):
         The name or ID of the Volume in which to create this Gateway.
         Some clients will only accept a name, and others only an ID.
         
      email (str):
         The email address of the user to own this Gateway.
      
      gateway_type (str):
         The type of Gateway to create.
         Pass UG for user gateway.
         Pass RG for replica gateway.
         Pass AG for acquisition gateway.
      
      gateway_name (str):
         The human-readable name of this Gateway.
      
      host (str):
         The name of the host on which this Gateway should run.
      
      port (int):
         The port number this Gateway should listen on.
      
      gateway_public_key (str):
         This Gateway's PEM-encoded public key.  The MS will
         distribute this public key to all other Gateways in
         the Volume, so they can use to authenticate messages sent 
         from this particular Gateway.  You will need to give
         the Gateway the corresponding private key at runtime.
         
         If you want to generate and store one automatically,
         pass MAKE_GATEWAY_KEY.
      
      signing_public_key (str):
         The PEM-encoded public key that the MS will
         use to authenticate a client program that wishes
         to read this volume.  The client program must use 
         the corresponding private key (the "signing key")
         to sign requests to the MS, in order to prove to
         the MS that the program is acting on behalf of the
         owner of this volume.  Currently, this must be 
         a 4096-bit RSA key.
         
         Pass MAKE_SIGNING_KEY if you want your client to 
         generate one for you, in which case the private 
         signing key will be stored to your user key directory
         on successful return of this method.

   Optional keyword arguments:
      closure (str):
         This is a serialized JSON structure that stores gateway-
         specific data.  Currently, this is only meaningful for 
         replica gateways.
         
         If you want to generate a replica gateway's closure from 
         a Python module, pass the path to the directory containing 
         the files.

   Returns:
      On success, this method returns a Gateway.  On failure, it 
      raises an exception.
   
   Authorization:
      An administrator can create a Gateway in any Volume, for any user.
      A user can only create Gateways in Volumes (s)he owns, or in Volumes
      in which (s)he has been given the right to do so by the Volume's owner.
      
      A user may be subject to a quota enforced for each type of Gateway.
   """
   
   return storage.create_gateway( volume_name_or_id, email, gateway_type, gateway_name, host, port, gateway_public_key, signing_public_key, **attrs )

@Authenticate( object_authenticator=Gateway.Authenticate, object_response_signer=Gateway.Sign,
               signing_key_types=["user", "gateway"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID, "g_name_or_id"],
               verify_key_type="gateway", verify_key_id="g_name_or_id" )
@ReadAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def read_gateway( g_name_or_id ):
   return storage.read_gateway( g_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@UpdateAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def update_gateway( g_name_or_id, **attrs ):
   return storage.update_gateway( g_name_or_id, **attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID] )
@UpdateAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def set_gateway_caps( g_name_or_id, caps ):
   return storage.set_gateway_caps( g_name_or_id, caps )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="gateway", revoke_key_id="g_name_or_id" )
@DeleteAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def delete_gateway( g_name_or_id ):
   return storage.delete_gateway( g_name_or_id )  

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, admin_only=True, parse_args=Gateway.ParseArgs )
def list_gateways( query_attrs ):
   return storage.list_gateways( query_attrs )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def list_gateways_by_user( email ):
   return storage.list_gateways_by_user( email )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, admin_only=True, parse_args=Gateway.ParseArgs )
def list_gateways_by_host( host ):
   return storage.list_gateways_by_host( host )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], verify_key_type="gateway", verify_key_id="name" )
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def list_gateways_by_volume( volume_name_or_id ):
   return storage.list_gateways_by_volume( volume_name_or_id )

@Authenticate( signing_key_types=["user"], signing_key_ids=[SIGNING_KEY_DEFAULT_USER_ID], revoke_key_type="gateway", revoke_key_id="g_name_or_id" )
@UpdateAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def set_gateway_public_signing_key( g_name_or_id, signing_public_key, **attrs ):
   return storage.set_gateway_public_signing_key( g_name_or_id, signing_public_key, **attrs )

# ----------------------------------
class API( object ):
   # NOTE: this should only be used by the MS
   
   def __init__(self):
      pass
   
   @classmethod
   def verifier( cls, method, args, kw, request_body, syndicate_data, data ):
      import logging 
      
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
   
   
# ----------------------------------
def is_method( method_name ):
   
   try:
      method = globals().get( method_name, None )
      
      assert_public_method( method )
      
      return True
   except:
      return False

# ----------------------------------
def get_method( method_name ):
   
   try:
      method = globals().get( method_name, None )
      
      assert_public_method( method )
   except Exception, e:
      raise Exception("Not an API method: '%s'" % method_name )
   
   return method

# ----------------------------------
def signing_key_types_from_method_name( method_name ):
   method = get_method( method_name )
   try:
      return method.signing_key_types
   except:
      raise Exception("Method '%s' has no known signing key types" % method_name)

# ----------------------------------
def signing_key_names_from_method_args( method_name, args, kw, default_user_id=None ):
   method = get_method( method_name )
   try:
      names = method.get_signing_key_names( method_name, args, kw )
      
      # fix up special type values 
      for i in xrange(0,len(names)):
         if names[i] == SIGNING_KEY_DEFAULT_USER_ID:
            names[i] = default_user_id
      
      return names 
   
   except Exception, e:
      traceback.print_exc()
      raise Exception("Method '%s' has no known signing key names" % method_name)

# ----------------------------------
def verify_key_type_from_method_name( method_name ):
   method = get_method( method_name )
   try:
      return method.verify_key_type
   except Exception, e:
      raise Exception("Method '%s' has no known verify key type" % method_name)

# ----------------------------------
def verify_key_name_from_method_result( method_name, args, kw, method_result, default_user_id=None ):
   method = get_method( method_name )
   try:
      # fix up special type values
      if method.verify_key_type == SIGNING_KEY_DEFAULT_USER_ID:
         return default_user_id
      else:
         return method.get_verify_key_name( method_name, args, kw, method_result )
   except Exception, e:
      traceback.print_exc()
      raise Exception("Method '%s' has no known verify key name" % method_name)
   
# ----------------------------------
def revoke_key_type_from_method_name( method_name ):
   method = get_method( method_name )
   try:
      return method.revoke_key_type
   except Exception, e:
      raise Exception("Method '%s' has no known revoke key type" % method_name)

# ----------------------------------
def revoke_key_name_from_method_args( method_name, args, kw ):
   method = get_method( method_name )
   try:
      return method.get_revoke_key_name( method_name, args, kw )
   except Exception, e:
      traceback.print_exc()
      raise Exception("Method '%s' has no known revoke key name" % method_name)
   
# ----------------------------------
def trust_key_type_from_method_name( method_name ):
   method = get_method( method_name )
   try:
      return method.trust_key_type
   except Exception, e:
      raise Exception("Method '%s' has no known trust key type" % method_name)

# ----------------------------------
def trust_key_name_from_method_args( method_name, args, kw ):
   method = get_method( method_name )
   try:
      return method.get_trust_key_name( method_name, args, kw )
   except Exception, e:
      traceback.print_exc()
      raise Exception("Method '%s' has no known trust key name" % method_name)

# ----------------------------------
def method_help_from_method_name( method_name ):
   method = get_method( method_name )
   return method.__doc__
   