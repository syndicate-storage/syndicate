#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University

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

"""
Front-end API
"""


from msconfig import *

in_client = False

try:
   # in the MS
   import storage.storage as storage
except ImportError, e:
   # in syntool
   if not in_client:
      in_client = True
      import log as Log
      log = Log.get_logger()
   
   log.warning("Using storage stub")
   from storage_stub import StorageStub as storage
   
try:
   # in the MS
   from MS.volume import Volume, VolumeAccessRequest
   from MS.user import SyndicateUser
   from MS.gateway import Gateway
except ImportError, e:
   # in syntool
   if not in_client:
      in_client = True
      import log as Log
      log = Log.get_logger()
   
   log.warning("Using object stubs")
   from object_stub import *

try:
   # in the MS
   from MS.auth import *
except ImportError, e:   
   # in syntool
   if not in_client:
      in_client = True
      import log as Log
      log = Log.get_logger()
   
   log.warning("Using authentication stub")
   from auth_stub import *

try:
   # in the MS
   from admin_info import SYNDICATE_PRIVKEY
except ImportError, e:
   # in syntool
   SYNDICATE_PRIVKEY = "Syndicate's private key is only available to the MS!"
   in_client = True
   
   
# ----------------------------------
from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import hashlib
import binascii
from itertools import izip

# ----------------------------------
def generate_key_pair( key_size ):
   rng = Random.new().read
   key = CryptoKey.generate(key_size, rng)

   private_key_pem = key.exportKey()
   public_key_pem = key.publickey().exportKey()

   return (public_key_pem, private_key_pem)


def sign_data( private_key_str, data ):
   try:
      key = CryptoKey.importKey( private_key_str )
   except Exception, e:
      log.error("importKey %s", traceback.format_exc() )
      return False
   
   h = HashAlg.new( data )
   signer = CryptoSigner.new(key)
   signature = signer.sign( h )
   return signature

# ----------------------------------
# The User API.

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@CreateAPIGuard( SyndicateUser, admin_only=True, parse_args=SyndicateUser.ParseArgs )
def create_user( email, openid_url, activate_password, activate_password_hash=None, activate_password_salt=None, **attrs ):
   """
   Create a user.
   
   Requred arguments:
      email (str): 
         The email address of the new user.  It will
         serve as the user's identifier, so it must 
         be unique.
                   
      openid_url (str):
         URL to your OpenID identity page.
         
      activate_password (str):
         A one-time-use password the user will use to call
         register_account().
         
         Your client will use this argument to generate values for
         registration_password_hash and registration_password_salt
         (see below).
      
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
         
      activate_password_hash (default: None):
         Do not pass anything for this unless you know what 
         you're doing.  Syntool will fill this in automatically
         from the 'registration_password' argument.
         
         This is the iterated SHA256 hash of the registration 
         salt concatenated with the password (iterated for
         MS.common.PASSWORD_HASH_ITERS times).  It must be 
         a hexadecimal string for encoding purposes.
         
      activate_password_salt (default: None):
         Do not pass anything for this unless you know what
         you're doing.  Syntool will fill this in automatically
         from the 'registration_password' argument.
         
         This is a salt to be used to generate the value for 
         registration_password_hash.  It must be 32 bytes, but
         represented as a hexadecimal string (64 characters) for
         encoding purposes.
         
      is_admin=bool: (default: False)
         Whether or not this user will be a Syndicate admin.
      
         
   Returns:
      A SyndicateUser object on success, or an exception
      on error.
      
   Authorization:
      Only an administrator can create new users.
   """
   
   return storage.create_user( email, openid_url, activate_password_hash=activate_password_hash,
                                                  activate_password_salt=activate_password_salt,
                                                  **attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
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


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
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


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
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


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
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


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD] )
@UpdateAPIGuard( SyndicateUser, target_object_name="email", parse_args=SyndicateUser.ParseArgs, check_write_attrs=False )
def register_account( email, password, signing_public_key=SyndicateUser.USER_KEY_UNSET ):
   """
   Register a recently-created user account.
   
   Positional arguments:
      email (str):
         The email of the desired user. 
         
      password (str):
         Password to change the signing key.

   Optional keyword arguments:
      signing_public_key (default: "unset"):
         The PEM-encoded public key that the MS will
         use to authenticate a client program that wishes
         to access this user.  The client program must use 
         the corresponding private key (the "signing key")
         to sign requests to the MS, in order to prove to
         the MS that the program is acting on behalf of the
         owner of this user account.  Currently, this must be 
         a 4096-bit RSA key.
         
         Pass MAKE_SIGNING_KEY if you want syntool to 
         generate one for you, in which case the private 
         signing key will be stored to your Syndicate key directory
         on successful return of this method.
         
   Returns:
      True on success, or an exception if the user does 
      not exist or the caller is not authorized to set 
      the API field.
   
   Authorization:
      An administrator can set any user's public signing key.
      A user can only set its own public signing key.
      
   Remarks:
      For a normal user, this method can only be called successfully once.
      Any subsequent calls will fail.  Usually, the user calls this method
      just after having their account created (i.e. "password" can be used 
      to construct a one-time registration URL).  In the event of a key
      compromise, the administrator can unset the key and change the
      password (see reset_account_credentials).
   """
   return storage.register_account( email, password, signing_public_key=signing_public_key )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@UpdateAPIGuard( SyndicateUser, admin_only=True, target_object_name="email", parse_args=SyndicateUser.ParseArgs, caller_user="caller_user" )
def reset_account_credentials( email, registration_password, registration_password_hash=None, registration_password_salt=None ):
   """
   Reset a user's account credentials.  Set a new one-time-use registration key,
   so the user can re-upload a public signing key.
   
   Positional arguments:
      email (str):
         The email of the desired user. 
      
      registration_password (str):
         A one-time-use password the user will use to 
         register a new signing public key.
         
   Optional keyword arguments:
      registration_password_hash (default: None):
         Do not pass anything for this unless you know what 
         you're doing.  Your client will fill this in automatically
         from the 'registration_password' argument.
         
         This is the iterated SHA256 hash of the registration 
         salt concatenated with the password (iterated for
         MS.common.PASSWORD_HASH_ITERS times).  It must be 
         a hexadecimal string for encoding purposes.
         
      registration_password_salt (default: None):
         Do not pass anything for this unless you know what
         you're doing.  Your client will fill this in automatically
         from the 'registration_password' argument.
         
         This is a salt to be used to generate the value for 
         registration_password_hash.  It must be 32 bytes, but
         represented as a hexadecimal string (64 characters) for
         encoding purposes.
         
   Returns:
      True on success, or an exception if the user does 
      not exist or the caller is not authorized to set 
      the API field.
   
   Authorization:
      Only an administrator can call this method.
   """
   return storage.reset_account_credentials( email, registration_password_salt, registration_password_hash )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
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

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@CreateAPIGuard( Volume, pass_caller_user="caller_user", parse_args=Volume.ParseArgs )
def create_volume( email, name, description, blocksize, metadata_private_key="MAKE_METADATA_KEY", **attrs ):
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
         
   Optional keyword arguments:
      private=bool (default: True)
         If True, this volume will not be searchable, and users
         will not be able to request access to it.  This value
         is True by default.
         
      metadata_private_key=str (default: MAKE_METADATA_KEY)
         The PEM-encoded private key the Volume will use to sign 
         metadata served to User Gateways.  It must be a 4096-bit
         RSA key.
         
         If pass "MAKE_METADATA_KEY" (the default), a key
         pair will be generated for you.  The private key will 
         be passed as this argument, and the public key will 
         be stored locally to validate Volume data.
      
      store_private_key=bool (default: False)
         If True, store the metadata private key to the local 
         key directory.
         
      archive=bool (default: False)
         If True, only an Acquisition Gateway owned by the given 
         user may write metadata to this Volume.  It will be read-
         only to every other Gateway.
         
      active=bool (default: True)
         If True, this Volume will be accessible by gateways immediately.
         If False, it will not be.
         
      default_gateway_caps=(str or int)
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
         
         You can also pass one of these aliases to common sets of
         capability bits.  These are:
         
         ALL            Set all capabilities
         READWRITE      Set all but GATEWAY_CAP_COORDINATE
         READONLY       GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA
         
   Returns:
      On success, this method returns a Volume.  On failure, it
      raises an exception.
      
   Authorization:
      An administrator can create an unlimited number of volumes.
      A user can only create as many as allowed by its max_volumes value.
   """
   return storage.create_volume( email, name, description, blocksize, metadata_private_key=metadata_private_key, **attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ReadAPIGuard( Volume, parse_args=Volume.ParseArgs )
def read_volume( volume_name_or_id ):
   """
   Read a Volume.
   
   Positional arguments:
      volume_name_or_id (str or int):
         The name or ID of the Volume to read.
   
   Returns:
      On success, this method returns the Volume data.  On failure, it 
      raises an exception.
   
   Authorization:
      An administrator can read any Volume.
      A user can only read Volumes that (s)he owns.
   """
   return storage.read_volume( volume_name_or_id )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@UpdateAPIGuard( Volume, target_object_name="volume_name_or_id", parse_args=Volume.ParseArgs )
def update_volume( volume_name_or_id, **attrs ):
   """
   Update a Volume.
   
   Positional arguments:
      volume_name_or_id (str or int):
         The name or ID of the Volume to update.
      
   Optional keyword arguments:
      active=bool:
         If True, this Volume will be available for Gateways to access.
         If False, they will be unable to interact with the Volume.
      
      description=str:
         The human-readable description of what this Volume is
         used for.
      
      private=bool:
         If True, Gateways must authenticate and prove that they
         are assigned to the Volume before they can access it.
         If False, Gateways can access Volume metadata without 
         authenticating first.
      
      file_quota=int:
         The soft limit on the number of files and directories 
         that can exist in this Volume.  Pass -1 for infinte.
      
      default_gateway_caps=(str or int):
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
         
         You can also pass one of these aliases to common sets of
         capability bits.  These are:
         
         ALL            Set all capabilities
         READWRITE      Set all but GATEWAY_CAP_COORDINATE
         READONLY       GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA
   
   Returns:
      If successful, returns True.  Raises an exception on failure.
   
   Authorization:
      An administrator can update any Volume.
      A user can only update Volumes (s)he owns.
   """
   return storage.update_volume( volume_name_or_id, **attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@DeleteAPIGuard( Volume, target_object_name="volume_name_or_id", parse_args=Volume.ParseArgs )
def delete_volume( volume_name_or_id ):
   """
   Delete a Volume.  Every file and directory within the Volume
   will also be deleted.
   
   Positional arguments:
      volume_name_or_id (str or int)
         The name or ID of the Volume to delete.

   Returns:
      If successful, returns True.  Raises an exception on failure.
      This method is idemopotent--it returns True if the Volume 
      does not exist.
   
   Authorization:
      An administrator can delete any Volume.
      A user can only delete a Volume (s)he owns.
   
   Remarks:
      Be sure to revoke the Volume's metadata key pair after 
      calling this method.  Syntool will do this automatically.
   """
   return storage.delete_volume( volume_name_or_id )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Volume, admin_only=True, parse_args=Volume.ParseArgs )
def list_volumes( query_attrs ):
   """
   List Volumes by attribute.
   
   Positional arguments:
      query_attrs (dict):
         The fields to query on.  Each item must be in
         the form of
         
            'Volume.${attr} ${op}': ${value}
         
         where ${attr} is a user attribute, ${op}
         is ==, !=, >, >=, <, <=, or IN, and ${value}
         is the value to compare the attribute against.
   
   Returns:
      On success, a list of Volumes that match the query.
      Raises an exception on error.
   
   Authorization:
      Only an administrator may call this method.
   """
   return storage.list_volumes( query_attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs, pass_caller_user="caller_user" )
def list_accessible_volumes( email, **caller_user_dict ):   
   """
   List the Volumes that a given user is allowed to access via User Gateways.
   
   Positional arguments:
      email (str):
         The email address of the user.
   
   Returns:
      On success, a list of Volumes that the identified user can access.
      Raises an exception on error.
   
   Authorization:
      An administrator can list any user's accessible Volumes.
      A user can only his/her their own accessible Volumes.
   """
      
   return storage.list_accessible_volumes( email, **caller_user_dict )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs, pass_caller_user="caller_user" )
def list_pending_volumes( email, **caller_user_dict ):   
   """
   List the Volumes that a given user has requested to join, but the Volume owner
   or administrator has not yet acted upon.
   
   Positional arguments:
      email (str):
         The email address of the user.
   
   Returns:
      On success, a list of Volumes that the identified user has requested to access.
      Raises an exception on error.
   
   Authorization:
      An administrator can list any user's access requests.
      A user can only his/her their own access requests.
   """
      
   return storage.list_pending_volumes( email, **caller_user_dict )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs )
def list_public_volumes():
   """
   List the Volumes that are publicly accessible.
   
   Returns:
      On success, a list of Volumes that are public (i.e. the Volumes
      for which the public field is set to True).
      Raises an exception on error.
   
   Authorization:
      Any user may call this method.
   """
   return storage.list_public_volumes()


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Volume, parse_args=Volume.ParseArgs )
def list_archive_volumes():
   """
   List Volumes that serve as public archives (i.e. those that
   can be accessed by anyone and contain read-only data).
   
   Returns:
      On success, a list of Volumes that are public archives.
      Raises an exception on error.
   
   Authorization:
      Any user may call this method.
   """
   return storage.list_archive_volumes()

# ----------------------------------
# The Volume Access Request API.

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@BindAPIGuard( SyndicateUser, Volume, source_object_name="email", target_object_name="volume_name_or_id", caller_owns_target=False, parse_args=VolumeAccessRequest.ParseArgs )
def remove_volume_access( email, volume_name_or_id ):
   """
   Remove a given user's access capabilities from a given volume.
   
   Positional arguments:
      email (str):
         Email address of the user
      
      volume_name_or_id (str or int):
         Name or ID of the volume
      
   Returns:
      True on success
      Raises an exception on error
   
   Authorization:
      The administrator can remove any user from any volume.
      A volume owner can remove any user from any volume he/she owns.
      A user cannot remove him/herself.
   
   Remarks:
      This method is idempotent.  It will succeed even if the user
      is not present in the volume.
   """
   return storage.remove_volume_access( email, volume_name_or_id )

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@BindAPIGuard( SyndicateUser, Volume, source_object_name="email", target_object_name="volume_name_or_id", caller_owns_target=False, parse_args=VolumeAccessRequest.ParseArgs )
def request_volume_access( email, volume_name_or_id, caps, message ):
   """
   Send a request for Volume access.  The Volume owner will be able
   to read the request, and decide whether or not to add or remove 
   the user.
   
   Positional arguments:
      email (str):
         Email address of the user
      
      volume_name_or_id (str or int):
         Name or ID of the volume to join
      
      caps (str or int)
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
         
         You can also pass one of these aliases to common sets of
         capability bits.  These are:
         
         ALL            Set all capabilities
         READWRITE      Set all but GATEWAY_CAP_COORDINATE
         READONLY       GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA

      message (str)
         Message to the volume owner, explaining the nature of the request.
         
   Returns:
      True on success.
      Raises an exception on error.
   
   Authorization:
      Any user can request to join any volume.
      FIXME: users are limited in the number of outstanding requests they may have
      FIXME: requests expire after a time if they are not acted upon.
   """   
   return storage.request_volume_access( email, volume_name_or_id, caps, message )

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( VolumeAccessRequest, pass_caller_user="caller_user", parse_args=VolumeAccessRequest.ParseArgs )
def list_volume_access_requests( volume_name_or_id, **attrs ):
   """
   List the set of pending access requests on a particular volume.
   
   Positional arguments:
      volume_name_or_id (str or int):
         Name or ID of the Volume to query
   
   Returns:
      A list of pending access requests on success.
      Raises an exception on error.
   
   Authorization:
      An administrator can list access requests for any volume.
      A volume owner can only list access requests for his/her volume.
      Normal users cannot list volume access requests.
   """
   return storage.list_volume_access_requests( volume_name_or_id, **attrs )

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( VolumeAccessRequest, pass_caller_user="caller_user", parse_args=VolumeAccessRequest.ParseArgs )
def list_volume_access( volume_name_or_id, **attrs ):
   """
   List the set of users that can access a particular volume.
   
   Positional arguments:
      volume_name_or_id (str or int):
         Name or ID of the volume to query
      
   Returns:
      A list of users that can access the givne volume.
      Raises an exception on error.
   
   Authorization:
      An administrator can list the users of any volume.
      A volume owner can list the users of his/her volumes.
      Normal users cannot list volume users.
   """
   return storage.list_volume_access( volume_name_or_id, **attrs )

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( VolumeAccessRequest, parse_args=VolumeAccessRequest.ParseArgs )
def list_user_access_requests( email ):
   """
   List the set of requests this user has issued, that have not been revoked.
   This includes both pending requests and granted requests.
   
   Positional arguments:
      email (str):
         Email address of the user
    
   Returns:
      A list of pending and granted volume requests.
      Raises an exception on error.
   
   Authorization:
      An administrator can list any users' requests.
      A user may only list his/her own requests.
   """
   return storage.list_user_access_requests( email )

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@BindAPIGuard( SyndicateUser, Volume, source_object_name="email", target_object_name="volume_name_or_id", caller_owns_source=False, parse_args=VolumeAccessRequest.ParseArgs, pass_caller_user="caller_user" )
def set_volume_access( email, volume_name_or_id, caps, **caller_user_dict ):
   """
   Set the access capabilities for a user in a volume.
   
   Positional arguments:
      email (str):
         Email address of the user
      
      volume_name_or_id (str or int):
         Name or ID of the volume to add the user to
      
      caps (str or int):
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
         
         You can also pass one of these aliases to common sets of
         capability bits.  These are:
         
         ALL            Set all capabilities
         READWRITE      Set all but GATEWAY_CAP_COORDINATE
         READONLY       GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA

   Returns:
      True on success.
      Raises an exception on error.
   
   Authorization:
      An administrator can set the capabilities of any user in any volume.
      A volume owner can set the capabilities of any user in the volumes he/she owns.
      A user cannot set their own capabilities.
   
   Remarks:
      This method is idempotent.
   """
      
   return storage.set_volume_access( email, volume_name_or_id, caps, **caller_user_dict )


# ----------------------------------
# The Gateway API

@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@CreateAPIGuard( Gateway, parse_args=Gateway.ParseArgs, pass_caller_user="caller_user" )
# NOTE: due to lexigraphically-ordered handling of keyword arguments, and the fact that if we're going to generate keys we must have before we can
# encrypt and host them, the argument for obtaining the public key must lexigraphically proceed the argument for indicating whether or not we should host the private key.
# For this reason, it's *e*ncryption_password, *g*ateway_public_key, *h*ost_private_key.
# NOTE: encryption_password and host_gateway_key are NOT passed to the MS.  These are interpreted by syntool.
def create_gateway( volume_name_or_id, email, gateway_type, gateway_name, host, port, encryption_password=None, gateway_public_key="MAKE_AND_HOST_GATEWAY_KEY", host_gateway_key=None, **attrs ):
   """
   Create a Gateway.  It will be owned by the given user (which must be the same as the calling user, if the calling user is not admin).
   
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
      
      name (str):
         The human-readable name of this Gateway.
      
      host (str):
         The hostname that should be resolved to contact this Gateway.
      
      port (int):
         The port number this Gateway should listen on.
      

   Optional keyword arguments:
      closure=str:
         This is a serialized JSON structure that stores gateway-
         specific data.  Currently, this is only meaningful for 
         replica gateways.
         
         If you want to use syntool to generate a replica gateway's
         closure from a Python module, pass the path to the
         directory containing the module's files.
         
      gateway_public_key=str (default: MAKE_AND_HOST_GATEWAY_KEY):
         Path to this Gateway's PEM-encoded public key.  The MS will
         distribute this public key to all other Gateways in
         the Volume, so they can use to authenticate messages sent 
         from this particular Gateway.  You will need to give
         the Gateway the corresponding private key at runtime.
         
         If you pass "MAKE_AND_HOST_GATEWAY_KEY" (the default), a key
         pair will be generated for you.  The private key will 
         be sealed with your current password and uploaded to the MS.
         The key will also be stored to your local Syndicate key directory.
         This gives you the convenience of not having to distribute 
         Gateway private keys yourself, but carries the risks of 
         disclosing the ciphertext to unauthorized readers and 
         of making your keys inaccessible if you forget your password.
         
         If you pass "MAKE_GATEWAY_KEY" instead, a key pair will
         be generated for you, but the private key will be written 
         to your local Syndicate key directory.
         
      host_private_key=str (default: None)
         If set, this is the path to the corresponding gateway public key.
         The private key will be sealed with your password and uploaded
         to the MS, so your Gateway can download and unseal it when it 
         starts.
         
   Returns:
      On success, this method returns a Gateway.  On failure, it 
      raises an exception.
   
   Authorization:
      An administrator can create a Gateway in any Volume, for any user.
      A user can only create Gateways in Volumes (s)he owns, or in Volumes
      in which (s)he has been given the right to do so by the Volume's owner.
      
      A user may be subject to a quota enforced for each type of Gateway.
   """
   
   return storage.create_gateway( volume_name_or_id, email, gateway_type, gateway_name, host, port, gateway_public_key=gateway_public_key, encrypted_gateway_private_key=host_gateway_key, **attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ReadAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def read_gateway( g_name_or_id ):
   """
   Read a gateway.
   
   Positional arguments:
      g_name_or_id (str):
         The name or ID of the desired gateway.

   Returns:
      On success, the gateway data.
      Raises an exception on error.
   
   Authorization:
      An administrator can read any gateway.
      A user can only read gateways (s)he owns.
   """
   
   return storage.read_gateway( g_name_or_id )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@UpdateAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def update_gateway( g_name_or_id, **attrs ):
   """
   Update a gateway.
   
   Positional arguments:
      g_name_or_id (str):
         The name or ID of the gateway to update.
      
   Optional keyword arguments:
      host=str:
         The hostname that should be resolved to contact this gateway.
      
      port=int:
         The port number this gateway should listen on.
      
      closure=str:
         This is a serialized JSON structure that stores gateway-
         specific data.  Currently, this is only meaningful for 
         replica gateways.
         
         If you want to use syntool to generate a replica gateway's
         closure from a Python module, pass the path to the
         directory containing the module's files.
      
      cert_expires=int:
         Date when this gateway's certificate expires, in seconds
         since the epoch.
      
      session_timeout=int:
         The longest the gateway will wait to renew its certificate
         with the MS (in seconds).  It will renew its certificate
         more frequently when it suspects it has become stale.
         
      session_expires=int:
         Date when this gateway's current session with the Volume expires,
         in seconds since the epoch.
   
   Returns:
      On success, this method returns True.
      Raises an exception on error.
   
   Authorization:
      An administrator can update any gateway.
      A user can only update his/her own gateways.
   """
   return storage.update_gateway( g_name_or_id, **attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@UpdateAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs, pass_caller_user="caller_user" )
def set_gateway_caps( g_name_or_id, caps, **caller_user_dict ):
   """
   Set a gateway's capabilities.
   
   Positional arguments:
      g_name_or_id (str):
         The name or ID of the gateway to update.
      
      caps (str or int):
         Capability bits.  Valid capability bits are:
            GATEWAY_CAP_READ_METADATA
            GATEWAY_CAP_WRITE_METADATA
            GATEWAY_CAP_READ_DATA
            GATEWAY_CAP_WRITE_DATA
            GATEWAY_CAP_COORDINATE
      
         You can pass capability bits by name and bitwise OR them
         together (e.g. "GATEWAY_CAP_COORDINATE | GATEWAY_CAP_WRITE_DATA").
         
         You can also pass one of these aliases to common sets of
         capability bits.  These are:
         
         ALL            Set all capabilities
         READWRITE      Set all but GATEWAY_CAP_COORDINATE
         READONLY       GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA
   
   Returns:
      True on success.
      Raises an exception on error.
   
   Authorization:
      An administrator can set the capabilities of any gateway.
      
      A user must not only own the gateway to set its capabilities, but
      also own the Volume in which the gateway resides.  This is to prevent
      an arbitrary user from gaining unwarranted capabilities.
   
   Remarks:
      This method only makes sense for User Gateways.  An Acquisition Gateway
      is only supposed to be able to write metadata into its Volume, and a Replica
      Gateway never needs to access a Volume.  Attempts to give AGs or RGs 
      different capabilities will silently fail.
   """
      
   return storage.set_gateway_caps( g_name_or_id, caps, **caller_user_dict )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@DeleteAPIGuard( Gateway, target_object_name="g_name_or_id", parse_args=Gateway.ParseArgs )
def delete_gateway( g_name_or_id ):
   """
   Delete a gateway.
   
   Positional arguments:
      g_name_or_id (str):
         The name or ID of the gateway to delete.
   
   Returns:
      True on success.
      Raises an exception on error.
   
   Authorization:
      An administrator can delete any gateway.
      A user can only delete a gateway (s)he owns.
   """
   
   return storage.delete_gateway( g_name_or_id )  


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Gateway, admin_only=True, parse_args=Gateway.ParseArgs )
def list_gateways( query_attrs ):
   """
   List gateways.
   
   Positional arguments:
      query_attrs (dict):
         The fields to query on.  Each item must be in
         the form of
         
            'Gateway.${attr} ${op}': ${value}
         
         where ${attr} is a user attribute, ${op}
         is ==, !=, >, >=, <, <=, or IN, and ${value}
         is the value to compare the attribute against.
   
   Returns:
      On success, a list of gateways matching the query.
      Raises an exception on error.
   
   Authorization:
      Only an administrator can call this method.
   """
   return storage.list_gateways( query_attrs )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs, pass_caller_user="caller_user" )
def list_gateways_by_user( email, **caller_user_dict ):
   """
   List the gateways owned by a particular user.
   
   Positional arguments:
      email (str):
         The email address of the user.
   
   Returns:
      On success, a list of gateways owned by the given user.
      Raises an exception on failure.
   
   Authorization:
      An administrator can list any user's gateways.
      A user can only list his/her own gateways.
   """
      
   return storage.list_gateways_by_user( email, **caller_user_dict )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Gateway, admin_only=True, parse_args=Gateway.ParseArgs )
def list_gateways_by_host( host ):
   """
   List gateways on a particular host.
   
   Positional arguments:
      host (str):
         The hostname on which to query
   
   Returns:
      On success, a list of gateways accessible with the given hostname.
      Raises an exception on failure.
   
   Authorization:
      Only an administrator can call this method.
   """
   return storage.list_gateways_by_host( host )


@Authenticate( auth_methods=[AUTH_METHOD_PASSWORD, AUTH_METHOD_PUBKEY] )
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs, pass_caller_user="caller_user" )
def list_gateways_by_volume( volume_name_or_id, **caller_user_dict ):
   """
   List gateways in a Volume.
   
   Positional arguments:
      volume_name_or_id (str or int):
         The name or ID of the Volume on which to query.
         
   Returns:
      On success, a list of gateways belonging to a particular Volume.
      Raises an exception on failure.
   
   Authorization:
      An administrator can list any Volume's gateways.
      A user can only list gateways in his/her own Volumes.
   """
   return storage.list_gateways_by_volume( volume_name_or_id, **caller_user_dict )


# ----------------------------------
class API( object ):
   # NOTE: this should only be used by the MS
   
   def __init__(self):
      pass
   
   @classmethod
   def pubkey_verifier( cls, method, args, kw, request_body, syndicate_data, data, **verifier_kw ):
      """
      Verify a request from a user, using public-key signature verification.
      """
      if not isinstance( method, AuthMethod ):
         raise Exception("Invalid method")
      
      username = syndicate_data.get("username")
      sig = syndicate_data.get("signature")
      
      # do we need to perform an authentication here?
      if not method.method_func.need_authentication:
         # the method will perform its own authentication.  Just get the requested object
         authenticated_user = SyndicateUser.Read( username )
         if not authenticated_user:
            log.error("No such user %s" % (username) )
            return False
         
         else:
            method.authenticated_user = authenticated_user
            return True
      
      else:
         authenticated_user = SyndicateUser.Authenticate( username, request_body, sig )
         
         if not authenticated_user:
            # failed to authenticate via keys.
            log.error("Failed to authenticate user %s" % (username))
            return False 
         
         method.authenticated_user = authenticated_user
         return True
   
   
   @classmethod
   def openid_verifier( cls, method, args, kw, request_body, syndicate_data, data, **verifier_kw ):
      """
      Verify an OpenID request.  This is really a matter of ensuring the OpenID user matches the user that sent the request.
      """
      
      if not isinstance( method, AuthMethod ):
         raise Exception("Invalid method")
      
      given_username = verifier_kw.get("username")
      username = syndicate_data.get("username")
      
      if username is None:
         log.error("No username given in method call")
         return None
      
      if username != given_username:
         log.error("Username mismatch: method call username: %s, OpenID username: %s" % (username, given_username))
         return None
      
      # user is already authenticated with OpenID, so we're good!
      authenticated_user = SyndicateUser.Read( username )
      if not authenticated_user:
         log.error("No such user %s" % (username) )
         return False
      
      else:
         method.authenticated_user = authenticated_user
         return True
      
   
   @classmethod
   def signer( cls, method, data ):
      """
      Sign the result of a method with the private key of this Syndicate drive.
      """
      if method == None:
         # no method means error code.
         return sign_data( SYNDICATE_PRIVKEY, data )
      
      if not isinstance( method, AuthMethod ):
         raise Exception("Invalid method")
      
      authenticated_user = method.authenticated_user
      
      if authenticated_user == None:
         raise Exception("Caller is not authenticated")
      
      return sign_data( SYNDICATE_PRIVKEY, data )
      
   
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
def method_auth_options( method_name ):
   method = get_method( method_name )
   if method is None:
      return None
   
   return method.auth_methods

# ----------------------------------
def method_help_from_method_name( method_name ):
   method = get_method( method_name )
   return method.__doc__
      
# ----------------------------------
def secure_hash_compare(s1, s2):
   # constant-time compare
   # see http://carlos.bueno.org/2011/10/timing.html
   diff = 0
   for char_a, char_b in izip(s1, s2):
      diff |= ord(char_a) ^ ord(char_b)
   return diff == 0

# ----------------------------------
def hash_password( password, salt ):
   sh = hashlib.sha256()
   
   for i in xrange(0, PASSWORD_HASH_ITERS):
      sh.update( salt )
      sh.update( password )
   
   return sh.hexdigest()

# ----------------------------------
def password_salt():
   salt_str = os.urandom( PASSWORD_SALT_LENGTH )
   return binascii.b2a_hex( salt_str )

# ----------------------------------
def check_password( password, salt, password_hash ):
   challenge_hash = hash_password( password, salt )
   return secure_hash_compare( challenge_hash, password_hash )
