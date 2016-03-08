#!/usr/bin/env python

"""
   Copyright 2015 The Trustees of Princeton University

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

# NOTE: shared between the MS and the Syndicate python package

in_client = True
log = None
SYNDICATE_PRIVKEY = None

# detect whether or not we are in a client program or the MS
try:
   import admin_info 
   in_client = False
except ImportError, e:
   in_client = True 

if not in_client:
   # in the MS
   from msconfig import *
   import storage.storage as storage
   from MS.volume import Volume
   from MS.user import SyndicateUser
   from MS.gateway import Gateway
   from MS.auth import *
   from admin_info import SYNDICATE_PRIVKEY
   
else:
    
   import inspect 
   
   # in client program.
   # stub everything out
   class StorageStub( object ):
      def __init__(self, *args, **kw ):
         pass
    
      def __getattr__(self, attrname ):
         def stub_storage(*args, **kw):
            logging.warn("Stub '%s'" % attrname)
            return None
         
         return stub_storage
   
    
   def assert_public_method( method ):
        return (method != None)

   class AuthMethod( object ):
        def __init__(self, method_func, authenticated_caller):
            self.method_func = method_func
        
        def __call__(self, *args, **kw):
            return self.method_func( *args, **kw )
    
    
   class StubAuth( object ):
        admin_only = False
        
        def __init__(self, *args, **kw ):
            self.admin_only = kw.get('admin_only', False)
            self.parse_args = kw.get('parse_args', None)
        
        def __call__(self, func):
            
            def inner( *args, **kw ):
                return func(*args, **kw)
            
            inner.__name__ = func.__name__
            inner.__doc__ = func.__doc__
            inner.admin_only = self.admin_only
            inner.argspec = inspect.getargspec( func )
            inner.parse_args = self.parse_args
            
            return inner

   class CreateAPIGuard( StubAuth ):
        pass

   class ReadAPIGuard( StubAuth ):
        pass

   class UpdateAPIGuard( StubAuth ):
        pass

   class DeleteAPIGuard( StubAuth ):
        pass

   class ListAPIGuard( StubAuth ):
        pass

   class BindAPIGuard( StubAuth ):
        pass

   class Authenticate( StubAuth ):
        
        def __call__(self, func):
            func.is_public = True
            return func
   
   storage = StorageStub
   
   from syndicate.util.objects import *
   
   import syndicate.util.config
   log = syndicate.util.config.log
   from syndicate.ms.msconfig import *
   
   SYNDICATE_PRIVKEY = "Syndicate's private key is only available to the MS!"
   
# ----------------------------------
from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import hashlib
import binascii
import traceback
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
      log.error("importKey %s" % traceback.format_exc() )
      return None
   
   h = HashAlg.new( data )
   signer = CryptoSigner.new(key)
   signature = signer.sign( h )
   return signature


def verify_data( pubkey_str, data, signature ):
   try:
      key = CryptoKey.importKey( pubkey_str )
   except Exception, e:
      log.error("importKey %s" % traceback.format_exc() )
      return False
   
   h = HashAlg.new( data )
   verifier = CryptoSigner.new(key)
   ret = verifier.verify( h, signature )
   return ret


# ----------------------------------
# The User API.

@Authenticate()
@CreateAPIGuard( SyndicateUser, admin_only=True, parse_args=SyndicateUser.ParseArgs, caller_user="caller_user" )
def create_user( email, private_key, **attrs ):
   """
   Create a user.
   
   Requred positional arguments:
      email (str): 
         The email address of the new user.  It will
         serve as the user's identifier, so it must 
         be unique.
      
      private_key (str)
         The PEM-encoded private key that the MS will
         use to authenticate a client program that wishes
         to access this user.  The client program must use 
         the key to sign requests to the MS, in order to
         prove to the MS that the program is acting on
         behalf of the owner of this user account. 
         Currently, this must be a 4096-bit RSA key.
         
         Pass "auto" if you want Syndicate to automatically
         generate a key pair for you, in which case the private 
         key will be stored to your Syndicate key directory
         on successful return of this method.
         
   Optional keyword arguments:
      
      max_volumes=int: (default: 10)
         Maximum number of Volumes this user may own.
         -1 means infinite.
      
      max_gateways=int: (default: 10)
         Maximum number of gateways this user can own.
         
      is_admin=bool: (default: False)
         Whether or not this user will be a Syndicate admin.
      
         
   Returns:
      A SyndicateUser object on success, or an exception
      on error.
      
   Authorization:
      Only an administrator can create new users.
      
   Remarks:
      Syndicate will generate a protobuf'ed certificate and send it 
      as a keyword argument called 'user_cert_b64'.  It will contain 
      the new user's ID, email, and public key.
      
      Syndicate does *not* send the private key.  It stores it locally 
      instead.
   """
   
   return storage.create_user( email, **attrs )


@Authenticate()
@ReadAPIGuard( SyndicateUser, parse_args=SyndicateUser.ParseArgs )
def read_user( email_or_user_id ):
   """
   Read a user.
   
   Positional arguments:
      email_or_user_id (str or int):
         The email address or numerical owner ID of the desired user.
   
   Returns:
      A SyndicateUser object on success, or an exception 
      if the user does not exist or if the caller is not 
      allowed to read this user's account.
   
   Authorization:
      The administrator can read any user account.
      A user can only read itself.
   """
   return storage.read_user( email_or_user_id )


@Authenticate()
@DeleteAPIGuard( SyndicateUser, target_object_name="email", parse_args=SyndicateUser.ParseArgs, caller_user="caller_user" )
def delete_user( email, **kw ):
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
   return storage.delete_user( email, **kw )


@Authenticate()
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


@Authenticate()
@UpdateAPIGuard( SyndicateUser, admin_only=True, target_object_name="email", parse_args=SyndicateUser.ParseArgs, caller_user="caller_user" )
def reset_user( email, public_key, **kwargs ):
   """
   Reset a user: set a new certificate.

   Positional arguments:
      email (str):
         The email of the desired user. 

      public_key (str):
         Path to the new public key for the user (PEM-encoded
         4096-bit RSA key).  Pass 'auto' to automatically
         generate a new key pair, and store the private key in
         your Syndicate key directory.

   Returns:
      True on success, or an exception if the user does 
      not exist or the caller is not authorized to set 
      the API field.
   
   Authorization:
      Only an administrator can call this method.
      
   Remarks:
      Syndicate implicitly passes 'user_cert_b64' as a keyword argument,
      which contains a serialized certificate signed by the admin that 
      contains the email and new public key.  It must be signed by an
      admin.
   """
   return storage.reset_user( email, **kwargs )


@Authenticate()
@ListAPIGuard( SyndicateUser, caller_user="caller_user", parse_args=SyndicateUser.ParseArgs )
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

@Authenticate()
@CreateAPIGuard( Volume, caller_user="caller_user", parse_args=Volume.ParseArgs )
def create_volume( **attrs ):
   """
   Create a Volume.  It will be owned by the calling user, or, if the caller is an admin, the user identified by the given email address.
   
   Required keyword arguments:
      email=str:
         The email of the user to own this Volume.
      
      name=str:
         The name of this volume.  It must be unique.
         It is recommend that it be human-readable, so other 
         users can request to join it.
      
      description=str:
         A human-readable description of the Volume's contents.
         
      blocksize=int
         The size of a block in this Volume.  Each block will
         be cached as an HTTP object in the underlying Web 
         caches, so it is probably best to pick block sizes between 
         4KB and 1MB or so.
         
   Optional keyword arguments:
      private=bool (default: True)
         If True, this volume will not be searchable, and users
         will not be able to request access to it.  This value
         is True by default.
         
      archive=bool (default: False)
         If True, then there can be exactly one writer gateway for 
         this volume.  It will be read-only to every other Gateway.
         
      allow_anon=bool (default: false)
         If True, anonymous gateways can read this volume.
         
   Returns:
      On success, this method returns a Volume.  On failure, it
      raises an exception.
      
   Authorization:
      An administrator can create an unlimited number of volumes.
      A user can only create as many as allowed by its max_volumes value.
   
   Remarks:
      In practice, Syndicate will generate two keyword arguments: 'volume_cert_b64'
      and 'cert_bundle_b64'.  'volume_cert_b64' is a serialized protobuf containing
      the above keywords, in an ms_pb2.ms_volume_metadata structure signed by 
      the user that created the volume.  'cert_bundle_b64' is a serialized protobuf 
      containing the new volume certificate bundle version vector, signed by 
      the same user.
   """
   return storage.create_volume( **attrs )


@Authenticate()
@ReadAPIGuard( Volume, parse_args=Volume.ParseArgs )
def read_volume( name ):
   """
   Read a Volume.
   
   Positional arguments:
      name (str):
         The name of the Volume to read.
   
   Returns:
      On success, this method returns the Volume data.  On failure, it 
      raises an exception.
   
   Authorization:
      An administrator can read any Volume.
      A user can only read Volumes that (s)he owns.
   """
   return storage.read_volume( name )


@Authenticate()
@UpdateAPIGuard( Volume, target_object_name="name", caller_user="caller_user", parse_args=Volume.ParseArgs )
def update_volume( name, **attrs ):
   """
   Update a Volume.
   
   Positional arguments:
      name (str):
         The name of the Volume to update.
      
   Optional keyword arguments:
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
      
   Returns:
      If successful, returns True.  Raises an exception on failure.
   
   Authorization:
      An administrator can update any Volume.
      A user can only update Volumes (s)he owns.
      
   Remakrs:
      In implementation, Syndicate will send along 'volume_cert_b64' and 
      'cert_bundle_b64' as keywords.  The former is a protobuf'ed ms_gateway_metadata 
      structure containing the above keywords, signed by the volume owner.  The 
      latter is the current version vector for all gateway and volume certs, signed 
      by the volume owner.
   """
   return storage.update_volume( name, **attrs )


@Authenticate()
@DeleteAPIGuard( Volume, target_object_name="name", caller_user="caller_user", parse_args=Volume.ParseArgs )
def delete_volume( name, **attrs ):
   """
   Delete a Volume.  Every file and directory within the Volume
   will also be deleted.
   
   Positional arguments:
      name (str)
         The name of the Volume to delete.

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
   return storage.delete_volume( name, **attrs )


@Authenticate()
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

   # don't show deleted volumes 
   query_attrs["Volume.deleted =="] = False
   return storage.list_volumes( query_attrs )


@Authenticate()
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


@Authenticate()
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
# The Gateway API

@Authenticate()
@CreateAPIGuard( Gateway, parse_args=Gateway.ParseArgs, caller_user="caller_user" )
def create_gateway( **kw ):
   """
   Create a Gateway.  It will be owned by the given user (which must be the same as the calling user, if the calling user is not admin).
   If it is a User Gateway, it will have the capabilities defined in the calling user's VolumeAccessRequest record (created via 
   set_volume_access()).
   
   This method takes only keyword arguments, because what's supposed to happen behind the scenes is the front-end will translate 
   the keyword arguments into a user-signed gateway certificate.  This method really just takes the serialized certificate for this
   gateway, constructs a gateway record, and keeps the certificate around for other gateways to fetch.
   
   Required Keyword arguments:
   
      email=str:
         Email address of the user that will own this Gateway.
         
      volume=str:
         Name of the Volume to which the Gateway will be attached.
      
      name=str:
         Name of the Gateway.  Must be unique for this Volume.
         
      private_key=str:
         PEM-encoded RSA 4096-bit private key for this gateway.
         Pass "auto" to have Syndicate generate one and store 
         the corresponding private key locally.
      
   Optional keyword arguments:
      
      type=int (Default: 0)
         Gateway type.  Used to identify deployment-specific
         categories of Gateways.
      
      driver=str
         Serialized driver data for this Gateway, or a path to 
         a driver's directory on disk.
         
      port=int (Default: 31111)
         Port this Gateway listens on.
         
   Returns:
      On success, this method returns a Gateway.  On failure, it 
      raises an exception.
   
   Authorization:
      An administrator can create any Gateway in any Volume, for any user.
      A user can only create Gateways in Volumes (s)he owns, or in Volumes
      in which (s)he has been given the right to do so by the Volume's owner.
      Moreover, the type of gateway the user may create is constrainted by 
      its access rights (set in a previous call to set_volume_access()).
      
      A user may be subject to a quota enforced for each type of Gateway.
      
   Remarks:
      Syndicate takes all of the keyword arguments and generates a keyword 
      argument called 'gateway_cert_b64', which is a base64-encoded serialized
      protobuf string that contains all of the above keyword arguments plus
      the user's signature over them.  It also contains 'cert_bundle_b64', 
      which is a base64-encoded serialized protobuf string that contains 
      the current volume certificate version vector (which must be updated 
      by the volume owner whenever a gateway is added or removed.
      
      The kw args optionally contains 'driver_text' which contains the
      JSON-serialized driver.
   """
   return storage.create_gateway( **kw  )
   

@Authenticate()
@ReadAPIGuard( Gateway, parse_args=Gateway.ParseArgs )
def read_gateway( name ):
   """
   Read a gateway.
   
   Positional arguments:
      name (str):
         The name of the desired gateway.

   Returns:
      On success, the gateway data.
      Raises an exception on error.
   
   Authorization:
      An administrator can read any gateway.
      A user can only read gateways (s)he owns.
   """
   
   return storage.read_gateway( name )


@Authenticate()
@UpdateAPIGuard( Gateway, target_object_name="name", parse_args=Gateway.ParseArgs, caller_user="caller_user" )
def update_gateway( name, **attrs ):
   """
   Update a gateway.
   
   Positional arguments:
      name (str):
         The name of the gateway to update.
      
   Optional keyword arguments:
      host=str:
         The hostname that should be resolved to contact this gateway.
      
      port=int:
         The port number this gateway should listen on.
      
      cert_expires=int:
         Date when this gateway's certificate expires, in seconds
         since the epoch.
         
      driver=str:
         This is serialized JSON string that contains this gateway's 
         driver logic.  The contents are specific to the gateway 
         implementation. 
     
      private_key=str:
         This is a PEM-encoded private key for the gateway.  Pass "auto"
         to generate one automatically.

      caps=str|int:
         This is the capabilities string (or value) for this gateway.
         Capabilities are a bit-field of the following:
         
         GATEWAY_CAP_READ_DATA          Gateway can read data
         GATEWAY_CAP_WRITE_DATA         Gateway can write data
         GATEWAY_CAP_READ_METADATA      Gateway can read metadata
         GATEWAY_CAP_WRITE_METADATA     Gateway can write metadta
         GATEWAY_CAP_COORDINATE         Gateway can coordinate writes
         
         The volume owner sets a whitelist of allowed capabilities in
         create_gateway.  The user can only enable these white-listed 
         capabilities.  Only the volume owner can change the capability
         white-list.
         
   Returns:
      On success, this method returns True.
      Raises an exception on error.
   
   Authorization:
      An administrator can update any gateway.
      A volume owner can update any gateway in the volumes (s)he owns.
      A user cannot update the gateway.
      
   Remarks:
      Syndicate sends 'gateway_cert_b64' as a keyword argument that contains 
      the user-signed base64-encoded gateway certificate which contains all of the 
      keyword arguments.  Syndicate will merge the keyword arguments with the 
      gateway certificate stored locally.  If the gateway certificate is not 
      stored locally, it will try to use the cached copy on the MS.
      
      Syndicate may optionally send 'cert_bundle_b64' as a keyword argument as well,
      which will contain a serialized, volume owner-signed certificate bundle 
      for all gateways in the volume.  This will only be passed if the volume 
      owner is altering a gateway's capability whitelist.
      
      In addition, kw may contain 'driver_text'--the JSON-encoded driver for 
      the gateway--as well as 'cert_bundle_b64' (the new volume cert 
      bundle version vector, base64-encoded).
   """
   return storage.update_gateway( name, **attrs )


@Authenticate()
@DeleteAPIGuard( Gateway, target_object_name="name", parse_args=Gateway.ParseArgs )
def delete_gateway( name, **kw ):
   """
   Delete a gateway.
   
   Positional arguments:
      name (str):
         The name of the gateway to delete.
   
   Returns:
      True on success.
      Raises an exception on error.
   
   Authorization:
      An administrator can delete any gateway.
      A volume oner can delete any gateway in the volumes (s)he owns.
      A user cannot delete the gateway.
      
   Remarks:
      Syndicate should generate 'cert_bundle_b64'
      as a keyword argument, which contains the base64-encoded serialized 
      cert bundle version vector for this volume.
   """
   
   return storage.delete_gateway( name, **kw )  


@Authenticate()
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

   # don't show deleted gateways
   query_attrs["Gateway.deleted =="] = False
   return storage.list_gateways( query_attrs )


@Authenticate()
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs, caller_user="caller_user" )
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


@Authenticate()
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


@Authenticate()
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs, caller_user="caller_user" )
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


@Authenticate()
@ListAPIGuard( Gateway, parse_args=Gateway.ParseArgs, caller_user="caller_user" )
def list_gateways_by_user_and_volume( email, volume_name_or_id, **caller_user_dict ):
   """
   List gateways in a Volume that belong to a particular user.
   
   Positional arguments:
      email (str or int):
         The email of the user to query.
         
      volume_name_or_id (str or int)
         The name or ID of the Volume to query on.
         
   Returns:
      On success, a list of gateways belonging to a particular user
      in a particular Volume.
      Raises an exception on failure.
      
   Authorization:
      An administrator can list any Volume's gateways, and any user's gateways.
      A user can only list their own gateways.
   """
   return storage.list_gateways_by_user_and_volume( email, volume_name_or_id, **caller_user_dict )


@Authenticate()
@DeleteAPIGuard( Volume, target_object_name="volume_name_or_id", parse_args=Volume.ParseArgs )        # caller must own the Volume
def remove_user_from_volume( email, volume_name_or_id ):
   """
   Remove a user from a volume.  This removes the user's volume
   access requests, and deletes every gateway in 
   the volume owned by the user.
   
   Positional Arguments:
      email (str or int):
         The email of the user to remove 
         
      volume_name_or_id (str or int):
         The name or ID of the Volume to remove the user from.
         
   Returns:
      On success, this returns True.
      Raises an exception on failure.
     
   Authorization:
      An administrator can remove any user from any volume.
      A volume owner can remove any user from a volume (s)he owns.
      A user cannot call this method.
   """
   
   return storage.remove_user_from_volume( email, volume_name_or_id )

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
         log.error("Invalid method '%s'" % method)
         return False
      
      username = syndicate_data.get("username")
      sig = syndicate_data.get("signature")
      
      authenticated_user = SyndicateUser.Authenticate( username, request_body, sig )
         
      if not authenticated_user:
          # failed to authenticate via keys.
          log.error("Failed to authenticate user %s" % (username))
          return False 
         
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
