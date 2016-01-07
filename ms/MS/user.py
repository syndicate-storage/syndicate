#!/usr/bin/pyhon

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

import storage.storagetypes as storagetypes

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import logging
import hashlib
import re
import base64

from common.msconfig import *
from common.admin_info import *

email_regex_str = r"^(?=^.{1,256}$)(?=.{1,64}@)(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22)(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|\x22(?:[^\x0d\x22\x5c\x80-\xff]|\x5c[\x00-\x7f])*\x22))*\x40(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d])(?:\x2e(?:[^\x00-\x20\x22\x28\x29\x2c\x2e\x3a-\x3c\x3e\x40\x5b-\x5d\x7f-\xff]+|[\x5b](?:[^\x0d\x5b-\x5d\x80-\xff]|\x5c[\x00-\x7f])*[\x5d]))*$"
   
email_regex = re.compile( email_regex_str )

def valid_email( email ):
   return (email_regex.match( email ) is not None)


class SyndicateUserNameHolder( storagetypes.Object ):
   '''
   Mark a SyndicateUser email as taken
   '''
   
   email = storagetypes.String()
   owner_id = storagetypes.Integer()
   
   required_attrs = [
      "email"
   ]
   
   
   @classmethod
   def make_key_name( cls, email ):
      return "SyndicateUserNameHolder: email=%s" % (email)
   
   @classmethod
   def create_async( cls,  _email, _id ):
      return SyndicateUserNameHolder.get_or_insert_async( SyndicateUserNameHolder.make_key_name( _email ), email=_email, owner_id=_id )


class SyndicateUser( storagetypes.Object ):
   
   email = storagetypes.String()         # used as the username to Syndicate
   owner_id = storagetypes.Integer()     # numeric ID for gateways
   admin_id = storagetypes.Integer()     # which admin made this user?
   
   max_volumes = storagetypes.Integer( default=10 )     # how many Volumes can this user create? (-1 means unlimited)
   max_gateways = storagetypes.Integer( default=10 )    # how many gateways can this user create?  (-1 means unlimited)
   
   is_admin = storagetypes.Boolean( default=False, indexed=False )      # is this user an administrator?
   
   public_key = storagetypes.Text()     # PEM-encoded public key for authenticating this user, or USER_KEY_UNSET if it is not set, or USER_KEY_UNUSED if it will not be used
   
   user_cert_protobuf = storagetypes.Blob()     # protobuf'ed certificate for this user
   signature = storagetypes.Blob()              # signature over the data used to generate this record
   
   # for RPC
   key_type = "user"
   
   required_attrs = [
      "email",
      "public_key"
   ]

   key_attrs = [
      "email"
   ]

   default_values = {
      "max_volumes": (lambda cls, attrs: 10),
      "max_gateways": (lambda cls, attrs: 10),
      "is_admin": (lambda cls, attrs: False),
   }

   validators = {
      "email" : (lambda cls, value: valid_email(value)),
      "public_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
   }
   
   read_attrs_api_required = [
      "email",
      "owner_id",
      "max_volumes",
      "max_gateways",
      "public_key",
   ]
   
   read_attrs = read_attrs_api_required
   
   
   write_attrs_api_required = [
      "public_key",
   ]
   
   write_attrs_admin_required = [
      "max_volumes",
      "max_gateways",
      "is_admin"
   ]
   
   write_attrs = write_attrs_api_required + write_attrs_admin_required
   
   def owned_by( self, user ):
      return user.owner_id == self.owner_id
   
   
   @classmethod
   def Authenticate( cls, email, data, data_signature ):
      """
      Authenticate a user via public-key cryptography.
      Verify that data was signed by the user's private key, given the signature and data.
      (use RSA PSS for security).
      Return the user on success; False on authentication error; None if the user doesn't exist
      """
      user = SyndicateUser.Read( email )
      if user == None:
         return None
      
      ret = cls.auth_verify( user.public_key, data, data_signature )
      if not ret:
         logging.error("Verification failed for %s" % email)
         return False
      
      else:
         return user


   @classmethod 
   def cert_to_dict( cls, user_cert ):
      
      attrs = {
         'email': str(user_cert.email),
         'owner_id': user_cert.user_id,
         'public_key': str(user_cert.public_key),
         'admin_id': user_cert.admin_id,
         'max_volumes': user_cert.max_volumes,
         'max_gateways': user_cert.max_gateways,
         'is_admin': user_cert.is_admin,
         'signature': str(user_cert.signature),
         'user_cert_protobuf': user_cert.SerializeToString()
      }
      
      return attrs

   """
   def makeCert( self ):
      ret = {}
      ret['expires'] = self.public_key_expiration
      ret['pubkey'] = self.public_key
      ret['email'] = self.email
      ret['openid_url'] = self.openid_url
      
      return ret
   """

   @classmethod
   def Create( cls, user_cert ):
      """
      Create a SyndicateUser from a user_cert.
      
      NOTE: the caller will need to have validated the user cert
      """
      
      kwargs = cls.cert_to_dict( user_cert )
      email = kwargs['email']
      
      missing = SyndicateUser.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = SyndicateUser.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )))

      user_key_name = SyndicateUser.make_key_name( email=email )
      user = storagetypes.memcache.get( user_key_name )
      if user == None:
         
         user_key = storagetypes.make_key( SyndicateUser, user_key_name )
         user = user_key.get()
         
         if user == None:
            
            # create!
            user = SyndicateUser.get_or_insert( user_key_name, **kwargs )
            
            # check for collisions
            if user.owner_id != kwargs['owner_id']:
               # collision
               raise Exception("User '%s' already exists" % email)
      
            return user.key
         
         else:
            raise Exception("User '%s' already exists" % email)
      
      else:
         raise Exception("User '%s' already exists" % email)
      
   
   @classmethod 
   def CreateAdmin( cls, email, owner_id, public_key, syndicate_private_key ):
      """
      Create the admin user.
      Called when the MS initializes itself for the first time 
      """
      
      import common.api as api 
      
      admin_cert = ms_pb2.ms_user_cert()
      
      admin_cert.user_id = owner_id 
      admin_cert.email = email 
      admin_cert.public_key = public_key 
      admin_cert.admin_id = owner_id
      admin_cert.max_volumes = -1 
      admin_cert.max_gateways = -1 
      admin_cert.is_admin = True 
      admin_cert.signature = "" 
      
      admin_cert_str = admin_cert.SerializeToString()
      
      sig = api.sign_data( syndicate_private_key, admin_cert_str )
      
      admin_cert.signature = base64.b64encode( sig )
      
      return SyndicateUser.Create( admin_cert )
      

   @classmethod
   def Read( cls, email_or_owner_id, async=False ):
      """
      Read a SyndicateUser
      
      Arguments:
      email_or_owner_id         -- Email address of the user to read, or the owner ID (str or int)
      """
      owner_id = None
      email = None
      
      try:
         owner_id = int(email_or_owner_id)
      except:
         email = email_or_owner_id
      
      if owner_id != None:
         return cls.Read_ByOwnerID( owner_id, async=async )
      
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = storagetypes.make_key( SyndicateUser, user_key_name )

      user = storagetypes.memcache.get( user_key_name )
      if user == None:
         if async:
            return user_key.get_async( use_memcache=False )
         
         else:
            user = user_key.get( use_memcache=False )
            if not user:
               return None
            else:
               storagetypes.memcache.set( user_key_name, user )

      elif async:
         user = storagetypes.FutureWrapper( user )
         
      return user


   @classmethod 
   def Read_ByOwnerID( cls, owner_id, async=False, use_memcache=True ):
      """
      Read a SyndicateUser
      
      Arguments:
      owner_id
      """
      owner_id_to_email_cached = None
      
      if use_memcache:
         owner_id_to_email_cached = "Read_ByOwnerID: owner_id=%s" % owner_id
         user_email = storagetypes.memcache.get( owner_id_to_email_cached )
         
         if user_email != None and isinstance( user_email, str ):
            user = SyndicateUser.Read( user_email, async=async, use_memcache=use_memcache )
            return user
         
      # no dice 
      user = SyndicateUser.ListAll( {"SyndicateUser.owner_id ==": owner_id}, async=async )
      
      if async:
         # this will be a Future 
         return storagetypes.FutureQueryWrapper( user )

      elif user:
         if len(user) > 1:
            # something's wrong...there should only be one
            raise Exception("Multiple SyndicateUsers with ID '%s'" % (owner_id))
         
         user = user[0]
         if not user:
            user = None
            
         elif use_memcache:
            to_set = {
               owner_id_to_email_cached: user.email,
               SyndicateUser.make_key_name( email=user.email ): user
            }
            
            storagetypes.memcache.set_multi( to_set )

      else:
         user = None 
         
      return user
      
   '''
   @classmethod
   def Update( cls, email, **fields ):
      """
      Update a SyndicateUser.
      
      TODO: finish
      """
      
      def verify_auth_change( user, attrs ):
         if not user.active:
            raise Exception("Account for %s is not active.  Please activate it." % user.email )
         
         if attrs.has_key('allow_password_auth'):
            # this can only be disabled
            if attrs['allow_password_auth']:
               raise Exception("For security reasons, re-enabling password authentication is forbidden.  Reset the account for %s instead." % user.email)
            
         if attrs.has_key('public_key'):
            # can't unset this if password auth is disabled
            if not user.allow_password_auth and not SyndicateUser.is_public_key_set( attrs['public_key'] ):
               raise Exception("Cannot disable public-key authentication once password authentication is disabled.")
            
         return True
         
      def update_txn( email, attrs ):
         user = SyndicateUser.Read(email)
         if user is None:
            raise Exception("No such user %s" % email)
         
         verify_auth_change( user, attrs )
         
         user_key_name = SyndicateUser.make_key_name( email=email)
         storagetypes.memcache.delete( user_key_name )

         for (k,v) in attrs.items():
            setattr(user, k, v )
         return user.put()
      
      # sanity check
      invalid = SyndicateUser.validate_fields( fields )
      if len(invalid) > 0:
         raise Exception( "Invalid fields: %s" % (', '.join( invalid )) )
      
      invalid = SyndicateUser.validate_write( fields )
      if len(invalid) > 0:
         raise Exception( "Unwritable fields: %s" % (', '.join( invalid )) )
      
      return storagetypes.transaction( lambda: update_txn( email, fields ) )
   '''
   
   '''
   @classmethod
   def is_public_key_set( cls, public_key ):
      return public_key != 'unset'
   '''

   '''
   @classmethod
   def Register( cls, activate_password, user_cert ):
      """
      Activate an account, and use the given cert henceforth for this user.
      
      NOTE: the caller will need to have validated the cert beforehand.
      """
      import common.api as api 
      
      # can only activate once 
      user = SyndicateUser.Read( user_cert.email )
      if user is None:
         raise Exception("No such user '%s'" % user_cert.email )
      
      if user.active:
         # already activated 
         raise Exception("User '%s' is already registered" % user_cert.email )
      
      # get the on-file cert
      local_user_cert = ms_pb2.ms_user_cert.ParseFromString( user.user_cert_protobuf )
      
      # verify password against the on-file cert
      rc = api.check_password( activate_password, local_user_cert.password_salt, local_user_cert.password_hash )
      if not rc:
         raise Exception("Invalid password")
      
      # good to go.  overwrite this user
      user_attrs = cls.cert_to_dict( user_cert )
      user_attrs['active'] = True 
      
      user_key_name = SyndicateUser.make_key_name( email=email )
      user_key = SyndicateUser.put( user_key_name, **user_attrs )
      
      storagetypes.memcache.delete( user_key_name )
      
      return user_key.get()
   '''

   @classmethod
   def Reset( cls, user_cert ):
      """
      Reset a user's account credentials.
      
      NOTE: the caller will need to have validated the cert beforehand
      """
      
      user = SyndicateUser.Read( user_cert.email )
      if user is None:
         raise Exception("No such user '%s'" % user_cert.email )
      
      # the cert will have the new hash and salt
      user_attrs = cls.cert_to_dict( user_cert )
      
      user_key_name = SyndicateUser.make_key_name( email=email )
      SyndicateUser.put( user_key_name, **user_attrs )
      
      storagetypes.memcache.delete( user_key_name )
      
      return True 
   
      
   @classmethod
   def Delete( cls, email ):
      """
      Delete a SyndicateUser
      """
      
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = storagetypes.make_key( SyndicateUser, user_key_name )
      
      user_key.delete()
      storagetypes.memcache.delete( user_key_name )
      return True

   
   def get_gateway_quota( self ):
      return self.max_gateways
      
   def get_volume_quota( self ):
      return self.max_volumes

   