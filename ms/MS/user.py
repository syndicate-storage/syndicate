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

from common.msconfig import *

valid_email = None

try:
   # build an e-mail validator from Django
   from django import forms

   def _valid_email( cls, email ):
      f = forms.EmailField()

      try:
         f.clean( email )
         return True
      except:
         return False

   valid_email = _valid_email

except ImportError:
   try:
      # build an e-mail validator from GAE's Mail API
      from google.appengine.api import mail

      def _valid_email( cls, email ):
         return mail.is_email_valid( email )

      valid_email = _valid_email

   except ImportError:
      raise Exception("No way to validate e-mails!")
   



class SyndicateUIDCounter( storagetypes.Object ):
   value = storagetypes.Integer()

   required_attrs = [
      "value"
   ]

   default_values = {
      "value" : (lambda cls, attrs: 1000)
   }

   @classmethod
   def make_key_name( cls, **args ):
      return "SyndicateUIDCounter"


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
   
   email = storagetypes.String()         # used as the username
   owner_id = storagetypes.Integer()     # UID field in Syndicate
   openid_url = storagetypes.Text()      # OpenID identifying URL
   
   max_volumes = storagetypes.Integer( default=10 )     # how many Volumes can this user create? (-1 means unlimited)
   max_UGs = storagetypes.Integer( default=10 )         # how many UGs can this user create?
   max_RGs = storagetypes.Integer( default=10 )         # how many RGs can this user create?
   max_AGs = storagetypes.Integer( default=10 )         # how many AGs can this user create?
   max_requests = storagetypes.Integer( default=10 )    # how many pending Volume requests can this user create?
   
   is_admin = storagetypes.Boolean( default=False, indexed=False )      # is this user an administrator?
   
   signing_public_key = storagetypes.Text()     # PEM-encoded public key for authenticating this user, or "unset" if it is not set.
   signing_public_key_expiration = storagetypes.Integer( default=-1 )           # seconds since the epoch
   
   # keys for signing responses to remote callers
   verifying_public_key = storagetypes.Text()
   verifying_private_key = storagetypes.Text()
   
   # one-time password for setting the signing public key
   set_signing_public_key_password_salt = storagetypes.String()         # 32 bytes, but encoded as a hex string
   set_signing_public_key_password_hash = storagetypes.String()         # SHA256
   
   # for RPC
   key_type = "user"
   
   required_attrs = [
      "email",
      "openid_url",
      "signing_public_key_expiration"
   ]

   key_attrs = [
      "email"
   ]

   default_values = {
      "max_volumes": (lambda cls, attrs: 10),
      "max_UGs": (lambda cls, attrs: 10),
      "max_RGs": (lambda cls, attrs: 10),
      "max_AGs": (lambda cls, attrs: 10),
      "is_admin": (lambda cls, attrs: False),
      "openid_url": (lambda cls, attrs: ""),
      "signing_public_key_expiration": (lambda cls, attrs: -1)
   }

   validators = {
      "email" : (lambda cls, value: valid_email(cls, value)),
      "signing_public_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
      "verifying_public_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
      "verifying_private_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
      "openid_url": (lambda cls, value: len(value) < 4096),              # not much of a check here...
      "set_signing_public_key_password_salt": (lambda cls, value: len(str(value).translate( None, "0123456789abcdefABCDEF" )) == 0 and
                                                                  len(str(value)) == 64),      # 32-byte salt, encoded as a hex number
      
      "set_signing_public_key_password_hash": (lambda cls, value: len(str(value).translate( None, "0123456789abcdefABCDEF" )) == 0 and
                                                                  len(str(value)) == 64)      # SHA256: 32-byte hash, encoded as a hex number
   }

   read_attrs_api_required = [
      "email",
      "owner_id",
      "openid_url",
      "max_volumes",
      "max_UGs",
      "max_RGs",
      "max_AGs",
      "signing_public_key",
      "signing_public_key_expiration",
      "verifying_public_key"
   ]
   
   read_attrs = read_attrs_api_required
   
   
   write_attrs_api_required = [
      "openid_url"
   ]
   
   write_attrs_admin_required = [
      "max_volumes",
      "max_UGs",
      "max_RGs",
      "max_AGs",
      "is_admin"
   ]
   
   write_attrs = write_attrs_api_required + write_attrs_admin_required
   
   def owned_by( self, user ):
      return user.owner_id == self.owner_id
   
   @classmethod
   def Authenticate( cls, email, data, data_signature ):
      """
      Authenticate a user, given some identifying data and its signature.
      Verify that it was signed by the user's private key.
      Use RSA PSS for security.
      """
      user = SyndicateUser.Read( email )
      if user == None:
         return None
      
      ret = cls.auth_verify( user.signing_public_key, data, data_signature )
      if not ret:
         logging.error("Verification failed")
         return False
      
      else:
         return user

   def makeCert( self ):
      ret = {}
      ret['expires'] = self.signing_public_key_expiration
      ret['pubkey'] = self.signing_public_key
      ret['email'] = self.email
      ret['openid_url'] = self.openid_url
      
      return ret

   @classmethod
   def Sign( cls, user, data ):
      # Sign an API response
      return SyndicateUser.auth_sign( user.verifying_private_key, data )
   

   @classmethod
   def Create( cls, email, **kwargs ):
      """
      Create a SyndicateUser.
      
      Required arguments:
      email                 -- Email address of the user.  Serves as the username (str)
      openid_url            -- OpenID identifier for authenticating this user (str)
      verifying_private_key -- PEM-encoded RSA-4096 private key (str)
      """
      
      kwargs['email'] = email
      
      # sanity check
      SyndicateUser.fill_defaults( kwargs )
      
      # extract public keys from private ones 
      SyndicateUser.extract_keys( 'verifying_public_key', 'verifying_private_key', kwargs, USER_RSA_KEYSIZE )
      
      # if we're given a signing public key, then set it. 
      # otherwise, use the given salted password hash.
      skip_verify = []
      if kwargs.has_key('set_signing_public_key_password_hash') and kwargs.has_key('set_signing_public_key_password_salt'):
         # don't check for this
         skip_verify = ['signing_public_key']
         kwargs['signing_public_key'] = "unset"
      
      elif kwargs.has_key('signing_public_key'):
         # this had better be a valid key
         if not SyndicateUser.validators['signing_public_key']( SyndicateUser, kwargs['signing_public_key'] ):
            raise Exception("Invalid field: %s" % 'signing_public_key')
         
         # don't check for password hash and salt
         skip_verify = ['set_signing_public_key_password_hash', 'set_signing_public_key_password_salt']
         
      else:
         # need either of these...
         raise Exception("Need either signing_public_key or (set_signing_public_key_password_hash, set_signing_public_key_password_salt)")
         
      missing = SyndicateUser.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = SyndicateUser.validate_fields( kwargs, skip=skip_verify )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )))

      user_key_name = SyndicateUser.make_key_name( email=email )
      user = storagetypes.memcache.get( user_key_name )
      if user == None:
         user_key = storagetypes.make_key( SyndicateUser, user_key_name )
         user = user_key.get()
         
         if user == None:
            
            # do not allow admin privileges
            kwargs['is_admin'] = False
            kwargs['owner_id'] = random.randint( 1, 2**63 - 1 )
            user_key_name = SyndicateUser.make_key_name( email=email )
            
            user = SyndicateUser.get_or_insert( user_key_name, **kwargs)
            
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
   def CreateAdmin( cls, email, openid_url, signing_public_key, verifying_private_key ):
      """
      Create the Admin user.  NOTE: this will be called repeatedly, so use memcache
      """
      user_key_name = SyndicateUser.make_key_name( email=email )
      user = storagetypes.memcache.get( user_key_name )
      
      if user == None:
         user_key = storagetypes.make_key( SyndicateUser, user_key_name )
         user = user_key.get()
         
         if user == None:
            # admin does not exist
            attrs = {}
            
            logging.info("Generating admin '%s'" % email)
            
            # extract verifying keys
            attrs['verifying_private_key'] = verifying_private_key
            SyndicateUser.extract_keys( 'verifying_public_key', 'verifying_private_key', attrs, USER_RSA_KEYSIZE )
            
            # fill defaults
            SyndicateUser.fill_defaults( attrs )
            
            attrs['email'] = email
            attrs['openid_url'] = openid_url
            attrs['owner_id'] = random.randint( 1, 2**63 - 1 )
            attrs['is_admin'] = True
            attrs['signing_public_key'] = signing_public_key
            
            invalid = SyndicateUser.validate_fields( attrs )
            if len(invalid) != 0:
               raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )))
         
            user = SyndicateUser.get_or_insert( user_key_name, **attrs )
         
            # check for collisions
            if user.owner_id != attrs['owner_id']:
               # collision
               logging.warning("Admin '%s' already exists" % email)
         
         storagetypes.memcache.set( user_key_name, user )
         
      return user.key
      

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
      user_id_to_email_cached = None
      
      if use_memcache:
         user_id_to_email_cached = "Read_ByOwnerID: owner_id=%s" % owner_id
         user_email = storagetypes.memcache.get( user_id_to_email_cached )
         
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
               user_id_to_email_cached: user.email,
               SyndicateUser.make_key_name( email=user.email ): user
            }
            
            storagetypes.memcache.set_multi( to_set )

      else:
         user = None 
         
      return user
      

   @classmethod
   def Update( cls, email, **fields ):
      '''
      Atomically (transactionally) update a SyndicateUser with the new fields.
      
      Positionl arguments:
      email             -- Email address of the user to update (str)
      
      Keyword arguments:
      openid_url        -- OpenID URL to authenticate this user (str)
      '''
      
      def update_txn( email, **fields ):
         user = SyndicateUser.Read(email)
         user_key_name = SyndicateUser.make_key_name( email=email)
         storagetypes.memcache.delete( user_key_name )

         for (k,v) in fields.items():
            setattr(user, k, v )
         return user.put()
      
      # sanity check
      invalid = SyndicateUser.validate_fields( fields )
      if len(invalid) > 0:
         raise Exception( "Invalid fields: %s" % (', '.join( invalid )) )
      
      invalid = SyndicateUser.validate_write( fields )
      if len(invalid) > 0:
         raise Exception( "Unwritable fields: %s" % (', '.join( invalid )) )
      
      return storagetypes.transaction( lambda: update_txn( email, **fields ) )
      
      
   def is_signing_public_key_set( self ):
      return self.signing_public_key != "unset"


   @classmethod
   def SetPublicSigningKey( cls, email, new_public_key, set_signing_public_key_password, override=False ):
      """
      Set the authenticator public key for this user
      """
      if not cls.is_valid_key( new_public_key, USER_RSA_KEYSIZE ):
         raise Exception("Invalid signing public key")
      
      # the key can only be set *once*
      user = SyndicateUser.Read( email )
      if user is None:
         raise Exception("No such user %s" % email)
      
      if not override and user.is_signing_public_key_set():
         raise Exception("Signing public key is already set.  Contact the administrator to clear it.")
      
      # verify password
      import common.api as api
      rc = api.check_password( set_signing_public_key_password, user.set_signing_public_key_password_salt, user.set_signing_public_key_password_hash )
      if not rc:
         raise Exception("Invalid password")
      
      user.signing_public_key = new_public_key
      user.put()
      
      user_key_name = SyndicateUser.make_key_name( email=email )
      storagetypes.memcache.delete( user_key_name )
      
      return True
      

   @classmethod
   def ResetPublicSigningKey( cls, email, password_salt, password_hash ):
      """
      Reset a user's public signing key.
      """
      
      user = SyndicateUser.Read( email )
      if user is None:
         raise Exception("No such user %s" % email)
      
      invalid = SyndicateUser.validate_fields( {"set_signing_public_key_password_hash": password_hash, "set_signing_public_key_password_salt": password_salt} )
      if len(invalid) != 0:
         raise Exception( "Invalid arguments: %s" % (", ".join( invalid )) )
      
      user.signing_public_key = "unset"
      user.set_signing_public_key_password_hash = password_hash
      user.set_signing_public_key_password_salt = password_salt
   
      user.put()
      
      user_key_name = SyndicateUser.make_key_name( email=email )
      storagetypes.memcache.delete( user_key_name )
      
      return True
   
      
   @classmethod
   def Delete( cls, email ):
      '''
      Delete a SyndicateUser
      
      Arguments:
      email             -- Email of the user to delete (str)
      '''
      
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = storagetypes.make_key( SyndicateUser, user_key_name )
      
      def delete_func( user_key ):
         
         user = user_key.get()
      
         if user == None:
            # done!
            return True
         
         user_key.delete()
         return
      
      user_key.delete()
      storagetypes.memcache.delete( user_key_name )
      return True

   
   def get_gateway_quota( self, gateway_type ):
      if gateway_type == GATEWAY_TYPE_UG:
         return self.max_UGs
      elif gateway_type == GATEWAY_TYPE_RG:
         return self.max_RGs
      elif gateway_type == GATEWAY_TYPE_AG:
         return self.max_AGs
      else:
         raise Exception("Unknown Gateway type '%s'" % gateway_type)
      
   def get_volume_quota( self ):
      return self.max_volumes

   def get_request_quota( self ):
      return self.max_requests
   