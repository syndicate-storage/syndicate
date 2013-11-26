#!/usr/bin/pyhon

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storagetypes as storagetypes

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import logging

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
   
   is_admin = storagetypes.Boolean( default=False, indexed=False )      # is this user an administrator?
   
   signing_public_key = storagetypes.Text()     # PEM-encoded public key for authenticating this user
   
   # keys for signing responses to remote callers
   verify_public_key = storagetypes.Text()
   verify_private_key = storagetypes.Text()
   
   required_attrs = [
      "email",
      "openid_url",
      "signing_public_key"
   ]

   key_attrs = [
      "email"
   ]

   default_values = {
      "max_volumes": (lambda cls, attrs: 10),
      "max_UGs": (lambda cls, attrs: 10),
      "max_RGs": (lambda cls, attrs: 10),
      "max_AGs": (lambda cls, attrs: 10),
      "is_admin": (lambda cls, attrs: False)
   }

   validators = {
      "email" : (lambda cls, value: valid_email(cls, value)),
      "signing_public_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
      "verify_public_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE )),
      "verify_private_key": (lambda cls, value: cls.is_valid_key( value, USER_RSA_KEYSIZE ))
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
      "verify_public_key"
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


   @classmethod
   def Sign( cls, user, data ):
      # Sign an API response
      return SyndicateUser.auth_sign( user.verify_private_key, data )
   

   @classmethod
   def Create( cls, email, **kwargs ):
      """
      Create a SyndicateUser.
      
      Required arguments:
      email                 -- Email address of the user.  Serves as the username (str)
      openid_url            -- OpenID identifier for authenticating this user (str)
      signing_public_key        -- PEM-encoded RSA-4096 public key (str)
      """
      
      kwargs['email'] = email
      
      # sanity check
      SyndicateUser.fill_defaults( kwargs )
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
            
            # do not allow admin privileges
            kwargs['is_admin'] = False
            
            # generate API keys if the caller didn't supply any
            if not kwargs.has_key("verify_public_key") or not kwargs.has_key("verify_private_key"):
               kwargs['verify_public_key'], kwargs['verify_private_key'] = SyndicateUser.generate_keys( USER_RSA_KEYSIZE )
            
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
   def CreateAdmin( cls, email, openid_url, signing_public_key ):
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
            
            # generate API keys
            verify_public_key_str, verify_private_key_str = SyndicateUser.generate_keys( USER_RSA_KEYSIZE )
            attrs['verify_public_key'] = verify_public_key_str
            attrs['verify_private_key'] = verify_private_key_str
         
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
      

   @classmethod
   def SetPublicSigningKey( cls, email, new_public_key ):
      """
      Set the authenticator public key for this user
      """
      if not cls.is_valid_key( new_public_key ):
         raise Exception("Invalid authentication key")
      
      cls.set_atomic( lambda: SyndicateUser.Read( email ), signing_public_key=new_public_key )
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
