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
   Mark a Volume name as taken
   '''
   
   name = storagetypes.String()
   owner_id = storagetypes.Integer()
   
   required_attrs = [
      "name"
   ]
   
   
   @classmethod
   def make_key_name( cls, name ):
      return "SyndicateUserNameHolder: name=%s" % (name)
   
   @classmethod
   def create_async( cls,  _name, _id ):
      return SyndicateUserNameHolder.get_or_insert_async( SyndicateUserNameHolder.make_key_name( _name ), name=_name, owner_id=_id )



class SyndicateUser( storagetypes.Object ):
   email = storagetypes.String()         # used as the username
   owner_id = storagetypes.Integer()     # UID field in Syndicate
   openid_url = storagetypes.Text()      # OpenID identifying URL
   volumes_o = storagetypes.Integer( repeated=True ) # Owned volumes
   volumes_r = storagetypes.Integer( repeated=True ) # Readable volumes
   volumes_rw = storagetypes.Integer( repeated=True ) # R/Writable volumes
   
   
   required_attrs = [
      "email",
      "openid_url"
   ]

   key_attrs = [
      "email"
   ]

   default_values = {
      "volumes_o" : (lambda cls, attrs: []),
      "volumes_r" : (lambda cls, attrs: []),
      "volumes_rw" : (lambda cls, attrs: []),
   }

   validators = {
      "email" : (lambda cls, value: valid_email(cls, value))
   }

   read_attrs = [
      "email",
      "owner_id",
      "openid_url"
   ]
   
   write_attrs = [
      "openid_url",
      "volumes_o",
      "volumes_r",
      "volumes_rw"
   ]
   

   @classmethod
   def Create( cls, **kwargs ):
      """
      Create a SyndicateUser.
      
      Required keyword arguments:
      email             -- Email address of the user.  Serves as the username (str)
      openid_url        -- OpenID identifier for authenticating this user (str)
      """
      
      SyndicateUser.fill_defaults( kwargs )
      missing = SyndicateUser.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = SyndicateUser.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )))


      email = kwargs.get( "email" )
      openid_url = kwargs.get( "openid_url" )
      owner_id = random.randint( 1, 2**63 - 1 )
      
      user_key_name = SyndicateUser.make_key_name( email=email )
      user = SyndicateUser.get_or_insert( user_key_name, email=email, openid_url=openid_url, owner_id=owner_id )
      
      # check for collisions
      if user.owner_id != owner_id:
         # collision
         raise Exception("User '%s' already exists" % email)
      
      return user.key
      

   @classmethod
   def Read( cls, email ):
      """
      Read a SyndicateUser
      
      Arguments:
      email             -- Email address of the user to read (str)
      """
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = storagetypes.make_key( SyndicateUser, user_key_name )

      user = storagetypes.memcache.get( user_key_name )
      if user == None:
         user = user_key.get( use_memcache=False )
         if not user:
            return None
         else:
            storagetypes.memcache.set( user_key_name, user )
            
      return user


   @classmethod
   def add_volume_to_user( cls, volume_id, email ):
      """
      Make a SyndicateUser the owner of a Volume, and give the user read/write permission in it.
      Unless the caller knows what its doing, this should be run as a transaction.
      
      Arguments:
      volume_id         -- ID of the Volume to add to the user (int)
      email             -- Email address of the user to own the Volume (str)
      """
      user = SyndicateUser.Read( email )

      # only put the user if there is a change.
      diff = False
      if volume_id not in user.volumes_o:
         user.volumes_o.append( volume_id )
         diff = True

      if volume_id not in user.volumes_rw:
         user.volumes_rw.append( volume_id )
         diff = True

      if diff:
         user_key_name = SyndicateUser.make_key_name( email=email )
         storagetypes.memcache.delete( user_key_name )
         return user.put()

      return None
      
      
   @classmethod
   def Update( cls, email, **fields ):
      '''
      Atomically (transactionally) update a SyndicateUser with the new fields.
      
      Arguments:
      email             -- Email address of the user to update (str)
      
      Keyword arguments:
      openid_url        -- OpenID URL to authenticate this user (str)
      volumes_o         -- list of Volume IDs this user owns ([int])
      volumes_rw        -- list of Volume IDs this user can read/write ([int])
      volumes_r         -- list of Volume IDs this user can read ([int])
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
   def Delete( cls, email, get_volumes=True ):
      '''
      Delete a SyndicateUser
      
      Arguments:
      email             -- Email of the user to delete (str)
      
      Keyword arguments:
      get_volumes       -- If true, return the list of Volumes this SyndicateUser owns
                           (this will cause the operation to run a transaction).
      '''
      
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = make_key( SyndicateUser, user_key_name )
      
      def delete_func( user_key ):
         
         user = user_key.get()
      
         if user == None:
            # done!
            return True
      
         volume_ids = user.volumes_o
         
         user_key.delete()
         return
      
      
      if get_volumes:
         # transactionally delete and get Volume IDs
         volume_ids = storagetypes.transaction( lambda: delete_func( user_key ) )
         return volume_ids
         
      else:
         user_key.delete()
         return True

   
   
