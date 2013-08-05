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

      

class SyndicateUser( storagetypes.Object ):
   email = storagetypes.String()         # used as the username
   owner_id = storagetypes.Integer()     # UID field in Syndicate
   openid_url = storagetypes.Text()      # OpenID identifying URL
   volumes_o = storagetypes.Integer( repeated=True ) # Owned volumes
   volumes_r = storagetypes.Integer( repeated=True ) # Readable volumes
   volumes_rw = storagetypes.Integer( repeated=True ) # R/Writable volumes
   
   
   required_attrs = [
      "email",
      "openid_url",
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


   @classmethod
   def _create_user( cls, **kwargs ):
      """
      Given user data, store it.

      kwargs:
         email: str
         Open_ID_url: str
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

      user_key = storagetypes.make_key( SyndicateUser, SyndicateUser.make_key_name( email=email ) )
      user = user_key.get()
      if user != None and user.owner_id > 0:
         # user already exists
         raise Exception("User '%s' already exists" % email)

      else:
         uid_counter = SyndicateUIDCounter.get_or_insert( SyndicateUIDCounter.make_key_name(), value=0 )
         
         uid_counter.value += 1

         uid_future = uid_counter.put_async()

         # new user
         user = SyndicateUser(key=user_key,
                              owner_id=uid_counter.value,
                              email=email,
                              openid_url=openid_url)


         user_future = user.put_async()

         storagetypes.wait_futures( [user_future, uid_future] )

         return user.key


   @classmethod
   def Create( cls, **kwargs ):
      return storagetypes.transaction( lambda: SyndicateUser._create_user( **kwargs ), xg=True, retries=10 )


   @classmethod
   def Read( cls, email ):
      """
      Read a user, given the username
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
   def add_volume_to_owner( cls, volume_id, username ):
      """
      Update a SyndicateUser, so that the SyndicateUser owns the Volume and the Volume is put to the datastore.
      Run this in a transaction.
      """
      user = SyndicateUser.Read( username )

      diff = False
      if volume_id not in user.volumes_o:
         user.volumes_o.append( volume_id )
         diff = True

      if volume_id not in user.volumes_rw:
         user.volumes_rw.append( volume_id )
         diff = True

      if diff:
         user_key_name = SyndicateUser.make_key_name( email=username )
         storagetypes.memcache.delete( user_key_name )
         return user.put()

      return None
      
      
   @classmethod
   def Update( cls, email, **fields ):
      '''
      Update volume identified by name with fields specified as a dictionary.
      '''
      user = SyndicateUser.Read(email)
      user_key_name = SyndicateUser.make_key_name( email=email)
      storagetypes.memcache.delete( user_key_name )

      for (k,v) in fields.items():
         setattr(user, k, v )
      return user.put()
      

   @classmethod
   def Delete( cls, email ):
      '''
      Delete user from datastore.
      '''
      user_key_name = SyndicateUser.make_key_name( email=email)
      user_key = make_key( SyndicateUser, user_key_name )
      return user_key.delete()

   """
   @classmethod
   def ListAll( cls, attrs ):
      '''
      Attributes must be in dictionary, using format "SyndicateUser.PROPERTY [operator]: [value]"
      '''
      qry = SyndicateUser.query()
      ret = cls.ListAll_runQuery( qry, attrs )

      return ret
   """
   
   
