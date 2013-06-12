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
   volumes = storagetypes.Integer( repeated=True )
   
   required_attrs = [
      "email",
      "openid_url"
   ]

   key_attrs = [
      "email"
   ]

   default_values = {
      "volumes" : (lambda cls, attrs: [])
   }

   validators = {
      "email" : (lambda cls, value: valid_email(cls, value))
   }


   @classmethod
   def _create_user( cls, **kwargs ):
      """
      Given user data, store it.

      kwargs:
         owenr_id: int
         email: str
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
                              openid_url=openid_url )


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
      user_key = storagetypes.make_key( SyndicateUser, SyndicateUser.make_key_name( email=email ) )
      user = user_key.get()
      return user


   @classmethod
   def Update( cls, email, **fields ):
      raise NotImplementedError

   @classmethod
   def Delete( cls, email ):
      raise NotImplementedError

   @classmethod
   def ListAll( cls, **filter_attrs ):
      raise NotImplementedError

   
   