#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storagetypes as storagetypes

import os
import base64
import urllib
import uuid
from Crypto.Hash import SHA256, SHA
from Crypto.PublicKey import RSA
from Crypto import Random
from Crypto.Signature import PKCS1_v1_5

#import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import logging
import string
import traceback

USERNAME_LENGTH = 256
PASSWORD_LENGTH = 256
GATEWAY_SALT_LENGTH = 256
SESSION_PASSWORD_LENGTH = 16
GATEWAY_RSA_KEYSIZE = 4096

class IDCounter( storagetypes.Object ):
   value = storagetypes.Integer()

   required_attrs = [
      "value"
   ]

   gateway_type = "G"

   @classmethod
   def make_key_name(cls, **attrs):
      return cls.gateway_type + "IDCounter"

   @classmethod
   def __next_value(cls):
      # gateway does not exist
      gid_counter = cls.get_or_insert( IDCounter.make_key_name(), value=0)
      gid_counter.value += 1
      ret = gid_counter.value
      gid_key = gid_counter.put()
      return ret

   @classmethod
   def next_value(cls):
      return cls.__next_value()

class AG_IDCounter( IDCounter ):
   gateway_type = "AG"

class RG_IDCounter( IDCounter ):
   gateway_type = "RG"

class UG_IDCounter( IDCounter ):
   gateway_type = "UG"

def is_int( x ):
   try:
      y = int(x)
      return True
   except:
      return False

class Gateway( storagetypes.Object ):

   owner_id = storagetypes.Integer()         # ID of the SyndicateUser that owns this gateway
   host = storagetypes.String()
   port = storagetypes.Integer()
   ms_username = storagetypes.String()
   ms_password_hash = storagetypes.Text()
   ms_password_salt = storagetypes.Text()
   g_id = storagetypes.Integer()

   public_key = storagetypes.Text()          # PEM-encoded RSA public key

   session_password = storagetypes.Text()
   session_expires = storagetypes.Integer(default=-1, indexed=False)     # -1 means "never expires"
   
   required_attrs = [
      "owner_id",
      "host",
      "port",
      "ms_username",
      "ms_password"
   ]

   validators = {
      "ms_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "session_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "ms_username": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.: ")) ) == 0 and not is_int(value) )
   }

   # TODO: session expires in 3600 seconds
   default_values = {
      "session_expires": (lambda cls, attrs: -1),
      "session_password": (lambda cls, attrs: Gateway.generate_session_credentials())
   }

   key_attrs = [
      "g_id"
   ]

   @classmethod
   def is_valid_pubkey( self, pubkey_str ):
      try:
         key = RSA.importKey( pubkey_str )
      except Exception, e:
         logging.error("RSA.importKey %s", traceback.format_exc() )
         return False

      # must be 4096 bits
      if key.size() != GATEWAY_RSA_KEYSIZE - 1:
         logging.error("invalid key size = %s" % key.size() )
         return False

      return True

   def authenticate( self, password ):
      """
      Verify that a password is correct
      """
      h = SHA256.new()
      h.update( self.ms_password_salt )
      h.update( password )
      pw_hash = h.hexdigest()

      return pw_hash == self.ms_password_hash


   def authenticate_session( self, password ):
      """
      Verify that the session password is correct
      """
      return self.session_password == password

      
   def verify_signature( self, data, sig ):
      key = RSA.importKey( self.public_key )
      h = SHA256.new( data )
      verifier = PKCS1_v1_5.new(key)
      return verifier.verify( h, sig )

      
   @classmethod
   def generate_password_hash( cls, pw, salt ):

      h = SHA256.new()
      h.update( salt )
      h.update( pw )

      pw_hash = h.hexdigest()

      return unicode(pw_hash)


   @classmethod
   def generate_password( cls, length ):
      password = "".join( [random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for i in xrange(length)] )
      return password


   @classmethod
   def generate_session_credentials( cls ):
      return cls.generate_password( SESSION_PASSWORD_LENGTH )
      
   def get_current_session_credentials( self ):
      """
      Get the session credentials, returning None if they are too old
      """
      now = int( time.time() )
      if self.session_password == None or (now > self.session_expires and self.session_expires > 0):
         return None

      return self.session_password

   def regenerate_session_credentials( self, volume ):
      """
      Regenerate a session password, given the Volume it's bound to.
      """
      self.session_password = Gateway.generate_session_credentials()
      if volume.session_timeout > 0:
         self.session_expires = now + volume.session_timeout
      else:
         self.session_expires = -1
         

   @classmethod
   def generate_credentials( cls ):
      """
      Generate (password, SHA256(password), salt) for this gateway
      """
      password = cls.generate_password( PASSWORD_LENGTH )
      salt = "".join( [random.choice(string.printable) for i in xrange(GATEWAY_SALT_LENGTH)] )
      pw_hash = Gateway.generate_password_hash( password, salt )

      return ( password, pw_hash, salt )  


   def load_pubkey( self, pubkey_str ):
      # pubkey_str is base64 encoded
      pubkey_str_unencoded = base64.b64decode( pubkey_str )
      if not Gateway.is_valid_pubkey( pubkey_str_unencoded ):
         return False

      self.public_key = RSA.importKey( pubkey_str_unencoded ).exportKey()
      return True
      
   def protobuf_cred( self, cred_pb ):
      """
      Populate an ms_volume_gateway_cred structure
      """
      cred_pb.owner_id = self.owner_id
      cred_pb.gateway_id = self.g_id
      cred_pb.name = self.ms_username
      cred_pb.host = self.host
      cred_pb.port = self.port

      if self.public_key != None:
         cred_pb.public_key = self.public_key
      else:
         cred_pb.public_key = "NONE"


   @classmethod
   def create_credentials( cls, user, kwargs ):

      password, password_hash, password_salt = UserGateway.generate_credentials()
      
      if kwargs.get("ms_username") == None:
         # generate new credentials
         kwargs["ms_username"] = uuid.uuid4().urn

      if kwargs.get("ms_password") != None:
         # use user-given password if provided
         kwargs["ms_password_salt"] = password_salt
         kwargs["ms_password_hash"] = Gateway.generate_password_hash( kwargs.get("ms_password"), password_salt )
      
      if kwargs.get("ms_password_hash") == None:
         kwargs["ms_password"] = password
         kwargs["ms_password_salt"] = password_salt
         kwargs["ms_password_hash"] = password_hash
      

   # override this in subclasses
   def is_in_volume( self, volume ):
      return False


   def verify_ms_update( self, ms_update ):
      """
      Verify the authenticity of a received ms_update message
      """

      sig = ms_update.signature
      sig_bin = base64.b64decode( sig )

      ms_update.signature = ""
      ms_update_str = ms_update.SerializeToString()

      ret = self.verify_signature( ms_update_str, sig_bin )

      ms_update.signature = sig

      return ret


   def is_valid_cred( self, cred_pb ):
      """
      Is a given ms_volume_gateway_cred valid?
      """

      if self.owner_id != cred_pb.owner_id:
         return False

      if self.ms_username != cred_pb.name:
         return False

      if self.host != cred_pb.host:
         return False

      if self.port != cred_pb.port:
         return False

      return True

   

class UserGateway( Gateway ):

   read_write = storagetypes.Boolean()
   volume_id = storagetypes.Integer()           # which volume are we attached to?


   required_attrs = Gateway.required_attrs + [
      "volume_id",
      "read_write"
   ]

   default_values = dict( Gateway.default_values.items() + {
      "read_write": (lambda cls, attrs: False), # Default is only read
      "port": (lambda cls, attrs:32780)
   }.items() )
   

   @classmethod
   def cache_listing_key( cls, **kwargs ):
      assert 'volume_id' in kwargs, "Required attributes: volume_id"
      return "UGs: volume=%s" % kwargs['volume_id']

         
   
   @classmethod
   def Create( cls, user, volume=None, **kwargs ):
      """
      Given a user and volume, create a user gateway.
      Extra kwargs:
         ms_username          str
         ms_password          str
         ms_password_hash     str
         read_write           bool
         host                 str
         port                 int
      """
      # Isolates the DB elements in a transactioned call
      @storagetypes.transactional(xg=True)
      def transactional_create(**kwargs):
         ug_id = UG_IDCounter.next_value()
         kwargs['g_id'] = ug_id

         ug_key_name = UserGateway.make_key_name( g_id=ug_id )
         ug_key = storagetypes.make_key( UserGateway, ug_key_name)
         ug = ug_key.get()
         
         if 'ms_password' in kwargs:
            del kwargs['ms_password']
         ug = UserGateway( key=ug_key, **kwargs )

         # clear cached UG listings
         storagetypes.memcache.delete( UserGateway.cache_listing_key( volume_id=kwargs['volume_id'] ) )
         return ug.put()


      if volume:
         kwargs['volume_id'] = volume.volume_id
      else:
         kwargs['volume_id'] = 0
      kwargs['owner_id'] = user.owner_id

      UserGateway.fill_defaults( kwargs )

      UserGateway.create_credentials( user, kwargs )

      missing = UserGateway.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = UserGateway.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      kwargs['ms_username'] = unicode(kwargs['ms_username']).strip().replace(" ","_")

      existing_gateways = UserGateway.ListAll( {"UserGateway.ms_username ==" : kwargs['ms_username']} )
      if len(existing_gateways) > 0:
         # gateway already exists
         raise Exception( "Gateway '%s' already exists" % kwargs['ms_username'] )
      else:
         return transactional_create(**kwargs)


   @classmethod
   def Read( cls, g_id ):
      """
      Given a UG ID, find the UG record
      """
      ug_key_name = UserGateway.make_key_name( g_id=g_id )

      ug = storagetypes.memcache.get( ug_key_name )
      if ug == None:
         #logging.info("UG with ID of {} not found in cache".format(g_id))
         ug_key = storagetypes.make_key( UserGateway, UserGateway.make_key_name( g_id=g_id ) )
         ug = ug_key.get( use_memcache=False )
         if not ug:
            logging.error("UG not found at all!.")
         storagetypes.memcache.set( ug_key_name, ug )
      return ug

   @classmethod
   def FlushCache( cls, g_id ):
      gateway_key_name = UserGateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

      
   @classmethod
   def Update( cls, g_id, **fields ):
      '''
      Update UG identified by ID with fields specified as a dictionary.
      '''
      gateway = UserGateway.Read(g_id)
      gateway_key_name = UserGateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

      for key, value in fields.iteritems():
         try:
            setattr(gateway, key, value)
         except:
            raise Exception("UserGatewayUpdate: Unable to set attribute: %s, %s." % (key, value))

      UG_future = gateway.put_async()
      storagetypes.wait_futures([UG_future])
      return gateway.key
      
   @classmethod
   def Delete( cls, g_id ):
      """
      Given a UG ID, delete it
      """
      ug_key_name = UserGateway.make_key_name( g_id=g_id )

      ug_key = storagetypes.make_key( UserGateway, ug_key_name )

      ug_key.delete()
      
      storagetypes.memcache.delete(ug_key_name)

      return True
      

   @classmethod
   def ListAll_ByVolume( cls, volume_id ):
      """
      Given a volume id, find all UserGateway records bound to it.  Cache the results
      """
      #cache_key = UserGateway.cache_listing_key( volume_id=volume_id )

      #results = storagetypes.memcache.get( cache_key )
      results = None
      if results == None:
         qry = UserGateway.query( UserGateway.volume_id == volume_id )
         results_qry = qry.fetch(None, batch_size=1000 )

         results = []
         for rr in results_qry:
            results.append( rr )

         #storagetypes.memcache.add( cache_key, results )

      return results

   def is_in_volume( self, volume ):
      return volume.volume_id == self.volume_id
      
   
class AcquisitionGateway( Gateway ):

   # This is temporary; we should know what is really needed.   
   json_config = storagetypes.Json()
   volume_ids = storagetypes.Integer(repeated=True)           # which volumes are we attached to?

   required_attrs = Gateway.required_attrs + [
      "json_config"
   ]

   default_values = dict( Gateway.default_values.items() + {
      "json_config": (lambda cls, attrs: {}) # Default is only read
   }.items() )


   @classmethod
   def cache_listing_key( cls, **kwargs ):
      assert 'volume_id' in kwargs, "Required attributes: volume_id"
      return "AGs: volume=%s" % kwargs['volume_id']

   @classmethod
   def Create( cls, user, **kwargs ):
      """
      Given a volume, create an AcquisitionGateway gateway.
      Extra kwargs:
         ms_password          str
         ms_password_hash     str
         json_config          JSON (dict)
      """
      # Isolates the DB elements in a transactioned call
      @storagetypes.transactional(xg=True)
      def transactional_create(**kwargs):
         ag_id = AG_IDCounter.next_value()
         kwargs['g_id'] = ag_id

         ag_key_name = AcquisitionGateway.make_key_name( g_id=ag_id ) 
         ag_key = storagetypes.make_key( AcquisitionGateway, ag_key_name )
         ag = ag_key.get()

         if 'ms_password' in kwargs:
            del kwargs['ms_password']
         ag = AcquisitionGateway( key=ag_key, **kwargs )

         # clear cached AG listings
         storagetypes.memcache.delete(ag_key_name)
         return ag.put()



      kwargs['owner_id'] = user.owner_id

      AcquisitionGateway.fill_defaults( kwargs )

      AcquisitionGateway.create_credentials( user, kwargs )

      missing = AcquisitionGateway.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = AcquisitionGateway.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      # TODO: transaction -jcnelson
      kwargs['ms_username'] = unicode(kwargs['ms_username']).strip().replace(" ","_")


      existing_gateways = AcquisitionGateway.ListAll( {"AcquisitionGateway.ms_username ==" : kwargs['ms_username']} )
      if len(existing_gateways) > 0:
         # gateway already exists
         raise Exception( "Gateway '%s' already exists" % kwargs['ms_username'] )
      else:
         return transactional_create(**kwargs)


   @classmethod
   def Read( cls, g_id ):
      """
      Given a AG id, find the AG record
      """
      ag_key_name = AcquisitionGateway.make_key_name( g_id=g_id )

      ag = storagetypes.memcache.get( ag_key_name )
      if ag == None:
         ag_key = storagetypes.make_key( AcquisitionGateway, AcquisitionGateway.make_key_name( g_id=g_id ) )
         ag = ag_key.get( use_memcache=False )
         storagetypes.memcache.set( ag_key_name, ag )

      return ag

   @classmethod
   def Update( cls, g_id, **fields ):
      '''
      Update AG identified by g_id with fields specified as a dictionary.
      '''
      gateway = AcquisitionGateway.Read(g_id)
      gateway_key_name = AcquisitionGateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

      for key, value in fields.iteritems():
         try:
            setattr(gateway, key, value)
         except:
            raise Exception("AcquisitionGatewayUpdate: Unable to set attribute: %s, %s." % (key, value))
      AG_future = gateway.put_async()
      storagetypes.wait_futures([AG_future])
      return gateway.key

   
   @classmethod
   def ListAll_ByVolume( cls, volume_id ):
      """
      Given a volume id, find all AcquisitionGateway records bound to it.  Cache the results
      """
      #cache_key = AcquisitionGateway.cache_listing_key( volume_id=volume_id )

      #results = storagetypes.memcache.get( cache_key )
      results = None
      if results == None:
         qry = AcquisitionGateway.query( AcquisitionGateway.volume_ids == volume_id )
         results_qry = qry.fetch(None, batch_size=1000 )

         results = []
         for rr in results_qry:
            results.append( rr )

         #storagetypes.memcache.add( cache_key, results )

      return results

      
   @classmethod
   def Delete( cls, g_id ):
      """
      Given a AG ID, delete it
      """
      ag_key_name = AcquisitionGateway.make_key_name( g_id=g_id )

      ag_key = storagetypes.make_key( AcquisitionGateway, ag_key_name )

      ag_key.delete()
      
      storagetypes.memcache.delete(ag_key_name)

      return True

   def is_in_volume( self, volume ):
      return volume.volume_id in self.volume_ids
      

class ReplicaGateway( Gateway ):

   # This is temporary; we should know what is really needed.
   json_config = storagetypes.Json()
   private = storagetypes.Boolean()
   volume_ids = storagetypes.Integer(repeated=True)           # which volume(s) are we attached to?

   required_attrs = Gateway.required_attrs + [
      "json_config"
   ]

   default_values = dict( Gateway.default_values.items() + {
      "json_config": (lambda cls, attrs: {}), # Default is only read
      "private": (lambda cls, attrs: False) # Default is public
   }.items() )

   @classmethod
   def cache_listing_key( cls, **kwargs ):
      assert 'volume_id' in kwargs, "Required attributes: volume_id"
      return "RGs: volume=%s" % kwargs['volume_id']

   @classmethod
   def Create( cls, user, **kwargs ):
      """
      Given a volume, create an Replica gateway.
      Extra kwargs:
         ms_password          str
         ms_password_hash     str
         json_config          JSON (dict)
         private              bool
      """
      # Isolates the DB elements in a transactioned call
      @storagetypes.transactional(xg=True)
      def transactional_create(**kwargs):
         rg_id = RG_IDCounter.next_value()
         kwargs['g_id'] = rg_id

         rg_key_name = ReplicaGateway.make_key_name( g_id=rg_id ) 
         rg_key = storagetypes.make_key( ReplicaGateway, rg_key_name )
         rg = rg_key.get()
         if 'ms_password' in kwargs:
            del kwargs['ms_password']
         rg = ReplicaGateway( key=rg_key, **kwargs )

         # clear cached RG listings
         storagetypes.memcache.delete(rg_key_name)
         
         return rg.put()

      kwargs['owner_id'] = user.owner_id

      ReplicaGateway.fill_defaults( kwargs )

      ReplicaGateway.create_credentials( user, kwargs )
      
      missing = ReplicaGateway.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = ReplicaGateway.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      # TODO: transaction -jcnelson
      kwargs['ms_username'] = unicode(kwargs['ms_username']).strip().replace(" ","_")      


      existing_gateways = ReplicaGateway.ListAll( {"ReplicaGateway.ms_username ==" : kwargs['ms_username']} )
      if len(existing_gateways) > 0:
         # gateway already exists
         raise Exception( "Gateway '%s' already exists" % kwargs['ms_username'] )
      else:
         return transactional_create(**kwargs)

   @classmethod
   def Read( cls, g_id ):
      """
      Given a RG ID, find the RG record
      """
      rg_key_name = ReplicaGateway.make_key_name( g_id=g_id )

      rg = storagetypes.memcache.get( rg_key_name )
      if rg == None:
         rg_key = storagetypes.make_key( ReplicaGateway, ReplicaGateway.make_key_name( g_id=g_id ) )
         rg = rg_key.get( use_memcache=False )
         storagetypes.memcache.set( rg_key_name, rg )

      return rg


   @classmethod
   def Update( cls, g_id, **fields ):
      '''
      Update RG identified by g_id with fields specified as a dictionary.
      '''
      gateway = ReplicaGateway.Read(g_id)
      gateway_key_name = ReplicaGateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

      for key, value in fields.iteritems():
         try:
            setattr(gateway, key, value)
         except Exception as e:
            logging.info(e)
            raise Exception("ReplicaGatewayUpdate: Unable to set attribute: %s, %s." % (key, value))
      RG_future = gateway.put_async()
      storagetypes.wait_futures([RG_future])
      return gateway.key

      
   @classmethod
   def Delete( cls, g_id ):
      """
      Given a RG ID, delete it
      """
      rg_key_name = ReplicaGateway.make_key_name( g_id=g_id )

      rg_key = storagetypes.make_key( ReplicaGateway, rg_key_name )

      rg_key.delete()
      
      storagetypes.memcache.delete(rg_key_name)

      return True
   
   @classmethod
   def ListAll_ByVolume( cls, volume_id ):
      """
      Given a volume id, find all ReplicaGateway records bound to it.  Cache the results
      """
      #cache_key = ReplicaGateway.cache_listing_key( volume_id=volume_id )

      #results = storagetypes.memcache.get( cache_key )
      results = None 
      if results == None:
         qry = ReplicaGateway.query( ReplicaGateway.volume_ids == volume_id )
         results_qry = qry.fetch(None, batch_size=1000 )

         results = []
         for rr in results_qry:
            results.append( rr )

         #storagetypes.memcache.add( cache_key, results )

      return results


   def is_in_volume( self, volume ):
      return volume.volume_id in self.volume_ids