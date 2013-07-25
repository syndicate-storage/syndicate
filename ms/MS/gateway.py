#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storagetypes as storagetypes

from Crypto.Hash import SHA256

import os
import base64
import uuid

#import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import logging


USERNAME_LENGTH = 256
PASSWORD_LENGTH = 256

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
      return storagetypes.transaction( lambda: cls.__next_value() )

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

   # owner ID of all files created by this gateway
   owner_id = storagetypes.Integer()
   host = storagetypes.Text()
   port = storagetypes.Integer()
   ms_username = storagetypes.String()
   ms_password_hash = storagetypes.String()
   g_id = storagetypes.Integer()

   required_attrs = [
      "owner_id",
      "host",
      "port",
      "ms_username",
      "ms_password",
   ]

   validators = {
      "ms_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "ms_username": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-. ")) ) == 0 and not is_int(value) )
   }

   key_attrs = [
      "g_id"
   ]

   def authenticate( self, password ):
      """
      Authenticate this Gateway
      """
      h = SHA256.new()
      h.update( password )
      pw_hash = h.hexdigest()

      return pw_hash == self.ms_password_hash

   @classmethod
   def generate_password_hash( cls, pw ):

      h = SHA256.new()
      h.update( pw )

      pw_hash = h.hexdigest()

      return unicode(pw_hash)
   

class UserGateway( Gateway ):

   read_write = storagetypes.Boolean()
   volume_id = storagetypes.Integer()           # which volume are we attached to?


   required_attrs = Gateway.required_attrs + [
      "read_write"
   ]

   default_values = {
      "read_write": (lambda cls, attrs: False), # Default is only read
      "port": (lambda cls, attrs:80)
   }

   
   def get_credential_entry(self):
      """
      Generate a serialized user record
      """
      return "%s:%s:%s" % (self.owner_id, self.ms_username, self.ms_password_hash)
      

   @classmethod
   def generate_credentials( cls ):
      """
      Generate (username, password, SHA256(password)) for this gateway
      """
      password = os.urandom( PASSWORD_LENGTH )

      username = base64.encodestring( uuid.uuid4().urn )
      pw_hash = Gateway.generate_password_hash( password )
      
      return (username, password, pw_hash)
      
   def new_credentials( self ):
      """
      Generate new credentials for this UG and save them.
      Return the (username, password) combo
      """
      username, password, pw_hash = UserGateway.generate_credentials()

      self.ms_username = username
      self.ms_password_hash = pw_hash
      self.put()

      return (username, password)
      

   def protobuf_cred( self, cred_pb ):
      """
      Populate an ms_volume_gateway_cred structure
      """
      cred_pb.owner_id = self.owner_id
      cred_pb.username = self.ms_username
      cred_pb.password_hash = self.ms_password_hash
      cred_pb.host = self.host
      cred_pb.port = self.port

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

      if volume:
         kwargs['volume_id'] = volume.volume_id
      else:
         kwargs['volume_id'] = 0
      kwargs['owner_id'] = user.owner_id

      UserGateway.fill_defaults( kwargs )

      if kwargs.get("ms_password") != None:
         kwargs[ "ms_password_hash" ] = Gateway.generate_password_hash( kwargs.get("ms_password") )

      if kwargs.get("ms_username") == None or kwargs.get("ms_password_hash") == None:
         # generate new credentials
         username, password, password_hash = UserGateway.generate_credentials()
         kwargs["ms_username"] = username
         kwargs["ms_password"] = password
         kwargs["ms_password_hash"] = password_hash

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
         # TODO: transaction -jcnelson
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


   @classmethod
   def Read( cls, g_id ):
      """
      Given a UG ID, find the UG record
      """
      ug_key_name = UserGateway.make_key_name( g_id=g_id )

      ug = storagetypes.memcache.get( ug_key_name )
      if ug == None:
         logging.info("UG with ID of {} not found in cache".format(g_id))
         ug_key = storagetypes.make_key( UserGateway, UserGateway.make_key_name( g_id=g_id ) )
         ug = ug_key.get( use_memcache=False )
         if not ug:
            logging.error("UG not found at all!.")
         storagetypes.memcache.set( ug_key_name, ug )
      return ug

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

   
class AcquisitionGateway( Gateway ):

   # This is temporary; we should know what is really needed.   
   json_config = storagetypes.Json()
   volume_ids = storagetypes.Integer(repeated=True)           # which volumes are we attached to?

   required_attrs = Gateway.required_attrs + [
      "json_config"
   ]

   default_values = {
      "json_config": (lambda cls, attrs: {}) # Default is only read
   }

   def get_credential_entry(self):
      """
      Generate a serialized user record
      """
      return "AG:%s:%s" % (self.ms_username, self.ms_password_hash)

   def protobuf_cred( self, cred_pb ):
      """
      Populate an ms_volume_gateway_cred structure
      """
      cred_pb.username = self.ms_username
      cred_pb.password_hash = self.ms_password_hash
      

   @classmethod
   def generate_credentials( cls ):
      """
      Generate (password, SHA256(password)) for this gateway
      """
      password = os.urandom( PASSWORD_LENGTH )
      pw_hash = Gateway.generate_password_hash( password )
      
      return ( password, pw_hash)  


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

      kwargs['owner_id'] = user.owner_id

      AcquisitionGateway.fill_defaults( kwargs )

      if kwargs.get("ms_password") != None:
         kwargs[ "ms_password_hash" ] = Gateway.generate_password_hash( kwargs.get("ms_password") )

      if kwargs.get("ms_password_hash") == None:
         # generate new credentials
         password, password_hash = AcquisitionGateway.generate_credentials()
         kwargs["ms_password"] = password
         kwargs["ms_password_hash"] = password_hash


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
      

class ReplicaGateway( Gateway ):

   # This is temporary; we should know what is really needed.
   json_config = storagetypes.Json()
   private = storagetypes.Boolean()
   volume_ids = storagetypes.Integer(repeated=True)           # which volume(s) are we attached to?

   required_attrs = Gateway.required_attrs + [
      "json_config"
   ]

   default_values = {
      "json_config": (lambda cls, attrs: {}), # Default is only read
      "private": (lambda cls, attrs: False) # Default is public
   }

   def get_credential_entry(self):
      """
      Generate a serialized user record
      """
      return "RG:%s:%s" % (self.ms_username, self.ms_password_hash)

   def protobuf_cred( self, cred_pb ):
      """
      Populate an ms_volume_gateway_cred structure
      """
      cred_pb.username = self.ms_username
      cred_pb.password_hash = self.ms_password_hash
   

   @classmethod
   def generate_credentials( cls ):
      """
      Generate (password, SHA256(password)) for this gateway
      """
      password = os.urandom( PASSWORD_LENGTH )
      pw_hash = Gateway.generate_password_hash( password )
      
      return ( password, pw_hash)  

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

      kwargs['owner_id'] = user.owner_id

      ReplicaGateway.fill_defaults( kwargs )

      if kwargs.get("ms_password") != None:
         kwargs[ "ms_password_hash" ] = Gateway.generate_password_hash( kwargs.get("ms_password") )

      if kwargs.get("ms_password_hash") == None:
         # generate new credentials
         password, password_hash = ReplicaGateway.generate_credentials()
         kwargs["ms_password"] = password
         kwargs["ms_password_hash"] = password_hash


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