#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""


import storage
import storage.storagetypes as storagetypes

import os
import base64
from Crypto.Hash import SHA256

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import string
import logging

VOLUME_SECRET_LENGTH = 256
VOLUME_SECRET_SALT_LENGTH = 256


class VolumeIDCounter( storagetypes.Object ):
   value = storagetypes.Integer()

   required_attrs = [
      "value"
   ]

   @classmethod
   def make_key_name( cls, **attrs ):
      return "VolumeIDCounter"

   

class Volume( storagetypes.Object ):

   HTTP_VOLUME_SECRET = "Syndicate-VolumeSecret"
   
   name = storagetypes.String()
   blocksize = storagetypes.Integer( indexed=False )
   active = storagetypes.Boolean()
   description = storagetypes.Text()
   owner_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   replica_gateway_urls = storagetypes.String( repeated=True )     # multiple replica servers allowed
   version = storagetypes.Integer( indexed=False )                 # version of this metadata

   num_shards = storagetypes.Integer(default=20, indexed=False)    # number of shards per entry in this volume

   volume_secret_salted_hash = storagetypes.Text()                 # salted hash of shared secret between the volume and its gateways
   volume_secret_salt = storagetypes.Text()                        # salt for the above hashed value

   @classmethod
   def generate_password_hash( cls, password, salt ):
      h = SHA256.new()
      h.update( salt )
      h.update( password )
      return h.hexdigest()
      
   @classmethod
   def generate_volume_secret( cls, secret ):
      
      salt = digits = "".join( [random.choice(string.printable) for i in xrange(VOLUME_SECRET_SALT_LENGTH)] )
      secret_salted_hash = Volume.generate_password_hash( secret, salt )

      return (salt, secret_salted_hash)
      
   
   required_attrs = [
      "name",
      "blocksize",
      "owner_id",
      "volume_secret_salt",
      "volume_secret_salted_hash"
   ]

   key_attrs = [
      "name"
   ]

   validators = {
      "name": (lambda cls, value: len( value.translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.@")) ) == 0 )
   }

   default_values = {
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1)
   }
   
   def protobuf( self, volume_metadata, user_gateways, **kwargs ):
      """
      Convert to a protobuf (ms_volume_metadata).
      Extra kwargs:
         user_gateways:          [UserGateway]
      """
      
      volume_metadata.owner_id = kwargs.get( 'owner_id', self.owner_id )
      volume_metadata.blocksize = kwargs.get( 'blocksize', self.blocksize )
      volume_metadata.name = kwargs.get( 'name', self.name )
      volume_metadata.description = kwargs.get( 'description', self.description )
      volume_metadata.volume_id = kwargs.get( 'volume_id', self.volume_id )
      volume_metadata.volume_version = kwargs.get('volume_version', self.version )

      replica_urls = kwargs.get( 'replica_gateway_urls', self.replica_gateway_urls )
      
      for url in replica_urls:
         volume_metadata.replica_urls.append( url )

      for ug in user_gateways:
         ug_pb = volume_metadata.user_gateway_creds.add()
         ug.protobuf_cred( ug_pb )
      
      return


   def authenticate_gateway( self, http_headers ):
      """
      Given HTTP headers, determine if the request came from an authenticated UG.
      """
      volume_secret = http_headers.get( Volume.HTTP_VOLUME_SECRET, None )
      if volume_secret == None:
         # no authentication header given
         return False

      h = SHA256.new()
      h.update( self.volume_secret_salt )
      h.update( volume_secret )

      result = h.hexdigest()

      if result != self.volume_secret_salted_hash:
         # incorrect secret
         return False

      return True


   @classmethod
   def Create( cls, user, **kwargs ):
      """
      Given volume data, store it.  Return the volume key.

      kwargs:
         name: str
         blocksize: int
         description: str
         volume_secret: str
      """

      kwargs['owner_id'] = user.owner_id
      Volume.fill_defaults( kwargs )



      # Get or finalize credentials
      volume_secret = kwargs.get("volume_secret")

      if volume_secret == None:
         raise Exception( "No password given")

      else:
         volume_secret_salt, volume_secret_salted_hash = Volume.generate_volume_secret(volume_secret)

      kwargs['volume_secret_salt'] = volume_secret_salt
      kwargs['volume_secret_salted_hash'] = volume_secret_salted_hash


      # Validate
      missing = Volume.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )



      name = kwargs.get( "name" )
      blocksize = kwargs.get( "blocksize" )
      description = kwargs.get( "description" )

      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( name=name ) )
      volume = volume_key.get()
      if volume != None and volume.volume_id > 0:
         # volume already exists
         raise Exception( "Volume '%s' already exists" % name )

      else:
         # volume does not exist
         vid_counter = VolumeIDCounter.get_or_insert( VolumeIDCounter.make_key_name(), value=0 )
         
         vid_counter.value += 1

         vid_future = vid_counter.put_async()


         # new volume
         volume = Volume( name=name,
                        key=volume_key,
                        blocksize=blocksize,
                        description=description,
                        owner_id=user.owner_id,
                        volume_id=vid_counter.value,
                        active=kwargs.get('active',False),
                        replica_gateway_urls=[],
                        version=1,
                        volume_secret_salted_hash=volume_secret_salted_hash,
                        volume_secret_salt=volume_secret_salt
                        )

         vol_future = volume.put_async()

         storagetypes.wait_futures( [vid_future, vol_future] )

         return volume.key

   @classmethod
   def ReadFresh( cls, name):
      """
      Given a volume ID (name), get the volume entity. Skip cache.
      """
      volume_key_name = Volume.make_key_name( name=name )
      volume_key = storagetypes.make_key( Volume, volume_key_name )   

      volume = volume_key.get( use_memcache=False )
      if not volume:
         return None
      else:
         return volume

   @classmethod
   def Read( cls, name ):
      """
      Given a volume ID (name), get the volume entity. Returns None on miss.
      """
      volume_key_name = Volume.make_key_name( name=name )
      volume_key = storagetypes.make_key( Volume, volume_key_name )
      
      volume = storagetypes.memcache.get( volume_key_name )
      if volume == None:
         volume = volume_key.get( use_memcache=False )
         if not volume:
            return None
         else:
            storagetypes.memcache.set( volume_key_name, volume )

      return volume


   @classmethod
   def __update_shard_count( cls, volume_key, num_shards ):
      """
      update the shard count of the volume.
      """
      volume = volume_key.get()
      if volume.num_shards < num_shards:
         volume.num_shards = num_shards
         volume.put()

      return volume.num_shards


   @classmethod
   def update_shard_count( cls, volume_name, num_shards, **txn_args ):
      """
      Update the shard count of the volume, but in a transaction.
      """

      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( name=volume_name ) )
      
      num_shards = storagetypes.transaction( lambda: __volume_update_shard_count( volume_key, num_shards ), **txn_args )
      
      return num_shards

# Changed volume_id to name in parameters - John
   @classmethod
   def Update( cls, name, **fields ):
      '''
      Update volume identified by name with fields specified as a dictionary.
      '''
      volume = Volume.ReadFresh(name)
      logging.info(volume)
      for key, value in fields.iteritems():
         logging.info(key)
         logging.info(value)
         setattr(volume, key, value)
      vol_future = volume.put_async()
      storagetypes.wait_futures([vol_future])
      return volume.key
         

   @classmethod
   def Delete( cls, name ):
      '''
      Delete volume from datastore.
      '''
      volume = Volume.Read(name) 
      return volume.key.delete()

   @classmethod
   def ListAll( cls, **attrs ):
      '''
      Attributes must be in dictionary, using format "Volume.PROPERTY: [operator] [value]"
      eg {'Volume.volume_id': '== 5', ...} Yet to be tested/debugged.

      '''
      query_clause = ""
      for key, value in attrs.iteritems():
         if query_clause: 
            query_clause+=","
         query_clause += (key + value)
      if query_clause:
         exec ("result = Volume.query(%s)" % query_clause)
         return result
      else:
         return Volume.query()