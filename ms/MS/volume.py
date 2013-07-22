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

# DEPRICATED
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
   blocksize = storagetypes.Integer( indexed=False ) # Stored in kilobytes!!
   active = storagetypes.Boolean()
   description = storagetypes.Text()
   owner_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   version = storagetypes.Integer( indexed=False )                 # version of this Volume's metadata
   UG_version = storagetypes.Integer( indexed=False )              # version of the UG listing in this Volume
   RG_version = storagetypes.Integer( indexed=False )              # version of the RG listing in this Volume
   private = storagetypes.Boolean()
   session_timeout = storagetypes.Integer( default=-1, indexed=False )  # how long a gateway session on this Volume lasts
   ag_ids = storagetypes.Integer( repeated=True )                  # AG's publishing data to this volume
   rg_ids = storagetypes.Integer( repeated=True )                  # RG's replicating data for this volume.


   num_shards = storagetypes.Integer(default=20, indexed=False)    # number of shards per entry in this volume

   # DEPRICATED
   volume_secret_salted_hash = storagetypes.Text()                 # salted hash of shared secret between the volume and its gateways
   volume_secret_salt = storagetypes.Text()                        # salt for the above hashed value

   # DEPRICATED
   @classmethod
   def generate_password_hash( cls, password, salt ):
      h = SHA256.new()
      h.update( salt )
      h.update( password )
      return h.hexdigest()

   # DEPRICATED
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
      "volume_secret_salted_hash",
      "ag_ids",
      "rg_ids"
   ]

   key_attrs = [
      "name"
   ]

   validators = {
      "name": (lambda cls, value: len( value.translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")) ) == 0 )
   }

   default_values = {
      "ag_ids": (lambda cls, attrs: []),
      "rg_ids": (lambda cls, attrs: []),
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1)
   }

      
   def protobuf( self, volume_metadata, caller_UG, **kwargs ):
      """
      Convert to a protobuf (ms_volume_metadata).
      """

      caller_UG.protobuf_cred( volume_metadata.cred )
      
      volume_metadata.owner_id = kwargs.get( 'owner_id', self.owner_id )
      volume_metadata.blocksize = kwargs.get( 'blocksize', self.blocksize )
      volume_metadata.name = kwargs.get( 'name', self.name )
      volume_metadata.description = kwargs.get( 'description', self.description )
      volume_metadata.volume_id = kwargs.get( 'volume_id', self.volume_id )
      volume_metadata.volume_version = kwargs.get('volume_version', self.version )
      volume_metadata.UG_version = kwargs.get('UG_version', self.UG_version )
      volume_metadata.RG_version = kwargs.get('RG_version', self.RG_version )
      volume_metadata.session_timeout = kwargs.get( 'session_timeout', self.session_timeout )
      
      return


   def authenticate_UG( self, UG ):
      if not self.private:
         return True
         
      if UG.volume_id != self.volume_id:
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
      """

      kwargs['owner_id'] = user.owner_id
      Volume.fill_defaults( kwargs )

      # DEPRICATED
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

      kwargs['name'] = unicode(kwargs['name']).strip().replace(" ","_")
      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )



      name = kwargs.get( "name" )
      blocksize = kwargs.get( "blocksize" )
      description = kwargs.get( "description" )
      if "private" in kwargs:
         if type(kwargs['private']) is bool:
            private = kwargs['private']
         else:
            raise Exception( "Private must be a boolean value")
      else:
         private = False

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
                        version=1,
                        UG_version=1,
                        RG_version=1,
                        private=private,
                        rg_ids=kwargs['rg_ids'],
                        ag_ids=kwargs['ag_ids'],
                        # DEPRICATED
                        volume_secret_salt = kwargs['volume_secret_salt'],
                        volume_secret_salted_hash = kwargs['volume_secret_salted_hash']
                        )

         vol_future = volume.put_async()

         storagetypes.wait_futures( [vid_future, vol_future] )

         return volume.key

   @classmethod
   def Read( cls, name ):
      """
      Given a volume ID (name), get the volume entity. Returns None on miss.
      """
      name = unicode(name).strip().replace(" ","_")
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
      volume_name = unicode(volume_name).replace(" ","_")
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( name=volume_name ) )
      
      num_shards = storagetypes.transaction( lambda: __volume_update_shard_count( volume_key, num_shards ), **txn_args )
      
      return num_shards

   # Changed volume_id to name in parameters - John
   @classmethod
   def Update( cls, name, **fields ):
      '''
      Update volume identified by name with fields specified as a dictionary.
      '''
      name = unicode(name).replace(" ","_")
      volume = Volume.Read(name)
      volume_key_name = Volume.make_key_name( name=name )
      storagetypes.memcache.delete(volume_key_name)

      for key, value in fields.iteritems():
         setattr(volume, key, value)
      oldversion = volume.version
      setattr(volume, 'version', oldversion+1)
      vol_future = volume.put_async()
      storagetypes.wait_futures([vol_future])
      return volume.key
         

   @classmethod
   def Delete( cls, name ):
      '''
      Delete volume from datastore.
      '''
      name = unicode(name).replace(" ","_")
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( name=volume_name ) )
      return volume_key.delete()

      
   @classmethod
   def ListAll( cls, attrs ):
      '''
      attrs is a dictionary of Volume.PROPERTY [operator]: [value]
      '''
      qry = Volume.query()
      ret = cls.ListAll_runQuery( qry, attrs )

      return ret
