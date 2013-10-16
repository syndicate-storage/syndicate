#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""


import storage
import storage.storagetypes as storagetypes
import storage.shardcounter as shardcounter

import os
import base64
from Crypto.Hash import SHA256, SHA
from Crypto.PublicKey import RSA
from Crypto import Random
from Crypto.Signature import PKCS1_v1_5

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import string
import logging

from user import SyndicateUser
from msconfig import *

class VolumeIDCounter( storagetypes.Object ):
   value = storagetypes.Integer()

   required_attrs = [
      "value"
   ]

   @classmethod
   def make_key_name( cls, **attrs ):
      return "VolumeIDCounter"

   @classmethod
   def __next_value( cls ):
      # volume does not exist
      vid_counter = VolumeIDCounter.get_or_insert( VolumeIDCounter.make_key_name(), value=0 )

      vid_counter.value += 1

      ret = vid_counter.value

      vid_key = vid_counter.put()

      return ret

   @classmethod
   def next_value( cls ):
      return VolumeIDCounter.__next_value()

      

def is_int( x ):
   try:
      y = int(x)
      return True
   except:
      return False
   
   
class Volume( storagetypes.Object ):

   HTTP_VOLUME_SECRET = "Syndicate-VolumeSecret"
   
   name = storagetypes.String()
   blocksize = storagetypes.Integer( indexed=False ) # Stored in bytes!!
   active = storagetypes.Boolean()
   description = storagetypes.Text()
   owner_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   
   version = storagetypes.Integer( default=1, indexed=False )                 # version of this Volume's metadata
   cert_version = storagetypes.Integer( default=1, indexed=False )            # certificate bundle version
   
   private = storagetypes.Boolean()
   archive = storagetypes.Boolean(default=False)                # only an AG can write to this Volume
   
   num_shards = storagetypes.Integer(default=20, indexed=False)    # number of shards per entry in this volume

   public_key = storagetypes.Text()          # Volume public key, in PEM format
   private_key = storagetypes.Text()         # Volume private key, in PEM format

   volume_secret_salted_hash = storagetypes.Text()                 # salted hash of shared secret between the volume and its administrator
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

   @classmethod
   def generate_volume_keys( cls ):
      rng = Random.new().read
      RSAkey = RSA.generate(VOLUME_RSA_KEYSIZE, rng)

      private_key_pem = RSAkey.exportKey()
      public_key_pem = RSAkey.publickey().exportKey()

      return (public_key_pem, private_key_pem)
      
   
   required_attrs = [
      "name",
      "blocksize",
      "owner_id",
      "volume_secret_salt",
      "volume_secret_salted_hash",
      "private",
      "public_key",
      "private_key",
   ]

   key_attrs = [
      "volume_id"
   ]

      
   validators = {
      "name": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")) ) == 0 and not is_int(value) ),
   }

   default_values = {
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1),
      "cert_version": (lambda cls, attrs: 1),
      "private": (lambda cls, attrs: True),
      "archive": (lambda cls, attrs: False)
   }
   
   def need_gateway_auth( self ):
      """
      Do we require an authentic gateway to interact with us?
      """
      if self.private or not self.archive:
         return True
      
      return False
      

   def protobuf( self, volume_metadata, **kwargs ):
      """
      Convert to a protobuf (ms_volume_metadata)
      """

      volume_metadata.owner_id = kwargs.get( 'owner_id', self.owner_id )
      volume_metadata.blocksize = kwargs.get( 'blocksize', self.blocksize )
      volume_metadata.volume_id = kwargs.get( 'volume_id', self.volume_id )
      volume_metadata.name = kwargs.get( 'name', self.name )
      volume_metadata.description = kwargs.get( 'description', self.description )
      volume_metadata.volume_version = kwargs.get('volume_version', self.version )
      volume_metadata.cert_version = kwargs.get('cert_version', self.cert_version )
      volume_metadata.volume_public_key = kwargs.get( 'public_key', self.public_key )
      volume_metadata.num_files = kwargs.get( 'num_files', Volume.get_num_files( volume_metadata.volume_id ) )
      volume_metadata.archive = kwargs.get( 'archive', self.archive )
      volume_metadata.private = kwargs.get( 'private', self.private )
      # sign it
      volume_metadata.signature = ""

      data = volume_metadata.SerializeToString()
      sig = self.sign_message( data )

      volume_metadata.signature = sig

      return
      
   
   def protobuf_gateway_cert( self, gateway_cert, gateway, sign=True ):
      gateway.protobuf_cert( gateway_cert )
      
      gateway_cert.signature = ""
      
      if sign:
         # sign the cert
         data = gateway_cert.SerializeToString()
         sig = self.sign_message( data )
         
         gateway_cert.signature = sig
      
      return 
   
   
   def protobuf_gateway_cert_manifest( self, manifest, gateway_classes, sign=True ):
      manifest.volume_id = self.volume_id
      manifest.gateway_id = 0
      manifest.file_id = 0
      manifest.file_version = self.cert_version
      manifest.mtime_sec = 0
      manifest.mtime_nsec = 0
      
      sz = 0
      
      # query certificate versions of all gateways
      futs = []
      for gwcls in gateway_classes:
         qry_fut = gwcls.ListAll_ByVolume( self.volume_id, async=True, projection=[gwcls.g_id, gwcls.caps, gwcls.cert_version] )
         futs.append( qry_fut )
      
      storagetypes.wait_futures( futs )
      
      gateway_lists = [g.get_result() for g in futs]
      results = zip( gateway_classes, gateway_lists )
      
      for r in results:
         cls = r[0]
         listing = r[1]
         
         for gateway_metadata in listing:
            cert_block = manifest.block_url_set.add()
         
            cert_block.gateway_id = gateway_metadata.g_id
            cert_block.start_id = cls.GATEWAY_TYPE
            cert_block.end_id = gateway_metadata.caps
            cert_block.block_versions.append( gateway_metadata.cert_version )
         
            logging.info("cert block: (%s, %s, %s, %x)" % (cls.GATEWAY_TYPE, gateway_metadata.g_id, gateway_metadata.cert_version, gateway_metadata.caps) )
            sz += 1
      
      manifest.size = sz
      manifest.signature = ""
      
      if sign:
         data = manifest.SerializeToString()
         sig = self.sign_message( data )
         
         manifest.signature = sig
      
      return
      
      

   def is_gateway_in_volume( self, gateway ):
      if not self.private:
         return True

      return gateway.is_in_volume( self )


   def sign_message( self, data ):
      """
      Return the base64-encoded crypto signature of the data,
      signed with our private key.
      """
      key = RSA.importKey( self.private_key )
      h = SHA256.new( data )
      signer = PKCS1_v1_5.new(key)
      signature = signer.sign( h )
      sigb64 = base64.b64encode( signature )
      return sigb64
      
      
   @classmethod
   def Create( cls, username, **kwargs ):
      """
      Given volume data, store it.  Return the volume key.
      If the user is given (as the owner), update the user atomically along with the create.
      
      kwargs:
         name: str
         blocksize: int
         description: str
      """

      # Isolates the DB elements in a transactioned call
      @storagetypes.transactional(xg=True)
      def transactional_create(username, **kwargs):
         # get the user
         user = SyndicateUser.Read( username )
         
         # Set up volume ID # and Key
         volume_id = VolumeIDCounter.next_value()
         volume_key_name = Volume.make_key_name( volume_id=volume_id )
         volume_key = storagetypes.make_key( Volume, volume_key_name )

         # new volume
         volume = Volume( name=kwargs['name'],
                        key = volume_key,
                        blocksize=kwargs['blocksize'],
                        description=kwargs['description'],
                        owner_id=user.owner_id,
                        volume_id=volume_id,
                        active=kwargs.get('active',False),
                        version=1,
                        cert_version=1,
                        private=kwargs['private'],
                        archive=kwargs['archive'],
                        volume_secret_salt = kwargs['volume_secret_salt'],
                        volume_secret_salted_hash = kwargs['volume_secret_salted_hash'],
                        public_key = kwargs['public_key'],
                        private_key = kwargs['private_key']
                        )
         volume.put()

         # add this Volume to this User
         try:
            SyndicateUser.add_volume_to_owner(volume_id, username)
         except Exception, e:
            logging.exception( "add Volume to SyndicateUser exception", e )

            # Roll back
            raise Exception( "System is under heavy load right now.  Please try again later." )

         # Ok, return key   
         return volume_key
         

      kwargs['owner_id'] = 0     # will look up user and fill with owner ID once we validate input.
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

      if kwargs.get('public_key') == None or kwargs.get('private_key') == None:
         kwargs['public_key'], kwargs['private_key'] = Volume.generate_volume_keys()

      # Validate
      missing = Volume.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      kwargs['name'] = unicode(kwargs['name']).strip().replace(" ","_")
      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      existing_volumes = Volume.ListAll( {"Volume.name ==" : kwargs['name']} )
      if len(existing_volumes) > 0:
         # volume already exists
         raise Exception( "Volume '%s' already exists" % kwargs['name'] )

      else:
         # Volume did not exist at the time of the query.
         return transactional_create(username, **kwargs)
         

   @classmethod
   def Read( cls, volume_id, set_memcache=True ):
      """
      Given a volume ID, get the volume entity. Returns None on miss.
      """
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      volume_key = storagetypes.make_key( Volume, volume_key_name )

      volume = storagetypes.memcache.get( volume_key_name )
      if volume == None:
         volume = volume_key.get( use_memcache=False )
         if not volume:
            return None
         elif set_memcache:
            storagetypes.memcache.set( volume_key_name, volume )

      return volume

   @classmethod
   def ReadAll( cls, volume_ids ):
      """
      Given a set of Volume IDs, get all of the Volumes.
      """
      volume_key_names = map( lambda x: Volume.make_key_name( volume_id=x ), volume_ids )

      volumes_dict = storagetypes.memcache.get_multi( volume_key_names )

      ret = [None] * len(volume_key_names)

      for i in xrange(0, len(volume_key_names)):
         volume_key_name = volume_key_names[i]
         volume = volumes_dict.get( volume_key_name )
         if volume == None:
            volume_key = storagetypes.make_key( Volume, volume_key_name )
            volume = volume_key.get( use_memcache=False )
            if not volume:
               ret[i] = None
            else:
               storagetypes.memcache.set( volume_key_name, volume )

         ret[i] = volume

      return ret
            


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
   def update_shard_count( cls, volume_id, num_shards, **txn_args ):
      """
      Update the shard count of the volume, but in a transaction.
      """
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( volume_id=volume_id ) )      
      num_shards = storagetypes.transaction( lambda: __volume_update_shard_count( volume_key, num_shards ), **txn_args )
      
      return num_shards

   @classmethod
   def FlushCache( cls, volume_id ):
      volume_key_name = Volume.make_key_name( volume_id=volume_id )

      storagetypes.memcache.delete(volume_key_name)
      
      
   @classmethod
   def SetCache( cls, volume_id, volume ):
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      
      storagetypes.memcache.set( volume_key_name, volume )
      
   @classmethod
   def Update( cls, volume_id, **fields ):
      '''
      Update volume identified by name with fields specified as a dictionary.
      '''
      invalid = Volume.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      volume = Volume.Read(volume_id)
      if not volume:
         raise Exception("No volume with the ID %d exists.", volume_id)

      Volume.FlushCache( volume_id )

      old_version = volume.version
      old_cert_version = volume.cert_version
      
      # apply update
      for (k,v) in fields.items():
         setattr( volume, k, v )
         
      volume.version = old_version + 1
      
      if "cert_version" in fields:
         volume.cert_version = old_cert_version + 1
         
      return volume.put()
         

   @classmethod
   def Delete( cls, volume_id ):
      '''
      Delete volume from datastore.
      '''
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( volume_id=volume_id ) )
      
      ret = volume_key.delete()
      FlushCache( volume_id )
      
      return ret
   
   @classmethod
   def shard_counter_name( cls, volume_id, suffix ):
      return "%s-%s" % (volume_id, suffix)

   @classmethod
   def increase_file_count( cls, volume_id ):
      name = Volume.shard_counter_name( volume_id, "file_count" )
      shardcounter.increment(name)
   
   @classmethod
   def decrease_file_count( cls, volume_id ):
      name = Volume.shard_counter_name( volume_id, "file_count" )
      shardcounter.decrement(name)
      
   @classmethod
   def get_num_files( cls, volume_id ):
      name = Volume.shard_counter_name( volume_id, "file_count" )
      ret = shardcounter.get_count( name )
      return ret
   
   
   