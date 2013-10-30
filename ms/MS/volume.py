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
from Crypto.Signature import PKCS1_v1_5 as CryptoSigner

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
import string
import logging
import traceback

from user import SyndicateUser
from msconfig import *

class VolumeNameHolder( storagetypes.Object ):
   '''
   Mark a Volume name as taken
   '''
   
   name = storagetypes.String()
   volume_id = storagetypes.Integer()
   
   required_attrs = [
      "name"
   ]
   
   
   @classmethod
   def make_key_name( cls, name ):
      return "VolumeNameHolder: name=%s" % (name)
   
   @classmethod
   def create_async( cls,  _name, _id ):
      return VolumeNameHolder.get_or_insert_async( VolumeNameHolder.make_key_name( _name ), name=_name, volume_id=_id )
      
      

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
   def hash_volume_secret( cls, secret ):

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
   
   @classmethod
   def is_valid_key( cls, key_str ):
      '''
      Validate a given PEM-encoded public key, both in formatting and security.
      '''
      try:
         key = RSA.importKey( key_str )
      except Exception, e:
         logging.error("RSA.importKey %s", traceback.format_exc() )
         return False

      # must have desired security level 
      if key.size() != VOLUME_RSA_KEYSIZE - 1:
         logging.error("invalid key size = %s" % key.size() )
         return False

      return True
      
      
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
      "public_key": (lambda cls, value: Volume.is_valid_key( value )),
      "private_key": (lambda cls, value: Volume.is_valid_key( value ))
   }

   default_values = {
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1),
      "cert_version": (lambda cls, attrs: 1),
      "private": (lambda cls, attrs: True),
      "archive": (lambda cls, attrs: False)
   }
   
   read_attrs = [
      "name",
      "blocksize",
      "active",
      "description",
      "owner_id",
      "volume_id",
      "version",
      "cert_version",
      "private",
      "archive",
      "public_key"
   ]
   
   write_attrs = [
      "active",
      "description",
      "owner_id",
      "volume_id",
      "private",
      "archive",
      "public_key",
      "private_key",
      "volume_secret"
   ]
   
   
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
      """
      Given an ms_gateway_cert protobuf and a gateway record, have the gateway populate the 
      cert protobuf and then have the Volume optionally sign it with its private key.
      """
      
      gateway.protobuf_cert( gateway_cert )
      
      gateway_cert.signature = ""
      
      if sign:
         # sign the cert
         data = gateway_cert.SerializeToString()
         sig = self.sign_message( data )
         
         gateway_cert.signature = sig
      
      return 
   
   
   def protobuf_gateway_cert_manifest( self, manifest, gateway_classes, sign=True ):
      """
      Generate a specially-crafted manifest protobuf, which a gateway can use to learn 
      the IDs and types of all gateways in the Volume, as well as their certs' versions.
      
      gateway_classes is a list of Gateway subclasses for us to process
      """
      
      manifest.volume_id = self.volume_id
      manifest.coordinator_id = 0
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
      """
      Determine whether a given Gateway instance belongs to this Volume.
      If the Volume is not private, then it "belongs" by default.
      """
      
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
      signer = CryptoSigner.new(key)
      signature = signer.sign( h )
      sigb64 = base64.b64encode( signature )
      return sigb64
      

   @classmethod
   def Create( cls, username, **kwargs ):
      """
      Given volume data, store it.
      Update the corresponding SyndicateUser atomically along with creating the Volume
      so that the SyndicateUser owns the Volume.
      
      Arguments:
      username          -- Name of the SyndicateUser that will own this Volume
      
      Required keyword arguments:
      name              -- name of the Volume (str)
      blocksize         -- size of the Volume's blocks in bytes (int)
      description       -- description of the Volume (str)
      private           -- whether or not this Volume is visible to other users (bool)
      volume_secret     -- password to use for administrating this Volume (str)
      
      Optional keyword arguments:
      public_key        -- PEM-encoded RSA public key, 4096 bits (str)
      private_key       -- PEM-encoded RSA private key, 4096 bits (str)
      archive           -- whether or not this Volume is populated only by Acquisition Gateways (bool)
      """
      
      # sanity check 
      if not username:
         raise Exception( "No username given" )
      
      kwargs['owner_id'] = 0     # will look up user and fill with owner ID once we validate input.
      Volume.fill_defaults( kwargs )

      # Get or finalize credentials
      volume_secret = kwargs.get("volume_secret")

      if volume_secret == None:
         raise Exception( "No password given")

      else:
         volume_secret_salt, volume_secret_salted_hash = Volume.hash_volume_secret(volume_secret)

         kwargs['volume_secret_salt'] = volume_secret_salt
         kwargs['volume_secret_salted_hash'] = volume_secret_salted_hash

      # generate keys if they're not given 
      if kwargs.get('public_key') == None or kwargs.get('private_key') == None:
         kwargs['public_key'], kwargs['private_key'] = Volume.generate_volume_keys()

      # Validate
      missing = Volume.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      
      #kwargs['name'] = unicode(kwargs['name']).strip().replace(" ","_")
      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # attempt to create the Volume
      volume_id = random.randint( 1, 2**63 - 1 )
      
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      volume_key = storagetypes.make_key( Volume, volume_key_name )
      
      # get the user
      user = SyndicateUser.Read( username )
      if not user:
         raise Exception( "No such user '%s'" % username )
      
      # put the Volume and nameholder at the same time---there's a good chance we'll succeed
      volume_nameholder_fut = VolumeNameHolder.create_async( kwargs['name'], volume_id )
      volume_fut = Volume.get_or_insert_async(  volume_key_name,
                                                name=kwargs['name'],
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
      
      storagetypes.wait_futures( [volume_nameholder_fut, volume_fut] )
      
      # verify that there was no collision
      volume = volume_fut.get_result()
      volume_nameholder = volume_nameholder_fut.get_result()
      
      if volume_nameholder.volume_id != volume_id:
         # name collision
         storagetypes.deferred.defer( Volume.delete_all, [volume_key] )
         raise Exception( "Volume '%s' already exists!" % kwargs['name'])
      
      if volume.volume_id != volume_id:
         # ID collision
         storagetypes.deferred.defer( Volume.delete_all, [volume_nameholder.key] )
         raise Exception( "Volume ID collision.  Please try again" )
      
      # bind the User to the Volume
      try:
         storagetypes.transaction( lambda: SyndicateUser.add_volume_to_user(volume_id, username) )
      except Exception, e:
         log.exception( e )
         raise e
      
      return volume_key
         

   @classmethod
   def Read( cls, volume_id, async=False, use_memcache=True ):
      """
      Given a volume ID, get the Volume.
      
      Arguments:
      volume_id         -- ID of the Volume to get (int)
      
      Keyword arguments:
      async             -- If true, return a Future for the Volume (bool)
      use_memcache      -- If True, check memcache for the Volume, and if async is false, cache the results.
      """
      
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      volume_key = storagetypes.make_key( Volume, volume_key_name )

      volume = None
      
      if use_memcache:
         volume = storagetypes.memcache.get( volume_key_name )
         
      if volume == None:
         if not async:
            volume = volume_key.get( use_memcache=False )
         else:
            return volume_key.get_async( use_memccahe=False )
         
         if not volume:
            return None
         elif use_memcache:
            storagetypes.memcache.set( volume_key_name, volume )

      return volume

   @classmethod
   def ReadAll( cls, volume_ids, async=False, use_memcache=True ):
      """
      Given a set of Volume IDs, get all of the Volumes.
      
      Arguments:
      volume_ids        -- IDs of the Volumes to get ([int])
      
      Keyword arguments:
      async             -- If true, return a list of Futures for each Volume
      use_memcache      -- If true, check memcache for each Volume and if async is false, cache the results.
      """
      
      volume_key_names = map( lambda x: Volume.make_key_name( volume_id=x ), volume_ids )

      volumes_dict = {}
      if use_memcache:
         volumes_dict = storagetypes.memcache.get_multi( volume_key_names )

      ret = [None] * len(volume_key_names)

      for i in xrange(0, len(volume_key_names)):
         volume_key_name = volume_key_names[i]
         volume = volumes_dict.get( volume_key_name )
         if volume == None:
            volume_key = storagetypes.make_key( Volume, volume_key_name )
            
            if async:
               ret[i] = volume_key.get_async( use_memcache=False )
            
            else:
               volume = volume_key.get( use_memcache=False )
               if not volume:
                  ret[i] = None
               elif use_memcache:
                  storagetypes.memcache.set( volume_key_name, volume )

         else:
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
      Atomically (transactionally) update a given Volume with the given fields.
      
      Arguments:
      volume_id         -- ID of the Volume to update.
      
      Keyword arguments: same as Create()
      '''
      
      invalid = Volume.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # make sure we're only writing correct fields
      invalid = Volume.validate_write( fields )
      if len(invalid) != 0:
         raise Exception( "Unwritable fields: %s" % (", ".join( invalid )) )
      
      # if we're updating either the public or private key, then we'll need to verify that both are present
      if (fields.has_key( "public_key" ) and not fields.has_key( "private_key" )) or (not fields.has_key("public_key") and fields.has_key("private_key")):
         raise Exception( "Need both public_key and private_key when changing the keys" )
      
      # if we're changing the password, then generate the salt and hash
      if "volume_secret" in fields.keys():
         volume_secret_salt, volume_secret_salted_hash = Volume.hash_volume_secret( fields['volume_secret'] )
         
         fields['volume_secret_salt'] = volume_secret_salt
         fields['volume_secret_salted_hash'] = volume_secret_salted_hash
      
      # are we changing the name? acquire the new name if so
      rename = False
      volume_nameholder_new_key = None
      old_name = None
      
      if "name" in fields.keys():
         volume_nameholder_new_fut = VolumeNameHolder.create_async( fields.get("name"), volume_id )
         
         volume_nameholder_new = volume_nameholder_new_fut.get_result()
         volume_nameholder_new_key = volume_nameholder_new.key
         
         if volume_nameholder_new.volume_id != volume_id:
            # name collision
            raise Exception("Volume '%s' already exists!" % (fields.get("name")) )
         
         else:
            # reserved!
            rename = True
      
      def update_txn( fields ):
         '''
         Update the Volume transactionally.
         '''
         
         volume = Volume.Read(volume_id)
         if not volume:
            # volume does not exist...
            # if we were to rename it, then delete the new nameholder
            if rename:
               storagetypes.deferred.defer( Volume.delete_all, [volume_nameholder_new_key] )
               
            raise Exception("No volume with the ID %d exists.", volume_id)

         
         old_name = volume.name 
         
         # purge from cache
         Volume.FlushCache( volume_id )
         
         old_version = volume.version
         old_cert_version = volume.cert_version
         
         # apply update
         for (k,v) in fields.items():
            setattr( volume, k, v )
         
         if "version" in fields.keys():
            # forced revision.  Ignore the actual value; just change it
            volume.version = old_version + 1
         
         if "cert_version" in fields.keys():
            # forced cert reverion.  Ignore the actual value, just change it
            volume.cert_version = old_cert_version + 1
         
         return volume.put()
      
      
      volume_key = None
      try:
         volume_key = storagetypes.transaction( lambda: update_txn( fields ), xg=True )
      except Exception, e:
         logging.exception( e )
         raise e
      
      if rename:
         # delete the old placeholder
         volume_nameholder_old_key = storagetypes.make_key( VolumeNameHolder, VolumeNameHolder.make_key_name( old_name ) )
         storagetypes.deferred.defer( Volume.delete_all, [volume_nameholder_old_key] )
         
      return volume_key
         

   @classmethod
   def Delete( cls, volume_id ):
      '''
      Delete volume from datastore.
      
      Arguments:
      volume_id         -- ID of the Volume to delete (str)
      '''
      
      volume = Volume.Read( volume_id )
      if volume == None:
         # done!
         return True
      
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( volume_id=volume_id ) )
      volume_nameholder_key = storagetypes.make_key( VolumeNameHolder, VolumeNameHolder.make_key_name( name=volume.name ) )
      
      volume_delete_fut = volume_key.delete_async()
      volume_nameholder_delete_fut = volume_nameholder_key.delete_async()
      
      FlushCache( volume_id )
      
      storagetypes.wait_futures( [volume_delete_fut, volume_nameholder_delete_fut] )
      
      return True
   
   @classmethod
   def Mount( cls, volume_id, mountpoint_id, new_volume_id ):
      '''
      Attach one Volume to a particular directory in another one.
      '''
      return volume_mount( volume_id, mountpoint_id, new_volume_id )
   
   @classmethod
   def Unmount( cls, volume_id, mountpoint_id ):
      '''
      Detach a Volume mounted at a given mountpoint.
      '''
      return volume_umount( volume_id, mountpoint_id )
   
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
   
  

def volume_mount( volume_id, mountpoint_id, new_volume_id ):
   '''
   Mount the Volume named by new_volume_id on the directory within the Volume
   indicated by volume_id at the directory indicated by mountpoint_id.
   
   WARNING: not tested yet!
   '''
   
   from entry import MSEntry, MSENTRY_TYPE_DIR
   
   def volume_mount_txn( vol, mountpoint_id, new_vol ):
      
      # load the mountpoint and new volume root
      mountpoint_fut = MSEntry.ReadBase( vol.volume_id, mountpoint_id, async=True )
      new_vol_root_fut = MSEntry.ReadBase( new_vol.volume_id, 0, async=True )
      
      storagetypes.wait_futures( [mountpoint_fut, new_vol_root_fut] )
      
      mountpoint = mountpoint_fut.get_result()
      new_vol_root = new_vol_root_fut.get_result()
      
      if mountpoint == None or new_vol_root == None:
         return -errno.ENOENT
      
      if mountpoint.ftype != MSENTRY_TYPE_DIR:
         return -errno.ENOTDIR 
      
      mountpoint_key_name = MSEntry.make_key_name( mountpoint.volume_id, mountpoint.file_id )
      umount_id = MSEntry.umount_key_name( mountpoint.volume_id, mountpoint.file_id )
      
      # put the new Volume's root as the mountpoint, and back up the mountpoint
      new_mountpoint = MSEntry( key=mountpoint_key_name, umount_id=umount_id )
      old_mountpoint = MSEntry( key=umount_id )
      
      new_mountpoint.populate( **new_vol_root.to_dict() )
      old_mountpoint.populate( **mountpoint.to_dict() )
      
      new_shard = MSEntry.update_shard( vol.num_shards, new_mountpoint )
      
      new_mountpoint_fut = new_mountpoint.put_async()
      old_mountpoint_fut = old_mountpoint.put_async()
      new_shard_fut = new_shard.put_async()
   
      storagetypes.wait_futures( [new_mountpoint_fut, old_mountpoint_fut, new_shard_fut] )
      
      return
   
   vol_fut = Volume.Read( volume_id, async=True )
   new_vol_fut = Volume.Read( new_volume_id, async=True )
   
   storagetypes.wait_futures( [vol_fut, new_vol_fut] )   

   vol = vol_fut.get_result()
   new_vol = new_vol_fut.get_result()
   
   if vol == None or new_vol == None:
      return -errno.ENODEV
   
   return storagetypes.transaction( lambda: volume_mount_txn( vol, mountpoint_id, new_vol ), xg=True )


def volume_unmount( volume_id, mountpoint_id ):
   '''
   Given the ID of a directory currently mounting a Volume, unmount it and restore 
   the original directory.
   
   WARNING: not tested yet!
   '''
   
   from entry import MSEntry, MSENTRY_TYPE_DIR
   
   def volume_unmount_txn( vol, mountpoint_id ):
      
      cur_mountpoint_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( vol.volume_id, mountpoint_id ) )
      
      cur_mountpoint = cur_mountpoint_key.get()
      
      if curr_mountpoint == None:
         return -errno.ENOENT
      
      old_mountpoint_key = storagetypes.make_key( MSEntry, cur_mountpoint.umount_id )
      
      old_mountpoint = old_mountpoint_key.get()
      
      if old_mountpoint == None:
         return -errno.ENOENT
      
      # restore the old directory
      restored_mountpoint = MSEntry( key=MSEntry.make_key_name( vol.volume_id, mountpoint_id ) )
      restored_mountpoint.populate( old_mountpoint.to_dict() )
      restored_mountpoint_shard = MSEntry.update_shard( vol.num_shards, restored_mountpoint )
      
      restored_mountpoint_fut = restored_mountpoint.put_async()
      restored_mountpoint_shard_fut = restored_mountpoint_shard.put_async()
      
      storagetypes.wait_futures( [restored_mountpoint_fut, restored_mountpoint_shard_fut] )
      
      return
  
   vol = Volume.Read( volume_id )
   
   return storagetypes.transaction( lambda: volume_umount_txn( vol, mountpoint_id ), xg=True )

