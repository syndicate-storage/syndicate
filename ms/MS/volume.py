#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import storage
import storage.storagetypes as storagetypes
import storage.shardcounter as shardcounter

import os
import base64
from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import protobufs.ms_pb2 as ms_pb2
import protobufs.sg_pb2 as sg_pb2

import types
import errno
import time
import datetime
import random
import string
import logging
import traceback

from common.msconfig import *

from MS.gateway import Gateway   

class VolumeNameHolder( storagetypes.Object ):
   """
   Mark a Volume name as taken
   """
   
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
      
      
class VolumeCertBundle( storagetypes.Object ):
   """
   Volume certificate bundle.
   Holds a protobuf'ed volume certificate for gateways to fetch.
   Signed by the Volume owner.
   """
   
   cert_protobuf = storagetypes.Blob()  # protobuf'ed cert bundle (i.e. SG manifest) protobuf
   volume_id = storagetypes.Integer()
   
   required_attrs = [
      "volume_id",
      "cert_protobuf"
   ]
   
   @classmethod 
   def make_key_name( cls, volume_id ):
      return "VolumeCertBundle: volume_id=%s" % (volume_id)
   
   
   @classmethod 
   def create_async( cls, _volume_id, _cert_protobuf ):
      return VolumeCertBundle.get_or_insert_async( VolumeCertBundle.make_key_name( _volume_id ), volume_id=_volume_id, cert_protobuf=_cert_protobuf )
   
   
   @classmethod 
   def Get( cls, volume_id, async=False ):
      """
      Fetch a volume cert bundle by volume_id
      """
      key_name = VolumeCertBundle.make_key_name( volume_id )
      volume_cert_bundle = storagetypes.memcache.get( key_name )
      if volume_cert_bundle is None:
         
         volume_cert_bundle_key = storagetypes.make_key( VolumeCertBundle, key_name )
         
         if async:
            volume_cert_bundle_fut = volume_cert_bundle_key.get_async()
            return volume_cert_bundle_fut
         
         else:
            volume_cert_bundle = volume_cert_bundle_key.get()
               
            if volume_cert_bundle is not None:
               storagetypes.memcache.set( key_name, volume_cert_bundle )
               
               
            return volume_cert_bundle
      
      else:
         
         if async:
            return storagetypes.FutureWrapper( volume_cert_bundle )
        
         else:
            
            return volume_cert_bundle
      
   
   @classmethod 
   def Put( cls, volume_id, cert_protobuf ):
      """
      Put a new volume cert bundle (which is really an SG Manifest repurposed)
      Verify that the version number has incremented.

      """
      
      cert = sg_pb2.Manifest() 
      cert.ParseFromString( cert_protobuf )
      
      if cert.volume_id != volume_id:
         raise Exception("Invalid volume ID: %s != %s" % (cert.volume_id, volume_id))
      
      def put_txn():
         
         volume_cert_bundle = cls.Get( volume_id )
         if volume_cert_bundle is not None:
         
            existing_cert = cls.Load( volume_cert_bundle )
            
            if existing_cert.volume_id != volume_id:
               raise Exception("BUG: existing cert bundle is for %s, but expected %s" % (volume_id, existing_cert.volume_id))
            
            if existing_cert.file_version > cert.file_version:
               raise Exception("Stale volume cert version: expected >= %s, got %s" % (existing_cert.file_version, cert.file_version))
            
            if existing_cert.mtime_sec > cert.mtime_sec or (existing_cert.mtime_sec == cert.mtime_sec and existing_cert.mtime_nsec > cert.mtime_nsec):
               # stale 
               raise Exception("Stale cert bundle timestamp: expected > %s.%s, got %s.%s" % (volume_id, existing_cert.mtime_sec, existing_cert.mtime_nsec, cert.mtime_sec, cert.mtime_nsec))
            
            volume_cert_bundle.cert_protobuf = cert_protobuf
            volume_cert_bundle.put()
            
            storagetypes.memcache.delete( VolumeCertBundle.make_key_name( volume_id ) )
         
         else:
            volume_cert_bundle = VolumeCertBundle( key=storagetypes.make_key( VolumeCertBundle, VolumeCertBundle.make_key_name( volume_id ) ), volume_id=volume_id, cert_protobuf=cert_protobuf )
            volume_cert_bundle.put()
         
         return True 
      
      return storagetypes.transaction( put_txn )
               
   
   @classmethod 
   def Delete( cls, volume_id ):
      """
      Delete a cert bundle
      """
      
      key_name = VolumeCertBundle.make_key_name( volume_id )
      volume_cert_bundle_key = storagetypes.make_key( VolumeCertBundle, key_name )
      volume_cert_bundle_key.delete()
      
      storagetypes.memcache.delete( key_name )
      return True 
   
   
   @classmethod 
   def Load( cls, volume_cert_bundle ):
      """
      Given an instance of this class, parse and load its cert bundle.
      Return the cert bundle as a deserialized protobuf.
      """
      
      if volume_cert_bundle.cert_protobuf is not None:
         m = sg_pb2.Manifest()
         m.ParseFromString( volume_cert_bundle.cert_protobuf )
         return m
      
      else:
         return None
      
      
   @classmethod 
   def SetCache( cls, volume_id, volume_cert_bundle ):
      """
      Cache a volume cert bundle
      """
      key_name = VolumeCertBundle.make_key_name( volume_id )
      storagetypes.memcache.set( key_name, volume_cert_bundle )
      

def is_int( x ):
   try:
      y = int(x)
      return True
   except:
      return False
   

class Volume( storagetypes.Object ):
   
   name = storagetypes.String()
   blocksize = storagetypes.Integer( indexed=False ) # Stored in bytes!!
   description = storagetypes.Text()
   owner_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   
   version = storagetypes.Integer( indexed=False )                 # version of this Volume's metadata
   
   private = storagetypes.Boolean()                             # if True, then this Volume won't be listed
   archive = storagetypes.Boolean()                # only an authenticated AG owned by the same user that owns this Volume can write to this Volume
   allow_anon = storagetypes.Boolean()             # if True, then anonymous users can access this Volume (i.e. users who don't have to log in)
   
   num_shards = storagetypes.Integer(default=20, indexed=False)    # number of shards per entry in this volume

   metadata_public_key = storagetypes.Text()          # Volume-owner-given public key, in PEM format.  Used to verify volume owner's signature over volume certs
   
   file_quota = storagetypes.Integer()                 # maximum number of files allowed here (-1 means unlimited)
   
   deleted = storagetypes.Boolean()      # is this Volume deleted?
   
   volume_cert_bin = storagetypes.Blob()                # volume certificate
   
   # for RPC
   key_type = "volume"
   
   # set at runtime to the unprotobufed cert bundle
   cert_bundle = None
   
   
   @classmethod
   def generate_metadata_keys( cls ):
      """
      Generate metadata public/private keys for metadata sign/verify
      """
      return cls.generate_keys( VOLUME_RSA_KEYSIZE )
      
      
   required_attrs = [
      "name",
      "blocksize",
      "owner_id",
      "private"
   ]

   key_attrs = [
      "volume_id"
   ]
   
   validators = {
      "name": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-. ")) ) == 0 and not is_int(value) ),
      "metadata_public_key": (lambda cls, value: cls.is_valid_key( value, VOLUME_RSA_KEYSIZE )),
      "blocksize": (lambda cls, value: value > 0)
   }

   default_values = {
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1),
      "private": (lambda cls, attrs: True),
      "archive": (lambda cls, attrs: False),
      "allow_anon": (lambda cls, attrs: False),
      "file_quota": (lambda cls, attrs: -1),
      "deleted": (lambda cls, attrs: False),
      "num_shards": (lambda cls, attrs: 20)
   }
   
   read_attrs_api_required = [
      "version",
      "allow_anon",
      "file_quota"
   ]
   
   read_attrs = [
      "name",
      "description",
      "owner_id",
      "volume_id",
      "metadata_public_key",
      "private",
      "archive",
      "blocksize"
   ] + read_attrs_api_required
   
   # fields an API call can set
   write_attrs = [
      "volume_cert_bin"
   ]
   
   # what fields in the cert can change across cert versions?
   modifiable_cert_fields = [
      "description",
      "private",
      "archive",
      "file_quota",
      "allow_anon",
      "version"
   ]
   
   write_attrs_api_required = write_attrs
   
   
   def owned_by( self, user ):
      return user.owner_id == self.owner_id
   
   
   def need_gateway_auth( self ):
      """
      Do we require an authentic gateway to interact with us?
      (i.e. do we forbid anonymous users)?
      """
      if not self.allow_anon:
         return True
      
      return False
      

   def is_gateway_in_volume( self, gateway ):
      """
      Determine whether a given Gateway instance belongs to this Volume.
      If the Volume is not private, then it "belongs" by default.
      """
      
      if self.allow_anon:
         return True

      return gateway.volume_id == self.volume_id

   
   @classmethod 
   def cert_to_dict( cls, volume_cert ):
      """
      Convert a protobuf structure to a dict of values,
      using the Volume property names.
      """
      
      # unpack certificate 
      # skip owner_email
      
      blocksize = volume_cert.blocksize 
      owner_id = volume_cert.owner_id 
      volume_id = volume_cert.volume_id 
      volume_version = volume_cert.volume_version
      name = volume_cert.name 
      description = volume_cert.description 
      volume_public_key = volume_cert.volume_public_key
      archive = volume_cert.archive 
      private = volume_cert.private 
      allow_anon = volume_cert.allow_anon 
      file_quota = volume_cert.file_quota
      
      kwargs = {
         "name": name,
         "blocksize": blocksize,
         "description": description,
         "owner_id": owner_id,
         "volume_id": volume_id,
         "version": volume_version,
         "private": private,
         "archive": archive,
         "allow_anon": allow_anon,
         "metadata_public_key": volume_public_key,
         "file_quota": file_quota
      }
      
      return kwargs


   @classmethod
   def Create( cls, user, volume_cert ):
      """
      Create a Volume to be owned by a user.  The user, being the volume owner, gets full control over it.
      
      NOTE: the caller will need to have validated and verified the authenticity of volume_cert.
      NOTE: this calls should be followed up with a VolumeCertBundle.Put() to put the caller's new volume cert bundle
      """
      
      # sanity check 
      if not user:
         raise Exception( "No user given" )
      
      if user.owner_id != volume_cert.owner_id:
         raise Exception("Invalid user: %s != %s" % (user.owner_id, volume_cert.owner_id ))
      
      kwargs = cls.cert_to_dict( volume_cert )
      
      # Validate (should be fine)
      missing = Volume.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))
      
      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
     
      # sanity check 
      if len(kwargs['name']) == 0:
         raise Exception("Empty volume name")

      volume_id = kwargs['volume_id']
      
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      volume_key = storagetypes.make_key( Volume, volume_key_name )
         
      # put the Volume and nameholder at the same time---there's a good chance we'll succeed
      volume_nameholder_fut = VolumeNameHolder.create_async( kwargs['name'], volume_id )
      volume_fut = Volume.get_or_insert_async(  volume_key_name,
                                                name=kwargs['name'],
                                                blocksize=kwargs['blocksize'],
                                                description=kwargs['description'],
                                                owner_id=user.owner_id,
                                                volume_id=volume_id,
                                                version=kwargs['version'],
                                                private=kwargs['private'],
                                                archive=kwargs['archive'],
                                                allow_anon = kwargs['allow_anon'],
                                                metadata_public_key = kwargs['metadata_public_key'],
                                                deleted=False,
                                                volume_cert_bin=volume_cert.SerializeToString()
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
         storagetypes.deferred.defer( Volume.delete_all, [volume_key, volume_nameholder.key] )
         raise Exception( "Volume ID collision.  Please try again" )
      
      # set permissions
      """
      req = VolumeAccessRequest.create_async( user.owner_id, volume_id, kwargs['name'], random.randint(-2**63, 2**63 - 1), VolumeAccessRequest.STATUS_GRANTED,
                                              gateway_caps=kwargs['default_gateway_caps'], allowed_gateways=[], request_message="Created").get_result()
      """
      return volume_key
         

   @classmethod
   def Read( cls, volume_name_or_id, async=False, use_memcache=True ):
      """
      Given a volume ID, get the Volume.
      
      Arguments:
      volume_name_or_id -- name or ID of the Volume to get (str or int)
      
      Keyword arguments:
      async             -- If True, return a Future for the Volume (bool)
      use_memcache      -- If True, check memcache for the Volume, and if async is false, cache the results.
      """
      
      volume_id = None
      volume_name = None
      
      try:
         volume_id = int( volume_name_or_id )
      except:
         volume_name = volume_name_or_id
         return cls.Read_ByName( volume_name, async=async, use_memcache=use_memcache )
         
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      volume_key = storagetypes.make_key( Volume, volume_key_name )

      volume = storagetypes.memcache.get( volume_key_name )
      if volume == None:
         if async:
            return volume_key.get_async( use_memcache=False )
         
         else:
            volume = volume_key.get( use_memcache=False )
            if not volume:
               return None
            else:
               storagetypes.memcache.set( volume_key_name, volume )

      elif async:
         volume = storagetypes.FutureWrapper( volume )
         
      return volume


   @classmethod
   def Read_ByName( cls, volume_name, async=False, use_memcache=True ):
      """
      Given a volume name, get the Volume.
      
      This is slower than Read(), since the name isn't in the object's key.
      
      Arguments:
      volume_name       -- name of the Volume to get (str)
      
      Keyword arguments:
      async             -- If true, return a Future for the query (bool)
      use_memcache      -- If true, check memcache for the Volume, and if async is false, cache the results
      """
      
      vol_name_to_id_cached = None
      
      if use_memcache:
         vol_name_to_id_cached = "Read_ByName: volume_name=%s" % volume_name
         volume_id = storagetypes.memcache.get( vol_name_to_id_cached )
         
         if volume_id != None and isinstance( volume_id, int ):
            vol = Volume.Read( volume_id, async=async, use_memcache=use_memcache )
            return vol
         
      # no dice 
      vol = Volume.ListAll( {"Volume.name ==": volume_name, "Volume.deleted ==": False}, async=async )
      
      if async:
         # this will be a Future 
         return storagetypes.FutureQueryWrapper( vol )

      elif vol:
         if len(vol) > 1:
            # something's wrong...there should only be one
            raise Exception("Multiple Volumes named '%s'" % (volume_name))
         
         vol = vol[0]
         if not vol:
            vol = None
            
         elif use_memcache and not vol.deleted:
            to_set = {
               vol_name_to_id_cached: vol.volume_id,
               Volume.make_key_name( volume_id=vol.volume_id ): vol
            }
            
            storagetypes.memcache.set_multi( to_set )
      else:
         vol = None

      if vol and vol.deleted:
         return None
         
      return vol
            
   '''
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
               elif use_memcache and not volume.deleted:
                  storagetypes.memcache.set( volume_key_name, volume )

         else:
            if not async:
               # get results directly 
               ret[i] = volume
            else:
               # wrap as future
               ret[i] = storagetypes.FutureWrapper( volume )

      return ret
   '''

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
      num_shards = storagetypes.transaction( lambda: Volume.__update_shard_count( volume_key, num_shards ), **txn_args )
      
      return num_shards


   @classmethod
   def FlushCache( cls, volume_id ):
      volume_key_name = Volume.make_key_name( volume_id=volume_id )
      storagetypes.memcache.delete( volume_key_name )
      
      
   @classmethod
   def SetCache( cls, volume_id, volume ):
      volume_key_name = Volume.make_key_name( volume_id=volume_id )      
      storagetypes.memcache.set( volume_key_name, volume )
      
      
   @classmethod
   def Update( cls, volume_name_or_id, volume_cert ):
      '''
      Atomically (transactionally) update a given Volume with the given fields.
      
      NOTE: volume_cert will need to have been validated by the caller.
      NOTE: this calls should be followed up with a VolumeCertBundle.Put() to put the caller's new volume cert bundle
      
      return the volume key on success 
      raise an Exception on error
      '''
      try:
         volume_id = int(volume_name_or_id)
      except:
         volume = Volume.Read_ByName( volume_name_or_id )
         if volume:
            volume_id = volume.volume_id
         else:
            raise Exception("No such Volume '%s'" % volume_name_or_id )
      
      fields = cls.cert_to_dict( volume_cert )
      
      invalid = Volume.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
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
      
      volume_cert_bin = volume_cert.SerializeToString()
      
      def update_txn( fields ):
         '''
         Update the Volume transactionally.
         '''
         
         volume = Volume.Read(volume_id)
         if not volume or volume.deleted:
            # volume does not exist...
            # if we were to rename it, then delete the new nameholder
            if rename:
               storagetypes.deferred.defer( Volume.delete_all, [volume_nameholder_new_key] )
               
            raise Exception("No volume with the ID %d exists.", volume_id)
         
         old_name = volume.name 
         
         # verify update
         unwriteable = []
         for (k, v) in fields.items():
            if k not in cls.modifiable_cert_fields and getattr(volume, k) != v:
               unwriteable.append(k)
               
         if len(unwriteable) > 0:
            raise Exception("Tried to modify read-only fields: %s" % ",".join(unwriteable))
         
         # check version...
         if volume.version > fields['version']:
            raise Exception("Stale Volume version: expected > %s, got %s" % (volume.version, fields['version']))
         
         # apply update
         for (k,v) in fields.items():
            setattr( volume, k, v )
         
         # store new cert 
         volume.volume_cert_bin = volume_cert_bin
         
         ret = volume.put()
         
         Volume.FlushCache( volume_id )
         
         return ret
      
      
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
         
         # make sure Read_ByName uses the right name
         vol_name_to_id_cached = "Read_ByName: volume_name=%s" % old_name
         storagetypes.memcache.delete( vol_name_to_id_cached )
         
      return volume_key
         

   @classmethod
   def delete_volume_and_friends( cls, volume_id, volume_name ):
      """
      Delete the following for a particular volume, as a deferred task:
         the Volume
         # all Volume access requests 
         the Volume name holder
         
      Does not delete attached gateways.
      """
      
      futs = []
      
      # delete volume 
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( volume_id=volume_id ) )
      futs.append( volume_key.delete_async() )
      
      # delete volume nameholder
      volume_nameholder_key = storagetypes.make_key( VolumeNameHolder, VolumeNameHolder.make_key_name( volume_name ) )
      futs.append( volume_nameholder_key.delete_async() )
      
      storagetypes.wait_futures( futs )
      

   @classmethod
   def Delete( cls, volume_name_or_id ):
      '''
      Delete volume from datastore, as well as access requests and rights.
      
      Arguments:
      volume_id         -- ID of the Volume to delete
      '''
      
      volume = Volume.Read( volume_name_or_id )
      if volume == None:
         # done!
         return True
      
      volume.deleted = True
      volume.put()
      
      Volume.FlushCache( volume.volume_id )
      
      vol_name_to_id_cached = "Read_ByName: volume_name=%s" % volume.name 
      storagetypes.memcache.delete( vol_name_to_id_cached )
      
      # blow away the associated data
      storagetypes.deferred.defer( Volume.delete_volume_and_friends, volume.volume_id, volume.name )
      
      return True
   
   @classmethod
   def shard_counter_name( cls, volume_id, suffix ):
      return "%s-%s" % (volume_id, suffix)

