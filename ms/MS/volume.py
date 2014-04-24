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


# ----------------------------------
class VolumeAccessRequest( storagetypes.Object ):
   """
   This object controls what kinds of Gateways a user can create within a Volume,
   and what capabilities they are allowed to have.
   """
   
   STATUS_PENDING = 1
   STATUS_GRANTED = 2
   
   requester_owner_id = storagetypes.Integer()          # owner id of the requester
   request_message = storagetypes.Text()                # message to the owner 
   volume_id = storagetypes.Integer()                   # ID of volume to join 
   gateway_caps = storagetypes.Integer( indexed=False )  # gateway capabilities requested (only apply to User Gateways)
   nonce = storagetypes.Integer( indexed=False )         # detect collision with another one of these
   request_timestamp = storagetypes.Integer()           # when was the request made?
   status = storagetypes.Integer()                      # granted or pending?
   allowed_gateways = storagetypes.Integer(default=0)   # bit vector representing GATEWAY_TYPE_*G (from msconfig)
   
   # purely for readability
   volume_name = storagetypes.String()
   
   required_attrs = [
      "requester_owner_id",
      "nonce",
      "request_timestamp",
      "status",
      "volume_name"
   ]
   
   read_attrs = [
      "requester_owner_id",
      "request_message",
      "volume_id",
      "gateway_caps",
      "request_timestamp",
      "status",
      "volume_name",
      "allowed_gateways"
   ]
   
   read_attrs_api_required = read_attrs
   
   def owned_by( self, user ):
      return (user.owner_id == self.requester_owner_id)
   
   @classmethod
   def make_key_name( cls, requester_owner_id, volume_id ):
      return "VolumeAccessRequest: owner_id=%s,volume_id=%s" % (requester_owner_id, volume_id)
   
   @classmethod
   def create_async( cls, _requester_owner_id, _volume_id, _volume_name, _nonce, _status, **attrs ):
      ts = int(storagetypes.get_time())
      return VolumeAccessRequest.get_or_insert_async( VolumeAccessRequest.make_key_name( _requester_owner_id, _volume_id ),
                                                      requester_owner_id = _requester_owner_id,
                                                      volume_id = _volume_id,
                                                      nonce=_nonce,
                                                      request_timestamp=ts,
                                                      status=_status,
                                                      volume_name=_volume_name,
                                                      **attrs )
   

   @classmethod
   def RequestAccess( cls, owner_id, volume_id, volume_name, allowed_gateways, gateway_caps, message ):
      """
      Create a request that a particular user be allowed to provision Gateways for a particular Volume.
      If User Gateways are allowed, then gateway_caps controls what those capabilities are allowed to be.
      Include a message that the Volume owner will be able to read.
      
      Return if the request was successfully placed.
      Raise an exception if there is already a pending request.
      """
      
      nonce = random.randint( -2**63, 2**63 - 1 )
      req_fut = VolumeAccessRequest.create_async( owner_id, volume_id, volume_name, nonce, VolumeAccessRequest.STATUS_PENDING, request_message=message, gateway_caps=gateway_caps, allowed_gateways=allowed_gateways )
      req = req_fut.get_result()
      
      # duplicate?
      if req.nonce != nonce:
         raise Exception( "User already attempted to join Volume '%s'" % (owner_id, volume_name) )
      
      return True
   
   @classmethod
   def GrantAccess( cls, owner_id, volume_id, volume_name, allowed_gateways=None, gateway_caps=None ):
      """
      Allow a given user to create Gateways within a given Volume, subject to given capabilities.
      """
      
      # verify the arguments are valid 
      if allowed_gateways is not None and (allowed_gateways & ~((1 << GATEWAY_TYPE_UG) | (1 << GATEWAY_TYPE_RG) | (1 << GATEWAY_TYPE_AG))) != 0:
         # extra bits 
         raise Exception("Invalid bit field for allowed_gateways (%x)" % (allowed_gateways))
      
      if gateway_caps is not None and (gateway_caps & ~(GATEWAY_CAP_READ_DATA | GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA | GATEWAY_CAP_COORDINATE)) != 0:
         # extra bits 
         raise Exception("Invalid bit field for gateway_caps (%x)" % (gateway_caps))
      
      nonce = random.randint( -2**63, 2**63 - 1 )
      req_fut = VolumeAccessRequest.create_async( owner_id, volume_id, volume_name, nonce, VolumeAccessRequest.STATUS_GRANTED, request_message="", gateway_caps=gateway_caps, allowed_gateways=allowed_gateways )
      req = req_fut.get_result()
      
      if req.nonce != nonce:
         # Request existed. update and put
               
         if gateway_caps != None:
            req.gateway_caps = gateway_caps 
         
         if allowed_gateways != None:
            req.allowed_gateways = allowed_gateways
            
         req.status = VolumeAccessRequest.STATUS_GRANTED
         req.put()
      
      req_key_name = VolumeAccessRequest.make_key_name( owner_id, volume_id )
      storagetypes.memcache.delete( req_key_name )
      
      return True
      
   @classmethod
   def GetAccess( cls, owner_id, volume_id ):
      """
      Get the access status of a user in a Volume.
      """
      
      req_key_name = VolumeAccessRequest.make_key_name( owner_id, volume_id )
      req = storagetypes.memcache.get( req_key_name )
      
      if req != None:
         return req
      
      req_key = storagetypes.make_key( VolumeAccessRequest, req_key_name )
      
      req = req_key.get() 
      if req != None:
         storagetypes.memcache.set( req_key_name, req )
      
      return req
   
   @classmethod
   def RemoveAccessRequest( cls, owner_id, volume_id ):
      """
      Delete an access request.
      """
      
      req_key_name = VolumeAccessRequest.make_key_name( owner_id, volume_id )
      req_key = storagetypes.make_key( VolumeAccessRequest, req_key_name )
      storagetypes.deferred.defer( cls.delete_all, [req_key] )
      storagetypes.memcache.delete( req_key_name )
      
      return True
   
   @classmethod
   def ListUserAccessRequests( cls, owner_id, **q_opts ):
      
      return VolumeAccessRequest.ListAll( {"requester_owner_id ==": owner_id}, **q_opts )
   
   @classmethod
   def ListVolumeAccessRequests( cls, volume_id, **q_opts ):
      
      return VolumeAccessRequest.ListAll( {"volume_id ==": volume_id, "status ==": VolumeAccessRequest.STATUS_PENDING}, **q_opts )

   @classmethod
   def ListVolumeAccess( cls, volume_id, **q_opts ):
      
      return VolumeAccessRequest.ListAll( {"volume_id ==": volume_id, "status ==": VolumeAccessRequest.STATUS_GRANTED}, **q_opts )



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
   
   name = storagetypes.String()
   blocksize = storagetypes.Integer( indexed=False ) # Stored in bytes!!
   active = storagetypes.Boolean()
   description = storagetypes.Text()
   owner_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   
   version = storagetypes.Integer( indexed=False )                 # version of this Volume's metadata
   cert_version = storagetypes.Integer( indexed=False )            # certificate bundle version
   
   private = storagetypes.Boolean()                             # if True, then this Volume won't be listed
   archive = storagetypes.Boolean()                # only an authenticated AG owned by the same user that owns this Volume can write to this Volume
   allow_anon = storagetypes.Boolean()             # if True, then anonymous users can access this Volume (i.e. users who don't have to log in)
   
   num_shards = storagetypes.Integer(default=20, indexed=False)    # number of shards per entry in this volume

   metadata_public_key = storagetypes.Text()          # Volume public key, in PEM format, for verifying metadata
   metadata_private_key = storagetypes.Text()         # Volume private key, in PEM format, for signing metadata
   
   file_quota = storagetypes.Integer()                 # maximum number of files allowed here (-1 means unlimited)
   
   deleted = storagetypes.Boolean()      # is this Volume deleted?
   
   default_gateway_caps = storagetypes.Integer( indexed=False )
   
   closure = storagetypes.Text()          # base64-encoded closure for connecting to the cache providers

   # for RPC
   key_type = "volume"
   
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
      "private",
      "metadata_private_key",
      "default_gateway_caps",
   ]

   key_attrs = [
      "volume_id"
   ]
   
   validators = {
      "name": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")) ) == 0 and not is_int(value) ),
      "metadata_public_key": (lambda cls, value: cls.is_valid_key( value, VOLUME_RSA_KEYSIZE )),
      "metadata_private_key": (lambda cls, value: cls.is_valid_key( value, VOLUME_RSA_KEYSIZE ))
   }

   default_values = {
      "blocksize": (lambda cls, attrs: 61440), # 60 KB
      "version": (lambda cls, attrs: 1),
      "cert_version": (lambda cls, attrs: 1),
      "private": (lambda cls, attrs: True),
      "archive": (lambda cls, attrs: False),
      "allow_anon": (lambda cls, attrs: False),
      "active": (lambda cls, attrs: True),
      "file_quota": (lambda cls, attrs: -1),
      "deleted": (lambda cls, attrs: False),
      "num_shards": (lambda cls, attrs: 20),
      "default_gateway_caps": (lambda cls, attrs: GATEWAY_CAP_READ_METADATA | GATEWAY_CAP_READ_DATA )           # read only
   }
   
   read_attrs_api_required = [
      "blocksize",
      "active",
      "volume_id",
      "version",
      "cert_version",
      "private",
      "archive",
      "allow_anon",
      "file_quota",
      "default_gateway_caps",
      "closure"
   ]
   
   read_attrs = [
      "name",
      "description",
      "owner_id",
      "metadata_public_key",
   ] + read_attrs_api_required
   
   write_attrs = [
      "active",
      "description",
      "private",
      "archive",
      "file_quota",
      "default_gateway_caps",
      "allow_anon",
      "closure"
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
      volume_metadata.volume_public_key = kwargs.get( 'metadata_public_key', self.metadata_public_key )
      volume_metadata.num_files = kwargs.get( 'num_files', Volume.get_num_files( volume_metadata.volume_id ) )
      volume_metadata.archive = kwargs.get( 'archive', self.archive )
      volume_metadata.private = kwargs.get( 'private', self.private )
      volume_metadata.allow_anon = kwargs.get( 'allow_anon', self.allow_anon )
      
      if kwargs.get('closure', self.closure) is not None:
         volume_metadata.cache_closure_text = kwargs.get( 'closure', self.closure )
      
      # sign it
      volume_metadata.signature = ""

      data = volume_metadata.SerializeToString()
      sig = self.sign_message( data )

      volume_metadata.signature = sig

      return
      
   
   def protobuf_gateway_cert( self, gateway_cert, gateway, sign=True, need_closure=True ):
      """
      Given an ms_gateway_cert protobuf and a gateway record, have the gateway populate the 
      cert protobuf and then have the Volume optionally sign it with its private key.
      """
      
      gateway.protobuf_cert( gateway_cert, need_closure=need_closure )
      
      gateway_cert.signature = ""
      
      if sign:
         # sign the cert
         data = gateway_cert.SerializeToString()
         sig = self.sign_message( data )
         
         gateway_cert.signature = sig
      
      return 
   
   
   def protobuf_gateway_cert_manifest( self, manifest, sign=True ):
      """
      Generate a specially-crafted manifest protobuf, which a gateway can use to learn 
      the IDs and types of all gateways in the Volume, as well as their certs' versions.
      """
      
      manifest.volume_id = self.volume_id
      manifest.coordinator_id = 0
      manifest.file_id = 0
      manifest.owner_id = 0
      manifest.file_version = self.cert_version
      manifest.mtime_sec = 0
      manifest.mtime_nsec = 0
      
      sz = 0
      
      # query certificate versions, types, and caps of all gateways          
      listing = Gateway.ListAll( {"Gateway.volume_id ==" : self.volume_id}, projection=["g_id", "gateway_type", "cert_version", "caps"] )
      
      for gateway_metadata in listing:
         cert_block = manifest.block_url_set.add()
      
         cert_block.gateway_id = gateway_metadata.g_id
         cert_block.start_id = gateway_metadata.gateway_type
         cert_block.end_id = gateway_metadata.caps
         cert_block.block_versions.append( gateway_metadata.cert_version )
      
         logging.info("cert block: (%s, %s, %s, %x)" % (gateway_metadata.gateway_type, gateway_metadata.g_id, gateway_metadata.cert_version, gateway_metadata.caps) )
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
      
      if self.allow_anon:
         return True

      return gateway.volume_id == self.volume_id


   def sign_message( self, data ):
      """
      Return the base64-encoded crypto signature of the data,
      signed with our metadata private key.
      """
      signature = Volume.auth_sign( self.metadata_private_key, data )
      if signature is None:
         raise Exception("Failed to sign data")
      
      sigb64 = base64.b64encode( signature )
      return sigb64
      

   @classmethod
   def Create( cls, user, **kwargs ):
      """
      Given volume data, store it.
      Update the corresponding SyndicateUser atomically along with creating the Volume
      so that the SyndicateUser owns the Volume.
      
      Arguments:
      user              -- SyndicateUser instance that will own this Volume
      
      Required keyword arguments:
      name              -- name of the Volume (str)
      blocksize         -- size of the Volume's blocks in bytes (int)
      description       -- description of the Volume (str)
      private           -- whether or not this Volume is visible to other users (bool)
      
      Optional keyword arguments:
      metadata_private_key       -- PEM-encoded RSA private key, 4096 bits (str)
      archive                    -- whether or not this Volume is populated only by Acquisition Gateways (bool)
      default_gateway_caps      -- bitfield of capabilities Gateways created within this Volume should receive
      """
      
      # sanity check 
      if not user:
         raise Exception( "No user given" )
      
      kwargs['owner_id'] = 0     # will look up user and fill with owner ID once we validate input.
      Volume.fill_defaults( kwargs )

      # extract public key from private key if needed
      Volume.extract_keys( 'metadata_public_key', 'metadata_private_key', kwargs, VOLUME_RSA_KEYSIZE )
            
      # Validate
      missing = Volume.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      
      invalid = Volume.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # vet the keys
      for key_field in ['metadata_public_key', 'metadata_private_key']:
         key_str = kwargs[key_field]
         valid = cls.is_valid_key( key_str, VOLUME_RSA_KEYSIZE )
         if not valid:
            raise Exception("Key must be a %s-bit RSA key" % (VOLUME_RSA_KEYSIZE) )
      
      # attempt to create the Volume
      volume_id = random.randint( 1, 2**63 - 1 )
      
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
                                                active=kwargs.get('active',False),
                                                version=1,
                                                cert_version=1,
                                                private=kwargs['private'],
                                                archive=kwargs['archive'],
                                                allow_anon = kwargs['allow_anon'],
                                                metadata_public_key = kwargs['metadata_public_key'],
                                                metadata_private_key = kwargs['metadata_private_key'],
                                                default_gateway_caps = kwargs['default_gateway_caps']
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
      req = VolumeAccessRequest.create_async( user.owner_id, volume_id, kwargs['name'], random.randint(-2**63, 2**63 - 1), VolumeAccessRequest.STATUS_GRANTED,
                                              gateway_caps=kwargs['default_gateway_caps'], allowed_gateways=(1 << GATEWAY_TYPE_AG)|(1 << GATEWAY_TYPE_UG)|(1 << GATEWAY_TYPE_RG), request_message="Created").get_result()
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
      vol = Volume.ListAll( {"Volume.name ==": volume_name}, async=async )
      
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
   def Update( cls, volume_name_or_id, **fields ):
      '''
      Atomically (transactionally) update a given Volume with the given fields.
      
      Arguments:
      volume_id         -- ID of the Volume to update.
      
      Keyword arguments: same as Create()
      '''
      try:
         volume_id = int(volume_name_or_id)
      except:
         volume = Volume.Read_ByName( volume_name_or_id )
         if volume:
            volume_id = volume.volume_id
         else:
            raise Exception("No such Volume '%s'" % volume_name_or_id )
      
      invalid = Volume.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # make sure we're only writing correct fields
      invalid = Volume.validate_write( fields )
      if len(invalid) != 0:
         raise Exception( "Unwritable fields: %s" % (", ".join( invalid )) )
      
      if fields.has_key("metadata_private_key"):
         # extract the public key
         try:
            metadata_private_key = CryptoKey.importKey( kwargs['metadata_private_key'] )
            kwargs['metadata_public_key'] = metadata_private_key.publickey().exportKey()
         except:
            raise Exception("Invalid metadata private key: could not load")
         
         if not Volume.is_valid_key( metadata_private_key ):
            raise Exception("Invalid metadata private key: not sufficiently secure")
      
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
         if not volume or volume.deleted:
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
         
         # make sure Read_ByName uses the right name
         vol_name_to_id_cached = "Read_ByName: volume_name=%s" % old_name
         storagetypes.memcache.delete( vol_name_to_id_cached )
         
      return volume_key
         

   @classmethod
   def delete_volume_and_friends( cls, volume_id, volume_name ):
      access_keys_fut = VolumeAccessRequest.ListVolumeAccessRequests( volume_id, async=True, keys_only=True )
      
      storagetypes.wait_futures( [access_keys_fut] )
      
      access_keys = access_keys_fut.get_result()
      
      futs = []
      
      if access_keys:
         for key in access_keys:
            futs.append( key.delete_async() )
      
      volume_key = storagetypes.make_key( Volume, Volume.make_key_name( volume_id=volume_id ) )
      futs.append( volume_key.delete_async() )
      
      volume_nameholder_key = storagetypes.make_key( VolumeNameHolder, VolumeNameHolder.make_key_name( volume_name ) )
      futs.append( volume_nameholder_key.delete_async() )
      
      storagetypes.wait_futures( futs )
      
      # TODO: reap gateways
      

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
      
      
      storagetypes.deferred.defer( Volume.delete_volume_and_friends, volume.volume_id, volume.name )
      
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

