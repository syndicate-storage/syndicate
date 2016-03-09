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

import storage.storagetypes as storagetypes

import os
import base64
import urllib
import uuid
import json
from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import types
import errno
import time
import datetime
import random
import logging
import string
import binascii
import traceback

from common.msconfig import *

def is_int( x ):
   try:
      y = int(x)
      return True
   except:
      return False
   
   
class GatewayNameHolder( storagetypes.Object ):
   '''
   Mark a Gateway's name as in use
   '''
   
   name = storagetypes.String()
   g_id = storagetypes.Integer()
   
   required_attrs = [
      "name"
   ]
   
   
   @classmethod
   def make_key_name( cls, name ):
      return "GatewayNameHolder: name=%s" % (name)
   
   @classmethod
   def create_async( cls,  _name, _id ):
      return GatewayNameHolder.get_or_insert_async( GatewayNameHolder.make_key_name( _name ), name=_name, g_id=_id )
      

class GatewayDriver( storagetypes.Object):
   """
   Gateway driver, addressed by hash.
   """
   
   driver_hash = storagetypes.String()  # hex string
   driver_text = storagetypes.Blob()
   refcount = storagetypes.Integer()
   
   @classmethod 
   def hash_driver( cls, driver_text ):
      h = HashAlg.new() 
      h.update( driver_text )
      return h.hexdigest()
      
   @classmethod 
   def make_key_name( cls, driver_hash ):
      return "GatewayDriver: hash=%s" % (driver_hash)
   
   @classmethod 
   def create_or_ref( cls, _text ):
      """
      Create a new driver, or re-ref the existing one.
      Do so atomically.
      """
      driver_hash = cls.hash_driver( _text )
      
      def txn():
          
          dk = storagetypes.make_key( GatewayDriver, GatewayDriver.make_key_name( driver_hash ) )
          d = dk.get()
          f = None 
          
          if d is None:
              d = GatewayDriver( key=dk, driver_hash=driver_hash, driver_text=_text, refcount=1 )
              d.put()
          
          else:
              d.refcount += 1
              d.put()
              
          return d
      
      return storagetypes.transaction( txn )
  
  
   @classmethod 
   def ref( cls, driver_hash ):
      """
      Increment reference count.
      Do this in an "outer" transaction (i.e. Gateway.Update)
      """
      dk = storagetypes.make_key( GatewayDriver, cls.make_key_name( driver_hash ) )
      d = dk.get()
      
      if d is None:
         return False 
    
      d.refcount += 1
      d.put()
      return True
      
   
   @classmethod 
   def unref( cls, driver_hash ):
      """
      Unref a driver
      Delete it if its ref count goes non-positive.
      Do this in an "outer" transaction (i.e. Gateway.Delete, Gateway.Update)
      """
      dk = storagetypes.make_key( GatewayDriver, cls.make_key_name( driver_hash ) )
      d = dk.get()
      
      if d is None:
          return True 
      
      d.refcount -= 1
      if d.refcount <= 0:
          dk.delete()
      else:
          d.put()
          
      return True
  
  
   @classmethod 
   def unref_async( cls, driver_hash ):
      """
      Unref a driver, asynchronously
      Delete it if its ref count goes non-positive.
      Do this in an "outer" transaction (i.e. Gateway.Delete, Gateway.Update)
      """
      dk = storagetypes.make_key( GatewayDriver, cls.make_key_name( driver_hash ) )
      d = dk.get()
      
      if d is None:
          return True 
      
      d.ref -= 1
      if d.ref <= 0:
          d.delete_async()
      else:
          d.put_async()
          
      return True
   

class Gateway( storagetypes.Object ):

   # signed gateaway certificate from the user
   gateway_cert = storagetypes.Blob()                   # protobuf'ed gateway certificate generated and signed by the gateway owner upon creation
   
   # all of the below information is derived from the above signed gateway certificate.
   # it is NOT filled in by any method.
   gateway_type = storagetypes.Integer(default=0)

   owner_id = storagetypes.Integer(default=-1)         # ID of the SyndicateUser that owns this gateway
   host = storagetypes.String()
   port = storagetypes.Integer()
   name = storagetypes.String()          # name of this gateway
   g_id = storagetypes.Integer()
   volume_id = storagetypes.Integer(default=-1)
   deleted = storagetypes.Boolean(default=False)

   gateway_public_key = storagetypes.Text()             # PEM-encoded RSA public key to verify control-plane messages (metadata) sent from this gateway.
   
   caps = storagetypes.Integer(default=0)                # capabilities
   
   cert_expires = storagetypes.Integer(default=-1)       # -1 means "never expires"
   
   cert_version = storagetypes.Integer( default=1 )   # certificate-related version of this gateway
   
   driver_hash = storagetypes.String()                # driver hash for this gateway (addresses GatewayDriver).  hex string, not byte string
   
   need_cert = storagetypes.Boolean(default=False)      # whether or not other gateways in the volume need this gateway's certificate (i.e. will this gateway ever serve data)
   
   # for RPC
   key_type = "gateway"
   
   required_attrs = [
      "gateway_cert"
   ]
   
   read_attrs_api_required = [
      "driver_hash",
      "host",
      "port",
      "owner_id",
      "g_id",
      "gateway_type",
      "volume_id",
      "cert_version",
      "cert_expires",
      "caps",
   ]
   
   read_attrs = [
      "gateway_public_key",
      "name",
   ] + read_attrs_api_required
   
   
   # fields an API call can set
   write_attrs = [
      "gateway_cert"
   ]
   
   # attrs from the cert that are allowed to change between cert versions
   modifiable_cert_attrs = [
      "gateway_type",
      "host",
      "port",
      "caps",
      "cert_expires",
      "cert_version",
      "driver_hash",
      "gateway_public_key"
   ]
   
   write_attrs_api_required = write_attrs
   
   default_values = {
      "gateway_cert": ""
   }

   key_attrs = [
      "g_id"
   ]
   
   validators = {
      "name": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.: ")) ) == 0 \
                                  and not is_int(value) \
                                  and len(value) > 0 ),
      "gateway_public_key": (lambda cls, value: Gateway.is_valid_key( value, GATEWAY_RSA_KEYSIZE ) )
   }
   
   
   @classmethod 
   def needs_cert( cls, gateway_type, caps ):
      """
      Given a gateway's capabilities, will another gateway need its certificate?
      """
      if (caps & (GATEWAY_CAP_WRITE_METADATA | GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_COORDINATE)) != 0:
         return True 
      
      return False
   
   
   def owned_by( self, user ):
      return user.owner_id == self.owner_id
   

   def load_pubkey( self, pubkey_str, in_base64=True ):
      """
      Load a PEM-encoded RSA public key.
      if in_base64 == True, then try to base64-decode it first (i.e. the PEM-encoded
      public key is itself base64-encoded again)
      
      return 0 on success
      return -EINVAL if the key is invalid 
      return -EEXIST if the key is the same as the one we have in this Gateway
      """
      
      pubkey_str_unencoded = None 
      
      if in_base64:
         pubkey_str_unencoded = base64.b64decode( pubkey_str )
      else:
         pubkey_str_unencoded = pubkey_str
         
      if not Gateway.is_valid_key( pubkey_str_unencoded, GATEWAY_RSA_KEYSIZE ):
         return -errno.EINVAL

      new_public_key = CryptoKey.importKey( pubkey_str_unencoded ).exportKey()
      if self.gateway_public_key is not None and new_public_key == self.gateway_public_key:
         return -errno.EEXIST
      
      self.gateway_public_key = new_public_key 
      
      return 0
      
   
   def protobuf_cert( self, cert_pb ):
      """
      Populate an ms_gateway_cert structure from our cert
      """
      
      gateway_cert_pb = ms_pb2.ms_gateway_cert.ParseFromString( self.gateway_cert )
      cert_pb.CopyFrom( gateway_cert_pb )
         

   def check_caps( self, caps ):
      """
      Given a bitmask of caps, verify that all of them are met by our caps.
      """
      return (self.caps & caps) == caps


   def verify_message( self, msg ):
      """
      Verify the authenticity of a received message with a signature field (which should store a base64-encoded signature)
      """
      sig = msg.signature
      sig_bin = base64.b64decode( sig )

      msg.signature = ""
      msg_str = msg.SerializeToString()

      ret = self.auth_verify( self.gateway_public_key, msg_str, sig_bin )

      msg.signature = sig

      return ret
   
   
   def authenticate_session( self, g_type, g_id, url, signature_b64 ):
      """
      Verify that the signature over the constructed string "${g_type}_${g_id}:${url}"
      was signed by this gateway's private key.
      """
      sig = base64.b64decode( signature_b64 )
      
      data = "%s_%s:%s" % (g_type, g_id, url)
      
      ret = self.auth_verify( self.gateway_public_key, data, sig )
      
      return ret
   
   
   @classmethod 
   def cert_to_dict( cls, gateway_cert ):
      """
      Convert a protobuf structure to a dict of values,
      using the Gateway property names.
      """
      
      # unpack certificate
      cert_version = gateway_cert.version
      gateway_name = gateway_cert.name 
      gateway_type = gateway_cert.gateway_type 
      gateway_id = gateway_cert.gateway_id
      host = gateway_cert.host 
      port = gateway_cert.port 
      pubkey_pem = gateway_cert.public_key 
      cert_expires = gateway_cert.cert_expires 
      requested_caps = gateway_cert.caps 
      driver_hash = binascii.hexlify( gateway_cert.driver_hash )
      volume_id = gateway_cert.volume_id 
      owner_id = gateway_cert.owner_id
      
      kwargs = {
         "cert_version": cert_version,
         "name": gateway_name,
         "gateway_type": gateway_type,
         "host": host,
         "port": port,
         "gateway_public_key": pubkey_pem,
         "cert_expires": cert_expires,
         "caps": requested_caps,
         "driver_hash": driver_hash,
         "volume_id": volume_id,
         "owner_id": owner_id,
         "g_id": gateway_id,
         "gateway_cert": gateway_cert.SerializeToString()
      }
      
      return kwargs
   
   
   @classmethod
   def Create( cls, user, volume, gateway_cert, driver_text ):
      """
      Create a gateway, using its user-signed gateway certificate.
      
      NOTE: the caller must verify the authenticity of the certificate.
      """
      
      kwargs = cls.cert_to_dict( gateway_cert )
      
      # sanity check 
      if kwargs['volume_id'] != volume.volume_id:
         raise Exception("Volume ID mismatch: cert has %s; expected %s" % (kwargs['volume_id'], volume.volume_id))
      
      if kwargs['owner_id'] != user.owner_id:
         raise Exception("User ID mismatch: cert has %s; expected %s" % (kwargs['owner_id'], user.owner_id) ) 
      
      # sanity check: do we have everything we need?
      missing = cls.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      # sanity check: are our fields valid?
      invalid = cls.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # sanity check: does the driver match the driver's hash in the cert?
      if driver_text is not None:
         driver_hash = GatewayDriver.hash_driver( driver_text )
         if driver_hash != binascii.hexlify( gateway_cert.driver_hash ):
             raise Exception("Driver hash mismatch: len = %s, expected = %s, got = %s" % (len(driver_text), driver_hash, binascii.hexlify( cert.driver_hash )))
      
      gateway_type = kwargs['gateway_type']
      
      # enforce cert distribution 
      kwargs['need_cert'] = Gateway.needs_cert( gateway_type, kwargs['caps'] )

      g_id = kwargs['g_id']
      g_key_name = Gateway.make_key_name( g_id=g_id )
      g_key = storagetypes.make_key( cls, g_key_name )
      
      # create a nameholder and this gateway at once---there's a good chance we'll succeed
      futs = []
      
      gateway_nameholder_fut = GatewayNameHolder.create_async( kwargs['name'], g_id )
      gateway_fut = cls.get_or_insert_async( g_key_name, **kwargs )
      
      futs = [gateway_nameholder_fut, gateway_fut]
      
      gateway_driver = None
      if driver_text is not None:
          gateway_driver = GatewayDriver.create_or_ref( driver_text )
      
      # wait for operations to complete
      storagetypes.wait_futures( futs )
      
      # check for collision...
      gateway_nameholder = gateway_nameholder_fut.get_result()
      gateway = gateway_fut.get_result()
      
      to_rollback = []

      if gateway_driver is not None:
         to_rollback.append( gateway_driver.key )
      
      if gateway_nameholder.g_id != g_id:
         # name collision...
         to_rollback.append( g_key )
         storagetypes.deferred.defer( Gateway.delete_all, to_rollback )
         raise Exception( "Gateway '%s' already exists!" % kwargs['name'] )
      
      if gateway.g_id != g_id:
         # ID collision...
         to_rollback.append( gateway_nameholder.key )
         to_rollback.append( g_key )
         storagetypes.deferred.defer( Gateway.delete_all, to_rollback )
         raise Exception( "Gateway ID collision.  Please try again." )
      
      # we're good!
      return g_key

   
   @classmethod 
   @storagetypes.concurrent
   def Read_Async( cls, key, deleted=False ):
       gw = yield key.get_async()
       if gw is None:
           storagetypes.concurrent_return(None)

       if gw.deleted and not deleted:
            storagetypes.concurrent_return(None)

       storagetypes.concurrent_return(gw)


   @classmethod
   def Read( cls, g_name_or_id, async=False, use_memcache=True, deleted=False ):
      """
      Given a Gateway name or ID, read its record.  Optionally cache it.
      """
      
      # id or name?
      gateway_id = None
      gateway_name = None
      
      try:
         g_id = int( g_name_or_id )
      except:
         gateway_name = g_name_or_id 
         return cls.Read_ByName( gateway_name, async=async, use_memcache=use_memcache )
      
      key_name = Gateway.make_key_name( g_id=g_id )
      g = None
      
      if use_memcache:
         g = storagetypes.memcache.get( key_name )
         if g is not None and not deleted and g.deleted:
             storagetypes.memcache.delete( key_name )
             g = None
         
      if g is None:
         g_key = storagetypes.make_key( cls, Gateway.make_key_name( g_id=g_id ) )
         
         if async:
            g_fut = cls.Read_Async( g_key, deleted=deleted )
            return g_fut
         
         else:
            g = g_key.get( use_memcache=False )
            
         if g is None:
            logging.error("Gateway %s not found at all!" % g_id)
            
         if g.deleted:
            g = None

         elif use_memcache and g is not None:
            storagetypes.memcache.set( key_name, g )

      else:
         if g is not None and not deleted and g.deleted:
             storagetypes.memcache.delete( key_name )
             g = None 

         if async:
             if g is None or (not deleted and g.deleted):
                g = storagetypes.FutureWrapper( None )
             else:
                g = storagetypes.FutureWrapper( g )
         else:
             if g is not None and g.deleted:
                g = None

      return g


   @classmethod
   def Read_ByName_name_cache_key( cls, gateway_name ):
      g_name_to_id_cache_key = "Read_ByName: Gateway: %s" % gateway_name
      return g_name_to_id_cache_key
  

   @classmethod
   def Read_ByName( cls, gateway_name, async=False, use_memcache=True ):
      """
      Given a gateway name, look it up and optionally cache it.
      """
      
      g_name_to_id_cache_key = None 
      
      if use_memcache:
         g_name_to_id_cache_key = Gateway.Read_ByName_name_cache_key( gateway_name )
         g_id = storagetypes.memcache.get( g_name_to_id_cache_key )
         
         if g_id != None and isinstance( g_id, int ):
            return cls.Read( g_id, async=async, use_memcache=use_memcache )
         
      
      # no dice
      if async:
         g_fut = cls.ListAll( {"Gateway.name ==": gateway_name, "Gateway.deleted ==": False}, async=async )
         return storagetypes.FutureQueryWrapper( g_fut )
      
      else:
         g = cls.ListAll( {"Gateway.name ==": gateway_name, "Gateway.deleted ==": False}, async=async )
         
         if len(g) > 1:
            raise Exception( "More than one Gateway named '%s'" % (gateway_name) )
         
         if g: 
            g = g[0]
         else:
            g = None
         
         if use_memcache:
            if g:
               to_set = {
                  g_name_to_id_cache_key: g.g_id,
                  Gateway.make_key_name( g_id=g_id ): g
               }
               
               storagetypes.memcache.set_multi( to_set )
            
         return g


   @classmethod 
   def ReadDriver( cls, driver_hash ):
      """
      Given a driver's hash, return the driver.
      """
      driver_hash = driver_hash.lower()
      driver_key_name = GatewayDriver.make_key_name( driver_hash )
      
      driver = storagetypes.memcache.get( driver_key_name )
      if driver is not None:
         return driver 
      
      driver_key = storagetypes.make_key( GatewayDriver, driver_key_name )
      driver = driver_key.get()
      if driver is None:
          return None 

      driver_text = driver.driver_text
      if driver is not None:
         storagetypes.memcache.set( driver_key_name, driver_text )
      
      return driver_text
   

   @classmethod 
   def SetCache( cls, g_id, gateway ):
      """
      Cache a loaded gateway.
      """
      gateway_key_name = Gateway.make_key_name( g_id=g_id )
      storagetypes.memcache.set(gateway_key_name, gateway)
      

   @classmethod
   def FlushCache( cls, g_id ):
      """
      Purge cached copies of this gateway
      """
      gateway_key_name = Gateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)


   @classmethod
   def FlushCacheDriver( cls, driver_hash ):
      """
      Purge cached copies of this gateway's driver
      """
      driver_key_name = GatewayDriver.make_key_name( driver_hash )
      storagetypes.memcache.delete(driver_key_name)
  

   @classmethod
   def Update( cls, gateway_cert, new_driver=None ):
      '''
      Update a gateway identified by ID with a new certificate.
      Do not call this method directly.
      
      Return the gateway record's key on success
      Raise an exception on error.
      
      NOTE: the caller must verify the authenticity of the certificate.
      Only the volume owner should be able to update a gateway cert's capabilities.
      '''
      
      fields = cls.cert_to_dict( gateway_cert )
      g_id = fields['g_id']
      
      # validate...
      invalid = cls.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      new_driver_hash = None 
      old_driver_hash = None
      
      # sanity check...
      if new_driver is not None:
         new_driver_hash = GatewayDriver.hash_driver( new_driver )
         if binascii.hexlify( gateway_cert.driver_hash ) != new_driver_hash:
            raise Exception("Certificate driver hash mismatch: expected %s, got %s" % (binascii.hexlify( gateway_cert.driver_hash ), new_driver_hash))
      
      # drop cert; we'll store it separately 
      gateway_cert_bin = fields['gateway_cert']
      del fields['gateway_cert']
      
      def update_txn( fields ):
         '''
         Update the Gateway transactionally.
         '''
         
         g_id = fields['g_id']
         
         gateway = cls.Read(g_id)
         if gateway is None:
            # gateway does not exist...
            raise Exception("No Gateway with the ID %d exists.", g_id)
         
         old_driver_hash = gateway.driver_hash
         
         # verify update
         unwriteable = []
         for (k, v) in fields.items():
            if k not in cls.modifiable_cert_attrs and getattr(gateway, k) != v:
               unwriteable.append(k)
               
         if len(unwriteable) > 0:
            raise Exception("Tried to modify read-only fields: %s" % ",".join(unwriteable))
         
         # sanity check: valid version?
         if gateway.cert_version >= gateway_cert.version:
            raise Exception("Stale Gateway certificate: expected > %s; got %s" % (gateway.cert_version, gateway_cert.version))
         
         # apply update
         for (k,v) in fields.items():
            setattr( gateway, k, v )
         
         gateway.need_cert = cls.needs_cert( gateway.gateway_type, fields['caps'] )
         gateway.gateway_cert = gateway_cert_bin
         
         gw_key = gateway.put()
         
         if old_driver_hash is not None:
             # unref the old one 
             GatewayDriver.unref( old_driver_hash )
             cls.FlushCacheDriver( old_driver_hash )
         
         # purge from cache
         cls.FlushCache( g_id )
         
         return gw_key
      
      
      gateway_key = None
      try:
         gateway_key = storagetypes.transaction( lambda: update_txn( fields ), xg=True )
         assert gateway_key is not None, "Transaction failed"
      except Exception, e:
         logging.exception( e )
         raise e
      
      # update the driver as well 
      if new_driver is not None:
          GatewayDriver.create_or_ref( new_driver )
          
      return gateway_key
   
   
   @classmethod
   def Delete( cls, g_name_or_id ):
      """
      Given a gateway ID, delete the corresponding gateway.
      That is, set it's "deleted" flag so it no longer gets read.
      Unref the driver as well.
      """
      
      gateway = Gateway.Read( g_name_or_id )
      if gateway:
         g_id = gateway.g_id 
      else:
         raise Exception("No such Gateway '%s'" % g_name_or_id )
      
      key_name = Gateway.make_key_name( g_id=g_id )
      
      def set_deleted():
          # atomically set the gateway to deleted
          g_key = storagetypes.make_key( cls, key_name )
          gw = g_key.get()
          if gw is None:
              return None

          gw.deleted = True
          gw.put()
          return gw.key 

      storagetypes.transaction( lambda: set_deleted() )

      g_name_key = storagetypes.make_key( GatewayNameHolder, GatewayNameHolder.make_key_name( gateway.name ) )
      
      g_name_delete_fut = g_name_key.delete_async()
      driver_fut = GatewayDriver.unref_async( gateway.driver_hash )
      
      storagetypes.wait_futures( [g_name_delete_fut, driver_fut] )
      
      Gateway.FlushCache( g_id )
      Gateway.FlushCacheDriver( gateway.driver_hash )
      
      g_name_to_id_cache_key = Gateway.Read_ByName_name_cache_key( g_name_or_id )
      storagetypes.memcache.delete( g_name_to_id_cache_key )
      
      return True

   @classmethod 
   def DeleteAll( cls, volume ):
      """
      Given a Volume, delete all Gateways attached to it.
      It's best to run this as a deferred task.
      """
      
      def __delete_gw( gateway ):
         cls.Delete( gateway.g_id )
      
      cls.ListAll( {"Gateway.volume_id ==": volume.volume_id}, map_func=__delete_gw, projection=["g_id"] )
      return True 
   
   
