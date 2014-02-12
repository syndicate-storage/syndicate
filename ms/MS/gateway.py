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
import traceback

from common.msconfig import *

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
      
   

class Gateway( storagetypes.Object ):
   
   gateway_type = storagetypes.Integer(default=0)

   owner_id = storagetypes.Integer(default=-1)         # ID of the SyndicateUser that owns this gateway
   host = storagetypes.String()
   port = storagetypes.Integer()
   name = storagetypes.String()          # name of this gateway
   g_id = storagetypes.Integer()
   volume_id = storagetypes.Integer(default=-1)

   gateway_public_key = storagetypes.Text()             # PEM-encoded RSA public key to verify control-plane messages (metadata) sent from this gateway.
   encrypted_gateway_private_key = storagetypes.Text()  # optional: corresponding RSA private key, sealed with user's password.  Can only be set on creation.
   
   caps = storagetypes.Integer(default=0)                # capabilities
   
   session_password_hash = storagetypes.Text()
   session_password_salt = storagetypes.Text()
   session_timeout = storagetypes.Integer(default=-1, indexed=False)
   session_expires = storagetypes.Integer(default=-1)     # -1 means "never expires"
   
   cert_expires = storagetypes.Integer(default=-1)       # -1 means "never expires"
   
   cert_version = storagetypes.Integer( default=1 )   # certificate-related version of this gateway
   
   closure = storagetypes.Json()                # gateway-specific configuration
   
   gateway_blocksize = storagetypes.Integer( default=0 )        # (AG only) advertized blocksize
   
   
   # for RPC
   key_type = "gateway"
   
   required_attrs = [
      "owner_id",
      "host",
      "port",
      "name",
      "gateway_type",
      "caps"
   ]
   
   read_attrs_api_required = [
      "closure",
      "host",
      "port",
      "owner_id",
      "g_id",
      "volume_id",
      "session_timeout",
      "session_expires",
      "cert_version",
      "cert_expires",
      "gateway_blocksize",
      "caps",
      "encrypted_gateway_private_key"
   ]
   
   read_attrs = [
      "gateway_public_key",
      "name"
   ] + read_attrs_api_required
   
   
   write_attrs = [
      "host",
      "port",
      "closure",
      "cert_expires",
      "session_expires",
      "session_timeout",
      "gateway_public_key"
   ]
   
   write_attrs_api_required = write_attrs
   
   
   # TODO: session expires in 3600 seconds
   # TODO: cert expires in 86400 seconds
   default_values = {
      "session_expires": (lambda cls, attrs: -1),
      "cert_version": (lambda cls, attrs: 1),
      "cert_expires": (lambda cls, attrs: -1),
      "gateway_blocksize": (lambda cls, attrs: 61440 if attrs.get('gateway_type') == GATEWAY_TYPE_AG else 0),
      "caps": (lambda cls, attrs: 0),
      "encrypted_gateway_private_key": (lambda cls, attrs: None)
   }

   key_attrs = [
      "g_id"
   ]
   
   validators = {
      "session_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "name": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.: ")) ) == 0 and not is_int(value) ),
      "gateway_public_key": (lambda cls, value: Gateway.is_valid_key( value, GATEWAY_RSA_KEYSIZE ) )
   }
   
   @classmethod 
   def safe_caps( cls, gateway_type, given_caps ):
      '''
      Get this gateway's capability bits, while making sure that AGs and RGs 
      get hardwired capabilities.
      '''
      if gateway_type == GATEWAY_TYPE_AG:
         # caps are always write metadata
         return GATEWAY_CAP_WRITE_METADATA
      
      elif gateway_type == GATEWAY_TYPE_RG:
         # caps are always 0
         return 0
      
      return given_caps
      

   def owned_by( self, user ):
      return user.owner_id == self.owner_id

   def authenticate_session( self, password ):
      """
      Verify that the session password is correct
      """
      pw_hash = Gateway.generate_password_hash( password, self.session_password_salt )
      return pw_hash == self.session_password_hash

      
   @classmethod
   def generate_password_hash( cls, pw, salt ):
      '''
      Given a password and salt, generate the hash to store.
      '''
      h = HashAlg.new()
      h.update( salt )
      h.update( pw )

      pw_hash = h.hexdigest()

      return unicode(pw_hash)


   @classmethod
   def generate_password( cls, length ):
      '''
      Create a random password of a given length
      '''
      password = "".join( [random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for i in xrange(length)] )
      return password


   @classmethod
   def generate_session_password( cls ):
      '''
      Generate a session password
      '''
      return cls.generate_password( GATEWAY_SESSION_PASSWORD_LENGTH )


   @classmethod
   def generate_session_secrets( cls ):
      """
      Generate a password, password hash, and salt for this gateway
      """
      password = cls.generate_session_password()
      salt = cls.generate_password( GATEWAY_SESSION_SALT_LENGTH )
      pw_hash = Gateway.generate_password_hash( password, salt )

      return ( password, pw_hash, salt )  
   

   def regenerate_session_password( self ):
      """
      Regenerate a session password.  The caller should put() 
      the gateway after this call to save the hash and salt.
      """
      password, pw_hash, salt = Gateway.generate_session_secrets()
      if self.session_timeout > 0:
         self.session_expires = now + self.session_timeout
      else:
         self.session_expires = -1

      self.session_password_hash = pw_hash
      self.session_password_salt = salt
      return password


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
      Populate an ms_volume_gateway_cred structure
      """
      cert_pb.version = self.cert_version
      cert_pb.gateway_type = self.gateway_type
      cert_pb.owner_id = self.owner_id
      cert_pb.gateway_id = self.g_id
      cert_pb.name = self.name
      cert_pb.host = self.host
      cert_pb.port = self.port
      cert_pb.caps = self.caps
      cert_pb.cert_expires = self.cert_expires
      cert_pb.volume_id = self.volume_id
      cert_pb.blocksize = self.gateway_blocksize
      
      if self.closure == None:
         cert_pb.closure_text = ""
      else:
         cert_pb.closure_text = str( self.closure )
         
      cert_pb.signature = ""

      if self.gateway_public_key != None:
         cert_pb.public_key = self.gateway_public_key
      else:
         cert_pb.public_key = "NONE"
         

   def check_caps( self, caps ):
      """
      Given a bitmask of caps, verify that all of them are met by our caps.
      """
      return (self.caps & caps) == caps


   def verify_ms_update( self, ms_update ):
      """
      Verify the authenticity of a received ms_update message
      """
      sig = ms_update.signature
      sig_bin = base64.b64decode( sig )

      ms_update.signature = ""
      ms_update_str = ms_update.SerializeToString()

      ret = self.auth_verify( self.gateway_public_key, ms_update_str, sig_bin )

      ms_update.signature = sig

      return ret
   
   @classmethod
   def Create( cls, user, volume, **kwargs ):
      """
      Create a gateway.
      NOTE: careful--caps are required!  don't let users call this directly.
      """
      
      # enforce volume ID
      kwargs['volume_id'] = volume.volume_id
      
      # enforce ownership--make sure the calling user owns this gateway
      kwargs['owner_id'] = user.owner_id

      # populate kwargs with default values for missing attrs
      cls.fill_defaults( kwargs )
      
      # sanity check: do we have everything we need?
      missing = cls.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      # sanity check: are our fields valid?
      invalid = cls.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )
      
      # what kind of gateway are we?
      gateway_type = kwargs['gateway_type']
      
      # set capabilities correctly and safely
      kwargs['caps'] = cls.safe_caps( gateway_type, volume.default_gateway_caps )
      
      # ID...
      g_id = random.randint( 0, 2**63 - 1 )
      kwargs['g_id'] = g_id
      
      g_key_name = Gateway.make_key_name( g_id=g_id )
      g_key = storagetypes.make_key( cls, g_key_name )
      
      # create a nameholder and this gateway at once---there's a good chance we'll succeed
      gateway_nameholder_fut = GatewayNameHolder.create_async( kwargs['name'], g_id )
      gateway_fut = cls.get_or_insert_async( g_key_name, **kwargs )
      
      # wait for operations to complete
      storagetypes.wait_futures( [gateway_nameholder_fut, gateway_fut] )
      
      # check for collision...
      gateway_nameholder = gateway_nameholder_fut.get_result()
      gateway = gateway_fut.get_result()
      
      if gateway_nameholder.g_id != g_id:
         # name collision...
         storagetypes.deferred.defer( Gateway.delete_all, [g_key] )
         raise Exception( "Gateway '%s' already exists!" % kwargs['name'] )
      
      if gateway.g_id != g_id:
         # ID collision...
         storagetypes.deferred.defer( Gateway.delete_all, [gateway_nameholder.key, g_key] )
         raise Exception( "Gateway ID collision.  Please try again." )
      
      # we're good!
      return g_key

   
   @classmethod
   def Read( cls, g_name_or_id, async=False, use_memcache=True ):
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
         
      if g == None:
         g_key = storagetypes.make_key( cls, Gateway.make_key_name( g_id=g_id ) )
         
         if async:
            g_fut = g_key.get_async( use_memcache=False )
            return g_fut
         
         else:
            g = g_key.get( use_memcache=False )
            
         if not g:
            logging.error("Gateway %s not found at all!" % g_id)
            
         elif use_memcache:
            storagetypes.memcache.set( key_name, g )

      elif async:
         g = storagetypes.FutureWrapper( g )
         
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
         g_fut = cls.ListAll( {"Gateway.name ==": gateway_name}, async=async )
         return storagetypes.FutureQueryWrapper( g_fut )
      
      else:
         g = cls.ListAll( {"Gateway.name ==": gateway_name}, async=async )
         
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
   def FlushCache( cls, g_id ):
      """
      Purge cached copies of this gateway
      """
      gateway_key_name = Gateway.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

   
   @classmethod
   def Update( cls, g_name_or_id, **fields ):
      '''
      Update a gateway identified by ID with fields specified as keyword arguments.
      '''
      
      try:
         g_id = int(g_name_or_id)
      except:
         gateway = Gateway.Read( g_name_or_id )
         if gateway:
            g_id = gateway.g_id 
         else:
            raise Exception("No such Gateway '%s'" % g_name_or_id )
      
      if len(fields.keys()) == 0:
         return storagetypes.make_key( Gateway, storagetypes.make_key_name( g_id=g_id ) )
      
      # validate...
      invalid = cls.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      invalid = cls.validate_write( fields )
      if len(invalid) != 0:
         raise Exception( "Unwritable fields: %s" % (", ".join(invalid)) )
      
      rename = False
      gateway_nameholder_new_key = None
      old_name = None
      
      # do we intend to rename?  If so, reserve the name
      if "name" in fields.keys():
         gateway_nameholder_new_fut = GatewayNameHolder.create_async( fields.get("name"), g_id )
         
         gateway_nameholder_new = gateway_nameholder_new_fut.get_result()
         gateway_nameholder_new_key = gateway_nameholder_new.key
         
         if gateway_nameholder_new.g_id != g_id:
            # name collision
            raise Exception("Gateway '%s' already exists!" % (fields.get("name")) )
         
         else:
            # reserved!
            rename = True
      
      
      def update_txn( fields ):
         '''
         Update the Gateway transactionally.
         '''
         
         gateway = cls.Read(g_id)
         if not gateway:
            # gateway does not exist...
            # if we were to rename it, then delete the new nameholder
            if rename:
               storagetypes.deferred.defer( Gateway.delete_all, [gateway_nameholder_new_key] )
               
            raise Exception("No Gateway with the ID %d exists.", g_id)

         
         old_name = gateway.name 
         
         # purge from cache
         Gateway.FlushCache( g_id )
         
         old_version = gateway.cert_version
         
         # apply update
         for (k,v) in fields.items():
            setattr( gateway, k, v )
         
         gateway.cert_version = old_version + 1
         
         return gateway.put()
      
      
      gateway_key = None
      try:
         gateway_key = storagetypes.transaction( lambda: update_txn( fields ), xg=True )
      except Exception, e:
         logging.exception( e )
         raise e
      
      if rename:
         # delete the old placeholder
         gateway_nameholder_old_key = storagetypes.make_key( GatewayNameHolder, GatewayNameHolder.make_key_name( old_name ) )
         storagetypes.deferred.defer( Gateway.delete_all, [gateway_nameholder_old_key] )
         
         # make sure Read_ByName uses the right name
         g_name_to_id_cache_key = Gateway.Read_ByName_name_cache_key( old_name )
         storagetypes.memcache.delete( g_name_to_id_cache_key )
         
      return gateway_key
   
   
   @classmethod
   def SetCaps( cls, g_name_or_id, caps ):
      """
      Set this gateway's capabilities.
      """
      def set_caps_txn( g_name_or_id ):
         gateway = Gateway.Read( g_name_or_id )
         if gateway == None:
            raise Exception("No such Gateway '%s'" % caps)
         
         gateway.caps = Gateway.safe_caps( gateway.gateway_type, caps )
         gateway.put()
         return gateway
      
      gateway = storagetypes.transaction( lambda: set_caps_txn( g_name_or_id ) )
      
      if gateway is not None:
         Gateway.FlushCache( gateway.g_id )
         return True
      else:
         return False
         
   
   @classmethod
   def Delete( cls, g_name_or_id ):
      """
      Given a gateway ID, delete the corresponding gateway
      """
      
      try:
         g_id = int(g_name_or_id)
      except:
         gateway = Gateway.Read( g_name_or_id )
         if gateway:
            g_id = gateway.g_id 
         else:
            raise Exception("No such Gateway '%s'" % g_name_or_id )
         
      key_name = Gateway.make_key_name( g_id=g_id )

      g_key = storagetypes.make_key( cls, key_name )

      # find the nameholder and delete it too
      gateway = g_key.get()
      if gateway == None:
         return True
      
      g_name_key = storagetypes.make_key( GatewayNameHolder, GatewayNameHolder.make_key_name( gateway.name ) )
      
      g_delete_fut = g_key.delete_async()
      g_name_delete_fut = g_name_key.delete_async()
            
      Gateway.FlushCache( g_id )
      
      g_name_to_id_cache_key = Gateway.Read_ByName_name_cache_key( g_name_or_id )
      storagetypes.memcache.delete( g_name_to_id_cache_key )
      
      storagetypes.wait_futures( [g_delete_fut, g_name_delete_fut] )
      
      return True
