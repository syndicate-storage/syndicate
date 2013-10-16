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
import json
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

from msconfig import *

# TODO: sync up with ms_gateway_cert and libsyndicate?
GATEWAY_TYPE_UG = 1
GATEWAY_TYPE_AG = 2
GATEWAY_TYPE_RG = 3

GATEWAY_CAP_READ_DATA = 1;
GATEWAY_CAP_WRITE_DATA = 2;
GATEWAY_CAP_READ_METADATA = 4;
GATEWAY_CAP_WRITE_METADATA = 8;
GATEWAY_CAP_COORDINATE = 16;

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

class Gateway( storagetypes.Object ):
   
   GATEWAY_TYPE = 0

   owner_id = storagetypes.Integer()         # ID of the SyndicateUser that owns this gateway
   host = storagetypes.String()
   port = storagetypes.Integer()
   ms_username = storagetypes.String()
   ms_password_hash = storagetypes.Text()
   ms_password_salt = storagetypes.Text()
   g_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   caps = storagetypes.Integer(default=0)
   file_quota = storagetypes.Integer(default=-1,indexed=False)                # -1 means unlimited

   public_key = storagetypes.Text()          # PEM-encoded RSA public key, given by the SyndicateUser

   session_password = storagetypes.Text()
   session_timeout = storagetypes.Integer(default=-1, indexed=False)
   session_expires = storagetypes.Integer(default=-1)     # -1 means "never expires"
   
   cert_expires = storagetypes.Integer(default=-1)       # -1 means "never expires"
   
   cert_version = storagetypes.Integer( default=1 )   # certificate-related version of this gateway
   
   config = storagetypes.Json()                # gateway-specific configuration
   
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
      "ms_username": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.: ")) ) == 0 and not is_int(value) ),
      "caps": (lambda cls, value: False)                # can't set this directly
   }

   # TODO: session expires in 3600 seconds
   # TODO: cert expires in 86400 seconds
   default_values = {
      "session_expires": (lambda cls, attrs: -1),
      "session_password": (lambda cls, attrs: Gateway.generate_session_credentials()),
      "cert_version": (lambda cls, attrs: 1),
      "cert_expires": (lambda cls, attrs: -1)
   }

   key_attrs = [
      "g_id"
   ]
   
   # do we have caps to set?  What are they?
   @classmethod
   def has_caps( cls, attrs ):
      return False
   
   @classmethod 
   def get_caps( cls, attrs ):
      return 0
   
   @classmethod
   def validate_caps( gateway_type, caps ):
      if gateway_type == GATEWAY_TYPE_RG:
         return caps == 0
      
      if gateway_type == GATEWAY_TYPE_AG:
         return (caps & ~(GATEWAY_CAP_WRITE_METADATA)) == 0
      
      return True

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
      return cls.generate_password( GATEWAY_SESSION_PASSWORD_LENGTH )
      
      
   def get_current_session_credentials( self ):
      """
      Get the session credentials, returning None if they are too old
      """
      now = int( time.time() )
      if self.session_password == None or (now > self.session_expires and self.session_expires > 0):
         return None

      return self.session_password


   def regenerate_session_password( self ):
      """
      Regenerate a session password, given the Volume it's bound to.
      """
      self.session_password = Gateway.generate_session_credentials()
      if self.session_timeout > 0:
         self.session_expires = now + self.session_timeout
      else:
         self.session_expires = -1

      return self.session_password
         

   @classmethod
   def generate_credentials( cls ):
      """
      Generate (password, SHA256(password), salt) for this gateway
      """
      password = cls.generate_password( GATEWAY_PASSWORD_LENGTH )
      salt = "".join( [random.choice(string.printable) for i in xrange(GATEWAY_SALT_LENGTH)] )
      pw_hash = Gateway.generate_password_hash( password, salt )

      return ( password, pw_hash, salt )  


   def load_pubkey( self, pubkey_str ):
      # pubkey_str is base64 encoded
      pubkey_str_unencoded = base64.b64decode( pubkey_str )
      if not Gateway.is_valid_pubkey( pubkey_str_unencoded ):
         return -errno.EINVAL

      new_public_key = RSA.importKey( pubkey_str_unencoded ).exportKey()
      if new_public_key == self.public_key:
         return -errno.EEXIST
      
      self.public_key = new_public_key 
      
      return 0
      
   
   def protobuf_cert( self, cert_pb ):
      """
      Populate an ms_volume_gateway_cred structure
      """
      cert_pb.version = self.cert_version
      cert_pb.gateway_type = self.GATEWAY_TYPE
      cert_pb.owner_id = self.owner_id
      cert_pb.gateway_id = self.g_id
      cert_pb.name = self.ms_username
      cert_pb.host = self.host
      cert_pb.port = self.port
      cert_pb.caps = self.caps
      cert_pb.cert_expires = self.cert_expires
      cert_pb.volume_id = self.volume_id
      
      if self.config == None:
         cert_pb.closure_text = ""
      else:
         cert_pb.closure_text = self.config
         
      cert_pb.signature = ""

      if self.public_key != None:
         cert_pb.public_key = self.public_key
      else:
         cert_pb.public_key = "NONE"


   @classmethod
   def create_credentials( cls, user, kwargs ):

      password, password_hash, password_salt = Gateway.generate_credentials()
      
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
      

   def check_caps( self, caps ):
      return (self.caps & caps) == caps


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

   
   @classmethod
   def Create( cls, user, volume=None, **kwargs ):
      
      @storagetypes.transactional(xg=True)
      def transactional_create(**kwargs):
         g_id = IDCounter.next_value()
         kwargs['g_id'] = g_id

         key_name = cls.make_key_name( g_id=g_id )
         g_key = storagetypes.make_key( cls, key_name)
         gw = g_key.get()
         
         if 'ms_password' in kwargs:
            del kwargs['ms_password']
         gw = cls( key=g_key, **kwargs )

         return gw.put()


      if volume:
         kwargs['volume_id'] = volume.volume_id
      elif kwargs.has_key('volume_id'):
         kwargs['volume_id'] = 0
         
      kwargs['owner_id'] = user.owner_id

      cls.fill_defaults( kwargs )

      cls.create_credentials( user, kwargs )

      missing = cls.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      invalid = cls.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      kwargs['ms_username'] = unicode(kwargs['ms_username']).strip().replace(" ","_")
      
      if cls.has_caps( kwargs ):
         kwargs['caps'] = cls.get_caps( kwargs )
      else:
         kwargs['caps'] = 0
      
      gateway_type_str = cls.__name__
      existing_gateways = cls.ListAll( {"%s.ms_username ==" % gateway_type_str : kwargs['ms_username']} )
      if len(existing_gateways) > 0:
         # gateway already exists
         raise Exception( "Gateway '%s' already exists" % kwargs['ms_username'] )
      else:
         return transactional_create(**kwargs)

   
   @classmethod
   def Read( cls, g_id, set_memcache=True ):
      """
      Given a UG ID, find the UG record
      """
      key_name = cls.make_key_name( g_id=g_id )

      g = storagetypes.memcache.get( key_name )
      if g == None:
         #logging.info("UG with ID of {} not found in cache".format(g_id))
         g_key = storagetypes.make_key( cls, cls.make_key_name( g_id=g_id ) )
         g = g_key.get( use_memcache=False )
         if not g:
            logging.error("Gateway not found at all!")
            
         if set_memcache:
            storagetypes.memcache.set( key_name, g )
            
      return g

   @classmethod
   def FlushCache( cls, g_id ):
      gateway_key_name = cls.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

   
   @classmethod
   def Update( cls, g_id, **fields ):
      '''
      Update UG identified by ID with fields specified as a dictionary.
      '''
      @storagetypes.transactional(xg=True)
      def update_gateway( caller_cls, g_id, **fields):
         gateway = caller_cls.Read(g_id, set_memcache=False)
         caller_cls.FlushCache( g_id )
         
         for key, value in fields.iteritems():
            try:
               setattr(gateway, key, value)
            except:
               raise Exception("Update: Unable to set attribute: %s, %s." % (key, value))

         gateway.version += 1
         return gateway.put()
      
      invalid = cls.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      if cls.has_caps( fields ):
         fields['caps'] = cls.get_caps( fields )
      
      return update_gateway( cls, g_id, **fields )
   
      
   @classmethod
   def Delete( cls, g_id ):
      """
      Given a UG ID, delete it
      """
      key_name = cls.make_key_name( g_id=g_id )

      g_key = storagetypes.make_key( cls, key_name )

      g_key.delete()
      
      storagetypes.memcache.delete(key_name)

      return True
      
   @classmethod
   def ListAll_ByVolume( cls, volume_id, async=False, projection=None ):
      """
      Given a volume id, find all gateway records bound to it.  Cache the results
      """
      
      #results = storagetypes.memcache.get( cache_key_name )
      results = None
      if results == None:
         qry = cls.query().filter( cls.volume_id == volume_id )
         
         if not async:
            gateway_futs = qry.fetch( None, projection=projection )
         else:
            return qry.fetch_async( None, projection=projection )
         
         storagetypes.wait_futures( gateway_futs )
         
         results = []
         for fut in gateway_futs:
            gw = fut.get_result()
            if gw != None:
               results.append( gw )

      return results
   

class UserGateway( Gateway ):

   GATEWAY_TYPE = GATEWAY_TYPE_UG
   
   read_write = storagetypes.Boolean(default=False)
   
   required_attrs = Gateway.required_attrs + [
      "read_write"                      # will be converted into caps 
   ]

   default_values = dict( Gateway.default_values.items() + {
      "read_write": (lambda cls, attrs: False), # Default is only read
      "port": (lambda cls, attrs:32780)
   }.items() )
   
   
   @classmethod 
   def has_caps(cls, attrs):
      return attrs.has_key("read_write")
   
   @classmethod 
   def get_caps( cls, attrs ):
      return UserGateway.caps_from_readwrite( attrs.get("read_write") )

   @classmethod
   def caps_from_readwrite( cls, readwrite ):
      caps = GATEWAY_CAP_READ_DATA | GATEWAY_CAP_READ_METADATA
      if readwrite:
         caps |= GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA | GATEWAY_CAP_COORDINATE
      
      return caps

   
class AcquisitionGateway( Gateway ):

   GATEWAY_TYPE = GATEWAY_TYPE_AG
   
   # This is temporary; we should know what is really needed.   
   block_size = storagetypes.Integer( default=61140 )         # block size
   
   @classmethod
   def has_caps(cls, attrs):
      return True 
   
   @classmethod
   def get_caps( cls, attrs ):
      return GATEWAY_CAP_WRITE_METADATA

   def protobuf_cert( self, cert_pb ):
      super( AcquisitionGateway, self ).protobuf_cert( cert_pb )
      
      cert_pb.blocksize = self.block_size
   
      

class ReplicaGateway( Gateway ):

   GATEWAY_TYPE = GATEWAY_TYPE_RG
   
   validators = dict( Gateway.validators.items() + {
      "config": lambda cls, value: ReplicaGateway.is_valid_config( value )
   }.items() )
   
   # This is temporary; we should know what is really needed.
   private = storagetypes.Boolean()

   default_values = dict( Gateway.default_values.items() + {
      "private": (lambda cls, attrs: False) # Default is public
   }.items() )
   
   @classmethod
   def is_valid_config( cls, json_str ):
      '''
         Is this config valid for replica gateways?
      '''
      try:
         config_dict = json.loads( json_str )
      except:
         log.error("Invalid RG config %s" % json_str)
         return False
      
      # TODO: common code with replica_manager...
      top_keys = ['closure', 'drivers']
      
      driver_keys_all = ['name', 'code']
      driver_keys_required = ['name']
      
      if set(top_keys) != set(config_dict.keys()):
         return False
      
      for driver in config_dict['drivers']:
         if not set(driver).issuperset( set(driver_keys_required) ):
            log.error("Invalid RG config %s" % json_str)
            return False
         
         if not set(driver_keys_all).issuperset( set(driver) ):
            log.error("Invalid RG config %s" % json_str)
            return False
      
      return True


GATEWAY_TYPE_TO_CLS = {
   GATEWAY_TYPE_UG: UserGateway,
   GATEWAY_TYPE_RG: ReplicaGateway,
   GATEWAY_TYPE_AG: AcquisitionGateway
}
