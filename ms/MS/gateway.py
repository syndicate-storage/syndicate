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
#from Crypto.Signature import PKCS1_v1_5 as CryptoSigner
from Crypto.Signature import PKCS1_PSS as CryptoSigner

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
   
   GATEWAY_TYPE = 0

   owner_id = storagetypes.Integer(default=-1)         # ID of the SyndicateUser that owns this gateway
   host = storagetypes.String()
   port = storagetypes.Integer()
   ms_username = storagetypes.String()          # name of this gateway
   ms_password_hash = storagetypes.Text()       # hash of the password needed to edit this gateway on the MS
   ms_password_salt = storagetypes.Text()
   g_id = storagetypes.Integer()
   volume_id = storagetypes.Integer(default=-1)
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
   
   read_attrs = [
      "owner_id",
      "host",
      "port",
      "ms_username",
      "g_id",
      "volume_id",
      "caps",
      "file_quota",
      "public_key",
      "config"
   ]
   
   write_attrs = [
      "owner_id",
      "host",
      "port",
      "file_quota",
      "public_key",
      "config",
      "caps"
   ]
   
   # TODO: session expires in 3600 seconds
   # TODO: cert expires in 86400 seconds
   default_values = {
      "session_expires": (lambda cls, attrs: -1),
      "session_password": (lambda cls, attrs: Gateway.generate_session_password()),
      "cert_version": (lambda cls, attrs: 1),
      "cert_expires": (lambda cls, attrs: -1)
   }

   key_attrs = [
      "g_id"
   ]
   
   @classmethod
   def is_valid_key( cls, key_str ):
      '''
      Validate a given PEM-encoded RSA key, both in formatting and security.
      '''
      try:
         key = RSA.importKey( key_str )
      except Exception, e:
         logging.error("RSA.importKey %s", traceback.format_exc() )
         return False

      # must have desired security level 
      if key.size() != GATEWAY_RSA_KEYSIZE - 1:
         logging.error("invalid key size = %s" % key.size() )
         return False

      return True
   

   validators = {
      "ms_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "session_password_hash": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "0123456789abcdef")) ) == 0),
      "ms_username": (lambda cls, value: len( unicode(value).translate(dict((ord(char), None) for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.: ")) ) == 0 and not is_int(value) ),
      "caps": (lambda cls, value: False),               # can't set this directly
      "public_key": (lambda cls, value: Gateway.is_valid_key( value ) )
   }

   # do we have caps to set?  What are they?
   @classmethod
   def has_caps( cls, attrs ):
      return False
   
   @classmethod 
   def get_caps( cls, attrs ):
      '''
      Get this gateway's capability bits
      '''
      return 0
   
   @classmethod
   def validate_caps( cls, gateway_type, caps ):
      '''
      Validate a given capability bitstring, given the type of Gateway this is.
      '''
      if gateway_type == GATEWAY_TYPE_RG:
         return caps == 0
      
      if gateway_type == GATEWAY_TYPE_AG:
         return (caps & ~(GATEWAY_CAP_WRITE_METADATA)) == 0
      
      return True


   def authenticate( self, password ):
      """
      Verify that the ms_password is correct (i.e. when editing a gateway)
      """
      h = SHA256.new()
      h.update( self.ms_password_salt )
      h.update( password )
      pw_hash = h.hexdigest()

      return pw_hash == self.ms_password_hash


   @classmethod 
   def Authenticate( cls, gateway_name_or_id, password ):
      """
      Load a gateway and then authenticate it against the password.
      Return the Gateway instance on success; False if found but wrong password; None if not found
      """
      
      # id or name?
      gateway_id = None
      gateway_name = None
      
      try:
         gateway_id = int( gateway_name_or_id )
      except:
         gateway_name = gateway_name_or_id
         pass
      
      g = None 
      if gateway_id != None:
         g = cls.Read( gateway_id )
      
      else:
         g = cls.Read_ByName( gateway_name )
      
      if g == None:
         # not found
         return None
      
      if g.authenticate( password ):
         # success!
         return g 
      else:
         # failure
         return False


   def authenticate_session( self, password ):
      """
      Verify that the session password is correct
      """
      return self.session_password == password

      
   def verify_signature( self, data, sig ):
      '''
      Verify that a message (as a byte string) was signed by this gateway
      '''
      key = RSA.importKey( self.public_key )
      h = SHA256.new( data )
      verifier = CryptoSigner.new(key)
      ret = verifier.verify( h, sig )
      if not ret:
         logging.error("Verification failed")
      
      return ret

      
   @classmethod
   def generate_password_hash( cls, pw, salt ):
      '''
      Given a password and salt, generate the hash to store.
      '''
      h = SHA256.new()
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


   def regenerate_session_password( self ):
      """
      Regenerate a session password
      """
      self.session_password = Gateway.generate_session_password()
      if self.session_timeout > 0:
         self.session_expires = now + self.session_timeout
      else:
         self.session_expires = -1

      return self.session_password
         

   @classmethod
   def generate_session_secrets( cls ):
      """
      Generate a password, password hash, and salt for this gateway
      """
      password = cls.generate_password( GATEWAY_PASSWORD_LENGTH )
      salt = "".join( [random.choice(string.printable) for i in xrange(GATEWAY_SALT_LENGTH)] )
      pw_hash = Gateway.generate_password_hash( password, salt )

      return ( password, pw_hash, salt )  


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
         
      if not Gateway.is_valid_key( pubkey_str_unencoded ):
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
         cert_pb.closure_text = str( self.config )
         
      cert_pb.signature = ""

      if self.public_key != None:
         cert_pb.public_key = self.public_key
      else:
         cert_pb.public_key = "NONE"


   @classmethod
   def setup_credentials( cls, attrs ):
      """
      Set up MS access credentials for this gateway
      """
      
      password, password_hash, password_salt = Gateway.generate_session_secrets()
      
      if attrs.get("ms_username") == None:
         # give a random gateway name
         attrs["ms_username"] = uuid.uuid4().urn

      if attrs.get("ms_password") != None:
         # use user-given password if provided
         attrs["ms_password_salt"] = password_salt
         attrs["ms_password_hash"] = Gateway.generate_password_hash( attrs.get("ms_password"), password_salt )
      
      if attrs.get("ms_password_hash") == None:
         attrs["ms_password"] = password
         attrs["ms_password_salt"] = password_salt
         attrs["ms_password_hash"] = password_hash
      

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

      ret = self.verify_signature( ms_update_str, sig_bin )

      ms_update.signature = sig

      return ret
   
   @classmethod
   def Create( cls, user, volume=None, **kwargs ):
      """
      Create a gateway
      """
      
      # do not set the Volume ID, unless volume was given (i.e. the caller allowed it).
      if volume != None:
         kwargs['volume_id'] = volume.volume_id
      elif kwargs.has_key('volume_id'):
         kwargs['volume_id'] = 0
      
      # enforce ownership--make sure the calling user owns this gateway
      kwargs['owner_id'] = user.owner_id

      # populate kwargs with default values for missing attrs
      cls.fill_defaults( kwargs )

      # sanity check: do we have everything we need?
      missing = cls.find_missing_attrs( kwargs )
      if len(missing) != 0:
         raise Exception( "Missing attributes: %s" % (", ".join( missing )))

      # populate kwargs with our credentials
      cls.setup_credentials( kwargs )
      
      # sanity check: are our fields valid?
      invalid = cls.validate_fields( kwargs )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      # do NOT remmeber ms_password
      if kwargs.has_key( 'ms_password' ):
         del kwargs['ms_password']
         
      #kwargs['ms_username'] = unicode(kwargs['ms_username']).strip().replace(" ","_")
      
      # set capabilities correctly
      if cls.has_caps( kwargs ):
         kwargs['caps'] = cls.get_caps( kwargs )
      else:
         kwargs['caps'] = 0
      
      # ID and key...
      g_id = random.randint( 0, 2**63 - 1 )
      kwargs['g_id'] = g_id
      
      g_key_name = cls.make_key_name( g_id=g_id )
      g_key = storagetypes.make_key( cls, g_key_name)
      
      # create a nameholder and this gateway at once---there's a good chance we'll succeed
      gateway_nameholder_fut = GatewayNameHolder.create_async( kwargs['ms_username'], g_id )
      gateway_fut = cls.get_or_insert_async( g_key_name, **kwargs )
      
      # wait for operations to complete
      storagetypes.wait_futures( [gateway_nameholder_fut, gateway_fut] )
      
      # check for collision...
      gateway_nameholder = gateway_nameholder_fut.get_result()
      gateway = gateway_fut.get_result()
      
      if gateway_nameholder.g_id != g_id:
         # name collision...
         storagetypes.deferred.defer( Gateway.delete_all, [g_key] )
         raise Exception( "Gateway '%s' already exists!" % kwargs['ms_username'] )
      
      if gateway.g_id != g_id:
         # ID collision...
         storagetypes.deferred.defer( Gateway.delete_all, [gateway_nameholder.key] )
         raise Exception( "Gateway ID collision.  Please try again." )
      
      # we're good!
      return g_key

   
   @classmethod
   def Read( cls, g_id, async=False, use_memcache=True ):
      """
      Given a Gateway ID, read its record.  Optionally cache it.
      """
      key_name = cls.make_key_name( g_id=g_id )

      g = None
      
      if use_memcache:
         g = storagetypes.memcache.get( key_name )
         
      if g == None:
         #logging.info("UG with ID of {} not found in cache".format(g_id))
         g_key = storagetypes.make_key( cls, cls.make_key_name( g_id=g_id ) )
         
         if async:
            g_fut = g_key.get_async( use_memcache=False )
            return g_fut
         
         else:
            g = g_key.get( use_memcache=False )
            
         if not g:
            logging.error("Gateway not found at all!")
            
         if use_memcache:
            storagetypes.memcache.set( key_name, g )
            
      return g


   @classmethod
   def Read_ByName( cls, gateway_name, async=False, use_memcache=True ):
      """
      Given a gateway name, look it up and optionally cache it.
      """
      
      g_name_to_id_cache_key = None 
      
      if use_memcache:
         g_name_to_id_cache_key = "Read_ByName: Gateway: %s" % gateway_name
         g_id = storagetypes.memcache.get( g_name_to_id_cache_key )
         
         if g_id != None and isinstance( g_id, int ):
            return cls.Read( g_id, async=async, use_memcache=use_memcache )
         
      
      # no dice
      if async:
         g_fut = cls.ListAll( {"%s.ms_username ==" % cls.__name__: gateway_name}, async=async )
         return g_fut
      
      else:
         g = cls.ListAll( {"%s.ms_username ==" % cls.__name__: gateway_name}, async=async )
         
         if len(g) > 1:
            raise Exception( "More than one %s named '%s'" % (cls.__name__, gateway_name) )
         
         if g: 
            g = g[0]
         else:
            g = None
         
         if use_memcache:
            if g:
               to_set = {
                  g_name_to_id_cache_key: g.g_id,
                  cls.make_key_name( g_id=g_id ): g
               }
               
               storagetypes.memcache.set_multi( to_set )
            
         return g

   @classmethod
   def FlushCache( cls, g_id ):
      """
      Purge cached copies of this gateway
      """
      gateway_key_name = cls.make_key_name( g_id=g_id )
      storagetypes.memcache.delete(gateway_key_name)

   
   @classmethod
   def Update( cls, g_id, **fields ):
      '''
      Update a gateway identified by ID with fields specified as keyword arguments.
      '''
      
      # validate...
      invalid = cls.validate_fields( fields )
      if len(invalid) != 0:
         raise Exception( "Invalid values for fields: %s" % (", ".join( invalid )) )

      invalid = cls.validate_write( fields )
      if len(invalid) != 0:
         raise Exception( "Unwritable fields: %s" % (", ".join(invalid)) )
      
      # gateway-specific capability processing
      if cls.has_caps( fields ):
         fields['caps'] = cls.get_caps( fields )
      
      rename = False
      gateway_nameholder_new_key = None
      old_name = None
      
      # do we intend to rename?  If so, reserve the name
      if "ms_username" in fields.keys():
         gateway_nameholder_new_fut = GatewayNameHolder.create_async( fields.get("ms_username"), g_id )
         
         gateway_nameholder_new = gateway_nameholder_new_fut.get_result()
         gateway_nameholder_new_key = gateway_nameholder_new.key
         
         if gateway_nameholder_new.g_id != g_id:
            # name collision
            raise Exception("Gateway '%s' already exists!" % (fields.get("ms_username")) )
         
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

         
         old_name = gateway.ms_username 
         
         # purge from cache
         Gateway.FlushCache( g_id )
         
         old_version = gateway.cert_version
         
         # apply update
         for (k,v) in fields.items():
            setattr( gateway, k, v )
         
         if "cert_version" in fields.keys():
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
         
      return gateway_key
   
      
   @classmethod
   def Delete( cls, g_id ):
      """
      Given a gateway ID, delete the corresponding gateway
      """
      key_name = cls.make_key_name( g_id=g_id )

      g_key = storagetypes.make_key( cls, key_name )

      # find the nameholder and delete it too
      gateway = g_key.get()
      if gateway == None:
         return True
      
      g_name_key = storagetypes.make_key( GatewayNameHolder, GatewayNameHolder.make_key_name( gateway.ms_username ) )
      
      g_delete_fut = g_key.delete_async()
      g_name_delete_fut = g_name_key.delete_async()
            
      storagetypes.memcache.delete(key_name)

      storagetypes.wait_futures( [g_delete_fut, g_name_delete_fut] )
      
      return True

   
   """
   @classmethod
   def ListAll_ByVolume( cls, volume_id, async=False, projection=None ):
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
   """
   
   def is_bound_to_volume( self ):
      '''
      Return True if this gateway is bound to a Volume
      '''
      return self.volume_id > 0
   

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
   
   private = storagetypes.Boolean()

   default_values = dict( Gateway.default_values.items() + {
      "private": (lambda cls, attrs: False) # Default is public
   }.items() )
   

GATEWAY_TYPE_TO_CLS = {
   GATEWAY_TYPE_UG: UserGateway,
   GATEWAY_TYPE_RG: ReplicaGateway,
   GATEWAY_TYPE_AG: AcquisitionGateway
}
