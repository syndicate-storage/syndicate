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


import random

import types
import errno
import time
import datetime
import logging

import backends as backend

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import traceback

SHARD_KEY_TEMPLATE = 'shard-{}-{:d}'

# aliases for types
Model = backend.Model
Integer = backend.Integer
Float = backend.Float
String = backend.String
Text = backend.Text
Blob = backend.Blob
Key = backend.Key
Boolean = backend.Boolean
Json = backend.Json
Blob = backend.Blob
Cursor = backend.Cursor         # NOTE: need to have a constructor that takes urlsafe= as an argument for deserialization, and needs a urlsafe() method for serialization
Computed = backend.Computed
Pickled = backend.Pickled

# aliases for keys
make_key = backend.make_key

# aliases for asynchronous operations
FutureWrapper = backend.FutureWrapper
FutureQueryWrapper = backend.FutureQueryWrapper
wait_futures = backend.wait_futures
deferred = backend.deferred
concurrent = backend.concurrent
concurrent_return = backend.concurrent_return

get_multi_async = backend.get_multi_async
put_multi_async = backend.put_multi_async

# synchronous operations
get_multi = backend.get_multi
put_multi = backend.put_multi
delete_multi = backend.delete_multi

# aliases for memcache
memcache = backend.memcache

# aliases for transaction
transaction = backend.transaction
transaction_async = backend.transaction_async
transactional = backend.transactional

# alises for query predicates
opAND = backend.opAND
opOR = backend.opOR

# toplevel decorator
toplevel = backend.toplevel

# aliases for common exceptions 
RequestDeadlineExceededError = backend.RequestDeadlineExceededError
APIRequestDeadlineExceededError = backend.APIRequestDeadlineExceededError
URLRequestDeadlineExceededError = backend.URLRequestDeadlineExceededError
TransactionFailedError = backend.TransactionFailedError


def clock_gettime():
   now = datetime.datetime.utcnow()
   nowtt = now.timetuple()
   now_sec = int(time.mktime( nowtt ))
   now_nsec = int(now.microsecond * 1e3)
   return (now_sec, now_nsec)


def get_time():
   now_sec, now_nsec = clock_gettime()
   return float(now_sec) + float(now_nsec) / 1e9


class Object( Model ):

   # list of names of attributes of required attributes
   required_attrs = []

   # list of names of attributes that will be used to generate a primary key
   key_attrs = []

   # list of names of attributes that can be read, period
   read_attrs = []
   
   # list of names of attributes that can be read, but only with the object's API key
   read_attrs_api_required = []
   
   # list of names of attributes that can be read, but only by the administrator
   read_attrs_admin_required = []

   # list of names of attributes that can be written, period
   write_attrs = []
   
   # list of names of attributes that can be written, but only with the object's API key
   write_attrs_api_required = []
   
   # list of names of attributes that can be written, but only by the administrator
   write_attrs_admin_required = []

   # dict of functions that generate default values
   # attribute name => lambda object_class, attribute_dict => default_value
   default_values = {}

   # dict of functions that validate fields
   # attribute name => lambda object_class, attribute_value => true/false
   validators = {}

   # class of an Object that contains sharded data
   shard_class = None

   # fields in this Object stored on a shard.
   shard_fields = []

   # dict of functions that read sharded fields
   # sharded attribute name => lambda instance, shard_objects => attribute_value
   shard_readers = {}

   # dict of functions that write shard fields
   # sharded attribute name => lambda insance => attribute value
   shard_writers = {}

   # instance of a shard that will be populated and written
   write_shard = None

   # for RPC
   key_type = None

   @classmethod
   def shard_key_name( cls, name, idx ):
      """
      Generate the name for a shard, given its base name and index
      """
      return SHARD_KEY_TEMPLATE.format( name, idx )

   @classmethod
   def get_shard_key( cls, name, idx ):
      key_str = cls.shard_key_name( name, idx )
      return make_key( cls.shard_class, key_str )
      
   @classmethod
   def get_shard_keys(cls, num_shards, key_name ):
      """
      Get keys for all shards, given the number of shards.
      The base name will be generated from the make_key_name() method, to which the given **attrs dict will be passed.
      """
      shard_key_strings = [cls.shard_key_name( key_name, index ) for index in range(num_shards)]
      return [make_key(cls.shard_class, shard_key_string) for shard_key_string in shard_key_strings]


   def populate_from_shards(self, shards):
      """
      Populate the base object using a list of shards.
      This will use the methods to fill the fields indicated by the base instance's shard_readers dict.
      This method throws an exception when passed a list of Nones
      """
      if shards == None or len(shards) == 0:
         return
      
      shards_existing = filter( lambda x: x is not None, shards )
      if len(shards_existing) == 0:
         raise Exception("No valid shards for %s" % self)
      
      # populate an instance with value from shards
      for (shard_field, shard_reader) in self.shard_readers.items():
         val = shard_reader( self, shards_existing )
         setattr( self, shard_field, val )

      
      
   def populate_base(self, **attrs):
      """
      Populate the base instance of an object.
      Specifically, populate fields in the object that are NOT in the shard_fields list.
      """
      base_attrs = {}
      for (key, value) in attrs.items():
         if key not in self.shard_fields:
            base_attrs[key] = value
         
      super( Object, self ).populate( **base_attrs )
      
      for (key, value) in attrs.items():
         if key not in self._properties.keys():
            setattr( self, key, value )

   
   @classmethod
   def get_shard_attrs( cls, inst, **attrs ):
      """
      Generate and return a dict of shard attributes and values, given an **attrs dictionary.
      The resulting dictionary will contain a key,value pair for each shard field, indicated by the base object instance's shard_fields list.
      The key,value pairings will be taken first from **attrs.  If a key does not have a value, it will be populated from the base object
      instance's shard_writers dictionary.
      """
      shard_attrs = {}
      for (shard_field, shard_value) in attrs.items():
         if shard_field in cls.shard_fields:
            shard_attrs[shard_field] = shard_value

      for (shard_field, shard_writer) in cls.shard_writers.items():
         if shard_attrs.get( shard_field, None ) == None:
            shard_attrs[shard_field] = shard_writer( inst )

      return shard_attrs
      
      
   @classmethod
   def populate_shard_inst(cls, inst, shard_inst, **attrs):
      """
      Populate an instance of a shard, given an instance of the base object and an instance of its associated shard class,
      with the given set of attributes.  Required attributes (from the base object's shard_fields list) that are not present
      in **attrs will be generated using the indicated method in the base object's shard_writers dictionary.
      """
      shard_attrs = cls.get_shard_attrs( inst, **attrs )
      shard_inst.populate( -1, **shard_attrs )
      

   def populate_shard(self, num_shards, **attrs ):
      """
      Populate the base object instance's shard with the given attributes (**attrs), and store it under self.write_shard.
      If num_shards <= 0 or the base object does not identify a shard class type (in self.shard_class), this method does nothing.
      Missing attributes from **attrs are generated using the get_shard_attrs() method.
      """
      if self.shard_class == None:
         return

      if num_shards <= 0:
         return
         
      shard_attrs = self.get_shard_attrs( self, **attrs )
      
      if self.write_shard == None:
         key_kwargs = {}
         for k in self.key_attrs:
            key_kwargs[k] = attrs.get(k)
            
         shard_name = self.shard_key_name( self.make_key_name( **key_kwargs ), random.randint(0, num_shards-1) )
         shard_key = make_key( self.shard_class, shard_name )

         self.write_shard = self.shard_class( key=shard_key, **shard_attrs )

      else:
         self.write_shard.populate( num_shards, **shard_attrs )
         

   def populate(self, num_shards, **attrs):
      """
      Populate this object with the given attributes.
      A shard will be generated as well, picked at random from the integer range [0, num_shards], and stored under self.write_shard
      """
      self.populate_base( **attrs )
      self.populate_shard( num_shards, **attrs )

   
   def put_shard(self, **ctx_opts):
      """
      Save the base object instance's shard instance to the data store, with the given context options (**ctx_opts).
      If the object does not have a shard instance (self.write_shard), an exception will be raised.
      """
      if self.write_shard == None:
         raise Exception("No shard information given.  Call populate_shard() first!")

      k = self.write_shard.put( **ctx_opts )
      return k

      
   def put_shard_async(self, **ctx_opts):
      """
      Asynchronously save the base object instance's shard instance to the data store, with the given context options (**ctx_opts)
      If the object does not have a shard instance (self.write_shard), an exception will be raised.
      """
      if self.write_shard == None:
         raise Exception("No shard information given.  Call populate_shard() first!")

      kf = self.write_shard.put_async( **ctx_opts )
      return kf

   @classmethod
   def cache_key_name( cls, **attrs ):
      return "cache: " + cls.make_key_name( **attrs )

   @classmethod
   def cache_shard_key_name( cls, **attrs ):
      return "shard: " + cls.make_key_name( **attrs )

   @classmethod
   def cache_listing_key_name( cls, **attrs ):
      return "listing: " + cls.make_key_name( **attrs )

   @classmethod
   def base_attrs( cls, attrs ):
      """
      Generate a dictionary of attributes from the given attr dictionary,
      but only containing key,value pairs that do NOT belong to the object's shard
      (i.e. not listed in the shard_fields list).
      """
      ret = {}
      for (attr_key, attr_value) in attrs.items():
         if attr_key not in cls.shard_fields:
            ret[attr_key] = attr_value

      return ret

      
   @classmethod
   def merge_dict( cls, d1, d2 ):
      """
      Merge two dictionaries, producing a copy.
      """
      ret = d1.copy()
      ret.update( d2 )
      return ret

   
   @classmethod
   def find_missing_attrs( cls, attrdict ):
      """
      Generate a list of missing attributes, given the class's required_attrs list of required attributes.
      """
      not_found = []
      for attr in cls.required_attrs:
         if not attrdict.has_key( attr ):
            not_found.append( attr )

      return not_found


   @classmethod
   def get_required_attrs( cls, attrdict ):
      """
      Generate a dictionary of required attributes, given a dictionary whose keys may include values in the class's required_attrs list.
      """
      found = {}
      for (attr, value) in attrdict.items():
         if attr in cls.required_attrs:
            found[attr] = value

      return found

   @classmethod
   def validate_read( cls, attrlist, read_attrs=None ):
      """
      Generate a list of attributes that CANNOT be read, given an iterable of attributes.
      """
      if read_attrs == None:
         read_attrs = cls.read_attrs
         
      not_readable = []
      for attr in attrlist:
         if attr not in read_attrs:
            not_readable.append( attr )

      return not_readable

   @classmethod
   def validate_write( cls, attrlist, write_attrs=None ):
      """
      Generate a list of attributes that CANNOT be written, given an iterable attributes.
      """
      if write_attrs == None:
         write_attrs = cls.write_attrs 
         
      not_writable = []
      for attr in attrlist:
         if attr not in write_attrs:
            not_writable.append( attr )

      return not_writable
      
   @classmethod
   def make_key_name( cls, **kwargs ):
      """
      Generate a key name, using the key_attrs and associated values in the given kwargs dict.
      """
      missing = []
      key_parts = []
      for kattr in cls.key_attrs:
         if not kwargs.has_key( kattr ):
            missing.append( kattr )
         else:
            key_parts.append("%s=%s" % (kattr, kwargs[kattr]))

      if len(missing) > 0:
         raise Exception("Missing key attributes %s" % (",".join(missing)))
      else:
         return cls.__name__ + ":" + ",".join( key_parts )


   @classmethod
   def fill_defaults( cls, attrdict ):
      """
      Fill in default values into a dictionary, given the class's default_values attr <--> function dictionary.
      """
      for (attr, value_func) in cls.default_values.items():
         if not attrdict.has_key( attr ):
            value = value_func(cls, attrdict)
            attrdict[attr] = value

            
   @classmethod
   def get_default( cls, attr, attrdict ):
      """
      Get the default value for a specific attribute.
      """
      if cls.default_values.has_key( attr ):
         value = cls.default_values[attr]( cls, attrdict )
         return value

         
   @classmethod
   def validate_fields( cls, attrdict, skip=[] ):
      """
      Apply the associated validators to this dictionary of attributes.
      Return the list of attribute keys to INVALID values
      """
      invalid = []
      for (attr, value) in attrdict.items():
         if attr in skip:
            continue
         
         if cls.validators.has_key( attr ):
            valid = cls.validators[attr]( cls, value )
            if not valid:
               invalid.append( attr )


      return invalid

   @classmethod
   def Create( cls, **obj_attrs ):
      raise NotImplementedError

   @classmethod
   def Read( cls, **key_attrs ):
      raise NotImplementedError

   @classmethod
   def Update( cls, **update_attrs ):
      raise NotImplementedError

   @classmethod
   def Delete( cls, **key_attrs ):
      raise NotImplementedError

   @classmethod
   def ListAll( cls, filter_attrs, **q_opts ):
      order = q_opts.get("order", None)
      limit = q_opts.get("limit", None)
      offset = q_opts.get('offset', None)
      pagesize = q_opts.get("pagesize", None)
      start_cursor = q_opts.get("start_cursor", None)
      async = q_opts.get("async", False)
      projection = q_opts.get("projection", None)
      query_only = q_opts.get("query_only", False )
      keys_only = q_opts.get("keys_only", False )
      map_func = q_opts.get("map_func", None )
      
      qry = cls.query()
      ret = cls.ListAll_runQuery( qry, filter_attrs,
                                  order=order,
                                  limit=limit,
                                  offset=offset,
                                  pagesize=pagesize,
                                  async=async,
                                  start_cursor=start_cursor,
                                  projection=projection,
                                  query_only=query_only,
                                  keys_only=keys_only,
                                  map_func=map_func )
      return ret

   @classmethod
   def ListAll_runQuery( cls, qry, filter_attrs, order=None, limit=None, offset=None, pagesize=None, start_cursor=None, async=False, projection=None, query_only=False, keys_only=False, map_func=None ):
      if filter_attrs == None:
         filter_attrs = {}
         
      operators = ['==', '!=', '<', '>', '<=', '>=', 'IN' ]
      for (attr, value) in filter_attrs.items():
         attr_parts = attr.split()
         op = ''
         if len(attr_parts) > 1:
            # sanity check--must be a valid operator
            if attr_parts[1] not in operators:
               raise Exception("Invalid operator '%s'" % attr_parts[1])

            attr = attr_parts[0]
            op = attr_parts[1]
         else:
            # default: =
            op = "=="

         # get the field name
         if '.' in attr_parts[0]:
            attr_name = attr_parts[0].split(".")[-1]
         else:
            attr_name = attr_parts[0]
         
         if op == '==':
            qry = qry.filter( cls._properties[attr_name] == value )
         elif op == "!=":
            qry = qry.filter( cls._properties[attr_name] != value )
         elif op == ">":
            qry = qry.filter( cls._properties[attr_name] > value )
         elif op == ">=":
            qry = qry.filter( cls._properties[attr_name] >= value )
         elif op == "<":
            qry = qry.filter( cls._properties[attr_name] < value )
         elif op == "<=":
            qry = qry.filter( cls._properties[attr_name] <= value )
         elif op == "IN":
            qry = qry.filter( cls._properties[attr_name].IN( value ) )
         else:
            raise Exception("Invalid operation '%s'" % op)

      if order is not None:
         # apply ordering
         for attr_name in order:
            if "." in attr_name:
               attr_name = attr_name.split(".")[-1]
               
            reverse = False 
            
            if attr_name[0] == '-':
               attr_name = attr_name[1:]
               reverse = True
            
            if reverse:
               qry = qry.order( -cls._properties[attr_name] )
            else:
               qry = qry.order( cls._properties[attr_name] )
         
      proj_attrs = None
      if projection is not None:
         proj_attrs = []
         for proj_attr in projection:
            proj_attrs.append( cls._properties[proj_attr] )
      
      qry_ret = None
      
      if query_only:
         # query only, no data
         qry_ret = qry
      
      else:
         if map_func is None:
            if pagesize is not None:
               # paging response 
               if async:
                  qry_ret = qry.fetch_page_async( pagesize, keys_only=keys_only, limit=limit, offset=offset, start_cursor=start_cursor, projection=proj_attrs )
               else:
                  qry_ret = qry.fetch_page( pagesize, keys_only=keys_only, limit=limit, offset=offset, start_cursor=start_cursor, projection=proj_attrs )
            
            else:
               # direct fetch
               if async:
                  qry_ret = qry.fetch_async( limit, keys_only=keys_only, offset=offset, projection=proj_attrs )
               else:
                  qry_ret = qry.fetch( limit, keys_only=keys_only, offset=offset, projection=proj_attrs )
         else:
            if async:
               qry_ret = qry.map_async( map_func, keys_only=keys_only, offset=offset, projection=proj_attrs )
            else:
               qry_ret = qry.map( map_func, keys_only=keys_only, offset=offset, projection=proj_attrs )
            
      return qry_ret

   # deferred operation
   @classmethod
   def delete_all( cls, keys ):
      delete_multi( keys )
      
   
   @classmethod
   def is_valid_key( cls, key_str, keysize ):
      '''
      Validate a given PEM-encoded public key, both in formatting and security.
      '''
      try:
         key = CryptoKey.importKey( key_str )
      except Exception, e:
         logging.error("importKey %s" % traceback.format_exc() )
         return False

      # must have desired security level 
      if key.size() != keysize - 1:
         logging.error("invalid key size = %s" % key.size() )
         return False

      return True
  

   @classmethod 
   def is_public_key( cls, key_str ):
      """
      Is this a public key?
      """
      try:
          key = CryptoKey.importKey( key_str )
          return not key.has_private()
      except Exception, e:
          logging.error("importKey %s" % traceback.format_exc() )
          return False 
   
   @classmethod
   def auth_verify( cls, public_key_str, data, data_signature ):
      """
      Verify that a given data is signed by a private key that corresponds to a given public key (given as a PEM-encoded string).
      """
      try:
         key = CryptoKey.importKey( public_key_str )
      except Exception, e:
         logging.error("importKey %s" % traceback.format_exc() )
         return False
      
      h = HashAlg.new( data )
      verifier = CryptoSigner.new(key)
      ret = verifier.verify( h, data_signature )
      return ret
   
   
   @classmethod
   def auth_sign( cls, private_key_str, data ):
      """
      Sign data with a private key.  Return the signature
      """
      try:
         key = CryptoKey.importKey( private_key_str )
      except Exception, e:
         logging.error("importKey %s" % traceback.format_exc() )
         return None
      
      h = HashAlg.new( data )
      signer = CryptoSigner.new(key)
      signature = signer.sign( h )
      return signature
   
   
   @classmethod
   def generate_keys( cls, key_size ):
      """
      Generate public/private keys of a given size.
      """
      rng = Random.new().read
      key = CryptoKey.generate(key_size, rng)

      private_key_pem = key.exportKey()
      public_key_pem = key.publickey().exportKey()

      return (public_key_pem, private_key_pem)
   
   
   @classmethod
   def load_keys( cls, privkey_str ):
      """
      Load a public and private key pair from a private key string
      """
      try:
         key = CryptoKey.importKey( privkey_str )
      except Exception, e:
         logging.error("importKey %s", traceback.format_exc() )
         return None, None
      
      return key.publickey().exportKey(), key.exportKey()
      
      
   
   @classmethod
   def extract_keys( cls, pubkey_name, privkey_name, kwargs, keysize ):
      """
      Extract the public part of a private key, and put it into the given kwargs dict under pubkey_name.
      """
      
      if kwargs.has_key(privkey_name) and not kwargs.has_key(pubkey_name):
         # extract the public key
         if not cls.is_valid_key( kwargs[privkey_name], keysize ):
            raise Exception("Invalid private key")
         else:
            kwargs[pubkey_name], kwargs[privkey_name] = cls.load_keys( kwargs[privkey_name] )
            if kwargs[pubkey_name] is None or kwargs[privkey_name] is None:
               raise Exception("Unable to load private keys")
            
   
   @classmethod
   def set_atomic( cls, read_func, **attrs ):
      """
      Set attributes atomically, in a transaction.
      Use read_func() to get the object.
      """
      def set_atomic_txn( **attrs ):
         obj = read_func()
         if not obj:
            raise Exception("No such object")
         
         for (attr_name, attr_value) in attrs:
            if hasattr( obj, attr_name ):
               setattr( obj, attr_name, attr_value )
            
         
         obj.put()
         return obj
      
      try:
         return transaction( set_atomic_txn, xg=True )
      except Exception, e:
         logging.exception( e )
         raise e
      
         
   @classmethod
   def get_public_read_attrs( cls ):
      return list( set(cls.read_attrs) - set(cls.read_attrs_api_required + cls.read_attrs_admin_required) )
   
   @classmethod
   def get_api_read_attrs( cls ):
      return list( set(cls.read_attrs) - set(cls.read_attrs_admin_required) )
   
   @classmethod
   def get_admin_read_attrs( cls ):
      return cls.read_attrs
   
   @classmethod
   def get_public_write_attrs( cls ):
      return list( set(cls.write_attrs) - set(cls.write_attrs_api_required + cls.write_attrs_admin_required) )
   
   @classmethod
   def get_api_write_attrs( cls ):
      return list( set(cls.write_attrs) - set(cls.write_attrs_admin_required) )
   
   @classmethod
   def get_admin_write_attrs( cls ):
      return cls.write_attrs
   
   @classmethod
   def ParseArgs( cls, *args, **kw ):
      # used by python clients (i.e. syntool), but not the MS.
      # it just needs to exist to keep the MS happy.
      pass
   
