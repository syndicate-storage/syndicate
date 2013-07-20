"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""



import random

import types
import errno
import time
import datetime

import backends as backend

SHARD_KEY_TEMPLATE = 'shard-{}-{:d}'

# aliases for types
Model = backend.Model
Integer = backend.Integer
String = backend.String
Text = backend.Text
Key = backend.Key
Boolean = backend.Boolean
Json = backend.Json

# aliases for keys
make_key = backend.make_key

# aliases for asynchronous operations
wait_futures = backend.wait_futures
deferred = backend.deferred
concurrent = backend.concurrent
concurrent_return = backend.concurrent_return

get_multi_async = backend.get_multi_async

# synchronous operations
get_multi = backend.get_multi
delete_multi = backend.delete_multi

# aliases for memcache
memcache = backend.memcache

# aliases for transaction
transaction = backend.transaction
transactional = backend.transactional


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

   # list of names of attributes that can be read
   read_attrs = []

   # list of names of attributes that can be written 
   write_attrs = []

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

   @classmethod
   def shard_key_name( cls, name, idx ):
      """
      Generate the name for a shard, given its base name and index
      """
      return SHARD_KEY_TEMPLATE.format( name, idx )

   @classmethod
   def get_shard_keys(cls, num_shards, **attrs ):
      """
      Get keys for all shards, given the number of shards.
      The base name will be generated from the make_key_name() method, to which the given **attrs dict will be passed.
      """
      name = cls.make_key_name( **attrs )
      shard_key_strings = [cls.shard_key_name( name, index ) for index in range(num_shards)]
      return [make_key(cls.shard_class, shard_key_string) for shard_key_string in shard_key_strings]


   def populate_from_shards(self, shards):
      """
      Populate the base object using a list of shards.
      This will use the methods to fill the fields indicated by the base instance's shard_readers dict.
      This method throws an exception when passed a list of Nones
      """
      good = False
      for s in shards:
         if s != None:
            good = True
            break

      if not good:
         raise Exception("No valid shards for %s" % self)
      
      # populate an instance with value from shards
      for (shard_field, shard_reader) in self.shard_readers.items():
         setattr( self, shard_field, shard_reader( self, shards ) )

      
      
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
         shard_name = self.shard_key_name( self.make_key_name( **attrs ), random.randint(0, num_shards-1) )
         shard_key = make_key( self.shard_class, shard_name )

         self.write_shard = self.shard_class( key=shard_key, **shard_attrs )

      else:
         self.write_shard.populate( **shard_attrs )
         

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
   def cache_key( cls, **attrs ):
      return "cache: " + cls.make_key_name( **attrs )

   @classmethod
   def cache_shard_key( cls, **attrs ):
      return "shard: " + cls.make_key_name( **attrs )

   @classmethod
   def cache_listing_key( cls, **attrs ):
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
   def validate_read( cls, attrlist ):
      """
      Generate a list of attributes that CANNOT be read, given an iterable of attributes.
      """
      not_readable = []
      for attr in attrlist:
         if attr not in cls.read_attrs:
            not_readable.append( attr )

      return not_readable

   @classmethod
   def validate_write( cls, attrlist ):
      """
      Generate a list of attributes that CANNOT be written, given an iterable attributes.
      """
      not_writable = []
      for attr in attrlist:
         if attr not in cls.write_attrs:
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
         if attrdict.get( attr ) == None:
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
   def validate_fields( cls, attrdict ):
      """
      Apply the associated validators to this dictionary of attributes.
      Return the list of attribute keys to INVALID values
      """
      invalid = []
      for (attr, value) in attrdict.items():
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
   def ListAll( cls, filter_attrs ):
      raise NotImplementedError

   @classmethod
   def ListAll_buildQuery( cls, qry, filter_attrs ):
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

         if op == '==':
            qry.filter( cls._properties[attr] == value )
         elif op == "!=":
            qry.filter( cls._properties[attr] != value )
         elif op == ">":
            qry.filter( cls._properties[attr] > value )
         elif op == ">=":
            qry.filter( cls._properties[attr] >= value )
         elif op == "<":
            qry.filter( cls._properties[attr] < value )
         elif op == "<=":
            qry.filter( cls._properties[attr] <= value )
         elif op == "IN":
            qry.filter( cls._properties[attr].IN( value ) )
         
      qry_ret = qry.fetch( None )
      return qry_ret
      
