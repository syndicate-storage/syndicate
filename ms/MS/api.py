#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

"""
Front-end API for command-line tools.
"""

import storage.storagetypes as storagetypes
import storage.storage as storage

import logging

import random
import os

import types
import errno
import time
import datetime
import collections

from msconfig import *


# list of reader API methods
READ_API = [
   storage.read_volume,
   storage.get_volume_by_name,
   storage.read_user,
   storage.read_user_gateway,
   storage.read_replica_gateway,
   storage.read_acquisition_gateway
]

# list of updater API methods
UPDATE_API = [
   storage.update_volume,
   storage.update_user,
   storage.update_user_gateway,
   storage.update_replica_gateway,
   storage.update_acquisition_gateway
]

# list of creator API methods
CREATE_API = [
   storage.create_volume,
   storage.create_user,
   storage.create_user_gateway,
   storage.create_replica_gateway,
   storage.create_acquisition_gateway
]

# list of deletor API methods
DELETE_API = [
   storage.delete_volume,
   storage.delete_user,
   storage.delete_user_gateway,
   storage.delete_replica_gateway,
   storage.delete_acquisition_gateway
]

# list of listor API methods
LIST_API = [
   storage.list_volume_users,
   storage.list_rw_volume_users,
   storage.list_ro_volume_users,
   storage.list_user_gateways_by_volume,
   storage.list_replica_gateways_by_volume,
   storage.list_acquisition_gateways_by_volume
]

PUBLIC_API = READ_API + UPDATE_API + CREATE_API + DELETE_API + LIST_API


def get_readable_attrs( obj ):
            
   ret = {}
   if len(obj.read_attrs) > 0:
      for attr in obj.read_attrs:
         ret[attr] = getattr( obj, attr )
   
   return ret


def json_attrs( func ):
   """
      Decorator to ensure that we return a dict of readable attributes for an object
   """
   def inner( *args, **kw ):
      
      result_raw = func( *args, **kw )
      
      result = None
      if func in READ_API and result_raw != None:
         # returned value is an object
         result = get_readable_attrs( result_raw )
      
      elif func in LIST_API and result_raw != None and isinstance( result_raw, list ):
         # returned value is a list of objects
         result = []
         for ret_raw in result_raw:
            ret = get_readable_attrs( ret_raw )
            result.append( ret )
         
      elif func in CREATE_API or func in UPDATE_API:
         # returned value is a key if it didn't throw an exception
         if result_raw:
            result = True
         else:
            result = False
      
      return result
   
   return inner


class API(object):
   
   def __init__( self ):
      # initialize the API
      global PUBLIC_API
      
      for func in PUBLIC_API:
         self.__setattr__( func.__name__, json_attrs( func ) )
      
   def methods( self ):
      """
      Get the list of methods supported.
      """
      global PUBLIC_API
      method_names = [f.__name__ for f in PUBLIC_API]
      return method_names
      
      
   def echo( self ):
      # echo test
      return "Hello World"

