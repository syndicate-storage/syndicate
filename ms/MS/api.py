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

import base64
import types
import errno
import time
import datetime
import collections

from volume import Volume
from user import SyndicateUser
from gateway import Gateway

from msconfig import *

# ----------------------------------

# list of reader API methods
__READ_API = [
   storage.read_volume,
   storage.read_volume_by_name,
   storage.get_volume_by_name,
   storage.read_user_gateway,
   storage.read_replica_gateway,
   storage.read_acquisition_gateway
]

# list of updater API methods
__UPDATE_API = [
   storage.update_volume,
   storage.update_user_gateway,
   storage.update_replica_gateway,
   storage.update_acquisition_gateway
]

# list of creator API methods
__CREATE_API = [
   storage.create_volume,
   storage.create_user_gateway,
   storage.create_replica_gateway,
   storage.create_acquisition_gateway
]

# list of deletor API methods
__DELETE_API = [
   storage.delete_volume,
   storage.delete_user_gateway,
   storage.delete_replica_gateway,
   storage.delete_acquisition_gateway
]

# list of listor API methods
__LIST_API = [
   storage.list_user_gateways_by_volume,
   storage.list_replica_gateways_by_volume,
   storage.list_acquisition_gateways_by_volume,
   storage.list_user_gateways_by_host,
   storage.list_replica_gateways_by_host,
   storage.list_acquisition_gateways_by_host
]

__PUBLIC_API = __READ_API + __UPDATE_API + __CREATE_API + __DELETE_API + __LIST_API

# map method names to methods 
PUBLIC_API_METHODS = dict( [ (f.__name__, f) for f in __PUBLIC_API] )

# map method names to each class of methods
READ_API_METHODS = dict( [ (f.__name__, f) for f in __READ_API] )
UPDATE_API_METHODS = dict( [ (f.__name__, f) for f in __UPDATE_API] )
CREATE_API_METHODS = dict( [ (f.__name__, f) for f in __CREATE_API] )
DELETE_API_METHODS = dict( [ (f.__name__, f) for f in __DELETE_API] )
LIST_API_METHODS = dict( [ (f.__name__, f) for f in __LIST_API] )

# ----------------------------------

# methods to authenticate using a Volume administrator password
AUTH_VOLUME_API = [
   storage.read_volume,
   storage.read_volume_by_name,
   storage.get_volume_by_name,
   storage.update_volume,
   storage.delete_volume,
   storage.create_user_gateway,
   storage.create_replica_gateway,
   storage.create_acquisition_gateway,
   storage.list_user_gateways_by_volume,
   storage.list_replica_gateways_by_volume,
   storage.list_acquisition_gateways_by_volume,
   storage.list_user_gateways_by_host,
   storage.list_replica_gateways_by_host,
   storage.list_acquisition_gateways_by_host
]

# methods to authenticate using a Gateway administrator password
AUTH_GATEWAY_API = [
   storage.read_user_gateway,
   storage.read_replica_gateway,
   storage.read_acquisition_gateway,
   storage.update_user_gateway,
   storage.update_replica_gateway,
   storage.update_acquisition_gateway
]

__AUTHENTICATORS = [(AUTH_VOLUME_API, Volume.Authenticate), (AUTH_GATEWAY_API, Gateway.Authenticate)]

# which authenticators do we use for particular methods?
AUTHENTICATORS = dict( reduce( lambda x, y: x + y, [ [(f.__name__, API_auth[1]) for f in API_auth[0]] for API_auth in __AUTHENTICATORS ] ) )

# ----------------------------------

def read_basic_auth( headers ):
   """
   Parse a basic auth header
   """
   
   basic_auth = headers.get("Authorization")
   if basic_auth == None:
      logging.info("No Authorization header")
      return (None, None)

   name, passwd = '', ''
   try:
      user_info = base64.b64decode( basic_auth[6:] )
      name, passwd = user_info.split(":")
   except Exception, e:
      logging.exception( e )
      logging.info("Incomprehensible Authorization header: '%s'" % basic_auth )
      return (None, None)

   return (name, passwd)


def get_readable_attrs( obj ):
   """
      Turn an object into a dictionary of its readable attributes.
   """
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
      
      funcname = func.__name__
      result = None
      
      if funcname in READ_API_METHODS.keys() and result_raw != None:
         # returned value is an object
         result = get_readable_attrs( result_raw )
      
      elif funcname in LIST_API_METHODS.keys() and result_raw != None and isinstance( result_raw, list ):
         # returned value is a list of objects
         result = []
         for ret_raw in result_raw:
            ret = get_readable_attrs( ret_raw )
            result.append( ret )
         
      elif funcname in CREATE_API_METHODS.keys() or funcname in UPDATE_API_METHODS.keys():
         # returned value is a key if it didn't throw an exception
         if result_raw:
            result = True
         else:
            result = False
      
      return result
   
   # preserve original function name
   inner.__name__ = func.__name__
   return inner


def authenticated( func, headers ):
   """
      Decorator to ensure that we have passed the appropriate credentials to access data
   """
   def inner( *args, **kw ):
      global AUTHENTICATORS
      authenticator = AUTHENTICATORS.get( func.__name__ )
      
      if authenticator:
         # attempt to authenticate 
         obj_name_or_id, passwd = read_basic_auth( headers )
         auth_result = authenticator( obj_name_or_id, passwd )
         
         if auth_result == None:
            # object does not exist
            logging.info('%s(%s): no record' % (func.__name__, obj_name_or_id) )
            raise Exception( "No such record" )
         
         elif auth_result == False:
            # object exists, but bad authentication token
            logging.info('%s(%s): authentication failed' % (func.__name__, obj_name_or_id) )
            raise Exception( "Authentication failed" )
         
         else:
            # success!
            ret = func( *args, **kw )
            return ret
      
      else:
         # no authenticator
         ret = func( *args, **kw )
         return ret
   
   # preserve original function name
   inner.__name__ = func.__name__
   return inner


class API(object):
   
   def __init__( self, headers ):
      self.headers = headers
   
   def __getattr__( self, name ):
      # lazily load functions from the API 
      global PUBLIC_API_METHODS
      if name in PUBLIC_API_METHODS.keys():
         return json_attrs( authenticated( PUBLIC_API_METHODS[name], self.headers ) )
      
   def methods( self ):
      """
      Get the list of methods supported.
      """
      global PUBLIC_API_METHODS
      return PUBLIC_API_METHODS.keys()
      
      
   def echo( self ):
      # echo test
      return "Hello World"

