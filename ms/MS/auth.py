#!/usr/bin/env python

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

"""
MS authentication framework
"""

import storage.storagetypes as storagetypes
import storage.storage as storage

import logging

import random
import os
import json

import base64
import types
import errno
import time
import datetime
import collections
import inspect

from volume import Volume #, VolumeAccessRequest
from user import SyndicateUser
from gateway import Gateway

from common.msconfig import *
import common.jsonrpc as jsonrpc

# ----------------------------------
def is_user( caller_user_or_object ):
   if caller_user_or_object.__class__ == SyndicateUser:
      return True
   
   else:
      return False
   
# ----------------------------------
def is_admin( caller_user ):
   if caller_user != None and is_user( caller_user ) and caller_user.is_admin:
      return True 
   
   return False
   
# ----------------------------------
def assert_admin_or( caller_user, predicate ):
   assert caller_user != None, "Authentication required"
   
   if not is_admin(caller_user) or not predicate:
      raise Exception("User '%s' is not sufficiently privileged" % caller_user.email)
   
   
# ----------------------------------
def assert_admin( caller_user ):
   assert_admin_or( caller_user, True )


# ----------------------------------
def __object_equivalent( obj1, obj2 ):
   return obj1.key == obj2.key

# ----------------------------------
def __get_readable_attrs( caller_user_or_object, target_object, object_cls ):
   read_attrs = []
   
   caller_user = None
   caller_object = None
   
   if is_user( caller_user_or_object ):
      caller_user = caller_user_or_object
   else:
      caller_object = caller_user_or_object
   
   if is_admin( caller_user ):
      # admin called us
      read_attrs = object_cls.get_admin_read_attrs()
   
   elif caller_user != None and target_object != None and target_object.owned_by( caller_user ):
      # the user that owns the read object called us
      read_attrs = object_cls.get_api_read_attrs()
   
   elif caller_object != None and target_object != None and __object_equivalent( caller_object, target_object ):
      # the object is reading itself 
      read_attrs = object_cls.get_api_read_attrs()
   
   else:
      # someone (possibly anonymous) read this object
      read_attrs = object_cls.get_public_read_attrs()
      
   return read_attrs


# ----------------------------------
def __to_dict( obj, attr_list ):
   """
      Turn an object into a dictionary of its readable attributes.
   """
   ret = {}
   if len(attr_list) > 0:
      for attr in attr_list:
         ret[attr] = getattr( obj, attr )
   
   return ret

# ----------------------------------
def filter_result( caller_user_or_object, object_cls, result_raw ):
   """
      Ensure that we return a dict of readable attributes for an object
   """
   
   if isinstance( result_raw, list ):
      # returned value is a list of objects
      result = []
      for ret_raw in result_raw:
         ret = None
         
         if isinstance( ret_raw, storagetypes.Object ):
            attr_list = __get_readable_attrs( caller_user_or_object, ret_raw, object_cls )
            ret = __to_dict( ret_raw, attr_list )
            
            if not ret:
               # nothing to return
               continue
               
         else:
            ret = ret_raw
         
         result.append( ret )
         
         
   elif isinstance( result_raw, storagetypes.Object ):
      # returned value is an object
      attr_list = __get_readable_attrs( caller_user_or_object, result_raw, object_cls )
      result = __to_dict( result_raw, attr_list )
      
   else:
      # returned value is an atom
      result = result_raw
      
   return result


# ----------------------------------
def object_id_from_name( object_name, func, args, kw ):
   argspec = inspect.getargspec( func )
   
   # is it a positional arg?
   for i in xrange(0, len(argspec.args)):
      if object_name == argspec.args[i]:
         return args[i]
   
   # is it a keyword arg?
   for (key, value) in kw.items():
      if object_name == key:
         return value 
   
   return None

# ----------------------------------
def assert_public_method( method ):
   if method == None:
      # does not exist
      raise Exception("No such method '%s'" % method_name)
   
   if type(method) != types.FunctionType:
      # not a function
      raise Exception("No such method '%s'" % method_name)
   
   if not getattr(method, "is_public", False):
      # not a function decorated by Authenticate (i.e. not part of the API)
      raise Exception("No such method '%s'" % method_name)
   
   return True

# ----------------------------------
class CreateAPIGuard:
   # creating an object requires a suitably capable user
   def __init__(self, object_cls, admin_only=False, caller_user=None, **kw ):
      self.object_cls = object_cls 
      self.admin_only = admin_only
      self.pass_caller_user = caller_user
   
   def __call__(self, func):
      def inner( caller_user, *args, **kw ):
         if caller_user is None:
            raise Exception("Caller has insufficient privileges")
         
         if not is_user( caller_user ):
            # not a user
            raise Exception("Caller is not a user")
         
         if self.admin_only:
            assert_admin( caller_user )
         
         if self.pass_caller_user:
            kw[self.pass_caller_user] = caller_user
            
         ret = func(*args, **kw)
         
         return filter_result( caller_user, self.object_cls, ret )
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = self.object_cls.key_attrs
      inner.mutable = True
      return inner
      

# ----------------------------------
class ReadAPIGuard:
   # reading an object requires one of three things:  user is an admin, user owns the object, or the object is trying to read itself.
   def __init__(self, object_cls, admin_only=False, **kw ):
      self.object_cls = object_cls
      self.admin_only = admin_only
   
   def __call__(self, func):
      def inner(caller_user, *args, **kw):
         
         if caller_user == None:
            # authentication failed
            raise Exception("Caller has insufficient privileges")
         
         if self.admin_only:
            assert_admin( caller_user )
         
         ret = func( *args, **kw )
         
         return filter_result( caller_user, self.object_cls, ret )
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = self.object_cls.key_attrs
      return inner
      

# ----------------------------------
class UpdateAPIGuard:
   """
   Decorator for an API method that will update an object.  The decorator ensures certain 
   security invariants are met before allowing the update to happen.
   """
   
   def __init__(self, target_object_cls, admin_only=False, caller_user=None, target_object_name=None, **kw ):
      """
      * target_object_cls:      Class of the object to be updated
      * admin_only:             if True, then only a user with the 'admin' flag set can call this method.
      * pass_caller_user:       if not None, then pass the SyndicateUser that called this method as a keyword argument with the name given in this variable.
      * target_object_name:     if not None, then this is the name of the argument in the API call whose value identifies the object (i.e. it can be fed into the object's Read() method).
      * parse_args (in **kw):   if set to a function, use that function to evaluate the API method's arguments before calling it (used client-side for pre-RPC processing)
      """
      self.target_object_cls = target_object_cls
      self.admin_only = admin_only
      self.pass_caller_user = caller_user
      self.target_object_name = target_object_name
   
   def __call__(self, func):
      def inner( caller_user, *args, **kw ):
         
         if caller_user == None:
            # authentication failed
            raise Exception("Caller has insufficient privileges")
         
         if not is_user( caller_user ):
            # not a user
            raise Exception("Caller is not a user")
      
         if self.admin_only:
            assert_admin( caller_user )
            
         # find the target object ID
         target_object_id = object_id_from_name( self.target_object_name, func, args, kw )
         
         if target_object_id is None:
            # invalid argument
            raise Exception("No %s ID given" % (self.target_object_cls.__name__))
         
         target_object = self.target_object_cls.Read( target_object_id )
         
         if target_object == None:
            raise Exception("No such %s: %s" % (self.target_object_cls.__name__, target_object_id))
         
         if not is_admin( caller_user ) and not target_object.owned_by( caller_user ):
            raise Exception("Object '%s: %s' is not owned by '%s'" % (self.target_object_cls.__name__, target_object_id, caller_user.email))
         
         # only filter keywords that are writable in the object
         method_kw = {}
         write_kw = {}
         
         for attr in kw.keys():
            if attr not in self.target_object_cls.write_attrs:
               method_kw[attr] = kw[attr]
            else:
               write_kw[attr] = kw[attr]
         
         method_kw.update( write_kw )
         
         if self.pass_caller_user:
            method_kw[self.pass_caller_user] = caller_user 
            
         ret = func( *args, **method_kw)
         
         return ret
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = self.target_object_cls.key_attrs
      inner.mutable = True
      return inner
   
   
# ----------------------------------
class DeleteAPIGuard:
   # Deleting an object requires a suitably capable user.
   # NOTE: the decorated function must take an object's ID as its first argument!
   def __init__(self, target_object_cls, caller_user=None, admin_only=False, target_object_name=None, **kw ):
      self.admin_only = admin_only
      self.target_object_cls = target_object_cls
      self.target_object_name = target_object_name
      self.pass_caller_user = caller_user
   
   def __call__(self, func):
      def inner( caller_user, *args, **kw ):
         
         if caller_user == None:
            # authentication failed
            raise Exception("Caller has insufficient privileges")
         
         if not is_user( caller_user ):
            # not a user
            raise Exception("Caller is not a user")
         
         if self.admin_only:
            assert_admin( caller_user )
         
         # get the target object ID 
         target_object_id = object_id_from_name( self.target_object_name, func, args, kw )
         
         if target_object_id is None:
            raise Exception("No %s ID given" % self.target_object_cls.__name__)
         
         target_object = self.target_object_cls.Read( target_object_id )
         
         if target_object == None:
            # done!
            return True
         
         if not is_admin( caller_user ) and not target_object.owned_by( caller_user ):
            raise Exception("Object '%s: %s' is not owned by '%s'" % (self.target_object_cls.__name__, target_object_id, caller_user.email))
         
         if self.pass_caller_user:
            kw[self.pass_caller_user] = caller_user 
            
         ret = func( *args, **kw)
         
         return ret
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = self.target_object_cls.key_attrs
      inner.mutable = True
      return inner
         
   
# ----------------------------------
class ListAPIGuard:
   # listing objects requires a suitably capable user.  An unprivileged user can only list API-level attributes of objects it owns, and only public attributes of objects it does not own.
   def __init__(self, object_cls, admin_only=False, caller_user=None, **kw ):
      self.object_cls = object_cls
      self.admin_only = admin_only
      self.pass_caller_user = caller_user
   
   def __call__(self, func):
      def inner(caller_user, *args, **kw):
         
         if caller_user == None:
            raise Exception("Caller has insufficient privileges")
         
         if not is_user( caller_user ):
            # not a user
            raise Exception("Caller is not a user")
         
         if self.admin_only:
            assert_admin( caller_user )
         
         if self.pass_caller_user != None:
            kw[self.pass_caller_user] = caller_user
         
         list_ret = func(*args, **kw)
         
         return filter_result( caller_user, self.object_cls, list_ret )
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = self.object_cls.key_attrs
      return inner


# ----------------------------------
class BindAPIGuard:
   # caller user is attempting to bind/unbind a source and target object.  Verify that the caller user owns it first, or is admin.
   # NOTE: the decorated function must take a source object ID as its first argument, and a target object ID as its second argument!
   def __init__(self, source_object_cls, target_object_cls, caller_owns_source=True, caller_owns_target=True, admin_only=False, caller_user=None, source_object_name=None, target_object_name=None, **kw ):
      self.source_object_cls = source_object_cls
      self.target_object_cls = target_object_cls
      self.admin_only = admin_only
      self.caller_owns_source = caller_owns_source
      self.caller_owns_target = caller_owns_target
      self.pass_caller_user = caller_user
      self.source_object_name = source_object_name
      self.target_object_name = target_object_name
   
   def __call__(self, func):
      def inner(caller_user, *args, **kw):
         if caller_user == None:
            # authentication failed
            raise Exception("Caller has insufficient privileges")
         
         if not is_user( caller_user ):
            # not a user
            raise Exception("Caller is not a user")
         
         if self.admin_only:
            assert_admin( caller_user )
         
         source_object_fut = None
         target_object_fut = None
         futs = []
         
         # get the source object
         source_object_id = object_id_from_name( self.source_object_name, func, args, kw )
         source_object_fut = self.source_object_cls.Read( source_object_id, async=True )
         futs.append( source_object_fut )
         
         # get the target object
         target_object_id = object_id_from_name( self.target_object_name, func, args, kw )
         target_object_fut = self.target_object_cls.Read( target_object_id, async=True )
         futs.append( target_object_fut )
      
         storagetypes.wait_futures( futs )
         
         source_object = None
         target_object = None
         
         if source_object_fut != None:
            source_object = source_object_fut.get_result()
         
         if target_object_fut != None:
            target_object = target_object_fut.get_result()
      
         # check the source object...
         source_object_id = object_id_from_name( self.source_object_name, func, args, kw )
         
         if source_object_id is None:
            raise Exception("BUG: No %s ID given" % self.source_object_cls.__name__)
         
         if source_object == None:
            raise Exception("Source object '%s' does not exist" % source_object_id )
         
         if self.caller_owns_source and not source_object.owned_by( caller_user ) and not caller_user.is_admin:
            raise Exception("Source object '%s' is not owned by '%s'" % (source_object_id, caller_user.email) )
            
         # check the target object...
         target_object_id = object_id_from_name( self.target_object_name, func, args, kw )
         
         if target_object_id is None:
            raise Exception("No %s ID given" % self.target_object_cls.__name__)
         
         if target_object == None:
            raise Exception("Target object '%s' does not exist" % target_object_id )
         
         if self.caller_owns_target and not target_object.owned_by( caller_user ) and not caller_user.is_admin:
            raise Exception("Target object '%s' is not owned by '%s'" % (target_object_id, caller_user.email))
            
         if self.pass_caller_user:
            kw[self.pass_caller_user] = caller_user 
            
         # all check pass...
         result = func( *args, **kw )
         
         assert isinstance( result, bool ), "Internal Bind error"
         
         return result
      
      inner.__name__ = func.__name__
      inner.mutable = True
      return inner

# ----------------------------------
class Authenticate:
   
   def __call__(self, func):
      def inner( authenticated_user, *args, **kw ):
         if authenticated_user is None and self.need_authentication:
            raise Exception("Unauthorized user")
         
         return func( authenticated_user, *args, **kw )
      
      inner.__name__ = func.__name__
      inner.object_id_attrs = getattr( func, "object_id_attrs", None )
      inner.target_object_name = getattr( func, "target_object_name", None )
      inner.source_object_name = getattr( func, "source_object_name", None )
      inner.mutable = getattr( func, "mutable", False )
      inner.is_public = True
      return inner

# ----------------------------------
class AuthMethod( object ):
   def __init__(self, method_func, authenticated_user ):
      # make sure this is decorated with Authenticate
      assert_public_method( method_func )
      self.authenticated_user = authenticated_user
      self.method_func = method_func
   
   def __call__(self, *args, **kw ):
      ret = self.method_func( self.authenticated_user, *args, **kw )
      return ret
         
