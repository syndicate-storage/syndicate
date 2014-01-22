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

from msconfig import *
import api
import inspect

def assert_public_method( method ):
   return (method != None)

class AuthMethod( object ):
   def __init__(self, method_func, authenticated_caller):
      self.method_func = method_func
   
   def __call__(self, *args, **kw):
      return self.method_func( *args, **kw )
   
   
class StubAuth( object ):
   expect_verifying_key = False
   admin_only = False
   
   def __init__(self, *args, **kw ):
      self.admin_only = kw.get('admin_only', False)
      self.parse_args = kw.get('parse_args', None)
   
   def __call__(self, func):
      
      def inner( *args, **kw ):
         return func(*args, **kw)
      
      inner.__name__ = func.__name__
      inner.__doc__ = func.__doc__
      inner.expect_verifying_key = self.expect_verifying_key
      inner.admin_only = self.admin_only
      inner.argspec = inspect.getargspec( func )
      inner.parse_args = self.parse_args
      
      return inner

class CreateAPIGuard( StubAuth ):
   expect_verifying_key = True

class ReadAPIGuard( StubAuth ):
   expect_verifying_key = True

class UpdateAPIGuard( StubAuth ):
   pass

class DeleteAPIGuard( StubAuth ):
   revoke_key_id = None
   revoke_key_name = None

class ListAPIGuard( StubAuth ):
   pass

class BindAPIGuard( StubAuth ):
   pass

class Authenticate( StubAuth ):
   changes_signing_key = False 
   signing_key_types = None
   signing_key_ids = None
   verify_key_id = None
   verify_key_type = None
   revoke_key_id = None
   revoke_key_type = None
   trust_key_id = None
   trust_key_type = None
   store_signing_key = True
   revoke_verify_key = True
   need_verifying_key = True
   default_object_key_type = None
   default_object_key_id = None
   
   @classmethod
   def get_signing_key_names( cls, func, signing_key_ids, method_name, args, kw ):
      if signing_key_ids == None:
         # this method does not return any keys for us to store
         return None
      
      ret = []
      
      for signing_key_id in signing_key_ids:
         # special key ID?
         if signing_key_id == api.SIGNING_KEY_DEFAULT_USER_ID:
            ret.append( api.SIGNING_KEY_DEFAULT_USER_ID )
               
         # positional arg?
         elif signing_key_id in func.argspec.args:
            idx = func.argspec.args.index( signing_key_id )
            ret.append( args[idx] )
         
         elif signing_key_id in kw.keys():
            ret.append( signing_key_id )
            
         else:
            raise Exception("No such key ID in method arguments: %s" % signing_key_id )
      
      return ret
   
   @classmethod
   def get_verify_key_name( cls, func, verify_key_id, method_name, args, kw, method_result ):
      # NOTE: pass None for method_result to query the arguments instead of the result.
      
      if verify_key_id == None:
         # this method does not return any keys for us to store
         return None
      
      # special key ID?
      if verify_key_id == api.SIGNING_KEY_DEFAULT_USER_ID:
         return verify_key_id
      
      if isinstance( method_result, dict ):
         # get key name from method result
         if verify_key_id in method_result.keys():
            return method_result[verify_key_id]
      
      elif verify_key_id in func.argspec.args:
         # get key name from argument list
         idx = func.argspec.args.index( verify_key_id )
         return args[idx]
      
      elif verify_key_id in kw.keys():
         return kw[verify_key_id]
         
      raise Exception("No such key ID in method arguments or result: %s" % verify_key_id )
      
      
   @classmethod
   def get_revoke_key_name( cls, func, revoke_key_id, method_name, args, kw ):
      if revoke_key_id == None:
         return None 
      
      if revoke_key_id in func.argspec.args:
         idx = func.argspec.args.index( revoke_key_id )
         return args[idx]
      
      elif revoke_key_id in kw.keys():
         return kw[revoke_key_id]
      
      raise Exception("No such key ID in method arguments: %s" % revoke_key_id )
   
   
   @classmethod
   def get_trust_key_name( cls, func, trust_key_id, method_name, args, kw ):
      if trust_key_id == None:
         return None 
      
      if trust_key_id in func.argspec.args:
         idx = func.argspec.args.index( trust_key_id )
         return args[idx]
      
      elif trust_key_id in kw.keys():
         return kw[trust_key_id]
      
      raise Exception("No such key ID in method arguments: %s" % trust_key_id )
   
   @classmethod
   def get_default_object_key_name( cls, func, default_object_key_id, method_name, args, kw ):
      if default_object_key_id == None:
         return None
      
      if default_object_key_id in func.argspec.args:
         idx = func.argspec.args.index( default_object_key_id )
         return args[idx]
      
      elif default_object_key_id in kw.keys():
         return kw[default_object_key_id]
      
      raise Exception("No such key ID in method arguments: %s" % default_object_key_id )
   
   def __init__(self, object_authenticator=None, object_response_signer=None,
                      signing_key_types=None, signing_key_ids=None,
                      verify_key_type=None, verify_key_id=None,
                      revoke_key_type=None, revoke_key_id=None,
                      trust_key_type=None, trust_key_id=None,
                      store_signing_key=True, revoke_verify_key=True,
                      default_object_key_type=None, default_object_key_id=None,
                      need_verifying_key=True ):
      
      if len(signing_key_types) != len(signing_key_ids):
         raise Exception("BUG: Signing key type/id length mismatch")
      
      self.signing_key_types = signing_key_types
      self.signing_key_ids = signing_key_ids
      self.verify_key_type = verify_key_type
      self.verify_key_id = verify_key_id
      self.revoke_key_type = revoke_key_type
      self.revoke_key_id = revoke_key_id
      self.trust_key_type = trust_key_type
      self.trust_key_id = trust_key_id
      self.default_object_key_id = default_object_key_id
      self.default_object_key_type = default_object_key_type
      self.store_signing_key = store_signing_key
      self.revoke_verify_key = revoke_verify_key
      self.need_verifying_key = need_verifying_key
   
   def __call__(self, func):
      func.changes_signing_key = self.changes_signing_key
      func.signing_key_types = self.signing_key_types
      func.signing_key_ids = self.signing_key_ids
      func.verify_key_type = self.verify_key_type
      func.verify_key_id = self.verify_key_id
      func.revoke_key_type = self.revoke_key_type 
      func.revoke_key_id = self.revoke_key_id
      func.trust_key_type = self.trust_key_type
      func.trust_key_id = self.trust_key_id
      func.default_object_key_type = self.default_object_key_type
      func.default_object_key_id = self.default_object_key_id
      func.store_signing_key = self.store_signing_key
      func.revoke_verify_key = self.revoke_verify_key
      func.need_verifying_key = self.need_verifying_key
      func.get_signing_key_names = lambda method_name, args, kw: Authenticate.get_signing_key_names( func, self.signing_key_ids, method_name, args, kw )
      func.get_verify_key_name = lambda method_name, args, kw, method_result: Authenticate.get_verify_key_name( func, self.verify_key_id, method_name, args, kw, method_result )
      func.get_revoke_key_name = lambda method_name, args, kw: Authenticate.get_revoke_key_name( func, self.revoke_key_id, method_name, args, kw )
      func.get_trust_key_name = lambda method_name, args, kw: Authenticate.get_trust_key_name( func, self.trust_key_id, method_name, args, kw )
      func.get_default_object_key_name = lambda method_name, args, kw: Authenticate.get_default_object_key_name( func, self.default_object_key_id, method_name, args, kw )
      func.is_public = True
      return func
      