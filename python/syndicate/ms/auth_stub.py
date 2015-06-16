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

from syndicate.ms.msconfig import *
import syndicate.ms.api
import inspect

def assert_public_method( method ):
   return (method != None)

class AuthMethod( object ):
   def __init__(self, method_func, authenticated_caller):
      self.method_func = method_func
   
   def __call__(self, *args, **kw):
      return self.method_func( *args, **kw )
   
   
class StubAuth( object ):
   admin_only = False
   
   def __init__(self, *args, **kw ):
      self.admin_only = kw.get('admin_only', False)
      self.parse_args = kw.get('parse_args', None)
   
   def __call__(self, func):
      
      def inner( *args, **kw ):
         return func(*args, **kw)
      
      inner.__name__ = func.__name__
      inner.__doc__ = func.__doc__
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
   
   def __init__(self, auth_methods=[] ):
      
      self.auth_methods = auth_methods
      if len(auth_methods) == 0:
         raise Exception("BUG: no authentication methods given!")
   
   def __call__(self, func):
      func.is_public = True
      func.auth_methods = self.auth_methods
      return func
      