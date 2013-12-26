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

import webapp2
import urlparse

import tests

import types
import random
import os
import time

class MSDebugHandler( webapp2.RequestHandler ):

   def get( self, testname, path ):
      start_time = time.time()
      
      if len(path) == 0:
         path = "/"

      if path[0] != '/':
         path = "/" + path
      
      args = self.request.GET.dict_of_lists()
      
      for (k,v) in args.items():
         if type(v) == types.ListType and len(v) == 1:
            args[k] = v[0]
      
      # debug request
      test = getattr( tests, testname )
      status = None
      msg = None
      if test == None:
         status = 404
         msg = "No such test '%s'" % testname
      else:
         status, msg = test.test( path, args )

      self.response.status = status
      self.response.headers['X-Total-Time'] = str( int( (time.time() - start_time) * 1e9) )
      self.response.write( msg )
      return

   def put( self, _path ):
      pass