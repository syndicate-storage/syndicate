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

try:
   import logging
except:
   import log
   logging = log.get_logger()

class StorageStub( object ):
   def __init__(self, *args, **kw ):
      pass
   
   def __getattr__(self, attrname ):
      def stub_storage(*args, **kw):
         logging.warn("Stub '%s'" % attrname)
         return None
      
      return stub_storage