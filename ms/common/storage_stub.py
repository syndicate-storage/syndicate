#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
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