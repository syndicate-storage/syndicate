#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import rm_common
import rm_request

REQUIRED_POST_FIELDS = [
   "metadata",
   "data"
]

#-------------------------
def post( self, headers_dict, post_dict ):
   '''
      headers_dict: header name (str): header value (str)
      post_dict: field name (str): field data (str)
   '''
   
   global REQUIRED_POST_FIELDS
   
   # sanity check
   for field in REQUIRED_POST_FIELDS:
      if not post_dict.has_key( field ):
         raise Exception("POST: Missing field: %s" % field)
   
   # parse 
   req_info = rm_request.parse_request_info( post_dict['metadata'] )
   if req_info == None:
      raise Exception("Invalid field 'metadata'")
   
   # store
   