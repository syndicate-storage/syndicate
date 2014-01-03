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

from wsgiref.simple_server import make_server 
from syndicate.apps.email.common import http as http_common

#-------------------------
def syndicatemail_server( environ, start_response ):
   '''
   WSGI application request handler for the SyndicateMail server
   '''
   
   if environ['REQUEST_METHOD'] == 'GET':
      # GET request
      url_path = environ['PATH_INFO']
      
      
   elif environ['REQUEST_METHOD'] == 'POST':
      # POST request
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
      

   elif environ['REQUEST_METHOD'] == 'DELETE':
      # DELETE request
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
         
   else:
      # not supported
      return http_common.invalid_request( start_response, status="501 No Such Method", resp="Method not supported\n" )
