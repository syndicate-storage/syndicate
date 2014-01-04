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


#-------------------------
def invalid_request( start_response, status="400 Invalid request", resp="Invalid request\n" ):
   '''
   HTTP error
   '''
   
   headers = [('Content-Type', 'text/plain'), ('Content-Length', str(len(resp)))]
   start_response( status, headers )
   
   return [resp]

#-------------------------
def valid_request( start_response, status="200 OK", resp="OK" ):
   '''
   HTTP OK
   '''
   
   headers = [('Content-Type', 'text/plain'), ('Content-Length', str(len(resp)))]
   start_response( status, headers )
   
   return [resp]
