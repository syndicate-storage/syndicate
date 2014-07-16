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

import os
import syndicate.rg.common as rg_common
import syndicate.rg.request as rg_request
import syndicate.rg.storage as rg_storage

from hashlib import sha256 as HashFunc

from wsgiref.simple_server import make_server 
from wsgiref.util import FileWrapper
from cgi import parse_qs, FieldStorage

from StringIO import StringIO

import types
import errno 

log = rg_common.get_logger()

#-------------------------
METADATA_FIELD_NAME = "metadata"
DATA_FIELD_NAME = "data"

#-------------------------
def validate_post( self, post_dict ):
   
   global REQUIRED_POST_FIELDS
   
   # sanity check
   try:
      rg_common.validate_fields( post_dict, REQUIRED_POST_FIELDS )
      return 0
   except:
      return 400


#-------------------------
def post_interpret_error( rc ):
   """
      Intepret a system error code into an HTTP error code,
      for purposes of validating a caller's POST request.
   """
   
   if rc == -errno.EAGAIN:
      return (503, "Try again later")
   
   elif rc == -errno.ENOENT:
      return (404, "Not found")
   
   else:
      return (400, "Invalid Request")


#-------------------------
def validate_infile( req_info, infile ):
   '''
      validate the incoming data.
      check its hash and size
      return (HTTP status, message)
   '''
   
   hf = HashFunc()
   total_read = 0
   buflen = 4096
   
   while True:
      inbuf = infile.read( buflen )
      hf.update( inbuf )
      
      total_read += len(inbuf)
      
      if len(inbuf) == 0:
         break
      
   infile_hash = hf.hexdigest()
   infile.seek(0)
   
   # check size 
   if req_info.size != total_read:
      log.error("Size mismatch: expected %s, got %s" % (req_info.size, total_read))
      return (400, "Invalid Request")
   
   # check hash
   if req_info.data_hash != infile_hash:
      log.error("Hash mismatch: expected '%s', got '%s'" % (req_info.data_hash, infile_hash))
      return (400, "Invalid request")

   return (200, "OK")


#-------------------------
def post( metadata_field, infile ):
   '''
      Process a POST request.  Return an HTTP status code.  Read all data from infile.
      
      metdata_field: a string containing a serialized ms_gateway_request_info structure
      infile: file-like object which can be read from
   '''
   
   # parse
   req_info = None
   try:
      req_info = rg_request.parse_request_info_from_pb( metadata_field )
   except Exception, e:
      # verification failure
      log.exception( e )
      return (403, "Authorization Required")
   
   if req_info == None:
      log.error("could not parse uploaded request info")
      return (400, "Invalid request")
   
   log.info("POST %s" % rg_request.req_info_to_string( req_info ) )
   
   # validate security--the calling gateway must be a UG with CAP_WRITE_DATA
   rc = rg_request.gateway_is_UG( req_info )
   if rc != 0:
      return post_interpret_error( rc )
   
   rc = rg_request.check_post_caps( req_info )
   if rc != 0:
      return post_interpret_error( rc )
   
   # validate the input
   rc, msg = validate_infile( req_info, infile )
   if rc != 200:
      return (rc, msg)
   
   # store
   rc = 0
   try:
      rc = rg_storage.write_data( req_info, infile )
   except Exception, e:
      log.exception( e )
      rc = (500, "Internal server error")
   
   log.info("write_data rc = %s" % (str(rc)) )
   
   return (rc, "OK")


#-------------------------
def get( url_path, outfile ):
   '''
      Process a GET request.  Return an HTTP status code.  Write all data to outfile.
      
      url_path: path of the object to fetch
      outfile: file-like object which can be written to
   '''
   
   # parse
   req_info = rg_request.parse_request_info_from_url_path( url_path )
   if req_info == None:
      log.error("Invalid URL path '%s'" % url_path )
      return (400, "Invalid request")
   
   # fetch
   rc = 0
   status = "OK"
   try:
      rc = rg_storage.read_data( req_info, outfile )
   except Exception, e:
      log.exception( e )
      rc = 500
      status = "Internal server error"
   
   log.info("read_data rc = %s" % str(rc) )
   
   return (rc, status)


#-------------------------
def delete( metadata_field, outfile ):
   '''
      Process a DELETE request.  Return an HTTP status code.
      Generate and write out a deletion receipt, if this is a manifest.
      Do this even if the data wasn't found (i.e. the HTTP status code 
      indicates the status of the operation, but we should always 
      give back a deletion receipt as proof of work).
      
      metadata_field: uploaded metadata value for the request
   '''
   
   # parse
   req_info = None
   try:
      req_info = rg_request.parse_request_info_from_pb( metadata_field )
   except Exception, e:
      # verification failure
      log.exception( e )
      return (403, "Authorization required")
   
   if req_info == None:
      log.error("could not parse uploaded request info")
      return (400, "Invalid request")
   
   log.info("DELETE %s" % rg_request.req_info_to_string( req_info ) )
   
   # validate security--the calling gateway must be a UG with CAP_COORDINATE
   if rg_request.gateway_is_UG( req_info ) != 0:
      return (400, "Invalid Request")
   
   if rg_request.check_delete_caps( req_info ) != 0:
      return (400, "Invalid Reqeust")
   
   # delete
   rc = 0
   try:
      rc = rg_storage.delete_data( req_info )
   except Exception, e:
      log.exception( e )
      rc = (500, "Internal server error")
   
   log.info("delete_data rc = %s" % str(rc) )
   
   if (rc == 200 or rc == 404) and req_info.type == rg_request.is_manifest_request( req_info ):
      
      # generate a signed deletion receipt, even if the data was not found 
      deletion_receipt = rg_request.make_deletion_receipt( req_info )
      
      data = deletion_receipt.SerializeToString()
      
      outfile.write( data )
   
   return (rc, "OK")



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


#-------------------------
def wsgi_handle_request( environ, start_response ):
   '''
   handle one WSGI request
   '''
   
   global METADATA_FIELD_NAME
   global DATA_FIELD_NAME
   
   required_post_fields = [METADATA_FIELD_NAME, DATA_FIELD_NAME]
   required_delete_fields = [METADATA_FIELD_NAME]
   
   if environ['REQUEST_METHOD'] == 'GET':
      # GET request
      url_path = environ['PATH_INFO']
      outfile = StringIO()
      
      rc, msg = get( url_path, outfile )
      
      if rc == 200:
         size = outfile.len
         outfile.seek(0)
         headers = [('Content-Type', 'application/octet-stream'), ('Content-Length', str(size))]
         start_response( '200 %s' % msg, headers )
         
         return FileWrapper( outfile )
      
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg) )
      
   elif environ['REQUEST_METHOD'] == 'POST':
      # POST request.
      # get POST'ed fields
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
      
      # validate
      for f in required_post_fields:
         if f not in post_fields.keys():
            return invalid_request( start_response )
      
      metadata_field = post_fields[METADATA_FIELD_NAME].value
      infile = post_fields[DATA_FIELD_NAME].file
      
      # if no file was given, then make a stringIO wrapper around the given string
      if infile == None:
         infile = StringIO( post_fields[DATA_FIELD_NAME].value )
      
      rc, msg = post( metadata_field, infile )
      
      if rc == 200:
         return valid_request( start_response )
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg), resp="error code %s\n" % str(rc))

   elif environ['REQUEST_METHOD'] == 'DELETE':
      # DELETE request
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
      outfile = StringIO()
      
      # validate
      if not post_fields.has_key(METADATA_FIELD_NAME):
         return invalid_request( start_response )
      
      metadata_field = post_fields[METADATA_FIELD_NAME].value
      
      rc, msg = delete( metadata_field, outfile )
      
      if rc == 200 or rc == 404:
         # send back the deletion receipt
         size = outfile.len
         outfile.seek(0)
         headers = [('Content-Type', 'application/octet-stream'), ('Content-Length', str(size))]
         start_response( '%s %s' % (rc, msg), headers )
         
         return FileWrapper( outfile )
      
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg), resp="error code %s\n" % str(rc))
         
   else:
      # not supported
      return invalid_request( start_response, status="501 No Such Method", resp="Method not supported\n" )
