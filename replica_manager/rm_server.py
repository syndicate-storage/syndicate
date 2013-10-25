#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import rm_common
import rm_request
import rm_storage
import rm_config

from hashlib import sha256 as HashFunc

from wsgiref.simple_server import make_server 
from wsgiref.util import FileWrapper
from cgi import parse_qs, FieldStorage

from StringIO import StringIO

import types

#-------------------------
METADATA_FIELD_NAME = "metadata"
DATA_FIELD_NAME = "data"

#-------------------------
def validate_post( self, post_dict ):
   
   global REQUIRED_POST_FIELDS
   
   # sanity check
   try:
      rm_common.validate_fields( post_dict, REQUIRED_POST_FIELDS )
      return 0
   except:
      return 400

#-------------------------
def post( metadata_field, infile ):
   '''
      Process a POST request.  Return an HTTP status code.  Read all data from infile.
      
      metdata_field: a string containing a serialized ms_gateway_request_info structure
      infile: file-like object which can be read from
   '''
   
   log = rm_common.log
   
   # parse
   req_info = None
   try:
      req_info = rm_request.parse_request_info_from_pb( metadata_field )
   except Exception, e:
      # verification failure
      log.exception( e )
      return (403, "Authorization Required")
      
   if req_info == None:
      log.error("could not parse uploaded request info")
      return (400, "Invalid request")
   
   # validate
   hf = HashFunc()
   hf.update( infile.read() )
   infile_hash = hf.hexdigest()
   
   if req_info.data_hash != infile_hash:
      log.error("Hash mismatch: expected '%s', got '%s'" % (req_info.data_hash, infile_hash))
      return (400, "Invalid request")
   
   infile.seek(0)
   
   # store
   rc = 0
   try:
      rc = rm_storage.write_data( req_info, infile )
   except Exception, e:
      log.exception( e )
      rc = (500, "Internal server error")
   
   log.info("write_data rc = %d" % rc )
   
   return (rc, "OK")


#-------------------------
def get( url_path, outfile ):
   '''
      Process a GET request.  Return an HTTP status code.  Write all data to outfile.
      
      url_path: path of the object to fetch
      outfile: file-like object which can be written to
   '''
   
   log = rm_common.log
   
   # parse
   req_info = rm_request.parse_request_info_from_url_path( url_path )
   if req_info == None:
      log.error("Invalid URL path '%s'" % url_path )
      return (400, "Invalid request")
   
   # fetch
   rc = 0
   try:
      rc = rm_storage.read_data( req_info, outfile )
   except Exception, e:
      log.exception( e )
      rc = (500, "Internal server error")
   
   log.info("read_data rc = %d" % rc )
   
   return (rc, "OK")


#-------------------------
def delete( metadata_field ):
   '''
      Process a DELETE request.  Return an HTTP status code.
      
      metadata_field: uploaded metadata value for the request
   '''
   
   log = rm_common.log
   
   # parse
   req_info = None
   try:
      req_info = rm_request.parse_request_info_from_pb( metadata_field )
   except Exception, e:
      # verification failure
      log.exception( e )
      return (403, "Authorization required")
      
   if req_info == None:
      log.error("could not parse uploaded request info")
      return (400, "Invalid request")
   
   # delete
   rc = 0
   try:
      rc = rm_storage.delete_data( req_info )
   except Exception, e:
      log.exception( e )
      rc = (500, "Internal server error")
   
   log.info("delete_data rc = %d" % rc )
   
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
def wsgi_application( environ, start_response ):
   '''
   WSGI application for the replica manager 
   '''
   
   global METADATA_FIELD_NAME
   global DATA_FIELD_NAME
   
   required_post_fields = [METADATA_FIELD_NAME, DATA_FIELD_NAME]
   required_delete_fields = [METADATA_FIELD_NAME]
   
   log = rm_common.log
   
   if environ['REQUEST_METHOD'] == 'GET':
      # GET request
      url_path = environ['PATH_INFO']
      outfile = StringIO()
      
      rc, msg = get( url_path, outfile )
      
      if rc == 200:
         size = outfile.len
         headers = [('Content-Type', 'application/octet-stream'), ('Content-Length', str(size))]
         start_response( '200 %s' % msg, headers )
         
         return FileWrapper( outfile )
      
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg) )
      
   elif environ['REQUEST_METHOD'] == 'POST':
      # POST request
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
      
      # validate
      for f in required_post_fields:
         if f not in post_fields.keys():
            return invalid_request( start_response )
      
      metadata_field = post_fields['metadata'].value
      infile = post_fields['data'].file
      
      # if no file was given, then make a stringIO wrapper around the given string
      if infile == None:
         infile = StringIO( post_fields['data'].value )
      
      rc, msg = post( metadata_field, infile )
      
      if rc == 200:
         return valid_request( start_response )
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg), resp="error code %s\n" % rc)

   elif environ['REQUEST_METHOD'] == 'DELETE':
      # DELETE request
      post_fields = FieldStorage( fp=environ['wsgi.input'], environ=environ )
      
      # validate
      if not post_fields.has_key('metadata'):
         return invalid_request( start_response )
      
      metadata_field = post_fields['metadata'].value
      
      rc, msg = delete( metadata_field )
      
      if rc == 200:
         return valid_request( start_response )
      else:
         return invalid_request( start_response, status="%s %s" % (rc, msg), resp="error code %s\n" % rc)
         
   else:
      # not supported
      return invalid_request( start_response, status="501 No Such Method", resp="Method not supported\n" )
