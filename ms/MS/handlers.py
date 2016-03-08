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

import MS
from MS.methods.benchmark import *
from MS.methods.response import *
from MS.methods.file import *

import protobufs.ms_pb2 as ms_pb2
import protobufs.sg_pb2 as sg_pb2

from storage import storage
import storage.storagetypes as storagetypes
import storage.generaluuid as generaluuid

import common.api as api
from entry import MSEntry, MSEntryXAttr
from volume import Volume, VolumeCertBundle
from user import SyndicateUser
from gateway import *
from common.msconfig import *
from common.admin_info import *

import errno
import logging
import random
import os
import urllib
import time
import cgi
import datetime
import common.jsonrpc as jsonrpc
       

# ----------------------------------
class MSVolumeCertRequestHandler(webapp2.RequestHandler):
   """
   Volume cert request handler: give back volume certs
   """

   def get( self, volume_name_or_id_str ):
      
      # try to cast to int, in case it's an ID 
      volume_name_or_id = None
      try:
         volume_name_or_id = int(volume_name_or_id_str)
      except:
         volume_name_or_id = volume_name_or_id_str
         
      volume = Volume.Read( volume_name_or_id )
      if volume is None:
         response_user_error( self, 404, "No such volume" )
         
      response_end( self, 200, volume.volume_cert_bin, "application/octet-stream")
      return


# ----------------------------------
class MSUserCertRequestHandler(webapp2.RequestHandler):
   """
   User cert request handler: give back user certs
   """

   def get( self, username_or_id_str ):
      
      username_or_id = None
      try:
         username_or_id = int(username_or_id_str)
      except:
         username_or_id = username_or_id_str
         
      user = SyndicateUser.Read( username_or_id )
      if user is None:
         response_user_error( self, 404, "No such user" )
         
      response_end( self, 200, user.user_cert_protobuf, "application/octet-stream")
      return


# ----------------------------------
class MSGatewayCertRequestHandler(webapp2.RequestHandler):
   """
   User cert request handler: give back user certs
   """

   def get( self, gateway_name_or_id_str ):
      
      gateway_name_or_id = None
      try:
         gateway_name_or_id = int(gateway_name_or_id_str)
      except:
         gateway_name_or_id = gateway_name_or_id_str
         
      gateway = Gateway.Read( gateway_name_or_id )
      if gateway is None:
         response_user_error( self, 404, "No such gateway" )
         return
         
      response_end( self, 200, gateway.gateway_cert, "application/octet-stream")
      return


# ----------------------------------
class MSCertBundleRequestHandler( webapp2.RequestHandler ):
   """
   Certificate bundle manifest request handler.
   Reply back the signed certificate bundle for a volume's publicly-routable gateways.
   Redirect requests to the version of the signed cert bundle (so the only time a GET succeeds is with the latest-version URL)
   """
   
   def get( self, volume_name_or_id_str, volume_cert_version_str ):
      
      volume_id = None
      volume_name = None
      volume_cert_version = None
      try:
         volume_id = int( volume_name_or_id_str )
      except:
         volume_name = volume_name_or_id_str

      try:
         volume_cert_version = int( volume_cert_version_str )
      except:
         response_end( self, 400, "Invalid Request", "text/plain" )
         return

      if volume_id is None:
         # look up volume 
         volume = Volume.Read( volume_name )
         if volume is None or volume.deleted:
             response_end( self, 404, "Not found", "text/plain" )
             return 

         volume_id = volume.volume_id
      
      volume_cert_bundle = VolumeCertBundle.Get( volume_id )
      if volume_cert_bundle is None:
         response_end( self, 404, "Not found", "text/plain" )
         return 
      
      # verify that it matches 
      cert_bundle = VolumeCertBundle.Load( volume_cert_bundle )
      
      if cert_bundle.mtime_sec != volume_cert_version:
         
         # send gateway the URL to the latest cert bundle
         hdr = "%s/CERTBUNDLE/%s/%s" % (MS_URL, volume_name_or_id_str, cert_bundle.mtime_sec)
         
         self.response.headers['Location'] = hdr
         response_end( self, 302, "Location: %s" % hdr, "text/plain" )
         return
      
      response_end( self, 200, cert_bundle.SerializeToString(), "application/octet-stream" )
      return 
      
      
# ----------------------------------
class MSDriverRequestHandler( webapp2.RequestHandler ):
   """
   Serve gateway drivers, identified by hash.
   The driver is served as-is.
   NOTE: the caller should already have the gateway's certificate, which is signed 
   by the gateway user and has the driver's hash (thereby proving the authenticity 
   of the driver)
   """
   
   def get( self, driver_hash ):
      
      data = Gateway.ReadDriver( driver_hash )
      
      if data is not None:
         response_end( self, 200, data, "application/octet-stream" )
      
      else:
         response_user_error( self, 404, "No such driver" )
      
      return 
   

# ----------------------------------
class MSPubkeyHandler( webapp2.RequestHandler ):
   """
   Serve the MS's public key
   """
   
   def get( self ):
      
      response_end( self, 200, SYNDICATE_PUBKEY, "text/plain" )
      return

# ----------------------------------
class MSFileHandler(webapp2.RequestHandler):
   
   """
   File metadata handler for the Volume (calls the File Metadata API).
   It will create, read, update, and delete metadata entries (files or directories), as well as get, update, list, and delete extended attributes.
   It does so via the GET and POST handlers only--GET for accessing data (read, get, list, resolve), and POST for mutating data (create, update, delete)
   """
   
   cgi_args = {
      "LISTDIR": {
         "page_id":        lambda arg: int(arg),
         "lug":            lambda arg: int(arg)
       }
   }
   
   # these return a serialized response
   get_api_calls = {
      "FETCHXATTRS":    lambda gateway, volume, file_id, args, kw: file_xattr_fetchxattrs( gateway, volume, file_id, *args, **kw ), # args == []
      "GETATTR":        lambda gateway, volume, file_id, args, kw: file_getattr( gateway, volume, file_id, *args, **kw ),           # args == [file_version_str, write_nonce]
      "GETCHILD":       lambda gateway, volume, file_id, args, kw: file_getchild( gateway, volume, file_id, *args, **kw ),          # args == [name]
      "LISTDIR":        lambda gateway, volume, file_id, args, kw: file_listdir( gateway, volume, file_id, *args, **kw ),           # args == [], kw={page_id, lug}
      "VACUUM":         lambda gateway, volume, file_id, args, kw: file_vacuum_log_peek( gateway, volume, file_id, *args, **kw )    # args == []
   }
   
   get_benchmark_headers = {
      "FETCHXATTRS":            "X-Fetchxattr-Time",
      "GETATTR":                "X-Getattr-Time",
      "GETCHILD":               "X-Getchild-Time",
      "LISTDIR":                "X-Listdir-Time",
      "VACUUM":                 "X-Vacuum-Time"
   }
   
   # ensure that the posted data has all of the requisite optional fields
   # return (boolean validation check, failure status)
   post_validators = {
      ms_pb2.ms_request.CREATE:          lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_METADATA), 403),
      ms_pb2.ms_request.CREATE_ASYNC:    lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_METADATA), 403),
      ms_pb2.ms_request.UPDATE:          lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA ), 403),
      ms_pb2.ms_request.UPDATE_ASYNC:    lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA ), 403),
      ms_pb2.ms_request.DELETE:          lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA ), 403),
      ms_pb2.ms_request.DELETE_ASYNC:    lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA ), 403),
      ms_pb2.ms_request.CHCOORD:         lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_COORDINATE ), 403),
      ms_pb2.ms_request.RENAME:          lambda gateway, update: (update.HasField("dest"), 400),
      ms_pb2.ms_request.PUTXATTR:        lambda gateway, update: (update.HasField("xattr_name") and update.HasField("xattr_value") and update.HasField("xattr_nonce") and update.HasField("xattr_hash"), 400),
      ms_pb2.ms_request.REMOVEXATTR:     lambda gateway, update: (update.HasField("xattr_name"), 400),
      ms_pb2.ms_request.VACUUM:          lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_COORDINATE ), 403),
      ms_pb2.ms_request.VACUUMAPPEND:    lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_COORDINATE ), 403)
   }
   
   # Map update values onto handlers
   post_api_calls = {
      ms_pb2.ms_request.CREATE:          lambda reply, gateway, volume, update: file_create( reply, gateway, volume, update ),
      ms_pb2.ms_request.CREATE_ASYNC:    lambda reply, gateway, volume, update: file_create_async( reply, gateway, volume, update ),
      ms_pb2.ms_request.UPDATE:          lambda reply, gateway, volume, update: file_update( reply, gateway, volume, update ),
      ms_pb2.ms_request.UPDATE_ASYNC:    lambda reply, gateway, volume, update: file_update_async( reply, gateway, volume, update ),
      ms_pb2.ms_request.DELETE:          lambda reply, gateway, volume, update: file_delete( reply, gateway, volume, update ),
      ms_pb2.ms_request.DELETE_ASYNC:    lambda reply, gateway, volume, update: file_delete_async( reply, gateway, volume, update ),
      ms_pb2.ms_request.RENAME:          lambda reply, gateway, volume, update: file_rename( reply, gateway, volume, update ),
      ms_pb2.ms_request.CHCOORD:         lambda reply, gateway, volume, update: file_chcoord( reply, gateway, volume, update ),
      ms_pb2.ms_request.PUTXATTR:        lambda reply, gateway, volume, update: file_xattr_putxattr( reply, gateway, volume, update ),
      ms_pb2.ms_request.REMOVEXATTR:     lambda reply, gateway, volume, update: file_xattr_removexattr( reply, gateway, volume, update ),
      ms_pb2.ms_request.VACUUM:          lambda reply, gateway, volume, update: file_vacuum_log_remove( reply, gateway, volume, update ),
      ms_pb2.ms_request.VACUUMAPPEND:    lambda reply, gateway, volume, update: file_vacuum_log_append( reply, gateway, volume, update )
   }
   
   
   # Map update values onto benchmark headers
   post_benchmark_headers = {
      ms_pb2.ms_request.CREATE:          "X-Create-Times",
      ms_pb2.ms_request.CREATE_ASYNC:    "X-Create-Async-Times",
      ms_pb2.ms_request.UPDATE:          "X-Update-Times",
      ms_pb2.ms_request.UPDATE_ASYNC:    "X-Update-Async-Times",
      ms_pb2.ms_request.DELETE:          "X-Delete-Times",
      ms_pb2.ms_request.DELETE_ASYNC:    "X-Delete-Async-Times",
      ms_pb2.ms_request.CHCOORD:         "X-Chcoord-Times",
      ms_pb2.ms_request.RENAME:          "X-Rename-Times",
      ms_pb2.ms_request.PUTXATTR:        "X-Putxattr-Times",
      ms_pb2.ms_request.REMOVEXATTR:     "X-Removexattr-Times",
      ms_pb2.ms_request.VACUUM:          "X-Vacuum-Times",
      ms_pb2.ms_request.VACUUMAPPEND:    "X-Vacuum-Append-Times",
   }
   
   @classmethod
   def parse_cgi( cls, oper, request, parsers ):
      """
      parse CGI arguments with a dict of parser functions.
      Return (200, args) on success
      Return (not 200, None) on error
      """
      
      # parse CGI args
      kw = {}
      
      # find the parser for this operation 
      parser = parsers.get(oper, None)
      
      if parser is not None:
         
         for cgi_arg in parser.keys():
            
            cgi_val = request.get( cgi_arg )
            if cgi_val is not None and len(cgi_val) > 0:
               
               try:
                  cgi_val = parser[cgi_arg]( cgi_val )
               except:
                  logging.error("Invalid CGI argument '%s' (= '%s')" % (cgi_arg, cgi_val))
                  return (400, None)
               
               kw[cgi_arg] = cgi_val
            
      return (200, kw)
            
   
   @storagetypes.toplevel 
   def get( self, operation, volume_id_str, volume_version_str, cert_version_str, file_id_str, *args ):

      # valid operation?
      if operation not in MSFileHandler.get_api_calls.keys():
         logging.error("Unrecognized operation '%s'" % operation)
         response_user_error( self, 401 )
         return 
      
      # valid file ID?
      file_id = -1
      try:
         file_id = MSEntry.unserialize_id( int( file_id_str, 16 ) )
      except:
         response_user_error( self, 400 )
         return
      
      # valid volume ID?
      volume_id = -1 
      try:
         volume_id = int( volume_id_str )
      except:
         response_user_error( self, 400 )
         return 
      
      # get gateway, volume, and timing...
      volume, gateway, status, response_timing = response_begin( self, volume_id )
      
      if volume is None:
         response_user_error( self, status )
         return
      
      # reader allowed?
      allowed = file_read_auth( gateway, volume )
      if not allowed:
         response_user_error( self, 403 )
         return 
      
      # reader has fresh volume cert?
      if volume.version != int(volume_version_str):
         # stale
         log.error( "volume.version = %s, volume_version_str = %s" % (volume.version, volume_version_str) )
         response_user_error( self, 410, "Stale volume version" )
         return 
      
      # reader has fresh cert bundle?
      if volume.cert_bundle is not None and volume.cert_bundle.mtime_sec != int(cert_version_str):
         # stale 
         log.error( "volume.cert_bundle.mtime_sec = %s, cert_version_str = %s" % (volume.cert_bundle.mtime_sec, cert_version_str))
         response_user_error( self, 410, "Stale gateway version" )
         return
         
      # parse CGI arguments 
      status, kw = MSFileHandler.parse_cgi( operation, self.request, self.cgi_args )
      
      if status != 200:
         response_user_error( self, status )
         return 
         
      benchmark_header = MSFileHandler.get_benchmark_headers[ operation ]
      api_call = MSFileHandler.get_api_calls[ operation ]
      
      timing = {}
      
      # run and benchmark the operation
      try:
         data = benchmark( benchmark_header, timing, lambda: api_call( gateway, volume, file_id, args, kw ) )
         
      except storagetypes.RequestDeadlineExceededError, de:
         response_user_error( self, 503 )
         return
      
      except Exception, e:
         logging.exception(e)
         response_user_error( self, 500 )
         return
      
      # finish the reply 
      timing_headers = benchmark_headers( timing )
      timing_headers.update( response_timing )
      
      response_end( self, 200, data, "application/octet-stream", timing_headers )
      return
   
   
   
   @storagetypes.toplevel
   def post(self, volume_id_str, volume_version_str, cert_version_str ):

      file_post_start = storagetypes.get_time()
      
      volume_id = None
      try:
         volume_id = int(volume_id_str)
      except Exception, e:
         response_user_error( self, 400 )
         return
                             
      update_set = file_update_parse( self )
      if update_set == None:
         # malformed data
         response_user_error( self, 202, "%s\n" % (-errno.EINVAL) )
         return
      
      # begin the response
      volume, gateway, status, response_timing = response_begin( self, volume_id )
      if volume == None:
         response_user_error( self, 404, "No such volume")
         return

      if status != 200:
         response_user_error( self, status )
         return 
      
      allowed = file_update_auth( gateway )
      if not allowed:
         logging.error("Failed to authenticate")
         response_user_error( self, 403 )
         return

      # verify the message integrity and authenticity
      if not gateway.verify_message( update_set ):
         # authentication failure
         logging.error("Signature verification failed")
         response_user_error( self, 403, "Signature verification failed")
         return
      
      # writer has fresh cert bundle?
      if volume.version != int(volume_version_str):
         # stale
         logging.error("stale volume: volume.version = %s, volume_version_str = %s" % (volume.version, volume_version_str))
         response_user_error( self, 410 )
         return 
      
      # writer has fresh cert bundle?
      if volume.cert_bundle is not None and volume.cert_bundle.mtime_sec != int(cert_version_str):
         # stale 
         log.error( "volume.cert_bundle.mtime_sec = %s, cert_version_str = %s" % (volume.cert_bundle.mtime_sec, cert_version_str))
         response_user_error( self, 410, "Stale gateway version" )
         return
      
      # TODO: rate-limit
      
      # populate the reply
      reply = file_update_init_response( volume )
      status = 200
      
      # validate requests before processing them 
      for request in update_set.requests:
         
         if request.type not in MSFileHandler.post_validators.keys():
            logging.error("Unrecognized request %s" % request.type)
            response_user_error( self, 501 )
            return 
         
         valid, failure_status = MSFileHandler.post_validators[request.type]( gateway, request )
         if not valid:
            logging.error("Failed to validate request")
            response_user_error( self, failure_status, "Argument validation failed" )
            return 
      
      timing = {}
      
      # carry out the operation(s), and count them
      num_processed = 0
      types = {}
      for request in update_set.requests:

         if not types.has_key(request.type):
            types[request.type] = 0
            
         types[request.type] += 1
         
         # these are guaranteed to be non-None...
         api_call = MSFileHandler.post_api_calls.get( request.type, None )
         benchmark_header = MSFileHandler.post_benchmark_headers.get( request.type, None )
         
         # run the API call, but benchmark it too
         try:
            rc = benchmark( benchmark_header, timing, lambda: api_call( reply, gateway, volume, request ) )
            reply.errors.append( rc )
            
            num_processed += 1
            
         except storagetypes.RequestDeadlineExceededError, de:
            # quickly now...
            response_user_error( self, 503 )
            return
            
         except Exception, e:
            logging.exception(e)
            reply.error = -errno.EREMOTEIO
            break
      
      logging.info("Processed %s requests (%s)" % (num_processed, types))
      
      # generate the response
      reply_str = file_update_complete_response( volume, reply )
      
      # turn our timing data into headers
      timing_headers = benchmark_headers( timing )
      timing_headers.update( response_timing )
      
      for h, v in timing_headers.items():
         logging.info( "%s: %s" % (h, v) )
         
      response_end( self, status, reply_str, "application/octet-stream", timing_headers )
      
      return
   

# ----------------------------------
class MSJSONRPCHandler(webapp2.RequestHandler):
   """
   JSON RPC request handler.
   Authenticates via OpenID or via public-key signatures.
   Takes an 'operation' that indicates either the OpenID step, or that the caller expects public-key authorization
   """
   
   def handle( self ):
      
      # pass on to JSON RPC server
      json_text = self.request.body
      json_data = None
      
      try:
         json_data = json.loads( json_text )
      except:
         response_user_error( self, 400 )
         return 
     
      # look up username
      syndicate_data = jsonrpc.extract_syndicate_json( json_data, jsonrpc.VERSION )
      if syndicate_data is None:
         response_user_error( self, 400 )
         return 
      
      if 'username' not in syndicate_data:
         logging.debug("Missing 'username' from syndicate JSONRPC data")
         response_user_error( self, 400 )
         return 
      
      username = syndicate_data['username']
      server = jsonrpc.Server( api.API(), jsonrpc.VERSION, signer=api.API.signer, verifier=api.API.pubkey_verifier )
      
      caller_uuids = server.get_call_uuids( json_text )
      if caller_uuids is None:
         # invalid request 
         server.error(None, jsonrpc.SERVER_ERROR_INVALID_REQUEST, response=self.response )
         return 
      
      # verify that none of these UUIDs exist 
      stored_uuids = generaluuid.get_uuids( caller_uuids, "jsonrpc" )
      for stored_uuid in stored_uuids:
         if stored_uuid is not None:
            server.error(stored_uuid, jsonrpc.SERVER_ERROR_INVALID_REQUEST, response=self.response )
            return 
      
      # NOTE: This writes the response
      result = server.handle( json_text, response=self.response, username=username )
      generaluuid.put_uuids( caller_uuids, "jsonrpc" )
         
      return
      
   def post( self ):
      return self.handle()
   
   def get( self ):
      return self.handle()
   
