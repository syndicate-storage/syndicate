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
from MS.methods.register import *
from MS.methods.response import *
from MS.methods.file import *

import protobufs.ms_pb2 as ms_pb2
import protobufs.serialization_pb2 as serialization_pb2

from storage import storage
import storage.storagetypes as storagetypes

import common.api as api
from entry import MSEntry, MSEntryXAttr
from volume import Volume
from gateway import *
from common.msconfig import *
from common.admin_info import *

import errno
import logging
import random
import os
import base64
import urllib
import time
import cgi
import datetime
import common.jsonrpc as jsonrpc

from openid.gaeopenid import GAEOpenIDRequestHandler
from openid.consumer import consumer
import openid.oidutil
   

# ----------------------------------
class MSVolumeRequestHandler(webapp2.RequestHandler):
   """
   Volume metadata request handler.
   """

   def get( self, volume_name_or_id_str ):
   
      # get the gateway, but we'll check for ourselves whether or not the gateway needs authentication
      gateway, volume, timing = response_begin( self, volume_name_or_id_str, fail_if_no_auth_header=False )
      if volume == None:
         return

      if volume.need_gateway_auth() and gateway == None:
         response_user_error( self, 403 )
         return 
   
      root = storage.get_volume_root( volume )
      
      if root == None:
         response_user_error( self, 404 )
         return

      # request for volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      
      root.protobuf( volume_metadata.root )
      volume.protobuf( volume_metadata )
      
      data = volume_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return



# ----------------------------------
class MSCertManifestRequestHandler( webapp2.RequestHandler ):
   """
   Certificate bundle manifest request handler
   """
   
   def get( self, volume_id_str, volume_cert_version_str ):
      volume_cert_version = 0
      try:
         volume_cert_version = int( volume_cert_version_str )
      except:
         response_end( self, 400, "Invalid Request", "text/plain" )
         return
      
      # get the Volume
      volume, status, _ = response_load_volume( self, volume_id_str )

      if status != 200:
         return
      
      # do we want to include a cert for a particular gateway?
      include_cert = None
      include_cert_qs = self.request.get("include_cert")
      
      if include_cert_qs is not None:
         try:
            include_cert = int(include_cert_qs)
         except:
            pass
      
      # check version
      if volume_cert_version != volume.cert_version:
         hdr = "%s/CERT/%s/manifest.%s" % (MS_URL, volume_id_str, volume.cert_version)
         self.response.headers['Location'] = hdr
         response_end( self, 302, "Location: %s" % hdr, "text/plain" )
         return

      # build the manifest
      manifest = serialization_pb2.ManifestMsg()
      
      volume.protobuf_gateway_cert_manifest( manifest, include_cert=include_cert )
      
      data = manifest.SerializeToString()
      
      response_end( self, 200, data, "application/octet-stream" )
      return
      
      
# ----------------------------------
class MSCertRequestHandler( webapp2.RequestHandler ):
   """
   Certificate bundle request handler.
   """
   
   def get( self, volume_id_str, volume_cert_version_str, gateway_type_str, gateway_name_or_id, gateway_cert_version_str ):
      volume_cert_version = 0
      gateway_cert_version = 0
      
      try:
         gateway_cert_version = int( gateway_cert_version_str )
         volume_cert_version = int( volume_cert_version_str )
      except:
         response_end( self, 400, "Invalid Request", "text/plain" )
         return
      
      
      # get the Volume
      volume, status, _ = response_load_volume( self, volume_id_str )

      if status != 200 or volume == None:
         return
      
      # get the gateway
      if gateway_type_str not in ["UG", "RG", "AG"]:
         logging.error("Invalid gateway type '%s'" % gateway_type_str )
         response_user_error( self, 401 )
         return
         
      gateway = storage.read_gateway( gateway_name_or_id )
      if gateway == None:
         logging.error("No such Gateway named %s" % (gateway_name_or_id))
         response_user_error( self, 404 )
         return
      
      for type_str, type_id in zip( ["UG", "RG", "AG"], [GATEWAY_TYPE_UG, GATEWAY_TYPE_RG, GATEWAY_TYPE_AG] ):
         if gateway_type_str == type_str and gateway.gateway_type != type_id:
            logging.error("No such %s named %s" % (gateway_type_str, gateway_name_or_id))
            response_user_error( self, 404 )
            return
      
      # request only the right version
      if volume_cert_version != volume.cert_version or gateway_cert_version != gateway.cert_version:
         hdr = "%s/CERT/%s/%s/%s/%s/%s" % (MS_URL, volume_id_str, volume.cert_version, gateway_type_str, gateway_name_or_id, gateway.cert_version)
         self.response.headers['Location'] = hdr
         response_end( self, 302, "Location: %s" % hdr, "text/plain" )
         return
      
      # generate the certificate
      gateway_cert = ms_pb2.ms_gateway_cert()
      
      volume.protobuf_gateway_cert( gateway_cert, gateway, need_closure=True )
      
      data = gateway_cert.SerializeToString()
      
      response_end( self, 200, data, "application/octet-stream" )
      return
   
   
# ----------------------------------
class MSUserRequestHandler( webapp2.RequestHandler ):
   """
   User certificate request handler.
   GET returns a user certificate, as JSON
   """
   
   def get( self, user_email ):
      # get the user
      try:
         user = storage.read_user( user_email )
      except:
         response_end( self, 404, "No such user", "text/plain")
         return
      
      if user == None:
         response_end( self, 404, "No such user", "text/plain")
         
      user_cert_dict = user.makeCert()
      
      user_cert_txt = jsonrpc.json_stable_serialize( user_cert_dict )
      
      response_end( self, 200, user_cert_txt, "application/json" )
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
class MSVolumeOwnerRequestHandler( webapp2.RequestHandler ):
   """
   Get the certificate of the user that owns a particular Volume.
   GET returns a user certificate, as JSON
   """
   
   def get( self, volume_name ):
      # get the volume
      try:
         volume = storage.read_volume( volume_name )
      except:
         response_end( self, 404, "No such volume", "text/plain")
         return
      
      # get the owner
      try:
         user = storage.read_user( volume.owner_id )
      except:
         response_end( self, 404, "No such user", "text/plain")
         return 
      
      if user == None:
         response_end( self, 404, "No such user", "text/plain")
         
      user_cert_dict = user.makeCert()
      user_cert_txt = json.dumps( user_cert_dict )
      response_end( self, 200, user_cert_txt, "application/json" )
      return
   
      
# ----------------------------------
class MSOpenIDRegisterRequestHandler( GAEOpenIDRequestHandler ):
   """
   Generate a session certificate from a SyndicateUser account for a gateway.
   """

   OPENID_RP_REDIRECT_METHOD = "POST"     # POST to us for authentication

   def protobuf_volume( self, volume_metadata, volume, root=None ):
      if root != None:
         root.protobuf( volume_metadata.root )
         
      volume.protobuf( volume_metadata )

      return
      
   get = None
   
   def post( self, gateway_type_str, gateway_name, username, operation ):
      # process a registration request...
      
      self.load_query()
      session = self.getSession( expiration_ts=(time.time() + 60))  # expire in 1 minute, if new session
      self.setSessionCookie(session)

      status, gateway, user = register_load_objects( gateway_type_str, gateway_name, username )
      
      if gateway == None or user == None or status != 200:
         
         if gateway is None:
            logging.error("No such %s '%s'" % (gateway_type_str, gateway_name))
         
         if user is None:
            logging.error("No such user '%s'" % (username))
            
         response_user_error( self, status )
         return

      # this SyndicateUser must own this Gateway
      if user.owner_id != gateway.owner_id:
         response_user_error( self, 403 )
         return

      if operation == "begin":
         
         # begin the OpenID authentication
         try:
            oid_request, rc = self.begin_openid_auth()
         except consumer.DiscoveryFailure, exc:

            fetch_error_string = 'Error in discovery: %s' % (cgi.escape(str(exc[0])))

            response_server_error( self, 500, fetch_error_string )
            return

         if rc != 0:
            response_server_error( self, 500, "OpenID error %s" % rc )
            return
         
         # reply with the redirect URL
         return_to = self.buildURL( "/REGISTER/%s/%s/%s/complete" % (gateway_type_str, gateway_name, username) )

         data = register_make_openid_reply( oid_request, self.OPENID_RP_REDIRECT_METHOD, return_to, self.query )
      
         session.save()         # it's okay to save this to memcache only
         
         response_end( self, 200, data, "application/octet-stream", None )
         return

      elif operation == "complete":
         
         # complete the authentication
         info, _, _ = self.complete_openid_auth()
         if info.status != consumer.SUCCESS:
            # failed
            response_user_error( self, 403 )
            return
         
         status, data = register_complete( gateway )
         if status != 200:
            # failed 
            logging.error("Register complete for %s %s failed, status = %s" % (gateway_type_str, gateway_name, status))
            
            session.terminate()
            
            response_user_error( self, status )
            
            return
         
         # clear this OpenID session, since we're registered
         session.terminate()
         
         response_end( self, 200, data, "application/octet-stream", None )
         
         return


# ----------------------------------
class MSPublicKeyRegisterRequestHandler( webapp2.RequestHandler ):
   """
   Register a gateway using its owner's signature on the request.
   """
   def post( self ):
      # get the request 
      reg_req = register_request_parse( self )
      if reg_req is None:
         logging.error("Failed to parse registration request")
         response_user_error( self, 400 )
         return 
      
      # verify the request, looking up the user in the process
      user = register_request_verify( reg_req )
      if user is None:
         # user doesn't exist (but don't tell the caller that)
         logging.error("No such user")
         response_user_error( self, 400 )
         return 
      
      if user == False:
         # signature mismatch 
         logging.error("Signature mismatch")
         response_user_error( self, 400 )
         return 
      
      # authenticated! get the gateway 
      gateway_type_str = GATEWAY_TYPE_TO_STR.get( reg_req.gateway_type, None )
      if gateway_type_str is None:
         # invalid request 
         logging.error("Invalid gateway type %s" % reg_req.gateway_type )
         response_user_error( self, 400 )
         return 
      
      status, gateway = register_load_gateway( gateway_type_str, reg_req.gateway_name )
      if status != 200 or gateway is None:
         # failed to load gateway 
         response_user_error( self, status )
         return 

      # does this user own the gateway?
      if gateway.owner_id != user.owner_id:
         # not ours
         response_user_error( self, 403 )
         return
      
      # finalize the registration
      status, data = register_complete( gateway )
      if status != 200:
         response_user_error( self, 200 )
         return 
      
      # success!
      response_end( self, 200, data, "application/octet-stream", None )
      
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
      "GETXATTR":       lambda gateway, volume, file_id, args, kw: file_xattr_getxattr( gateway, volume, file_id, *args, **kw ),    # args == [xattr_name]
      "LISTXATTR":      lambda gateway, volume, file_id, args, kw: file_xattr_listxattr( gateway, volume, file_id, *args, **kw ),   # args == []
      "GETATTR":        lambda gateway, volume, file_id, args, kw: file_getattr( gateway, volume, file_id, *args, **kw ),           # args == [file_version_str, write_nonce]
      "GETCHILD":       lambda gateway, volume, file_id, args, kw: file_getchild( gateway, volume, file_id, *args, **kw ),          # args == [name]
      "LISTDIR":        lambda gateway, volume, file_id, args, kw: file_listdir( gateway, volume, file_id, *args, **kw ),           # args == []
      "VACUUM":         lambda gateway, volume, file_id, args, kw: file_vacuum_log_peek( gateway, volume, file_id, *args, **kw )    # args == []
   }
   
   get_benchmark_headers = {
      "GETXATTR":               "X-Getxattr-Time",
      "LISTXATTR":              "X-Listxattr-Time",
      "GETATTR":                "X-Getattr-Time",
      "GETCHILD":               "X-Getchild-Time",
      "LISTDIR":                "X-Listdir-Time",
      "VACUUM":                 "X-Vacuum-Time"
   }
   
   # ensure that the posted data has all of the requisite optional fields
   # return (boolean validation check, failure status)
   post_validators = {
      ms_pb2.ms_update.CREATE:          lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_UG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA )) or 
                                                                 (gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),
      
      ms_pb2.ms_update.CREATE_ASYNC:    lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),   # AG only
      
      ms_pb2.ms_update.UPDATE:          lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_UG and gateway.check_caps( GATEWAY_CAP_WRITE_DATA )) or 
                                                                 (gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),
      
      ms_pb2.ms_update.UPDATE_ASYNC:    lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),   # AG only
      
      ms_pb2.ms_update.DELETE:          lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_UG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA )) or 
                                                                 (gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),
      
      ms_pb2.ms_update.DELETE_ASYNC:    lambda gateway, update: ((gateway.gateway_type == GATEWAY_TYPE_AG and gateway.check_caps( GATEWAY_CAP_WRITE_METADATA)), 403),   # AG only
      
      ms_pb2.ms_update.CHCOORD:         lambda gateway, update: (gateway.check_caps( GATEWAY_CAP_COORDINATE ), 403),
      ms_pb2.ms_update.RENAME:          lambda gateway, update: (update.HasField("dest"), 400),
      ms_pb2.ms_update.SETXATTR:        lambda gateway, update: (update.HasField("xattr_name") and update.HasField("xattr_value") and update.HasField("xattr_mode") and update.HasField("xattr_owner"), 400),
      ms_pb2.ms_update.REMOVEXATTR:     lambda gateway, update: (update.HasField("xattr_name"), 400),
      ms_pb2.ms_update.CHMODXATTR:      lambda gateway, update: (update.HasField("xattr_name") and update.HasField("xattr_mode"), 400),
      ms_pb2.ms_update.CHOWNXATTR:      lambda gateway, update: (update.HasField("xattr_name") and update.HasField("xattr_owner"), 400),
      ms_pb2.ms_update.VACUUM:          lambda gateway, update: (True, 200)
   }
   
   # Map update values onto handlers
   post_api_calls = {
      ms_pb2.ms_update.CREATE:          lambda reply, gateway, volume, update: file_create( reply, gateway, volume, update ),
      ms_pb2.ms_update.CREATE_ASYNC:    lambda reply, gateway, volume, update: file_create_async( reply, gateway, volume, update ),
      ms_pb2.ms_update.UPDATE:          lambda reply, gateway, volume, update: file_update( reply, gateway, volume, update ),
      ms_pb2.ms_update.UPDATE_ASYNC:    lambda reply, gateway, volume, update: file_update_async( reply, gateway, volume, update ),
      ms_pb2.ms_update.DELETE:          lambda reply, gateway, volume, update: file_delete( reply, gateway, volume, update ),
      ms_pb2.ms_update.DELETE_ASYNC:    lambda reply, gateway, volume, update: file_delete_async( reply, gateway, volume, update ),
      ms_pb2.ms_update.RENAME:          lambda reply, gateway, volume, update: file_rename( reply, gateway, volume, update ),
      ms_pb2.ms_update.CHCOORD:         lambda reply, gateway, volume, update: file_chcoord( reply, gateway, volume, update ),
      ms_pb2.ms_update.SETXATTR:        lambda reply, gateway, volume, update: file_xattr_setxattr( reply, gateway, volume, update ),
      ms_pb2.ms_update.REMOVEXATTR:     lambda reply, gateway, volume, update: file_xattr_removexattr( reply, gateway, volume, update ),
      ms_pb2.ms_update.CHMODXATTR:      lambda reply, gateway, volume, update: file_xattr_chmodxattr( reply, gateway, volume, update ),
      ms_pb2.ms_update.CHOWNXATTR:      lambda reply, gateway, volume, update: file_xattr_chownxattr( reply, gateway, volume, update ),
      ms_pb2.ms_update.VACUUM:          lambda reply, gateway, volume, update: file_vacuum_log_remove( reply, gateway, volume, update )
   }
   
   
   # Map update values onto benchmark headers
   post_benchmark_headers = {
      ms_pb2.ms_update.CREATE:          "X-Create-Times",
      ms_pb2.ms_update.CREATE_ASYNC:    "X-Create-Async-Times",
      ms_pb2.ms_update.UPDATE:          "X-Update-Times",
      ms_pb2.ms_update.UPDATE_ASYNC:    "X-Update-Async-Times",
      ms_pb2.ms_update.DELETE:          "X-Delete-Times",
      ms_pb2.ms_update.DELETE_ASYNC:    "X-Delete-Async-Times",
      ms_pb2.ms_update.CHCOORD:         "X-Chcoord-Times",
      ms_pb2.ms_update.RENAME:          "X-Rename-Times",
      ms_pb2.ms_update.SETXATTR:        "X-Setxattr-Times",
      ms_pb2.ms_update.REMOVEXATTR:     "X-Removexattr-Times",
      ms_pb2.ms_update.CHMODXATTR:      "X-Chmodxattr-Times",
      ms_pb2.ms_update.CHOWNXATTR:      "X-Chownxattr-Times",
      ms_pb2.ms_update.VACUUM:          "X-Vacuum-Times"
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
                  log.error("Invalid CGI argument '%s' (= '%s')" % (cgi_arg, cgi_val))
                  return (400, None)
               
               kw[cgi_arg] = cgi_val
            
      return (200, kw)
            
   
   @storagetypes.toplevel 
   def get( self, operation, volume_id_str, file_id_str, *args ):

      # valid operation?
      if operation not in MSFileHandler.get_api_calls.keys():
         response_user_error( self, 401 )
         return 
      
      # valid file ID?
      file_id = -1
      try:
         file_id = MSEntry.unserialize_id( int( file_id_str, 16 ) )
      except:
         response_user_error( self, 400 )
         return
      
      # valid gateway and volume?
      gateway, volume, response_timing = response_begin( self, volume_id_str, fail_if_no_auth_header=False )
      if volume == None:
         return
      
      # reader allowed?
      allowed = file_read_auth( gateway, volume )
      if not allowed:
         response_user_error( self, 403 )
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
   def post(self, volume_id_str ):

      file_post_start = storagetypes.get_time()
      
      update_set = file_update_parse( self )
      if update_set == None:
         # malformed data
         response_user_error( self, 202, "%s\n" % (-errno.EINVAL) )
         return
      
      # begin the response
      gateway, volume, response_timing = response_begin( self, volume_id_str )
      if volume == None:
         return

      allowed = file_update_auth( gateway, volume )
      if not allowed:
         log.error("Failed to authenticate")
         response_user_error( self, 403 )
         return

      # verify the message integrity and authenticity
      if not gateway.verify_message( update_set ):
         # authentication failure
         response_user_error( self, 401, "Signature verification failed")
         return
      
      # TODO: rate-limit
      
      # populate the reply
      reply = file_update_init_response( volume )
      status = 200
      
      # validate requests before processing them 
      for update in update_set.updates:
         
         if update.type not in MSFileHandler.post_validators.keys():
            logging.error("Unrecognized update %s" % update.type)
            response_user_error( self, 501 )
            return 
         
         
         valid, failure_status = MSFileHandler.post_validators[update.type]( gateway, update )
         if not valid:
            log.error("Failed to validate update")
            response_user_error( self, failure_status, "Argument validation failed" )
            return 
      
      timing = {}
      
      # carry out the operation(s), and count them
      num_processed = 0
      types = {}
      for update in update_set.updates:

         if not types.has_key(update.type):
            types[update.type] = 0
            
         types[update.type] += 1
         
         # these are guaranteed to be non-None...
         api_call = MSFileHandler.post_api_calls.get( update.type, None )
         benchmark_header = MSFileHandler.post_benchmark_headers.get( update.type, None )
         
         # run the API call, but benchmark it too
         try:
            rc = benchmark( benchmark_header, timing, lambda: api_call( reply, gateway, volume, update ) )
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
      
      log.info("Processed %s requests (%s)" % (num_processed, types))
      
      # generate the response
      reply_str = file_update_complete_response( volume, reply )
      
      # turn our timing data into headers
      timing_headers = benchmark_headers( timing )
      timing_headers.update( response_timing )
      
      response_end( self, status, reply_str, "application/octet-stream", timing_headers )
      
      return



# ----------------------------------
class MSJSONRPCHandler(GAEOpenIDRequestHandler):
   """
   JSON RPC request handler.
   Authenticates via OpenID or via public-key signatures.
   Takes an 'operation' that indicates either the OpenID step, or that the caller expects public-key authorization
   """
   
   API_AUTH_PUBKEY = "pubkey"
   API_AUTH_OPENID_BEGIN = "begin"
   API_AUTH_OPENID_COMPLETE = "complete"
   API_AUTH_OPENID = "openid"
   API_AUTH_SESSION = "session"
   
   OPENID_RP_REDIRECT_METHOD = "POST"     # POST to us for authentication
   
   def call_method( self, auth_method, username=None ):
      # pass on to JSON RPC server
      json_text = self.request.body
      api_call_verifier = None
      
      if auth_method == self.API_AUTH_PUBKEY:
         api_call_verifier = api.API.pubkey_verifier            # performs crypto verification
         
      elif auth_method == self.API_AUTH_OPENID:
         api_call_verifier = api.API.openid_verifier            # ensures username in session matches username in RPC
         
      server = jsonrpc.Server( api.API(), JSON_MS_API_VERSION, signer=api.API.signer, verifier=api_call_verifier )
      
      # NOTE: This writes the response
      result = server.handle( json_text, response=self.response, username=username )
   
      # TODO: save the UUID of this request, to prevent replays
      uuids = server.get_result_uuids( result )
   
   
   def handle( self, action ):
      self.load_query()
      
      if action is None:
         # in a session.  Check the session cookie to authenticate.
         session = self.getSession( expiration_ts=int(time.time() + 60) )      # expire in 1 minute
         if not session.get("authenticated"):
            # unauthorized
            logging.error("Unauthorized session")
            response_user_error( self, 403 )
            return 
         
         username = session.get("username")
         if username is None:
            # no username given
            logging.error("No username given")
            response_user_error( self, 403 )
            return 
         
         self.call_method( self.API_AUTH_OPENID, username=username )
         return
      
      elif action == self.API_AUTH_PUBKEY:
         self.call_method( action )
         
         
      elif action == self.API_AUTH_OPENID_BEGIN:
         # start an API call, using OpenID to authenticate the caller
         
         # get the username
         username = self.query.get( OPENID_POST_USERNAME )
         if username is None:
            logging.error("No username given")
            response_user_error( self, 401 )
            return
         
         # get the user
         user = storage.read_user( username )
         if user == None:
            logging.error("No such user '%s'" % username)
            response_user_error( self, 401 )
            return

         # start a session
         session = self.getSession( expiration_ts=int(time.time() + 60) )      # expire in 1 minute
         
         # begin the OpenID authentication
         try:
            oid_request, rc = self.begin_openid_auth()
         except consumer.DiscoveryFailure, exc:

            fetch_error_string = 'Error in discovery: %s' % (cgi.escape(str(exc[0])))

            response_server_error( self, 500, fetch_error_string )
            return

         if rc != 0:
            response_server_error( self, 500, "OpenID error %s" % rc )
            return
         
         # reply with the redirect URL
         return_to = self.buildURL( "/API/%s" % (self.API_AUTH_OPENID_COMPLETE) )

         data = register_make_openid_reply( oid_request, self.OPENID_RP_REDIRECT_METHOD, return_to, self.query )
         
         # save this for the 'complete' operation
         session['username'] = username
         self.setSessionCookie(session)
         session.save( persist_even_if_using_cookie=True )
         
         response_end( self, 200, data, "application/octet-stream", None )
         return


      elif action == self.API_AUTH_OPENID_COMPLETE:
         # complete the authentication
         info, _, _ = self.complete_openid_auth()
         if info.status != consumer.SUCCESS:
            # failed
            response_user_error( self, 403 )
            return
         
         session = self.getSession( expiration_ts=int(time.time() + 60) )      # expire in 1 minute
         self.setSessionCookie(session)
         session.save( persist_even_if_using_cookie=True )
         
         response_end( self, 200, "OK", "text/plain", None )
         
         return
      
      
   def post( self, action=None ):
      return self.handle( action )
   
   def get( self, action=None ):
      return self.handle( action )
   
