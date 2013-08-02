#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import webapp2
import urlparse

import MS
from MS.methods.resolve import Resolve

import protobufs.ms_pb2 as ms_pb2

from storage import storage
import storage.storagetypes as storagetypes

from entry import MSEntry
from volume import Volume
from gateway import UserGateway, ReplicaGateway

import errno
import logging
import random
import os
import base64
import urllib
import time
import cgi
import datetime

from openid.gaeopenid import GAEOpenIDRequestHandler
from openid.consumer import consumer
import openid.oidutil

HTTP_MS_LASTMOD = "X-MS-LastMod"

def get_client_lastmod( headers ):
   lastmod = headers.get( HTTP_MS_LASTMOD )
   if lastmod == None:
      return None

   try:
      lastmod_f = float( lastmod )
      return lastmod_f
   except:
      return None

   

def read_basic_auth( headers ):
   basic_auth = headers.get("Authorization")
   if basic_auth == None:
      logging.info("no authorization header")
      return (None, None)

   username, password = '', ''
   try:
      user_info = base64.decodestring( basic_auth[6:] )
      username, password = user_info.split(":")
   except:
      logging.info("incomprehensible Authorization header: '%s'" % basic_auth )
      return (None, None)

   return username, password



def get_UG( ug_id ):
   if ug_id == None:
      # invalid header
      return (None, 403, None)

   ug_read_start = storagetypes.get_time()
   UG = UserGateway.Read( ug_id )
   ug_read_time = storagetypes.get_time() - ug_read_start
   return (UG, 200, ug_read_time)


def get_volume( volume_name_or_id ):

   volume_id = -1
   try:
      volume_id = int( volume_name_or_id )
   except:
      pass

   volume_read_start = storagetypes.get_time()

   volume = None
   if volume_id > 0:
      volume = storage.read_volume( volume_id )
   else:
      volume = storage.get_volume_by_name( volume_name_or_id )

   volume_read_time = storagetypes.get_time() - volume_read_start

   if volume == None:
      # no volume
      return (None, 404, None)

   if not volume.active:
      # inactive volume
      return (None, 503, None)

   return (volume, 200, volume_read_time)


def response_volume_error( request_handler, status ):

   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"
   
   if status == 404:
      # no volume
      request_handler.response.write("No such volume\n")

   elif status == 503:
      # inactive volume
      request_handler.response.write("Service Not Available\n")

   return
   

def response_server_error( request_handler, status, msg=None ):
   
   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"

   if status == 500:
      # server error
      if msg == None:
         msg = "Internal Server Error"
      request_handler.response.write( msg )

   return
   

def response_UG_error( request_handler, status, message=None ):

   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"

   if status == 400:
      if message == None:
         messsage = "Invalid Request\n"
      request_handler.response.write(message)
      
   elif status == 404:
      if message == None:
         messsage = "No such gateway\n"
      request_handler.response.write(message)
      
   elif status == 401:
      if message == None:
         message = "Authentication required\n"
      request_handler.response.write(message)

   elif status == 403:
      if message == None:
         message = "Authorization Failed\n"
      request_handler.response.write(message)

   return

response_user_error = response_UG_error



def response_begin( request_handler, volume_name_or_id ):
   
   timing = {}
   
   timing['request_start'] = storagetypes.get_time()

   volume, status, volume_read_time = get_volume( volume_name_or_id )

   if status != 200:
      response_volume_error( request_handler, status )
      return (None, None, None)
      
   # get the UG's credentials
   ug_id_str, password = read_basic_auth( request_handler.request.headers )

   if ug_id_str == None or password == None:
      response_UG_error( request_handler, 401 )
      return (None, None, None)

   ug_id = int( ug_id_str )
   
   # look up the requesting UG
   UG, status, ug_read_time = get_UG( ug_id )

   if status != 200:
      response_UG_error( request_handler, status )
      return (None, None, None)
      
   # make sure this UG is legit, if needed
   valid_UG = UG.authenticate_session( password )

   if not valid_UG:
      # invalid credentials
      logging.error("Invalid session credentials")
      response_UG_error( request_handler, 403 )
      return (None, None, None)

   # make sure this UG is allowed to access this Volume
   valid_UG = volume.is_UG_allowed( UG )
   if not valid_UG:
      # UG does not belong to this Volume
      logging.error("Not in this Volume")
      response_UG_error( request_handler, 403 )
      return (None, None, None)

   # if we're still here, we're good to go

   timing['X-Volume-Time'] = str(volume_read_time)
   timing['X-UG-Time'] = str(ug_read_time)
   
   return (UG, volume, timing)


def response_end( request_handler, status, data, content_type=None, timing=None ):
   if content_type == None:
      content_type = "application/octet-stream"

   if timing != None:
      request_total = storagetypes.get_time() - timing['request_start']
      timing['X-Total-Time'] = str(request_total)
      
      del timing['request_start']
      
      for (time_header, time) in timing.items():
         request_handler.response.headers[time_header] = time

   request_handler.response.headers['Content-Type'] = content_type
   request_handler.response.status = status
   request_handler.response.write( data )
   return
   
   
   

class MSVolumeRequestHandler(webapp2.RequestHandler):
   """
   Volume metadata request handler.
   """

   def get( self, volume_id_str ):
      UG, volume, timing = response_begin( self, volume_id_str )
      if UG == None or volume == None:
         return

      # request for volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      volume.protobuf( volume_metadata, UG )
      data = volume_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return



class MSUGRequestHandler( webapp2.RequestHandler ):
   """
   Get the list of (writeable) UGs in a Volume.
   """
   def get( self, volume_id_str ):
      UG, volume, timing = response_begin( self, volume_id_str )
      if UG == None or volume == None:
         return

      ug_metadata = ms_pb2.ms_volume_UGs()
      
      user_gateways = storage.list_user_gateways( {'UserGateway.volume_id ==' : volume.volume_id} )

      volume.protobuf_UGs( ug_metadata, user_gateways )
      
      data = ug_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSRGRequestHandler( webapp2.RequestHandler ):
   """
   Get the list of RGs in a Volume.
   """
   def get( self, volume_id_str ):
      UG, volume, timing = response_begin( self, volume_id_str )
      if UG == None or volume == None:
         return

      rg_metadata = ms_pb2.ms_volume_RGs()

      rgs = []
      
      if len(volume.rg_ids) > 0:
         rgs = storage.list_replica_gateways( {'ReplicaGateway.rg_id IN' : volume.rg_ids} )

      volume.protobuf_RGs( rg_metadata, rgs )

      data = rg_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSRegisterRequestHandler( GAEOpenIDRequestHandler ):
   """
   Generate a session certificate from a SyndicateUser account for a UG.
   """

   OPENID_PROVIDER_NAME = "VICCI"
   OPENID_PROVIDER_URL = "https://www.vicci.org/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "https://www.vicci.org/id-allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"
   
   """
   OPENID_PROVIDER_NAME = "localhost"
   OPENID_PROVIDER_URL = "http://localhost:8081/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "http://localhost:8081/allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"
   """

   
   OPENID_RP_REDIRECT_METHOD = "POST"     # POST to us for authentication, since we need to send the public key (which doesn't fit into a GET)

   def load_objects( self, volume_name, ug_name, username ):
      # get the Volume
      volume, status, volume_read_time = get_volume( volume_name )

      if status != 200:
         logging.error("get_volume status = %d" % status)
         response_volume_error( self, status )
         return (None, None, None)

      # get the UG
      UG = storage.get_user_gateway_by_name( ug_name )
      if UG == None:
         logging.error("storage.get_user_gateway_by_name returned None")
         response_UG_error( self, 404 )
         return (None, None, None)

      user = storage.read_user( username )
      if user == None:
         logging.error("storage.read_user returned None")
         response_user_error( self, 401 )
         return (None, None, None)

      return (volume, UG, user)
      
   get = None
   
   def post( self, volume_name, ug_name, username, operation ):
      self.load_query()
      session = self.getSession()
      self.setSessionCookie(session)

      volume, UG, user = self.load_objects( volume_name, ug_name, username )

      if volume == None or UG == None or user == None:
         return

      # this SyndicateUser must own this Gateway
      if user.owner_id != UG.owner_id:
         response_user_error( self, 403 )
         return

      """
      # this must be a valid public key
      if not Gateway.is_valid_pubkey( pubkey ):
         response_user_error( self, 400 )
         return
      """

      if operation == "begin":

         # load the public key
         if not "syndicatepubkey" in self.request.POST:
            response_user_error( self, 400 )
            return

         pubkey = self.request.POST.get("syndicatepubkey")
         if pubkey == None:
            response_user_error( self, 400 )
            return
         
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

         # preserve the public key
         session['syndicatepubkey'] = pubkey

         # reply with the redirect URL
         trust_root = self.HOST_URL
         return_to = self.buildURL( "/REGISTER/%s/%s/%s/complete" % (volume_name, ug_name, username) )
         immediate = self.IMMEDIATE_MODE in self.query

         redirect_url = oid_request.redirectURL( trust_root, return_to, immediate=immediate )

         openid_reply = ms_pb2.ms_openid_provider_reply()
         openid_reply.redirect_url = redirect_url
         openid_reply.auth_handler = self.OPENID_PROVIDER_AUTH_HANDLER
         openid_reply.username_field = self.OPENID_PROVIDER_USERNAME_FIELD
         openid_reply.password_field = self.OPENID_PROVIDER_PASSWORD_FIELD
         openid_reply.extra_args = urllib.urlencode( self.OPENID_PROVIDER_EXTRA_ARGS )
         openid_reply.challenge_method = self.OPENID_PROVIDER_CHALLENGE_METHOD
         openid_reply.response_method = self.OPENID_PROVIDER_RESPONSE_METHOD
         openid_reply.redirect_method = self.OPENID_RP_REDIRECT_METHOD

         data = openid_reply.SerializeToString()

         session.save()
         
         response_end( self, 200, data, "application/octet-stream", None )
         return

      elif operation == "complete":

         # get our saved pubkey
         pubkey = session.get('syndicatepubkey')
         if pubkey == None:
            logging.error("could not load public key")
            response_user_error( self, 400 )
            return

         # complete the authentication
         info, _, _ = self.complete_openid_auth()
         if info.status != consumer.SUCCESS:
            # failed
            response_user_error( self, 401 )
            return
         
         # attempt to load it into the UG
         if not UG.load_pubkey( pubkey ):
            logging.error("invalid public key")
            response_user_error( self, 400 )
            return
         
         # generate a session password
         UG.regenerate_session_credentials( volume )
         
         volume_metadata = ms_pb2.ms_volume_metadata()
         volume.protobuf( volume_metadata, UG )
         data = volume_metadata.SerializeToString()

         # save the UG
         UG.put()
         UG.FlushCache( UG.g_id )

         response_end( self, 200, data, "application/octet-stream", None )
         return


class MSFileRequestHandler(webapp2.RequestHandler):

   """
   Volume file request handler.
   It will read and list metadata entries via GET.
   It will create, delete, and update metadata entries via POST.
   """

   def get( self, volume_id_str, path ):
      file_request_start = storagetypes.get_time()
      
      if len(path) == 0:
         path = "/"

      if path[0] != '/':
         path = "/" + path

      UG, volume, timing = response_begin( self, volume_id_str )
      if UG == None or volume == None:
         return

      # request for a path's worth of metadata
      resolve_start = storagetypes.get_time()
      reply = Resolve( UG.owner_id, volume, path )
      resolve_time = storagetypes.get_time() - resolve_start

      timing['X-Resolve-Time'] = str(resolve_time)

      data = reply.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return
      
      
   def post(self, volume_id_str, path ):

      file_post_start = storagetypes.get_time()
      
      # will have gotten metadata updates
      updates_field = self.request.POST.get( 'ms-metadata-updates' )

      if updates_field == None:
         # no valid data given (malformed)
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.EINVAL)
         return

      # extract the data
      data = updates_field.file.read()
      
      # parse it 
      updates_set = ms_pb2.ms_updates()

      try:
         updates_set.ParseFromString( data )
      except:
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.EINVAL)
         return

      # begin the response
      UG, volume, timing = response_begin( self, volume_id_str )
      if UG == None or volume == None:
         return

      # validate the message
      if not UG.verify_ms_update( updates_set ):
         # authentication failure
         self.response.status = 401
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write( "Signature validation failed\n" )
         return

      create_times = []
      update_times = []
      delete_times = []

      # carry out the operation(s)
      for update in updates_set.updates:

         # extract
         attrs = MSEntry.unprotobuf_dict( update.entry )

         rc = 0

         # create?
         if update.type == ms_pb2.ms_update.CREATE:
            create_start = storagetypes.get_time()
            rc = MSEntry.Create( UG.owner_id, volume, **attrs )
            create_time = storagetypes.get_time() - create_start
            create_times.append( create_time )

         # update?
         elif update.type == ms_pb2.ms_update.UPDATE:
            update_start = storagetypes.get_time()
            rc = MSEntry.Update( UG.owner_id, volume, **attrs )
            update_time = storagetypes.get_time() - update_start
            update_times.append( update_time )

         # delete?
         elif update.type == ms_pb2.ms_update.DELETE:
            delete_start = storagetypes.get_time()
            rc = MSEntry.Delete( UG.owner_id, volume, **attrs )
            delete_time = storagetypes.get_time() - delete_start
            delete_times.append( delete_time )

         else:
            # not a valid method
            response_end( self, 202, "%s\n" % -errno.ENOSYS, "text/plain", None )
            return

         if rc != 0:
            # error in creation, but not in processing.
            # send back the error code
            response_end( self, 202, "%s\n" % -errno.ENOSYS, "text/plain", None )
            return


      if len(create_times) > 0:
         timing['X-Create-Times'] = ",".join( [str(t) for t in create_times] )

      if len(update_times) > 0:
         timing['X-Update-Times'] = ",".join( [str(t) for t in update_times] )

      if len(delete_times) > 0:
         timing['X-Delete-Times'] = ",".join( [str(t) for t in delete_times] )


      response_end( self, 200, "OK\n", "text/plain", timing )
      return


class MSOpenIDRequestHandler(GAEOpenIDRequestHandler):



   def auth_redirect( self, **kwargs ):
      """
      What to do if the user is already authenticated
      """
      session = self.getSession()
      if 'login_email' not in session:
         # invalid session
         response_user_error( self, 400, "Invalid or missing session cookie" )
         return 

      self.setRedirect('/syn/')
      return 0
      

   def verify_success( self, request, openid_url ):
      session = self.getSession()
      session['login_email'] = self.query.get('openid_username')
      return 0


   def process_success( self, info, sreg_resp, pape_resp ):
      self.auth_redirect()
      return 0
      
     