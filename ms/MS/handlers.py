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
from gateway import UserGateway, ReplicaGateway, AcquisitionGateway
from msconfig import *

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
      return (None, None, None)

   # basic auth format:
   # ${gateway_type}_${gateway_id}:${password}
   # example:
   # UG_3:01234567890abcdef

   gateway_type, gateway_id, password = '', '', ''
   try:
      user_info = base64.decodestring( basic_auth[6:] )
      gateway, password = user_info.split(":")
      gateway_type, gateway_id = gateway.split("_")
      gateway_id = int(gateway_id)
   except:
      logging.info("incomprehensible Authorization header: '%s'" % basic_auth )
      return (None, None, None)

   return gateway_type, gateway_id, password



def get_gateway( gateway_type, gateway_id ):
   if gateway_id == None:
      # invalid header
      return (None, 403, None)

   gateway_read_start = storagetypes.get_time()
   gateway = None

   # get the gateway and validate its type
   if gateway_type == "UG":
      gateway = UserGateway.Read( gateway_id )
   elif gateway_type == "RG":
      gateway = ReplicaGateway.Read( gateway_id )
   elif gateway_type == "AG":
      gateway = AcquisitionGateway.Read( gateway_id )
   else:
      return (None, 401, None)

      
   gateway_read_time = storagetypes.get_time() - gateway_read_start
   return (gateway, 200, gateway_read_time)


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
   

def response_user_error( request_handler, status, message=None ):

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


def response_load_gateway( request_handler ):
   # get the gateway's credentials
   gateway_type_str, g_id, password = read_basic_auth( request_handler.request.headers )

   if gateway_type_str == None or g_id == None or password == None:
      response_user_error( request_handler, 401 )
      return (None, None, None)

   # look up the requesting gateway
   gateway, status, gateway_read_time = get_gateway( gateway_type_str, g_id )

   if status != 200:
      response_user_error( request_handler, status )
      return (None, None, None)

   # make sure this gateway is legit, if needed
   valid_gateway = gateway.authenticate_session( password )

   if not valid_gateway:
      # invalid credentials
      logging.error("Invalid session credentials")
      response_user_error( request_handler, 403 )
      return (None, None, None)

   return (gateway, status, gateway_read_time)
   

def response_begin( request_handler, volume_name_or_id ):
   
   timing = {}
   
   timing['request_start'] = storagetypes.get_time()

   # get the Volume
   volume, status, volume_read_time = get_volume( volume_name_or_id )

   if status != 200:
      response_volume_error( request_handler, status )
      return (None, None, None)

   # get the Gateway
   gateway, status, gateway_read_time = response_load_gateway( request_handler )

   if status != 200:
      return (None, None, None)
   
   # make sure this gateway is allowed to access this Volume
   valid_gateway = volume.is_gateway_in_volume( gateway )
   if not valid_gateway:
      # gateway does not belong to this Volume
      logging.error("Not in this Volume")
      response_user_error( request_handler, 403 )
      return (None, None, None)

   # if we're still here, we're good to go

   timing['X-Volume-Time'] = str(volume_read_time)
   timing['X-Gateway-Time'] = str(gateway_read_time)
   
   return (gateway, volume, timing)


   
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
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
         return

      # request for volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      volume.protobuf( volume_metadata )
      data = volume_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSUGRequestHandler( webapp2.RequestHandler ):
   """
   Get the list of (writeable) UGs in a Volume.
   """
   def get( self, volume_id_str ):
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
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
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
         return

      rg_metadata = ms_pb2.ms_volume_RGs()

      rgs = []
      
      if len(volume.rg_ids) > 0:
         rgs = storage.list_replica_gateways( {'ReplicaGateway.g_id IN' : volume.rg_ids} )

      volume.protobuf_RGs( rg_metadata, rgs )

      data = rg_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSAGRequestHandler( webapp2.RequestHandler ):
   """
   Get the list of AGs in a Volume.
   """
   def get( self, volume_id_str ):
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
         return

      ag_metadata = ms_pb2.ms_volume_AGs()

      ags = []

      if len(volume.ag_ids) > 0:
         ags = storage.list_acquisition_gateways( {'AcquisitionGateway.g_id IN' : volume.ag_ids} )

      volume.protobuf_AGs( ag_metadata, ags )

      data = ag_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSRegisterRequestHandler( GAEOpenIDRequestHandler ):
   """
   Generate a session certificate from a SyndicateUser account for a gateway.
   """

   OPENID_RP_REDIRECT_METHOD = "POST"     # POST to us for authentication, since we need to send the public key (which doesn't fit into a GET)

   def load_objects( self, gateway_type_str, gateway_name, username ):

      # get the gateway
      gateway = None
      if gateway_type_str == "UG":
         gateway = storage.get_user_gateway_by_name( gateway_name )
      elif gateway_type_str == "RG":
         gateway = storage.get_replica_gateway_by_name( gateway_name )
      elif gateway_type_str == "AG":
         gateway = storage.get_acquisition_gateway_by_name( gateway_name )
      else:
         logging.error("Invalid gateway type '%s'" % gateway_type_str )
         response_user_error( self, 401 )
         return (None, None)
         
      if gateway == None:
         logging.error("No such %s named %s" % (gateway_type_str, gateway_name))
         response_user_error( self, 404 )
         return (None, None)

      user = storage.read_user( username )
      if user == None:
         logging.error("storage.read_user returned None")
         response_user_error( self, 401 )
         return (None, None)

      return (gateway, user)

      
   def protobuf_volume( self, volume_metadata, volume, root=None ):
      # UGs
      ug_metadata = volume_metadata.ugs
      user_gateways = storage.list_user_gateways( {'UserGateway.volume_id ==' : volume.volume_id} )
      volume.protobuf_UGs( ug_metadata, user_gateways, sign=False )

      # RGs
      rg_metadata = volume_metadata.rgs
      rgs = []
      if len(volume.rg_ids) > 0:
         rgs = storage.list_replica_gateways( {'ReplicaGateway.g_id IN' : volume.rg_ids} )
         
      volume.protobuf_RGs( rg_metadata, rgs, sign=False )

      # AGs
      ag_metadata = volume_metadata.ags
      ags = []
      if len(volume.ag_ids) > 0:
         ags = storage.list_acquisition_gateways( {"AcquisitionGateway.g_id IN" : volume.ag_ids} )

      volume.protobuf_AGs( ag_metadata, ags, sign=False )

      # Volume
      if root != None:
         root.protobuf( volume_metadata.root )
         
      volume.protobuf( volume_metadata )

      return
      
      
   get = None
   
   def post( self, gateway_type_str, gateway_name, username, operation ):
      self.load_query()
      session = self.getSession()
      self.setSessionCookie(session)

      gateway, user = self.load_objects( gateway_type_str, gateway_name, username )

      if gateway == None or user == None:
         logging.info("load_objects failed")
         return

      # this SyndicateUser must own this Gateway
      if user.owner_id != gateway.owner_id:
         response_user_error( self, 403 )
         return

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
         trust_root = OPENID_HOST_URL
         return_to = self.buildURL( "/REGISTER/%s/%s/%s/complete" % (gateway_type_str, gateway_name, username) )
         immediate = self.IMMEDIATE_MODE in self.query

         redirect_url = oid_request.redirectURL( trust_root, return_to, immediate=immediate )

         openid_reply = ms_pb2.ms_openid_provider_reply()
         openid_reply.redirect_url = redirect_url
         openid_reply.auth_handler = OPENID_PROVIDER_AUTH_HANDLER
         openid_reply.username_field = OPENID_PROVIDER_USERNAME_FIELD
         openid_reply.password_field = OPENID_PROVIDER_PASSWORD_FIELD
         openid_reply.extra_args = urllib.urlencode( OPENID_PROVIDER_EXTRA_ARGS )
         openid_reply.challenge_method = OPENID_PROVIDER_CHALLENGE_METHOD
         openid_reply.response_method = OPENID_PROVIDER_RESPONSE_METHOD
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
         
         # attempt to load it into the gateway
         if not gateway.load_pubkey( pubkey ):
            logging.error("invalid public key")
            response_user_error( self, 400 )
            return
         
         # generate a session password
         session_password = gateway.regenerate_session_credentials()
         gateway_fut = gateway.put_async()
         futs = [gateway_fut]
         

         registration_metadata = ms_pb2.ms_registration_metadata()

         # registration information
         registration_metadata.session_password = session_password
         registration_metadata.session_timeout = gateway.session_timeout
         gateway.protobuf_cred( registration_metadata.cred )
        
         # find all Volumes
         volume_ids = gateway.volumes()
         volumes = storage.get_volumes( volume_ids )
         roots = storage.get_roots( volumes )

         for i in xrange(0, len(volume_ids)):
            volume = volumes[i]
            if volume == None:
               logging.error("No volume %s" % volume_ids[i])
               continue

            # next version of the Volume, since this gateway has now registered
            if isinstance( gateway, UserGateway ) or isinstance( gateway, ReplicaGateway ):
               if isinstance( gateway, UserGateway ):
                  volume.UG_version += 1
               else:
                  volume.RG_version += 1

               vol_fut = volume.put_async()
               futs.append( vol_fut )
            
            registration_volume = registration_metadata.volumes.add()
            self.protobuf_volume( registration_volume, volume, roots[i] )

         data = registration_metadata.SerializeToString()

         # save the gateway
         storage.wait_futures( futs )
         
         gateway.FlushCache( gateway.g_id )
         for i in xrange(0, len(volume_ids)):
            volume.FlushCache( volume_ids[i] )

         response_end( self, 200, data, "application/octet-stream", None )
         return


class MSFileReadHandler(webapp2.RequestHandler):

   """
   Volume file request handler.
   It will read and list metadata entries via GET.
   """

   def get( self, volume_id_str, file_id_str, file_version_str, file_mtime_sec_str, file_mtime_nsec_str ):

      file_request_start = storagetypes.get_time()

      file_id = -1
      file_version = -1
      file_mtime_sec = -1
      file_mtime_nsec = -1
      try:
         file_id = MSEntry.unserialize_id( int( file_id_str, 16 ) )
         file_version = int( file_version )
         file_mtime_sec = int( file_mtime_sec )
         file_mtime_nsec = int( file_mtime_nsec )
      except:
         response_end( self, 400, "BAD REQUEST", "text/plain", None )
         return
      
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
         return

      # this must be a User Gateway
      if not isinstance( gateway, UserGateway ):
         response_user_error( self, 403 )
         return

      logging.info("resolve /%s/%s" % (volume.volume_id, file_id) )
      
      # request a file or directory
      resolve_start = storagetypes.get_time()

      reply = Resolve( gateway.owner_id, volume, file_id, file_version, file_mtime_sec, file_mtime_nsec )
      
      resolve_time = storagetypes.get_time() - resolve_start
      
      logging.info("resolve /%s/%s rc = %d" % (volume.volume_id, file_id, reply.error) )

      timing['X-Resolve-Time'] = str(resolve_time)

      data = reply.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return
      

class MSFileWriteHandler(webapp2.RequestHandler):
   
   """
   Volume file request handler.
   It will create, delete, and update metadata entries via POST.
   """
   
   @storagetypes.toplevel
   def post(self, volume_id_str ):

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
      gateway, volume, timing = response_begin( self, volume_id_str )
      if gateway == None or volume == None:
         return

      # this must be a User Gateway or an Acquisition Gateway
      if not isinstance( gateway, UserGateway ) and not isinstance( gateway, AcquisitionGateway ):
         response_user_error( self, 403 )
         return

      # validate the message
      if not gateway.verify_ms_update( updates_set ):
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
         file_id = -1

         # create?
         if update.type == ms_pb2.ms_update.CREATE:
            logging.info("create /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
            
            create_start = storagetypes.get_time()
            file_id = MSEntry.Create( gateway.owner_id, volume, **attrs )
            create_time = storagetypes.get_time() - create_start
            create_times.append( create_time )

            logging.info("create /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], file_id ) )
            
            rc = file_id

         # update?
         elif update.type == ms_pb2.ms_update.UPDATE:
            logging.info("update /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
            
            update_start = storagetypes.get_time()
            rc = MSEntry.Update( gateway.owner_id, volume, **attrs )
            update_time = storagetypes.get_time() - update_start
            update_times.append( update_time )
            
            logging.info("update /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )

         # delete?
         elif update.type == ms_pb2.ms_update.DELETE:
            logging.info("delete /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
            
            delete_start = storagetypes.get_time()
            rc = MSEntry.Delete( gateway.owner_id, volume, **attrs )
            delete_time = storagetypes.get_time() - delete_start
            delete_times.append( delete_time )
            
            logging.info("delete /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )

         else:
            # not a valid method
            response_end( self, 202, "%s\n" % -errno.ENOSYS, "text/plain", None )
            return

         if rc < 0:
            # send back the error code
            response_end( self, 202, "%s\n" % rc, "text/plain", None )
            return


      if len(create_times) > 0:
         timing['X-Create-Times'] = ",".join( [str(t) for t in create_times] )

      if len(update_times) > 0:
         timing['X-Update-Times'] = ",".join( [str(t) for t in update_times] )

      if len(delete_times) > 0:
         timing['X-Delete-Times'] = ",".join( [str(t) for t in delete_times] )

      response_end( self, 200, "%s\n" % rc, "text/plain", timing )
         
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
      
     
