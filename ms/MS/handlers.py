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
import datetime

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


def response_UG_error( request_handler, status ):

   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"

   if status == 400:
      request_handler.response.write("Invalid request\n")
      
   elif status == 404:
      request_handler.response.write("No such gateway\n")
      
   elif status == 401:
      request_handler.response.write("Authentication Required\n")

   elif status == 403:
      request_handler.response.write("Authorization Failed\n")

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
      response_UG_error( request_handler, 403 )
      return (None, None, None)

   # make sure this UG is allowed to access this Volume
   valid_UG = volume.is_UG_allowed( UG )
   if not valid_UG:
      # UG does not belong to this Volume
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


class MSAuthRequestHandler( webapp2.RequestHandler ):
   """
   Generate a session certificate from a SyndicateUser account for a UG.
   """
   def post( self, volume_name, ug_name ):
      # get the Volume
      volume, status, volume_read_time = get_volume( volume_name )

      if status != 200:
         response_volume_error( self, status )
         return

      # get the UG
      UG = storage.get_user_gateway_by_name( ug_name )
      if UG == None:
         response_UG_error( self, 404 )
         return

      # get the SyndicateUser
      username, password = read_basic_auth( self.request.headers )
      if username == None or password == None:
         response_user_error( self, 401 )
         return

      user = storage.read_user( username )
      if user == None:
         response_user_error( self, 401 )
         return

      # this SyndicateUser must own this Gateway
      if user.owner_id != UG.owner_id:
         response_user_error( self, 403 )
         return

      # TODO: use OpenID password verification instead
      if not UG.authenticate( password ):
         response_user_error( self, 403 )
         return

      cred_field = self.request.POST.get( 'cred' )
      if cred_field == None:
         # malformed
         response_user_error( self, 400 )
         return
         
      # extract the data
      data = cred_field.file.read()

      cred = ms_pb2.ms_volume_gateway_cred()

      try:
         cred.ParseFromString( data )
      except:
         response_user_error( self, 400 )
         return

      # validate the credentials
      if not UG.is_valid_cred( cred ):
         response_user_error( self, 403 )
         return

      # register the public key
      UG.public_key = cred.public_key

      # serve back Volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      volume.protobuf( volume_metadata, UG, new_session=True )    # NOTE: new_session=True will cause the UG to be put()
      data = volume_metadata.SerializeToString()

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

         
         