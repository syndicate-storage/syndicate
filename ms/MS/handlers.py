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



def get_UG( username, password ):
   if username == None or password == None:
      # invalid header
      return None

   UG = UserGateway.Read( username )
   return UG


def response_begin( request_handler, volume_name ):
   timing = {}
   
   timing['request_start'] = storagetypes.get_time()

   volume_read_start = storagetypes.get_time()
   volume = Volume.Read( volume_name )
   volume_read_time = storagetypes.get_time() - volume_read_start

   if volume == None:
      # no volume
      request_handler.response.status = 404
      request_handler.response.write("No such volume\n")
      return (None, None, None)

   if not volume.active:
      # inactive volume
      request_handler.response.status = 503
      request_handler.response.headers['Content-Type'] = "text/plain"
      request_handler.response.write("Service Not Available\n")
      return (None, None, None)

   # get the UG's credentials
   username, password = read_basic_auth( request_handler.request.headers )

   if username == None or password == None:
      request_handler.response.status = 401
      request_handler.response.headers['Content-Type'] = "text/plain"
      request_handler.response.write("Authentication Required\n")
      return (None, None, None)

   # look up the requesting UG
   ug_read_start = storagetypes.get_time()
   UG = get_UG( username, password )
   ug_read_time = storagetypes.get_time() - ug_read_start

   if UG == None:
      # no UG
      request_handler.response.status = 403
      request_handler.response.headers['Content-Type'] = "text/plain"
      request_handler.response.write("Authorization Failed\n")
      return (None, None, None)

   # make sure this UG account is legit
   valid_UG = UG.authenticate( password )
   if not valid_UG:
      # invalid credentials
      request_handler.response.status = 403
      request_handler.response.headers['Content-Type'] = "text/plain"
      request_handler.response.write("Authorization Failed\n")
      return (None, None, None)

   # make sure this UG is allowed to access this Volume
   valid_UG = volume.authenticate_UG( UG )
   if not valid_UG:
      # UG does not belong to this Volume
      request_handler.response.status = 403
      request_handler.response.headers['Content-Type'] = "text/plain"
      request_handler.response.write("Authorization Failed\n")
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

   def get( self, volume_name ):
      UG, volume, timing = response_begin( self, volume_name )
      if UG == None or volume == None:
         return

      # request for volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      volume.protobuf( volume_metadata, UG )
      data = volume_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return



class MSUGRequestHandler( webapp2.RequestHandler ):
   def get( self, volume_name ):
      UG, volume, timing = response_begin( self, volume_name )
      if UG == None or volume == None:
         return

      ug_metadata = ms_pb2.ms_volume_UGs()
      
      user_gateways = storage.list_user_gateways( {'UserGateway.volume_id ==' : volume.volume_id} )

      ug_metadata.ug_version = volume.UG_version
      for ug in user_gateways:
         ug_pb = ug_metadata.ug_creds.add()
         ug.protobuf_cred( ug_pb )

      data = ug_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return


class MSRGRequestHandler( webapp2.RequestHandler ):
   def get( self, volume_name ):
      UG, volume, timing = response_begin( self, volume_name )
      if UG == None or volume == None:
         return

      rg_metadata = ms_pb2.ms_volume_RGs()

      rgs = []
      
      if len(volume.rg_ids) > 0:
         rgs = storage.list_replica_gateways( {'ReplicaGateway.rg_id IN' : volume.rg_ids} )

      rg_metadata.rg_version = volume.RG_version
      for rg in rgs:
         rg_metadata.rg_hosts.append( rg.host )
         rg_metadata.rg_ports.append( rg.port )

      data = rg_metadata.SerializeToString()

      response_end( self, 200, data, "application/octet-stream", timing )
      return

   
class MSFileRequestHandler(webapp2.RequestHandler):

   """
   Volume file request handler.
   It will read and list metadata entries via GET.
   It will create, delete, and update metadata entries via POST.
   """

   def get( self, volume_name, path ):
      file_request_start = storagetypes.get_time()
      
      if len(path) == 0:
         path = "/"

      if path[0] != '/':
         path = "/" + path

      UG, volume, timing = response_begin( self, volume_name )
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
      
      
   def post(self, volume_name, path ):

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
      UG, volume, timing = response_begin( self )
      if UG == None or volume == None:
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

         
         