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
from gateway import UserGateway

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

   

class MSVolumeRequestHandler(webapp2.RequestHandler):
   """
   Volume metadata request handler.
   """

   def get( self, volume_name ):
      volume_request_start = storagetypes.get_time()

      volume_read_start = storagetypes.get_time()
      volume = Volume.Read( volume_name )
      volume_read_time = storagetypes.get_time() - volume_read_start
      
      if volume == None:
         # no volume
         self.response.status = 404
         self.response.write("No such volume\n")
         return

      # authenticate the request to the Volume
      authenticated = volume.authenticate_gateway( self.request.headers )
      if not authenticated:
         self.response.status = 403
         self.response.write("Authorization Failed\n")
         return
      
      # get the UG's credentials
      username, password = read_basic_auth( self.request.headers )

      if username == None or password == None:
         self.response.status = 401
         self.response.write("Authentication Required\n")
         return
      
      # look up the requesting UG
      ug_read_start = storagetypes.get_time()
      UG = get_UG( username, password )
      ug_read_time = storagetypes.get_time() - ug_read_start
      
      if UG == None:
         # no UG
         self.response.status = 403
         self.response.write("Authorization Failed\n")
         return

      # authenticate the requesting UG
      valid_UG = UG.authenticate( password )
      if not valid_UG:
         # invalid credentials
         self.response.status = 403
         self.response.write("Authorization Failed\n")
         return

      # if we're still here, we're good to go
      
      # request for volume metadata
      volume_metadata = ms_pb2.ms_volume_metadata();
      user_gateways = storage.list_user_gateways_by_volume( volume.volume_id )
      
      volume.protobuf( volume_metadata, user_gateways )
      
      volume_metadata.requester_id = UG.owner_id
      
      data = volume_metadata.SerializeToString()

      self.response.status = 200
      self.response.headers['X-Volume-Time'] = str(volume_read_time)
      self.response.headers['X-UG-Time'] = str(ug_read_time)
      self.response.headers['X-Total-Time'] = str( storagetypes.get_time() - volume_request_start )
      
      self.response.write( data )
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
      
      # request to a volume.  Look up the volume
      volume_read_start = storagetypes.get_time()
      volume = Volume.Read( volume_name )
      volume_read_time = storagetypes.get_time() - volume_read_start
      
      if volume == None:
         # no volume
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.ENODEV)
         return

      if not volume.active:
         # volume is inactive
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.ENODATA)
         return


      # get the UG's credentials
      username, password = read_basic_auth( self.request.headers )

      if username == None or password == None:
         self.response.status = 401
         self.response.write("Authentication Required")
         return

      # look up the requesting UG
      ug_read_start = storagetypes.get_time()
      UG = get_UG( username, password )
      ug_read_time = storagetypes.get_time() - ug_read_start
      
      if UG == None:
         # no UG
         self.response.status = 403
         self.response.write("Authorization Failed")
         return

      # authenticate the requesting UG
      valid_UG = UG.authenticate( password )
      if not valid_UG:
         # invalid credentials
         self.response.status = 403
         self.response.write("Authorization Failed")
         return

      # request for a path's worth of metadata
      resolve_start = storagetypes.get_time()
      reply = Resolve( UG.owner_id, volume, path )
      resolve_time = storagetypes.get_time() - resolve_start

      # serialize
      reply_str = reply.SerializeToString()
      
      self.response.status = 200
      self.response.headers['Content-Type'] = "application/octet-stream"
      self.response.headers['X-Volume-Time'] = str( volume_read_time )
      self.response.headers['X-UG-Time'] = str( ug_read_time )
      self.response.headers['X-Resolve-Time'] = str( resolve_time )
      self.response.headers['X-Total-Time'] =  str( storagetypes.get_time() - file_request_start )
      self.response.write( reply_str )

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
         
      # get authentication tokens
      username, password = read_basic_auth( self.request.headers )

      if username == None or password == None:
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.EINVAL )
         return

      # get the UG
      ug_read_start = storagetypes.get_time()
      UG = get_UG( username, password )
      ug_read_time = storagetypes.get_time() - ug_read_start
      
      if UG == None:
         # no such UG
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.EPERM)

      # authenticate the UG
      valid_UG = UG.authenticate( password )
      if not valid_UG:
         # invalid credentials
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.EACCES)
         
      # look up the volume
      volume_read_start = storagetypes.get_time()
      volume = Volume.Read( volume_name )
      volume_read_time = storagetypes.get_time() - volume_read_start
      
      if volume == None:
         # no volume
         self.response.status = 202
         self.response.headers['Content-Type'] = "text/plain"
         self.response.write("%s\n" % -errno.ENODEV )
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
            self.response.status = 202
            self.response.headers['Content-Type'] = "text/plain"
            self.response.write("%s\n" % -errno.ENOSYS)
            return
            
         if rc != 0:
            # error in creation, but not in processing.
            # send back the error code
            self.response.status = 202
            self.response.headers['Content-Type'] = "text/plain"
            self.response.write( "%s\n" % rc )
            return
      
      self.response.status = 200
      self.response.headers['Content-Type'] = "text/plain"
      self.response.headers['X-UG-Time'] = str(ug_read_time)
      self.response.headers['X-Volume-Time'] = str(volume_read_time)

      if len(create_times) > 0:
         self.response.headers['X-Create-Times'] = ",".join( [str(t) for t in create_times] )

      if len(update_times) > 0:
         self.response.headers['X-Update-Times'] = ",".join( [str(t) for t in update_times] )

      if len(delete_times) > 0:
         self.response.headers['X-Delete-Times'] = ",".join( [str(t) for t in delete_times] )
         
      self.response.headers['X-Total-Time'] = str( storagetypes.get_time() - file_post_start )
      self.response.write("OK")
      return
            
         
         