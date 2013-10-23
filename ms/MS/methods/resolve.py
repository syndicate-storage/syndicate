"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import MS
from MS.volume import Volume
from MS.entry import *

import common

import random
import os
import errno

import logging


def prettyprint( ent ):
   ret = ""
   


def Resolve( owner_id, volume, file_id, file_version, write_nonce ):
   """
   Read file and listing of the given file_id
   """

   file_memcache = MSEntry.Read( volume, file_id, memcache_keys_only=True )
   file_data = storagetypes.memcache.get( file_memcache )
   listing = MSEntry.ListAll( volume, file_id )

   all_ents = None
   file_fut = None
   error = 0
   need_refresh = True
   file_data_fut = None
   
   # do we need to consult the datastore?
   if file_data == None:
      logging.info( "file %s not cached" % file_id )
      file_data_fut = MSEntry.Read( volume, file_id, futs_only=True )
   
   if file_data_fut != None:
      all_futs = []
      
      if file_data_fut != None:
         all_futs += MSEntry.FlattenFuture( file_data_fut )
         
      storagetypes.wait_futures( all_futs )
      
      cacheable = {}
      if file_data_fut != None:
         file_data = MSEntry.FromFuture( file_data_fut )
         cacheable[ file_memcache ] = file_data
         logging.info( "cache file %s (%s)" % (file_id, file_data) )
      
      if len(cacheable) > 0:
         storagetypes.memcache.set_multi( cacheable )

   if file_data != None:
      # do we need to actually send this?
      if file_data.version == file_version and file_data.write_nonce == write_nonce:
         need_refresh = False

      else:
         if file_data.ftype == MSENTRY_TYPE_DIR:
            if listing != None:
               all_ents = [file_data] + listing
            else:
               all_ents = [file_data]
         
         else:
            all_ents = [file_data]

   # check security
   error = -errno.EACCES
   
   # error evaluation
   if file_data == None:
      error = -errno.ENOENT
      
   elif file_data.ftype == MSENTRY_TYPE_DIR:
      # directory. check permissions
      if file_data.owner_id == owner_id or (file_data.mode & 0055) != 0:
         # readable
         error = 0

   elif file_data.ftype == MSENTRY_TYPE_FILE:
      # file.  check permissions
      if file_data.owner_id == owner_id or (file_data.mode & 0044) != 0:
         # readable
         error = 0

   reply = common.make_ms_reply( volume, error )
   
   if error == 0:
      # all is well.
      
      reply.listing.ftype = file_data.ftype
      
      # modified?
      if not need_refresh:
         reply.listing.status = ms_pb2.ms_listing.NOT_MODIFIED
      else:
         reply.listing.status = ms_pb2.ms_listing.NEW

         for ent in all_ents:
            ent_pb = reply.listing.entries.add()
            ent.protobuf( ent_pb )
         
         #logging.info("Resolve %s: Serve back: %s" % (file_id, all_ents))
   
   else:
      reply.listing.ftype = 0
      reply.listing.status = ms_pb2.ms_listing.NONE
      
   # sign and deliver
   reply.signature = ""

   reply_str = reply.SerializeToString()
   sig = volume.sign_message( reply_str )

   reply.signature = sig

   return reply
   
            
      