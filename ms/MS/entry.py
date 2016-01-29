#!/usr/bin/pyhon

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


import storage.storagetypes as storagetypes
import storage.shardcounter as shardcounter

import protobufs.ms_pb2 as ms_pb2
import logging

import random
import os

import types
import errno
import time
import datetime
import collections
import pickle
import base64
import math
import binascii

from volume import Volume
from gateway import Gateway, GATEWAY_CAP_COORDINATE
from index import MSEntryIndex

from common.msconfig import *

MSENTRY_TYPE_FILE = ms_pb2.ms_entry.MS_ENTRY_TYPE_FILE
MSENTRY_TYPE_DIR = ms_pb2.ms_entry.MS_ENTRY_TYPE_DIR
MSENTRY_TYPE_NONE = ms_pb2.ms_entry.MS_ENTRY_TYPE_NONE

MSEntryFutures = collections.namedtuple( 'MSEntryFutures', ['base_future', 'shard_futures', "num_children_future", 'num_shards'] )

def is_readable( user_id, volume_owner_id, ent_owner_id, ent_mode ):
   return ent_owner_id == user_id or volume_owner_id == user_id or (ent_mode & 0044) != 0

def is_writable( user_id, volume_owner_id, ent_owner_id, ent_mode ):
   return ent_owner_id == user_id or volume_owner_id == user_id or (ent_mode & 0022) != 0


def latest_time( mtime_sec, mtime_nsec ):
   now_sec, now_nsec = storagetypes.clock_gettime()

   if mtime_sec < now_sec or (mtime_sec == now_sec and mtime_nsec < now_nsec):
      mtime_sec = now_sec
      mtime_nsec = now_nsec + 1
      if mtime_nsec > 1e9:
         mtime_nsec = 0
         mtime_sec += 1

   return (mtime_sec, mtime_nsec)

   
class MSEntryShard(storagetypes.Object):
   # Sharded component of an MSEntry.
   # NOTE: size doesn't change for directories, but the mtime_sec, mtime_nsec fields will.
   # They don't have to increase monotonically for directories---all the UG needs to determine is that the directory
   # has changed at all.  The concept of a "latest" shard is applicable only for files.
   mtime_sec = storagetypes.Integer(default=0, indexed=False)
   mtime_nsec = storagetypes.Integer(default=0, indexed=False)
   manifest_mtime_sec = storagetypes.Integer(default=0, indexed=False)
   manifest_mtime_nsec = storagetypes.Integer(default=0, indexed=False)
   size = storagetypes.Integer(default=0, indexed=False )
   write_nonce = storagetypes.Integer( default=0, indexed=False )
   xattr_nonce = storagetypes.Integer( default=0, indexed=False )
   xattr_hash = storagetypes.String( default="", indexed=False )
   ent_sig = storagetypes.String( default="", indexed=False )
   nonce_ts = storagetypes.Integer( default=0, indexed=False )          # for finding the latest write or xattr nonces
   
   # version of the MSEntry we're associated with
   msentry_version = storagetypes.Integer(default=0, indexed=False )

   # volume ID of the MSEntry we're associated with
   msentry_volume_id = storagetypes.Integer()
   
   
   @classmethod
   def manifest_modtime_max( cls, m1, m2 ):
      # return the later of two MSEntryShards, based on manifest_modtime
      if m1.manifest_mtime_sec < m2.manifest_mtime_sec:
         return m2

      elif m1.manifest_mtime_sec > m2.manifest_mtime_sec:
         return m1

      elif m1.manifest_mtime_nsec < m2.manifest_mtime_nsec:
         return m2

      elif m1.manifest_mtime_nsec > m2.manifest_mtime_nsec:
         return m1

      return m1

   @classmethod
   def size_max( cls, sz1, sz2 ):
      if sz1.size > sz2.size:
         return sz1

      return sz2
   
   @classmethod 
   def nonce_max( cls, n1, n2 ):
      if n1.nonce_ts > n2.nonce_ts:
         return n1 
   
      return n2 
   
   @classmethod
   def get_latest_attr( cls, ent, shards, attr ):
      mm = MSEntryShard.get_latest_shard( ent, shards )
      if mm != None:
         return getattr( mm, attr )
      else:
         return None
   
   @classmethod
   def get_latest_shard( cls, ent, shards ):
      s = None
      latest_version = ent.version
      latest_shard = None
      for shard in shards:
         if s == None:
            s = shard
            continue

         if shard == None:
            continue

         if shard.msentry_version != latest_version:
            continue
         
         s = MSEntryShard.nonce_max( s, shard )
      
      return s

   @classmethod
   def get_mtime_from_shards( cls, ent, shards ):
      mm = MSEntryShard.get_latest_shard( ent, shards )
      if mm is not None:
         return (mm.mtime_sec, mm.mtime_nsec)
      else:
         return (None, None)

   @classmethod
   def get_size_from_shards( cls, ent, shards ):
      # NOTE: size can never decrease for a given version.
      # truncate() will re-version a file
      sz = None
      for shard in shards:
         
         if shard == None:
            continue

         if shard.msentry_version != ent.version:
            continue

         if sz is None:
             sz = shard
         
         sz = MSEntryShard.size_max( sz, shard )

      return sz.size
   
   @classmethod 
   def get_manifest_mtime_from_shards( cls, ent, shards ):
      # get the latest manifest modification time
      mm = None
      latest_version = ent.version
      latest_shard = None
      for shard in shards:
         if mm == None:
            mm = shard
            continue

         if shard == None:
            continue

         if shard.msentry_version != latest_version:
            continue
         
         mm = MSEntryShard.manifest_modtime_max( mm, shard )
      
      if mm is not None:
         return (mm.manifest_mtime_sec, mm.manifest_mtime_nsec)
      else:
         return (None, None)
   
   @classmethod 
   def Delete_ByVolume( cls, volume_id, async=False ):
      """
      Asynchronously delete by Volume
      """
      def __delete_shard_mapper( ent_shard_key ):
         ent_shard_key.delete()
      
      return MSEntryShard.ListAll( {"MSEntryShard.msentry_volume_id ==": volume_id}, map_func=__delete_shard_mapper, keys_only=True, async=async )
   
   
class MSEntryNameHolder( storagetypes.Object ):
   """
   Exists to prove that an MSEntry record exists in a parent under a given name.
   There is one of these per MSEntry.
   Index so that we can query by parent_id and name.
   """
   volume_id = storagetypes.Integer( default=-1 )
   parent_id = storagetypes.String( default="None", indexed=False )
   file_id = storagetypes.String( default="None", indexed=False )
   name = storagetypes.String( default="", indexed=False )
   
   @classmethod
   def make_key_name( cls, volume_id, parent_id, name ):
      return "MSEntryNameHolder: volume_id=%s,parent_id=%s,name=%s" % (volume_id, parent_id, name)
   
   @classmethod
   def create_async( cls, _volume_id, _parent_id, _file_id, _name ):
      return MSEntryNameHolder.get_or_insert_async( MSEntryNameHolder.make_key_name( _volume_id, _parent_id, _name ), volume_id=_volume_id, parent_id=_parent_id, file_id=_file_id, name=_name )
   
   @classmethod 
   def Read( cls, volume_id, parent_id, name, async=False ):
      """
      Read a nameholder hitting memcache if we can.
      """
      key_name = cls.make_key_name( volume_id, parent_id, name )
      nameholder = storagetypes.memcache.get( key_name )
      
      if nameholder is None:
         # get from datastore 
         nameholder_key = storagetypes.make_key( MSEntryNameHolder, key_name )
         if async:
            nameholder_fut = nameholder_key.get_async()
            return nameholder_fut 
         
         else:
            nameholder = nameholder_key.get()
            
            storagetypes.memcache.set( key_name, nameholder )
            return nameholder 
         
      else:
         if async:
            return storagetypes.FutureWrapper( nameholder )
         else:
            return nameholder 
         
   
   @classmethod
   def Delete_ByVolume( cls, volume_id, async=False ):
      """
      Asynchronously delete by volume.
      """
      def __delete_nameholder_mapper( ent_nameholder_key ):
         # TODO: obsolete this 
         nameholder = ent_nameholder_key.get()
         if nameholder is not None:
            key_name = cls.make_key_name( nameholder.volume_id, nameholder.parent_id, nameholder.name )
            storagetypes.memcache.delete( key_name )
            ent_nameholder_key.delete()
         
      return MSEntryNameHolder.ListAll( {"MSEntryNameHolder.volume_id ==": volume_id}, map_func=__delete_nameholder_mapper, keys_only=True, async=async )
      
   
   
class MSEntryXAttr( storagetypes.Object ):
   """
   A single extended attribute for an MSEntry.
   There are many of these per MSEntry.
   """
   
   file_id = storagetypes.String( default="None" )              # has to be a string, since this is an unsigned 64-bit int (and GAE only supports signed 64-bit int)
   volume_id = storagetypes.Integer( default=-1 )
   
   xattr_name = storagetypes.String( default="None" )
   xattr_value = storagetypes.Text()
   
   nonce = storagetypes.Integer( default=0 )
   
   @classmethod 
   def make_key_name( cls, volume_id, file_id, name="" ):
      return "MSEntryXAttr: volume_id=%s,file_id=%s,name=%s" % (volume_id, file_id, name)
   
   @classmethod
   def FetchXAttrs( cls, volume, msent ):
      """
      Get the names and values of the xattrs for this MSEntry.
      Return (rc, list_of_xattrs)
      """
      cached_listing_name = MSEntryXAttr.make_key_name( msent.volume_id, msent.file_id )
      names_and_values = storagetypes.memcache.get( cached_listing_name )
      if names_and_values is None:
            
         names_and_values = cls.ListAll( {"MSEntryXAttr.file_id ==": msent.file_id, "MSEntryXAttr.volume_id ==": msent.volume_id} )
         
         storagetypes.memcache.set( cached_listing_name, names_and_values )
      
      return 0, names_and_values
      
   
   @classmethod
   def ReadXAttr( cls, volume_id, file_id, xattr_name ):
      """
      Get an extended attribute record from the datastore
      Fail with ENOATTR (ENODATA) if there is no such attribute.
      """
      xattr_key_name = MSEntryXAttr.make_key_name( volume_id, file_id, xattr_name )
      xattr = storagetypes.memcache.get( xattr_key_name )
      rc = 0
      
      if xattr == None:
         xattr_key = storagetypes.make_key( MSEntryXAttr, xattr_key_name )
         xattr = xattr_key.get()
         
         if xattr != None:
            # cache for later 
            storagetypes.memcache.set( xattr_key_name, xattr )
            
      
      if xattr == None:
         # not found 
         return (-errno.ENODATA, None)
         
      else:
         return (0, xattr)
      
      
   @classmethod 
   def PutXAttr( cls, volume, msent, xattr_name, xattr_value, xattr_nonce, xattr_hash ):
      """
      Put an xattr, possibly overwriting it.
      Return 0 on success 
      
      NOTE: The caller must ensure that the msent is sent by the coordinator.
      """
      
      xattr_key_name = MSEntryXAttr.make_key_name( msent.volume_id, msent.file_id, xattr_name )
      rc = 0
      
      xattr = MSEntryXAttr( key=storagetypes.make_key( MSEntryXAttr, xattr_key_name ), file_id=msent.file_id, volume_id=volume.volume_id,
                            xattr_name=xattr_name, xattr_value=xattr_value )
         
      xattr_fut = xattr.put_async()
   
      # update the xattr_nonce in the msentry
      new_shard = MSEntry.update_shard( volume.num_shards, msent, xattr_nonce=xattr_nonce, xattr_hash=xattr_hash )
      shard_fut = new_shard.put_async()
      
      storagetypes.wait_futures( [xattr_fut, shard_fut] )
      
      # clear cached xattr value, xattr listing, and msentry
      ent_cache_key_name = MSEntry.make_key_name( msent.volume_id, msent.file_id )
      storagetypes.memcache.delete_multi( [xattr_key_name, MSEntryXAttr.make_key_name( msent.volume_id, msent.file_id ), ent_cache_key_name] )
      
      return rc
      
   
   @classmethod 
   def RemoveXAttr( cls, volume, msent, xattr_name, xattr_nonce, xattr_hash ):
      """
      Delete an extended attribute from the MSEntry msent.
      
      The caller needs to ensure that only the coordinator can call this method.
      """
      
      storagetypes.deferred.defer( MSEntryXAttr.remove_and_uncache, volume, msent, xattr_name, xattr_nonce, xattr_hash )
      
      return 0
      
      
   @classmethod 
   def remove_and_uncache( cls, volume, msent, xattr_name, xattr_nonce, xattr_hash ):
      """
      Remove and uncache an xattr.
      Updates the MSEntry shard.
      Use this with removexattr()
      """
      
      # get the xattr
      xattr_key_name = MSEntryXAttr.make_key_name( volume.volume_id, msent.file_id, xattr_name )
      xattr_key = storagetypes.make_key( MSEntryXAttr, xattr_key_name )
      
      # get a new shard 
      new_shard = MSEntry.update_shard( volume.num_shards, msent, xattr_nonce=xattr_nonce, xattr_hash=xattr_hash )
      
      # delete the xattr and put the new shard
      delete_fut = xattr_key.delete_async()
      shard_fut = new_shard.put_async()
      
      storagetypes.wait_futures( [delete_fut, shard_fut] )
      
      # clear caches
      listing_key = MSEntryXAttr.make_key_name( msent.volume_id, msent.file_id )
      ent_cache_key_name = MSEntry.make_key_name( msent.volume_id, msent.file_id )
      storagetypes.memcache.delete_multi( [xattr_key_name, listing_key, ent_cache_key_name] )
      
   
   @classmethod 
   def delete_and_uncache( cls, volume_id, file_id, xattr_name ):
      """
      Delete and uncache an xattr.
      Does NOT update the associated MSEntry shard.
      Only use this when deleting an MSEntry.
      """
      # get the xattr
      xattr_key_name = MSEntryXAttr.make_key_name( volume.volume_id, msent.file_id, xattr_name )
      xattr_key = storagetypes.make_key( MSEntryXAttr, xattr_key_name )
      
      # delete the xattr
      xattr_key.delete()
      
      # clear caches
      storagetypes.memcache.delete( xattr_key_name )
      
   
   @classmethod
   def Delete_ByFile( cls, volume_id, file_id, async=False ):
      """
      Delete all xattrs for a given file asynchronously
      """
      
      def __xattr_deferred_delete( xattr ):
         MSEntryXAttr.delete_and_uncache( volume_id, file_id, xattr.xattr_name )
         
      return cls.ListAll( {"MSEntryXAttr.file_id ==": file_id, "MSEntryXAttr.volume_id ==": volume_id}, projection=['xattr_name'], map_func=__xattr_deferred_delete, async=async )
   
   
   @classmethod
   def Delete_ByVolume( cls, volume_id, async=False ):
      """
      Delete all xattrs for a given volume asynchronously
      """
      def __xattr_deferred_delete( xattr ):
         MSEntryXAttr.delete_and_uncache( volume_id, xattr.file_id, xattr.xattr_name )
      
      return cls.ListAll( {"MSEntryXAttr.volume_id ==": volume_id}, projection=['xattr_name', 'file_id'], map_func=__xattr_deferred_delete, async=async )



class MSEntryVacuumLog( storagetypes.Object ):
   """
   Log of manifest metadata of an MSEntry, for use in garbage collection.
   Gateways poll these in order to garbage-collect stale manifests and blocks.
   The MS remembers which previous manifests (and thus blocks) exist through a set of these records.

   The log does not preserve order, since garbage collection is done concurrently.
   """
   
   file_id = storagetypes.String( default="None" )              # has to be a string, since this is an unsigned 64-bit int (and GAE only supports signed 64-bit int)
   volume_id = storagetypes.Integer( default=-1 )
   writer_id = storagetypes.Integer( default=-1 )
   
   version = storagetypes.Integer( default=-1, indexed=False )
   manifest_mtime_sec = storagetypes.Integer(default=0, indexed=False)
   manifest_mtime_nsec = storagetypes.Integer(default=0, indexed=False)
   
   affected_blocks = storagetypes.Integer( repeated=True )
   
   signature = storagetypes.String( default="None", indexed=False )
   
   nonce = storagetypes.Integer( default=0, indexed=False )
   
   @classmethod
   def make_key_name( cls, volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec ):
      return "MSEntryVacuumLog: volume_id=%s,writer_id=%s,file_id=%s,version=%s,manifest_mtime_sec=%s,manifest_mtime_nsec=%s" % (volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec)
   
   @classmethod
   def create_async( cls, _volume_id, _writer_id, _file_id, _version, _manifest_mtime_sec, _manifest_mtime_nsec, _affected_blocks, _signature, nonce=None ):
      
      if _affected_blocks is None:
         _affected_blocks = []
         
      key_name = MSEntryVacuumLog.make_key_name( _volume_id, _writer_id, _file_id, _version, _manifest_mtime_sec, _manifest_mtime_nsec )
      
      if nonce is None:
         nonce = random.randint(-2**63, 2**63 - 1)
      
      return MSEntryVacuumLog.get_or_insert_async( key_name,
                                                   volume_id=_volume_id,
                                                   writer_id=_writer_id,
                                                   file_id=_file_id,
                                                   version=_version,
                                                   manifest_mtime_sec=_manifest_mtime_sec,
                                                   manifest_mtime_nsec=_manifest_mtime_nsec,
                                                   affected_blocks=_affected_blocks,
                                                   signature=_signature,
                                                   nonce=nonce )
   
   @classmethod 
   def delete_async( cls, volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec ):
      
      key_name = MSEntryVacuumLog.make_key_name( volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec )
      record_key = storagetypes.make_key( MSEntryVacuumLog, key_name )
      
      storagetypes.deferred.defer( MSEntryVacuumLog.delete_all, [record_key] )
      
      return True
   
   
   @classmethod 
   def Peek( cls, volume_id, file_id, async=False ):
      """
      Get a single vacuum log entry, or None if there are none
      Order is not preserved.
      """
      
      log_head = MSEntryVacuumLog.ListAll( {"MSEntryVacuumLog.volume_id ==": volume_id, "MSEntryVacuumLog.file_id ==": file_id},
                                           limit=1,
                                           async=async )
     
      if not async:
          if len(log_head) == 0:
              return None 

          else:
              log_head = log_head[0]

      return log_head
  
   
   @classmethod 
   def Insert( cls, volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec, affected_blocks, signature ):
      """
      Add a new manifest log record.
      Verify that the MSEntry exists afterwards, and undo (delete) the newly-inserted record if not
      """
      
      nonce = random.randint(-2**63, 2**63 - 1)
      
      rec_fut = cls.create_async( volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec, affected_blocks, signature, nonce=nonce )
      
      rec = rec_fut.get_result()
      
      msent = MSEntry.ReadBase( volume_id, file_id )
      
      if msent is None or msent.deleted:
         # undo, if we created 
         if rec.nonce == nonce:
            cls.delete_async( rec.volume_id, rec.writer_id, rec.file_id, rec.version, rec.manifest_mtime_sec, rec.manifest_mtime_nsec )
            
      return rec
   
   
   @classmethod 
   def Remove( cls, volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec ):
      """
      Remove a manifest log record.
      Return -errno.ENOENT if it doesn't exist 
      """
      
      logging.info( "remove log head /%s/%s (version = %s, gateway = %s, manifest_mtime_sec = %s, manifest_mtime_nsec = %s)" % (volume_id, file_id, version, writer_id, manifest_mtime_sec, manifest_mtime_nsec) )
      
      key_name = MSEntryVacuumLog.make_key_name( volume_id, writer_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec )
      log_ent_key = storagetypes.make_key( MSEntryVacuumLog, key_name )
      
      log_ent = log_ent_key.get()
      
      if log_ent is None:
         return -errno.ENOENT 
      
      else:
         
         MSEntryVacuumLog.delete_all( [log_ent_key] )
         return 0



class MSEntry( storagetypes.Object ):
   """
   Syndicate metadata entry.
   Each entry is named by its file ID and volume ID.
   Each entry is in its own entity group, and is named by its file ID and volume ID.   
   """

   # stuff the MS needs to know to function
   ftype = storagetypes.Integer( default=-1, indexed=False )
   file_id = storagetypes.String( default="None" )              # has to be a string, not an int, since file IDs are *unsigned* 64-bit numbers
   owner_id = storagetypes.Integer( default=-1 )
   coordinator_id = storagetypes.Integer( default=1 )
   volume_id = storagetypes.Integer( default=-1 )
   mode = storagetypes.Integer( default=0 )
   parent_id = storagetypes.String( default="-1" )                # file_id value of parent directory
   deleted = storagetypes.Boolean( default=False, indexed=False ) # whether or not this directory is considered to be deleted
   version = storagetypes.Integer( default=0, indexed=False ) 
   capacity = storagetypes.Integer( default=16, indexed=False )         # for directories, the smallest 2^i after its number of children.  Used for choosing directory indexes
   generation = storagetypes.Integer( default=0, indexed=False )
   
   # stuff the MS can ignore
   name = storagetypes.String( default="", indexed=False )
   ctime_sec = storagetypes.Integer( default=0, indexed=False )
   ctime_nsec = storagetypes.Integer( default=0, indexed=False )
   max_read_freshness = storagetypes.Integer( default=0, indexed=False )
   max_write_freshness = storagetypes.Integer( default=0, indexed=False )
   
   # filled in from a shard 
   mtime_sec = None
   mtime_nsec = None
   manifest_mtime_sec = None               # NOTE: only used for files
   manifest_mtime_nsec = None              # NOTE: only used for files
   size = None
   write_nonce = None
   xattr_nonce = None       # set each time an xattr is added, updated, or deleted.
   xattr_hash = None        # hash over all key/values
   ent_sig = None           # signature over the latest MSEntry data from the writer.
   nonce_ts = None          # mostly-increasing timestamp from coordinator for xattr and write nonces
   
   # filled in from the index 
   num_children = -1
   
   pickled = storagetypes.Text()           # for caching sharded and indexed data
   
   
   # attributes that must be supplied on creation
   required_attrs = [
      "ftype",
      "file_id",
      "parent_id",
      "name",
      "version",
      "owner_id",
      "coordinator_id",
      "volume_id",
      "mtime_sec",
      "mtime_nsec",
      "manifest_mtime_sec",
      "manifest_mtime_nsec",
      "ctime_sec",
      "ctime_nsec",
      "mode",
      "size",
      "capacity",
      "ent_sig"
   ]

   # attributes that uniquely identify this entry
   key_attrs = [
      "volume_id",
      "file_id",
   ]

   # required for operations that mutate the entry (i.e. create, update, rename, delete, and chcoord)
   mutate_attrs = [
      "volume_id",
      "file_id",
      "name",
      "parent_id",
      "coordinator_id",
      "manifest_mtime_sec",
      "manifest_mtime_nsec",
      "write_nonce",
      "ent_sig"
   ]

   # methods for generating default values for attributes (sharded or not)
   default_values = {
      "max_read_freshness": (lambda cls, attrs: 0),
      "max_write_freshness": (lambda cls, attrs: 0),
      "file_id": (lambda cls, attrs: "None"),
      "capacity": (lambda cls, attrs: 16)
   }

   # publicly readable attributes via the API, sharded or not
   read_attrs = [
      "ftype",
      "version",
      "name",
      "ctime_sec",
      "ctime_nsec",
      "mtime_sec",
      "mtime_nsec",
      "manifest_mtime_sec",
      "manifest_mtime_nsec",
      "owner_id",
      "coordinator_id",
      "volume_id",
      "mode",
      "size",
      "max_read_freshness",
      "max_write_freshness",
      "write_nonce",
      "xattr_nonce",
      "xattr_hash",
      "capacity",
      "ent_sig"
   ]

   # publicly writable attributes in the API, sharded or not
   write_attrs = [
      "version",
      "owner_id",
      "mode",
      "size",
      "mtime_sec",
      "mtime_nsec",
      "manifest_mtime_sec",
      "manifest_mtime_nsec",
      "max_read_freshness",
      "max_write_freshness",
      "write_nonce",
      "xattr_hash",
      "ent_sig"
   ]

   # shard class
   shard_class = MSEntryShard
   
   # sharded fields
   shard_fields = [
      "manifest_mtime_sec",
      "manifest_mtime_nsec",
      "mtime_sec",
      "mtime_nsec",
      "size",
      "msentry_version",                # prevent collisions with self.version 
      "msentry_volume_id",              # prevent collisions with self.volume_id
      "write_nonce",
      "xattr_nonce",
      "xattr_hash",
      "nonce_ts",
      "ent_sig"
   ]
   
   # fields loaded from the directory index 
   indexed_fields = [
      "num_children"
   ]

   # functions that read a sharded value from shards for an instance of this ent
   shard_readers = {
      "manifest_mtime_sec": (lambda ent, shards: MSEntryShard.get_manifest_mtime_from_shards( ent, shards )[0]),
      "manifest_mtime_nsec": (lambda ent, shards: MSEntryShard.get_manifest_mtime_from_shards( ent, shards )[1]),
      "mtime_sec": (lambda ent, shards: MSEntryShard.get_mtime_from_shards( ent, shards )[0]),
      "mtime_nsec": (lambda ent, shards: MSEntryShard.get_mtime_from_shards( ent, shards )[1]),
      "size": (lambda ent, shards: MSEntryShard.get_size_from_shards( ent, shards )),
      "write_nonce": (lambda ent, shards: MSEntryShard.get_latest_attr( ent, shards, "write_nonce" )),
      "xattr_nonce": (lambda ent, shards: MSEntryShard.get_latest_attr( ent, shards, "xattr_nonce" )),
      "xattr_hash": (lambda ent, shards: MSEntryShard.get_latest_attr( ent, shards, "xattr_hash" )),
      "nonce_ts": (lambda ent, shards: MSEntryShard.get_latest_attr( ent, shards, "nonce_ts" )),
      "ent_sig": (lambda ent, shards: MSEntryShard.get_latest_attr( ent, shards, "ent_sig"))
   }

   # functions that write a sharded value, given this ent
   shard_writers = {
      "manifest_mtime_sec": (lambda ent: ent.manifest_mtime_sec),
      "manifest_mtime_nsec": (lambda ent: ent.manifest_mtime_nsec),
      "mtime_sec": (lambda ent: ent.mtime_sec),
      "mtime_nsec": (lambda ent: ent.mtime_nsec),
      "msentry_version": (lambda ent: ent.version),
      "msentry_volume_id": (lambda ent: ent.volume_id),
      "size": (lambda ent: ent.size),
      "write_nonce": (lambda ent: ent.write_nonce),
      "xattr_nonce": (lambda ent: ent.xattr_nonce),
      "xattr_hash": (lambda ent: ent.xattr_hash),
      "nonce_ts": (lambda ent: ent.nonce_ts),
      "ent_sig": (lambda ent: ent.ent_sig)
   }
   
   @classmethod 
   def is_serialized_id( self, i ):
      """
      Is this a serialized ID?  i.e. is it an int?
      """
      return isinstance( i, types.IntType ) or isinstance( i, types.LongType )
   
   @classmethod 
   def unserialize_id( self, i ):
      """
      Convert a numeric file_id to a string
      """
      return '{:016X}'.format(i)
   
   @classmethod
   def serialize_id( self, i_str ):
      """
      Convert a file_id as a hex string to an int.
      """
      return int(i_str, 16)
   
   def to_dict( self ):
      """
      Overloaded to_dict to include shard fields
      """
      
      attrs = super( MSEntry, self ).to_dict()
      
      for sf in self.shard_fields:
         if hasattr( self, sf ):
            attrs[ sf ] = getattr( self, sf )
            
      return attrs
   
   
   def __getstate__( self ):
      """
      Populate the shard data before serializing 
      """
      pickle_data = {}
      
      for sf in self.shard_fields + self.indexed_fields:
         if hasattr(self, sf):
            pickle_data[sf] = getattr( self, sf )
      
      serialized_str = pickle.dumps( pickle_data )
      self.pickled = serialized_str
      
      return super( MSEntry, self ).__getstate__()
   
   
   def __setstate__( self, state ):
      """
      Populate base data from cached shard data
      """
      super( MSEntry, self ).__setstate__( state )
      
      if self.pickled is not None:
         
         pickle_data = pickle.loads( self.pickled )
         
         for (sf, sv) in pickle_data.items():
            setattr( self, sf, sv )
            
      self.pickled = None
         
      
   @classmethod 
   def unprotobuf_dict( cls, ent_cert ):
      """
      Given a protobuf'ed MSEntry, turn it into a dict of attributes.
      Include shard attributes as well
      """
      
      attrs = {
         "ftype": ent_cert.type,
         "file_id": MSEntry.unserialize_id( ent_cert.file_id ),
         "ctime_sec": ent_cert.ctime_sec,
         "ctime_nsec": ent_cert.ctime_nsec,
         "mtime_sec": ent_cert.mtime_sec,
         "mtime_nsec": ent_cert.mtime_nsec,
         "manifest_mtime_sec": ent_cert.manifest_mtime_sec,
         "manifest_mtime_nsec": ent_cert.manifest_mtime_nsec,
         "owner_id": ent_cert.owner,
         "coordinator_id": ent_cert.coordinator,
         "volume_id": ent_cert.volume,
         "mode": ent_cert.mode,
         "size": ent_cert.size,
         "version": ent_cert.version,
         "max_read_freshness": ent_cert.max_read_freshness,
         "max_write_freshness": ent_cert.max_write_freshness,
         "name": ent_cert.name,
         "write_nonce": ent_cert.write_nonce,
         "xattr_nonce": ent_cert.xattr_nonce,
         "generation": ent_cert.generation,
         "parent_id": MSEntry.unserialize_id( ent_cert.parent_id ),
         "capacity": ent_cert.capacity,         
         "ent_sig": ent_cert.signature,
         
         # shard-specific 
         "msentry_version": ent_cert.version,
         "msentry_volume_id": ent_cert.volume
      }
     
      if hasattr(ent_cert, "xattr_hash") and len(ent_cert.xattr_hash) > 0:
          attrs["xattr_hash"] = binascii.hexlify( ent_cert.xattr_hash )
          
      return attrs
   
   @classmethod 
   def protobuf( cls, ent, ent_pb ):
      """
      Given a loaded MSEntry, reconstruct the signed protobuf that created it.
      """
      
      ent_pb.type = ent.ftype
      ent_pb.file_id = MSEntry.serialize_id( ent.file_id )
      ent_pb.ctime_sec = ent.ctime_sec
      ent_pb.ctime_nsec = ent.ctime_nsec
      ent_pb.mtime_sec = ent.mtime_sec
      ent_pb.mtime_nsec = ent.mtime_nsec
      ent_pb.manifest_mtime_sec = ent.manifest_mtime_sec
      ent_pb.manifest_mtime_nsec = ent.manifest_mtime_nsec
      ent_pb.owner = ent.owner_id
      ent_pb.coordinator = ent.coordinator_id
      ent_pb.volume = ent.volume_id
      ent_pb.mode = ent.mode
      ent_pb.size = ent.size
      ent_pb.version = ent.version
      ent_pb.max_read_freshness = ent.max_read_freshness
      ent_pb.max_write_freshness = ent.max_write_freshness
      ent_pb.name = ent.name
      ent_pb.write_nonce = ent.write_nonce
      ent_pb.xattr_nonce = ent.xattr_nonce
      ent_pb.generation = ent.generation
      ent_pb.signature = ent.ent_sig
      ent_pb.parent_id = MSEntry.serialize_id( ent.parent_id )
      ent_pb.num_children = ent.num_children
      ent_pb.capacity = ent.capacity
      
      if ent.xattr_hash is not None:
         if len(ent.xattr_hash) > 0:
             ent_pb.xattr_hash = binascii.unhexlify( ent.xattr_hash )
         else:
             # empty string 
             ent_pb.xattr_hash = ent.xattr_hash
      
      return
      
   
   @classmethod
   def get_parent_path( cls, path ):
      """
      Get the path of the parent entry.
      """
      ppath = path.rstrip("/")
      if len(ppath) == 0:
         return "/"
      else:
         parent_dirname = os.path.dirname( ppath )
         if len(parent_dirname) == 0:
            return "/"
            
         return parent_dirname


   @classmethod
   def sanitize_fs_path( cls, fs_path ):
      # path should not end in /, unless it's the root directory
      if fs_path != '/':
         if fs_path[-1] == '/':
            return fs_path[:-1]
         else:
            return fs_path[:]

      else:
         return fs_path[:]


   @classmethod
   def make_key_name( cls, volume_id=None, file_id=None ):
      return super( MSEntry, cls ).make_key_name( volume_id=volume_id, file_id=file_id )
   
   @classmethod
   def cache_listing_key_name( cls, volume_id=None, file_id=None, page_id=None ):
      return super( MSEntry, cls ).cache_listing_key_name( volume_id=volume_id, file_id=file_id ) + ",page_id=%s" % page_id

   @classmethod
   @storagetypes.concurrent
   def __read_msentry_base( cls, volume_id, file_id, **ctx_opts ):
      ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id, file_id ) )
      ent = yield ent_key.get_async( **ctx_opts )
      storagetypes.concurrent_return( ent )


   @classmethod
   @storagetypes.concurrent
   def __read_msentry_shards( cls, volume_id, file_id, num_shards, **ctx_opts ):
      key_name = MSEntry.make_key_name( volume_id, file_id )
      shard_keys = MSEntry.get_shard_keys( num_shards, key_name )
      shards = yield storagetypes.get_multi_async( shard_keys, **ctx_opts )
      storagetypes.concurrent_return( shards )

   @classmethod
   @storagetypes.concurrent
   def __read_msentry( cls, volume_id, file_id, num_shards, **ctx_opts ):
      
      ent_key_name = MSEntry.make_key_name( volume_id, file_id )
      
      ent, shards = yield MSEntry.__read_msentry_base( volume_id, file_id, **ctx_opts ), MSEntry.__read_msentry_shards( volume_id, file_id, num_shards, **ctx_opts )
      if ent is not None:
         ent.populate_from_shards( shards )

      storagetypes.concurrent_return( ent )


   @classmethod
   def check_mutate_attrs( cls, ent_attrs, safe_to_ignore=[], extra_required=[] ):
      """
      Verify that we have the appropriate attributes for an op that will mutate state.
      """
      
      needed = []
      for key_attr in cls.mutate_attrs + extra_required:
         if key_attr in safe_to_ignore:
            continue 
         
         if key_attr not in ent_attrs.keys():
            needed.append( key_attr )

      if len(needed) > 0:
         logging.error( "Missing call attrs: %s" % (",".join(needed) ) )
         return -errno.EINVAL

      return 0


   @classmethod 
   def make_dir_write_nonce( cls ):
      """
      Make a write nonce for a directory.
      """
      return random.randint( -2**63, 2**63 - 1 )
   
   
   @classmethod
   def preprocess_attrs( cls, ent_attrs ):
      # do some preprocessing on the ent attributes, autogenerating them if need be
      if ent_attrs['ftype'] == MSENTRY_TYPE_DIR:
         ent_attrs['write_nonce'] = cls.make_dir_write_nonce()
         
      ent_attrs['nonce_ts'] = cls.make_nonce_ts()
      
   
   @classmethod
   def update_shard( cls, num_shards, ent, **extra_shard_attrs ):
      """
      Update a shard for an msentry.
      Files should include an write nonce of their own.
      Directories should include a randomly-generated write nonce.
      """
      
      # generate attributes to put
      attrs = {}
      attrs.update( ent.to_dict() )
      attrs.update( extra_shard_attrs )
      
      attrs['nonce_ts'] = MSEntry.make_nonce_ts()
      
      key_name = MSEntry.make_key_name( ent.volume_id, ent.file_id )
      shard_key = MSEntry.get_shard_key( key_name, random.randint( 0, num_shards-1 ) )
      
      shard = ent.write_shard
      if shard == None:
         shard = MSEntry.shard_class( key=shard_key )
      
      MSEntry.populate_shard_inst( ent, shard, **attrs )
      
      ent.write_shard = shard
      
      return shard
   
   
   @classmethod
   def update_shard_async( cls, num_shards, ent, **extra_shard_attrs ):
      shard = MSEntry.update_shard( num_shards, ent, **extra_shard_attrs )

      shard_fut = shard.put_async()
      return shard_fut
      
      
   @classmethod
   def IndexPropagate( cls, volume_id, parent_id, file_id, num_shards, attempt_count, start_time, total_attempt_count, was_deferred ):
      """
      Propagate information from a Create to the directory index and to the entry.
      Return 0 on success
      Return -ENOENT if the parent or child disappeared
      Return -EAGAIN if we failed to insert in the allotted amount of attempts
      Do this as a deferred task.
      
      """
      
      num_attempts = 0
      next_index = None
      parent_capacity = 1
      
      
      try:
         
         while attempt_count < 0 or num_attempts < attempt_count:
            
            num_attempts += 1
            total_attempt_count += 1
            
            try:
               
               # find the new capacity
               num_children_fut = MSEntryIndex.GetNumChildren( volume_id, parent_id, num_shards, async=True )
               parent_fut = MSEntry.ReadBase( volume_id, parent_id, async=True )
               
               storagetypes.wait_futures( [parent_fut, num_children_fut] )
               
               num_children = num_children_fut.get_result()
               parent_ent = parent_fut.get_result()
               
               # capacity to go with 
               parent_capacity = 0
               
               if num_children > parent_ent.capacity / 2:
                  
                  # try to double the parent capacity 
                  rc = MSEntry.parent_capacity_try_double( volume_id, parent_id, num_shards, parent_ent.capacity )
                  if rc < 0:
                     
                     return rc
                  
                  else:
                     parent_capacity = rc

                  logging.debug("doubled capacity of /%s/%s to %s" % (volume_id, parent_id, parent_capacity) )
                  
               else:
                  
                  parent_capacity = parent_ent.capacity
                  
               if next_index is None:
                   
                  # choose a new index
                  free_gaps = MSEntryIndex.FindFreeGaps( volume_id, parent_id, num_children + 1 )
                  
                  if len(free_gaps) > 0:
                     next_index = free_gaps[ random.randint( 0, len(free_gaps) - 1 ) ]
                  
                  else:
                     next_index = random.randint( num_children, parent_capacity - 1 )
               
               # try to allocate a slot 
               rc, next_dir_index_or_generation = MSEntryIndex.TryInsert( volume_id, parent_id, file_id, next_index, parent_capacity, num_shards )
               
               if rc != 0:
                  
                  logging.debug("Retry inserting /%s/%s (rc = %s)" % (volume_id, file_id, rc) )
                  
                  next_index = next_dir_index_or_generation
                  
                  continue
               
               else:
                  
                  generation = next_dir_index_or_generation
                  
                  # succeeded! update the entry with its generation
                  ent = MSEntry.ReadBase( volume_id, file_id )
                  
                  if ent is None or ent.deleted:
                     # nothing to do 
                     return -errno.ENOENT 
                  
                  ent.generation = generation 
                  
                  ent.put()
                  
                  logging.debug("\n=== Inserted /%s/%s into /%s/%s (attempts=%s) (deferred=%s) (time=%s)\n" % (volume_id, file_id, volume_id, parent_id, total_attempt_count, was_deferred, storagetypes.get_time() - start_time) )
                  
                  return 0
         
            except storagetypes.TransactionFailedError, tfe:
               
               logging.error("Transaction failed in propagating index for /%s/%s child=%s; trying again" % (volume_id, parent_id, file_id))
               continue 
            
            except Exception, e:
               
               logging.exception(e)
               return -errno.EIO
         
         
      except storagetypes.RequestDeadlineExceededError, rdee:
         
         # try again--deadline exceeded 
         storagetypes.deferred.defer( MSEntry.IndexPropagate, volume_id, parent_id, file_id, num_shards, int(math.log( parent_capacity + 1, 2 )) + 1, start_time, total_attempt_count, True )
         
         return -errno.EAGAIN
      
      except Exception, e:
         
         logging.exception(e)
      
      
      logging.debug("Defer insert /%s/%s" % (volume_id, file_id) )
      
      # try again--deadline exceeded 
      storagetypes.deferred.defer( MSEntry.IndexPropagate, volume_id, parent_id, file_id, num_shards, int(math.log( parent_capacity + 1, 2 )) + 1, start_time, total_attempt_count, True )
      


   @classmethod
   def parent_capacity_try_double( cls, volume_id, parent_id, num_shards, old_parent_capacity ):
      """
      Attempt to atomically double the capacity of a directory's index.
      Return the new capacity on success.
      Return -ENOENT if the directory does not exist.
      """
      def txn():
         # transactionally put the new capacity, but off the critical path 
         parent_base = MSEntry.ReadBase( volume_id, parent_id, use_memcache=False )
         
         if parent_base is None or parent_base.deleted:
            return -errno.ENOENT 
         
         if parent_base.capacity != old_parent_capacity:
            return parent_base.capacity 
         
         parent_base.capacity *= 2 
         parent_base.put()
         
         parent_cache_key_name = MSEntry.make_key_name( volume_id, parent_id )
         storagetypes.memcache.delete( parent_cache_key_name )
         
         return parent_base.capacity
      
      new_capacity = storagetypes.transaction( txn )
      
      whole_parent = cls.Read( None, parent_id, volume_id=volume_id, num_shards=num_shards )
      
      if whole_parent is None:
         return new_capacity 
      
      # update parent write nonce as well, so clients discover it (i.e. only write a new shard)
      cls.__write_msentry( whole_parent, num_shards, write_nonce=random.randint( -2**63, 2**63 - 1 )  )
      
      return new_capacity
   
   
   @classmethod
   def undo_create( cls, volume_id, parent_id, num_shards, keys_to_delete ):
      """
      Undo a create:
      * delete the list of keys created by the Create()
      * decrement the number of children
      
      Call this as a deferred task.
      """
      
      MSEntry.delete_all( keys_to_delete )
      MSEntryIndex.NumChildrenDec( volume_id, parent_id, num_shards )
      
      storagetypes.memcache.delete( MSEntry.make_key_name( volume_id, parent_id ) )
   
      
   @classmethod
   def Create( cls, user_owner_id, volume, **ent_attrs ):
      
      # return the file_id on success
      # coerce fields
      ent_attrs['volume_id'] = volume.volume_id
      ent_attrs['capacity'] = cls.get_default('capacity', ent_attrs)

      # only mutate fields allowed
      rc = MSEntry.check_mutate_attrs( ent_attrs )
      if rc != 0:
         return (rc, None)

      # ensure we have every required attribute
      parent_id = ent_attrs['parent_id']
      MSEntry.fill_defaults( ent_attrs )
      
      # necessary input
      missing = MSEntry.find_missing_attrs( ent_attrs )
      if len(missing) > 0:
         logging.error("missing: %s" % missing)
         return (-errno.EINVAL, None)

      # get child vitals
      child_name = ent_attrs["name"]
      child_id = ent_attrs["file_id"]
      volume_id = volume.volume_id

      # valid input
      invalid = MSEntry.validate_fields( ent_attrs )
      if len(invalid) > 0:
         logging.error("not allowed: %s" % invalid)
         return (-errno.EINVAL, None)

      # get a Child ID
      if child_name == '/':
         # are we creating root?
         child_id = 0
         ent_attrs['file_id'] = '0000000000000000'

      if child_id == 0 and user_owner_id != 0 and volume.owner_id != user_owner_id:
         # can't create root if we don't own the Volume, or aren't admin
         return (-errno.EACCES, None)
      
      # get the parent entry outside of the transaction
      parent_ent = None
      child_ent = None
      nameholder = None
      parent_fut = None
      child_fut = None
      parent_key_name = MSEntry.make_key_name( volume_id, parent_id )
      child_key_name = MSEntry.make_key_name( volume_id, child_id );
      nameholder_key_name = MSEntryNameHolder.make_key_name( volume_id, parent_id, ent_attrs['name'] )
      nameholder_key = storagetypes.make_key( MSEntryNameHolder, nameholder_key_name )
      futs = []

      parent_cache_key_name = MSEntry.make_key_name( volume_id, parent_id )
      child_cache_key_name = MSEntry.make_key_name( volume_id, child_id )

      try:
         parent_ent = storagetypes.memcache.get( parent_cache_key_name )
         child_ent = storagetypes.memcache.get( child_cache_key_name )
         
         # try to get the child--it shouldn't exist 
         if child_ent is not None:
            return (-errno.EEXIST, None)
         else:
            child_fut = MSEntry.Read( volume, child_id, futs_only=True )
            futs.append( MSEntry.FlattenFuture( child_fut ) )
            
         if parent_ent is None:
            parent_fut = MSEntry.Read( volume, parent_id, futs_only=True )
            futs.append( MSEntry.FlattenFuture( parent_fut ) )
            
         # do some preprocessing on the ent attributes...
         MSEntry.preprocess_attrs( ent_attrs )
         
         # try to add a nameholder (should create a new one)
         nameholder_fut = MSEntryNameHolder.create_async( volume_id, parent_id, child_id, ent_attrs['name'] )
         futs.append( nameholder_fut )
         
         storagetypes.wait_futures( futs )
         
         # get futures...
         if child_fut is not None:
            child_ent = MSEntry.FromFuture( child_fut )
            
            if child_ent is not None:
               return (-errno.EEXIST, None)
               
         if parent_ent is None:
            parent_ent = MSEntry.FromFuture( parent_fut )
         
         nameholder = nameholder_fut.get_result()
      
      except storagetypes.RequestDeadlineExceededError:
         
         # roll back 
         storagetypes.deferred.defer( MSEntryNameHolder.delete_all, [nameholder_key] )
         return (-errno.ETIMEDOUT, None)
      
      
      # if parent was deleted, then roll back
      if parent_ent is None or parent_ent.deleted:
         storagetypes.deferred.defer( MSEntry.delete_all, [nameholder.key] )
         logging.debug("Parent /%s/%s does not exist" % (volume_id, parent_id ) )
         return (-errno.ENOENT, None)

      # if parent isn't writeable, then roll back
      if not is_writable( user_owner_id, volume_id, parent_ent.owner_id, parent_ent.mode ):
         storagetypes.deferred.defer( MSEntry.delete_all, [nameholder.key] )
         return (-errno.EACCES, None)
      
      # check for namespace collision
      if nameholder.file_id != child_id or nameholder.parent_id != parent_id or nameholder.volume_id != volume_id or nameholder.name != ent_attrs['name']:
         # nameholder already existed
         logging.error("/%s/%s, parent_id=%s, name=%s exists (nameholder: /%s/%s, parent_id=%s, name=%s)" % \
             (volume_id, child_id, parent_id, ent_attrs['name'], nameholder.volume_id, nameholder.file_id, nameholder.parent_id, nameholder.name))
         
         return (-errno.EEXIST, None)
      
      # no namespace collision.  Create the child
      
      # cache parent...
      storagetypes.memcache.add( parent_cache_key_name, parent_ent )
      
      delete = False 
      index_fut = None 
      
      # need to try/catch timeouts here, so we can queue decrement and rollback on timeout
      try:
         
         futs = []
         ret = 0
         
         child_ent = MSEntry( key=storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id, child_id ) ) )
         child_ent.populate( volume.num_shards, **ent_attrs )
         
         parent_shard = MSEntry.update_shard( volume.num_shards, parent_ent, write_nonce=random.randint(-2**63, 2**63-1) )
         
         futs = storagetypes.put_multi_async( [child_ent, child_ent.write_shard, parent_shard] )
         
         # make sure parent still exists
         parent_ent = storagetypes.memcache.get( parent_cache_key_name )
         
         if parent_ent is None:
            parent_ent = MSEntry.Read( volume, parent_id )
         
         if parent_ent is None or parent_ent.deleted:
            # roll back...
            logging.debug("Parent /%s/%s was deleted out from under us" % (volume_id, parent_id ) )
            delete = True 
            ret = -errno.ENOENT
         
         if not delete:
            fut = MSEntryIndex.NumChildrenInc( volume_id, parent_id, volume.num_shards, async=True )
            futs.append( fut )
         
            # wait for operations to finish...
            storagetypes.wait_futures( futs )
         
      except storagetypes.RequestDeadlineExceededError:
         
         storagetypes.deferred.defer( MSEntry.delete_all, [nameholder.key, child_ent.key, child_ent.write_shard.key] )
         
         return (-errno.ETIMEDOUT, None)
      
      finally:
         
         if delete:
            # roll back on error
            storagetypes.deferred.defer( MSEntry.undo_create, volume_id, parent_id, volume.num_shards, [nameholder.key, child_ent.key, child_ent.write_shard.key] )
            
         else:
            # increment children count 
            MSEntry.IndexPropagate( volume_id, parent_id, child_id, volume.num_shards, 5, storagetypes.get_time(), 0, False )
            
            
         # invalidate caches
         storagetypes.memcache.delete_multi( [MSEntry.make_key_name( volume_id, parent_id ) ] )
         
      
      return (ret, child_ent)


   @classmethod 
   def make_nonce_ts( cls ):
      """
      Create a nonce timestamp
      """
      now_sec, now_nsec = storagetypes.clock_gettime()
      nonce_ts = now_sec * 100000 + now_nsec / 10000        # 10-microsecond units
      return nonce_ts
   

   @classmethod
   def MakeRoot( cls, user_owner_id, volume, root_protobuf ):
      """
      Create a Volume's root directory.
      NOTE: root_protobuf should come from an ms_volume_metadata structure, whose authenticity should have 
      been verified by the caller.
      
      Return 0 on success
      """
      root_attrs = cls.unprotobuf_dict( root_protobuf )
      
      root_key_name = MSEntry.make_key_name( volume.volume_id, root_attrs['file_id'] )
      root = MSEntry( key=storagetypes.make_key( MSEntry, root_key_name ) )
      
      root.populate( volume.num_shards, **root_attrs )
      
      root_fut = MSEntry.__write_msentry_async( root, volume.num_shards, write_base=True, **root_attrs )
      root_nameholder_fut = MSEntryNameHolder.create_async( volume.volume_id, root_attrs['parent_id'], root_attrs['file_id'], root_attrs['name'] )
      
      storagetypes.wait_futures( [root_fut, root_nameholder_fut] )

      return 0
  

   @classmethod
   @storagetypes.concurrent
   def __write_msentry_async( cls, ent, num_shards, write_base=False, **write_attrs ):
      """
      Update and then put an entry if it is changed.  Always put a shard.
      If write_base is True, always write the base entry and a shard, even if the base is not affected.
      """
      
      # do some preprocessing on the ent attributes...
      write_attrs['ftype'] = ent.ftype
      MSEntry.preprocess_attrs( write_attrs )
      
      # necessary, since the version can change (i.e. on a truncate)
      for write_attr in write_attrs.keys():
         if write_attr in MSEntry.shard_fields:
            continue
         
         if getattr( ent, write_attr, None ) != write_attrs[write_attr]:
            write_base = True
            ent.populate_base( **write_attrs )
            break
         
      
      # make a new shard
      if 'file_id' in write_attrs:
          del write_attrs['file_id']
       
      if 'volume_id' in write_attrs:
          del write_attrs['volume_id']
          
      ent.populate_shard( num_shards, volume_id=ent.volume_id, file_id=ent.file_id, **write_attrs )

      if write_base:
         yield storagetypes.put_multi_async( [ent, ent.write_shard] )
      else:
         yield ent.put_shard_async()
      
      cache_ent_key = MSEntry.make_key_name( ent.volume_id, ent.file_id )
      
      # invalidate cached items
      storagetypes.memcache.delete( cache_ent_key )
      
      storagetypes.concurrent_return( ent )
   
   
   @classmethod
   def __write_msentry( cls, ent, num_shards, write_base=False, async=False, **write_attrs ):
      write_fut = MSEntry.__write_msentry_async( ent, num_shards, write_base=write_base, **write_attrs )
      
      if not async:
         return write_fut.get_result()
      
      else:
         return write_fut


   @classmethod
   def Update( cls, user_owner_id, volume, gateway, **ent_attrs ):

      # it's okay if we aren't given the parent_id here...
      rc = MSEntry.check_mutate_attrs( ent_attrs, safe_to_ignore=['parent_id', 'xattr_hash'] )
      if rc != 0:
         logging.error("check_mutate_attrs rc = %s" % rc)
         return (rc, None)
      
      # Update an MSEntry.
      # A file will be updated by at most one gateway, so we don't need a transaction.  File updates must only come from the designated coordinator.
      # A directory can be updated by anyone, so the MS applies update conflict resolution with last-write-wins on a shard.
      
      write_attrs = {}
      write_attrs.update( ent_attrs )
      
      volume_id = volume.volume_id
      file_id = ent_attrs['file_id']
      
      not_writable = MSEntry.validate_write( write_attrs.keys() )
      for nw in not_writable:
         del write_attrs[nw]

      # get the ent
      # try from cache first
      cache_ent_key = MSEntry.make_key_name( volume_id, file_id )

      ent = storagetypes.memcache.get( cache_ent_key )
      if ent == None:
         # not in the cache.  Get from the datastore
         ent_fut = MSEntry.__read_msentry( volume_id, file_id, volume.num_shards, use_memcache=False )
         ent = ent_fut.get_result()

      if ent == None or ent.deleted:
         return (-errno.ENOENT, None)

      # a file's coordinator must match the gateway
      if ent.ftype == MSENTRY_TYPE_FILE and ent.coordinator_id != gateway.g_id:
         # not the coordinator--refresh
         return (-errno.EAGAIN, None)
      
      # does this user have permission to write?
      if not is_writable( user_owner_id, volume.owner_id, ent.owner_id, ent.mode ):
         return (-errno.EACCES, None)
      
      # can't have a lesser write-nonce for files
      if ent.ftype == MSENTRY_TYPE_FILE and ent_attrs['write_nonce'] <= ent.write_nonce:
         logging.error("File: Write nonce is %s, expected > %s" % (ent_attrs['write_nonce'], ent.write_nonce))
         return (-errno.EINVAL, None)
     
      # can't have an identical write-nonce for dirs 
      if ent.ftype == MSENTRY_TYPE_DIR and ent_attrs['write_nonce'] == ent.write_nonce:
         logging.error("Dir: Write nonce is %s, expected != %s" % (ent_attrs['write_nonce'], ent.write_nonce))
         return (-errno.EINVAL, None)
      
      # if we're going to change mode, then we must own the ent 
      if ent_attrs['mode'] != ent.mode and user_owner_id != ent.owner_id:
         return (-errno.EACCES, None)
      
      # if we're going to change owner, then we must own the ent 
      if ent_attrs['owner_id'] != ent.owner_id and user_owner_id != ent.owner_id:
         return (-errno.EACCES, None)
      
      # if we're going to decrease the size, then we must include a new version 
      if ent_attrs['size'] < ent.size and not ent_attrs.has_key('version'):
         logging.error("Size decreased but no version given")
         return (-errno.EINVAL, None)
     
      # write the update
      ent_fut = MSEntry.__write_msentry( ent, volume.num_shards, async=True, **write_attrs )
      
      storagetypes.wait_futures( [ent_fut] )
      
      ent = ent_fut.get_result()
      
      return (0, ent)


   @classmethod
   def Chcoord( cls, user_owner_id, gateway, volume, **attrs ):
      """
      Switch coordinators.
      Performs a transaction--either the chcoord happens, or the caller learns the current coordinator.
      The version of the entry must increment.
      """
      
      # it's okay if we don't include the parent_id, but we do need a new version
      rc = MSEntry.check_mutate_attrs( attrs, safe_to_ignore=['parent_id'], extra_required=['version', 'xattr_hash'] )
      if rc != 0:
         return (rc, None)
      
      # does this gateway have coordination capability?
      if gateway.check_caps( GATEWAY_CAP_COORDINATE ) == 0:
         return (-errno.EACCES, None)
      
      # vet the write
      file_id = attrs['file_id']
      current_coordinator_id = attrs['coordinator_id']
      current_write_nonce = attrs['write_nonce']
      current_version = attrs['version']
      
      write_attrs = {}
      write_attrs.update( attrs )
      
      not_writable = MSEntry.validate_write( write_attrs.keys() )
      for nw in not_writable:
         del write_attrs[nw]
      
      write_attrs['coordinator_id'] = gateway.g_id

      def chcoord_txn( file_id, current_coordinator_id, volume, gateway, **attrs ):
         volume_id = volume.volume_id
         
         # get the ent
         ent_fut = MSEntry.__read_msentry_base( volume_id, file_id, use_memcache=False )
         ent = ent_fut.get_result()

         if ent == None or ent.deleted:
            return (-errno.ENOENT, None)
        
         # only valid for files 
         if ent.ftype != MSENTRY_TYPE_FILE:
            return (-errno.EPERM, None)

         # does this user have permission to write?
         if not is_writable( user_owner_id, volume.owner_id, ent.owner_id, ent.mode ):
            return (-errno.EACCES, None)
         
         # only allow the change if the requesting gateway knows the current coordinator.
         if current_coordinator_id != ent.coordinator_id:
            return (0, ent)
        
         # only allow the change if the requesting gateway has a write nonce (must be greater than the one on file)
         if current_write_nonce <= ent.write_nonce:
            return (0, ent)
         
         # only allow the change if the requesting gateway knows the current xattr hash 
         if current_xattr_hash != ent.xattr_hash:
            return (0, ent)
         
         # only allow the change if the requesting gateway has a valid version (must be greater than the one on file)
         if current_version <= ent.version:
            return (0, ent)
         
         # otherwise, allow the change
         ent.coordinator_id = gateway.g_id
         
         ent = MSEntry.__write_msentry( ent, volume.num_shards, write_base=True, **attrs )
         
         return (0, ent)
      
      
      rc, ent = storagetypes.transaction( lambda: chcoord_txn( file_id, current_coordinator_id, volume, gateway, **write_attrs ), xg=True )
      return (rc, ent)
      
   
   @classmethod
   @storagetypes.concurrent
   def __rename_verify_no_loop_async( cls, volume, absent_file_id, ent ):
      """
      Verify that an MSEntry does not appear in the path from root to a given entry.
      This is used to verify that renaming a directory would not make it its own parent.
      """
      
      volume_id = volume.volume_id
      num_shards = volume.num_shards
      
      while True:
         if ent.file_id == absent_file_id:
            # absent_file_id occurs in the path
            logging.error("loop: %s" % ent.file_id)
            storagetypes.concurrent_return( -errno.EINVAL )
         
         parent_id_int = MSEntry.serialize_id( ent.parent_id )
         if parent_id_int == 0:
            # at root; done!
            if ent.parent_id == absent_file_id:
               logging.error("loop: %s" % ent.parent_id)
               storagetypes.concurrent_return( -errno.EINVAL )
            
            else:
               storagetypes.concurrent_return( 0 )
         
         # next ent
         ent = yield MSEntry.__read_msentry( volume_id, ent.parent_id, num_shards )
         
         if ent == None:
            # this ent got removed concurrently
            storagetypes.concurrent_return( -errno.ENOENT )
      
      storagetypes.concurrent_return( 0 )
     
     
   @classmethod 
   def __read_msentry_and_parent( cls, volume_id, file_id, num_shards, use_memcache=True ):
      """
      Read both an entry and its parent.
      Check memcache if use_memcache is True
      Return (entry, parent), where either/or can be None
      """
      
      ent = None
      parent = None 
      
      if use_memcache:
         cache_ent_key = MSEntry.make_key_name( volume_id, file_id )
         ent = storagetypes.memcache.get( cache_ent_key )
      
      if ent is None:
         
         ent_fut = MSEntry.__read_msentry( volume_id, file_id, num_shards, use_memcache=False )
         storagetypes.wait_futures( [ent_fut] )
         
         ent = ent_fut.get_result()
         
         if ent is None:
            
            return (None, None)
         
      if use_memcache:
         cache_parent_key = MSEntry.make_key_name( volume_id, ent.parent_id )
         parent = storagetypes.memcache.get( cache_parent_key )
      
      if parent is None:
         
         parent_fut = MSEntry.__read_msentry( volume_id, ent.parent_id, num_shards, use_memcache=False )
         storagetypes.wait_futures( [parent_fut] )
         
         parent = parent_fut.get_result()

      return (ent, parent)
      
      
      
   @classmethod
   def Rename( cls, user_owner_id, gateway, volume, src_attrs, dest_attrs ):
      """
      Rename an MSEntry.
      src_attrs describes the file/directory to be renamed (src)
      dest_attrs describes the file/directory that will be overwritten (dest)

      dest will be put into the filesystem, and src will be deleted.
      The old dest will be returned, if overwritten.
      
      Only src's coordinator can rename it.
      """
      
      rc = MSEntry.check_mutate_attrs( src_attrs )
      if rc != 0:
         logging.error("Invalid src mutate attrs")
         return (rc, None)
      
      rc = MSEntry.check_mutate_attrs( dest_attrs )
      if rc != 0:
         logging.error("Invalid dest mutate attrs")
         return (rc, None)
      
      src_name = src_attrs['name']
      src_file_id = src_attrs['file_id']
      src_parent_id = src_attrs['parent_id']
      
      dest_file_id = 0
      dest_parent_id = dest_attrs['parent_id']
      dest_name = dest_attrs['name']

      delete_dest = False
      
      # file IDs are strings, so convert them to ints
      src_file_id_int = MSEntry.serialize_id( src_file_id )
      
      if src_file_id_int == 0:
         # not allowed to rename root
         logging.error("Cannot rename root")
         return (-errno.EINVAL, None)
      
      if len(dest_name) == 0:
         # invalid name
         logging.error("Missing dest_name")
         return (-errno.EINVAL, None)
      
      volume_id = volume.volume_id
      futs = []
      ents = {}
      
      # find dest 
      dest_nameholder = MSEntryNameHolder.Read( volume.volume_id, dest_parent_id, dest_name )
      if dest_nameholder is not None:
          dest_file_id = dest_nameholder.file_id 
          if dest_file_id == 0:
              # can't rename over root
              logging.error("Cannot rename over root")
              return (-errno.EINVAL, None)


      ents_to_get = [src_file_id, src_parent_id, dest_parent_id]
      if dest_file_id != 0:
         ents_to_get.append( dest_file_id )
         
      # get all entries
      for fid in ents_to_get:
         cache_ent_key = MSEntry.make_key_name( volume_id, fid )
         ent = storagetypes.memcache.get( cache_ent_key )
         
         if ent == None:
            ent_fut = MSEntry.__read_msentry( volume_id, fid, volume.num_shards, use_memcache=False )
            futs.append( ent_fut )
         
         else:
            ents[fid] = ent
      
      
      if len(futs) > 0:
         storagetypes.wait_futures( futs )
         
         for fut in futs:
            ent = fut.get_result()
            if ent:
               ents[ ent.file_id ] = ent
               
      src = ents.get( src_file_id, None )
      dest = ents.get( dest_file_id, None )
      src_parent = ents.get( src_parent_id, None )
      dest_parent = ents.get( dest_parent_id, None )
      
      # does src exist?
      if src is None:
         return (-errno.ENOENT, None)
      
      # file rename request originated from src's coordinator
      if src.ftype == MSENTRY_TYPE_FILE and src.coordinator_id != gateway.g_id:
         # not the coordinator--refresh
         return (-errno.EAGAIN, None)

      # src and dest must match on most fields... 
      if len(src_attrs.keys()) != len(dest_attrs.keys()):
          logging.error("Src/Dest mismatch: different field sets")
          return (-errno.EINVAL, None)

      missing_src = []
      missing_dest = []
      for field in src_attrs.keys():
          if field not in dest_attrs:
              missing_dest.append(field)

      for field in dest_attrs.keys():
          if field not in src_attrs:
              missing_src.append(fied)

      if len(missing_src) != 0 or len(missing_dest) != 0:
          logging.error("Src/Dest mismatch: missing in src=%s, missing in dest=%s" % (",".join(missing_src), ",".join(missing_dest)))
          return (-errno.EINVAL, None)

     
      # must be fresh 
      if src.version > src_attrs['version']:
          # stale 
          return (-errno.EAGAIN, None )

      if src.write_nonce > src_attrs['write_nonce']:
          # stale 
          return (-errno.EAGAIN, None )

      if src.xattr_nonce > src_attrs['xattr_nonce']:
          # stale 
          return (-errno.EAGAIN, None )

      # does dest parent exist?
      if dest_parent is None:
         return (-errno.ENOENT, None)
      
      # does src parent exist?
      if src_parent is None:
         return (-errno.ENOENT, None)
     
      # src vs src from db... 
      if src_file_id != src.file_id:
         logging.error("Mismatch src file ID")
         return (-errno.EINVAL, None)

      # src parent vs src parent from db... 
      if src_parent.file_id != src_parent_id:
         logging.error("Mismatch src parent ID")
         return (-errno.EINVAL, None)

      # src parent matches src?
      if src_parent.file_id != src_parent_id:
         logging.error("Mismatch src parent ID")
         return (-errno.EINVAL, None)
      
      # dest parent matches dest, if dest exists?
      if dest is not None and dest.parent_id != dest_parent.file_id:
         logging.error("Mismatch dest parent ID")
         return (-errno.EINVAL, None)
      
      # src read permssion check
      if not is_readable( user_owner_id, volume.owner_id, src.owner_id, src.mode ):
         return (-errno.EACCES, None)

      # src parent write permission check
      if not is_writable( user_owner_id, volume.owner_id, src_parent.owner_id, src_parent.mode ):
         return (-errno.EACCES, None)
      
      # dest parent write permission check
      if not is_writable( user_owner_id, volume.owner_id, dest_parent.owner_id, dest_parent.mode ):
         return (-errno.EACCES, None)
      
      # if dest exists, it must be writable
      if dest is not None and not is_writable( user_owner_id, volume.owner_id, dest.owner_id, dest.mode ):
         return (-errno.EACCES, None)
     
      if dest is not None and src.ftype != dest.ftype:
         # types must match 
         if src.ftype == MSENTRY_TYPE_DIR:
            return (-errno.ENOTDIR, None)
         else:
            return (-errno.EISDIR, None)
      
      dest_delete_fut = None
      src_verify_absent = None
      
      # if dest exists and is distinct from src, proceed to delete it.
      if dest is not None and dest.file_id != src.file_id:
         delete_dest = True
         dest_delete_fut = MSEntry.__delete_begin_async( volume, dest )
      
      # while we're at it, make sure we're not moving src to a subdirectory of itself
      src_verify_absent = MSEntry.__rename_verify_no_loop_async( volume, src_file_id, dest_parent )
      
      # gather results
      futs = [src_verify_absent]
      if dest_delete_fut != None:
         futs.append( dest_delete_fut )
      
      storagetypes.wait_futures( futs )
      
      # check results...
      src_absent_rc = src_verify_absent.get_result()
      dest_empty_rc = 0
      
      if dest_delete_fut != None:
         dest_empty_rc = dest_delete_fut.get_result()
      
      if dest_empty_rc != 0:
         # dest is not empty, but we were about to rename over it
         if delete_dest:
            MSEntry.__delete_undo( dest )
           
         logging.error("dest is not empty")
         return (dest_empty_rc, None)
      
      if src_absent_rc != 0:
         # src is its own parent
         if delete_dest:
            MSEntry.__delete_undo( dest )
         
         logging.error("src is its own parent")
         return (src_absent_rc, None)
      
      # put a new nameholder for src
      new_nameholder_fut = MSEntryNameHolder.create_async( volume_id, dest_parent_id, src_file_id, dest_name )
      
      if dest is not None:
         dest_delete_fut = MSEntry.__delete_finish_async( volume, dest_parent, dest )
      
      src_write_fut = MSEntry.__write_msentry_async( src, volume.num_shards, write_base=True, **dest_attrs )
      
      futs = [src_write_fut, new_nameholder_fut]
      
      if dest_delete_fut != None:
         futs.append( dest_delete_fut )
      
      # remove src's old nameholder
      old_nameholder_key = storagetypes.make_key( MSEntryNameHolder, MSEntryNameHolder.make_key_name( volume_id, src_parent_id, src_name ) )
      storagetypes.deferred.defer( MSEntry.delete_all, [old_nameholder_key] )
      
      # wait for the operation to complete
      storagetypes.wait_futures( futs )
      
      # clean up cache
      cache_delete = [
         MSEntry.make_key_name( volume_id, src_parent_id ),
         MSEntry.make_key_name( volume_id, dest_parent_id ),
         MSEntry.make_key_name( volume_id, src_file_id ),
         MSEntry.make_key_name( volume_id, dest_file_id ),
         MSEntryNameHolder.make_key_name( volume_id, src_file_id, src_attrs['name'] ),
         MSEntryNameHolder.make_key_name( volume_id, dest_file_id, dest_attrs['name'] )
      ]
      
      storagetypes.memcache.delete_multi( cache_delete )
     
      return (0, dest)
      
   
   @classmethod
   @storagetypes.concurrent
   def __delete_begin_async( cls, volume, ent ):
      """
      Begin deleting an entry by marking it as deleted.
      Verify that it is empty if it is a directory.
      """
      
      ent_cache_key_name = MSEntry.make_key_name( ent.volume_id, ent.file_id )
      ent_key_name = MSEntry.make_key_name( ent.volume_id, ent.file_id )
      
      # mark as deleted.  If this is a directory, creates will fail from now on
      ent.deleted = True
      yield ent.put_async()
      
      # clear from cache
      storagetypes.memcache.delete( ent_cache_key_name )
      
      # make sure that ent, if it is a directory, was actually empty
      if ent.ftype == MSENTRY_TYPE_DIR:
         
         # how many children are there?
         num_expected_children = MSEntryIndex.GetNumChildren( volume.volume_id, ent.file_id, volume.num_shards )
         
         if num_expected_children > 0:
            
            logging.debug("Directory /%s/%s (%s) has %s children" % (ent.volume_id, ent.file_id, ent.name, num_expected_children))
            
            # not empty
            yield MSEntry.__delete_undo_async( ent )

            # uncache
            storagetypes.memcache.delete( ent_cache_key_name )
            
            # not empty
            storagetypes.concurrent_return( -errno.ENOTEMPTY )
         
               
      # otherwise, ent is a file.  Make sure there are no outstanding writes that need to be vacuumed 
      else:
         
         # log check---there must be no outstanding writes 
         vacuum_log_head_list = yield MSEntryVacuumLog.Peek( ent.volume_id, ent.file_id, async=True )
         
         if vacuum_log_head_list is not None and len(vacuum_log_head_list) != 0:
            
            # outstanding unvacuumed writes...must undo 
            yield MSEntry.__delete_undo_async( ent )
            
            # uncache 
            storagetypes.memcache.delete( ent_cache_key_name )
            
            # not permitted 
            storagetypes.concurrent_return( -errno.EAGAIN )
      
      # safe to delete
      storagetypes.concurrent_return( 0 )
      
      
   @classmethod
   def __delete_begin( cls, volume, ent ):
      delete_fut = MSEntry.__delete_begin_async( volume, ent )
      rc = delete_fut.get_result()
      return rc
   
   @classmethod 
   def __compactify_continuation_uncache( cls, replaced_index=None, compacted_index_node=None ):
      """
      Continuation called once a node in the index is compactified.
      Leveraged here to uncache the now-invalid page data.
      """
      
      if compacted_index_node is None:
         return 
      
      # uncache the affected entries
      updated_child_cache_key = MSEntry.make_key_name( compacted_index_node.volume_id, compacted_index_node.file_id )
      
      storagetypes.memcache.delete( updated_child_cache_key )
         
   
   @classmethod
   @storagetypes.concurrent
   def __delete_finish_async( cls, volume, parent_ent, ent ):
      """
      Finish deleting an entry.
      Remove it and its nameholder and xattrs, update its parent's shard, and update the index
      """

      if not ent.deleted:
         logging.error("Entry is not yet deleted: %s" % str(ent))
         storagetypes.concurrent_return( -errno.EINVAL )
      
      volume_id = volume.volume_id
      parent_id = parent_ent.file_id
      
      ent_key_name = MSEntry.make_key_name( volume_id, ent.file_id )
      ent_shard_keys = MSEntry.get_shard_keys( volume.num_shards, ent_key_name )
      ent_cache_key_name = MSEntry.make_key_name( volume_id, ent.file_id )
      
      # get the index node...
      ent_idx = yield MSEntryIndex.ReadIndex( volume_id, ent.file_id, async=True )
      
      # update parent status and free the dead child's dir index
      yield MSEntry.update_shard_async( volume.num_shards, parent_ent, write_nonce=random.randint(-2**63, 2**63-1) )
      
      ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id, ent.file_id ) )
      nh_key_name = MSEntryNameHolder.make_key_name( volume_id, parent_id, ent.name )
      nh_key = storagetypes.make_key( MSEntryNameHolder, nh_key_name )
      
      # queue delete this entry and its nameholders
      storagetypes.deferred.defer( MSEntry.delete_all, [nh_key, ent_key] + ent_shard_keys )
      
      # queue delete xattrs
      storagetypes.deferred.defer( MSEntryXAttr.Delete_ByFile, volume.volume_id, ent.file_id )
      
      # uncache any listings of this parent
      storagetypes.memcache.delete_multi( [MSEntry.make_key_name( volume_id, parent_id ), ent_cache_key_name, nh_key_name] )
        
      # compactify the parent's directory index
      MSEntryIndex.Delete( volume_id, parent_id, ent_idx.file_id, ent_idx.dir_index, volume.num_shards, compactify_continuation=MSEntry.__compactify_continuation_uncache )
      
      storagetypes.concurrent_return( 0 )
      
      
   @classmethod
   def __delete_finish( cls, volume, parent_ent, ent ):
      delete_fut = MSEntry.__delete_finish_async( volume, parent_ent, ent )
      rc = delete_fut.get_result()
      return rc
   
   @classmethod
   @storagetypes.concurrent
   def __delete_undo_async( cls, ent ):
      ent.deleted = False
      yield ent.put_async()
      storagetypes.concurrent_return( 0 )

   @classmethod
   def __delete_undo( cls, ent ):
      delete_undo_fut = MSEntry.__delete_undo( ent )
      delete_undo_fut.get_result()
      return 0

   @classmethod
   def Delete( cls, user_owner_id, volume, gateway, **ent_attrs ):
      
      # delete an MSEntry.
      # A file will be deleted by at most one UG
      # A directy can be deleted by anyone, and it must be empty
      # only ent_attrs['file_id', 'volume_id', and 'name'] need to be given.

      volume_id = volume.volume_id
      
      file_id = ent_attrs['file_id']
      
      ent, parent_ent = MSEntry.__read_msentry_and_parent( volume_id, file_id, volume.num_shards )
      
      # sanity check
      if ent is None or parent_ent is None:
         return -errno.ENOENT

      # permission checks...
      if not is_writable( user_owner_id, volume.owner_id, parent_ent.owner_id, parent_ent.mode ):
         return -errno.EACCES

      if not is_writable( user_owner_id, volume.owner_id, ent.owner_id, ent.mode ):
         return -errno.EACCES

      # coordinator check for files 
      if ent.ftype == MSENTRY_TYPE_FILE and ent.coordinator_id != gateway.g_id:
         return -errno.EAGAIN 
      
      # sanity check
      if parent_ent.ftype != MSENTRY_TYPE_DIR:
         return -errno.ENOTDIR
      
      ret = 0
      
      rc = MSEntry.__delete_begin( volume, ent )
      if rc == 0:
         ret = MSEntry.__delete_finish( volume, parent_ent, ent )
      else:
         ret = rc
         
      return ret


   @classmethod 
   def Purge( cls, volume, ent ):
      """
      Obliterate a single MSEntry record.  This also includes:
      * all of its xattrs
      * its nameholder
      * its shards
      * its index data
      
      NOTE: no effort is made to keep the metadata consistent.
      This is what makes this method different from Delete().  Don't 
      use this method for anything besides mass-erasure of metadata,
      like when deleting an AG or a Volume.
      """
      
      volume_id = volume.volume_id 
      
      futs = []
      
      ent_key_name = MSEntry.make_key_name( volume_id, ent.file_id )
      ent_cache_key_name = MSEntry.make_key_name( volume_id, ent.file_id )
      ent_shard_keys = MSEntry.get_shard_keys( volume.num_shards, ent_key_name )
      ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id, ent.file_id ) )
      nh_key = storagetypes.make_key( MSEntryNameHolder, MSEntryNameHolder.make_key_name( volume_id, ent.parent_id, ent.name ) )
      
      # delete entry and nameholder and shards
      for key in [nh_key, ent_key] + ent_shard_keys:
         fut = key.delete_async()
         futs.append( fut )
      
      # delete xattrs
      fut = MSEntryXAttr.Delete_ByFile( volume_id, ent.file_id, async=True )
      futs.append( fut )
      
      # blow away the index entries
      fut_list = MSEntryIndex.Purge( volume_id, ent.parent_id, ent.file_id, volume.num_shards, async=True )
      futs += fut_list
      
      storagetypes.wait_futures( futs )
      
      # uncache any listings of this parent
      storagetypes.memcache.delete_multi( [MSEntry.make_key_name( volume_id, ent.parent_id ), ent_cache_key_name] )
      
      logging.debug("Purged entry /%s/%s" % (volume.volume_id, ent.file_id) )
      
      return True 
      
         
   @classmethod 
   def PurgeAll( cls, volume ):
      """
      deferred deletion for volume data.
      delete blocks of entries at a time.
      re-enqueue ourselves in the deferred task queue if we run out of time.
      """
      
      def __purge_mapper( ent ):
         
         MSEntry.Purge( volume, ent )
         return True
      
      while True:
         try:
            
            # incrementally delete stuff
            ret = MSEntry.ListAll( {"MSentry.volume_id ==": volume.volume_id}, map_func=__purge_mapper, limit=50 )
            
            if len(ret) == 0:
               # out of stuff to delete 
               break
            
            logging.debug("Deleted %s entries from %s" % (len(ret), volume.volume_id) )
            
         except storagetypes.RequestDeadlineExceededError:
            storagetypes.deferred.defer( MSEntry.PurgeAll, volume )
      
      return True
      

   @classmethod
   def DeleteAll( cls, volume ):
      """
      Obliterate all MSEntry records in this Volume.
      Break this down into multiple deferred tasks.
      """
      
      # caller must own the root
      root_file_id = MSEntry.unserialize_id(0)
      
      root_ent = MSEntry.Read( volume, root_file_id )
      if root_ent is None:
         # done here
         return 0
      
      # stop further creation
      root_ent.deleted = True
      root_ent.put()
      
      # delete everything else 
      storagetypes.deferred.defer( MSEntry.PurgeAll, volume )
      return 0
   
   
   @classmethod 
   @storagetypes.concurrent 
   def __read_msentry_from_index_async( cls, dir_index_node, num_shards ):
      
      volume_id = dir_index_node.volume_id 
      file_id = dir_index_node.file_id 
      
      # check cache first 
      cache_key_name = MSEntry.make_key_name( volume_id, file_id )
      msentry = storagetypes.memcache.get( cache_key_name )
      shards = None 
      
      if msentry is None:
         msentry, shards = yield MSEntry.__read_msentry_base( volume_id, file_id ), MSEntry.__read_msentry_shards( volume_id, file_id, num_shards )
      
         if msentry is None:
            storagetypes.concurrent_return( None )
         
         msentry.populate_from_shards( shards )
         
         # set index fields 
         if msentry.ftype == MSENTRY_TYPE_DIR:
            msentry.num_children = yield MSEntryIndex.GetNumChildren( volume_id, file_id, num_shards, async=True )
            
         else:
            msentry.num_children = 0
         
         msentry.generation = dir_index_node.generation 
         
         storagetypes.memcache.set( cache_key_name, msentry )
         
      storagetypes.concurrent_return( msentry )
      
   
   @classmethod
   def ListDir( cls, volume, file_id, page_id=None, least_unknown_generation=None ):
      """
      Generate a listing of directory entries, up to RESOLVE_MAX_PAGE_SIZE.
      
      If page_id is not None, then generate the listing of entries whose dir_indexes are in the range [page_id * RESOLVE_MAX_PAGE_SIZE, (page_id + 1) * RESOLVE_MAX_PAGE_SIZE)
      If least_unknown_generation is not None, then generate a listing of entries (at most RESOLVE_MAX_PAGE_SIZE) whose generation numbers are at least as high
      
      Return (rc, listing), where rc is 0 on success, or negative on error.
      Return (0, []) if least_unknown_generation or page_id is EOF.
      """
      
      if page_id is not None and least_unknown_generation is not None:
         return (-errno.EINVAL, None)
      
      if MSEntry.is_serialized_id( file_id ):
         file_id = MSEntry.unserialize_id( file_id )
      
      volume_id = volume.volume_id
      
      # get the directory 
      dirent = MSEntry.Read( volume, file_id )
      children = []
   
      # must exist
      if dirent is None:
         return (-errno.ENOENT, None)
      
      # this had better be a directory...
      if dirent.ftype != MSENTRY_TYPE_DIR:
         logging.debug("Not a directory: %s" % file_id)
         return (-errno.ENOTDIR, None)
      
      if least_unknown_generation is not None:
         # want the next batch of entries created 
         index_query = MSEntryIndex.GenerationQuery( dirent.volume_id, dirent.file_id, least_unknown_generation, -1 )
         
         @storagetypes.concurrent 
         def walk_index( dir_index_node ):
            
            if dir_index_node is None:
               storagetypes.concurrent_return( None )
            
            # walk the index within the query 
            MSEntryIndex.SetCache( dir_index_node )
            
            msentry = yield MSEntry.__read_msentry_from_index_async( dir_index_node, volume.num_shards )
            
            storagetypes.concurrent_return( msentry )
            
         children = index_query.map( walk_index, limit=RESOLVE_MAX_PAGE_SIZE )
         
         
      elif page_id is not None:
         
         # want a page 
         dir_indexes = range( page_id * RESOLVE_MAX_PAGE_SIZE, (page_id + 1)* RESOLVE_MAX_PAGE_SIZE )
            
         # resolve an index node into the MSEntry, and cache both 
         @storagetypes.concurrent 
         def walk_index( dir_index ):
            
            if dir_index is None:
               storagetypes.concurrent_return( None )
            
            dir_index_node = yield MSEntryIndex.Read( dirent.volume_id, dirent.file_id, dir_index, async=True )
            
            if dir_index_node is None:
               storagetypes.concurrent_return( None )
            
            msentry = yield MSEntry.__read_msentry_from_index_async( dir_index_node, volume.num_shards )
            storagetypes.concurrent_return( msentry )
         
         children_futs = [ walk_index(i) for i in dir_indexes ]
         storagetypes.wait_futures( children_futs )
         
         children = [ c.get_result() for c in children_futs ]
      
      
      children = filter( lambda x: x is not None, children )
      
      logging.info("/%s/%s page=%s l.u.g.=%s: num_children=%s capacity=%s" % (dirent.volume_id, dirent.file_id, page_id, least_unknown_generation, len(children), dirent.capacity) )
      
      return (0, children)
   
   
   @classmethod
   def SetCache( cls, ent ):
      ent_cache_key_name = MSEntry.make_key_name( ent.volume_id, ent.file_id )
      storagetypes.memcache.set( ent_cache_key_name, ent )
      return 0
      

   @classmethod
   def SetCacheMulti( cls, ents ):
      ent_dict = {}
      for i in xrange(0,len(ents)):
         ent_cache_key_name = MSEntry.make_key_name( ents[i].volume_id, ents[i].file_id )
         ent_dict[ ent_cache_key_name ] = ents[i]
         
      storagetypes.memcache.set_multi( ent_dict )
      return
   
      
   @classmethod
   def ReadBase( cls, volume_id, file_id, async=False, use_memcache=True ):
      """
      Read an MSEntry without merging in its shards.
      """
      
      if MSEntry.is_serialized_id( file_id ):
         file_id = MSEntry.unserialize_id( file_id )
         
      ent_key_name = MSEntry.make_key_name( volume_id, file_id )
      ent_key = storagetypes.make_key( MSEntry, ent_key_name )
      
      ent = None
      
      if use_memcache:
         ent_cache_key_name = MSEntry.make_key_name( volume_id, file_id )
         ent = storagetypes.memcache.get( ent_cache_key_name )
         
      if ent is None:
         if async:
            return ent_key.get_async()
         else:
            ent = ent_key.get()
            
            # NOTE: don't cache, since it's only the base
            return ent

      else:
         if async:
            return storagetypes.FutureWrapper( ent )
         else:
            return ent 
         
      

   @classmethod
   def Read( cls, volume, file_id, volume_id=None, num_shards=None, futs_only=False ):
      """
      Given an entry's key information, get the MSEntry.
      Return None if there is no such entry.
      Return an MSEntryFutures if futs_only is True
      """
      
      if volume is not None:
         volume_id = volume.volume_id 
         num_shards = volume.num_shards 
      
      if MSEntry.is_serialized_id( file_id ):
         file_id = MSEntry.unserialize_id( file_id )
         
      ent_key_name = MSEntry.make_key_name( volume_id, file_id)
      ent_cache_key_name = MSEntry.make_key_name( volume_id, file_id )

      ent_key = storagetypes.make_key( MSEntry, ent_key_name )
      shard_keys = MSEntry.get_shard_keys( num_shards, ent_key_name )
      
      ent = storagetypes.memcache.get( ent_cache_key_name )
      if ent is not None:
         if futs_only:
            return MSEntryFutures( base_future=storagetypes.FutureWrapper( ent ), shard_futures=[], num_children_future=MSEntryIndex.GetNumChildren( volume_id, file_id, num_shards, async=True ), num_shards=num_shards )
         else:
            return ent
      
      # get the values from the datastore, if they weren't cached 
      futs = {}
      all_futs = []

      futs["base"] = ent_key.get_async()
      all_futs.append( futs["base"] )
      
      futs["num_children"] = MSEntryIndex.GetNumChildren( volume_id, file_id, num_shards, async=True )
      all_futs.append( futs["num_children"] )
      
      # get shards
      futs["shard"] = [None] * len(shard_keys)
      
      for i in xrange(0, len(shard_keys)):
         shard_key = shard_keys[i]
         fut = shard_key.get_async()
         futs["shard"][i] = fut
         all_futs.append( fut )
      
      ent_fut = MSEntryFutures( base_future=futs["base"], shard_futures=futs["shard"], num_children_future=futs["num_children"], num_shards=num_shards )
      if futs_only:
         # only want futures, but if we have the entries, then just return them
         return ent_fut

      # get the values
      storagetypes.wait_futures( all_futs )
      ent = MSEntry.FromFuture( ent_fut )

      # cache result
      storagetypes.memcache.set( ent_cache_key_name, ent )
      
      return ent


   @classmethod 
   def ReadByParent( cls, volume, parent_id, name, futs_only=False ):
      """
      Read an entry given its parent ID and the entry's name 
      Return None if there is no such entry.
      Return MSEntryFutures if futs_only is True
      """
      
      # get the nameholder 
      child_nameholder = MSEntryNameHolder.Read( volume.volume_id, parent_id, name )
      if child_nameholder is None:
         return None
      
      # get the child
      child = MSEntry.Read( volume, child_nameholder.file_id, futs_only=futs_only )
      return child
      

   @classmethod
   def FlattenFuture( cls, ent_fut ):
      
      all_futures = [None] * (len(ent_fut.shard_futures) + 2)
      for i in xrange(0, len(ent_fut.shard_futures)):
         all_futures[i] = ent_fut.shard_futures[i]
      
      all_futures[len(all_futures)-2] = ent_fut.base_future
      all_futures[len(all_futures)-1] = ent_fut.num_children_future
      
      return all_futures
   
   @classmethod
   def FromFuture( cls, ent_fut ):
      ent = ent_fut.base_future.get_result()
      
      if ent is not None:
         shards = filter( lambda x: x is not None, [x.get_result() for x in ent_fut.shard_futures] )
         
         ent.populate_from_shards( shards )
         ent.populate_shard( ent_fut.num_shards, **ent.to_dict() )
         
         if ent.ftype == MSENTRY_TYPE_DIR:
            num_children_counter = ent_fut.num_children_future.get_result()
            
            if num_children_counter is None:
               ent.num_children = 0
            
            else:
               ent.num_children = num_children_counter
            
         # logging.info("FromFuture: %s\nshards = %s" % (ent.to_dict(), shards))
      
      return ent

   @classmethod
   def WaitFutures( cls, futures ):
      # futures is a list of MSEntryFutures namedtuple, from Read()
      
      all_futures = []
      for fut in futures:
         all_futures += cls.FlattenFuture( fut )
         
      storagetypes.wait_futures( all_futures )

      ret = []
      
      for fut in futures:
         ent = cls.FromFuture( fut )
         ret.append( ent )

      return ret
   
   
