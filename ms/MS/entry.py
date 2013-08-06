#!/usr/bin/pyhon

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""


import storage.storagetypes as storagetypes

import protobufs.ms_pb2 as ms_pb2
import logging

import random
import os

import types
import errno
import time
import datetime

MSENTRY_TYPE_FILE = ms_pb2.ms_entry.MS_ENTRY_TYPE_FILE
MSENTRY_TYPE_DIR = ms_pb2.ms_entry.MS_ENTRY_TYPE_DIR

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
   # Sharded component of an MSEntry
   mtime_sec = storagetypes.Integer(default=0, indexed=False)
   mtime_nsec = storagetypes.Integer(default=0, indexed=False)
   size = storagetypes.Integer(default=0, indexed=False )

   # version of the MSEntry we're associated with
   msentry_version = storagetypes.Integer(default=0, indexed=False )

   # volume ID of the MSEntry we're associated with
   msentry_volume_id = storagetypes.Integer( indexed=False )

   @classmethod
   def modtime_max( cls, m1, m2 ):
      # return the later of two MSEntryShards, based on moddtime
      if m1.mtime_sec < m2.mtime_sec:
         return m2

      elif m1.mtime_sec > m2.mtime_sec:
         return m1

      elif m1.mtime_nsec < m2.mtime_nsec:
         return m2

      elif m1.mtime_nsec > m2.mtime_nsec:
         return m1

      return m1

   @classmethod
   def size_max( cls, sz1, sz2 ):
      if sz1.size > sz2.size:
         return sz1

      return sz2
      
   @classmethod
   def get_latest_shard( cls, ent, shards ):
      mm = None
      latest_version = ent.version
      for shard in shards:
         if mm == None:
            mm = shard
            continue

         if shard == None:
            continue

         if shard.msentry_version != latest_version:
            continue
         
         mm = MSEntryShard.modtime_max( mm, shard )
      return mm

   @classmethod
   def get_mtime_from_shards( cls, ent, shards ):
      mm = MSEntryShard.get_latest_shard( ent, shards )
      if mm != None:
         return (mm.mtime_sec, mm.mtime_nsec)
      else:
         return (None, None)

   @classmethod
   def get_size_from_shards( cls, ent, shards ):
      # NOTE: size can never decrease for a given version 
      sz = None
      latest_version = ent.version
      for shard in shards:
         if sz == None:
            sz = shard
            continue

         if shard == None:
            continue

         if shard.msentry_version != latest_version:
            continue

         sz = MSEntryShard.size_max( sz, shard )

      return sz.size
   
         
class MSEntry( storagetypes.Object ):
   """
   Syndicate metadata entry.
   Each entry is named by its path.
   Each entry is in its own entity group, and is named by its path.
   Each entry refers to its parent, so we can resolve a path's worth of metadata quickly.
   """

   ftype = storagetypes.Integer( indexed=False )
   
   fs_path = storagetypes.Text()     # just the filesystem path
   url = storagetypes.Text()
   version = storagetypes.Integer( indexed=False )
   
   ctime_sec = storagetypes.Integer( indexed=False )
   ctime_nsec = storagetypes.Integer( indexed=False )

   owner_id = storagetypes.Integer()
   coordinator_id = storagetypes.Integer()
   volume_id = storagetypes.Integer()
   mode = storagetypes.Integer( indexed=False )

   max_read_freshness = storagetypes.Integer( indexed=False )
   max_write_freshness = storagetypes.Integer( indexed=False )

   parent_key = storagetypes.Key()

   # whether or not this directory is considered to be deleted
   deleted = storagetypes.Boolean( default=False, indexed=False )

   # to be filled in from shard
   mtime_sec = 0
   mtime_nsec = 0
   size = 0

   # attributes that must be supplied on creation
   required_attrs = [
      "ftype",
      "fs_path",
      "url",
      "version",
      "owner_id",
      "coordinator_id",
      "volume_id",
      "mtime_sec",
      "mtime_nsec",
      "ctime_sec",
      "ctime_nsec",
      "mode",
      "size",
      "parent_key",
   ]

   # attributes that uniquely identify this entry
   key_attrs = [
      "volume_id",
      "fs_path"
   ]

   # methods for generating default values for attributes (sharded or not)
   default_values = {
      "parent_key": (lambda cls, attrs: storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=attrs['volume_id'], fs_path=MSEntry.get_parent_path( attrs['fs_path'] ) ) ) if attrs['fs_path'] != "/" else None),
      "max_read_freshness": (lambda cls, attrs: 0),
      "max_write_freshness": (lambda cls, attrs: 0)
   }

   # publicly readable attributes, sharded or not
   read_attrs = [
      "ftype",
      "fs_path",
      "url",
      "version",
      "ctime_sec",
      "ctime_nsec",
      "mtime_sec",
      "mtime_nsec",
      "owner_id",
      "coordinator_id",
      "volume_id",
      "mode",
      "size",
      "max_read_freshness",
      "max_write_freshness",
   ]

   # publicly writable attributes, sharded or not
   write_attrs = [
      "url",
      "version",
      "mode",
      "size",
      "mtime_sec",
      "mtime_nsec",
      "max_read_freshness",
      "max_write_freshness",
      "coordinator_id"
   ]

   # shard class
   shard_class = MSEntryShard
   
   # sharded fields
   shard_fields = [
      "mtime_sec",
      "mtime_nsec",
      "size",
      "msentry_version",
      "msentry_volume_id"
   ]

   # functions that read a sharded value from shards for an instance of this ent
   shard_readers = {
      "mtime_sec": (lambda ent, shards: MSEntryShard.get_mtime_from_shards( ent, shards )[0]),
      "mtime_nsec": (lambda ent, shards: MSEntryShard.get_mtime_from_shards( ent, shards )[1]),
      "size": (lambda ent, shards: MSEntryShard.get_size_from_shards( ent, shards ))
   }

   # functions that write a sharded value, given this ent
   shard_writers = {
      "msentry_version": (lambda ent: ent.version),
      "msentry_volume_id": (lambda ent: ent.volume_id)
   }

   
   def protobuf( self, pbent, **kwargs ):
      """
      Return an ms_entry instance containing this entry's data
      """

      pbent.type = kwargs.get( 'ftype', self.ftype )
      pbent.ctime_sec = kwargs.get( 'ctime_sec', self.ctime_sec )
      pbent.ctime_nsec = kwargs.get( 'ctime_nsec', self.ctime_nsec )
      pbent.mtime_sec = kwargs.get( 'mtime_sec', self.mtime_sec )
      pbent.mtime_nsec = kwargs.get( 'mtime_nsec', self.mtime_nsec )
      pbent.owner = kwargs.get( 'owner_id', self.owner_id )
      pbent.coordinator = kwargs.get( 'coordinator_id', self.coordinator_id )
      pbent.volume = kwargs.get( 'volume_id', self.volume_id )
      pbent.mode = kwargs.get( 'mode', self.mode )
      pbent.size = kwargs.get( 'size', self.size )
      pbent.version = kwargs.get( 'version', self.version )
      pbent.url = kwargs.get( 'url', self.url )
      pbent.path = kwargs.get( 'fs_path', self.fs_path )
      pbent.max_read_freshness = kwargs.get( 'max_read_freshness', self.max_read_freshness )
      pbent.max_write_freshness = kwargs.get( 'max_write_freshness', self.max_write_freshness )
      
      # make sure the path ends in / if this is a directory (and is not .)
      if pbent.type == MSENTRY_TYPE_DIR and pbent.path != ".":
         if pbent.path[-1] != '/':
            pbent.path += '/'

      return
      

   @classmethod
   def unprotobuf( cls, ent ):
      """
      Return an MSEntry instance from a protobuf.ms_pb2.ms_entry
      """
      ret = MSEntry( key=storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=ent.volume, fs_path=ent.path )) )
      
      ret.ftype = ent.type
      ret.ctime_sec = ent.ctime_sec
      ret.ctime_nsec = ent.ctime_nsec
      ret.mtime_sec = ent.mtime_sec
      ret.mtime_nsec = ent.mtime_nsec
      ret.owner_id = ent.owner
      ret.coordinator_id = ent.coordinator
      ret.volume_id = ent.volume
      ret.mode = ent.mode
      ret.size = ent.size
      ret.version = ent.version
      ret.url = ent.url
      ret.fs_path = ent.path
      ret.parent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=ent.volume, fs_path=MSEntry.get_parent_path( ent.path ) ) )
      ret.max_read_freshness = ent.max_read_freshness
      ret.max_write_freshness = ent.max_write_freshness
      
      return ret


   @classmethod
   def unprotobuf_dict( cls, ent ):
      d = cls.unprotobuf( ent ).to_dict()

      # included sharded fields (which share the same name)
      for shard_field in cls.shard_fields:
         d[shard_field] = getattr(ent, shard_field, None)

      return d
      
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
   def make_key_name( cls, **kwargs ):
      assert kwargs.has_key( 'fs_path' ) and kwargs.has_key( 'volume_id' ), "Required key fields: volume_id, fs_path"
      kwargs['fs_path'] = MSEntry.sanitize_fs_path( kwargs['fs_path'] )
      return super( MSEntry, cls ).make_key_name( volume_id=kwargs['volume_id'], fs_path=kwargs['fs_path'] )

   @classmethod
   def cache_key( cls, **kwargs ):
      assert kwargs.has_key( 'fs_path' ) and kwargs.has_key( 'volume_id' ), "Required key fields: volume_id, fs_path"
      kwargs['fs_path'] = MSEntry.sanitize_fs_path( kwargs['fs_path'] )
      return super( MSEntry, cls ).cache_key( volume_id=kwargs['volume_id'], fs_path=kwargs['fs_path'] )
      
   @classmethod
   def cache_listing_key( cls, **kwargs ):
      assert kwargs.has_key( 'fs_path' ) and kwargs.has_key( 'volume_id' ), "Required key fields: volume_id, fs_path"
      kwargs['fs_path'] = MSEntry.sanitize_fs_path( kwargs['fs_path'] )
      return super( MSEntry, cls ).cache_listing_key( volume_id=kwargs['volume_id'], fs_path=kwargs['fs_path'] )

   @classmethod
   def cache_shard_key( cls, **kwargs ):
      assert kwargs.has_key( 'fs_path' ) and kwargs.has_key( 'volume_id' ), "Required key fields: volume_id, fs_path"
      kwargs['fs_path'] = MSEntry.sanitize_fs_path( kwargs['fs_path'] )
      return super( MSEntry, cls ).cache_shard_key( volume_id=kwargs['volume_id'], fs_path=kwargs['fs_path'] )
   
   def update_dir_shard( self, num_shards, **parent_attrs ):
      """
      NOTE: This does NOT need to run in a transaction.
      For files, only one UG will ever send size updates, and they will be serialized and sanity-checked by the UG.
      For directories, size does not ever change, and mtime only has to be different across updates.
      """
      shard_keys = self.get_shard_keys( num_shards, **parent_attrs )
      shard_key = shard_keys[ random.randint( 0, len(shard_keys)-1 ) ]
      
      shard = shard_key.get()
      if shard == None:
         shard = self.shard_class( key=shard_key )
      
      MSEntry.populate_shard_inst( self, shard, **parent_attrs )

      shard.put()

      self.write_shard = shard
      
      return shard


   @classmethod
   @storagetypes.concurrent
   def __read_msentry_base( cls, volume_id, fs_path, **ctx_opts ):
      ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=fs_path ) )
      ent = yield ent_key.get_async( **ctx_opts )
      #ent = storagetypes.concurrent_yield( ent_key.get_async( **ctx_opts ) )
      #raise ndb.Return( ent )
      storagetypes.concurrent_return( ent )


   @classmethod
   @storagetypes.concurrent
   def __read_msentry_shards( cls, volume_id, fs_path, num_shards, **ctx_opts ):
      shard_keys = MSEntry.get_shard_keys( num_shards, volume_id=volume_id, fs_path=fs_path )
      shards = yield storagetypes.get_multi_async( shard_keys, **ctx_opts )
      #shards = yield ndb.get_multi_async( shard_keys, **ctx_opts )
      #shards = storagetypes.concurrent_yield( storagetypes.get_multi_async( shard_keys, **ctx_opts ) )
      #raise ndb.Return( shards )
      storagetypes.concurrent_return( shards )

   @classmethod
   @storagetypes.concurrent
   def __read_msentry( cls, volume_id, fs_path, num_shards, **ctx_opts ):
      #ent, shards = yield __read_msentry_base( volume_id, fs_path, **ctx_opts ), __read_msentry_shards( volume_id, fs_path, num_shards, **ctx_opts )
      ent, shards = yield __read_msentry_base( volume_id, fs_path, **ctx_opts ), __read_msentry_shards( volume_id, fs_path, num_shards, **ctx_opts )
      if ent != None:
         ent.populate_from_shards( shards )

      #raise ndb.Return( ent )
      storagetypes.concurrent_return( ent )


   @classmethod
   def cache_shard( cls, volume_id, fs_path, write_shard ):
      """
      Cache the last written shard
      """
      client = storagetypes.memcache.Client()
      shard_key = MSEntry.cache_shard_key( volume_id=volume_id, fs_path=fs_path )
      timeout = 0.001
      while True:

         cur_shard = client.gets( shard_key )

         if cur_shard == None:
            client.set( shard_key, write_shard )
            return

         if cur_shard.mtime_sec > write_shard.mtime_sec or (cur_shard.mtime_sec == write_shard.mtime_sec and cur_shard.mtime_nsec > write_shard.mtime_sec):
            return

         if client.cas( shard_key, write_shard ):
            return

         time.sleep( timeout )
         timeout = (timeout + float(random.randint(0,100)) / 100) * 2


   @classmethod
   def get_cached_shard( cls, volume_id, fs_path ):
      """
      Get the last-written shard from the cache
      """
      shard_key = MSEntry.cache_shard_key( volume_id=volume_id, fs_path=fs_path )
      return storagetypes.memcache.get( shard_key )


   @classmethod
   def delete_cached_shard( cls, volume_id, fs_path ):
      """
      Delete the last-written shard from the cache
      """
      shard_key = MSEntry.cache_shard_key( volume_id=volume_id, fs_path=fs_path )
      return storagetypes.memcache.delete( shard_key )


   @classmethod
   def Create( cls, user_owner_id, volume, **ent_attrs ):

      # coerce volume_id
      ent_attrs['volume_id'] = volume.volume_id
      
      # ensure we have every required attribute
      MSEntry.fill_defaults( ent_attrs )

      # necessary input
      missing = MSEntry.find_missing_attrs( ent_attrs )
      if len(missing) > 0:
         logging.error("missing: %s" % missing)
         return -errno.EINVAL
      
      fs_path = ent_attrs['fs_path']
      volume_id = volume.volume_id

      logging.info("create '%s'" % ent_attrs['fs_path'])

      # valid input
      invalid = MSEntry.validate_fields( ent_attrs )
      if len(invalid) > 0:
         logging.error("not allowed: %s" % invalid)
         return -errno.EINVAL

      # access allowed?
      if ent_attrs['fs_path'] == '/' and user_owner_id != 0 and volume.owner_id != user_owner_id:
         return -errno.EACCES

      # get the parent entry outside of the transaction
      parent_ent = None
      child_ent = None
      parent_fut = None
      futs = []

      parent_path = MSEntry.get_parent_path( ent_attrs['fs_path'] )
      parent_cache_key = MSEntry.cache_key( volume_id=volume_id, fs_path=parent_path )

      parent_ent = storagetypes.memcache.get( parent_cache_key )

      if parent_ent == None:
         parent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=parent_path ) )
         parent_fut = parent_key.get_async( use_memcache=False )
         futs.append( parent_fut )

      # try to get child (shouldn't exist)
      base_attrs = MSEntry.base_attrs( ent_attrs )
      child_fut = MSEntry.get_or_insert_async( MSEntry.make_key_name( volume_id=volume_id, fs_path=ent_attrs['fs_path'] ), **base_attrs )
      futs.append( child_fut )

      storagetypes.wait_futures( futs )

      if parent_fut != None:
         parent_ent = parent_fut.get_result()

      child_ent = child_fut.get_result()

      if parent_ent == None or parent_ent.deleted:
         return -errno.ENOENT

      if not is_writable( user_owner_id, volume_id, parent_ent.owner_id, parent_ent.mode ):
         return -errno.EACCES

      if child_ent.fs_path != ent_attrs['fs_path'] or child_ent.owner_id != ent_attrs['owner_id']:
         # already exists
         return -errno.EEXIST

      # cache parent, for the time being
      storagetypes.memcache.add( parent_cache_key, parent_ent )

      # commit child shard
      child_ent.populate_shard( volume.num_shards, **ent_attrs )
      child_shard_fut = child_ent.put_shard_async()

      # put the parent shard, updating the number of children
      parent_attrs = {}
      parent_attrs.update( ent_attrs )

      now_sec, now_nsec = storagetypes.clock_gettime()

      parent_attrs['fs_path'] = MSEntry.get_parent_path( ent_attrs['fs_path'] )

      parent_shard = parent_ent.update_dir_shard( volume.num_shards, **parent_attrs )

      # make sure parent still exists
      parent_ent = storagetypes.memcache.get( parent_cache_key )

      if parent_ent == None:
         parent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=parent_path ) )
         parent_ent = parent_key.get()

      delete = False
      ret = 0

      if parent_ent == None or parent_ent.deleted:
         # roll back
         delete = True
         child_ent.key.delete_async()
         ret = -errno.ENOENT

      child_shard_fut.wait()

      if delete:
         child_shard_key = child_shard_fut.get_result()
         child_shard_key.delete_async()

      # invalidate cached listing of parent directory
      storagetypes.memcache.delete( MSEntry.cache_listing_key( volume_id=volume.volume_id, fs_path=parent_path ) )

      # cache the parent's last-written shard
      MSEntry.cache_shard( volume_id, parent_path, parent_shard )

      return ret



   @classmethod
   def Update( cls, user_owner_id, volume, **ent_attrs ):

      logging.info("update '%s'" % ent_attrs['fs_path'] )

      # Update an MSEntry.
      # A file will be updated by at most one UG, so we don't need a transaction.
      # A directory can be updated by anyone, but the update conflict resolution is last-write-wins.

      fs_path = ent_attrs['fs_path']
      volume_id = volume.volume_id

      # only write writable attributes
      writable_attrs = {}
      writable_attrs.update( ent_attrs )

      not_writable = MSEntry.validate_write( ent_attrs.keys() )
      for nw in not_writable:
         del writable_attrs[nw]

      # get the ent
      # try from cache first
      client = storagetypes.memcache.Client()
      cache_ent_key = MSEntry.cache_key( volume_id=volume_id, fs_path=fs_path )

      ent = client.get( cache_ent_key )
      if ent == None:
         # not in the cache.  Get from the datastore
         ent_fut = MSEntry.__read_msentry_base( volume_id, fs_path, use_memcache=False )
         ent = ent_fut.get_result()

      if ent == None:
         return -errno.ENOENT

      if not is_writable( user_owner_id, volume.owner_id, ent.owner_id, ent.mode ):
         return -errno.EACCES

      put_ent = False

      # necessary, since the version can change (i.e. on a truncate)
      for write_attr in writable_attrs.keys():
         if getattr(ent, write_attr, None) != writable_attrs[write_attr]:
            put_ent = True
            ent.populate_base( **writable_attrs )
            break
         
      # make a new shard with the mtime and size
      ent.populate_shard( volume.num_shards, **ent_attrs )

      # put the entry and its shard outside of the transaction.
      # This is okay, since regardless of concurrent base or shard writes, either the size or the mtime will be different.
      futs = []
      if put_ent:
         ent_fut = ent.put_async()
         futs.append( ent_fut )

         client.set( cache_ent_key, ent )

      ent_shard_fut = ent.put_shard_async()

      if ent_shard_fut != None:
         futs.append( ent_shard_fut )

      # update our shard
      MSEntry.cache_shard( volume_id, fs_path, ent.write_shard )

      # invalidate cached listing of the directory
      storagetypes.memcache.delete( MSEntry.cache_listing_key( volume_id=volume.volume_id, fs_path=MSEntry.get_parent_path( fs_path ) ) )

      storagetypes.wait_futures( futs )

      return 0



   # deferred operation
   @classmethod
   def delete_msentry_shards( cls, shard_keys ):
      storagetypes.delete_multi( shard_keys )

   @classmethod
   def Delete( cls, user_owner_id, volume, **ent_attrs ):
      logging.info("delete %s" % ent_attrs['fs_path'] )

      # delete an MSEntry.
      # A file will be deleted by at most one UG
      # A directy can be deleted by anyone, and it must be empty

      volume_id = volume.volume_id
      fs_path = ent_attrs['fs_path']
      parent_path = MSEntry.get_parent_path( fs_path )
      futs = []

      # get ent, parent_ent from the cache
      ent_cache_key = MSEntry.cache_key( volume_id=volume_id, fs_path=fs_path )
      parent_cache_key = MSEntry.cache_key( volume_id=volume_id, fs_path=parent_path )

      ret = storagetypes.memcache.get_multi( [ent_cache_key, parent_cache_key] )

      ent = ret.get( ent_cache_key, None )
      parent_ent = ret.get( parent_cache_key, None )

      # if ent is not cached, then read from the datastore
      if ent == None:
         ent_fut = MSEntry.__read_msentry_base( volume_id, fs_path, use_memcache=False )
         futs.append( ent_fut )

      # if parent_ent is not cached, then read from the datastore
      if parent_ent == None:
         parent_ent_fut = __read_msentry_base( volume_id, MSEntry.get_parent_path( fs_path ), use_memcache=False )
         futs.append( parent_ent_fut )

      # wait for the datastore to get back to us...
      if len(futs) != 0:
         storagetypes.wait_futures( futs )

      if ent == None:
         ent = ent_fut.get_result()

      if parent_ent == None:
         parent_ent = parent_ent_fut.get_result()

      # sanity check
      if ent == None or parent_ent == None:
         return -errno.ENOENT

      # permission checks...
      if not is_writable( user_owner_id, volume.owner_id, parent_ent.owner_id, parent_ent.mode ):
         return -errno.EACCES

      if not is_writable( user_owner_id, volume.owner_id, ent.owner_id, ent.mode ):
         return -errno.EACCES

      # sanity check
      if parent_ent.ftype != MSENTRY_TYPE_DIR:
         return -errno.ENOTDIR

      do_delete = False
      ret = 0
      ent_shard_keys = MSEntry.get_shard_keys( volume.num_shards, volume_id=volume_id, fs_path=fs_path )
      ent_fut = None

      if ent.ftype == MSENTRY_TYPE_FILE:
         # this is a file; just delete it and update the parent mtime
         do_delete = True
         ret = 0

         # erase from the cache
         storagetypes.memcache.delete( ent_cache_key )

      else:

         # mark as deleted.  Creates will fail from now on
         ent.deleted = True
         ent.put()

         # erase from the cache
         storagetypes.memcache.delete( ent_cache_key )

         # make sure the ent was actually empty
         qry_ents = MSEntry.query( MSEntry.parent_key == storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=fs_path ) ) )
         child = qry_ents.fetch( 1, keys_only=True )

         if len(child) != 0:
            # attempt to get this (since queries are eventually consistent, but gets on an entity group are strongly consistent)
            child_ent = child[0].get()

            if child_ent != None:
               # not empty
               ent.deleted = False
               ent.put()

               # uncache
               storagetypes.memcache.delete( ent_cache_key )
               return -errno.ENOTEMPTY

         # safe to delete
         do_delete = True
         ret = 0

      if do_delete:

         # delete this entry
         ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=fs_path ) )
         ent_fut = ent_key.delete_async()
         storagetypes.deferred.defer( MSEntry.delete_msentry_shards, ent_shard_keys )

         # put a new parent shard
         parent_attrs = {}
         parent_attrs.update( ent_attrs )

         now_sec, now_nsec = storagetypes.clock_gettime()

         parent_attrs['fs_path'] = parent_path
         parent_attrs['mtime_sec'] = now_sec
         parent_attrs['mtime_nsec'] = now_nsec

         parent_shard = parent_ent.update_dir_shard( volume.num_shards, **parent_attrs )

         MSEntry.cache_shard( volume.volume_id, parent_attrs['fs_path'], parent_shard )

         # delete any listings of this parent
         storagetypes.memcache.delete( MSEntry.cache_listing_key( volume_id=volume.volume_id, fs_path=parent_path) )
         storagetypes.memcache.delete( ent_cache_key )

         if ent_fut != None:
            ent_fut.wait()

      return ret


   @classmethod
   def ListAll( cls, volume, fs_path ):
      """
      Given a volume id and an fs_path and the client-given lastmod time for a directory, find all entries that are immediate children
      """

      logging.info("list %s" % fs_path )

      volume_id = volume.volume_id

      @storagetypes.concurrent
      def __read_msentry_children_mapper( msentry ):
         #shards = yield __read_msentry_shards( msentry.volume_id, msentry.fs_path, volume.num_shards )
         shards = yield MSEntry.__read_msentry_shards( msentry.volume_id, msentry.fs_path, volume.num_shards )
         
         #logging.info("shards of %s: %s" % (msentry.fs_path, shards))
         msentry.populate_from_shards( shards )

         #raise ndb.Return( msentry )
         storagetypes.concurrent_return( msentry )
         

      results_key = MSEntry.cache_listing_key( volume_id=volume_id, fs_path=fs_path )

      client = storagetypes.memcache.Client()
      results = client.gets( results_key )

      if results == None:
         # get the entries
         qry_ents = MSEntry.query( MSEntry.parent_key == storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=fs_path ) ) )
         results = qry_ents.map( __read_msentry_children_mapper, batch_size=1000 )

         client.cas( results_key, results )

      return results


   @classmethod
   def Read( cls, volume, fs_path ):
      """
      Given a volume id and an fs_path and lastmod time, find all MSEntrys in the path
      """

      logging.info("read %s" % fs_path )

      result_futs = []
      results = []

      volume_id = volume.volume_id

      client = storagetypes.memcache.Client()

      path_parts = fs_path.split("/")

      # directory?
      if path_parts[-1] == "":
         path_parts.pop()

      # list of entry paths
      cur_path = "/"
      msentry_paths = ["/"]
      for i in xrange(1, len(path_parts)):
         if cur_path != "/":
            cur_path += "/" + path_parts[i]
         else:
            cur_path += path_parts[i]

         msentry_paths.append( cur_path )


      # fetch as much as possible from cache
      cache_keys = []

      for i in xrange(0, len(msentry_paths)):
         fs_path = msentry_paths[i]

         ent_key = MSEntry.cache_key( volume_id=volume_id, fs_path=fs_path )
         shard_key = MSEntry.cache_shard_key( volume_id=volume_id, fs_path=fs_path )

         cache_keys.append( ent_key )
         cache_keys.append( shard_key )


      # fetch from memcache
      cached_results = client.get_multi( cache_keys )
      results = []

      for i in xrange(0, len(msentry_paths)):
         ent_key = cache_keys[ 2*i ]
         shard_key = cache_keys[ 2*i + 1 ]

         ent = cached_results.get( ent_key, None )
         shard = cached_results.get( shard_key, None )

         if ent != None and shard != None:
            ent.populate_from_shards( [shard] )
            results.append( ent )

         else:
            results.append( None )

      # get the rest from the datastore
      all_keys = []

      for i in xrange(0, len(msentry_paths)):
         if results[i] != None:
            continue

         fs_path = msentry_paths[i]

         ent_key = storagetypes.make_key( MSEntry, MSEntry.make_key_name( volume_id=volume_id, fs_path=fs_path ) )
         shard_keys = MSEntry.get_shard_keys( volume.num_shards, volume_id=volume_id, fs_path=fs_path )

         all_keys.append( ent_key )
         all_keys += shard_keys


      # get the remainder of stuff from the datastore
      # cache the results
      if len(all_keys) > 0:
         ret = storagetypes.get_multi( all_keys, use_memcache=False )

         ri = 0
         i = 0
         cache_dict = {}

         while ri < len(results):
            # find the next unknown result
            if results[ri] == None:

               # end of path
               if ret[i] == None:
                  break

               # populate this entry
               shards = []

               for k in xrange(i+1, i + volume.num_shards + 1):
                  shards.append( ret[k] )

               ret[i].populate_from_shards( shards )
               write_shard = MSEntryShard.get_latest_shard( ret[i], shards )

               # store the result
               results[ri] = ret[i]

               # prepare to cache the result
               ent_key = MSEntry.cache_key( volume_id=volume_id, fs_path=ret[i].fs_path )
               shard_key = MSEntry.cache_shard_key( volume_id=volume_id, fs_path=ret[i].fs_path )

               cache_dict[ ent_key ] = ret[i]
               cache_dict[ shard_key ] = write_shard

               # next result
               i += volume.num_shards + 1

            ri += 1

         # cache everything to memcache
         client.set_multi( cache_dict )

      return results

      