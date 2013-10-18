#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storagetypes
from storagetypes import *

from google.appengine.ext import ndb
from google.appengine.api import memcache
from google.appengine.ext import deferred
from google.appengine.ext.db import TransactionFailedError

import logging

import MS
import MS.entry
from MS.entry import MSEntry, MSEntryShard, MSENTRY_TYPE_DIR
from MS.volume import Volume, VolumeIDCounter
from MS.user import SyndicateUser, SyndicateUIDCounter
from MS.gateway import UserGateway, AcquisitionGateway, ReplicaGateway

import protobufs.ms_pb2 as ms_pb2

import types
import errno
import time
import datetime
import random
   
def create_volume( username, **kwargs ):
   return Volume.Create( username, **kwargs )
   

def read_volume( volume_id ):
   return Volume.Read( volume_id )

def get_volumes( volume_ids ):
   return Volume.ReadAll( volume_ids )

def get_roots( volumes ):
   # read all root directories
   roots = [None] * len(volumes)
   root_memcache = [None] * len(volumes)
   for i in xrange(0, len(volumes)):
      root_memcache[i] = MSEntry.Read( volumes[i], 0, memcache_keys_only=True )

   cached_roots = storagetypes.memcache.get_multi( root_memcache )
   for i in xrange(0, len(volumes)):
      roots[i] = cached_roots.get( root_memcache[i], None )

   if None in roots:
      
      ent_futs = []
      for i in xrange(0, len(volumes)):
         if roots[i] != None:
            continue
         
         ent_futs.append( MSEntry.Read( volumes[i], 0, futs_only=True ) )

      
      results = MSEntry.WaitFutures( ent_futs )
      k = 0
      for i in xrange(0, len(volumes)):
         if roots[i] != None:
            continue

         roots[i] = results[k]
         k += 1

   # cache the results
   cache_roots = {}
   for root in roots:
      if root == None:
         continue
      
      cache_name = MSEntry.cache_key_name( root.volume_id, root.file_id )
      cache_roots[ cache_name ] = root

   storagetypes.memcache.set_multi( cache_roots )
   return roots
   

def volume_update_shard_count( volume_id, num_shards ):
   return Volume.update_shard_count( volume_id, num_shards )

   
def update_volume( volume_id, **fields ):
   return Volume.Update( volume_id, **fields )

   
def delete_volume( volume_id ):
   return Volume.Delete( volume_id )

   
def list_volumes( attrs=None, limit=None ):
   return Volume.ListAll( attrs, limit=limit )


def get_volume( volume_id ):
   return read_volume( volume_id )

def get_volume_by_name( volume_name ):
   vols = list_volumes( {"Volume.name ==": volume_name} )
   if len(vols) > 1:
      raise Exception("More than one Volume by the name of '%s'" % volume_name)

   if len(vols) == 0:
      return None

   return vols[0]
   
   
def create_user( **kwargs ):
   return SyndicateUser.Create( **kwargs )
   

def read_user( email ):
   return SyndicateUser.Read( email )


def update_user( email, **fields ):
   return SyndicateUser.Update( email, **fields )

   
def delete_user( email, **fields ):
   return SyndicateUser.Delete( email, **fields )

   
def list_users( attrs=None, limit=None ):
   return SyndicateUser.ListAll( attrs, limit=limit )

def get_user( attr ):
   allusers = list_users(attr)
   if len(allusers) > 1:
      raise Exception("More than one users satisfies attrs: %s" % attr)
   for u in allusers:
      return u
   return None


   
def make_root( volume, owner_id, **root_attrs ):
   now_sec, now_nsec = storagetypes.clock_gettime()
   
   basic_root_attrs = {
      "file_id": "0000000000000000",
      "parent_id": "0000000000000000",
      "name": "/",
      "ftype": MSENTRY_TYPE_DIR,
      "version": 1,
      "ctime_sec" : now_sec,
      "ctime_nsec" : now_nsec,
      "mtime_sec" : now_sec,
      "mtime_nsec" : now_nsec,
      "owner_id" : owner_id,
      "coordinator_id": root_attrs.get("coordinator_id", 0),
      "volume_id" : volume.volume_id,
      "mode" : 0775,
      "size": 4096,
      "max_read_freshness" : 5000,
      "max_write_freshness" : 0
   }

   basic_root_attrs.update( **root_attrs )
   
   root = MSEntry( key=make_key( MSEntry, MSEntry.make_key_name( volume.volume_id, basic_root_attrs['file_id'] ) ) )
   
   root.populate( volume.num_shards, **basic_root_attrs )

   root.update_dir_shard( volume.num_shards, volume.volume_id, basic_root_attrs['file_id'], **basic_root_attrs )

   root_fut = root.put_async()
   root_shard_fut = root.put_shard_async()
   
   storagetypes.wait_futures( [root_fut, root_shard_fut] )

   return 0
   
   
def create_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Create( user_owner_id, volume, **ent_attrs )
   

def update_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Update( user_owner_id, volume, **ent_attrs )

   
def delete_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Delete( user_owner_id, volume, **ent_attrs )

   
def list_msentry_children( volume, file_id ):
   return MSEntry.ListAll( volume, file_id )


def read_msentry( volume, file_id ):
   return MSEntry.Read( volume, file_id )

   
def create_user_gateway( user, volume=None, **kwargs ):
   return UserGateway.Create( user, volume, **kwargs )

def read_user_gateway( g_id ):
   return UserGateway.Read( g_id )

def update_user_gateway( g_id, **fields):
   return UserGateway.Update( g_id, **fields )

def list_user_gateways(attrs=None, limit=None):
   return UserGateway.ListAll(attrs, limit=limit)

def list_user_gateways_by_volume( volume_id ):
   return UserGateway.ListAll_ByVolume( volume_id )

def get_user_gateway_by_name( name ):
   ugs = list_user_gateways( {"ms_username ==" : name} )
   if len(ugs) > 1:
      raise Exception("%s UGs named %s" % (len(ugs), name))

   if len(ugs) == 0:
      return None

   return ugs[0]
   
   
def delete_user_gateway( g_id ):
   return UserGateway.Delete( g_id )


def create_acquisition_gateway( user, volume=None, **kwargs ):
   return AcquisitionGateway.Create( user, **kwargs )
      
def read_acquisition_gateway( g_id ):
   return AcquisitionGateway.Read( g_id )

def update_acquisition_gateway( g_id, **fields ):
   return AcquisitionGateway.Update( g_id, **fields )

def list_acquisition_gateways_by_volume( volume_id ):
   return AcquisitionGateway.ListAll_ByVolume( volume_id )

def list_acquisition_gateways( attrs=None, limit=None ):
   return AcquisitionGateway.ListAll( attrs, limit=limit )

def get_acquisition_gateway_by_name( name ):
   ags = list_acquisition_gateways( {"ms_username ==" : name} )
   if len(ags) > 1:
      raise Exception("%s AGs named %s" % (len(ags), name))

   if len(ags) == 0:
      return None

   return ags[0]

def delete_acquisition_gateway( g_id ):
   return AcquisitionGateway.Delete( g_id )

   

def create_replica_gateway( user, volume=None, **kwargs ):
   return ReplicaGateway.Create( user, **kwargs )


def read_replica_gateway( g_id ):
   return ReplicaGateway.Read( g_id )

def update_replica_gateway( g_id, **fields ):
   return ReplicaGateway.Update( g_id, **fields )

def list_replica_gateways_by_volume( volume_id ):
   return ReplicaGateway.ListAll_ByVolume( volume_id )

def list_replica_gateways( attrs=None, limit=None ):
   return ReplicaGateway.ListAll( attrs, limit=limit )

def get_replica_gateway_by_name( name ):
   rgs = list_replica_gateways( {"ms_username ==" : name} )
   if len(rgs) > 1:
      raise Exception("%s RGs named %s" % (len(rgs), name))

   if len(rgs) == 0:
      return None

   return rgs[0]
   
def delete_replica_gateway( g_id ):
   return ReplicaGateway.Delete( g_id )
   