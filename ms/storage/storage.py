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
from MS.entry import MSEntry, MSEntryShard
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


   
def make_root( volume, **root_attrs ):
   root = MSEntry( key=make_key( MSEntry, MSEntry.make_key_name( volume_id=root_attrs['volume_id'], fs_path="/" ) ) )
   root.populate( volume.num_shards, **root_attrs )

   root.update_dir_shard( volume.num_shards, **root_attrs )

   root_fut = root.put_async()
   root_fut.wait()

   return 0
   
   
def create_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Create( user_owner_id, volume, **ent_attrs )
   

def update_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Update( user_owner_id, volume, **ent_attrs )

   
def delete_msentry( user_owner_id, volume, **ent_attrs ):
   return MSEntry.Delete( user_owner_id, volume, **ent_attrs )

   
def read_msentry_children( volume, fs_path ):
   return MSEntry.ListAll( volume, fs_path )


def read_msentry_path( volume, fs_path ):
   return MSEntry.Read( volume, fs_path )

   
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


def create_acquisition_gateway( user, **kwargs ):
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

   

def create_replica_gateway( user, **kwargs ):
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
   