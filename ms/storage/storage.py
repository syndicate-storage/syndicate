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
   
def create_volume( user, **kwargs ):
   return Volume.Create( user, **kwargs )
   

def read_volume( name ):
   return Volume.Read( name )
   

def volume_update_shard_count( volume_name, num_shards ):
   return Volume.update_shard_count( volume_name, num_shards )

   
def update_volume( volume_name, **fields ):
   return Volume.Update( volume_name, **fields )

   
def delete_volume( volume_name ):
   return Volume.Delete( volume_name )

   
def list_volumes( **attrs ):
   return Volume.ListAll( **attrs )

   
def create_user( **kwargs ):
   return SyndicateUser.Create( **kwargs )
   

def read_user( email ):
   return SyndicateUser.Read( email )


def update_user( email, **fields ):
   return SyndicateUser.Update( email, **fields )

   
def delete_user( email, **fields ):
   return SyndicateUser.Delete( email, **fields )

   
def list_users( **attrs ):
   return SyndicateUser.ListAll( **attrs )

   
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

   
def create_user_gateway( user, volume, **kwargs ):
   return UserGateway.Create( user, volume, **kwargs )
      

def read_user_gateway( ms_username ):
   return UserGateway.Read( ms_username )
   

def list_user_gateways( volume_id ):
   return UserGateway.ListAll( volume_id )
   
def delete_user_gateway( ms_username ):
   return UserGateway.Delete( ms_username )
   