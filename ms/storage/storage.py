#!/usr/bin/env python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

"""
This is the high-level storage API.
"""

import storagetypes
from storagetypes import *

import logging

import MS
import MS.entry
from MS.entry import MSEntry, MSEntryShard, MSENTRY_TYPE_DIR
from MS.volume import Volume, VolumeAccessRequest
from MS.user import SyndicateUser
from MS.gateway import Gateway

import types
import errno
import time
import datetime
import random

from common.msconfig import *

def _read_user_and_volume( email, volume_name_or_id ):
   user_fut = SyndicateUser.Read( email, async=True )
   volume_fut = Volume.Read( volume_name_or_id, async=True )
   
   storagetypes.wait_futures( [user_fut, volume_fut] )
   
   user = user_fut.get_result()
   volume = volume_fut.get_result()
   
   if user == None:
      raise Exception("No such User '%s'" % email)
   
   if volume == None:
      raise Exception("No such Volume '%s'" % volume_name_or_id )
   
   return user, volume



def _get_volume_id( volume_name_or_id ):
   try:
      volume_id = int(volume_name_or_id)
      return volume_id
   except:
      volume = Volume.Read( volume_name_or_id )
      if volume == None:
         raise Exception("No such Volume '%s'" % volume_name_or_id )
      else:
         return volume.volume_id
   

# ----------------------------------
def create_user( email, openid_url, signing_public_key, **fields ):
   user_key = SyndicateUser.Create( email, openid_url=openid_url, signing_public_key=signing_public_key, **fields )
   return user_key.get()

def read_user( email ):
   return SyndicateUser.Read( email )

def update_user( email, **fields ):
   SyndicateUser.Update( email, **fields )
   return True

def delete_user( email ):
   return SyndicateUser.Delete( email )

def list_users( attrs=None, **q_opts ):
   return SyndicateUser.ListAll( attrs, **q_opts )
   
def set_user_public_signing_key( email, pubkey ):
   return SyndicateUser.SetPublicSigningKey( email, pubkey )

# ----------------------------------
def list_user_access_requests( email, **q_opts ):
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email)
   return VolumeAccessRequest.ListUserAccessRequests( user.owner_id, **q_opts )

def request_volume_access( email, volume_name_or_id, caps, message ):
   user, volume = _read_user_and_volume( email, volume_id )
   return VolumeAccessRequest.RequestAccess( user.owner_id, volume.volume_id, caps, message )

def remove_volume_access_request( email, volume_name_or_id ):
   user, volume = _read_user_and_volume( email, volume_id )
   return VolumeAccessRequest.RemoveAccessRequest( user.owner_id, volume.volume_id )

def list_volume_access_requests( volume_name_or_id, **q_opts ):
   caller_user = q_opts.get("caller_user")
   if caller_user != None:
      # verify ownership
      volume = read_volume( volume_name_or_id )
      if volume == None or volume.deleted:
         raise Exception("No such volume '%s'" % volume_name_or_id )
      
      if volume.owner_id != caller_user.owner_id:
         raise Exception("User '%s' cannot read Volume '%s'" % (caller_user.email, volume.name))
   
   return VolumeAccessRequest.ListAccessRequests( volume_id, **q_opts )


# ----------------------------------
def list_volume_user_ids( volume_name_or_id, **q_opts ):
   volume_id = _get_volume_id( volume_name_or_id )
   
   def __user_from_access_request( req ):
      return SyndicateUser.Read_ByOwnerID( req.requester_owner_id, async=True )
   
   q_opts["map_func"] = __user_from_access_request
   user_futs = VolumeAccessRequest.ListAll( {"VolumeAccessRequest.volume_id ==": volume_id}, **q_opts )
   
   storagetypes.wait_futures( user_futs )
   
   ret = [u.email for u in filter( lambda x: x != None, [uf.get_result() for uf in user_futs] )]
   return ret

# ----------------------------------
def create_volume( email, name, description, blocksize, signing_public_key, **attrs ):
   user = read_user( email )
   if user != None:
      caller_user = attrs.get("caller_user")
      if caller_user != None:
         if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
            raise Exception("Caller user cannot create Volumes for other users")
         
      # check quota
      user_volume_ids = list_user_volumes( email, projection=['volume_id'] )
      if len(user_volume_ids) > user.get_volume_quota():
         raise Exception("User '%s' has exceeded Volume quota" % email )
      
      new_volume_key = Volume.Create( user, blocksize=blocksize, signing_public_key=signing_public_key, name=name, description=description, **attrs )
      if new_volume_key != None:
         # create succeed.  Make a root directory
         volume = new_volume_key.get()
         MSEntry.MakeRoot( user.owner_id, volume )
         return new_volume_key.get()
         
   else:
      raise Exception("No such user '%s'" % email)
   
   
def read_volume( volume_name_or_id ):
   return Volume.Read( volume_name_or_id )

def volume_update_shard_count( volume_id, num_shards ):
   return Volume.update_shard_count( volume_id, num_shards )
   
def update_volume( volume_name_or_id, **fields ):
   Volume.Update( volume_name_or_id, **fields )
   return True

   
def delete_volume( volume_name_or_id ):
   volume = Volume.Read( volume_name_or_id )
   if volume == None:
      return True
   
   ret = Volume.Delete( volume.volume_id )
   if ret:
      # delete succeeded.  Blow away the MSEntries
      MSEntry.DeleteAll( volume )
   
   return ret
   

def list_volumes( attrs=None, **q_opts ):
   return Volume.ListAll( attrs, **q_opts )


def list_public_volumes( **q_opts ):
   return list_volumes( {"Volume.private !=": False}, **q_opts )


def list_archive_volumes( **q_opts ):
   return list_volumes( {"Volume.archive ==": True}, **q_opts )


def list_user_volumes( email, **q_opts ):
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email)
   
   def __volume_from_user_gateway( ug ):
      return Volume.Read( ug.volume_id, async=True )
   
   q_opts["map_func"] = __volume_from_user_gateway
   vol_futs = Gateway.ListAll( {"Gateway.owner_id ==": user.owner_id, "Gateway.gateway_type ==": GATEWAY_TYPE_UG}, **q_opts )
   
   storagetypes.wait_futures( vol_futs )
   
   ret = filter( lambda x: x != None, [vf.get_result() for vf in vol_futs] )
   return ret


def set_volume_public_signing_key( volume_id, new_key, **attrs ):
   caller_user = attrs.get("caller_user")
   if caller_user != None:
      # verify that this user owns this volume
      volume = storage.read_volume( volume_id )
      if volume == None or volume.deleted:
         raise Exception("Volume '%s' does not exist" % volume_id )
      
      if volume.owner_id != caller_user.owner_id:
         raise Exception("Caller user does not own Volume '%s'" % volume_id )

   return Volume.SetPublicSigningKey( volume_id, new_key )
   

# ----------------------------------
def create_gateway( volume_id, email, gateway_type, signing_public_key, **kwargs ):
   user, volume = _read_user_and_volume( email, volume_id )
   
   if user == None:
      raise Exception("No such user '%s'" % email)
   
   if (volume == None or volume.deleted):
      raise Exception("No such volume '%s'" % volume_id)
      
   # if the user doesn't own this gateway, (s)he must have a valid access request
   if not user.is_admin and user.owner_id != volume.owner_id:
         
      # user must either own this volume, or be allowed to create gateways in it
      access_request = VolumeAccessRequest.GetAccess( user.owner_id, volume.volume_id )
      
      if access_request.status != VolumeAccessRequest.STATUS_GRANTED:
         # user has not been given permission to create a gateway here 
         raise Exception("User '%s' is not allowed to create a Gateway in Volume '%s'" % (email, volume.name))
   
   # verify quota 
   gateway_quota = user.get_gateway_quota( gateway_type )
   
   if gateway_quota > 0:
      gateway_ids = list_gateways_by_user( user.email, keys_only=True )
      if len(gateway_ids) >= gateway_quota:
         raise Exception("User '%s' has too many Gateway instances" % (email))
   
   gateway_key = Gateway.Create( user, volume, gateway_type=gateway_type, signing_public_key=signing_public_key, **kwargs )
   return gateway_key.get()


def read_gateway( g_name_or_id ):
   return Gateway.Read( g_name_or_id )


def update_gateway( g_name_or_id, **fields):
   Gateway.Update( g_id, **fields )
   return True

def set_gateway_caps( g_name_or_id, caps ):
   Gateway.SetCaps( g_name_or_id )
   return True

def list_gateways( attrs=None, **q_opts):
   return Gateway.ListAll(attrs, **q_opts)


def list_gateways_by_volume( volume_id, **q_opts ):
   return Gateway.ListAll( {"Gateway.volume_id ==" : volume_id}, **q_opts )


def list_gateways_by_host( hostname, **q_opts ):
   return Gateway.ListAll( {"Gateway.host ==" : hostname}, **q_opts )


def list_gateways_by_user( email, **q_opts ):
   user = SyndicateUser.Read( email )
   if user == None:
      raise Exception("No such user '%s'" % email )
   
   return Gateway.ListAll( {"Gateway.owner_id ==": user.owner_id}, **q_opts )
   
def count_user_gateways( email, **q_opts ):
   user = SyndicateUser.Read( email )
   if user == None:
      raise Exception("No such user '%s'" % email )
   
   qry = Gateway

def delete_gateway( g_id ):
   return Gateway.Delete( g_id )

def set_gateway_public_signing_key( g_name_or_id, new_key, **attrs ):
   caller_user = attrs.get("caller_user")
   if caller_user != None:
      # verify that this user owns this gateway
      gateway = storage.read_gateway( g_name_or_id )
      if gateway == None:
         raise Exception("Gateway '%s' does not exist" % g_name_or_id )
      
      if gateway.owner_id != caller_user.owner_id:
         raise Exception("Caller user does not own Gateway '%s'" % g_name_or_id )

   return Gateway.SetPublicSigningKey( g_id, new_key )

def set_gateway_caps( g_name_or_id, caps ):
   return Gateway.SetCaps( g_name_or_id, caps )


# ----------------------------------
def get_volume_root( volume ):
   return MSEntry.Read( volume, 0 )

"""
def __msentry_get_user_and_volume( email, volume_id ):
   user_fut = SyndicateUser.Read( email, async=True )
   volume_fut = Volume.Read( volume_id, async=True )
   
   storagtypes.wait_futures( [user_fut, volume_fut] )
   
   user = user_fut.get_result()
   volume = volume_fut.get_result()
   
   return (user, volume)
   
def msentry_op( email, volume_id, func, **ent_attrs ):
   user, volume = __msentry_get_user_and_volume( email, volume_id )
   
   if user == None or volume == None:
      return -errno.ENOENT 
   
   return func( user.owner_id, volume, **ent_attrs )

   
def create_msentry( email, volume_id, **ent_attrs ):
   return msentry_op( email, volume_id, MSEntry.Create, **ent_attrs )
   
   
def read_msentry( email, volume_id, file_id ):
   user, volume = __msentry_get_user_and_volume( email, volume_id )
   
   if user == None or volume == None:
      return -errno.ENOENT 
   
   ent = MSEntry.Read( volume.volume_id, file_id )
   if ent != None:
      if not entry.is_readable( user.owner_id, volume.owner_id, ent.owner_id, ent.mode ):
         return -errno.EACCES
   
   return ent

def update_msentry( email, volume_id, **ent_attrs ):
   return msentry_op( email, volume_id, MSEntry.Update, **ent_attrs )

def delete_msentry( email, volume_id, **ent_attrs ):
   return msentry_op( email, volume_id, MSEntry.Delete, **ent_attrs )

   
def list_msentry_children( volume_id, file_id ):
   volume = Volume.Read( volume_id )
   if volume == None:
      return -errno.ENOENT
   
   return MSEntry.ListAll( volume, file_id )
"""

