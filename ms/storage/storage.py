#!/usr/bin/env python

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
def create_user( email, openid_url, **fields ):
   user_key = SyndicateUser.Create( email, openid_url=openid_url, **fields )
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
   user, volume = _read_user_and_volume( email, volume_name_or_id )
   if user == None:
      raise Exception("No such user '%s'" % email)
   if volume == None or volume.deleted:
      raise Exception("No such volume '%s'" % volume_name_or_id )
   
   return VolumeAccessRequest.RequestAccess( user.owner_id, volume.volume_id, caps, message )

def remove_volume_access_request( email, volume_name_or_id ):
   user, volume = _read_user_and_volume( email, volume_id )
   if user == None:
      raise Exception("No such user '%s'" % email)
   if volume == None or volume.deleted:
      raise Exception("No such volume '%s'" % volume_name_or_id )
   
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
   
   else:
      raise Exception("Anonymous user is insufficiently privileged")
   
   return VolumeAccessRequest.ListAccessRequests( volume_id, **q_opts )

def set_volume_access( email, volume_name_or_id, caps, **caller_user_dict ):
   caller_user = caller_user_dict.get("caller_user")
   if caller_user != None:
      # verify ownership
      volume = read_volume( volume_name_or_id )
      if volume == None or volume.deleted:
         raise Exception("No such volume '%s'" % volume_name_or_id )
      
      if volume.owner_id != caller_user.owner_id:
         raise Exception("User '%s' cannot read Volume '%s'" % (caller_user.email, volume.name))
      
   else:
      raise Exception("Anonymous user is insufficiently privileged")
   
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email)
   
   return VolumeAccessRequest.GrantAccess( user.owner_id, volume.volume_id, caps )

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
def create_volume( email, name, description, blocksize, **attrs ):
   user = read_user( email )
   if user != None:
      caller_user = attrs.get("caller_user")
      if caller_user != None:
         if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
            raise Exception("Caller user cannot create Volumes for other users")
         
      # check quota
      user_volume_ids = list_accessible_volumes( email, projection=['volume_id'] )
      if len(user_volume_ids) > user.get_volume_quota():
         raise Exception("User '%s' has exceeded Volume quota" % email )
      
      new_volume_key = Volume.Create( user, blocksize=blocksize, name=name, description=description, **attrs )
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
   return list_volumes( {"Volume.private ==": False}, **q_opts )


def list_archive_volumes( **q_opts ):
   return list_volumes( {"Volume.archive ==": True, "Volume.private ==": False}, **q_opts )


def list_accessible_volumes( email, **q_opts ):
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email)
   
   if 'caller_user' in q_opts.keys():
      caller_user = q_opts['caller_user']
      if user.owner_id != caller_user.owner_id and not caller_user.is_admin:
         raise Exception("User '%s' is not sufficiently privileged" % caller_user.email)
   else:
      raise Exception("Anonymous user is not sufficiently privileged")
      
   def __volume_from_access_request( var ):
      return Volume.Read( var.volume_id, async=True )
   
   q_opts["map_func"] = __volume_from_access_request
   vols = VolumeAccessRequest.ListAll( {"VolumeAccessRequest.requester_owner_id ==": user.owner_id, "VolumeAccessRequest.status ==": VolumeAccessRequest.STATUS_GRANTED}, **q_opts )
   
   ret = filter( lambda x: x != None, vols )
   return ret


def set_volume_public_signing_key( volume_id, new_key, **attrs ):
   caller_user = attrs.get("caller_user")
   if caller_user != None:
      # verify that this user owns this volume
      volume = storage.read_volume( volume_id )
      if volume == None or volume.deleted:
         raise Exception("Volume '%s' does not exist" % volume_id )
      
      if volume.owner_id != caller_user.owner_id and not caller_user.is_admin:
         raise Exception("Caller does not own Volume '%s'" % volume_id )

   else:
      raise Exception("Anonymous user is not sufficiently privileged")
   
   return Volume.SetPublicSigningKey( volume_id, new_key )
   

# ----------------------------------
def create_gateway( volume_id, email, gateway_type, gateway_name, host, port, **kwargs ):
   user, volume = _read_user_and_volume( email, volume_id )
   
   if user == None:
      raise Exception("No such user '%s'" % email)
   
   if (volume == None or volume.deleted):
      raise Exception("No such volume '%s'" % volume_id)
      
   # if the user doesn't own this Volume (or isn't an admin), (s)he must have a valid access request
   if not user.is_admin and user.owner_id != volume.owner_id:
         
      # user must either own this volume, or be allowed to create gateways in it
      access_request = VolumeAccessRequest.GetAccess( user.owner_id, volume.volume_id )
      
      if access_request == None:
         # user has not been given permission to access this volume
         raise Exception("User '%s' is not allowed to access Volume '%s'" % (email, volume.name))
      
      if access_request.status != VolumeAccessRequest.STATUS_GRANTED:
         # user has not been given permission to create a gateway here 
         raise Exception("User '%s' is not allowed to create a Gateway in Volume '%s'" % (email, volume.name))
      
      else:
         if not user.is_admin:
            # admins can set caps to whatever they want, but not users 
            kwargs['caps'] = access_request.gateway_caps
         elif kwargs.has_hey('caps') and kwargs['caps'] != access_request.gateway_caps:
            raise Exception("User '%s' is not allowed to set Gateway capabilities")
   
   # verify quota 
   gateway_quota = user.get_gateway_quota( gateway_type )
   
   if gateway_quota == 0:
      raise Exception("User '%s' cannot create Gateways" % (email))
   
   if gateway_quota > 0:
      gateway_ids = list_gateways_by_user( user.email, keys_only=True )
      if len(gateway_ids) >= gateway_quota:
         raise Exception("User '%s' has too many Gateways" % (email))
   
   gateway_key = Gateway.Create( user, volume, gateway_type=gateway_type, name=gateway_name, host=host, port=port, **kwargs )
   return gateway_key.get()


def read_gateway( g_name_or_id ):
   return Gateway.Read( g_name_or_id )


def update_gateway( g_name_or_id, **fields):
   Gateway.Update( g_name_or_id, **fields )
   return True

def set_gateway_caps( g_name_or_id, caps ):
   gateway = storage.read_gateway( g_name_or_id )
   
   if gateway == None:
      raise Exception("No such Gateway '%s'" % g_name_or_id )
   
   user, volume = _read_user_and_volume( gateway.owner_id, gateway.volume_id )
   
   if user == None:
      raise Exception("No user with ID '%s'" % gateway.owner_id )
   
   if gateway == None:
      raise Exception("No volume with ID '%s'" % gateway.volume_id )
   
   # verify that the user has the capability to do this
   if not user.is_admin and user.owner_id != volume.owner_id:
      raise Exception("User '%s' is not allowed to set the capabilities of Gateway '%s'" % user.email, gateway.name )
   
   Gateway.SetCaps( g_name_or_id )
   return True

def list_gateways( attrs=None, **q_opts):
   return Gateway.ListAll(attrs, **q_opts)


def list_gateways_by_volume( volume_name_or_id, **q_opts ):
   volume = storage.read_volume( volume_name_or_id )
   
   if volume == None or volume.deleted:
      raise Exception("No such Volume '%s'" % volume_name_or_id )
   
   caller_user = q_opts.get("caller_user", None)
   if caller_user:
      if volume.owner_id != caller_user.owner_id and not caller_user.is_admin:
         raise Exception("User '%s' is not sufficiently privileged" % caller_user.email)
   else:
      raise Exception("Anonymous user is not sufficiently privileged")
   
   return Gateway.ListAll( {"Gateway.volume_id ==" : volume_id}, **q_opts )


def list_gateways_by_host( hostname, **q_opts ):
   return Gateway.ListAll( {"Gateway.host ==" : hostname}, **q_opts )


def list_gateways_by_user( email, **q_opts ):
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email )
   
   caller_user = q_opts.get("caller_user", None)
   if caller_user:
      if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
         raise Exception("User '%s' is not sufficiently privileged" % caller_user.email )
      
   else:
      raise Exception("Anonymous user is not sufficiently privileged")
   
   return Gateway.ListAll( {"Gateway.owner_id ==": user.owner_id}, **q_opts )
   
def count_user_gateways( email, **q_opts ):
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email )
   
   qry = Gateway

def delete_gateway( g_id ):
   # TODO: when deleting an AG, delete all of its files and directories as well
   return Gateway.Delete( g_id )

def set_gateway_public_signing_key( g_name_or_id, new_key, **caller_user_dict ):
   caller_user = caller_user_dict.get("caller_user")
   if caller_user != None:
      # verify that this user owns this gateway
      gateway = storage.read_gateway( g_name_or_id )
      if gateway == None:
         raise Exception("Gateway '%s' does not exist" % g_name_or_id )
      
      if gateway.owner_id != caller_user.owner_id:
         raise Exception("Caller user does not own Gateway '%s'" % g_name_or_id )

   else:
      raise Exception("Anonymous user is not sufficiently privileged")
   
   return Gateway.SetPublicSigningKey( g_id, new_key )

def set_gateway_caps( g_name_or_id, caps, caller_user=None ):
   # get the gateway
   gateway = read_gateway( g_name_or_id )
   if gateway == None:
      raise Exception("No such Gateway '%s'" % g_name_or_id )
   
   # get its user and volume
   user_fut = None 
   volume = None
   user = caller_user
   futs = []
   if caller_user == None:
      raise Exception("Anonymous user is not sufficiently privileged")
   
   volume_fut = Volume.Read( gateway.volume_id, async=True )
   futs.append( volume_fut )
   
   storagetypes.wait_futures( futs )
   
   volume = volume_fut.get_result()
   if user_fut != None:
      user = user_fut.get_result()
   
   if volume == None or volume.deleted:
      raise Exception("No such Volume with ID %s" % gateway.volume_id )
   
   if user == None:
      raise Exception("No such User with ID %s" % gateway.owner_id )
   
   # user must be an admin or a volume owner
   if not user.is_admin and user.owner_id != volume.owner_id:
      raise Exception("User '%s' cannot set Gateway '%s' capabilities" % (user.email, gateway.name))
   
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

