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
This is the high-level storage API.  It executes storage-related operations (i.e. CRUD, listing, and binding/unbinding)
over collections of objects.  The public API wraps these methods.
"""

import storagetypes
from storagetypes import *

import logging

import MS
import MS.entry
from MS.entry import MSEntry, MSEntryShard, MSENTRY_TYPE_DIR
from MS.volume import Volume, VolumeCertBundle
from MS.user import SyndicateUser
from MS.gateway import Gateway
from MS.index import MSEntryIndex

import types
import errno
import time
import datetime
import random
import base64

import protobufs.ms_pb2 as ms_pb2
import protobufs.sg_pb2 as sg_pb2

from common.msconfig import *
import common.admin_info as admin_info

# ----------------------------------
def _read_user_and_volume( email_or_id, volume_name_or_id ):
   user_fut = SyndicateUser.Read( email_or_id, async=True )
   volume_fut = Volume.Read( volume_name_or_id, async=True )
   
   storagetypes.wait_futures( [user_fut, volume_fut] )
   
   user = user_fut.get_result()
   volume = volume_fut.get_result()
   
   return user, volume

# ----------------------------------
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
def _check_authenticated( kw ):
   # check a method's kw dict for caller_user, and verify it's there 
   caller_user = kw.get("caller_user", None)
   if caller_user is None:
      raise Exception("Anonymous user is insufficiently privileged")
   
   del kw['caller_user']
   return caller_user


# ----------------------------------
def validate_user_cert( user_cert, signing_user_id, check_admin=False ):
   """
   Given a deserialized user certificate, check its validity:
   * make sure user identified by signing_user_id exists
   * make sure the admin identified in user_cert exists, if check_admin is set
   * verify that the user identified by signing_user_id has signed the cert
   
   Return the signing user and the admin user on success
   Raise an exception on error
   """
   
   from common.api import verify_data 
   
   signing_user_fut = SyndicateUser.Read( signing_user_id, async=True )
   admin_user_fut = None
   
   if user_cert.admin_id != signing_user_id:
      admin_user_fut = SyndicateUser.Read( user_cert.admin_id, async=True )
      storagetypes.wait_futures( [signing_user_fut, admin_user_fut] )
   
   else:
      storagetypes.wait_futures( [signing_user_fut] )
   
   signing_user = signing_user_fut.get_result()
   
   if admin_user_fut is not None:
      admin_user = admin_user_fut.get_result()
   else:
      
      # signing user and admin are the same
      admin_user = signing_user 
      
   if check_admin:
      
      if admin_user is None:
         raise Exception("The admin (%s) who signed this user cert does not exist" % user_cert.admin_id )
      
      if not admin_user.is_admin:
         raise Exception("User '%s' is not an admin" % (admin_user.email))
   
   if signing_user is None:
      raise Exception("No such user '%s'" % signing_user_id )
   
   user_pubkey = signing_user.public_key 
   user_sig = user_cert.signature 
   
   user_cert.signature = ""
   
   user_cert_nosigs = user_cert.SerializeToString()
   
   rc = verify_data( user_pubkey, user_cert_nosigs, base64.b64decode( user_sig ) )
   
   user_cert.signature = user_sig 
   
   if not rc:
      raise Exception("User '%s' did not sign cert" % (signing_user.email))
   
   return signing_user, admin_user
   

# ----------------------------------
def create_user( email, **fields ):
   """
   Create a user:
   * must be given user_cert_bin as a keyword, which must be an ms_user_cert
   * the cert must be signed by the admin it identifies 
   * the user must not exist
   * an existing admin must have signed the user cert
   * said admin has to have been the one to call this method
   """
   
   if not fields.has_key('user_cert_b64'):
      raise Exception("Missing serialized user cert")
   
   user_cert = None
   try:
      user_cert_bin = base64.b64decode( fields['user_cert_b64'] )
      user_cert = ms_pb2.ms_user_cert()
      user_cert.ParseFromString( user_cert_bin )
   except Exception, e:
      traceback.print_exc()
      raise Exception("Failed to parse serialized user cert")
   
   caller_user = _check_authenticated( fields )
   
   if not caller_user.is_admin:
      raise Exception("Only an admin can call this method")
   
   _, admin_user = validate_user_cert( user_cert, caller_user.owner_id, check_admin=True )
   
   if admin_user.owner_id != caller_user.owner_id:
      raise Exception("This admin user did not sign the user cert")
   
   user_key = SyndicateUser.Create( user_cert )
   return user_key.get()

# ----------------------------------
def read_user( email_or_user_id ):
   """
   Read a user, given either its email or user ID.
   """
   return SyndicateUser.Read( email_or_user_id )


# ----------------------------------
def delete_user( email, **kw ):
   """
   Delete the given user.  Only an admin can do this,
   and only if the user was created by that admin.
   """
   
   caller_user = _check_authenticated( kw )
   
   if not caller_user.is_admin:
      raise Exception("Only an admin can call this method")
   
   user = SyndicateUser.Read( email )
   if user is None:
      raise Exception("No such user")
   
   if not caller_user.is_admin:
      raise Exception("Caller is not an admin")

   if user.admin_id != caller_user.owner_id:
      raise Exception("Admin '%s' did not create user '%s'" % (caller_user.email, email))
  
   ret = SyndicateUser.Delete( email )
   return {'result': ret}


# ----------------------------------
def list_users( attrs=None, **q_opts ):
   return SyndicateUser.ListAll( attrs, **q_opts )


# ----------------------------------
def reset_user( email, **fields ):
   
   if not fields.has_key('user_cert_b64'):
      raise Exception("Missing serialized user cert")
   
   user_cert = None
   try:
      user_cert_bin = base64.b64decode( fields['user_cert_b64'] )
      user_cert = ms_pb2.ms_user_cert()
      user_cert.ParseFromString( user_cert_bin )
   except Exception, e:
      raise Exception("Failed to parse serialized user cert")
   
   if user_cert.email != email:
      raise Exception("Invalid certificate")
   
   caller_user = _check_authenticated( fields )
   
   if not caller_user.is_admin:
      raise Exception("Only an admin can call this method")
   
   _, admin_user = validate_user_cert( user_cert, caller_user.owner_id, check_admin=True )
   
   if admin_user.owner_id != caller_user.owner_id:
      raise Exception("This admin user did not sign the user cert")
   
   # do the reset 
   user_key = SyndicateUser.Reset( user_cert )
   if user_key is not None:
       return {"result": True}
   else:
       return {"result": False}


def validate_volume_cert( volume_cert, signing_user_id=None ):
   """
   Given a deserialized volume certificate, check its validity:
   * make sure the user exists
   * make sure the user in the cert signed it, or verify that the user identified by signing_user_id did 
     (e.g. signing_user_id might correspond to a volume owner or admin)
   
   Return the (user, volume) on success (volume can be None if we haven't created it yet)
   Raise an exception on error
   """
   
   from common.api import verify_data 
   
   # user and volume must both exist
   user, volume = _read_user_and_volume( volume_cert.owner_id, volume_cert.volume_id )
   
   if user is None:
      raise Exception("No such user '%s'" % volume_cert.owner_id)
   
   if signing_user_id is not None and user.owner_id != signing_user_id:
      raise Exception("User '%s' did not sign this volume cert (got '%s' instead)" % (signing_user_id, user.owner_id))
      
   # verify that the user has signed the cert 
   owner_pubkey = user.public_key 
   owner_sig = volume_cert.signature 
   volume_root = None 
   
   if volume_cert.HasField("root"):
       volume_root = volume_cert.root 
       volume_cert.ClearField("root")
       
   volume_cert.signature = ""
   
   volume_cert_nosigs = volume_cert.SerializeToString()
   
   if volume_root is not None:
      volume_cert.root.MergeFrom( volume_root )
      
   volume_cert.signature = owner_sig
   
   rc = verify_data( owner_pubkey, volume_cert_nosigs, base64.b64decode( owner_sig ) )
   
   if not rc:
      raise Exception("Volume certificate not signed by user '%s'" % volume_cert.owner_id )

   return user, volume



def validate_cert_bundle( cert_bundle, user, volume_id, volume_version, new_gateway_cert=None ):
   """
   Given a deserialized cert bundle, check its validity:
   * make sure the user it points to exists
   * make sure it has the right volume ID (load from the datastore by default, or use volume_id or volume kw if given)
   * make sure the user signed it, or verify that the user identified by signing_user_id did.
   * make sure its volume version is equal to or exceeds the given volume_version
   * make sure the cert bundle's timestamp exceeds the previous cert bundle's timestamp
   * make sure each of its gateway versions are equal to or exceeds the local copies of the gateway certs' versions
   --- if new_gateway_cert is given, it will be checked in place of an on-file gateway cert.  This is useful 
       when updating a gateway--we need to check the gateway cert that will be written, not the one on file.
       
   Return True if so 
   Raise an exception on error.
   """
   
   from common.api import verify_data 
   
   if user.owner_id != cert_bundle.owner_id:
      raise Exception("Invalid cert bundle: invalid owner ID")
   
   # verify the user signed the cert 
   owner_pubkey = user.public_key 
   owner_sig = cert_bundle.signature 
   
   cert_bundle.signature = ""
   cert_bundle_nosigs = cert_bundle.SerializeToString()
   
   rc = verify_data( owner_pubkey, cert_bundle_nosigs, base64.b64decode( owner_sig ) )
   
   cert_bundle.signature = owner_sig 
   
   if not rc:
      raise Exception("Cert bundle not signed by user '%s'" % signing_user_id)
   
   # volume ID must match
   if volume_id != cert_bundle.volume_id:
      raise Exception("Invalid volume ID: %s != %s" % (volume_id, cert_bundle.volume_id))
   
   # volume version must increase or stay the same
   # NOTE: Manifest.file_version encodes the volume version, when being used as a cert bundle
   if volume_version > cert_bundle.file_version:
      raise Exception("Stale volume version for %s: expected > %s, got %s" % (volume_id, volume_version, cert_bundle.file_version))
   
   # get the previous cert bundle 
   prev_cert_bundle_rec = VolumeCertBundle.Get( volume_id )
   if prev_cert_bundle_rec is not None:
      
      # this cert bundle's timestamp must be newer 
      prev_cert_bundle = VolumeCertBundle.Load( prev_cert_bundle_rec )
      if prev_cert_bundle is not None:
         
         if prev_cert_bundle.mtime_sec > cert_bundle.mtime_sec or (prev_cert_bundle.mtime_sec == cert_bundle.mtime_sec and prev_cert_bundle.mtime_nsec > cert_bundle.mtime_nsec):
            # stale 
            raise Exception("Stale cert bundle for %s: expected > %s.%s, got %s.%s" % (volume_id, prev_cert_bundle.mtime_sec, prev_cert_bundle.mtime_nsec, cert_bundle.mtime_sec, cert_bundle.mtime_nsec))
         
   
   gateway_cert_and_fut = {}    # map gateway ID to (gateway cert block, gateway future)
   all_futs = []
   
   # cert is valid.
   # validate volume cert--it must match information in the cert manifest 
   if len(cert_bundle.blocks) == 0:
       raise Exception("Missing volume cert in cert bundle")
   
   volume_cert_block = cert_bundle.blocks[0]
   if volume_cert_block.block_id != volume_id:
       raise Exception("Cert block for volume does not match volume ID (expected %s, got %s)" % (volume_id, volume_cert_block.block_id))
   
   if volume_cert_block.block_version != cert_bundle.file_version:
       raise Exception("Cert block for volume does not match volume version (expected %s, got %s)" % (cert_bundle.file_version, volume_cert_block.block_version))
   
   if volume_cert_block.owner_id != user.owner_id:
       raise Exception("Cert block for volume does not match owner ID (expected %s, got %s)" % (user.owner_id, volume_cert_block.owner_id))
   
   # go validate the affected gateways    
   # NOTE: blocks[0] contains the volume cert (used by gateways)
   for i in xrange( 1, len(cert_bundle.blocks) ):
      
      block = cert_bundle.blocks[i]
      gateway_id = block.block_id
      
      if new_gateway_cert is not None and new_gateway_cert.gateway_id == gateway_id:
         # skip this one--we'll check below
         continue
      
      gateway_fut = Gateway.Read( gateway_id, async=True )
      
      gateway_cert_and_fut[ gateway_id ] = (block, gateway_fut)
      all_futs.append( gateway_fut )
   
   
   storagetypes.wait_futures( all_futs )
   
   for (gateway_id, (block, gateway_fut)) in gateway_cert_and_fut.items():
      
      # from the cert bundle
      given_gateway_version = None 
      given_gateway_caps = None 
      
      # on file
      gateway_version = None 
      gateway_caps = None
      
      if new_gateway_cert is not None and new_gateway_cert.gateway_id == gateway_id:
         
         # make sure the new gateway cert has an appropriate version 
         given_gateway_version = new_gateway_cert.gateway_version
         given_gateway_caps = new_gateway_cert.caps
         
         # don't worry about the caps--we'll be updating them shortly 
         gateway_version = gateway.version
         gateway_caps = new_gateway_cert.caps
         
      else:
         
         # use the gateway on file
         gateway = gateway_fut.get_result()
         if gateway is None:
            
            # requested a gateway that didn't exist 
            raise Exception("Cert bundle identifies non-existant gateway %s" % gateway_id)
         
         if gateway.g_id != gateway_id:
            
            # this is a bug--the gateway is keyed by id 
            raise Exception("BUG: requested %s, got %s" % (gateway_id, gateway.g_id))
         
         given_gateway_caps = block.caps 
         given_gateway_version = gateway.cert_version      # can only check new gateway certs
         
         # check against on-file cert 
         gateway_version = gateway.cert_version 
         gateway_caps = gateway.caps
      
      
      # gateway version must increase or stay the same
      if gateway_version > given_gateway_version:
         
         # stale version
         raise Exception("Stale version for %s: expected >= %s, got %s" % (gateway_id, gateway_version, given_gateway_version))
      
      if (gateway_caps | given_gateway_caps) != given_gateway_caps:
         
         # invalid caps (only works when checked against on-file gateways)
         raise Exception("Invalid caps for %s: expected %s, got %s" % (gateway_id, gateway.caps, given_gateway_caps))
      
   # valid!
   return True


def create_volume( **attrs ):
   """
   Create a volume.
   * extract the parameters from the volume cert, given as 'volume_cert_b64' in **attrs 
   * verify that the user-to-receive signed the cert 
   * extract the volume cert bundle manifest ('cert_bundle_b64' in attrs), and verify that it is signed by the same user.
   * generate and store the volume from the certificate, keeping the cert on file.
   * put the cert bundle manifest.
   
   Return the Volume on success.
   Raise an exception on error.
   """
   
   from common.api import verify_data 
   
   volume_cert_b64 = attrs.get('volume_cert_b64', None)
   if volume_cert_b64 is None:
      raise Exception("Missing 'volume_cert_b64'")
   
   cert_bundle_b64 = attrs.get('cert_bundle_b64', None)
   if cert_bundle_b64 is None:
      raise Exception("Missing 'cert_bundle_b64'")
   
   try:
      volume_cert_bin = base64.b64decode( volume_cert_b64 )
      volume_cert = ms_pb2.ms_volume_metadata()
      volume_cert.ParseFromString( volume_cert_bin )
   except Exception, e:
      log.error("Failed to deserialize volume certificate")
      raise e
   
   try:
      cert_bundle_bin = base64.b64decode( cert_bundle_b64 )
      cert_bundle = sg_pb2.Manifest()
      cert_bundle.ParseFromString( cert_bundle_bin )
   except Exception, e:
      log.error("Failed to deserialize certificate bundle version vector")
      raise e
   
   # must include a root 
   if not volume_cert.HasField("root"):
      raise Exception("Missing root inode")
   
   caller_user = _check_authenticated( attrs )
   user, volume = validate_volume_cert( volume_cert, signing_user_id=caller_user.owner_id )
   
   # user should exist
   if user is None:
      raise Exception("No such user '%s'" % volume_cert.owner_id)
   
   # check both email and id 
   if user.email != volume_cert.owner_email or user.owner_id != volume_cert.owner_id:
      raise Exception("Cert/User mismatch: expected %s (%s), got %s (%s)" % (user.email, user.owner_id, volume_cert.owner_email, volume_cert.owner_id))
   
   # volume shouldn't exist
   if volume is not None and not volume.deleted:
      raise Exception("Volume '%s' exists" % volume_cert.name )
   
   # "fake" volume version, since the volume version doesn't exist yet...
   rc = validate_cert_bundle( cert_bundle, user, volume_cert.volume_id, 0 )
   if not rc:
      raise Exception("Invalid volume cert bundle")
   
   # a user can only create volumes for herself (admin can for anyone)
   if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
      raise Exception("Caller cannot create Volumes for other users")
   
   # check quota for this user
   if user.get_volume_quota() >= 0:
            
      user_volume_ids_qry = Volume.ListAll( {"Volume.owner_id ==": user.owner_id}, keys_only=True, query_only=True )
      num_volumes = user_volume_ids_qry.count()
        
      if num_volumes > user.get_volume_quota():
         raise Exception("User '%s' has exceeded Volume quota of %s" % (user.email, user.get_volume_quota()) )
   
   # verify the root inode
   root_inode = volume_cert.root
   
   root_inode_sigb64 = root_inode.signature
   root_inode_sig = base64.b64decode( root_inode_sigb64 )
   root_inode.signature = "" 
   
   root_inode_nosig = root_inode.SerializeToString()
   
   root_inode.signature = root_inode_sigb64
   
   rc = verify_data( caller_user.public_key, root_inode_nosig, root_inode_sig )
   if not rc:
      raise Exception("Root inode not signed by user '%s'" % (user.email))
   
   # finally, verify that the name is *not* numeric 
   tmp = None 
   try:
      tmp = int( volume_cert.name )
   except:
      pass 
   
   if tmp is not None:
      raise Exception("Invalid volume name '%s'" % volume_cert.name )
   
   new_volume_key = Volume.Create( user, volume_cert )
   if new_volume_key is not None:
      
      volume = new_volume_key.get()
      
      # put the root inode 
      MSEntry.MakeRoot( user.owner_id, volume, volume_cert.root )
      
      # put the cert bundle 
      VolumeCertBundle.Put( volume.volume_id, cert_bundle_bin )
      
      return volume 
   
   else:
      raise Exception("Failed to create Volume")
   

# ----------------------------------
def read_volume( volume_name_or_id ):
   return Volume.Read( volume_name_or_id )

# ----------------------------------
def volume_update_shard_count( volume_id, num_shards ):
   return Volume.update_shard_count( volume_id, num_shards )
   
# ----------------------------------
def update_volume( volume_name_or_id, **attrs ):
   """
   Update a volume.
   Expect 'volume_cert_b64' as a keyword argument, which contains the serialized ms_volume_metadata cert with the new information.
   Expect 'cert_bundle_b64' as a keyword argument, which contains the serialized Manifest with the new cert version vector.
   
   Return True on success
   Raise an Exception on error.
   """
   
   volume_cert_b64 = attrs.get('volume_cert_b64', None)
   if volume_cert_b64 is None:
      raise Exception("Missing 'volume_cert_b64'")
   
   cert_bundle_b64 = attrs.get('cert_bundle_b64', None)
   if cert_bundle_b64 is None:
      raise Exception("Missing 'cert_bundle_b64'")
   
   
   volume_cert_bin = base64.b64decode( volume_cert_b64 )
   cert_bundle_bin = base64.b64decode( cert_bundle_b64 )
   
   try:
      volume_cert = ms_pb2.ms_volume_metadata() 
      volume_cert.ParseFromString( volume_cert_bin )
   except Exception, e:
      log.error("Failed to deserialize volume certificate")
      raise e
   
   # check name here...
   if volume_name_or_id != volume_cert.name and volume_name_or_id != volume_cert.volume_id:
      raise Exception("Invalid volume name or id '%s'" % volume_name_or_id)
   
   try:
      cert_bundle = sg_pb2.Manifest()
      cert_bundle.ParseFromString( cert_bundle_bin )
   except Exception, e:
      log.error("Failed to deserialize certificate bundle version vector")
      raise e
   
   caller_user = _check_authenticated( attrs )
   user, volume = validate_volume_cert( volume_cert, signing_user_id=caller_user.owner_id )
   
   # user should exist 
   if user is None:
      raise Exception("No such user '%s'" % volume_cert.owner_id)
   
   # volume should exist 
   if volume is None or volume.deleted:
      raise Exception("No such volume '%s'" % volume_cert.volume_id)
   
   # user should have signed the cert bundle
   rc = validate_cert_bundle( cert_bundle, user, volume.volume_id, volume.version )
   if not rc:
      raise Exception("Invalid volume cert bundle")
   
   # a user can only update volumes she owns (admin can for anyone)
   if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
      raise Exception("Caller cannot update volume for user %s" % user.owner_id)
   
   # finally, verify that the name is *not* numeric 
   tmp = None 
   try:
      tmp = int( volume_cert.name )
   except:
      pass 
   
   if tmp is not None:
      raise Exception("Invalid volume name '%s'" % volume_cert.name )
   
   volume_key = Volume.Update( volume_name_or_id, volume_cert )
   if volume_key is not None:
      
      # put the new cert bundle
      rc = VolumeCertBundle.Put( volume.volume_id, cert_bundle_bin )
      if not rc:
         raise Exception("Failed to put cert bundle for volume '%s'" % volume.volume_id )
      
   return {"result": True}


# ----------------------------------
def delete_volume( volume_name_or_id, **attrs ):
   """
   Delete a volume, given its name or ID
   """
   caller_user = _check_authenticated( attrs )
   
   volume = Volume.Read( volume_name_or_id )
   if volume is None or volume.deleted:
      return {"result": True}

   if volume.owner_id != caller_user.owner_id and not caller_user.is_admin:
      raise Exception("Caller cannot delete volume")
   
   ret = Volume.Delete( volume.volume_id )
   if ret:
      # delete succeeded.
      
      # Blow away cert bundle, in deferred task 
      storagetypes.deferred.defer( VolumeCertBundle.Delete, volume.volume_id )
      
      # Blow away the MSEntries, in a deferred task 
      storagetypes.deferred.defer( MSEntry.DeleteAll, volume )
      
      # Blow away the Gateways, in a deferred task 
      storagetypes.deferred.defer( Gateway.DeleteAll, volume )
   
   return {'result': ret}
   
# ----------------------------------
def list_volumes( attrs=None, **q_opts ):
   return Volume.ListAll( attrs, **q_opts )

# ----------------------------------
def list_public_volumes( **q_opts ):
   return list_volumes( {"Volume.private ==": False, "Volume.deleted ==": False}, **q_opts )

# ----------------------------------
def list_archive_volumes( **q_opts ):
   return list_volumes( {"Volume.archive ==": True, "Volume.private ==": False, "Volume.deleted ==": False}, **q_opts )


def validate_gateway_cert( gateway_cert, signing_user_id ):
   """
   Given a deserialized gateway certificate, check its validity:
   * make sure the user and volume it points to exist
   * make sure the user in the cert signed it, or verify that the user identified by signing_user_id did 
     (e.g. signing_user_id might correspond to a volume owner or admin)
   
   Return the user, volume, and signing user (if found) on success 
   Raise an exception on error
   """
   
   from common.api import verify_data 
   
   # user and volume must both exist
   user, volume = _read_user_and_volume( gateway_cert.owner_id, gateway_cert.volume_id )
   signing_user = None
   
   if user is None:
      raise Exception("No such user '%s'" % gateway_cert.owner_id)
   
   if (volume is None or volume.deleted):
      raise Exception("No such volume '%s'" % gateway_cert.volume_id)

   pubkey = None
   signer_email = None
   signing_user = None

   if signing_user_id is not None:
       signing_user = SyndicateUser.Read( signing_user_id )
       if signing_user is None:
           raise Exception("No such signing user '%s'" % signing_user_id)

       pubkey = signing_user.public_key
       signer_email = signing_user.email

   else:
       pubkey = user.public_key
       signer_email = user.email
   
   # verify that the user (or signing user) has signed the cert 
   owner_sig = gateway_cert.signature 
   
   gateway_cert.signature = ""
   gateway_cert_nosigs = gateway_cert.SerializeToString()
   
   rc = verify_data( pubkey, gateway_cert_nosigs, base64.b64decode( owner_sig ) )
   
   gateway_cert.signature = owner_sig 
   
   if not rc:
      raise Exception("Gateway certificate not signed by user '%s'" % signer_email )

   return (user, volume)
   

def create_gateway( **kw ):
   """
   Create a gateway.
   * make sure the calling user exists 
   * make sure the owning user exists 
   * make sure the volume exists
   * make sure the calling user has access rights to the volume
   * make sure we have the certificate and everything we need from it.
   * (DEPRECATED) if this gateway is going into an archive volume, make sure that it's the only writer.
   
   Expects 'gateway_cert_b64', 'cert_bundle_b64', 'driver_text', and 'caller_user' from kw.
   * gateway_cert_bin must be a protobuf'ed gateway certificate, signed by the user to own the gateway 
   * cert_bundle_bin must be a protobuf'ed cert bundle version vector, signed by the volume owner
   
   Return the gateway on success, and put the new cert bundle for all publicly-routable gateways
   Raise an exception on error.

   TODO: allow multiple archive writers; deny coordinator changes in archives
   """
   
   gateway_cert_b64 = kw.get('gateway_cert_b64', None)
   if gateway_cert_b64 is None:
      raise Exception("No gateway certificate given")
   
   cert_bundle_b64 = kw.get('cert_bundle_b64', None)
   if cert_bundle_b64 is None:
      raise Exception("No certificate bundle version vector given")
   
   driver_text = None
   if 'driver_text' in kw.keys():
      driver_text = kw['driver_text']
   
   # check well-formed
   try:
      gateway_cert_bin = base64.b64decode( gateway_cert_b64 )
      gateway_cert = ms_pb2.ms_gateway_cert()
      gateway_cert.ParseFromString( gateway_cert_bin )
   except Exception, e:
      log.error("Failed to deserialize gateway certificate")
      raise e
   
   # check authentication
   caller_user = _check_authenticated( kw )
   if caller_user is None:
      raise Exception("User is not authenticated")
   
   # check existence of related objects
   user = None 
   volume = None
   try:
      user, volume = validate_gateway_cert( gateway_cert, None )
   except Exception, e:
      log.error("Failed to find either the user or volume")
      raise e
  
   # only the volume owner can call this method 
   if volume.owner_id != caller_user.owner_id:
      log.error("Caller user '%s' is not the volume owner (%s)" % (caller_user.email, volume.owner_id))
      raise Exception("Caller user '%s' is not the volume owner (%s)" % (caller_user.email, volume.owner_id))

   volume_owner = read_user( volume.owner_id )
   if volume_owner is None:
       raise Exception("BUG: No owner for volume '%s'" % volume.name)

   try:
      cert_bundle_bin = base64.b64decode( cert_bundle_b64 )
      cert_bundle = sg_pb2.Manifest()
      cert_bundle.ParseFromString( cert_bundle_bin )
   except Exception, e:
      log.error("Failed to deserialize certificate bundle version vector")
      raise e
   
   rc = validate_cert_bundle( cert_bundle, volume_owner, volume.volume_id, volume.version, new_gateway_cert=gateway_cert )
   if not rc:
      raise Exception("Failed to validate cert bundle version vector")
   
   # unpack certificate
   gateway_name = gateway_cert.name 
   volume_id = gateway_cert.volume_id 
   
   # verify volume ID against the cert bundle 
   if volume_id != cert_bundle.volume_id != volume_id:
      raise Exception("Cert bundle is not for volume %s" % volume_id)
   
   # verify quota 
   gateway_quota = user.get_gateway_quota()
   
   if gateway_quota == 0 and not caller_user.is_admin:
      raise Exception("User '%s' cannot create Gateways for User %s" % (caller_user.email, user.email))
   
   if gateway_quota > 0:
      gateway_ids = list_gateways_by_user( user.email, caller_user=user, keys_only=True )
      if len(gateway_ids) > gateway_quota and not caller_user.is_admin:
         raise Exception("User '%s' has exceeded quota (%s) for gateways" % (user.email, gateway_quota))
   
   # caller user must be the same as the owning user, unless admin
   if not caller_user.is_admin and caller_user.owner_id != user.owner_id:
      raise Exception("Caller can only create gateways for itself.")
      
   if volume.private:
       
      # only volume owner or admin can create gateways in private volumes
      if not caller_user.is_admin and caller_user.owner_id != volume.owner_id:
         raise Exception("User '%s' is not allowed to create gateways for '%s'" % (caller_user.email, volume.name))
   
   # if this is an archive volume, then there can be no other writers (DEPRECATED)
   if volume.archive and (gateway_cert.caps & (GATEWAY_CAP_WRITE_DATA | GATEWAY_CAP_WRITE_METADATA)):
      writer_gateways_qry = Gateway.ListAll( {"Gateway.need_cert ==": True}, keys_only=True, query_only=True )
      if writer_gateways_qry.count() > 0:
         # there's already a writer 
         raise Exception("Archive volume '%s' already has a writer" % (volume.name))
      
   # sanity check: name can't be numeric
   tmp = None
   try:
      tmp = int( gateway_name )
   except:
      pass
   
   if tmp is not None:
      raise Exception("Invalid gateway name: cannot be numeric")
   
   gateway_key = Gateway.Create( user, volume, gateway_cert, driver_text )
   
   # put cert bundle as well 
   rc = VolumeCertBundle.Put( volume_id, cert_bundle_bin )
   if not rc:
      raise Exception("Invalid volume cert bundle")
   
   gw = gateway_key.get()
   
   return gw


# ----------------------------------
def read_gateway( g_name_or_id ):
   return Gateway.Read( g_name_or_id )


# ----------------------------------
def update_gateway( g_name_or_id, **kw ):
   """
   Update a Gateway. **kw contains both the caller_user and the serialized gateway certificate.
   * verify that the user and volume exist 
   * verify that the user signed the cert 
   
   If updating a gateway's capabilities to beyond what the current cert bundle allows,
   a new cert bundle will be required.
   
   Return True on success, and store the updated gateway and cert volume version vector
   Raise an exception on error
   """
   
   from common.api import verify_data 
   
   gateway_cert_b64 = kw.get('gateway_cert_b64', None)
   if gateway_cert_b64 is None:
      raise Exception("No gateway certificate given")
   
   gateway_cert_bin = base64.b64decode( gateway_cert_b64 )
   
   try:
      gateway_cert = ms_pb2.ms_gateway_cert()
      gateway_cert.ParseFromString( gateway_cert_bin )
   except Exception, e:
      log.error("Failed to deserialize gateway certificate")
      raise e
   
   # sanity check 
   if g_name_or_id != gateway_cert.gateway_id and g_name_or_id != gateway_cert.name:
      raise Exception("Gateway name mismatch: expected %s or %s, got %s" % (gateway_cert.gateway_id, gateway_cert.name, g_name_or_id))
   
   caller_user = _check_authenticated( kw )
   
   # validate the gateway cert
   user, volume = validate_gateway_cert( gateway_cert, gateway_cert.owner_id ) 
   gateway = read_gateway( g_name_or_id )
   if gateway is None:
       raise Exception("No such gateway '%s'" % g_name_or_id)

   # user must own this gateway 
   if gateway.owner_id != user.owner_id:
       raise Exception("User '%s' does not own gateway '%s'" % (user.email, gateway.name))

   # caller user must be the volume owner, the admin, or the gateway owner
   if not caller_user.is_admin and caller_user.owner_id != volume.owner_id and caller_user.owner_id != gateway.owner_id:
      raise Exception("Only the volume owner, gateway owner, or admin can update this gateway")
   
   cert_bundle = None
   cert_bundle_bin = None
   new_cert_bundle = False
   if kw.has_key( 'cert_bundle_b64' ):
       
       # given a cert bundle
       new_cert_bundle = True
       cert_bundle_bin = base64.b64decode( kw['cert_bundle_b64'] )
       
       try:
           cert_bundle = sg_pb2.Manifest()
           cert_bundle.ParseFromString( cert_bundle_bin )
       except Exception ,e:
           log.error("Failed to deserialize certificate bundle")
           raise e
       
       # caller user must be the volume owner, or admin
       if caller_user.owner_id != volume.owner_id:
           raise Exception("Calling user is not the volume owner")

       # validate the cert bundle
       rc = validate_cert_bundle( cert_bundle, caller_user, volume.volume_id, volume.version )
       if not rc:
          raise Exception("Failed to validate cert bundle version vector")
      
   else:
       # get on-file cert bundle 
       cert_bundle_rec = VolumeCertBundle.Get( volume.volume_id )
       if cert_bundle_rec is None:
          raise Exception("No cert bundle on file for Volume '%s'" % (volume.name))
  
       # deserialize 
       cert_bundle = VolumeCertBundle.Load( cert_bundle_rec )
   
   # this gateway must be in the cert bundle 
   present = False
   caps = 0
   for block in cert_bundle.blocks:
       if block.block_id == gateway_cert.gateway_id:
           # found!
           present = True 
           caps = block.caps
           break
   
   if not present:
       raise Exception("Gateway '%s' is not in cert bundle for Volume '%s'" % (gateway.name, volume.name))
   
   # caps can decrease, but not increase (unless we're the volume owner)
   if (caps | gateway_cert.caps) != caps:
       if not caller_user.is_admin and caller_user.owner_id != gateway.owner_id:
          raise Exception("Invalid capability bits %s (expected subset of %s)" % (hex(gateway_cert.caps), hex(caps)))

       elif not kw.has_key( 'cert_bundle_b64' ):
          # need a volume cert bundle to attest to the cap change 
          raise Exception("CLIENT BUG: cannot change gateway cert caps without a cert bundle")
  
   # cert expiration time can decrease, but not increase 
   if gateway_cert.cert_expires > gateway.cert_expires:
       raise Exception("Gateway certificate expiration cannot increase (%s > %s)" % (gateway_cert.cert_expires, gateway.cert_expires))

   # version must be newer
   if gateway_cert.version < gateway.cert_version:
       raise Exception("Stale cert version (%s < %s)" % (gateway_cert.cert_version, gateway.cert_version))

   # if given a new cert bundle, put it in place
   if new_cert_bundle:
       rc = VolumeCertBundle.Put( volume.volume_id, cert_bundle_bin )
       if not rc:
          raise Exception("Invalid volume cert bundle")
   
   # do the update
   new_driver = kw.get('driver_text', None)
   if new_driver is not None:
       new_driver = str(new_driver)

   gw_key = Gateway.Update( gateway_cert, new_driver=new_driver )   
   return {"result": True}


# ----------------------------------
def list_gateways( attrs=None, **q_opts):
   return Gateway.ListAll(attrs, **q_opts)


# ----------------------------------
def list_gateways_by_volume( volume_name_or_id, **q_opts ):
   caller_user = _check_authenticated( q_opts )
   
   # volume must exist
   volume = read_volume( volume_name_or_id )
   if volume == None or volume.deleted:
      raise Exception("No such Volume '%s'" % volume_name_or_id )
   
   # only admin can list gateways of volumes she doesn't own
   if volume.owner_id != caller_user.owner_id and not caller_user.is_admin:
      raise Exception("User '%s' is not sufficiently privileged" % caller_user.email)
   
   return Gateway.ListAll( {"Gateway.volume_id ==" : volume.volume_id, "Gateway.deleted ==": False}, **q_opts )


# ----------------------------------
def list_gateways_by_host( hostname, **q_opts ):
   return Gateway.ListAll( {"Gateway.host ==" : hostname, "Gateway.deleted ==": False}, **q_opts )

# ----------------------------------
def list_gateways_by_user( email, **q_opts ):
   caller_user = _check_authenticated( q_opts )
   
   # user must exist
   user = read_user( email )
   if user == None:
      raise Exception("No such user '%s'" % email )
   
   # only admin can list other users' gateways
   if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
      raise Exception("User '%s' is not sufficiently privileged" % caller_user.email )
   
   return Gateway.ListAll( {"Gateway.owner_id ==": user.owner_id, "Gateway.deleted ==": False}, **q_opts )

# ----------------------------------
def list_gateways_by_user_and_volume( email, volume_name_or_id, **q_opts ):
   caller_user = _check_authenticated( q_opts )
   
   user, volume = _read_user_and_volume( email, volume_name_or_id )
   
   # user and volume must exist 
   if user is None:
      raise Exception("No such user '%s'" % email)
   
   if volume is None or volume.deleted:
      raise Exception("No such Volume '%s'" % email)
   
   # only admin can list other users' gateways 
   if caller_user.owner_id != user.owner_id and not caller_user.is_admin:
      raise Exception("User '%s' is not sufficiently privileged" % caller_user.email )
   
   return Gateway.ListAll( {"Gateway.owner_id ==": user.owner_id, "Gateway.volume_id ==": volume.volume_id, "Gateway.deleted ==": False}, **q_opts )


# ----------------------------------
def _remove_user_from_volume_helper( owner_id, volume_id ):
   # helper function for remove_user_from_volume, to be run deferred.
   
   def _remove_gateway( gw ):
      Gateway.Delete( gw.g_id )
      return None
      
   Gateway.ListAll( {"Gateway.owner_id ==": owner_id, "Gateway.volume_id ==": volume_id, "Gateway.deleted ==": False}, map_func=_remove_gateway, projection=['g_id'] )
   

# ----------------------------------
def remove_user_from_volume( email, volume_name_or_id ):
   # NOTE: it's assumed that the caller has done the appropriate authentication.
   
   user, volume = _read_user_and_volume( email, volume_name_or_id )
   
   # user and volume must exist 
   if user is None:
      raise Exception("No such user '%s'" % email)
   
   if volume is None or volume.deleted:
      raise Exception("No such Volume '%s'" % email)
   
   # delete the volume access request 
   rc = VolumeAccessRequest.RemoveAccessRequest( user.owner_id, volume.volume_id )
   assert rc is True, "Failed to remove access request for %s in %s" % (email, volume.name)
   
   storagetypes.deferred.defer( _remove_user_from_volume_helper, user.owner_id, volume.volume_id )
      
   return True

# ----------------------------------
def delete_gateway( g_id, **kw ):
   # TODO: garbage-collect gateways... 
   ret = Gateway.Delete( g_id )
   return {'result': ret}


# ----------------------------------
def get_volume_root( volume ):
   return MSEntry.Read( volume, 0 )

# ----------------------------------
def get_num_children( volume_id, file_id, async=False ):
   return MSEntryIndex.GetNumChildren( volume_id, file_id, async=async )

# ----------------------------------
def get_generation( volume_id, file_id, async=False ):
   return MSEntryIndex.GetGeneration( volume_id, file_id, async=async )
