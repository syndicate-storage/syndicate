#!/usr/bin/python 

"""
   Copyright 2014 The Trustees of Princeton University

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

import os
import sys 
import json
import time
import traceback
import base64
from collections import namedtuple

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

import syndicate.observer.core as observer_core
import syndicate.observer.cred as observer_cred
import syndicate.observer.push as observer_push

CONFIG = observer_core.get_config()

# objects expected by these methods 
SyndicatePrincipal = namedtuple("SyndicatePrincipal", ["principal_id", "public_key_pem", "sealed_private_key"])
Volume = namedtuple("Volume", ["name", "owner_id", "description", "blocksize", "private", "archive", "cap_read_data", "cap_write_data", "cap_host_data", "slice_id"] )
VolumeAccessRight = namedtuple( "VolumeAccessRight", ["owner_id", "volume", "cap_read_data", "cap_write_data", "cap_host_data"] )
SliceSecret = namedtuple( "SliceSecret", ["slice_id", "secret"] )
VolumeSlice = namedtuple( "VolumeSlice", ["volume_id", "slice_id", "cap_read_data", "cap_write_data", "cap_host_data", "UG_portnum", "RG_portnum", "credentials_blob"] )

#-------------------------------
def sync_volume_record( volume ):
   """
   Synchronize a Volume record with Syndicate.
   """
   
   logger.info( "Sync Volume = %s\n\n" % volume.name )
   
   principal_id = volume.owner_id.email
   config = observer_core.get_config()
   
   max_UGs = None 
   max_RGs = None
   volume_principal_id = observer_core.make_volume_principal_id( principal_id, volume.name )

   # get the observer secret 
   try:
      max_UGs = CONFIG.SYNDICATE_UG_QUOTA 
      max_RGs = CONFIG.SYNDICATE_RG_QUOTA
      observer_secret = observer_core.get_syndicate_observer_secret( config.SYNDICATE_OBSERVER_SECRET )
   except Exception, e:
      traceback.print_exc()
      logger.error("config is missing SYNDICATE_OBSERVER_SECRET, SYNDICATE_UG_QUOTA, SYNDICATE_RG_QUOTA")
      raise e
   
   # volume owner must exist as a Syndicate user...
   try:
      rc, user = observer_core.ensure_principal_exists( volume_principal_id, observer_secret, is_admin=False, max_UGs=max_UGs, max_RGs=max_RGs)
      assert rc == True, "Failed to create or read volume principal '%s'" % volume_principal_id
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to ensure principal '%s' exists" % volume_principal_id )
      raise e

   # volume must exist 
   # create or update the Volume
   try:
      new_volume = observer_core.ensure_volume_exists( volume_principal_id, volume, user=user )
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to ensure volume '%s' exists" % volume.name )
      raise e
      
   # did we create the Volume?
   if new_volume is not None:
      # we're good
      pass 
         
   # otherwise, just update it 
   else:
      try:
         rc = observer_core.update_volume( volume )
      except Exception, e:
         traceback.print_exc()
         logger.error("Failed to update volume '%s', exception = %s" % (volume.name, e.message))
         raise e
            
   return True


#-------------------------------
def delete_volume_record( volume ):
   """
   Delete a volume from Syndicate.
   """
   
   logger.info( "Delete Volume =%s\n\n" % volume.name )
   
   volume_name = volume.name 
   config = observer_core.get_config()
   
   # delete the Volume on Syndicate.
   try:
      rc = observer_core.ensure_volume_absent( volume_name )
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to delete volume %s", volume_name )
      raise e
   
   return rc


#-------------------------------
def sync_volumeaccessright_record( vac ):
   """
   Synchronize a volume access record 
   """
   
   syndicate_caps = "UNKNOWN"  # for exception handling
   
   # get arguments
   config = observer_core.get_config()
   principal_id = vac.owner_id.email
   volume_name = vac.volume.name
   syndicate_caps = observer_core.opencloud_caps_to_syndicate_caps( vac.cap_read_data, vac.cap_write_data, vac.cap_host_data ) 
   
   logger.info( "Sync VolumeAccessRight for (%s, %s)" % (principal_id, volume_name) )
   
   # validate config
   try:
      observer_secret = observer_core.get_syndicate_observer_secret( config.SYNDICATE_OBSERVER_SECRET )
   except Exception, e:
      traceback.print_exc()
      logger.error("syndicatelib config is missing SYNDICATE_RG_DEFAULT_PORT, SYNDICATE_OBSERVER_SECRET")
      raise e
      
   # ensure the user exists and has credentials
   try:
      rc, user = observer_core.ensure_principal_exists( principal_id, observer_secret, is_admin=False, max_UGs=1100, max_RGs=1 )
      assert rc is True, "Failed to ensure principal %s exists (rc = %s,%s)" % (principal_id, rc, user)
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to ensure user '%s' exists" % principal_id )
      raise e

   # grant the slice-owning user the ability to provision UGs in this Volume
   try:
      rc = observer_core.ensure_volume_access_right_exists( principal_id, volume_name, syndicate_caps )
      assert rc is True, "Failed to set up Volume access right for slice %s in %s" % (principal_id, volume_name)
      
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to set up Volume access right for slice %s in %s" % (principal_id, volume_name))
      raise e
   
   except Exception, e:
      traceback.print_exc()
      logger.error("Faoed to ensure user %s can access Volume %s with rights %s" % (principal_id, volume_name, syndicate_caps))
      raise e

   return True


#-------------------------------
def delete_volumeaccessright_record( vac ):
   """
   Ensure that a principal no longer has access to a particular volume.
   """
   
   principal_id = vac.owner_id.email 
   volume_name = vac.volume.name 
   
   try:
      observer_core.ensure_volume_access_right_absent( principal_id, volume_name )
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to revoke access from %s to %s" % (principal_id, volume_name))
      raise e
   
   return True


#-------------------------------
def sync_volumeslice_record( vs ):
   """
   Synchronize a VolumeSlice record 
   """
   
   logger.info("Sync VolumeSlice for (%s, %s)" % (vs.volume_id.name, vs.slice_id.name))
   
   # extract arguments...
   principal_id = vs.slice_id.creator.email
   slice_name = vs.slice_id.name
   volume_name = vs.volume_id.name
   syndicate_caps = observer_core.opencloud_caps_to_syndicate_caps( vs.cap_read_data, vs.cap_write_data, vs.cap_host_data )
   RG_port = vs.RG_portnum
   UG_port = vs.UG_portnum
   slice_secret = None
   gateway_name_prefix = None
   
   config = observer_core.get_config()
   try:
      observer_secret = observer_core.get_syndicate_observer_secret( config.SYNDICATE_OBSERVER_SECRET )
      RG_closure = config.SYNDICATE_RG_CLOSURE
      observer_pkey_path = config.SYNDICATE_OBSERVER_PRIVATE_KEY
      syndicate_url = config.SYNDICATE_SMI_URL
      gateway_name_prefix = config.SYNDICATE_GATEWAY_NAME_PREFIX
   except Exception, e:
      traceback.print_exc()
      logger.error("syndicatelib config is missing one or more of the following: SYNDICATE_OBSERVER_SECRET, SYNDICATE_RG_CLOSURE, SYNDICATE_OBSERVER_PRIVATE_KEY, SYNDICATE_SMI_URL")
      raise e
      
   # get secrets...
   try:
      observer_pkey_pem = observer_core.get_observer_private_key_pem( observer_pkey_path )
      assert observer_pkey_pem is not None, "Failed to load Observer private key"
      
      # get/create the slice secret
      slice_secret = observer_core.get_or_create_slice_secret( observer_pkey_pem, slice_name )    
      assert slice_secret is not None, "Failed to get or create slice secret for %s" % slice_name
      
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to load secret credentials")
      raise e
   
   # make sure there's a slice-controlled Syndicate user account for the slice owner
   slice_principal_id = observer_core.make_slice_principal_id( principal_id, slice_name )
   
   try:
      rc, user = observer_core.ensure_principal_exists( slice_principal_id, observer_secret, is_admin=False, max_UGs=1100, max_RGs=1 )
      assert rc is True, "Failed to ensure principal %s exists (rc = %s,%s)" % (slice_principal_id, rc, user)
   except Exception, e:
      traceback.print_exc()
      logger.error('Failed to ensure slice user %s exists' % slice_principal_id)
      raise e
      
   # grant the slice-owning user the ability to provision UGs in this Volume
   try:
      rc = observer_core.ensure_volume_access_right_exists( slice_principal_id, volume_name, syndicate_caps )
      assert rc is True, "Failed to set up Volume access right for slice %s in %s" % (slice_principal_id, volume_name)
      
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to set up Volume access right for slice %s in %s" % (slice_principal_id, volume_name))
      raise e
      
   # provision for the user the (single) RG the slice will instantiate in each VM.
   try:
      rc = observer_core.setup_global_RG( slice_principal_id, volume_name, gateway_name_prefix, slice_secret, RG_port, RG_closure )
   except Exception, e:
      logger.exception(e)
      return False

   # generate and save slice credentials....
   try:
      slice_cred = observer_core.save_slice_credentials( observer_pkey_pem, syndicate_url, slice_principal_id, volume_name, slice_name, observer_secret, slice_secret,
                                                         instantiate_UG=True,  run_UG=True,  UG_port=UG_port, UG_closure=None,
                                                         instantiate_RG=None,  run_RG=True,  RG_port=RG_port, RG_closure=None, RG_global_hostname="localhost",
                                                         instantiate_AG=None,  run_AG=None,  AG_port=0,       AG_closure=None,
                                                         gateway_name_prefix=gateway_name_prefix,
                                                         existing_user=user )
      
      assert slice_cred is not None, "Failed to generate slice credential for %s in %s" % (slice_principal_id, volume_name )
            
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to generate slice credential for %s in %s" % (slice_principal_id, volume_name))
      raise e
         
   # ... and push them all out.
   try:
      rc = observer_push.push_credentials_to_slice( slice_name, slice_cred )
      assert rc is True, "Failed to push credentials to slice %s for volume %s" % (slice_name, volume_name)
         
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to push slice credentials to %s for volume %s" % (slice_name, volume_name))
      raise e
   
   return True


#-------------------------------
def delete_volumeslice_record( vs ):
   """
   Unmount a volume from a slice.
   That is, prevent the slice from mounting it, by revoking the slice's principal's permissions and deleting its gateways.
   """
   
   principal_id = vs.slice_id.creator.email
   slice_name = vs.slice_id.name
   volume_name = vs.volume_id.name 
   
   slice_principal_id = observer_core.make_slice_principal_id( principal_id, slice_name )
   
   try:
      observer_core.revoke_volume_access( slice_principal_id, volume_name )
   except Exception, e:
      traceback.print_exc()
      logger.error("Failed to remove slice principal %s from %s" % (slice_principal_id, volume_name))
      raise e
   
   return True 

