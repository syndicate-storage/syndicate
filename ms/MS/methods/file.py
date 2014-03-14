#!/usr/bin/env python

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

import MS
from MS.volume import Volume
from MS.entry import *
from storage import storage
import storage.storagetypes as storagetypes

from common import *

import random
import os
import errno

import logging

# ----------------------------------
def make_ms_reply( volume, error ):
   """
   Generate a pre-populated ms_reply protobuf.
   Include the volume's certificate versioning information.
   """
   
   reply = ms_pb2.ms_reply()

   reply.volume_version = volume.version
   reply.cert_version = volume.cert_version
   reply.error = error

   return reply

# ----------------------------------
def file_update_init_response( volume ):
   """
   Create a protobuf ms_reply structure, and populate it with preliminary details.
   """
   reply = make_ms_reply( volume, 0 )
   reply.listing.status = 0
   reply.listing.ftype = 0
   reply.error = 0
   reply.signature = ""

   return reply
   
   
# ----------------------------------
def file_update_complete_response( volume, reply ):
   """
   Sign a protobuf ms_reply structure, using the Volume's private key.
   """
   reply.signature = ""
   reply_str = reply.SerializeToString()
   sig = volume.sign_message( reply_str )
   reply.signature = sig
   reply_str = reply.SerializeToString()
   
   return reply_str


# ----------------------------------
def _resolve( owner_id, volume, file_id, file_version, write_nonce ):
   """
   Read file and listing of the given file_id.
   """

   file_memcache = MSEntry.Read( volume, file_id, memcache_keys_only=True )
   file_data = storagetypes.memcache.get( file_memcache )
   listing = MSEntry.ListDir( volume, file_id )

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
         
         if file_data != None:
            cacheable[ file_memcache ] = file_data
      
      if len(cacheable) > 0:
         logging.info( "cache file %s (%s)" % (file_id, file_data) )
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

   reply = make_ms_reply( volume, error )
   
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
   return (error, file_update_complete_response( volume, reply ))
   

# ----------------------------------
def file_resolve( gateway, volume, file_id, file_version_str, write_nonce_str ):
   """
   Resolve a (volume_id, file_id, file_vesion, write_nonce) to file metadata.
   This is part of the File Metadata API, so it takes strings for file_version_str and write_nonce_str.
   If these do not parse to Integers, then this method fails (returns None).
   """
   file_version = -1
   write_nonce = -1
   try:
      file_version = int(file_version_str)
      write_nonce = int(write_nonce_str)
   except:
      return None 
   
   logging.info("resolve /%s/%s/%s/%s" % (volume.volume_id, file_id, file_version, write_nonce) )
   
   owner_id = volume.owner_id
   if gateway != None:
      owner_id = gateway.owner_id
      
   rc, reply = _resolve( owner_id, volume, file_id, file_version, write_nonce )
   
   logging.info("resolve /%s/%s/%s/%s rc = %d" % (volume.volume_id, file_id, file_version, write_nonce, rc) )
   
   return reply


# ----------------------------------
def file_create( reply, gateway, volume, update ):
   """
   Create a file or directory, using the given ms_update structure.
   Add the new entry to the given ms_reply protobuf, containing data from the created MSEntry.
   This is part of the File Metadata API.
   """
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("create /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
   
   rc, ent = MSEntry.Create( gateway.owner_id, volume, **attrs )
   
   logging.info("create /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
   
   # have an entry?
   if ent is not None:
      ent_pb = reply.listing.entries.add()
      ent.protobuf( ent_pb )
   
   return rc


# ----------------------------------
def file_update( reply, gateway, volume, update ):
   """
   Update the metadata records of a file or directory, using the given ms_update structure.
   Add the new entry to the given ms_reply protobuf, containing data from the updated MSEntry.
   This is part of the File Metadata API.
   """
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("update /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
   
   rc, ent = MSEntry.Update( gateway.owner_id, volume, **attrs )
   
   logging.info("update /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
   
   # have an entry 
   if ent is not None:
      ent_pb = reply.listing.entries.add()
      ent.protobuf( ent_pb )
   
   return rc


# ----------------------------------
def file_delete( reply, gateway, volume, update ):
   """
   Delete a file or directory, using the fields of the given ms_update structure.
   This is part of the File Metadata API.
   """
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("delete /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
   
   rc = MSEntry.Delete( gateway.owner_id, volume, **attrs )
   
   logging.info("delete /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
   
   return rc


# ----------------------------------
def file_rename( reply, gateway, volume, update ):
   """
   Rename a file or directory, using the fields of the given ms_update structure.
   This is part of the File Metadata API.
   """
   src_attrs = MSEntry.unprotobuf_dict( update.entry )
   dest_attrs = MSEntry.unprotobuf_dict( update.dest )
   
   logging.info("rename /%s/%s (name=%s, parent=%s) to (name=%s, parent=%s)" % 
                  (src_attrs['volume_id'], src_attrs['file_id'], src_attrs['name'], src_attrs['parent_id'], dest_attrs['name'], dest_attrs['parent_id']) )
   
   rc = MSEntry.Rename( gateway.owner_id, volume, src_attrs, dest_attrs )
   
   logging.info("rename /%s/%s (name=%s, parent=%s) to (name=%s, parent=%s) rc = %s" % 
                  (src_attrs['volume_id'], src_attrs['file_id'], src_attrs['name'], src_attrs['parent_id'], dest_attrs['name'], dest_attrs['parent_id'], rc) )
   
   return rc


# ----------------------------------
def file_chcoord( reply, gateway, volume, update ):
   """
   Change the coordinator of a file, using the fields of the given ms_update structure.
   Add a new entry to the given ms_reply protobuf, containing data from the updated MSEntry.
   This is part of the File Metadata API.
   """
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("chcoord /%s/%s (%s) to %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], gateway.g_id) )
   
   rc, ent = MSEntry.Chcoord( gateway.owner_id, gateway, volume, **attrs )
   
   logging.info("chcoord /%s/%s (%s) rc = %d" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
   
   # have an entry 
   if ent is not None:
      ent_pb = reply.listing.entries.add()
      ent.protobuf( ent_pb )
   
   return rc


# ----------------------------------
def file_update_parse( request_handler ):
   """
   Parse the raw data from the 'ms-metadata-updates' POST field into an ms_updates protobuf.
   Return None on failure.
   """
   
   # will have gotten metadata updates
   updates_field = request_handler.request.POST.get( 'ms-metadata-updates' )

   if updates_field == None:
      # no valid data given (malformed)
      return None

   # extract the data
   data = updates_field.file.read()
   
   # parse it 
   updates_set = ms_pb2.ms_updates()

   try:
      updates_set.ParseFromString( data )
   except:
      return None
   
   return updates_set


# ----------------------------------
def file_update_auth( gateway, volume ):
   """
   Verify whether or not the given gateway (which can be None) is allowed to update (POST) metadata
   in the given volume.
   """
   
   # gateway must be known
   if gateway == None:
      logging.error("Unknown gateway")
      return False
   
   # NOTE: gateways run on behalf of a user, so gateway.owner_id is equivalent to the user's ID.
   
   # this can only be a User Gateway or an Acquisition Gateway
   if gateway.gateway_type != GATEWAY_TYPE_UG and gateway.gateway_type != GATEWAY_TYPE_AG:
      logging.error("Not a UG or RG")
      return False
   
   # if this is an archive, on an AG owned by the same person as the Volume can write to it
   if volume.archive:
      if gateway.gateway_type != GATEWAY_TYPE_AG or gateway.owner_id != volume.owner_id:
         logging.error("Not an AG, or not the Volume owner")
         return False
   
   # if this is not an archive, then the gateway must have CAP_WRITE_METADATA
   elif not gateway.check_caps( GATEWAY_CAP_WRITE_METADATA ):
      logging.error("Write metadata is forbidden to this Gateway")
      return False
   
   # allowed!
   return True


# ----------------------------------
def file_read_auth( gateway, volume ):
   """
   Verify whether or not the given gateway (which can be None) is allowed 
   to read (GET) file metadata from the given volume.
   """
   
   # gateway authentication required?
   if volume.need_gateway_auth() and gateway == None:
      print "no gateway"
      return False

   # this must be a User Gateway, if there is a specific gateway
   if gateway != None and gateway.gateway_type != GATEWAY_TYPE_UG:
      print "not UG"
      return False
   
   # this gateway must be allowed to read metadata
   if gateway != None and not gateway.check_caps( GATEWAY_CAP_READ_METADATA ):
      print "bad caps: %x" % gateway.caps
      return False
   
   return True


# ----------------------------------
def file_xattr_get_and_check_msentry_readable( gateway, volume, file_id):
   """
   Verify whether or not the given gateway (which can be None) is allowed 
   to read or list an MSEntry's extended attributes.
   """
   rc = 0
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent == None:
      # does not exist
      rc = -errno.ENOENT
      
   elif msent.owner_id != gateway.owner_id and (msent.mode & 0044) == 0:
      # not readable.
      # don't tell the reader that this entry even exists.
      rc = -errno.ENOENT
      
   if rc != 0:
      msent = None
      
   return (rc, msent)


# ----------------------------------
def file_xattr_get_and_check_msentry_writeable( gateway, volume, file_id):
   """
   Verify whether or not the given gateway (which can be None) is allowed 
   to update or delete an MSEntry's extended attributes.
   """
   
   rc = 0
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent == None:
      # does not exist
      rc = -errno.ENOENT
      
   elif msent.owner_id != gateway.owner_id and (msent.mode & 0022) == 0:
      # not writeable 
      # if not readable, then say ENOENT instead (don't reveal the existence of a metadata entry to someone who can't read it)
      if msent.owner_id != gateway.owner_id and (msent.mode & 0044) == 0:
         rc = -errno.ENOENT
      else:
         rc = -errno.EACCES

   if rc != 0:
      msent = None
      
   return (rc, msent)


# ----------------------------------
def file_xattr_getxattr_response( volume, rc, xattr_value ):
   """
   Generate a serialized, signed ms_reply protobuf from
   a getxattr return code (rc) and xattr value.
   """
   
   # create and sign the response 
   reply = file_update_init_response( volume )
   reply.error = rc
   
   if rc == 0:
      reply.xattr_value = xattr_value
   
   return file_update_complete_response( volume, reply )


# ----------------------------------
def file_xattr_listxattr_response( volume, rc, xattr_names ):
   """
   Generate a serialized, signed ms_reply protobuf from
   a listxattr return code (rc) and xattr names list.
   """

   # create and sign the response 
   reply = file_update_init_response( volume )
   reply.error = rc
   
   if rc == 0:
      
      for name in xattr_names:
         reply.xattr_names.add( name )
      
   return file_update_complete_response( volume, reply )


# ----------------------------------
def file_xattr_getxattr( gateway, volume, file_id, xattr_name ):
   """
   Get the value of the file's extended attributes, subject to access controls.
   This is part of the File Metadata API.
   """
   
   logging.info("getxattr /%s/%s/%s" % (volume.volume_id, file_id, xattr_name) )

   rc, msent = file_xattr_get_and_check_msentry_readable( gateway, volume, file_id )
   xattr_value = None
   
   if rc == 0 and msent != None:
      # get the xattr
      rc, xattr_value = MSEntryXAttr.GetXAttr( msent, xattr_name )
   
   logging.info("getxattr /%s/%s/%s rc = %d" % (volume.volume_id, file_id, xattr_name, rc) )

   return file_xattr_getxattr_response( volume, rc, xattr_value )


# ----------------------------------
def file_xattr_listxattr( gateway, volume, file_id, unused=None ):
   """
   Get the names of a file's extended attributes, subject to access controls.
   This is part of the File Metadata API.
   
   NOTE: unused=None is required for the File Metadata API dispatcher to work.
   """
   
   logging.info("listxattr /%s/%s" % (volume.volume_id, file_id) )

   rc, msent = file_xattr_get_and_check_msentry_readable( gateway, volume, file_id )
   xattr_names = []
   
   if rc == 0 and msent != None:
      # get the xattr
      rc, xattr_names = MSEntryXAttr.ListXAttr( msent )
   
   logging.info("listxattr /%s/%s rc = %d" % (volume.volume_id, file_id, rc) )

   return file_xattr_listxattr_response( volume, rc, xattr_names )


# ----------------------------------
def file_xattr_setxattr( reply, gateway, volume, update ):
   """
   Set the value of a file's extended attributes, subject to access controls.
   The affected file and attribute are determined from the given ms_update structure.
   Use the XATTR_CREATE and XATTR_REPLACE semantics from setxattr(2) (these 
   are fields in the given ms_update structure).
   This is part of the File Metadata API.
   """
   
   xattr_create = False 
   xattr_replace = False 
   
   if update.HasField( 'xattr_create' ):
      xattr_create = update.xattr_create
   
   if update.HasField( 'xattr_replace' ):
      xattr_replace = update.xattr_replace 
      
   logging.info("setxattr /%s/%s (name=%s, parent=%s) %s = %s (create=%s, replace=%s)" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_create, xattr_replace))
      
   rc, msent = xattr_get_and_check_msentry_writeable( gateway, volume, attrs['file_id'] )
   
   if rc == 0:
      # allowed!
      rc = MSEntryXAttr.SetXAttr( msent, update.xattr_name, update.xattr_value, xattr_create=xattr_create, xattr_replace=xattr_replace )
      
   logging.info("setxattr /%s/%s (name=%s, parent=%s) %x = %x (create=%s, replace=%s) rc = %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_create, xattr_replace, rc) )
         
   return rc


# ----------------------------------
def file_xattr_removexattr( reply, gateway, volume, update ):
   """
   Remove a file's extended attribute, subject to access controls.
   The affected file and attribute are determined from the given ms_update structure.
   This is part of the File Metadata API.
   """

   logging.info("removexattr /%s/%s (name=%s, parent=%s) %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name))
      
   rc, msent = xattr_get_and_check_msentry_writeable( gateway, volume, attrs['file_id'] )
   
   if rc == 0:
      # allowed!
      rc = MSEntryXAttr.RemoveXAttr( msent, update.xattr_name )
   
   logging.info("removexattr /%s/%s (name=%s, parent=%s) %x rc = %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, rc) )
   
   
   return rc
