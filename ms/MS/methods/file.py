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
import common.msconfig as msconfig

import random
import os
import errno

import traceback
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
def file_update_get_attrs( entry_dict, attr_list ):
   """
   Get a set of required attriutes.
   Return None if at least one is missing.
   """
   ret = {}
   for attr_name in attr_list:
      if not entry_dict.has_key(attr_name):
         return None 
      
      ret[attr_name] = entry_dict[attr_name]
   
   return ret


# ----------------------------------
def file_read_allowed( owner_id, file_data ):
   """
   Can the user access the file?  Return the appropriate error code.
   """
   error = -errno.EACCES
   
   if file_data.ftype == MSENTRY_TYPE_DIR:
      # directory. check permissions
      if file_data.owner_id == owner_id or (file_data.mode & 0055) != 0:
         # readable
         error = 0

   elif file_data.ftype == MSENTRY_TYPE_FILE:
      # file.  check permissions
      if file_data.owner_id == owner_id or (file_data.mode & 0044) != 0:
         # readable
         error = 0

   return error


# ----------------------------------
def _getattr( owner_id, volume, file_id, file_version, write_nonce ):
   """
   Read one file/directory's metadata, by file ID
   """
   
   error = 0
   access_error = 0
   need_refresh = True
   
   file_data = MSEntry.Read( volume, file_id )
   
   if file_data is not None:
      # got data...
      # do we need to actually send this?
      if file_data.version == file_version and file_data.write_nonce == write_nonce:
         need_refresh = False
         
         logging.info("%s has type %s version %s write_nonce %s, status=NOCHANGE" % (file_data.name, file_data.ftype, file_data.version, file_data.write_nonce))

      else:
         
         logging.info("%s has type %s version %s write_nonce %s, status=NEW" % (file_data.name, file_data.ftype, file_data.version, file_data.write_nonce))

      error = file_read_allowed( owner_id, file_data )
      
   else:
      # not found
      error = -errno.ENOENT 
   
   reply = make_ms_reply( volume, error )
   
   if error == 0:
      
      # all is well.
      reply.listing.ftype = file_data.ftype
      
      # modified?
      if not need_refresh:
         reply.listing.status = ms_pb2.ms_listing.NOT_MODIFIED
         
      else:
         reply.listing.status = ms_pb2.ms_listing.NEW
         
         # child count if directory 
         num_children = file_data.num_children
         generation = file_data.generation
         
         if num_children is None:
            num_children = 0
            
         if generation is None:
            generation = 0
         
         if file_data.ftype == MSENTRY_TYPE_DIR:
            num_children_fut = MSEntryIndex.GetNumChildren( volume.volume_id, file_id, volume.num_shards, async=True )
            
            storagetypes.wait_futures( [num_children_fut] )
            
            num_children = num_children_fut.get_result()
            
         # full ent 
         ent_pb = reply.listing.entries.add()
         file_data.protobuf( ent_pb, num_children=num_children, generation=generation )
         
         # logging.info("Getattr %s: Serve back: %s" % (file_id, file_data))
         
   else:
      # not possible to reply
      reply.listing.ftype = 0
      reply.listing.status = ms_pb2.ms_listing.NONE
      
   # sign and deliver
   return (error, file_update_complete_response( volume, reply ))


# ----------------------------------
def _getchild( owner_id, volume, parent_id, name ):
   """
   Read one file/directory's metadata, by file name and parent ID
   """
   
   error = 0
   dir_data_fut = None
   
   dir_data = MSEntry.Read( volume, parent_id )
   file_data = None
      
   if dir_data is not None:
      
      # got directory.  is it readable?
      error = file_read_allowed( owner_id, dir_data )
      
      if error == 0:
         # can read.  Get file data 
         file_data = MSEntry.ReadByParent( volume, parent_id, name )
         
         if file_data is None:
            error = -errno.ENOENT
         
         else:
            error = file_read_allowed( owner_id, file_data )
         
   else:
      error = -errno.ENOENT 
   
   reply = make_ms_reply( volume, error )
   
   if error == 0:
      
      # all is well.
      reply.listing.ftype = file_data.ftype
      reply.listing.status = ms_pb2.ms_listing.NEW

      # child count if directory 
      num_children = 0
      if file_data.ftype == MSENTRY_TYPE_DIR:
         num_children = MSEntryIndex.GetNumChildren( volume.volume_id, file_data.file_id, volume.num_shards )
         
      # full ent 
      ent_pb = reply.listing.entries.add()
      file_data.protobuf( ent_pb, num_children=num_children )
      
      # logging.info("Getchild %s: Serve back: %s" % (parent_id, file_data))
   
   else:
      # not possible to reply
      reply.listing.ftype = 0
      reply.listing.status = ms_pb2.ms_listing.NONE
      
   # sign and deliver
   return (error, file_update_complete_response( volume, reply ))


# ----------------------------------
def _listdir( owner_id, volume, file_id, page_id=None, least_unknown_generation=None ):
   
   error, listing = MSEntry.ListDir( volume, file_id, page_id=page_id, least_unknown_generation=least_unknown_generation )
   
   if error == 0:
      # only give back visible entries
      listing = filter( lambda ent: file_read_allowed( owner_id, ent ) == 0, listing )
   
   reply = make_ms_reply( volume, error )
   
   if error == 0:
      
      reply.listing.ftype = MSENTRY_TYPE_DIR
      reply.listing.status = ms_pb2.ms_listing.NEW

      for ent in listing:         
         ent_pb = reply.listing.entries.add()
         ent.protobuf( ent_pb )
   
         # logging.info("Resolve %s: Serve back: %s" % (file_id, all_ents))
   
   else:
      reply.listing.ftype = 0
      reply.listing.status = ms_pb2.ms_listing.NONE
   
   # sign and deliver
   return (error, file_update_complete_response( volume, reply ))
   

# ----------------------------------
def file_getattr( gateway, volume, file_id, file_version_str, write_nonce_str ):
   """
   Get all metadata for a file, given the (volume_id, file_id, file_vesion, write_nonce) key.
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
   
   logging.info("getattr /%s/%s/%s/%s" % (volume.volume_id, file_id, file_version, write_nonce) )
   
   owner_id = msconfig.GATEWAY_ID_ANON
   if gateway != None:
      owner_id = gateway.owner_id
      
   rc, reply = _getattr( owner_id, volume, file_id, file_version, write_nonce )
   
   logging.info("getattr /%s/%s/%s/%s rc = %d" % (volume.volume_id, file_id, file_version, write_nonce, rc) )
   
   return reply


# ----------------------------------
def file_getchild( gateway, volume, parent_id, name ):
   """
   Get all metadata for a file, given the its parent ID and name.
   """
   
   logging.info("getchild /%s/%s/%s" % (volume.volume_id, parent_id, name) )
   
   owner_id = msconfig.GATEWAY_ID_ANON
   if gateway != None:
      owner_id = gateway.owner_id
      
   rc, reply = _getchild( owner_id, volume, parent_id, name )
   
   logging.info("getchild /%s/%s/%s rc = %d" % (volume.volume_id, parent_id, name, rc) )
   
   return reply


# ----------------------------------
def file_listdir( gateway, volume, file_id, page_id=None, lug=None ):
   """
   Get up to RESOLVE_MAX_PAGE_SIZE of (type, file ID) pairs.
   when the caller last called listdir (or -1 if this is the first call to listdir).
   """
   
   logging.info("listdir /%s/%s, page_id=%s, l.u.g.=%s" % (volume.volume_id, file_id, page_id, lug) )
   
   owner_id = msconfig.GATEWAY_ID_ANON
   if gateway != None:
      owner_id = gateway.owner_id
      
   rc, reply = _listdir( owner_id, volume, file_id, page_id=page_id, least_unknown_generation=lug )
   
   logging.info("listdir /%s/%s, page_id=%s, l.u.g.=%s rc = %d" % (volume.volume_id, file_id, page_id, lug, rc) )
   
   return reply


# ----------------------------------
def file_create( reply, gateway, volume, update, async=False ):
   """
   Create a file or directory, using the given ms_update structure.
   Add the new entry to the given ms_reply protobuf, containing data from the created MSEntry.
   This is part of the File Metadata API.
   
   If async is True, then this method is called from a deferred work queue.
   "update" and "reply" will be serialized, so we'll need to unserialize them
   """
   
   try:
      
      if async:
         # reply and update will have been serialized 
         # parse it 
         updatepb = ms_pb2.ms_update()
         updatepb.ParseFromString( update )
         update = updatepb
         
         replypb = ms_pb2.ms_reply()
         replypb.ParseFromString( reply )
         reply = replypb
         
      attrs = MSEntry.unprotobuf_dict( update.entry )
      
      logging.info("create /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
      
      rc, ent = MSEntry.Create( gateway.owner_id, volume, **attrs )
      
      logging.info("create /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
      
      if not async:
         # have an entry?
         if ent is not None:
            ent_pb = reply.listing.entries.add()
            ent.protobuf( ent_pb )
            
         return rc
      
      else:
         # just log the error
         if rc != 0:
            logging.error( "create /%s/%s (%s) failed, rc = %d" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
            return rc
   
   except Exception, e:
      
      logging.error("file_create(async=%s) raised an exception" % async)
      
      if async:
         logging.exception(e)
         
         # stop trying 
         raise storagetypes.deferred.PermanentTaskFailure()
      
      else:
         traceback.print_exc()
         raise e

# ----------------------------------
def file_create_async( reply, gateway, volume, update ):
   """
   Create a file/directory asynchronously.  Put it into the deferred task queue.
   This is so AGs can create a huge batch of data efficiently.
   """
   
   storagetypes.deferred.defer( file_create, reply.SerializeToString(), gateway, volume, update.SerializeToString(), async=True )
   
   return 0


# ----------------------------------
def file_update( reply, gateway, volume, update, async=False ):
   """
   Update the metadata records of a file or directory, using the given ms_update structure.
   Add the new entry to the given ms_reply protobuf, containing data from the updated MSEntry.
   This is part of the File Metadata API.
   
   If async is True, this method was called by a deferred work queue, and update will have been serialized.
   """
   
   try:
      
      if async:
         # update will have been serialized 
         # parse it 
         updatepb = ms_pb2.ms_update()
         updatepb.ParseFromString( update )
         update = updatepb
         
         replypb = ms_pb2.ms_reply()
         replypb.ParseFromString( reply )
         reply = replypb
         
      attrs = MSEntry.unprotobuf_dict( update.entry )
      
      affected_blocks = update.affected_blocks[:]
      log_affected_blocks = True
      
      # don't log affected blocks if the writer was an AG, since they don't replicate anything
      if gateway.gateway_type == GATEWAY_TYPE_AG:
         log_affected_blocks = False
      
      logging.info("update /%s/%s (%s), affected blocks = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], affected_blocks ) )
      
      rc, ent = MSEntry.Update( gateway.owner_id, volume, gateway, log_affected_blocks, affected_blocks, **attrs )
      
      logging.info("update /%s/%s (%s), affected blocks = %s rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], affected_blocks, rc ) )
      
      if not async:
         
         # have an entry 
         if ent is not None:
            ent_pb = reply.listing.entries.add()
            ent.protobuf( ent_pb )
         
         return rc
      
      else:
         if rc != 0:
            # just log the error 
            logging.error( "update /%s/%s (%s) failed, rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
            
         return rc
   
   except Exception, e:
      
      logging.error("file_update(async=%s) raised an exception" % async)
      
      if async:
         logging.exception(e)
         
         # stop trying 
         raise storagetypes.deferred.PermanentTaskFailure()
      
      else:
         raise e


# ----------------------------------
def file_update_async( reply, gateway, volume, update ):
   """
   Perform a file update, but put it on a task queue so it runs in the background.
   This is so AGs can publish a huge batch of data efficiently.
   """
   storagetypes.deferred.defer( file_update, reply.SerializeToString(), gateway, volume, update.SerializeToString(), async=True )
   
   return 0
   

# ----------------------------------
def file_delete( reply, gateway, volume, update, async=False ):
   """
   Delete a file or directory, using the fields of the given ms_update structure.
   This is part of the File Metadata API.
   """
   
   try:
      
      if async:
         # update will have been serialized 
         # parse it 
         updatepb = ms_pb2.ms_update()
         updatepb.ParseFromString( update )
         update = updatepb
         
         replypb = ms_pb2.ms_reply()
         replypb.ParseFromString( reply )
         reply = replypb
         
      attrs = MSEntry.unprotobuf_dict( update.entry )
   
      logging.info("delete /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
   
      rc = MSEntry.Delete( gateway.owner_id, volume, gateway, **attrs )
   
      logging.info("delete /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
      
      if not async:
         return rc
      
      else:
         if rc != 0:
            # just log it
            logging.error( "delete /%s/%s (%s) failed, rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
         
         return rc
   
   except Exception, e:
      
      logging.error("file_delete(async=%s) raised an exception" % async)
      logging.exception(e)
      
      if async:
         
         # stop trying
         raise storagetypes.deferred.PermanentTaskFailure()
      else:
         raise e
   

# ----------------------------------
def file_delete_async( reply, gateway, volume, update ):
   """
   Delete a file or directory, but put it on a task queue so it runs in the background.
   This is so AGs can delete a huge batch of data efficiently.
   """
   storagetypes.deferred.defer( file_delete, reply.SerializeToString(), gateway, volume, update.SerializeToString(), async=True )
   return 0

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
   
   rc = MSEntry.Rename( gateway.owner_id, gateway, volume, src_attrs, dest_attrs )
   
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
      logging.error("No 'ms-metadata-updates' field given")
      return None

   # extract the data
   data = updates_field.file.read()
   
   # parse it 
   updates_set = ms_pb2.ms_updates()

   try:
      updates_set.ParseFromString( data )
   except Exception, e:
      logging.exception(e)
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
   if volume.need_gateway_auth() and gateway is None:
      logging.error( "no gateway" )
      return False

   # this must be a User Gateway or Acquisition Gateway, if there is a specific gateway
   if gateway is not None and not (gateway.gateway_type == GATEWAY_TYPE_UG or gateway.gateway_type == GATEWAY_TYPE_AG):
      logging.error( "not UG or AG" )
      return False
   
   # this gateway must be allowed to read metadata
   if gateway is not None and not gateway.check_caps( GATEWAY_CAP_READ_METADATA ):
      logging.error( "bad caps: %s" % gateway.caps )
      return False
   
   return True


# ----------------------------------
def file_xattr_get_and_check_xattr_readable( gateway, volume, file_id, xattr_name, caller_is_admin=False ):
   """
   Verify that an extended attribute is readable to the given gateway.
   Return (rc, xattr)
   """
   
   rc = 0
   
   rc, xattr = MSEntryXAttr.ReadXAttr( volume.volume_id, file_id, xattr_name )
   
   if rc != 0:
      return (rc, None)
   
   if xattr is None:
      return (-errno.ENOENT, None)
   
   # get gateway owner ID
   gateway_owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      gateway_owner_id = gateway.owner_id

   # check permissions 
   if not MSEntryXAttr.XAttrReadable( gateway_owner_id, xattr, caller_is_admin ):
      logging.error("XAttr %s not readable by %s" % (xattr_name, gateway_owner_id))
      return (-errno.EACCES, None)
   
   return (0, xattr)


# ----------------------------------
def file_xattr_get_and_check_xattr_writable( gateway, volume, file_id, xattr_name, caller_is_admin=False ):
   """
   Verify that an extended attribute is writable to the given gateway.
   Return (rc, xattr)
   """
   
   rc = 0
   
   rc, xattr = MSEntryXAttr.ReadXAttr( volume.volume_id, file_id, xattr_name )
   
   if xattr is None and rc == -errno.ENODATA:
      # doesn't exist 
      return (0, None)
   
   if rc != 0:
      return (rc, None)
   
   if xattr is None:
      return (-errno.ENOENT, None)
   
   # get gateway owner ID
   gateway_owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      gateway_owner_id = gateway.owner_id
   
   # check permissions 
   if not MSEntryXAttr.XAttrWritable( gateway_owner_id, xattr, caller_is_admin ):
      logging.error("XAttr %s not writable by %s" % (xattr_name, gateway_owner_id))
      return (-errno.EACCES, None)
   
   return (0, xattr)


# ----------------------------------
def file_xattr_get_and_check_msentry_readable( gateway, volume, file_id, caller_is_admin=False ):
   """
   Verify whether or not the given gateway (which can be None) is allowed 
   to read or list an MSEntry's extended attributes.
   """
   
   rc = 0
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent is None:
      # does not exist
      rc = -errno.ENOENT
      
   else:
      # which gateway ID are we using?
      gateway_owner_id = GATEWAY_ID_ANON
      if gateway is not None:
         gateway_owner_id = gateway.owner_id
         
      if not caller_is_admin and msent.owner_id != gateway_owner_id and (msent.mode & 0044) == 0:
         # not readable.
         # don't tell the reader that this entry even exists.
         rc = -errno.ENOENT
         
   if rc != 0:
      msent = None
      
   return (rc, msent)


# ----------------------------------
def file_xattr_get_and_check_msentry_writeable( gateway, volume, file_id, caller_is_admin=False ):
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
      
   elif not caller_is_admin and msent.owner_id != gateway.owner_id and (msent.mode & 0022) == 0:
      logging.error("MSEntry %s not writable by %s" % (file_id, gateway.owner_id))
      
      # not writeable 
      # if not readable, then say ENOENT instead (don't reveal the existence of a metadata entry to someone who can't read it)
      if msent.owner_id != gateway.owner_id and (msent.mode & 0044) == 0:
         rc = -errno.ENOENT
      else:
         rc = -errno.EACCES

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
         reply.xattr_names.append( name )
      
   return file_update_complete_response( volume, reply )


# ----------------------------------
def file_xattr_getxattr( gateway, volume, file_id, xattr_name, caller_is_admin=False ):
   """
   Get the value of the file's extended attributes, subject to access controls.
   This is part of the File Metadata API.
   """
   
   logging.info("getxattr /%s/%s/%s" % (volume.volume_id, file_id, xattr_name) )

   rc, msent = file_xattr_get_and_check_msentry_readable( gateway, volume, file_id, caller_is_admin )
   xattr_value = None
   
   if rc == 0 and msent != None:
      # check xattr readable 
      rc, xattr = file_xattr_get_and_check_xattr_readable( gateway, volume, file_id, xattr_name, caller_is_admin )
      
      if rc == 0:
         # success!
         xattr_value = xattr.xattr_value 
         
         
   logging.info("getxattr /%s/%s/%s rc = %d" % (volume.volume_id, file_id, xattr_name, rc) )

   return file_xattr_getxattr_response( volume, rc, xattr_value )


# ----------------------------------
def file_xattr_listxattr( gateway, volume, file_id, unused=None, caller_is_admin=False ):
   """
   Get the names of a file's extended attributes, subject to access controls.
   This is part of the File Metadata API.
   
   NOTE: unused=None is required for the File Metadata API dispatcher to work.
   """
   
   logging.info("listxattr /%s/%s" % (volume.volume_id, file_id) )

   rc, msent = file_xattr_get_and_check_msentry_readable( gateway, volume, file_id, caller_is_admin )
   xattr_names = []
   
   if rc == 0 and msent != None:
      
      # get gateway owner ID
      gateway_owner_id = GATEWAY_ID_ANON
      if gateway is not None:
         gateway_owner_id = gateway.owner_id
      
      # get the xattr names
      rc, xattr_names = MSEntryXAttr.ListXAttrs( volume, msent, gateway_owner_id, caller_is_admin )
   
   logging.info("listxattr /%s/%s rc = %d" % (volume.volume_id, file_id, rc) )

   return file_xattr_listxattr_response( volume, rc, xattr_names )


# ----------------------------------
def file_xattr_setxattr( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Set the value of a file's extended attributes, subject to access controls.
   The affected file and attribute are determined from the given ms_update structure.
   Use the XATTR_CREATE and XATTR_REPLACE semantics from setxattr(2) (these 
   are fields in the given ms_update structure).
   This is part of the File Metadata API.
   """
   
   xattr_create = False 
   xattr_replace = False 
   xattr_mode = update.xattr_mode
   xattr_owner = update.xattr_owner
   
   if update.HasField( 'xattr_create' ):
      xattr_create = update.xattr_create
   
   if update.HasField( 'xattr_replace' ):
      xattr_replace = update.xattr_replace 
   
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("setxattr /%s/%s (name=%s, parent=%s) %s = %s (create=%s, replace=%s, mode=0%o)" % 
                (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_create, xattr_replace, xattr_mode))
      
   file_id = attrs['file_id']
   rc = 0

   # find gateway owner ID
   gateway_owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      gateway_owner_id = gateway.owner_id
   
   # if we're creating, then the requested xattr owner ID must match the gateway owner ID 
   if xattr_create and gateway_owner_id != xattr_owner:
      logging.error("Request to create xattr '%s' owned by %s does not match Gateway owner %s" % (update.xattr_name, xattr_owner, gateway_owner_id))
      rc = -errno.EACCES
   
   if rc == 0:
      msent_rc, msent = file_xattr_get_and_check_msentry_writeable( gateway, volume, file_id, caller_is_admin )
      xattr_rc, xattr = file_xattr_get_and_check_xattr_writable( gateway, volume, file_id, update.xattr_name, caller_is_admin )
      
      # if the xattr doesn't exist and the msent isn't writable by the caller, then this is an error 
      if (xattr is None or xattr_rc == -errno.ENOENT) and msent_rc != 0:
         rc = msent_rc
      
      else:
         # set the xattr
         rc = MSEntryXAttr.SetXAttr( volume, msent, update.xattr_name, update.xattr_value, create=xattr_create, replace=xattr_replace, mode=xattr_mode, owner=gateway_owner_id, caller_is_admin=caller_is_admin )
      
   logging.info("setxattr /%s/%s (name=%s, parent=%s) %s = %s (create=%s, replace=%s, mode=0%o) rc = %s" % 
                (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_create, xattr_replace, xattr_mode, rc) )
         
   return rc


# ----------------------------------
def file_xattr_removexattr( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Remove a file's extended attribute, subject to access controls.
   The affected file and attribute are determined from the given ms_update structure.
   This is part of the File Metadata API.
   """

   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("removexattr /%s/%s (name=%s, parent=%s) %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name))
      
   file_id = attrs['file_id']
   rc, msent = file_xattr_get_and_check_msentry_writeable( gateway, volume, file_id, caller_is_admin )
   
   if rc == 0:
      # check xattr writable 
      rc, xattr = file_xattr_get_and_check_xattr_writable( gateway, volume, file_id, update.xattr_name, caller_is_admin )
      
      if rc == 0:
         # get user id
         gateway_owner_id = GATEWAY_ID_ANON
         if gateway is not None:
            gateway_owner_id = gateway.owner_id
         
         # delete it 
         rc = MSEntryXAttr.RemoveXAttr( volume, msent, update.xattr_name, gateway_owner_id, caller_is_admin )
   
   logging.info("removexattr /%s/%s (name=%s, parent=%s) %s rc = %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, rc) )
   
   
   return rc


# ----------------------------------
def file_xattr_chmodxattr( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Set the access mode for an extended attribute.
   """
   
   xattr_mode = None
   
   if update.HasField( 'xattr_mode' ):
      xattr_mode = update.xattr_mode 
   
   if xattr_mode is None:
      logging.error("chmodxattr: Missing xattr_mode field")
      rc = -errno.EINVAL
   
   else:
      attrs = MSEntry.unprotobuf_dict( update.entry )
      
      logging.info("chmodxattr /%s/%s (name=%s, parent=%s) %s = %s (mode=0%o)" % 
                     (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_mode))
         
      file_id = attrs['file_id']
      
      # is this xattr writable?
      rc, xattr = file_xattr_get_and_check_xattr_writable( gateway, volume, file_id, update.xattr_name, caller_is_admin )
      
      if rc == 0:
         # allowed!
         # get user id
         gateway_owner_id = GATEWAY_ID_ANON
         if gateway is not None:
            gateway_owner_id = gateway.owner_id
         
         rc = MSEntryXAttr.ChmodXAttr( volume, file_id, update.xattr_name, xattr_mode, gateway_owner_id, caller_is_admin )
         
      logging.info("chmodxattr /%s/%s (name=%s, parent=%s) %s = %s (mode=0%o) rc = %s" % 
                     (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_mode, rc) )
            
   return rc


# ----------------------------------
def file_xattr_chownxattr( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Set the access mode for an extended attribute.
   """
   
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("chownxattr /%s/%s (name=%s, parent=%s) %s = %s (owner=%s)" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_owner))
   
   
   xattr_owner = None
   
   if update.HasField( 'xattr_owner' ):
      xattr_owner = update.xattr_owner 
   
   if xattr_owner is None:
      logging.error("Missing xattr_owner field")
      rc = -errno.EINVAL
   
   else:
      file_id = attrs['file_id']
      
      # is this xattr writable?
      rc, xattr = file_xattr_get_and_check_xattr_writable( gateway, volume, file_id, update.xattr_name, caller_is_admin )
      
      if rc == 0:
         # allowed!
         # get user id
         gateway_owner_id = GATEWAY_ID_ANON
         if gateway is not None:
            gateway_owner_id = gateway.owner_id
         
         rc = MSEntryXAttr.ChownXAttr( volume, file_id, update.xattr_name, xattr_owner, gateway_owner_id, caller_is_admin )
      
   logging.info("chownxattr /%s/%s (name=%s, parent=%s) %s = %s (owner=%s) rc = %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, xattr_owner, rc) )
         
   return rc


# ----------------------------------
def file_vacuum_log_check_access( gateway, msent ):
   """
   Verify that the gateway is allowed to manipulate the MSEntry's manifest log.
   """
   return msent.coordinator_id == gateway.g_id and gateway.check_caps( GATEWAY_CAP_COORDINATE | GATEWAY_CAP_WRITE_METADATA | GATEWAY_CAP_WRITE_DATA )


# ----------------------------------
def file_vacuum_log_response( volume, rc, log_record ):
   """
   Create a file response for the log record, if given.
   """
   
   reply = file_update_init_response( volume )
   reply.error = rc
   
   if rc == 0:
      reply.file_version = log_record.version
      reply.manifest_mtime_sec = log_record.manifest_mtime_sec 
      reply.manifest_mtime_nsec = log_record.manifest_mtime_nsec
      reply.affected_blocks.extend( log_record.affected_blocks )
   
   return file_update_complete_response( volume, reply )


# ----------------------------------
def file_vacuum_log_peek( gateway, volume, file_id, caller_is_admin=False ):
   """
   Get the head of the vacuum log for a particular file.
   Only serve data back if the requester is the coordinator of the file.
   Returning -ENOENT means that there is no log data available
   """
   
   logging.info("vacuum log peek /%s/%s by %s" % (volume.volume_id, file_id, gateway.g_id))
   
   rc = 0
   log_head = None
   
   msent = MSEntry.Read( volume, file_id )
   if msent is None:
      logging.error("No entry for %s" % file_id)
      rc = -errno.ENOENT 
      
   else:
      
      # security check--the caller must either be an admin, or the file's coordinator
      if not caller_is_admin and not file_vacuum_log_check_access( gateway, msent ):
         
         logging.error("Gateway %s is not allowed to access the vacuum log of %s" % (gateway.name, file_id))
         rc = -errno.EACCES
      
      else:
         # get the log head 
         log_head_list = MSEntryVacuumLog.Peek( volume.volume_id, file_id )
         
         if log_head_list is None or len(log_head_list) == 0:
            # no more data
            rc = -errno.ENOENT 
         
         else:
            log_head = log_head_list[0]
            
   logging.info("vacuum log peek /%s/%s by %s rc = %s" % (volume.volume_id, file_id, gateway.g_id, rc))
   
   return file_vacuum_log_response( volume, rc, log_head )


# ----------------------------------
def file_vacuum_log_remove( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Remove a record of the vacuum log for a particular file.  Only a coordinator can do this.
   """
   
   # check entry attrs
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   rc = 0
   required_attrs =  ['file_id', 'version', 'manifest_mtime_sec', 'manifest_mtime_nsec']
   
   attrs = file_update_get_attrs( attrs, required_attrs )
   
   if attrs is None:
      logging.error("vacuum log remove: Missing one of %s" % required_attrs )
      rc = -errno.EINVAL
   
   else:
      
      # extract attrs
      file_id = attrs['file_id']
      
      logging.info("vacuum log remove /%s/%s by %s" % (volume.volume_id, file_id, gateway.g_id))
   
      version = attrs['version']
      manifest_mtime_sec = attrs['manifest_mtime_sec']
      manifest_mtime_nsec = attrs['manifest_mtime_nsec']
      
      # get msent
      msent = MSEntry.Read( volume, file_id )
      if msent is None:
         logging.error("No entry for %s" % file_id )
         rc = -errno.ENOENT
      
      else:
         # security check 
         if not caller_is_admin and not file_vacuum_log_check_access( gateway, msent ):
            logging.error("Gateway %s is not allowed to access the vacuum log of %s" % (gateway.name, file_id))
            rc = -errno.EACCES 
            
         else:
            # Delete!
            rc = MSEntryVacuumLog.Remove( volume.volume_id, file_id, version, manifest_mtime_sec, manifest_mtime_nsec )
   
      logging.info("vacuum log remove /%s/%s by %s rc = %s" % (volume.volume_id, file_id, gateway.g_id, rc))
   
   return rc


# ----------------------------------
def file_vacuum_log_append( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Append a vacuum record to a file.  Only a coordinator can do this
   """
   

   attrs = MSEntry.unprotobuf_dict( update.entry )
   rc = 0
   
   required_attrs =  ['volume_id', 'file_id', 'version', 'manifest_mtime_sec', 'manifest_mtime_nsec']
   
   attrs = file_update_get_attrs( attrs, required_attrs )
   
   if attrs is None:
      logging.error("vacuum log remove: Missing one of %s" % required_attrs )
      rc = -errno.EINVAL
   
   else:
      
      affected_blocks = update.affected_blocks[:]
      if affected_blocks is not None and len(affected_blocks) > 0:
      
         logging.info("vacuume log append /%s/%s (%s), affected blocks = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], affected_blocks ) )
         
         # entry must exist 
         msent = MSEntry.Read( volume, attrs['file_id'] )
         if msent is None:
            
            logging.error( "No entry for %s" % attrs['file_id'] )
            rc = -errno.ENOENT 
         
         else:
            
            # security check 
            if not caller_is_admin and not file_vacuum_log_check_access( gateway, msent ):
               logging.error("Gateway %s is not allowed to access vacuum log of %s" % (gateway.name, attrs['file_id']))
               rc = -errno.EACCES
            
            else:
               
               # append!
               storagetypes.deferred.defer( MSEntryVacuumLog.Insert, attrs['volume_id'], ent_attrs['file_id'], ent_attrs['version'], ent_attrs['manifest_mtime_sec'], ent_attrs['manifest_mtime_nsec'], affected_blocks )
               rc = 0

         logging.info("vacuume log append /%s/%s (%s), affected blocks = %s rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], affected_blocks ), rc )
   
   return rc
