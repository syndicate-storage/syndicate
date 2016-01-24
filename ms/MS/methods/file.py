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
from common.admin_info import *
import common.msconfig as msconfig

import random
import os
import errno
import base64

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
   reply.cert_version = volume.cert_bundle.mtime_sec
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
   Sign a protobuf ms_reply structure, using Syndicate's private key
   """
   import common.api as api
   
   # sign each directory entry in the reply with the Syndicate key,
   # so the MS attests to its index information
   for ent_pb in reply.listing.entries:
       
       if ent_pb.type == MSENTRY_TYPE_DIR:
           
           # sign the MS-related fields
           ent_pb.ms_signature = ""
           ent_str = ent_pb.SerializeToString()
           sig = api.sign_data( SYNDICATE_PRIVKEY, ent_str )
           sigb64 = base64.b64encode( sig )
           
           ent_pb.ms_signature = sigb64
   
   
   # sign the entire reply
   reply.signature = ""
   reply_str = reply.SerializeToString()
   
   sig = api.sign_data( SYNDICATE_PRIVKEY, reply_str )
   sigb64 = base64.b64encode( sig )
   
   reply.signature = sigb64
   reply_str = reply.SerializeToString()
   
   return reply_str


# ----------------------------------
def file_update_get_attrs( entry_dict, attr_list ):
   """
   Get a set of required attriutes.
   Return None if at least one is missing.
   """
   ret = {}
   missing = []
   for attr_name in attr_list:
      if not entry_dict.has_key(attr_name):

         for attr_name in attr_list:
             if not entry_dict.has_key(attr_name):
                 missing.append( attr_name )

         log.error("Missing: %s" % (",".join(missing)))
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
def file_write_allowed( owner_id, file_data ):
   """
   Can the user write to the file?  Return the appropriate error code.
   """
   
   if file_data.owner_id == owner_id and (file_data.mode & 0444) == 0:
      return -errno.EACCES
   
   if file_data.owner_id != owner_id and (file_data.mode & 0044) == 0:
      return -errno.EACCES
   
   return 0


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
         ent_pb = reply.listing.entries.add()
         MSEntry.protobuf( file_data, ent_pb )
         
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
      MSEntry.protobuf( file_data, ent_pb )
      
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
         MSEntry.protobuf( ent, ent_pb )
   
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
   
   if page_id is not None or lug is not None:
        
      owner_id = msconfig.GATEWAY_ID_ANON
      if gateway != None:
         owner_id = gateway.owner_id
        
      rc, reply = _listdir( owner_id, volume, file_id, page_id=page_id, least_unknown_generation=lug )
   
   else:
      logging.error("page_id or l.u.g. required")
      rc = -errno.EINVAL
      reply = make_ms_reply( volume, rc )
      
      reply.listing.ftype = 0
      reply.listing.status = ms_pb2.ms_listing.NONE 
   
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
            MSEntry.protobuf( ent, ent_pb )
            
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
      
      logging.info("update /%s/%s (%s)" % (attrs['volume_id'], attrs['file_id'], attrs['name'] ) )
      
      rc, ent = MSEntry.Update( gateway.owner_id, volume, gateway, **attrs )
      
      logging.info("update /%s/%s (%s) rc = %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
      
      if not async:
         
         # have an entry 
         if ent is not None:
            ent_pb = reply.listing.entries.add()
            MSEntry.protobuf( ent, ent_pb )
         
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
   
   rc, ent = MSEntry.Rename( gateway.owner_id, gateway, volume, src_attrs, dest_attrs )
   
   logging.info("rename /%s/%s (name=%s, parent=%s) to (name=%s, parent=%s) rc = %s" % 
                  (src_attrs['volume_id'], src_attrs['file_id'], src_attrs['name'], src_attrs['parent_id'], dest_attrs['name'], dest_attrs['parent_id'], rc) )

   # have an entry 
   if ent is not None:
      ent_pb = reply.listing.entries.add()
      MSEntry.protobuf( ent, ent_pb )
     
   return rc


# ----------------------------------
def file_chcoord( reply, gateway, volume, update ):
   """
   Change the coordinator of a file, using the fields of the given ms_update structure.
   Add a new entry to the given ms_reply protobuf, containing data from the updated MSEntry.
   This is part of the File Metadata API.
   """
   
   if gateway is None:
     # coordinators can't be anonymous 
     return -errno.EACCES
 
   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("chcoord /%s/%s (%s) to %s" % (attrs['volume_id'], attrs['file_id'], attrs['name'], gateway.g_id) )
  
   rc, ent = MSEntry.Chcoord( gateway.owner_id, gateway, volume, **attrs )
   
   logging.info("chcoord /%s/%s (%s) rc = %d" % (attrs['volume_id'], attrs['file_id'], attrs['name'], rc ) )
   
   # have an entry 
   if ent is not None:
      ent_pb = reply.listing.entries.add()
      MSEntry.protobuf( ent, ent_pb )

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
   updates_set = ms_pb2.ms_request_multi()

   try:
      updates_set.ParseFromString( data )
   except Exception, e:
      logging.exception(e)
      return None
   
   return updates_set


# ----------------------------------
def file_update_auth( gateway ):
   """
   Verify whether or not the given gateway (which can be None) is allowed to update (POST) metadata.
   """
   
   # gateway must be known
   if gateway == None:
      logging.error("Unknown gateway")
      return False
   
   # must have GATEWAY_CAP_WRITE_DATA
   if (gateway.caps & msconfig.GATEWAY_CAP_WRITE_METADATA ) == 0:
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

   # this gateway must be allowed to read metadata
   if gateway is not None and not gateway.check_caps( msconfig.GATEWAY_CAP_READ_METADATA ):
      logging.error( "bad caps: %s" % gateway.caps )
      return False
   
   return True


# ----------------------------------
def file_xattr_fetchxattrs_response( volume, rc, xattr_names_and_values, xattr_nonce, xattr_hash ):
   """
   Generate a serialized, signed ms_reply protobuf from
   a fetchxattrs return code (rc) and xattr names list.
   """

   # create and sign the response 
   reply = file_update_init_response( volume )
   reply.error = rc
   
   if rc == 0:
      
      for xattr_data in xattr_names_and_values:
         reply.xattr_names.append( xattr_data.xattr_name )
         reply.xattr_values.append( xattr_data.xattr_value )
      
      if xattr_hash is not None:
          reply.xattr_hash = xattr_hash 
      else:
          reply.xattr_hash = ""
          
      reply.xattr_nonce = xattr_nonce
      
   return file_update_complete_response( volume, reply )


# ----------------------------------
def file_xattr_fetchxattrs( gateway, volume, file_id, unused=None, caller_is_admin=False ):
   """
   Get the names, values, and hash of a file's extended attributes.
   This is part of the File Metadata API.
   
   NOTE: unused=None is required for the File Metadata API dispatcher to work.
   """
   
   logging.info("fetchxattrs /%s/%s" % (volume.volume_id, file_id) )

   rc = 0
   owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      owner_id = gateway.owner_id
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent == None:
      # does not exist
      rc = -errno.ENOENT
   
   elif file_read_allowed( owner_id, msent ) != 0:
      # not allowed 
      rc = -errno.EACCES
      
   else:
      # get the xattr names
      rc, xattr_names_and_values = MSEntryXAttr.FetchXAttrs( volume, msent )
   
   logging.info("fetchxattrs /%s/%s rc = %d" % (volume.volume_id, file_id, rc) )

   return file_xattr_fetchxattrs_response( volume, rc, xattr_names_and_values, msent.xattr_nonce, msent.xattr_hash )


# ----------------------------------
def file_xattr_putxattr( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Unconditionally put a new xattr.
   This is part of the File Metadata API
   
   Only the coordinator can do this.
   """

   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("putxattr /%s/%s (name=%s, parent=%s) %s = %s" % 
                (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value ))
      
   file_id = attrs['file_id']
   rc = 0
   
   owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      owner_id = gateway.owner_id
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent == None:
      # does not exist
      rc = -errno.ENOENT
   
   elif file_write_allowed( owner_id, msent ) != 0:
      # not allowed 
      rc = -errno.EACCES
   
   elif msent.coordinator_id != GATEWAY_ID_ANON and msent.coordinator_id != gateway.g_id:
      # only coordinator can call this
      rc = -errno.EAGAIN
   
   else:
      # can write
      # set the xattr
      rc = MSEntryXAttr.PutXAttr( volume, msent, update.xattr_name, update.xattr_value, update.xattr_nonce, update.xattr_hash )
      
   logging.info("putxattr /%s/%s (name=%s, parent=%s) %s = %s rc = %s" % 
                (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, update.xattr_value, rc) )
         
   return rc


# ----------------------------------
def file_xattr_removexattr( reply, gateway, volume, update ):
   """
   Remove a file's extended attribute.
   The affected file and attribute are determined from the given ms_update structure.
   This is part of the File Metadata API.
   """

   attrs = MSEntry.unprotobuf_dict( update.entry )
   
   logging.info("removexattr /%s/%s (name=%s, parent=%s) %s" % 
                  (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name))
      
   file_id = attrs['file_id']
   
   owner_id = GATEWAY_ID_ANON
   if gateway is not None:
      owner_id = gateway.owner_id
   
   # get the msentry
   msent = MSEntry.Read( volume, file_id )
   if msent == None:
      # does not exist
      rc = -errno.ENOENT
   
   elif file_write_allowed( owner_id, msent ) != 0:
      # not allowed 
      rc = -errno.EACCES
   
   elif msent.coordinator_id != GATEWAY_ID_ANON and msent.coordinator_id != gateway.g_id:
      # only coordinator can call this
      rc = -errno.EAGAIN
   
   else: 
      # delete it 
      rc = MSEntryXAttr.RemoveXAttr( volume, msent, update.xattr_name, update.xattr_nonce, update.xattr_hash )
   
   logging.info("removexattr /%s/%s (name=%s, parent=%s) %s rc = %s" % 
                 (attrs['volume_id'], attrs['file_id'], attrs['name'], attrs['parent_id'], update.xattr_name, rc) )
   
   
   return rc


# ----------------------------------
def file_vacuum_log_check_access( gateway, msent ):
   """
   Verify that the gateway is allowed to manipulate the MSEntry's vacuum log.
   A gateway may do so if
   * it is in the same volume
   * it has the capabilties to coordinate, write metadata, and write data
   * the file is writeable to the volume, or this gateway's owner is the owner
   * the file is not a directory
   
   It does *not* have to be the coordinator, since the vacuum log is built before the write is submitted.
   """
   return gateway.volume_id == msent.volume_id and (gateway.owner_id == msent.owner_id or (msent.mode & 0060)) \
       and gateway.check_caps( msconfig.GATEWAY_CAP_COORDINATE | msconfig.GATEWAY_CAP_WRITE_METADATA | msconfig.GATEWAY_CAP_WRITE_DATA ) \
       and msent.ftype == MSENTRY_TYPE_FILE


# ----------------------------------
def file_vacuum_log_response( volume, rc, log_record ):
   """
   Create a file response for the log record, if given.
   """
   
   reply = file_update_init_response( volume )
   reply.error = rc
   
   if rc == 0:
      reply.vacuum_ticket.volume_id = log_record.volume_id
      reply.vacuum_ticket.file_id = MSEntry.serialize_id( log_record.file_id )
      reply.vacuum_ticket.writer_id = log_record.writer_id
      reply.vacuum_ticket.file_version = log_record.version
      reply.vacuum_ticket.manifest_mtime_sec = log_record.manifest_mtime_sec 
      reply.vacuum_ticket.manifest_mtime_nsec = log_record.manifest_mtime_nsec
      reply.vacuum_ticket.affected_blocks.extend( log_record.affected_blocks )
      reply.vacuum_ticket.signature = log_record.signature
   
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
         log_head = MSEntryVacuumLog.Peek( volume.volume_id, file_id )
         
         if log_head is None:
            # no more data
            rc = -errno.ENOENT 
         
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
   required_attrs =  ['volume_id', 'coordinator_id', 'file_id', 'version', 'manifest_mtime_sec', 'manifest_mtime_nsec']
   
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
            rc = MSEntryVacuumLog.Remove( volume.volume_id, attrs['coordinator_id'], file_id, version, manifest_mtime_sec, manifest_mtime_nsec )
   
      logging.info("vacuum log remove /%s/%s by %s rc = %s" % (volume.volume_id, file_id, gateway.g_id, rc))
   
   return rc


# ----------------------------------
def file_vacuum_log_append( reply, gateway, volume, update, caller_is_admin=False ):
   """
   Append a vacuum record to a file.  Only a coordinator can do this
   """
   
   attrs = MSEntry.unprotobuf_dict( update.entry )
   rc = 0
   
   required_attrs =  ['volume_id', 'coordinator_id', 'file_id', 'version', 'manifest_mtime_sec', 'manifest_mtime_nsec']
   
   attrs = file_update_get_attrs( attrs, required_attrs )
   
   if attrs is None:
      logging.error("vacuum log remove: Missing one of %s" % required_attrs )
      rc = -errno.EINVAL
   
   else:
      
      if not hasattr(update, 'affected_blocks') or not hasattr(update, 'vacuum_signature'):
          loging.error("Missing affected_blocks and/or vacuum_signature")
          rc = -errno.EINVAL

      else:
          
          vacuum_signature = update.vacuum_signature
          affected_blocks = update.affected_blocks[:]
          if affected_blocks is not None and len(affected_blocks) > 0:
          
             logging.info("vacuum log append /%s/%s, affected blocks = %s" % (attrs['volume_id'], attrs['file_id'], affected_blocks ) )
             
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
                   storagetypes.deferred.defer( MSEntryVacuumLog.Insert, attrs['volume_id'], attrs['coordinator_id'], attrs['file_id'], attrs['version'], \
                                                attrs['manifest_mtime_sec'], attrs['manifest_mtime_nsec'], affected_blocks, vacuum_signature )
                   rc = 0

             logging.info("vacuum log append /%s/%s, affected blocks = %s rc = %s" % (attrs['volume_id'], attrs['file_id'], affected_blocks, rc ))
   
   return rc
