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

import collections
import storage
import network
import contact
import uuid
import syndicate.client.common.log as Log
import hashlib

log = Log.get_logger()

STORAGE_DIR = "/messages"
ATTACHMENTS_DIR = storage.path_join( STORAGE_DIR, "attachments" )
INCOMING_DIR = storage.path_join( STORAGE_DIR, "incoming" )
FOLDERS_DIR = storage.path_join( STORAGE_DIR, "folders" )

INBOX_FOLDER = "Inbox"
SENT_FOLDER = "Sent"
DRAFTS_FOLDER = "Drafts"
SPAM_FOLDER = "Spam"
TRASH_FOLDER = "Trash"

DEFAULT_FOLDERS = [
   INBOX_FOLDER,
   SENT_FOLDER,
   DRAFTS_FOLDER,
   SPAM_FOLDER,
   TRASH_FOLDER
]

# if we list one of these folders, check incoming as well
CHECK_INCOMING_FOLDERS = [
   INBOX_FOLDER,
   SPAM_FOLDER
]

# for setup
STORAGE_DIRS = [
   STORAGE_DIR,
   ATTACHMENTS_DIR
] + [storage.path_join(STORAGE_DIR, x) for x in DEFAULT_FOLDERS]

# locally-produced message and attachment
SyndicateMessage = collections.namedtuple( "SyndicateMessage", ["id", "sender_addr", "receiver_addrs", "cc_addrs", "bcc_addrs", "subject", "body", "timestamp", "handle", "attachment_names"] )
SyndicateAttachment = collections.namedtuple( "SyndicateAttachment", ["name", "data"] )

# locally-stored record for a message currently hosted on the sender's volume
SyndicateIncomingMessage = collections.namedtuple( "SyndicateIncomingMessage", ["id", "sender_addr", "receiver_addrs", "cc_addrs", "bcc_addrs", "subject", "timestamp", "handle", "attachment_names"] )

# given back in a listing
SyndicateMessageMetadata = collections.namedtuple( "SyndicateMessageMetadata", ["id", "sender_addr", "receiver_addrs", "cc_addrs", "bcc_addrs", "subject", "timestamp", "handle", "has_attachments", "is_read"] )


#-------------------------
def folder_dir( folder_name ):
   global FOLDERS_DIR
   
   return storage.path_join( storage.ROOT_DIR, FOLDERS_DIR, folder )

#-------------------------
def incoming_dir():
   global INCOMING_DIR
   
   return storage.path_join( storage.ROOT_DIR, INCOMING_DIR )

#-------------------------
def message_handle( message_timestamp, message_id ):
   return "%s-%s" % (str(message_timestamp), str(message_id))

#-------------------------
def stored_message_path( folder, message_timestamp, message_id ):
   # message path for locally hosted messages
   return storage.path_join( folder_dir( folder_name ), message_handle( message_timestamp, message_id ))

#-------------------------
def timestamp_from_message_path( message_path ):
   mp = os.path.basename( message_path )
   mp_parts = split(mp, "-")
   
   if len(mp_parts) != 2:
      return -1
   
   try:
      ts = int(mp_parts[0])
   except:
      return -1
   
   return ts

#-------------------------
def id_from_message_path( message_path ):
   mp = os.path.basename( message_path )
   mp_parts = split(mp, "-")
   
   if len(mp_parts) != 2:
      return None
   
   return mp_parts[1]

#-------------------------
def incoming_message_path( message_timestamp, message_id ):
   # message path for remotely-hosted messages that we know about from our server
   global INCOMING_DIR
   
   return storage.path_join( storage.ROOT_DIR, INCOMING_DIR, message_handle(message_timestamp, message_id))

#-------------------------
def attachment_storage_name( message_timestamp, message_id, attachment ):
   m = hashlib.sha256()
   m.update( storage.PATH_SALT )        # defend against attacker knowing the hash of the content
   m.update( str(attachment) )
   attachment_hash = m.hexdigest()
   
   full_name = "%s-%s-%s" % (str(message_timestamp), str(message_id), str(attachment_hash))
   
   full_name_safe = storage.salt_string( full_name )
   return full_name_safe

#-------------------------
def attachment_path( message_timestamp, message_id, attachment ):
   global STORAGE_DIR
   
   name = attachment_storage_name( message_timestamp, message_id, attachment )
   return storage.path_join( storage.ROOT_DIR, STORAGE_DIR, ATTACHMENTS_DIR, name )
   

#-------------------------
def folder_cache_name( folder_name ):
   return ".cache.%s" % folder_name

#-------------------------
def create_folder( folder_name ):
   return storage.setup_dirs( storage.ROOT_DIR, [folder_name] )

#-------------------------
def delete_folder( folder_name ):
   global DEFAULT_FOLDERS
   if folder_name not in DEFAULT_FOLDERS:
      return storage.delete_dirs( storage.ROOT_DIR, [folder_name] )
   else:
      # can't delete built-in folder
      return False

#-------------------------
def store_message( pubkey_str, folder, _message, _attachments=None ):
   # calculate attachment paths
   attachment_paths = {}
      
   if _attachments != None:
      assert isinstance( _attachments, dict ), "Invalid argument: attachments must be a dict"
      
      for attachment_name, attachment_data in _attachments.items():
         apath = attachment_path( _message.timestamp, _message.id, attachment_name )
         attachment_paths[ attachment_name ] = apath
   
   # generate the message with the attachments
   attrs = dict( [(field, getattr(_message, field)) for field in _message._fields] )
   attrs['attachment_names'] = attachment_paths.keys()
   
   message = SyndicateMessage( **attrs )
   
   try:
      message_json = storage.tuple_to_json( message )
   except Exception, e:
      log.error("Failed to serialize message")
      log.exception(e)
      return False
   
   # what files have been stored?
   stored = []
   
   # store message
   mpath = stored_message_path( folder, message.timestamp, message.id )
   rc = storage.write_encrypted_file( pubkey_str, message_json, mpath )
   if not rc:
      log.error("Failed to store message")
      return False
   
   stored.append( mpath )
   
   failed = False
   for (attachment_name, attachment_path) in attachment_paths.items():
      attachment_data = _attachments[attachment_name]
      rc = storage.write_encrypted_file( pubkey_str, attachment_path, attachment_data )
      if not rc:
         failed = True
         break
      
      stored.append( attachment_path )
   
   if failed:
      # roll back
      for path in stored:
         storage.delete_file( path )
      
      return False
   
   else:
      storage.purge_cache( STORAGE_DIR, folder_cache_name( folder_name ) )
      return True

#-------------------------
def read_stored_message( privkey_str, folder, msg_timestamp, msg_id ):
   mpath = stored_message_path( folder, msg_timestamp, msg_id )
   
   msg_json = storage.read_encrypted_file( privkey_str, mpath )
   if msg_json is None:
      log.error("Failed to read message")
      return None
   
   try:
      msg = storage.json_to_tuple( SyndicateMessage, msg_json )
   except Exception, e:
      log.error("Failed to parse message")
      log.exception(e)
      return None
   
   return msg

#-------------------------
def delete_message( folder, msg_timestamp, msg_id ):
   rc = storage.delete_file( stored_message_path( folder, msg_timestamp, msg_id ) )
   storage.purge_cache( STORAGE_DIR, folder_cache_name( folder_name ) )
   return rc


#-------------------------
def send_message( pubkey_str, privkey_str, sender_addr, receiver_addrs, cc_addrs, bcc_addrs, subject, body, attachments={}, folder=unicode(SENT_FOLDER) ):
   now = int(time.time())
   msg_id = uuid.uuid4().get_hex()
   handle = message_handle( now, msg_id )
   
   # sanity check...
   if receiver_addrs == None:
      receiver_addrs = []
   
   if cc_addrs == None:
      cc_addrs = []
   
   if bcc_addrs == None:
      bcc_addrs = []
   
   subject = unicode(subject)
   body = unicode(body)
   folder = unicode(folder)
   
   assert isinstance( receiver_addrs, list ), "Invalid argument for receiver_addrs"
   assert isinstance( cc_addrs, list ), "Invalid argument for cc_addrs"
   assert isinstance( bcc_addrs, list ), "Invalid argument for bcc_addrs"
   assert isinstance( folder, unicode ), "Invalid argument for folder"
   
   for addr in [sender_addr] + receiver_addrs + cc_addrs + bcc_addrs:
      assert isinstance(addr, str) or isinstance(addr, unicode), "Invalid address '%s'" % addr
   
   try:
      # parse addresses
      sender_addr_parsed = contact.parse_addr( sender_addr )
      
      receiver_addrs_parsed = [contact.parse_addr( x ) for x in receiver_addrs]
      cc_addrs_parsed = [contact.parse_addr( x ) for x in cc_addrs]
      bcc_addrs_parsed = [contact.parse_addr( x ) for x in bcc_addrs]
      
   except Exception, e:
      log.error("Failed to parse addresses")
      log.exception(e)
      raise Exception("Invalid address")
      
   # construct the message
   message = SyndicateMessage( id=msg_id,
                               sender_addr=sender_addr,
                               receiver_addr=receiver_addrs,
                               cc_addrs=cc_addrs,
                               bcc_addrs=bcc_addrs,
                               subject=subject,
                               body=body,
                               timestamp=now,
                               handle=handle,
                               attachment_names=attachments.keys() )
      
   contacts = []
   new_contacts = []
   missing = []
   send_via_gateway = []
   
   # get contact public keys from Volume
   for addr in receiver_addrs_parsed + cc_addrs_parsed + bcc_addrs_parsed:
      if contact_exists( addr.addr ):
         # get the contact public key
         contact_rec = contact.read_contact( privkey_str, addr.addr )
         if contact_rec is None:
            missing.append( addr )
         else:
            contacts.append( contact_rec )
      
      else:
         missing.append( addr )
            
   # get remaining contact public keys from the network
   for missing_addr in missing:
      # new contact...
      pubkey_pem = network.download_user_pubkey( missing_addr )
      if pubkey_pem is None:
         # not on Syndicate
         send_via_gateway.append( missing_addr )
      
      else:
         missing_contact = SyndicateContact( addr=missing_addr, pubkey_pem=pubkey_pem, extras={} )
         new_contacts.append( missing_contact )
         
   # TODO: finish
         
   rc = store_message( pubkey_str, folder, message, attachments )
   if not rc:
      log.error("Failed to store message")
      return False
   
   return True

#-------------------------
def store_incoming_message( pubkey_str, message ):
   try:
      message_json = storage.tuple_to_json( message )
   except Exception, e:
      log.error("Failed to serialize incoming message")
      log.exception(e)
      return False
   
   # store incoming message
   mpath = incoming_message_path( message.timestamp, message.id )
   rc = storage.write_encrypted_file( pubkey_str, message_json, mpath )
   if not rc:
      log.error("Failed to store incoming message")
      return False
   
   return True


#-------------------------
def read_incoming_message( privkey_str, msg_timestamp, msg_id ):
   mpath = incoming_message_path( msg_timestamp, msg_id )
   
   msg_json = storage.read_encrypted_file( privkey_str, mpath )
   if msg_json is None:
      log.error("Failed to read incoming message")
      return None
   
   try:
      msg = storage.json_to_tuple( SyndicateIncomingMessage, msg_json )
   except Exception, e:
      log.error("Failed to parse incoming message")
      log.exception(e)
      return None
   
   return msg
   

#-------------------------
def read_message( privkey_str, folder, msg_timestamp, msg_id ):
   # is this an incoming message?
   mpath = incoming_message_path( msg_timestamp, msg_id )
   if os.path.exists( mpath ):
      # get the incoming message record
      incoming_message = read_incoming_message( privkey_str, msg_timestamp, msg_id )
      
      # fetch the whole message from the remote Volume's SENT folder
      return storage.read_volume_file( stored_message_path( SENT_FOLDER, msg_timestamp, msg_id ) )
      
   else:
      # it's a local message
      return read_stored_message( privkey_str, folder, msg_timestamp, msg_id )
      

#-------------------------
def list_messages( privkey_str, pubkey_str, folder_name, start_timestamp=None, end_timestamp=None, length=None ):
   global STORAGE_DIR, CHECK_INCOMING_FOLDERS
   
   cached_items = storage.get_cached_data( privkey_str, STORAGE_DIR, folder_cache_name( folder_name ) )
   if cached_items == None:
      log.info("No cached data for %s" % folder_name)
   
   try:
      dir_ents = os.listdir( folder_dir(folder_name) )
   except OSError, oe:
      log.error("Failed to list folder, errno = %s" % oe.errno)
      log.exception(oe)
      raise Exception("Internal error")
   
   # tag these dir entries as having come from the folder
   FROM_FOLDER = 1
   FROM_INCOMING = 2
   
   dir_ents = [(x, FROM_FOLDER) for x in dir_ents]
   
   if folder_name in CHECK_INCOMING_FOLDERS:
      # check incoming as well
      try:
         incoming_dir_ents = os.listdir( incoming_dir() )
      except OSError, oe:
         log.error("Failed to list folder, errno = %s" % oe.errno)
         log.exception(oe)
         raise Exception("Internal error")
      
      # tag these dir entries as having come from incoming
      incoming_dir_ents = [(x, FROM_INCOMING) for x in incoming_dir_ents]
      
      dir_ents += incoming_dir_ents
   
   # will sort on dir_ents[i][0]
   dir_ents.sort()
   dir_ents.reverse()
   
   # get all messages between start and end timestamps
   if start_timestamp is None:
      start_timestamp = 0
   
   if end_timestamp is None:
      end_timestamp = 0
   
   start_idx = -1
   end_idx = -1
   
   if length is None:
      length = len(dir_ents)
      
   for i in xrange(0, len(dir_ents)):
      timestamp = timestamp_from_message_path( dir_ents[i][0] )
      
      # too early?
      if timestamp < start_timestamp:
         continue
      
      # too late?
      if timestamp > end_timestamp and end_timestamp > 0:
         continue
      
      if start_idx == -1:
         start_idx = i
      
      end_idx = i
      
      if end_idx - start_idx > length:
         break

   wanted = dir_ents[start_idx: end_idx]
   ret = []
   
   # generate a metadata record for each of these
   for (dir_ent, origin) in wanted:
      msg_data = None
      is_read = False
      if origin == FROM_FOLDER:
         msg_data = read_stored_message( privkey_str, folder_name, timestamp_from_message_path( dir_ent ), id_from_message_path( dir_ent ) )
         is_read = True
      
      else:
         msg_data = read_incoming_message( privkey_str, timestamp_from_message_path( dir_ent ), id_from_message_path( dir_ent ) )
      
      if msg_data == None:
         log.warning("Failed to read message")
         continue
      
      metadata = SyndicateMessageMetadata( id=msg_data.id,
                                           sender_addr=msg_data.sender_addr,
                                           receiver_addrs=msg_data.receiver_addrs,
                                           cc_addrs=msg_data.cc_addrs,
                                           bcc_addrs=msg_data.bcc_addrs,
                                           subject=msg_data.subject,
                                           timestamp=msg_data.timestamp,
                                           handle=message_handle( msg_data.timestamp, msg_data.id ),
                                           has_attachments=(len(msg_data.attachment_names) > 0),
                                           is_read=is_read )
      
      ret.append( metadata )
   
   storage.cache_data( pubkey_str, STORAGE_DIR, folder_cache_name( folder_name ), ret )
   return ret

         
         
if __name__ == "__main__":
   print "fill in some tests here"