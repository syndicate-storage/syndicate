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
VOLUME_STORAGE_DIRS = [
   STORAGE_DIR,
   ATTACHMENTS_DIR
] + [storage.path_join(STORAGE_DIR, x) for x in DEFAULT_FOLDERS]

LOCAL_STORAGE_DIRS = []

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
   
   return storage.volume_path( FOLDERS_DIR, folder )

#-------------------------
def incoming_dir():
   global INCOMING_DIR
   
   return storage.volume_path( INCOMING_DIR )

#-------------------------
def message_handle( message_timestamp, message_id ):
   return "%s-%s" % (str(message_timestamp), str(message_id))

#-------------------------
def stored_message_path( folder, message_timestamp, message_id ):
   # message path for messages hosted on our own Volume
   return storage.volume_path( folder_dir( folder_name ), message_handle( message_timestamp, message_id ))

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
   
   return storage.volume_path( INCOMING_DIR, message_handle(message_timestamp, message_id))

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
   return storage.volume_path( STORAGE_DIR, ATTACHMENTS_DIR, name )
   

#-------------------------
def folder_cache_name( folder_name ):
   return ".cache.%s" % folder_name

#-------------------------
def create_folder( folder_name ):
   return storage.setup_dirs( [folder_dir( folder_name )] )

#-------------------------
def delete_folder( folder_name ):
   global DEFAULT_FOLDERS
   if folder_name not in DEFAULT_FOLDERS:
      return storage.delete_dirs( [folder_dir( folder_name )] )
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
      # TODO: remote volume instance!
      return storage.read_file( stored_message_path( SENT_FOLDER, msg_timestamp, msg_id ) )
      
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
   
   fake_module = collections.namedtuple( "FakeModule", ["VOLUME_STORAGE_DIRS", "LOCAL_STORAGE_DIRS"] )
   session.do_test_volume( "/tmp/storage-test/volume" )
   
   fake_mod = fake_module( LOCAL_STORAGE_DIRS=LOCAL_STORAGE_DIRS, VOLUME_STORAGE_DIRS=VOLUME_STORAGE_DIRS )
   assert storage.setup_storage( "/apps/syndicatemail/data", "/tmp/storage-test/local", [fake_mod] ), "setup_storage failed"
   
   pubkey_str = """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxwhi2mh+f/Uxcx6RuO42
EuVpxDHuciTMguJygvAHEuGTM/0hEW04Im1LfXldfpKv772XrCq+M6oKfUiee3tl
sVhTf+8SZfbTdR7Zz132kdP1grNafGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0R
rXyEnxpJmnLyNYHaLN8rTOig5WFbnmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsY
QPywYw8nJaax/kY5SEiUup32BeZWV9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmx
L1LRX5T2v11KLSpArSDO4At5qPPnrXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8V
WpsmzZaFExJ9Nj05sDS1YMFMvoINqaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65
A7d9Fn/B42n+dCDYx0SR6obABd89cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kw
JtgiKSCt6m7Hwx2kwHBGI8zUfNMBlfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CB
hGBRJQFWVutrVtTXlbvT2OmUkRQT9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyP
GuKX1KO5JLQjcNTnZ3h3y9LIWHsCTCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2
lPCia/UWfs9eeGgdGe+Wr4sCAwEAAQ==
-----END PUBLIC KEY-----
""".strip()
   
   privkey_str = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
""".strip()

   