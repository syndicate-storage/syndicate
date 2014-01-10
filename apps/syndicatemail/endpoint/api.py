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

# top-level API, to be called by the Javascript client

import syndicatemail.common.contact as contact
import syndicatemail.common.message as message
import syndicatemail.common.session as session
import syndicatemail.common.storage as storage

import time

# ------------------------------------------------
def session_required( func ):
   # deocrator to ensure that the session is not expired
   def inner( cls, *args, **kw ):
      if session.is_expired( cls.config ):
         raise Exception("Session expired")
      else:
         return func( cls, *args, **kw )
      
   inner.__name__ = func.__name__
   return inner

# ------------------------------------------------
def store_attachment( attachment_name, attachment_path ):
   return True

# ------------------------------------------------
def get_attachments( attachment_names ):
   return {}


class API( object ):
   
   config = None
   
   @classmethod
   def setup( cls, conf ):
      cls.config = conf
      
   @classmethod
   def key_str( cls, key ):
      return key.exportKey()
   
   @classmethod
   def publicKeyStr( cls ):
      return cls.key_str( cls.config['pubkey'] ) if (cls.config.has_key('pubkey') and cls.config['pubkey'] is not None) else None
   
   @classmethod
   def privateKeyStr( cls ):
      return cls.key_str( cls.config['privkey'] ) if (cls.config.has_key('privkey') and cls.config['privkey'] is not None) else None
   
   
   # ------------------------------------------------
   @classmethod
   def hello_world( cls, name, question="How are you?" ):
      return "Hello, %s.  %s" % (name, question)
   
   # ------------------------------------------------
   @classmethod
   @session_required
   def read_contact( cls, email_addr ):
      contact_tuple = contact.read_contact( cls.publicKeyStr(), cls.privateKeyStr(), email_addr )
      if contact_tuple != None:
         contact_dict = storage.tuple_to_dict( contact_tuple )
         return contact_dict
      else:
         return None

   @classmethod
   @session_required
   def list_contacts( cls, start_idx=None, length=None ):
      listing = contact.list_contacts( cls.publicKeyStr(), cls.privateKeyStr(), start_idx=start_idx, length=length )
      if listing != None:
         listing_dicts = [storage.tuple_to_dict( x ) for x in listing]
         return listing_dicts
      else:
         return []
   
   @classmethod
   @session_required
   def add_contact( cls, email_addr, pubkey_pem, **fields ):
      return contact.add_contact( cls.publicKeyStr(), email_addr, pubkey_pem, fields )

   @classmethod
   @session_required
   def delete_contact( cls, email_addr ):
      return contact.delete_contact( cls.publicKeyStr(), email_addr )

   # ------------------------------------------------
   @classmethod
   def login( email, password, oid_username, oid_password ):
      return session.do_login( cls.config, email, password, oid_username, oid_password )
   
   @classmethod
   def logout( email, password ):
      return session.do_logout( cls.config, email, password )

   # ------------------------------------------------
   @classmethod
   @session_required
   def read_message( cls, folder, msg_handle ):
      msg_id = message.id_from_message_path( msg_handle )
      msg_ts = message.timestamp_from_message_path( msg_handle )
      msg = message.read_message( cls.config['volume'], cls.publicKeyStr(), cls.privateKeyStr(), cls.config['gateway_privkey_pem'], folder, msg_ts, msg_id )
      
      msg_json = dict( [(attr, getattr(msg, attr)) for attr in msg._fields] )
      return msg_json

   @classmethod
   @session_required
   def read_attachment( cls, attachment_name ):
      pass
   
   @classmethod
   @session_required
   def list_messages( cls, folder, timestamp_start, timestamp_end, length=None ):
      msg_list = message.list_messages( cls.config['volume'], cls.publicKeyStr(), cls.privateKeyStr(), folder, start_timestamp=timestamp_start, end_timestamp=timestamp_end, length=length )
      
      # turn these into JSON-esque structures
      msg_list_json = []
      for msg in msg_list:
         msg_list_json.append( dict( [(attr, getattr(msg, attr)) for attr in msg._fields] ) )
      
      return msg_list_json

   @classmethod
   @session_required
   def delete_message( cls, folder, msg_handle ):
      msg_id = message.id_from_message_path( msg_handle )
      msg_ts = message.timestamp_from_message_path( msg_handle )
      return message.delete_message( folder, msg_ts, msg_id )

   @classmethod
   @session_required
   def send_message( cls, recipient_addrs, cc_addrs, bcc_addrs, subject, msg_body, attachment_names={} ):
      attachment_data = get_attachments( attachment_names )
      status, failed = message.send_message( cls.publicKeyStr(), cls.privateKeyStr(), cls.config['email'], recipient_addrs, cc_addrs, bcc_addrs, subject, msg_body, attachment_data )
      return {"status": status, "failed": failed}
