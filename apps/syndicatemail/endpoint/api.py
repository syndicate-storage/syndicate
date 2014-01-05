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

def session_required( func ):
   # deocrator to ensure that the session is not expired
   def inner( cls, *args, **kw ):
      if session.is_expired( cls.config ):
         raise Exception("Session expired")
      else:
         return func( cls, *args, **kw )
      
   inner.__name__ = func.__name__
   return inner


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
      contact_tuple = contact.read_contact( cls.privateKeyStr(), email_addr )
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
      return contact.delete_contact( email_addr )

   # ------------------------------------------------
   @classmethod
   def login( email, password ):
      return session.do_login( cls.config, email, password )
   
   @classmethod
   def logout( email, password ):
      return session.do_logout( cls.config, email, password )

   # ------------------------------------------------
   @classmethod
   @session_required
   def read_message( cls, folder, msg_handle ):
      msg_id = "a6322463ec5e4e4cb65ad88746aa832e"
      msg_ts = 1388897380
      
      h = message.message_handle( msg_ts, msg_id )
      if msg_handle == h:
         fake_message = message.SyndicateMessage(  id=msg_id,
                                                   sender_addr="testuser.mail.syndicate.com@example.com",
                                                   receiver_addrs=["nobody1.mail.syndicate1.com@example1.com", "nobody2.mail.syndicate2.com@example2.com"],
                                                   cc_addrs=["nobody3.myvolume.syndicate3.com@example3.com"],
                                                   bcc_addrs=["nobody4.myvolume.syndicate4.com@example4.com"],
                                                   subject="This is a fake message",
                                                   body="This method is not implemented",
                                                   timestamp=msg_ts,
                                                   handle=h,
                                                   attachment_names=["79e54e60bcf2142a4d7c3131e2ebeef774be7dceb643f83ae2d16ee31e3e3dee"] )
                                          
         return storage.tuple_to_dict( fake_message )
      
      else:
         return None

   @classmethod
   @session_required
   def read_attachment( cls, attachment_name ):
      fake_attachment = message.SyndicateAttachment( name="79e54e60bcf2142a4d7c3131e2ebeef774be7dceb643f83ae2d16ee31e3e3dee", data="NOT IMPLEMENTED" )
      return storage.tuple_to_dict( fake_attachment )

   @classmethod
   @session_required
   def list_messages( cls, folder, timestamp_start, timestamp_end, length=None ):
      msg_id1 = "a6322463ec5e4e4cb65ad88746aa832e"
      msg_ts1 = 1388897380
      
      fake_message1 = message.SyndicateMessageMetadata( id=msg_id1,
                                                         sender_addr="testuser.mail.syndicate.com@example.com",
                                                         receiver_addrs=["nobody1.mail.syndicate1.com@example1.com", "nobody2.mail.syndicate2.com@example2.com"],
                                                         cc_addrs=["nobody3.myvolume.syndicate3.com@example3.com"],
                                                         bcc_addrs=["nobody4.myvolume.syndicate4.com@example4.com"],
                                                         subject="This is a fake message",
                                                         timestamp=msg_ts1,
                                                         handle=message.message_handle( msg_ts1, msg_id1 ),
                                                         has_attachments=True,
                                                         is_read=False )

      msg_id2 = "8011d599ed984edb9115dd71b68402be"
      msg_ts2 = 1388897434
      fake_message2 = message.SyndicateMessageMetadata(  id=msg_id2,
                                                         sender_addr="testuser2.mail.syndicate.com@example.com",
                                                         receiver_addrs=["nobody5.mail.syndicate1.com@example1.com", "nobody6.mail.syndicate2.com@example2.com"],
                                                         cc_addrs=["nobody7.myvolume.syndicate3.com@example3.com"],
                                                         bcc_addrs=["nobody8.myvolume.syndicate4.com@example4.com"],
                                                         subject="This is a fake message, too",
                                                         timestamp=msg_ts2,
                                                         handle=message.message_handle( msg_ts2, msg_id2 ),
                                                         has_attachments=False,
                                                         is_read=True)
      
      listing = [fake_message1, fake_message2]
      if listing != None:
         listing_dicts = [storage.tuple_to_dict( x ) for x in listing]
         return listing_dicts
      else:
         return []

   @classmethod
   @session_required
   def delete_message( cls, folder, msg_handle ):
      # FIXME: implement
      return True

   @classmethod
   @session_required
   def send_message( cls, recipient_addrs, cc_addrs, bcc_addrs, subject, msg_body, attachments={} ):
      # FIXME: implement
      return True
