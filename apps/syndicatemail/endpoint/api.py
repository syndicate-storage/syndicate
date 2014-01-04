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
   def publicKeyStr():
      return cls.key_str( cls.config['pubkey'] ) if (cls.config.has_key('pubkey') and cls.config['pubkey'] is not None) else None
   
   @classmethod
   def privateKeyStr():
      return cls.key_str( cls.config['privkey'] ) if (cls.config.has_key('privkey') and cls.config['privkey'] is not None) else None
   
   
   # ------------------------------------------------
   @classmethod
   def hello_world( cls, name, question="How are you?" ):
      return "Hello, %s.  %s" % (name, question)
   
   # ------------------------------------------------
   @classmethod
   @session_required
   def read_contact( cls, email_addr ):
      return contact.read_contact( cls.privateKeyStr(), email_addr )

   @classmethod
   @session_required
   def list_contacts( cls, start_idx=None, end_idx=None ):
      return contact.list_contacts( cls.publicKeyStr(), cls.privateKeyStr(), start_idx=start_idx, end_idx=end_idx )
   
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
   def read_message( folder, msg_handle ):
      return "NOT IMPLEMENTED"

   @classmethod
   @session_required
   def read_attachment( folder, msg_handle, attachment_name ):
      return "NOT IMPLEMENTED"

   @classmethod
   @session_required
   def list_messages( folder, timestamp_start, timestamp_end, length=None ):
      return "NOT IMPLEMENTED"

   @classmethod
   @session_required
   def delete_message( folder, msg_handle ):
      return "NOT IMPLEMENTED"

   @classmethod
   @session_required
   def send_message( recipient_addrs, msg_body, attachments=None ):
      return "NOT IMPLEMENTED"
