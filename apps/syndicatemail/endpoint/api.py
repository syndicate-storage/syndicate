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

import common.contact as contact
import common.message as message

class API( object ):
   
   config = None
   
   @classmethod
   def setup( cls, conf ):
      cls.config = conf
   
   # ------------------------------------------------
   @classmethod
   def read_contact( cls, email_addr ):
      return "NOT IMPLEMENTED"

   @classmethod
   def list_contacts( cls, start_idx=None, end_idx=None ):
      return "NOT IMPLEMENTED"
   
   @classmethod
   def add_contact( cls, email_addr, pubkey_pem, **fields ):
      return "NOT IMPLEMENTED"

   @classmethod
   def delete_contact( cls, email_addr ):
      return "NOT IMPLEMENTED"

   # ------------------------------------------------
   @classmethod
   def login( email, password ):
      return "NOT IMPLEMENTED"
   
   @classmethod
   def logout( email, password ):
      return "NOT IMPLEMENTED"

   # ------------------------------------------------
   @classmethod
   def read_message( folder, msg_handle ):
      return "NOT IMPLEMENTED"

   @classmethod
   def read_attachment( folder, msg_handle, attachment_name ):
      return "NOT IMPLEMENTED"

   @classmethod
   def list_messages( folder, timestamp_start, timestamp_end, length=None ):
      return "NOT IMPLEMENTED"

   @classmethod
   def delete_message( folder, msg_handle ):
      return "NOT IMPLEMENTED"

   @classmethod
   def send_message( recipient_addrs, msg_body, attachments=None ):
      return "NOT IMPLEMENTED"
