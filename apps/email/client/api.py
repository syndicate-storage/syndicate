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

# top-level API, to be called by the Javascript front-end

import common.contact as contact
import common.message as message

# ------------------------------------------------
def read_contact( email_addr ):
   pass

def list_contacts( start_idx=None, end_idx=None ):
   pass

def add_contact( email_addr, pubkey_pem, **fields ):
   pass

def delete_contact( email_addr ):
   pass


# ------------------------------------------------
def read_message( folder, msg_handle ):
   pass

def read_attachment( folder, msg_handle, attachment_name ):
   pass

def list_messages( folder, timestamp_start, timestamp_end, length=None ):
   pass

def delete_message( folder, msg_handle ):
   pass

def send_message( recipient_addrs, msg_body, attachments=None ):
   pass