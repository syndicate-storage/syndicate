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

import urllib2

import contact
import message

STORAGE_DIR = "/tmp"

# -------------------------------------
def download( url ):
   pass

# -------------------------------------
def download_pubkey( url ):
   pass

# -------------------------------------
def download_user_pubkey_from_MS( addr ):
   pass
   
# -------------------------------------
def download_user_pubkey_from_SyndicateMail( addr ):
   pass

# -------------------------------------
def begin_post_message( recipient_addr ):
   # called by the endpoint
   # start to inform the recipient's server that they have a new message
   print "FIXME: stub"
   pass

# -------------------------------------
def end_post_message( recipient_addr ):
   # called by the endpoint
   # process acknowledgement from the recipient's server
   print "FIXME: stub"
   pass

# -------------------------------------
def get_incoming_messages( addr ):
   # called by the endpoint
   # get the list of new messages from the server and store them locally
   pass

# -------------------------------------
def clear_incoming_messages( addr, timestamp ):
   # called by the endpoint
   # tell server that we have all new messages up to a timestamp
   pass
