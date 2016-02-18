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

import webapp2

import MS.handlers
# import openid.gaeopenid

import logging

from MS.user import SyndicateUser
from common.admin_info import *

from MS.handlers import *

VALID_PUNCTUATION = '!"#$%&\'\(\)\*\+,\-.:;<=>?@\[\\\]^_`\{\|\}~'         # missing '/'

handlers = [
    (r'[/]+FILE[/]+(GETCHILD)[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]+([0123456789ABCDEFabcdef]+)[/]+([^/]+)[/]*', MSFileHandler ),                               # GET: for reading file metadata by name
    (r'[/]+FILE[/]+(GETATTR)[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]+([0123456789ABCDEFabcdef]+).([-0123456789]+).([-0123456789]+)[/]*', MSFileHandler ),   # GET: for refreshing file metadata.
    (r'[/]+FILE[/]+(LISTDIR)[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]+([0123456789ABCDEFabcdef]+)[/]*', MSFileHandler ),                                           # GET: for listing file metadata
    (r'[/]+FILE[/]+(FETCHXATTRS)[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]+([0123456789ABCDEFabcdef]+)[/]*', MSFileHandler ),                                       # GET: for getting the set of xattrs.
    (r'[/]+FILE[/]+(VACUUM)[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]+([0123456789ABCDEFabcdef]+)[/]*', MSFileHandler ),
    (r'[/]+FILE[/]+([0123456789]+).([0123456789]+).([0123456789]+)[/]*', MSFileHandler ),                         # POST: for creating, updating, deleting, renaming,
                                                                                  # changing coordinator, setting/deleting/chown-ing/chmod-ing xattrs, and garbage collection
                                                                                  # The specific operation is encoded in the posted data.  This handler dispatches the call to the appropriate objects.
    (r'[/]+VOLUME[/]+([^/]+)[/]*', MSVolumeCertRequestHandler),
    (r'[/]+USER[/]+([^/]+)[/]*', MSUserCertRequestHandler),
    (r'[/]+CERTBUNDLE[/]+([^/]+)[/]+([0123456789]+)[/]*', MSCertBundleRequestHandler),
    (r'[/]+GATEWAY[/]+([^/]+)[/]*', MSGatewayCertRequestHandler),
    (r'[/]+DRIVER[/]+([0123456789ABCDEFabcdef]+)[/]*', MSDriverRequestHandler),
    (r'[/]+API[/]*', MSJSONRPCHandler),
    (r'[/]+PUBKEY[/]*', MSPubkeyHandler)
]

app = webapp2.WSGIApplication( handlers )

def ms_initialize():
   """
   Initialize the Syndicate MS
   """
   admin = SyndicateUser.Read( ADMIN_EMAIL )
   if admin is None:
      admin_key = SyndicateUser.CreateAdmin( ADMIN_EMAIL, ADMIN_ID, ADMIN_PUBLIC_KEY, SYNDICATE_PRIVKEY )
   
   
ms_initialize()
