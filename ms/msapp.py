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
import openid.gaeopenid

import logging

from MS.user import SyndicateUser
from common.admin_info import *

from MS.handlers import *

have_debug = False

try:
   from tests.debughandler import MSDebugHandler
   have_debug = True
except:
   pass

handlers = [
    ('[/]+FILE[/]+(RESOLVE)[/]+([0123456789]+)[/]+([0123456789ABCDEF]+)[/]+([0123456789]+)[/]+([-0123456789]+)[/]*', MSFileHandler ),   # GET: for reading/resolving file metadata.
    ('[/]+FILE[/]+(GETXATTR)[/]+([0123456789]+)[/]+([0123456789ABCDEF]+)[/]+([a-zA-Z0-9_.]+)[/]*', MSFileHandler),                                 # GET: for getting xattrs.
    ('[/]+FILE[/]+(LISTXATTR)[/]+([0123456789]+)[/]+([0123456789ABCDEF]+)[/]*', MSFileHandler ),                                        # GET: for listing xattrs.
    ('[/]+FILE[/]+([0123456789]+)[/]*', MSFileHandler ),                          # POST: for creating, updating, deleting, renaming, changing coordinator, setting, and deleting xattrs.
                                                                                  # The specific operation is encoded in the posted data.  This handler dispatches the call to the appropriate objects.
    ('[/]+VOLUME[/]+([^/]+)[/]*', MSVolumeRequestHandler),
    ('[/]+REGISTER[/]*', MSPublicKeyRegisterRequestHandler),
    ('[/]+REGISTER[/]+([^/]+)[/]+([^/]+)[/]+([^/]+)[/]+([^/]+)[/]*', MSOpenIDRegisterRequestHandler),
    ('[/]+CERT[/]+([0123456789]+)[/]+manifest.([0123456789]+)[/]*', MSCertManifestRequestHandler),
    ('[/]+CERT[/]+([0123456789]+)[/]+([0123456789]+)[/]+(UG|RG|AG)[/]+([0123456789]+)[/]+([0123456789]+)[/]*', MSCertRequestHandler),
    ('[/]+USER[/]+([^/]+)[/]*', MSUserRequestHandler),
    ('[/]+VOLUMEOWNER[/]+([^/]+)[/]*', MSVolumeOwnerRequestHandler),
    ('[/]+API[/]+([^/]+)[/]*', MSJSONRPCHandler),
    ('[/]+API[/]*', MSJSONRPCHandler),
    ('[/]+PUBKEY[/]*', MSPubkeyHandler)
]

if have_debug:
   handlers.append(('/debug/([^/]+)/(.*)', MSDebugHandler))

app = webapp2.WSGIApplication( handlers, debug=have_debug )

def ms_initialize():
   """
   Initialize the Syndicate MS
   """
   admin_key = SyndicateUser.CreateAdmin( ADMIN_EMAIL, ADMIN_OPENID_URL, ADMIN_PUBLIC_KEY, ADMIN_REGISTER_PASSWORD )
   
   
ms_initialize()
