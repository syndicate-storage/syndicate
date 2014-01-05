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
import tests.debughandler
import openid.gaeopenid

import logging

from MS.user import SyndicateUser
from common.admin_info import *

from MS.handlers import MSFileWriteHandler, MSFileReadHandler, MSVolumeRequestHandler, MSCertRequestHandler, MSCertManifestRequestHandler, MSRegisterRequestHandler, MSOpenIDRequestHandler, MSJSONRPCHandler, MSUserRequestHandler
from tests.debughandler import MSDebugHandler

app = webapp2.WSGIApplication([
    ('/', MSOpenIDRequestHandler),
    ('/verify', MSOpenIDRequestHandler),
    ('/process', MSOpenIDRequestHandler),
    ('/affiliate', MSOpenIDRequestHandler),
    ('/debug/([^/]+)/(.*)', MSDebugHandler),
    ('/FILE/([0123456789]+)/([0123456789ABCDEF]+)/([0123456789]+)/([-0123456789]+)', MSFileReadHandler),
    ('/FILE/([0123456789]+)', MSFileWriteHandler ),
    ('/VOLUME/([0123456789]+)', MSVolumeRequestHandler),
    ('/REGISTER/([^/]+)/([^/]+)/([^/]+)/([^/]+)', MSRegisterRequestHandler),
    ('/CERT/([0123456789]+)/manifest.([0123456789]+)', MSCertManifestRequestHandler),
    ('/CERT/([0123456789]+)/([0123456789]+)/(UG|RG|AG)/([0123456789]+)/([0123456789]+)', MSCertRequestHandler),
    ('/USER/([^/]+)', MSUserRequestHandler),
    ('/api', MSJSONRPCHandler)
], debug=True)


def ms_initialize():
   """
   Initialize the Syndicate MS
   """
   admin_key = SyndicateUser.CreateAdmin( ADMIN_EMAIL, ADMIN_OPENID_URL, ADMIN_PUBLIC_KEY )
   
   
ms_initialize()
