#!/usr/bin/env python

import webapp2

import MS.handlers
import tests.debughandler
import openid.gaeopenid

import logging

from MS.user import SyndicateUser
from common.msconfig import *

from MS.handlers import MSFileWriteHandler, MSFileReadHandler, MSVolumeRequestHandler, MSCertRequestHandler, MSCertManifestRequestHandler, MSRegisterRequestHandler, MSOpenIDRequestHandler, MSJSONRPCHandler
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
    ('/api', MSJSONRPCHandler)
], debug=True)


def ms_initialize():
   """
   Initialize the Syndicate MS
   """
   admin_key = SyndicateUser.CreateAdmin( ADMIN_EMAIL, ADMIN_OPENID_URL, ADMIN_PUBLIC_KEY )
   
   
ms_initialize()