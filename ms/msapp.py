#!/usr/bin/env python

import webapp2

import MS.handlers
import tests.debughandler
import openid.gaeopenid

from MS.handlers import MSFileWriteHandler, MSFileReadHandler, MSVolumeRequestHandler, MSCertRequestHandler, MSCertManifestRequestHandler, MSRegisterRequestHandler, MSOpenIDRequestHandler
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
    ('/CERT/([0123456789]+)/([0123456789]+)/(UG|RG|AG)/([0123456789]+)/([0123456789]+)', MSCertRequestHandler)
], debug=True)

