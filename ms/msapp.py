#!/usr/bin/env python

import webapp2

import MS.handlers
import tests.debughandler
import openid.gaeopenid

from MS.handlers import MSFileRequestHandler, MSVolumeRequestHandler, MSUGRequestHandler, MSListUGRequestHandler, MSRGRequestHandler, MSRegisterRequestHandler, MSOpenIDRequestHandler
from tests.debughandler import MSDebugHandler

app = webapp2.WSGIApplication([
    ('/', MSOpenIDRequestHandler),
    ('/verify', MSOpenIDRequestHandler),
    ('/process', MSOpenIDRequestHandler),
    ('/affiliate', MSOpenIDRequestHandler),
    ('/debug/([^/]+)/(.*)', MSDebugHandler),
    ('/FILE/([0123456789]+)/(.*)', MSFileRequestHandler),
    ('/VOLUME/([0123456789]+)', MSVolumeRequestHandler),
    ('/UG/([0123456789]+)', MSListUGRequestHandler),
    ('/UG/([0123456789]+)/([0123456789]+)', MSUGRequestHandler),
    ('/RG/([0123456789]+)', MSRGRequestHandler),
    ('/REGISTER/([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^/]+)', MSRegisterRequestHandler)
], debug=True)

