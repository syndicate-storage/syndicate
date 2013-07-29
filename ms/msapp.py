#!/usr/bin/env python

import webapp2

import MS.handlers
import tests.debughandler
import openid.gaeopenid

from MS.handlers import MSFileRequestHandler, MSVolumeRequestHandler, MSUGRequestHandler, MSRGRequestHandler
from tests.debughandler import MSDebugHandler
from openid.gaeopenid import OpenIDRequestHandler

app = webapp2.WSGIApplication([
    ('/', OpenIDRequestHandler),
    ('/verify', OpenIDRequestHandler),
    ('/process', OpenIDRequestHandler),
    ('/affiliate', OpenIDRequestHandler),
    ('/debug/([^/]+)/(.*)', MSDebugHandler),
    ('/FILE/([0123456789]+)/(.*)', MSFileRequestHandler),
    ('/VOLUME/([^/]+)', MSVolumeRequestHandler),
    ('/UG/([0123456789]+)', MSUGRequestHandler),
    ('/RG/([0123456789]+)', MSRGRequestHandler)
], debug=True)

