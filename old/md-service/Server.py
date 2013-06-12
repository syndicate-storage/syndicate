#!/usr/bin/python
#
# Simple standalone HTTP server for testing MDAPI
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# Modifications by Jude Nelson
# $Id$
#

import os
import sys
import getopt
import traceback
import BaseHTTPServer

sys.path.append("/usr/share/SMDS")

from SMDS.mdapi import MDAPI
import SMDS.logger as logger

class MDAPIRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    Simple standalone HTTP request handler for testing MDAPI.
    """

    def do_POST(self):
        try:
            # Read request
            request = self.rfile.read(int(self.headers["Content-length"]))

            # Handle request
            response = self.server.api.handle(self.client_address, request)

            # Write response
            self.send_response(200)
            self.send_header("Content-type", "text/xml")
            self.send_header("Content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

            self.wfile.flush()
            self.connection.shutdown(1)

        except Exception, e:
            # Log error
            sys.stderr.write(traceback.format_exc())
            sys.stderr.flush()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", 'text/html')
        self.end_headers()
        self.wfile.write("""
<html><head>
<title>MDAPI XML-RPC/SOAP Interface</title>
</head><body>
<h1>MDAPI XML-RPC/SOAP Interface</h1>
<p>Please use XML-RPC or SOAP to access the MDAPI.</p>
</body></html>
""")

class MDAPIServer(BaseHTTPServer.HTTPServer):
    """
    Simple standalone HTTP server for testing MDAPI.
    """

    def __init__(self, addr, config):
        self.api = MDAPI(config)
        self.allow_reuse_address = 1
        BaseHTTPServer.HTTPServer.__init__(self, addr, MDAPIRequestHandler)


# Defaults
addr = "localhost"
port = 8888
config = "/etc/syndicate/syndicate-metadata-service.conf"

def usage():
    print "Usage: %s [OPTION]..." % sys.argv[0]
    print "Options:"
    print "     -p PORT, --port=PORT    TCP port number to listen on (default: %d)" % port
    print "     -F FILE, --cconfig=FILE COB configuration file (default: %s)" % config
    print "     -h, --help              This message"
    sys.exit(1)

logger.init()

# Get options
try:
    (opts, argv) = getopt.getopt(sys.argv[1:], "p:f:F:h", ["port=", "pconfig=", "config=", "help"])
except getopt.GetoptError, err:
    print "Error: " + err.msg
    usage()

for (opt, optval) in opts:
    if opt == "-p" or opt == "--port":
        try:
            port = int(optval)
        except ValueError:
            usage()
    elif opt == "-f" or opt == "--config":
        cob_config = optval
    elif opt == "-h" or opt == "--help":
        usage()

# Start server
MDAPIServer((addr, port), config).serve_forever()
