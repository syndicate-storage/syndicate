#!/usr/bin/python

import httplib
import BaseHTTPServer
import os
import sys

data = "%2findex.html http%3a%2f%2fs3.amazon.com%2fhome%2fjcnelson%2findex.html http%3a%2f%2fvcoblitz-cmi.cs.princeton.edu%2fbackups%2fhome%2fjcnelson%2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html 4294967295 644 14441 1329026453 b72ff11f6af13b6db07942c81ca99e942fc3ab99"

class ProxyRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
   def do_GET(s):
      global data
      s.send_response(200)
      s.send_header("Content-type", "text/plain")
      s.end_headers()
      s.wfile.write(data)
      return


if __name__ == "__main__":
   server_class = BaseHTTPServer.HTTPServer
   httpd = server_class( ("localhost", int(sys.argv[1])), ProxyRequestHandler )
   httpd.serve_forever()

