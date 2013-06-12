#!/usr/bin/python

import os
import sys
import socket
import BaseHTTPServer
import SocketServer
import urlparse
import subprocess
import cgi
import time

PORT = 40000
HOSTINFO="/tmp/experiments/"


class ExperimentHandler( BaseHTTPServer.BaseHTTPRequestHandler ):

   def do_GET(self):
      global HOSTINFO

      if self.path == "/":
         self.send_response(200)
         self.end_headers()
         self.wfile.write("<table border=\"1\">")
         names = os.listdir(HOSTINFO)
         my_hostname = socket.gethostname()
         my_portnum = self.server.server_address[1]
         for name in names:
            last_line_subproc = subprocess.Popen("tail -n 1 %s" % (os.path.join(HOSTINFO, name)), shell=True, stdout=subprocess.PIPE)
            last_line = last_line_subproc.stdout.read().strip()
            last_line_subproc.wait()

            self.wfile.write("<tr><td><a href=\"http://%s:%d/%s\">%s</a></td><td>%s</td></tr>" % (my_hostname, my_portnum, name, name, last_line))

         self.wfile.write("</table><br><br>")
         return

      else:
         hostname = os.path.basename(self.path)
         if not os.path.exists( os.path.join(HOSTINFO,hostname)):
            self.send_response(404)
            self.end_headers()
            self.wfile.write("<h1>Host '%s' has not reported data</h1>" % hostname)
            return

         else:
            fd = open( os.path.join(HOSTINFO, hostname), "r" )
            self.send_response(200)
            self.end_headers()
            self.wfile.write("<h1>Tail of '%s'</h1><br>" % hostname)
            
            last_100_lines_subproc = subprocess.Popen("tail -n 100 %s" % (os.path.join(HOSTINFO, hostname)), shell=True, stdout=subprocess.PIPE)
            last_100_lines = last_100_lines_subproc.stdout.read()
            last_100_lines_subproc.wait()

            lines = last_100_lines.split("\n")
            for line in lines:
               self.wfile.write(line + "<br>")

            self.wfile.write("<br><br>")

      return

   def do_POST(self):
      global HOSTINFO

      # get the parameters
      content_len = int(self.headers.getheader("Content-Length"))
      encoded_data = self.rfile.read( content_len )
      data = cgi.parse_qs(encoded_data)

      print data

      # record the data for this host
      hostfile_path = os.path.join( HOSTINFO, data['host'][0] )
      hostfile = open( hostfile_path, "a" )
      hostfile.write( "[%s] %s: %s\n" % (data['date'][0], data['loglevel'][0], data['msg'][0]) )
      hostfile.close()

      self.end_headers()
      self.send_response(200)

      return



class ThreadedHTTPServer( SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer ):
   pass


if __name__ == "__main__":
   if not os.path.exists( HOSTINFO ):
      os.mkdir( HOSTINFO )

   httpd = ThreadedHTTPServer( ('', PORT), ExperimentHandler )
   while True:
      httpd.handle_request()
