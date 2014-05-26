#!/usr/bin/python

import socket
import time
import sys
import urllib2
import os
import base64

auth = "jude:sniff"

hostname = sys.argv[1]
port = int(sys.argv[2] )
filename = sys.argv[3]
data_fd = None
data_path = None
if len(sys.argv) > 4:
   data_path = sys.argv[4]
   data_fd = open( data_path, "r" )

mode = '0644'
if filename[-1] == '/':
   mode = '0755'

size = 0
if data_fd != None:
   size = os.stat(data_path).st_size

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

boundary = "AaBbCcDdEe"

http_header = ""
http_header += "PUT %s HTTP/1.0\r\n" % filename
http_header += "Host: t510\r\n"
http_header += "Content-Type: application/octet-stream\r\n"
http_header += "Content-Length: %s\r\n" % size
http_header += "Authorization: Basic %s\r\n" % base64.b64encode(auth)
http_header += "X-POSIX-mode: %s\r\n" % mode
http_header += "\r\n"

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_header
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_header )
while data_fd != None:
   buf = data_fd.read(32768)
   if len(buf) == 0:
      break
   s.send( buf )
   
   
ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
