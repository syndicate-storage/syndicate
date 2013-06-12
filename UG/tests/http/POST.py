#!/usr/bin/python

import socket
import time
import sys
import urllib2
import base64

auth = "jude:sniff"

hostname = sys.argv[1]
port = int(sys.argv[2] )
filename = sys.argv[3]
data = sys.argv[4]
offset = 0

if len(sys.argv) > 5:
   offset = int(sys.argv[5])

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

http_m = ""
http_m += "POST %s HTTP/1.0\r\n" % filename
http_m += "Host: t510\r\n"
http_m += "Authorization: Basic %s\r\n" % base64.b64encode(auth)
http_m += "Content-Length: %s\r\n" % len(data)
http_m += "Content-Type: application/octet-stream\r\n"
http_m += "Content-Range: bytes=%s-%s\r\n" % (offset, offset + len(data) - 1)
http_m += "\r\n";
http_m += data

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_m
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_m )

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
