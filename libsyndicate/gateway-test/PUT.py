#!/usr/bin/python

import socket
import time
import sys
import urllib2

hostname = sys.argv[1]
port = int(sys.argv[2] )
filename = sys.argv[3]
data = ""

mode = '0644'
if filename[-1] == '/':
   mode = '0755'

if len(sys.argv) > 4:
   data = sys.argv[4]

offset = 0

if len(sys.argv) > 5:
   offset = int(sys.argv[4])

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

boundary = "AaBbCcDdEe"

http_header = ""
http_header += "PUT %s HTTP/1.0\r\n" % filename
http_header += "Host: t510\r\n"
http_header += "Content-Type: application/octet-stream\r\n"
http_header += "Content-Length: %s\r\n" % len(data)
http_header += "X-POSIX-mode: %s\r\n" % mode
http_header += "\r\n"
http_header += data

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_header
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_header )
   
time.sleep(1.0)

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
