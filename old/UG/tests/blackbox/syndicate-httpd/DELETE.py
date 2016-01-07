#!/usr/bin/python

import socket
import time
import sys
import urllib2

hostname = sys.argv[1]
port = int(sys.argv[2] )
filename = sys.argv[3]

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

http_header = ""
http_header += "DELETE %s HTTP/1.0\r\n" % filename
http_header += "Host: t510\r\n\r\n"

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_header
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_header )

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
