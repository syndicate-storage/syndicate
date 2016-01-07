#!/usr/bin/python

import socket
import time
import sys

hostname = sys.argv[1]
port = int(sys.argv[2] )
filename = sys.argv[3]

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

http_m = ""
http_m += "HEAD %s HTTP/1.0\r\n" % filename
http_m += "Host: t510\r\n"
http_m += "\r\n"

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_m
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_m )

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
