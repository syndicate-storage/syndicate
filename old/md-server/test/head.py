#!/usr/bin/python

import socket
import time

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( ("node40.gt.vicci.org", 8008) )

http_header = "HEAD /www.cs.princeton.edu/~jcnelson/index.html HTTP/1.0\r\nHost: node40.gt.vicci.org\r\n\r\n"

http_m = http_header

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_m
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_m )
   
time.sleep(1.0)

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
