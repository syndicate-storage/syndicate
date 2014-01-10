#!/usr/bin/python

import socket
import time
import sys
import urllib2
import base64
import uuid
import json
import time

hostname = "localhost"
port = 33333

now = int(time.time())

folder = "Inbox"

if len(sys.argv) > 1:
   folder = sys.argv[1]

data_dict = { 
   'id': str(uuid.uuid4()),
   'method': 'list_messages',
   'params': {
       'args': [ folder, 1388897379, now ],
       'kw' : { "length": 2 },
    },
    'jsonrpc': '1.0'
}

data = json.dumps( data_dict )

print data 

s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
s.connect( (hostname, port) )

http_m = ""
http_m += "POST /api HTTP/1.0\r\n"
http_m += "Host: t510\r\n"
http_m += "Content-Length: %s\r\n" % len(data)
http_m += "Content-Type: application/json\r\n"
http_m += "\r\n";
http_m += data

print "<<<<<<<<<<<<<<<<<<<<<<<<<"
print http_m
print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

s.send( http_m )

time.sleep(2.0)

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
