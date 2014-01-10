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

recipient_addrs = ['bob.mail2.localhost:8080@localhost:33334']

if len(sys.argv) > 1:
   recipient_addrs = sys.argv[1:]

cc_addrs = []
bcc_addrs = []
subject = "Hello at %s" % time.time()
body = "This is a time update.  It is now %s" % time.time()
attachment_names = {}

data_dict = { 
   'id': str(uuid.uuid4()),
   'method': 'send_message',
   'params': {
       'args': [ recipient_addrs, cc_addrs, bcc_addrs, subject, body, attachment_names ],
       'kw' : {},
    },
    'jsonrpc': '1.0'
}

data = json.dumps( data_dict )

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

time.sleep(5.0)

ret = s.recv(16384)

print ">>>>>>>>>>>>>>>>>>>>>>>>>"
print ret
print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

s.close()
