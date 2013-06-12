#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/SMDS")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}

print "Start Metadata Server"
(read_url,write_url) = server.StartMetadataServer( testuser_auth, 'testmetadataserver' )
print "   Start metadata server with (read=%s, write=%s)" % (read_url,write_url)

for i in xrange(10,15):
   print "Start metadata server 'testmetadataserver%s'" % i
   (read_url,write_url) = server.StartMetadataServer( testuser_auth, "testmetadataserver%s" % i )
   print "   Start metadata server with (read=%s, write=%s)" % (read_url,write_url)


