#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/SMDS")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}

print "Stop Metadata Server"
rc = server.StopMetadataServer( testuser_auth, 'testmetadataserver' )
print "   Stop metadata server with rc = %s" % rc

for i in xrange(10,15):
   print "Stop metadata server 'testmetadataserver%s'" % i
   rc = server.StopMetadataServer( testuser_auth, "testmetadataserver%s" % i )
   print "   Stop metadata server with rc = %s" % rc


