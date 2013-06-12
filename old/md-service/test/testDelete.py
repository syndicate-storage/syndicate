#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/SMDS")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}

print "Delete metadata server testmetadataserver14"
testuser2 = server.GetUsers( testuser_auth, {'username':'testuser'} )
print "   BEFORE: testuser = %s" % testuser2
rc = server.DeleteMetadataServer( maint_auth, 'testmetadataserver14' )
print "   rc = %s" % rc
testuser2 = server.GetUsers( testuser_auth, {'username':'testuser'} )
print "   AFTER: testuser = %s" % testuser2

for i in xrange(10,15):
   print "Delete user testuser%s" % i
   rc = server.DeleteUser( {"AuthMethod":"password", "Username":"testuser%s" % i, "AuthString" : str(i) }, 'testuser%s' % i)
   print "   rc = %s" % rc

print "Delete user 'testuser2'"
mdserver13 = server.GetMetadataServers( maint_auth, {'name':'testmetadataserver13'}, [] )
print "   BEFORE: testmetadataserver13 = %s" % mdserver13
rc = server.DeleteUser( maint_auth, "testuser2" )
print "   rc = %s" % rc
mdserver13 = server.GetMetadataServers( maint_auth, {'name':'testmetadataserver13'}, [] )
print "   AFTER: testmetadataserver13 = %s" % mdserver13

print "Delete user 'testuser'"
rc = server.DeleteUser( maint_auth, "testuser" )
print "   rc = %s" % rc
mdservers = server.GetMetadataServers( maint_auth, {}, [] )
print "   remaining metadata servers = %s" % mdservers
contents = server.GetContentServers( maint_auth, {}, [] )
print "   remaining content servers = %s" % contents

