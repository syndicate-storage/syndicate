#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/SMDS")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}

print "Add user 'testuser'"
uid = server.AddUser( maint_auth, {'username': "testuser", "password": "w00t", "email": "testuser@mydomain.com"} )
print "   Added user %s" % uid

print "Add user 'testuser2'"
uid2 = server.AddUser( maint_auth, {'username': "testuser2", "password": "w00t", "email": "testuser2@mydomain.com"} )
print "   Added user %s" % uid2

print "Enable user 'testuser'"
rc = server.UpdateUser( maint_auth, "testuser", {"enabled":True} )
print "   rc = %s" % rc

print "Enable user 'testuser2'"
rc = server.UpdateUser( maint_auth, uid2, {'enabled':True} )
print "   rc = %s" % rc

print "Add Metadata Server"
server_id = server.AddMetadataServer( testuser_auth, {'portnum':30000,'name':'testmetadataserver'} )
print "   Added metadata server %s" % server_id

print "Add Content Server"
content_id = server.AddContentServer( testuser_auth, {'host_url':'testuser.amazonaws.com'} )
print "   Added content server %s" % content_id

print "Add user %s to metadata server %s" % (uid2, server_id)
rc = server.AddUserToMetadataServer( testuser_auth, "testuser2", server_id )
print "   rc = %s" % rc

for i in xrange(10,15):
   print "Add user 'testuser%s'" % i
   uid = server.AddUser( maint_auth, {'username': "testuser%s" % i, "password": str(i), "email": "testuser%s@mydomain.com" % i} )
   rc = server.UpdateUser( maint_auth, "testuser%s" % i, {'enabled':True})
   print "   Added user %s" % uid

   print "Add metadata server 'testmetadataserver%s'" % i
   server_id = server.AddMetadataServer( testuser_auth, {'portnum': 30000 + i + 1, 'name':'testmetadataserver%s' % i} )
   print "   Added metadata server %s" % server_id


print "Add user %s to testmetadataserver13" % (uid2)
rc = server.AddUserToMetadataServer( testuser_auth, "testuser2", "testmetadataserver13" )
print "   rc = %s" % rc

print "Add user %s to testmetadataserver14" % (uid2)
rc = server.AddUserToMetadataServer( testuser_auth, "testuser2", 'testmetadataserver14' )
print "   rc = %s" % rc
