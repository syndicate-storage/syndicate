#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/syndicate_md")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}
testuser2_auth={"AuthMethod":"password","Username":"testuser2","AuthString":"w00t"}

print "Update 'testuser'"
testuser = server.GetUsers( testuser_auth, {'username':'testuser'}, [] )
print "  BEFORE: testuser = %s" % testuser
rc = server.UpdateUser( testuser_auth, 'testuser', {'email':'testuserUPDATED@mydomain.com'} )
testuser = server.GetUsers( testuser_auth, {'username':'testuser'}, [] )
print "  AFTER: testuser = %s" % testuser

print "Update 'testuser2'"
testuser2 = server.GetUsers( testuser2_auth, {'username':'testuser2'}, [] )
print "  BEFORE: testuser2 = %s" % testuser2
rc = server.UpdateUser( maint_auth, 'testuser2', {'enabled':False, 'roles':['UPDATED','user'], 'max_contents': 1000000, 'max_mdservers': 200000} )
testuser2 = server.GetUsers( maint_auth, {'username':'testuser2'}, [] )
print "  AFTER: testuser2 = %s" % testuser2

print "Update metadata server"
testmetadataserver = server.GetMetadataServers( testuser_auth, {'name':'testmetadataserver'}, [] )
print "  BEFORE: testmetadataserver = %s" % testmetadataserver
rc = server.UpdateMetadataServer( testuser_auth, 'testmetadataserver', {'auth_write':False, 'portnum':33333} )
testmetadataserver = server.GetMetadataServers( testuser_auth, {'name':'testmetadataserver'}, [] )
print "  AFTER: testmetadataserver = %s" % testmetadataserver


