#!/usr/bin/python

import os
import sys
import xmlrpclib

sys.path.append("/usr/share/SMDS")

import SMDS

server = xmlrpclib.Server("http://localhost:8888/RPC2", allow_none=True)

maint_auth={"AuthMethod":"password","Username":"maint","AuthString":"maint"}
testuser_auth={"AuthMethod":"password","Username":"testuser","AuthString":"w00t"}
testuser2_auth={"AuthMethod":"password","Username":"testuser2","AuthString":"w00t"}

print "Get user 'testuser'"
testuser = server.GetUsers( testuser_auth, {'username':'testuser'}, [] )
print "  testuser = %s" % testuser

print "Get user 'testuser2'"
testuser2 = server.GetUsers( testuser_auth, {'username':'testuser2'}, [] )
print "  testuser2 = %s" % testuser2

print "Get all users"
allusers = server.GetUsers( testuser_auth, {}, [] )
print "  all users = %s" % allusers

print "Get testuser's metadata servers"
testuser_metadata_servers = server.GetMetadataServers( testuser_auth, {}, [] )
print "  testuser's metadata servers = %s" % testuser_metadata_servers

print "Get testuser2's metadata servers"
testuser2_metadata_servers = server.GetMetadataServers( testuser2_auth, {}, [] )
print "  testuser2's metadata servers = %s" % testuser2_metadata_servers

print "Get testmetadataserver"
testmetadataserver = server.GetMetadataServers( testuser_auth, {'name':'testmetadataserver'}, [] )
print "  testmetadataserver = %s" % testmetadataserver

print "Get all metadata servers"
all_metadata_servers = server.GetMetadataServers( maint_auth, {}, [] )
print "  all metadata servers = %s" % all_metadata_servers

print "Get testuser's content servers"
testuser_content_servers = server.GetContentServers( testuser_auth, {}, [] )
print "  testuser's content servers = %s" % testuser_content_servers

print "Get testuser2's content servers"
testuser2_content_servers = server.GetContentServers( testuser2_auth, {}, [] )
print "  testuser2's content servers = %s" % testuser2_content_servers

print "Get all content servers"
all_content_servers = server.GetContentServers( maint_auth, {}, [] )
print "  all content servers = %s" % all_content_servers


