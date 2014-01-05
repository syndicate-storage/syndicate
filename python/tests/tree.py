#!/usr/bin/python

from syndicate.volume import Volume
import os
import shutil

from settings import settings_kw

vol = Volume( **settings_kw )

def touch( vol, path, mode ):
   fd = vol.create( path, mode )
   assert fd != None, "create %s failed" % path
   vol.close( fd )
   return 0

def whitespace():
   for i in xrange(0, 5):
      print ""


whitespace()
print "---- make some directories ----"
whitespace()
assert vol.mkdir( "/dir1", 0755 ) == 0, "mkdir /dir1 failed"
whitespace()
assert vol.mkdir( "/dir1/dir2", 0755 ) == 0, "mkdir /dir1/dir2 failed"
whitespace()
assert vol.mkdir( "/dir3", 0755 ) == 0, "mkdir /dir3 failed"

whitespace()
print "---- create some files -----"

whitespace()
assert touch( vol, "/foo", 0700 ) == 0, "create /foo failed"
whitespace()
assert touch( vol, "/bar", 0770 ) == 0, "create /bar failed"
whitespace()
assert touch( vol, "/baz", 0777 ) == 0, "create /baz failed"

whitespace()
assert touch( vol, "/dir1/foo2", 0700 ) == 0, "create /dir1/foo2 failed"
whitespace()
assert touch( vol, "/dir1/dir2/bar3", 0644 ) == 0, "create /dir1/dir2/bar3 failed"
whitespace()
assert touch( vol, "/dir3/baz4", 0644 ) == 0, "create /dir3/baz4 failed"

whitespace()
print "---- list directories -----"

whitespace()
print "list /"

whitespace()
dfd = vol.opendir("/")

whitespace()
print vol.readdir( dfd )

whitespace()
vol.closedir( dfd )

whitespace()
print "list /dir1"

whitespace()
dfd = vol.opendir("/dir1")

whitespace()
print vol.readdir( dfd )

whitespace()
vol.closedir( dfd )

whitespace()
print "list /dir1/dir2"

whitespace()
dfd = vol.opendir( "/dir1/dir2" )

whitespace()
print vol.readdir( dfd )

whitespace()
vol.closedir( dfd )

whitespace()
print "list /dir3"

whitespace()
dfd = vol.opendir( "/dir3" )

whitespace()
print vol.readdir( dfd )

whitespace()
vol.closedir( dfd )

whitespace()
print "---- remove files ----"

whitespace()
assert vol.unlink( "/foo" ) == 0, "unlink /foo failed"

whitespace()
assert vol.unlink( "/bar" ) == 0, "unlink /bar failed"

whitespace()
assert vol.unlink( "/baz" ) == 0, "unlink /baz failed"

whitespace()
assert vol.unlink( "/dir1/foo2" ) == 0, "unlink /dir1/foo2 failed"

whitespace()
assert vol.unlink( "/dir1/dir2/bar3" ) == 0, "unlink /dir1/dir2/bar3 failed"

whitespace()
assert vol.unlink( "/dir3/baz4" ) == 0, "unlink /dir3/baz4 failed"

whitespace()
print "---- remove dirs ----"

whitespace()
assert vol.rmdir( "/dir1/dir2" ) == 0, "rmdir /dir1/dir2 failed"

whitespace()
assert vol.rmdir( "/dir1" ) == 0, "rmdir /dir1 failed"

whitespace()
assert vol.rmdir( "/dir3" ) == 0, "rmdir /dir3 failed"

