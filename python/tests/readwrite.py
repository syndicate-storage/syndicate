#!/usr/bin/python

from syndicate.volume import Volume
import os
import shutil

from settings import settings_kw

vol = Volume( **settings_kw )

def whitespace():
   for i in xrange(0, 5):
      print ""

def put_file( vol, path, mode, data ):
   fd = vol.create( path, mode )
   assert fd != None, "failed to create %s" % path
   vol.write( fd, data )
   vol.close( fd )

def get_file( vol, path ):
   statbuf = vol.stat( path )
   size = statbuf.st_size

   # we only try small files here...
   assert size >= 0 and size < 1024, "Unreasonable size %s" % size

   fd = vol.open( path, os.O_RDONLY )
   assert fd != None, "failed to open %s for reading" % path
   
   buf = vol.read( fd, size )
   vol.close( fd )

   return buf

whitespace()
print "---- put files ----"

txt1 = "foocorp"
txt2 = "This is a longer sentence.  Let's hope this works."

whitespace()
put_file( vol, "/foo", 0700, txt1 )

whitespace()
put_file( vol, "/bar", 0700, txt2 )

whitespace()
print "---- get files ----"

whitespace()
dat1 = get_file( vol, "/foo" )
assert dat1 == txt1, "Expected '%s', got '%s'" % (txt1, dat1)

whitespace()
dat2 = get_file( vol, "/bar" )
assert dat2 == txt2, "Expected '%s', got '%s'" % (txt2, dat2)

whitespace()
print "---- remove files ----"

whitespace()
assert vol.unlink( "/foo" ) == 0, "unlink /foo failed"

whitespace()
assert vol.unlink( "/bar" ) == 0, "unlink /bar failed"

