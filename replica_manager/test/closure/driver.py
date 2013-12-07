#!/usr/bin/env python 

def read_file( filename, outfile, **kw ):
   import traceback

   print ""
   print "  read_file called!"
   print "  filename = " + str(filename)
   print "  outfile = " + str(outfile)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "r" )
      outfile.write( fd.read() )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def write_file( filename, infile, **kw ):
   import traceback

   print ""
   print "  write_file called!"
   print "  filename = " + str(filename)
   print "  infile = " + str(infile)
   print "  kw = " + str(kw)
   
   buf = infile.read()
   
   print "  Got data: '" + str(buf) + "'"
   
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "w" )
      fd.write( buf )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def delete_file( filename, **kw ):
   import traceback
   import os

   print ""
   print "  delete_file called!"
   print "  filename = " + str(filename)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      os.unlink( STORAGE_DIR + filename )
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200
