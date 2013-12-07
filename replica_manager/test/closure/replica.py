#!/usr/bin/env python 

CONFIG = {'foo': 'bar', 'STORAGE_DIR': '/tmp/'}

def replica_read( drivers, request_info, filename, outfile ):
   print ""
   print "replica_read called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "outfile = " + str(outfile)
   print ""
   
   rc = drivers['sd_test'].read_file( filename, outfile, extra_param="Foo", **CONFIG )
   
   return rc
   
def replica_write( drivers, request_info, filename, infile ):
   print ""
   print "replica_write called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "infile = " + str(infile)
   print ""
   
   rc = drivers['sd_test'].write_file( filename, infile, extra_param="Foo", **CONFIG )
   
   return rc

def replica_delete( drivers, request_info, filename ):
   print ""
   print "replica_delete called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print ""
   
   rc = drivers['sd_test'].delete_file( filename, extra_param="Foo", **CONFIG )
   
   return rc
