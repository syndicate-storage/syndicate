#!/usr/bin/python

import os
import sys

CONF_INPUT = "input.conf"
OUT_FILE_FMT = "write_file.conf.%s"

PORTNUM_BASE=32780
HTTP_PORTNUM_BASE=44444

if __name__ == "__main__":
   portnum = PORTNUM_BASE
   httpd_portnum = HTTP_PORTNUM_BASE

   try:
      num_confs = int( sys.argv[1] )
   except:
      print >> sys.stderr, "Usage: %s NUM_CONFS" % (sys.argv[0])

   conf_fd = open(CONF_INPUT, "r" )
   conf_buf = conf_fd.read()
   conf_fd.close()

   for i in xrange(0,num_confs):
      out_file = OUT_FILE_FMT % (i)
      conf_i = conf_buf % locals()

      fd = open(out_file, "w")
      fd.write( conf_i )
      fd.close()

      portnum += 1
      httpd_portnum += 1

