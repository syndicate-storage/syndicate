#!/usr/bin/python

import os
import sys

path = sys.argv[1]
fd = open( path, "r+" )

for i in xrange(0,5):
   fd.seek( 10 - i*2 )
   fd.write(r"goodbye")

fd.close()

