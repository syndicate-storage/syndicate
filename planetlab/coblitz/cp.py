#!/usr/bin/python

import os
import sys

fd1 = open(sys.argv[1], "r")
fd2 = open(sys.argv[2], "w")

while True:
   buf = fd1.read(65536)
   if len(buf) == 0:
      break

   fd2.write( buf )

sys.exit(0)
