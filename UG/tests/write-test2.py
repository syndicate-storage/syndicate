#!/usr/bin/python

import os
import sys

path = sys.argv[1]

fd = open( path, "r+" )
fd.seek(15)
fd.write(r"goodbye")
fd.close()

