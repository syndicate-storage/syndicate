#!/usr/bin/python

import os
import sys

path = sys.argv[1]

fd = open( path, "r+" )
fd.write(r"goodbye")
fd.close()

