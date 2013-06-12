#!/usr/bin/python

import os
import subprocess
import sys

if __name__ == "__main__":
   
   rc = subprocess.call(["/bin/mount", "-t", "proc", "-o", "remount,rw", "proc", "/proc/sys"])
   sys.exit(rc)
