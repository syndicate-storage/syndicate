#!/usr/bin/python

import os
import time
import sys

os.system("sudo yum -y install python-dateutil")
os.system("curl http://vcoblitz-cmi.cs.princeton.edu/tools/experiments/experimentd.py > /tmp/experimentd.py")
os.system("curl http://vcoblitz-cmi.cs.princeton.edu/tools/experiments/experimentd > /tmp/experimentd")
os.system("chmod +x /tmp/experimentd.py")
os.system("chmod +x /tmp/experimentd")
os.system("mv /tmp/experimentd /etc/init.d/experimentd")

for i in xrange(2,6):
   os.system("ln -s /etc/init.d/experimentd /etc/rc%s.d/S99experimentd" % i)

os.system("sudo /etc/init.d/experimentd restart")

