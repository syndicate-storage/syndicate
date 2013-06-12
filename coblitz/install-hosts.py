#!/usr/bin/python

import os

os.system("sudo /etc/init.d/coblitz_proxy stop")
os.system("curl http://vcoblitz-cmi.cs.princeton.edu/hosts > /tmp/hosts")
os.system("sudo cp /tmp/hosts /etc/hosts")
os.system("sudo /etc/init.d/coblitz_proxy start")
