#!/usr/bin/python

import os

os.system( "sudo /usr/sbin/puppetd --onetime --no-daemonize --server=vcoblitz-cmi.cs.princeton.edu --certname=princeton_vcoblitz.`hostname` --logdest console --debug" )
