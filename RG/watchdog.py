#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import daemon
import time
from rm_common import *

WATCHDOG_MONITOR_TIME = 15
HOST_TO_WATCH = 'localhost:5000'

#---------------------
def restart():
	with open(LOG_PATH + "log_watchdog.txt", "a") as f:
				f.write("(" + time.ctime() + ") Server was down. Restarting\n")

	#restarting code here

#---------------------
def watchdog():

	import urllib3
	#need to set timeout otherwise timeout is None by default
	http = urllib3.PoolManager(timeout=5)

	while True:

		try:
			r = http.request('STATUS', HOST_TO_WATCH)
		except:
			restart()
		else:
			if(r.status == 200):
				pass
			else:
				restart() 

		time.sleep(WATCHDOG_MONITOR_TIME)

#---------------------
def run():
	'''
	   Using python-daemon to meet the requirements of PEP 3143
	   for correct daemon behavior 
	   http://www.python.org/dev/peps/pep-3143/
	'''
	with daemon.DaemonContext():
		watchdog()

#---------------------
if __name__ == "__main__":
	run() 
