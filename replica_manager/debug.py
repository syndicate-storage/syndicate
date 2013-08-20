#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import sys

RG_STORE = "none"

#-------------------------
def debug():

	from rm_common import * 

	file_name = "test"

	data = 'GET /SYNDICATE-DATA/tmp/hello.1375782135401/0.8649607004776574730'

	#parse_block_request(data)
	
	import urllib3
	http = urllib3.PoolManager(timeout=5)

	try:
		r = http.request('STATUS', 'localhost:5000')
	except urllib3.exceptions.TimeoutError:
		print "Timed out"
	else:
		print r.status, r.data

	return True 

#-------------------------    
if __name__ == "__main__":
  
	debug()