#!/usr/bin/python

import sys
import os
import xmlrpclib
import traceback

sys.path.append("/usr/share/SMDS")

try:
   server_url = sys.argv[1]
   server_id = int(sys.argv[2])
   users = eval( "".join(sys.argv[3:]) )
except Exception, e:
   traceback.print_exc()

   print >> sys.stderr, "Usage: %s SERVER_URL SERVER_ID USER_LIST" % sys.argv[0]
   exit(1)

server = xmlrpclib.Server( server_url, allow_none=True )
print server.create_server( {'server_id': server_id}, users )

