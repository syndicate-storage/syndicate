#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import sys
import rm_common
import rm_config
import rm_server

from wsgiref.simple_server import make_server 

#-------------------------
def debug():
   log = rm_common.log
   
   rm_common.syndicate_lib_path( "../libsyndicate/python" )
   
   gateway_name = "RG-t510-1-691"
   gateway_portnum = 23471
   rg_username = "jcnelson@cs.princeton.edu"
   rg_password = "nya!"
   ms_url = "http://localhost:8080/"
   my_key_file = "../../../replica_manager/test/replica_manager_key.pem"
   volume_name = "testvolume-jcnelson-cs.princeton.edu"
   
   # start up libsyndicate
   syndicate = rm_common.syndicate_init( ms_url=ms_url, gateway_name=gateway_name, portnum=gateway_portnum, volume_name=volume_name, gateway_cred=rg_username, gateway_pass=rg_password, my_key_filename=my_key_file )
   
   # start up config
   rm_config.init( syndicate )
   
   # start serving!
   httpd = make_server( "t510", gateway_portnum, rm_server.wsgi_application )
   
   httpd.serve_forever()
   
   return True 

#-------------------------    
if __name__ == "__main__":
   debug()