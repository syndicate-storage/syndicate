#!/usr/bin/python

import sys
import os
import urllib2
import socket
import xmlrpclib
import time

phonehome_url = "http://quake.cs.arizona.edu/gec4demo/"         # base URL for demo

coord_path = sys.argv[1]
coord = open(coord_path, "r").readlines()

pollfile = sys.argv[2]
latitude = coord[0].strip()
longitude = coord[1].strip()
version = 1

while True:
   if os.path.isfile( pollfile ):
      os.system("cp " + pollfile + " /tmp/")
      break

   time.sleep(0.1)


# get our physical hostname
hostname = socket.gethostname()

final_url = phonehome_url + "notify?lat=" + str(latitude) + "&long=" + str(longitude) + "&site=" + hostname + "&version=" + str(version)
print final_url

# send latitude, longitude, site name, and version to quake
urllib2.urlopen( final_url )
