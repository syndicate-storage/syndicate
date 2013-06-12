#!/usr/bin/python

import sys
import os
import urllib2
import socket
import xmlrpclib
import time

phonehome_url = "http://quake.cs.arizona.edu/gec4demo/"         # base URL for demo
mdurl = "http://node2.princeton.vicci.org:30000/hello2"
planetlab_api_url = "https://www.planet-lab.org/PLCAPI/"

planetlab_api_server = xmlrpclib.Server( planetlab_api_url )

# authentication data for PlanetLab
auth = {}
auth['AuthMethod'] = "anonymous" # we don't need username/password to get slices...
auth['Role'] = "user"

pollfile = sys.argv[1]
version = 1

not_found = True
while not_found:
   md = urllib2.urlopen( mdurl )
   md_lines = md.readlines()
   md.close()

   for md_l in md_lines:
      print md_l
      if pollfile in md_l:
         not_found = False

   time.sleep(1.0)


# get our physical hostname
hostname = socket.gethostname()

# get the list of nodes from PlanetLab
nodes = planetlab_api_server.GetNodes( auth, { 'hostname' : hostname }, ['site_id'])

site_id = nodes[0]['site_id']

# get info about this site
site_info = planetlab_api_server.GetSites( auth, {'site_id' : site_id }, ['latitude', 'longitude'] )

latitude = site_info[0]['latitude']
longitude = site_info[0]['longitude']

final_url = phonehome_url + "notify?lat=" + str(latitude) + "&long=" + str(longitude) + "&site=" + hostname + "&version=" + str(version)
print final_url

# send latitude, longitude, site name, and version to quake
urllib2.urlopen( final_url )
