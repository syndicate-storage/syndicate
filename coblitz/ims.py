#!/usr/bin/python

import httplib
import os
import sys
import time
import thread

running = True
checking = False

def ims_thread( host, path, t ):
   global running
   global checking
   while running:
      if checking:
         # repeatedly try to download a URL if it has been modified since a particular epoch date
         con = httplib.HTTPConnection( host )
         con.request( "GET", path, headers={'If-Modified-Since': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(t))} )

         resp = con.getresponse()
   
         print resp.status, resp.reason

         con.close()

         time.sleep(0.1)
      else:
         time.sleep(1)



def control_thread( host, path ):
   global running
   global checking
   while True:
      # repeatedly phone home once a minute and see if we still need to spam the CDN
      con = httplib.HTTPConnection( host )
      con.request("GET", path )
      resp = con.getresponse()

      try:
         data = resp.read().strip()
         if data == "WAIT":
            checking = False
         elif data == "STOP":
            checking = False
            running = False
         else:
            running = True
            checking = True

      except Exception, e:
         print >> sys.stderr, "ERR: HTTP exception '%s'" % e
         break

      time.sleep(5)



thread.start_new_thread( ims_thread, ("vcoblitz.vicci.org:8008", "/www.cs.princeton.edu/~jcnelson/index.html", 1338593769) )
thread.start_new_thread( control_thread, ("vcoblitz-cmi.cs.princeton.edu", "/ims") )

while running:
   time.sleep(1)

