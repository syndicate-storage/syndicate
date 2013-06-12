#!/usr/bin/python

import sys
import socket
import time
import os
import urllib

g_host = "localhost"
g_port = 8889
g_driver = "/drivers/debug"

def make_http_header( op, path, length, boundary ):
   return "%s %s HTTP/1.0\r\nAccept: */*\r\nContent-Length: %s\r\nContent-Type: multipart/form-data; boundary=%s\r\n\r\n" % (op, path, length, boundary)

def make_http_form( boundary, name, content_type, data ):
   return "--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\nContent-Type: %s\r\n\r\n%s\r\n\r\n" % (boundary, name, content_type, data)

def end_http_form( boundary ):
   return "--%s--" % boundary

   
def post_record( path, metadata_str, data=None ):
   global g_host
   global g_port
   global g_driver
   
   body_fields = zip( [metadata_str, data], ["metadata", "data"], ["text/plain", "application/octet-stream"] )
   
   s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
   s.connect( (g_host, g_port) )
   
   boundary = "7d226f700d0"
   
   msg_body = ""
   
   for (component, field_name, mime_type) in body_fields:
      if component != None:
         msg_body += make_http_form( boundary, field_name, mime_type, component )
   
   msg_body += end_http_form( boundary )

   msg_hdr = make_http_header( "POST", os.path.join( g_driver, path ), len( msg_body ), boundary )
   
   http_msg = msg_hdr + msg_body
   
   print "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
   print http_msg
   print "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
   
   s.send( http_msg )
   time.sleep(1.0)
   
   ret = s.recv(32768)
   
   print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   print ret
   print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   
   s.close()
   
   return
   

m1 = "A " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"
m2 = "U " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html http%3a%2f%2fs3.amazon.com%2fhome%2fjcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"
m3 = "U " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html http%3a%2f%2fs3.amazon.com%2fhome%2fjcnelson%2findex.html http%3a%2f%2fvcoblitz-cmi.cs.princeton.edu%2fbackups%2fhome%2fjcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"

metadata = [m1]


if __name__ == "__main__":
   if len(sys.argv) <= 1:
      for m in metadata:
         post_record( "index.html", m, None )

   else:
      for path in sys.argv[1:]:
         m = "A " + str(int(time.time())) + " " + urllib.quote(path, safe="") + " " + urllib.quote("http://www.cs.princeton.edu/~jcnelson/index.html", safe="") + " 12345 644 " + str(os.stat(path).st_size) + " " + str(int(os.stat(path).st_mtime))

         fd = open(path, "r")
         data = fd.read()
         fd.close()

         post_record( path, m, data )
   
