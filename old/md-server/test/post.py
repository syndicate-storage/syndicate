#!/usr/bin/python

import socket
import time

m1 = "A " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"
m2 = "U " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html http%3a%2f%2fs3.amazon.com%2fhome%2fjcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"
m3 = "U " + str(int(time.time())) + " %2findex.html http%3a%2f%2fwww.cs.princeton.edu%2f~jcnelson%2findex.html http%3a%2f%2fs3.amazon.com%2fhome%2fjcnelson%2findex.html http%3a%2f%2fvcoblitz-cmi.cs.princeton.edu%2fbackups%2fhome%2fjcnelson%2findex.html 12345 644 14441 1332828266 b72ff11f6af13b6db07942c81ca99e942fc3ab99"

ms = [m1, m2, m3]


for m in ms:
   s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
   s.connect( ("localhost", 32780) )

   boundary = "7d226f700d0"
   http_body = "--%s\r\nContent-Disposition: form-data; name=\"metadata-updates\"\r\nContent-Type: text/plain\r\n\r\n%s\n\r\n--%s--" % (boundary, m, boundary)
   http_header = "POST /hello HTTP/1.0\r\nAccept: */*\r\nContent-Length: %s\r\nContent-Type: multipart/form-data; boundary=%s\r\n\r\n" % (len(http_body), boundary)

   http_m = http_header + http_body

   print "<<<<<<<<<<<<<<<<<<<<<<<<<"
   print http_m
   print "<<<<<<<<<<<<<<<<<<<<<<<<<\n"

   s.send( http_m )
   
   time.sleep(1.0)

   ret = s.recv(16384)

   print ">>>>>>>>>>>>>>>>>>>>>>>>>"
   print ret
   print ">>>>>>>>>>>>>>>>>>>>>>>>>\n"

   s.close()
