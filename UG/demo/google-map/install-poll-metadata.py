#!/usr/bin/python

import os
import sys
import subprocess
import time

hostfile = sys.argv[1]
installable = sys.argv[2]

hosts = []
hf = open(hostfile,"r")
for line in hf.readlines():
   hosts.append( line.strip() )

hf.close()

for h in hosts:
   scp_proc = subprocess.Popen( ["/usr/bin/scp", installable, "princeton_syndicate@" + h + ":~/"] )

   stop = time.time() + 5
   while True:
      if time.time() > stop:
         break

      if scp_proc.poll() != None:
         break

      time.sleep(0.1)

   if scp_proc.poll() == None:
      scp_proc.kill()
      continue

   if scp_proc.returncode != 0:
      print "rc = " + str(scp_proc.returncode)
      continue

   start_proc = subprocess.Popen( ["/usr/bin/ssh", "princeton_syndicate@" + h, "nohup", "~/" + installable, "foo", "&"] )

   stop = time.time() + 5
   while True:
      if time.time() > stop:
         break

      if start_proc.poll() != None:
         break

      time.sleep(0.1)

   if start_proc.poll() == None:
      start_proc.kill()
      continue

   if start_proc.returncode != 0:
      print "rc = " + str(start_proc.returncode)
      continue

   print "Started on " + h


