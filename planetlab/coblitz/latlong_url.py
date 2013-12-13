#!/usr/bin/python

import sys

base = "http://maps.googleapis.com/maps/api/staticmap?size=2048x2048&maptype=roadmap&"

while True:
   line = sys.stdin.readline()
   if len(line) == 0:
      break

   parts = line.strip().split(" ")
   base += "markers=color:red|" + parts[0] + "," + parts[1] + "&"

base += "sensor=false"

print base

