#!/usr/bin/python

import os
import sys
import subprocess

def run_experiment( wfile, experiment_cmd ):
   # run the experiment!
   experiment_data = None
   experiment_log = None
   try:
      experiment_log = open(experiment_cmd + ".log", "w")
      rc = subprocess.call( experiment_cmd, stdout=experiment_log, stderr=experiment_log, shell=True )
      experiment_log.close()
   except Exception, e:
      experiment_data = str(e)
      try:
         if experiment_log != None:
            experiment_log.close()
      except:
         pass

   if experiment_data == None:
      try:
         experiment_log = open(experiment_cmd + ".log", "r")
         experiment_data = experiment_log.read()
         experiment_log.close()
      except:
         experiment_data = "NO DATA"

   print >> wfile, "---------- BEGIN %s ----------\n%s\n---------- END %s ----------\n" % (experiment_cmd, experiment_data, experiment_cmd)
   return 0


if __name__ == "__main__":
   try:
      experiment_cmd = " ".join( sys.argv[1:] )
   except:
      print "Usage: %s EXPERIMENT [ARGS...]" % sys.argv[0]
      sys.exit(1)

   run_experiment( sys.stdout, experiment_cmd )

