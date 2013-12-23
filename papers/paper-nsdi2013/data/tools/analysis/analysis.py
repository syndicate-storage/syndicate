#!/usr/bin/python

import os
import sys

def parse_experiments( fd, ignore_blank=False ):
   ret = {}    # map experiment name to data
   mode = "none"
   experiment_name = ""
   fc_distro = ""
   experiment_lines = []
   
   while True:
      line = fd.readline()

      if len(line) == 0:
         break

      line = line.strip()

      if mode == "none":
         if len(line) == 0:
            continue

         if line.startswith("---------- BEGIN"):
            parts = line.split()
            experiment_name = parts[2]
            mode = "experiment"
            continue

         if "redhat release" in line:
            fc_distro = line.split("'")[1]
            continue

      elif mode == "experiment":
         if ignore_blank and len(line) == 0:
            continue

         if line.startswith("PRAGMA"):
            continue
         
         if line.startswith("---------- END"):
            parts = line.split()
            if parts[2] == experiment_name:
               # end of this experiment
               if ret.get(experiment_name) == None:
                  ret[experiment_name] = []

               ret[experiment_name].append( experiment_lines )

               experiment_name = ""
               experiment_lines = []
               
               mode = "none"
               
               continue

            else:
               # could not parse
               print >> sys.stderr, "%s: Unexpected END of %s" % (experiment_name, parts[2])
               continue
               
         experiment_lines.append( line )

      else:
         break


   ret['fcdistro'] = fc_distro
   return ret



def read_experiment_data( experiment_dict, experiment_name ):
   if not experiment_dict.has_key( experiment_name ):
      return None

   data = []
   for run in experiment_dict[experiment_name]:
      ret = None
      try:
         # try to cast as a dict
         exec("ret = " + str(run) )
         data.append( ret )
      except:
         pass
      
   return data   
      
   
if __name__ == "__main__":
   experiment_name = sys.argv[1]
   fd = open( experiment_name, "r" )
   ret = parse_experiments( fd, ignore_blank=True )
   fd.close()

   print ret

   print ""

   data = read_experiment_data( ret, "Nr1w-x5-50M-syndicate-4.py" )
   print data