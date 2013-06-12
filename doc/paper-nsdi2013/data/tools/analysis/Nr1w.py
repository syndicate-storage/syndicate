#!/usr/bin/python

import analysis
import os
import sys
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

def eval_dict( s ):
   ret = None
   try:
      exec("ret = " + s)
   except:
      return None

   return ret

def cdf_compare( dists, title, xl, xr, yl, yr, labels ):
   mm = min(dists[0])
   ma = max(dists[0])
   cnt = len(dists[0])
   for i in xrange(1,len(dists)):
      mm = min( mm, min(dists[i]) )
      ma = max( ma, max(dists[i]) )
      cnt = min( cnt, len(dists[i]) )

   print "cnt = " + str(cnt)
   x = np.linspace( mm, ma, cnt )

   i = 0
   for dist in dists:
      ecdf = sm.distributions.ECDF( dist )
      plt.step( x, ecdf(x), label=labels[i] )
      i += 1

      dist.sort()
      #print dist

   plt.title( title )
   plt.xticks( xr )
   plt.yticks( yr )
   plt.xlabel( xl )
   plt.ylabel( yl )
   plt.legend( labels, loc=4 )
   plt.show()


   
   
if __name__ == "__main__":
   syndicate_data_1k = {}
   syndicate_data_1M = {}
   syndicate_data_50M = {}
   
   s3_data_20k = {}
   s3_data_50M = {}
   s3_data_100blk = {}
   s3_data_100blk_nocache = {}
   plc_data_100blk = {}
   syndicate_data_100blk = {}
   
   intersection = []

   for expfile in os.listdir( sys.argv[1] ):
      expfd = open( os.path.join( sys.argv[1], expfile ), "r" )
      expdata = analysis.parse_experiments( expfd )
      expfd.close()

      if len(expdata['fcdistro']) > 0 and "12" not in expdata['fcdistro']:
         print >> sys.stderr, "%s: wrong distro '%s'" % (expfile, expdata['fcdistro'])
         continue

      syndicate_exp_1k = analysis.read_experiment_data( expdata, "Nr1w-x5-small-syndicate.py" )
      syndicate_exp_1M = analysis.read_experiment_data( expdata, "Nr1w-x5-1M-syndicate.py" )
      syndicate_exp_50M = analysis.read_experiment_data( expdata, "Nr1w-x5-50M-syndicate-4.py" )
      syndicate_exp_100blk = analysis.read_experiment_data( expdata, "Nr1w-syndicate-3.py" )
      
      s3_exp_20k = analysis.read_experiment_data( expdata, "Nr1w-x5.py" )
      s3_exp_100blk = analysis.read_experiment_data( expdata, "Nr1w-x5-100blk-s3-cache-chunked.py" )
      plc_exp_100blk = analysis.read_experiment_data( expdata, "Nr1w-x5-100blk-planetlab-cache-chunked.py" )
      s3_exp_50M = analysis.read_experiment_data( expdata, "Nr1w-x5-50M.py" )

      s3_exp_100blk_nocache = analysis.read_experiment_data( expdata, "Nr1w-x5-100blk-s3-chunked.py" )
      
      intersect = True
      
      """
      if syndicate_exp_1k != None and len(syndicate_exp_1k) > 0 and syndicate_exp_1k[0] != None:
         syndicate_data_1k[expfile] = eval_dict( syndicate_exp_1k[0][0] )
      else:
         intersect = False

      if syndicate_exp_1M != None and len(syndicate_exp_1M) > 0 and syndicate_exp_1M[0] != None:
         syndicate_data_1M[expfile] = eval_dict( syndicate_exp_1M[0][0] )
      else:
         intersect = False

      if syndicate_exp_50M != None and len(syndicate_exp_50M) > 0 and syndicate_exp_50M[0] != None:
         syndicate_data_50M[expfile] = eval_dict( syndicate_exp_50M[0][0] )
      else:
         intersect = False

      if s3_exp_20k != None and len(s3_exp_20k) > 0 and s3_exp_20k[0] != None:
         s3_data_20k[expfile] = eval_dict( s3_exp_20k[0][0] )
      else:
         intersect = False

      if s3_exp_50M != None and len(s3_exp_50M) > 0 and s3_exp_50M[0] != None:
         s3_data_50M[expfile] = eval_dict( s3_exp_50M[0][0] )
      else:
         intersect = False
      """
      
      if s3_exp_100blk != None and len(s3_exp_100blk) > 0 and s3_exp_100blk[0] != None:
         s3_data_100blk[expfile] = eval_dict( s3_exp_100blk[0][0] )
      else:
         intersect = False

      if plc_exp_100blk != None and len(plc_exp_100blk) > 0 and plc_exp_100blk[-1] != None:
         plc_data_100blk[expfile] = eval_dict( plc_exp_100blk[-1][0] )
      else:
         intersect = False

      if s3_exp_100blk_nocache != None and len(s3_exp_100blk_nocache) > 0 and s3_exp_100blk_nocache[-1] != None:
         s3_data_100blk_nocache[expfile] = eval_dict( s3_exp_100blk_nocache[-1][0] )
      else:
         intersect = False

      if syndicate_exp_100blk != None and len(syndicate_exp_100blk) > 0 and syndicate_exp_100blk[-1] != None:
         syndicate_data_100blk[expfile] = eval_dict( syndicate_exp_100blk[-1][0] )
      else:
         intersect = False
         
      if intersect:
         intersection.append( expfile )

   for expfile in os.listdir( sys.argv[1] ):
      if expfile not in intersection:
         print >> sys.stderr, "Node %s did not pass all tests" % expfile

   print >> sys.stderr, "%s nodes have data" % len(intersection)

   syndicate = { 'first_1k': [], 'last_1k': [], 'first_1m': [], 'last_1m': [], 'first_50m': [], 'last_50m': [], 'first_100blk': [], 'last_100blk': [] }
   s3 = { 'first_20k': [], 'last_20k': [], 'first_50m': [], 'last_50m': [], 'first_100blk': [], 'last_100blk': [], 'first_100blk_nocache': [], 'last_100blk_nocache': [] }
   plc = {'first_100blk' : [], 'last_100blk': [] }

   num_valid = 0
   slow = []
   for node in intersection:
      valid = True
      #data_list = [("syndicate 1k", syndicate_data_1k), ("syndicate 1M", syndicate_data_1M), ("syndicate 50M", syndicate_data_50M), ("S3 20k", s3_data_20k), ("S3 50M", s3_data_50M), ("S3 100blk", s3_data_100blk), ("PLC 100blk", plc_data_100blk)]
      data_list = [("S3 100blk", s3_data_100blk), ("PLC 100blk", plc_data_100blk), ("S3 nocache 100blk", s3_data_100blk_nocache), ("Syndicate 100blk", syndicate_data_100blk)]
      for (data_name, data) in data_list:
         if data.get(node) == None:
            print >> sys.stderr, "%s: no data for %s" % (node, data_name)
            valid = False
         elif data[node] == None:
            print >> sys.stderr, "%s: unparseable data" % (node, data_name)
            valid = False
         elif len(data[node]['exception']) > 0:
            print >> sys.stderr, "%s: exceptions on %s" % (node, data_name)
            valid = False

      if not valid:
         continue;
         

      """
      syndicate['first_1k'].append( syndicate_data_1k[node]['end_recv'][0] - syndicate_data_1k[node]['start_recv'][0] )
      syndicate['last_1k'].append( syndicate_data_1k[node]['end_recv'][-1] - syndicate_data_1k[node]['start_recv'][-1] )
      syndicate['first_1m'].append( syndicate_data_1M[node]['end_recv'][0] - syndicate_data_1M[node]['start_recv'][0] )
      syndicate['last_1m'].append( syndicate_data_1M[node]['end_recv'][-1] - syndicate_data_1M[node]['start_recv'][-1] )
      syndicate['first_50m'].append( syndicate_data_50M[node]['end_recv'][0] - syndicate_data_50M[node]['start_recv'][0] )
      syndicate['last_50m'].append( syndicate_data_50M[node]['end_recv'][-1] - syndicate_data_50M[node]['start_recv'][-1] )
      s3['first_20k'].append( s3_data_20k[node]['end_recv'][0] - s3_data_20k[node]['start_recv'][0] )
      s3['last_20k'].append( s3_data_20k[node]['end_recv'][-1] - s3_data_20k[node]['start_recv'][-1] )
      s3['first_50m'].append( s3_data_50M[node]['end_recv'][0] - s3_data_50M[node]['start_recv'][0] )
      s3['last_50m'].append( s3_data_50M[node]['end_recv'][-1] - s3_data_50M[node]['start_recv'][-1] )
      """
      s3['first_100blk'].append( s3_data_100blk[node]['end_recv'][0] - s3_data_100blk[node]['start_recv'][0])
      s3['last_100blk'].append( s3_data_100blk[node]['end_recv'][-1] - s3_data_100blk[node]['start_recv'][-1])

      s3['first_100blk_nocache'].append( s3_data_100blk_nocache[node]['end_recv'][0] - s3_data_100blk_nocache[node]['start_recv'][0] )
      plc['first_100blk'].append( plc_data_100blk[node]['end_recv'][0] - plc_data_100blk[node]['start_recv'][0])
      plc['last_100blk'].append( plc_data_100blk[node]['end_recv'][-1] - plc_data_100blk[node]['start_recv'][-1])
      syndicate['first_100blk'].append( syndicate_data_100blk[node]['end_recv'][0] - syndicate_data_100blk[node]['start_recv'][0] )
      syndicate['last_100blk'].append( syndicate_data_100blk[node]['end_recv'][-1] - syndicate_data_100blk[node]['start_recv'][-1] )


      if syndicate['first_100blk'][-1] > 150:
         slow.append( node )
         
      num_valid += 1

   #print "s3_first_100blk = " + str(s3['first_100blk'])
   #print "s3_last_100blk = " + str(s3['last_100blk'])

   print "valid: " + str(num_valid)
   print "slow: \n" + "\n".join(slow)
   
   # first 1K vs last 1K
   cdf_compare( [syndicate['first_100blk'], syndicate['last_100blk'], plc['first_100blk'] ], "Syndicate One-Writer-Many-Reader Download Times", "Seconds", np.arange(0, 1000, 100), "CDF(x)", np.arange(0, 1.05, 0.05), ["Syndicate 0% Cache Hit", "Syndicate 100% Cache Hit", "Python HTTP Server and Clients"] )
   
   #cdf_compare( [plc['first_100blk'], s3['first_100blk']], "Amazon S3 vs PLC Cache Miss Download Times", "Seconds", np.arange(0, 425, 30), "CDF(x)", np.arange(0, 1.05, 0.05) )
   cdf_compare( [s3['first_100blk'], s3['first_100blk_nocache']], "Amazon S3 Cache and Direct Download Times", "Seconds", np.arange(0, 1200, 100), "CDF(x)", np.arange(0, 1.05, 0.05), ["0% hit cache hit rate", "Direct Download"] )
   cdf_compare( [s3['first_100blk'], s3['last_100blk']], "Amazon S3 Cache Miss and Cache Hit Download Times", "Seconds", np.arange(0, 425, 30), "CDF(x)", np.arange(0, 1.05, 0.05) )
   
   cdf_compare( [syndicate['first_1k'], syndicate['last_1k']] )
   cdf_compare( [syndicate['first_50m'], s3['first_50m']] )
   cdf_compare( [syndicate['last_50m'], s3['last_50m']] )
   #cdf_compare( [syndicate['last_1m'], s3['last_20k']] )
   