#!/usr/bin/python

import matplotlib.pyplot as plt
import numpy

import sys
sys.path.append("/home/jude/Desktop/research/syndicate/data")

import datautil

import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt


def cdf_compare( dists, title, xl, xr, yl, yr ):
   mm = min(dists[0])
   ma = max(dists[0])
   cnt = len(dists[0])
   for i in xrange(1,len(dists)):
      mm = min( mm, min(dists[i]) )
      ma = max( ma, max(dists[i]) )
      cnt = min( cnt, len(dists[i]) )

   x = np.linspace( mm, ma, cnt )

   for dist in dists:
      ecdf = sm.distributions.ECDF( dist )
      plt.step( x, ecdf(x) )

      dist.sort()
      #print dist

   plt.title( title )
   plt.xticks( xr )
   plt.yticks( yr )
   plt.xlabel( xl )
   plt.ylabel( yl )
   plt.show()


def make_graphs_cdf():
   data = {}
   times = {}
   cdfs = []
   title = sys.argv[1]
   xl = sys.argv[2]
   xr = np.arange( float(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]))
   yl = sys.argv[6]
   yr = np.arange( float(sys.argv[7]), float(sys.argv[8]), float(sys.argv[9]))
   percent = float(sys.argv[10])
   for filename in sys.argv[11:]:
      data[filename] = datautil.read_httpress_file( filename )
      times[filename] = map( lambda x : x.time, filter( lambda x: x.status == 'S', data[filename] ) )
      #cdfs.append( datautil.make_cdf( times[filename], 1000 ) )
      cdfs.append( times[filename][0:int(len(times[filename])*percent)] )
      #cdfs.append( times[filename] )

   cdf_compare( cdfs, title, xl, xr, yl, yr )

   #datautil.show_cdfs( cdfs, xticks=0.0005, xspacing=5, yticks=0.0125, yspacing=2, xlabel="Seconds", ylabel="CDF(x)")
   
"""
def show_GET():
   data = datautil.read_httpress_file( get_1 )
   data_ssl = datautil.read_httpress_file( get_2 )

   success_times = map( lambda x: x.time, filter( lambda x: x.status == 'S', data ) )
   success_times_ssl = map( lambda x: x.time, filter( lambda x: x.status == 'S', data_ssl ) )

   success_cdf = datautil.make_cdf( success_times, 1000 )
   success_cdf_ssl = datautil.make_cdf( success_times_ssl, 1000 )

   success_ccdf = datautil.cdf_to_ccdf( success_cdf )
   success_ccdf_ssl = datautil.cdf_to_ccdf( success_cdf_ssl )

   datautil.show_cdfs( [success_ccdf, success_ccdf_ssl], xticks=0.05, xspacing=10, yticks=0.0125, yspacing=2, xlabel="Seconds", ylabel="1 - CDF(x)" )

   
def show_POST():
   data_ssl = datautil.read_httpress_file("md4-b1-c1000-POST-ssl-keepalive.txt")
   data = datautil.read_httpress_file("md4-b1-c1000-GET-11-ssl-keepalive.txt")

   success_times = map( lambda x: x.time, filter( lambda x: x.status == 'S', data ) )
   success_times_ssl = map( lambda x: x.time, filter( lambda x: x.status == 'S', data_ssl ) )

   success_cdf = datautil.make_cdf( success_times, 1000 )
   success_cdf_ssl = datautil.make_cdf( success_times_ssl, 1000 )

   success_ccdf = datautil.cdf_to_ccdf( success_cdf )
   success_ccdf_ssl = datautil.cdf_to_ccdf( success_cdf_ssl )

   datautil.show_cdfs( [success_ccdf, success_ccdf_ssl], xticks=0.1, xspacing=25, yticks=0.0125, yspacing=2, xlabel="Seconds", ylabel="1 - CDF(x)" )
"""
   
if __name__ == "__main__":
   make_graphs_cdf()
   #show_GET()
   #show_POST()
