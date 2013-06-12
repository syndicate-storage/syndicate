#!/usr/bin/python

import matplotlib.pyplot as plt
import numpy

import sys
sys.path.append("/home/jude/Desktop/research/syndicate/data")

import datautil


if __name__ == "__main__":
   data_100 = datautil.read_httpress_file("rp4-b1-c100-GET-61140.txt")
   data_500 = datautil.read_httpress_file("rp4-b1-c500-GET-61140.txt")

   success_times_100 = map( lambda x: x.time, filter( lambda x: x.status == 'S', data_100 ) )
   success_times_500 = map( lambda x: x.time, filter( lambda x: x.status == 'S', data_500 ) )
   
   success_cdf_100 = datautil.make_cdf( success_times_100, 1000 )
   success_cdf_500 = datautil.make_cdf( success_times_500, 1000 )

   datautil.show_cdfs( [success_cdf_100], xticks=0.1, xspacing=25, yticks=0.0125, yspacing=2, xlabel="Seconds", ylabel="CDF(x)" )
   datautil.show_cdfs( [success_cdf_500], xticks=0.1, xspacing=25, yticks=0.0125, yspacing=2, xlabel="Seconds", ylabel="CDF(x)" )