import os
import collections
import matplotlib.pyplot as plt
import numpy

httpress_entry = collections.namedtuple( 'httpress_entry', ['status', 'time'] )

# read a file produced from httpress
def read_httpress_file( dat_path ):
   fd = open( dat_path, "r" )

   ret = []
   
   while True:
      line = fd.readline().strip()

      if len(line) == 0:
         break
         
      # format: [S|F] TIME
      parts = line.split()

      e = httpress_entry( status=parts[0], time=float(parts[1]) )
      ret.append( e )
      
   return ret


# given a list of data points, calculate a CDF
def make_cdf( data, step ):
   cdf = []
   data_min = min(data)
   data_max = max(data)

   ran = []
   for i in xrange(0, step):
      ran.append( data_min + i * (data_max - data_min) / float(step) )

   for s in ran:
      c = 0
      for t in data:
         if t < s:
            c += 1

      cdf.append( (s, float(c) / len(data)) )

   return cdf

# make a CCDF from a CDF
def cdf_to_ccdf( cdf_data ):
   ccdf = []
   for i in xrange(0,len(cdf_data)):
      ccdf.append( (cdf_data[i][0], 1 - cdf_data[i][1]) )

   return ccdf
   
# make a CDF of a list of lists of points
def show_cdfs( datas, xticks=1, xspacing=1, yticks=1, yspacing=1, xlabel="", ylabel="" ):

   # get min/max
   x_min = 0
   x_max = 0
   y_min = 0
   y_max = 0

   data_x = []
   data_y = []
   for data in datas:
      xx = map( lambda x: x[0], data )
      yy = map( lambda y: y[1], data )
      x_min = min( x_min, min( xx ) )
      x_max = max( x_max, max( xx ) )
      y_min = min( y_min, min( xx ) )
      y_max = max( y_max, max( yy ) )
      data_x.append( xx )
      data_y.append( yy )


   # find out where the ticks are
   xpts = numpy.arange( x_min, x_max, xticks )
   ypts = numpy.arange( y_min, y_max, yticks )
   xtick_lbls = []
   ytick_lbls = []
   
   for i in xrange(0, len(xpts)/xspacing):
      xtick_lbls.append( "%8.1f" % xpts[i * xspacing] )
      for j in xrange(1, xspacing):
         xtick_lbls.append( "" )

   for i in xrange(0, len(ypts)/yspacing):
      ytick_lbls.append( "%8.3f" % ypts[i * yspacing] )
      for j in xrange(1, yspacing):
         ytick_lbls.append( "" )

   for i in xrange(0,len(datas)):
      plt.plot( data_x[i], data_y[i] )

   plt.xticks( xpts, xtick_lbls )
   plt.yticks( ypts, ytick_lbls )
   plt.xlabel( xlabel )
   plt.ylabel( ylabel )
   plt.show()

   