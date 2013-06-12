"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import matplotlib.pyplot as plt
import numpy as np
import common

def default_styles( length ):
   markers = ['o', '+', '*', '^', '.', 'x', 'v', '<', '>', '|', 'D', 'H', 'h', '_', 'p', '8']
   ret = []
   for i in xrange(0,length):
      ret.append(markers[ i % len(markers) ])

   return ret

def make_lines( aggregate, yerror_aggregate=None, series_order=None, x_labels=None, x_ticks=None, series_labels=False, point_yoffs=None, legend_labels=None, styles=None, title="Title", xlabel="X", ylabel="Y", x_range=None, y_range=None, y_res=None, legend_pos=None, x_log=False, y_log=False ):

   if series_order == None:
      series_order = common.graph_default_order( aggregate )
      
   y_data_series, yerror_series = common.graph_series( aggregate, yerror_aggregate, series_order )
   
   x_data_series = None
   if x_ticks != None:
      x_data_series = x_ticks
   else:
      x_data_series = range(0, len(y_data_series))

   data_series = []
   yerror = []
   
   for i in xrange(0, len(y_data_series[0])):
      data_series.append( [] )
      yerror.append( [] )
      
   for i in xrange(0, len(y_data_series)):
      xs = [i] * len(y_data_series[i])
      pts = zip(xs, y_data_series[i])

      k = 0
      for j in xrange(0,len(data_series)):
         data_series[j].append( pts[k] )
         yerror[j].append( yerror_series[j][k] )
         k += 1
   
   fig = plt.figure()
   ax = fig.add_subplot( 111 )
   lines = []

   if styles == None:
      styles = default_styles( len(data_series) )

   for i in xrange(0,len(data_series)):
      x_series = [x for (x,y) in data_series[i]]
      y_series = [y for (x,y) in data_series[i]]

      style = 'k'
      if styles != None:
         if styles[i] != None:
            style = styles[i]

      ll, = ax.plot( x_series, y_series, style, markersize=10 )
      lines.append(ll)

      if yerror != None:
         if yerror[i] != None:
            ax.errorbar( x_series, y_series, yerr=yerror[i] )

   # apply labels
   ax.set_xlabel( xlabel )
   ax.set_ylabel( ylabel )
   ax.set_title( title )

   if legend_labels == None:
      legend_labels = common.graph_legend_labels( series_order )
      
   if legend_labels != None:
      kwargs={}
      if legend_pos != None:
         kwargs['loc'] = legend_pos
      ax.legend( lines, legend_labels, **kwargs )


   if x_ticks == None:
      x_ticks = x_data_series
      
   if x_labels == None:
      x_labels = [str(x) for x in x_ticks]
      
   if x_labels != None and x_ticks != None:
      ax.set_xticks( x_ticks )
      ax.set_xticklabels( x_labels )
      ax.autoscale()

   if y_res != None and y_range != None:
      ax.set_yticks( np.arange(y_range[0], y_range[1], y_res) )

   if x_range != None:
      ax.set_autoscalex_on(False)
      ax.set_xlim( [x_range[0], x_range[1]] )

   if y_range != None:
      ax.set_autoscaley_on(False)
      ax.set_ylim( [y_range[0], y_range[1]] )

   # label points
   if series_labels:
      j=0
      for ll in lines:
         x_series = ll.get_xdata()
         y_series = ll.get_ydata()
         i = 0
         for (x,y) in zip(x_series, y_series):
            yoff = y
            if point_yoffs:
               yoff = y + point_yoffs[j][i]

            i+=1
            ax.text( x, yoff, '%3.2f' % float(y), ha = 'left', va = 'bottom' )
         j+=1

   if x_log:
      ax.set_xscale('log')

   if y_log:
      ax.set_yscale('log', nonposy="clip")


if __name__ == "__main__":
   data, error = common.mock_experiment( ".scatter_experiment", 3, 3 )

   series_order = []
   steps = []
   keys = []
   for step in data.keys():
      steps.append(step)
      for key in data[step].keys():
         keys.append(key)

   steps = set(steps)
   keys = set(keys)

   steps = list(steps)
   keys = list(keys)

   steps.sort()
   keys.sort()

   for step in steps:
      for key in keys:
         series_order.append( (step, key) )

   make_lines( data, yerror_aggregate = error, series_order = series_order, y_range = [0, 15], series_labels = True, legend_labels=["Average", "Median", "90th Percentile", "99th Percentile"] )

   plt.show()
      