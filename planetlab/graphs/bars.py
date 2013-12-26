#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import sys
import common
import matplotlib.pyplot as plt
import numpy as np
import types

def default_hatches( num_series ):
   # default hatches
   hh = ['/', '-', '\\', '+', 'o', 'x', '*']
   hatches = []
   i = 0
   k = 0
   l = 1
   while i < num_series:
      hatches.append( hh[k] * l )
      k += 1
      if k >= len(hh):
         k = 0
         l += 1
      i += 1

   return hatches
   

def default_colors( num_series):
   # default colors 
   colors = ['white'] * num_series
   return colors

   
def make_bars( aggregate, yerror_aggregate=None, series_order=None, x_labels=None, legend_labels=None, legend_cols=None, legend_pos=None, series_labels=False, hatches=None, colors=None, title="Title", xlabel="X", ylabel="Y", x_range=None, y_range=None, y_res=None, x_log=False, y_log=False ):
   # aggregate = {step: {key: [method_result_1, method_result_2, ...], ...}, ...}
   # yerror_aggregate = {step: {key: [yerror_1, yerror_2, ...], ...}, ...}
   # series_order = [(step1, key1), (step2, key2), ...]

   # extract data series into [[bar11, bar12, bar13, ...], [bar21, bar22, bar23, ...], ...]
   # extract yerror series into [[yerror11, yerror12, yerror13, ...], [yerror21, yerror22, yerror23, ...], ...]

   if series_order == None:
      series_order = common.graph_default_order( aggregate )

   if legend_labels == None:
      legend_labels = common.graph_legend_labels( series_order )
      
   data_series, yerror_series = common.graph_series( aggregate, yerror_aggregate, series_order )

   width = 0.8 / len(data_series)
   N = len(data_series[0])
   ind = np.arange(N)
   fig = plt.figure()
   ax = fig.add_subplot( 111 )
   rects = []

   y_max = 0
   for op in data_series:
      y_max = max( y_max, max(op) )

   if hatches == None and colors == None:
      hatches = default_hatches( len(data_series) )

   if colors == None:
      colors = default_colors( len(data_series) )
      
   for i in xrange(0,len(data_series)):
      rect = None
      kwargs = {}
      if yerror_series != None:
         if yerror_series[i] != None:
            kwargs['yerr'] = yerror_series[i]

      if hatches != None:
         if hatches[i] != None:
            kwargs['hatch'] = hatches[i]

      if colors != None:
         if colors[i] != None:
            kwargs['color'] = colors[i]

      rect = ax.bar( ind + i * width, data_series[i], width, log=y_log, **kwargs )
      rects.append( rect )

   # apply labels
   ax.set_xlabel( xlabel )
   ax.set_ylabel( ylabel )
   ax.set_title( title )

   if legend_labels != None:
      kwargs={}
      if legend_pos != None:
         kwargs['loc'] = legend_pos
      if legend_cols != None:
         kwargs['ncol'] = legend_cols

      ax.legend( rects, legend_labels, **kwargs )


   if x_labels != None:
      ax.set_xticks( ind + width * len(data_series) / 2.0 )
      ax.set_xticklabels( x_labels )
      ax.autoscale()

   if y_res != None and y_range != None:
      ax.set_yticks( np.arange(y_range[0], y_range[1], y_res) )

   # label bars
   if series_labels:
      for rect_series in rects:
         for rect in rect_series:
            height = rect.get_height()
            y_off = 0
            if y_log:
               y_off = height * 1.05
            else:
               y_off = height + y_max * 0.005

            ax.text( rect.get_x() + rect.get_width() / 2.0, y_off, '%3.2f' % float(height), ha = 'center', va = 'bottom' )

   if x_range != None:
      ax.set_autoscalex_on(False)
      ax.set_xlim( [x_range[0], x_range[1]] )

   if y_range != None:
      ax.set_autoscaley_on(False)
      ax.set_ylim( [y_range[0], y_range[1]] )

   if x_log:
      ax.set_xscale('log')

   if y_log:
      ax.set_yscale('log', nonposy="clip")




if __name__ == "__main__":
   data, error = common.mock_experiment( ".bar_experiment", 3, 3 )

   bar_order = []
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
         bar_order.append( (step, key) )
         
   make_bars( data, yerror_aggregate = error, series_order = bar_order, y_range = [0, 20], series_labels = True, x_labels=["Means", "Medians", "90th Percentiles", "99th Percentiles"] )

   plt.show()
   
   
   
         