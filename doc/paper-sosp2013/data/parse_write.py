#!/usr/bin/python
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import math
import datautil

exp_delim = "--------------------------------"

def read_block( lines, start ):
   i = start + 1
   while i < len(lines) and lines[i] != exp_delim:
      i += 1

   block = lines[start + 1: min(len(lines),i)]
   
   return block


def parse_block( block ):
   data = {}
   for line in block:
      parts = line.split()
      if len(parts) == 0 or parts[0] != 'TIME':
         continue
      
      data[parts[1]] = float(parts[2])

   #print data
   return data
      
   
def parse( lines ):
   if lines == None or len(lines) <= 0:
      return None
      
   i = 0
   ret = {}
   while i < len(lines) and lines[i] != exp_delim:
      i += 1

   if i >= len(lines):
      return None

   # first block is raw diskwrite
   diskwrite_block = read_block( lines, i )
   diskwrite_stats = parse_block( diskwrite_block )

   i += len(diskwrite_block) + 1

   # local diskread
   diskread_block = read_block( lines, i )
   diskread_stats = parse_block( diskread_block )

   i += len(diskread_block) + 1

   ret['raw_disk_create'] = 0
   ret['raw_disk_delete'] = 0
   ret['raw_write_time'] = diskwrite_stats['end_diskwrite'] - diskwrite_stats['begin_diskwrite']
   ret['raw_read_time'] = diskread_stats['end_diskread'] - diskread_stats['begin_diskread']


   # mkdir
   mkdir_block = read_block( lines, i )
   mkdir_stats = parse_block( mkdir_block )

   ret['mkdir_revalidate'] = mkdir_stats['end_revalidate_path'] - mkdir_stats['begin_revalidate_path']
   ret['mkdir_time'] = mkdir_stats['end_mkdir'] - mkdir_stats['begin_mkdir']
   ret['mkdir_MS_create'] = mkdir_stats['end_ms_send'] - mkdir_stats['begin_ms_send']
   ret['mkdir_internal'] = ret['mkdir_time'] - ret['mkdir_MS_create']
   
   i += len(mkdir_block) + 1
   
   # create
   create_block = read_block( lines, i )
   create_stats = parse_block( create_block )

   ret['create_revalidate'] = create_stats['end_revalidate_path'] - create_stats['begin_revalidate_path']
   ret['create_time'] = create_stats['end_create'] - create_stats['begin_create']
   ret['create_MS_create'] = create_stats['end_ms_send'] - create_stats['begin_ms_send']
   ret['create_internal'] = ret['create_time'] - ret['create_MS_create']

   i += len(create_block) + 1

   # local write
   write_block = read_block( lines, i )
   write_stats = parse_block( write_block )
   ret['local_write_revalidate'] = write_stats['end_revalidate_path'] - write_stats['begin_revalidate_path']
   ret['local_write_time'] = write_stats['end_local_write'] - write_stats['begin_local_write']
   ret['local_write_MS_update'] = write_stats['end_ms_send'] - write_stats['begin_ms_send']
   ret['local_write_time_internal'] = ret['local_write_time'] - ret['local_write_MS_update'] - ret['local_write_revalidate']
   ret['local_write_latency_refresh'] = write_stats['end_revalidate_path'] - write_stats['begin_local_write']
   ret['local_write_latency_norefresh'] = ret['local_write_latency_refresh'] - (write_stats['end_ms_recv'] - write_stats['begin_ms_recv'])

   i += len(write_block) + 1

   # local read
   local_read_block = read_block( lines, i )
   read_stats = parse_block( local_read_block )
   ret['local_read_revalidate'] = read_stats['end_revalidate_path'] - read_stats['begin_revalidate_path']
   ret['local_read_time'] = read_stats['end_local_read'] - read_stats['begin_local_read']
   ret['local_read_time_internal'] = ret['local_read_time'] - ret['local_read_revalidate']
   ret['local_read_revalidate_internal'] = ret['local_read_revalidate'] - (read_stats['end_ms_recv'] - read_stats['begin_ms_recv'])
   ret['local_read_latency_refresh'] = read_stats['end_revalidate_path'] - read_stats['begin_local_read']
   ret['local_read_latency_norefresh'] = read_stats['end_revalidate_path'] - read_stats['begin_local_read'] - (read_stats['end_ms_recv'] - read_stats['begin_ms_recv'])
   
   i += len(write_block) + 1

   # close
   close_block = read_block( lines, i )
   i += len(close_block) + 1

   # unlink
   unlink_block = read_block( lines, i )
   unlink_stats = parse_block( unlink_block )
   ret['unlink_MS_delete'] = unlink_stats['end_ms_send'] - unlink_stats['begin_ms_send']
   ret['unlink_time'] = unlink_stats['end_unlink'] - unlink_stats['begin_unlink']
   ret['unlink_internal'] = ret['unlink_time'] - ret['unlink_MS_delete']

   i += len(unlink_block) + 1
   
   # rmdir
   rmdir_block = read_block( lines, i )
   rmdir_stats = parse_block( rmdir_block )
   ret['rmdir_MS_delete'] = rmdir_stats['end_ms_send'] - rmdir_stats['begin_ms_send']
   ret['rmdir_time'] = rmdir_stats['end_rmdir'] - rmdir_stats['begin_rmdir']
   ret['rmdir_internal'] = ret['rmdir_time'] - ret['rmdir_MS_delete']

   return ret

def mean( x ):
   return sum(x) / len(x)

def median( x ):
   return x[ len(x)/2 ]

def percent( x, per ):
   return x[ int(per * len(x)) ]
   
def stddev( data, avg ):
   val = 0.0
   for x in data:
      val += (x - avg) * (x - avg)

   val /= len(data)

   val = math.sqrt( val )
   return val

def make_cdf( data, label ):
   ecdf = sm.distributions.ECDF( data )
   x = np.linspace( min(data), max(data) )
   y = ecdf( x )

   return (x,y,label)

def show_cdfs( xys ):
   for (x, y, label) in xys:
      plt.plot( x, y, label=label )

   plt.legend( loc=4 )
   plt.show()


def make_bars( data_series, yerror_series=None, x_labels=None, legend_labels=None, bar_labels=None, hatches=None, colors=None, legend_cols=None, title="Title", xlabel="X", ylabel="Y", x_range=None, y_range=None, y_res=None, legend_pos=None, x_log=False, y_log=False ):

   width = 0.8 / len(data_series)
   N = len(data_series[0])
   ind = np.arange(N)
   fig = plt.figure()
   ax = fig.add_subplot( 111 )
   rects = []

   y_max = 0
   for op in data_series:
      y_max = max( y_max, max(op) )
      
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
   if bar_labels != None:
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




def make_lines( data_series, yerror_series=None, x_labels=None, x_ticks=None, point_labels=False, point_yoffs=None, legend_labels=None, styles=None, title="Title", xlabel="X", ylabel="Y", x_range=None, y_range=None, y_res=None, legend_pos=None, x_log=False, y_log=False ):

   fig = plt.figure()
   ax = fig.add_subplot( 111 )
   lines = []
   
   for i in xrange(0,len(data_series)):
      x_series = [x for (x,y) in data_series[i]]
      y_series = [y for (x,y) in data_series[i]]
      
      style = 'k'
      if styles != None:
         if styles[i] != None:
            style = styles[i]

      ll, = ax.plot( x_series, y_series, style, markersize=10 )
      lines.append(ll)

      if yerror_series != None:
         if yerror_series[i] != None:
            ax.errorbar( x_series, y_series, yerr=yerror_series[i] )

   # apply labels
   ax.set_xlabel( xlabel )
   ax.set_ylabel( ylabel )
   ax.set_title( title )

   if legend_labels != None:
      kwargs={}
      if legend_pos != None:
         kwargs['loc'] = legend_pos
      ax.legend( lines, legend_labels, **kwargs )


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

   # label bars
   if point_labels:
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
            ax.text( x + 1, yoff, '%3.2f' % float(y), ha = 'left', va = 'bottom' )
         j+=1
            
   if x_log:
      ax.set_xscale('log')

   if y_log:
      ax.set_yscale('log', nonposy="clip")


def get_results( path, parser ):
   files = os.listdir(path)

   success = 0
   data = {}
   for fname in files:
      
      fd = open( os.path.join(path,fname), "r" )
      buf = fd.read()
      fd.close()

      lines = buf.split("\n")
      for i in xrange(0,len(lines)):
         lines[i] = lines[i].strip()

      try:
         dat = parser(lines)

         if dat != None:
            data[fname] = dat
            success += 1

      except:
         pass

   results = {}
   for fname in data.keys():
      record = data[fname]
      
      for k in record.keys():
         if not results.has_key(k):
            results[k] = []

         if record[k] < 0:
            continue
         
         results[k].append( record[k] )

   for k in results:
      results[k].sort()

   return (data, results)
   
      
if __name__ == "__main__":
   
   data, results = get_results( sys.argv[1], parse )
   
   create_time_avg = mean(results['create_MS_create'])
   create_time_avg_no_ms = mean(results['create_internal'])
   create_time_std_no_ms = stddev( results['create_internal'], create_time_avg_no_ms )
   create_time_median = median(results['create_MS_create'])
   create_time_median_no_ms = median( results['create_internal'] )
   create_time_std = stddev( results['create_MS_create'], create_time_avg )
   create_time_90 = percent( results['create_MS_create'], 0.90 )
   create_time_99 = percent( results['create_MS_create'], 0.99 )
   
   mkdir_time_avg = mean(results['mkdir_MS_create'])
   mkdir_time_avg_no_ms = mean(results['mkdir_internal'])
   mkdir_time_median = median(results['mkdir_MS_create'])
   mkdir_time_median_no_ms = median( results['mkdir_internal'] )
   mkdir_time_std = stddev( results['mkdir_MS_create'], mkdir_time_avg )
   mkdir_time_std_no_ms = stddev( results['mkdir_internal'], mkdir_time_avg_no_ms )
   mkdir_time_90 = percent( results['mkdir_MS_create'], 0.90 )
   mkdir_time_99 = percent( results['mkdir_MS_create'], 0.99 )
   
   unlink_time_avg = mean(results['unlink_MS_delete'])
   unlink_time_avg_no_ms = mean(results['unlink_internal'])
   unlink_time_median = median(results['unlink_MS_delete'])
   unlink_time_median_no_ms = median(results['unlink_internal'])
   unlink_time_std = stddev( results['unlink_MS_delete'], unlink_time_avg )
   unlink_time_std_no_ms = stddev( results['unlink_MS_delete'], unlink_time_avg_no_ms )
   unlink_time_90 = percent( results['unlink_MS_delete'], 0.90 )
   unlink_time_99 = percent( results['unlink_MS_delete'], 0.99 )

   rmdir_time_avg = mean(results['rmdir_MS_delete'])
   rmdir_time_avg_no_ms = mean(results['rmdir_internal'])
   rmdir_time_median = median(results['rmdir_MS_delete'])
   rmdir_time_median_no_ms = median(results['rmdir_internal'])
   rmdir_time_std = stddev( results['rmdir_MS_delete'], rmdir_time_avg )
   rmdir_time_std_no_ms = stddev( results['rmdir_internal'], rmdir_time_avg_no_ms )
   rmdir_time_90 = percent( results['rmdir_MS_delete'], 0.90 )
   rmdir_time_99 = percent( results['rmdir_MS_delete'], 0.99 )

   update_time_avg = mean(results['local_write_MS_update'])
   update_time_avg_no_ms = mean(results['local_write_time_internal'])
   update_time_median = median(results['local_write_MS_update'])
   update_time_median_no_ms = median(results['local_write_time_internal'])
   update_time_std = stddev(results['local_write_MS_update'], update_time_avg)
   update_time_std_no_ms = stddev(results['local_write_time_internal'], update_time_avg_no_ms )
   update_time_90 = percent( results['local_write_MS_update'], 0.90 )
   update_time_99 = percent( results['local_write_MS_update'], 0.99 )
   
   update_time_raw_avg = mean(results['raw_write_time'])
   update_time_raw_median = median(results['raw_write_time'])
   update_time_raw_std = stddev(results['raw_write_time'], update_time_raw_avg )
   update_time_raw_90 = percent(results['raw_write_time'], 0.90 )
   update_time_raw_99 = percent( results['raw_write_time'], 0.99 ) 
   
   read_time_avg = mean(results['local_read_time'])
   read_time_avg_no_ms = mean(results['local_read_time_internal'])
   read_time_median = median(results['local_read_time'])
   read_time_median_no_ms = median(results['local_read_time_internal'])
   read_time_std = stddev( results['local_read_time'], read_time_avg )
   read_time_std_no_ms = stddev( results['local_read_time_internal'], read_time_avg_no_ms )
   read_time_90 = percent( results['local_read_time'], 0.90 )
   read_time_99 = percent( results['local_read_time'], 0.99 )
   read_time_raw_avg = mean(results['raw_read_time'])
   
   read_time_raw_median = median(results['raw_read_time'])
   read_time_raw_std = stddev( results['raw_read_time'], read_time_raw_avg)
   read_time_raw_90 = percent( results['raw_read_time'], 0.90 )
   read_time_raw_99 = percent( results['raw_read_time'], 0.99 )

   revalidate_time_avg = mean(results['local_read_revalidate'])
   revalidate_time_avg_no_ms = mean(results['local_read_revalidate_internal'])
   revalidate_time_median = median(results['local_read_revalidate'])
   revalidate_time_median_no_ms = median(results['local_read_revalidate_internal'])
   revalidate_time_std = stddev( results['local_read_revalidate'], revalidate_time_avg )
   revalidate_time_std_no_ms = stddev( results['local_read_revalidate_internal'], revalidate_time_avg_no_ms )
   revalidate_time_90 = percent( results['local_read_revalidate'], 0.90 )
   revalidate_time_99 = percent( results['local_read_revalidate'], 0.99 )

   read_latency_refresh_avg = mean(results['local_read_latency_refresh'])
   read_latency_refresh_stddev = stddev( results['local_read_latency_refresh'], read_latency_refresh_avg )
   read_latency_refresh_median = median( results['local_read_latency_refresh'] )
   read_latency_refresh_90 = percent( results['local_read_latency_refresh'], 0.90 )
   read_latency_refresh_99 = percent( results['local_read_latency_refresh'], 0.99 )

   read_latency_norefresh_avg = mean(results['local_read_latency_norefresh'])
   read_latency_norefresh_stddev = stddev( results['local_read_latency_norefresh'], read_latency_norefresh_avg )
   read_latency_norefresh_median = median( results['local_read_latency_norefresh'] )
   read_latency_norefresh_90 = percent( results['local_read_latency_norefresh'], 0.90 )
   read_latency_norefresh_99 = percent( results['local_read_latency_norefresh'], 0.99 )

   

   ops = [[create_time_avg, create_time_median, create_time_90, create_time_99],
          [mkdir_time_avg, mkdir_time_median, mkdir_time_90, mkdir_time_99],
          [unlink_time_avg, unlink_time_median, unlink_time_90, unlink_time_99],
          [rmdir_time_avg, rmdir_time_median, rmdir_time_90, rmdir_time_99],
          [revalidate_time_avg, revalidate_time_median, revalidate_time_90, revalidate_time_99],
          [update_time_avg, update_time_median, update_time_90, update_time_99]]

   yerror = [[create_time_std, 0, 0, 0],
             [mkdir_time_std, 0, 0, 0],
             [unlink_time_std, 0, 0, 0],
             [rmdir_time_std, 0, 0, 0],
             [revalidate_time_std, 0, 0, 0],
             [update_time_std, 0, 0, 0]]
   """
   ops = [[create_time_avg_no_ms, mkdir_time_avg_no_ms, unlink_time_avg_no_ms, rmdir_time_avg_no_ms, revalidate_time_avg_no_ms, update_time_avg_no_ms],
          [create_time_avg, mkdir_time_avg, unlink_time_avg, rmdir_time_avg, revalidate_time_avg, update_time_avg ],
          [create_time_median, mkdir_time_median, unlink_time_median, rmdir_time_median, revalidate_time_median, update_time_median],
          [create_time_90, mkdir_time_90, unlink_time_90, rmdir_time_90, revalidate_time_90, update_time_90],
          [create_time_99, mkdir_time_99, unlink_time_99, rmdir_time_99, revalidate_time_99, update_time_99]]
   """

   y_height = 0
   for op in ops:
      y_height = max( y_height, max(op) )
   
   make_bars( ops,
              x_labels=["Mean", "Median", "90th percentile", "99th percentile"],
              legend_labels=["Create", "Mkdir", "Delete", "Rmdir", "Revalidate", "Update"],
              hatches=['//', '--', '\\\\', '++', 'oo', 'xx'],
              bar_labels=ops,
              colors = ['#FFFFFF'] * len(ops),
              yerror_series=yerror,
              x_range=[-0.25, len(ops) - 2],
              y_range=[0, y_height + 0.25],
              y_res=0.25,
              title="Metadata Operations",
              xlabel="",
              ylabel="Time (seconds)",
              legend_pos=2)
              
   plt.show()
   
   
   """
   cdfs = []
   cdfs.append( make_cdf( results['create_MS_create'], "Concurrent create" ) )
   cdfs.append( make_cdf( results['mkdir_MS_create'], "Serial create" ) )
   cdfs.append( make_cdf( results['unlink_MS_delete'], "Concurrent delete" ) )
   cdfs.append( make_cdf( results['rmdir_MS_delete'], "Serial delete" ) )

   show_cdfs( cdfs )
   """

   """
   cdfs = []
   cdfs.append( make_cdf( results['create_MS_create'], "MS Create" ) )
   cdfs.append( make_cdf( results['local_write_MS_update'], "MS Update" ) )
   cdfs.append( make_cdf( results['local_read_revalidate'], "MS Revalidate" ) )
   cdfs.append( make_cdf( results['unlink_MS_delete'], "MS Delete" ) )

   show_cdfs( cdfs )
   
   
   cdfs = []
   cdfs.append( make_cdf( results['raw_read_time'], "Raw disk read" ) )
   cdfs.append( make_cdf( results['local_read_time_noconsistency'], "Local read, no revalidate" ) )
   cdfs.append( make_cdf( results['local_read_time'], "Local read" ) )
   cdfs.append( make_cdf( results['local_read_revalidate'], "Revalidate" ) )
   
   show_cdfs( cdfs )

   cdfs = []
   cdfs.append( make_cdf( results['raw_read_time'], "Raw disk read" ) )
   cdfs.append( make_cdf( results['local_read_time_noconsistency'], "Syndicate read, no consistency" ) )
   
   show_cdfs( cdfs )

   cdfs = []
   cdfs.append( make_cdf( results['raw_write_time'], "Raw disk write") )
   cdfs.append( make_cdf( results['local_write_time_noconsistency'], "Syndicate write, no consistency" ) )

   show_cdfs( cdfs )
   

   cdfs = []
   cdfs.append( make_cdf( results['raw_write_time'], "Raw disk write" ) )
   cdfs.append( make_cdf( results['local_write_time_noconsistency'], "Local write, no update" ) )
   cdfs.append( make_cdf( results['local_write_time'], "Local write" ) )
   cdfs.append( make_cdf( results['local_write_MS_update'], "MS Update" ) )

   show_cdfs( cdfs )
   """