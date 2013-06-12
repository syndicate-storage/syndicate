#!/usr/bin/python
import os
import sys
import math
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import parse_write as pw
import cdn

def mean( x ):
   return sum(x) / len(x)

def median( x ):
   return x[ len(x)/2 ]

def stddev( data, avg ):
   val = 0.0
   for x in data:
      val += (x - avg) * (x - avg)

   val /= len(data)

   val = math.sqrt( val )
   return val



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

   # first block is the open
   open_block = read_block( lines, i )
   open_stats = parse_block( open_block )

   # extract meaningful data
   ret['remote_open_time'] = open_stats['end_open'] - open_stats['begin_open']
   ret['remote_open_revalidate'] = open_stats['end_revalidate_path'] - open_stats['begin_revalidate_path']
   ret['remote_open_manifest'] = open_stats['end_revalidate_manifest'] - open_stats['begin_revalidate_manifest']
   ret['remote_open_internal'] = ret['remote_open_time'] - (ret['remote_open_revalidate'] + ret['remote_open_manifest'])

   i += len(open_block) + 1

   # next 10 blocks are remote_read
   read_stats = []
   read_blocks = []

   for j in xrange(0,10):
      blk = read_block( lines, i )
      i += len(blk) + 1
      read_blocks.append( blk )
      read_stats.append( parse_block( blk ) )

   for j in xrange(0,1):
      blk = read_stats[j]
      ret['remote_read_latency_miss'] = blk['end_revalidate_manifest'] - blk['begin_remote_read_' + str(j)]
      ret['remote_read_miss'] = (blk['end_remote_read_' + str(j)] - blk['begin_remote_read_' + str(j)])
      ret['remote_read_noconsistency_miss'] = (blk['end_remote_read_' + str(j)] - blk['end_revalidate_manifest'])
      ret['remote_read_latency_internal'] = ret['remote_read_latency_miss'] - (blk['end_ms_recv'] - blk['begin_ms_recv'])
      ret['remote_read_latency_miss_all'] = ret['remote_open_manifest'] + ret['remote_read_latency_miss']
      
   worst_read_latency = 0
   best_read_latency = 10000000000000
   avg_read_latency = 0

   worst_read_hit = 0
   best_read_hit = 1000000000000
   avg_read_hit = 0

   worst_read_hit_noconsistency = 0
   best_read_hit_noconsistency = 1000000000000
   avg_read_hit_noconsistency = 0

   
   for j in xrange(1,10):
      blk = read_stats[j]
      ret['remote_read_latency_hit'] = blk['end_revalidate_manifest'] - blk['begin_remote_read_' + str(j)]
      ret['remote_read_hit'] = (blk['end_remote_read_' + str(j)] - blk['begin_remote_read_' + str(j)])
      ret['remote_read_noconsistency_hit'] = (blk['end_remote_read_' + str(j)] - blk['end_revalidate_manifest'])

      worst_read_hit = max( worst_read_hit, ret['remote_read_hit'] )
      best_read_hit = min( best_read_hit, ret['remote_read_hit'] )
      avg_read_hit += ret['remote_read_hit']

      worst_read_latency = max( worst_read_latency, ret['remote_read_latency_hit'] )
      best_read_latency = min( best_read_latency, ret['remote_read_latency_hit'] )
      avg_read_latency += ret['remote_read_latency_hit']

      worst_read_hit_noconsistency = max( worst_read_hit_noconsistency, ret['remote_read_noconsistency_hit'] )
      best_read_hit_noconsistency = min( best_read_hit_noconsistency, ret['remote_read_noconsistency_hit'] )
      avg_read_hit_noconsistency += ret['remote_read_noconsistency_hit']

   avg_read_hit /= 9.0
   avg_read_hit_noconsistency /= 9.0
   avg_read_latency /= 9.0

   ret['worst_read_hit'] = worst_read_hit
   ret['best_read_hit'] = best_read_hit
   ret['avg_read_hit'] = avg_read_hit
   ret['worst_read_latency'] = worst_read_latency
   ret['best_read_latency'] = best_read_latency
   ret['avg_read_latency'] = avg_read_latency
   ret['worst_read_hit_noconsistency'] = worst_read_hit_noconsistency
   ret['best_read_hit_noconsistency'] = best_read_hit_noconsistency
   ret['avg_read_hit_noconsistency'] = avg_read_hit_noconsistency
      
   # close block
   close_block = read_block( lines, i )
   close_stats = parse_block( close_block )

   return ret
   

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



   
if __name__ == "__main__":

   cdn_data = cdn.cdn_perf
   cdn_results = {}
   for host in cdn_data.keys():
      cdn_data[host]['download_time_miss'] = 6144000.0 / cdn_data[host]['bandwidth_0']
      cdn_data[host]['download_time_hit'] = 6144000.0 / cdn_data[host]['bandwidth_4']

   for host in cdn_data.keys():
      rec = cdn_data[host]
      for k in rec.keys():
         if not cdn_results.has_key(k):
            cdn_results[k] = []

         if rec[k] < 0:
            continue
           
         cdn_results[k].append( rec[k] )

   for k in cdn_results:
      cdn_results[k].sort()
   
   write_data, results = pw.get_results( sys.argv[1], pw.parse )
   read_data, read_results = pw.get_results( sys.argv[2], parse )
   
   read_data = []
   read_res = []
   
   for i in xrange(1, 10):
      read_dir = "read_" + str(i) + "0"
      data, res = pw.get_results( read_dir, parse )
      read_data.append( data )
      read_res.append( res )

   for i in xrange(0, 1):
      read_dir = "read_" + str(i) + "0"
      data, res = pw.get_results( read_dir, parse )
      read_data.append( data )
      read_res.append( res )


   read_time_avg = mean(results['local_read_time'])
   read_time_avg_no_ms = mean(results['local_read_time_internal'])
   read_time_median = median(results['local_read_time'])
   read_time_median_no_ms = median(results['local_read_time_internal'])
   read_time_std = stddev( results['local_read_time'], read_time_avg )
   read_time_std_no_ms = stddev( results['local_read_time_internal'], read_time_avg_no_ms )
   read_time_90 = pw.percent( results['local_read_time'], 0.90 )
   read_time_90_no_ms = pw.percent( results['local_read_time_internal'], 0.90 )
   read_time_99 = pw.percent( results['local_read_time'], 0.99 )
   read_time_99_no_ms = pw.percent( results['local_read_time_internal'], 0.99 )
   
   read_time_raw_avg = mean(results['raw_read_time'])
   read_time_raw_median = median(results['raw_read_time'])
   read_time_raw_std = stddev( results['raw_read_time'], read_time_raw_avg)
   read_time_raw_90 = pw.percent( results['raw_read_time'], 0.90 )
   read_time_raw_99 = pw.percent( results['raw_read_time'], 0.99 )

   update_time_avg = mean(results['local_write_MS_update'])
   update_time_avg_no_ms = mean(results['local_write_time_internal'])
   update_time_median = median(results['local_write_MS_update'])
   update_time_median_no_ms = median(results['local_write_time_internal'])
   update_time_std = stddev(results['local_write_MS_update'], update_time_avg)
   update_time_std_no_ms = stddev(results['local_write_time_internal'], update_time_avg_no_ms )
   update_time_90_no_ms = pw.percent( results['local_write_time_internal'], 0.90 )
   update_time_99_no_ms = pw.percent( results['local_write_time_internal'], 0.99 )
   update_time_90 = pw.percent( results['local_write_time'], 0.90 )
   update_time_99 = pw.percent( results['local_write_time'], 0.99 )
   
   update_time_raw_avg = mean(results['raw_write_time'])
   update_time_raw_median = median(results['raw_write_time'])
   update_time_raw_std = stddev(results['raw_write_time'], update_time_raw_avg )
   update_time_raw_90 = pw.percent(results['raw_write_time'], 0.90 )
   update_time_raw_99 = pw.percent( results['raw_write_time'], 0.99 )

   update_latency_avg = mean(results['local_write_latency_norefresh'])
   update_latency_std = stddev( results['local_write_latency_norefresh'], update_latency_avg )
   update_latency_median = median(results['local_write_latency_norefresh'])
   update_latency_90 = pw.percent(results['local_write_latency_norefresh'], 0.90)
   update_latency_99 = pw.percent(results['local_write_latency_norefresh'], 0.99)

   remote_read_hit_avg = mean(read_results['remote_read_noconsistency_hit'])
   remote_read_hit_stddev = stddev(read_results['remote_read_noconsistency_hit'], remote_read_hit_avg)
   remote_read_hit_median = median(read_results['remote_read_noconsistency_hit'])
   remote_read_hit_90 = pw.percent( read_results['remote_read_noconsistency_hit'], 0.90)
   remote_read_hit_99 = pw.percent( read_results['remote_read_noconsistency_hit'], 0.99)
   
   remote_read_miss_avg = mean( read_results['remote_read_noconsistency_miss'] )
   remote_read_miss_stddev = stddev(read_results['remote_read_noconsistency_miss'], remote_read_miss_avg )
   remote_read_miss_median = median(read_results['remote_read_noconsistency_miss'])
   remote_read_miss_90 = pw.percent( read_results['remote_read_noconsistency_miss'], 0.9 )
   remote_read_miss_99 = pw.percent( read_results['remote_read_noconsistency_miss'], 0.99 )

   manifest_avg = mean(read_results['remote_open_manifest'] )
   manifest_stddev = stddev( read_results['remote_open_manifest'], manifest_avg )
   manifest_median = median( read_results['remote_open_manifest'] )
   manifest_90 = pw.percent( read_results['remote_open_manifest'], 0.9 )
   manifest_99 = pw.percent( read_results['remote_open_manifest'], 0.99 )

   revalidate_avg = mean(read_results['remote_open_revalidate'] )
   revalidate_stddev = stddev( read_results['remote_open_revalidate'], revalidate_avg )
   revalidate_median = median( read_results['remote_open_revalidate'] )
   revalidate_90 = pw.percent( read_results['remote_open_revalidate'], 0.90 )
   revalidate_99 = pw.percent( read_results['remote_open_revalidate'], 0.99 )

   open_avg = mean(read_results['remote_open_time'] )
   open_stddev = stddev( read_results['remote_open_time'], open_avg )
   open_median = median( read_results['remote_open_time'] )
   open_90 = pw.percent( read_results['remote_open_time'], 0.90 )
   open_99 = pw.percent( read_results['remote_open_time'], 0.99 )

   read_latency_refresh_avg = mean(results['local_read_latency_refresh'])
   read_latency_refresh_stddev = stddev( results['local_read_latency_refresh'], read_latency_refresh_avg )
   read_latency_refresh_median = median( results['local_read_latency_refresh'] )
   read_latency_refresh_90 = pw.percent( results['local_read_latency_refresh'], 0.90 )
   read_latency_refresh_99 = pw.percent( results['local_read_latency_refresh'], 0.99 )

   read_latency_norefresh_avg = mean(results['local_read_latency_norefresh'])
   read_latency_norefresh_stddev = stddev( results['local_read_latency_norefresh'], read_latency_norefresh_avg )
   read_latency_norefresh_median = median( results['local_read_latency_norefresh'] )
   read_latency_norefresh_90 = pw.percent( results['local_read_latency_norefresh'], 0.90 )
   read_latency_norefresh_99 = pw.percent( results['local_read_latency_norefresh'], 0.99 )

   remote_read_latency_norefresh_avg = mean(read_results['remote_read_latency_internal'])
   remote_read_latency_norefresh_stddev = stddev( read_results['remote_read_latency_internal'], remote_read_latency_norefresh_avg )
   remote_read_latency_norefresh_median = median( read_results['remote_read_latency_internal'] )
   remote_read_latency_norefresh_90 = pw.percent( read_results['remote_read_latency_internal'], 0.90 )
   remote_read_latency_norefresh_99 = pw.percent( read_results['remote_read_latency_internal'], 0.99 )

   remote_read_latency_miss_avg = mean(read_results['remote_read_latency_miss_all'])
   remote_read_latency_miss_stddev = stddev( read_results['remote_read_latency_miss_all'], remote_read_latency_miss_avg )
   remote_read_latency_miss_median = median( read_results['remote_read_latency_miss_all'] )
   remote_read_latency_miss_90 = pw.percent( read_results['remote_read_latency_miss_all'], 0.90 )
   remote_read_latency_miss_99 = pw.percent( read_results['remote_read_latency_miss_all'], 0.99 )

   remote_read_latency_hit_avg = mean(read_results['remote_read_latency_hit'])
   remote_read_latency_hit_stddev = stddev( read_results['remote_read_latency_hit'], remote_read_latency_hit_avg )
   remote_read_latency_hit_median = median( read_results['remote_read_latency_hit'] )
   remote_read_latency_hit_90 = pw.percent( read_results['remote_read_latency_hit'], 0.90 )
   remote_read_latency_hit_99 = pw.percent( read_results['remote_read_latency_hit'], 0.99 )
   
   cdn_read_latency_miss_avg = mean( cdn_results['latency_0'] )
   cdn_read_latency_miss_stddev = stddev( cdn_results['latency_0'], cdn_read_latency_miss_avg )
   cdn_read_latency_miss_median = median( cdn_results['latency_0'] )
   cdn_read_latency_miss_90 = pw.percent( cdn_results['latency_0'], 0.90 )
   cdn_read_latency_miss_99 = pw.percent( cdn_results['latency_0'], 0.99 )

   cdn_read_latency_hit_avg = mean( cdn_results['latency_4'] )
   cdn_read_latency_hit_stddev = stddev( cdn_results['latency_4'], cdn_read_latency_hit_avg )
   cdn_read_latency_hit_median = median( cdn_results['latency_4'] )
   cdn_read_latency_hit_90 = pw.percent( cdn_results['latency_4'], 0.90 )
   cdn_read_latency_hit_99 = pw.percent( cdn_results['latency_4'], 0.99 )

   cdn_read_time_miss_avg = mean( cdn_results['download_time_miss'] )
   cdn_read_time_miss_stddev = stddev( cdn_results['download_time_miss'], cdn_read_time_miss_avg )
   cdn_read_time_miss_median = median( cdn_results['download_time_miss'] )
   cdn_read_time_miss_90 = pw.percent( cdn_results['download_time_miss'], 0.90 )
   cdn_read_time_miss_99 = pw.percent( cdn_results['download_time_miss'], 0.99 )

   cdn_read_time_hit_avg = mean( cdn_results['download_time_hit'] )
   cdn_read_time_hit_stddev = stddev( cdn_results['download_time_hit'], cdn_read_time_hit_avg )
   cdn_read_time_hit_median = median( cdn_results['download_time_hit'] )
   cdn_read_time_hit_90 = pw.percent( cdn_results['download_time_hit'], 0.90 )
   cdn_read_time_hit_99 = pw.percent( cdn_results['download_time_hit'], 0.99 )
   

   ops = [
      [],
      [],
      [],
      []
      ]

   yerror = [
      [],
      None,
      None,
      None
      ]
      
   perc = 0
   y_height = 0
   x_ticks = []
   
   for read in read_res:
      avg = mean(read['remote_read_noconsistency_miss'])
      std = stddev( read['remote_read_noconsistency_miss'], avg )
      med = median( read['remote_read_noconsistency_miss'] )
      p90 = pw.percent( read['remote_read_noconsistency_miss'], 0.90 )
      p99 = pw.percent( read['remote_read_noconsistency_miss'], 0.99 )

      ops[0].append( (perc, avg) )
      ops[1].append( (perc, med) )
      ops[2].append( (perc, p90) )
      ops[3].append( (perc, p99) )

      yerror[0].append( std )

      x_ticks.append( perc )
      perc += 10

      y_height = max(y_height, avg, med, p90, p99)

   pw.make_lines( ops,
                  point_labels=True,
                  point_yoffs=[[0,0,0,0,1,1,5,5,5,5],
                               [0,0,-3,0,0,-5,-5,-5,-5,-5],
                               [0,0,0,0,0,0,0,0,0,0],
                               [0,0,0,0,0,0,0,0,0,0]],
                  x_ticks=x_ticks,
                  x_labels=["10%", "20%", "30%", "40%", "50%", "60%", "70%", "80%", "90%", "100%"],
                  legend_labels=["Average", "Median", "90th Percentile", "99th Percentile"],
                  styles=["o", "^", "+", "x"],
                  title="Remote Read Times after Write",
                  xlabel="Percent Modified",
                  ylabel="Time (log seconds)",
                  x_range=[-1, perc + 10 ],
                  y_range=[3, y_height + 100 ],
                  y_res = 10,
                  y_log = True,
                  legend_pos=4,
                )

   plt.show()
      

   ops = [[0.0, 0.0, 0.0, 0.0],
          [read_latency_norefresh_avg, read_latency_norefresh_median, read_latency_norefresh_90, read_latency_norefresh_99],
          [read_latency_refresh_avg, read_latency_refresh_median, read_latency_refresh_90, read_latency_refresh_99]]
          
   yerror = [[0, 0, 0, 0],
             [read_latency_norefresh_stddev, 0, 0, 0],
             [read_latency_refresh_stddev, 0, 0, 0]]

   y_height = 0
   for op in ops:
      y_height = max( y_height, max(op) )

   pw.make_bars( ops,
              x_labels=["Mean", "Median", "90th percentile", "99th percentile"],
              legend_labels=["Read, disk", "Read, local object", "Read, local object + Revalidate"],
              hatches=['//', '--', '\\\\'],
              bar_labels=ops,
              colors = ['#FFFFFF'] * len(ops),
              yerror_series=yerror,
              x_range=[-0.25, len(ops) + 1 ],
              y_range=[0, y_height + 0.25],
              y_res=0.5,
              legend_pos=2,
              title="Local Read Latency for a 100-block Object with 60KB Blocks",
              xlabel="",
              ylabel="Time (seconds)")

   plt.show()

   ops = [[cdn_read_latency_miss_avg, cdn_read_latency_miss_median, cdn_read_latency_miss_90, cdn_read_latency_miss_99],
          [manifest_avg, manifest_median, manifest_90, manifest_99],
          [remote_read_latency_miss_avg, remote_read_latency_miss_median, remote_read_latency_miss_90, remote_read_latency_miss_99]]

   yerror = [[cdn_read_latency_miss_stddev, 0, 0, 0],
             [manifest_stddev, 0, 0, 0],
             [remote_read_latency_miss_stddev, 0, 0, 0]]

   y_height = 0
   for op in ops:
      y_height = max( y_height, max(op) )

   pw.make_bars( ops,
                 x_labels=["Mean", "Median", "90th percentile", "99th percentile"],
                 legend_labels=["Read, CDN (cold cache)", "Read, manifest (cold cache)", "Read, manifest (cold cache) + Revalidate"],
                 hatches=['++','--','xx','oo'],
                 bar_labels=ops,
                 colors = ['#FFFFFF'] * len(ops),
                 yerror_series = yerror,
                 x_range=[-0.25, len(ops) + 1 ],
                 y_range=[0, y_height + 0.25],
                 y_res=0.5,
                 legend_pos=2,
                 title = "Remote Read Latency for a 100-block Object with 60KB Blocks",
                 xlabel = "",
                 ylabel="Time (seconds)")

   plt.show()
   

   ops = [[read_time_raw_avg, read_time_raw_median, read_time_raw_90, read_time_raw_99],
          [read_time_avg_no_ms, read_time_median_no_ms, read_time_90_no_ms, read_time_99_no_ms],
          [cdn_read_time_hit_avg, cdn_read_time_hit_median, cdn_read_time_hit_90, cdn_read_time_hit_99],
          [remote_read_hit_avg, remote_read_hit_median, remote_read_hit_90, remote_read_hit_99],
          [cdn_read_time_miss_avg, cdn_read_time_miss_median, cdn_read_time_miss_90, cdn_read_time_miss_99],
          [remote_read_miss_avg, remote_read_miss_median, remote_read_miss_90, remote_read_miss_99]]

   yerror = [[read_time_raw_std, 0, 0, 0],
             [read_time_std_no_ms, 0, 0, 0],
             [remote_read_hit_stddev, 0, 0, 0],
             [cdn_read_time_hit_stddev, 0, 0, 0],
             [remote_read_miss_stddev, 0, 0, 0],
             [cdn_read_time_miss_stddev, 0, 0, 0]]

   y_height = 0
   for op in ops:
      y_height = max(y_height, max(op))

   pw.make_bars( ops,
                 x_labels=["Mean", "Median", "90th percentile", "99th percentile"],
                 legend_labels=["Read, disk", "Read, local object", "Read, CDN (warm cache)", "Read, remote object (warm cache)", "Read, CDN (cold cache)", "Read, remote object (cold cache)"],
                 bar_labels=ops,
                 hatches=['//', '--', '\\\\', '++', '**', 'xx'],
                 colors = ['#FFFFFF'] * len(ops),
                 yerror_series = yerror,
                 x_range=[-0.25, len(ops) - 2],
                 y_range=[0.001, y_height + 5000],
                 y_res=10,
                 legend_pos=9,
                 legend_cols=3,
                 title="Read Times for a 100-Block Object with 60KB Blocks",
                 xlabel="",
                 ylabel="Time (log seconds)",
                 y_log=True)

   plt.show()

   ops = [[update_time_raw_avg, update_time_raw_median, update_time_raw_90, update_time_raw_99],
          [update_time_avg_no_ms, update_time_median_no_ms, update_time_90_no_ms, update_time_99_no_ms],
          [update_time_avg, update_time_median, update_time_90, update_time_99]]

   yerror = [[update_time_raw_std, 0, 0, 0],
             [update_time_std_no_ms, 0, 0, 0],
             [update_time_std, 0, 0, 0]]

   y_height = 0
   for op in ops:
      y_height = max(y_height, max(op))

   
   pw.make_bars( ops,
                 x_labels=["Mean", "Median", "90th percentile", "99th percentile"],
                 legend_labels=["Write, disk", "Write, local object", "Write + MS Update, local object"],
                 bar_labels=ops,
                 hatches=['//', '--', '\\\\', '++'],
                 colors = ['#FFFFFF'] * len(ops),
                 yerror_series = yerror,
                 x_range=[-0.25, len(ops) + 1],
                 y_range=[0, y_height + 1],
                 y_res=0.5,
                 legend_pos=2,
                 title="Write Times for a 100-Block Object with 60KB Blocks",
                 xlabel="",
                 ylabel="Time (seconds)"
                 )

   plt.show()

   
   
   """
   cdfs = []
   cdfs.append( make_cdf( results['remote_open_time'], "Remote Open" ) )
   cdfs.append( make_cdf( results['remote_open_revalidate'], "Revalidate" ) )
   cdfs.append( make_cdf( results['remote_open_manifest'], "Download Manifest") )

   show_cdfs( cdfs )

   cdfs = []
   cdfs.append( make_cdf( results['remote_read_latency_miss'], "Read Miss Latency" ) )
   cdfs.append( make_cdf( results['worst_read_latency'], "Worst Read Hit Latency" ) )
   cdfs.append( make_cdf( results['avg_read_latency'], "Avg Read Hit Latency" ) )
   cdfs.append( make_cdf( results['best_read_latency'], "Best Read Hit Latency" ) )

   show_cdfs( cdfs )

   cdfs = []
   #cdfs.append( make_cdf( results['remote_read_miss'], "Read Miss Time" ) )
   cdfs.append( make_cdf( writes['raw_read_time'], "Disk Read" ) )
   cdfs.append( make_cdf( writes['local_read_time'], "Local Read" ) )
   cdfs.append( make_cdf( results['best_read_hit'], "Remote Read Hit" ) )

   show_cdfs( cdfs )
   """





   