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

import types
import math
import random

class datum:
   def __init__(self, value, name_info):
      self.name_info = name_info
      self.value = value

   def __repr__(self):
      return "datum(%s, %s)" % (self.value, self.name_info)

   def __str__(self):
      return self.__repr__()

   def __lt__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value < other
         
      else:
         return self.value < other.value

   def __le__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value <= other

      else:
         return self.value <= other.value

   def __gt__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value > other

      else:
         return self.value > other.value
         
   def __ge__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value >= other

      else:
         return self.value >= other.value

   def __ne__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value != other

      else:
         return self.value != other.value

   def __eq__(self, other ):
      if type(other) == types.IntType or type(other) == types.FloatType:
         return self.value == other

      else:
         return self.value == other.value
      

def mean( x ):
   # x is [datum]
   data = [r.value for r in x]   
   return datum( sum(data) / len(data), None )
   
def median( x ):
   # x is [datum]
   return x[ len(x)/2 ]

def stddev( ll, avg=0.0 ):
   # x is [datum]
   data = [r.value for r in ll]
   
   val = 0.0
   for x in data:
      val += (x - avg) * (x - avg)

   val /= len(data)

   val = math.sqrt( val )
   return datum( val, None )

def percentile( data, p=0.5 ):
   # data is [datum]
   off = int(len(data) * p)
   names = [data[i].name_info for i in xrange(off, len(data))]
   return datum( data[off].value, names )   

exp_delim = "--------------------------------"

def read_block( lines, start ):
   i = start + 1
   while i < len(lines) and not lines[i].startswith(exp_delim):
      i += 1

   block = lines[start + 1: min(len(lines),i)]
   name = " ".join( lines[start].split()[1:] )
   
   return (name, block)


def parse_block( block ):
   data = {}
   for line in block:
      parts = line.split()
      if len(parts) == 0 or parts[0] != 'DATA':
         continue

      value = float(parts[-1])
      key = " ".join( parts[1:-1] )

      data[key] = value

         
   #print data
   return data


def parse_output( lines ):
   if lines == None or len(lines) <= 0:
      return None

   i = 0
   ret = {}

   while True:
      while i < len(lines) and not lines[i].startswith( exp_delim ):
         i += 1

      if i >= len(lines):
         break

      block_name, block_lines = read_block( lines, i )
      if len(block_name) == 0:
         break

      i += len(block_lines) + 1

      ret[block_name] = parse_block( block_lines )

   return ret
      


def get_data( outputs_dir ):
   import os
   files = os.listdir( outputs_dir )

   data = {}
   for fname in files:

      fd = open( os.path.join(outputs_dir,fname), "r" )
      buf = fd.read()
      fd.close()

      lines = buf.split("\n")
      for i in xrange(0,len(lines)):
         lines[i] = lines[i].strip()

      dat = parse_output(lines)

      if dat != None:
         data[fname] = dat

   return data
   
   
def get_results( data ):
   
   results = {}
   for fname in data.keys():
      record = data[fname]

      for step in record.keys():
         step_record = record[step]

         if not results.has_key(step):
            results[step] = {}
            
         for k in step_record.keys():
            if not results[step].has_key(k):
               results[step][k] = []

            results[step][k].append( datum(step_record[k], fname) )

   for step in results.keys():
      for key in results[step].keys():
         results[step][key].sort()
         
   return results


def default_fields( results ):
   # default field names from results
   fields = {}
   for step in results:
      rec = results[step]
      fields[step] = []
      for k in rec.keys():
         fields[step].append( k )

   return fields



def results_apply( results, func, func_args, fields=None ):
   # apply a function (with arguments) on a set of fields and return a copy
   if fields == None:
      fields = default_fields( results )

   ret = {}
   for step in fields.keys():
      ret[step] = {}
      for key in fields[step]:
         args = {}
         try:
            args = func_args[step][key]
         except:
            pass

         ret[step][key] = func(results[step][key], **args)

   return ret
   

def fields_value( fields, name, p ):
   # generate a set of function arguments for results_apply, given fields, and given a value
   ps = {}
   for step in fields:
      ps[step] = {}
      for key in fields[step]:
         ps[step][key] = {name: p}

   return ps


def percentiles_closure( p ):
   # generate a function that, when called with results and fields, will apply the given percentile to results
   def percentiles_func( results, fields=None ):
      if fields == None:
         fields = default_fields(results)

      ps = fields_value( fields, "p", p )
      return results_apply( results, percentile, ps, fields )

   return percentiles_func
   

def error_values_closure( results, function, input_data, fields=None ):
   # apply a value function to results, given the percentile desired
   def error_values( results, input_data, fields=None ):
      return results_apply( results, function, input_data, fields )

   return error_values
   
   
def means( results, fields=None ):
   # apply a mean function to results
   return results_apply( results, mean, {}, fields )
   

def stddevs( results, mean_data, fields=None ):
   # apply a stddev function to results, given mean_data
   md = {}
   for step in mean_data.keys():
      md[step] = {}   
      for key in mean_data[step].keys():
         md[step][key] = {'avg': mean_data[step][key].value}

   evc = error_values_closure( results, stddev, md, fields )
   return evc( results, md, fields )
   

def zero_error( results, ignored, fields=None ):
   evc = error_values_closure( results, lambda data: datum(0, None), {}, fields )
   return evc( results, {}, fields )

   
def medians( results, fields=None ):
   # apply median function to results
   return results_apply( results, median, {}, fields )

   
def percentiles( results, p, fields=None ):
   # apply a percentile function to results, given the percentile desired
   pf = percentiles_closure( p )
   return pf( results, fields )



def aggregate_data( results, fields=None, methods=[] ):
   # apply many data methods to results to form an aggregate
   # method signature: method( results, fields )
   
   if fields == None:
      fields = common.default_fields()

   # {step: {key: [method_result_1, method_result_2, ...], ...}, ...}
   aggregate = {}

   for method in methods:
      data = method( results, fields )

      for step in data.keys():

         if not aggregate.has_key(step):
            aggregate[step] = {}

         for key in data[step]:

            if not aggregate[step].has_key( key ):
               aggregate[step][key] = []

            aggregate[step][key].append( data[step][key] )


   return aggregate

   
def aggregate_error( results, data, fields=None, methods=[] ):
   # apply many error methods to results.
   # method signature: method( results, data, fields )

   if fields == None:
      fields = common.default_fields()

   # {step: {key: [error_result_1, error_result_1, ...], ...}, ...}
   aggregate = {}

   i = 0
   for method in methods:

      # get the aggregated data corresponding to this error method
      err_input = {}
      for step in data.keys():
         err_input[step] = {}
         for key in data[step].keys():
            err_input[step][key] = data[step][key][i]

      i += 1
      
      err = method( results, err_input, fields )
      op = []

      for step in err.keys():
         if not aggregate.has_key( step ):
            aggregate[step] = {}

         for key in err[step]:
            if not aggregate[step].has_key( key ):
               aggregate[step][key] = []

            aggregate[step][key].append( err[step][key] )


   return aggregate

   
def aggregate_series( results, names, data_methods, error_methods ):
   # generate data from results, given names = [(experiment_step, experiment_key), ...] and *_methods = [method, ...] and kwargs = {arguments to make_bars}

   fields = {}
   for (step, key) in names:
      if not fields.has_key(step):
         fields[step] = []

      fields[step].append( key )

   aggregated_data = None
   aggregated_error = None

   if data_methods != None and len(data_methods) > 0:
      aggregated_data = aggregate_data( results, fields, data_methods )

   if error_methods != None and len(error_methods) > 0:
      aggregated_error = aggregate_error( results, aggregated_data, fields, error_methods )

   return (aggregated_data, aggregated_error)


def graph_default_order( aggregate ):
   order = []
   # derive bar order from aggregate
   for step in aggregate.keys():
      for key in aggregate[step].keys():
         order.append( (step, key) )

   return order

def graph_legend_labels( order ):
   legend_labels = []
   for (step, key) in order:
      legend_labels.append( "%s: %s" % (step, key) )
   return legend_labels
   
def graph_series( aggregate, yerror_aggregate, order=None ):
   # extract aggregate data and put it into 2D value arrays
   data_series = []
   yerror_series = []

   if order == None:
      order = graph_default_order( aggregate )

   i = 0
   for (step, key) in order:
      data_series.append( [r.value for r in aggregate[step][key]] )

      has_error = False

      if yerror_aggregate != None:
         if yerror_aggregate.has_key(step):
            if yerror_aggregate[step].has_key(key):
               if len(yerror_aggregate[step][key]) != len(aggregate[step][key]):
                  raise Exception ("Mismatched aggregate and yerror lengths (%d vs %d) for (%s, %s)" % (len(yerror_aggregate[step][key]), len(aggregate[step][key]), step, key))

               yerror_series.append( [r.value for r in yerror_aggregate[step][key]] )
               has_error = True

      if not has_error:
         yerror_series.append( [0] * len(aggregate[step][key]) )

   return (data_series, yerror_series)

   
def experiment_write_block( outputs, fname, step ):
   import os
   fpath = os.path.join( outputs, fname )
   fd = open( fpath, "a" )
   fd.write( exp_delim + " " + step + "\n" )
   fd.close()
   
def experiment_write_data( outputs, fname, key, value ):
   import os
   fpath = os.path.join( outputs, fname )
   fd = open( fpath, "a" )
   fd.write("DATA %s %s\n" % (key, value) )
   fd.close()

def mock_experiment( mock_outputs, step_count, key_count ):
   import os
   try:
      os.mkdir( mock_outputs )
   except:
      raise Exception("Mock experiment directory %s already exists.  Remove it first" % mock_outputs )


   mock_experiment_names = [ "host-%s.txt" % i for i in xrange(0,100) ]
   series_names = []

   for fname in mock_experiment_names:
      # do a mock exeriment
      for i in xrange(0,step_count):
         step = "step %s" % (i + 1)

         experiment_write_block( mock_outputs, fname, step )

         for j in xrange(0,key_count):
            key = "key %s" % (j + 1)

            series_names.append( (step,key) )
            experiment_write_data( mock_outputs, fname, key, float(random.randint(0,1000)) / 100.0 )

      experiment_write_block( mock_outputs, fname, "" )

   data = get_data( mock_outputs )
   results = get_results( data )

   aggregated_data, aggregated_error = aggregate_series( results, series_names, [means, medians, percentiles_closure(0.9), percentiles_closure(0.99)], [stddevs, zero_error, zero_error, zero_error] )

   return (aggregated_data, aggregated_error)
   

if __name__ == "__main__":

   import pprint
   import sys
   import os
   
   try:
      mock_outputs = sys.argv[1]
   except:
      mock_outputs = ".mock_experiment"

   try:
      os.mkdir( mock_outputs )
   except:
      raise Exception("Mock experiment directory %s already exists.  Remove it first" % mock_outputs )


   mock_experiment_names = [ "host-%s.txt" % i for i in xrange(0,100) ]
   series_names = []

   for fname in mock_experiment_names:
      # do a mock exeriment
      for i in xrange(0,10):
         step = "step %s" % (i + 1)

         experiment_write_block( mock_outputs, fname, step )
         
         for j in xrange(0,10):
            key = "key %s" % (j + 1)

            series_names.append( (step,key) )
            experiment_write_data( mock_outputs, fname, key, float(random.randint(0,1000)) / 100.0 )
            
      experiment_write_block( mock_outputs, fname, "" )

   data = get_data( mock_outputs )
   results = get_results( data )

   pp = pprint.PrettyPrinter()
   
   print "data"
   pp.pprint( data )

   print "results"
   pp.pprint( results )

   aggregated_data, aggregated_error = aggregate_series( results, series_names, [means, medians, percentiles_closure(0.9), percentiles_closure(0.99)], [stddevs, zero_error, zero_error, zero_error] )

   print "aggregated data"
   pp.pprint( aggregated_data )

   print "aggregated error"
   pp.pprint( aggregated_error )

   graph_data, graph_error = graph_series( aggregated_data, aggregated_error )

   print "graph data"
   pp.pprint( graph_data )

   print "error data"
   pp.pprint( error_data )
   
   