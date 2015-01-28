"""
   Copyright 2014 The Trustees of Princeton University

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

import storage.storagetypes as storagetypes

# ----------------------------------
def benchmark( category, benchmark_data, func ):
   """
   Call function func, and time how long it takes to run.
   Store it in the given benchmark_data (dict) under the key 'category'.
   Return the result of func.
   """
   start = storagetypes.get_time()
   rc = func()
   t = storagetypes.get_time() - start
   
   if not benchmark_data.has_key(category):
      benchmark_data[category] = []
      
   benchmark_data[category].append( t )
   
   return rc
   

# ----------------------------------
def benchmark_headers( benchmark_data ):
   """
   Convert benchmark data from calls to benchmark() into a dictionary of HTTP headers,
   to be fed into a request handler.
   """
   
   ret = {}
   for category in benchmark_data.keys():
      if type(benchmark_data[category]) != type(list):
         benchmark_data[category] = [benchmark_data[category]]
         
      if len(benchmark_data[category]) > 0:
         ret[category] = ",".join( [str(x) for x in benchmark_data[category]] )
      
   return ret