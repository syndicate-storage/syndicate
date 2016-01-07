
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

import google
from google.appengine.ext import ndb
import google.appengine.api.memcache as google_memcache
import google.appengine.ext.deferred as google_deferred
from google.appengine.datastore.datastore_query import Cursor as GoogleCursor

def raise_(ex):
   raise ex

class FutureWrapper( ndb.Future ):
   
   state = ndb.Future.FINISHING
   _done = True 
   
   def __init__( self, result ):
      self.result = result
   
   def get_result( self ):
      return self.result
   
   def done( self ):
      return True
   
   def wait( self ):
      pass
   
   def check_success( self ):
      return None
   
   def get_exception( self ):
      return None 
   
   def get_traceback( self ):
      return None
   

# TODO: wrap query for one item into a future 
class FutureQueryWrapper( object ):
   def __init__(self, query_fut):
      self.query_fut = query_fut
   
   def get_result( self ):
      res = self.query_fut.get_result()
      if res != None and len(res) > 0:
         return res[0]
      else:
         return None

   def done( self ):
      return self.query_fut.done()
   
   def wait( self):
      return self.query_fut.wait()
   
   def check_success( self ):
      return self.query_fut.check_success()
   
   def get_exception( self ):
      return self.query_fut.get_exception()
   
   def get_traceback( self ):
      return self.query_fut.get_traceback()
   
      

# aliases for types
Model = ndb.Model
Integer = ndb.IntegerProperty
Float = ndb.FloatProperty
String = ndb.StringProperty
Text = ndb.TextProperty
Key = ndb.KeyProperty
Boolean = ndb.BooleanProperty
Json = ndb.JsonProperty
Blob = ndb.BlobProperty
Computed = ndb.ComputedProperty
Pickled = ndb.PickleProperty
Cursor = GoogleCursor

# aliases for keys
make_key = ndb.Key

def wait_futures( future_list ):
   """
   Wait for all of a list of futures to finish.
   Works with FutureWrapper.
   """
   # see if any of these are NOT futures...then just wrap them into a future object
   # that implements a get_result()
   ret = []
   futs = []
   for f in future_list:
      if f is None:
         continue 
      
      if not isinstance( f, ndb.Future ) and not isinstance( f, FutureWrapper ):
         # definitely not a future
         ret.append( FutureWrapper( f ) )
      else:
         # a future or something compatible
         futs.append( f )
      
   ndb.Future.wait_all( futs )
   return futs + ret


deferred = google_deferred
concurrent = ndb.tasklet
concurrent_return = (lambda x: (raise_(ndb.Return( x ))))

# asynchronous operations
get_multi_async = ndb.get_multi_async
put_multi_async = ndb.put_multi_async

# synchronous operations
get_multi = ndb.get_multi
put_multi = ndb.put_multi
delete_multi = ndb.delete_multi

# aliases for memcache
memcache = google_memcache

# aliases for transaction
transaction = ndb.transaction
transaction_async = ndb.transaction_async
transactional = ndb.transactional

# alises for query predicates
opAND = ndb.AND
opOR = ndb.OR

# aliases for top-level asynchronous loop
toplevel = ndb.toplevel

# aliases for common exceptions 
RequestDeadlineExceededError = google.appengine.runtime.DeadlineExceededError
APIRequestDeadlineExceededError = google.appengine.runtime.apiproxy_errors.DeadlineExceededError
URLRequestDeadlineExceededError = google.appengine.api.urlfetch_errors.DeadlineExceededError
TransactionFailedError = google.appengine.ext.db.TransactionFailedError
