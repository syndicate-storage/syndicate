
"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

from google.appengine.ext import ndb
import google.appengine.api.memcache as google_memcache
import google.appengine.ext.deferred as google_deferred

def raise_(ex):
   raise ex

class FutureWrapper( object ):
   def __init__( self, result ):
      self.result = result
      self.state = ndb.Future.FINISHING
   
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
String = ndb.StringProperty
Text = ndb.TextProperty
Key = ndb.KeyProperty
Boolean = ndb.BooleanProperty
Json = ndb.JsonProperty

# aliases for keys
make_key = ndb.Key

# aliases for asynchronous operations
def wait_futures( future_list ):
   # see if any of these are NOT futures...then just wrap them into a future object
   # that implements a get_result()
   ret = []
   futs = []
   for f in future_list:
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
transactional = ndb.transactional

# alises for query predicates
opAND = ndb.AND
opOR = ndb.OR

# aliases for top-level asynchronous loop
toplevel = ndb.toplevel