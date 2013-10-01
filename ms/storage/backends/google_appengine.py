
"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

from google.appengine.ext import ndb
import google.appengine.api.memcache as google_memcache
import google.appengine.ext.deferred as google_deferred

def raise_(ex):
   raise ex

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
wait_futures = ndb.Future.wait_all
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