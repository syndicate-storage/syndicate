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

# borrowed from https://developers.google.com/appengine/articles/sharding_counters

import random

from google.appengine.api import memcache
from google.appengine.ext import ndb

import backends as backend


SHARD_KEY_TEMPLATE = 'shardcounter-{}-{:d}'

def _all_keys(name, num_shards):
   """Returns all possible keys for the counter name given the config.

   Args:
      name: The name of the counter.

   Returns:
      The full list of ndb.Key values corresponding to all the possible
            counter shards that could exist.
   """
   
   shard_key_strings = [SHARD_KEY_TEMPLATE.format(name, index) for index in range(num_shards)]
   return [ndb.Key(GeneralCounterShard, shard_key_string) for shard_key_string in shard_key_strings]


class GeneralCounterShard(ndb.Model):
    """Shards for each named counter."""
    count = ndb.IntegerProperty(default=0)

@ndb.tasklet
def _get_count_async(name, key_gen, use_memcache=True):
    """Retrieve the value for a given sharded counter.

    Args:
        name: The name of the counter.
        num_shards: the number of shards
    Returns:
        Integer; the cumulative count of all sharded counters for the given
            counter name.
    """
    
    # only cache if it exists at all 
    do_cache = False
    
    total = None 
    
    if use_memcache:
      total = memcache.get(name)
      
    if total is None:
        total = 0
        all_keys = key_gen()
        counters = yield ndb.get_multi_async( all_keys, use_cache=False, use_memcache=False )
        for counter in counters:
            if counter is not None:
                total += counter.count
                do_cache = True 
                
        if do_cache and use_memcache:
            memcache.set(name, total)
            
    raise ndb.Return( total )


def get_count(name, num_shards, use_memcache=True):
   count_fut = _get_count_async( name, lambda: _all_keys( name, num_shards ), use_memcache=use_memcache )
   return count_fut.get_result()
 

def get_count_async(name, num_shards, use_memcache=True):
   return _get_count_async( name, lambda: _all_keys(name, num_shards), use_memcache=use_memcache )


def count_from_futures( name, futs, do_cache=True ):
   """
   Get the value of a counter from a list of futures returned from get_count_async 
   """
   
   count = 0
   for f in futs:
      count += f.get_result()
   
   if do_cache:
      memcache.set(name, count)
   
   return count


def flush_cache(name):
   """
   Flush the cache for this counter 
   """
   memcache.delete(name)
   

def increment(name, num_shards, delta=1, do_transaction=True, use_memcache=True):
    """
    Increment the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
    return _change(name, num_shards, delta, do_transaction=True, use_memcache=use_memcache)
 

def decrement(name, num_shards, delta=-1, do_transaction=True, use_memcache=True ):
   """
    Decrement the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
   return _change(name, num_shards, delta, do_transaction=True, use_memcache=use_memcache )


def delete(name, num_shards):
   """
   Delete the named counter and all of its shards.

   Args:
       name: The name of the counter.
       num_shards: the number of shards in the counter
   """
   
   all_keys = _all_keys(name, num_shards)
   ndb.delete_multi( all_keys )
   
   memcache.delete( name )
   
   return


def create(name, do_transaction=False ):
   """
   create a named counter
   """
   return _change(name, 1, 0, do_transaction=do_transaction )


def increment_async(name, num_shards, delta=1, do_transaction=True, use_memcache=True):
    """
    Asynchronously increment the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
        
    Return:
        A future for the transaction 
    """
    return _change_async(name, num_shards, delta, do_transaction=do_transaction, use_memcache=use_memcache)


def decrement_async(name, num_shards, delta=-1, do_transaction=True, use_memcache=True):
   """
    Asynchronously decrement the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
   return _change_async(name, num_shards, delta, do_transaction=do_transaction, use_memcache=use_memcache)


def delete_async(name, num_shards):
   """
   Asynchronously delete the named counter and all of its shards.

   Args:
       name: The name of the counter.
       num_shards: the number of shards in the counter
       
   Return:
       A list of Futures for each entity making up the counter.
   """
   
   all_keys = _all_keys(name, num_shards)
   delete_futs = ndb.delete_multi_async( all_keys )
   
   memcache.delete( name )
   
   return delete_futs


def create_async(name, do_transaction=False ):
   """
   create a named counter, asynchronously
   """
   return _change_async(name, 1, 0, do_transaction=do_transaction )


def _change_async(name, num_shards, value, do_transaction=True, use_memcache=True):
    """
    Asynchronous transaction helper to increment the value for a given sharded counter.

    Also takes a number of shards to determine which shard will be used.

    Args:
        name: The name of the counter.
        num_shards: How many shards to use.
        
    """
    
    @ndb.tasklet
    def txn():
      
      index = random.randint(0, num_shards - 1)
      
      shard_key_string = SHARD_KEY_TEMPLATE.format(name, index)
      
      counter = yield GeneralCounterShard.get_by_id_async(shard_key_string)
      if counter is None:
         counter = GeneralCounterShard(id=shard_key_string)
         
      counter.count += value
      yield counter.put_async()
      
      if use_memcache:
         if value > 0:
            memcache.incr( name, delta=value )
         elif value < 0:
            memcache.decr( name, delta=-value )
      
      else:
         memcache.delete( name )
         
      raise ndb.Return( True )
   
    if do_transaction:
      return ndb.transaction_async( txn )
   
    else:
      return txn()


def _change(name, num_shards, value, do_transaction=True, use_memcache=True ):
   """
   Synchronous wrapper around _change_async
   """
   tf = _change_async(name, num_shards, value, do_transaction=do_transaction, use_memcache=use_memcache)
   tf.wait()
   return tf.get_result()
