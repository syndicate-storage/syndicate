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


def get_count(name, num_shards):
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
    total = memcache.get(name)
    if total is None:
        total = 0
        all_keys = _all_keys(name, num_shards)
        for counter in ndb.get_multi(all_keys, use_cache=False, use_memcache=False):
            if counter is not None:
                total += counter.count
                do_cache = True 
                
        if do_cache:
            memcache.set(name, total)
            
    return total

def flush_cache(name):
   """
   Flush the cache for this counter 
   """
   memcache.delete(name)
   

def increment(name, num_shards):
    """
    Increment the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
    _change(name, num_shards, 1)


def decrement(name, num_shards):
   """
    Decrement the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
   _change(name, num_shards, -1)


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


def increment_async(name, num_shards):
    """
    Asynchronously increment the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
        
    Return:
        A future for the transaction 
    """
    return _change_async(name, num_shards, 1)


def decrement_async(name, num_shards):
   """
    Asynchronously decrement the value for a given sharded counter.
    This will create the counter if it does not exist.

    Args:
        name: The name of the counter.
        num_shards: the number of shards in the counter
    """
   return _change_async(name, num_shards, -1)


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


def _change_async(name, num_shards, value):
    """
    Asynchronous transaction helper to increment the value for a given sharded counter.

    Also takes a number of shards to determine which shard will be used.

    Args:
        name: The name of the counter.
        num_shards: How many shards to use.
        
    Returns:
      A future whose result is the transaction
    """
    
    @ndb.tasklet
    def txn():
      
      if value != 1 and value != -1:
         raise Exception("Invalid shard count value %s" % value)

      memcache.delete( name )
      
      index = random.randint(0, num_shards - 1)
      
      shard_key_string = SHARD_KEY_TEMPLATE.format(name, index)
      
      counter = yield GeneralCounterShard.get_by_id_async(shard_key_string)
      if counter is None:
         counter = GeneralCounterShard(id=shard_key_string)
         
      counter.count += value
      yield counter.put_async()
      
      memcache.delete( name )
   
    return ndb.transaction_async( txn )


def _change(name, num_shards, value):
   """
   Synchronous wrapper around _change_async
   """
   tf = _change_async(name, num_shards, value)
   tf.wait()
   return

