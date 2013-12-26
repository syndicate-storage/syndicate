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

# modified from https://developers.google.com/appengine/articles/sharding_counters

import random

import storagetypes

from google.appengine.api import memcache
from google.appengine.ext import ndb


SHARD_KEY_TEMPLATE = 'shard-{}-{:d}'
NUM_SHARDS=20

class ShardConfig(storagetypes.Object):
    """Tracks the number of shards for each named value."""
    num_shards = storagetypes.Integer(default=NUM_SHARDS, indexed=False)
    
    name = storagetypes.Text(default=None)
    
    @classmethod
    def shard_all_keys(cls, name):
        """Returns all possible keys for the maximum name given the config.

        Args:
            name: The name of the maximum.

        Returns:
            The full list of ndb.Key values corresponding to all the possible
                maximum shards that could exist.
        """
        config = cls.get_or_insert(name)
        
        shard_key_strings = [SHARD_KEY_TEMPLATE.format(name, index)
                             for index in range(config.num_shards)]
        return [ndb.Key(Shard, shard_key_string)
                for shard_key_string in shard_key_strings]
                

class Shard(storagetypes.Object):
    
    mtime_sec = storagetypes.Integer(default=0, indexed=False)
    mtime_nsec = storagetypes.Integer(default=0, indexed=False)
    size = storagetypes.Integer(default=0, indexed=False )
    num_children = storagetypes.Integer( default=0, indexed=False )

    

def modtime_max( m1, m2 ):
   if m1.mtime_sec < m2.mtime_sec:
      return m2

   elif m1.mtime_sec > m2.mtime_sec:
      return m1

   elif m1.mtime_nsec < m2.mtime_nsec:
      return m2

   elif m1.mtime_nsec > m2.mtime_nsec:
      return m1

   return m1
   
def get_modtime( config, name ):
    """Retrieve the value for a given sharded maximum.

    Args:
        name: The name of the counter.

    Returns:
        Integer; the maximum of all sharded maximums for the given
            maximum name.
    """
    mm = memcache.get(name)
    if mm is None:
        all_keys = ShardConfig.all_keys_config( config, name )

        for value in ndb.get_multi(all_keys):
           if value is not None:
              if mm != None:
                 mm = value
              else:
                 mm = modtime_max( mm, value )
                 
        memcache.add(name, mm, 60)
    return (mm.mtime_sec, mm.mtime_nsec)



def get_modtime_from_shards( name, results ):
   mm = None
   for value in results:
      if mm == None:
         mm = value
         continue

      if value == None:
         continue

      mm = modtime_max( mm, value )

   if mm != None:
      memcache.add( name, mm, 60 )
      return mm
   else:
      return None

      
def get_modtime_from_futures( name, futs ):
   results = [f.get_result() for f in futs]
   return get_modtime_from_shards( name, results )


   
def get_modtime_async(config, name ):
   all_keys = ShardConfig.all_keys_config(config, name)

   all_futures = ndb.get_multi_async(all_keys)
   return all_futures

    
def set_modtime(config, name, mtime_sec, mtime_nsec):
    """Increment the value for a given sharded counter.

    Args:
        name: The name of the counter.
    """
    return ndb.transaction( lambda: set_modtime_notrans(config, name, config.num_shards, mtime_sec, mtime_nsec, sync=True) )
    

def set_modtime_notrans(config, name, mtime_sec, mtime_nsec, sync=True):
    """Transactional helper to increment the value for a given sharded counter.

    Also takes a number of shards to determine which shard will be used.

    Args:
        name: The name of the counter.
        num_shards: How many shards to use.
    """
    num_shards = config.num_shards
    
    index = random.randint(0, num_shards - 1)

    shard_key_string = SHARD_KEY_TEMPLATE.format(name, index)
    
    mtime = Shard.get_by_id(shard_key_string)
    if mtime is None:
       mtime = Shard(id=shard_key_string)

    mtime.mtime_sec = mtime_sec
    mtime.mtime_nsec = mtime_nsec

    memcache.set( name, mtime )

    fut = None
    if sync:
       mtime.put()
    else:
       fut = mtime.put_async()

    return fut
      

@ndb.transactional
def increase_shards(name, num_shards):
    """Increase the number of shards for a given sharded maximum.

    Will never decrease the number of shards.

    Args:
        name: The name of the maximum.
        num_shards: How many shards to use.
    """
    config = ShardConfig.get_or_insert(name)
    if config.num_shards < num_shards:
       config.num_shards = num_shards
       config.put()
        