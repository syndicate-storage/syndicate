#!/usr/bin/pyhon

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
import storage.shardcounter as shardcounter

import protobufs.ms_pb2 as ms_pb2
import logging

import random
import os

import types
import errno
import time
import datetime
import collections
import pickle
import base64

from common.msconfig import *
   
   
class MSEntryIndexNode( storagetypes.Object ):
   """
   Directory entry index node.
   """
   
   parent_id = storagetypes.String( default="None" )
   file_id = storagetypes.String( default="None", indexed=False )
   volume_id = storagetypes.Integer( default=-1 )
   dir_index = storagetypes.Integer( default=-1 )
   generation = storagetypes.Integer( default=-1 )
   alloced = storagetypes.Boolean( default=False )
   
   nonce = storagetypes.Integer( default=0, indexed=False )     # for uniqueness
   

class MSEntryEntDirIndex( MSEntryIndexNode ):
   """
   (file_id --> dir_index) relation
   NOTE: define this separately so it can be indexed independently of other node types.
   """
   
   @classmethod 
   def make_key_name( cls, volume_id, file_id ):
      # name for index nodes that resolve file_id to dir_index (entry index node)
      return "MSEntryEntDirIndex: volume_id=%s,file_id=%s" % (volume_id, file_id)
   
   
class MSEntryDirEntIndex( MSEntryIndexNode ):
   """
   (dir_index --> file_id) relation
   NOTE: define this separately so it can be indexed independently of other node types
   """
   @classmethod 
   def make_key_name( cls, volume_id, parent_id, dir_index ):
      # name for index nodes that resolve dir_index to file_id (directory index node)
      return "MSEntryDirEntIndex: volume_id=%s,parent_id=%s,dir_index=%s" % (volume_id, parent_id, dir_index)
   

class MSEntryIndex( storagetypes.Object ):
   """
   
   """
   
   NUM_COUNTER_SHARDS = 20
   
   @classmethod 
   def __parent_child_counter_name( cls, volume_id, parent_id ):
      return "MSEntryIndex: volume_id=%s,parent_id=%s" % (volume_id, parent_id)
   
   @classmethod
   @storagetypes.concurrent 
   def __update_index_node_async( cls, volume_id, parent_id, file_id, dir_index, alloced, **attrs ):
      """
      Set the allocation status of a directory index node (but not its matching entry index node).
      
      Return 0 on success
      Return -EINVAL if the given file_id doesn't match the directory index node's file_id 
      Return -EEXIST if the given directory index node's allocation status is the same as alloced
      """
      
      index_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, dir_index )
      index_key = storagetypes.make_key( MSEntryDirEntIndex, index_key_name )
      old_alloced = None
      
      idx = yield index_key.get_async()
      
      if idx is None:
         old_alloced = alloced
         idx = MSEntryDirEntIndex( key=index_key, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, alloced=alloced, **attrs )
      
      else:
         if idx.file_id != file_id:
            # wrong node
            storagetypes.concurrent_return( -errno.EINVAL )
         
         old_alloced = idx.alloced
      
      if old_alloced != alloced:
         # changing allocation status
         idx.populate( -1, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, alloced=alloced, **attrs )
         yield idx.put_async()
      
         storagetypes.concurrent_return( 0 )
      
      else:
         storagetypes.concurrent_return( -errno.EEXIST )
      
   
   @classmethod
   @storagetypes.concurrent
   def __update_or_alloc_async( cls, volume_id, parent_id, file_id, dir_index, generation, alloced ):
      """
      Update or allocate the index node pair and/or set the directory index node's allocation status, asynchronously.
      If the directory index node does not exist, it and its entry index node will be created and the allocation status set accordingly.
      If the directory index node exists, but has a different allocation status, then the allocation status will be set accordingly.
      
      If we succeed in allocating a new index node, incremenet the number of children in the parent directory.
      
      Return True on success.
      Return False if the index node existed, but the file_id did not match its record or the allocation status did not change.
      """
      
      index_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, dir_index )
      
      idx = None
      nonce = random.randint( -2**63, 2**63 - 1 )
      result = True
      
      if idx is None:
         idx = yield MSEntryDirEntIndex.get_or_insert_async( index_key_name, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, generation=generation, alloced=alloced, nonce=nonce )
      
      if idx.nonce == nonce:
         # created.
         
         """
         if alloced:
            logging.info("Directory /%s/%s: allocated index slot for /%s/%s at %s" % (volume_id, parent_id, volume_id, file_id, dir_index))
         else:
            logging.info("Directory /%s/%s: freed index slot at %s" % (volume_id, parent_id, dir_index))
         """
         
         # need to create an entry index node as well.
         entry_key_name = MSEntryEntDirIndex.make_key_name( volume_id, file_id )
         entry_key = storagetypes.make_key( MSEntryEntDirIndex, entry_key_name )
         entry_idx = MSEntryEntDirIndex( key=entry_key, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, generation=generation, alloced=alloced, nonce=nonce )
         
         yield entry_idx.put_async()
         
         # storagetypes.memcache.set( entry_key_name, entry_idx )
      
      else:
         
         # already exists.  changing allocation status?
         if idx.alloced != alloced:
            # allocation status needs to be changed
            # want to change allocation status
            rc = yield storagetypes.transaction_async( lambda: cls.__update_index_node_async( volume_id, parent_id, file_id, dir_index, alloced, generation=generation ), xg=True )
            
            if rc == 0:
               result = True 
               
            else:
               logging.error("__update_index_node_async(/%s/%s file_id=%s dir_index=%s alloced=%s) rc = %s" % (volume_id, parent_id, file_id, dir_index, alloced, rc ))
               result = False
         
         else:
            
            if alloced and idx.file_id != file_id:
               # collision
               logging.error("Directory /%s/%s: collision inserting /%s/%s at %s (occupied by /%s/%s)" % (volume_id, parent_id, volume_id, file_id, dir_index, volume_id, idx.file_id))
               result = False
               
            else:
               # created/set correctly
               result = True
      
      """
      if result:
         storagetypes.memcache.set( index_key_name, idx )
      """
      storagetypes.concurrent_return( result )
   
   
   @classmethod 
   def __alloc( cls, volume_id, parent_id, file_id, dir_index, generation, async=False ):
      """
      Get or create an allocated index node, for the given directory index.
      Return True if we succeeded.
      Return False if the node already exists for this dir_index value, or the file ID is wrong.
      """
      
      result_fut = cls.__update_or_alloc_async( volume_id, parent_id, file_id, dir_index, generation, True )
      
      if not async:
         storagetypes.wait_futures( [result_fut] )
         return result_fut.get_result()
      
      else:
         return result_fut 
      
      
   @classmethod 
   def __free( cls, volume_id, parent_id, file_id, dir_index, async=False ):
      """
      Get or create a free index node, for a given directory index.
      Return True if we succeeded.
      Return False if the node already exists for this dir_index value, or if it's already freed (or the file ID is wrong)
      """
      result_fut = cls.__update_or_alloc_async( volume_id, parent_id, file_id, dir_index, -1, False )
      
      if not async:
         storagetypes.wait_futures( [result_fut] )
         return result_fut.get_result()
      
      else:
         return result_fut 
   
   
   @classmethod 
   def __num_children_inc( cls, volume_id, parent_id, num_shards, do_transaction=True, async=False ):
      """
      Increment the number of children in a directory.
      """
      
      counter_name = cls.__parent_child_counter_name( volume_id, parent_id )
      
      if async:
         fut = shardcounter.increment_async( counter_name, num_shards, do_transaction=do_transaction, use_memcache=False )
         return fut
      
      else:
         shardcounter.increment( counter_name, num_shards, do_transaction=do_transaction, use_memcache=False )
         return 0
      
      
   @classmethod 
   def __num_children_dec( cls, volume_id, parent_id, num_shards, do_transaction=True, async=False ):
      """
      Decrement the number of children in a directory 
      """
      
      counter_name = cls.__parent_child_counter_name( volume_id, parent_id )
      
      if async:
         fut = shardcounter.decrement_async( counter_name, num_shards, do_transaction=do_transaction, use_memcache=False )
         return fut
      
      else:
         shardcounter.decrement( counter_name, num_shards, do_transaction=do_transaction, use_memcache=False )
         return 0
         
   @classmethod 
   def __num_children_delete( cls, volume_id, parent_id, num_shards, async=False ):
      """
      Delete a shard counter for the number of children.
      """
      
      counter_name = cls.__parent_child_counter_name( volume_id, parent_id )
      
      if async:
         fut = shardcounter.delete_async( counter_name, num_shards )
         return fut 
      
      else:
         shardcounter.delete( counter_name, num_shards )
         return 0
   
   @classmethod 
   @storagetypes.concurrent 
   def __read_node( cls, file_id, index, idx_key, check_file_id=True ):
      """
      Read a dir-index node, given its key. 
      Return (rc, idx):
      * return -ENOENT if the index node doesn't exist 
      * return -EINVAL if the file IDs don't match, and check_file_id is true
      * return -EPERM if the directory indexes don't match
      """
      
      idx = yield idx_key.get_async( use_cache=False, use_memcache=False )
      
      if idx is None:
         storagetypes.concurrent_return( (-errno.ENOENT, None) )
      
      if check_file_id and idx.file_id != file_id:
         storagetypes.concurrent_return( (-errno.EINVAL, None) )
      
      if idx.dir_index != index:
         storagetypes.concurrent_return( (-errno.EPERM, None) )
      
      storagetypes.concurrent_return( (0, idx) )
   
   
   @classmethod 
   def __read_dirent_node( cls, volume_id, parent_id, file_id, index, async=False, check_file_id=True ):
      """
      Read a node key, and verify that it is consistent.
      Return (rc, idx)
      """
      
      idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, index )
      idx_key = storagetypes.make_key( MSEntryDirEntIndex, idx_key_name )
      
      ret_fut = cls.__read_node( file_id, index, idx_key, check_file_id=check_file_id )
      if async:
         return ret_fut 
      
      else:
         storagetypes.wait_futures( [ret_fut] )
         return ret_fut.get_result()
   
   
   @classmethod 
   def __compactify_get_candidates_delete( cls, volume_id, parent_id, dir_index_cutoff, async=False ):
      """
      Find the set of allocated index nodes beyond a given offset, suitable for swapping into a newly-freed slot.
      """
      to_compactify = MSEntryDirEntIndex.ListAll( {"MSEntryDirEntIndex.parent_id ==": parent_id,
                                                   "MSEntryDirEntIndex.volume_id ==": volume_id,
                                                   "MSEntryDirEntIndex.alloced =="  : True,
                                                   "MSEntryDirEntIndex.dir_index >=": dir_index_cutoff}, async=async, limit=1024 )
      
      return to_compactify
   
   
   @classmethod 
   def FindFreeGaps( cls, volume_id, parent_id, dir_index_cutoff, async=False, limit=1024 ):
      """
      Find the set of unallocated index slots less than a given offset, suitable for holding a newly-allocated directory index.
      """
      to_compactify = MSEntryDirEntIndex.ListAll( {"MSEntryDirEntIndex.parent_id ==": parent_id,
                                                   "MSEntryDirEntIndex.volume_id ==": volume_id,
                                                   "MSEntryDirEntIndex.alloced =="  : True,
                                                   "MSEntryDirEntIndex.dir_index <": dir_index_cutoff}, async=async, limit=limit )
      
      gaps = list( set(range(0, dir_index_cutoff)) - set([idx.dir_index for idx in to_compactify]) )
      
      return gaps
   
   
   @classmethod 
   def __compactify_swap( cls, volume_id, parent_id, alloced_file_id, alloced_dir_index, free_file_id, free_dir_index, async=False ):
      
      """
      Atomically swap an allocated directory index node with an unallocated (or non-existant) directory index node, thereby placing the
      allocated directory index node into the "gap" left by the unallocated directory index node.
      
      This will delete the unallocated directory index node and its companion entry index node, and move the
      allocated directory index node's companion entry index node into place.
      
      alloced_file_id corresponds to the existing MSEntry (i.e. the one associated with the allocated index node)
      free_file_id corresponds to the deleted MSEntry, if applicable (i.e. the one associated with the free index node).  It can be None if there is no index node to delete for the file id.
      
      Return the dir index of the overwritten gap node on success, or None if free_file_id was None
      Return -ENOENT if the allocated dir index node no longer exists.
      Return -EAGAIN if we raced another process to allocate this slot and lost
      Return -ESTALE if the index allocation data is invalid (i.e. the free index got allocated, or the allocated index got freed)
      """
      
      alloced_idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, alloced_dir_index )
      alloced_entry_idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, alloced_file_id )
      
      free_idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, free_dir_index )
      
      alloced_entry_idx_key = storagetypes.make_key( MSEntryEntDirIndex, alloced_entry_idx_key_name )
      free_idx_key = storagetypes.make_key( MSEntryDirEntIndex, free_idx_key_name )
      
      # if the free file ID is not known, get it 
      if free_file_id is None:
         
         free_idx_data = cls.__read_dirent_node( volume_id, parent_id, None, free_dir_index, check_file_id=False )
         
         if free_idx_data is not None:
            free_idx_rc, free_idx = free_idx_data
            
            # it's okay if this index node does not exist
            if free_idx_rc != 0 and free_idx_rc != -errno.ENOENT:
            
               # some other error
               logging.error("/%s/%s: __read_dirent_node( /%s/%s, %s ) rc = %s" % (volume_id, parent_id, volume_id, free_file_id, free_dir_index, free_idx_rc ))
               
               if async:
                  return storagetypes.FutureWrapper( free_idx_rc )
               
               else:
                  return free_idx_rc 
               
            elif free_idx_rc == 0 and free_idx is not None:
               
               if free_idx.alloced:
                  
                  logging.error("/%s/%s: free index (/%s/%s, %s) is allocated" % (volume_id, parent_id, volume_id, free_idx.file_id, free_dir_index) )
                  
                  storagetypes.memcache.delete_multi( [alloced_idx_key_name, alloced_entry_idx_key_name, free_idx_key_name] )
                  
                  if async:
                     return storagetypes.FutureWrapper( -errno.ESTALE )
                  else:
                     return -errno.ESTALE
                  
               else:
                  
                  logging.info("/%s/%s: file id of /%s/%s at %s is %s\n" % (volume_id, parent_id, volume_id, parent_id, free_dir_index, free_idx.file_id) )
                  
      
      @storagetypes.concurrent
      def do_swap( free_file_id ):
         
         # confirm that the allocated directory index node and free directory index node still exist
         free_idx_data = None 
         free_idx_rc = None 
         free_idx = None
         free_idx_file_id = None
         
         check_free_file_id = True
         
         if free_file_id is None:
            check_free_file_id = False 
            
         alloced_idx_data, free_idx_data = yield cls.__read_dirent_node( volume_id, parent_id, alloced_file_id, alloced_dir_index, async=True ), cls.__read_dirent_node( volume_id, parent_id, free_file_id, free_dir_index, async=True, check_file_id=check_free_file_id )
         
         alloced_idx_rc, alloced_idx = alloced_idx_data
         
         if free_idx_data is not None:
            free_idx_rc, free_idx = free_idx_data
            
         # possible that we raced another compactify operation and lost (in which case the allocated node might be different than what we expect)
         if alloced_idx_rc != 0:
            logging.error("/%s/%s: alloced index (/%s/%s, %s) rc = %s" % (volume_id, parent_id, volume_id, alloced_file_id, alloced_dir_index, alloced_idx_rc) )
            storagetypes.concurrent_return( (-errno.EAGAIN, None, None) )
         
         elif not alloced_idx.alloced:
            logging.error("/%s/%s: alloced index (/%s/%s, %s) is free" % (volume_id, parent_id, volume_id, alloced_file_id, alloced_dir_index) )
            storagetypes.concurrent_return( (-errno.ESTALE, None, None) )
         
         if free_idx_data is not None:
            
            if free_idx_rc != 0:
               
               if free_idx_rc == -errno.ENOENT:
                  # the entry doesn't exist, which is fine by us since we're about to overwrite it anyway
                  free_idx_rc = None 
                  free_idx = None
                  free_idx_data = None
               
               else:
                  logging.error("/%s/%s: __read_dirent_node(/%s/%s, %s) rc = %s" % (volume_id, parent_id, volume_id, free_file_id, free_dir_index, free_idx_rc) )
                  storagetypes.concurrent_return( (free_idx_rc, None, None) )
               
            elif free_idx.alloced:
               
               logging.error("/%s/%s: free index (/%s/%s, %s) is allocated" % (volume_id, parent_id, volume_id, free_idx.file_id, free_dir_index) )
               storagetypes.concurrent_return( (-errno.ESTALE, None, None) )
            
            elif free_idx.dir_index != free_dir_index:
               raise Exception("/%s/%s: free index slot mismatch: %s != %s" % (volume_id, free_file_id, free_idx.dir_index, free_dir_index))
            
            else:
               # save this for later...
               free_idx_file_id = free_idx.file_id
            
         # sanity check 
         if alloced_idx.dir_index != alloced_dir_index:
            raise Exception("/%s/%s: allocated index slot mismatch: %s != %s" % (volume_id, alloced_file_id, alloced_idx.dir_index, alloced_dir_index))
         
         # do the swap:
         # * overwrite the free dir index node with the allocated dir index node's data (moving it into place over the freed one)
         # * update the alloced ent node with the free dir index node's dir index (compactifying the index)
         new_dir_idx = MSEntryDirEntIndex( key=free_idx_key, **alloced_idx.to_dict() )
         new_entry_dir_idx = MSEntryEntDirIndex( key=alloced_entry_idx_key, **alloced_idx.to_dict() )   # overwrites existing entry index node
         
         new_dir_idx.dir_index = free_dir_index
         new_entry_dir_idx.dir_index = free_dir_index 
         
         logging.debug( "swap index slot of /%s/%s: slot %s --> slot %s (overwrites %s)" % (volume_id, alloced_file_id, alloced_dir_index, free_dir_index, free_file_id) )
         
         yield new_dir_idx.put_async(), new_entry_dir_idx.put_async(), alloced_idx.key.delete_async()
         
         storagetypes.memcache.delete_multi( [alloced_idx_key_name, alloced_entry_idx_key_name, free_idx_key_name] )
         
         storagetypes.concurrent_return( (0, alloced_idx, free_idx_file_id) )
      
      
      @storagetypes.concurrent
      def swap( free_file_id ):
         
         rc, alloced_idx, free_idx_file_id = yield storagetypes.transaction_async( lambda: do_swap( free_file_id ), xg=True )
         
         if rc < 0:
            storagetypes.concurrent_return( rc )
         
         old_dir_index = None
         
         if free_file_id is None:
            free_file_id = free_idx_file_id 
            
         if free_file_id is not None:
            # blow away the newly-freed index node
            old_entry_idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, free_file_id )
            old_entry_idx_key = storagetypes.make_key( MSEntryEntDirIndex, old_entry_idx_key_name )
            
            yield old_entry_idx_key.delete_async()
            
            storagetypes.memcache.delete( old_entry_idx_key_name )
         
            old_dir_index = alloced_idx.dir_index
         
         storagetypes.concurrent_return( old_dir_index )
         
      
      rc_fut = swap( free_file_id )
      
      if async:
         return rc_fut
      
      else:
         storagetypes.wait_futures( [rc_fut] )
         return rc_fut.get_result()
      
   
   @classmethod 
   @storagetypes.concurrent
   def __compactify_remove_index_async( cls, volume_id, parent_id, dead_file_id, dead_dir_index ):
      """
      Remove a freed index slot's node data.
      """
      idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, dead_dir_index )
      ent_key_name = MSEntryEntDirIndex.make_key_name( volume_id, dead_file_id )
      
      idx_key = storagetypes.make_key( MSEntryDirEntIndex, idx_key_name )
      ent_key = storagetypes.make_key( MSEntryEntDirIndex, ent_key_name )
      
      @storagetypes.concurrent
      def delete_index_if_unallocated():
         
         idx_node = yield idx_key.get_async( use_cache=False, use_memcache=False )
         
         if idx_node is None:
             # already gone
             storagetypes.concurrent_return( 0 )
         
         if not idx_node.alloced:
            
            yield idx_key.delete_async()
         
         storagetypes.concurrent_return( 0 )
      
      yield ent_key.delete_async(), storagetypes.transaction_async( delete_index_if_unallocated )
      
      storagetypes.memcache.delete_multi( [idx_key_name, ent_key_name] )
      
   
   @classmethod 
   def __compactify_child_delete( cls, volume_id, parent_id, free_file_id, free_dir_index, dir_index_cutoff ):
      """
      Repeatedly find a child's index node that (1) is allocated, and (2) is beyond a given cutoff (i.e. the number of index nodes at the time of the call),
      and then atomically swap the identified freed node with child's node in the index order.  The effect is that allocated index nodes at the end 
      of the index get moved to replace the gaps in the index, thereby compactifying it.
      
      Return (old_dir_index, free dir index node) on success
      Return -EPERM if no compactification can happen (i.e. all children have directory index values smaller than the maximum)
      Return -EAGAIN if the caller should refresh the parent directory index maximum value
      """
      
      # find all entries in parent with a dir index greater than the current one
      to_compactify = None 
      
      while True:
         
         to_compactify = cls.__compactify_get_candidates_delete( volume_id, parent_id, dir_index_cutoff )
      
         if len(to_compactify) > 0:
            
            # it's possible there are more than one.  Pick one that's allocated (but go in random order)
            order = range( 0, len(to_compactify) )
            random.shuffle( order )
            idx = None 
            
            for i in order:
               if to_compactify[i].alloced:
                  idx = to_compactify[i]
                  break 
            
            if idx is None:
               # try again--there are no candidates to swap at this time 
               logging.info("Directory /%s/%s: no compactifiable candidates >= %s" % (volume_id, parent_id, dir_index_cutoff))
               return (-errno.EAGAIN, None)
            
            
            old_dir_index = cls.__compactify_swap( volume_id, parent_id, idx.file_id, idx.dir_index, free_file_id, free_dir_index )
            if old_dir_index >= 0:
               # swapped!
               return (old_dir_index, idx)
            
            else:
               logging.info("Directory /%s/%s: __compactify_swap(%s (%s <--> %s)) rc = %s" % (volume_id, parent_id, idx.file_id, free_dir_index, idx.dir_index, old_dir_index))
               
               if old_dir_index == -errno.ENOENT:
                  # entry queried doesn't exist, for some reason.  Need dir_index_cutoff to be refreshed 
                  return (-errno.EAGAIN, None)
               
               if old_dir_index in [-errno.EAGAIN, -errno.EPERM, -errno.ESTALE]:
                  # had multiple concurrent deletes, and they tried to operate on the same entry.  Try again
                  continue
         else:
            # No compactification needs to happen
            # (assuming the ListAll returned consistent data)
            logging.info("Directory /%s/%s: no compactifiable candidates at this time" % (volume_id, parent_id))
            
            return (-errno.EPERM, None)
      
      
   @classmethod 
   def __compactify_parent_delete( cls, volume_id, parent_id, free_file_id, free_dir_index, num_shards, compactify_continuation=None ):
      """
      Given a free directory index, repeatedly find a child with a
      directory index value that can be swapped into a gap in the parent's index.
      That is, find children with index values that are beyond the number of children,
      and swap their index nodes with index nodes that represent gaps.
      """
      
      old_max_cutoff = None
      
      while True:
         
         # refresh the index max cutoff--it may have changed
         # the cutoff is the new number of children, after this entry has been deleted
         parent_max_cutoff = cls.GetNumChildren( volume_id, parent_id, num_shards ) - 1
         if parent_max_cutoff is None:
            
            # directory doesn't exist anymore...nothing to compactify 
            logging.info("Index node /%s/%s does not exist" % (volume_id, parent_id) )
            return 0
         
         if parent_max_cutoff == 0:
            
            # nothing left to compactify!
            logging.info("Directory /%s/%s appears to be empty" % (volume_id, parent_id))
            return 0
         
         if old_max_cutoff is not None:
            
            # choose the smallest parent size seen so far as the cutoff, since it maimizes the number of entries
            # that can be selected to fill the gap.  If we don't do this, we could accidentally
            # loop forever by never finding an entry to replace the gap.
            parent_max_cutoff = min( old_max_cutoff, parent_max_cutoff )
         
         if parent_max_cutoff < free_dir_index:
            
            # gap no longer exists--the directory shrank out from under it 
            logging.info("Directory /%s/%s compactification threshold %s exceeded (by %s)" % (volume_id, parent_id, parent_max_cutoff, free_dir_index) )
            return 0
         
         if parent_max_cutoff == free_dir_index:
            
            # gap is at the end.
            logging.info("Directory /%s/%s entry is at the end (%s)" % (volume_id, parent_id, free_dir_index))
            rc_fut = cls.__compactify_remove_index_async( volume_id, parent_id, free_file_id, free_dir_index )
            storagetypes.wait_futures( [rc_fut] )
            return 0
         
         old_max_cutoff = parent_max_cutoff 
         
         replaced_dir_index, child_idx = cls.__compactify_child_delete( volume_id, parent_id, free_file_id, free_dir_index, parent_max_cutoff )
         
         if replaced_dir_index >= 0:
            # success!
            
            if compactify_continuation is not None:
               compactify_continuation( compacted_index_node=child_idx, replaced_index=replaced_dir_index )
               
            # verify that we didn't leave a gap by compactifying
            # (can happen if another process creates an entry while we're compactifying)
            new_parent_max_cutoff = cls.GetNumChildren( volume_id, parent_id, num_shards )
            if new_parent_max_cutoff is None:
               
               # directory doesn't exist anymore...nothing to compactify 
               logging.info("Index node /%s/%s does not exist" % (volume_id, parent_id) )
               return 0
            
            if parent_max_cutoff < new_parent_max_cutoff:
               
               # left a gap--need to compactify again 
               free_dir_index = parent_max_cutoff
               old_max_cutoff = None
               continue 
            
            else:
               # done!
               logging.info("Directory /%s/%s compactified" % (volume_id, parent_id))
               return 0
         
         elif replaced_dir_index == -errno.EAGAIN or replaced_dir_index == -errno.EPERM:
            # need to re-check the maximum cutoff
            # (NOTE: EPERM can mean that the children beyond the cutoff aren't showing up in queries yet)
            # TODO: can loop forever?
            logging.info("__compactify_child_delete( /%s/%s index=%s threshold=%s ) rc = %s" % (volume_id, parent_id, free_dir_index, parent_max_cutoff, replaced_dir_index))
            continue 
         
         else:
            logging.error("BUG: failed to compactify /%s/%s, rc = %s\n", volume_id, parnet_id, replaced_dir_index )
            return replaced_dir_index
   
   
   @classmethod
   def __compactify_on_delete( cls, volume_id, parent_id, free_file_id, free_dir_index, num_shards, retry=True, compactify_continuation=None ):
      """
      Compactify the parent's index on delete, trying again in a deferred task if need be.
      This is the top-level entry point.
      """
      
      try:
         
         # compact the index--move entries from the end of the index into the gaps 
         cls.__compactify_parent_delete( volume_id, parent_id, free_file_id, free_dir_index, num_shards, compactify_continuation=compactify_continuation )
         
         # account that there is now one less index node
         cls.__num_children_dec( volume_id, parent_id, num_shards )
         
      except storagetypes.RequestDeadlineExceededError:
         
         if retry:
            # keep trying
            storagetypes.deferred.defer( cls.__compactify_on_delete, volume_id, parent_id, free_dir_index )
            return
         
         else:
            raise
      
      except Exception, e:
         logging.exception( e )
         raise e
   
   
   @classmethod 
   def __compactify_parent_insert_once( cls, volume_id, parent_id, new_file_id, new_dir_index, num_shards ):
      
      """
      Given a newly-allocated directory index, try to find a free slot lower in the index to swap it with.
      Return the (0, final directory index) on success.
      Return negative on error.
      """
      
      # find gaps to swap into that are in range [0, new_dir_index].
      # see if we can get new_dir_index to be beneath the number of children.
      free_gaps = cls.FindFreeGaps( volume_id, parent_id, new_dir_index )
      
      if len(free_gaps) == 0:
         logging.info("Directory /%s/%s: no free gaps, so keep /%s/%s at %s" % (volume_id, parent_id, volume_id, new_file_id, new_dir_index))
         return (0, new_dir_index)
         
      
      # move us there
      free_gap = free_gaps[ random.randint( 0, len(free_gaps) - 1 ) ]
      
      # are we in a "gap" already?
      if free_gap == new_dir_index:
         logging.info("Directory /%s/%s: already inserted /%s/%s at %s" % (volume_id, parent_id, volume_id, new_file_id, free_gap))
         return (0, free_gap)
         
      
      # attempt the swap 
      rc = cls.__compactify_swap( volume_id, parent_id, new_file_id, new_dir_index, None, free_gap )
      
      # get the number of children again--maybe the set expanded up to include us, even if we failed
      parent_num_children = cls.GetNumChildren( volume_id, parent_id, num_shards )
      
      if rc is None:
         
         # success
         if free_gap < parent_num_children:
            
            # swapped into a lower slot!
            logging.info("Directory /%s/%s: inserted /%s/%s at %s" % (volume_id, parent_id, volume_id, new_file_id, free_gap))
               
            return (0, free_gap)
      
         else:
            
            # succeeded, but after the number of children got decreased
            # try again 
            logging.error("Directory /%s/%s: inserted /%s/%s at %s (from %s), but directory size is %s" % (volume_id, parent_id, volume_id, new_file_id, free_gap, new_dir_index, parent_num_children))
            
            new_dir_index = free_gap
            return (-errno.EAGAIN, new_dir_index)
         
      else:
         
         logging.debug("Directory /%s/%s: failed to swap /%s/%s from %s to %s, num_children = %s" % (volume_id, parent_id, volume_id, new_file_id, new_dir_index, free_gap, parent_num_children))
         
         # maybe we failed to swap, but are we now in range?
         if new_dir_index < parent_num_children:
            
            # success
            logging.debug("Directory /%s/%s: inserted /%s/%s at %s, since directory size is %s" % (volume_id, parent_id, volume_id, new_file_id, new_dir_index, parent_num_children))
            
            return (0, new_dir_index)
         
         elif rc == -errno.ENOENT:
            
            # someone else swapped us 
            logging.debug("Directory /%s/%s: swapped /%s/%s out of %s by another process" % (volume_id, parent_id, volume_id, new_file_id, new_dir_index))
            
            return (0, -1)
         
         else:
            
            # otherwise, try again 
            logging.error("Directory /%s/%s: __compactify_swap(%s (%s <--> %s)) rc = %s; try again" % (volume_id, parent_id, new_file_id, new_dir_index, free_gap, rc))
            return (-errno.EAGAIN, new_dir_index)
         
        
        
   @classmethod
   def __compactify_on_insert( cls, volume_id, parent_id, new_file_id, new_dir_index, num_shards ):
      """
      Compactify the parent's index on insert, trying again in a deferred task if need be.
      Return an error code and the directory entry's new directory index, if successful.
      
      (0, nonnegative) means successfully inserted at nonzero place
      (0, negative) means successfully inserted, but we no longer know where 
      (-EAGAIN, nonegative) means try again at the new directory index.
      """
      
      try:
         
         # compact the index--move entries from the end of the index into the gaps 
         rc, final_dir_index = cls.__compactify_parent_insert_once( volume_id, parent_id, new_file_id, new_dir_index, num_shards )
         
         return (rc, final_dir_index)
         
      except Exception, e:
         logging.exception( e )
         raise e
      
      
   @classmethod 
   def NextGeneration( cls ):
      """
      Get a generation number.
      """
      
      now_sec, now_nsec = storagetypes.clock_gettime()
      
      # 10ths of milliseconds
      generation = now_sec * 10000 + now_nsec / 100000
            
      return generation 
   
   
   @classmethod 
   def TryInsert( cls, volume_id, parent_id, file_id, new_dir_index, parent_capacity, num_shards, async=False ):
      """
      Try to insert a given file ID into its parent's index, atomically.
      Otherwise, it selects a slot at random that is likely to be free
      Return a (return code, value)
      
      If return code is 0, then the value is the generation number
      If return code is negative, then the value is either the new directory index to try, or None to choose a new one outright.
      """
      
      generation = cls.NextGeneration()
            
      @storagetypes.concurrent
      def try_insert( new_dir_index ):
         
         rc = yield cls.__alloc( volume_id, parent_id, file_id, new_dir_index, generation, async=True )
         
         if rc:
            
            # compactify--see if we can shift it closer
            rc, final_dir_index = cls.__compactify_on_insert( volume_id, parent_id, file_id, new_dir_index, num_shards )
            
            if rc == 0:
               storagetypes.concurrent_return( (0, generation) )
            
            elif rc == -errno.EAGAIN:
               # try again 
               storagetypes.concurrent_return( (-errno.EAGAIN, final_dir_index) )
            
            else:
               storagetypes.concurrent_return( (rc, None) )
         
         else:
            
            logging.info("Directory /%s/%s: Failed to insert /%s/%s (capacity %s) at %s; will need to retry" % (volume_id, parent_id, volume_id, file_id, parent_capacity, new_dir_index) )
            
            # probably collided.  Try again, and have the caller pick a different index
            storagetypes.concurrent_return( (-errno.EAGAIN, None) )
      
      fut = try_insert( new_dir_index )
      
      if async:
         return fut 
      
      else:
         storagetypes.wait_futures( [fut] )
         return fut.get_result()
      
      
   @classmethod 
   def Delete( cls, volume_id, parent_id, file_id, dir_index, num_shards, async=False, retry=True, compactify_continuation=None ):
      """
      Free and then compactify the index.  This will result in the directory index and 
      entry nodes getting deleted.
      """
      
      @storagetypes.concurrent 
      def do_delete():
         
         rc = yield cls.__free( volume_id, parent_id, file_id, dir_index, async=True )
         
         if not rc:
            logging.error("Failed to free index node /%s/%s (%s,%s)" % (volume_id, parent_id, file_id, dir_index))
            storagetypes.concurrent_return( -errno.EAGAIN )
         
         cls.__compactify_on_delete( volume_id, parent_id, file_id, dir_index, num_shards, retry=retry, compactify_continuation=compactify_continuation )
         
         storagetypes.concurrent_return( 0 )
         
      result_fut = do_delete()
      
      if async:
         return result_fut 
      
      storagetypes.wait_futures( [result_fut] )
      return result_fut.get_result()
   
   
   @classmethod 
   def Purge( cls, volume_id, parent_id, file_id, num_shards, async=False ):
      """
      Remove both DirEnt and EntDir index nodes, and don't bother to compactify.
      This is suitable for deleting the index wholesale, such as when deleting a Volume 
      or an AG.
      
      Return 0 on success, if async == False 
      Return a list of futures to wait on, if async == True
      """
      
      futs = []
      
      fut = cls.__num_children_delete( volume_id, parent_id, num_shards, async=True )
      futs.append( futs )
      
      entdir = MSEntryIndex.ReadIndex( volume_id, file_id )
      
      if entdir is not None:
         
         dirent_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, entdir.dir_index )
         entdir_key_name = MSEntryEntDirIndex.make_key_name( volume_id, file_id )
         
         dirent_key = storagetypes.make_key( MSEntryDirEntIndex, dirent_key_name )
         entdir_key = storagetypes.make_key( MSEntryEntDirIndex, entdir_key_name )
         
         dirent_del_fut = dirent_key.delete_async()
         entdir_del_fut = entdir_key.delete_async()
         
         futs.append( dirent_del_fut )
         futs.append( entdir_del_fut )
      
      if not async:
         storagetypes.wait_futures( futs )
         return 0 
      
      else:
         return futs
      
   
   @classmethod 
   def GetNumChildren( cls, volume_id, parent_id, num_shards, async=False ):
      """
      Get the number of children in a directory
      """
      
      num_children_counter = cls.__parent_child_counter_name( volume_id, parent_id )
      
      if async:
         return shardcounter.get_count_async( num_children_counter, num_shards, use_memcache=False )
      
      else:
         num_children = shardcounter.get_count( num_children_counter, num_shards, use_memcache=False )
         return num_children
   
   
   @classmethod 
   def NumChildrenInc( cls, volume_id, parent_id, num_shards, async=False ):
      """
      Increase the number of children in a directory 
      """
      
      return cls.__num_children_inc( volume_id, parent_id, num_shards, async=async )
   
   
   @classmethod 
   def NumChildrenDec( cls, volume_id, parent_id, num_shards, async=False ):
      """
      Get the number of children in a directory
      """
      
      return cls.__num_children_inc( volume_id, parent_id, num_shards, async=async )
   
   @classmethod 
   def Read( cls, volume_id, parent_id, dir_index, async=False ):
      """
      Read a directory index node.  Return the whole node, not the value
      """
      idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, dir_index )
      idx_key = storagetypes.make_key( MSEntryDirEntIndex, idx_key_name )
      idx = storagetypes.memcache.get( idx_key_name )
      
      if idx is not None:
         
         if async:
            return storagetypes.FutureWrapper( idx )
         
         else:
            return idx
         
      else:
         
         if async:
            
            @storagetypes.concurrent
            def read_and_cache():
               
               idx = yield idx_key.get_async()
               
               if idx is not None and idx.alloced:
                  MSEntryIndex.SetCache( idx )
               
               storagetypes.concurrent_return( idx )
            
            return read_and_cache()
         
         else:
            return idx_key.get()
         
         
   @classmethod 
   def ReadIndex( cls, volume_id, file_id, async=False ):
      """
      Read an entry index node.  Return an MSEntryEntDirIndex
      """
      idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, file_id )
      idx_key = storagetypes.make_key( MSEntryEntDirIndex, idx_key_name )
      idx = storagetypes.memcache.get( idx_key_name )
      
      if idx is not None:
         
         if async:
            return storagetypes.FutureWrapper( idx )
         
         else:
            return idx
         
      else:
         
         if async:
            
            @storagetypes.concurrent
            def read_and_cache():
               
               idx = yield idx_key.get_async()
               
               if idx is not None and idx.alloced:
                  MSEntryIndex.SetCache( idx )
               
               if not idx.alloced:
                  storagetypes.concurrent_return( None )
               
               storagetypes.concurrent_return( idx )
            
            return read_and_cache()
         
         else:
            return idx_key.get()
         
         
   @classmethod 
   def SetCache( cls, dir_index_node ):
      """
      Cache a node
      """
      idx_key_name = MSEntryDirEntIndex.make_key_name( dir_index_node.volume_id, dir_index_node.parent_id, dir_index_node.dir_index )
      ent_key_name = MSEntryEntDirIndex.make_key_name( dir_index_node.volume_id, dir_index_node.file_id )
      
      storagetypes.memcache.set_multi( {idx_key_name: dir_index_node, ent_key_name: dir_index_node} )
   
   
   @classmethod 
   def GenerationQuery( cls, volume_id, parent_id, generation_begin, generation_end ):
      """
      Get a range of directory index nodes for a given parent/volume, by generation
      """
      
      qry = MSEntryDirEntIndex.query()
      qry = qry.filter( storagetypes.opAND( MSEntryDirEntIndex.volume_id == volume_id, MSEntryDirEntIndex.parent_id == parent_id ) )
      
      if generation_begin >= 0:
         qry = qry.filter( MSEntryDirEntIndex.generation >= generation_begin )
      
      if generation_end >= 0:
         qry = qry.filter( MSEntryDirEntIndex.generation < generation_end )
         
      return qry
   
