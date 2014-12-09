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


class MSEntryIndexCounter( storagetypes.Object ):
   """
   Index record for a directory.
   Used for:
   * tracking the generation number for a directory (the number of create's to have occurred on it)
   * tracking the number of children in a directory
   """
   value = storagetypes.Integer( default=0 )
   
   @classmethod 
   def make_dir_index_name( cls, volume_id, parent_id ):
      return "MSEntryIndexCounter-dir_index: volume_id=%s,parent_id=%s" % (volume_id, parent_id)
   
   @classmethod 
   def make_generation_name( cls, volume_id, parent_id ):
      return "MSEntryIndexCounter-generation: volume_id=%s,parent_id=%s" % (volume_id, parent_id)
   
   @classmethod 
   def __update( cls, key_name, value, do_transaction=True, async=False ):
      """
      Add the given value to the given counter.
      Optionally do so atomically, and/or asynchronously.
      """
      
      @storagetypes.concurrent
      def delta_txn():
         
         key = storagetypes.make_key( MSEntryIndexCounter, key_name )
         
         cnt = yield key.get_async()
         
         if cnt is None:
            cnt = MSEntryIndexCounter( key=key, value=0 )
         
         cnt.value += value
         yield cnt.put_async()
         
         storagetypes.memcache.set( key_name, cnt )
         
         storagetypes.concurrent_return( cnt.value )
         
         
      if do_transaction:
         
         result_fut = storagetypes.transaction_async( lambda: delta_txn() )
         
      else:
         
         result_fut = delta_txn()
         
      if async:
         return result_fut 
      
      else:
         storagetypes.wait_futures( [result_fut] )
         result = result_fut.get_result()
         return result
         
         
   @classmethod 
   def __read( cls, key_name, async=False ):
      """
      Read the value of the counter, optionally asynchronously.
      Use memcache whenever possible.
      """
      result = storagetypes.memcache.get( key_name )
      if result is not None:
         
         if async:
            return storagetypes.FutureWrapper( result )
         else:
            return result 
         
      key = storagetypes.make_key( MSEntryIndexCounter, key_name )
      if async:
         
         @storagetypes.concurrent
         def __read_async():
            cnt = yield key.get_async()
            
            if cnt is not None:
               storagetypes.concurrent_return( cnt.value )
            else:
               storagetypes.concurrent_return( 0 )
         
         return __read_async()
         
      else:
         result = key.get()
         if result is None:
            return 0 
         
         storagetypes.memcache.add( key_name, result.value )
         return result.value
         
         
   @classmethod 
   def update_dir_index( cls, volume_id, parent_id, value, do_transaction=True, async=False ):
      """
      Update the dir index (adding the given value), optionally in a trasaction and/or asynchronously.
      """
      key_name = cls.make_dir_index_name( volume_id, parent_id )
      return cls.__update( key_name, value, do_transaction=do_transaction, async=async )
   
   @classmethod 
   def update_generation( cls, volume_id, parent_id, value, do_transaction=True, async=False ):
      """
      Update the generation count (adding the given value), optionally in a trasaction and/or asynchronously.
      """
      key_name = cls.make_generation_name( volume_id, parent_id )
      return cls.__update( key_name, value, do_transaction=do_transaction, async=async )
   
   @classmethod 
   def read_dir_index( cls, volume_id, parent_id, async=False ):
      """
      Read the dir index, optionally asynchronously.
      """
      key_name = cls.make_dir_index_name( volume_id, parent_id )
      return cls.__read( key_name, async=async )
   
   @classmethod 
   def read_generation( cls, volume_id, parent_id, async=False ):
      """
      Read the generation number, optionally asynchronously.
      """
      key_name = cls.make_generation_name( volume_id, parent_id )
      return cls.__read( key_name, async=async )
   
   
   
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
   
   @classmethod
   @storagetypes.concurrent 
   def __update_index_node_async( cls, volume_id, parent_id, file_id, dir_index, alloced, **attrs ):
      """
      Set the allocation status of a directory index node (but not its matching entry index node).
      
      Return 0 on success
      Return -EINVAL if the given file_id doesn't match the directory index node's file_id 
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
      
   
   @classmethod
   @storagetypes.concurrent
   def __create_or_alloc_async( cls, volume_id, parent_id, file_id, dir_index, generation, alloced ):
      """
      Create the index node pair and/or set the directory index node's allocation status, asynchronously.
      If the directory index node does not exist, it and its entry index node will be created and the allocation status set accordingly.
      If the directory index node exists, but has a different allocation status, then the allocation status will be set accordingly.
      
      Return True on success.
      Return False if the index node existed, but the file_id did not match its record.
      """
      
      index_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, dir_index )
      
      nonce = random.randint( -2**63, 2**63 - 1 )
      idx = storagetypes.memcache.get( index_key_name )
      result = True
      
      if idx is None:
         idx = yield MSEntryDirEntIndex.get_or_insert_async( index_key_name, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, generation=generation, alloced=alloced, nonce=nonce )
      
      if idx.file_id != file_id:
         # already exists, and caller gave invalid data 
         storagetypes.concurrent_return( False )
      
      if idx.nonce == nonce:
         # created.  need to create an entry index node as well.
         entry_key_name = MSEntryEntDirIndex.make_key_name( volume_id, file_id )
         entry_key = storagetypes.make_key( MSEntryEntDirIndex, entry_key_name )
         entry_idx = MSEntryEntDirIndex( key=entry_key, volume_id=volume_id, parent_id=parent_id, file_id=file_id, dir_index=dir_index, generation=generation, alloced=alloced, nonce=nonce )
         
         yield entry_idx.put_async()
         
         storagetypes.memcache.set( entry_key_name, entry_idx )
      
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
            # created/set correctly
            result = True
      
      storagetypes.memcache.set( index_key_name, idx )
      
      storagetypes.concurrent_return( result )
   
   
   @classmethod 
   def __alloc( cls, volume_id, parent_id, file_id, dir_index, generation, async=False ):
      """
      Create a new index node.  Do not set its allocation status, unless told to.
      Return True if we succeeded.
      Return False if the node already exists for this dir_index value.
      """
      
      result_fut = cls.__create_or_alloc_async( volume_id, parent_id, file_id, dir_index, generation, True )
      
      if not async:
         storagetypes.wait_futures( [result_fut] )
         return result_fut.get_result()
      
      else:
         return result_fut 
      
      
   @classmethod 
   def __free( cls, volume_id, parent_id, file_id, dir_index, async=False ):
      """
      Mark an index node as freed, optionally creating it so we know that the gap exists.
      Return True if we succeeded.
      Return False if the node already exists for this dir_index value, or if it's already freed.
      """
      result_fut = cls.__create_or_alloc_async( volume_id, parent_id, file_id, dir_index, -1, False )
      
      if not async:
         storagetypes.wait_futures( [result_fut] )
         return result_fut.get_result()
      
      else:
         return result_fut 


   @classmethod 
   def __parent_index_inc( cls, volume_id, parent_id, async=False ):
      """
      Atomically increment the parent's generation and dir_index values.
      """
      
      @storagetypes.concurrent
      def txn():
         next_dir_index, next_generation = yield MSEntryIndexCounter.update_dir_index( volume_id, parent_id, 1, do_transaction=False, async=True ), MSEntryIndexCounter.update_generation( volume_id, parent_id, 1, do_transaction=False, async=True )
         storagetypes.concurrent_return( (next_dir_index, next_generation) )
      
      if async:
         return storagetypes.transaction_async( lambda: txn(), xg=True )
      
      else:
         return storagetypes.transaction( lambda: txn(), xg=True )
      
      
   @classmethod 
   def __parent_index_dec( cls, volume_id, parent_id, async=False ):
      """
      Atomically decrement parent's dir index, and return the new value.
      """
      return MSEntryIndexCounter.update_dir_index( volume_id, parent_id, -1, async=async )
   
   
   @classmethod 
   @storagetypes.concurrent 
   def __read_node( cls, file_id, index, idx_key ):
      """
      Read an index node, given its key. 
      Return (rc, idx):
      * return -ENOENT if the index node doesn't exist 
      * return -EPERM if the index node is inconsistent with the given data
      """
      
      idx = yield idx_key.get_async()
      
      if idx is None:
         storagetypes.concurrent_return( (-errno.ENOENT, None) )
      
      if idx.file_id != file_id:
         storagetypes.concurrent_return( (-errno.EPERM, None) )
      
      if idx.dir_index != index:
         storagetypes.concurrent_return( (-errno.EPERM, None) )
      
      storagetypes.concurrent_return( (0, idx) )
   
   
   @classmethod 
   def __read_dirent_node( cls, volume_id, parent_id, file_id, index, async=False ):
      """
      Read a node key, and verify that it is consistent.
      Return (rc, idx)
      """
      
      idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, index )
      idx_key = storagetypes.make_key( MSEntryDirEntIndex, idx_key_name )
      
      ret_fut = cls.__read_node( file_id, index, idx_key )
      if async:
         return ret_fut 
      
      else:
         storagetypes.wait_futures( [ret_fut] )
         return ret_fut.get_result()
   
   
   @classmethod 
   def __read_entdir_node( cls, volume_id, parent_id, file_id, index, async=False ):
      """
      Read a node key, and verify that it is consistent.
      Return (rc, idx)
      """
      
      idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, parent_id, file_id )
      idx_key = storagetypes.make_key( MSEntryEntDirIndex, idx_key_name )
      
      ret_fut = cls.__read_node( file_id, index, idx_key )
      if async:
         return ret_fut 
      
      else:
         storagetypes.wait_futures( [ret_fut] )
         return ret_fut.get_result()
         
   
   @classmethod 
   def __compactify_get_candidates( cls, volume_id, parent_id, dir_index_cutoff, async=False ):
      """
      Find the set of allocated index nodes beyond a given size.
      """
      to_compactify = MSEntryDirEntIndex.ListAll( {"MSEntryDirEntIndex.parent_id ==": parent_id,
                                                   "MSEntryDirEntIndex.volume_id ==": volume_id,
                                                   "MSEntryDirEntIndex.alloced =="  : True,
                                                   "MSEntryDirEntIndex.dir_index >=": dir_index_cutoff}, async=async )
      
      return to_compactify
   
   
   @classmethod 
   def __compactify_swap( cls, volume_id, parent_id, alloced_file_id, alloced_dir_index, free_file_id, free_dir_index, dir_index_cutoff, async=False ):
      
      """
      Atomically swap an allocated directory index node with a freed directory index node, thereby placing the
      allocated directory index node into the "gap" left by freeing a directory index node earlier.
      
      This will delete the freed directory index node and its companion entry index node, and move the
      allocated directory index node's companion entry index node into place.
      
      alloced_file_id corresponds to the existing MSEntry (i.e. the one associated with the allocated index node)
      free_file_id corresponds to the now-deleted MSEntry (i.e. the one associated with the free index node)
      
      Return the dir index of the overwritten gap node on success.
      Return -ENOENT if the allocated dir index node no longer exists.
      Return -EAGAIN if the allocated dir index was beneath the dir index cutoff.
      Return -EPERM if the index node data was not consistent 
      Return -ESTALE if the index allocation data is invalid
      """
      
      alloced_idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, alloced_dir_index )
      alloced_entry_idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, alloced_file_id )
      
      #free_entry_idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, free_file_id )
      free_idx_key_name = MSEntryDirEntIndex.make_key_name( volume_id, parent_id, free_dir_index )
      
      alloced_entry_idx_key = storagetypes.make_key( MSEntryEntDirIndex, alloced_entry_idx_key_name )
      free_idx_key = storagetypes.make_key( MSEntryDirEntIndex, free_idx_key_name )
      
      #free_entry_index_key = storagetypes.make_key( MSEntryEntDirIndex, free_entry_idx_key_name )
      
      @storagetypes.concurrent
      def do_swap():
         
         # confirm that the allocated directory index node and free directory index node still exist
         alloced_idx_data, free_idx_data = yield cls.__read_dirent_node( volume_id, parent_id, alloced_file_id, alloced_dir_index, async=True ), cls.__read_dirent_node( volume_id, parent_id, free_file_id, free_dir_index, async=True )
         
         alloced_idx_rc, alloced_idx = alloced_idx_data
         free_idx_rc, free_idx = free_idx_data
         
         # possible that we raced another compactify operation and lost (in which case the node won't be allocated),
         # or our allocated dir index node might have been changed before we got here.
         if alloced_idx_rc != 0:
            storagetypes.concurrent_return( (alloced_idx_rc, None) )
         
         if free_idx_rc != 0:
            storagetypes.concurrent_return( (free_idx_rc, None) )
         
         if not alloced_idx.alloced:
            storagetypes.concurrent_return( (-errno.ESTALE, None) )
         
         if free_idx.alloced:
            storagetypes.concurrent_return( (-errno.ESTALE, None) )
         
         # do the swap:
         # * overwrite the free dir index node with the allocated dir index node's data (moving it into place over the freed one)
         # * update the alloced ent node with the free dir index node's dir index (compactifying the index)
         new_dir_idx = MSEntryDirEntIndex( key=free_idx_key, **alloced_idx.to_dict() )
         new_entry_dir_idx = MSEntryEntDirIndex( key=alloced_entry_idx_key, **alloced_idx.to_dict() )   # overwrites existing entry index node
         
         new_dir_idx.dir_index = free_dir_index
         new_entry_dir_idx.dir_index = free_dir_index 
         
         logging.info("put %s" % free_idx_key )
         logging.info("put %s" % alloced_entry_idx_key )
         logging.info("delete %s" % alloced_idx.key )
         
         yield new_dir_idx.put_async(), new_entry_dir_idx.put_async(), alloced_idx.key.delete_async()
         
         storagetypes.memcache.delete_multi( [alloced_idx_key_name, alloced_entry_idx_key_name, free_idx_key_name] )
         
         storagetypes.concurrent_return( (0, alloced_idx) )
      
      
      @storagetypes.concurrent
      def swap():
         
         rc, alloced_idx = yield storagetypes.transaction_async( do_swap, xg=True )
         
         if rc < 0:
            storagetypes.concurrent_return( rc )
         
         old_entry_idx_key_name = MSEntryEntDirIndex.make_key_name( volume_id, free_file_id )
         old_entry_idx_key = storagetypes.make_key( MSEntryEntDirIndex, old_entry_idx_key_name )
         
         logging.info("delete %s" % old_entry_idx_key )
         
         yield old_entry_idx_key.delete_async()
         
         storagetypes.memcache.delete( old_entry_idx_key_name )
         
         old_dir_index = alloced_idx.dir_index
         
         storagetypes.concurrent_return( old_dir_index )
         
      
      rc_fut = swap()
      
      if async:
         rc_fut
      
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
      
      yield idx_key.delete_async(), ent_key.delete_async()
      
      storagetypes.memcache.delete_multi( [idx_key_name, ent_key_name] )
      
   
   @classmethod 
   def __compactify_child( cls, volume_id, parent_id, free_file_id, free_dir_index, dir_index_cutoff ):
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
         
         to_compactify = cls.__compactify_get_candidates( volume_id, parent_id, dir_index_cutoff )
      
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
            
            old_dir_index = cls.__compactify_swap( volume_id, parent_id, idx.file_id, idx.dir_index, free_file_id, free_dir_index, dir_index_cutoff )
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
   def __compactify_parent( cls, volume_id, parent_id, free_file_id, free_dir_index, compactify_continuation=None ):
      """
      Given a free directory index, repeatedly find a child with a
      directory index value that can be swapped into a gap in the parent's index.
      That is, find children with index values that are beyond the number of children,
      and swap their index nodes with index nodes that represent gaps.
      """
      
      old_max_cutoff = None
      
      while True:
         
         # refresh the index max cutoff--it may have changed
         # NOTE: subtract 1, since this is the number of *allocated* index nodes the directory has (i.e. don't count the free one)
         max_node = MSEntryIndexCounter.read_dir_index( volume_id, parent_id )
         if max_node is None:
            # directory doesn't exist anymore...nothing to compactify 
            logging.info("Index node /%s/%s does not exist" % (volume_id, parent_id) )
            return 0
            
         parent_max_cutoff = max_node.value
         
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
         
         replaced_dir_index, child_idx = cls.__compactify_child( volume_id, parent_id, free_file_id, free_dir_index, parent_max_cutoff )
         
         if replaced_dir_index >= 0:
            # success!
            
            if compactify_continuation is not None:
               compactify_continuation( compacted_index_node=child_idx, replaced_index=replaced_dir_index )
               
            # verify that we didn't leave a gap by compactifying
            # (can happen if another process creates an entry while we're compactifying)
            new_max_node = MSEntryIndexCounter.read_dir_index( volume_id, parent_id )
            if new_max_node is None:
               # directory doesn't exist anymore...nothing to compactify 
               logging.info("Index node /%s/%s does not exist" % (volume_id, parent_id) )
               return 0
            
            new_parent_max_cutoff = new_max_node.value 
            
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
            logging.info("__compactify_child( /%s/%s index=%s threshold=%s ) rc = %s" % (volume_id, parent_id, free_dir_index, parent_max_cutoff, replaced_dir_index))
            continue 
         
         else:
            logging.error("BUG: failed to compactify /%s/%s, rc = %s\n", volume_id, parnet_id, replaced_dir_index )
            return replaced_dir_index
   
   
   @classmethod
   def Compactify( cls, volume_id, parent_id, free_file_id, free_dir_index, retry=True, compactify_continuation=None ):
      """
      Compactify the parent's index, trying again in a deferred task if need be.
      """
      
      try:
         
         # compact the index--move entries from the end of the index into the gaps 
         cls.__compactify_parent( volume_id, parent_id, free_file_id, free_dir_index, compactify_continuation=compactify_continuation )
         
         # account that there is now one less index node
         new_count = cls.__parent_index_dec( volume_id, parent_id )
         logging.info("Directory /%s/%s now has %s children" % (volume_id, parent_id, new_count))
         
      except storagetypes.RequestDeadlineExceededError:
         
         if retry:
            # keep trying
            storagetypes.deferred.defer( cls.__compactify, volume_id, parent_id, free_dir_index )
            return
         
         else:
            raise
      
      except Exception, e:
         logging.exception( e )
         raise e
      

   @classmethod 
   def Insert( cls, volume_id, parent_id, file_id, async=False ):
      """
      Insert an index node for a given entry (file_id) into its parent's (parent_id) index.
      
      Return the entry's dir_index and generation values on success.
      """
      
      @storagetypes.concurrent 
      def do_insert():
         
         next_dir_index, next_generation = yield cls.__parent_index_inc( volume_id, parent_id, async=True )
         
         # create the directory index node, but mark it as freed
         succeeded = yield cls.__alloc( volume_id, parent_id, file_id, next_dir_index, next_generation, async=True )
         
         if not succeeded:
            logging.error("BUG: failed to insert index node for /%s/%s in /%s/%s at (%s, %s)" % (volume_id, file_id, volume_id, parent_id, next_dir_index, next_generation))
            next_dir_index = -1
            next_generation = -1
            
         storagetypes.concurrent_return( (next_dir_index, next_generation) )
      
      result_fut = do_insert()
      if async:
         return result_fut 
      
      storagetypes.wait_futures( [result_fut] )
      
      return result_fut.get_result()
      
   
   @classmethod 
   def Free( cls, volume_id, parent_id, file_id, dir_index, async=False ):
      """
      Free an index node for a given entry (file_id) from its parent's (parent_id)'s index.
      The caller must know the directory index value, obtainable from a ReadIndex() call.
      
      NOTE: you should follow this call up with Compactify() to free up memory
      """
      
      return cls.__free( volume_id, parent_id, file_id, dir_index, async=async )
      
   
   @classmethod 
   def Delete( cls, volume_id, parent_id, file_id, dir_index, async=False, retry=True ):
      """
      Free and then compactify the index.  This will result in the directory index and 
      entry nodes getting deleted.
      """
      
      @storagetypes.concurrent 
      def do_delete():
         
         rc = yield cls.Free( volume_id, parent_id, file_id, dir_index, async=True )
         
         if not rc:
            logging.error("Failed to free index node /%s/%s (%s,%s)" % (volume_id, parent_id, file_id, dir_index))
            storagetypes.concurrent_return( -errno.EAGAIN )
         
         cls.Compactify( volume_id, parent_id, dir_index, retry=retry )
         
         storagetypes.concurrent_return( 0 )
         
      result_fut = do_delete()
      
      if async:
         return result_fut 
      
      storagetypes.wait_futures( [result_fut] )
      return result_fut.get_result()
      
   
   @classmethod 
   def GetNumChildren( cls, volume_id, parent_id, async=False ):
      """
      Get the number of children in a directory
      """
      return MSEntryIndexCounter.read_dir_index( volume_id, parent_id, async=async )
   
   @classmethod 
   def GetGeneration( cls, volume_id, parent_id, async=False ):
      """
      Get the generation number of a directory
      """
      return MSEntryIndexCounter.read_generation( volume_id, parent_id, async=async )
   
   @classmethod 
   def Read( cls, volume_id, parent_id, dir_index, async=False ):
      """
      Read a directory index node 
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
      Read an entry index node.
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
   