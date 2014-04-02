/*
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
*/

#include "write.h"
#include "replication.h"
#include "read.h"
#include "consistency.h"
#include "cache.h"
#include "driver.h"

// can a block be garbage-collected?
static bool fs_entry_is_garbage_collectable_block( struct fs_core* core, off_t fent_size, uint64_t block_id ) {
   
   // no blocks exist, so this is guaranteed new
   if( fent_size == 0 )
      return false;
   
   // block is beyond the last block in the file, so guaranteed new
   if( block_id > ((uint64_t)fent_size / core->blocking_factor) && fent_size > 0 )
      return false;
   
   // block is just on the edge of the file, so guaranteed new
   if( block_id >= ((uint64_t)fent_size / core->blocking_factor) && fent_size > 0 && (fent_size % core->blocking_factor == 0) )
      return false;
   
   // has an older copy to be removed
   return true;
}


// replicate a new manifest and delete the old one.
// fent must be write-locked
// fh must be write-locked
int fs_entry_replace_manifest( struct fs_core* core, struct fs_file_handle* fh, struct fs_entry* fent, struct replica_snapshot* fent_snapshot_prewrite ) {
   // replicate our new manifest
   int rc = fs_entry_replicate_manifest( core, fh->path, fent, false, fh );
   if( rc != 0 ) {
      errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fh->path, rc );
      rc = -EIO;
   }
   else {
      if( fh->flags & O_SYNC ) {
         // wait for all replicas to finish, since we're synchronous
         fs_entry_replicate_wait( core, fh );
      }
   }
   
   if( rc == 0 ) {
      // garbage-collect the old manifest.
      // First, update the snapshot to indicate that we coordinate this file
      uint64_t old_writer_id = fent_snapshot_prewrite->writer_id;
      fent_snapshot_prewrite->writer_id = fent->coordinator;
      
      fs_entry_garbage_collect_manifest( core, fent_snapshot_prewrite );
      
      // restore
      fent_snapshot_prewrite->writer_id = old_writer_id;
      
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fh->path, rc );
         rc = 0;
      }
   }
   
   return rc;
}


// write a block to a file (processing it first), asynchronously putting it on underlying storage, and updating the filesystem entry's manifest to refer to it.
// this will update the modtime of the fs_entry on success.
// return a cache_write_future for it.
// fent MUST BE WRITE LOCKED, SINCE WE MODIFY THE MANIFEST
struct cache_block_future* fs_entry_write_block_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_data, size_t write_len, bool evict_old_block, int* _rc ) {
   
   int64_t old_block_version = -1;
   int64_t new_block_version = fs_entry_next_block_version();
   
   *_rc = 0;
   
   int rc = 0;
   
   // do pre-upload write processing...
   char* processed_block = NULL;
   size_t processed_block_len = 0;
   
   rc = driver_write_block_preup( core->closure, fs_path, fent, block_id, new_block_version, block_data, write_len, &processed_block, &processed_block_len );
   if( rc != 0 ) {
      errorf("driver_write_block_preup(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, new_block_version, rc );
      *_rc = rc;
      return NULL;
   }
   
   // hash the contents of this block and the processed block, including anything read with fs_entry_read_block
   unsigned char* block_hash = BLOCK_HASH_DATA( processed_block, processed_block_len );
   
   if( evict_old_block ) {
      old_block_version = fent->manifest->get_block_version( block_id );
   
      dbprintf("evict %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", fent->file_id, fent->version, block_id, old_block_version );
      
      // evict the old block
      rc = fs_entry_cache_evict_block( core, core->cache, fent->file_id, fent->version, block_id, old_block_version );
      if( rc != 0 && rc != -ENOENT ) {
         errorf("fs_entry_cache_evict_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", fent->file_id, fent->version, block_id, old_block_version, rc );
      }
      
      rc = 0;
   }
   
   // for debugging...
   char prefix[21];
   memset( prefix, 0, 21 );
   memcpy( prefix, processed_block, MIN( 20, processed_block_len ) );
   
   // cache the new block.  Get back the future (caller will manage it).
   struct cache_block_future* f = fs_entry_cache_write_block_async( core, core->cache, fent->file_id, fent->version, block_id, new_block_version, processed_block, processed_block_len, false, &rc );
   if( f == NULL ) {
      errorf("WARN: failed to cache %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], rc = %dn", fent->file_id, fent->version, block_id, new_block_version, rc );
      *_rc = rc;
      free( block_hash );
      return NULL;
   }
   else {
      dbprintf("cache %zu bytes for %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]: data: '%s'...\n", processed_block_len, fent->file_id, fent->version, block_id, new_block_version, prefix );
      
      // update the manifest
      fs_entry_manifest_put_block( core, core->gateway, fent, block_id, new_block_version, block_hash );
      
      // update our modtime
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );
      
      fent->mtime_sec = ts.tv_sec;
      fent->mtime_nsec = ts.tv_nsec;
      
      free( block_hash );
      
      return f;
   }
}


// write one block of data 
// fh must be write-locked
// fh->fent must be write-locked
struct cache_block_future* fs_entry_write_one_block( struct fs_core* core, struct fs_file_handle* fh, uint64_t block_id, char* block, size_t num_affected_bytes,
                                                     modification_map* modified_blocks, modification_map* overwritten_blocks, int* ret ) {

   *ret = 0;
   
   // need to get the old version of this block, so we can garbage-collect it 
   bool need_garbage_collect = fs_entry_is_garbage_collectable_block( core, fh->fent->size, block_id );
   int64_t old_version = 0;
   unsigned char* old_hash = NULL;
   
   if( need_garbage_collect ) {
      // this block is guaranteed to exist in the manifest...
      old_version = fh->fent->manifest->get_block_version( block_id );
      old_hash = fh->fent->manifest->get_block_hash( block_id );
   }
   
   // write the data and update the manifest...
   int rc = 0;
   struct cache_block_future* block_fut = fs_entry_write_block_async( core, fh->path, fh->fent, block_id, block, num_affected_bytes, need_garbage_collect, &rc );
   
   if( block_fut == NULL ) {
      errorf("fs_entry_write_block_async(%s/%" PRId64 ", num_affected_bytes=%zu) failed, rc = %d\n", fh->path, block_id, num_affected_bytes, rc );
      *ret = -EIO;
      return NULL;
   }
   
   // get the hash of the newly-written block...
   unsigned char* new_hash = fh->fent->manifest->get_block_hash( block_id );
   int64_t new_version = fh->fent->manifest->get_block_version( block_id );
   
   char* hash_printable = BLOCK_HASH_TO_STRING( new_hash );
   dbprintf("hash of %" PRIX64 "[%" PRId64 ".%" PRIu64 "] is %s, num_affected_bytes=%zu\n", fh->fent->file_id, block_id, new_version, hash_printable, num_affected_bytes );
   free( hash_printable );
   
   // is this a block to garbage collect?
   if( need_garbage_collect ) {
      
      // writing this block succeeded.
      // mark the old version of the block that we've overwritten to be garbage-collected
      struct fs_entry_block_info binfo_overwritten;
      
      fs_entry_block_info_garbage_init( &binfo_overwritten, old_version, old_hash, BLOCK_HASH_LEN(), core->gateway );
      
      (*overwritten_blocks)[ block_id ] = binfo_overwritten;
   }
   
   // record that we've written this block
   struct fs_entry_block_info binfo;
   
   // NOTE: we're passing the block_fd into the fs_entry_block_info structure from the cache_block_future.  DO NOT CLOSE IT!
   fs_entry_block_info_replicate_init( &binfo, new_version, new_hash, BLOCK_HASH_LEN(), core->gateway, block_fut->block_fd );
   
   (*modified_blocks)[ block_id ] = binfo;
   
   return block_fut;
}


// wait for all cache writes to finish 
int fs_entry_finish_writes( list<struct cache_block_future*>& block_futures, bool close_fds ) {
   
   int rc = 0;
   
   // wait for all cache writes to finish
   for( list<struct cache_block_future*>::iterator itr = block_futures.begin(); itr != block_futures.end(); itr++ ) {
      struct cache_block_future* f = *itr;
      
      int frc = fs_entry_cache_block_future_wait( f );
      if( frc != 0 ) {
         errorf("WARN: fs_entry_cach_block_future_wait rc = %d\n", frc );
      }
      
      if( f->write_rc < 0 ) {
         errorf("WARN: write %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", f->key.file_id, f->key.file_version, f->key.block_id, f->key.block_version, f->write_rc );
         rc = f->write_rc;
      }
      else if( !close_fds ) {
         // don't close the fd (replication subsystem takes care of this)
         fs_entry_cache_block_future_release_fd( f );
      }
      
      // free memory
      fs_entry_cache_block_future_free( f );
   }
   
   return rc;
}


// replicate all data from a write.
// fh must be write-locked.
// fh->fent must be write-locked
int fs_entry_replicate_write( struct fs_core* core, struct fs_file_handle* fh, modification_map* modified_blocks ) {
   
   int ret = 0;
   
   struct timespec replicate_ts, ts2;
   
   // if we wrote data, replicate the manifest and blocks.
   if( modified_blocks->size() > 0 ) {
      
      if( FS_ENTRY_LOCAL( core, fh->fent ) ) {
         BEGIN_TIMING_DATA( replicate_ts );
   
         // replicate the new manifest
         int rc = fs_entry_replicate_manifest( core, fh->path, fh->fent, false, fh );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fh->path, rc );
            ret = -EIO;
         }
         
         END_TIMING_DATA( replicate_ts, ts2, "replicate manifest" );
      }

      if( ret >= 0 ) {
         // replicate written blocks
         BEGIN_TIMING_DATA( replicate_ts );
         
         int rc = fs_entry_replicate_blocks( core, fh->fent, modified_blocks, false, fh );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_write(%s) rc = %d\n", fh->path, rc );
            ret = -EIO;
         }
         
         END_TIMING_DATA( replicate_ts, ts2, "replicate block data" );
      }
      
      if( fh->flags & O_SYNC ) {
         // wait for all replicas to finish, since we're synchronous
         fs_entry_replicate_wait( core, fh );
      }
   }
   
   return ret;
}


// send a remote gateway our write message for the file, possibly becoming coordinator in the process.
// file handle fh must be write-locked.
// fh->fent must be write-locked.
// return 0 on success, 1 if we succeeded AND became the coordinator, or negative on error.
int fs_entry_remote_write_or_coordinate( struct fs_core* core, struct fs_file_handle* fh, struct replica_snapshot* fent_old_snapshot, uint64_t start_id, uint64_t end_id ) {
   int ret = 0;
   
   // tell the remote owner about our write
   Serialization::WriteMsg *write_msg = new Serialization::WriteMsg();

   // send a prepare message
   int64_t* versions = fh->fent->manifest->get_block_versions( start_id, end_id );
   unsigned char** hashes = fh->fent->manifest->get_block_hashes( start_id, end_id );
   
   fs_entry_prepare_write_message( write_msg, core, fh->path, fh->fent, start_id, end_id, versions, hashes );
   
   free( versions );
   FREE_LIST( hashes );
   
   Serialization::WriteMsg *write_ack = new Serialization::WriteMsg();
   
   int rc = fs_entry_send_write_or_coordinate( core, fh->path, fh->fent, fent_old_snapshot, write_msg, write_ack );
   
   if( rc > 0 ) {
      // we're the coordinator!
      ret = 1;
   }
   
   if( rc >= 0 && write_ack->type() != Serialization::WriteMsg::PROMISE ) {
      // got something back, but not a PROMISE
      if( write_ack->type() == Serialization::WriteMsg::ERROR ) {
         if( write_ack->errorcode() == -ESTALE ) {
            // crucial file metadata changed out from under us.
            // we're going to have to try this again.
            dbprintf("file metadata mismatch; can't write to old version of %s\n", fh->path );
            
            fs_entry_mark_read_stale( fh->fent );
            ret = -EAGAIN;
         }
         else {
            errorf( "remote write error = %d (%s)\n", write_ack->errorcode(), write_ack->errortxt().c_str() );
            ret = -abs( write_ack->errorcode() );
         }
      }
      else {
         errorf( "remote write invalid message %d\n", write_ack->type() );
         ret = -EIO;
      }
   }

   delete write_ack;
   delete write_msg;
   
   return ret;
}


// send the MS the new file metadata
// fh must be at least read-locked
// fh->fent must be at least read-locked
int fs_entry_send_metadata_update( struct fs_core* core, struct fs_file_handle* fh ) {

   int ret = 0;
   
   // synchronize the new modifications with the MS
   struct md_entry ent;
   fs_entry_to_md_entry( core, &ent, fh->fent, fh->parent_id, fh->parent_name );

   int up_rc = 0;
   char const* errstr = NULL;
   
   if( fh->fent->max_write_freshness > 0 && !(fh->flags & O_SYNC) ) {
      up_rc = ms_client_queue_update( core->ms, &ent, currentTimeMillis() + fh->fent->max_write_freshness, 0 );
      errstr = "ms_client_queue_update";
   }
   else {
      up_rc = ms_client_update( core->ms, &ent );
      errstr = "ms_client_update";
   }
   
   md_entry_free( &ent );

   if( up_rc != 0 ) {
      errorf("%s(%s) rc = %d\n", errstr, fh->path, up_rc );
      ret = -EREMOTEIO;
   }
   
   return ret;
}


// garbage collect a file's old manifest and blocks
int fs_entry_garbage_collect_overwritten_data( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, modification_map* overwritten_blocks ) {

   int rc = 0;
   
   if( FS_ENTRY_LOCAL( core, fent ) ) {
      // garbage collect the old manifest
      rc = fs_entry_garbage_collect_manifest( core, fent_before_write );
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_manifest(%" PRIX64 ".%" PRId64 " (%s)) rc = %d\n", fent->file_id, fent->version, fent->name, rc );
         rc = 0;
      }
   }
   
   // garbage-collect written blocks
   if( overwritten_blocks->size() > 0 ) {
      rc = fs_entry_garbage_collect_blocks( core, fent_before_write, overwritten_blocks );
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_blocks(%" PRIX64 ".%" PRId64 " (%s)) rc = %d\n", fent->file_id, fent->version, fent->name, rc );
         rc = 0;
      }
   }
   
   return 0;
}


// revert a write, given the modified blocks and a pre-write snapshot.
// fent must be write-locked
int fs_entry_revert_write( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, uint64_t new_size,
                           modification_map* new_block_info, modification_map* old_block_info, bool garbage_collect_manifest ) {

   
   struct replica_snapshot fent_after_write;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_after_write );
   
   // cancel any ongoing replications, and erase recently-uploaded data (reverting the write)
   if( new_block_info )
      fs_entry_garbage_collect_blocks( core, &fent_after_write, new_block_info );
   
   if( garbage_collect_manifest )
      fs_entry_garbage_collect_manifest( core, &fent_after_write );
   
   // had an error along the way.  Restore the old fs_entry's manifest
   fs_entry_replica_snapshot_restore( core, fent, fent_before_write );
   
   uint64_t old_end_block = fent_before_write->size / core->blocking_factor;
   uint64_t proposed_end_block = new_size / core->blocking_factor;
   
   if( old_end_block < proposed_end_block ) {
      // truncate the manifest back to its original size
      fent->manifest->truncate( old_end_block );
   }
   
   // restore old block information
   if( old_block_info ) {
      for( modification_map::iterator itr = old_block_info->begin(); itr != old_block_info->end(); itr++ ) {
         uint64_t block_id = itr->first;
         
         // skip blocks written beyond the end of the original manifest
         if( block_id > old_end_block )
            continue;
         
         struct fs_entry_block_info* old_binfo = &itr->second;
         
         fs_entry_manifest_put_block( core, old_binfo->gateway_id, fent, block_id, old_binfo->version, old_binfo->hash );
      }
   }
   
   // evict newly-written blocks
   if( new_block_info ) {
      for( modification_map::iterator itr = new_block_info->begin(); itr != new_block_info->end(); itr++ ) {
         int evict_rc = fs_entry_cache_evict_block( core, core->cache, fent->file_id, fent->version, itr->first, itr->second.version );
         if( evict_rc != 0 && evict_rc != -ENOENT ) {
            errorf("fs_entry_cache_evict_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", fent->file_id, fent->version, itr->first, itr->second.version, evict_rc );
         }
      }
   }
   
   return 0;
}

                  
// write data to a file
// Zeroth, revalidate path and manifest and optionally expand the file if we're writing beyond the end of it.
// First, write blocks to disk for subsequent re-read and for serving to other UGs.
// Second, replicate blocks to all RGs.
// Third, if this file is local, send the MS the new file metadata.  Otherwise, send a remote-write message to the coordinator.
ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset ) {
   // sanity check
   if( count == 0 )
      return 0;
   
   // lock handle--prevent the file from being destroyed
   fs_file_handle_rlock( fh );
   if( fh->fent == NULL || fh->open_count <= 0 ) {
      // invalid
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   struct timespec ts, ts2;
   struct timespec write_ts, replicate_ts_total, garbage_collect_ts, remote_write_ts, update_ts;

   BEGIN_TIMING_DATA( ts );
   
   int rc = fs_entry_revalidate_metadata( core, fh->path, fh->fent, NULL );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_metadata(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   fs_entry_wlock( fh->fent );
   
   BEGIN_TIMING_DATA( write_ts );

   ssize_t ret = 0;
   ssize_t num_written = 0;

   // record which blocks we've modified
   modification_map modified_blocks;
   
   // record the blocks we're garbage-collecting (i.e. old block info)
   modification_map overwritten_blocks;
   
   // did we replicate the manifest?
   bool replicated_manifest = false;
   
   // list of futures created when writing to the cache
   list<struct cache_block_future*> block_futures;
   
   // snapshot fent before we do anything to it
   struct replica_snapshot fent_old_snapshot;
   fs_entry_replica_snapshot( core, fh->fent, 0, 0, &fent_old_snapshot );

   fs_entry_unlock( fh->fent );
   
   // generate a block to be pushed into the cache.
   char* block = CALLOC_LIST( char, core->blocking_factor );
   
   while( (size_t)num_written < count ) {
      
      // which block are we about to write?
      uint64_t block_id = fs_entry_block_id( core, offset + num_written );
      
      // make sure the write is aligned to the block size.
      // what is the write offset into the block?
      off_t block_write_offset = (offset + num_written) % core->blocking_factor;
      off_t block_fill_offset = 0;
      
      // how much data are we going to consume from the write buffer and write into this block?
      size_t consume_len = MIN( core->blocking_factor - block_write_offset, count - num_written );
      
      if( block_write_offset != 0 ) {
         
         // need to fill this block with the contents of the current block first, since we're writing unaligned
         ssize_t read_rc = fs_entry_read_block( core, fh->path, fh->fent, block_id, block, core->blocking_factor );
         if( read_rc < 0 ) {
            errorf("fs_entry_read_block( %s ) rc = %d\n", fh->path, (int)read_rc );
            rc = (int)read_rc;
            break;
         }
         
         // fill the rest of the block at the unaligned offset
         block_fill_offset = block_write_offset;
      }
      
      // get the data...
      memcpy( block + block_fill_offset, buf + num_written, consume_len );
      
      // how much of this block is affected by this write?  Include both the number of bytes consumed, as well as bytes pulled in from reading unaligned
      size_t num_affected_bytes = block_fill_offset + consume_len;
      
      fs_entry_wlock( fh->fent );
      
      struct cache_block_future* block_fut = fs_entry_write_one_block( core, fh, block_id, block, num_affected_bytes, &modified_blocks, &overwritten_blocks, &rc );
      
      fs_entry_unlock( fh->fent );
      
      if( rc != 0 ) {
         ret = -EIO;
         break;
      }
      
      num_written += consume_len;
      
      // preserve this, so we can synchronize later 
      block_futures.push_back( block_fut );
      
      // next block 
      memset( block, 0, core->blocking_factor );
   }
   
   free( block );

   if( ret >= 0 )
      ret = count;
   
   // wait for all writes to the cache to complete.
   int wait_rc = fs_entry_finish_writes( block_futures, false );
   if( wait_rc != 0 ) {
      errorf("fs_entry_finish_writes() rc = %d\n", wait_rc );
      
      // don't mask a previous error...
      if( ret > 0 )
         ret = wait_rc;
   }
   
   END_TIMING_DATA( write_ts, ts2, "write data" );

   fs_entry_wlock( fh->fent );
   
   // update file metadata
   if( ret > 0 ) {
      // update size
      // NOTE: size may have changed due to expansion, but it shouldn't affect this computation
      fh->fent->size = MAX( (off_t)fh->fent->size, offset + (off_t)count );
      
      // update mtime
      struct timespec new_mtime;
      clock_gettime( CLOCK_REALTIME, &new_mtime );
      
      fh->fent->mtime_sec = new_mtime.tv_sec;
      fh->fent->mtime_nsec = new_mtime.tv_nsec;
   }

   BEGIN_TIMING_DATA( replicate_ts_total );
   
   // begin replicating the manifest and blocks (wait till they complete if the file is opened with O_SYNC).
   rc = fs_entry_replicate_write( core, fh, &modified_blocks );
   if( rc != 0 ) {
      errorf("fs_Entry_replicate_write rc = %d\n", rc );
      ret = rc;
   }
   else {
      if( FS_ENTRY_LOCAL( core, fh->fent ) ) {
         // we will have replicated the manifest 
         replicated_manifest = true;
      }
   }

   END_TIMING_DATA( replicate_ts_total, ts2, "replicate data" );
   
   // send the remote gateway our new block versions and hashes, or become the coordinator.
   // If we're the coordinator, finish the write by uploading the new metadata.
   if( ret > 0 ) {
      if( !FS_ENTRY_LOCAL( core, fh->fent ) ) {
         // send the remote gateway our write request 
         
         BEGIN_TIMING_DATA( remote_write_ts );

         rc = fs_entry_remote_write_or_coordinate( core, fh, &fent_old_snapshot, modified_blocks.begin()->first, modified_blocks.rbegin()->first + 1 );
         if( rc > 0 ) {
            
            // we're now the coordinator.  Replicate our new manifest and remove the old one.
            rc = fs_entry_replace_manifest( core, fh, fh->fent, &fent_old_snapshot );
            if( rc == 0 ) {
               replicated_manifest = true;
            }
            else {
               // failed to replicate!
               errorf("fs_entry_replicate_manifest(%s (%" PRId64 ".%d)) rc = %d\n", fh->path, fh->fent->mtime_sec, fh->fent->mtime_nsec, rc );
               ret = -EIO;
            }
         }
         
         END_TIMING_DATA( remote_write_ts, ts2, "send remote write" );
      }
      
      // last step: do we need to send an update to the MS?
      // either we were local all along, or we just became the coordinator
      if( ret > 0 && FS_ENTRY_LOCAL( core, fh->fent ) ) {
         
         BEGIN_TIMING_DATA( update_ts );
         
         int rc = fs_entry_send_metadata_update( core, fh );
         if( rc != 0 ) {
            errorf("fs_entry_send_metadata_update(%s) rc = %d\n", fh->path, rc );
            ret = rc;
         }
         
         END_TIMING_DATA( update_ts, ts2, "MS update" );
      }
   }
   
   if( ret < 0 ) {
      // revert the write
      fs_entry_revert_write( core, fh->fent, &fent_old_snapshot, fh->fent->size, &modified_blocks, &overwritten_blocks, replicated_manifest );
   }
   else {
      
      // begin garbage-collecting overwritten data.
      BEGIN_TIMING_DATA( garbage_collect_ts );
      
      fs_entry_garbage_collect_overwritten_data( core, fh->fent, &fent_old_snapshot, &overwritten_blocks );
      
      END_TIMING_DATA( garbage_collect_ts, ts2, "garbage collect data" );
   }
   
   fs_entry_unlock( fh->fent );
   fs_file_handle_unlock( fh );
   
   fs_entry_free_modification_map( &overwritten_blocks );
   fs_entry_free_modification_map( &modified_blocks );
   
   END_TIMING_DATA( ts, ts2, "write" );

   return ret;
}


ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset ) {
   // TODO
   return -ENOSYS;
}


// Handle a remote write.  The given write_msg must have been verified prior to calling this method.
// Zeroth, sanity check.
// First, update the local manifest.
// Second, synchronously replicate the manifest to all RGs.
// Third, upload new metadata to the MS for this file.
// Fourth, acknowledge the remote writer.
int fs_entry_remote_write( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t coordinator_id, Serialization::WriteMsg* write_msg ) {
   
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   int err = 0;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, write_msg->user_id(), write_msg->volume_id(), true, &err, &parent_id, &parent_name );
   if( err != 0 || fent == NULL ) {
      return err;
   }
   
   // validate
   if( fent->file_id != file_id ) {
      errorf("Remote write to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", fs_path, file_id, fent->file_id );
      fs_entry_unlock( fent );
      free( parent_name );
      return -EINVAL;
   }
   
   if( fent->coordinator != coordinator_id ) {
      errorf("Remote write to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", fs_path, coordinator_id, fent->coordinator );
      fs_entry_unlock( fent );
      free( parent_name );
      return -ESTALE;
   }
   
   if( fent->version != file_version ) {
      errorf("Remote write to file %s version %" PRId64 ", expected %" PRId64 "\n", fs_path, file_version, fent->version );
      fs_entry_unlock( fent );
      free( parent_name );
      return -ESTALE;
   }
   
   // validate fields
   unsigned int num_blocks = write_msg->blocks().end_id() - write_msg->blocks().start_id();
   if( (unsigned)write_msg->blocks().version_size() != num_blocks ) {
      errorf("Invalid write message: number of blocks = %u, but number of versions = %u\n", num_blocks, write_msg->blocks().version_size() );
      fs_entry_unlock( fent );
      free( parent_name );
      return -EINVAL;
   }
   
   if( (unsigned)write_msg->blocks().hash_size() != num_blocks ) {
      errorf("Invalid write message: number of blocks = %u, but number of hashes = %u\n", num_blocks, write_msg->blocks().hash_size() );
      fs_entry_unlock( fent );
      free( parent_name );
      return -EINVAL;
   }
   
   // snapshot the fent so we can garbage-collect the manifest 
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );
   
   uint64_t gateway_id = write_msg->gateway_id();

   struct timespec ts, ts2, replicate_ts, garbage_collect_ts, update_ts;
   struct timespec mts;
   
   BEGIN_TIMING_DATA( ts );

   modification_map old_block_info;
   
   // update the blocks
   for( unsigned int i = 0; i < write_msg->blocks().end_id() - write_msg->blocks().start_id(); i++ ) {
      uint64_t block_id = i + write_msg->blocks().start_id();
      int64_t new_version = write_msg->blocks().version(i);
      unsigned char* block_hash = (unsigned char*)write_msg->blocks().hash(i).data();

      // back up old version and gateway, in case we have to restore it
      int64_t old_version = fent->manifest->get_block_version( block_id );
      uint64_t old_gateway_id = fent->manifest->get_block_host( core, block_id );
      unsigned char* old_block_hash = fent->manifest->hash_dup( block_id );
      
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(struct fs_entry_block_info) );
      
      // remember the old block information, in case we need to revert it
      fs_entry_block_info_garbage_init( &binfo, old_version, old_block_hash, BLOCK_HASH_LEN(), old_gateway_id );
      
      old_block_info[ block_id ] = binfo;
      
      // put the new version into the manifest
      fs_entry_manifest_put_block( core, gateway_id, fent, block_id, new_version, block_hash );
   }
   
   fent->size = write_msg->metadata().size();

   clock_gettime( CLOCK_REALTIME, &mts );

   fent->mtime_sec = mts.tv_sec;
   fent->mtime_nsec = mts.tv_nsec;
   
      
   // replicate the manifest, synchronously
   BEGIN_TIMING_DATA( replicate_ts );

   err = fs_entry_replicate_manifest( core, fs_path, fent, true, NULL );
   if( err != 0 ) {
      errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fs_path, err );
      err = -EIO;
   }
   
   END_TIMING_DATA( replicate_ts, ts2, "replicate manifest" );
   
   if( err == 0 ) {
   
      BEGIN_TIMING_DATA( update_ts );
      
      // replicated!
      // propagate the update to the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, fent, parent_id, parent_name );
      
      uint64_t max_write_freshness = fent->max_write_freshness;
      uint64_t file_id = fent->file_id;
      int64_t file_version = fent->version;
      
      fs_entry_unlock( fent );
      
      // NOTE: this will send the update immediately if max_write_freshness == 0
      err = ms_client_queue_update( core->ms, &data, currentTimeMillis() + max_write_freshness, 0 );
      if( err != 0 ) {
         errorf("%ms_client_queue_update(%s) rc = %d\n", fs_path, err );
         err = -EREMOTEIO;
      }
      
      md_entry_free( &data );      
      
      END_TIMING_DATA( update_ts, ts2, "MS update" );
      
      if( err == 0 ) {
         // metadata update succeeded!
         // garbage-collect the old manifest
         
         BEGIN_TIMING_DATA( garbage_collect_ts );
         
         int rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
            rc = -EIO;
         }
         
         END_TIMING_DATA( garbage_collect_ts, ts2, "garbage collect manifest" );
         
         // evict cached blocks 
         for( modification_map::iterator itr = old_block_info.begin(); itr != old_block_info.end(); itr++ ) {
            int evict_rc = fs_entry_cache_evict_block( core, core->cache, file_id, file_version, itr->first, itr->second.version );
            if( evict_rc != 0 && evict_rc != -ENOENT ) {
               errorf("fs_entry_cache_evict_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", file_id, file_version, itr->first, itr->second.version, evict_rc );
            }
         }
      }
   }
   else {
      // revert the write
      fs_entry_revert_write( core, fent, &fent_snapshot, fent->size, NULL, &old_block_info, true );
      
      fs_entry_unlock( fent );
   }
   
   free( parent_name );
   
   // free memory
   fs_entry_free_modification_map( &old_block_info );
   
   END_TIMING_DATA( ts, ts2, "write, remote" );
   return err;
}
