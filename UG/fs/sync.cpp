/*
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
*/

#include "sync.h"
#include "consistency.h"
#include "write.h"
#include "network.h"

// wait for our turn to run the metadata synchronization 
int fs_entry_sync_context_wait( struct sync_context* sync_ctx ) {
   int rc = md_download_sem_wait( &sync_ctx->sem, -1 );
   if( rc != 0 ) {
      errorf("md_download_sem_wait rc = %d\n", rc );
   }
   return rc;
}

// wake up the next synchronization context for a file.
// the resulting synchronization context should be held by (or available to) another thread, so we won't free it
// fent must be write-locked
int fs_entry_sync_context_wakeup_next( struct fs_entry* fent ) {
   struct sync_context* sync_ctx = NULL;
   
   int rc = fs_entry_sync_context_dequeue( fent, &sync_ctx );
   
   if( rc == 0 && sync_ctx != NULL ) {
      // weke up
      sem_post( &sync_ctx->sem );
   }
   
   return 0;
}

// send a remote gateway our write message for the file, possibly becoming coordinator in the process.
// fent must be write-locked.
// return 0 on success, 1 if we succeeded AND became the coordinator, or negative on error.
int fs_entry_remote_write_or_coordinate( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct sync_context* sync_ctx ) {
   int ret = 0;
   
   // tell the remote owner about our write
   Serialization::WriteMsg *write_msg = new Serialization::WriteMsg();
   Serialization::WriteMsg *write_ack = new Serialization::WriteMsg();
   
   // prepare the message
   fs_entry_prepare_write_message( write_msg, core, fs_path, sync_ctx->fent_snapshot, fent->write_nonce, sync_ctx->dirty_blocks );
   
   int rc = fs_entry_send_write_or_coordinate( core, fs_path, fent, write_msg, write_ack );
   
   if( rc > 0 ) {
      // we're the coordinator!
      ret = 1;
   }
   else {
      // coordinator gave us a reply
      if( rc >= 0 && write_ack->type() != Serialization::WriteMsg::PROMISE ) {
         // got something back, but not a PROMISE
         if( write_ack->type() == Serialization::WriteMsg::ERROR ) {
            if( write_ack->errorcode() == -ESTALE ) {
               // crucial file metadata changed out from under us.
               // we're going to have to try this again.
               dbprintf("file metadata mismatch; can't write to old version of %s\n", fs_path );
               
               fs_entry_mark_read_stale( fent );
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
   }
   delete write_ack;
   delete write_msg;
   
   return ret;
}


// flush in-core buffer blocks to cache for a particular file.
// fent must be write-locked
int fs_entry_flush_bufferred_blocks_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, vector<struct cache_block_future*>* cache_futs ) {
   
   modification_map bufferred_blocks;
   modification_map dirty_blocks;
   modification_map garbage_blocks;
   
   int rc = 0;
   
   // get bufferred blocks
   fs_entry_extract_bufferred_blocks( fent, &bufferred_blocks );
   
   // flush'em, but asynchronously
   for( modification_map::iterator itr = bufferred_blocks.begin(); itr != bufferred_blocks.end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* binfo = &itr->second;
      
      struct fs_entry_block_info old_binfo, new_binfo;
      
      memset( &old_binfo, 0, sizeof(old_binfo) );
      memset( &new_binfo, 0, sizeof(new_binfo) );
      
      // flush it, updating the manifest
      struct cache_block_future* fut = fs_entry_write_block_async( core, fs_path, fent, block_id, binfo->block_buf, binfo->block_len, &old_binfo, &new_binfo, &rc );
      if( rc < 0 || fut == NULL ) {
         errorf("fs_entry_write_block_async( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, rc );
         break;
      }
      
      // remember these, so we know to replicate them later
      dirty_blocks[ block_id ] = new_binfo;
      garbage_blocks[ block_id ] = old_binfo;
      
      cache_futs->push_back( fut );
   }
   
   if( rc == 0 ) {
      // merge dirty and garbage blocks into fent.
      // a bufferred block is a "new" dirty block, since it is guaranteed to be part of the last write to that block
      // (otherwise it would have been flushed on a subsequent write).
      fs_entry_merge_new_dirty_blocks( fent, &dirty_blocks );
      
      modification_map unmerged_garbage;
      
      fs_entry_merge_garbage_blocks( core, fent, fent->file_id, fent->version, &garbage_blocks, &unmerged_garbage );
      
      fs_entry_free_modification_map_ex( &unmerged_garbage, false );
   }
   
   return rc;
}


// initialize a synchronization context
// fent must be read-locked
int sync_context_init( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t parent_id, char const* parent_name, struct sync_context* sync_ctx ) {
   
   memset( sync_ctx, 0, sizeof(struct sync_context) );
   
   sync_ctx->fent_snapshot = CALLOC_LIST( struct replica_snapshot, 1 );
   
   fs_entry_replica_snapshot( core, fent, 0, 0, sync_ctx->fent_snapshot );
   fs_entry_extract_dirty_blocks( fent, &sync_ctx->dirty_blocks );
   fs_entry_extract_garbage_blocks( fent, &sync_ctx->garbage_blocks );
   fs_entry_to_md_entry( core, &sync_ctx->md_snapshot, fent, parent_id, parent_name );
   
   sync_ctx->replica_futures = new replica_list_t();
   
   sem_init( &sync_ctx->sem, 0, 0 );
   
   return 0;
}

// destroy a sync context 
int sync_context_free_ex( struct sync_context* sync_ctx, bool close_dirty_fds ) {
   
   md_entry_free( &sync_ctx->md_snapshot );
   
   if( sync_ctx->replica_futures ) {
      fs_entry_replica_list_free( sync_ctx->replica_futures );
      delete sync_ctx->replica_futures;
   }
   
   if( sync_ctx->dirty_blocks ) {
      fs_entry_free_modification_map_ex( sync_ctx->dirty_blocks, close_dirty_fds );
      delete sync_ctx->dirty_blocks;
   }
   
   if( sync_ctx->garbage_blocks ) {
      fs_entry_free_modification_map( sync_ctx->garbage_blocks );
      delete sync_ctx->garbage_blocks;
   }
   
   if( sync_ctx->fent_snapshot ) {
      free( sync_ctx->fent_snapshot );
      sync_ctx->fent_snapshot = NULL;
   }
   
   sem_destroy( &sync_ctx->sem );
   
   return 0;
}


int fs_entry_sync_context_free( struct sync_context* sync_ctx ) {
   return sync_context_free_ex( sync_ctx, true );
}

// snapshot fent, flush all in-core blocks to cache, and asynchronously replicate its data.
// on success, return 0, and populate fent_snapshot, md_snapshot (if non-NULL), _dirty_blocks, _garbage_blocks so we can go on to garbage-collect and update metadata (or revert the flush)
// fent must be write-locked
int fs_entry_sync_data_begin( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t parent_id, char const* parent_name, struct sync_context* _sync_ctx ) {
   
   int rc = 0;
   uint64_t file_id = fent->file_id;
   
   // dirty blocks and garbage blocks and cache futures and metadata
   struct sync_context sync_ctx;
   vector<struct cache_block_future*> cache_futs;
   
   // flush all bufferred blocks, asynchronously
   // (this updates fent's dirty_blocks and garbage_blocks)
   rc = fs_entry_flush_bufferred_blocks_async( core, fs_path, fent, &cache_futs );
   if( rc != 0 ) {
      errorf("fs_entry_flush_bufferred_blocks( %s %" PRIX64 " ) rc = %d\n", fs_path, fent->file_id, rc);
      return rc;
   }
   
   // while we're writing, extract snapshot of fent's dirty state
   sync_context_init( core, fs_path, fent, parent_id, parent_name, &sync_ctx );
   
   // wait for all cache writes to finish
   rc = fs_entry_flush_cache_writes( &cache_futs );
   if( rc != 0 ) {
      errorf("fs_entry_flush_cache_writes( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
      
      // restore dirty and garbage blocks 
      fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
      fs_entry_replace_garbage_blocks( fent, sync_ctx.garbage_blocks );
      md_entry_free( &sync_ctx.md_snapshot );
      
      return rc;
   }
   
   // free cache block futures
   // (preserving their file descriptors)
   fs_entry_cache_block_future_free_all( &cache_futs, false );
   
   // anything to replicate?  If not, return early.
   if( sync_ctx.dirty_blocks->size() == 0 && sync_ctx.garbage_blocks->size() == 0 ) {
      // done!
      memset( _sync_ctx, 0, sizeof(struct sync_context) );
      *_sync_ctx = sync_ctx;
      return 0;
   }
   
   // start replicating the manifest, if we're local
   struct replica_context* manifest_fut = NULL;
   
   if( FS_ENTRY_LOCAL( core, fent ) ) {
      
      // we're the coordinator for this file; replicate its manifests
      manifest_fut = fs_entry_replicate_manifest_async( core, fs_path, fent, &rc );
      
      // check for error
      if( manifest_fut == NULL || rc != 0 ) {
         errorf("fs_entry_replicate_manifest_async( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
         
         // restore dirty and garbage blocks 
         fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
         fs_entry_replace_garbage_blocks( fent, sync_ctx.garbage_blocks );
         md_entry_free( &sync_ctx.md_snapshot );
         
         return rc;
      }
   }
   
   // replicate blocks
   rc = fs_entry_replicate_blocks_async( core, fent, sync_ctx.dirty_blocks, sync_ctx.replica_futures );
   
   // check for error
   if( rc != 0 ) {
      errorf("fs_entry_replicate_blocks_async( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
      
      // cancel manifest, if we replicated it 
      if( manifest_fut ) {
         rc = fs_entry_garbage_collect_manifest( core, sync_ctx.fent_snapshot );
         
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest( %s %" PRIX64 ") rc = %d\n", fs_path, file_id, rc );
         }
      }
      
      // restore dirty and garbage blocks 
      fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
      fs_entry_replace_garbage_blocks( fent, sync_ctx.garbage_blocks );
      md_entry_free( &sync_ctx.md_snapshot );
      
      return rc;
   }
   
   // complete future list 
   if( manifest_fut ) {
      sync_ctx.replica_futures->push_back( manifest_fut );
   }
   
   // success!
   *_sync_ctx = sync_ctx;
   
   return 0;
}


// revert a data sync (i.e. on error).
// any written blocks that have not been overwritten and have not been flushed will be restored, so a subsequent data sync can try again later.
// TODO: garbage collection persistence
// fent must be write-locked.
int fs_entry_sync_data_revert( struct fs_core* core, struct fs_entry* fent, struct sync_context* sync_ctx ) {

   // which blocks were not replicated?
   modification_map unreplicated;
   
   // which blocks can we not merge back?
   modification_map unmerged_dirty;
   modification_map unmerged_garbage;
   
   uint64_t old_file_id = sync_ctx->fent_snapshot->file_id;
   int64_t old_file_version = sync_ctx->fent_snapshot->file_version;
   
   // free futures, but extract unreplicated bock information
   fs_entry_extract_block_info_from_failed_block_replicas( sync_ctx->replica_futures, &unreplicated );
   
   // merge old dirty and garbage blocks back in, since new writes will have superceded them.
   fs_entry_merge_old_dirty_blocks( core, fent, old_file_id, old_file_version, &unreplicated, &unmerged_dirty );        // don't overwrite subsequently-written data
   fs_entry_merge_garbage_blocks( core, fent, old_file_id, old_file_version, sync_ctx->garbage_blocks, &unmerged_garbage );       // TODO: deal with the case where subsequent writes garbage collect newer versions of the same block
   
   fs_entry_free_modification_map_ex( &unreplicated, false );        // keep unreplicated blocks' file descriptors open, so we can replicate them later
   fs_entry_free_modification_map_ex( &unmerged_dirty, true );       // unmerged blocks are overwritten.  Close their file descriptors, so they can be evicted.
   fs_entry_free_modification_map_ex( &unmerged_garbage, false );    // TODO: deal with the case where subsequent writes garbage collect newer versions of the same block
   
   // clear out any instances of this sync context 
   fs_entry_sync_context_remove( fent, sync_ctx );
   
   return 0;
}


// finish synchronizing data, and garbage collect it when finished.
// fent must NOT be locked
// return 0 on success
// return -EIO on failure (but if failed, revert the flush)
int fs_entry_sync_data_finish( struct fs_core* core, struct sync_context* sync_ctx ) {
   
   int rc = 0;

   // wait for all blocks (and possibly the manifest) to finish replicating 
   rc = fs_entry_replica_wait_all( core, sync_ctx->replica_futures, 0 );
   
   // if we fail, revert the flush 
   if( rc != 0 ) {
      errorf("fs_entry_replica_wait_all( %" PRIX64 " ) rc = %d\n", sync_ctx->fent_snapshot->file_id, rc );
      
      return -EIO;
   }
   
   return 0;
}


// begin synchronizing data, and enqueue ourselves into the sync queue so we replicate metadata in order.
// return 0 on success, return 1 if we need to wait to replicate metadata, and return negative on error
// if we need to wait, the caller should call fs_entry_sync_context_wait prior to replicating metadata.
// fent must be write-locked 
int fs_entry_fsync_begin_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx ) {
   int rc = 0;
   
   // replicate blocks, and manifest as well if we're the coordinator
   rc = fs_entry_sync_data_begin( core, fh->path, fh->fent, fh->parent_id, fh->parent_name, sync_ctx );

   if( rc != 0 ) {
      errorf("fs_entry_sync_data_begin( %s %" PRIX64 " ) rc = %d\n", fh->path, fh->fent->file_id, rc );
      
      return -EIO;
   }
   
   bool wait = false;
   
   // are we the first sync context to go?
   // if not, we'll have to wait our turn
   if( fs_entry_sync_context_size( fh->fent ) > 0 ) {
      wait = true;
   }
   
   // record ourselves as in progress
   fs_entry_sync_context_enqueue( fh->fent, sync_ctx );
   
   if( wait )
      return 1;
   else
      return 0;
}


// finish synchronizing data in fsync.
// begin_rc is the return value from fs_entry_fsync_begin_data
// return 0 on success 
int fs_entry_fsync_end_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx, int begin_rc ) {

   int rc = 0;
   
   // finish replication 
   rc = fs_entry_sync_data_finish( core, sync_ctx );
   
   if( rc != 0 ) {
      errorf("fs_entry_sync_data_finish( %s %" PRIX64 " ) rc = %d\n", fh->path, sync_ctx->fent_snapshot->file_id, rc );
      
      return -EREMOTEIO;
   }
   
   // wait our turn to replicate metadata, if we're not the first thread to replicate
   if( begin_rc > 0 )
      fs_entry_sync_context_wait( sync_ctx );
   
   return 0;
}


// synchronize metadata as part of an fsync.
// it is possible that we can become the coordinator of a file if we are currently not.
// if we become the coordinator, we also replicate the manifest.
// return 0 on success, return 1 if we became the coordinator, return negative on error.
// fh->fent must be write-locked
int fs_entry_fsync_metadata( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx ) {
   
   // if we're not the coordinator, tell the coordinator about the new blocks.
   bool local = FS_ENTRY_LOCAL( core, fh->fent );
   bool chcoord = false;
   int rc = 0;
   
   if( !local ) {
      // tell the coordinator about the new blocks
      rc = fs_entry_remote_write_or_coordinate( core, fh->path, fh->fent, sync_ctx );
      if( rc > 0 ) {
         // we're now the coordinator!
         local = true;
         chcoord = true;
      }
      else if( rc < 0 ) {
         // error 
         errorf("fs_entry_remote_write_or_coordinate( %s ) rc = %d\n", fh->path, rc );
         
         return rc;
      }
   }
   if( local ) {
      
      if( chcoord ) {
         
         // we became the coodinator, so we have to replicate the manifest 
         rc = fs_entry_replicate_manifest( core, fh->path, fh->fent );
         
         if( rc != 0 ) {
            errorf("fs_entry_replica_wait( %s manifest ) rc = %d\n", fh->path, rc );
            
            return rc;
         }
      }
      
      // we're the coordinator, so we have to synchronize metadata
      rc = ms_client_update( core->ms, &sync_ctx->md_snapshot );
      
      if( rc != 0 ) {
         errorf("ms_client_update( %s ) rc = %d\n", fh->path, rc );
         
         return rc;
      }
   }
   
   if( chcoord )
      return 1;
   else
      return 0;
}


// garbage-collect old data.
// metadata_rc is the return code from fs_entry_fsync_metadata
// return 0 on success, negative on error 
// fent must be write-locked
int fs_entry_fsync_garbage_collect( struct fs_core* core, struct fs_entry* fent, struct sync_context* sync_ctx, int metadata_rc ) {
   
   // garbage-collect!
   fs_entry_garbage_collect_blocks( core, sync_ctx->fent_snapshot, sync_ctx->garbage_blocks );
   
   if( metadata_rc > 0 ) {
      
      // get the old manifest too 
      fs_entry_garbage_collect_manifest( core, fent->old_snapshot );
      
      // preserve the current snapshot, so we can garbage-collect the manifest we just replicated
      memcpy( fent->old_snapshot, sync_ctx->fent_snapshot, sizeof( struct replica_snapshot ) );
      
      // TODO: work out how to do this when we change coordinators
   }
   
   return 0;
}

// run an fsync, once fh and fh->fent are write-locked.
// this will unlock fh->fent during replication, so other threads can access it.
// fh->fent will be re-write-locked before this method returns
// only use this if you know what you are doing--it is meant to deduplicate code for close() and fsync().
// return 0 on success
// return -EIO if we failed to start
// return -EREMOTEIO if we failed to replicate data
int fs_entry_fsync_locked( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx ) {

   int rc = 0;
   
   // start fsync
   int begin_rc = fs_entry_fsync_begin_data( core, fh, sync_ctx );
   
   if( begin_rc < 0 ) {
      errorf("fs_entry_sync_data_begin( %s %" PRIX64 " ) rc = %d\n", fh->path, fh->fent->file_id, begin_rc );
      
      return -EIO;
   }
   
   // allow other accesses to proceed while we replicate 
   fs_entry_unlock( fh->fent );
   
   // finish sync'ing data
   rc = fs_entry_fsync_end_data( core, fh, sync_ctx, begin_rc );
   
   if( rc != 0 ) {
      errorf("fs_entry_fsync_end_data( %s %" PRIX64 " ) rc = %d\n", fh->path, sync_ctx->fent_snapshot->file_id, rc );
      
      fs_entry_wlock( fh->fent );
      fs_entry_sync_data_revert( core, fh->fent, sync_ctx );
      
      sync_context_free_ex( sync_ctx, false );
      
      // let the next sync go
      fs_entry_sync_context_wakeup_next( fh->fent );
      
      return -EREMOTEIO;
   }
   
   // re-acquire
   fs_entry_wlock( fh->fent );
   
   // sync metadata, possibly becoming the coordinator
   int metadata_rc = fs_entry_fsync_metadata( core, fh, sync_ctx );
   
   if( rc < 0 ) {
      errorf("fs_entry_fsync_metadata( %s ) rc = %d\n", fh->path, rc );
      
      fs_entry_sync_data_revert( core, fh->fent, sync_ctx );
      
      sync_context_free_ex( sync_ctx, false );
      
      // let the next sync go
      fs_entry_sync_context_wakeup_next( fh->fent );
      
      return -EREMOTEIO;
   }
   
   // garbage-collect everything
   fs_entry_fsync_garbage_collect( core, fh->fent, sync_ctx, metadata_rc );
   
   return 0;
}


// sync a file's data and metadata with the MS and flush replicas
int fs_entry_fsync( struct fs_core* core, struct fs_file_handle* fh ) {
   fs_file_handle_wlock( fh );
   if( fh->fent == NULL ) {
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   int rc = 0;
   struct sync_context sync_ctx;
   
   fs_entry_wlock( fh->fent );
   
   rc = fs_entry_fsync_locked( core, fh, &sync_ctx );
   
   if( rc != 0 ) {
      // do we need to wake anyone up?
      
      fs_entry_unlock( fh->fent );
      fs_file_handle_unlock( fh );
      
      return rc;
   }
   
   // next fsync request 
   fs_entry_sync_context_wakeup_next( fh->fent );
   
   fs_entry_unlock( fh->fent );
   
   // flushed!
   fh->dirty = false;
   
   fs_file_handle_unlock( fh );
   
   // success!
   sync_context_free_ex( &sync_ctx, true );

   return rc;
}


// synchronize only a file's data
int fs_entry_fdatasync( struct fs_core* core, struct fs_file_handle* fh ) {
   // TODO
   return -ENOSYS;
}

