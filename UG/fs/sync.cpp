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
#include "vacuumer.h"
#include "syndicate.h"
#include "driver.h"

// wait for our turn to run the metadata synchronization 
int fs_entry_sync_context_wait( struct sync_context* sync_ctx ) {
   int rc = md_download_sem_wait( &sync_ctx->sem, -1 );
   if( rc != 0 ) {
      SG_error("md_download_sem_wait rc = %d\n", rc );
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
// return 0 on successful remote write, 1 if we succeeded AND became the coordinator in the process, or negative on error.
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
      if( rc >= 0 && write_ack->type() != Serialization::WriteMsg::ACCEPTED ) {
         // got something back, but not an ACCEPTED
         if( write_ack->type() == Serialization::WriteMsg::ERROR ) {
            if( write_ack->errorcode() == -ESTALE ) {
               // crucial file metadata changed out from under us.
               // we're going to have to try this again.
               SG_debug("file metadata mismatch; can't write to old version of %s\n", fs_path );
               
               fs_entry_mark_read_stale( fent );
               ret = -EAGAIN;
            }
            else {
               SG_error( "remote write error = %d (%s)\n", write_ack->errorcode(), write_ack->errortxt().c_str() );
               ret = -abs( write_ack->errorcode() );
            }
         }
         else {
            SG_error( "remote write invalid message %d\n", write_ack->type() );
            ret = -EIO;
         }
      }
   }
   delete write_ack;
   delete write_msg;
   
   return ret;
}


// flush dirty in-core buffer blocks to disk cache for a particular file.
// this will update the manifest with the new versions of the blocks, as well as advance its mtime
// return 0 on success, negative on error
// fent must be write-locked
int fs_entry_flush_bufferred_blocks_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, vector<struct md_cache_block_future*>* cache_futs ) {
   
   modification_map bufferred_blocks;
   modification_map dirty_blocks;
   modification_map old_blocks;
   
   int rc = 0;
   
   // get bufferred blocks
   fs_entry_extract_bufferred_blocks( fent, &bufferred_blocks );
   
   SG_debug("%" PRIX64 " has %zu bufferred blocks\n", fent->file_id, bufferred_blocks.size() );
   
   // flush'em, but asynchronously
   for( modification_map::iterator itr = bufferred_blocks.begin(); itr != bufferred_blocks.end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* binfo = &itr->second;
      
      // don't flush bufferred blocks that we simply read in.
      if( !binfo->dirty )
         continue;
      
      SG_debug("Flush bufferred block %" PRIX64 ".%" PRId64 "[%" PRIu64 "]\n", fent->file_id, fent->version, block_id );
      
      struct fs_entry_block_info old_binfo, new_binfo;
      
      memset( &old_binfo, 0, sizeof(old_binfo) );
      memset( &new_binfo, 0, sizeof(new_binfo) );
      
      // flush it, updating the manifest
      struct md_cache_block_future* fut = fs_entry_write_block_async( core, fs_path, fent, block_id, binfo->block_buf, binfo->block_len, &old_binfo, &new_binfo, &rc );
      if( rc < 0 || fut == NULL ) {
         SG_error("fs_entry_write_block_async( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, rc );
         break;
      }
      
      // remember these, so we know to replicate them later
      dirty_blocks[ block_id ] = new_binfo;
      
      if( rc > 0 ) {
         // rc > 0 indicates that there is a garbage block
         old_blocks[ block_id ] = old_binfo;
         rc = 0;
      }
      
      cache_futs->push_back( fut );
   }
   
   if( rc == 0 ) {
      // merge dirty blocks into fent.
      // a bufferred block is a "new" dirty block, since it is guaranteed to be part of the last write to that block
      // (otherwise it would have been flushed on a subsequent write).
      fs_entry_merge_new_dirty_blocks( fent, &dirty_blocks );
      
      fs_entry_free_modification_map( &bufferred_blocks );
      fs_entry_free_modification_map( &old_blocks );
   }
   else {
      // put bufferred blocks back
      fs_entry_emplace_bufferred_blocks( fent, &bufferred_blocks );
      
      // revert the block versions (NOTE: the old end block is simply the current end block)
      fs_entry_revert_blocks( core, fent, fs_entry_block_id( core, fent->size ), &old_blocks );
      
      // clear out any blocks we cached
      fs_entry_cache_evict_blocks_async( core, fent, &dirty_blocks );
   }
   
   return rc;
}


// initialize a synchronization context
// fent must be read-locked
int sync_context_init( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t parent_id, char const* parent_name, struct sync_context* sync_ctx ) {
   
   SG_debug("initialize sync context at %p\n", sync_ctx );
   
   memset( sync_ctx, 0, sizeof(struct sync_context) );
   
   sync_ctx->fent_snapshot = SG_CALLOC( struct replica_snapshot, 1 );
   
   fs_entry_replica_snapshot( core, fent, 0, 0, sync_ctx->fent_snapshot );
   fs_entry_extract_dirty_blocks( fent, &sync_ctx->dirty_blocks );
   fs_entry_copy_garbage_blocks( fent, &sync_ctx->garbage_blocks );
   fs_entry_to_md_entry( core, &sync_ctx->md_snapshot, fent, parent_id, parent_name );
   
   sync_ctx->replica_futures = new replica_list_t();
   
   sem_init( &sync_ctx->sem, 0, 0 );
   
   return 0;
}

// destroy a sync context 
int sync_context_free_ex( struct sync_context* sync_ctx, bool close_dirty_fds ) {
   
   SG_debug("free sync context at %p\n", sync_ctx );
   
   md_entry_free( &sync_ctx->md_snapshot );
   
   if( sync_ctx->replica_futures ) {
      SG_debug("free sync context %p replica futures %p\n", sync_ctx, sync_ctx->replica_futures );
      fs_entry_replica_list_free( sync_ctx->replica_futures );
      delete sync_ctx->replica_futures;
   }
   else {
      SG_error("WARN: sync context %p replica futures = %p\n", sync_ctx, sync_ctx->replica_futures );
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
   
   memset( sync_ctx, 0, sizeof(struct sync_context) );
   
   return 0;
}


int fs_entry_sync_context_free( struct sync_context* sync_ctx ) {
   return sync_context_free_ex( sync_ctx, true );
}


// snapshot fent, flush all in-core blocks to cache, and asynchronously replicate its data.
// on success, return SYNC_SUCCESS, and populate fent_snapshot, md_snapshot (if non-NULL), _dirty_blocks, _garbage_blocks so we can go on to garbage-collect and update metadata (or revert the flush)
// return SYNC_NOTHING if there's nothing to replicate
// fent must be write-locked
int fs_entry_sync_data_begin( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t parent_id, char const* parent_name, struct sync_context* _sync_ctx ) {
   
   int rc = 0;
   uint64_t file_id = fent->file_id;
   
   // dirty blocks and garbage blocks and cache futures and metadata
   struct sync_context sync_ctx;
   vector<struct md_cache_block_future*> cache_futs;

   memset( &sync_ctx, 0, sizeof(struct sync_context) );
   
   // flush all bufferred blocks, asynchronously
   // (this updates fent's dirty_blocks)
   rc = fs_entry_flush_bufferred_blocks_async( core, fs_path, fent, &cache_futs );
   if( rc < 0 ) {
      SG_error("fs_entry_flush_bufferred_blocks( %s %" PRIX64 " ) rc = %d\n", fs_path, fent->file_id, rc);
      
      memset( _sync_ctx, 0, sizeof(struct sync_context) );
      return rc;
   }
   
   // while we're writing, extract snapshot of fent's dirty state
   sync_context_init( core, fs_path, fent, parent_id, parent_name, &sync_ctx );
   
   // wait for all cache writes to finish
   rc = md_cache_flush_writes( &cache_futs );
   if( rc != 0 ) {
      SG_error("md_cache_flush_writes( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
      
      // restore dirty blocks 
      fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
      
      sync_ctx.dirty_blocks = NULL;
      
      // clean up garbage blocks
      fs_entry_free_modification_map_ex( sync_ctx.garbage_blocks, false );
      delete sync_ctx.garbage_blocks;
      
      sync_ctx.garbage_blocks = NULL;
      
      // free up the sync context
      fs_entry_sync_context_free( &sync_ctx );
      
      memset( _sync_ctx, 0, sizeof(struct sync_context) );
      return rc;
   }
   
   // free cache block futures
   // (preserving their file descriptors)
   md_cache_block_future_free_all( &cache_futs, false );
   
   // anything to replicate?  If not, return early.
   if( sync_ctx.dirty_blocks->size() == 0 && sync_ctx.garbage_blocks->size() == 0 ) {
      // done!
      SG_debug("Nothing to replicate for %" PRIX64 "\n", fent->file_id );
      *_sync_ctx = sync_ctx;
      return SYNC_NOTHING;
   }
   
   // start replicating the manifest, if we're local
   struct replica_context* manifest_fut = NULL;
   
   if( FS_ENTRY_LOCAL( core, fent ) ) {
      
      // we're the coordinator for this file; replicate its manifests
      manifest_fut = fs_entry_replicate_manifest_async( core, fs_path, fent, &rc );
      
      // check for error
      if( manifest_fut == NULL || rc != 0 ) {
         SG_error("fs_entry_replicate_manifest_async( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
         
         // restore dirty blocks 
         fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
         sync_ctx.dirty_blocks = NULL;
         
         
         // clean up garbage blocks
         fs_entry_free_modification_map_ex( sync_ctx.garbage_blocks, false );
         delete sync_ctx.garbage_blocks;
         sync_ctx.garbage_blocks = NULL;
         
         fs_entry_sync_context_free( &sync_ctx );
         
         memset( _sync_ctx, 0, sizeof(struct sync_context) );
         return rc;
      }
   }
   
   // replicate blocks
   rc = fs_entry_replicate_blocks_async( core, fent, sync_ctx.dirty_blocks, sync_ctx.replica_futures );
   
   // check for error
   if( rc != 0 ) {
      SG_error("fs_entry_replicate_blocks_async( %s %" PRIX64 " ) rc = %d\n", fs_path, file_id, rc );
      
      // cancel manifest, if we replicated it 
      if( manifest_fut ) {
         
         SG_debug("garbage collect new manifest for %" PRIX64 "(%s), snapshot = %" PRIX64 ".%" PRId64 "\n", fent->file_id, fent->name, sync_ctx.fent_snapshot->file_id, sync_ctx.fent_snapshot->file_version );
         
         rc = fs_entry_garbage_collect_manifest( core, sync_ctx.fent_snapshot );
         
         if( rc != 0 ) {
            SG_error("fs_entry_garbage_collect_manifest( %s %" PRIX64 ") rc = %d\n", fs_path, file_id, rc );
         }
      }
      
      // restore dirty and garbage blocks 
      fs_entry_replace_dirty_blocks( fent, sync_ctx.dirty_blocks );
      sync_ctx.dirty_blocks = NULL;
      
      // clean up garbage blocks
      fs_entry_free_modification_map_ex( sync_ctx.garbage_blocks, false );
      delete sync_ctx.garbage_blocks;
      sync_ctx.garbage_blocks = NULL;
      
      fs_entry_sync_context_free( &sync_ctx );
      
      memset( _sync_ctx, 0, sizeof(struct sync_context) );
      return rc;
   }
   
   // complete future list 
   if( manifest_fut ) {
      sync_ctx.replica_futures->push_back( manifest_fut );
      sync_ctx.manifest_fut = manifest_fut;
   }
   
   // success!
   *_sync_ctx = sync_ctx;
   SG_debug("initialized sync context %p\n", _sync_ctx );
   
   return SYNC_SUCCESS;
}


// revert a data sync (i.e. on error).
// any written blocks that have not been overwritten and have not been flushed will be restored, so a subsequent data sync can try again later.
// fent must be write-locked.
int fs_entry_sync_data_revert( struct fs_core* core, struct fs_entry* fent, struct sync_context* sync_ctx ) {

   SG_debug("Reverting synchronization for (%s) %" PRIX64 "\n", fent->name, fent->file_id );
   
   // which blocks were not replicated?
   modification_map unreplicated;
   
   // which blocks can we not merge back?
   modification_map unmerged_dirty;
   
   uint64_t old_file_id = sync_ctx->fent_snapshot->file_id;
   int64_t old_file_version = sync_ctx->fent_snapshot->file_version;
   
   // free futures, but extract unreplicated bock information
   fs_entry_extract_block_info_from_failed_block_replicas( sync_ctx->replica_futures, &unreplicated );
   
   // merge old dirty blocks back in, since new writes will have superceded them.
   // don't overwrite subsequently-written data.
   fs_entry_merge_old_dirty_blocks( core, fent, old_file_id, old_file_version, &unreplicated, &unmerged_dirty );
   
   // free everything
   // TODO: cache-evict unreplicated?
   fs_entry_free_modification_map_ex( &unreplicated, false );        // keep unreplicated blocks' file descriptors open, so we can replicate them later
   
   // NOTE: no need to free unmerged_dirty, since it only contains pointers to data in unreplicated (which we just freed)
   
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
      SG_error("fs_entry_replica_wait_all( %" PRIX64 " ) rc = %d\n", sync_ctx->fent_snapshot->file_id, rc );
      
      return -EIO;
   }
   
   return 0;
}


// begin synchronizing data, and enqueue ourselves into the sync queue so we replicate metadata in order.
// return SYNC_SUCCESS on success, return SYNC_WAIT if we need to wait to replicate metadata, return SYNC_NOTHING if there's nothing to replicate, and return negative on error
// if we need to wait, the caller should call fs_entry_sync_context_wait prior to replicating metadata.
// fent must be write-locked 
int fs_entry_fsync_begin_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx ) {
   int rc = 0;
   
   // replicate blocks, and manifest as well if we're the coordinator
   rc = fs_entry_sync_data_begin( core, fh->path, fh->fent, fh->parent_id, fh->parent_name, sync_ctx );

   if( rc < 0 ) {
      SG_error("fs_entry_sync_data_begin( %s %" PRIX64 " ) rc = %d\n", fh->path, fh->fent->file_id, rc );
      
      return -EIO;
   }
   
   if( rc == SYNC_NOTHING ) {
      // there's nothing to replicate 
      return SYNC_NOTHING;
   }
   
   bool wait = false;
   
   // are we the first sync context to go?
   // if not, we'll have to wait our turn
   if( rc == SYNC_SUCCESS && fs_entry_sync_context_size( fh->fent ) > 0 ) {
      wait = true;
   }
   
   // record ourselves as in progress
   fs_entry_sync_context_enqueue( fh->fent, sync_ctx );
   
   if( wait )
      return SYNC_WAIT;
   else
      return SYNC_SUCCESS;
}


// finish synchronizing data in fsync.
// begin_rc is the return value from fs_entry_fsync_begin_data
// return 0 on success 
int fs_entry_fsync_end_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx, int begin_rc ) {

   if( begin_rc == SYNC_NOTHING ) {
      // nothing to replicate, nothing to wait for 
      SG_debug("Nothing to wait for in replicating data for %s %" PRIX64 "\n", fh->path, sync_ctx->fent_snapshot->file_id );
      return 0;
   }
   
   int rc = 0;
   
   // finish replication 
   rc = fs_entry_sync_data_finish( core, sync_ctx );
   
   if( rc != 0 ) {
      SG_error("fs_entry_sync_data_finish( %s %" PRIX64 " ) rc = %d\n", fh->path, sync_ctx->fent_snapshot->file_id, rc );
      
      return -EREMOTEIO;
   }
   
   // wait our turn to replicate metadata, if we're not the first thread to replicate
   if( begin_rc == SYNC_WAIT )
      fs_entry_sync_context_wait( sync_ctx );
   
   return rc;
}


// synchronize metadata as part of an fsync.
// it is possible that we can become the coordinator of a file if we are currently not.
// if we become the coordinator, we also replicate the manifest.
// return 0 on success, return 1 if we succeeded AND are currently the coordinator, return negative on error.
// unlike fs_entry_fsync_metadata, this method returns 1 if we were the coordinator all along
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
         SG_error("fs_entry_remote_write_or_coordinate( %s ) rc = %d\n", fh->path, rc );
         
         return rc;
      }
   }
   if( local ) {
      
      if( chcoord ) {
         
         // we became the coodinator, so we have to replicate the manifest 
         rc = fs_entry_replicate_manifest( core, fh->path, fh->fent );
         
         if( rc != 0 ) {
            SG_error("fs_entry_replica_wait( %s manifest ) rc = %d\n", fh->path, rc );
            
            return rc;
         }
      }
      
      // we're the coordinator, so we have to synchronize metadata.
      // calculate affected blocks 
      uint64_t* affected_blocks = NULL;
      size_t num_affected_blocks = 0;
      
      fs_entry_list_block_ids( sync_ctx->dirty_blocks, &affected_blocks, &num_affected_blocks );
      
      rc = ms_client_update_write( core->ms, &fh->fent->write_nonce, &sync_ctx->md_snapshot, affected_blocks, num_affected_blocks );
      
      // free memory 
      if( affected_blocks != NULL ) {
         free( affected_blocks );
         affected_blocks = NULL;
      }
      
      // check status
      if( rc != 0 ) {
         SG_error("ms_client_update_write( %s ) rc = %d\n", fh->path, rc );
         
         return rc;
      }
   }
   
   if( chcoord || local )
      return 1;
   else
      return 0;
}


// initialize a completion map 
static int fs_entry_fsync_completion_map_init( sync_completion_map_t* completion_map, uint64_t file_id, int64_t file_version, modification_map* old_blocks ) {
   
   SG_debug("will complete %zu blocks\n", old_blocks->size() );
   
   for( modification_map::iterator itr = old_blocks->begin(); itr != old_blocks->end(); itr++ ) {
      
      struct sync_gc_block_info gc_block_info;
      
      gc_block_info.file_id = file_id;
      gc_block_info.file_version = file_version;
      gc_block_info.block_id = itr->first;
      gc_block_info.block_version = itr->second.version;
      
      (*completion_map)[ gc_block_info ] = SYNC_COMPLETION_MAP_STATUS_UNKNOWN;
      
      SG_debug("expect completed: %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", file_id, file_version, itr->first, itr->second.version );
   }
   
   return 0;
}


// set up a garbage collection cls 
int fs_entry_fsync_gc_cls_init( struct sync_gc_cls* gc_cls, struct fs_core* core, struct fs_vacuumer* vac, char const* fs_path, struct replica_snapshot* old_fent, modification_map* old_blocks, bool gc_manifest ) {
   
   memset( gc_cls, 0, sizeof(struct sync_gc_cls) );
   
   gc_cls->core = core;
   gc_cls->vac = vac;
   gc_cls->gc_manifest = gc_manifest;
   gc_cls->fs_path = strdup(fs_path);
   
   // duplicate snapshot
   memcpy( &gc_cls->old_snapshot, old_fent, sizeof( struct replica_snapshot ) );
   
   gc_cls->completion_map = new sync_completion_map_t();
   
   fs_entry_fsync_completion_map_init( gc_cls->completion_map, old_fent->file_id, old_fent->file_version, old_blocks );
   
   pthread_mutex_init( &gc_cls->lock, NULL );
   
   return 0;
}

// free a gc_cls 
int fs_entry_fsync_gc_cls_free( struct sync_gc_cls* gc_cls ) {
   
   if( gc_cls->completion_map ) {
      delete gc_cls->completion_map;
      gc_cls->completion_map = NULL;
   }
   
   if( gc_cls->fs_path ) {
      free( gc_cls->fs_path );
      gc_cls->fs_path = NULL;
   }
   
   pthread_mutex_destroy( &gc_cls->lock );
   
   memset( gc_cls, 0, sizeof(struct sync_gc_cls) );
   
   return 0;
}


// continuation for garbage collection: once the manifest has been garbage collected, request the vacuumer to vacuum the old vacuum log entries 
int fs_entry_fsync_gc_manifest_cont( struct rg_client* rg, struct replica_context* rctx, void* cls ) {
   
   SG_debug("continue manifest for %p\n", rctx );
   
   struct sync_gc_cls* gc_cls = (struct sync_gc_cls*)cls;
   
   pthread_mutex_lock( &gc_cls->lock );
   
   struct replica_snapshot* old_snapshot = &gc_cls->old_snapshot;
   
   int rc = 0;
   
   if( rctx->error != 0 ) {
      // failed to garbage-collect the manifest
      SG_error("Failed to garbage collect manifest %" PRIX64 "/manifest.%" PRId64 ".%d, replica context rc = %d\n",
             fs_entry_replica_context_get_file_id( rctx ), old_snapshot->manifest_mtime_sec, old_snapshot->manifest_mtime_nsec, rctx->error );
      
      rc = -EAGAIN;
   }
   else {
      // got the manifest!  delete the vacuum log entry for it on the MS, in the background 
      fs_entry_vacuumer_log_entry_bg( gc_cls->vac, gc_cls->fs_path, old_snapshot );
   }
   
   pthread_mutex_unlock( &gc_cls->lock );
   
   // clean up 
   fs_entry_fsync_gc_cls_free( gc_cls );
   free( gc_cls );
   
   return rc;
}


// is a completion map filled?
static bool fs_entry_is_completion_map_filled( sync_completion_map_t* completion_map ) {
   // none of the values can be initialized to SYNC_COMPLETION_MAP_STATUS_UNKNOWN 
   bool finished = true;
   
   for( sync_completion_map_t::iterator itr = completion_map->begin(); itr != completion_map->end(); itr++ ) {
      
      if( itr->second == SYNC_COMPLETION_MAP_STATUS_UNKNOWN ) {
         finished = false;
         break;
      }
   }
   
   return finished;
}


// convert a replica context into a sync_gc_block_info 
static int fs_entry_replica_context_to_gc_block_info( struct sync_gc_block_info* gc_info, struct replica_context* rctx ) {
   
   // must be a replica context for a block 
   int replica_type = fs_entry_replica_context_get_type( rctx );
   if( replica_type != REPLICA_CONTEXT_TYPE_BLOCK ) {
      return -EINVAL;
   }
   
   memset( gc_info, 0, sizeof(struct sync_gc_block_info) );
   
   struct replica_snapshot* old_snapshot = fs_entry_replica_context_get_snapshot( rctx );
   
   gc_info->file_id = fs_entry_replica_context_get_file_id( rctx );
   gc_info->file_version = old_snapshot->file_version;
   gc_info->block_id = fs_entry_replica_context_get_block_id( rctx );
   gc_info->block_version = fs_entry_replica_context_get_block_version( rctx );
   
   return 0;
}


// continuation for garbage collection: once the blocks have been garbage-collected, enqueue the manifest for garbage collection
int fs_entry_fsync_gc_block_cont( struct rg_client* rg, struct replica_context* rctx, void* cls ) {
   
   struct sync_gc_cls* gc_cls = (struct sync_gc_cls*)cls;
   
   // completed this context
   struct sync_gc_block_info gc_block_info;
   int rc = fs_entry_replica_context_to_gc_block_info( &gc_block_info, rctx );
   if( rc != 0 ) {
      
      // couldn't process
      SG_error("fs_entry_extract_gc_block_info(%p) rc = %d\n", rctx, rc );
      return rc;
   }
   
   SG_debug("continue blocks for %p (%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "])\n", rctx, gc_block_info.file_id, gc_block_info.file_version, gc_block_info.block_id, gc_block_info.block_version );
    
   bool unlocked = false;
   pthread_mutex_lock( &gc_cls->lock );
   
   // verify that this rctx is present in the completion map.
   // it should be; it's a bug otherwise!
   sync_completion_map_t::iterator itr = gc_cls->completion_map->find( gc_block_info );
   if( itr == gc_cls->completion_map->end() ) {
      
      pthread_mutex_unlock( &gc_cls->lock );
      
      // wrong context
      SG_error("invalid replica context %p\n", rctx );
      
      return -EINVAL;
   }
   
   // get the status
   int rctx_rc = fs_entry_replica_context_get_error( rctx );
   
   (*gc_cls->completion_map)[ gc_block_info ] = rctx_rc;
   
   // if failed, remember, so we don't garbage-collect the manifest
   if( rctx_rc != 0 && gc_cls->rc == 0 ) {
      gc_cls->rc = rctx_rc;
   }
   
   // if the completion map is full, and all blocks were garbage-collected, then queue the manifest 
   bool blocks_finished = fs_entry_is_completion_map_filled( gc_cls->completion_map );
   
   if( blocks_finished ) {
      
      if( gc_cls->rc == 0 ) {
         // successfully garbage-collected all blocks (this is the last block context)
         
         struct replica_snapshot* old_snapshot = &gc_cls->old_snapshot;
         
         if( gc_cls->gc_manifest ) {
            
            // We should garbage-collect the manifest 
            rc = fs_entry_garbage_collect_manifest_ex( gc_cls->core, old_snapshot, NULL, REPLICATE_BACKGROUND, fs_entry_fsync_gc_manifest_cont, gc_cls );
            
            if( rc != 0 ) {
               // failed to enqueue; we're done
               SG_error("fs_entry_garbage_collect_manifest_ex( %" PRIX64 "/manifest.%" PRId64 ".%d ) rc = %d\n",
                     fs_entry_replica_context_get_file_id( rctx ), old_snapshot->manifest_mtime_sec, old_snapshot->manifest_mtime_nsec, rc );
               
               gc_cls->rc = rc;
            }
         }
         
         if( !gc_cls->gc_manifest || rc != 0 ) {
            
            // we're done--either due to error, or because we're not supposed to go any further
            if( rc != 0 ) {
               SG_error("Not going to garbage-collect manifest %" PRIX64 "/manifest.%" PRId64 ".%d due to error (code %d)\n",
                     fs_entry_replica_context_get_file_id( rctx ), old_snapshot->manifest_mtime_sec, old_snapshot->manifest_mtime_nsec, rc );
            }
            
            pthread_mutex_unlock( &gc_cls->lock );
            unlocked = true;
            
            // clean up 
            fs_entry_fsync_gc_cls_free( gc_cls );
            free( gc_cls );
         }
      }
      else {
         SG_error("Garbage collection for %" PRIX64 " failed, rc = %d\n", fs_entry_replica_context_get_file_id( rctx ), gc_cls->rc );
         
         // unsuccessful.  we're done
         pthread_mutex_unlock( &gc_cls->lock );
         unlocked = true;
         
         // clean up 
         fs_entry_fsync_gc_cls_free( gc_cls );
         free( gc_cls );
      }
   }
   
   if( !unlocked )
      pthread_mutex_unlock( &gc_cls->lock );
   
   return 0;
}


// top-level garbage collection method for a write.
// kicks off garbage collection in the background.
// asks the driver whether or not to proceed with the blocks.
// fent must be read-locked.
// NOTE: the replica_snapshot doesn't have to be from fent.  fent is only needed for the driver.
int fs_entry_garbage_collect_kickoff( struct fs_core* core, char const* fs_path, struct replica_snapshot* gc_snapshot, modification_map* garbage_blocks, bool gc_manifest ) {
   
   SG_debug("Garbage collect %zu blocks; garbage collect manifest = %d\n", garbage_blocks->size(), gc_manifest );
   
   // tell the driver that we're going to garbage-collect the blocks 
   uint64_t* garbage_block_ids = SG_CALLOC( uint64_t, garbage_blocks->size() );
   int64_t* garbage_block_versions = SG_CALLOC( int64_t, garbage_blocks->size() );
   
   int i = 0;
   
   for( modification_map::iterator itr = garbage_blocks->begin(); itr != garbage_blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      int64_t block_version = itr->second.version;
      
      garbage_block_ids[i] = block_id;
      garbage_block_versions[i] = block_version;
      
      i++;
   }
      
   // tell the driver 
   int rc = driver_garbage_collect( core, core->closure, fs_path, gc_snapshot, garbage_block_ids, garbage_block_versions, garbage_blocks->size() );
   
   free( garbage_block_ids );
   free( garbage_block_versions );
   
   if( rc == DRIVER_NOT_GARBAGE ) {
      // this data will be handled by the driver
      SG_debug("Driver indicates that write for %s %" PRIX64 " at %" PRId64 ".%d is not garbage\n", fs_path, gc_snapshot->file_id, gc_snapshot->manifest_mtime_sec, gc_snapshot->manifest_mtime_nsec );
      return 0;
   }
   else if( rc < 0 ) {
      // error 
      SG_error("driver_garbage_collect(%s %" PRIX64 " at %" PRId64 ".%d rc = %d\n", fs_path, gc_snapshot->file_id, gc_snapshot->manifest_mtime_sec, gc_snapshot->manifest_mtime_nsec, rc );
      return rc;
   }
   
   // good to go!
   // start blocks, and continue to the manifest.
   // set up continuation cls
   struct sync_gc_cls* gc_cls = SG_CALLOC( struct sync_gc_cls, 1 );
   
   fs_entry_fsync_gc_cls_init( gc_cls, core, &core->state->vac, fs_path, gc_snapshot, garbage_blocks, gc_manifest );
   
   rc = fs_entry_garbage_collect_blocks_ex( core, gc_snapshot, garbage_blocks, NULL, REPLICATE_BACKGROUND, fs_entry_fsync_gc_block_cont, gc_cls );
   if( rc != 0 ) {
      SG_error("fs_entry_garbage_collect_blocks_ex(%" PRIX64 ") rc = %d\n", gc_snapshot->file_id, rc );
   }
   
   return rc;
}


// kick off garbage collection in the background
// NOTE: this erases the pointer sync_ctx->garbage_blocks, and puts it into the sync_gc_cls that gets passed into block and manifest continuations
int fs_entry_fsync_garbage_collect( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct sync_context* sync_ctx, bool was_coordinator ) {
   
   if( sync_ctx->garbage_blocks->size() == 0 ) {
      // nothing to do--the manifest won't be stale, since nothing has changed
      return 0;
   }
   
   int rc = fs_entry_garbage_collect_kickoff( core, fs_path, fent->old_snapshot, sync_ctx->garbage_blocks, was_coordinator );
   
   if( rc != 0 ) {
      SG_error("fs_entry_garbage_collect_blocks_ex(%" PRIX64 " (%s)) rc = %d\n", fent->file_id, fent->name, rc );
   }
   return rc;
}

// argument structure for truncating queued sync contexts
struct sync_context_truncate_args {
   struct fs_core* core;
   struct fs_entry* fent;
};


// apply the effects of a truncate to a queued sync context 
void fs_entry_sync_context_truncate( struct sync_context* sync_ctx, void* cls ) {
   
   struct sync_context_truncate_args* truncate_args = (struct sync_context_truncate_args*)cls;
   
   struct fs_core* core = truncate_args->core;
   struct fs_entry* fent = truncate_args->fent;
   
   uint64_t max_block_id = fs_entry_block_id( core, fent->size );
   
   // blocks in this sync context have not yet been replicated, so just clear out the ones that are no longer part of the file
   for( modification_map::iterator itr = sync_ctx->dirty_blocks->begin(); itr != sync_ctx->dirty_blocks->end(); ) {
      
      modification_map::iterator curr = itr;
      itr++;
      
      uint64_t block_id = curr->first;
      struct fs_entry_block_info* binfo = &curr->second;
      
      // drop it if we need to
      if( block_id > max_block_id ) {
         fs_entry_block_info_free_ex( binfo, true );    // close the cache fd, since we won't be replicating it 
         sync_ctx->dirty_blocks->erase( curr );
      }
   }
   
   // re-snapshot this 
   fs_entry_replica_snapshot( core, fent, 0, 0, sync_ctx->fent_snapshot );
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
   int gc_rc = 0;
   
   // do we have data to flush?  if not, then done 
   if( !fh->dirty ) {
      SG_debug("Not dirtied by handle %p: %s %" PRIX64 "\n", fh, fh->path, fh->fent->file_id );
      return 0;
   }
   
   // start fsync
   int begin_rc = fs_entry_fsync_begin_data( core, fh, sync_ctx );
   
   if( begin_rc < 0 ) {
      SG_error("fs_entry_fsync_begin_data( %s %" PRIX64 " ) rc = %d\n", fh->path, fh->fent->file_id, begin_rc );
      
      return -EIO;
   }
   
   // will we need to garbage collect the manifest, if we succeed?
   bool was_coordinator = FS_ENTRY_LOCAL( core, fh->fent );
   
   // allow other accesses to proceed while we replicate 
   fs_entry_unlock( fh->fent );
   
   // finish sync'ing data
   rc = fs_entry_fsync_end_data( core, fh, sync_ctx, begin_rc );
   
   if( rc != 0 ) {
      SG_error("fs_entry_fsync_end_data( %s %" PRIX64 " ) rc = %d\n", fh->path, sync_ctx->fent_snapshot->file_id, rc );
      
      // re-acquire
      fs_entry_wlock( fh->fent );
      
      // revert the sync
      fs_entry_sync_data_revert( core, fh->fent, sync_ctx );
      
      // free memory
      sync_context_free_ex( sync_ctx, false );
      
      // let the next sync go
      fs_entry_sync_context_wakeup_next( fh->fent );
      
      return -EREMOTEIO;
   }
   
   // re-acquire
   fs_entry_wlock( fh->fent );
   
   bool replicate_metadata = true;
   
   // did we get re-versioned (i.e. due to a truncate)?
   if( fh->fent->version != sync_ctx->fent_snapshot->file_version ) {
      // no need to replicate metadata--the truncate pushed that for us.
      replicate_metadata = false;
      
      // garbage-collect everything for this sync context
      gc_rc = fs_entry_fsync_garbage_collect( core, fh->path, fh->fent, sync_ctx, was_coordinator );
      
      // the truncate occurred "after" all of the pending sync requests, so apply it to them (bringing them "forward" in time)
      
      struct sync_context_truncate_args args;
      memset( &args, 0, sizeof(struct sync_context_truncate_args) );
      args.core = core;
      args.fent = fh->fent;
      
      fs_entry_sync_queue_apply( fh->fent, fs_entry_sync_context_truncate, &args );
   }
   
   // only replicate metadata if there was data to replicate 
   if( begin_rc >= 0 && begin_rc != SYNC_NOTHING && replicate_metadata ) {
      
      // sync metadata, possibly becoming the coordinator
      int metadata_rc = fs_entry_fsync_metadata( core, fh, sync_ctx );
      
      if( metadata_rc < 0 ) {
         SG_error("fs_entry_fsync_metadata( %s ) rc = %d\n", fh->path, metadata_rc );
         
         // revert the sync
         fs_entry_sync_data_revert( core, fh->fent, sync_ctx );
         
         // free memory
         sync_context_free_ex( sync_ctx, false );
         
         // let the next sync go
         fs_entry_sync_context_wakeup_next( fh->fent );
         
         return -EREMOTEIO;
      }
      
      // garbage-collect everything
      gc_rc = fs_entry_fsync_garbage_collect( core, fh->path, fh->fent, sync_ctx, was_coordinator );
   }
   
   // let the next sync go
   fs_entry_sync_context_wakeup_next( fh->fent );
   
   // success!  Advance our knowledge of which state has been replicated
   fs_entry_clear_garbage_blocks( fh->fent );
   fs_entry_setup_garbage_blocks( fh->fent );
   
   fs_entry_store_snapshot( fh->fent, sync_ctx->fent_snapshot );
   
   if( gc_rc != 0 ) {
      SG_error("fs_entry_fsync_garbage_collect(%" PRIX64 ") rc = %d\n", fh->fent->file_id, gc_rc );
      return gc_rc;
   }
   else {
      
      // flushed!
      fh->dirty = false;
      return 0;
   }
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
   memset( &sync_ctx, 0, sizeof(struct sync_context) );
   
   fs_entry_wlock( fh->fent );
   
   rc = fs_entry_fsync_locked( core, fh, &sync_ctx );
   
   if( rc != 0 ) {
      
      SG_error("fs_entry_fsync_locked(%" PRIX64 ") rc = %d\n", fh->fent->file_id, rc );
      
      fs_entry_unlock( fh->fent );
      fs_file_handle_unlock( fh );
      
      sync_context_free_ex( &sync_ctx, false );
      
      return rc;
   }
   
   fs_entry_unlock( fh->fent );
   
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

