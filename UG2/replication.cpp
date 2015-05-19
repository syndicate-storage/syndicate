/*
   Copyright 2015 The Trustees of Princeton University

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

#include "replication.h"
#include "inode.h"
#include "core.h"
#include "sync.h"

// block replication context
struct UG_chunk_replication_context {
   
   struct UG_dirty_block* block;
   struct SG_manifest* manifest;
   
   SG_messages::Request* request;
   size_t chunk_queue_idx;
};

typedef map< struct md_download_context*, struct UG_chunk_replication_context > UG_chunk_replication_set_t;


// set up a block replication context 
// always succeeds
// NOTE: chunk_ctx takes ownership of request, and a call to UG_chunk_replication_context_free will free it
int UG_chunk_replication_context_init( struct UG_chunk_replication_context* chunk_ctx, struct SG_manifest* manifest, struct UG_dirty_block* block, SG_messages::Request* request, size_t chunk_queue_idx ) {
   
   chunk_ctx->block = block;
   chunk_ctx->manifest = manifest;
   chunk_ctx->request = request;
   chunk_ctx->chunk_queue_idx = chunk_queue_idx;
   return 0;
}

// free a block replication context 
// always succeeds 
int UG_chunk_replication_context_free( struct UG_chunk_replication_context* chunk_ctx ) {
   
   SG_safe_delete( chunk_ctx->request );
   
   memset( chunk_ctx, 0, sizeof(struct UG_chunk_replication_context) );
   
   return 0;
}


// set up a replica context from an inode's dirty blocks and manifest
// flushed_blocks is allowed to be NULL, in which case only the manifest will be replicated.
// NOTE: rctx takes onwership of flushed_blocks--the caller must NOT free them
// NOTE: inode->entry should be read-locked
// NOTE: flushed_blocks must be in-RAM, and must all be dirty
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if at least one of the dirty blocks has not been flushed to disk, or is not in fact dirty
int UG_replica_context_init( struct UG_replica_context* rctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* manifest, struct timespec* old_manifest_timestamp, UG_dirty_block_map_t* flushed_blocks ) {
   
   int rc = 0;
   uint64_t* rg_ids = NULL;
   uint64_t* affected_blocks = NULL;
   size_t num_affected_blocks = 0;
   size_t num_rgs = 0;
   size_t block_idx = 0;
   size_t num_chunks = 1;       // the manifest
   size_t i = 0;
   
   memset( rctx, 0, sizeof( struct UG_replica_context ) );
   
   if( flushed_blocks != NULL ) {
      
      num_affected_blocks = flushed_blocks->size();
      
      affected_blocks = SG_CALLOC( uint64_t, num_affected_blocks );
      if( affected_blocks == NULL ) {
         
         return -ENOMEM;
      }
      
      // sanity check 
      for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {
         
         // in-RAM?
         if( UG_dirty_block_buf( &itr->second )->data == NULL ) {
            
            SG_safe_free( affected_blocks );
            return -EINVAL;
         }
         
         // not dirty?
         if( !UG_dirty_block_dirty( &itr->second ) ) {
         
            SG_safe_free( affected_blocks );
            return -EINVAL;
         }
         
         affected_blocks[i] = itr->first;
         i++;
      }
      
      num_chunks = 1 + flushed_blocks->size();
   }
   
   rctx->blocks = flushed_blocks;
   
   rctx->fs_path = SG_strdup_or_null( fs_path );
   if( rctx->fs_path == NULL ) {
      
      // OOM 
      SG_safe_free( affected_blocks );
      return -ENOMEM;
   }
   
   // snapshot the inode 
   rc = UG_inode_export( &rctx->inode_data, inode, 0, NULL );
   if( rc != 0 ) {
      
      SG_safe_free( affected_blocks );
      SG_safe_free( rctx->fs_path );
      return rc;
   }
   
   // get the manifest
   rc = SG_manifest_dup( &rctx->manifest, manifest );
   if( rc != 0 ) {
      
      SG_safe_free( rctx->fs_path );
      md_entry_free( &rctx->inode_data );
      SG_safe_free( affected_blocks );
      return rc;
   }
   
   // get RGs 
   rc = UG_state_list_replica_gateway_ids( ug, &rg_ids, &num_rgs );
   if( rc != 0 ) {
      
      UG_replica_context_free( rctx );
      SG_safe_free( affected_blocks );
      return rc;
   }
   
   // generate queue of manifests and blocks to send 
   rctx->chunk_queue = SG_CALLOC( struct UG_block_gateway_pair, num_rgs * num_chunks );
   if( rctx->chunk_queue == NULL ) {
      
      UG_replica_context_free( rctx );
      SG_safe_free( rg_ids );
      SG_safe_free( affected_blocks );
      return rc;
   }
   
   rctx->chunk_queue_len = num_rgs * num_chunks;
   
   // generate manifest queue
   for( size_t i = 0; i < num_rgs; i++ ) {
      
      rctx->chunk_queue[ i ].gateway_id = rg_ids[i];
      rctx->chunk_queue[ i ].block_id = SG_INVALID_BLOCK_ID;
   }
   
   if( flushed_blocks != NULL ) {
      
      // generate block queue 
      for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {
            
         for( size_t i = 0; i < num_rgs; i++ ) {
            
            rctx->chunk_queue[ (block_idx + 1) * flushed_blocks->size() + i ].gateway_id = rg_ids[i];
            rctx->chunk_queue[ (block_idx + 1) * flushed_blocks->size() + i ].block_id = itr->first;
         }
         
         block_idx++;
      }
   }
   
   // push the timestamp into the manifest 
   SG_manifest_set_modtime( &rctx->manifest, old_manifest_timestamp->tv_sec, old_manifest_timestamp->tv_nsec );
   
   SG_safe_free( rg_ids );
   
   rctx->affected_blocks = affected_blocks;
   rctx->num_affected_blocks = num_affected_blocks;
   
   return rc;
}


// free up a replica context 
// always succeeds 
int UG_replica_context_free( struct UG_replica_context* rctx ) {
   
   SG_safe_free( rctx->fs_path );
   
   md_entry_free( &rctx->inode_data );
   
   SG_manifest_free( &rctx->manifest );
   
   if( rctx->blocks != NULL ) {
      
      UG_dirty_block_map_free( rctx->blocks );
      SG_safe_delete( rctx->blocks );
   }
   
   SG_safe_free( rctx->affected_blocks );
   
   SG_safe_free( rctx->chunk_queue );
   
   memset( rctx, 0, sizeof(struct UG_replica_context) );
   
   return 0;
}

// append a file's replication log on the MS
// does *NOT* set rctx->sent_vacuum_log
// return 0 on success
// return -ENOMEM on OOM 
// return -errno on connection errors 
static int UG_replicate_manifest_log( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   uint64_t* affected_blocks = NULL;
   size_t num_affected_blocks = 0;
   size_t i = 0;
   struct ms_vacuum_entry ve;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   if( rctx->blocks != NULL ) {
      
      // build up affected blocks, if there are any 
      affected_blocks = SG_CALLOC( uint64_t, rctx->blocks->size() );
      if( affected_blocks == NULL ) {
         
         return -ENOMEM;
      }
      
      num_affected_blocks = rctx->blocks->size();
      
      for( UG_dirty_block_map_t::iterator itr = rctx->blocks->begin(); itr != rctx->blocks->end(); itr++ ) {
         
         affected_blocks[i] = itr->first;
         i++;
      }
   }
      
   // set up the vacuum entry
   rc = ms_client_vacuum_entry_init( &ve, rctx->inode_data.volume, rctx->inode_data.file_id, rctx->inode_data.version,
                                     rctx->inode_data.manifest_mtime_sec, rctx->inode_data.manifest_mtime_nsec, affected_blocks, num_affected_blocks );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_vacuum_entry_init( %" PRIX64 ".%" PRId64 " (%zu blocks) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, num_affected_blocks, rc );
      
      SG_safe_free( affected_blocks );
      return rc;
   }
        
   // send it off
   rc = ms_client_append_vacuum_log_entry( ms, &ve );
   if( rc != 0 ) {
      
      SG_error("ms_client_append_volume_log_entry( %" PRIX64 ".%" PRId64 " (%zu blocks) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, num_affected_blocks, rc );
   }
   
   ms_client_vacuum_entry_free( &ve );
   
   return rc;
}


// set up block replication state
// return 0 on success, and populate *chunk_ctx
// return -ENOMEM on OOM 
// return -EINVAL on invalid request information 
static int UG_replicate_block_setup( struct SG_gateway* gateway, struct UG_replica_context* rctx, uint64_t remote_gateway_id, size_t chunk_queue_idx, struct UG_dirty_block* block, struct UG_chunk_replication_context* chunk_ctx ) {

   int rc = 0;
   struct SG_request_data reqdat;
   SG_messages::Request* request = NULL;                // request buffer
   
   // next request buffer 
   request = SG_safe_new( SG_messages::Request() );
   if( request == NULL ) {
      
      rc = -ENOMEM;
      return rc;
   }
   
   // next block context
   UG_chunk_replication_context_init( chunk_ctx, NULL, block, request, chunk_queue_idx );
   
   // next request 
   rc = SG_request_data_init_block( gateway, rctx->fs_path, rctx->inode_data.file_id, rctx->inode_data.version, UG_dirty_block_id( block ), UG_dirty_block_version( block ), &reqdat );
   if( rc != 0 ) {
      
      // OOM
      SG_safe_free( request );
      return rc;
   }
   
   // remote gateway 
   reqdat.coordinator_id = remote_gateway_id;
   
   // set up the request itself 
   rc = SG_client_request_PUTBLOCK_setup( gateway, request, &reqdat, UG_dirty_block_info( block ) );
   
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_safe_free( request );
      return rc;
   }
   
   // mmap block from disk, if we need to
   if( UG_dirty_block_buf( block )->data == NULL && !UG_dirty_block_mmaped( block ) ) {
      
      rc = UG_dirty_block_mmap( block );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_mmap( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                  rctx->inode_data.file_id, rctx->inode_data.version, UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
         
         SG_safe_free( request );
         
         return rc;
      }
   }
   
   // sanity check...
   if( UG_dirty_block_buf( block )->data == NULL ) {
      
      SG_error("BUG: buffer for %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] is NULL \n",
               rctx->inode_data.file_id, rctx->inode_data.version, UG_dirty_block_id( block ), UG_dirty_block_version( block ) );
   
      rc = -EINVAL;
      
      SG_safe_free( request );
      
      return rc;
   }
   
   // success!
   return rc;
}


// set up manifest replication state
// return 0 on success, and populate *chunk_ctx
// return -ENOMEM on OOM 
// return -EINVAL on invalid request information 
static int UG_replicate_manifest_setup( struct SG_gateway* gateway, struct UG_replica_context* rctx, uint64_t remote_gateway_id, size_t queue_idx, struct SG_manifest* manifest, struct UG_chunk_replication_context* chunk_ctx ) {

   int rc = 0;
   SG_messages::Request* request = NULL;                // request buffer
   
   // next request buffer 
   request = SG_safe_new( SG_messages::Request() );
   if( request == NULL ) {
      
      rc = -ENOMEM;
      return rc;
   }
   
   // next block context
   UG_chunk_replication_context_init( chunk_ctx, manifest, NULL, request, queue_idx );
   
   // set up the request itself 
   rc = SG_client_request_WRITE_setup( gateway, request, rctx->fs_path, manifest, NULL, NULL, NULL );
   
   if( rc != 0 ) {
      
      SG_safe_free( request );
      return rc;
   }
   
   // success!
   return rc;
}


// start replicating a chunk (manifest or block) to the given gateway.
// return 0 on success
// return -EAGAIN if all download slots are full 
// return -EINVAL if both manifest and block are non-NULL
// return -ENOMEM if OOM
// return -errno on failure 
static int UG_replicate_chunk_start( struct SG_gateway* gateway, struct UG_replica_context* rctx, size_t queue_idx, uint64_t remote_gateway_id,
                                     struct SG_manifest* manifest, struct UG_dirty_block* block, struct md_download_loop* dlloop, UG_chunk_replication_set_t* replicating ) {
   
   int rc = 0;
   struct UG_chunk_replication_context chunk_ctx;
   const char* method = NULL;
   struct md_download_context* dlctx = NULL;
   
   if( manifest != NULL && block != NULL ) {
      return -EINVAL;
   }
   
   // set up chunck context
   if( block != NULL ) {
      
      method = "UG_replicate_block_setup";
      rc = UG_replicate_block_setup( gateway, rctx, remote_gateway_id, queue_idx, block, &chunk_ctx );
   }
   else {
      
      method = "UG_replicate_manifest_setup";
      rc = UG_replicate_manifest_setup( gateway, rctx, remote_gateway_id, queue_idx, manifest, &chunk_ctx );
   }
   
   if( rc != 0 ) {
      
      SG_error("%s rc = %d\n", method, rc );
      
      return rc;
   }
   
   // next download slot
   rc = md_download_loop_next( dlloop, &dlctx );
   if( rc != 0 ) {
      
      UG_chunk_replication_context_free( &chunk_ctx );
      
      if( rc == -EAGAIN ) {
         
         return rc;
      }
      
      SG_error("md_download_loop_next rc = %d\n", rc );
      return rc;
   }
   
   // remember this block request 
   try {
      
      (*replicating)[ dlctx ] = chunk_ctx;
   }
   catch( bad_alloc& ba ) {
      
      UG_chunk_replication_context_free( &chunk_ctx );
      
      rc = -ENOMEM;
      return rc;
   }
   
   // start sending it 
   rc = SG_client_request_send_async( gateway, remote_gateway_id, chunk_ctx.request, UG_dirty_block_buf( block ), dlloop, dlctx );
   if( rc != 0 ) {
      
      // clear out and try again later 
      replicating->erase( dlctx );
      UG_chunk_replication_context_free( &chunk_ctx );
      
      SG_error("SG_client_request_send_async( %" PRIX64 " [%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
               remote_gateway_id, UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
      
      return rc;
   }
   
   return rc;
}


// finish up a manifest or a block
// remove it from *replicating on success, and remove from the replicating queue
// return 0 on success
// return -errno on failure 
static int UG_replicate_chunk_finish( struct SG_gateway* gateway, struct UG_replica_context* rctx, struct md_download_loop* dlloop, UG_chunk_replication_set_t* replicating ) {
   
   int rc = 0;
   struct md_download_context* dlctx = NULL;
   SG_messages::Reply reply;                            // reply buffer 
   
   size_t finished_chunk_idx = 0;
   
   UG_chunk_replication_set_t::iterator replicating_itr;
   
   struct UG_dirty_block* dirty_block = NULL;
   struct SG_manifest* manifest = NULL;
   uint64_t remote_gateway_id = 0;

   // finished block context
   struct UG_chunk_replication_context chunk_ctx;
   
   // next finished block
   rc = md_download_loop_finished( dlloop, &dlctx );
   if( rc != 0 ) {
      
      if( rc != -EAGAIN ) {
         SG_error("md_download_loop_finished rc = %d\n", rc );
      }
      
      return rc;
   }
   
   // finish the request 
   rc = SG_client_request_send_finish( gateway, dlctx, &reply );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_send_finish rc = %d\n", rc );
      
      return rc;
   }
   
   // which block was this?
   replicating_itr = replicating->find( dlctx );
   if( replicating_itr == replicating->end() ) {
      
      // weird--not tracking this one 
      SG_error("BUG: untracked download %p\n", dlctx );
      
      return -EINVAL;
   }
   
   // extract chunk context
   chunk_ctx = replicating_itr->second;
   replicating->erase( replicating_itr );
   
   dirty_block = chunk_ctx.block;
   manifest = chunk_ctx.manifest;
   remote_gateway_id = chunk_ctx.request->coordinator_id();
   
   finished_chunk_idx = chunk_ctx.chunk_queue_idx;
   
   // done with this request
   UG_chunk_replication_context_free( &chunk_ctx );
   
   // status?
   if( reply.error_code() != 0 ) {
      
      if( dirty_block != NULL ) {
         SG_error("Replicate %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] to %" PRIu64 " reply error %d\n",
                  rctx->inode_data.file_id, rctx->inode_data.version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), remote_gateway_id, reply.error_code() );
      }
      else {
         SG_error("Replicate %" PRIX64 ".%" PRId64 "/manifest.%" PRId64 ".%d to %" PRIu64 " reply error %d\n",
                  rctx->inode_data.file_id, rctx->inode_data.version, SG_manifest_get_modtime_sec( manifest ), SG_manifest_get_modtime_nsec( manifest ), remote_gateway_id, reply.error_code() );
      }
      
      return reply.error_code();
   }
   
   // finished this (block, gateway) pair!
   rctx->chunk_queue[ finished_chunk_idx ].gateway_id = 0;
   rctx->chunk_queue[ finished_chunk_idx ].block_id = 0;
   
   return rc;
}


// finish at least one outstanding request.  Remove them from *replicating
// return 0 on success
// return -errno on failure 
static int UG_replicate_chunks_finish( struct SG_gateway* gateway, struct UG_replica_context* rctx, struct md_download_loop* dlloop, UG_chunk_replication_set_t* replicating ) {
   
   int rc = 0;

   // run replications
   rc = md_download_loop_run( dlloop );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_run rc = %d\n", rc );
      return rc;
   }
   
   // finish replications 
   while( true ) {
      
      rc = UG_replicate_chunk_finish( gateway, rctx, dlloop, replicating );
      if( rc != 0 ) {
         
         if( rc == -EAGAIN ) {
            
            // nothing to reap 
            rc = 0;
            break;
         }
         
         SG_error("UG_replicate_chunk_finish rc = %d\n", rc );
         break;
      }
   }
   
   return rc;
}


// replicate the manifest and set of dirty blocks to the set of RGs.
// return 0 if all blocks were successfully replicated 
// return -EINVAL if we're not ready to replicate yet
// return -errno on failure to send
static int UG_replicate_chunks( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   
   struct md_download_loop dlloop;
   size_t chunk_queue_idx = 0;
   bool started = false;                        // set to true if we started at least one chunk
   
   struct UG_dirty_block* dirty_block = NULL;
   struct SG_manifest* manifest = NULL;
   
   UG_dirty_block_map_t::iterator dirty_block_itr;
   
   UG_chunk_replication_set_t replicating;              // currently-replicating blocks
   UG_chunk_replication_set_t::iterator replicating_itr;
   
   SG_messages::Reply reply;                            // reply buffer 
   
   if( rctx->replicated_blocks ) {
      
      // done 
      return 0;
   }
      
   if( !rctx->flushed_blocks ) {
      
      // skipped a step 
      return -EINVAL;
   }
   
   // set up uploads (no more than 6 at once per gateway, but this is arbitrary)
   rc = md_download_loop_init( &dlloop, SG_gateway_dl( gateway ), 6 * (rctx->chunk_queue_len / (1 + (rctx->blocks != NULL ? rctx->blocks->size() : 0))) );
   if( rc != 0 ) {
      
      // OOM 
      return rc;
   }
   
   // upload each block to each gatewy
   do {
      
      // start as many as we can 
      while( chunk_queue_idx < rctx->chunk_queue_len ) {
         
         uint64_t gateway_id = rctx->chunk_queue[chunk_queue_idx].gateway_id;
         uint64_t block_id = rctx->chunk_queue[chunk_queue_idx].block_id;
         
         if( gateway_id == 0 ) {
            
            // finished this pair 
            chunk_queue_idx++;
            continue;
         }
         
         if( block_id != SG_INVALID_BLOCK_ID && rctx->blocks != NULL ) {
            
            // send a block
            dirty_block_itr = rctx->blocks->find( block_id );
            if( dirty_block_itr == rctx->blocks->end() ) {
               
               // shouldn't happen...
               chunk_queue_idx++;
               continue;
            }
            
            manifest = NULL;
            dirty_block = &dirty_block_itr->second;
         }
         else {
            
            // send the manifest 
            manifest = &rctx->manifest;
            dirty_block = NULL;
         }
         
         rc = UG_replicate_chunk_start( gateway, rctx, chunk_queue_idx, gateway_id, manifest, dirty_block, &dlloop, &replicating );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // all downloads are busy 
               rc = 0;
               break;
            }
            
            if( manifest == NULL ) {
               SG_error("UG_replicate_chunk_start( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) to %" PRIX64 " rc = %d\n", 
                        rctx->inode_data.file_id, rctx->inode_data.version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rctx->chunk_queue[ chunk_queue_idx ].gateway_id, rc );
            }
            else {
               SG_error("UG_replicate_chunk_start( %" PRIX64 ".%" PRId64 "/manifest.%" PRId64 ".%d ) to %" PRIX64 " rc = %d\n", 
                        rctx->inode_data.file_id, rctx->inode_data.version, SG_manifest_get_modtime_sec( manifest ), SG_manifest_get_modtime_nsec( manifest ), rctx->chunk_queue[ chunk_queue_idx ].gateway_id, rc );
            }
            
            break;
         }
         
         // next (block, gateway) pair
         chunk_queue_idx++;
         
         // started at least one 
         started = true;
      }
      
      if( !started ) {
         
         // nothing to do 
         break;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // finish at least one replication 
      rc = UG_replicate_chunks_finish( gateway, rctx, &dlloop, &replicating );
      if( rc != 0 ) {
         
         SG_error("UG_replicate_chunks_finish rc = %d\n", rc );
         break;
      }
      
   } while( md_download_loop_running( &dlloop ) );
   
   if( rc != 0 ) {
      
      // finish the remaining running replications
      while( replicating.size() > 0 ) {
         
         int quiesce_rc = 0;
         
         quiesce_rc = UG_replicate_chunks_finish( gateway, rctx, &dlloop, &replicating );
         if( quiesce_rc != 0 ) {
            
            SG_error("UG_replicate_chunks_finish rc = %d\n", quiesce_rc );
            
            // abort at this point 
            md_download_loop_abort( &dlloop );
            
            break;
         }
      }
      
      // clear up outstanding requests 
      for( replicating_itr = replicating.begin(); replicating_itr != replicating.end(); replicating_itr++ ) {
         
         UG_chunk_replication_context_free( &replicating_itr->second );
      }
      
      replicating.clear();
   }
   
   // clean up
   md_download_loop_cleanup( &dlloop, NULL, NULL );
   md_download_loop_free( &dlloop );
   
   return rc;
}


// replicate the blocks and manifest to a given gateway.
// (0) make sure all blocks are flushed to disk cache
// (1) if we're the coordinator, append to this file's vacuum log on the MS 
// (2) replicate the blocks and manifest to each replica gateway
// (3) if we're the coordinator, send the new inode information to the MS
// free up blocks and manifest information as they succeed, so the caller can try a different gateway on a subsequent call resulting from a partial replication failure.
// return 0 on success
// return -EIO if this method failed to flush data to disk
// return -EAGAIN if this method should be called again, with the same arguments
// return -ENOMEM on OOM
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EREMOTEIO if the HTTP error is >= 500
// return between -499 and -400 if the HTTP error was in the range 400 to 499
// return other -errno on socket- and recv-related errors
int UG_replicate( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t gateway_id = SG_gateway_id( gateway );
      
   // (0) flush remaining dirty blocks to disk, if need be
   if( !rctx->flushed_blocks && rctx->blocks != NULL ) {
      
      rc = UG_sync_blocks_flush_async( gateway, rctx->fs_path, rctx->inode_data.file_id, rctx->inode_data.version, rctx->blocks );
      if( rc != 0 ) {
         
         SG_error("UG_sync_blocks_flush_async( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->fs_path, rc );
         return -EIO;
      }
      
      rc = UG_sync_blocks_flush_finish( rctx->inode_data.file_id, rctx->inode_data.version, rctx->blocks );
      if( rc != 0 ) {
         
         SG_error("UG_sync_blocks_flush_finish( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->fs_path, rc );
         return -EIO;
      }
      
      // all blocks are on disk
      rctx->flushed_blocks = true;
   }
   
   // (1) make sure the MS knows about this replication request, if we're the coordinator
   if( !rctx->sent_vacuum_log && rctx->inode_data.coordinator == gateway_id ) {
      
      rc = UG_replicate_manifest_log( gateway, rctx );
      if( rc != 0 ) {
         
         SG_error("UG_replicate_manifest_log( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->fs_path, rc );
         return -EAGAIN;
      }
      else {
         
         // success!
         rctx->sent_vacuum_log = true;
      }
   }
   
   // (2) replicate the manifest and each block to each gateway
   if( !rctx->replicated_blocks ) {
      
      // send off to all RGs
      rc = UG_replicate_chunks( gateway, rctx );
      if( rc != 0 ) {
         
         SG_error("UG_replicate_chunks() rc = %d\n", rc );
         
         return rc;
      }
      else {
         
         rctx->replicated_blocks = true;
      }
   }
   
   // (3) update the record on the MS
   if( !rctx->sent_ms_update && rctx->inode_data.coordinator == gateway_id ) {
      
      struct ms_client_request request;
      struct ms_client_request_result result;
      
      rc = ms_client_update_request( ms, &rctx->inode_data, &request );
      if( rc != 0 ) {
         
         SG_error("ms_client_update_request( %" PRIX64 ".%" PRId64 " ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rc );
         return rc;
      }
      
      rc = ms_client_single_rpc( ms, &request, &result );
      if( rc != 0 ) {
         
         SG_error("ms_client_single_rpc( %" PRIX64 ".%" PRId64 " ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rc );
      }
      
      if( result.reply_error != 0 ) {
         
         rc = result.reply_error;
         
         SG_error("ms_client_single_rpc( %" PRIX64 ".%" PRId64 " ) result reply_error = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, result.reply_error );
      }
      
      else if( result.rc != 0 ) {
         
         rc = result.rc;
         
         SG_error("ms_client_single_rpc( %" PRIX64 ".%" PRId64 " ) result rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, result.rc );
      }
      
      ms_client_request_result_free( &result );
   }
   
   // done!
   return rc;
}


// run a replication context
// try it again always on EAGAIN 
// return the result of UG_replicate
int UG_replicate_run( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   
   // replicate it
   while( true ) {
      
      rc = UG_replicate( gateway, rctx );
      if( rc == -EAGAIN ) {
         
         // try again 
         continue;
      }
      else {
         
         if( rc != 0 ) {
            
            SG_error("UG_replicate('%s') rc = %d\n", rctx->fs_path, rc );
         }
         
         break;
      }
   }
   
   return rc;
}


// release from ownership the map of dirty blocks
// the caller will be responsible for managing them; the replica context will no longer have access to them 
// return a pointer to the blocks (or NULL if already released)
UG_dirty_block_map_t* UG_replica_context_release_blocks( struct UG_replica_context* rctx ) {
   
   UG_dirty_block_map_t* ret = rctx->blocks;
   rctx->blocks = NULL;
   return ret;
}
