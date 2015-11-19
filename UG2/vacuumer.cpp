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


#include "vacuumer.h"
#include "consistency.h"
#include "core.h"

// block deletion context
struct UG_chunk_vacuum_context {
   
   struct SG_manifest_block* block_info;
   struct SG_manifest* manifest;
   size_t vacuum_queue_idx;
   SG_messages::Request* request;
};

typedef map< struct md_download_context*, struct UG_chunk_vacuum_context > UG_vacuuming_map_t;


// set up a block deletion context 
// always succeeds
// NOTE: block_ctx takes ownership of request, and a call to UG_chunk_vacuum_context_free will free it
int UG_chunk_vacuum_context_init( struct UG_chunk_vacuum_context* block_ctx, struct SG_manifest* manifest, struct SG_manifest_block* block_info, size_t vacuum_queue_idx, SG_messages::Request* request ) {
   
   block_ctx->manifest = manifest;
   block_ctx->block_info = block_info;
   block_ctx->request = request;
   block_ctx->vacuum_queue_idx = vacuum_queue_idx;
   
   return 0;
}

// free a block deletion context 
// always succeeds 
int UG_chunk_vacuum_context_free( struct UG_chunk_vacuum_context* block_ctx ) {
   
   SG_safe_delete( block_ctx->request );
   
   memset( block_ctx, 0, sizeof(struct UG_chunk_vacuum_context) );
   
   return 0;
}


// set up a vacuum context
// prepare to vacuum only the blocks listed in replaced_blocks 
// if replaced_blocks is NULL, then vacuum everything
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if this is a directory
// NOTE: inode->entry must be at least read-locked
int UG_vacuum_context_init( struct UG_vacuum_context* vctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* replaced_blocks ) {
   
   int rc = 0;
   size_t block_idx = 0;
   size_t num_blocks = 0;
   
   uint64_t* rg_ids = NULL;
   size_t num_rgs = 0;
   
   // sanity check 
   if( fskit_entry_get_type( UG_inode_fskit_entry( inode ) ) != FSKIT_ENTRY_TYPE_FILE ) {
     
      SG_error("Entry '%s' is not a file (type %d)\n", fs_path, fskit_entry_get_type( UG_inode_fskit_entry( inode ) ) ); 
      return -EINVAL;
   }
   
   char* path = SG_strdup_or_null( fs_path );
   if( path == NULL ) {
      
      return -ENOMEM;
   }
   
   // get RGs 
   rc = UG_state_list_replica_gateway_ids( ug, &rg_ids, &num_rgs );
   if( rc != 0 ) {
     
      SG_error("UG_state_list_replica_gateway_ids rc = %d\n", rc ); 
      SG_safe_free( path );
      return rc;
   }
   
   // snapshot inode data 
   rc = UG_inode_export( &vctx->inode_data, inode, 0 );
   if( rc != 0 ) {
      
      SG_error("UG_inode_export('%s') rc = %d\n", fs_path, rc );
      SG_safe_free( path );
      SG_safe_free( rg_ids );
      return rc;
   }
   
   if( replaced_blocks != NULL ) {
      
      // only vacuum these blocks 
      rc = SG_manifest_dup( &vctx->old_blocks, replaced_blocks );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_dup rc = %d\n", rc ); 
         md_entry_free( &vctx->inode_data );
         SG_safe_free( path );
         SG_safe_free( rg_ids );
         return rc;
      }
   }
   else {
      
      // vacuum all of this inode's blocks
      rc = SG_manifest_dup( &vctx->old_blocks, UG_inode_replaced_blocks( inode ) );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_dup rc = %d\n", rc ); 
         md_entry_free( &vctx->inode_data );
         SG_safe_free( path );
         SG_safe_free( rg_ids );
         return rc;
      }
   }
   
   // set up vacuum queue 
   num_blocks = SG_manifest_get_block_count( &vctx->old_blocks );
   vctx->vacuum_queue_len = num_rgs * num_blocks;
   vctx->vacuum_queue = SG_CALLOC( struct UG_block_gateway_pair, vctx->vacuum_queue_len );
   
   // generate block queue 
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( &vctx->old_blocks ); itr != SG_manifest_block_iterator_end( &vctx->old_blocks ); itr++ ) {
      
      for( size_t i = 0; i < num_rgs; i++ ) {
         
         vctx->vacuum_queue[ block_idx * num_blocks + i ].gateway_id = rg_ids[i];
         vctx->vacuum_queue[ block_idx * num_blocks + i ].block_id = itr->first;
      }
      
      block_idx++;
   }
   
   // get the old manifest timestamp
   vctx->old_manifest_timestamp = UG_inode_old_manifest_modtime( inode );
   
   vctx->have_old_manifest_timestamp = true;
   vctx->have_old_blocks = true;
   
   vctx->delay = 1;
   
   vctx->fs_path = path;
   
   SG_safe_free( rg_ids );
   
   return 0;
}


// free up a vacuum context 
int UG_vacuum_context_free( struct UG_vacuum_context* vctx ) {
   
   SG_manifest_free( &vctx->old_blocks );
   md_entry_free( &vctx->inode_data );
   
   SG_safe_free( vctx->fs_path );
   
   memset( vctx, 0, sizeof(struct UG_vacuum_context) );
   
   return 0;
}


// restore a vacuum context's data to an inode 
// return 0 on success
// return -ENOMEM on OOM 
int UG_vacuum_context_restore( struct UG_vacuum_context* vctx, struct UG_inode* inode ) {
   
   int rc = 0;
   
   // put back replaced blocks 
   rc = SG_manifest_patch_nocopy( UG_inode_replaced_blocks( inode ), &vctx->old_blocks, false );
   SG_manifest_clear_nofree( &vctx->old_blocks );
   
   if( rc != 0 ) {
      SG_error("SG_manifest_patch_nocopy rc = %d\n", rc );
      
      return rc;
   }
   
   // put back old manifest timestamp 
   UG_inode_set_old_manifest_modtime( inode, &vctx->old_manifest_timestamp );
   
   return 0;
}


// start vacuuming data.  It will be retried indefinitely.
// return 0 on successful enqueue 
// return -ENOMEM on OOM
// NOTE: the vacuumer takes ownership of vctx.  do not free or access it after this call.
int UG_vacuumer_enqueue( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   
   pthread_rwlock_wrlock( &vacuumer->lock );
   
   try {
      vacuumer->vacuum_queue->push( vctx );
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   if( rc == 0 ) {
      
      // wake up the work thread 
      sem_post( &vacuumer->sem );
   }
   
   pthread_rwlock_unlock( &vacuumer->lock );
   
   return rc;
}


// prepare to delete a block
// run the request through the driver as well.
// return 0 on success, and populate *deletioN_context
// return -ENOMEM on OOM 
// return -EINVAL on invalid request information 
static int UG_vacuum_block_setup( struct SG_gateway* gateway, struct UG_vacuum_context* vctx, uint64_t remote_gateway_id,
                                  struct SG_manifest_block* block_info, size_t vacuum_queue_idx, struct UG_chunk_vacuum_context* deletion_context ) {

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
   UG_chunk_vacuum_context_init( deletion_context, NULL, block_info, vacuum_queue_idx, request );
   
   // next request 
   rc = SG_request_data_init_block( gateway, vctx->fs_path, vctx->inode_data.file_id, vctx->inode_data.version, SG_manifest_block_id( block_info ), SG_manifest_block_version( block_info ), &reqdat );
   if( rc != 0 ) {
      
      // OOM
      SG_safe_free( request );
      return rc;
   }
   
   // forward to the driver 
   rc = SG_gateway_driver_delete_block( gateway, &reqdat );
   if( rc != 0 && rc != -ENOSYS ) {
      
      // not allowed 
      SG_safe_free( request );
      return rc;
   }
   
   rc = 0;
   
   // remote gateway 
   reqdat.coordinator_id = remote_gateway_id;
   
   // set up the request itself 
   rc = SG_client_request_DELETEBLOCK_setup( gateway, request, &reqdat, block_info );
   
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_safe_free( request );
      return rc;
   }
   
   // success!
   return rc;
}


// prepare to delete a manifest.
// run the request through the driver.
// return 0 on success, and populate *deletion_context
// return -ENOMEM on OOM 
// return -EINVAL on invalid request information 
static int UG_vacuum_manifest_setup( struct SG_gateway* gateway, struct UG_vacuum_context* vctx, uint64_t remote_gateway_id,
                                     struct SG_manifest* manifest, size_t vacuum_queue_idx, struct UG_chunk_vacuum_context* deletion_context ) {

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
   UG_chunk_vacuum_context_init( deletion_context, manifest, NULL, vacuum_queue_idx, request );
   
   // next request 
   rc = SG_request_data_init_manifest( gateway, vctx->fs_path, vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, &reqdat );
   if( rc != 0 ) {
      
      // OOM
      SG_safe_free( request );
      return rc;
   }
   
   // remote gateway 
   reqdat.coordinator_id = remote_gateway_id;
   
   rc = SG_gateway_driver_delete_manifest( gateway, &reqdat );
   if( rc != 0 && rc != -ENOSYS ) {
      
      SG_error("SG_gateway_driver_delete_manifest( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat.file_id, reqdat.file_version, reqdat.fs_path, rc );
      SG_safe_free( request );
   }
   
   rc = 0;
   
   // set up the request itself 
   rc = SG_client_request_DETACH_setup( gateway, request, &reqdat );
   
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_safe_free( request );
      return rc;
   }
   
   // success!
   return rc;
}


// start deleting a chunk (a block or manifest 
// return 0 on success, such that *vacuuming tracks a new, now-processing UG_chunk_vacuum_context instance
// return -EAGAIN if all download slots are full 
// return -ENOMEM on OOM
// return -EINVAL if both manifest and block_info are non-NULL, or are both NULL
// return -errno on failure 
static int UG_vacuumer_chunk_delete_start( struct SG_gateway* gateway, struct UG_vacuum_context* vctx, size_t vacuum_queue_idx, uint64_t remote_gateway_id, 
                                           struct SG_manifest* manifest, struct SG_manifest_block* block_info, struct md_download_loop* dlloop, UG_vacuuming_map_t* vacuuming ) {
   
   int rc = 0;
   struct UG_chunk_vacuum_context deletion_ctx;
   struct md_download_context* dlctx = NULL;
   char const* method = NULL;
   
   if( manifest != NULL && block_info != NULL ) {
      
      return -EINVAL;
   }
   
   if( manifest == NULL && block_info == NULL ) {
      
      return -EINVAL;
   }

   // next deletion 
   if( block_info != NULL ) {
      
      method = "UG_vacuum_block_setup";
      rc = UG_vacuum_block_setup( gateway, vctx, remote_gateway_id, block_info, vacuum_queue_idx, &deletion_ctx );
   }
   else {
      
      method = "UG_vacuum_manifest_setup";
      rc = UG_vacuum_manifest_setup( gateway, vctx, remote_gateway_id, manifest, vacuum_queue_idx, &deletion_ctx );
   }
   if( rc != 0 ) {
      
      SG_error("%s( [%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                method, SG_manifest_block_id( block_info ), SG_manifest_block_version( block_info ), rc );
      
      return rc;
   }
   
   // next download slot
   rc = md_download_loop_next( dlloop, &dlctx );
   if( rc != 0 ) {
      
      UG_chunk_vacuum_context_free( &deletion_ctx );
      
      if( rc == -EAGAIN ) {
         
         return rc;
      }
      
      SG_error("md_download_loop_next rc = %d\n", rc );
      return rc;
   }
   
   // remember this block deletion request 
   try {
      
      (*vacuuming)[ dlctx ] = deletion_ctx;
   }
   catch( bad_alloc& ba ) {
      
      UG_chunk_vacuum_context_free( &deletion_ctx );
      
      rc = -ENOMEM;
      return rc;
   }
   
   // start sending it 
   rc = SG_client_request_send_async( gateway, remote_gateway_id, deletion_ctx.request, NULL, dlloop, dlctx );
   if( rc != 0 ) {
      
      // try again later 
      vacuuming->erase( dlctx );
      UG_chunk_vacuum_context_free( &deletion_ctx );
      
      SG_error("SG_client_request_send_async( %" PRIX64 " [%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
               remote_gateway_id, SG_manifest_block_id( block_info ), SG_manifest_block_version( block_info ), rc );
      
      return rc;
   }
   
   return rc;
}


// find and clean up a finished chunk vacuuming 
// on success, remove the chunk from *vacuuming
// return 0 on success
// return -errno on failure 
static int UG_vacuumer_chunk_delete_finish( struct SG_gateway* gateway, struct UG_vacuum_context* vctx, struct md_download_loop* dlloop, UG_vacuuming_map_t* vacuuming ) {
   
   int rc = 0;
   struct md_download_context* dlctx = NULL;
   SG_messages::Reply reply;                            // reply buffer 
   
   UG_vacuuming_map_t::iterator vacuuming_itr;
   
   struct SG_manifest* manifest = NULL;
   struct SG_manifest_block* block_info = NULL;
   
   size_t vacuum_queue_idx = 0;
   
   // finished chunk context
   struct UG_chunk_vacuum_context deletion_ctx;
   
   // next finished chunk
   rc = md_download_loop_finished( dlloop, &dlctx );
   if( rc != 0 ) {
      
      if( rc != -EAGAIN ) {
         SG_error("md_download_loop_finished rc = %d\n", rc );
         rc = 0;
      }
      
      return rc;
   }
   
   // finish the request 
   rc = SG_client_request_send_finish( gateway, dlctx, &reply );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_send_finish rc = %d\n", rc );
      
      return rc;
   }
   
   // which chunk was this?
   vacuuming_itr = vacuuming->find( dlctx );
   if( vacuuming_itr == vacuuming->end() ) {
      
      // weird--not tracking this one 
      SG_error("BUG: untracked download %p\n", dlctx );
      
      return -EINVAL;
   }
   
   // extract chunk context
   deletion_ctx = vacuuming_itr->second;
   vacuuming->erase( vacuuming_itr );
   
   manifest = deletion_ctx.manifest;
   block_info = deletion_ctx.block_info;
   vacuum_queue_idx = deletion_ctx.vacuum_queue_idx;
   
   // done with this request
   UG_chunk_vacuum_context_free( &deletion_ctx );
   
   // status?
   if( reply.error_code() != 0 ) {
      
      if( block_info != NULL ) {
         SG_error("Vacuum %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] reply error %d\n",
                  vctx->inode_data.file_id, vctx->inode_data.version, SG_manifest_block_id( deletion_ctx.block_info ), SG_manifest_block_version( deletion_ctx.block_info ), reply.error_code() );
      }
      else {
         SG_error("Vacuum %" PRIX64 ".%" PRId64 "/manifest.%" PRId64 ".%d reply error %d\n",
                  vctx->inode_data.file_id, vctx->inode_data.version, SG_manifest_get_modtime_sec( manifest ), SG_manifest_get_modtime_nsec( manifest ), reply.error_code() );
      }
      
      return reply.error_code();
   }
   
   // clear out the chunk 
   vctx->vacuum_queue[ vacuum_queue_idx ].gateway_id = 0;
   vctx->vacuum_queue[ vacuum_queue_idx ].block_id = 0;
   
   return rc;
}


// quiesce vacuuming--finish at least one request, but start no new ones 
// return 0 on success 
// return -errno on failure
static int UG_vacuumer_chunks_delete_quiesce( struct SG_gateway* gateway, struct UG_vacuum_context* vctx, struct md_download_loop* dlloop, UG_vacuuming_map_t* vacuuming ) {
   
   int rc = 0;

   // run deletions   
   rc = md_download_loop_run( dlloop );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_run rc = %d\n", rc );
      return rc;
   }

   // finish deletions 
   while( true ) {
      
      rc = UG_vacuumer_chunk_delete_finish( gateway, vctx, dlloop, vacuuming );
      if( rc != 0 ) {
         
         if( rc == -EAGAIN ) {
            
            // nothing to reap 
            rc = 0;
            break;
         }
         
         SG_error("UG_vacuumer_chunk_delete_finish rc = %d\n", rc );
         break;
      }
   }
   
   return rc;
}


// delete blocks and manifests from a set of gateways
// return 0 on success 
// return -ENOMEM on OOM
// return -EINVAL if vctx does not have the requisite state
// return -errno on network failure 
static int UG_vacuumer_chunks_delete( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   
   struct md_download_loop dlloop;
   struct SG_gateway* gateway = vacuumer->gateway;
   
   UG_vacuuming_map_t vacuuming;
   size_t vacuum_queue_idx = 0;
   bool started = false;                                // set to true if we start at least one vacuuming
   
   struct SG_manifest_block* block_info = NULL;
   struct SG_manifest* manifest = NULL;
   
   SG_manifest_block_iterator block_info_itr;
   UG_vacuuming_map_t::iterator vacuuming_itr;
   
   // do we have the prerequisite state?
   if( !vctx->have_old_manifest_timestamp || !vctx->have_old_blocks ) {
      return -EINVAL;
   }
   
   if( vctx->vacuumed_old_blocks ) {
      // done 
      return 0;
   }
   
   // set up downloader
   // no more than 6 connections per gateway (arbitrary)
   rc = md_download_loop_init( &dlloop, SG_gateway_dl( gateway ), 6 * (vctx->vacuum_queue_len / (1 + SG_manifest_get_block_count( &vctx->old_blocks )) ) );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_init rc = %d\n", rc );
      return rc;
   }
   
   
   // run each chunk 
   do {
      
      while( vacuum_queue_idx < vctx->vacuum_queue_len ) {
         
         uint64_t gateway_id = vctx->vacuum_queue[ vacuum_queue_idx ].gateway_id;
         uint64_t block_id = vctx->vacuum_queue[ vacuum_queue_idx ].block_id;
         
         if( gateway_id == 0 ) {
            
            // already processed 
            vacuum_queue_idx++;
            continue;
         }
         
         if( block_id != SG_INVALID_BLOCK_ID ) {
            
            // vacuum block 
            block_info = SG_manifest_block_lookup( &vctx->old_blocks, block_id );
            if( block_info == NULL ) {
               
               // shouldn't happen...
               vacuum_queue_idx++;
               continue;
            }
            
            manifest = NULL;
         }
         else {
            
            manifest = &vctx->old_blocks;
            block_info = NULL;
         }
         
         // start next chunk
         rc = UG_vacuumer_chunk_delete_start( gateway, vctx, vacuum_queue_idx, gateway_id, manifest, block_info, &dlloop, &vacuuming );
         if( rc != 0 ) {
            
            if( block_id != SG_INVALID_BLOCK_ID ) {
               SG_error("UG_vacuumer_chunk_delete_start( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) to %" PRIX64 " rc = %d\n", 
                        vctx->inode_data.file_id, vctx->inode_data.version, SG_manifest_block_id( block_info ), SG_manifest_block_version( block_info ), gateway_id, rc );
            }
            else {
               SG_error("UG_vacuumer_chunk_delete_start( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) to %" PRIX64 " rc = %d\n", 
                        vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, gateway_id, rc );
            }
            
            break;
         }
         
         // next chunk 
         vacuum_queue_idx++;
         started = true;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      if( !started ) {
         break;
      }
      
      rc = UG_vacuumer_chunks_delete_quiesce( gateway, vctx, &dlloop, &vacuuming );
      if( rc != 0 ) {
         
         SG_error("UG_vacuumer_chunks_delete_quiesce rc = %d\n", rc );
         break;
      }
      
   } while( md_download_loop_running( &dlloop ) );
   
   if( rc != 0 ) {
      
      // finish the remaining running deletions
      while( vacuuming.size() > 0 ) {
         
         int quiesce_rc = 0;
         
         quiesce_rc = UG_vacuumer_chunks_delete_quiesce( gateway, vctx, &dlloop, &vacuuming );
         if( quiesce_rc != 0 ) {
            
            SG_error("UG_vacuumer_chunks_delete_quiesce rc = %d\n", quiesce_rc );
            
            // abort at this point 
            md_download_loop_abort( &dlloop );
            
            break;
         }
      }
      
      // clear up outstanding requests 
      for( vacuuming_itr = vacuuming.begin(); vacuuming_itr != vacuuming.end(); vacuuming_itr++ ) {
         
         UG_chunk_vacuum_context_free( &vacuuming_itr->second );
      }
      
      vacuuming.clear();
   }
   
   // clean up
   md_download_loop_cleanup( &dlloop, NULL, NULL );
   md_download_loop_free( &dlloop );
   
   vctx->vacuumed_old_blocks = true;
   
   return rc;
}


// get the next manifest timestamp and blocks to vacuum
// on success, put it into the vacuum context
// return 0 on success, or if we already have the timestamp
// return -ENOENT if there is no manifest timestamp to be had
// return -errno on error 
static int UG_vacuumer_peek_vacuum_log( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   struct ms_vacuum_entry ve;
   
   struct SG_gateway* gateway = vacuumer->gateway;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t file_id = vctx->inode_data.file_id;
   
   // have it already?
   if( vctx->have_old_manifest_timestamp ) {
      return 0;
   }
   
   // nope; go get it 
   memset( &ve, 0, sizeof(struct ms_vacuum_entry) );
   
   // get the head of the vacuum log, and keep the ticket so we can pass it along to the RG
   rc = ms_client_peek_vacuum_log( ms, volume_id, file_id, &ve );
   if( rc != 0 ) {
      
      SG_error("ms_client_peek_vacuum_log(%" PRIX64 ") rc = %d\n", file_id, rc );
      return rc;
   }
   
   vctx->old_manifest_timestamp.tv_sec = ve.manifest_mtime_sec;
   vctx->old_manifest_timestamp.tv_nsec = ve.manifest_mtime_nsec;
   
   // set up the manifest, and store the block IDs 
   rc = SG_manifest_init( &vctx->old_blocks, ve.volume_id, SG_gateway_id( gateway ), ve.file_id, ve.file_version );
   if( rc != 0 ) {
      
      // OOM 
      ms_client_vacuum_entry_free( &ve );
      return rc;
   }
   
   // remember the affected block IDs
   for( size_t i = 0; i < ve.num_affected_blocks; i++ ) {
      
      struct SG_manifest_block block_info;
      
      rc = SG_manifest_block_init( &block_info, ve.affected_blocks[i], 0, NULL, 0 );
      if( rc != 0 ) {
         
         SG_manifest_free( &vctx->old_blocks );
         ms_client_vacuum_entry_free( &ve );
         return rc;
      }
      
      rc = SG_manifest_put_block( &vctx->old_blocks, &block_info, true );
      if( rc != 0 ) {
         
         SG_manifest_block_free( &block_info );
         SG_manifest_free( &vctx->old_blocks );
         ms_client_vacuum_entry_free( &ve );
         return rc;
      }
   }
   
   ms_client_vacuum_entry_free( &ve );
   
   vctx->have_old_manifest_timestamp = true;
   
   return 0;
}


// get the old manifest block versions and hashes 
// return 0 on success, and populate the vctx's old_blocks manifest with versioning and (if present) hash data (old_blocks should already have been initialized and populated with blok IDs).
// return 0 if we've already populated old_blocks 
// return -EINVAL if we're not read to do this step (indicates a bug)
// return -ENODATA if we're missing some manifest data
// return -errno on failure
static int UG_vacuumer_get_manifest_data( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   
   struct SG_gateway* gateway = vacuumer->gateway;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   struct SG_request_data reqdat;
   struct SG_manifest old_manifest;
   
   uint64_t* rg_ids = NULL;
   size_t num_rg_ids = 0;
   
   int worst_rc = 0;
   
   // have it already?
   if( vctx->have_old_blocks ) {
      return 0;
   }
   
   // skipped a step?
   if( !vctx->have_old_manifest_timestamp ) {
      return -EINVAL;
   }
   
   // find all RGs
   rc = UG_state_list_replica_gateway_ids( ug, &rg_ids, &num_rg_ids );
   if( rc != 0 ) {
      
      // OOM 
      return rc;
   }
   
   // build a request 
   rc = SG_request_data_init_manifest( gateway, vctx->fs_path, vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, &reqdat );
   if( rc != 0 ) {
      
      // OOM 
      SG_safe_free( rg_ids );
      return rc;
   }
   
   // try to get the manifest from each one 
   rc = UG_consistency_manifest_download( gateway, &reqdat, rg_ids, num_rg_ids, &old_manifest );
   
   SG_safe_free( rg_ids );
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_error("UG_manifest_download( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, rc );
      
      return rc;
   }
   
   // fill in the parts of the manifest that we need (i.e. version, hash)
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( &vctx->old_blocks ); itr != SG_manifest_block_iterator_end( &vctx->old_blocks ); itr++ ) {
      
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( &old_manifest, SG_manifest_block_iterator_id( itr ) );
      
      if( block_info == NULL ) {
         
         // that's odd...the old manifest doesn't have a record of the block we're supposed to delete (even though the MS says so).
         // Big.  Fat.  Warning.
         SG_error("CRITICAL: Manifest %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld is missing [%" PRIu64 ".%" PRId64 "]\n",
                   vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec,
                   SG_manifest_block_iterator_id( itr ), SG_manifest_block_version( SG_manifest_block_iterator_block( itr ) ) );
         
         worst_rc = -ENODATA;
      }
      else {
         
         // fill in 
         SG_manifest_block_free( SG_manifest_block_iterator_block( itr ) );
         
         rc = SG_manifest_block_dup( SG_manifest_block_iterator_block( itr ), block_info );
         if( rc != 0 ) {
            
            // OOM 
            worst_rc = rc;
            break;
         }
      }
   }
   
   SG_manifest_free( &old_manifest );
   
   if( worst_rc == 0 ) {
      
      // success!
      vctx->have_old_blocks = true;
   }
      
   return worst_rc;
}


// clear the vacuum log for this write 
// return 0 on success
// return -EINVAL if we're not done vacuuming
// return -errno on failure to contact the MS
static int UG_vacuumer_clear_vacuum_log( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   
   // prerequisites met?
   if( !vctx->vacuumed_old_blocks ) {
      return -EINVAL;
   }
   
   struct SG_gateway* gateway = vacuumer->gateway;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   rc = ms_client_remove_vacuum_log_entry( ms, volume_id, vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec );
   
   return rc;
}


// increase delay factor by exponentially backing off with random jitter
// always succeeds
int UG_vacuumer_set_delay( struct UG_vacuum_context* vctx ) {
   
   int64_t jitter = (int64_t)(md_random64() % vctx->delay);     // TODO: more fair
   vctx->delay = (vctx->delay << 1L) + jitter;
   
   struct timespec ts;
   
   clock_gettime( CLOCK_REALTIME, &ts );
   
   ts.tv_sec += vctx->delay;
   
   vctx->retry_deadline = ts;
   return 0;
}

// run a single vacuum context 
// return 0 on success
// return negative on error
// NOTE: this method is idempotent, and should be retried continuously until it succeeds
int UG_vacuum_run( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   
   if( vctx->delay > 0 ) {
      
      // try to wait until the deadline comes (don't worry if interrupted or if passed)
      clock_nanosleep( CLOCK_REALTIME, TIMER_ABSTIME, &vctx->retry_deadline, NULL );
   }
   
   // peek and get the ticket
   rc = UG_vacuumer_peek_vacuum_log( vacuumer, vctx );
   if( rc != 0 ) {
      
      SG_error("UG_vacuumer_peek_vacuum_log( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, rc );
      
      UG_vacuumer_set_delay( vctx );
      return rc;
   }
   
   // get manifest 
   rc = UG_vacuumer_get_manifest_data( vacuumer, vctx );
   if( rc != 0 ) {
      
      SG_error("UG_vacuumer_get_manifest_data( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, rc );
      
      UG_vacuumer_set_delay( vctx );
      return rc;
   }
   
   // delete manifest and blocks 
   rc = UG_vacuumer_chunks_delete( vacuumer, vctx );
   if( rc != 0 ) {
      
      SG_error("UG_vacuumer_chunks_delete( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, rc );
      
      UG_vacuumer_set_delay( vctx );
      return rc;
   }
   
   // update vacuum log 
   rc = UG_vacuumer_clear_vacuum_log( vacuumer, vctx );
   if( rc != 0 ) {
      
      SG_error("UG_vacuumer_clear_vacuum_log( %" PRIX64 ".%" PRId64 "/manifest.%ld.%ld ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, vctx->old_manifest_timestamp.tv_sec, vctx->old_manifest_timestamp.tv_nsec, rc );
      
      // try again 
      UG_vacuumer_set_delay( vctx );
      return rc;
   }
   
   // done!
   return rc;
}


// main vacuumer loop 
static void* UG_vacuumer_main( void* arg ) {
   
   int rc = 0;
   struct UG_vacuumer* vacuumer = (struct UG_vacuumer*)arg;
   
   struct UG_vacuum_context* vctx = NULL;
   
   while( vacuumer->running ) {
      
      // wait for vacuum requests
      while( true ) {
         
         rc = sem_wait( &vacuumer->sem );
         
         if( rc != 0 ) {
            
            rc = -errno;
            if( rc == -EINTR ) {
               
               rc = 0;
               continue;
            }
            else {
               
               SG_error("sem_wait rc = %d\n", rc );
               break;
            }
         }
         
         break;
      }
      
      // signaled?
      if( !vacuumer->running || rc != 0 ) {
         break;
      }
      
      // next context 
      pthread_rwlock_wrlock( &vacuumer->lock );
      
      if( vacuumer->vacuum_queue->size() == 0 ) {
         
         // nothing to do 
         pthread_rwlock_unlock( &vacuumer->lock );
         continue;
      }
      
      vctx = vacuumer->vacuum_queue->front();
      vacuumer->vacuum_queue->pop();
      
      pthread_rwlock_unlock( &vacuumer->lock );
      
      // run it
      rc = UG_vacuum_run( vacuumer, vctx );
      if( rc != 0 ) {
         
         // try again 
         UG_vacuumer_enqueue( vacuumer, vctx );
         rc = 0;
      }
      
      // done!
      UG_vacuum_context_free( vctx );
      SG_safe_free( vctx );
   }
   
   return NULL;
}


// set up a vacuumer 
// return 0 on success
// return -ENOMEM on OOM 
int UG_vacuumer_init( struct UG_vacuumer* vacuumer, struct SG_gateway* gateway ) {
   
   int rc = 0;
   
   memset( vacuumer, 0, sizeof( struct UG_vacuumer ) );
   
   rc = pthread_rwlock_init( &vacuumer->lock, NULL );
   if( rc != 0 ) {
      return rc;
   }
   
   vacuumer->vacuum_queue = SG_safe_new( UG_vacuum_queue_t() );
   if( vacuumer->vacuum_queue == NULL ) {
      
      pthread_rwlock_destroy( &vacuumer->lock );
      return -ENOMEM;
   }
   
   sem_init( &vacuumer->sem, 0, 0 );
   
   return 0;
}

// start vacuuming 
// return 0 if we started a thread 
// return -EPERM if not
int UG_vacuumer_start( struct UG_vacuumer* vacuumer ) {
   
   int rc = 0;
   
   if( vacuumer->running ) {
      return 0;
   }
   
   vacuumer->running = true;
   
   rc = md_start_thread( &vacuumer->thread, UG_vacuumer_main, vacuumer, false );
   if( rc < 0 ) {
      
      SG_error("md_start_thread rc = %d\n", rc );
      vacuumer->running = false;
      return -EPERM;
   }
   
   return 0;
}

// stop vacuuming 
// return 0 if we stopped the thread
// return -ESRCH if the thread isn't running (indicates a bug)
// return -EDEADLK if we would deadlock 
// return -EINVAL if the thread ID is invalid (this is a bug; it should never happen)
int UG_vacuumer_stop( struct UG_vacuumer* vacuumer ) {
   
   int rc = 0;
   
   if( !vacuumer->running ) {
      return 0;
   }
   
   vacuumer->running = false;
   
   rc = pthread_cancel( vacuumer->thread );
   if( rc != 0 ) {
      
      // -ESRCH
      return -abs(rc);
   }
   
   rc = pthread_join( vacuumer->thread, NULL );
   if( rc != 0 ) {
      
      return -abs(rc);
   }
   
   return 0;
}


// shut down a vacuumer 
// return 0 on success
// return -EINVAL if the vacuumer is running
int UG_vacuumer_shutdown( struct UG_vacuumer* vacuumer ) {
   
   if( vacuumer->running ) {
      return -EINVAL;
   }
   
   SG_safe_delete( vacuumer->vacuum_queue );
   pthread_rwlock_destroy( &vacuumer->lock );
   sem_destroy( &vacuumer->sem );
   
   return 0;
}
