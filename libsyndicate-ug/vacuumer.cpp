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
#include "replication.h"

// state for vacuuming data
struct UG_vacuum_context {
   
   char* fs_path;                               // path to the inode
   struct md_entry inode_data;                  // exported inode
   struct SG_manifest* old_blocks;              // blocks to remove

   struct UG_RG_context* rg_context;            // connection to all RGs
   SG_messages::Request* vacuum_request;        // request to send to all RGs
   bool sent_delete;                            // did we send the request successfully? 

   int64_t delay;                               // delay delta for retry_deadline
   struct timespec retry_deadline;              // earliest time in the future when we can try this context again (if it failed)

   sem_t sem;                                   // caller can block on this to wait for the vacuum request to finish
   volatile bool wait;                          // if set, the caller will wait for the context to finish

   bool unlinking;                              // delete *everything*, including the current manifest
   bool result_clean;                           // set to true if there's no more data to vacuum

   int64_t manifest_modtime_sec;                // manifest timestamp being vacuumed 
   int32_t manifest_modtime_nsec;
};

// global vacuum state 
struct UG_vacuumer {
   
   pthread_t thread;
   
   UG_vacuum_queue_t* vacuum_queue;             // queue of vacuum requests to perform
   pthread_rwlock_t lock;                       // lock governing access to the vacuum queue 
   
   sem_t sem;                                   // used to wake up the vacuumer when there's work to be done
   
   volatile bool running;                       // is this thread running?
   volatile bool quiesce;                       // stop taking requests?
   volatile bool exited;                        // set to true if exited
   
   struct SG_gateway* gateway;                  // parent gateway
};


struct UG_vacuumer* UG_vacuumer_new() {
   return SG_CALLOC( struct UG_vacuumer, 1 );
}

struct UG_vacuum_context* UG_vacuum_context_new() {
   return SG_CALLOC( struct UG_vacuum_context, 1 );
}


// set up a vacuum context
// prepare to vacuum only the blocks listed in replaced_blocks 
// if replaced_blocks is NULL, then look up the set of blocks from the MS and vacuum those.
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if this is a directory
// NOTE: inode->entry must be at least read-locked
int UG_vacuum_context_init( struct UG_vacuum_context* vctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* replaced_blocks ) {
   
   int rc = 0;
   struct UG_RG_context* rg_context = NULL;

   // sanity check 
   if( fskit_entry_get_type( UG_inode_fskit_entry( inode ) ) != FSKIT_ENTRY_TYPE_FILE ) {
    
      SG_error("BUG: %" PRIX64 " (%s) is not a file\n", UG_inode_file_id( inode ), fs_path );
      exit(1); 
   }
   
   char* path = SG_strdup_or_null( fs_path );
   if( path == NULL ) {
      
      return -ENOMEM;
   }
   
   // get RGs 
   rg_context = UG_RG_context_new();
   if( rg_context == NULL ) {

      SG_safe_free( path );
      return -ENOMEM;
   }

   rc = UG_RG_context_init( ug, rg_context );
   if( rc != 0 ) {

      SG_safe_free( rg_context );
      SG_safe_free( path );
      return (rc == -ENOMEM ? rc : -EPERM);
   }
   
   // snapshot inode data 
   rc = UG_inode_export( &vctx->inode_data, inode, 0 );
   if( rc != 0 ) {
      
      SG_error("UG_inode_export('%s') rc = %d\n", fs_path, rc );
      SG_safe_free( path );
      UG_RG_context_free( rg_context );
      SG_safe_free( rg_context );
      return rc;
   }

   vctx->rg_context = rg_context;
   vctx->fs_path = path;
   sem_init( &vctx->sem, 0, 0 );
   
   if( replaced_blocks != NULL ) {
     
      // vacuum the given blocks only
      vctx->old_blocks = SG_manifest_new();
      if( vctx->old_blocks == NULL ) {
         
         UG_vacuum_context_free( vctx );
         return -ENOMEM;
      }

      rc = SG_manifest_dup( vctx->old_blocks, replaced_blocks );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_dup rc = %d\n", rc ); 
         UG_vacuum_context_free( vctx );
         return rc;
      }

      vctx->manifest_modtime_sec = SG_manifest_get_modtime_sec( vctx->old_blocks );
      vctx->manifest_modtime_nsec = SG_manifest_get_modtime_nsec( vctx->old_blocks );
   }
  
   return 0;
}


// set the manifest modtime for a vacuum context, overwriting whatever was given in the set of old blocks 
int UG_vacuum_context_set_manifest_modtime( struct UG_vacuum_context* vctx, int64_t sec, int32_t nsec ) {
   vctx->manifest_modtime_sec = sec;
   vctx->manifest_modtime_nsec = nsec;
   return 0;
}


// allow deletion of the current manifest 
int UG_vacuum_context_set_unlinking( struct UG_vacuum_context* vctx, bool unlinking ) {
   vctx->unlinking = unlinking;
   return 0;
}


// free up a vacuum context 
int UG_vacuum_context_free( struct UG_vacuum_context* vctx ) {
   
   md_entry_free( &vctx->inode_data );
   UG_RG_context_free( vctx->rg_context );
   SG_safe_free( vctx->rg_context );
   SG_safe_delete( vctx->vacuum_request ); 
   SG_safe_free( vctx->fs_path );
   sem_destroy( &vctx->sem );
   
   if( vctx->old_blocks != NULL ) {
      SG_manifest_free( vctx->old_blocks );
      SG_safe_free( vctx->old_blocks );
   }
   
   memset( vctx, 0, sizeof(struct UG_vacuum_context) );
   
   return 0;
}


// gift a vacuum context's block data to an inode.
// This merges them into the inode's set of vacuum-able blocks, such that on conflict, the inode's
// blocks are accepted instead of the vacuum context's.
// return 0 on success
// return -ENOMEM on OOM 
int UG_vacuum_context_restore( struct UG_vacuum_context* vctx, struct UG_inode* inode ) {
   
   int rc = 0;
   
   // put back replaced blocks 
   rc = SG_manifest_patch_nocopy( UG_inode_replaced_blocks( inode ), vctx->old_blocks, false );
   SG_manifest_clear_nofree( vctx->old_blocks );
   
   if( rc != 0 ) {
      SG_error("SG_manifest_patch_nocopy rc = %d\n", rc );
      
      return rc;
   }
   
   return 0;
}


// start vacuuming data.  It will be retried indefinitely until it succeeds.
// return 0 on successful enqueue 
// return -ENOMEM on OOM
// return -ENOTCONN if we're quiescing
// NOTE: the vacuumer takes ownership of vctx if wait == false.  do not free or access it after this call.
static int UG_vacuumer_enqueue_ex( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx, bool wait ) {
   
   int rc = 0;
   
   pthread_rwlock_wrlock( &vacuumer->lock );

   // taking requests?
   if( vacuumer->quiesce ) {
      pthread_rwlock_unlock( &vacuumer->lock );
      return -ENOTCONN;
   }

   vctx->wait = wait;
   
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


// start vacuuming data.  It will be retried indefinitely until it succeeds
// caller is not expected to wait for the vacuum request to finish.
// return 0 on successful enqueue 
// return -ENOMEM on OOM
// return -ENOTCONN if we're quiescing 
int UG_vacuumer_enqueue( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   return UG_vacuumer_enqueue_ex( vacuumer, vctx, false );
}


// start vacuuming data.  It will be retried indefinitely until it succeeds.
// caller is expected to wait for the vacuum request to finish.
// return 0 on successful enqueue
// return -ENOMEM on OOM 
// return -ENOTCONN if we're quiescing 
int UG_vacuumer_enqueue_wait( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   return UG_vacuumer_enqueue_ex( vacuumer, vctx, true );
}


// wait for a vacuum context to finish.
// return 0 on success
// return -EINVAL if the vacuum context was not set up to be waited on 
int UG_vacuum_context_wait( struct UG_vacuum_context* vctx ) {
   sem_wait( &vctx->sem );
   return 0;
}


// did this vacuum context indicate that we're done vacuuming?
bool UG_vacuum_context_is_clean( struct UG_vacuum_context* vctx ) {
   return vctx->result_clean;
}


// get the next manifest timestamp and blocks to vacuum
// on success, put it into the vacuum context
// return 0 on success, or if we already have the timestamp
// return -ENODATA if there is no manifest timestamp to be had (i.e. we're all caught up with vacuuming)
// return -errno on error 
static int UG_vacuumer_peek_vacuum_log( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx, struct SG_manifest* old_write_delta ) {
   
   int rc = 0;
   struct ms_vacuum_entry ve;
   
   struct SG_gateway* gateway = vacuumer->gateway;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t file_id = vctx->inode_data.file_id;
   
   memset( &ve, 0, sizeof(struct ms_vacuum_entry) );
   
   // get the head of the vacuum log, and keep the ticket so we can pass it along to the RG
   rc = ms_client_peek_vacuum_log( ms, volume_id, file_id, &ve );
   if( rc != 0 ) {
      
      SG_error("ms_client_peek_vacuum_log(%" PRIX64 ") rc = %d\n", file_id, rc );
      if( rc == -EPROTO ) {
         // no data to be had
         rc = -ENOENT;
      }

      return rc;
   }
   
   // set up the manifest, and store the block IDs 
   rc = SG_manifest_init( old_write_delta, ve.volume_id, ve.writer_id, ve.file_id, ve.file_version );
   if( rc != 0 ) {
      
      // OOM 
      ms_client_vacuum_entry_free( &ve );
      return rc;
   }
  
   SG_manifest_set_modtime( old_write_delta, ve.manifest_mtime_sec, ve.manifest_mtime_nsec );

   // remember the affected block IDs
   for( size_t i = 0; i < ve.num_affected_blocks; i++ ) {
      
      struct SG_manifest_block block_info;
      
      rc = SG_manifest_block_init( &block_info, ve.affected_blocks[i], 0, NULL, 0 );
      if( rc != 0 ) {
         
         SG_manifest_free( old_write_delta );
         ms_client_vacuum_entry_free( &ve );
         return rc;
      }
      
      rc = SG_manifest_put_block( old_write_delta, &block_info, true );
      if( rc != 0 ) {
         
         SG_manifest_block_free( &block_info );
         SG_manifest_free( old_write_delta );
         ms_client_vacuum_entry_free( &ve );
         return rc;
      }
   }

   ms_client_vacuum_entry_free( &ve );
   
   return 0;
}


// get the old manifest block versions and hashes at a particular time, given the timestamp and a list of requests in *block_requests (which only has block IDs and block versions filled in)
// return 0 on success, and populate *block_requests with versioning and (if present) hash data (*block_requests should already have been initialized and populated with block IDs).
// return -ENODATA if we're missing some manifest data
// return -errno on failure
static int UG_vacuumer_get_block_data( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx, struct SG_manifest* block_requests ) {
   
   int rc = 0;
   int64_t old_mtime_sec = SG_manifest_get_modtime_sec( block_requests );
   int32_t old_mtime_nsec = SG_manifest_get_modtime_nsec( block_requests );
   struct SG_manifest* old_manifest = NULL;
  
   struct SG_gateway* gateway = vacuumer->gateway;
   struct SG_request_data reqdat;
   int worst_rc = 0;
   
   old_manifest = SG_manifest_new();
   if( old_manifest == NULL ) {
      return -ENOMEM;
   }
   
   // build a request for this prior manifest
   rc = SG_request_data_init_manifest( gateway, vctx->fs_path, vctx->inode_data.file_id, vctx->inode_data.version, old_mtime_sec, old_mtime_nsec, &reqdat );
   if( rc != 0 ) {
      
      // OOM 
      SG_safe_free( old_manifest );
      return rc;
   }
   
   // try to get the manifest 
   rc = UG_consistency_manifest_download( gateway, &reqdat, UG_RG_context_RG_ids( vctx->rg_context ), UG_RG_context_num_RGs( vctx->rg_context ), old_manifest );
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_error("UG_manifest_download( %" PRIX64 ".%" PRId64 "/manifest.%ld.%d ) rc = %d\n",
               vctx->inode_data.file_id, vctx->inode_data.version, (long)old_mtime_sec, (int)old_mtime_nsec, rc );
      
      SG_manifest_free( old_manifest );
      SG_safe_free( old_manifest );
      return rc;
   }

   SG_debug("Vacuum %" PRIX64 "/manifest.%ld.%d (%zu blocks)\n", vctx->inode_data.file_id, (long)old_mtime_sec, (int)old_mtime_nsec, SG_manifest_get_block_count( block_requests ) );
   
   // fill in the parts of the manifest that we need (i.e. version, hash)
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests ); itr != SG_manifest_block_iterator_end( block_requests ); itr++ ) {
      
      uint64_t block_id = SG_manifest_block_iterator_id( itr );
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( old_manifest, block_id );

      if( block_info == NULL ) {
         
         // that's odd...the old manifest doesn't have a record of the block we're supposed to delete (even though the MS says so).
         SG_error("CRITICAL: Manifest %" PRIX64 ".%" PRId64 "/manifest.%ld.%d is missing [%" PRIu64 ".%" PRId64 "]\n",
                   vctx->inode_data.file_id, vctx->inode_data.version, (long)old_mtime_sec, (int)old_mtime_nsec,
                   SG_manifest_block_iterator_id( itr ), SG_manifest_block_version( SG_manifest_block_iterator_block( itr ) ) );
         
         worst_rc = -ENODATA;
         break;
      }
      else {
         
         SG_manifest_block_free( SG_manifest_block_iterator_block( itr ) );
         rc = SG_manifest_block_dup( SG_manifest_block_iterator_block( itr ), block_info );
         if( rc != 0 ) {
            
            // OOM 
            worst_rc = rc;
            break;
         }

         SG_debug("Vacuum %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
                  vctx->inode_data.file_id, SG_manifest_block_iterator_id( itr ), SG_manifest_block_version( SG_manifest_block_iterator_block( itr ) ) );
      }
   }
   
   SG_manifest_free( old_manifest );
   SG_safe_free( old_manifest );
   return worst_rc;
}


// clear the vacuum log for this write
// return 0 on success
// return -EINVAL if we're not done vacuuming
// return -errno on failure to contact the MS
static int UG_vacuumer_clear_vacuum_log( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx, uint64_t writer_id, int64_t old_mtime_sec, int32_t old_mtime_nsec ) {
   
   int rc = 0;
   struct SG_gateway* gateway = vacuumer->gateway;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   rc = ms_client_remove_vacuum_log_entry( ms, volume_id, writer_id, vctx->inode_data.file_id, vctx->inode_data.version, old_mtime_sec, old_mtime_nsec );
   
   return rc;
}


// increase delay factor by exponentially backing off with random jitter
// always succeeds
int UG_vacuumer_set_delay( struct UG_vacuum_context* vctx ) {
  
   if( vctx->delay <= 1 ) {
       vctx->delay = 1;
   }

   if( vctx->delay < 3600 ) {
       // cap at 1 hour
       int64_t jitter = (int64_t)(md_random64() % vctx->delay);     // TODO: more fair
       vctx->delay = (vctx->delay << 1L) + jitter;
   }

   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   
   ts.tv_sec += vctx->delay;
   
   vctx->retry_deadline = ts;

   return 0;
}


// given a write delta (as a manifest), create a DELETECHUNKS request for both the write delta and its associated blocks
// return 0 on success, and populate *request
// return -ENOMEM on OOM
// return -EPERM otherwise
static int UG_vacuum_create_request( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx, struct SG_manifest* write_delta, SG_messages::Request* request ) {

   int rc = 0;
   struct SG_manifest_block* chunk_info = NULL;
   size_t num_chunks = SG_manifest_get_block_count( write_delta ) + 1;
   int i = 0;
   struct SG_request_data reqdat;
   unsigned char dummy_hash[SHA256_DIGEST_LENGTH];
   
   memset( dummy_hash, 0, SHA256_DIGEST_LENGTH );

   chunk_info = SG_manifest_block_alloc( num_chunks );
   if( chunk_info == NULL ) {
      return -ENOMEM;
   }
    
   // create manifest chunk info 
   rc = SG_manifest_block_init( &chunk_info[i], vctx->manifest_modtime_sec, vctx->manifest_modtime_nsec, dummy_hash, SHA256_DIGEST_LENGTH );
   if( rc != 0 ) {
      SG_safe_free( chunk_info );

      if( rc != -ENOMEM ) {
         rc = -EPERM;
      }

      return rc;
   }
   
   SG_manifest_block_set_type( &chunk_info[i], SG_MANIFEST_BLOCK_TYPE_MANIFEST );

   i++;

   // create chunk infos from manifest
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( write_delta ); itr != SG_manifest_block_iterator_end( write_delta ); itr++ ) {
    
      rc = SG_manifest_block_dup( &chunk_info[i], SG_manifest_block_iterator_block( itr ) );
      if( rc != 0 ) {
         
         goto UG_vacuum_create_request_fail;
      }

      SG_manifest_block_set_type( &chunk_info[i], SG_MANIFEST_BLOCK_TYPE_BLOCK );

      i++;
   }

   // set up request header
   rc = SG_request_data_init_common( vacuumer->gateway, vctx->fs_path, vctx->inode_data.file_id, vctx->inode_data.version, &reqdat );
   if( rc != 0 ) {

      goto UG_vacuum_create_request_fail;
   }

   // generate request 
   rc = SG_client_request_DELETECHUNKS_setup( vacuumer->gateway, request, &reqdat, chunk_info, num_chunks );
   if( rc != 0 ) {

      goto UG_vacuum_create_request_fail;
   }

   // success!

UG_vacuum_create_request_fail:

   // clean up 
   for( int j = 0; j < i; j++ ) {
      SG_manifest_block_free( &chunk_info[j] );
   }
   SG_safe_free( chunk_info );
   SG_request_data_free( &reqdat );

   if( rc < 0 && rc != -ENOMEM ) {
      rc = -EPERM;
   }

   return rc;
}


// run a single vacuum context 
// return 0 on success
// return negative on error
// NOTE: this method is idempotent, and should be retried continuously until it succeeds
int UG_vacuum_run( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx ) {
   
   int rc = 0;
   struct SG_manifest* old_write_delta = NULL;
   SG_messages::Request* vacuum_request = NULL;
   struct SG_gateway* gateway = vacuumer->gateway;
   
   if( vctx->delay > 0 ) {
      
      // try to wait until the deadline comes (don't worry if interrupted or if passed)
      clock_nanosleep( CLOCK_REALTIME, TIMER_ABSTIME, &vctx->retry_deadline, NULL );
   }
  
   if( vctx->vacuum_request == NULL && !vctx->sent_delete ) {
      
      // generate a vacuum request 
      vacuum_request = SG_safe_new( SG_messages::Request() );
      if( vacuum_request == NULL ) {
         // always try again
         return -EAGAIN;
      }

      if( vctx->old_blocks == NULL ) {

         old_write_delta = SG_manifest_new();
         if( old_write_delta == NULL ) {
            // always try again 
            return -EAGAIN;
         }

         // will vacuum everything, except for the current manifest
         // peek and get the set of affected blocks
         rc = UG_vacuumer_peek_vacuum_log( vacuumer, vctx, old_write_delta );
         if( rc != 0 ) {
         
             SG_error("UG_vacuumer_peek_vacuum_log( %" PRIX64 ".%" PRId64 " ) rc = %d\n",
                      vctx->inode_data.file_id, vctx->inode_data.version, rc );
         
             SG_safe_free( old_write_delta );
             SG_safe_delete( vacuum_request );

             if( rc != -EPROTO && rc != -ENODATA ) {
                return rc;
             }
             else {
                // not our place to vacuum in the first place,
                // or we're up-to-date
                vctx->result_clean = true;
                return 0;
             }
          }

          // skip if this is the current manifest, and if we're not unlinking 
          if( !vctx->unlinking &&
              SG_manifest_get_modtime_sec( old_write_delta ) == vctx->inode_data.manifest_mtime_sec &&
              SG_manifest_get_modtime_nsec( old_write_delta ) == vctx->inode_data.manifest_mtime_nsec ) {

             SG_debug("Will not vacuum current manifest %" PRIX64 "/manifest.%" PRId64 ".%d\n",
                      vctx->inode_data.file_id, vctx->inode_data.manifest_mtime_sec, vctx->inode_data.manifest_mtime_nsec );

             SG_manifest_free( old_write_delta );
             SG_safe_free( old_write_delta );
             SG_safe_delete( vacuum_request );

             vctx->result_clean = true;
             return 0;
          }
          
          // get old block data at this timestamp
          rc = UG_vacuumer_get_block_data( vacuumer, vctx, old_write_delta );
          if( rc != 0 ) {
         
             SG_error("UG_vacuumer_get_block_data( %" PRIX64 ".%" PRId64 "/manifest.%ld.%d ) rc = %d\n",
                      vctx->inode_data.file_id, vctx->inode_data.version, (long)vctx->manifest_modtime_sec, (int)vctx->manifest_modtime_nsec, rc );
         
             SG_manifest_free( old_write_delta );
             SG_safe_free( old_write_delta );
             SG_safe_delete( vacuum_request );
             return -EAGAIN;
          }

          vctx->old_blocks = old_write_delta;
          vctx->manifest_modtime_sec = SG_manifest_get_modtime_sec( vctx->old_blocks );
          vctx->manifest_modtime_nsec = SG_manifest_get_modtime_nsec( vctx->old_blocks );
      }
      
      // sanity check
      if( vctx->manifest_modtime_sec == 0 && vctx->manifest_modtime_nsec == 0 ) {

         SG_error("%s", "BUG: did not set manifest timestamp\n");
         exit(1);
      }

      // prepare to delete
      rc = UG_vacuum_create_request( vacuumer, vctx, vctx->old_blocks, vacuum_request );
      if( rc != 0 ) {

         SG_error("UG_vacuum_create_request( %" PRIX64 ".%" PRId64 "/manifest.%ld.%d ) rc = %d\n",
                  vctx->inode_data.file_id, vctx->inode_data.version, (long)vctx->manifest_modtime_sec, (int)vctx->manifest_modtime_nsec, rc );

         SG_safe_delete( vacuum_request );
         return -EAGAIN;
      }

      vctx->vacuum_request = vacuum_request;
   }

   if( vctx->vacuum_request != NULL ) {

      // run the deletion 
      rc = UG_RG_send_all( gateway, vctx->rg_context, vctx->vacuum_request, NULL );
      if( rc != 0 ) {

         // need to try again!
         SG_error("UG_RG_send_all rc = %d\n", rc );

         // TODO: record vacuum into to disk, so we can try again across gateway stop/start
         return -EAGAIN;
      }

      // success!
      vctx->sent_delete = true;
      SG_safe_delete( vctx->vacuum_request );
      vctx->vacuum_request = NULL;
   }

   if( vctx->sent_delete ) {

      // sanity check 
      if( vctx->manifest_modtime_sec == 0 && vctx->manifest_modtime_nsec == 0 ) {
         SG_error("BUG: did not set an old manifest timestamp for vacuum context %p\n", vctx );
         exit(1);
      }

      // dequeue vacuum log 
      rc = UG_vacuumer_clear_vacuum_log( vacuumer, vctx, SG_manifest_get_coordinator( vctx->old_blocks ), vctx->manifest_modtime_sec, vctx->manifest_modtime_nsec );
      if( rc == -ENOENT ) {
         // cleared already 
         rc = 0;
      }

      if( rc != 0 ) {
     
        SG_error("UG_vacuumer_clear_vacuum_log( %" PRIX64 ".%" PRId64 "/manifest.%ld.%d ) rc = %d\n",
                  vctx->inode_data.file_id, vctx->inode_data.version, (long)vctx->manifest_modtime_sec, (int)vctx->manifest_modtime_nsec, rc );
      
        // try again 
        return -EAGAIN;
      }
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
         if( vacuumer->quiesce ) {
            // done!
            break;
         }
         else {
            continue;
         }
      }
      
      vctx = vacuumer->vacuum_queue->front();
      pthread_rwlock_unlock( &vacuumer->lock );
      
      // run it
      rc = UG_vacuum_run( vacuumer, vctx );

      // remove it 
      pthread_rwlock_wrlock( &vacuumer->lock );
      vacuumer->vacuum_queue->pop();
      pthread_rwlock_unlock( &vacuumer->lock );

      if( rc == -EAGAIN ) {
         
         // try again, but later 
         SG_debug("Try to vacuum %" PRIX64 " again\n", vctx->inode_data.file_id);
         UG_vacuumer_set_delay( vctx );
         UG_vacuumer_enqueue( vacuumer, vctx );
         rc = 0;
         continue;
      }
      else if( rc != 0 ) {
         SG_error("UG_vacuum_run rc = %d\n", rc);
      }
      
      // done!
      if( !vctx->wait ) {
          UG_vacuum_context_free( vctx );
          SG_safe_free( vctx );
      }
      else {
         // caller will free
         sem_post( &vctx->sem );
      }
   }
  
   SG_debug("%s", "Vacuumer thread exited\n");
   vacuumer->exited = true; 
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
  
   vacuumer->gateway = gateway; 
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


// stop taking new requests
// return 0 on success
// return -EINVAL if the vacuumer is stopped
int UG_vacuumer_quiesce( struct UG_vacuumer* vacuumer ) {
   if( !vacuumer->running ) {
      return -EINVAL;
   }

   vacuumer->quiesce = true;
   return 0;
}


// wait for all outstanding requests to finish 
// return 0 on success
int UG_vacuumer_wait_all( struct UG_vacuumer* vacuumer ) {

   bool again = false;
   bool exited = false;
   size_t count = 0;

   while( !exited ) {

      again = false;
      pthread_rwlock_rdlock( &vacuumer->lock );
      
      count = vacuumer->vacuum_queue->size();
      if( count > 0 ) {
         again = true;
      }

      exited = vacuumer->exited;

      pthread_rwlock_unlock( &vacuumer->lock );

      if( again ) {
         SG_debug("Wait for %zu vacuum requests to finish\n", count );
         sleep(1);
      }
      else {
         // done!
         break;
      }
   }

   return 0;
}


// stop vacuuming 
// return 0 if we stopped the thread
// return -ESRCH if the thread isn't running (indicates a bug)
// return -EDEADLK if we would deadlock 
// return -EINVAL if the thread ID is invalid (this is a bug; it should never happen)
// return -EINVAL if the vacuumer is NULL
int UG_vacuumer_stop( struct UG_vacuumer* vacuumer ) {
   
   int rc = 0;
   if( vacuumer == NULL ) {
      return -EINVAL;
   }
   
   if( !vacuumer->running ) {
      return 0;
   }
   
   vacuumer->quiesce = true;
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
// return -EINVAL if the vacuumer is running, or NULL
int UG_vacuumer_shutdown( struct UG_vacuumer* vacuumer ) {
  
   if( vacuumer == NULL ) {
      return -EINVAL;
   }

   if( vacuumer->running ) {
      return -EINVAL;
   }
   
   SG_safe_delete( vacuumer->vacuum_queue );
   pthread_rwlock_destroy( &vacuumer->lock );
   sem_destroy( &vacuumer->sem );
   
   return 0;
}

