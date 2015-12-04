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
#include "client.h"

#define REPLICA_NOT_STARTED     0
#define REPLICA_IN_PROGRESS     1
#define REPLICA_SUCCESS         2

// snapshot of inode fields needed for replication and garbage collection 
struct UG_replica_context {
  
   struct UG_state* state;              // pointer to UG

   char* fs_path;                       // path to the file to replicate
   SG_messages::Request* controlplane_request;  // control-plane component 
   int dataplane_fd;                            // data-plane component
   struct SG_chunk dataplane_mmap;              // mmap'ed data-plane component 

   struct md_entry inode_data;          // exported inode
   uint64_t* affected_blocks;           // block IDs affected by the write
   size_t num_affected_blocks;          // length of the above list
   
   struct SG_manifest write_delta;      // write delta to send to the coordinator

   uint64_t* rg_ids;                    // replica gateway IDs
   int* rg_status;                      // rg_status[i] is the replication status for rg_ids[i] (0: not started; 1: in progress; 2: success; negative: failure)
   size_t num_rgs;                      // number of replica gateways
   
   bool flushed_blocks;                 // if true, then the blocks have all been flushed to disk and can be replicated 
   bool sent_vacuum_log;                // if true, then we've told the MS about the manifest and blocks we're about to replicate 
   bool replicated_blocks;              // if true, then we've replicated blocks and manifests
   bool sent_ms_update;                 // if true, then we've sent the new inode metadata to the MS
};


struct UG_replica_context* UG_replica_context_new() {
   return SG_CALLOC( struct UG_replica_context, 1 );
}

// sign and serialize a manifest to a chunk 
// return 0 on success, and populate *chunk 
// return -ENOMEM on OOM 
// return -EPERM if the signing or serialization failed 
// return -ENODATA if we failed to serialize with the driver
static int UG_replica_sign_serialize_manifest_to_chunk( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* manifest, struct SG_chunk* raw_chunk ) {

   int rc = 0;
   SG_messages::Manifest mmsg;
   struct ms_client* ms = SG_gateway_ms( gateway );
   EVP_PKEY* privkey = ms_client_my_privkey( ms );
   struct SG_chunk chunk;
   char* chunk_buf = NULL;
   size_t chunk_buflen = 0;

   struct SG_request_data reqdat;
   
   rc = SG_request_data_init_manifest( gateway, fs_path, SG_manifest_get_file_id( manifest ), SG_manifest_get_file_version( manifest ), SG_manifest_get_modtime_sec( manifest ), SG_manifest_get_modtime_nsec( manifest ), &reqdat );
   if( rc != 0 ) {
      return rc;
   }
   
   // serialize 
   rc = SG_manifest_serialize_to_protobuf( manifest, &mmsg );
   if( rc != 0 ) {
      
      SG_request_data_free( &reqdat );

      if( rc == -ENOMEM ) {
         return rc;
      }
      else {
         return -EPERM;
      }
   }

   // sign 
   rc = md_sign< SG_messages::Manifest >( privkey, &mmsg );
   if( rc != 0 ) {

      SG_request_data_free( &reqdat );
      return rc;
   }

   // convert to chunk 
   rc = md_serialize< SG_messages::Manifest >( &mmsg, &chunk_buf, &chunk_buflen );
   if( rc != 0 ) {

      SG_request_data_free( &reqdat );
      return rc;
   }

   SG_chunk_init( &chunk, chunk_buf, chunk_buflen );

   // serialize the chunk
   rc = SG_gateway_impl_serialize( gateway, &reqdat, &chunk, raw_chunk );
   SG_request_data_free( &reqdat );
   SG_chunk_free( &chunk );

   if( rc != 0 ) {

      SG_error("SG_gateway_impl_serialize rc = %d\n", rc );
      return -ENODATA;
   }

   return rc;
}


// generate chunk info from a manifest chunk
// return 0 on success, and populate *chunk_info 
// return -ENOMEM on OOM 
static int UG_replica_make_manifest_chunk_info( struct SG_chunk* manifest_chunk, int64_t mtime_sec, int32_t mtime_nsec, struct SG_manifest_block* chunk_info ) {

   int rc = 0;
   unsigned char* hash = NULL;

   hash = sha256_hash_data( manifest_chunk->data, manifest_chunk->len );
   if( hash == NULL ) {
      return -ENOMEM;
   }

   rc = SG_manifest_block_init( chunk_info, mtime_sec, mtime_nsec, hash, SG_BLOCK_HASH_LEN );
   if( rc != 0 ) {

      SG_safe_free( hash );
      return rc;
   }

   return rc;
}


// generate chunk info from a dirty block
// NOTE: not thread-safe w.r.t. the block; the block must *not* be modified while this method is being called!
// return 0 on success, and populate *chunk_info 
// return -ENOMEM on OOM 
// return -EPERM if the block could not be mapped into RAM
static int UG_replica_make_block_chunk_info( struct UG_dirty_block* block, uint64_t block_id, int64_t block_version, struct SG_manifest_block* chunk_info ) {

   unsigned char* hash = NULL;
   bool mmaped = false;
   int rc = 0;
   struct SG_chunk* chunk_buf = NULL;

   // if not in RAM; then mmap it
   if( !UG_dirty_block_in_RAM( block ) ) {
      rc = UG_dirty_block_mmap( block );
      if( rc != 0 ) {

         SG_error("UG_dirty_block_mmap(%" PRIu64 ".%" PRId64 ") rc = %d\n", block_id, block_version, rc );
         return -EPERM;
      }

      mmaped = true;
   }

   chunk_buf = UG_dirty_block_buf( block );

   hash = sha256_hash_data( chunk_buf->data, chunk_buf->len );
   if( hash == NULL ) {

      if( mmaped ) {
         UG_dirty_block_munmap( block );
      }

      return -ENOMEM;
   }

   rc = SG_manifest_block_init( chunk_info, block_id, block_version, hash, SG_BLOCK_HASH_LEN );
   if( rc != 0 ) {

      if( mmaped ) {
         UG_dirty_block_munmap( block );
      }

      SG_safe_free( hash );
      return rc;
   }

   if( mmaped ) {
      UG_dirty_block_munmap( block );
   }

   return rc;
}


// given the whole manifest and the blocks to replicate, calculate the delta to send to the coordinator.
// return 0 on success, and populate *write_delta
// return -ENOMEM on OOM
static int UG_replica_make_write_delta( struct SG_manifest* whole_manifest, UG_dirty_block_map_t* flushed_blocks, struct SG_manifest* write_delta ) {

   int rc = 0;
   rc = SG_manifest_init( write_delta, SG_manifest_get_volume_id( whole_manifest ), SG_manifest_get_coordinator( whole_manifest ), SG_manifest_get_file_id( whole_manifest ), SG_manifest_get_file_version( whole_manifest ) );
   if( rc != 0 ) {
      return rc;
   }

   for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {

      struct SG_manifest_block* write_block_info = UG_dirty_block_info( &itr->second );
      rc = SG_manifest_put_block( write_delta, write_block_info, true );
      if( rc != 0 ) {

         // EINVAL indicats a bug
         if( rc != -ENOMEM ) {
            SG_error("BUG: SG_manifest_put_block rc = %d\n", rc );
            exit(1);
         }

         SG_manifest_free( write_delta );
         return rc;
      }
   }

   return 0;
}


// create the replica control-plane message 
// return 0 on success, and populate *request and *serialized_manifest.  Does *NOT* calculate size and offset fields in the request
// return -ENOMEM on OOM 
static int UG_replica_context_make_controlplane_message( struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* manifest,
                                                         UG_dirty_block_map_t* flushed_blocks, SG_messages::Request* request, struct SG_chunk* serialized_manifest ) {
   
   int rc = 0;
   int num_chunks = 0;
   struct SG_request_data reqdat;
   struct SG_gateway* gateway = UG_state_gateway( ug );
   size_t chunks_capacity = 0;
   struct SG_manifest_block* chunk_info = NULL;
   struct SG_chunk manifest_chunk;

   // get basic info
   rc = SG_request_data_init_common( gateway, fs_path, UG_inode_file_id( inode ), UG_inode_file_version( inode ), &reqdat );
   if( rc != 0 ) {
      goto UG_replica_context_make_controlplane_message_fail;
   }

   // make chunk info 
   chunks_capacity = 1;
   if( flushed_blocks != NULL ) {
      chunks_capacity += flushed_blocks->size();
   }

   chunk_info = SG_manifest_block_alloc( chunks_capacity );
   if( chunk_info == NULL ) {
      rc = -ENOMEM;
      goto UG_replica_context_make_controlplane_message_fail;
   }

   // manifest chunk 
   rc = UG_replica_sign_serialize_manifest_to_chunk( gateway, fs_path, manifest, &manifest_chunk );
   if( rc != 0 ) {

      SG_error("UG_replica_sign_serialize_manifest_to_chunk rc = %d\n", rc );
      goto UG_replica_context_make_controlplane_message_fail;
   }

   // manifest chunk info 
   rc = UG_replica_make_manifest_chunk_info( &manifest_chunk, SG_manifest_get_modtime_sec( manifest ), SG_manifest_get_modtime_nsec( manifest ), &chunk_info[0] );
   if( rc != 0 ) {

      SG_error("UG_replica_make_manifest_chunk_info(%s) rc = %d\n", fs_path, rc );
      goto UG_replica_context_make_controlplane_message_fail;
   }

   num_chunks++;

   if( flushed_blocks != NULL ) {
      
      // sanity check 
      for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {
        
         // not dirty?
         if( !UG_dirty_block_dirty( &itr->second ) ) {
         
            SG_error("BUG: %" PRIX64 "[%" PRIu64 ".%" PRId64 "] not dirty\n", UG_inode_file_id( inode ), itr->first, UG_dirty_block_version( &itr->second ) );
            exit(1);
         }
      }
      
      for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {

         struct UG_dirty_block* block = &itr->second;
         rc = UG_replica_make_block_chunk_info( block, UG_dirty_block_id( block ), UG_dirty_block_version( block ), &chunk_info[num_chunks] );
         if( rc != 0 ) {

            SG_error("UG_replica_make_chunk_info rc = %d\n", rc );
            goto UG_replica_context_make_controlplane_message_fail;
         }

         num_chunks++;
      }
   }

   // generate the message
   rc = SG_client_request_PUTCHUNKS_setup( gateway, request, &reqdat, chunk_info, num_chunks );
   if( rc != 0 ) {

      goto UG_replica_context_make_controlplane_message_fail;
   }

   // transfer manifest
   *serialized_manifest = manifest_chunk;
   memset( &manifest_chunk, 0, sizeof(struct SG_chunk) );

UG_replica_context_make_controlplane_message_fail:

   // clean up
   SG_request_data_free( &reqdat );

   for( int i = 0; i < num_chunks; i++ ) {
      SG_manifest_block_free( &chunk_info[i] );
   }

   SG_safe_free( chunk_info );
   
   return rc;
}


// create the replica data-plane message.
// write out the serialized data-plane message to disk.
// NOTE: each block in flushed_blocks must be in-RAM, dirty, and already flushed to disk (i.e. it must also have a file descriptor)
// return 0 on success, and populate the size and offset fields for each block in the control-plane request *request, and populate *dataplane_fd
// return -errno on fs-related errors.
// return -ENAMETOOLONG on temporary path overflow
// TODO DRIVER
static int UG_replica_context_make_dataplane_message( struct UG_state* ug, SG_messages::Request* request, struct SG_chunk* manifest_chunk, UG_dirty_block_map_t* flushed_blocks, int* dataplane_fd ) {

   int rc = 0;
   int fd = -1;
   uint64_t off = 0;
   struct SG_gateway* gateway = UG_state_gateway( ug );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   char* data_root = md_conf_get_data_root( conf );
   char tmppath[PATH_MAX];
   struct stat sb;

   rc = snprintf( tmppath, PATH_MAX-1, "%s/.replica-XXXXXX", data_root );
   if( rc >= PATH_MAX-1 ) {
      return -ENAMETOOLONG;
   }
   
   // sanity check: all blocks must be in RAM
   for( int i = 0; i < request->blocks_size(); i++ ) {

      SG_messages::ManifestBlock* block_info = request->mutable_blocks(i);
      struct UG_dirty_block* block = &(*flushed_blocks)[ block_info->block_id() ];
      if( block == NULL ) {

         SG_error("BUG: block %" PRIu64 " not present\n", block_info->block_id() );
         exit(1);
      }

      if( !UG_dirty_block_in_RAM( block ) ) {

         SG_error("BUG: block %" PRIu64 " not in RAM\n", block_info->block_id() );
         exit(1);
      }

      if( UG_dirty_block_fd( block ) < 0 ) {

         SG_error("BUG: block %" PRIu64 " not flushed\n", block_info->block_id() );
         exit(1);
      }
   }

   // flush to disk
   fd = mkostemp( tmppath, O_CLOEXEC );
   if( fd < 0 ) {
      rc = -errno;
      
      SG_error("mkostemp: %s\n", strerror(-rc));
      goto UG_replica_context_make_dataplane_message_fail;
   }

   rc = unlink( tmppath );
   if( rc < 0 ) {
      rc = -errno;

      SG_error("unlink(%s) rc = %d\n", tmppath, rc );
      goto UG_replica_context_make_dataplane_message_fail;
   }

   // flush manifest 
   rc = md_write_uninterrupted( fd, manifest_chunk->data, manifest_chunk->len );
   if( rc != 0 ) {

      SG_error("md_write_uninterrupted rc = %d\n", rc );
      goto UG_replica_context_make_dataplane_message_fail;
   }

   // flush each block
   // NOTE: blocks[0] should be the manifest info; blocks[1..n] are the block info
   for( int i = 1; i < request->blocks_size(); i++ ) {
    
      SG_messages::ManifestBlock* block_info = request->mutable_blocks(i);
      struct UG_dirty_block* block = &(*flushed_blocks)[ block_info->block_id() ];
      int block_fd = UG_dirty_block_fd( block );

      // fetch data from the serialized block on disk
      rc = fstat( block_fd, &sb );
      if( rc != 0 ) {
         rc = -errno;
         SG_error("fstat(%d) rc = %d\n", block_fd, rc );
         goto UG_replica_context_make_dataplane_message_fail;
      }
      
      // extend with info
      block_info->set_offset( off );
      block_info->set_size( sb.st_size );

      // transfer the serialized on-disk block chunk
      rc = md_transfer( block_fd, fd, sb.st_size );
      if( rc != 0 ) {

         SG_error("md_transfer rc = %d\n", rc );
         goto UG_replica_context_make_dataplane_message_fail;
      }

      off += sb.st_size;
   }

   // success!
   *dataplane_fd = fd;
   return 0;

UG_replica_context_make_dataplane_message_fail:

   // clean up
   if( fd >= 0 ) {
      close( fd );
      fd = -1;
   }

   return rc;
}


// set up a replica context from an inode's dirty blocks and its current *whole* manifest.
// flushed_blocks is allowed to be NULL, in which case only the manifest will be replicated.
// NOTE: inode->entry should be read-locked
// NOTE: if non-NULL, then flushed_blocks must all be dirty and in RAM
// return 0 on success
// return -ENOMEM on OOM 
int UG_replica_context_init( struct UG_replica_context* rctx, struct UG_state* ug,
                             char const* fs_path, struct UG_inode* inode, struct SG_manifest* manifest, UG_dirty_block_map_t* flushed_blocks ) {
   
   int rc = 0;
   SG_messages::Request* controlplane = NULL;
   struct SG_chunk serialized_manifest;
   int dataplane_fd = -1;
   char* fd_buf = NULL;
   struct stat sb;
   uint64_t* affected_blocks = NULL;
   size_t num_affected_blocks = 0;
   
   memset( rctx, 0, sizeof( struct UG_replica_context ) );
   
   if( flushed_blocks != NULL ) {

      affected_blocks = SG_CALLOC( uint64_t, flushed_blocks->size() );
      if( affected_blocks == NULL ) {
         return -ENOMEM;
      }
      
      // sanity check 
      for( UG_dirty_block_map_t::iterator itr = flushed_blocks->begin(); itr != flushed_blocks->end(); itr++ ) {
        
         struct UG_dirty_block* block = &itr->second;

         // not dirty?
         if( !UG_dirty_block_dirty( block ) ) {
         
            SG_error("BUG: %" PRIX64 "[%" PRIu64 ".%" PRId64 "] not dirty\n", UG_inode_file_id( inode ), itr->first, UG_dirty_block_version( block ) );
            exit(1);
         }

         // can't be flushing
         if( UG_dirty_block_is_flushing( block ) ) {

            SG_error("BUG: %" PRIX64 "[%" PRIu64 ".%" PRId64 "] is flushing\n", UG_inode_file_id( inode ), itr->first, UG_dirty_block_version( block ) );
            exit(1);
         }
      
         // must be in RAM
         if( !UG_dirty_block_in_RAM( block ) ) {

            SG_error("BUG: %" PRIX64 "[%" PRIu64 ".%" PRId64 "] not in RAM\n", UG_inode_file_id( inode ), itr->first, UG_dirty_block_version( block ) );
            exit(1);
         } 

         affected_blocks[num_affected_blocks] = itr->first;
         num_affected_blocks++;
      }
   }

   rctx->state = ug;
   rctx->affected_blocks = affected_blocks;
   rctx->num_affected_blocks = num_affected_blocks;

   // get RGs 
   rc = UG_state_list_replica_gateway_ids( ug, &rctx->rg_ids, &rctx->num_rgs );
   if( rc != 0 ) {
     
      SG_error("UG_state_list_replica_gateway_ids rc = %d\n", rc ); 
      return rc;
   }

   rctx->rg_status = SG_CALLOC( int, rctx->num_rgs );
   if( rc != 0 ) {

      rc = -ENOMEM;
      UG_replica_context_free( rctx );
      return rc;
   }

   // create fields
   rctx->fs_path = SG_strdup_or_null( fs_path );
   if( rctx->fs_path == NULL ) {

      UG_replica_context_free( rctx );
      return rc;
   }

   rc = UG_inode_export( &rctx->inode_data, inode, 0 );
   if( rc != 0 ) {

      UG_replica_context_free( rctx );
      return rc;
   }

   rc = UG_replica_make_write_delta( manifest, flushed_blocks, &rctx->write_delta );
   if( rc != 0 ) {

      UG_replica_context_free( rctx );
      return rc; 
   }

   // make control-plane component
   controlplane = SG_safe_new( SG_messages::Request() );
   if( controlplane == NULL ) {

      UG_replica_context_free( rctx );
      return rc;
   }

   rc = UG_replica_context_make_controlplane_message( ug, fs_path, inode, manifest, flushed_blocks, controlplane, &serialized_manifest );
   if( rc != 0 ) {

      UG_replica_context_free( rctx );
      SG_safe_delete( controlplane );
      SG_error("UG_replica_context_make_controlplane_message rc = %d\n", rc );
      return rc;
   }

   // make data-plane component
   rc = UG_replica_context_make_dataplane_message( ug, controlplane, &serialized_manifest, flushed_blocks, &dataplane_fd );
   SG_chunk_free( &serialized_manifest );
   if( rc != 0 ) {

      UG_replica_context_free( rctx );
      SG_safe_delete( controlplane );
      SG_error("UG_replica_context_make_dataplane_message rc = %d\n", rc );
      return rc;
   }

   rctx->dataplane_fd = dataplane_fd;
   rctx->controlplane_request = controlplane;

   // mmap data-plane component so we can send it 
   rc = fstat( dataplane_fd, &sb );
   if( rc != 0 ) {

      rc = -errno;
      UG_replica_context_free( rctx );
      SG_error("fstat: %s\n", strerror(-rc) );
      return rc;
   }

   fd_buf = (char*)mmap( NULL, sb.st_size, PROT_READ, MAP_SHARED, dataplane_fd, 0 );
   if( fd_buf == NULL ) {

      rc = -errno;
      UG_replica_context_free( rctx );
      SG_error("mmap: %s\n", strerror(-rc) );
      return rc;
   }

   SG_chunk_init( &rctx->dataplane_mmap, fd_buf, sb.st_size );
   return rc;
}


// free up a replica context 
// always succeeds 
int UG_replica_context_free( struct UG_replica_context* rctx ) {
   
   SG_safe_free( rctx->fs_path ); 
   md_entry_free( &rctx->inode_data );
   SG_manifest_free( &rctx->write_delta );
   SG_safe_delete( rctx->controlplane_request );
   SG_safe_free( rctx->rg_ids );
   SG_safe_free( rctx->rg_status );
   SG_safe_free( rctx->affected_blocks );
   
   memset( rctx, 0, sizeof(struct UG_replica_context) );
   
   return 0;
}

// append a file's vacuum log on the MS
// does *NOT* set rctx->sent_vacuum_log
// return 0 on success
// return -ENOMEM on OOM 
// return -errno on connection errors 
static int UG_replicate_vacuum_log( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   struct ms_vacuum_entry ve;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // set up the vacuum entry
   rc = ms_client_vacuum_entry_init( &ve, rctx->inode_data.volume, ms_client_get_gateway_id( ms ), rctx->inode_data.file_id, rctx->inode_data.version,
                                     rctx->inode_data.manifest_mtime_sec, rctx->inode_data.manifest_mtime_nsec, rctx->affected_blocks, rctx->num_affected_blocks );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_vacuum_entry_init( %" PRIX64 ".%" PRId64 " (%zu blocks) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->num_affected_blocks, rc );
      return rc;
   }
    
   // send it off
   rc = ms_client_append_vacuum_log_entry( ms, &ve );
   if( rc != 0 ) {
      
      SG_error("ms_client_append_volume_log_entry( %" PRIX64 ".%" PRId64 " (%zu blocks) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->num_affected_blocks, rc );
   }
   
   ms_client_vacuum_entry_free( &ve );
   
   return rc;
}


// Send data to be replicated to each RG 
// Replication is all-or-nothing: if even one RG fails to receive the data, the replication is
// considered to have failed.  Complex "partial replication" error handling and masking should be performed
// in the RG implementation, not the UG.
// return 0 on successful replication to *all* RGs
// return -EIO if at least one replication fails, for any reason.
int UG_replicate_send( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {

   int rc = 0;
   struct md_download_loop dlloop;
   struct md_download_context* dlctx = NULL;
   SG_messages::Reply reply;

   rc = md_download_loop_init( &dlloop, SG_gateway_dl( gateway ), rctx->num_rgs );
   if( rc != 0 ) {

      SG_error("md_download_loop_init rc = %d\n", rc );
      return rc;
   }

   // try to send to each RG 
   do {

       // next free download context
       rc = md_download_loop_next( &dlloop, &dlctx );
       if( rc == 0 ) {

          // have one to start.
          // start sending to the next REPLICA_NOT_STARTED-tagged RG 
          for( size_t i = 0; i < rctx->num_rgs; i++ ) {

              if( rctx->rg_status[i] == REPLICA_NOT_STARTED ) {
                  
                  SG_debug("Replicate %" PRIu64 ": %p\n", rctx->rg_ids[i], dlctx );

                  rc = SG_client_request_send_async( gateway, rctx->rg_ids[i], rctx->controlplane_request, &rctx->dataplane_mmap, &dlloop, dlctx );
                  if( rc != 0 ) {

                     SG_error("SG_client_request_send_async(to %" PRIu64 ") rc = %d\n", rctx->rg_ids[i], rc );
                     break;
                  }

                  rctx->rg_status[i] = REPLICA_IN_PROGRESS;
                  break;
              }
          }

          if( rc != 0 ) {
             // failed to connect 
             break;
          }
       }
       else if( rc != -EAGAIN ) {

          // fatal error 
          break;
       }

       // wait for an upload to finish
       rc = md_download_loop_finished( &dlloop, &dlctx );
       if( rc == 0 ) {

          // one finished
          rc = SG_client_request_send_finish( gateway, dlctx, &reply );
          if( rc != 0 ) {
      
              SG_error("SG_client_request_send_finish rc = %d\n", rc );
              break;
          }

          // did the replication succeed?
          if( reply.error_code() != 0 ) {

             SG_error("replication %p failed: %d\n", dlctx, reply.error_code());
             rc = reply.error_code();
             break;
          }
       }
       else if( rc != -EAGAIN ) {

          // fatal error 
          break;
       }

   } while( md_download_loop_running( &dlloop ) );

   if( rc != 0 ) {
      
      // replication failed. terminate.
      SG_error("Terminating replication, rc = %d\n", rc );
      md_download_loop_abort( &dlloop );

      rc = -EIO;
   }

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
// return -EINVAL for improper arguments
// return -ENODATA if the HTTP error was a 400-level error
// return other -errno on socket- and recv-related errors
int UG_replicate( struct SG_gateway* gateway, struct UG_replica_context* rctx ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
      
   // (1) make sure the MS knows about this replication request
   if( !rctx->sent_vacuum_log ) {
      
      SG_debug("%" PRIX64 ": replicate vacuum log\n", rctx->inode_data.file_id );

      rc = UG_replicate_vacuum_log( gateway, rctx );
      if( rc != 0 ) {
         
         SG_error("UG_replicate_vacuum_log( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", rctx->inode_data.file_id, rctx->inode_data.version, rctx->fs_path, rc );
         if( rc == -EINVAL ) {

            // indicates a bug 
            SG_error("BUG: UG_replicate_vacuum_log rc = %d\n", rc );
            return -EINVAL;
         }
         else {
            return -EAGAIN;
         }
      }
      else {
         
         // success!
         rctx->sent_vacuum_log = true;
      }
   }
   
   // (2) replicate the manifest and each block to each replica gateway
   if( !rctx->replicated_blocks ) {
      
      SG_debug("%" PRIX64 ": replicate manifest and blocks\n", rctx->inode_data.file_id );

      // send off to all RGs
      rc = UG_replicate_send( gateway, rctx );
      if( rc != 0 ) {
         
         SG_error("UG_replicate_send() rc = %d\n", rc );
         
         return rc;
      }
      else {
         
         rctx->replicated_blocks = true;
      }
   }
   
   // (3) update the record on the MS: either send the update to the MS ourselves (if we're the coordinator),
   // or send it to the coordinator directly.
   if( !rctx->sent_ms_update ) {
      
      SG_debug("%" PRIX64 ": send MS updates\n", rctx->inode_data.file_id );

      // send it to the MS if we're the coordinator, 
      // or send it to the coordinator itself.
      struct SG_client_WRITE_data* write_data = SG_client_WRITE_data_new();
      struct timespec mtime;
      
      if( write_data == NULL ) {
          return -ENOMEM;
      }
      
      mtime.tv_sec = rctx->inode_data.mtime_sec;
      mtime.tv_nsec = rctx->inode_data.mtime_nsec;
      
      SG_client_WRITE_data_init( write_data );
      SG_client_WRITE_data_set_mtime( write_data, &mtime );
      SG_client_WRITE_data_set_write_delta( write_data, &rctx->write_delta );
      SG_client_WRITE_data_set_routing_info( write_data, volume_id, rctx->inode_data.coordinator, rctx->inode_data.file_id, rctx->inode_data.version );
     
      // NOTE: this could turn us into the coordinator 
      rc = UG_update( rctx->state, rctx->fs_path, write_data );
      if( rc != 0 ) {
          
         SG_error("UG_update('%s') rc = %d\n", rctx->fs_path, rc );
      }
      else {

         rctx->sent_ms_update = true;
      }
      
      SG_safe_free( write_data );
   }

   // done!
   return rc;
}

// get a pointer to inode snapshot 


