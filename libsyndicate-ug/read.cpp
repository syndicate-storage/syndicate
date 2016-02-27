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

#include "read.h"
#include "block.h"
#include "inode.h"
#include "consistency.h"
#include "client.h"

// track which gateway to download a given block from
typedef map< uint64_t, int > UG_block_gateway_map_t;

// set up a manifest and dirty block map to receive a block into a particular buffer 
// the block put into *blocks takes ownership of buf, so the caller must not free it
// NOTE: buf must be at least the size of a volume block.  IT WILL BE MODIFIED. 
// return 0 on success
// return -ENOMEM on OOM
// inode->entry must be read-locked
int UG_read_setup_block_buffer( struct UG_inode* inode, uint64_t block_id, char* buf, uint64_t buf_len, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;

   struct UG_dirty_block block_data;
   struct SG_manifest_block* block_info = NULL;
   unsigned char empty_hash[SG_BLOCK_HASH_LEN];
   
   // look up this block's info from the manifest 
   block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), block_id );
   if( block_info == NULL ) {
      
      SG_debug("Write hole: no manifest info for %" PRIX64 "[%" PRIu64 "]\n", UG_inode_file_id( inode ), block_id );
      block_info = SG_manifest_block_alloc( 1 );
      if( block_info == NULL ) {
         return -ENOMEM;
      }

      // hash of zero's
      memset( empty_hash, 0, SG_BLOCK_HASH_LEN );
      memset( buf, 0, buf_len );
      sha256_hash_buf( buf, buf_len, empty_hash );

      SG_manifest_block_init( block_info, block_id, 0, empty_hash, SG_BLOCK_HASH_LEN );
   }
   
   // generate the dirty block 
   rc = UG_dirty_block_init_ram_nocopy( &block_data, block_info, buf, buf_len );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // gifted, so unshared
   UG_dirty_block_set_unshared( &block_data, true );

   // and put it in place 
   try {
      
      (*blocks)[ block_id ] = block_data;
   }
   catch( bad_alloc& ba ) {
      
      UG_dirty_block_free_keepbuf( &block_data );
      return -ENOMEM;
   }
   
   return rc;
}


// is there an unaligned head?
static bool UG_read_has_unaligned_head( off_t offset, uint64_t block_size ) {

   return (offset % block_size) != 0;
}

// is there an unaligned tail?
static bool UG_read_has_unaligned_tail( off_t offset, size_t len, uint64_t inode_size, uint64_t block_size ) {

   uint64_t first_block = 0;
   uint64_t last_block = 0; 

   if( offset + len > inode_size ) {
      // will hit EOF, so think of the read as reading to the end but no further 
      len = inode_size - offset;
   }
   
   first_block = offset / block_size;
   last_block = (offset + len) / block_size;

   return ((offset + len) % block_size != 0 && (first_block != last_block || offset % block_size == 0));
}

// set up reads to unaligned blocks.  *dirty_blocks must NOT contain the unaligned block information yet.
// return the number of bytes that will be read on success, and put the block structure into *dirty_blocks.
//    also, put the head and tail block sizes into *head_len and *tail_len, respectively.
// return -ENOMEM on OOM
// return -EINVAL if we don't have block info in the inode's block manifest for the unaligned blocks 
// NOTE: inode->entry must be read-locked 
int UG_read_unaligned_setup( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks, uint64_t* head_len, uint64_t* tail_len ) {
   
   int rc = 0;
   int num_read = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   char* buf = NULL;
   uint64_t read_size = 0;
   uint64_t first_block = 0;
   uint64_t last_block = 0; 

   if( offset + buf_len > UG_inode_size( inode ) ) {
      // will hit EOF, so think of the read as reading to the end but no further 
      buf_len = UG_inode_size( inode ) - offset;
   }
   
   first_block = offset / block_size;
   last_block = (offset + buf_len) / block_size;

   // scratch area for fetching blocks 
   UG_dirty_block_map_t unaligned_blocks;
   
   // is the first block unaligned?
   // if( (offset % block_size) != 0 ) {
   if( UG_read_has_unaligned_head( offset, block_size ) ) {
      
      // head is unaligned 
      // make a head buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }
      
      // set up the request 
      rc = UG_read_setup_block_buffer( inode, first_block, buf, block_size, &unaligned_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf ); 
         UG_dirty_block_map_free( &unaligned_blocks );
         
         return rc;
      }
      
      buf = NULL;

      read_size = MIN(
                     UG_inode_size( inode ) - offset, 
                     block_size - (offset % block_size)
                  );


      *head_len = read_size;

      num_read += MIN( read_size, buf_len );
      SG_debug("Read unaligned HEAD block %" PRIu64 " (%" PRIu64 " bytes)\n", first_block, read_size );
   }
   
   // is the last block unaligned, and if so, is it either 
   // distinct from the first block, or if they're the same,
   // does the read start at a block boundary but not finish on one?
   // if( (offset + buf_len) % block_size != 0 && 
   //    (first_block != last_block || 
   //     offset % block_size == 0) ) {
   if( UG_read_has_unaligned_tail( offset, buf_len, UG_inode_size( inode ), block_size ) ) {
      
      // tail unaligned 
      // make a tail buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }
     
      // set up the tail request 
      rc = UG_read_setup_block_buffer( inode, last_block, buf, block_size, &unaligned_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf );
         UG_dirty_block_map_free( &unaligned_blocks );
         return rc;
      }
      
      buf = NULL;

      read_size = (offset + buf_len) % block_size;
      *tail_len = read_size;

      num_read += read_size;
      SG_debug("Read unaligned TAIL block %" PRIu64 " (%" PRIu64 " bytes)\n", last_block, read_size );
   }
   
   // transfer data over to the dirty_blocks set
   for( UG_dirty_block_map_t::iterator itr = unaligned_blocks.begin(); itr != unaligned_blocks.end(); ) {
      
      try {
         (*dirty_blocks)[ itr->first ] = itr->second;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         
         UG_dirty_block_map_free( &unaligned_blocks );
         break;
      }
      
      UG_dirty_block_map_t::iterator old_itr = itr;
      itr++;
      
      unaligned_blocks.erase( old_itr );
   }
   
   return num_read;
}


// set up reads to aligned blocks, in a zero-copy manner.  *dirty_blocks must NOT contain the aligned block information yet.
// return the number of bytes to read on success, and put the block structure into *dirty_blocks
// return -EINVAL if we don't have block info in the inode's block manifest for the aligned blocks 
// return -errno on failure 
// NOTE: inode->entry must be read-locked 
int UG_read_aligned_setup( struct UG_inode* inode, char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   
   uint64_t start_block_id = 0;
   uint64_t end_block_id = 0;
   off_t aligned_offset = 0;            // offset into buf where the first aligned block starts
   off_t last_block_overflow = 0;
   int num_read = 0;

   UG_dirty_block_aligned( offset, buf_len, block_size, &start_block_id, &end_block_id, &aligned_offset, &last_block_overflow );
   if( (unsigned)last_block_overflow == block_size ) {

      // the last block can be treated as aligned
      end_block_id ++;
   }
   
   // make blocks 
   for( uint64_t block_id = start_block_id; block_id <= end_block_id; block_id++ ) {
      
      struct UG_dirty_block dirty_block;
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), block_id );
      uint64_t read_offset = aligned_offset + (block_id - start_block_id) * block_size;
      uint64_t read_len = 0;

      if( block_id * block_size >= UG_inode_size( inode ) ) {
         // EOF 
         SG_debug("Skip block %" PRIu64 ", it is beyond EOF\n", block_id);
         continue;
      }

      // skip partials 
      if( dirty_blocks->find( block_id ) != dirty_blocks->end() ) {
         SG_debug("Already filled in %" PRIu64 "\n", block_id );
         continue;
      }
      
      if( read_offset + block_size >= buf_len ) {
         read_len = buf_len - read_offset;
      }
      else {
         read_len = block_size;
      }
      
      num_read += read_len;

      if( block_info == NULL ) {
        
         SG_debug("Read aligned write-hole block %" PRIu64 "\n", block_id );

         // this is a write hole 
         // satisfy this read immediately
         memset( buf + read_offset, 0, block_size );
         continue;
      }
      
      SG_debug("Read aligned block %" PRIu64 " (%" PRIu64 " bytes)\n", block_id, read_len );

      // set up this dirty block 
      rc = UG_dirty_block_init_ram_nocopy( &dirty_block, block_info, buf + read_offset, block_size );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_init_nocopy( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), block_id, block_info->block_version, rc );
         
         break;
      }
      
      // put it in place 
      try {
         
         (*dirty_blocks)[ block_id ] = dirty_block;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         break;
      }
   }
   
   return num_read;
}


// get the list of gateways to download from 
// return 0 on success, and set *gateway_ids and *num_gateway_ids
// return -ENOMEM on OOM
int UG_read_download_gateway_list( struct SG_gateway* gateway, uint64_t coordinator_id, uint64_t** ret_gateway_ids, size_t* ret_num_gateway_ids ) {
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   int rc = 0;
   uint64_t coordinator_type = 0;

   uint64_t* gateway_ids = NULL;
   size_t num_gateway_ids = 0;

   coordinator_type = ms_client_get_gateway_type( ms, coordinator_id );
   
   // what are the RGs?
   rc = UG_state_list_replica_gateway_ids( ug, &gateway_ids, &num_gateway_ids );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   // if the coordinator is an AG, then try it first
   if( coordinator_type == SYNDICATE_AG ) {

      SG_debug("Gateway %" PRIu64 " is an AG\n", coordinator_id );
      
      uint64_t* tmp = SG_CALLOC( uint64_t, num_gateway_ids + 1 );
      if( tmp == NULL ) {
         return -ENOMEM;
      }

      tmp[0] = coordinator_id;
      memcpy( &tmp[1], gateway_ids, num_gateway_ids * sizeof(uint64_t) );

      SG_safe_free( gateway_ids );
      gateway_ids = tmp;
      num_gateway_ids = num_gateway_ids + 1;
   }

   *ret_gateway_ids = gateway_ids;
   *ret_num_gateway_ids = num_gateway_ids;

   return 0;
}


// download multiple blocks at once.
// return 0 on success, and populate *blocks and *num_blocks with the blocks requested in the block_requests manifest.
// return -EINVAL if blocks has reserved chunk data that is unallocated, or does not have enough space
// return -ENOMEM on OOM 
// return -errno on failure to download
int UG_read_download_blocks( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* block_requests, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t* gateway_ids = NULL;
   size_t num_gateway_ids = 0;
   
   struct md_download_context* dlctx = NULL;
   struct md_download_loop* dlloop = NULL;
   
   struct SG_request_data reqdat;
   
   uint64_t block_id = 0;
   uint64_t next_block_id = 0;

   struct SG_chunk next_block;
   struct SG_manifest_block* block_info = NULL;

   UG_block_gateway_map_t block_gateway_idx;        // map block ID to the index into gateway_ids for the next gateway to try
   int gateway_idx = 0;
   SG_manifest_block_iterator itr;
   uint64_t block_size = ms_client_get_volume_blocksize( ms );

   set<uint64_t> blocks_in_flight;                  // set of blocks being downloaded right now (keyed by ID)
   bool cycled_through = false;                     // set to true once the maximum number of in-flight blocks has been started

   memset( &next_block, 0, sizeof(struct SG_chunk) );
   
   next_block.len = block_size;
   next_block.data = SG_CALLOC( char, block_size );
   if( next_block.data == NULL ) {
      return -ENOMEM;
   }
   
   // sanity check--every block in blocks must be RAM-mapped to the reader's buffer
   for( UG_dirty_block_map_t::iterator block_itr = blocks->begin(); block_itr != blocks->end(); block_itr++ ) {
      
      if( !UG_dirty_block_in_RAM( &block_itr->second ) ) {

         SG_error("BUG: block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] not RAM-mapped\n",
               SG_manifest_get_file_id( block_requests ), UG_dirty_block_id( &block_itr->second ), UG_dirty_block_version( &block_itr->second ) );

         exit(1);
      }

      // need a full buffer
      if( (unsigned)UG_dirty_block_buf( &block_itr->second )->len < block_size ) {
         SG_error("BUG: block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] has insufficient space (%zu)\n",
               SG_manifest_get_file_id( block_requests ), UG_dirty_block_id( &block_itr->second ), UG_dirty_block_version( &block_itr->second ), (size_t)UG_dirty_block_buf( &block_itr->second )->len );

         exit(1);
      }
   }
   
   // what are the gateways?
   rc = UG_read_download_gateway_list( gateway, SG_manifest_get_coordinator( block_requests ), &gateway_ids, &num_gateway_ids );
   if( rc != 0 ) {
      
      // OOM
      SG_chunk_free( &next_block );
      return rc;
   }
   
   // seed block <--> gateway index
   for( SG_manifest_block_iterator seed_itr = SG_manifest_block_iterator_begin( block_requests ); seed_itr != SG_manifest_block_iterator_end( block_requests ); seed_itr++ ) {
     
      try {
         block_id = SG_manifest_block_iterator_id( seed_itr );
         block_gateway_idx[ block_id ] = 0;
      }
      catch( bad_alloc& ba ) {
         
         SG_safe_free( gateway_ids ); 
         SG_chunk_free( &next_block );
         return -ENOMEM;
      }
   }
   
   // prepare to download blocks 
   dlloop = md_download_loop_new();
   if( dlloop == NULL ) {
      SG_safe_free( gateway_ids );
      SG_chunk_free( &next_block );
      return -ENOMEM;
   }

   rc = md_download_loop_init( dlloop, SG_gateway_dl( gateway ), MIN( (unsigned)ms->max_connections, SG_manifest_get_block_count( block_requests ) ) );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_init rc = %d\n", rc );
      SG_safe_free( dlloop );
      SG_safe_free( gateway_ids );
      SG_chunk_free( &next_block );
   
      return rc;
   }
   
   itr = SG_manifest_block_iterator_begin( block_requests );

   // download each block 
   do {
      
      // start as many downloads as we can 
      while( block_gateway_idx.size() > 0 ) {
        
         // cycle through the manifest of blocks to fetch 
         if( itr == SG_manifest_block_iterator_end( block_requests ) ) {
            itr = SG_manifest_block_iterator_begin( block_requests );

            if( cycled_through ) {
               // all download slots are filled 
               cycled_through = false;
               break;
            }

            cycled_through = true;
         }
         
         block_id = SG_manifest_block_iterator_id( itr );
         block_info = SG_manifest_block_iterator_block( itr );
         
         // did we get this block already?
         if( block_gateway_idx.find( block_id ) == block_gateway_idx.end() ) {
            
            itr++;
            continue;
         }

         // are we getting this block already?
         if( blocks_in_flight.count( block_id ) > 0 ) {

            itr++;
            continue;
         }

         // have we tried each gateway?
         if( (unsigned)(block_gateway_idx[ block_id ]) >= num_gateway_ids ) {

            SG_error("Tried all RGs for block %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", SG_manifest_get_file_id( block_requests ), block_id, SG_manifest_block_version( block_info ) );
            rc = -ENODATA;
            break;
         }
         
         // next block download 
         rc = md_download_loop_next( dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // next gateway 
         gateway_idx = block_gateway_idx[ block_id ];
         
         // start this block
         rc = SG_request_data_init_block( gateway, fs_path, block_requests->file_id, block_requests->file_version, block_id, SG_manifest_block_version( block_info ), &reqdat );
         if( rc != 0 ) {
            break;
         }

         rc = SG_client_get_block_async( gateway, &reqdat, gateway_ids[ gateway_idx ], dlloop, dlctx );
         SG_request_data_free( &reqdat );

         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               // gateway ID is not found--we should reload the cert bundle 
               SG_gateway_start_reload( gateway );
            }

            SG_error("SG_client_get_block_async( %" PRIu64 " ) rc = %d\n", gateway_ids[ gateway_idx ], rc );
            break;
         }
         
         try {
         
             // next block
             itr++;

             // next gateway for this block
             block_gateway_idx[ block_id ]++;
         
             // in-flight!
             blocks_in_flight.insert( block_id );
         }
         catch( bad_alloc& ba ) {
             rc = -ENOMEM;
             break;
         }

         SG_debug("Will download %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", block_requests->file_id, block_id, SG_manifest_block_version( block_info ) );

         // started at least one block; try to start more 
         cycled_through = false;
      }
     
      if( rc != 0 ) {
         break;
      }
      
      // wait for at least one of the downloads to finish 
      rc = md_download_loop_run( dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // find the finished downloads.  check at least once.
      while( true ) {
         
         // next finished download
         rc = md_download_loop_finished( dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // out of finished downloads 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finished rc = %d\n", rc );
            break;
         }
         
         // process the block and free up the download handle
         memset( next_block.data, 0, next_block.len ); 
         rc = SG_client_get_block_finish( gateway, block_requests, dlctx, &next_block_id, &next_block );
         if( rc != 0 ) {
            
            SG_error("SG_client_get_block_finish rc = %d\n", rc );
            break;
         }
         
         // copy the data in
         // NOTE: we do not emplace the data, since this method is used to directly copy
         // downloaded data into a client reader's read buffer
         SG_chunk_copy( &(*blocks)[ next_block_id ].buf, &next_block );
        
         try { 
             // finished this block 
             block_gateway_idx.erase( next_block_id );
             blocks_in_flight.erase( next_block_id );
         }
         catch( bad_alloc& ba ) {
             rc = -ENOMEM;
             break;
         }

         SG_debug("Downloaded block %" PRIu64 "\n", next_block_id );
      }
      
      if( rc != 0 ) {
         break;
      }
      
   } while( md_download_loop_running( dlloop ) );
   
   // failure?
   if( rc != 0 ) {
      
      md_download_loop_abort( dlloop );
      rc = -EIO;
   }
    
   SG_client_get_block_cleanup_loop( dlloop );
   SG_client_download_async_cleanup_loop( dlloop );
   md_download_loop_free( dlloop );
   SG_safe_free( dlloop );
   
   SG_safe_free( gateway_ids );
   SG_chunk_free( &next_block );
   
   // blocks is (partially) populated with chunks 
   return rc;
}


// read a set of blocks from the cache, but optionally keep a tally of those that were *not* cached
// every block in *blocks should be mapped to the read buffer
// return 0 on success, and populate *blocks with the requested data and optionally *absent with data we didn't find.
// return -ENOMEM on OOM
// return -EINVAL if we're missing a block 
// NOTE: each block in blocks must be pre-allocated 
int UG_read_cached_blocks( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* block_requests, UG_dirty_block_map_t* blocks, uint64_t offset, uint64_t len, struct SG_manifest* absent ) {
   
   int rc = 0;
   struct SG_IO_hints io_hints;
   struct UG_dirty_block* dirty_block = NULL;
   
   // verify that all non-dirty block buffers exist and are mapped (sanity check)
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests ); itr != SG_manifest_block_iterator_end( block_requests ); itr++ ) {
      
      if( blocks->find( SG_manifest_block_iterator_id( itr ) ) == blocks->end() ) {
         
         // unaccounted for 
         SG_error("BUG: missing block %" PRIX64 "[%" PRIu64 "]\n", SG_manifest_get_file_id( block_requests ), SG_manifest_block_iterator_id( itr ) );
         exit(1);
      }
      
      if( !UG_dirty_block_in_RAM( &(*blocks)[ SG_manifest_block_iterator_id( itr ) ] ) ) {
         
         // block is not mapped to read buffer
         SG_error("BUG: not mapped to RAM: %" PRIX64 "[%" PRIu64 "]\n", SG_manifest_get_file_id( block_requests ), SG_manifest_block_iterator_id( itr ) );
         exit(1);
      }
   }

   // hints to the driver as to what these requests will entail
   SG_IO_hints_init( &io_hints, SG_IO_READ, offset, len );
   
   // find all cached blocks...
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests ); itr != SG_manifest_block_iterator_end( block_requests ); itr++ ) {
      
      dirty_block = &(*blocks)[ SG_manifest_block_iterator_id( itr ) ];
    
      // NOTE: this will pass the block through the deserialize driver 
      rc = UG_dirty_block_load_from_cache( gateway, fs_path, block_requests->file_id, block_requests->file_version, dirty_block, &io_hints );
      
      if( rc != 0 ) {
         
         if( rc != -ENOENT ) {
            SG_error("UG_dirty_block_load_from_cache( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                     SG_manifest_get_file_id( block_requests ), SG_manifest_get_file_version( block_requests ), UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

         }
         
         rc = 0;
         
         if( absent != NULL ) {
               
            // not cached. note it.
            struct SG_manifest_block absent_block_info;
            
            rc = SG_manifest_block_dup( &absent_block_info, &itr->second );
            if( rc != 0 ) {
               
               // OOM
               SG_error("SG_manifest_block_dup rc = %d\n", rc ); 
               break;
            }
            
            rc = SG_manifest_put_block_nocopy( absent, &absent_block_info, true );
            if( rc != 0 ) {
               
               SG_manifest_block_free( &absent_block_info );
               
               // OOM 
               SG_error("SG_manifest_put_block_nocopy rc = %d\n", rc ); 
               break;
            }

            memset( &absent_block_info, 0, sizeof(struct SG_manifest_block) );
            SG_debug("Block not cached: %" PRIu64 "\n", UG_dirty_block_id( dirty_block ) );
         }
      }
      else {

         SG_debug("Read cached block %" PRIu64 "\n", UG_dirty_block_id( dirty_block ) );
      }
   }
   
   return rc;
}


// read a set of blocks from an inode's dirty blocks set, but optionally keep a tally of those that were *not* available in said set.
// return 0 on success, and populate *blocks with the requested data and optionally *absent with the data we didn't find.
// return -ENOMEM on OOM.
// NOTE: inode must be read-locked
int UG_read_dirty_blocks( struct SG_gateway* gateway, struct UG_inode* inode, UG_dirty_block_map_t* blocks, struct SG_manifest* absent ) {
   
   int rc = 0;

   for( UG_dirty_block_map_t::iterator itr = blocks->begin(); itr != blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;

      // in RAM and present in the dirty block set?
      if( UG_dirty_block_in_RAM( &itr->second ) && UG_inode_dirty_blocks( inode )->find( block_id ) != UG_inode_dirty_blocks( inode )->end() ) {
         
         // copy it over!
         SG_debug("Read block %" PRIu64 " from in-RAM dirty block cache\n", block_id );
         SG_chunk_copy( &(*blocks)[ block_id ].buf, &(*UG_inode_dirty_blocks( inode ))[ block_id ].buf );
      }
      
      else if( absent != NULL ) {
         
         // absent.  note it.
         struct SG_manifest_block absent_block_info;
         
         rc = SG_manifest_block_dup( &absent_block_info, UG_dirty_block_info( &itr->second ) );
         if( rc != 0 ) {
            
            // OOM 
            break;
         }
         
         rc = SG_manifest_put_block_nocopy( absent, &absent_block_info, true );
         if( rc != 0 ) {
            
            // OOM
            SG_error("SG_manifest_put_block_nocopy rc = %d\n", rc );
            SG_manifest_block_free( &absent_block_info ); 
            break;
         }
            
         memset( &absent_block_info, 0, sizeof(struct SG_manifest_block) );
         SG_debug("Block not dirty: %" PRIu64 "\n", block_id );
      }
   }
   
   return rc;
}

// read locally-available blocks 
// try the inode's dirty blocks, and then disk cached blocks 
// return 0 on success, and fill in *blocks on success
// return -ENOMEM on OOM 
// NOTE: inode->entry must be read-locked!
int UG_read_blocks_local( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks, uint64_t offset, uint64_t len, struct SG_manifest* blocks_not_local ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   struct SG_manifest blocks_not_dirty;
   uint64_t head_id = (offset / block_size);
   uint64_t tail_id = ((offset + len) / block_size);
   
   rc = SG_manifest_init( &blocks_not_dirty, UG_inode_volume_id( inode ), UG_inode_coordinator_id( inode ), UG_inode_file_id( inode ), UG_inode_file_version( inode ) );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // try dirty blocks
   rc = UG_read_dirty_blocks( gateway, inode, blocks, &blocks_not_dirty );
   if( rc != 0 ) {
      
      SG_error("UG_read_dirty_blocks( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), rc );
      
      SG_manifest_free( &blocks_not_dirty );
      return rc;
   }
   
   // done?
   if( SG_manifest_get_block_count( &blocks_not_dirty ) == 0 ) {
      
      SG_manifest_free( &blocks_not_dirty );
      return 0;
   }
   
   // try cached blocks 
   rc = UG_read_cached_blocks( gateway, fs_path, &blocks_not_dirty, blocks, offset, len, blocks_not_local );
   SG_manifest_free( &blocks_not_dirty );
   
   if( rc != 0 ) {
      
      SG_error("UG_read_cached_blocks( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), rc );
   }
  
   // if we have write-holes at the head or tail, remove them from blocks_not_local (they'bve already been satisfied) 
   if( !SG_manifest_is_block_present( UG_inode_manifest( inode ), head_id ) && UG_read_has_unaligned_head( offset, block_size ) ) {

      // already filled in 
      SG_debug("Will not download unaligned head/write-hole %" PRIu64 "\n", head_id );
      SG_manifest_delete_block( blocks_not_local, head_id );
   }
   if( !SG_manifest_is_block_present( UG_inode_manifest( inode ), tail_id ) && UG_read_has_unaligned_tail( offset, len, UG_inode_size( inode ), block_size ) ) {

      // already filled in 
      SG_debug("Will not download unaligned tail/write-hole %" PRIu64 "\n", tail_id );
      SG_manifest_delete_block( blocks_not_local, tail_id );
   }

   return rc;
}


// read remotely-available blocks, trying first the remote coordinator (if needed), and then all replica gateways.
// NOTE: this consumes the contents of blocks_not_local.  the caller can call this method repeatedly to retry on failure.
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on download error 
int UG_read_blocks_remote( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* blocks_not_local, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;

   rc = UG_read_download_blocks( gateway, fs_path, blocks_not_local, blocks );
   if( rc != 0 ) {
      
      SG_error("UG_read_download_blocks( '%s' (%" PRIX64 ".%" PRId64 ") ) rc = %d\n", fs_path, SG_manifest_get_file_id( blocks_not_local ), SG_manifest_get_file_version( blocks_not_local ), rc );
      return rc;
   }
   
   // clear out satisfied requests 
   for( UG_dirty_block_map_t::iterator itr = blocks->begin(); itr != blocks->end(); itr++ ) {
      
      SG_manifest_delete_block( blocks_not_local, itr->first );
   }
   
   return rc;
}


// read a set of blocks into RAM, given by the already-set-up *blocks
// try the inode's dirty blocks, then the cached blocks, and finally download any that were not in the cache from remote_gateway_ids, trying each gateway in sequence.
// return 0 on success, and fill in *blocks on success 
// return -ENOMEM on OOM 
// NOTE: the caller must still free blocks, even if this method fails, since this method tries to get even partial data
// NOTE: inode->entry must be at least read-locked!
int UG_read_blocks( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks, uint64_t offset, uint64_t len ) {
   
   int rc = 0;
   struct SG_manifest blocks_to_download;   // coordinator set to inode's listed coordinator
   uint64_t max_block_id = 0;
   uint64_t min_block_id = (uint64_t)(-1);
   
   // convert *blocks to a manifest, for tracking purposes 
   rc = SG_manifest_init( &blocks_to_download, UG_inode_volume_id( inode ), UG_inode_coordinator_id( inode ), UG_inode_file_id( inode ), UG_inode_file_version( inode ) );
   if( rc != 0 ) {
      
      // OOM 
      return rc;
   }
 
   // fetch local 
   rc = UG_read_blocks_local( gateway, fs_path, inode, blocks, offset, len, &blocks_to_download );
   if( rc != 0 ) {
      
      SG_error("UG_read_blocks_local( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
               UG_inode_file_id( inode ), UG_inode_file_version( inode ), min_block_id, max_block_id, rc );
     
      goto UG_read_blocks_end; 
   }
   
   // anything left to fetch rmeotely?
   if( SG_manifest_get_block_count( &blocks_to_download ) > 0 ) {
      
      // fetch remote 
      rc = UG_read_blocks_remote( gateway, fs_path, &blocks_to_download, blocks );
      if( rc != 0 ) {
         
         SG_error("UG_read_blocks_remote( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), min_block_id, max_block_id, rc );
   
         goto UG_read_blocks_end;
      }
   }

UG_read_blocks_end: 
   SG_manifest_free( &blocks_to_download );
   return rc;
}


// fskit route to read data from a file 
// return -errno on failure 
// fent should not be locked
int UG_read_impl( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data ) {
   
   int rc = 0;
   
   struct UG_file_handle* fh = (struct UG_file_handle*)handle_data;
   struct UG_inode* inode = fh->inode_ref;
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   char const* fs_path = fskit_route_metadata_get_path( route_metadata );
   
   uint64_t file_id = 0;
   int64_t file_version = 0;
   uint64_t coordinator_id = 0;
   int64_t write_nonce = 0;
   int64_t num_read = 0;

   bool have_unaligned_head = false;
   bool have_unaligned_tail = false;
   uint64_t head_len = 0;
   uint64_t tail_len = 0;
   uint64_t first_block = 0;
   uint64_t last_block = 0;
   uint64_t file_size = 0;
   uint64_t copy_len = 0;
   uint64_t copy_at = 0;
   uint64_t buf_len_eof = 0;
   
   struct SG_chunk* head_buf = NULL;
   struct SG_chunk* tail_buf = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   UG_dirty_block_map_t read_blocks;
   UG_dirty_block_map_t unaligned_blocks;
   
   struct UG_dirty_block* last_block_read = NULL;
   UG_dirty_block_map_t::iterator last_block_read_itr;
   
   struct SG_manifest blocks_to_download;
   memset( &blocks_to_download, 0, sizeof(struct SG_manifest) );

   SG_debug("Read %s offset %jd length %zu\n", fs_path, offset, buf_len );
   if( buf_len == 0 ) {
      return 0;
   }

   // make sure the inode is fresh
   rc = UG_consistency_inode_ensure_fresh( gateway, fs_path, inode );
   if( rc < 0 ) {
     SG_error("UG_consistency_inode_ensure_fresh('%s') rc = %d\n", fs_path, rc );
     return rc;
   }

   // make sure the manifest is fresh
   rc = UG_consistency_manifest_ensure_fresh( gateway, fs_path );
   
   fskit_entry_rlock( fent );
   
   file_id = UG_inode_file_id( inode );
   file_version = UG_inode_file_version( inode );
   coordinator_id = UG_inode_coordinator_id( inode );
   write_nonce = UG_inode_write_nonce( inode );
   file_size = UG_inode_size( inode );

   if( file_size > 0 ) {
       first_block = offset / file_size;
   }
   else {
       first_block = 0;
   }

   last_block = MIN( file_size / block_size, (offset + buf_len) / block_size);
   
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_consistency_manifest_ensure_fresh( %" PRIX64 " ('%s')) rc = %d\n", file_id, fs_path, rc );
      return rc;
   }
  
   // sanity check: can't exceed file size 
   if( (unsigned)offset >= UG_inode_size( inode ) ) {

      // EOF 
      fskit_entry_unlock( fent );
      SG_debug("EOF on %" PRIX64 "\n", file_id );
      return 0;
   }

   // set of blocks to download
   rc = SG_manifest_init( &blocks_to_download, volume_id, coordinator_id, file_id, file_version );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("SG_manifest_init rc = %d\n", rc );
      
      return rc;
   }
   
   // get unaligned blocks 
   rc = UG_read_unaligned_setup( gateway, fs_path, inode, buf_len, offset, &read_blocks, &head_len, &tail_len );
   if( rc < 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_read_unaligned_setup( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      SG_manifest_free( &blocks_to_download );
      return rc;
   }

   // head/tail unaligned?
   if( head_len != 0 ) {
      have_unaligned_head = true;
   }
   if( tail_len != 0 ) {
      have_unaligned_tail = true;
   }
   
   SG_debug("Unaligned read: %d bytes (head unaligned: %d, tail unaligned: %d)\n", rc, have_unaligned_head, have_unaligned_tail );
   num_read += rc;

   // set up aligned read 
   rc = UG_read_aligned_setup( inode, buf, buf_len, offset, block_size, &read_blocks );
   if( rc < 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_read_aligned_setup( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      UG_dirty_block_map_free( &read_blocks );
      
      SG_manifest_free( &blocks_to_download );
      
      return rc;
   }

   SG_debug("Aligned read: %d bytes\n", rc );
   num_read += rc;
   
   // fetch local 
   rc = UG_read_blocks_local( gateway, fs_path, inode, &read_blocks, offset, buf_len, &blocks_to_download );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_read_blocks_local( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
               file_id, file_version, (offset / block_size), ((offset + buf_len) / block_size), rc );
      
      UG_dirty_block_map_free( &read_blocks );
      
      SG_manifest_free( &blocks_to_download );
      return rc;
   }

   // don't hold the lock during network I/O
   fskit_entry_unlock( fent );

   // anything left to fetch remotely?
   if( SG_manifest_get_block_count( &blocks_to_download ) > 0 ) {
   
      // fetch remote 
      rc = UG_read_blocks_remote( gateway, fs_path, &blocks_to_download, &read_blocks );
      if( rc != 0 ) {
         
         SG_error("UG_read_blocks_remote( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
                  file_id, file_version, (offset / block_size), ((offset + buf_len) / block_size), rc );

         num_read = rc;
         goto UG_read_impl_fail;
      }
   }

   // success!
   
   // copy unaligned blocks back into the buffer 
   if( have_unaligned_head ) {

      auto head_itr = read_blocks.find( first_block );
      if( head_itr == read_blocks.end() ) {
         SG_error("BUG: head block %" PRIu64 " is missing\n", first_block );
         exit(1);
      }

      copy_len = head_len;
      if( buf_len < head_len ) {
         copy_len = buf_len;
      }

      SG_debug("Copy unaligned head %" PRIu64 " at %" PRIu64 " (%" PRIu64 " bytes)\n", first_block, (uint64_t)offset, copy_len );

      head_buf = UG_dirty_block_buf( &head_itr->second );
      memcpy( buf, head_buf->data + block_size - head_len, copy_len );
   }

   if( have_unaligned_tail ) {

      buf_len_eof = buf_len;
      if( offset + buf_len > file_size ) {
         buf_len_eof = file_size - offset;
      }

      auto tail_itr = read_blocks.find( last_block );
      if( tail_itr == read_blocks.end() ) {
         SG_error("BUG: tail block %" PRIu64 " is missing\n", last_block );
         exit(1);
      }
      
      if( tail_len < buf_len_eof ) {
         copy_at = buf_len_eof - tail_len;
      }
      else {
         copy_at = 0;
      }

      SG_debug("Copy unaligned tail %" PRIu64 " at %" PRIu64 " (%" PRIu64 " bytes); buf_len_eof = %" PRIu64 "\n", last_block, copy_at, tail_len, buf_len_eof );

      tail_buf = UG_dirty_block_buf( &tail_itr->second );
      memcpy( buf + copy_at, tail_buf->data, tail_len );
   }

   // optimization: cache last read block, but only if no writes occurred while we were fetching remote blocks
   fskit_entry_wlock( fent );
   
   if( file_version == UG_inode_file_version( inode ) && write_nonce == UG_inode_write_nonce( inode ) ) {
      
      last_block_read_itr = read_blocks.find( last_block );
      if( last_block_read_itr != read_blocks.end() ) {
         
         last_block_read = &last_block_read_itr->second;
         
         // remember to evict this block when we close 
         UG_file_handle_evict_add_hint( fh, last_block, UG_dirty_block_version( last_block_read ) );
         
         // cache this block
         rc = UG_inode_dirty_block_put( gateway, inode, last_block_read, false );
         if( rc != 0 ) {
            
            // not fatal, but annoying...
            SG_error("UG_inode_dirty_block_put( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_get_path( route_metadata ), buf_len, offset, rc );
            rc = 0;
         }

         read_blocks.erase( last_block_read_itr ); 
      }
   }
   
   fskit_entry_unlock( fent );

UG_read_impl_fail:

   UG_dirty_block_map_free( &read_blocks );
   
   SG_manifest_free( &blocks_to_download );
   
   return num_read;
}

