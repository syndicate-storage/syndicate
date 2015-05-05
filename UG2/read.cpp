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

// track which gateway to download a given block from
typedef map< uint64_t, int > UG_block_gateway_map_t;

// set up a manifest and dirty block map to receive a block into a particular buffer 
// the block put into *blocks takes ownership of buf, so the caller must not free it
// NOTE: buf must be at least the size of a volume block 
// return 0 on success
// return -ENOMEM on OOM 
int UG_read_setup_block_buffer( struct UG_inode* inode, uint64_t block_id, char* buf, uint64_t buf_len, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;

   struct UG_dirty_block block_data;
   struct SG_manifest_block* block_info = NULL;
   
   // look up this block's info from the manifest 
   block_info = SG_manifest_block_lookup( &inode->manifest, block_id );
   if( block_info == NULL ) {
      
      return -EINVAL;
   }
   
   // generate the dirty block 
   rc = UG_dirty_block_init_ram_nocopy( &block_data, block_info, buf, buf_len );
   if( rc != 0 ) {
      
      return rc;
   }
   
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


// set up reads to unaligned blocks, in a zero-copy manner.  *dirty_blocks must NOT contain the unaligned block information yet.
// return 0 on success, and put the block structure into *dirty_blocks
// return -EINVAL if we don't have block info in the inode's block manifest for the unaligned blocks 
// return -errno on failure 
// NOTE: inode->entry must be read-locked 
int UG_read_unaligned_setup( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   uint64_t block_id = 0;
   char* buf = NULL;
   
   // scratch area for fetching blocks 
   UG_dirty_block_map_t unaligned_blocks;
   
   // is the first block unaligned?
   if( offset > 0 && (offset % block_size) != 0 ) {
      
      // head is unaligned 
      block_id = offset / block_size;
      
      // make a head buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }
      
      // set up the request 
      rc = UG_read_setup_block_buffer( inode, block_id, buf, block_size, &unaligned_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf );
         
         UG_dirty_block_map_free( &unaligned_blocks );
         
         return rc;
      }
      
      buf = NULL;
   }
   
   // is the last block unaligned, and distinct from the head?
   if( (offset + buf_len) > 0 && (((offset + buf_len) % block_size) != 0 && ((offset + buf_len) / block_size) != (offset / block_size)) ) {
      
      // tail is distinct and unaligned 
      block_id = (offset + buf_len) / block_size;
      
      // make a tail buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }
      
      // set up the tail request 
      rc = UG_read_setup_block_buffer( inode, block_id, buf, block_size, &unaligned_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf );
         
         UG_dirty_block_map_free( &unaligned_blocks );
         
         return rc;
      }
      
      buf = NULL;
   }
   
   // transfer data over 
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
   
   return 0;
}


// set up reads to aligned blocks, in a zero-copy manner.  *dirty_blocks must NOT contain the aligned block information yet.
// return 0 on success, and put the block structure into *dirty_blocks
// return -EINVAL if we don't have block info in the inode's block manifest for the aligned blocks 
// return -errno on failure 
// NOTE: inode->entry must be read-locked 
int UG_read_aligned_setup( struct UG_inode* inode, char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   
   uint64_t start_block_id = 0;
   uint64_t end_block_id = 0;
   off_t aligned_offset = 0;            // offset into buf where the first aligned block starts
   
   UG_dirty_block_aligned( offset, buf_len, block_size, &start_block_id, &end_block_id, &aligned_offset );
   
   // make blocks 
   for( uint64_t block_id = start_block_id; block_id <= end_block_id; block_id++ ) {
      
      struct UG_dirty_block dirty_block;
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( &inode->manifest, block_id );
      
      if( block_info == NULL ) {
         
         // this is a write hole 
         // satisfy this read immediately 
         memset( buf + aligned_offset + (block_id - start_block_id) * block_size, 0, block_size );
         continue;
      }
      
      // set up this dirty block 
      rc = UG_dirty_block_init_ram_nocopy( &dirty_block, block_info, buf + aligned_offset + (block_id - start_block_id) * block_size, block_size );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_init_nocopy( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                  UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), block_id, block_info->block_version, rc );
         
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
   
   return 0;
}


// get the list of gateways to download from 
// return 0 on success, and set *gateway_ids and *num_gateway_ids
// return -ENOMEM on OOM
int UG_read_download_gateway_list( struct SG_gateway* gateway, uint64_t coordinator_id, uint64_t** gateway_ids, size_t* num_gateway_ids ) {
   
   uint64_t* rg_ids = NULL;
   size_t num_rgs = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   int rc = 0;
   
   // what are the RGs?
   rc = UG_state_list_replica_gateway_ids( ug, &rg_ids, &num_rgs );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   uint64_t* ret = SG_CALLOC( uint64_t, num_rgs + 1 );
   if( ret == NULL ) {
      
      // OOM 
      SG_safe_free( rg_ids );
      return rc;
   }
   
   ret[0] = coordinator_id;
   memcpy( ret + 1, rg_ids, num_rgs * sizeof(uint64_t) );
   
   *gateway_ids = ret;
   *num_gateway_ids = num_rgs + 1;
   
   return 0;
}


// download multiple blocks at once.  Start from the coordinator, then try all RGs
// return 0 on success, and populate *blocks and *num_blocks with the blocks requested in the block_requests manifest.
// return -EINVAL if blocks has reserved chunk data that is unallocated, or does not have enough space
// return -ENOMEM on OOM 
// return -errno on failure to download
int UG_read_download_blocks( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* block_requests, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;
   uint64_t gateway_id = SG_gateway_id( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t* gateway_ids = NULL;
   size_t num_gateway_ids = 0;
   
   struct md_download_context* dlctx = NULL;
   struct md_download_loop dlloop;
   
   struct SG_request_data reqdat;
   
   uint64_t next_block_id = 0;
   struct SG_chunk next_block;
   
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   UG_block_gateway_map_t block_downloads;
   
   SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests );
   
   // sanity check--every block in blocks must be allocated
   for( UG_dirty_block_map_t::iterator block_itr = blocks->begin(); block_itr != blocks->end(); block_itr++ ) {
      
      if( UG_dirty_block_buf( block_itr->second ).data == NULL ) {
         return -EINVAL;
      }
      if( (unsigned)UG_dirty_block_buf( block_itr->second ).len < block_size ) {
         return -EINVAL;
      }
   }
   
   // what are the gateways?
   rc = UG_read_download_gateway_list( gateway, SG_manifest_get_coordinator( block_requests ), &gateway_ids, &num_gateway_ids );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   // seed block downloads 
   for( SG_manifest_block_iterator seed_itr = SG_manifest_block_iterator_begin( block_requests ); seed_itr != SG_manifest_block_iterator_end( block_requests ); seed_itr++ ) {
      
      try {
         block_downloads[ SG_manifest_block_iterator_id( seed_itr ) ] = 0;
      }
      catch( bad_alloc& ba ) {
         
         SG_safe_free( gateway_ids );
         return -ENOMEM;
      }
   }
   
   // prepare to download blocks 
   rc = md_download_loop_init( &dlloop, SG_gateway_dl( gateway ), MIN( (unsigned)ms->max_connections, block_requests->blocks->size() ) );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_init rc = %d\n", rc );
      SG_safe_free( gateway_ids );
   
      return rc;
   }
   
   // download each block 
   do {
      
      // start as many downloads as we can 
      while( block_downloads.size() > 0 ) {
         
         if( itr == SG_manifest_block_iterator_end( block_requests ) ) {
            itr = SG_manifest_block_iterator_begin( block_requests );
         }
         
         uint64_t block_id = SG_manifest_block_iterator_id( itr );
         
         // did we get this block already?
         if( block_downloads.find( block_id ) == block_downloads.end() ) {
            
            itr++;
            continue;
         }
         
         // next block download 
         rc = md_download_loop_next( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // next gateway 
         int gateway_idx = block_downloads[ block_id ];
         
         // next reqdat 
         SG_request_data_init_block( gateway, fs_path, block_requests->file_id, block_requests->file_version, SG_manifest_block_iterator_id( itr ), SG_manifest_block_version( SG_manifest_block_iterator_block( itr ) ), &reqdat );
         
         // start it 
         rc = SG_client_get_block_async( gateway, &reqdat, gateway_ids[ gateway_idx ], &dlloop, dlctx );
         if( rc != 0 ) {
            
            SG_error("SG_client_cert_download_async( %" PRIu64 " ) rc = %d\n", gateway_id, rc );
            break;
         }
         
         SG_request_data_free( &reqdat );
         
         // next gateway for this block
         block_downloads[ block_id ]++;
         
         // next block
         itr++;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // wait for at least one of the downloads to finish 
      rc = md_download_loop_run( &dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // find the finished downloads.  check at least once.
      while( true ) {
         
         // next finished download
         rc = md_download_loop_finished( &dlloop, &dlctx );
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
         rc = SG_client_get_block_finish( gateway, block_requests, dlctx, &next_block_id, &next_block );
         if( rc != 0 ) {
            
            SG_error("SG_client_get_block_finish rc = %d\n", rc );
            break;
         }
         
         // copy the data in
         // NOTE: we do not emplace the data, since this method is used to directly copy
         // downloaded data into a client reader's read buffer
         SG_chunk_copy( &(*blocks)[ next_block_id ].buf, &next_block );
         
         SG_chunk_free( &next_block );
         
         // finished this block 
         block_downloads.erase( next_block_id );
      }
      
      if( rc != 0 ) {
         break;
      }
      
   } while( md_download_loop_running( &dlloop ) );
   
   // failure?
   if( rc != 0 ) {
      
      md_download_loop_abort( &dlloop );
   }
   
   SG_client_download_async_cleanup_loop( &dlloop );
   md_download_loop_free( &dlloop );
   
   SG_safe_free( gateway_ids );
   
   // blocks is (partially) populated with chunks 
   return rc;
}


// read a set of blocks from the cache, but optionally keep a tally of those that were *not* cached
// return 0 on success, and populate *blocks with the requested data and optionally *absent with data we didn't find.
// return -ENOMEM on OOM 
// NOTE: each block in blocks must be pre-allocated 
int UG_read_cached_blocks( struct SG_gateway* gateway, char const* fs_path, struct SG_manifest* block_requests, UG_dirty_block_map_t* blocks, struct SG_manifest* absent ) {
   
   int rc = 0;
   struct SG_request_data reqdat;
   
   struct UG_dirty_block* dirty_block = NULL;
   
   // verify that all block buffers exist 
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests ); itr != SG_manifest_block_iterator_end( block_requests ); itr++ ) {
      
      if( blocks->find( SG_manifest_block_iterator_id( itr ) ) == blocks->end() ) {
         
         // unaccounted for 
         return -EINVAL;
      }
      
      if( (*blocks)[ SG_manifest_block_iterator_id( itr ) ].buf.data == NULL ) {
         
         // no memory 
         return -EINVAL;
      }
   }
   
   // find all cached blocks...
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( block_requests ); itr != SG_manifest_block_iterator_end( block_requests ); itr++ ) {
      
      dirty_block = &(*blocks)[ SG_manifest_block_iterator_id( itr ) ];
      
      rc = UG_dirty_block_load_from_cache( gateway, fs_path, block_requests->file_id, block_requests->file_version, dirty_block );
      
      if( rc != 0 ) {
         
         if( rc != -ENOENT ) {
            SG_error("UG_dirty_block_load_from_cache( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                     reqdat.file_id, reqdat.file_version, reqdat.block_id, reqdat.block_version, rc );
         }
         
         rc = 0;
         
         if( absent != NULL ) {
               
            // not cached. note it.
            struct SG_manifest_block absent_block_info;
            
            rc = SG_manifest_block_dup( &absent_block_info, &itr->second );
            if( rc != 0 ) {
               
               // OOM 
               break;
            }
            
            rc = SG_manifest_put_block( absent, &absent_block_info, true );
            if( rc != 0 ) {
               
               SG_manifest_block_free( &absent_block_info );
               
               // OOM 
               break;
            }
         }
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
      
      // present in the dirty block set?
      if( inode->dirty_blocks->find( block_id ) != inode->dirty_blocks->end() ) {
         
         // copy it over!
         SG_chunk_copy( &(*blocks)[ block_id ].buf, &(*inode->dirty_blocks)[ block_id ].buf );
      }
      
      else if( absent != NULL ) {
         
         // absent.  note it.
         struct SG_manifest_block absent_block_info;
         
         rc = SG_manifest_block_dup( &absent_block_info, UG_dirty_block_info( itr->second ) );
         if( rc != 0 ) {
            
            // OOM 
            break;
         }
         
         rc = SG_manifest_put_block( absent, &absent_block_info, true );
         if( rc != 0 ) {
            
            // OOM 
            break;
         }
      }
   }
   
   return rc;
}

// read locally-available blocks 
// try the inode's dirty blocks, and then disk cached blocks 
// return 0 on success, and fill in *blocks on success
// return -ENOMEM on OOM 
// NOTE: inode->entry must be read-locked!
int UG_read_blocks_local( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks, struct SG_manifest* blocks_not_local ) {
   
   int rc = 0;
   
   struct SG_manifest blocks_not_dirty;
   
   rc = SG_manifest_init( &blocks_not_dirty, UG_inode_volume_id( *inode ), UG_inode_coordinator_id( *inode ), UG_inode_file_id( *inode ), UG_inode_file_version( *inode ) );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // try dirty blocks
   rc = UG_read_dirty_blocks( gateway, inode, blocks, &blocks_not_dirty );
   if( rc != 0 ) {
      
      SG_error("UG_read_dirty_blocks( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), rc );
      
      SG_manifest_free( &blocks_not_dirty );
      return rc;
   }
   
   // done?
   if( SG_manifest_get_block_count( &blocks_not_dirty ) == 0 ) {
      
      SG_manifest_free( &blocks_not_dirty );
      return 0;
   }
   
   // try cached blocks 
   rc = UG_read_cached_blocks( gateway, fs_path, &blocks_not_dirty, blocks, blocks_not_local );
   
   SG_manifest_free( &blocks_not_dirty );
   
   if( rc != 0 ) {
      
      SG_error("UG_read_cached_blocks( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), rc );
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
int UG_read_blocks( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;
   struct SG_manifest blocks_to_download;
   uint64_t max_block_id = 0;
   uint64_t min_block_id = (uint64_t)(-1);
   
   // convert *blocks to a manifest, for tracking purposes 
   rc = SG_manifest_init( &blocks_to_download, UG_inode_volume_id( *inode ), UG_inode_coordinator_id( *inode ), UG_inode_file_id( *inode ), UG_inode_file_version( *inode ) );
   if( rc != 0 ) {
      
      // OOM 
      return rc;
   }
   
   for( UG_dirty_block_map_t::iterator itr = blocks->begin(); itr != blocks->end(); itr++ ) {
      
      rc = SG_manifest_put_block( &blocks_to_download, UG_dirty_block_info( itr->second ), true );
      if( rc != 0 ) {
         
         SG_manifest_free( &blocks_to_download );
         return rc;
      }
      
      // track min and max for debugging purposes 
      min_block_id = (min_block_id < itr->first ? min_block_id : itr->first);
      max_block_id = (max_block_id > itr->first ? max_block_id : itr->first);
   }
   
   // fetch local 
   rc = UG_read_blocks_local( gateway, fs_path, inode, blocks, &blocks_to_download );
   if( rc != 0 ) {
      
      SG_error("UG_read_blocks_local( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
               UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), min_block_id, max_block_id, rc );
      
      SG_manifest_free( &blocks_to_download );
      return rc;
   }
   
   // anything left to fetch rmeotely?
   if( SG_manifest_get_block_count( &blocks_to_download ) > 0 ) {
      
      // fetch remote 
      rc = UG_read_blocks_remote( gateway, fs_path, &blocks_to_download, blocks );
      if( rc != 0 ) {
         
         SG_error("UG_read_blocks_remote( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
                  UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), min_block_id, max_block_id, rc );
      }
   }
   
   SG_manifest_free( &blocks_to_download );
   
   return rc;
}


// fskit route to read data from a file 
// return 0 on success
// return -errno on failure 
// fent should not be locked
int UG_read( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data ) {
   
   int rc = 0;
   
   struct UG_file_handle* fh = (struct UG_file_handle*)handle_data;
   struct UG_inode* inode = fh->inode_ref;
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   
   uint64_t file_id = 0;
   int64_t file_version = 0;
   uint64_t coordinator_id = 0;
   int64_t write_nonce = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   uint64_t last_block_id = 0;
   
   UG_dirty_block_map_t read_blocks;
   UG_dirty_block_map_t unaligned_blocks;
   
   struct UG_dirty_block* last_block_read;
   UG_dirty_block_map_t::iterator last_block_read_itr;
   
   struct SG_manifest blocks_to_download;
   
   // make sure the manifest is fresh
   rc = UG_consistency_manifest_ensure_fresh( gateway, fskit_route_metadata_path( route_metadata ) );
   
   fskit_entry_rlock( fent );
   
   file_id = UG_inode_file_id( *inode );
   file_version = UG_inode_file_version( *inode );
   coordinator_id = UG_inode_coordinator_id( *inode );
   write_nonce = UG_inode_write_nonce( *inode );
   
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_consistency_manifest_ensure_fresh( %" PRIX64 " ('%s')) rc = %d\n", file_id, fskit_route_metadata_path( route_metadata ), rc );
      return rc;
   }
   
   
   // set of blocks to download
   rc = SG_manifest_init( &blocks_to_download, volume_id, coordinator_id, file_id, file_version );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("SG_manifest_init rc = %d\n", rc );
      
      return rc;
   }
   
   // get unaligned blocks 
   rc = UG_read_unaligned_setup( gateway, fskit_route_metadata_path( route_metadata ), inode, buf_len, offset, &read_blocks );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_read_unaligned_setup( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
      
      SG_manifest_free( &blocks_to_download );
      return rc;
   }
   
   // set up aligned read 
   rc = UG_read_aligned_setup( inode, buf, buf_len, offset, block_size, &read_blocks );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_read_aligned_setup( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
      
      UG_dirty_block_map_free( &read_blocks );
      
      SG_manifest_free( &blocks_to_download );
      
      return rc;
   }
   
   // fetch local 
   rc = UG_read_blocks_local( gateway, fskit_route_metadata_path( route_metadata ), inode, &read_blocks, &blocks_to_download );
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

   // anything left to fetch rmeotely?
   if( SG_manifest_get_block_count( &blocks_to_download ) > 0 ) {
   
      // fetch remote 
      rc = UG_read_blocks_remote( gateway, fskit_route_metadata_path( route_metadata ), &blocks_to_download, &read_blocks );
      if( rc != 0 ) {
         
         SG_error("UG_read_blocks_remote( %" PRIX64 ".%" PRId64 "[%" PRIu64 " - %" PRIu64 "] ) rc = %d\n", 
                  file_id, file_version, (offset / block_size), ((offset + buf_len) / block_size), rc );
      }
   }
   
   fskit_entry_wlock( fent );
   
   // cache last read block, but only if no writes occurred
   if( file_version == UG_inode_file_version( *inode ) && write_nonce == UG_inode_write_nonce( *inode ) ) {
      
      
      last_block_id = (buf_len + offset) / block_size;
      
      last_block_read_itr = read_blocks.find( last_block_id );
      if( last_block_read_itr != read_blocks.end() ) {
         
         last_block_read = &last_block_read_itr->second;
         
         // remember to evict this block when we close 
         UG_file_handle_evict_add_hint( fh, last_block_id, UG_dirty_block_version( *last_block_read ) );
         
         // cache this block
         rc = UG_inode_dirty_block_cache( inode, last_block_read );
         if( rc != 0 ) {
            
            // not fatal, but annoying...
            SG_error("UG_inode_dirty_block_cache( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
            rc = 0;
         }
      }
   }
   
   fskit_entry_unlock( fent );
   
   UG_dirty_block_map_free( &read_blocks );
   
   SG_manifest_free( &blocks_to_download );
   
   return rc;
}

