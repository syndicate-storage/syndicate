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

#include "write.h"
#include "read.h"
#include "consistency.h"
#include "inode.h"
#include "replication.h"

// update timestamps on an inode on write/truncate
// always succeeds
// NOTE: inode->entry must be write-locked!
int UG_write_timestamp_update( struct UG_inode* inode, struct timespec* ts ) {
   
   fskit_entry_set_mtime( inode->entry, ts );
   return 0;
}


// update the write nonce on an inode, on write/truncate 
// always succeeds 
// NOTE: inode->entry must be write-locked!
int UG_write_nonce_update( struct UG_inode* inode ) {
   
   inode->write_nonce = md_random64();
   
   return 0;
}


// allocate and download the unaligned blocks of the write.
// merge the relevant portions of buf into them.
// *dirty_blocks must NOT contain the affected blocks--they will be allocated and put in place by this method.
// return 0 on success 
// return -EINVAL if we don't have block info in the inode's block manifest for the unaligned blocks
// return -errno on failure 
// NOTE: inode->entry must be read-locked
static int UG_write_read_unaligned_blocks( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   UG_dirty_block_map_t unaligned_blocks;
   
   // set up unaligned read
   rc = UG_read_unaligned_setup( gateway, fs_path, inode, buf_len, offset, &unaligned_blocks );
   if( rc != 0 ) {

      SG_error("UG_read_unaligned_setup( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), fs_path, rc );
      return rc;
   }
   
   // get the blocks
   rc = UG_read_blocks( gateway, fs_path, inode, &unaligned_blocks );
   if( rc != 0 ) {
   
      SG_error("UG_read_blocks( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), fs_path, rc );
      
      UG_dirty_block_map_free( &unaligned_blocks );
      
      return rc;
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
   
   return rc;
}


// merge written data to unaligned blocks
// return 0 on success
// return -ENOENT if a requested unaligned block is not found in dirty_blocks
// return -EINVAL if the unaligned dirty block is not in RAM
static int UG_write_unaligned_merge_data( char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks ) {
   
   uint64_t first_aligned_block = 0;
   uint64_t last_aligned_block = 0;
   off_t first_aligned_block_offset = 0;  // offset into buf where the first aligned block starts
   
   UG_dirty_block_map_t::iterator dirty_block_itr;
   struct UG_dirty_block* dirty_block = NULL;
   
   UG_dirty_block_aligned( offset, buf_len, block_size, &first_aligned_block, &last_aligned_block, &first_aligned_block_offset );
   
   if( first_aligned_block_offset != 0 || first_aligned_block != 0 ) {
      
      // first block is unaligned 
      dirty_block_itr = dirty_blocks->find( first_aligned_block );
      if( dirty_block_itr == dirty_blocks->end() ) {
         
         // not present 
         return -ENOENT;
      }
      
      dirty_block = &dirty_block_itr->second;
      
      // sanity check...
      if( UG_dirty_block_buf( *dirty_block ).data == NULL ) {
         
         // not allocated
         return -EINVAL;
      }
      
      memcpy( UG_dirty_block_buf( *dirty_block ).data + block_size - first_aligned_block_offset, buf, block_size - first_aligned_block_offset );
   }
   
   if( last_aligned_block != first_aligned_block && last_aligned_block > 0 ) {
      
      // last block is unaligned 
      dirty_block_itr = dirty_blocks->find( last_aligned_block );
      if( dirty_block_itr == dirty_blocks->end() ) {
         
         // not present 
         return -ENOENT;
      }
      
      dirty_block = &dirty_block_itr->second;
      
      // sanity check...
      if( UG_dirty_block_buf( *dirty_block ).data == NULL ) {
         
         // not allocated
         return -EINVAL;
      }
      
      // where does the last block touched by buf begin in buf?
      off_t last_block_off = first_aligned_block_offset + (block_size * (last_aligned_block - first_aligned_block));
      
      memcpy( UG_dirty_block_buf( *dirty_block ).data, buf + last_block_off, buf_len - last_block_off );
   }
   
   return 0;
}

// set up writes to aligned blocks, constructing dirty blocks from offsets in buf (i.e. zero-copy write)
// *dirty_blocks must NOT contain any of the blocks over which this write applies
// return 0 on success
// return -ENOMEM on OOM 
// NOTE: inode->entry must be read-locked at least
static int UG_write_aligned_setup( struct UG_inode* inode, char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   
   uint64_t first_aligned_block = 0;
   uint64_t last_aligned_block = 0;
   off_t first_aligned_block_offset = 0;  // offset into buf where the first aligned block starts
   
   UG_dirty_block_aligned( offset, buf_len, block_size, &first_aligned_block, &last_aligned_block, &first_aligned_block_offset );
   
   // accumulate dirty blocks, and then merge them into dirty_blocks
   UG_dirty_block_map_t blocks;
   
   // set up dirty blocks to point to buf accordingly 
   for( uint64_t aligned_block_id = first_aligned_block; aligned_block_id <= last_aligned_block; aligned_block_id++ ) {
      
      uint64_t aligned_offset = first_aligned_block_offset + (aligned_block_id - first_aligned_block) * block_size;
      struct UG_dirty_block next_block;
      
      // if we have a write hole...
      struct SG_manifest_block write_hole;
      struct SG_chunk write_hole_block;
      
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( &inode->manifest, aligned_block_id );
      
      if( block_info == NULL ) {
         
         // write hole, initialize a new block 
         write_hole_block.data = buf + aligned_offset;
         write_hole_block.len = block_size;
         
         // make a block for this write hole, and give it a new version 
         rc = SG_manifest_block_init_from_chunk( &write_hole, aligned_block_id, md_random64(), &write_hole_block );
         if( rc != 0 ) {
            
            // OOM 
            break;
         }
         
         block_info = &write_hole;
      }
      
      rc = UG_dirty_block_init_ram_nocopy( &next_block, block_info, buf + aligned_offset, block_size );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_init_ram_nocopy( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), block_info->block_id, block_info->block_version, rc );
         
         break;
      }
      
      // emplace 
      try {
         
         (*dirty_blocks)[ block_info->block_id ] = next_block;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         break;
      }
   }
   
   return rc;
}


// merge dirty blocks back into an inode, i.e. on write, or on failure to replicate.
// make sure each block we put in either has an unshared RAM buffer, or is mmaped from disk.
// coalesce while we do it--free up blocks that really do not have to be replicated.
// (i.e. file was reversioned --> drop all blocks beyond the size; block was overwritten --> drop old block).
// if there is a record of a block being replicated before (i.e. it already exists in the manifest), then put the overwritten block info
// from the inode's manifest into the inode's replaced_blocks listing, so we can vacuum it.
// if overwrite is false, then free dirty_blocks that are already present in the inode.
// return 0 on success--all blocks in new_dirty_blocks will be either freed, or re-inserted into inode.
// return -ENOMEM on OOM
// NOTE: inode->entry must be write-locked 
int UG_write_dirty_blocks_merge( struct SG_gateway* gateway, struct UG_inode* inode, int64_t old_file_version, off_t old_size, uint64_t block_size, UG_dirty_block_map_t* new_dirty_blocks, bool overwrite ) {
   
   UG_dirty_block_map_t::iterator tmp_itr;
   int rc = 0;
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   
   // merge blocks into the inode
   for( UG_dirty_block_map_t::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); ) {
      
      uint64_t block_id = itr->first;
      struct UG_dirty_block* block = &itr->second;
      
      // skip non-dirty 
      if( !UG_dirty_block_dirty( *block ) ) {
         
         UG_dirty_block_free( block );
         
         tmp_itr = itr;
         itr++;
         
         new_dirty_blocks->erase( tmp_itr );
         
         continue;
      }
      
      // skip truncated 
      if( old_file_version != UG_inode_file_version( *inode ) ) {
         
         if( block_id * block_size >= (unsigned)old_size ) {
            
            UG_dirty_block_evict_and_free( cache, inode, block );
            
            tmp_itr = itr;
            itr++;
            
            new_dirty_blocks->erase( tmp_itr );
            
            continue;
         }
      }
      
      // skip if we shouldn't overwrite 
      if( !overwrite && UG_inode_dirty_blocks( *inode )->find( block_id ) != UG_inode_dirty_blocks( *inode )->end() ) {
      
         tmp_itr = itr;
         itr++;
         
         new_dirty_blocks->erase( tmp_itr );
         
         continue;
      }
      
      // if flushed to disk, then mmap it
      if( UG_dirty_block_buf( itr->second ).data == NULL && UG_dirty_block_fd( itr->second ) > 0 ) {
         
         rc = UG_dirty_block_mmap( &itr->second );
         if( rc != 0 ) {
            
            // OOM
            return rc;
         }
      }
      
      // make sure the block has a private copy of its RAM buffer, if it has one at all
      else if( !UG_dirty_block_mmaped( itr->second ) && !UG_dirty_block_unshared( itr->second ) && UG_dirty_block_buf( itr->second ).data != NULL ) {
         
         // make sure this dirty block has its own copy of the RAM buffer
         rc = UG_dirty_block_buf_unshare( &itr->second );
         if( rc != 0 ) {
            
            // OOM 
            return rc;
         }
      }
      
      // insert this dirty block 
      rc = UG_inode_dirty_block_commit( gateway, inode, block );
      if( rc != 0 ) {
         
         // failed 
         SG_error("UG_inode_dirty_block_commit( %" PRIX64 ".%" PRIu64" [%" PRId64 ".%" PRIu64 "] ) rc = %d\n", 
               UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), UG_dirty_block_id( *block ), UG_dirty_block_version( *block ), rc );
         
         break;
      }
      
      // next dirty block 
      tmp_itr = itr;
      itr++;
      
      new_dirty_blocks->erase( tmp_itr );
   }
   
   return rc;
}



// fskit callback for write.
// write data, locally.  Buffer data to RAM if possible, and flush to the disk cache if we need to.
// refresh the manifest before writing.
// return 0 on success
// return -ENOMEM on OOM
// return -errno on failure to read unaligned blocks or flush data to cache
// NOTE: fent should not be locked
int UG_write( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data ) {
   
   int rc = 0;
   struct UG_file_handle* fh = (struct UG_file_handle*)handle_data;
   struct UG_inode* inode = fh->inode_ref;
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   
   UG_dirty_block_map_t write_blocks;                   // all the blocks we'll write.
   
   uint64_t gateway_id = SG_gateway_id( gateway );
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   struct timespec ts;
   
   // ID of the last block written 
   uint64_t last_block_id = (offset + buf_len) / block_size;
   
   // handle supports write?
   if( (fh->flags & (O_WRONLY | O_RDWR)) == 0 ) {
      return -EBADF;
   }
   
   // make sure the manifest is fresh
   rc = UG_consistency_manifest_ensure_fresh( gateway, fskit_route_metadata_path( route_metadata ) );
   if( rc != 0 ) {
      
      SG_error("UG_consistency_manifest_ensure_fresh( %" PRIX64 " ('%s')) rc = %d\n", UG_inode_file_id( *inode ), fskit_route_metadata_path( route_metadata ), rc );
      return rc;
   }
   
   fskit_entry_rlock( fent );
   
   // get unaligned blocks 
   rc = UG_write_read_unaligned_blocks( gateway, fskit_route_metadata_path( route_metadata ), inode, buf_len, offset, &write_blocks );
   
   fskit_entry_unlock( fent );
   
   if( rc != 0 ) {
      
      SG_error("UG_write_read_unaligned_blocks( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
      
      return rc;
   }
   
   // merge data into unaligned blocks 
   rc = UG_write_unaligned_merge_data( buf, buf_len, offset, block_size, &write_blocks );
   if( rc != 0 ) {
      
      // bug 
      SG_error("BUG: UG_write_unaligned_merge_data( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
      
      UG_dirty_block_map_free( &write_blocks );
      
      return -EINVAL;
   }
   
   // direct buf to aligned block writes
   rc = UG_write_aligned_setup( inode, buf, buf_len, offset, block_size, &write_blocks );
   if( rc != 0 ) {
      
      SG_error("UG_write_aligned_setup( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
      
      UG_dirty_block_map_free( &write_blocks );
      
      return rc;
   }
   
   // put written blocks
   while( 1 ) {
      
      // merge written blocks into the inode 
      // NOTE: moves contents of write_blocks into the inode, or frees them
      rc = UG_write_dirty_blocks_merge( gateway, inode, UG_inode_file_version( *inode ), fskit_entry_get_size( fent ), block_size, &write_blocks, true );
      if( rc != 0 ) {
         
         SG_error("UG_write_dirty_blocks_merge( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
         
         if( rc == -ENOMEM ) {
            
            // try again 
            continue;
         }
         else {
            
            // fatal 
            break;
         }
      }
      
      break;
   }
   
   // trim the inode's dirty blocks, flushing all but the last-written one to disk (but keeping it in RAM if it is unaligned)
   if( (offset + buf_len) % block_size != 0 ) {
      
      rc = UG_inode_dirty_blocks_trim( gateway, fskit_route_metadata_path( route_metadata ), inode, &last_block_id, 1 );
      if( rc != 0 ) {
         
         // not fatal, but annoying...
         SG_error("UG_inode_dirty_blocks_trim( %s, %zu, %jd ) rc = %d\n", fskit_route_metadata_path( route_metadata ), buf_len, offset, rc );
         rc = 0;
      }
   }
   
   // update timestamps and write nonce
   clock_gettime( CLOCK_REALTIME, &ts );
   
   UG_write_timestamp_update( inode, &ts );
   UG_write_nonce_update( inode );
   
   if( UG_inode_coordinator_id( *inode ) == gateway_id ) {
      
      // we're the coordinator--advance the manifest's modtime 
      SG_manifest_set_modtime( UG_inode_manifest( *inode ), ts.tv_sec, ts.tv_nsec );
   }
   
   inode->dirty = true;
   
   return rc;
}


// patch an inode's manifest.  Evict affected dirty blocks, cached blocks, and garbage blocks (the latter if the dirty block that got evicted was responsible for its creation).
// return 0 on success 
// return -ENOMEM on OOM 
// NOTE: inode->entry must be write-locked
int UG_write_patch_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct UG_inode* inode, struct SG_manifest* write_delta ) {
   
   int rc = 0;
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   struct UG_replica_context rctx;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   // clone manifest--we'll patch it and then move it into place as an atomic operation 
   struct SG_manifest new_manifest;
   
   rc = SG_manifest_dup( &new_manifest, UG_inode_manifest( *inode ) );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   rc = SG_manifest_patch( &new_manifest, write_delta, true );
   if( rc != 0 ) {
      
      if( rc != -ENOMEM ) {
         SG_error("SG_manifest_patch( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), rc );
      }
      
      SG_manifest_free( &new_manifest );
      return rc;
   }
   
   // try to replicate the patch manifest
   rc = UG_replica_context_init( &rctx, ug, reqdat->fs_path, inode, &new_manifest, &inode->old_manifest_modtime, NULL );
   if( rc != 0 ) {
      
      // failed!
      if( rc != -ENOMEM ) {
         SG_error("UG_replica_context_init( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), rc );
      }
      
      SG_manifest_free( &new_manifest );
      return rc;
   }
   
   // success! replace the manifest locally 
   UG_inode_manifest_replace( inode, &new_manifest );
   
   // clear out overwritten dirty blocks, and replaced block listings
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( write_delta ); itr != SG_manifest_block_iterator_end( write_delta ); itr++ ) {
      
      UG_dirty_block_map_t::iterator dirty_block_itr;
      struct UG_dirty_block* dirty_block = NULL;
      struct SG_manifest_block* replaced_block = NULL;
      uint64_t block_id = SG_manifest_block_iterator_id( itr );
      
      // did this patch touch a cached block?
      dirty_block_itr = UG_inode_dirty_blocks( *inode )->find( block_id );
      if( dirty_block_itr != UG_inode_dirty_blocks( *inode )->end() ) {
         
         dirty_block = &dirty_block_itr->second;
         
         // did this dirty block displace a replicated block?
         replaced_block = SG_manifest_block_lookup( UG_inode_replaced_blocks( *inode ), block_id );
         if( replaced_block != NULL ) {
            
            if( SG_manifest_block_version( replaced_block ) == UG_dirty_block_version( *dirty_block ) ) {
               
               // this dirty block displaced a replicated block, but now this dirty block has been remotely overwritten.
               // blow it away.
               SG_manifest_delete_block( UG_inode_replaced_blocks( *inode ), block_id );
            }
         }
         
         // blow away this dirty block 
         UG_dirty_block_evict_and_free( cache, inode, dirty_block );
         
         UG_inode_dirty_blocks( *inode )->erase( dirty_block_itr );
      }
   }
   
   return 0;
}
