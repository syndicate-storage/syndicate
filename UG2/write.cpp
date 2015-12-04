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
   
   fskit_entry_set_mtime( UG_inode_fskit_entry( inode ), ts );
   return 0;
}


// update the write nonce on an inode, on write/truncate 
// always succeeds 
// NOTE: inode->entry must be write-locked!
int UG_write_nonce_update( struct UG_inode* inode ) {
   
   int64_t write_nonce = UG_inode_write_nonce( inode );
   UG_inode_set_write_nonce( inode, write_nonce + 1 );
   
   return 0;
}


// allocate and download the unaligned blocks of the write.
// merge the relevant portions of buf into them.
// dirty_blocks must NOT contain the affected blocks--they will be allocated and put in place by this method.
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

      SG_error("UG_read_unaligned_setup( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), fs_path, rc );
      return rc;
   }
   
   // get the blocks
   rc = UG_read_blocks( gateway, fs_path, inode, &unaligned_blocks );
   if( rc != 0 ) {
   
      SG_error("UG_read_blocks( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), fs_path, rc );
      
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
      if( UG_dirty_block_buf( dirty_block )->data == NULL ) {
         
         // not allocated
         return -EINVAL;
      }
      
      memcpy( UG_dirty_block_buf( dirty_block )->data + block_size - first_aligned_block_offset, buf, block_size - first_aligned_block_offset );
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
      if( UG_dirty_block_buf( dirty_block )->data == NULL ) {
         
         // not allocated
         return -EINVAL;
      }
      
      // where does the last block touched by buf begin in buf?
      off_t last_block_off = first_aligned_block_offset + (block_size * (last_aligned_block - first_aligned_block));
      
      memcpy( UG_dirty_block_buf( dirty_block )->data, buf + last_block_off, buf_len - last_block_off );
   }
   
   return 0;
}

// set up writes to aligned blocks, constructing dirty blocks from offsets in buf (i.e. zero-copy write)
// dirty_blocks must NOT contain any of the blocks over which this write applies
// return 0 on success
// return -ENOMEM on OOM 
// NOTE: inode->entry must be read-locked at least
static int UG_write_aligned_setup( struct UG_inode* inode, char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   
   uint64_t first_aligned_block = 0;
   uint64_t last_aligned_block = 0;
   uint64_t file_id = UG_inode_file_id( inode );
   int64_t file_version = UG_inode_file_version( inode );
   
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
      
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), aligned_block_id );
      
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
                  file_id, file_version, block_info->block_id, block_info->block_version, rc );
         
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
// this flushes each block to disk, and updates its hash in the inode's manifest.
// coalesce while we do it--free up blocks that do not have to be replicated.
// (i.e. file was reversioned --> drop all blocks beyond the size; block was overwritten --> drop old block).
// preserve vacuum information for every block we overwrite (NOTE: blocks will only be overwritten on conflict if overwrite is true)
// if overwrite is false, then free dirty_blocks that are already present in the inode.
// return 0 on success--all blocks in new_dirty_blocks will be either freed, or re-inserted into inode.
// return -ENOMEM on OOM
// NOTE: inode->entry must be write-locked 
// NOTE: this modifies new_dirty_blocks by removing successfully-merged dirty blocks.  new_dirty_blocks will contain all *unmerged* dirty blocks on return.
int UG_write_dirty_blocks_merge( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, int64_t old_file_version, off_t old_size, uint64_t block_size, UG_dirty_block_map_t* new_dirty_blocks, bool overwrite ) {
   
   UG_dirty_block_map_t::iterator tmp_itr;
   int rc = 0;

   // flush all blocks that we intend to merge
   for( UG_dirty_block_map_t::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); ) {
      
      uint64_t block_id = itr->first;
      struct UG_dirty_block* block = &itr->second;

      // sanity check 
      if( !UG_dirty_block_dirty( block ) ) {
         
         SG_error("FATAL BUG: dirty block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] is not dirty\n", UG_inode_file_id( inode ), block_id, UG_dirty_block_version(block) );
         exit(1); 
      }
      
      // sanity check: block must be in RAM 
      if( !UG_dirty_block_in_RAM( block ) ) {

         SG_error("FATAL BUG: Not in RAM: %" PRIX64 "[%" PRId64 "]\n", UG_inode_file_id( inode ), block_id );
         exit(1);
      }

      // don't include if the file was truncated before we could merge dirty data 
      if( old_file_version != UG_inode_file_version( inode ) ) {
         
         if( block_id * block_size >= (unsigned)old_size ) {
            
            UG_dirty_block_free( block );
            
            tmp_itr = itr;
            itr++;
            
            new_dirty_blocks->erase( tmp_itr );
            
            SG_debug("Skip truncated: %" PRIX64 "[%" PRId64 "]\n", UG_inode_file_id( inode ), block_id ); 
            continue;
         }
      }
      
      // don't include if we shouldn't overwrite on conflict 
      if( !overwrite && UG_inode_dirty_blocks( inode )->find( block_id ) != UG_inode_dirty_blocks( inode )->end() ) {
      
         tmp_itr = itr;
         itr++;
         
         UG_dirty_block_free( block );
         new_dirty_blocks->erase( tmp_itr );
         
         SG_debug("Won't overwrite: %" PRIX64 "[%" PRId64 "]\n", UG_inode_file_id( inode ), block_id ); 
         continue;
      }

      // if already flushing, then simply skip 
      if( UG_dirty_block_is_flushing( block ) ) {
         
         SG_debug("Already flushing: %" PRIX64 "[%" PRId64 "]\n", UG_inode_file_id( inode ), block_id );
         itr++;
         continue;
      }
      
      // make sure the block has a private copy of its RAM buffer, if it has one at all
      if( !UG_dirty_block_mmaped( block ) && !UG_dirty_block_unshared( block ) ) {
         
         // make sure this dirty block has its own copy of the RAM buffer
         rc = UG_dirty_block_buf_unshare( &itr->second );
         if( rc != 0 ) {
            
            // OOM 
            return rc;
         }
      }

      // serialize and send to disk 
      rc = UG_dirty_block_flush_async( gateway, fs_path, UG_inode_file_id( inode ), UG_inode_file_version( inode ), block );
      if( rc != 0 ) {
          
         SG_error("UG_dirty_block_flush_async( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
            
         return rc;
      }
      
      // insert this dirty block into the manifest, and retain info from the old version of this block so we can garbage-collect it later 
      // this propagates the new block hash to the inode manifest.
      rc = UG_inode_dirty_block_commit( gateway, inode, block );
      if( rc != 0 ) {
         
         // failed 
         SG_error("UG_inode_dirty_block_commit( %" PRIX64 ".%" PRIu64" [%" PRId64 ".%" PRIu64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
         
         return rc;
      }
   }

   // finish flushing all blocks
   // (NOTE: the act of flushing will update the block's information, but we need to keep it coherent with the manifest's information) 
   for( UG_dirty_block_map_t::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); ) {

      struct UG_dirty_block* block = &itr->second;

      if( !UG_dirty_block_is_flushing( block ) ) {

         // already processed 
         tmp_itr = itr;
         itr++;

         new_dirty_blocks->erase( tmp_itr );
         continue;
      }

      // finish flushing (NOTE: regenerates block hash)
      rc = UG_dirty_block_flush_finish( block );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_flush_finish( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
        
         return rc; 
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
int UG_write_impl( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data ) {
  
   int rc = 0;
   struct UG_file_handle* fh = (struct UG_file_handle*)handle_data;
   struct UG_inode* inode = NULL;
   struct SG_gateway* gateway = (struct SG_gateway*)fskit_core_get_user_data( core );
   
   UG_dirty_block_map_t write_blocks;                   // all the blocks we'll write.
   
   uint64_t gateway_id = SG_gateway_id( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   struct timespec ts;

   char* fs_path = fskit_route_metadata_get_path( route_metadata );
  
   // ID of the last block written 
   uint64_t last_block_id = (offset + buf_len) / block_size;
   
   // handle supports write?
   if( (fh->flags & (O_WRONLY | O_RDWR)) == 0 ) {
      return -EBADF;
   }
   
   // make sure the manifest is fresh
   rc = UG_consistency_manifest_ensure_fresh( gateway, fs_path );
   if( rc != 0 ) {
      
      fskit_entry_rlock( fent );
      SG_error("UG_consistency_manifest_ensure_fresh( %" PRIX64 " ('%s')) rc = %d\n", fskit_entry_get_file_id( fent ), fs_path, rc );
      fskit_entry_unlock( fent );

      return rc;
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   int64_t file_version = UG_inode_file_version( inode );
   uint64_t coordinator_id = UG_inode_coordinator_id( inode );
   
   // get unaligned blocks 
   rc = UG_write_read_unaligned_blocks( gateway, fs_path, inode, buf_len, offset, &write_blocks );
   
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      
      SG_error("UG_write_read_unaligned_blocks( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      return rc;
   }

   // merge data into unaligned blocks 
   rc = UG_write_unaligned_merge_data( buf, buf_len, offset, block_size, &write_blocks );
   if( rc != 0 ) {
       
      fskit_entry_unlock( fent );
      
      // bug 
      SG_error("BUG: UG_write_unaligned_merge_data( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      UG_dirty_block_map_free( &write_blocks );
      return -EINVAL;
   }
   
   // direct buf to aligned block writes
   rc = UG_write_aligned_setup( inode, buf, buf_len, offset, block_size, &write_blocks );
   if( rc != 0 ) {
       
      fskit_entry_unlock( fent );
      
      SG_error("UG_write_aligned_setup( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      UG_dirty_block_map_free( &write_blocks );
      return rc;
   }
  
   SG_debug("%s: write blocks %" PRIu64 " through %" PRIu64 "\n", fs_path, write_blocks.begin()->first, write_blocks.rbegin()->first );

   // mark all modified blocks as dirty...
   for( UG_dirty_block_map_t::iterator itr = write_blocks.begin(); itr != write_blocks.end(); itr++ ) {
      UG_dirty_block_set_dirty( &itr->second, true );
   }

   // don't flush the last block; keep it in RAM, so a subsequent write does not need to fetch it from disk.
   // Instead, recalculate the hash of the last block.
   // the other blocks' hashes will be re-calculated as a result of flushing them to disk (via UG_write_dirty_blocks_merge)
   UG_dirty_block_map_t::iterator itr = write_blocks.find( last_block_id );
   if( itr != write_blocks.end() && UG_dirty_block_in_RAM( &itr->second ) ) {
      
      struct UG_dirty_block* last_dirty_block = &itr->second;
      struct SG_chunk serialized;
      struct SG_request_data reqdat;

      // synthesize a block request
      rc = SG_request_data_init_block( gateway, fs_path, UG_inode_file_id( inode ), UG_inode_file_version( inode ), last_block_id, UG_dirty_block_version( last_dirty_block ), &reqdat );
      if( rc != 0 ) {

         fskit_entry_unlock( fent );
         SG_error("SG_request_data_init rc = %d\n", rc );
         UG_dirty_block_map_free( &write_blocks );

         return -EIO;
      }

      // serialize the last block, in order to calculate its new hash
      rc = SG_gateway_impl_serialize( gateway, &reqdat, UG_dirty_block_buf( last_dirty_block ), &serialized );
      if( rc != 0 ) {

         fskit_entry_unlock( fent );
         SG_error("UG_impl_block_serialize rc = %d\n", rc );
         UG_dirty_block_map_free( &write_blocks );

         return -EIO;
      }
             
      rc = UG_dirty_block_rehash( last_dirty_block, serialized.data, serialized.len );
      SG_chunk_free( &serialized );

      if( rc != 0 ) {

         fskit_entry_unlock( fent );
         SG_error("UG_dirty_block_rehash rc = %d\n", rc );
         UG_dirty_block_map_free( &write_blocks );

         return -EIO;
      }

      // put the last block's new metadata into place
      rc = UG_inode_dirty_block_commit( gateway, inode, last_dirty_block );
      if( rc != 0 ) {
        
         fskit_entry_unlock( fent ); 
         SG_error("UG_inode_dirty_block_commit rc = %d\n", rc );
         UG_dirty_block_map_free( &write_blocks );

         return -EIO;
      }
   }

   // put the rest of the written blocks into the manifest, flush them to disk, and prepare to vacuum overwritten blocks
   while( 1 ) {
      
      // merge written blocks into the inode, including their new hashes 
      // NOTE: moves contents of write_blocks into the inode, or frees them
      rc = UG_write_dirty_blocks_merge( gateway, fs_path, inode, file_version, fskit_entry_get_size( fent ), block_size, &write_blocks, true );
      if( rc != 0 ) {
         
         SG_error("UG_write_dirty_blocks_merge( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
         
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

   if( rc != 0 ) {

      fskit_entry_unlock( fent );
      UG_dirty_block_map_free( &write_blocks );
      return -EIO;
   }

   // update timestamps
   clock_gettime( CLOCK_REALTIME, &ts );
   UG_write_timestamp_update( inode, &ts );
   
   if( coordinator_id == gateway_id ) {
      
      // we're the coordinator--advance the manifest's modtime and write nonce
      SG_manifest_set_modtime( UG_inode_manifest( inode ), ts.tv_sec, ts.tv_nsec );
      UG_write_nonce_update( inode );
   }

   // will need to contact MS with new metadata
   UG_inode_set_dirty( inode, true );
   
   fskit_entry_unlock( fent );
   return rc;
}


// patch an inode's manifest.  Evict affected dirty blocks, cached blocks, and garbage blocks (the latter if the dirty block that got evicted was responsible for its creation).
// Does not affect other metadata, like modtime or size
// return 0 on success 
// return -ENOMEM on OOM 
// return -EPERM if we're not the coordinator
// NOTE: inode->entry must be write-locked
int UG_write_patch_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct UG_inode* inode, struct SG_manifest* write_delta ) {
   
   int rc = 0;
   struct md_syndicate_cache* cache = SG_gateway_cache( gateway );
   struct UG_replica_context* rctx = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   // basic sanity check: we must be the coordinator 
   if( SG_manifest_get_coordinator( write_delta ) != SG_gateway_id( gateway ) ) {
       return -EPERM;
   }
   
   // clone manifest--we'll patch it and then move it into place as an atomic operation 
   struct SG_manifest new_manifest;
   
   rc = SG_manifest_dup( &new_manifest, UG_inode_manifest( inode ) );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   rc = SG_manifest_patch( &new_manifest, write_delta, true );
   if( rc != 0 ) {
      
      if( rc != -ENOMEM ) {
         SG_error("SG_manifest_patch( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), rc );
      }
      
      SG_manifest_free( &new_manifest );
      return rc;
   }
   
   // prepare to replicate the patched manifest 
   rctx = UG_replica_context_new();
   if( rctx == NULL ) {
      
      // OOM 
      SG_manifest_free( &new_manifest );
      return -ENOMEM;
   }
   
   rc = UG_replica_context_init( rctx, ug, reqdat->fs_path, inode, &new_manifest, NULL );
   if( rc != 0 ) {
      
      // failed!
      if( rc != -ENOMEM ) {
         SG_error("UG_replica_context_init( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), rc );
      }
      
      SG_manifest_free( &new_manifest );
      SG_safe_free( rctx );
      return rc;
   }
   
   // do the replication 
   rc = UG_replicate( gateway, rctx );
   if( rc != 0 ) {
      
      // failed!
      SG_error("UG_replicate( %" PRIX64 ".%" PRId64 " ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), rc );
      
      UG_replica_context_free( rctx );
      SG_safe_free( rctx );
      SG_manifest_free( &new_manifest );
      return rc;
   }

   // success!
   UG_replica_context_free( rctx );
   SG_safe_free( rctx );
   
   // replace the manifest locally 
   UG_inode_manifest_replace( inode, &new_manifest );
   
   // clear out overwritten dirty blocks, and replaced block listings
   for( SG_manifest_block_iterator itr = SG_manifest_block_iterator_begin( write_delta ); itr != SG_manifest_block_iterator_end( write_delta ); itr++ ) {
      
      UG_dirty_block_map_t::iterator dirty_block_itr;
      struct UG_dirty_block* dirty_block = NULL;
      struct SG_manifest_block* replaced_block = NULL;
      uint64_t block_id = SG_manifest_block_iterator_id( itr );
      
      // did this patch touch a cached block?
      dirty_block_itr = UG_inode_dirty_blocks( inode )->find( block_id );
      if( dirty_block_itr != UG_inode_dirty_blocks( inode )->end() ) {
         
         dirty_block = &dirty_block_itr->second;
         
         // did this dirty block displace a replicated block?
         replaced_block = SG_manifest_block_lookup( UG_inode_replaced_blocks( inode ), block_id );
         if( replaced_block != NULL ) {
            
            if( SG_manifest_block_version( replaced_block ) == UG_dirty_block_version( dirty_block ) ) {
               
               // this dirty block displaced a replicated block, but now this dirty block has been remotely overwritten.
               // blow it away.
               SG_manifest_delete_block( UG_inode_replaced_blocks( inode ), block_id );
            }
         }
         
         // blow away this dirty block 
         UG_dirty_block_evict_and_free( cache, inode, dirty_block );
         
         UG_inode_dirty_blocks( inode )->erase( dirty_block_itr );
      }
   }
   
   return 0;
}
