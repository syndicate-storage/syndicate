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


// set up block buffer for an existing block that will be partially overwritten.
// the block will be gifted the buf
// return 0 on success
// return -ENOMEM on OOM 
// inode->entry must be read-locked
static int UG_write_setup_partial_block_buffer( struct UG_inode* inode, uint64_t block_id, char* buf, uint64_t buf_len, UG_dirty_block_map_t* blocks ) {
   
   int rc = 0;

   struct UG_dirty_block block_data;
   struct SG_manifest_block* block_info = NULL;
   
   // look up this block's info from the manifest 
   block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), block_id );
   if( block_info == NULL ) {
      
      SG_error("BUG: No manifest info for %" PRIX64 "[%" PRIu64 "]\n", UG_inode_file_id( inode ), block_id );
      exit(1);
      return -EINVAL;
   }

   // generate the dirty block 
   rc = UG_dirty_block_init_ram_nocopy( &block_data, block_info, buf, buf_len );
   if( rc != 0 ) {
      
      return rc;
   }

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


// set up reads to existing but partially-written blocks, in a zero-copy manner.  *dirty_blocks must NOT contain the partial block information yet.
// A block is partially-written if the write buffer represented by (buf_len, offset) encompasses part of it.
// return 0 on success
// return -errno on failure 
// NOTE: inode->entry must be read-locked 
int UG_write_read_partial_setup( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   uint64_t block_id = 0;
   char* buf = NULL;

   uint64_t first_affected_block = (offset) / block_size;
   uint64_t last_affected_block = (offset + buf_len) / block_size;

   bool first_affected_block_exists = (SG_manifest_block_lookup( UG_inode_manifest( inode ), first_affected_block ) != NULL );
   bool last_affected_block_exists = (SG_manifest_block_lookup( UG_inode_manifest( inode ), last_affected_block ) != NULL );
   
   // scratch area for fetching blocks 
   UG_dirty_block_map_t partial_blocks;
  
   SG_debug("First affected block: %" PRIu64 " (exists = %d)\n", first_affected_block, first_affected_block_exists );
   SG_debug("Last affected block: %" PRIu64 " (exists = %d)\n", last_affected_block, last_affected_block_exists );

   // is the first block partial?
   // also, if the first block is the same as the last block, and the last block is partial,
   // then the first block is considered partial
   if( first_affected_block_exists &&
       (( first_affected_block == last_affected_block && (offset % block_size != 0 || (offset + buf_len) % block_size) != 0) ||
        ( first_affected_block < last_affected_block && (offset % block_size) != 0)) ) {
      
      // head is partial
      block_id = offset / block_size;
      
      // make a head buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }

      SG_debug("Read partial HEAD block %" PRIu64 "\n", block_id );
      
      // set up the request 
      rc = UG_write_setup_partial_block_buffer( inode, block_id, buf, block_size, &partial_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf ); 
         UG_dirty_block_map_free( &partial_blocks );
         
         return rc;
      }
      
      buf = NULL;
   }
   
   // is the last block partial, and distinct from the head?
   if( last_affected_block_exists && (offset + buf_len) > 0 && (((offset + buf_len) % block_size) != 0 && first_affected_block < last_affected_block )) {
      
      // tail is distinct and partial
      block_id = (offset + buf_len) / block_size;
      
      // make a tail buffer 
      buf = SG_CALLOC( char, block_size );
      if( buf == NULL ) {
         
         return -ENOMEM;
      }
     
      SG_debug("Read partial TAIL block %" PRIu64 "\n", block_id );

      // set up the tail request 
      rc = UG_write_setup_partial_block_buffer( inode, block_id, buf, block_size, &partial_blocks );
      if( rc != 0 ) {
         
         SG_safe_free( buf );
         
         UG_dirty_block_map_free( &partial_blocks );
         
         return rc;
      }
      
      buf = NULL;
   }
   
   // transfer data over to the dirty_blocks set
   for( UG_dirty_block_map_t::iterator itr = partial_blocks.begin(); itr != partial_blocks.end(); ) {
      
      try {
         (*dirty_blocks)[ itr->first ] = itr->second;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         
         UG_dirty_block_map_free( &partial_blocks );
         break;
      }
      
      UG_dirty_block_map_t::iterator old_itr = itr;
      itr++;
      
      partial_blocks.erase( old_itr );
   }
   
   return 0;
}

// allocate and download the existing but partially-overwitten blocks of the write.
// merge the relevant portions of buf into them.
// dirty_blocks must NOT contain the affected blocks--they will be allocated and put in place by this method.
// return 0 on success 
// return -EINVAL if we don't have block info in the inode's block manifest for the unaligned blocks
// return -errno on failure 
// NOTE: inode->entry must be read-locked
static int UG_write_read_partial_blocks( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks ) {
   
   int rc = 0;
   UG_dirty_block_map_t partial_blocks;

   // set up read on partial blocks
   rc = UG_write_read_partial_setup( gateway, fs_path, inode, buf_len, offset, &partial_blocks );
   if( rc < 0 ) {

      SG_error("UG_read_unaligned_setup( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), fs_path, rc );
      return rc;
   }

   if( partial_blocks.size() == 0 ) {
      // no existing partial blocks 
      SG_debug("%s", "No existing partial blocks to fetch\n");
      return 0;
   }
 
   // get the blocks
   rc = UG_read_blocks( gateway, fs_path, inode, &partial_blocks, offset, buf_len );
   if( rc != 0 ) {
   
      SG_error("UG_read_blocks( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", UG_inode_file_id( inode ), UG_inode_file_version( inode ), fs_path, rc );
      
      UG_dirty_block_map_free( &partial_blocks );
      
      return rc;
   }
   
   // transfer data over 
   for( UG_dirty_block_map_t::iterator itr = partial_blocks.begin(); itr != partial_blocks.end(); ) {
      
      char data[21];
      memset( data, 0, 21 );
      memcpy( data, UG_dirty_block_buf( &itr->second )->data, 20 );

      SG_debug("Partial: %" PRIX64 "[%" PRId64 "], data = '%s'\n", UG_inode_file_id( inode ), itr->first, data );

      try {
         (*dirty_blocks)[ itr->first ] = itr->second;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         
         UG_dirty_block_map_free( &partial_blocks );
         break;
      }
      
      UG_dirty_block_map_t::iterator old_itr = itr;
      itr++;
      
      partial_blocks.erase( old_itr );
   }
   
   return rc;
}


// merge written data to partially-overwritten blocks
// blocks in unaligned_dirty_blocks must be in RAM, contain the first and last block touched by the write.
// return 0 on success
// return -ENOENT if a requested unaligned block is not found in dirty_blocks
// return -EINVAL if the unaligned dirty block is not in RAM
static int UG_write_partial_merge_data( char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* unaligned_dirty_blocks ) {
   
   struct UG_dirty_block* dirty_block = NULL;
   UG_dirty_block_map_t::iterator dirty_block_itr;
   uint64_t first_affected_block = (offset) / block_size;
   uint64_t last_affected_block = (offset + buf_len) / block_size;
   uint64_t block_copy_start = 0;
   uint64_t buf_offset = 0;
   uint64_t buf_copy_len = 0;
   int rc = 0;
  
   if( offset % block_size != 0 ) {

       // write starts unaligned 
       block_copy_start = offset % block_size;
       buf_offset = 0;
       buf_copy_len = MIN( buf_len, block_size - block_copy_start );

       dirty_block_itr = unaligned_dirty_blocks->find( first_affected_block );
       if( dirty_block_itr != unaligned_dirty_blocks->end() ) {

          // HEAD unaligned block 
          dirty_block = &dirty_block_itr->second;

          // copy in data from write buffer to this unaligned block
          SG_debug("Partial HEAD: fill block %" PRIu64 " at %" PRId64 " from %" PRIu64 " length %" PRIu64 "\n", first_affected_block, block_copy_start, buf_offset, buf_copy_len );
          memcpy( UG_dirty_block_buf( dirty_block )->data + block_copy_start, buf + buf_offset, buf_copy_len );
      
          // reversion 
          UG_dirty_block_set_version( dirty_block, md_random64() );
       }
       else {

          // new HEAD unaligned block 
          struct UG_dirty_block head_block;
          struct SG_manifest_block head_block_info;
          struct SG_chunk head_data;

          memset( &head_data, 0, sizeof(struct SG_chunk) );
          head_data.len = block_size;
          head_data.data = SG_CALLOC( char, block_size );
          if( head_data.data == NULL ) {

             return -ENOMEM;
          }

          SG_debug("New partial HEAD: fill block %" PRIu64 " at %" PRId64 " from %" PRIu64 " length %" PRIu64 "\n", first_affected_block, block_copy_start, buf_offset, buf_copy_len );
          memcpy( head_data.data + block_copy_start, buf + buf_offset, buf_copy_len );

          // put dirty block with new version 
          rc = SG_manifest_block_init_from_chunk( &head_block_info, first_affected_block, md_random64(), &head_data );
          if( rc != 0 ) {

             SG_chunk_free( &head_data );
             return rc;
          }

          // gift buffer to dirty block
          rc = UG_dirty_block_init_ram_nocopy( &head_block, &head_block_info, head_data.data, head_data.len );
          SG_manifest_block_free( &head_block_info );
          if( rc != 0 ) {

             SG_chunk_free( &head_data );
             return rc;
          }

          // gifted, so unshared 
          UG_dirty_block_set_unshared( &head_block, true );

          try {

             (*unaligned_dirty_blocks)[ first_affected_block ] = head_block;
          }
          catch( bad_alloc& ba ) {

             // OOM 
             UG_dirty_block_free( &head_block );
             return -ENOMEM;
          }
       }
   }

   if( (last_affected_block > first_affected_block && ((offset + buf_len) % block_size) != 0) ||
       (last_affected_block == first_affected_block && (offset % block_size) == 0 && ((offset + buf_len) % block_size) != 0 ) ) {

       // last block is incomplete
       block_copy_start = 0;
       buf_copy_len = (offset + buf_len) % block_size;
       buf_offset = buf_len - buf_copy_len;

       dirty_block_itr = unaligned_dirty_blocks->find( last_affected_block );
       if( dirty_block_itr != unaligned_dirty_blocks->end() ) {

          // existing TAIL unaligned block 
          dirty_block = &dirty_block_itr->second;

          // copy data from the write buffer to this unaligned block 
          SG_debug("Existing partial TAIL: fill block %" PRIu64 " at %" PRId64 " from %" PRIu64 " length %" PRIu64 "\n", last_affected_block, block_copy_start, buf_offset, buf_copy_len );
          memcpy( UG_dirty_block_buf( dirty_block )->data + block_copy_start, buf + buf_offset, buf_copy_len );

          // reversion 
          UG_dirty_block_set_version( dirty_block, md_random64() );
       }
       else {

          // new TAIL unaligned block 
          struct UG_dirty_block tail_block;
          struct SG_manifest_block tail_block_info;
          struct SG_chunk tail_data;

          memset( &tail_data, 0, sizeof(struct SG_chunk) );
          tail_data.len = block_size;
          tail_data.data = SG_CALLOC( char, block_size );
          if( tail_data.data == NULL ) {

             return -ENOMEM;
          }

          SG_debug("New partial TAIL: fill block %" PRIu64 " at %" PRId64 " from %" PRIu64 " length %" PRIu64 "\n", last_affected_block, block_copy_start, buf_offset, buf_copy_len );
          memcpy( tail_data.data + block_copy_start, buf + buf_offset, buf_copy_len );

          // put dirty block with new version 
          rc = SG_manifest_block_init_from_chunk( &tail_block_info, last_affected_block, md_random64(), &tail_data );
          if( rc != 0 ) {

             SG_chunk_free( &tail_data );
             return rc;
          }

          // gift buffer to dirty block
          rc = UG_dirty_block_init_ram_nocopy( &tail_block, &tail_block_info, tail_data.data, tail_data.len );
          SG_manifest_block_free( &tail_block_info );
          if( rc != 0 ) {

             SG_chunk_free( &tail_data );
             return rc;
          }

          // gifted, so unshared 
          UG_dirty_block_set_unshared( &tail_block, true );

          try {

             (*unaligned_dirty_blocks)[ last_affected_block ] = tail_block;
          }
          catch( bad_alloc& ba ) {

             // OOM 
             UG_dirty_block_free( &tail_block );
             return -ENOMEM;
          }
       }
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
   off_t last_block_len = 0;
   
   UG_dirty_block_aligned( offset, buf_len, block_size, &first_aligned_block, &last_aligned_block, &first_aligned_block_offset, &last_block_len );

   // are there any aligned blocks?
   if( first_aligned_block > last_aligned_block || (first_aligned_block == last_aligned_block && last_block_len > 0 && (unsigned)last_block_len < block_size) ) {
      return 0;
   }

   SG_debug("Write aligned blocks (offset %jd, len %zu): %" PRIu64 " thru %" PRIu64 "\n", offset, buf_len, first_aligned_block, last_aligned_block );
   
   // set up dirty blocks to point to buf accordingly 
   for( uint64_t aligned_block_id = first_aligned_block; aligned_block_id <= last_aligned_block; aligned_block_id++ ) {
      
      uint64_t aligned_offset = first_aligned_block_offset + (aligned_block_id - first_aligned_block) * block_size;
      struct UG_dirty_block next_block;
      
      // if we have a write hole...
      struct SG_manifest_block new_block;
      struct SG_chunk new_block_data;
      uint64_t block_id = 0;
      int64_t block_version = 0;
      
      struct SG_manifest_block* block_info = SG_manifest_block_lookup( UG_inode_manifest( inode ), aligned_block_id );
      
      if( block_info == NULL ) {
         
         SG_debug("Write NEW aligned block %" PRIu64 "\n", aligned_block_id );

         // initialize a new block 
         new_block_data.data = buf + aligned_offset;
         new_block_data.len = block_size;

         block_id = aligned_block_id;
         block_version = md_random64();
         
         rc = SG_manifest_block_init_from_chunk( &new_block, block_id, block_version, &new_block_data );
         if( rc != 0 ) {
            
            // OOM 
            break;
         }
         
         // reversion 
         block_info = &new_block;
      }
      else {

         SG_debug("Write EXISTING aligned block %" PRIu64 " from %" PRIu64 "\n", aligned_block_id, aligned_offset );
      }
       
      rc = UG_dirty_block_init_ram_nocopy( &next_block, block_info, buf + aligned_offset, block_size );

      if( block_info == &new_block ) {
         SG_manifest_block_free( &new_block );
      }

      // reversion
      UG_dirty_block_set_version( &next_block, md_random64() );
      block_info = NULL;

      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_init_ram_nocopy( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  file_id, file_version, block_id, block_version, rc );
        
         break;
      }

      // emplace 
      try {
         
         (*dirty_blocks)[ block_id ] = next_block;
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
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
int UG_write_dirty_blocks_merge( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* new_dirty_blocks, uint64_t offset, uint64_t len, bool overwrite ) {
   
   UG_dirty_block_map_t::iterator tmp_itr;
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   int64_t file_version = UG_inode_file_version( inode );
   uint64_t size = fskit_entry_get_size( UG_inode_fskit_entry(inode) );
   struct SG_IO_hints io_hints;

   SG_debug("Merge %zu blocks to %" PRIX64 "\n", new_dirty_blocks->size(), UG_inode_file_id( inode ) );

   SG_IO_hints_init( &io_hints, SG_IO_WRITE, offset, len );

   // flush all blocks that we intend to merge
   for( UG_dirty_block_map_t::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); ) {
      
      uint64_t block_id = itr->first;
      struct UG_dirty_block* block = &itr->second;

      // sanity check: block must be dirty
      if( !UG_dirty_block_dirty( block ) ) {
         
         SG_error("FATAL BUG: dirty block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] is not dirty\n", UG_inode_file_id( inode ), block_id, UG_dirty_block_version(block) );
         exit(1); 
      }
      
      // sanity check: block must be in RAM
      if( !UG_dirty_block_in_RAM( block ) ) {

         SG_error("FATAL BUG: Not in RAM: %" PRIX64 "[%" PRId64 ".%" PRId64 "]\n", UG_inode_file_id( inode ), block_id, UG_dirty_block_version( block ) );
         exit(1);
      }

      // sanity check: block must not be flushing 
      if( UG_dirty_block_is_flushing( block ) ) {

         SG_error("FATAL BUG: dirty block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] is already flushed\n", UG_inode_file_id( inode ), block_id, UG_dirty_block_version( block ) );
         exit(1);
      }

      // sanity check: block must not be mmaped 
      if( UG_dirty_block_mmaped( block ) ) {

         SG_error("FATAL BUG: dirty block %" PRIX64 "[%" PRIu64 ".%" PRId64 "] is mmaped\n", UG_inode_file_id( inode ), block_id, UG_dirty_block_version( block ) );
         exit(1);
      }

      // don't include if the file was truncated before we could merge dirty data 
      if( file_version != UG_inode_file_version( inode ) ) {
         
         if( block_id * block_size >= size ) {
            
            tmp_itr = itr;
            itr++;
            
            UG_dirty_block_free( block );
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

      // make sure the block has a private copy of its RAM buffer, if it has one at all
      if( !UG_dirty_block_unshared( block ) ) {
         
         // make sure this dirty block has its own copy of the RAM buffer
         rc = UG_dirty_block_buf_unshare( &itr->second );
         if( rc != 0 ) {
            
            // OOM 
            return rc;
         }
      }

      // serialize and send to disk
      // NOTE: this will update the block's hash 
      rc = UG_dirty_block_flush_async( gateway, fs_path, UG_inode_file_id( inode ), UG_inode_file_version( inode ), block, &io_hints );
      if( rc != 0 ) {
          
         SG_error("UG_dirty_block_flush_async( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
            
         return rc;
      }

      itr++;
   }

   // finish flushing all blocks, and commit them to the manifest
   // (NOTE: the act of flushing will update the block's information, but we need to keep it coherent with the manifest's information) 
   for( UG_dirty_block_map_t::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); ) {

      struct UG_dirty_block* block = &itr->second;

      // finish flushing (NOTE: regenerates block hash)
      rc = UG_dirty_block_flush_finish( block );
      if( rc != 0 ) {
         
         SG_error("UG_dirty_block_flush_finish( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                  UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
        
         return rc; 
      }
 
      // insert this dirty block into the manifest, and retain info from the old version of this block so we can garbage-collect it later. 
      // this propagates the new block hash to the inode manifest.
      rc = UG_inode_dirty_block_commit( gateway, inode, block );
      if( rc != 0 ) {
         
         // failed 
         SG_error("UG_inode_dirty_block_commit( %" PRIX64 ".%" PRIu64" [%" PRId64 ".%" PRIu64 "] ) rc = %d\n", 
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
  
   SG_debug("Write %zu bytes at %jd\n", buf_len, offset );

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
   
   fskit_entry_rlock(fent);
   
   uint64_t file_id = fskit_entry_get_file_id( fent );
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   bool manifest_stale = SG_manifest_is_stale( UG_inode_manifest( inode ) );
   int64_t file_version = UG_inode_file_version( inode );
   uint64_t file_coordinator = UG_inode_coordinator_id( inode );
   int64_t manifest_mtime_sec = SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) );
   int32_t manifest_mtime_nsec = SG_manifest_get_modtime_nsec( UG_inode_manifest( inode ) );

   fskit_entry_unlock(fent);
  
   // ID of the last block written 
   uint64_t last_block_id = (offset + buf_len) / block_size;
   
   // handle supports write?
   if( (fh->flags & (O_WRONLY | O_RDWR)) == 0 ) {
      return -EBADF;
   }

   // make sure we're still the coordinator for this file
   rc = UG_consistency_inode_ensure_fresh( gateway, fs_path, inode );
   if( rc < 0 ) {

      SG_error("UG_consistency_inode_ensure_fresh( %" PRIX64 " ('%s')) rc = %d\n", file_id, fs_path, rc );
      return rc;
   }

   fskit_entry_wlock( fent );
   
   if( rc > 0 ) {
      
      // got new data, but does it indicate that we need a new manifest?
      if( UG_inode_file_version( inode ) != file_version ) {
         SG_debug("%" PRIX64 ": version change\n", file_id );
         manifest_stale = true;
      }
      if( UG_inode_coordinator_id( inode ) != file_coordinator ) {
         SG_debug("%" PRIX64 ": coordinator change\n", file_id );
         manifest_stale = true;
      
         if( manifest_mtime_sec != SG_manifest_get_modtime_sec( UG_inode_manifest( inode ) ) ) {
            SG_debug("%" PRIX64 ": manifest mtime_sec change\n", file_id );
            manifest_stale = true;
         }
         else if( manifest_mtime_nsec != SG_manifest_get_modtime_nsec( UG_inode_manifest( inode ) ) ) {
            SG_debug("%" PRIX64 ": manifest mtime_nsec change\n", file_id );
            manifest_stale = true;
         }
      }
   }
   else if( manifest_stale ) {
      SG_debug("%" PRIX64 ": manifest was marked stale\n", file_id );
   }

   if( manifest_stale ) {

      // manifest is not consistent with latest write,
      // we're not the coordinator, or the file was truncated.
      // make sure the manifest is fresh.
      SG_manifest_set_stale( UG_inode_manifest( inode ), true );
      fskit_entry_unlock( fent );

      rc = UG_consistency_manifest_ensure_fresh( gateway, fs_path );
      if( rc != 0 ) {

         SG_error("UG_consistency_manifest_ensure_fresh(%" PRIX64 " ('%s')) rc = %d\n", file_id, fs_path, rc );
         if( rc == -ENODATA ) {
            rc = -EIO;
         }
         return rc;
      }
   }
   else {

      // we're still the coordinator, and we have the freshest manifest
      fskit_entry_unlock( fent );
   }
   
   fskit_entry_wlock( fent );
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   uint64_t coordinator_id = UG_inode_coordinator_id( inode );
   
   // get unaligned blocks 
   // TODO: is there a way we can avoid keeping fent locked through all of this?
   rc = UG_write_read_partial_blocks( gateway, fs_path, inode, buf_len, offset, &write_blocks );
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      SG_error("UG_write_read_unaligned_blocks( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      
      return rc;
   }

   // merge data into unaligned blocks 
   rc = UG_write_partial_merge_data( buf, buf_len, offset, block_size, &write_blocks );
   if( rc != 0 ) {
       
      fskit_entry_unlock( fent );
      
      // bug 
      SG_error("BUG: UG_write_unaligned_merge_data( %s, %zu, %jd ) rc = %d\n", fs_path, buf_len, offset, rc );
      exit(1);
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
   // Instead, just add it to the inode's set of dirty blocks.
   // Do not commit it.
   UG_dirty_block_map_t::iterator itr = write_blocks.find( last_block_id );
   if( itr != write_blocks.end() && UG_dirty_block_in_RAM( &itr->second ) ) {
      
      struct UG_dirty_block* last_dirty_block = &itr->second;
   
      SG_debug("Keep in RAM block %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
             UG_inode_file_id( inode ), UG_dirty_block_id( last_dirty_block ), UG_dirty_block_version( last_dirty_block ) );

      // put into dirty blocks
      rc = UG_inode_dirty_block_put( gateway, inode, last_dirty_block, true );
      if( rc != 0 ) {

        SG_error("UG_ionde_dirty_block_put( %" PRIX64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n",
             UG_inode_file_id( inode ), UG_dirty_block_id( last_dirty_block ), UG_dirty_block_version( last_dirty_block ), rc );

        fskit_entry_unlock( fent );
        UG_dirty_block_map_free( &write_blocks );
        return -EIO;
      }

      write_blocks.erase( itr );
   }

   // flush the rest of the written blocks, and synchronize them with the manifest.
   // back up the old dirty block and old replicated block data, so we can evict and vacuum them (respectively)
   while( write_blocks.size() > 0 ) {
      
      // merge written blocks into the inode, including their new hashes 
      // NOTE: moves contents of write_blocks into the inode, or frees them
      rc = UG_write_dirty_blocks_merge( gateway, fs_path, inode, &write_blocks, offset, buf_len, true );
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
   UG_inode_preserve_old_manifest_modtime( inode );

   if( coordinator_id == gateway_id ) {
      
      // we're the coordinator--advance the manifest's modtime and write nonce
      SG_manifest_set_modtime( UG_inode_manifest( inode ), ts.tv_sec, ts.tv_nsec );
      UG_write_nonce_update( inode );
   }
      
   // advance size 
   SG_debug("%" PRIX64 ": offset + buflen = %" PRIu64 ", fent size = %" PRIu64 "\n", UG_inode_file_id(inode), (uint64_t)(offset + buf_len), fskit_entry_get_size( fent ) );

   fskit_entry_set_size( fent, MAX( (unsigned)fskit_entry_get_size( fent ), (unsigned)(offset + buf_len) ) );
   SG_manifest_set_size( UG_inode_manifest( inode ), fskit_entry_get_size( fent ) ); 

   // will need to contact MS with new metadata
   UG_inode_set_dirty( inode, true );

   SG_debug("%" PRIX64 " has %zu dirty blocks, and is now %" PRIu64 " bytes\n", UG_inode_file_id( inode ), UG_inode_dirty_blocks( inode )->size(), fskit_entry_get_size( fent ) );
   
   fskit_entry_unlock( fent );
   if( rc >= 0 ) {
      return buf_len;
   }
   else {
      return rc;
   }
}


// patch an inode's manifest.  Evict affected dirty blocks, cached blocks, and garbage blocks (the latter if the dirty block that got evicted was responsible for its creation).
// Does not affect other metadata, like modtime or size
// return 0 on success 
// return -ENOMEM on OOM 
// return -EPERM if we're not the coordinator
// NOTE: inode->entry must be write-locked and ref'ed; it will be unlocked intermittently
int UG_write_patch_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct UG_inode* inode, struct SG_manifest* write_delta ) {
   
   int rc = 0;
   struct UG_replica_context* rctx = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   uint64_t file_id = UG_inode_file_id( inode );
   int64_t file_version = UG_inode_file_version( inode );
   struct timespec ts;

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
         SG_error("SG_manifest_patch( %" PRIX64 ".%" PRId64 " ) rc = %d\n", file_id, file_version, rc );
      }
      
      SG_manifest_free( &new_manifest );
      return rc;
   }

   // advance inode timestamp 
   clock_gettime( CLOCK_REALTIME, &ts );
   UG_write_timestamp_update( inode, &ts );
   UG_inode_preserve_old_manifest_modtime( inode );

   // we're the coordinator--advance the manifest's modtime and write nonce
   SG_manifest_set_modtime( UG_inode_manifest( inode ), ts.tv_sec, ts.tv_nsec );
   SG_manifest_set_modtime( &new_manifest, ts.tv_sec, ts.tv_nsec );
   UG_write_nonce_update( inode );

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
         SG_error("UG_replica_context_init( %" PRIX64 ".%" PRId64 " ) rc = %d\n", file_id, file_version, rc );
      }
      
      SG_manifest_free( &new_manifest );
      SG_safe_free( rctx );
      return rc;
   }
   
   fskit_entry_unlock( UG_inode_fskit_entry( inode ) );
   
   // do the replication 
   rc = UG_replicate( gateway, rctx );

   if( rc != 0 ) {
      
      // failed!
      SG_error("UG_replicate( %" PRIX64 ".%" PRId64 " ) rc = %d\n", file_id, file_version, rc );
      
      UG_replica_context_free( rctx );
      SG_safe_free( rctx );
      SG_manifest_free( &new_manifest );
      return rc;
   }

   // success!
   UG_replica_context_free( rctx );
   SG_safe_free( rctx );
  
   // NOTE: safe, since inode->entry is ref'ed by the caller
   fskit_entry_wlock( UG_inode_fskit_entry( inode ) ); 

   rc = UG_inode_manifest_merge_blocks( gateway, inode, &new_manifest );
   SG_manifest_free( &new_manifest );

   if( rc != 0 ) {

      SG_error("UG_inode_manifest_merge_blocks(%" PRIX64 ".%" PRId64 ") rc = %d\n", file_id, file_version, rc );
   }
   
   return rc;
}
