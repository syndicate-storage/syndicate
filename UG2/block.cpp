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

#include "block.h"
#include "inode.h"

// init dirty block by copying in a buffer
// return 0 on success
// return -ENOMEM on OOM 
int UG_dirty_block_init_ram( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, char const* buf, size_t buflen ) {
   
   int rc = 0;
   char* buf_dup = NULL;
   
   memset( dirty_block, 0, sizeof(struct UG_dirty_block) );
   
   rc = SG_manifest_block_dup( &dirty_block->info, info );
   if( rc != 0 ) {
      
      return rc;
   }
   
   buf_dup = SG_CALLOC( char, buflen );
   if( buf_dup == NULL ) {
      
      SG_manifest_block_free( &dirty_block->info );
      return -ENOMEM;
   }
   
   SG_chunk_init( &dirty_block->buf, buf_dup, buflen );
   
   dirty_block->block_fd = -1;
   dirty_block->unshared = true;
   
   clock_gettime( CLOCK_MONOTONIC, &dirty_block->load_time );
   
   return 0;
}


// init dirty block by taking onwership of a buffer
// return 0 on success
// return -ENOMEM on OOM 
int UG_dirty_block_init_ram_nocopy( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, char* buf, size_t buflen ) {
   
   int rc = 0;
   
   memset( dirty_block, 0, sizeof(struct UG_dirty_block) );
   
   rc = SG_manifest_block_dup( &dirty_block->info, info );
   if( rc != 0 ) {
      
      return rc;
   }
   
   SG_chunk_init( &dirty_block->buf, buf, buflen );
   
   dirty_block->block_fd = -1;
   dirty_block->unshared = false;
   
   clock_gettime( CLOCK_MONOTONIC, &dirty_block->load_time );
   
   return 0;
}


// init dirty block from open file descriptor 
// return 0 on success
// return -ENOMEM on OOM 
int UG_dirty_block_init_fd( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, int block_fd ) {
   
   int rc = 0;
   
   memset( dirty_block, 0, sizeof(struct UG_dirty_block) );
   
   rc = SG_manifest_block_dup( &dirty_block->info, info );
   if( rc != 0 ) {
      
      return rc;
   }
   
   dirty_block->block_fd = block_fd;
   
   return 0;
}


// make a deep copy of a dirty block's memory 
// if dupfd is true, then duplicate the file descriptor as well if it is defined.
// return 0 on success 
// return -ENOMEM on OOM 
int UG_dirty_block_deepcopy( struct UG_dirty_block* dest, struct UG_dirty_block* src, bool dupfd ) {

   int rc = 0;
   
   memset( dest, 0, sizeof(struct UG_dirty_block) );
   
   rc = SG_manifest_block_dup( &dest->info, &src->info );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // mark dirty 
   SG_manifest_block_set_dirty( &dest->info, SG_manifest_block_is_dirty( &src->info ) );
   
   if( src->buf.data != NULL ) {
      
      char* buf_dup = SG_CALLOC( char, src->buf.len );
      if( buf_dup == NULL ) {
         
         SG_manifest_block_free( &dest->info );
         return -ENOMEM;
      }
      
      memcpy( buf_dup, src->buf.data, src->buf.len );
      
      SG_chunk_init( &dest->buf, buf_dup, src->buf.len );
   }
   
   if( src->block_fd >= 0 && dupfd ) {
      
      dest->block_fd = dup( src->block_fd );
      if( dest->block_fd < 0 ) {
         
         rc = -errno;
         
         SG_manifest_block_free( &dest->info );
         
         if( dest->buf.data ) {
            
            SG_chunk_free( &dest->buf );
         }
         
         return rc;
      }
   }
   
   clock_gettime( CLOCK_MONOTONIC, &dest->load_time );
   
   return rc;
}


// load a block from the cache, into dirty_block->buf
// do NOT mark it dirty.
// if the buffer is already allocated and is big enough, then copy the block data directly in.
// if the buffer is too small, or is NULL, then (re)allocate it.
// dirty_block must be instantiated
// return 0 on success
// return -ENOENT if not cached 
// return -ENOMEM on OOM
int UG_dirty_block_load_from_cache( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, uint64_t file_version, struct UG_dirty_block* dirty_block ) {
   
   int rc = 0;
   struct SG_request_data reqdat;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   bool free_on_failure = false;
   
   // sanity check: is the block buffer big enough?
   if( dirty_block->buf.data == NULL ) {
      
      char* buf_data = SG_CALLOC( char, block_size );
      if( buf_data == NULL ) {
         
         return -ENOMEM;
      }
      
      SG_chunk_init( &dirty_block->buf, buf_data, block_size );
      
      free_on_failure = true;
   }
   else if( (unsigned)dirty_block->buf.len < block_size ) {
      
      // try to expand 
      char* buf_data = (char*)realloc( dirty_block->buf.data, block_size );
      if( buf_data == NULL ) {
         
         return -ENOMEM;
      }
      
      SG_chunk_init( &dirty_block->buf, dirty_block->buf.data, block_size );
   }
   
   // zero-copy initialize a request
   SG_request_data_init_block( gateway, fs_path, file_id, file_version, UG_dirty_block_id( *dirty_block ), UG_dirty_block_version( *dirty_block ), &reqdat );
   
   rc = SG_gateway_cached_block_get( gateway, &reqdat, &dirty_block->buf );
   
   if( rc != 0 ) {
      
      if( rc != -ENOENT ) {
         
         // some other error 
         SG_error( "SG_gateway_cached_block_get( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                   file_id, file_version, UG_dirty_block_id( *dirty_block ), UG_dirty_block_version( *dirty_block ), rc );
      }
      
      if( free_on_failure ) {
         
         SG_chunk_free( &dirty_block->buf );
      }
      
      SG_request_data_free( &reqdat );
   }
   
   return rc;
}


// mmap a block as read/write memory
// required the buf data to be NULL, and the block to already be flushed to disk 
// return 0 on success
// return -EINVAL if buf is not NULL, or we're not flushed
// return -errno no mmap failure 
int UG_dirty_block_mmap( struct UG_dirty_block* dirty_block ) {
   
   int rc = 0;
   char* buf = NULL;
   struct stat sb;
   
   if( dirty_block->mmaped || dirty_block->buf.data != NULL || dirty_block->block_fd < 0 ) {
      
      return -EINVAL;
   }
   
   // how big?
   rc = fstat( dirty_block->block_fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fstat(%d) rc = %d\n", dirty_block->block_fd, rc );
      return rc;
   }
   
   buf = (char*)mmap( NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, dirty_block->block_fd, 0 );
   if( buf == NULL ) {
      
      rc = -errno;
      SG_error("mmap(%d) rc = %d\n", dirty_block->block_fd, rc );
      return rc;
   }
   
   dirty_block->mmaped = true;
   SG_chunk_init( &dirty_block->buf, buf, sb.st_size );
   
   return 0;
}


// munmap a block 
// return 0 on success
// return -EINVAL if not mmaped 
int UG_dirty_block_munmap( struct UG_dirty_block* dirty_block ) {
   
   int rc = 0;
   
   if( !dirty_block->mmaped ) {
      
      return -EINVAL;
   }
   
   rc = munmap( dirty_block->buf.data, dirty_block->buf.len );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("munmap(%p) rc = %d\n", dirty_block->buf.data, rc );
      return rc;
   }
   
   dirty_block->buf.data = NULL;
   dirty_block->buf.len = 0;
   
   dirty_block->mmaped = false;
   
   return 0;
}


// free dirty block 
// always succeeds
int UG_dirty_block_free( struct UG_dirty_block* dirty_block ) {
   
   SG_manifest_block_free( &dirty_block->info );
   
   if( !dirty_block->mmaped && dirty_block->unshared ) {
      
      SG_chunk_free( &dirty_block->buf );
   }
   else if( dirty_block->mmaped ) {
      
      UG_dirty_block_munmap( dirty_block );
   }
   
   if( dirty_block->block_fd >= 0 ) {
      
      close( dirty_block->block_fd );
   }
   
   return 0;
}


// free dirty block, but not the block data
// this is useful for recovering from errors, when we don't want to free the buffer passed into the dirty block
// always succeeds
int UG_dirty_block_free_keepbuf( struct UG_dirty_block* dirty_block ) {
   
   SG_manifest_block_free( &dirty_block->info );
   
   if( dirty_block->block_fd >= 0 ) {
      
      close( dirty_block->block_fd );
   }
   
   return 0;
}


// free a block map 
// always succeeds
int UG_dirty_block_map_free( UG_dirty_block_map_t* dirty_blocks ) {
   
   for( UG_dirty_block_map_t::iterator itr = dirty_blocks->begin(); itr != dirty_blocks->end(); itr++ ) {
      
      UG_dirty_block_free( &itr->second );
   }
   
   dirty_blocks->clear();
   return 0;
}


// free a block map, but don't touch the buffers 
// always succeeds
int UG_dirty_block_map_free_keepbuf( UG_dirty_block_map_t* dirty_blocks ) {
   
   for( UG_dirty_block_map_t::iterator itr = dirty_blocks->begin(); itr != dirty_blocks->end(); itr++ ) {
      
      UG_dirty_block_free_keepbuf( &itr->second );
   }
   
   dirty_blocks->clear();
   return 0;
}


// set the dirty flag on a dirty block 
// always succeeds
int UG_dirty_block_set_dirty( struct UG_dirty_block* dirty_block, bool dirty ) {
   
   dirty_block->dirty = dirty;
   return 0;
}


// flush a block to disk, if it is dirty.  If it is not dirty, do nothing and return 0
// if the block was in RAM, create a cache future and put it into *block_fut 
// if the block was on disk, then no action is taken.  *ret_block_fut will be set to NULL, and this method succeeds.
// return 0 on success
// return -EINPROGRESS if this block is already being flushed
// return -EINVAL if the block was already flushed, or is mmaped
// return -errno on cache failure
// NOTE: be careful not to free dirty_block until the future (*ret_block_fut) has been finalized!
int UG_dirty_block_flush_async( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, struct UG_dirty_block* dirty_block ) {
   
   int rc = 0;
   struct md_cache_block_future* fut = NULL;
   struct SG_request_data reqdat;
   
   if( dirty_block->block_fut != NULL ) {
      
      // in progress
      return -EINPROGRESS;
   }
   
   if( dirty_block->mmaped || dirty_block->block_fd >= 0 ) {
      
      // already on disk
      return -EINVAL;
   }
   
   if( !dirty_block->dirty ) {
      
      // nothing to do 
      return rc;
   }
   
   if( dirty_block->buf.data != NULL ) {
      
      // set up a reqdat 
      SG_request_data_init_block( gateway, fs_path, file_id, file_version, dirty_block->info.block_id, dirty_block->info.block_version, &reqdat );
      
      rc = SG_gateway_cached_block_put_async( gateway, &reqdat, &dirty_block->buf, 0, &fut );
      
      if( rc != 0 ) {
         
         SG_error("SG_gateway_cached_block_put_async( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", 
                  file_id, file_version, dirty_block->info.block_id, dirty_block->info.block_version, rc );
      }
      
      else {
         
         dirty_block->block_fut = fut;
      }
      
      SG_request_data_free( &reqdat );
   }
   
   return rc;
}
   
// wait for a block to get flushed.  If the block is not dirty and is not flushing, return 0.
// if the flush succeeds, then set dirty_block->block_fd to the fd on disk to the flushed block.
// if free_chunk is set, free dirty_block's RAM buffer as well if we successfully flush
// return 0 on success
// return -EINVAL if the block is dirty, but the block is not being flushed.
// return -errno on flush failure (in which case, none of the above side-effects occur)
int UG_dirty_block_flush_finish_ex( struct UG_dirty_block* dirty_block, bool free_chunk ) {
   
   int rc = 0;
   int block_fd = -1;
   
   struct md_cache_block_future* block_fut = dirty_block->block_fut;
   
   if( block_fut == NULL && dirty_block->dirty ) {
      
      // nothing to do
      return -EINVAL;
   }
   
   else if( !dirty_block->dirty && block_fut == NULL ) {
      
      // nothing to do 
      return 0;
   }
   
   rc = md_cache_flush_write( block_fut );
   if( rc != 0 ) {
      
      SG_error("md_cache_flush_write( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
               md_cache_block_future_file_id( *block_fut ), md_cache_block_future_file_version( *block_fut ), md_cache_block_future_block_id( *block_fut ), md_cache_block_future_block_version( *block_fut ), rc );
      
      return rc;
   }
   
   // detach the file descriptor from the future, and put it into the dirty block 
   block_fd = md_cache_block_future_release_fd( block_fut );
   if( block_fd < 0 ) {
      
      SG_error("md_cache_block_future_release_fd( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
               md_cache_block_future_file_id( *block_fut ), md_cache_block_future_file_version( *block_fut ), md_cache_block_future_block_id( *block_fut ), md_cache_block_future_block_version( *block_fut ), block_fd );
      
      return block_fd;
   }
   
   dirty_block->block_fd = block_fd;
   
   if( free_chunk && !dirty_block->mmaped && dirty_block->unshared ) {
      SG_chunk_free( &dirty_block->buf );
   }
   
   md_cache_block_future_free( block_fut );
   SG_safe_free( block_fut );
   
   dirty_block->block_fut = NULL;
   
   return 0;
}


// wait for a block to get flushed.
// on success, put the block future's fd into the dirty_block, and free the dirty block's memory
// return 0 on success
// return -errno on flush failure 
int UG_dirty_block_flush_finish( struct UG_dirty_block* dirty_block ) {
   
   return UG_dirty_block_flush_finish_ex( dirty_block, true );
}


// wait for a block to get flushed.
// don't free the associated chunk, if present.
// on success, put the block future's fd into the dirty_block, and free the dirty block's memory
// return 0 on success
// return -errno on flush failure 
int UG_dirty_block_flush_finish_keepbuf( struct UG_dirty_block* dirty_block ) {
   
   return UG_dirty_block_flush_finish_ex( dirty_block, false );
}


// unshare a block's buffer--make a private copy, and replace the buffer 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if there is no associated RAM buffer for this dirty block, or if this block was already unshared
int UG_dirty_block_buf_unshare( struct UG_dirty_block* dirty_block ) {
   
   int rc = 0;
   struct SG_chunk chunk_dup;
   
   if( dirty_block->buf.data == NULL ) {
      
      return -EINVAL;
   }
   
   if( dirty_block->unshared ) {
      
      return -EINVAL;
   }
   
   rc = SG_chunk_dup( &chunk_dup, &dirty_block->buf );
   if( rc != 0 ) {
      
      return rc;
   }
   
   dirty_block->buf = chunk_dup;
   dirty_block->unshared = true;
   
   clock_gettime( CLOCK_MONOTONIC, &dirty_block->load_time );
   
   return 0;
}


// given an offset and a write length, find the IDs of the first aligned block and last aligned block 
// always succeeds 
int UG_dirty_block_aligned( off_t offset, size_t buf_len, uint64_t block_size, uint64_t* aligned_start_id, uint64_t* aligned_end_id, off_t* aligned_start_offset ) {
   
   int rc = 0;
   
   uint64_t first_block = offset / block_size;
   uint64_t first_aligned_block = 0;
   off_t first_aligned_block_offset = 0;  // offset into buf where the first aligned block starts
   
   uint64_t last_aligned_block = 0;
   
   // is the first block aligned?
   if( offset > 0 && (offset % block_size) != 0 ) {
      
      // nope--it's the next one 
      first_aligned_block = first_block + 1;
      
      first_aligned_block_offset = block_size - (offset % block_size);
   }
   else {
      
      // yup--aligned 
      first_aligned_block = first_block;
      
      first_aligned_block_offset = 0;
   }
   
   // is the last block aligned?
   if( (offset + buf_len) > 0 && ((offset + buf_len) % block_size) != 0 ) {
      
      // nope--it's the previous one 
      last_aligned_block = (offset + buf_len) / block_size;
      
      // careful, overflow...
      if( last_aligned_block > 0 ) {
         last_aligned_block--;
      }
   }
   else {
      
      // yup--aligned 
      last_aligned_block = (offset + buf_len) / block_size;
   }
   
   if( aligned_start_id != NULL ) {
      
      *aligned_start_id = first_aligned_block;
   }
   
   if( aligned_end_id != NULL ) {
      
      *aligned_end_id = last_aligned_block;
   }
   
   if( aligned_start_offset != NULL ) {
      
      *aligned_start_offset = first_aligned_block_offset;
   }
   
   return rc;
}


// evict and free a dirty block 
// always succeeds
int UG_dirty_block_evict_and_free( struct md_syndicate_cache* cache, struct UG_inode* inode, struct UG_dirty_block* block ) {
   
   // evict, if needed
   md_cache_evict_block( cache, UG_inode_file_id( *inode ), UG_inode_file_version( *inode ), UG_dirty_block_id( *block ), UG_dirty_block_version( *block ) );
   
   // free up
   UG_dirty_block_free( block );
   return 0;
}
