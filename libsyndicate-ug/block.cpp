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


// set a dirty block's buffer.  Use with care.
// only works if the block is *not* shared/RAM-allocated.  If unshared, frees the buffer first.
int UG_dirty_block_set_buf( struct UG_dirty_block* dest, struct SG_chunk* new_buf ) {

   if( !UG_dirty_block_unshared( dest ) && UG_dirty_block_in_RAM( dest ) ) {
      SG_error("%s", "BUG: dirty block not in RAM\n");
      exit(1);
   }

   if( UG_dirty_block_unshared( dest ) ) {
      SG_chunk_free( &dest->buf );
   }

   dest->buf = *new_buf;
   return 0;
}

// set version 
int UG_dirty_block_set_version( struct UG_dirty_block* blk, int64_t version ) {
   SG_manifest_block_set_version( &blk->info, version );
   return 0;
}

// load a block from the cache, into dirty_block->buf
// if dirty_block->buf is allocated, this loads the deserialized block directly into it.
// If it is not allocated, it will be with malloc.
// transform it using the driver
// do NOT mark it dirty.
// dirty_block must be instantiated, but must not be in RAM
// return 0 on success
// return -ENOENT if not cached 
// return -EIO if we failed to access the cache
// return -ENOMEM on OOM
// return -EINVAL if dirty_block is in RAM
// return -ENODATA if we failed to serialize the block
int UG_dirty_block_load_from_cache( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, uint64_t file_version, struct UG_dirty_block* dirty_block, struct SG_IO_hints* io_hints ) {
   
   int rc = 0;
   struct SG_request_data reqdat;
   
   struct SG_chunk raw_block;
   struct SG_chunk block_buf;
   struct SG_chunk* buf_ptr = NULL;

   memset( &raw_block, 0, sizeof(struct SG_chunk) );
   memset( &block_buf, 0, sizeof(struct SG_chunk) );

   if( UG_dirty_block_is_flushed( dirty_block ) || UG_dirty_block_mmaped( dirty_block ) ) {

      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] flushed or mmap'ed\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
   }

   if( UG_dirty_block_in_RAM( dirty_block ) ) {

      // load directly 
      buf_ptr = UG_dirty_block_buf( dirty_block );
   }
   else {

      // implementation must allocate 
      buf_ptr = &block_buf;
   }

   rc = SG_request_data_init_block( gateway, fs_path, file_id, file_version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), &reqdat );
   if( rc != 0 ) {
      return rc;
   }

   SG_request_data_set_IO_hints( &reqdat, io_hints );

   // fetch serialized block from cache 
   rc = SG_gateway_cached_block_get_raw( gateway, &reqdat, &raw_block );
   
   if( rc != 0 ) {

      SG_error("SG_gateway_cached_block_get_raw( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                   file_id, file_version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

      SG_request_data_free( &reqdat );

      if( rc == -ENOENT) {
         return -ENOENT;
      }
      else {
         return -EIO;
      }
   }

   // deserialize (might allocate buf_ptr)
   rc = SG_gateway_impl_deserialize( gateway, &reqdat, &raw_block, buf_ptr );

   SG_request_data_free( &reqdat );
   SG_chunk_free( &raw_block );

   if( rc == -ENOSYS ) {
      // no-op 
      rc = 0;
   }

   if( rc != 0 ) {
       
      SG_error("SG_gateway_impl_deserialize( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
                   file_id, file_version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );

      if( rc != -ENOMEM ) {
          rc = -ENODATA;
      }

      return rc;
   }

   // put into place, if not there already
   if( buf_ptr == &block_buf ) {
      UG_dirty_block_set_buf( dirty_block, buf_ptr );
   }

   return 0;
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
  
   // sanity check: can't mmap if in ram
   // should never happen. 
   if( !dirty_block->mmaped && dirty_block->buf.data != NULL) {
      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] still in RAM\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ));
      exit(1);
   }

   // sanity check: needs to be flushed
   if(  dirty_block->block_fd < 0 ) {
      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] not flushed to disk\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
   }

   if( dirty_block->mmaped ) {
      // already mmaped 
      return -EINVAL;
   }

   // how big?
   rc = fstat( dirty_block->block_fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fstat(%d) rc = %d\n", dirty_block->block_fd, rc );
      return rc;
   }
   
   buf = (char*)mmap( NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, dirty_block->block_fd, 0 );
   if( buf == MAP_FAILED ) {
      
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


// set the unshared flag on a dirty block
// this is the case if we gift data into a block
// always succeeds 
int UG_dirty_block_set_unshared( struct UG_dirty_block* dirty_block, bool unshared ) {

   dirty_block->unshared = true;
   return 0;
}


// flush a dirty block from RAM to disk.
// return 0 on success, put the cache-write future into *dirty_block, and re-calculate the hash over the block's driver-serialized form
// return -EINPROGRESS if this block is already being flushed
// return -EINVAL if the block was already flushed, or is not in RAM, or is not dirty
// return -ENODATA if we failed to serialize the block 
// return -errno on cache failure
// NOTE: be careful not to free dirty_block until the future has been finalized!
// NOTE: not thread-safe--don't try flushing the same block twice
int UG_dirty_block_flush_async( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, struct UG_dirty_block* dirty_block, struct SG_IO_hints* io_hints ) {
   
   int rc = 0;
   struct md_cache_block_future* fut = NULL;
   struct SG_request_data reqdat;
   struct SG_chunk serialized_data;
   
   if( dirty_block->block_fut != NULL ) {
      
      // in progress
      return -EINPROGRESS;
   }
   
   if( !UG_dirty_block_in_RAM( dirty_block ) ) {
      // can't flush
      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not in RAM\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
   }

   if( UG_dirty_block_mmaped( dirty_block ) || UG_dirty_block_is_flushed( dirty_block ) ) {
      
      // already on disk
      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is flushed to disk already\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
   }
   
   if( !dirty_block->dirty ) {
      
      // nothing to do 
      SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not dirty\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ) );
      exit(1);
   }
  
   // synthesize a block request
   rc = SG_request_data_init_block( gateway, fs_path, file_id, file_version, UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), &reqdat );
   if( rc != 0 ) {

      SG_error("SG_request_data_init rc = %d\n", rc );
      return rc;
   }
    
   // serialize and update hash 
   rc = UG_dirty_block_serialize( gateway, &reqdat, dirty_block, io_hints, &serialized_data );
   if( rc != 0 ) {

      SG_error("UG_dirty_block_serialize([%" PRIu64 ".%" PRId64 "]) rc = %d\n", UG_dirty_block_id( dirty_block ), UG_dirty_block_version( dirty_block ), rc );
      SG_request_data_free( &reqdat );
      return -ENODATA;
   }
    
   // gift the serialized data to the cache
   rc = SG_gateway_cached_block_put_raw_async( gateway, &reqdat, &serialized_data, SG_CACHE_FLAG_UNSHARED, &fut );
   SG_request_data_free( &reqdat );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_cached_block_put_raw_async( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", 
               file_id, file_version, dirty_block->info.block_id, dirty_block->info.block_version, rc );

      SG_chunk_free( &serialized_data );
   }
   
   else {
      
      dirty_block->block_fut = fut;
   }
   
   return rc;
}


// wait for a block to get flushed.  If the block is not dirty and is not flushing, return 0.
// if the flush succeeds, then set dirty_block->block_fd to the fd on disk to the flushed block, and update the block's hash.
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
               md_cache_block_future_file_id( block_fut ), md_cache_block_future_file_version( block_fut ), md_cache_block_future_block_id( block_fut ), md_cache_block_future_block_version( block_fut ), rc );
      
      return rc;
   }
   
   // detach the file descriptor from the future, and put it into the dirty block (in order to keep the data referenced) 
   block_fd = md_cache_block_future_release_fd( block_fut );
   if( block_fd < 0 ) {
      
      SG_error("md_cache_block_future_release_fd( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", 
               md_cache_block_future_file_id( block_fut ), md_cache_block_future_file_version( block_fut ), md_cache_block_future_block_id( block_fut ), md_cache_block_future_block_version( block_fut ), block_fd );
      
      return block_fd;
   }

   dirty_block->block_fd = block_fd;
   
   if( free_chunk && !dirty_block->mmaped && dirty_block->unshared ) {
      SG_chunk_free( &dirty_block->buf );
   }
   
   md_cache_block_future_free( block_fut );
   
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


// given an offset and a write length, find the IDs of the first aligned block and last aligned block.
// that is, the IDs of the first and last block that correspond to whole blocks in the range [offset, offset + buf_len].
// always succeeds 
int UG_dirty_block_aligned( off_t offset, size_t buf_len, uint64_t block_size, uint64_t* aligned_start_id, uint64_t* aligned_end_id, off_t* aligned_start_offset, off_t* last_block_len ) {
   
   int rc = 0;
   
   uint64_t first_affected_block = offset / block_size;
   uint64_t first_aligned_block = 0;
   off_t first_aligned_block_offset = 0;  // offset into buf where the first aligned block starts
   
   uint64_t last_aligned_block = 0;
   uint64_t last_block_overflow = 0;
   
   // is the first block aligned?
   if( offset > 0 && (offset % block_size) != 0 ) {
      
      // nope--it's the next one 
      first_aligned_block = first_affected_block + 1;
      
      first_aligned_block_offset = block_size - (offset % block_size);
   }
   else {
      
      // yup--aligned 
      first_aligned_block = first_affected_block;
      
      first_aligned_block_offset = 0;
   }
  
   last_aligned_block = (offset + buf_len) / block_size;
   if( last_aligned_block > 0 ) {
      last_aligned_block--;
   }

   // if the last whole block ends at a block boundry, then the block overflow is 
   // exactly one block and the last whole block is the preceding block.
   last_block_overflow = (offset + buf_len) % block_size;
   if( last_block_overflow == 0 ) {
      last_block_overflow = block_size;
      if( last_aligned_block > 0 ) {
         last_aligned_block--;
      }
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

   if( last_block_len != NULL ) {

      *last_block_len = last_block_overflow;
   }
   
   return rc;
}


// evict and free a dirty block 
// always succeeds
int UG_dirty_block_evict_and_free( struct md_syndicate_cache* cache, struct UG_inode* inode, struct UG_dirty_block* block ) {
   
   // evict, if needed
   md_cache_evict_block( cache, UG_inode_file_id( inode ), UG_inode_file_version( inode ), UG_dirty_block_id( block ), UG_dirty_block_version( block ) );
   
   // free up
   UG_dirty_block_free( block );
   return 0;
}


// getters
uint64_t UG_dirty_block_id( struct UG_dirty_block* blk ) {
   return blk->info.block_id;
}

int64_t UG_dirty_block_version( struct UG_dirty_block* blk ) {
   return blk->info.block_version;
}

// NOTE: can only be called once the block has been (re)hashed
int UG_dirty_block_hash_buf( struct UG_dirty_block* blk, unsigned char* hash_buf ) {

   if( SG_manifest_block_hash( &blk->info ) == NULL ) {
      SG_error("BUG: hash for block [%" PRIu64 ".%" PRId64 "] is NULL\n", UG_dirty_block_id( blk ), UG_dirty_block_version( blk ) );
      exit(1);
   }

   memcpy( hash_buf, SG_manifest_block_hash( &blk->info ), SG_BLOCK_HASH_LEN );
   return 0;
}
   
struct SG_chunk* UG_dirty_block_buf( struct UG_dirty_block* blk ) {
   return &blk->buf;
}

int UG_dirty_block_fd( struct UG_dirty_block* blk ) {
   return blk->block_fd;
}

struct SG_manifest_block* UG_dirty_block_info( struct UG_dirty_block* blk ) {
   return &blk->info;
}

bool UG_dirty_block_unshared( struct UG_dirty_block* blk ) {
   return blk->unshared;
}

bool UG_dirty_block_dirty( struct UG_dirty_block* blk ) {
   return blk->dirty;
}

bool UG_dirty_block_is_flushing( struct UG_dirty_block* blk ) {
   return (blk->block_fut) != NULL;
}

bool UG_dirty_block_is_flushed( struct UG_dirty_block* blk ) {
   return (blk->block_fd >= 0);
}

bool UG_dirty_block_mmaped( struct UG_dirty_block* blk ) {
   return blk->mmaped;
}

bool UG_dirty_block_in_RAM( struct UG_dirty_block* blk ) {
   return (blk->buf.data != NULL);
}


// re-calculate the hash of the block
// the block must be resident in memory, but not mmap'ed
// store it into its block info
// NOT ATOMIC
// return 0 on success
// return -ENOMEM on OOM
int UG_dirty_block_rehash( struct UG_dirty_block* blk, char const* serialized_data, size_t serialized_data_len ) {

   unsigned char* hash = NULL;

   hash = SG_CALLOC( unsigned char, SHA256_DIGEST_LENGTH);
   if( hash == NULL ) {
      return -ENOMEM;
   }

   sha256_hash_buf( serialized_data, serialized_data_len, hash );
   SG_manifest_block_set_hash( &blk->info, hash );

   char hash_str[2*SG_BLOCK_HASH_LEN + 1];
   memset( hash_str, 0, 2*SG_BLOCK_HASH_LEN+1 );

   sha256_printable_buf( hash, hash_str );
   SG_debug("Hash of block [%" PRIu64 ".%" PRId64 "] (%p) is now %s\n", UG_dirty_block_id( blk ), UG_dirty_block_version( blk ), blk, hash_str );
   return 0;
}


// serialize a block, and update its hash 
// the block must be resident in memory, but not mmaped
// return 0 on success
// return -ENOMEM on OOM 
int UG_dirty_block_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct UG_dirty_block* block, struct SG_IO_hints* io_hints, struct SG_chunk* serialized_data ) {

   int rc = 0;

   // sanity check: must be in RAM 
   if( !UG_dirty_block_in_RAM( block ) ) {
     SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] is not in RAM\n", UG_dirty_block_id( block ), UG_dirty_block_version( block ) );
     exit(1);
   } 

   memset( serialized_data, 0, sizeof(struct SG_chunk) );

   // serialize the block
   rc = SG_gateway_impl_serialize( gateway, reqdat, UG_dirty_block_buf( block ), serialized_data );

   if( rc != 0 && rc != -ENOSYS ) {

      SG_error("UG_impl_block_serialize([%" PRIu64 ".%" PRId64 "]) rc = %d\n", UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
      return rc;
   }
   else {
      rc = 0;
   }
   
   // calculate the new block hash 
   rc = UG_dirty_block_rehash( block, serialized_data->data, serialized_data->len );

   if( rc != 0 ) {

      SG_error("UG_dirty_block_rehash([%" PRIu64 ".%" PRId64 "]) rc = %d\n", UG_dirty_block_id( block ), UG_dirty_block_version( block ), rc );
      SG_chunk_free( serialized_data );
      return rc;
   }

   return 0;
}


