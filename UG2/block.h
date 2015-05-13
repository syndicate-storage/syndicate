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

#ifndef _UG_BLOCK_H_
#define _UG_BLOCK_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>

// dirty block--a block fetched into RAM from the cache, network, etc.
// its RAM buffer will contain the deserialized (i.e. driver-processed) data 
struct UG_dirty_block {
   
   // block metadata
   struct SG_manifest_block info;
   
   // block data, if in RAM (buf.data == NULL if not)
   struct SG_chunk buf;
   
   // is the data modified? do we need to flush?
   bool dirty;
   
   // is the buf owned by this object?
   bool unshared;
   
   ///// for writing ///////
   
   // serialized block data, if on disk (== -1 if not)
   int block_fd;
   
   // flushing block future, if this block is flushing 
   struct md_cache_block_future* block_fut;
   
   // time loaded into RAM 
   struct timespec load_time;
   
   // did we mmap the on-disk data into buf?
   bool mmaped;
};

#define UG_dirty_block_id( blk ) (blk).info.block_id
#define UG_dirty_block_version( blk ) (blk).info.block_version
#define UG_dirty_block_buf( blk ) (blk).buf
#define UG_dirty_block_fd( blk ) (blk).block_fd
#define UG_dirty_block_info( blk ) &(blk).info
#define UG_dirty_block_unshared( blk ) (blk).unshared
#define UG_dirty_block_dirty( blk ) (blk).dirty
#define UG_dirty_block_is_flushing( blk ) ((blk).block_fut != NULL)
#define UG_dirty_block_mmaped( blk ) (blk).mmaped

// map block ID to dirty block to define a set of modified blocks across an inode's handles
typedef map< uint64_t, struct UG_dirty_block > UG_dirty_block_map_t;

// init dirty block 
int UG_dirty_block_init_ram( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, char const* buf, size_t buflen );
int UG_dirty_block_init_ram_nocopy( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, char* buf, size_t buflen );
int UG_dirty_block_init_fd( struct UG_dirty_block* dirty_block, struct SG_manifest_block* info, int block_fd );
int UG_dirty_block_deepcopy( struct UG_dirty_block* dest, struct UG_dirty_block* src, bool dupfd );

// mark dirty 
int UG_dirty_block_set_dirty( struct UG_dirty_block* dirty_block, bool dirty );

// get from disk cache 
int UG_dirty_block_load_from_cache( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, uint64_t file_version, struct UG_dirty_block* dirty_block );

// make private the instance of the block's buffer 
int UG_dirty_block_buf_unshare( struct UG_dirty_block* dirty_block );

// flush to disk cache
int UG_dirty_block_flush_async( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, struct UG_dirty_block* dirty_block );
int UG_dirty_block_flush_finish( struct UG_dirty_block* dirty_block );
int UG_dirty_block_flush_finish_keepbuf( struct UG_dirty_block* dirty_block );

// free dirty block 
int UG_dirty_block_free( struct UG_dirty_block* dirty_block );
int UG_dirty_block_free_keepbuf( struct UG_dirty_block* dirty_block );
int UG_dirty_block_evict_and_free( struct md_syndicate_cache* cache, struct UG_inode* inode, struct UG_dirty_block* block );

// free dirty block map 
int UG_dirty_block_map_free( UG_dirty_block_map_t* dirty_blocks );
int UG_dirty_block_map_free_keepbuf( UG_dirty_block_map_t* dirty_blocks );

// mmap 
int UG_dirty_block_mmap( struct UG_dirty_block* dirty_block );
int UG_dirty_block_munmap( struct UG_dirty_block* dirty_block );

// alignment 
int UG_dirty_block_aligned( off_t offset, size_t buf_len, uint64_t block_size, uint64_t* aligned_start_id, uint64_t* aligned_end_id, off_t* aligned_start_offset );

#endif 