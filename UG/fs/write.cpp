/*
   Copyright 2013 The Trustees of Princeton University

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
#include "replication.h"
#include "read.h"
#include "consistency.h"
#include "cache.h"
#include "driver.h"

// does a previous version of the block exist within a file?
bool fs_entry_has_old_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id ) {
   
   // no blocks exist
   if( fent->size == 0 )
      return false;
   
   // not in the manifest
   if( !fent->manifest->is_block_present( block_id ) )
      return false;
   
   // is it a write hole?
   if( fent->manifest->is_hole( block_id ) )
      return false;
   
   // present
   return true;
}


// does a previous version of the block need to be garbage-collected?
bool fs_entry_has_old_garbage_collectable_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id ) {
   
   // if there isn't an old version of this block anyway, then no
   if( !fs_entry_has_old_block( core, fent, block_id ) )
      return false;
   
   // if this block isn't waiting to be flushed, then no
   if( !fs_entry_has_dirty_block( fent, block_id ) )
      return false;
   
   return true;
}


// clear out all bufferred blocks affected by a write 
// fent must be write-locked
static int fs_entry_clear_bufferred_blocks( struct fs_entry* fent, modification_map* old_blocks ) {
   
   for( modification_map::iterator itr = old_blocks->begin(); itr != old_blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      
      fs_entry_clear_bufferred_block( fent, block_id );
   }
   
   return 0;
}

// write a block to a file (processing it first with the driver), asynchronously putting it on local storage, and updating the filesystem entry's manifest to refer to it.
// This updates the manifest's last-mod time, but not the fs_entry's
// return a cache_block_future for it.
// fent MUST BE WRITE LOCKED, SINCE WE MODIFY THE MANIFEST
struct cache_block_future* fs_entry_flush_block_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char const* block_data, size_t block_len, int* _rc ) {
   
   int64_t new_block_version = fs_entry_next_block_version();
   
   *_rc = 0;
   
   int rc = 0;
   
   // do pre-upload write processing...
   char* processed_block = NULL;
   size_t processed_block_len = 0;
   
   rc = driver_write_block_preup( core, core->closure, fs_path, fent, block_id, new_block_version, block_data, block_len, &processed_block, &processed_block_len );
   if( rc != 0 ) {
      errorf("driver_write_block_preup(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, new_block_version, rc );
      *_rc = rc;
      return NULL;
   }
   
   // hash the contents of this block and the processed block, including anything read with fs_entry_read_block
   unsigned char* block_hash = BLOCK_HASH_DATA( processed_block, processed_block_len );
   
   // for debugging...
   char prefix[21];
   memset( prefix, 0, 21 );
   memcpy( prefix, processed_block, MIN( 20, processed_block_len ) );
   
   // cache the new block.  Get back the future (caller will manage it).
   struct cache_block_future* f = fs_entry_cache_write_block_async( core, core->cache, fent->file_id, fent->version, block_id, new_block_version, processed_block, processed_block_len, false, &rc );
   if( f == NULL ) {
      errorf("WARN: failed to cache %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], rc = %dn", fent->file_id, fent->version, block_id, new_block_version, rc );
      *_rc = rc;
      free( block_hash );
      return NULL;
   }
   else {
      dbprintf("cache %zu bytes for %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]: data: '%s'...\n", processed_block_len, fent->file_id, fent->version, block_id, new_block_version, prefix );
      
      // update the manifest (including its lastmod)
      fs_entry_manifest_put_block( core, core->gateway, fent, block_id, new_block_version, block_hash );
      
      free( block_hash );
      
      return f;
   }
}

// does a partial head "complete" a block?  As in, does it write up to the end of the block?
bool fs_entry_head_completes_block( struct fs_core* core, struct fs_entry_partial_head* head ) {
   return (head->write_offset + head->write_len >= core->blocking_factor);
}


// split up a client write into write_vector: (partial_head, [list of overwritten blocks], partial_tail).
// partial_head refers to the partially-overwritten block at the beginning of the write.
// partial_tail refers to the partially-overwritten block at the end of the write; it will not be the same block as the block affected by the partial_head
// the list of overwritten blocks are the sequence of blocks that will be completely overwritten by the write.
// fent must be at least read-locked
int fs_entry_split_write( struct fs_core* core, struct fs_entry* fent, char const* buf, off_t offset, size_t len, struct fs_entry_write_vec* wvec ) {
   
   memset( wvec, 0, sizeof(struct fs_entry_write_vec) );
   
   // unpack for legibility
   struct fs_entry_partial_head* head = &wvec->head;
   struct fs_entry_partial_tail* tail = &wvec->tail;
   
   uint64_t start_block_id = fs_entry_block_id( core, offset );
   uint64_t end_block_id = fs_entry_block_id( core, offset + len );
   
   off_t buf_off = 0;
   
   // do we have a partial head?
   if( (offset % core->blocking_factor) != 0 || start_block_id == end_block_id ) {
      
      // fill in the head
      head->buf_ptr = buf;
      head->block_id = start_block_id;
      head->write_offset = (offset % core->blocking_factor);
      head->write_len = MIN( len, core->blocking_factor - head->write_offset );
      
      head->has_old_version = fs_entry_has_old_block( core, fent, start_block_id );
      if( head->has_old_version ) {
         head->block_version = fent->manifest->get_block_version( start_block_id );
      }
      
      wvec->has_head = true;
      
      // align the next blocks to the block boundary 
      buf_off = core->blocking_factor - (offset % core->blocking_factor);
      
      ////////////////////////////////////////////////////////////
      char* tmp = CALLOC_LIST( char, head->write_len + 1 );
      memcpy( tmp, head->buf_ptr, head->write_len );
      
      dbprintf("Partial head: block %" PRIu64 " [%zu] offset %zu, data '%s'\n", head->block_id, head->write_len, head->write_offset, tmp );
      
      free( tmp );
      ////////////////////////////////////////////////////////////
   }
   
   // do we have a partial tail?
   if( ((offset + len) % core->blocking_factor) != 0 && end_block_id > start_block_id ) {
      
      // fill in the tail.
      // NOTE: if we have a head, then don't count the first block 
      off_t tail_off = 0;
      if( wvec->has_head )
         tail_off = buf_off + ((end_block_id - start_block_id - 1) * core->blocking_factor);
      else 
         tail_off = buf_off + ((end_block_id - start_block_id) * core->blocking_factor);
      
      tail->buf_ptr = buf + tail_off;
      tail->block_id = end_block_id;
      tail->write_len = (offset + len) % core->blocking_factor;
      
      tail->has_old_version = fs_entry_has_old_block( core, fent, end_block_id );
      if( tail->has_old_version ) {
         tail->block_version = fent->manifest->get_block_version( end_block_id );
      }
      
      wvec->has_tail = true;
      
      ////////////////////////////////////////////////////////////
      char* tmp = CALLOC_LIST( char, tail->write_len + 1 );
      memcpy( tmp, tail->buf_ptr, tail->write_len );
      
      dbprintf("Partial tail: block %" PRIu64 " [%zu], data '%s'\n", tail->block_id, tail->write_len, tmp );
      
      free( tmp );
      ////////////////////////////////////////////////////////////
   }
   
   // do we have overwritten blocks?
   uint64_t overwrite_start = start_block_id;
   uint64_t overwrite_end = end_block_id;
   
   if( wvec->has_head ) {
      // have a partial_head; exclude the first affected block 
      overwrite_start ++;
   }
   
   if( wvec->has_tail || (offset + len) % core->blocking_factor == 0 ) {
      // have a partial_tail, exclude the last affeted block and exclude 0-length tails 
      overwrite_end --;
   }
   
   if( overwrite_start <= overwrite_end ) {
      
      // will overwrite at least one block 
      wvec->overwritten = new fs_entry_whole_block_list_t();
      
      for( uint64_t block_id = overwrite_start; block_id <= overwrite_end; block_id++ ) {
         
         // whole-block overwrite
         struct fs_entry_whole_block whole_block;
         memset( &whole_block, 0, sizeof(struct fs_entry_whole_block) );
         
         whole_block.buf_ptr = buf + buf_off + (block_id - overwrite_start) * core->blocking_factor;
         whole_block.block_id = block_id;
         
         wvec->overwritten->push_back( whole_block );
         
         ////////////////////////////////////////////////////////////
         char* tmp = CALLOC_LIST( char, core->blocking_factor + 1 );
         memcpy( tmp, whole_block.buf_ptr, core->blocking_factor );
         
         dbprintf("Whole block: block %" PRIu64 ", data '%s'\n", block_id, tmp );
            
         free( tmp );
         ////////////////////////////////////////////////////////////
      }
   }
   
   wvec->start_block_id = start_block_id;
   wvec->end_block_id = end_block_id;
   
   return 0;
}


// destroy a write vector 
int fs_entry_write_vec_free( struct fs_entry_write_vec* wvec ) {
   if( wvec->overwritten ) {
      delete wvec->overwritten;
      wvec->overwritten = NULL;
   }
   memset( wvec, 0, sizeof(struct fs_entry_write_vec) );
   
   return 0;
}


// write a partial head to an existing block buffer.  block must be at least core->blocking_factor bytes long
// fent must be write-locked
int fs_entry_apply_partial_head( char* block, struct fs_entry_partial_head* head ) {
   
   // sanity check
   if( head->buf_ptr == NULL )
      return -EINVAL;
   
   // apply the data from the head
   memcpy( block + head->write_offset, head->buf_ptr, head->write_len );
   return 0;
}


// write a partial tail to an existing block buffer.  block must be at least core->blocking_factor bytes long
// fent must be read-locked
int fs_entry_apply_partial_tail( char* block, struct fs_entry_partial_tail* tail ) {
   
   // sanity check 
   if( tail->buf_ptr == NULL )
      return -EINVAL;
   
   // apply the data from the tail 
   memcpy( block, tail->buf_ptr, tail->write_len );
   return 0;
}

// put any write holes up to the start of the write.
// fent must be write-locked
int fs_entry_put_write_holes( struct fs_core* core, struct fs_entry* fent, off_t offset ) {
   // will there be holes?
   if( fent->size >= offset )
      return 0;
   
   uint64_t start_hole_id = (fent->size / core->blocking_factor) + ((fent->size % core->blocking_factor) != 0 ? 1 : 0);
   uint64_t end_hole_id = (offset / core->blocking_factor);
   
   dbprintf("fent->size = %jd, offset = %jd, so blocks [%" PRIu64 ",%" PRIu64 ") are holes\n", fent->size, offset, start_hole_id, end_hole_id );
   
   // put each hole
   for( uint64_t hole_id = start_hole_id; hole_id < end_hole_id; hole_id++ ) {
      fent->manifest->put_hole( core, fent, hole_id );
   }
   
   return 0;
}

// Write one block of data for a local file:  process it and flush it to disk cache
// Return a cache future on success; NULL on failure (and set the error code in ret).
// fent must be write-locked--we modify the manifest
// On success:
// return 0 or 1 (via ret)
// if there is an old block, store it to binfo_old and return 1.  Otherwise, return 0 via ret.
// store the new block info to binfo_new.
// fent must be write-locked--we'll update the manifest
struct cache_block_future* fs_entry_write_block_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char const* block, size_t block_len,
                                                       struct fs_entry_block_info* binfo_old, struct fs_entry_block_info* binfo_new, int* ret ) {

   *ret = 0;
   
   bool has_old_version = fs_entry_has_old_block( core, fent, block_id );
   
   // need to get the old version of this block, so we can garbage-collect it later (i.e. on last close)
   int64_t old_version = 0;
   unsigned char* old_hash = NULL;
   size_t old_hash_len = 0;
   
   if( has_old_version ) {
      // there's an old version of this block
      old_version = fent->manifest->get_block_version( block_id );
      old_hash = fent->manifest->get_block_hash( block_id );
      old_hash_len = BLOCK_HASH_LEN();
   }
   
   // write the data and update the manifest...
   int rc = 0;
   struct cache_block_future* block_fut = fs_entry_flush_block_async( core, fs_path, fent, block_id, block, block_len, &rc );
   
   if( block_fut == NULL ) {
      errorf("ERR: fs_entry_flush_block_async(%s/%" PRId64 ", block_len=%zu) failed, rc = %d\n", fs_path, block_id, block_len, rc );
      *ret = -EIO;
      return NULL;
   }
   
   // get the hash of the newly-written block...
   unsigned char* new_hash = fent->manifest->get_block_hash( block_id );
   int64_t new_version = fent->manifest->get_block_version( block_id );
   
   // store information about the old and new block
   // NOTE: we're passing the block_fd into the fs_entry_block_info structure from the cache_block_future.  DO NOT CLOSE IT!
   // NOTE: don't free the hash--pass it in
   fs_entry_block_info_replicate_init( binfo_new, new_version, new_hash, BLOCK_HASH_LEN(), core->gateway, block_fut->block_fd );
   
   if( has_old_version ) {
      fs_entry_block_info_garbage_init( binfo_old, old_version, old_hash, old_hash_len, core->gateway );
      *ret = 1;
   }
   else {
      *ret = 0;
   }
   
   ////////////////////////////////
   char* hash_printable = BLOCK_HASH_TO_STRING( new_hash );
   dbprintf("hash of %" PRIX64 "[%" PRId64 ".%" PRIu64 "] is %s, block_len=%zu\n", fent->file_id, block_id, new_version, hash_printable, block_len );
   free( hash_printable );
   ////////////////////////////////
   
   return block_fut;
}


// get the partially-overwritten blocks' data, so we can put a full block.
// fent must be at least read-locked, but we need to indicate if it is write-locked (since the downloader needs to know)
// return 0 on success
// return -ENODATA if not all data could be obtained
int fs_entry_read_partial_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, bool write_locked, struct fs_entry_read_context* read_ctx ) {
   
   int rc = 0;
   
   // get blocks from local sources
   rc = fs_entry_read_context_run_local( core, fs_path, fent, read_ctx );
   
   if( rc != 0 && rc != -EREMOTE ) {
      // failed to get from local sources
      errorf("fs_entry_try_read_block_local(%s %" PRIX64 ".%" PRId64 ") rc = %d\n", fs_path, fent->file_id, fent->version, rc );
      
      return -ENODATA;
   }
   
   else if( rc == -EREMOTE ) {
      // at least one block was remote. Set up downloads 
      rc = fs_entry_read_context_setup_downloads( core, fent, read_ctx );
      if( rc != 0 ) {
         // failed...
         errorf("fs_entry_read_context_setup_downloads( %s ) rc = %d\n", fs_path, rc );
         return -ENODATA;
      }
      
      // Go get it/them.
      while( fs_entry_read_context_has_downloading_blocks( read_ctx ) ) {
         
         // get some data
         rc = fs_entry_read_context_run_downloads_ex( core, fent, read_ctx, write_locked, NULL, NULL );
         if( rc != 0 ) {
            errorf("fs_entry_read_context_run_downloads( %s ) rc = %d\n", fs_path, rc );
            return -ENODATA;
         }
      }
   }
   
   return 0;
}


// start writing all whole blocks to the cache
// record the old and new versions of the blocks as we do so.
// fent must be write-locked
// return 0 on success.
// return negative and fail fast otherwise
// fent must be write-locked--we'll update the manifest, and clear out bufferred blocks
static int fs_entry_write_full_blocks_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, fs_entry_whole_block_list_t* whole_blocks,
                                             modification_map* old_blocks, modification_map* new_blocks, vector<struct cache_block_future*>* block_futs ) {
   
   int rc = 0;
   
   dbprintf("write %zu whole blocks\n", whole_blocks->size() );
   
   // flush each one
   for( fs_entry_whole_block_list_t::iterator itr = whole_blocks->begin(); itr != whole_blocks->end(); itr++ ) {
      
      // old and new block info 
      struct fs_entry_block_info old_binfo, new_binfo;
      
      memset( &old_binfo, 0, sizeof(old_binfo) );
      memset( &new_binfo, 0, sizeof(new_binfo) );
      
      // get the block 
      struct fs_entry_whole_block* blk = &(*itr);
      
      // flush it 
      struct cache_block_future* fut = fs_entry_write_block_async( core, fs_path, fent, blk->block_id, blk->buf_ptr, core->blocking_factor, &old_binfo, &new_binfo, &rc );
      if( rc < 0 || fut == NULL ) {
         errorf("fs_entry_write_block_async( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "]) rc = %d\n", fs_path, fent->file_id, fent->version, blk->block_id, rc );
         break;
      }
      
      // remember the new block info 
      (*new_blocks)[ blk->block_id ] = new_binfo;
      
      // remember the old information, if there was any.
      if( rc > 0 ) {
         (*old_blocks)[ blk->block_id ] = old_binfo;
         rc = 0;
      }
      
      // remember the future 
      block_futs->push_back( fut );
   }
   
   return rc;
}

// set up a read future for getting a partially-written block's data.
// return the read buffer
static char* fs_entry_setup_partial_read_future( struct fs_core* core, struct fs_entry_read_block_future* read_fut,
                                                 uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, off_t file_size ) {
   
   char* block_buf = CALLOC_LIST( char, core->blocking_factor );
   
   // get the block
   fs_entry_read_block_future_init( read_fut, gateway_id, fs_path, file_version, block_id, block_version, block_buf, core->blocking_factor, 0, core->blocking_factor, true );
   
   return block_buf;
}


// evict a set of blocks, asynchronously
// fent must be read-locked
// always succeeds (returns 0)
int fs_entry_cache_evict_blocks_async( struct fs_core* core, struct fs_entry* fent, modification_map* blocks ) {
   
   // each block...
   for( modification_map::iterator itr = blocks->begin(); itr != blocks->end(); itr++ ) {
      
      // do the eviction
      int evict_rc = fs_entry_cache_evict_block_async( core, core->cache, fent->file_id, fent->version, itr->first, itr->second.version );
      
      if( evict_rc != 0 && evict_rc != -ENOENT ) {
         errorf("fs_entry_cache_evict_block_async( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", fent->file_id, fent->version, itr->first, itr->second.version, evict_rc );
      }
   }
   
   return 0;
}

// apply partial blocks to existing block data.
// block_head and block_tail are full-sized blocks that need to be patched
// if we patch the head to the point where it forms a complete block, then put it into the overwritten list
static int fs_entry_apply_partial_blocks( struct fs_core* core, struct fs_entry_write_vec* wvec, char* block_head, char* block_tail, fs_entry_whole_block_list_t* overwritten ) {

   if( block_head ) {
      // write the head to the buffer
      fs_entry_apply_partial_head( block_head, &wvec->head );
      
      // has this block been made whole?
      if( fs_entry_head_completes_block( core, &wvec->head ) ) {
         
         dbprintf("partial head %" PRIu64 " is now a whole block\n", wvec->head.block_id );
         
         // the head block is now a full block (which we allocated)
         struct fs_entry_whole_block full_head;
         
         full_head.block_id = wvec->head.block_id;
         full_head.buf_ptr = block_head;
         
         overwritten->push_back( full_head );
      }
   }
   else {
      dbprintf("%s", "No partial head to read\n");
   }
   
   if( block_tail ) {
      // write the tail 
      fs_entry_apply_partial_tail( block_tail, &wvec->tail );
   }
   else {
      dbprintf("%s", "No partial tail to read\n");
   }

   return 0;
}


// put a block version for a bufferred block, putting the new version and hash into the manifest
// fent must be write-locked
static int fs_entry_manifest_put_bufferred_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id ) {
   // update manifest 
   int64_t new_version = fs_entry_next_block_version();
   
   unsigned char* block_hash = NULL;
   size_t block_hash_len = 0;
   
   int rc = fs_entry_hash_bufferred_block( fent, block_id, &block_hash, &block_hash_len );
   if( rc != 0 ) {
      errorf("fs_entry_hash_bufferred_block( %" PRIu64 " ) rc = %d\n", block_id, rc );
      return rc;
   }
   
   fs_entry_manifest_put_block( core, core->gateway, fent, block_id, new_version, block_hash );
   
   free( block_hash );
   
   return 0;
}

// write partial blocks to the block buffer.
// if opt_block_head is not NULL, it must be a full block and will serve as the partial head to be bufferred.  Otherwise, the partial head client buffer in the write vector will be used.
// if opt_block_tail is not NULL, it must be a full block and will serve as the partial tail to be bufferred.  Otherwise, the partial tail client buffer in the write vector will be used.
// return 0 on success
// fent must be write-locked, in the same context as fs_entry_writev.  we update the manifest
static int fs_entry_buffer_partial_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct fs_entry_write_vec* wvec, char* opt_block_head, char* opt_block_tail ) {
   
   int rc = 0;
   
   // write the partial head to the block buffer, if we can
   if( wvec->has_head && !fs_entry_head_completes_block( core, &wvec->head ) ) {
      
      dbprintf("write partial head block %" PRIu64 " to bufferred blocks\n", wvec->head.block_id );
      
      // update the block 
      if( opt_block_head ) {
         // use the data we read
         rc = fs_entry_write_bufferred_block( core, fent, wvec->head.block_id, opt_block_head, 0, core->blocking_factor );
      }
      else {
         // use the client buffer
         rc = fs_entry_write_bufferred_block( core, fent, wvec->head.block_id, wvec->head.buf_ptr, wvec->head.write_offset, wvec->head.write_len );
      }
      
      if( rc != 0 ) {
         errorf("fs_entry_write_bufferred_block( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "] ) rc = %d\n",
                fs_path, fent->file_id, fent->version, wvec->head.block_id, rc );
         
         return rc;
      }
      else {
         // update manifest 
         rc = fs_entry_manifest_put_bufferred_block( core, fent, wvec->head.block_id );
         if( rc != 0 ) {
            errorf("fs_entry_manifest_put_bufferred_block( %" PRIu64 " ) rc = %d\n", wvec->head.block_id, rc );
            return rc;
         }
      }
   }
   
   // write the partial tail to the block buffer, if it exists 
   else if( wvec->has_tail ) {
      
      dbprintf("write partial tail block %" PRIu64 " to bufferred blocks\n", wvec->tail.block_id );
      
      // update the block 
      if( opt_block_tail ) {
         // use the data we read
         rc = fs_entry_write_bufferred_block( core, fent, wvec->tail.block_id, opt_block_tail, 0, core->blocking_factor );
      }
      else {
         // use the client buffer
         rc = fs_entry_write_bufferred_block( core, fent, wvec->tail.block_id, wvec->tail.buf_ptr, 0, wvec->tail.write_len );
      }
      
      if( rc != 0 ) {
         errorf("fs_entry_write_bufferred_block( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "] ) rc = %d\n",
                fs_path, fent->file_id, fent->version, wvec->tail.block_id, rc );
         
         return rc;
      }
      else {
         // update manifest
         rc = fs_entry_manifest_put_bufferred_block( core, fent, wvec->tail.block_id );
         if( rc != 0 ) {
            errorf("fs_entry_manifest_put_bufferred_block( %" PRIu64 " ) rc = %d\n", wvec->tail.block_id, rc );
            return rc;
         }
      }
   }
   
   return rc;
}
   


// write a write vector in its entirety.
// fent must be write-locked
// return 0 on success; negative on error.
// if this method fails, the caller should call fs_entry_revert_write to restore the fent
// NOTE: this is a "heavy" method.  Only use it if you're writing to multiple blocks, or you can't write to a bufferred block
static int fs_entry_writev( struct fs_core* core, char* fs_path, struct fs_entry* fent, struct fs_entry_write_vec* wvec, modification_map* old_blocks ) {
   
   int rc = 0;
   
   // head and tail buffers (pointers to buffers flushed to cache)
   char* block_head = NULL;
   char* block_tail = NULL;
   
   // overwritten and new blocks 
   modification_map new_blocks;
   
   // futures for reading partial heads and tails
   struct fs_entry_read_block_future head_fut;
   struct fs_entry_read_block_future tail_fut;
   
   memset( &head_fut, 0, sizeof(struct fs_entry_read_block_future) );
   memset( &tail_fut, 0, sizeof(struct fs_entry_read_block_future) );
   
   // overwritten blocks 
   fs_entry_whole_block_list_t overwritten;
   
   struct fs_entry_read_context read_ctx;      // for getting partial heads and tails
   fs_entry_read_context_init( &read_ctx );
   
   // if we have a partial head and an older version of the affected block, then populate block with the block-to-be-modified
   if( wvec->has_head && wvec->head.has_old_version ) {
      
      dbprintf("Read partial head %" PRIu64 "\n", wvec->head.block_id );
      
      uint64_t gateway_id = fent->manifest->get_block_host( core, wvec->head.block_id );
      
      block_head = fs_entry_setup_partial_read_future( core, &head_fut, gateway_id, fs_path, fent->version, wvec->head.block_id, wvec->head.block_version, fent->size );
      
      // add it to the read context 
      fs_entry_read_context_add_block_future( &read_ctx, &head_fut );
   }
   
   // if we have a partial tail and an older version of the affected block, then populate the block with the block-to-be-modified
   if( wvec->has_tail && wvec->tail.has_old_version ) {
      
      dbprintf("Read partial tail %" PRIu64 "\n", wvec->tail.block_id );
      
      uint64_t gateway_id = fent->manifest->get_block_host( core, wvec->tail.block_id );
      
      block_tail = fs_entry_setup_partial_read_future( core, &tail_fut, gateway_id, fs_path, fent->version, wvec->tail.block_id, wvec->tail.block_version, fent->size );
      
      // add it to the read context 
      fs_entry_read_context_add_block_future( &read_ctx, &tail_fut );
   }
   
   // get partially-overwritten blocks, if we need to, and patch them
   if( fs_entry_read_context_size( &read_ctx ) > 0 ) {
      // NOTE: we're write-locked in this method
      rc = fs_entry_read_partial_blocks( core, fs_path, fent, true, &read_ctx );
      if( rc != 0 ) {
         // failed to read data...can't proceed 
         errorf("fs_entry_read_partial_blocks(%s %" PRIX64 ".%" PRId64 ") rc = %d\n", fs_path, fent->file_id, fent->version, rc );
         
         // free everything 
         // NOTE: this frees block_head and block_tail, since they were assigned to their respective read futures
         fs_entry_read_context_free_all( core, &read_ctx );
         return rc;
      }
      
      // apply writes to the head and tail block buffers
      fs_entry_apply_partial_blocks( core, wvec, block_head, block_tail, &overwritten );
   }
   
   // write all full blocks to cache.
   // gather the full list of overwritten blocks 
   if( wvec->overwritten != NULL ) {
      for( fs_entry_whole_block_list_t::iterator itr = wvec->overwritten->begin(); itr != wvec->overwritten->end(); itr++ ) {
         overwritten.push_back( *itr );
      }
   }
   
   // accumulate cache write futures
   vector<struct cache_block_future*> futs;
   
   // process and flush them to disk, if we have any.  This updates the manifest.
   int write_rc = fs_entry_write_full_blocks_async( core, fs_path, fent, &overwritten, old_blocks, &new_blocks, &futs );
   
   // wait for each cache write to complete
   int wait_rc = fs_entry_flush_cache_writes( &futs );
   
   if( write_rc != 0 || wait_rc != 0 ) {
      
      if( write_rc != 0 ) {
         errorf("fs_entry_write_full_blocks_async( %s ) rc = %d\n", fs_path, write_rc );
         rc = write_rc;
      }
      
      if( wait_rc != 0 ) {
         errorf("fs_entry_flush_cache_writes( %s ) rc = %d\n", fs_path, wait_rc );
         
         if( rc == 0 )
            rc = wait_rc;
      }
      
      // roll back cache write 
      fs_entry_cache_evict_blocks_async( core, fent, &new_blocks );
      
      // free everything 
      // NOTE: this frees block_head and block_tail, since they were assigned to their respective read futures
      fs_entry_read_context_free_all( core, &read_ctx );
      return rc;
   }
   
   // free all cache futures
   fs_entry_cache_block_future_free_all( &futs, false );
   
   // clear all flushed bufferred blocks 
   fs_entry_clear_bufferred_blocks( fent, &new_blocks );
   
   // write partial blocks to the block buffer, updating the manifest in the process
   rc = fs_entry_buffer_partial_blocks( core, fs_path, fent, wvec, block_head, block_tail );
   if( rc != 0 ) {
      errorf("fs_entry_buffer_partial_blocks( %s %" PRIX64 ".%" PRId64 ") rc = %d\n",
               fs_path, fent->file_id, fent->version, rc );
      
      
      // roll back cache write 
      fs_entry_cache_evict_blocks_async( core, fent, &new_blocks );
      
      // free everything 
      // NOTE: this frees block_head and block_tail, since they were assigned to their respective read futures
      fs_entry_read_context_free_all( core, &read_ctx );
      
      return rc;
   }
   
   // all data committed to disk!
   // merge new writes into the dirty block set, freeing the old ones to be evicted
   fs_entry_merge_new_dirty_blocks( fent, &new_blocks );
   
   // merge old blocks into the garbage collect set 
   modification_map unmerged_garbage;
   fs_entry_merge_garbage_blocks( core, fent, fent->file_id, fent->version, old_blocks, &unmerged_garbage );
   
   // evict old blocks asynchronously
   fs_entry_cache_evict_blocks_async( core, fent, old_blocks );
   
   // free memory 
   fs_entry_free_modification_map( &unmerged_garbage );         // TODO: log it somewhere
   
   // free everything, including block_head and block_tail
   fs_entry_read_context_free_all( core, &read_ctx );
   
   return 0;
}


// can we service this write quickly?  As in, will this write only affect the bufferred block?
// fent must be at least read-locked
bool fs_entry_can_fast_write( struct fs_core* core, struct fs_entry* fent, off_t offset, size_t count ) {
   
   // which block?
   uint64_t start_block_id = offset / core->blocking_factor;
   uint64_t end_block_id = (offset + count) / core->blocking_factor;
   
   // must be in the same block 
   if( start_block_id != end_block_id )
      return false;
   
   // block must be bufferred
   int has_block = fs_entry_has_bufferred_block( fent, start_block_id );
   if( has_block <= 0 )
      return false;
   
   return true;
}

// do a fast write--a write affecting a bufferred block
// fent must be write-locked.
int fs_entry_do_fast_write( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char const* buf, off_t offset, size_t count ) {
   
   uint64_t block_id = offset / core->blocking_factor;
   off_t block_offset = offset % core->blocking_factor;
   
   // do we have this block resident?
   int has_block = fs_entry_has_bufferred_block( fent, block_id );
   if( has_block == -ENOENT ) {
      
      // not possible to do a fast write
      return -EINVAL;
   }
   if( has_block > 0 ) {
      // this block is already cached, so patch it 
      int rc = fs_entry_write_bufferred_block( core, fent, block_id, buf, block_offset, count );
      
      if( rc != 0 ) {
         errorf("fs_entry_write_bufferred_block(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 "] ) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, rc );
         return rc;
         
      }
      
      return rc;
   }
   else {
      // some other error 
      errorf("fs_entry_has_bufferred_block(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 "] ) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, has_block );
      return has_block;
   }
}


// perform a write.
// On success, return number of bytes written
// On failure, return negative 
ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset ) {
   
   // sanity check
   if( count == 0 )
      return 0;
   
   // first things first: check open mode vs whether or not we're a client and/or have read-only caps 
   if( core->gateway == GATEWAY_ANON ) {
      errorf("%s", "Writing is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
   // lock handle--prevent the file from being destroyed
   fs_file_handle_rlock( fh );
   if( fh->fent == NULL || fh->open_count <= 0 ) {
      // invalid
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   // revalidate metadata
   int rc = fs_entry_revalidate_metadata( core, fh->path, fh->fent, NULL );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_metadata(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   // block other writers
   fs_entry_wlock( fh->fent );
   
   // what's the new size going to be?
   off_t new_size = MAX( (unsigned)fh->fent->size, (unsigned)offset + count );
   
   // is this a fast-path write?
   if( fs_entry_can_fast_write( core, fh->fent, offset, count ) ) {
      
      dbprintf("fast write: %s offset %jd count %zu\n", fh->path, offset, count );
      
      // do a fast write 
      rc = fs_entry_do_fast_write( core, fh->path, fh->fent, buf, offset, count );
      if( rc != 0 ) {
         errorf("fs_entry_do_fast_write( %s offset %jd count %zu ) rc = %d\n", fh->path, offset, count, rc );
         
         fs_entry_unlock( fh->fent );
         fs_file_handle_unlock( fh );
         
         return rc;
      }
   }
   // heavy write--affects multiple non-cached blocks
   else {
      
      // sanpshot fent's metadata
      struct replica_snapshot fent_prewrite;
      fs_entry_replica_snapshot( core, fh->fent, 0, 0, &fent_prewrite );
      
      modification_map old_blocks;
      
      // put write holes 
      fs_entry_put_write_holes( core, fh->fent, offset );
      
      // split the write into a write vector
      struct fs_entry_write_vec wvec;
      
      rc = fs_entry_split_write( core, fh->fent, buf, offset, count, &wvec );
      if( rc != 0 ) {
         errorf("fs_entry_split_write(%s offset %jd size %zu) rc = %d\n", fh->path, offset, count, rc );
      
         // roll back metadata and manifest (only size and other fields; no blocks really beyond truncating any holes)
         fs_entry_revert_write( core, fh->fent, &fent_prewrite, new_size, NULL );
         
         fs_entry_unlock( fh->fent );
         fs_file_handle_unlock( fh );
         
         return rc;
      }
      
      // write the write vector
      rc = fs_entry_writev( core, fh->path, fh->fent, &wvec, &old_blocks );
      if( rc != 0 ) {
         errorf("fs_entry_writev(%s offset %jd size %zu) rc = %d\n", fh->path, offset, count, rc );
         
         // roll back metadata and manifest (only size and other fields; no blocks really beyond truncating any holes)
         fs_entry_revert_write( core, fh->fent, &fent_prewrite, new_size, &old_blocks );
         
         fs_entry_unlock( fh->fent );
         fs_file_handle_unlock( fh );
         
         fs_entry_free_modification_map( &old_blocks );
         return rc;
      }
      
      // free the write vector
      fs_entry_write_vec_free( &wvec );
      
      // free old block data
      // fs_entry_free_modification_map( &old_blocks );
   }
   
   // update the size
   fh->fent->size = new_size;
   
   // advance mod time
   fs_entry_update_modtime( fh->fent );
   
   // mark dirty
   fh->fent->dirty = true;
   
   // unlock
   fs_entry_unlock( fh->fent );
   
   // mark handle as dirty, so a future fsync() or close() will push data
   // TODO: preserve metadata so current replicated manifest can be garbage-collected
   fh->dirty = true;
   fs_file_handle_unlock( fh );
   
   return count;
}


// revert a write, given the modified blocks and a pre-write snapshot.
// fent must be write-locked, and must be locked in the same context as when the manifest was updated.
int fs_entry_revert_write( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, uint64_t new_size, modification_map* old_block_info ) {

   
   struct replica_snapshot fent_after_write;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_after_write );
   
   // restore fent's metadata
   fs_entry_replica_snapshot_restore( core, fent, fent_before_write );
   
   uint64_t old_end_block = fent_before_write->size / core->blocking_factor;
   uint64_t proposed_end_block = new_size / core->blocking_factor;
   
   if( old_end_block < proposed_end_block ) {
      // truncate the manifest back to its original size
      fent->manifest->truncate( old_end_block );
   }
   
   // restore old block information to the manifest
   if( old_block_info ) {
      for( modification_map::iterator itr = old_block_info->begin(); itr != old_block_info->end(); itr++ ) {
         uint64_t block_id = itr->first;
         
         // skip blocks written beyond the end of the original manifest
         if( block_id > old_end_block )
            continue;
         
         struct fs_entry_block_info* old_binfo = &itr->second;
         
         fs_entry_manifest_put_block( core, old_binfo->gateway_id, fent, block_id, old_binfo->version, old_binfo->hash );
      }
   }
   
   // restore manifest modtime
   fent->manifest->set_modtime( fent_before_write->manifest_mtime_sec, fent_before_write->manifest_mtime_nsec );
   
   return 0;
}


ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset ) {
   // TODO
   return -ENOSYS;
}

// validate a remote write 
static int fs_entry_validate_remote_write( char const* fs_path, struct fs_entry* fent, uint64_t file_id, int64_t file_version, uint64_t coordinator_id, Serialization::WriteMsg* write_msg ) {
   
   // validate
   if( fent->file_id != file_id ) {
      errorf("Remote write to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", fs_path, file_id, fent->file_id );
      return -EINVAL;
   }
   
   if( fent->coordinator != coordinator_id ) {
      errorf("Remote write to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", fs_path, coordinator_id, fent->coordinator );
      return -ESTALE;
   }
   
   if( fent->version != file_version ) {
      errorf("Remote write to file %s version %" PRId64 ", expected %" PRId64 "\n", fs_path, file_version, fent->version );
      return -ESTALE;
   }
   
   // make sure each hash is BLOCK_HASH_LEN() bytes long 
   for( int i = 0; i < write_msg->blocks_size(); i++ ) {
      
      // get the block 
      const Serialization::BlockInfo& msg_binfo = write_msg->blocks(i);
      
      if( msg_binfo.hash().size() != BLOCK_HASH_LEN() ) {
         errorf("Remote write to file %s version %" PRId64 " block %" PRIu64 ".%" PRId64 " has hash length of %zu (expected %zu)\n",
                fs_path, file_version, msg_binfo.block_id(), msg_binfo.block_version(), msg_binfo.hash().size(), BLOCK_HASH_LEN() );
         
         return -EINVAL;
      }
   }
   
   return 0;
}


// Reversion all affected blocks from a remote write.
// fent must be write-locked
static int fs_entry_reversion_blocks( struct fs_core* core, struct fs_entry* fent, uint64_t gateway_id, modification_map* old_block_info, Serialization::WriteMsg* write_msg ) {
   
   // update the blocks
   for( int i = 0; i < write_msg->blocks_size(); i++ ) {
      
      // get the block 
      const Serialization::BlockInfo& msg_binfo = write_msg->blocks(i);
      
      // get new fields
      uint64_t block_id = msg_binfo.block_id();
      int64_t new_version = msg_binfo.block_version();
      unsigned char* block_hash = (unsigned char*)msg_binfo.hash().data();

      // back up old version and gateway, in case we have to restore it
      int64_t old_version = fent->manifest->get_block_version( block_id );
      uint64_t old_gateway_id = fent->manifest->get_block_host( core, block_id );
      unsigned char* old_block_hash = fent->manifest->hash_dup( block_id );
      
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(struct fs_entry_block_info) );
      
      // remember the old block information, in case we need to revert it
      fs_entry_block_info_garbage_init( &binfo, old_version, old_block_hash, BLOCK_HASH_LEN(), old_gateway_id );
      
      (*old_block_info)[ block_id ] = binfo;
      
      // put the new version into the manifest
      fs_entry_manifest_put_block( core, gateway_id, fent, block_id, new_version, block_hash );
   }
   
   return 0;
}

// Handle a remote write.  The given write_msg must have been verified prior to calling this method.
// A remote write is really a batch of one or more writes sent on fsync().  So, write_msg may encode sparse byte ranges
// Zeroth, sanity check.
// First, update the local manifest.
// Second, synchronously replicate the manifest to all RGs.
// Third, upload new metadata to the MS for this file.
// Fourth, acknowledge the remote writer.
int fs_entry_remote_write( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t coordinator_id, Serialization::WriteMsg* write_msg ) {
   
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   int err = 0;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, write_msg->user_id(), write_msg->volume_id(), true, &err, &parent_id, &parent_name );
   if( err != 0 || fent == NULL ) {
      return err;
   }
   
   err = fs_entry_validate_remote_write( fs_path, fent, file_id, file_version, coordinator_id, write_msg );
   if( err != 0 ) {
      errorf("fs_entry_validate_remote_write( %s %" PRIX64 ".%" PRId64 " from %" PRIu64 " ) rc = %d\n", fs_path, fent->file_id, file_version, coordinator_id, err );
      
      fs_entry_unlock( fent );
      free( parent_name );
      return err;
   }
   
   // snapshot the fent so we can garbage-collect the manifest 
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );
   
   uint64_t gateway_id = write_msg->gateway_id();

   struct timespec ts, ts2, replicate_ts, garbage_collect_ts, update_ts;
   
   BEGIN_TIMING_DATA( ts );

   modification_map old_block_info;
   
   // reversion all affected blocks
   fs_entry_reversion_blocks( core, fent, gateway_id, &old_block_info, write_msg );
   
   // update size
   fent->size = write_msg->metadata().size();

   // update modtime
   fs_entry_update_modtime( fent );
      
   // replicate the manifest, synchronously
   BEGIN_TIMING_DATA( replicate_ts );

   err = fs_entry_replicate_manifest( core, fs_path, fent );
   if( err != 0 ) {
      errorf("fs_entry_replicate_manifest(%s) rc = %d\n", fs_path, err );
      err = -EIO;
   }
   
   END_TIMING_DATA( replicate_ts, ts2, "replicate manifest" );
   
   if( err == 0 ) {
      
      // replicated!
      // propagate the update to the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, fent, parent_id, parent_name );
   
      // did the write touch bufferred blocks?  If so, clear them out 
      fs_entry_clear_bufferred_blocks( fent, &old_block_info );
      
      BEGIN_TIMING_DATA( update_ts );
      
      err = ms_client_update( core->ms, &data );
      if( err != 0 ) {
         errorf("%ms_client_update(%s) rc = %d\n", fs_path, err );
         err = -EREMOTEIO;
      }
      
      md_entry_free( &data );      
      
      END_TIMING_DATA( update_ts, ts2, "MS update" );
      
      if( err == 0 ) {
         // metadata update succeeded!
         // garbage-collect the old manifest
         
         BEGIN_TIMING_DATA( garbage_collect_ts );
         
         int rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
            // TODO: record this somewhere so we can try again later
         }
         
         END_TIMING_DATA( garbage_collect_ts, ts2, "garbage collect manifest" );
         
         // evict old cached blocks 
         fs_entry_cache_evict_blocks_async( core, fent, &old_block_info );
      }
      else {
         // MS update failed; undo manifest replication before we revert the write
         struct replica_snapshot new_snapshot;
         fs_entry_replica_snapshot( core, fent, 0, 0, &new_snapshot );
         
         int rc = fs_entry_garbage_collect_manifest( core, &new_snapshot );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
            // TODO: record this somewhere so we can try again later
         }
      }
   }
   if( err != 0 ) {
      // something went wrong; revert the write
      fs_entry_revert_write( core, fent, &fent_snapshot, fent->size, &old_block_info );
   }
   
   fs_entry_unlock( fent );
   
   free( parent_name );
   
   // free memory
   fs_entry_free_modification_map( &old_block_info );
   
   END_TIMING_DATA( ts, ts2, "write, remote" );
   return err;
}
