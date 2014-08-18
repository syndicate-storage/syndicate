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

#include "cache.h"
#include "read.h"
#include "manifest.h"
#include "network.h"
#include "url.h"
#include "fs_entry.h"
#include "driver.h"
#include "syndicate.h"
#include "write.h"


// read one block, synchronously 
// return 0 on success, negative on error
// fent must be at least read-locked
static int fs_entry_read_block_ex( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_buf, size_t block_len, bool download_if_not_local ) {
   
   // block version 
   int64_t block_version = fent->manifest->get_block_version( block_id );
   
   // make a read context 
   struct fs_entry_read_context read_ctx;
   fs_entry_read_context_init( &read_ctx );
   
   // make a read future for this block 
   struct fs_entry_read_block_future block_fut;
   fs_entry_read_block_future_init( &block_fut, fent->coordinator, fs_path, fent->version, block_id, block_version, block_buf, block_len, 0, block_len, false );
   
   // add it to the context 
   fs_entry_read_context_add_block_future( &read_ctx, &block_fut );
   
   // try local read
   int rc = fs_entry_read_context_run_local( core, fs_path, fent, &read_ctx );
   
   // check for error...
   if( (rc != 0 && rc != -EREMOTE) || (rc == -EREMOTE && !download_if_not_local) ) {
      errorf("fs_entry_read_context_run_local( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
              fs_path, fent->file_id, fent->version, block_id, block_version, rc );
      
      fs_entry_read_context_free_all( core, &read_ctx );
      
      if( (rc == -EREMOTE && !download_if_not_local) )
         return -EREMOTE;
      else
         return -ENODATA;
   }
   
   else if( rc == -EREMOTE ) {
      // missing local data; download it (synchronously, and without unlocking fent)
      // at least one block was remote. Set up downloads 
      rc = fs_entry_read_context_setup_downloads( core, fent, &read_ctx );
      if( rc != 0 ) {
         // failed...
         errorf("fs_entry_read_context_setup_downloads( %s ) rc = %d\n", fs_path, rc );
         fs_entry_read_context_free_all( core, &read_ctx );
         
         return -ENODATA;
      }
      
      // Go get it/them.
      while( fs_entry_read_context_has_downloading_blocks( &read_ctx ) ) {
         
         // get some data
         rc = fs_entry_read_context_run_downloads( core, fent, &read_ctx );
         if( rc != 0 ) {
            errorf("fs_entry_read_context_run_downloads( %s ) rc = %d\n", fs_path, rc );
            fs_entry_read_context_free_all( core, &read_ctx );
            
            return -ENODATA;
         }
      }
   }
   
   // success!
   fs_entry_read_context_free_all( core, &read_ctx );
   return 0;
}

// read a block from anywhere.
// return 0 on success
// fent must be read-locked
int fs_entry_read_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_buf, size_t block_len ) {
   return fs_entry_read_block_ex( core, fs_path, fent, block_id, block_buf, block_len, true );
}

// read one block, synchronously.
// only works if the block is locally hosted
// return the size of the block on success
ssize_t fs_entry_read_block_local( struct fs_core* core, char const* fs_path, uint64_t block_id, char* block_buf, size_t block_len ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      return err;
   }

   int rc = fs_entry_read_block_ex( core, fs_path, fent, block_id, block_buf, block_len, false );
   
   fs_entry_unlock( fent );

   if( rc != 0 ) {
      errorf("fs_entry_read_block( %s %" PRIu64 " ) rc = %d\n", fs_path, block_id, rc );
      return rc;
   }
   else {
      return (ssize_t)block_len;
   }
}

// verify the integrity of a block, given the fent (and its manifest).
// fent must be at least read-locked
static int fs_entry_verify_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len ) {
   // TODO: get hash from AG, somehow.  But for now, ignore
   bool is_AG = ms_client_is_AG( core->ms, fent->coordinator );
   if( is_AG )
      return 0;
   
   
   unsigned char* block_hash = BLOCK_HASH_DATA( block_bits, block_len );
   
   int rc = fent->manifest->hash_cmp( block_id, block_hash );
   
   free( block_hash );
   
   if( rc != 0 ) {
      errorf("Hash mismatch (rc = %d, len = %zu)\n", rc, block_len );
      return -EPROTO;
   }
   else {
      return 0;
   }
}


// determine if a file was modified
static int fs_entry_was_modified( struct fs_entry* fent, uint64_t file_id, int64_t file_version, int64_t write_nonce ) {
   return fent->file_id != file_id || fent->version != file_version || fent->write_nonce != write_nonce;
}


// initialize a read future 
// NOTE: fs_path is not duplicated
int fs_entry_read_block_future_init( struct fs_entry_read_block_future* block_fut, uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version,
                                     char* result_buf, off_t result_buf_len, off_t block_read_start, off_t block_read_end, bool free_result_buf ) {
   
   memset( block_fut, 0, sizeof(struct fs_entry_read_block_future) );
   
   sem_init( &block_fut->sem, 0, 0 );
   
   block_fut->gateway_id = gateway_id;
   block_fut->status = READ_NOT_STARTED;
   block_fut->result = result_buf;
   block_fut->result_len = result_buf_len;
   block_fut->result_allocd = free_result_buf;
   block_fut->result_start = block_read_start;
   block_fut->result_end = block_read_end;
   
   block_fut->fs_path = fs_path;
   block_fut->file_version = file_version;
   block_fut->block_id = block_id;
   block_fut->block_version = block_version;
   
   return 0;
}

// destroy a read future.
// detach the read handle from the core downloader
int fs_entry_read_block_future_free( struct fs_core* core, struct fs_entry_read_block_future* block_fut ) {
   
   md_download_context_cancel( &core->state->dl, &block_fut->dlctx );
   
   sem_destroy( &block_fut->sem );
   
   if( block_fut->has_dlctx ) {
      CURL* conn = NULL;
      
      // get back the cls 
      struct driver_connect_cache_cls* cache_cls = (struct driver_connect_cache_cls*) md_download_context_get_cache_cls( &block_fut->dlctx );
      
      // TODO: recycle connection
      int rc = md_download_context_free( &block_fut->dlctx, &conn );
      if( rc == -EAGAIN ) {
         errorf("BUG: tried to free context %p, which is still in use!\n", (void*)&block_fut->dlctx );
      }
      curl_easy_cleanup( conn );
      
      free( cache_cls );
   }
   
   if( block_fut->curr_URL ) {
      free( block_fut->curr_URL );
      block_fut->curr_URL = NULL;
   }
   
   if( block_fut->result && block_fut->result_allocd ) {
      free( block_fut->result );
      block_fut->result = NULL;
   }
   
   memset( block_fut, 0, sizeof(struct fs_entry_read_block_future) );
   
   return 0;
}


// finalize a read future.
// NOTE: the caller must NOT free block
static int fs_entry_read_block_future_finalize( struct fs_entry_read_block_future* block_fut ) {
   block_fut->status = READ_FINISHED;
   
   dbprintf("block %" PRIu64 ": finalized successfully\n", block_fut->block_id );
   
   sem_post( &block_fut->sem );
   return 0;
}


// finalize a read future in error 
static int fs_entry_read_block_future_finalize_error( struct fs_entry_read_block_future* block_fut, int err ) {
   block_fut->status = READ_ERROR;
   block_fut->err = err;
   
   dbprintf("block %" PRIu64 ": finalized in error (rc = %d)\n", block_fut->block_id, err );
   
   sem_post( &block_fut->sem );
   return 0;
}


// process a block and put the result into its read future (finalizing the future)
// fent must be read-locked
static int fs_entry_process_and_finalize_read_future( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                                                      char* buf, off_t buf_len, struct fs_entry_read_block_future* block_fut ) {
   
   int rc = 0;
   
   // process the block 
   ssize_t processed_len = driver_read_block_postdown( core, core->closure, fs_path, fent, block_id, block_version, buf, buf_len, block_fut->result, block_fut->result_len );
   
   if( processed_len < 0 ) {
      errorf("driver_read_block_postdown( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
             block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_version, rc );
      
      // finalize error
      fs_entry_read_block_future_finalize_error( block_fut, processed_len );
      
      rc = (int)processed_len;
   }
   else {
      // finalize success
      fs_entry_read_block_future_finalize( block_fut );
   }
    
   return rc;
}



// read from the on-disk block cache.  If we succeed, return the raw cached data to buf and buf_len.
// NOTE: this does NOT process the block data!  The driver will need to be called to process it.
// return 0 on success.
// return -ENOENT if not hit.
// return negative on error
// fent must be read-locked at least.  In fact, fent should be read-locked across successive calls of this method in a single read, so that fent->version does not change.
static int fs_entry_try_cache_block_read( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char** buf, off_t* buf_len ) {
   
   char* block_buf = NULL;
   ssize_t read_len = 0;
   int rc = 0;
   
   // lookaside: if this block is being written, then we can't read it 
   rc = fs_entry_cache_is_block_readable( core->cache, fent->file_id, fent->version, block_id, block_version );
   if( rc == -EAGAIN ) {
      // not available in the cache 
      return -ENOENT;
   }
   
   // stored in local cache?
   int block_fd = fs_entry_cache_open_block( core, core->cache, fent->file_id, fent->version, block_id, block_version, O_RDONLY );
   
   if( block_fd < 0 ) {
      if( block_fd != -ENOENT ) {
         errorf("WARN: fs_entry_cache_open_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n", fent->file_id, fent->version, block_id, block_version, fs_path, block_fd );
      }
      else {
         rc = -ENOENT;
      }
   }
   else {
      read_len = fs_entry_cache_read_block( block_fd, &block_buf );
      if( read_len < 0 ) {
         errorf("fs_entry_cache_read_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n", fent->file_id, fent->version, block_id, block_version, fs_path, (int)read_len );
         rc = read_len;
      }
      else {
         // success! promote!
         fs_entry_cache_promote_block( core, core->cache, fent->file_id, fent->version, block_id, block_version );
         
         dbprintf("Cache HIT on %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", fent->file_id, fent->version, block_id, block_version );
      }
      
      close( block_fd );
      
      *buf = block_buf;
      *buf_len = read_len;
   }
   
   return rc;
}


// initialize a read context 
int fs_entry_read_context_init( struct fs_entry_read_context* read_ctx ) {
   read_ctx->reads = new fs_entry_read_block_future_set_t();
   read_ctx->download_to_future = new fs_entry_download_to_future_map_t();
   
   md_download_set_init( &read_ctx->dlset );
   return 0;
}


// free a read context and optionally its read futures
int fs_entry_read_context_free_ex( struct fs_entry_read_context* read_ctx, fs_entry_read_block_future_set_t** ret_reads ) {
   
   if( read_ctx->reads ) {
      // free each block 
      if( ret_reads ) {
         *ret_reads = read_ctx->reads;
      }
      else {
         delete read_ctx->reads;
      }
      read_ctx->reads = NULL;
   }
   
   if( read_ctx->download_to_future ) {
      delete read_ctx->download_to_future;
      read_ctx->download_to_future = NULL;
   }
   
   md_download_set_free( &read_ctx->dlset );
   
   return 0;
}

// free a read context and its read futures' data.  read futures themselves are unaffected
int fs_entry_read_context_free_all( struct fs_core* core, struct fs_entry_read_context* read_ctx ) {
   fs_entry_read_block_future_set_t* reads = NULL;
   fs_entry_read_context_free_ex( read_ctx, &reads );
   
   for( fs_entry_read_block_future_set_t::iterator itr = reads->begin(); itr != reads->end(); itr++ ) {
      fs_entry_read_block_future_free( core, *itr );
   }
   
   delete reads;
   return 0;
}

// free a list of read futures' data, and its futures' data.  don't free the futures themselves (i.e. useful if they're on the stack)
static int fs_entry_read_block_futures_free_all( struct fs_core* core, fs_entry_read_block_future_set_t* reads ) {
   
   for( fs_entry_read_block_future_set_t::iterator itr = reads->begin(); itr != reads->end(); itr++ ) {
      
      fs_entry_read_block_future_free( core, *itr );
      free( *itr );
   }
   
   delete reads;
   return 0;
}

// have a read context track a downloading block
static int fs_entry_read_context_track_downloading_block( struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut ) {
   
   // start tracking this download
   md_download_set_add( &read_ctx->dlset, &block_fut->dlctx );
   
   (*read_ctx->download_to_future)[ &block_fut->dlctx ] = block_fut;
   
   return 0;
}

// stop tracking a downloading block (given the iterator to it)
static int fs_entry_read_context_untrack_downloading_block_itr( struct fs_entry_read_context* read_ctx, md_download_set_iterator itr ) {

   // stop tracking this download 
   struct md_download_context* dlctx = md_download_set_iterator_get_context( itr );
   
   md_download_set_clear_itr( &read_ctx->dlset, itr );
   
   read_ctx->download_to_future->erase( dlctx );
   
   return 0;
}


// stop tracking a block (don't do this in a for() loop involving download set iterators)
static int fs_entry_read_context_untrack_downloading_block( struct fs_entry_read_context* read_ctx, struct md_download_context* dlctx ) {

   // stop tracking this download 
   md_download_set_clear( &read_ctx->dlset, dlctx );
   
   read_ctx->download_to_future->erase( dlctx );
   
   return 0;
}


// find a (tracked) downloading block 
static struct fs_entry_read_block_future* fs_entry_read_context_find_downloading_block( struct fs_entry_read_context* read_ctx, struct md_download_context* dlctx ) {
   
   struct fs_entry_read_block_future* block_fut = NULL;
   
   // which read future does it refer to?
   fs_entry_download_to_future_map_t::iterator block_fut_itr = read_ctx->download_to_future->find( dlctx );
   if( block_fut_itr != read_ctx->download_to_future->end() ) {
      
      // get the read block future 
      block_fut = block_fut_itr->second;
   }
   
   return block_fut;
}


// start up a read download.
static int fs_entry_read_block_future_setup_download( struct fs_core* core, struct fs_entry_read_block_future* block_fut ) {
   
   // TODO: use connection pool 
   CURL* curl = curl_easy_init();
   
   // connect to the CDN
   struct driver_connect_cache_cls* driver_cls = CALLOC_LIST( struct driver_connect_cache_cls, 1 );
   driver_cls->core = core;
   driver_cls->client = core->ms;
   
   int rc = md_download_context_init( &block_fut->dlctx, curl, driver_connect_cache, driver_cls, -1 );
   if( rc != 0 ) {
      
      errorf("md_download_context_init(%s) rc = %d\n", block_fut->fs_path, rc );
      
      // TODO: use connection pool
      curl_easy_cleanup( curl );
      
      return -ENODATA;
   }
   
   block_fut->has_dlctx = true;
   
   // store info we need to 
   block_fut->curr_RG = -1;
   
   // next step: start a download
   block_fut->status = READ_DOWNLOAD_NOT_STARTED;
   
   return 0;
}

// start a primary download.
// fent must be read-locked (but we only access static data: fent->volume, fent->coordinator, fent->file_id)
static int fs_entry_read_block_future_start_primary_download( struct fs_core* core, struct fs_entry_read_block_future* block_fut, struct fs_entry* fent ) {

   // corner case: don't download from ourselves; fail over to RG
   if( core->gateway == block_fut->gateway_id ) {
      errorf("Cannot download %s (%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) from ourselves\n", block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version );
      
      return -ENETUNREACH;
   }
   
   // kick off the read from the remote UG (the primary source)
   char* block_url = NULL;
   
   int rc = fs_entry_make_block_url( core, block_fut->fs_path, block_fut->gateway_id, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, &block_url );
   if( rc != 0 ) {
      errorf("fs_entry_make_block_url( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
      return -ENODATA;
   }
   
   dbprintf("block %" PRId64 ": try from primary, URL = %s\n", block_fut->block_id, block_url );
   
   // reset the download context
   // TODO: use connection pool--point the dlctx to the keep-alive connection to the UG
   md_download_context_reset( &block_fut->dlctx, NULL );
   
   // re-insert it 
   rc = md_download_context_start( &core->state->dl, &block_fut->dlctx, core->closure, block_url );
   if( rc != 0 ) {
      errorf("md_download_context_start( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
      
      free( block_url );
      return rc;
   }
   
   if( block_fut->curr_URL ) {
      free( block_fut->curr_URL );
   }
   
   block_fut->curr_URL = block_url;
   
   // is this an AG? remember if so 
   int gateway_type = ms_client_get_gateway_type( core->ms, fent->coordinator );
   if( gateway_type == SYNDICATE_AG ) {
      
      // from an AG
      block_fut->is_AG = true;
   }
   
   return rc;
}


// start a replica download, advancing the index of the next RG to try.
// fent must be read-locked (but we only access static data: fent->volume, fent->file_id)
static int fs_entry_read_block_future_start_next_replica_download( struct fs_core* core, struct fs_entry_read_block_future* block_fut, struct fs_entry* fent ) {
   
   // get list of RGs
   uint64_t* rg_ids = ms_client_RG_ids( core->ms );
   int rc = 0;
   
   // kick off the next replica
   if( rg_ids != NULL ) {
      
      // next RG 
      block_fut->curr_RG ++;
      
      // how many RGs?
      uint64_t num_RGs = 0;
      for( ; rg_ids[num_RGs] != 0; num_RGs++ );
      
      // have we exceeded them?
      if( (unsigned)block_fut->curr_RG >= num_RGs ) {
         errorf("No more RGs to try (after %d attempts)\n", block_fut->curr_RG );
         return -ENODATA;
      }
      
      // get the current RG ID
      uint64_t rg_id = rg_ids[ block_fut->curr_RG ];
      
      // get the URL to the block 
      char* replica_url = fs_entry_RG_block_url( core, rg_id, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version );
         
      // reset the download context
      // TODO: use connection pool--point the dlctx to the keep-alive connection to the RG
      md_download_context_reset( &block_fut->dlctx, NULL );
      
      dbprintf("block %" PRId64 ": try from RG, URL = %s\n", block_fut->block_id, replica_url );
      
      // re-insert it 
      rc = md_download_context_start( &core->state->dl, &block_fut->dlctx, core->closure, replica_url );
      if( rc != 0 ) {
         errorf("md_download_context_start( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
         
         free( replica_url );
         return rc;
      }
      
      if( block_fut->curr_URL ) {
         free( block_fut->curr_URL );
      }
      
      // save this for later 
      block_fut->curr_URL = replica_url;
      
      free( rg_ids );
      
      return 0;
   }
   else {
      // out of options
      errorf("No more RGs to try (after %d attempts)\n", block_fut->curr_RG );
      return -ENODATA;
   }
}


// start up a block download, and advance the state of block_fut.
// fent must be read-locked (but we only access static data)
static int fs_entry_read_block_future_start_next_download( struct fs_core* core, struct fs_entry_read_block_future* block_fut, struct fs_entry* fent ) {
   
   int rc = 0;
   
   // what state are we in?
   if( block_fut->status == READ_DOWNLOAD_NOT_STARTED ) {
      
      // kick off the read from the remote UG (the primary source)
      block_fut->status = READ_PRIMARY;
      
      rc = fs_entry_read_block_future_start_primary_download( core, block_fut, fent );
      
      if( rc != 0 ) {
         if( rc == -ENETUNREACH ) {
            // means that we tried to download from ourselves.
            // fall over to replica gateway immediately
            rc = 0;
         }
         else {  
            errorf("fs_entry_read_block_future_start_primary_download( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                   block_fut->fs_path, fent->file_id, fent->version, block_fut->block_id, block_fut->block_version, rc );

            return -ENODATA;
         }
      }
      else {
         return rc;
      }
   }
   
   if( block_fut->status == READ_PRIMARY || block_fut->status == READ_REPLICA ) {
      
      if( block_fut->is_AG ) {
         // don't try to read from replica if the file comes from an AG
         block_fut->status = READ_ERROR;
         return -ENODATA;
      }
      
      // kick off replica downloads 
      block_fut->status = READ_REPLICA;
      
      rc = fs_entry_read_block_future_start_next_replica_download( core, block_fut, fent );
      
      if( rc != 0 ) {
         errorf("fs_entry_read_block_future_start_next_replica_download( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                 block_fut->fs_path, fent->file_id, fent->version, block_fut->block_id, block_fut->block_version, rc );
         
         return -ENODATA;
      }
   }
   
   return rc;
}

// does a read context have any downloads pending?
bool fs_entry_read_context_has_downloading_blocks( struct fs_entry_read_context* read_ctx ) {
   
   return ( md_download_set_size( &read_ctx->dlset ) != 0 );
}


// is a read future finalized?
static bool fs_entry_is_read_block_future_finalized( struct fs_entry_read_block_future* block_fut ) {
   return (block_fut->status == READ_FINISHED || block_fut->status == READ_ERROR);
}

// is a read future downloading?
static bool fs_entry_is_read_block_future_downloading( struct fs_entry_read_block_future* block_fut ) {
   return (block_fut->status == READ_DOWNLOAD_NOT_STARTED || block_fut->status == READ_PRIMARY || block_fut->status == READ_REPLICA);
}

// set up a read context to download all non-finalized blocks.
// all non-finalized read futures will be set up to download.
// finalized read futures will be ignored.
// fent must be read-locked
int fs_entry_read_context_setup_downloads( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx ) {
   
   // look-aside map for whether or not a block should be downloaded 
   set<uint64_t> download_lookaside;
   
   for( fs_entry_read_block_future_set_t::iterator itr = read_ctx->reads->begin(); itr != read_ctx->reads->end(); itr++ ) {
      
      // get block future 
      struct fs_entry_read_block_future* block_fut = *itr;
      
      // find un-finalized read contexts
      if( !fs_entry_is_read_block_future_finalized( block_fut ) ) {
         
         dbprintf("block %" PRIu64 ": setup download\n", block_fut->block_id );
         
         // set up for download 
         int rc = fs_entry_read_block_future_setup_download( core, block_fut );
         if( rc != 0 ) {
            errorf( "fs_entry_read_block_future_setup_download( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                    block_fut->fs_path, fent->file_id, fent->version, block_fut->block_id, block_fut->block_version, rc );
            
            return -ENODATA;
         }
         
         // will need to insert 
         download_lookaside.insert( block_fut->block_id );
      }
      else {
         dbprintf("block %" PRIu64": not downloading, status = %d\n", block_fut->block_id, block_fut->status );
      }
   }
   
   // successfully set them up!
   // track the downloads
   for( fs_entry_read_block_future_set_t::iterator itr = read_ctx->reads->begin(); itr != read_ctx->reads->end(); itr++ ) {
      
      // get block future 
      struct fs_entry_read_block_future* block_fut = *itr;
      
      // need to download?  if not, then skip
      if( download_lookaside.count( block_fut->block_id ) == 0 )
         continue;
      
      // track this block future 
      fs_entry_read_context_track_downloading_block( read_ctx, block_fut );
      
      // start downloading it 
      int rc = fs_entry_read_block_future_start_next_download( core, block_fut, fent );
      if( rc != 0 ) {
         errorf("fs_entry_read_block_future_start_next_download( %s ) rc = %d\n", block_fut->fs_path, rc );
         
         fs_entry_read_block_future_finalize_error( block_fut, rc );
         return rc;
      }
   }
   
   return 0;
}


// process a finished now-untracked download of a block future.  Finalize it if it is done.
// return 0 on success
// return EAGAIN if we re-tracked the read download
// return negative on error.
// fent must be read-locked
static int fs_entry_read_block_future_process_download( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut ) {
   
   // sanity check..
   if( !block_fut->has_dlctx )
      return -EINVAL;
   
   if( fs_entry_is_read_block_future_finalized( block_fut ) )
      return -EINVAL;
   
   // download context...
   struct md_download_context* dlctx = &block_fut->dlctx;
   int rc = 0;
   
   // not found?
   if( !md_download_context_succeeded( dlctx, 200 ) ) {
      
      // if the block didn't exist (i.e. the size is not known, or it got truncated out from under us), then EOF
      if( md_download_context_succeeded( dlctx, 404 ) ) {
      
         // found EOF on this block
         block_fut->eof = true;
         
         errorf("WARN: EOF on %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version );
         
         // done with the block 
         fs_entry_read_block_future_finalize( block_fut );
      
         return 0;
      }
      else {
         // some other error--e.g. the gateway is offline, or the connection took too long
         errorf("download of %s failed, CURL rc = %d, transfer errno = %d, HTTP status = %d\n",
                 block_fut->curr_URL, md_download_context_get_curl_rc( dlctx ), md_download_context_get_errno( dlctx ), md_download_context_get_http_status( dlctx ) );
         
         // try again 
         // track this block future 
         fs_entry_read_context_track_downloading_block( read_ctx, block_fut );
      
         rc = fs_entry_read_block_future_start_next_download( core, block_fut, fent );
         
         if( rc != 0 ) {
            // out of options here 
            errorf("fs_entry_read_block_future_prepare_next_download( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                    block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
            
            // finalize in error 
            fs_entry_read_context_untrack_downloading_block( read_ctx, &block_fut->dlctx );
            fs_entry_read_block_future_finalize_error( block_fut, rc );
         }
         else {
            rc = EAGAIN;
         }
      }
   }
   else {
      // succeeded!  Get the data 
      char* buf = NULL;
      off_t buflen = 0;
      
      // get the block 
      md_download_context_get_buffer( dlctx, &buf, &buflen );
      
      char prefix[11];
      memset(prefix, 0, 11);
      memcpy(prefix, buf, 10);
      
      dbprintf("Downloaded data for %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], prefix = '%s'\n", fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, prefix);
      
      // verify the block's integrity 
      rc = fs_entry_verify_block( core, fent, block_fut->block_id, buf, buflen );
      if( rc != 0 ) {
         errorf("fs_entry_verify_block( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                 block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
         
         // finalize error 
         fs_entry_read_block_future_finalize_error( block_fut, rc );
      }
      
      // block is valid
      else {
            
         // process the block through the driver 
         ssize_t processed_len = driver_read_block_postdown( core, core->closure, block_fut->fs_path, fent, block_fut->block_id, block_fut->block_version, buf, buflen, block_fut->result, block_fut->result_len );
         if( processed_len < 0 ) {
            errorf("driver_read_block_postdown( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                  block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
            
            // finalize error
            fs_entry_read_block_future_finalize_error( block_fut, processed_len );
            rc = (int)processed_len;
         }
         else {
            // finalize success
            block_fut->downloaded = true;
            fs_entry_read_block_future_finalize( block_fut );
         }
      }  
      
      free( buf );
   }
   
   return rc;
}


// cancel and finalize all block downloads that are after a given block ID (i.e. an EOF condition).
// pass 0 for the block_id to cancel all
static int fs_entry_read_context_cancel_downloads( struct fs_core* core, struct fs_entry_read_context* read_ctx, uint64_t start_block_id, bool set_eof ) {
   
   for( fs_entry_read_block_future_set_t::iterator itr = read_ctx->reads->begin(); itr != read_ctx->reads->end(); itr++ ) {
      
      struct fs_entry_read_block_future* block_fut = *itr;
      
      // ignore finalized reads 
      if( fs_entry_is_read_block_future_finalized( block_fut ) )
         continue;
      
      // ignore blocks before the given one 
      if( block_fut->block_id < start_block_id )
         continue;
      
      // is this block donwloading?
      if( fs_entry_is_read_block_future_downloading( block_fut ) ) {
         
         // verify download context is initialized...
         if( block_fut->has_dlctx ) {
            
            // get the download context 
            struct md_download_context* dlctx = &block_fut->dlctx;
            
            dbprintf("Cancel download of %s at [%" PRIu64 ".%" PRId64 "]\n",
                      block_fut->fs_path, block_fut->block_id, block_fut->block_version );
            
            // cancel it 
            md_download_context_cancel( &core->state->dl, dlctx );
            
            // untrack the block
            fs_entry_read_context_untrack_downloading_block( read_ctx, dlctx );
            
            // due to eof?
            if( set_eof ) {
               block_fut->eof = true;
            }
            
            // finalize the block
            fs_entry_read_block_future_finalize( block_fut );
            
         }
         else {
            // shouldn't happen, so lot it
            errorf("BUG: block future %s (.%" PRId64 "[%" PRIu64 ".%" PRId64 "]) has no download context, but is in downloading status\n",
                   block_fut->fs_path, block_fut->file_version, block_fut->block_id, block_fut->block_version );
         }
      }
   }
   
   return 0;
}

// run one or more read downloads in a read context.
// stop downloading if we encounter an EOF condition
// fent will be read-locked and read-unlocked across multiple download completions, unless we indicate otherwise in the write_locked flag
int fs_entry_read_context_run_downloads_ex( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx, bool write_locked,
                                            fs_entry_read_block_future_download_finalizer_func finalizer, void* finalizer_cls ) {
   
   int rc = 0;
   
   if( !fs_entry_read_context_has_downloading_blocks( read_ctx ) ) {
      dbprintf("%" PRIX64 " has no downloads\n", fent->file_id );
      return 0;
   }
   
   // It's entirely possible that the block got truncated out from under us.
   // Or, it's entirely possible that we're reading from an AG, and we've reached the end of a file.
   // In either case, identify which block is the new end block, and assume that all blocks 
   // beyond it are nonexistant (i.e. mark them as beyond EOF).
   
   // wait for a download to finish
   rc = md_download_context_wait_any( &read_ctx->dlset, core->conf->transfer_timeout * 1000 );
   if( rc != 0 ) {
      errorf("md_download_context_wait_any rc = %d\n", rc );
      return rc;
   }
   else {
      bool do_cancel = false;
      bool do_eof = false;
      uint64_t cancel_after = 0;
      
      // find the one(s) that finished 
      for( md_download_set_iterator itr = md_download_set_begin( &read_ctx->dlset ); itr != md_download_set_end( &read_ctx->dlset ); ) {
         
         // extract current iterator
         md_download_set_iterator curr_itr = itr;
         itr++;
         
         // get the current download context 
         struct md_download_context* dlctx = md_download_set_iterator_get_context( curr_itr );
         
         // did this context finish?
         if( md_download_context_finalized( dlctx ) ) {
            
            // which read future does it refer to?
            struct fs_entry_read_block_future* block_fut = fs_entry_read_context_find_downloading_block( read_ctx, dlctx );
            
            if( block_fut != NULL ) {
               
               dbprintf("block %" PRIu64 ": finished downloading from %s\n", block_fut->block_id, block_fut->curr_URL );
                        
               // untrack the download 
               fs_entry_read_context_untrack_downloading_block_itr( read_ctx, curr_itr );
               
               // lock this so we can access it in processing the next download step
               if( !write_locked )
                  fs_entry_rlock( fent );
               
               // do internal processing of the future (finalizing it, or re-tracking it)
               rc = fs_entry_read_block_future_process_download( core, fent, read_ctx, block_fut );
               
               // internal processing failed?
               if( rc < 0 ) {
                  // out of options here 
                  errorf("fs_entry_read_context_process_download( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n",
                        block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
                  
                  // failed to get a block
                  do_cancel = true;
                  
                  // cancel all blocks
                  cancel_after = 0;
                  
                  if( !write_locked )
                     fs_entry_unlock( fent );
                  
                  break;
               }
               
               // if we have a finalizer, and we're not re-trying, run it
               int finalizer_rc = 0;
               if( rc == 0 ) {
                  
                  if( finalizer ) {
                     finalizer_rc = (*finalizer)( core, fent, block_fut, finalizer_cls );
                  }
               }
               
               if( !write_locked )
                  fs_entry_unlock( fent );
               
               // finalizer succeeded?
               if( rc == 0 && finalizer_rc != 0 ) {
                  // finalizer failed
                  errorf("block future finalizer %p failed, rc = %d\n", finalizer, finalizer_rc );
                  
                  // cancel this 
                  do_cancel = true;
                  cancel_after = 0;
                  break;
               }
               
               // did we find EOF?
               if( rc == 0 && block_fut->eof ) {
                  // cancel all blocks after this one, since they are EOF
                  errorf("EOF on %s at [%" PRIu64 ".%" PRId64 "]\n",
                          block_fut->fs_path, block_fut->block_id, block_fut->block_version );
                  
                  do_cancel = true;
                  do_eof = true;
                  cancel_after = block_fut->block_id;
                  break;
               }
               
               rc = 0;
            }
            else {
               // shouldn't happen; indicates a bug
               errorf("No block future for context %p\n", dlctx );
               rc = -EIO;
               break;
            }
         }
      }
      
      if( do_cancel ) {
         // nothing more to do 
         fs_entry_read_context_cancel_downloads( core, read_ctx, cancel_after, do_eof );
      }
   }
   
   return rc;
}

// default download runner--doesn't do any finalization of its own
int fs_entry_read_context_run_downloads( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_context* read_ctx ) {
   return fs_entry_read_context_run_downloads_ex( core, fent, read_ctx, false, NULL, NULL );
}


// cache finalizer 
static int fs_entry_read_block_future_finalizer_cache_async( struct fs_core* core, struct fs_entry* fent, struct fs_entry_read_block_future* block_fut, void* cls ) {
   
   // argument: pointer to a vector of cache futures 
   vector<struct cache_block_future*>* cache_futs = (vector<struct cache_block_future*>*) cls;
   
   // cache if we succeeded in getting something cacheable
   if( block_fut->err == 0 && !block_fut->eof ) {
      
      // cache this!
      int rc = 0;
      struct cache_block_future* f = fs_entry_cache_write_block_async( core, core->cache, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version,
                                                                       block_fut->result, block_fut->result_len, false, &rc );
      if( rc != 0 || f == NULL ) {
         errorf("fs_entry_cache_write_block_async( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n",
                block_fut->fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
      }
      else {
         // save this for later 
         cache_futs->push_back( f );
      }
      
      return rc;
   }
   
   // nothing to do 
   return 0;
}


// try to read a block's data from local sources.  If it's remote, return -EREMOTE
// on success, finalize the block_fut and return the number of blocks read
// on error besides there not being data, finalize the block_fut in error.
// fent must be read-locked; we access its RAM buffer.
// In fact, fent should be read-locked across successive calls of this method in a single read, so that fent->version does not change.
static int fs_entry_try_read_block_local( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version, struct fs_entry_read_block_future* block_fut ) {
   
   int rc = 0;
   
   // EOF?
   if( block_id * core->blocking_factor >= (uint64_t)fent->size ) {
      dbprintf("%" PRIu64 " is EOF (%" PRIu64 " >= %" PRIu64 ")\n", block_id, block_id * core->blocking_factor, (uint64_t)fent->size );
      
      block_fut->eof = true;
      
      fs_entry_read_block_future_finalize( block_fut );
      return 0;      // EOF
   }
   
   // is this a write hole?
   if( fent->manifest->is_hole( block_id ) ) {
      dbprintf("%" PRIX64 " is part of a write hole\n", block_id );
      
      // a hole, so 0's (no need to invoke the driver)
      memset( block_fut->result, 0, block_fut->result_len );
      
      // finalize...
      fs_entry_read_block_future_finalize( block_fut );
      return core->blocking_factor;
   }
   
   // in the block buffer?
   rc = fs_entry_has_bufferred_block( fent, block_id );
   if( rc > 0 ) {
      // have a bufferred block.  Read the appropriate part of it 
      rc = fs_entry_read_bufferred_block( fent, block_id, block_fut->result, block_fut->result_start, block_fut->result_end - block_fut->result_start );
      if( rc != 0 ) {
         errorf("fs_entry_read_bufferred_block( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, block_version, rc );
         
         fs_entry_read_block_future_finalize_error( block_fut, rc );
         return rc;
      }
      else {
         // got it!
         dbprintf("bufferred block HIT on %" PRIu64 "\n", block_id );
         
         fs_entry_read_block_future_finalize( block_fut );
         return block_fut->result_len;
      }
   }
   
   // in the disk cache?
   char* buf = NULL;
   off_t buflen = 0;
   
   rc = fs_entry_try_cache_block_read( core, fs_path, fent, block_id, block_version, &buf, &buflen );
   if( rc == 0 ) {
      
      // hit cache!  Process the block 
      rc = fs_entry_process_and_finalize_read_future( core, fs_path, fent, block_id, block_version, buf, buflen, block_fut );
      free( buf );
      
      if( rc != 0 ) {
         errorf("fs_entry_process_and_finalize_read_future( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", fs_path, fent->file_id, fent->version, block_id, block_version, rc );
         return rc;
      }
      else {
         return block_fut->result_len;
      }
   }
   
   // nothing 
   return -EREMOTE;
}


// try to read all blocks in a read context from local sources.
// return 0 if all reads were satisfied.
// return -EREMOTE if at least one block must be downloaded
// fent must be read-locked--we can't have the file version change out from under us (i.e. due to a truncate)
int fs_entry_read_context_run_local( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct fs_entry_read_context* read_ctx ) {
   
   int final_rc = 0;
   
   for( fs_entry_read_block_future_set_t::iterator itr = read_ctx->reads->begin(); itr != read_ctx->reads->end(); itr++ ) {
      
      struct fs_entry_read_block_future* block_fut = *itr;
      
      int rc = fs_entry_try_read_block_local( core, fs_path, fent, block_fut->block_id, block_fut->block_version, block_fut );
      
      // got an error that didn't indicate that we need to download?
      if( rc < 0 ) {
         if( rc == -EREMOTE ) {
            // will need to download
            final_rc = -EREMOTE;
            dbprintf("block %" PRIu64 ": not cached; need to download\n", block_fut->block_id );
         }
         else {
            // some other error 
            errorf("fs_entry_try_read_block_local(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n",
                     fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, rc );
            
            final_rc = rc;
            break;
         }
      }
   }
   
   return final_rc;
}


// add a block_future to a read context 
int fs_entry_read_context_add_block_future( struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut ) {
   read_ctx->reads->insert( block_fut );
   return 0;
}

// remove a block future from a read context 
int fs_entry_read_context_remove_block_future( struct fs_entry_read_context* read_ctx, struct fs_entry_read_block_future* block_fut ) {
   read_ctx->reads->erase( block_fut );
   return 0;
}


// how many read futures?
size_t fs_entry_read_context_size( struct fs_entry_read_context* read_ctx ) {
   return read_ctx->reads->size();
}

// split a client's read buffer into one or more read block futures 
// fent must be at least read-locked.
// In fact, for best results, fent must be read-locked through this call, as well as any call that reads data locally
static int fs_entry_split_read( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* buf, size_t real_count, off_t offset, fs_entry_read_block_future_set_t* block_futs ) {
   
   // start and end ids of blocks that will be read in their entirety
   uint64_t start_id = offset / core->blocking_factor;
   uint64_t end_id = (offset + real_count) / core->blocking_factor;
   uint64_t last_block_id = fent->size / core->blocking_factor;
   
   // partial head or tail?
   bool has_partial_head = false;
   bool has_partial_tail = false;
   
   // is this fent hosted by an AG?
   bool is_AG = ms_client_is_AG( core->ms, fent->coordinator );
   
   // does the read start within the file?
   if( start_id > last_block_id ) {
      // we're EOF 
      return 0;
   }
   
   // does the read go past the end of the file?
   if( end_id > last_block_id ) {
      // don't read past the last block
      end_id = last_block_id;
   }
   
   // to be added to buf to calculate the block-aligned offset into which to copy data into the read buffer
   off_t buf_off = 0;
   
   // is the read unaligned with the first block boundary, and/or hits only inside this block?  i.e. is it a partial read head?
   if( (offset % core->blocking_factor) != 0 || start_id == end_id ) {
      // read a whole block, but we'll only take the part that doesn't overlap
      uint64_t block_id = start_id;
      int64_t block_version = fent->manifest->get_block_version( block_id );
      uint64_t gateway_id = fent->manifest->get_block_host( core, block_id );
      
      off_t block_read_start = (offset % core->blocking_factor);
      off_t block_read_end = MIN( ((offset) % core->blocking_factor) + real_count, core->blocking_factor );
      
      // if we're reading from an AG, then we don't know the size in advance.
      // Otherwise, we do, and we should not read past it.
      if( !is_AG && last_block_id == start_id ) {
         
         // only read up to the end of the file (even if the reader asked for more).
         block_read_end = MIN( (unsigned)block_read_end, fent->size % core->blocking_factor );
      }
      
      char* partial_result = CALLOC_LIST( char, core->blocking_factor );
      
      struct fs_entry_read_block_future* block_fut = CALLOC_LIST( struct fs_entry_read_block_future, 1 );
      fs_entry_read_block_future_init( block_fut, gateway_id, fs_path, fent->version, block_id, block_version, partial_result, core->blocking_factor, block_read_start, block_read_end, true );
      
      // align the next block future to the block boundary.
      buf_off = core->blocking_factor - (offset % core->blocking_factor);
      
      // this is a partial head block of the read 
      block_fut->result_is_partial_head = true;
      has_partial_head = true;
      
      dbprintf("block %" PRIu64 " is partial head, start = %jd, end = %jd\n", block_id, block_read_start, block_read_end );
      
      block_futs->insert( block_fut );
   }
   
   // is the read unaligned with the last block boundary, and is the last block different from the first one?  i.e. is it a partial read tail?
   if( ((offset + real_count) % core->blocking_factor) != 0 && start_id < end_id ) {
      // read a whole block, but we'll only keep the head of it
      uint64_t block_id = end_id;
      int64_t block_version = fent->manifest->get_block_version( block_id );
      uint64_t gateway_id = fent->manifest->get_block_host( core, block_id );
      
      off_t block_read_end = ((offset + real_count) % core->blocking_factor);
      
      // if we're reading from an AG, then we don't know the size in advance.
      // Otherwise, we do, and we should not read past it.
      if( !is_AG && last_block_id == end_id ) {
         
         // only read up to the end of the file (even if the reader asked for more).
         block_read_end = MIN( (unsigned)block_read_end, fent->size % core->blocking_factor );
      }
      
      char* partial_result = CALLOC_LIST( char, core->blocking_factor );
      
      struct fs_entry_read_block_future* block_fut = CALLOC_LIST( struct fs_entry_read_block_future, 1 );
      fs_entry_read_block_future_init( block_fut, gateway_id, fs_path, fent->version, block_id, block_version, partial_result, core->blocking_factor, 0, block_read_end, true );
      
      // this is a partial tail of the read 
      block_fut->result_is_partial_tail = true;
      has_partial_tail = true;
      
      dbprintf("block %" PRIu64 " is partial tail, end = %jd\n", block_id, block_read_end );
      
      block_futs->insert( block_fut );
   }
   
   if( has_partial_head )
      // covered this block already--adjust whole block start
      start_id++;
   
   if( end_id > 0 && (has_partial_tail || (offset + real_count) % core->blocking_factor == 0) )
      // covered this block already--adjust whole block end.  Also, skip a 0-length tail
      end_id--;
   
   // read whole blocks, if there is some data not covered by the head or tail
   for( uint64_t block_id = start_id; block_id <= end_id; block_id++ ) {
      
      int64_t block_version = fent->manifest->get_block_version( block_id );
      uint64_t gateway_id = fent->manifest->get_block_host( core, block_id );
      
      struct fs_entry_read_block_future* block_fut = CALLOC_LIST( struct fs_entry_read_block_future, 1 );
      
      // result_buf refers to data inside the client's read buffer.  It will be aligned to a block boundary, relative to the lowest-requested block
      char* result_buf = buf + buf_off + (core->blocking_factor * (block_id - start_id));
      fs_entry_read_block_future_init( block_fut, gateway_id, fs_path, fent->version, block_id, block_version, result_buf, core->blocking_factor, 0, core->blocking_factor, false );
      
      dbprintf("block %" PRIu64 " is whole\n", block_id);
      
      block_futs->insert( block_fut );
   }
   
   return 0;
}


// set up a read context, given the client's request
static int fs_entry_setup_read_context( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* buf, size_t real_count, off_t offset, struct fs_entry_read_context* read_ctx ) {
   // create a read context for this data 
   fs_entry_read_context_init( read_ctx );
   
   // split up the read into futures
   fs_entry_split_read( core, fs_path, fent, buf, real_count, offset, read_ctx->reads );
   
   return 0;
}


// merge a set of read blocks back into the client's read buffer.
// return the total length read (accounting for EOF), or negative on error
static ssize_t fs_entry_read_block_future_combine( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* buf, size_t real_count, off_t offset, fs_entry_read_block_future_set_t* reads ) {
   ssize_t rc = 0;
   int error_rc = 0;
   
   // go through the blocks and merge the ones that we allocated into the client's read buffer 
   for( fs_entry_read_block_future_set_t::iterator itr = reads->begin(); itr != reads->end(); itr++ ) {
      
      // get the block future
      struct fs_entry_read_block_future* block_fut = *itr;
      
      // did we encounter an error?
      if( block_fut->err != 0 ) {
         errorf("ERR: %s %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] download error = %d\n", 
                fs_path, fent->file_id, block_fut->file_version, block_fut->block_id, block_fut->block_version, block_fut->err );
         
         error_rc = -EREMOTEIO;
      }
      
      if( error_rc != 0 ) {
         // don't process any more blocks if we encountered an error 
         continue;
      }
      
      // number of bytes to copy for the result
      uint64_t result_copy = block_fut->result_end - block_fut->result_start;
      
      // allocated?  NOTE: if not, then the read already copied data in
      if( block_fut->result_allocd ) {
         // copy the relevant part into the read buffer 
         
         // partial head of the read?
         if( block_fut->result_is_partial_head ) {
            
            // EOF?
            if( block_fut->eof ) {
               dbprintf("block %" PRIu64 " is a partial read head EOF\n", block_fut->block_id );
               memset( buf, 0, result_copy );
               result_copy = 0;
            }
            else {
               // copy the partial head over 
               char prefix[11];
               memset( prefix, 0, 11 );
               memcpy( prefix, block_fut->result + block_fut->result_start, MIN(10, result_copy));
               
               dbprintf("block %" PRIu64 " head offset %jd length %" PRIu64 " prefix '%s'\n", block_fut->block_id, block_fut->result_start, result_copy, prefix );
               memcpy( buf, block_fut->result + block_fut->result_start, result_copy );
            }
         }
         // partial tail of the read?
         else if( block_fut->result_is_partial_tail ) {
            uint64_t buf_last_block_aligned_offset = real_count - result_copy;
            
            // EOF?
            if( block_fut->eof ) {
               dbprintf("block %" PRIu64 " is a partial read tail EOF, aligned offset = %jd\n", block_fut->block_id, buf_last_block_aligned_offset );
               memset( buf + buf_last_block_aligned_offset, 0, result_copy );
               result_copy = 0;
            }
            else {
               // copy the partial tail over 
               char prefix[11];
               memset( prefix, 0, 11 );
               memcpy( prefix, block_fut->result, MIN(10, result_copy));
               
               dbprintf("block %" PRIu64 " tail length %" PRIu64 ", aligned offset = %jd, prefix = '%s'\n", block_fut->block_id, result_copy, buf_last_block_aligned_offset, prefix );
               memcpy( buf + buf_last_block_aligned_offset, block_fut->result, result_copy );
            }
         }
         else {
            // shouldn't reach here
            errorf("BUG: %s offset %jd real_count %zu: block_fut->result_allocd, but neither a partial head nor tail\n", fs_path, offset, real_count );
            rc = -EIO;
            break;
         }
      }
      
      // Not allocated. Full-block EOF?  Then zero it
      else if( block_fut->eof ) {
         // nothing to copy; make sure all 0's
         dbprintf("block %" PRIu64 " eof\n", block_fut->block_id );
         memset( block_fut->result, 0, result_copy );
         result_copy = 0;
         continue;
      }
      
      else {
         dbprintf("block %" PRIu64 " filled in client buffer\n", block_fut->block_id );
         result_copy = block_fut->result_len;
      }
      
      // accounted for this block
      rc += result_copy;
   }
   
   if( error_rc != 0 ) {
      // exit in error 
      return (ssize_t)error_rc;
   }
   
   return rc;
}

// find the latest block downloaded.
// if fail_if_eof is true, then this fails if *any* block was EOF'ed.
// if fail_if_error is true, then this fails if *any* block encountered an error.
static struct fs_entry_read_block_future* fs_entry_find_latest_block( fs_entry_read_block_future_set_t* reads, bool fail_if_eof, bool fail_if_error ) {
   
   // Find the last block read
   struct fs_entry_read_block_future* last_block_fut = NULL;
   uint64_t last_block_id = 0;
   
   for( fs_entry_read_block_future_set_t::iterator itr = reads->begin(); itr != reads->end(); itr++ ) {
      
      // get the block future 
      struct fs_entry_read_block_future* block_fut = *itr;
      
      // EOF?
      if( block_fut->eof && fail_if_eof ) {
         return NULL;
      }
      
      // failure?
      if( block_fut->err != 0 && fail_if_error ) {
         return NULL;
      }
      
      if( block_fut->block_id > last_block_id ) {
         last_block_fut = block_fut;
         last_block_id = block_fut->block_id;
      }
   }
   
   return last_block_fut;
}



// update a file handle's cached block, IF none of the given blocks have EOF'ed or errored
// return 0 on success
// return negative on error.
// fent must be read-locked 
static int fs_entry_update_bufferred_block_read( struct fs_core* core, struct fs_entry* fent, fs_entry_read_block_future_set_t* reads ) {
   
   // Find the last block read, and if there wasn't an EOF or error, cache it in RAM.
   struct fs_entry_read_block_future* last_block_fut = fs_entry_find_latest_block( reads, true, true );
   
   if( last_block_fut != NULL ) {
      // we expect that a client reader will read blocks sequentially, for the most part.
      // so, cache the last read block to RAM so we can hit it on the next read.
      int has_block = fs_entry_has_bufferred_block( fent, last_block_fut->block_id );
      if( has_block == -ENOENT ) {
         // this block is not cached, so do so.
         dbprintf("block %" PRIu64 " will be bufferred in RAM\n", last_block_fut->block_id );
         fs_entry_replace_bufferred_block( core, fent, last_block_fut->block_id, last_block_fut->result, last_block_fut->result_len, false );
      }
   }
   
   return 0;
}

// clean up from running a read 
static int fs_entry_read_run_cleanup( struct fs_core* core, struct fs_entry_read_context* read_ctx ) {
   
   fs_entry_read_block_future_set_t* reads = NULL;
   
   fs_entry_read_context_free_ex( read_ctx, &reads );
   
   if( reads ) {
      fs_entry_read_block_futures_free_all( core, reads );
   }
   
   return 0;
}


// clean up cache futures.
// don't free the cache future's internal buffer (since it's just a pointer to a block future's internal buffer), but free everything else 
static void fs_entry_cleanup_cache_future( struct cache_block_future* f, void* cls ) {
   
   // remove the buffer from the future 
   fs_entry_cache_block_future_release_data( f );
   
   // clean up everything else 
   fs_entry_cache_block_future_free( f );
}

// service a read request.
// split the read into a series of block requests, and fetch each block.
// Try the bufferred block cache, then the disk block cache, then the CDN
// First, try to get each block from a local source (failing fast if it's not local).
// Second, if some blocks are not local, download them.
// fent must not be locked in any way.
// return 0 on success
static ssize_t fs_entry_read_run( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* buf, size_t count, off_t offset ) {
   
   ssize_t rc = 0;
   struct fs_entry_read_context read_ctx;
   
   uint64_t file_id = 0;
   int64_t file_version = 0;
   int64_t write_nonce = 0;
   off_t file_size = 0;
   
   fs_entry_rlock( fent );
   
   // preserve information on fent 
   file_id = fent->file_id;
   file_version = fent->version;
   write_nonce = fent->write_nonce;
   file_size = fent->size;
   
   // are we EOF already?
   if( offset > fent->size ) {
      // EOF 
      fs_entry_unlock( fent );
      return 0;
   }
   
   // how many bytes are we actually going to combine into the read buffer?
   size_t real_count = count;
   if( (signed)(count + offset) >= file_size )
      real_count = file_size - offset;
   
   // cache block futures 
   vector<struct cache_block_future*> cache_futs;
   
   // set up a read context for this request 
   fs_entry_setup_read_context( core, fs_path, fent, buf, real_count, offset, &read_ctx );
   
   // get local blocks
   rc = fs_entry_read_context_run_local( core, fs_path, fent, &read_ctx );
   
   fs_entry_unlock( fent );
   
   if( rc != 0 && rc != -EREMOTE ) {
      // failed, for some reason besides some blocks being non-local 
      errorf("fs_entry_read_context_run_local( %s ) rc = %zd\n", fs_path, rc );
      fs_entry_read_run_cleanup( core, &read_ctx );
      return rc;
   }
   else if( rc == -EREMOTE ) {
      // at least one block was remote. Set up downloads 
      rc = fs_entry_read_context_setup_downloads( core, fent, &read_ctx );
      if( rc != 0 ) {
         // failed...
         errorf("fs_entry_read_context_setup_downloads( %s ) rc = %zd\n", fs_path, rc );
         fs_entry_read_run_cleanup( core, &read_ctx );
         return rc;
      }
      
      dbprintf("Begin downloading blocks for %" PRIX64 "\n", fent->file_id );
         
      struct timespec ts, ts2;
      BEGIN_TIMING_DATA( ts );
      
      // Go get it/them.
      while( fs_entry_read_context_has_downloading_blocks( &read_ctx ) ) {
         
         // get some data, and cache blocks as we get them 
         rc = fs_entry_read_context_run_downloads_ex( core, fent, &read_ctx, false, fs_entry_read_block_future_finalizer_cache_async, &cache_futs );
         if( rc < 0 ) {
            errorf("fs_entry_read_context_run_downloads_ex( %s ) rc = %zd\n", fs_path, rc );
            fs_entry_read_run_cleanup( core, &read_ctx );
            return rc;
         }
         rc = 0;
      }
      
      END_TIMING_DATA( ts, ts2, "read remote blocks" );
   
      dbprintf("End downloading blocks for %" PRIX64 "\n", fent->file_id );
   }
   
   // succeess!
   
   // combine the blocks into the client buffer
   rc = fs_entry_read_block_future_combine( core, fs_path, fent, buf, real_count, offset, read_ctx.reads );
   if( rc < 0 ) {
      errorf("fs_entry_read_context_combine( %s ) rc = %zd\n", fs_path, rc );
      fs_entry_read_run_cleanup( core, &read_ctx );
      return rc;
   }
   
   // finish caching all data, but only cache to bufferred blocks if the file was not modified 
   fs_entry_wlock( fent );

   if( fs_entry_was_modified( fent, file_id, file_version, write_nonce ) ) {
      
      // file is not the same anymore--the data sent to the client is now stale
      dbprintf("WARN: Will NOT cache read: file ID %" PRIX64 " --> %" PRIX64 ", version %" PRId64 " --> %" PRId64 ", write nonce %" PRId64 " --> %" PRId64 "\n",
               file_id, fent->file_id, file_version, fent->version, write_nonce, fent->write_nonce );
   }
   else {
      // file not modified during our read, so update the cached data
      fs_entry_update_bufferred_block_read( core, fent, read_ctx.reads );
   }
   
   fs_entry_unlock( fent );

   // finish caching all downloaded blocks to disk
   int cache_rc = fs_entry_flush_cache_writes( &cache_futs );
   if( cache_rc != 0 ) {
      errorf("fs_entry_flush_cache_writes( %s ) rc = %d\n", fs_path, cache_rc );
   }
   
   // clean up cache futures, releasing their internal buffers (since they point to block future buffers)
   fs_entry_cache_block_future_apply_all( &cache_futs, fs_entry_cleanup_cache_future, NULL );
   
   // free the context
   fs_entry_read_run_cleanup( core, &read_ctx );
   return rc;
}


// top-level read request 
ssize_t fs_entry_read( struct fs_core* core, struct fs_file_handle* fh, char* buf, size_t count, off_t offset ) {
   
   fs_file_handle_rlock( fh );
   
   // sanity check
   if( fh->open_count <= 0 ) {
      // invalid
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   // refresh metadata
   int rc = fs_entry_revalidate_metadata( core, fh->path, fh->fent, NULL );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_metadata(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   fs_entry_rlock( fh->fent );
   
   // sanity check
   if( !IS_STREAM_FILE( *(fh->fent) ) && fh->fent->size < offset ) {
      // EOF
      fs_entry_unlock( fh->fent );
      fs_file_handle_unlock( fh );
      return 0;
   }

   fs_entry_unlock( fh->fent );
   
   
   // run the read
   ssize_t num_read = fs_entry_read_run( core, fh->path, fh->fent, buf, count, offset );
   if( num_read < 0 ) {
      errorf("fs_entry_read_run( %s offset = %jd, count = %zu ) rc = %zd\n", fh->path, offset, count, num_read );
      fs_file_handle_unlock( fh );
      return num_read;
   }
   
   fs_file_handle_unlock( fh );
   
   return num_read;
}
