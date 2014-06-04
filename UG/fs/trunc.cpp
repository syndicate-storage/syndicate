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


#include "trunc.h"
#include "manifest.h"
#include "url.h"
#include "network.h"
#include "stat.h"
#include "read.h"
#include "write.h"
#include "replication.h"
#include "cache.h"
#include "sync.h"


// read one block, and fill the end of it with zeros 
static int fs_entry_get_truncated_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, off_t block_zero_offset, char** _block_buf, size_t* _block_len ) {
   
   // go get the block 
   char* block_buf = CALLOC_LIST( char, core->blocking_factor );
   int rc = fs_entry_read_block( core, fs_path, fent, block_id, block_buf, core->blocking_factor );
   
   if( rc != 0 ) {
      errorf("fs_entry_read_block( %s %" PRIu64 " ) rc = %d\n", fs_path, block_id, rc );
      
      free( block_buf );
      
      return -ENODATA;
   }
   
   // success!
   // truncate it 
   memset( block_buf + block_zero_offset, 0, core->blocking_factor - block_zero_offset );
   
   *_block_buf = block_buf;
   *_block_len = core->blocking_factor;
   
   return 0;
}


// shrink a file down to a new size--
// accumulate garbage blocks, so we can go garbage-collect them.
// this updates the manifest and file size and modtime
// fent must be write-locked
static int fs_entry_shrink_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t new_size, modification_map* garbage_blocks ) {
   
   // sanity check
   if( fent->size < new_size ) 
      return -EINVAL;
   
   if( fent->size == new_size )
      return 0;
   
   int err = 0;
   
   uint64_t old_max_block = fs_entry_block_id( core, fent->size );
   uint64_t new_max_block = fs_entry_block_id( core, new_size );
   
   // mark all blocks beyond new_max_block as garbage blocks
   for( uint64_t block_id = new_max_block; block_id != old_max_block; block_id++ ) {
      
      struct fs_entry_block_info old_binfo;
      
      memset( &old_binfo, 0, sizeof(struct fs_entry_block_info) );
      
      // put a garbage block
      int64_t old_block_version = fent->manifest->get_block_version( block_id );
      unsigned char* old_block_hash = fent->manifest->get_block_hash( block_id );
      
      fs_entry_block_info_garbage_init( &old_binfo, old_block_version, old_block_hash, BLOCK_HASH_LEN(), fent->coordinator );
      
      (*garbage_blocks)[block_id] = old_binfo;
   }
   
   // cut off the records in the manifest
   // (NOTE: updates manifest modtime)
   fent->manifest->truncate( new_max_block );
   
   // set the new size
   fent->size = new_size;
   
   fs_entry_update_modtime( fent );
   
   return err;
}

// expand a file to a new size (e.g. if we write to it beyond the end of the file).
// add the block info for the block at the end of the file to garbage_blocks and dirty blocks, since it will need to be written and garbage-collected
// fent must be write-locked
static int fs_entry_expand_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t new_size, modification_map* dirty_blocks, modification_map* garbage_blocks, struct cache_block_future** last_block_fut ) {

   int rc = 0;
   
   uint64_t start_id = fs_entry_block_id( core, fent->size );
   uint64_t end_id = fs_entry_block_id( core, new_size );
   
   // get the block to truncate 
   off_t block_truncate_offset = core->blocking_factor % fent->size;
   char* truncated_block = NULL;
   size_t truncated_block_len = 0;
   
   rc = fs_entry_get_truncated_block( core, fs_path, fent, start_id, block_truncate_offset, &truncated_block, &truncated_block_len );
   if( rc != 0 ) {
      // failed
      errorf("fs_entry_get_truncated_block( %s %" PRIu64 " ) rc = %d\n", fs_path, start_id, rc );
      return -ENODATA;
   }
   
   // process and flush it to disk.
   // record block information for the old and new versions.
   
   struct fs_entry_block_info old_binfo, new_binfo;
   
   memset( &old_binfo, 0, sizeof(struct fs_entry_block_info) );
   memset( &new_binfo, 0, sizeof(struct fs_entry_block_info) );
   
   struct cache_block_future* cache_fut = fs_entry_write_block_async( core, fs_path, fent, start_id, truncated_block, truncated_block_len, &old_binfo, &new_binfo, &rc );
   
   // did it work?  If not, bail
   if( rc != 0 || cache_fut == NULL ) {
      errorf("fs_entry_write_block_async( %s %" PRIX64 ".%" PRId64 "[%" PRIu64 "] rc = %d\n",
              fs_path, fent->file_id, fent->version, start_id, rc );
      
      return -EIO;
   }
   
   // record dirty and garbage
   (*dirty_blocks)[ start_id ] = new_binfo;
   (*garbage_blocks)[ start_id ] = old_binfo;
   
   // put write holes in for the remaining blocks 
   for( uint64_t block_id = start_id + 1; block_id < end_id; block_id++ ) {
      fent->manifest->put_hole( core, fent, block_id );
   }
   
   // set the new size and modtime
   fent->size = new_size;
   fs_entry_update_modtime( fent );
   
   *last_block_fut = cache_fut;
   
   return rc;
}


// reversion a file on the MS, and keep the cache coherent at the same time.  Only valid for local files 
// fent must be write-locked
static int fs_entry_reversion_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t new_version, uint64_t parent_id, char const* parent_name ) {

   if( !FS_ENTRY_LOCAL( core, fent ) ) {
      return -EINVAL;
   }
   
   // reversion the data locally.  ENOENT here is fine, since it means that the data wasn't cached
   int rc = fs_entry_cache_reversion_file( core, core->cache, fent->file_id, fent->version, new_version );
   if( rc != 0 ) {
      if( rc != -ENOENT ) {
         errorf("fs_entry_cache_reversion_file(%s (%" PRIX64 ".%" PRId64 " --> %" PRId64 ")) rc = %d\n", fs_path, fent->file_id, fent->version, new_version, rc );
         return rc;
      }
   }

   // set the version on local data, since the local reversioning succeeded
   int64_t old_version = fent->version;
   
   fent->version = new_version;
   fent->manifest->set_file_version( core, new_version );

   struct md_entry ent;
   fs_entry_to_md_entry( core, &ent, fent, parent_id, parent_name );

   // synchronously update
   rc = ms_client_update( core->ms, &ent );

   md_entry_free( &ent );

   if( rc != 0 ) {
      // failed to reversion remotely
      errorf("ms_client_update( %s %" PRId64 " --> %" PRId64 ") rc = %d\n", fs_path, fent->version, new_version, rc );
      
      // recover 
      fent->version = old_version;
      fent->manifest->set_file_version( core, old_version );
   }
   
   return rc;
}


// truncate a local file's data, updating its manifest and size and remembering the now-dirty and garbage blocks
// fent must be write-locked
// NOTE: we reversion on truncate
static int fs_entry_truncate_local( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size ) {

   // last block in the file now
   uint64_t max_block = fs_entry_block_id( core, fent->size );
   if( fent->size % core->blocking_factor > 0 ) {
      max_block++;      // preserve the remainder of the last block
   }

   // potentially, the last block in the file on success
   uint64_t new_max_block = fs_entry_block_id( core, size );
   if( size % core->blocking_factor > 0 ) {
      new_max_block++;  // preserve the remainder of the last block
   }
   
   // remember blocks that we need to garbage-collect, and blocks that we need to replicate
   modification_map dirty_blocks;
   modification_map garbage_blocks;
   modification_map unmerged_garbage;
   
   int rc = 0;
   
   if( new_max_block <= max_block ) {
      // file's getting smaller 
      rc = fs_entry_shrink_file( core, fs_path, fent, size, &garbage_blocks );
      
      if( rc != 0 ) {
         errorf("fs_entry_shrink_file( %s to %jd ) rc = %d\n", fs_path, size, rc );
         
         return -EIO;
      }
   }
   else {
      // file's getting bigger.  we'll put one block 
      struct cache_block_future* cache_fut = NULL;
      rc = fs_entry_expand_file( core, fs_path, fent, size, &dirty_blocks, &garbage_blocks, &cache_fut );
      
      if( rc != 0 ) {
         errorf("fs_entry_expand_file( %s to %jd ) rc = %d\n", fs_path, size, rc );
         
         if( rc == -ENODATA ) {
            // remote IO error
            return -EREMOTEIO;
         }
         else {
            return -EIO;
         }
      }
      
      // flush the write
      rc = fs_entry_flush_cache_write( cache_fut );
      if( rc != 0 ) {
         errorf( "fs_entry_flush_cache_write( %s %" PRIu64 " ) rc = %d\n", fs_path, max_block, rc );
         
         fs_entry_cache_block_future_free( cache_fut );
         return -EIO;
      }
   }
   
   // merge new blocks and garbage, so we can sync it
   fs_entry_merge_new_dirty_blocks( fent, &dirty_blocks );
   fs_entry_merge_garbage_blocks( core, fent, fent->file_id, fent->version, &garbage_blocks, &unmerged_garbage );
   
   fs_entry_free_modification_map_ex( &dirty_blocks, false );   // keep file descriptors open
   
   // NOTE: no need to free garbage blocks
   //fs_entry_free_modification_map( &garbage_blocks );
   
   // TODO; garbage-collect now?
   fs_entry_free_modification_map( &unmerged_garbage );         // TODO: garbage-collection persistence?
   
   return 0;
}


// remote truncate 
// inform the remote block owner that the data must be truncated
// return 0 on success
// return 1 if we're now the coordinator
// return -EIO on network error
// fent must be write-locked
int fs_entry_truncate_remote( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size ) {

   bool coordinator = false;
   int err = 0;
   uint64_t max_block = fs_entry_block_id( core, fent->size );
   uint64_t new_max_block = fs_entry_block_id( core, size );
   
   // build up a truncate write message
   Serialization::WriteMsg *truncate_msg = new Serialization::WriteMsg();
   Serialization::WriteMsg *withdraw_ack = new Serialization::WriteMsg();
      
   fs_entry_init_write_message( truncate_msg, core, Serialization::WriteMsg::TRUNCATE );
   fs_entry_prepare_truncate_message( truncate_msg, fs_path, fent, new_max_block );

   // send the truncate, or try to coordainte
   err = fs_entry_send_write_or_coordinate( core, fs_path, fent, truncate_msg, withdraw_ack );
   
   if( err == 1 ) {
      // we're now the coordinator!
      err = 0;
      coordinator = true;
   }
   
   if( err != 0 ) {
      errorf( "fs_entry_post_write(%" PRIu64 "-%" PRIu64 ") rc = %d\n", new_max_block, max_block, err );
      err = -EIO;
   }
   else if( withdraw_ack->type() != Serialization::WriteMsg::ACCEPTED ) {
      if( withdraw_ack->type() == Serialization::WriteMsg::ERROR ) {
         errorf( "remote truncate failed, error = %d (%s)\n", withdraw_ack->errorcode(), withdraw_ack->errortxt().c_str() );
         err = withdraw_ack->errorcode();
      }
      else {
         errorf( "remote truncate invalid message %d\n", withdraw_ack->type() );
         err = -EIO;
      }
   }
   else {
      // success!
      err = 0;
   }
   
   delete withdraw_ack;
   delete truncate_msg;

   // the remote host will have reversioned the file.
   // we need to refresh its metadata before then.
   if( !coordinator && err == 0 ) {
      fs_entry_mark_read_stale( fent );
   }
   
   if( err == 0 && coordinator ) {
      return 1;
   }
   else {
      return err;
   }
}


// perform the truncate 
// fent must be write-locked
// NOTE: we must block concurrent writers
int fs_entry_run_truncate( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size, uint64_t parent_id, char* parent_name ) {
   
   // make sure we have the latest manifest 
   int rc = 0;
   int err = fs_entry_revalidate_manifest( core, fs_path, fent );
   if( err != 0 ) {
      errorf( "fs_entry_revalidate_manifest(%s) rc = %d\n", fs_path, err );
      
      return err;
   }
   
   bool local = FS_ENTRY_LOCAL( core, fent );
   
   // if not local, then send the truncate request.
   // we might become the coordinator.
   if( !local ) {
      // remote 
      rc = fs_entry_truncate_remote( core, fs_path, fent, size );
      
      if( rc < 0 ) {
         errorf("fs_entry_truncate_remote( %s to %jd ) rc = %d\n", fs_path, size, rc );
         
         return rc;
      }
      
      if( rc == 1 ) {
         // we're now the coordinator
         local = true;
      }
   }
   
   // if this entry is local, then truncate it
   if( local ) {
      rc = fs_entry_truncate_local( core, fs_path, fent, size );
      
      if( rc != 0 ) {
         errorf("fs_entry_truncate_local( %s to %jd ) rc = %d\n", fs_path, size, rc );
         
         return rc;
      }
      
      // start synchronizing the entry's data (getting a snapshot in the process)
      // bypass the sync queue, since we're reversioning
      struct sync_context sync_ctx;
      memset( &sync_ctx, 0, sizeof(struct sync_context) );
      
      rc = fs_entry_sync_data_begin( core, fs_path, fent, parent_id, parent_name, &sync_ctx );
      if( rc < 0 ) {
         errorf("fs_entry_sync_data_begin( %s ) rc = %d\n", fs_path, rc );
         
         return -EIO;
      }
      
      // wait for synchronization to finish 
      rc = fs_entry_sync_data_finish( core, &sync_ctx );
      if( rc != 0 ) {
         errorf("fs_entry_sync_data_finish( %s ) rc = %d\n", fs_path, rc );
         
         // revert 
         fs_entry_sync_data_revert( core, fent, &sync_ctx );
         
         fs_entry_sync_context_free( &sync_ctx );
         return -EIO;
      }
      
      // replicate data; do the reversion 
      int64_t new_version = fs_entry_next_file_version();
      rc = fs_entry_reversion_file( core, fs_path, fent, new_version, parent_id, parent_name );
      
      if( rc != 0 ) {
         errorf("fs_entry_reversion_file( %s %" PRId64 " --> %" PRId64 " ) rc = %d\n", fs_path, fent->version, new_version, rc );
         
         // undo truncate
         fs_entry_sync_data_revert( core, fent, &sync_ctx );
         
         fs_entry_sync_context_free( &sync_ctx );
         return rc;
      }
      
      fs_entry_sync_context_free( &sync_ctx );
   }
   
   return 0;
}


// truncate, only if the version is correct (or ignore it if it's -1)
int fs_entry_versioned_truncate(struct fs_core* core, const char* fs_path, uint64_t file_id, uint64_t coordinator_id, off_t newsize,
                                int64_t known_version, uint64_t user, uint64_t volume, uint64_t gateway_id, bool check_file_id_and_coordinator_id ) {

   
   if( core->gateway == GATEWAY_ANON ) {
      errorf("%s", "Truncating is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
   int err = fs_entry_revalidate_path( core, volume, fs_path );
   if( err != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fs_path, err );
      return -EREMOTEIO;
   }

   // entry exists
   // write-lock the fs entry
   char* parent_name = NULL;
   uint64_t parent_id = 0;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, user, volume, true, &err, &parent_id, &parent_name );
   if( fent == NULL || err ) {
      errorf( "fs_entry_resolve_path(%s), rc = %d\n", fs_path, err );
      return err;
   }
   
   // validate input
   if( check_file_id_and_coordinator_id ) {
      if( fent->file_id != file_id ) {
         errorf("Remote truncate to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", fs_path, file_id, fent->file_id );
         fs_entry_unlock( fent );
         free( parent_name );
         return -ESTALE;
      }
      
      if( fent->coordinator != coordinator_id ) {
         errorf("Remote truncate to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", fs_path, coordinator_id, fent->coordinator );
         fs_entry_unlock( fent );
         free( parent_name );
         return -ESTALE;
      }
   }
   
   if( known_version > 0 && fent->version > 0 && fent->version != known_version ) {
      errorf("Remote truncate to file %s version %" PRId64 ", expected %" PRId64 "\n", fs_path, known_version, fent->version );
      fs_entry_unlock( fent );
      free( parent_name );
      return -ESTALE;
   }

   // do the truncate
   int rc = fs_entry_run_truncate( core, fs_path, fent, newsize, parent_id, parent_name );
   free( parent_name );
   
   if( rc != 0 ) {
      errorf( "fs_entry_truncate(%s) rc = %d\n", fs_path, rc );

      fs_entry_unlock( fent );
      return rc;
   }

   fs_entry_unlock( fent );

   return rc;
}


// truncate an file
int fs_entry_truncate( struct fs_core* core, char const* fs_path, off_t size, uint64_t user, uint64_t volume ) {
   
   int err = fs_entry_revalidate_path( core, volume, fs_path );
   if( err != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fs_path, err );
      return -EREMOTEIO;
   }

   // entry exists
   // write-lock the fs entry
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, user, volume, true, &err, &parent_id, &parent_name );
   if( fent == NULL || err ) {
      errorf( "fs_entry_resolve_path(%s), rc = %d\n", fs_path, err );
      return err;
   }
   
   err = fs_entry_run_truncate( core, fs_path, fent, size, parent_id, parent_name );

   fs_entry_unlock( fent );
   free( parent_name );

   return err;
}

// truncate a file
int fs_entry_ftruncate( struct fs_core* core, struct fs_file_handle* fh, off_t size, uint64_t user, uint64_t volume ) {
   fs_file_handle_rlock( fh );
   fs_entry_wlock( fh->fent );

   int rc = fs_entry_run_truncate( core, fh->path, fh->fent, size, fh->parent_id, fh->parent_name );
   
   fs_entry_unlock( fh->fent );
   fs_file_handle_unlock( fh );
   return rc;
}
