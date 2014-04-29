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


// truncate a specific block to a given size 
// fent must be write-locked
int fs_entry_truncate_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t trunc_block_id, size_t new_block_size,
                             modification_map* modified_blocks, modification_map* overwritten_blocks, list<struct cache_block_future*>* futures ) {
   // truncate the last block
   char* block = CALLOC_LIST( char, core->blocking_factor );
   int err = 0;
   
   ssize_t nr = fs_entry_read_block( core, fs_path, fent, trunc_block_id, block, core->blocking_factor );
   if( nr < 0 ) {
      errorf( "fs_entry_read_block(%s[%" PRIu64 "]) rc = %zd\n", fs_path, trunc_block_id, nr );
      err = nr;
   }
   else {
      // truncate this block
      uint64_t old_version = fent->manifest->get_block_version( trunc_block_id );
      unsigned char* old_hash = fent->manifest->get_block_hash( trunc_block_id );
      int rc = 0;
      
      struct cache_block_future* f = fs_entry_write_block_async( core, fs_path, fent, trunc_block_id, block, new_block_size, true, &rc );
      if( f == NULL ) {
         errorf("fs_entry_write_block_async(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 "]) failed, rc = %d\n", fs_path, fent->file_id, fent->version, trunc_block_id, rc );
         free( old_hash );
      }
      else {
         // record that we've written this block
         struct fs_entry_block_info binfo;
         
         unsigned char* new_hash = fent->manifest->get_block_hash( trunc_block_id );
         
         // NOTE: pass in the block file descriptor, so don't close it!
         fs_entry_block_info_replicate_init( &binfo, fent->manifest->get_block_version( trunc_block_id ), new_hash, BLOCK_HASH_LEN(), core->gateway, f->block_fd );
         
         (*modified_blocks)[ trunc_block_id ] = binfo;
         
         // garbage collect the old version
         struct fs_entry_block_info erase_binfo;
         
         fs_entry_block_info_garbage_init( &binfo, old_version, old_hash, BLOCK_HASH_LEN(), core->gateway );
         
         (*overwritten_blocks)[ trunc_block_id ] = erase_binfo;
         
      }
      
      futures->push_back( f );
   }

   free( block );
   
   return err;
}


// shrink a file down to a new size.
// fent must be write-locked
int fs_entry_shrink_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size,
                          modification_map* modified_blocks, modification_map* overwritten_blocks, list<struct cache_block_future*>* futures ) {
   if( fent->size < size ) 
      return -EINVAL;
   
   if( fent->size == size )
      return 0;
   
   int err = 0;
   bool local = FS_ENTRY_LOCAL( core, fent );
   
   uint64_t trunc_block_id = 0;
   
   // which blocks need to be withdrawn?
   uint64_t max_block = fent->size / core->blocking_factor;
   if( fent->size % core->blocking_factor > 0 ) {
      max_block++;      // preserve the remainder of the last block
   }

   uint64_t new_max_block = size / core->blocking_factor;
   if( size % core->blocking_factor > 0 ) {
      trunc_block_id = new_max_block;      // need to truncate the last block
      new_max_block++;
   }
   
   if( trunc_block_id > 0 || size > 0 ) {
      // truncate this block
      size_t new_len = size % core->blocking_factor;
      err = fs_entry_truncate_block( core, fs_path, fent, trunc_block_id, new_len, modified_blocks, overwritten_blocks, futures );
      if( err != 0 ) {
         errorf("fs_entry_truncate_block(%s (%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "])) rc = %d\n", fs_path, fent->file_id, fent->version, trunc_block_id, fent->manifest->get_block_version( trunc_block_id ), err );
      }
   }
   
   if( local && err == 0 ) {
      // unlink the blocks that would have been cut off
      for( uint64_t i = new_max_block; i < max_block; i++ ) {
         
         struct fs_entry_block_info erase_binfo;
         
         uint64_t old_version = fent->manifest->get_block_version(i);
         
         unsigned char* old_hash = fent->manifest->get_block_hash(i);
         
         fs_entry_block_info_garbage_init( &erase_binfo, old_version, old_hash, BLOCK_HASH_LEN(), fent->manifest->get_block_host( core, i ) );
         
         (*overwritten_blocks)[ i ] = erase_binfo;
         
         int rc = fs_entry_cache_evict_block( core, core->cache, fent->file_id, fent->version, i, old_version);
         if( rc != 0 && rc != -ENOENT ) {
            errorf("fs_entry_cache_evict_block(%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s)) rc = %d\n", fent->file_id, fent->version, i, old_version, fs_path, rc );
         }
      }

      // cut off the records in the manifest
      fent->manifest->truncate( new_max_block );
   }

   if( err == 0 ) {
      fent->size = size;
   }
   
   return err;
}

// expand a file to a new size (e.g. if we write to it beyond the end of the file).
// record the blocks newly-added, and the blocks that were modified.  Also, fill in all the write futures for these blocks
// fent must be write-locked
int fs_entry_expand_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t new_size,
                          modification_map* modified_blocks, modification_map* overwritten_blocks, list<struct cache_block_future*>* futures ) {
   
   off_t old_size = fent->size;

   fent->size = new_size;

   int err = 0;
   int rc = 0;
   
   uint64_t start_id = fs_entry_block_id( core, old_size );
   uint64_t end_id = fs_entry_block_id( core, new_size );
   
   // expand the block at the end of the file, if needed
   if( old_size > 0 && (old_size % core->blocking_factor) > 0) {
      size_t new_block_size = 0;
      if( end_id == start_id ) {
         // truncate is in the same block, so expand the block up to the new size
         new_block_size = new_size % core->blocking_factor;
      }
      else {
         // fill the remainder of this block with 0's
         new_block_size = core->blocking_factor;
      }
      
      rc = fs_entry_truncate_block( core, fs_path, fent, start_id, new_block_size, modified_blocks, overwritten_blocks, futures );
      if( rc != 0 ) {
         errorf("fs_entry_truncate_block(%s (%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "])) rc = %d\n", fs_path, fent->file_id, fent->version, start_id, fent->manifest->get_block_version( start_id ), rc );
         err = rc;
      }
   }
   
   if( err == 0 ) {
      char* block = CALLOC_LIST( char, core->blocking_factor );
      
      // replicate the expanded file data
      for( uint64_t id = start_id + 1; id <= end_id; id++ ) {
         
         struct cache_block_future* f = fs_entry_write_block_async( core, fs_path, fent, id, block, core->blocking_factor, true, &rc );
         if( f == NULL ) {
            errorf("fs_entry_write_block_async(%s %" PRIX64 ".%" PRId64 "[%" PRIu64 "]) failed, rc = %d\n", fs_path, fent->file_id, fent->version, id, rc );
            err = rc;
            break;
         }
         else {
            // record that we've written this block
            struct fs_entry_block_info binfo;
            
            unsigned char* new_hash = fent->manifest->get_block_hash( id );
            
            // NOTE: pass in the block file descriptor, so don't close it!
            fs_entry_block_info_replicate_init( &binfo, fent->manifest->get_block_version( id ), new_hash, BLOCK_HASH_LEN(), core->gateway, f->block_fd );
            
            (*modified_blocks)[ id ] = binfo;
         }
         
         futures->push_back( f );
      }
      
      free( block );
   }
   return err;
}

// make a truncate message
// fent must be at least read-locked
static void fs_entry_prepare_truncate_message( Serialization::WriteMsg* truncate_msg, char const* fs_path, struct fs_entry* fent, uint64_t new_max_block ) {
   Serialization::TruncateRequest* truncate_req = truncate_msg->mutable_truncate();
   
   truncate_req->set_volume_id( fent->volume );
   truncate_req->set_coordinator_id( fent->coordinator );
   truncate_req->set_file_id( fent->file_id );
   truncate_req->set_fs_path( fs_path );
   truncate_req->set_file_version( fent->version );
   truncate_req->set_size( fent->size );

   Serialization::BlockList* blocks = truncate_msg->mutable_blocks();
   blocks->set_start_id( 0 );
   blocks->set_end_id( new_max_block );

   for( uint64_t i = 0; i < new_max_block; i++ ) {
      int64_t block_version = fent->manifest->get_block_version( i );

      blocks->add_version( block_version );
   }
}

                                               
// truncate an open file.
// fent must be write locked.
// NOTE: we must reversion the file on truncate, since size can't decrease on the MS for the same version of the entry!
int fs_entry_truncate_real( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size, uint64_t user, uint64_t volume, uint64_t parent_id, char const* parent_name ) {

   // make sure we have the latest manifest 
   int rc = 0;
   int err = fs_entry_revalidate_manifest( core, fs_path, fent );
   if( err != 0 ) {
      errorf( "fs_entry_revalidate_manifest(%s) rc = %d\n", fs_path, err );
      fs_entry_unlock( fent );
      return err;
   }
   
   // which blocks need to be withdrawn?
   uint64_t max_block = fent->size / core->blocking_factor;
   if( fent->size % core->blocking_factor > 0 ) {
      max_block++;      // preserve the remainder of the last block
   }

   uint64_t new_max_block = size / core->blocking_factor;
   if( size % core->blocking_factor > 0 ) {
      new_max_block++;
   }

   // which blocks are modified?
   modification_map modified_blocks;
   
   // old block information 
   modification_map overwritten_blocks;
   
   // block futures
   list<struct cache_block_future*> futures;
   
   // fent snapshot before we do anything
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );

   // are we going to lose any remote blocks?
   bool local = FS_ENTRY_LOCAL( core, fent );

   // if we're removing blocks, then we'll need to withdraw them.
   if( size < fent->size ) {
      rc = fs_entry_shrink_file( core, fs_path, fent, size, &modified_blocks, &overwritten_blocks, &futures );
      if( rc == 0 ) {
         errorf("fs_entry_shrink_file(%s) rc = %d\n", fs_path, rc );
         err = rc;
      }
   }
   else if( size > fent->size ) {
      rc = fs_entry_expand_file( core, fs_path, fent, size, &modified_blocks, &overwritten_blocks, &futures );
      if( rc != 0 ) {
         errorf("fs_entry_expand_file(%s) rc = %d\n", fs_path, rc );
         err = rc;
      }
   }
   
   // wait for all cache writes to finish
   rc = fs_entry_finish_writes( futures, false );
   
   if( rc == 0 && err == 0 && !local ) {
      // inform the remote block owner that the data must be truncated
      // build up a truncate write message
      Serialization::WriteMsg *truncate_msg = new Serialization::WriteMsg();
      fs_entry_init_write_message( truncate_msg, core, Serialization::WriteMsg::TRUNCATE );
      fs_entry_prepare_truncate_message( truncate_msg, fs_path, fent, new_max_block );

      Serialization::WriteMsg *withdraw_ack = new Serialization::WriteMsg();
      
      err = fs_entry_send_write_or_coordinate( core, fs_path, fent, &fent_snapshot, truncate_msg, withdraw_ack );
      
      if( err == 1 ) {
         // we're now the coordinator!
         err = 0;
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
      if( !FS_ENTRY_LOCAL( core, fent ) ) {
         fs_entry_mark_read_stale( fent );
      }
   }


   // replicate data
   if( err == 0 ) {
      
      // make a file handle, but only for purposes of replication.
      // This lets us start all replicas concurrently, and then block until they're all done.
      struct fs_file_handle fh;
      memset( &fh, 0, sizeof(fh) );
      fs_entry_replica_file_handle( core, fs_path, fent, O_SYNC, &fh );
      
      rc = fs_entry_replicate_write( core, &fh, &modified_blocks );
      
      fs_entry_free_replica_file_handle( &fh );
      
      if( rc != 0 )
         err = rc;
   }

   // reversion this file atomically
   if( err == 0 && local ) {

      int64_t new_version = fs_entry_next_file_version();

      err = fs_entry_reversion_file( core, fs_path, fent, new_version, parent_id, parent_name );

      if( err != 0 ) {
         errorf("fs_entry_reversion_file(%s.%" PRId64 " --> %" PRId64 ") rc = %d\n", fs_path, fent->version, new_version, err );
      }
      else {
         err = 0;
      }
   }
   
   // garbage collect old manifest and block on success
   if( err == 0 ) {
      rc = fs_entry_garbage_collect_overwritten_data( core, fent, &fent_snapshot, &overwritten_blocks );
      
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_blocks(%s) rc = %d\n", fs_path, rc );
      }
   }
   
   // recover if error
   if( err != 0 ) {
      rc = fs_entry_revert_write( core, fent, &fent_snapshot, size, &modified_blocks, &overwritten_blocks, true );
      if( rc != 0 ) {
         errorf("fs_entry_revert_write(%s) rc = %d\n", fs_path, rc );
         rc = 0;
      }
   }
   
   fs_entry_free_modification_map( &modified_blocks );
   fs_entry_free_modification_map( &overwritten_blocks );
   
   dbprintf("file size is now %" PRId64 "\n", (int64_t)fent->size );

   return err;
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

   int rc = fs_entry_truncate_real( core, fs_path, fent, newsize, user, volume, parent_id, parent_name );
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
   
   err = fs_entry_truncate_real( core, fs_path, fent, size, user, volume, parent_id, parent_name );

   fs_entry_unlock( fent );
   free( parent_name );

   return err;
}

// truncate a file
int fs_entry_ftruncate( struct fs_core* core, struct fs_file_handle* fh, off_t size, uint64_t user, uint64_t volume ) {
   fs_file_handle_rlock( fh );
   fs_entry_wlock( fh->fent );

   int rc = fs_entry_truncate_real( core, fh->path, fh->fent, size, user, volume, fh->parent_id, fh->parent_name );
   
   fs_entry_unlock( fh->fent );
   fs_file_handle_unlock( fh );
   return rc;
}
