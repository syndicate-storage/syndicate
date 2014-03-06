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
   int err = fs_entry_revalidate_manifest( core, fs_path, fent );
   if( err != 0 ) {
      errorf( "fs_entry_revalidate_manifest(%s) rc = %d\n", fs_path, err );
      fs_entry_unlock( fent );
      return err;
   }

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

   // which blocks are modified?
   modification_map modified_blocks;
   
   // which blocks do we garbage-collect?
   modification_map garbage_blocks;
   
   // block futures
   list<struct cache_block_future*> futures;
   
   // did we replicate successfully?
   bool replicated = false;
   
   // fent snapshot before we do anything
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );

   // are we going to lose any remote blocks?
   bool local = FS_ENTRY_LOCAL( core, fent );

   // if we're removing blocks, then we'll need to withdraw them.
   if( size < fent->size ) {

      if( trunc_block_id > 0 ) {
         // truncate the last block
         char* block = CALLOC_LIST( char, core->blocking_factor );

         ssize_t nr = fs_entry_read_block( core, fs_path, fent, trunc_block_id, block, core->blocking_factor );
         if( nr < 0 ) {
            errorf( "fs_entry_read_block(%s[%" PRIu64 "]) rc = %zd\n", fs_path, trunc_block_id, nr );
            err = nr;
         }
         else {
            // truncate this block
            memset( block + (size % core->blocking_factor), 0, core->blocking_factor - (size % core->blocking_factor) );

            uint64_t old_version = fent->manifest->get_block_version( trunc_block_id );
            
            unsigned char* hash = BLOCK_HASH_DATA( block, core->blocking_factor );
            
            struct cache_block_future* f = fs_entry_write_block_async( core, fent, trunc_block_id, block, core->blocking_factor, hash );
            if( f == NULL ) {
               errorf("fs_entry_put_block(%s[%" PRId64 "]) failed\n", fs_path, trunc_block_id );
               free( hash );
            }
            else {
               // record that we've written this block
               struct fs_entry_block_info binfo;
               
               // NOTE: pass in the block file descriptor, so don't close it!
               fs_entry_block_info_replicate_init( &binfo, fent->manifest->get_block_version( trunc_block_id ), hash, BLOCK_HASH_LEN(), core->gateway, f->block_fd );
               
               modified_blocks[ trunc_block_id ] = binfo;
               
               // garbage collect the old version
               struct fs_entry_block_info erase_binfo;
               
               fs_entry_block_info_garbage_init( &binfo, old_version, core->gateway );
               
               garbage_blocks[ trunc_block_id ] = erase_binfo;
               
            }
            
            futures.push_back( f );
         }

         free( block );
      }
      
      if( local ) {
         // unlink the blocks that would have been cut off
         for( uint64_t i = new_max_block; i < max_block; i++ ) {
            
            if( garbage_blocks.find( i ) != garbage_blocks.end() ) {
               struct fs_entry_block_info erase_binfo;
               memset( &erase_binfo, 0, sizeof(erase_binfo) );
               
               fs_entry_block_info_garbage_init( &erase_binfo, fent->manifest->get_block_version(i), fent->manifest->get_block_host( core, i ) );
               
               garbage_blocks[ i ] = erase_binfo;
            }
            
            int rc = fs_entry_cache_evict_block( core, core->cache, fent->file_id, fent->version, i, fent->manifest->get_block_version(i) );
            if( rc != 0 && rc != -ENOENT ) {
               errorf("fs_entry_cache_evict_block(%" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s)) rc = %d\n", fent->file_id, fent->version, i, fent->manifest->get_block_version(i), fs_path, rc );
            }
         }

         fent->manifest->truncate( new_max_block );
      }

      if( err == 0 )
         fent->size = size;
      
   }
   else if( size > fent->size ) {
      // truncate to expand the file
      
      int rc = fs_entry_expand_file( core, fs_path, fent, size, &modified_blocks, &futures );
      if( rc != 0 ) {
         errorf("fs_entry_expand_file(%s) rc = %d\n", fs_path, rc );
         err = rc;
      }
   }
   
   // wait for all cache writes to finish
   err = fs_entry_finish_writes( futures, false );
   
   if( err == 0 && !local ) {
      // inform the remote block owner that the data must be truncated
      // build up a truncate write message
      Serialization::WriteMsg *truncate_msg = new Serialization::WriteMsg();
      fs_entry_init_write_message( truncate_msg, core, Serialization::WriteMsg::TRUNCATE );
      fs_entry_prepare_truncate_message( truncate_msg, fs_path, fent, new_max_block );

      Serialization::WriteMsg *withdraw_ack = new Serialization::WriteMsg();
      
      err = fs_entry_post_write( withdraw_ack, core, fent->coordinator, truncate_msg );

      if( err != 0 ) {
         // TODO: ms_client_claim
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
      fs_entry_mark_read_stale( fent );
   }


   // replicate if the file is local
   if( err == 0 && local && modified_blocks.size() > 0 ) {
      
      uint64_t modified_block_start = modified_blocks.begin()->first;
      uint64_t modified_block_end = modified_blocks.rbegin()->first + 1;     // exclusive
      
      // make a file handle, but only for purposes of replication.
      // This lets us start all replicas concurrently, and then block until they're all done.
      struct fs_file_handle fh;
      fs_entry_replica_file_handle( core, fent, &fh );
      
      int rc = fs_entry_replicate_manifest( core, fent, false, &fh );
      if( rc != 0 ) {
         errorf("fs_entry_replicate_manifest(%s[%" PRId64 "-%" PRId64 "]) rc = %d\n", fs_path, modified_block_start, modified_block_end, rc );
         err = -EIO;
      }
      
      else {
         rc = fs_entry_replicate_blocks( core, fent, &modified_blocks, false, &fh );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_write(%s[%" PRId64 "-%" PRId64 "]) rc = %d\n", fs_path, modified_block_start, modified_block_end, rc );
            err = -EIO;
         }
      }
      
      // wait for replicas to complete
      if( err == 0 ) {
         rc = fs_entry_replicate_wait( core, &fh );
         if( rc != 0 ) {
            errorf("fs_entry_replica_wait(%s) rc = %d\n", fs_path, rc );
            err = -EIO;
         }
      }
      
      fs_entry_free_replica_file_handle( &fh );
      
      if( err == 0 )
         replicated = true;
   }

   // reversion this file atomically
   if( err == 0 && local ) {

      int64_t new_version = fs_entry_next_file_version();

      err = fs_entry_reversion_file( core, fs_path, fent, new_version, parent_id, parent_name );

      if( err != 0 && err != -ENOENT ) {
         errorf("fs_entry_reversion_file(%s.%" PRId64 " --> %" PRId64 ") rc = %d\n", fs_path, fent->version, new_version, err );
      }
      else {
         // if this was ENOENT (indicating that we couldn't rename the cache), this isn't a bug.  The cached data was evicted earlier (i.e. truncate to size 0).
         err = 0;
      }
   }
   
   // garbage collect old manifest and block on success
   if( err == 0 && local ) {
      int rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
      }
      
      rc = fs_entry_garbage_collect_blocks( core, &fent_snapshot, &garbage_blocks );
      if( rc != 0 ) {
         errorf("fs_entry_garbage_collect_blocks(%s) rc = %d\n", fs_path, rc );
      }
      rc = 0;
   }
   
   // recover if error
   if( err != 0 ) {
      errorf("Reverting %s due to error\n", fs_path );
      
      if( local && replicated ) {
         struct replica_snapshot new_fent_snapshot;
         fs_entry_replica_snapshot( core, fent, 0, 0, &new_fent_snapshot );
         
         int rc = fs_entry_garbage_collect_manifest( core, &new_fent_snapshot );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_manifest(%s) rc = %d\n", fs_path, rc );
         }
         
         rc = fs_entry_garbage_collect_blocks( core, &new_fent_snapshot, &modified_blocks );
         if( rc != 0 ) {
            errorf("fs_entry_garbage_collect_blocks(%s) rc = %d\n", fs_path, rc );
         }
         rc = 0;
      }
      
      // roll back metadata
      fent->size = fent_snapshot.size;
      fent->mtime_sec = fent_snapshot.mtime_sec;
      fent->mtime_nsec = fent_snapshot.mtime_nsec;
      fent->version = fent_snapshot.file_version;
   }

   if( modified_blocks.size() > 0 ) {
      // free memory
      for( modification_map::iterator itr = modified_blocks.begin(); itr != modified_blocks.end(); itr++ ) {
         if( itr->second.hash )
            free( itr->second.hash );
         
         // if we're exiting due to error, then clean up the blocks we wrote to the cache.
         if( err < 0 ) {
            fs_entry_cache_evict_block( core, core->cache, fent->file_id, fent->version, itr->first, itr->second.version );
         }
      }
   }
   
   dbprintf("file size is now %" PRId64 "\n", (int64_t)fent->size );

   return err;
}



// truncate, only if the version is correct (or ignore it if it's -1)
int fs_entry_versioned_truncate(struct fs_core* core, const char* fs_path, uint64_t file_id, uint64_t coordinator_id, off_t newsize,
                                int64_t known_version, uint64_t user, uint64_t volume, uint64_t gateway_id, bool check_file_id_and_coordinator_id ) {

   
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
