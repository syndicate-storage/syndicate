/*
   Copyright 2014 The Trustees of Princeton University

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

void* fs_entry_cache_main_loop( void* arg );
void cache_aio_write_completion( sigval_t sigval );

// lock primitives for the pending buffer
int fs_entry_cache_pending_rlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->pending_lock );
}

int fs_entry_cache_pending_wlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->pending_lock );
}

int fs_entry_cache_pending_unlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->pending_lock );
}

// lock primitives for the completed writes buffer
int fs_entry_cache_completed_rlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->completed_lock );
}

int fs_entry_cache_completed_wlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->completed_lock );
}

int fs_entry_cache_completed_unlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->completed_lock );
}

// lock primitives for the lru buffer
int fs_entry_cache_lru_rlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->cache_lru_lock );
}

int fs_entry_cache_lru_wlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->cache_lru_lock );
}

int fs_entry_cache_lru_unlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->cache_lru_lock );
}

// arguments to the cb below
struct cache_cb_add_lru_args {
   cache_lru_t* cache_lru;
   uint64_t file_id;
   int64_t file_version;
};

// callback to apply over a file's blocks.
// cls must be of type struct cache_cb_add_lru_args
static int cache_cb_add_lru( char const* block_path, void* cls ) {
   struct cache_cb_add_lru_args* args = (struct cache_cb_add_lru_args*)cls;
   
   cache_lru_t* cache_lru = args->cache_lru;
   
   uint64_t file_id = args->file_id;
   int64_t file_version = args->file_version;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   
   // scan path for block ID and block version
   char* block_path_basename = md_basename( block_path, NULL );
   
   int rc = sscanf( block_path_basename, "%" PRIu64 ".%" PRId64, &block_id, &block_version );
   if( rc != 2 ) {
      errorf("Unparsable block name '%s'\n", block_path_basename );
      rc = -EINVAL;
   }
   else {
      
      struct cache_entry_key lru_key;
      memset( &lru_key, 0, sizeof(lru_key) );
      
      lru_key.file_id = file_id;
      lru_key.file_version = file_version;
      lru_key.block_id = block_id;
      lru_key.block_version = block_version;
      lru_key.data_len = 0;
      
      cache_lru->push_back( lru_key );
      
      rc = 0;
   }
   
   free( block_path_basename );
   
   return rc;
}

// clean up a cache_write_ctx structure
static int cache_write_ctx_clean( struct cache_write_ctx* w ) {
   if( w->aio.aio_buf ) {
      void* tmp = (void*)w->aio.aio_buf;
      free( tmp );
      w->aio.aio_buf = NULL;
   }
   
   if( w->aio.aio_sigevent.sigev_value.sival_ptr ) {
      free( w->aio.aio_sigevent.sigev_value.sival_ptr );
      w->aio.aio_sigevent.sigev_value.sival_ptr = NULL;
   }
   
   if( w->aio.aio_fildes >= 0 ) {
      close( w->aio.aio_fildes );
      w->aio.aio_fildes = -1;
   }
   
   return 0;
}


// set up a file's cache directory.
static int fs_entry_cache_file_setup( struct fs_core* core, uint64_t file_id, int64_t version, mode_t mode ) {
   // it is possible for there to be a 0-sized non-directory here, to indicate the next version to be created.
   // if so, remove it

   char* local_file_url = fs_entry_local_file_url( core, file_id, version );
   char* local_path = GET_PATH( local_file_url );

   dbprintf("create %s. mode %o\n", local_path, mode);

   int rc = md_mkdirs3( local_path, mode | 0700 );
   if( rc < 0 )
      rc = -errno;
   
   free( local_file_url );
   
   return rc;
}



// open a block in the cache
int fs_entry_cache_open_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int flags ) {
   char* block_url = fs_entry_local_block_url( core, file_id, file_version, block_id, block_version );
   char* block_path = GET_PATH( block_url );
   int fd = 0;
   int rc = 0;
   
   // if we're creating the block, go ahead and create all the directories up to it.
   if( flags & O_CREAT ) {
      rc = fs_entry_cache_file_setup( core, file_id, file_version, 0700 );
      if( rc != 0 ) {
         errorf("fs_entry_cache_file_setup( %" PRIX64 ".%" PRId64 " ) rc = %d\n", file_id, file_version, rc );
         free( block_url );
         return rc;
      }
   }
   
   fd = open( block_path, flags );
   if( fd < 0 ) {
      fd = -errno;
   }
   
   free( block_url );
   return fd;
}


// stat a block in the cache (system use only)
int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct stat* sb ) {
   
   char* block_url = fs_entry_local_block_url( core, file_id, file_version, block_id, block_version );
   char* stat_path = GET_PATH( block_url );
   int rc = stat( stat_path, sb );
   
   if( rc != 0 )
      rc = -errno;

   free( block_url );
   
   return rc;
}

// stat a block in the cache (system use only)
int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, char const* fs_path, uint64_t block_id, int64_t block_version, struct stat* sb ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }
   
   int rc = fs_entry_cache_stat_block( core, cache, fent->file_id, fent->version, block_id, block_version, sb );
   
   fs_entry_unlock( fent );

   return rc;
}



// delete a block in the cache
int fs_entry_cache_evict_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* block_url = fs_entry_local_block_url( core, file_id, file_version, block_id, block_version );
   char* block_path = GET_PATH( block_url );
   int rc = unlink( block_path );
   if( rc != 0 ) {
      rc = -errno;
   }
   if( rc == 0 || rc == -ENOENT ) {
      // let another block get queued
      sem_post( &cache->sem_write_hard_limit );
   }
   
   free( block_url );
   
   return rc;
}



// apply a function over a file's cached blocks
static int fs_entry_cache_file_blocks_apply( char const* local_path, int (*block_func)( char const*, void* ), void* cls ) {
   
   DIR* dir = opendir( local_path );
   if( dir == NULL ) {
      int rc = -errno;
      errorf( "opendir(%s) errno = %d\n", local_path, rc );
      return rc;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(local_path, _PC_NAME_MAX) + 1;

   struct dirent* dent = (struct dirent*)malloc( dirent_sz );;
   struct dirent* result = NULL;
   char block_path[PATH_MAX+1];
   int rc = 0;
   int worst_rc = 0;

   do {
      readdir_r( dir, dent, &result );
      if( result != NULL ) {
         if( strcmp(result->d_name, ".") == 0 || strcmp(result->d_name, "..") == 0 )
            continue;

         md_fullpath( local_path, result->d_name, block_path );
         
         rc = (*block_func)( local_path, cls );
         if( rc != 0 ) {
            // could not unlink
            rc = -errno;
            errorf( "block_func(%s) errno = %d\n", block_path, rc );
            worst_rc = rc;
         }
      }
   } while( result != NULL );

   closedir( dir );
   free( dent );
   
   return worst_rc;
}


// evict a file from the cache
int fs_entry_cache_evict_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version ) {
   struct local {
      // lambda function for deleting a block and evicting it 
      static int evict_block( char const* block_path, void* cls ) {
         struct syndicate_cache* c = (struct syndicate_cache*)cls;
         
         int rc = unlink( block_path );
         if( rc != 0 ) {
            rc = -errno;
         }
         
         if( rc == 0 || rc == -ENOENT ) {
            // evicted!
            __sync_fetch_and_sub( &c->num_blocks_written, 1 );
            
            // let another block get queued
            sem_post( &c->sem_write_hard_limit );
         }
         else {
            // not evicted!
            errorf("WARN: unlink( %s ) rc = %d\n", block_path, rc );
            
            // nevertheless, try to evict as much as possible
            rc = 0;
         }
         
         return rc;
      }
   };
   
   // path to the file...
   char* local_file_url = fs_entry_local_file_url( core, file_id, file_version );
   char* local_file_path = GET_PATH( local_file_url );
   
   int rc = fs_entry_cache_file_blocks_apply( local_file_path, local::evict_block, cache );
   
   if( rc == 0 ) {
      // remove this file's directories
      md_rmdirs( local_file_path );
   }
   
   free( local_file_url );
   return rc;
}


// reversion a file.
// move it into place, and then insert the new cache_entry_key records for it to the cache_lru list.
// don't bother removing the old cache_entry_key records; they will be removed from the cache_lru list automatically.
// NOTE: the corresponding fent structure should be write-locked for this, to make it atomic.
int fs_entry_cache_reversion_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t old_file_version, int64_t new_file_version ) {
   char* cur_local_url = fs_entry_local_file_url( core, file_id, old_file_version );
   char* new_local_url = fs_entry_local_file_url( core, file_id, new_file_version );

   char* cur_local_path = GET_PATH( cur_local_url );
   char* new_local_path = GET_PATH( new_local_url );
   
   // new path shouldn't exist, but old path should
   struct stat old_sb;
   struct stat new_sb;
   int rc = 0;
   
   rc = stat( cur_local_path, &old_sb );
   if( rc != 0 ) {
      rc = -errno;
      if( rc != -ENOENT ) {
         // problem 
         errorf("Failed to stat %s, rc = %d\n", cur_local_path, rc );
         
         free( cur_local_url );
         free( new_local_url );
         
         return rc;
      }
   }
   
   rc = stat( new_local_path, &new_sb );
   if( rc == 0 ) {
      rc = -EEXIST;
   }
   else {
      rc = -errno;
   }
   if( rc != -ENOENT ) {
      errorf("Failed to stat %s, rc = %d\n", new_local_path, rc );
      
      free( cur_local_url );
      free( new_local_url );
      
      return rc;
   }

   // move the file data over
   rc = rename( cur_local_path, new_local_path );
   if( rc != 0 ) {
      rc = -errno;
      errorf("rename(%s,%s) rc = %d\n", cur_local_path, new_local_path, rc );
   
      free( cur_local_url );
      free( new_local_url );
      
      return rc;
   }
   
   // insert the new records
   cache_lru_t lru;
   
   struct cache_cb_add_lru_args lru_args;
   lru_args.cache_lru = &lru;
   lru_args.file_id = file_id;
   lru_args.file_version = new_file_version;
   
   rc = fs_entry_cache_file_blocks_apply( new_local_path, cache_cb_add_lru, &lru_args );
   
   if( rc == 0 ) {
      // splice the LRU list in
      fs_entry_cache_lru_wlock( cache );
      
      cache->cache_lru->splice( cache->cache_lru->end(), lru );
      
      fs_entry_cache_lru_unlock( cache );
   }
   
   free( cur_local_url );
   free( new_local_url );
   
   return rc;
}


// initialize the cache 
int fs_entry_cache_init( struct fs_core* core, struct syndicate_cache* cache, size_t soft_limit, size_t hard_limit ) {
   memset( cache, 0, sizeof(struct syndicate_cache) );
   
   pthread_rwlock_t* locks[] = {
      &cache->pending_lock,
      &cache->completed_lock,
      &cache->cache_lru_lock,
      NULL
   };
   
   for( int i = 0; locks[i] != NULL; i++ ) {
      pthread_rwlock_init( locks[i], NULL );
   }
   
   cache->hard_max_size = hard_limit;
   cache->soft_max_size = soft_limit;
   
   sem_init( &cache->sem_write_hard_limit, 0, hard_limit );
   sem_init( &cache->sem_blocks_pending, 0, 0 );
   
   cache->pending_1 = new block_buffer_t();
   cache->pending_2 = new block_buffer_t();
   cache->pending = cache->pending_1;
   
   cache->completed_1 = new completion_buffer_t();
   cache->completed_2 = new completion_buffer_t();
   cache->completed = cache->completed_1;
   
   cache->cache_lru = new cache_lru_t;
   
   // start the thread up 
   struct syndicate_cache_thread_args* args = CALLOC_LIST( struct syndicate_cache_thread_args, 1 );
   args->core = core;
   args->cache = cache;
   
   cache->running = true;
   
   cache->thread = md_start_thread( fs_entry_cache_main_loop, (void*)args, false );
   if( cache->thread == (pthread_t)(-1) ) {
      errorf("md_start_thread rc = %d\n", (int)cache->thread );
      return -1;
   }
   
   dbprintf("Cache %p started\n", cache );
   return 0;
}


// destroy the cache
int fs_entry_cache_destroy( struct syndicate_cache* cache ) {
   dbprintf("Cache %p shutting down\n", cache);
   
   cache->running = false;
   
   // wake up the writer
   sem_post( &cache->sem_blocks_pending );
   
   // wait for cache thread to finish 
   pthread_join( cache->thread, NULL );
   
   cache->pending = NULL;
   cache->completed = NULL;
   
   block_buffer_t* pendings[] = {
      cache->pending_1,
      cache->pending_2,
      NULL
   };
   
   for( int i = 0; pendings[i] != NULL; i++ ) {
      for( block_buffer_t::iterator itr = pendings[i]->begin(); itr != pendings[i]->end(); itr++ ) {
         if( itr->second ) {
            free( itr->second );
         }
      }
      
      delete pendings[i];
   }
   
   completion_buffer_t* completeds[] = {
      cache->completed_1,
      cache->completed_2,
      NULL
   };
   
   for( int i = 0; completeds[i] != NULL; i++ ) {
      for( completion_buffer_t::iterator itr = completeds[i]->begin(); itr != completeds[i]->end(); itr++ ) {
         struct cache_write_ctx wctx = *itr;
         cache_write_ctx_clean( &wctx );
      }
      
      delete completeds[i];
   }
   
   cache->pending_1 = NULL;
   cache->pending_2 = NULL;
   
   cache->completed_1 = NULL;
   cache->completed_2 = NULL;
   
   delete cache->cache_lru;
   cache->cache_lru = NULL;
   
   pthread_rwlock_destroy( &cache->pending_lock );
   pthread_rwlock_destroy( &cache->completed_lock );
   pthread_rwlock_destroy( &cache->cache_lru_lock );
   
   sem_destroy( &cache->sem_blocks_pending );
   sem_destroy( &cache->sem_write_hard_limit );
   
   return 0;
}


// asynchronously write a block 
static int cache_aio_write( struct fs_core* core, struct syndicate_cache* cache, const struct cache_entry_key* c, char* data, struct cache_write_ctx* w ) {
   
   int block_fd = fs_entry_cache_open_block( core, cache, c->file_id, c->file_version, c->block_id, c->block_version, O_CREAT | O_WRONLY | O_TRUNC );
   if( block_fd < 0 ) {
      errorf("fs_entry_cache_open_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, block_fd );
      return block_fd;
   }
   
   memset( w, 0, sizeof(struct cache_write_ctx) );
   
   // fill in aio structure
   w->aio.aio_fildes = block_fd;
   w->aio.aio_buf = data;
   w->aio.aio_nbytes = c->data_len;
   w->aio.aio_offset = 0;
   
   // set up callback
   w->aio.aio_sigevent.sigev_notify = SIGEV_THREAD;
   w->aio.aio_sigevent.sigev_notify_function = cache_aio_write_completion;
   w->aio.aio_sigevent.sigev_notify_attributes = NULL;
   
   // set up completion args
   struct syndicate_cache_aio_write_args* wargs = CALLOC_LIST( struct syndicate_cache_aio_write_args, 1 );
   
   wargs->core = core;
   wargs->cache = cache;
   wargs->write = w;
   
   w->aio.aio_sigevent.sigev_value.sival_ptr = (void*)wargs;
   
   int rc = aio_write( &w->aio );
   return rc;
}


// handle a completed write operation
void cache_aio_write_completion( sigval_t sigval ) {
   
   struct syndicate_cache_aio_write_args* wargs = (struct syndicate_cache_aio_write_args*)sigval.sival_ptr;
   
   struct syndicate_cache* cache = wargs->cache;
   struct cache_write_ctx* write = wargs->write;
   
   // successful completion?
   int write_rc = 0;
   int aio_rc = aio_error( &write->aio );
   if( aio_rc == 0 ) {
      // yup!
      write_rc = aio_return( &write->aio );
   }
   
   write->aio_rc = aio_rc;
   write->write_rc = write_rc;
   
   // enqueue for reaping
   fs_entry_cache_completed_wlock( cache );
   
   cache->completed->push_back( *write );
   
   fs_entry_cache_completed_unlock( cache );
}


// start pending writes
void fs_entry_cache_begin_writes( struct fs_core* core, struct syndicate_cache* cache ) {
   
   // get the pending set, and switch the cache over to the other one
   block_buffer_t* pending = NULL;
   
   fs_entry_cache_pending_wlock( cache );
   
   pending = cache->pending;
   if( cache->pending == cache->pending_1 )
      cache->pending = cache->pending_2;
   else
      cache->pending = cache->pending_1;
   
   fs_entry_cache_pending_unlock( cache );
   
   // now we have exclusive access to this pending writes buffer
   
   // start pending writes
   for( block_buffer_t::iterator itr = pending->begin(); itr != pending->end(); itr++ ) {
      const struct cache_entry_key* c = &itr->first;
      char* block = itr->second;
      
      struct cache_write_ctx w;
      memset( &w, 0, sizeof(w) );
      
      int rc = cache_aio_write( core, cache, c, block, &w );
      if( rc < 0 ) {
         errorf("cache_aio_write( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ), rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, rc );
         
         free( block );
         itr->second = NULL;
      }
      else {
         // count this write 
         __sync_fetch_and_add( &cache->num_aio_writes, 1 );
      }
   }
   
   pending->clear();
}


// reap completed writes
void fs_entry_cache_complete_writes( struct fs_core* core, struct syndicate_cache* cache, cache_lru_t* write_lru ) {
   
   completion_buffer_t* completed = NULL;
   
   fs_entry_cache_completed_wlock( cache );
   
   completed = cache->completed;
   if( cache->completed == cache->completed_1 )
      cache->completed = cache->completed_2;
   else
      cache->completed = cache->completed_1;
   
   fs_entry_cache_completed_unlock( cache );
   
   // now we have exclusive access to this completed writes buffer
   
   int reap_count = 0;
   int write_count = 0;
   
   // reap completed writes
   for( completion_buffer_t::iterator itr = completed->begin(); itr != completed->end(); itr++ ) {
      struct cache_write_ctx write = *itr;
      struct cache_entry_key* c = &write.key;
      
      if( write.aio_rc != 0 ) {
         errorf("WARN: write aio %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, write.aio_rc );
         
         // clean up 
         fs_entry_cache_evict_block( core, cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else if( write.write_rc != 0 ) {
         errorf("WARN: write %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, write.write_rc );
         
         // clean up 
         fs_entry_cache_evict_block( core, cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else {
         // finished!
         if( write_lru ) {
            // log this as written
            write_lru->push_back( *c );
         } 
         
         write_count += 1;
      }
      
      reap_count += 1;
      
      // clean up
      cache_write_ctx_clean( &write );
   }
   
   // finished aio writes
   __sync_fetch_and_sub( &cache->num_aio_writes, reap_count );
   
   // successfully cached blocks
   __sync_fetch_and_add( &cache->num_blocks_written, write_count );
   
   completed->clear();
}


// evict blocks
void fs_entry_cache_evict_blocks( struct fs_core* core, struct syndicate_cache* cache, cache_lru_t* new_writes ) {
   
   fs_entry_cache_lru_wlock( cache );
   
   // merge in the new writes
   if( new_writes )
      cache->cache_lru->splice( cache->cache_lru->end(), *new_writes );
   
   int num_blocks_written = __sync_fetch_and_add( &cache->num_blocks_written, 0 );
   int blocks_removed = 0;
   
   // work to do?
   if( cache->cache_lru->size() > 0 && (unsigned)num_blocks_written > cache->soft_max_size ) {
      // start evicting
      do { 
         struct cache_entry_key c = cache->cache_lru->front();
         cache->cache_lru->pop_front();
         
         int rc = fs_entry_cache_evict_block( core, cache, c.file_id, c.file_version, c.block_id, c.block_version );
         
         if( rc != 0 ) {
            // if it wasn't there, then it was already evicted.
            if( rc != -ENOENT ) {
               // otherwise, we have a problem.
               errorf("WARN: failed to evict %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], rc = %d\n", c.file_id, c.file_version, c.block_id, c.block_version, rc );
            }
         }
         
         else {
            // successfully evicted a block
            blocks_removed ++;
         }
         
      } while( cache->cache_lru->size() > 0 && (unsigned)num_blocks_written - (unsigned)blocks_removed > cache->soft_max_size );
      
      // blocks evicted!
      __sync_fetch_and_sub( &cache->num_blocks_written, blocks_removed );
   }
   
   fs_entry_cache_lru_unlock( cache );
}


// cache main loop.
// * start new writes
// * reap completed writes
// * evict blocks after the soft size limit has been exceeded
void* fs_entry_cache_main_loop( void* arg ) {
   struct syndicate_cache_thread_args* args = (struct syndicate_cache_thread_args*)arg;
   
   struct syndicate_cache* cache = args->cache;
   struct fs_core* core = args->core;
   
   dbprintf("%s", "Cache writer thread strated\n" );
   
   while( cache->running ) {
      
      // wait for there to be blocks
      sem_wait( &cache->sem_blocks_pending );
      
      // waken up to die?
      if( !cache->running )
         break;
      
      // begin all pending writes
      fs_entry_cache_begin_writes( core, cache );
      
      cache_lru_t new_writes;
      
      // reap completed writes
      fs_entry_cache_complete_writes( core, cache, &new_writes );
      
      // evict blocks 
      fs_entry_cache_evict_blocks( core, cache, &new_writes );
   }
   
   // wait for remaining writes to finish 
   while( cache->num_aio_writes > 0 ) {
      dbprintf("Waiting for %d writes...\n", cache->num_aio_writes );
      
      // reap completed writes
      fs_entry_cache_complete_writes( core, cache, NULL );
      
      sleep(1);
   }
   
   free( args );
   
   dbprintf("%s", "Cache writer thread exited\n" );
   
   return NULL;
}


// make a cache entry
static int cache_entry_key_init( struct cache_entry_key* c, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, size_t data_len ) {
   c->file_id = file_id;
   c->file_version = file_version;
   c->block_id = block_id;
   c->block_version = block_version;
   c->data_len = data_len;
   return 0;
}

// add a block to the cache, to be written asynchronously
int fs_entry_cache_write_block_async( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, char* data, size_t data_len ) {
   int rc = 0;
   
   if( !cache->running )
      return -EAGAIN;
   
   // reserve the right to cache this block
   sem_wait( &cache->sem_write_hard_limit );
   
   fs_entry_cache_pending_wlock( cache );
   
   struct cache_entry_key c;
   cache_entry_key_init( &c, file_id, file_version, block_id, block_version, data_len );
   
   (*cache->pending)[ c ] = data;
   
   // wake up the thread--we have another block
   sem_post( &cache->sem_blocks_pending );
   
   fs_entry_cache_pending_unlock( cache );
   
   return rc;
}

// read a block from the cache, in its entirety
ssize_t fs_entry_cache_read_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int block_fd, char* buf, size_t len ) {
   ssize_t nr = 0;
   while( nr < (signed)len ) {
      ssize_t tmp = read( block_fd, buf + nr, len - nr );
      if( tmp < 0 ) {
         ssize_t rc = -errno;
         return rc;
      }

      if( tmp == 0 ) {
         break;
      }

      nr += tmp;
   }
   
   // TODO: promote in the cache
   return nr;
}

