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

bool cache_entry_key_comp_func( const struct cache_entry_key& c1, const struct cache_entry_key& c2 ) {
   if( c1.file_id < c2.file_id ) {
      return true;
   }
   else if( c1.file_id > c2.file_id ) {
      return false;
   }
   else {
      if( c1.file_version < c2.file_version ) {
         return true;
      }
      else if( c1.file_version > c2.file_version) {
         return false;
      }
      else {
         if( c1.block_id < c2.block_id ) {
            return true;
         }
         else if( c1.block_id > c2.block_id ) {
            return false;
         }
         else {
            if( c1.block_version < c2.block_version ) {
               return true;
            }
            else {
               return false;
            }
         }
      }
   }
}


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


// lock primitives for the promotion buffer
int fs_entry_cache_promotes_rlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->promotes_lock );
}

int fs_entry_cache_promotes_wlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->promotes_lock );
}

int fs_entry_cache_promotes_unlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->promotes_lock );
}


// lock primitives on the ongoing writes buffer
int fs_entry_cache_ongoing_writes_rlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->ongoing_writes_lock );
}

int fs_entry_cache_ongoing_writes_wlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->ongoing_writes_lock );
}

int fs_entry_cache_ongoing_writes_unlock( struct syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->ongoing_writes_lock );
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
      
      cache_lru->push_back( lru_key );
      
      rc = 0;
   }
   
   free( block_path_basename );
   
   return rc;
}

// clean up a future
static int cache_block_future_clean( struct cache_block_future* f ) {
   if( f->block_fd >= 0 ) {
      fsync( f->block_fd );
      close( f->block_fd );
      f->block_fd = -1;
   }
   
   if( f->block_data ) {
      free( f->block_data );
      f->block_data = NULL;
   }
   
   if( f->aio.aio_sigevent.sigev_value.sival_ptr ) {
      free( f->aio.aio_sigevent.sigev_value.sival_ptr );
      f->aio.aio_sigevent.sigev_value.sival_ptr = NULL;
   }
   
   memset( &f->aio, 0, sizeof(f->aio) );
   
   sem_destroy( &f->sem_ongoing );
   
   return 0;
}

// free a future
int fs_entry_cache_block_future_free( struct cache_block_future* f ) {
   cache_block_future_clean( f );
   free( f );
   return 0;
}

// set up a file's cache directory.
static int fs_entry_cache_file_setup( struct fs_core* core, uint64_t file_id, int64_t version, mode_t mode ) {
   // it is possible for there to be a 0-sized non-directory here, to indicate the next version to be created.
   // if so, remove it

   char* local_file_url = fs_entry_local_file_url( core, file_id, version );
   char* local_path = GET_PATH( local_file_url );

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
   
   fd = open( block_path, flags, 0600 );
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
static int fs_entry_cache_evict_block_internal( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* block_url = fs_entry_local_block_url( core, file_id, file_version, block_id, block_version );
   char* block_path = GET_PATH( block_url );
   int rc = unlink( block_path );
   if( rc != 0 ) {
      rc = -errno;
   }
   if( rc == 0 || rc == -ENOENT ) {
      // let another block get queued
      sem_post( &cache->sem_write_hard_limit );
      
      char* local_file_url = fs_entry_local_file_url( core, file_id, file_version );
      char* local_file_path = GET_PATH( local_file_url );
      
      // remove the file's empty directories
      md_rmdirs( local_file_path );
      
      free( local_file_url );
   }
   
   free( block_url );
   
   return rc;
}

// delete a block in the cache, and decrement the number of blocks.
// for use with external clients of this module only.
int fs_entry_cache_evict_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   int rc = fs_entry_cache_evict_block_internal( core, cache, file_id, file_version, block_id, block_version );
   if( rc == 0 ) {
      __sync_fetch_and_sub( &cache->num_blocks_written, 1 );
   }
   
   return rc;
}


// apply a function over a file's cached blocks
int fs_entry_cache_file_blocks_apply( char const* local_path, int (*block_func)( char const*, void* ), void* cls ) {
   
   DIR* dir = opendir( local_path );
   if( dir == NULL ) {
      int rc = -errno;
      errorf( "opendir(%s) errno = %d\n", local_path, rc );
      return rc;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(local_path, _PC_NAME_MAX) + 1;

   struct dirent* dent = (struct dirent*)malloc( dirent_sz );
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
         
         rc = (*block_func)( block_path, cls );
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
      static int cache_evict_block( char const* block_path, void* cls ) {
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
   
   int rc = fs_entry_cache_file_blocks_apply( local_file_path, local::cache_evict_block, cache );
   
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
      // promote these blocks in the cache
      fs_entry_cache_promotes_wlock( cache );
      
      for( cache_lru_t::iterator itr = lru.begin(); itr != lru.end(); itr++ ) 
         cache->promotes->push_back( *itr );
      
      fs_entry_cache_promotes_unlock( cache );
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
      &cache->promotes_lock,
      &cache->ongoing_writes_lock,
      NULL
   };
   
   for( int i = 0; locks[i] != NULL; i++ ) {
      pthread_rwlock_init( locks[i], NULL );
   }
   
   dbprintf("Soft limit: %zu blocks.  Hard limit: %zu blocks\n", soft_limit, hard_limit );
   
   cache->hard_max_size = hard_limit;
   cache->soft_max_size = soft_limit;
   
   sem_init( &cache->sem_write_hard_limit, 0, hard_limit );
   sem_init( &cache->sem_blocks_writing, 0, 0 );
   
   cache->pending_1 = new block_buffer_t();
   cache->pending_2 = new block_buffer_t();
   cache->pending = cache->pending_1;
   
   cache->completed_1 = new completion_buffer_t();
   cache->completed_2 = new completion_buffer_t();
   cache->completed = cache->completed_1;
   
   cache->cache_lru = new cache_lru_t();
   
   cache->promotes_1 = new cache_lru_t();
   cache->promotes_2 = new cache_lru_t();
   cache->promotes = cache->promotes_1;
   
   cache->ongoing_writes = new ongoing_writes_t();
   
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
   sem_post( &cache->sem_blocks_writing );
   
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
         if( *itr ) {
            free( *itr );
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
         struct cache_block_future* f = *itr;
         fs_entry_cache_block_future_free( f );
      }
      
      delete completeds[i];
   }
   
   cache_lru_t* lrus[] = {
      cache->cache_lru,
      cache->promotes_1,
      cache->promotes_2,
      NULL
   };
   
   for( int i = 0; lrus[i] != NULL; i++ ) {
      delete lrus[i];
   }
   
   cache->pending_1 = NULL;
   cache->pending_2 = NULL;
   
   cache->completed_1 = NULL;
   cache->completed_2 = NULL;
   
   cache->cache_lru = NULL;
   cache->promotes_1 = NULL;
   cache->promotes_2 = NULL;
   
   delete cache->ongoing_writes;
   cache->ongoing_writes = NULL;
   
   pthread_rwlock_t* locks[] = {
      &cache->pending_lock,
      &cache->completed_lock,
      &cache->cache_lru_lock,
      &cache->promotes_lock,
      &cache->ongoing_writes_lock,
      NULL
   };
   
   for( int i = 0; locks[i] != NULL; i++ ) {
      pthread_rwlock_destroy( locks[i] );
   }
   
   sem_destroy( &cache->sem_blocks_writing );
   sem_destroy( &cache->sem_write_hard_limit );
   
   return 0;
}


// create an ongoing write
// NOTE: the future will need to hold onto data, so the caller shouldn't free it!
int cache_block_future_init( struct fs_core* core, struct syndicate_cache* cache, struct cache_block_future* f,
                             uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int block_fd,
                             char* data, size_t data_len,
                             bool detached ) {
   
   memset( f, 0, sizeof( struct cache_block_future ) );
   
   f->key.file_id = file_id;
   f->key.file_version = file_version;
   f->key.block_id = block_id;
   f->key.block_version = block_version;
   
   f->block_fd = block_fd;
   f->block_data = data;
   f->data_len = data_len;
   f->detached = detached;
   
   // fill in aio structure
   f->aio.aio_fildes = block_fd;
   f->aio.aio_buf = data;
   f->aio.aio_nbytes = data_len;
   f->aio.aio_offset = 0;
   
   // set up callback
   f->aio.aio_sigevent.sigev_notify = SIGEV_THREAD;
   f->aio.aio_sigevent.sigev_notify_function = cache_aio_write_completion;
   f->aio.aio_sigevent.sigev_notify_attributes = NULL;
   
   // set up completion args
   struct syndicate_cache_aio_write_args* wargs = CALLOC_LIST( struct syndicate_cache_aio_write_args, 1 );
   
   wargs->core = core;
   wargs->cache = cache;
   wargs->future = f;
   
   f->aio.aio_sigevent.sigev_value.sival_ptr = (void*)wargs;
   
   sem_init( &f->sem_ongoing, 0, 0 );
   
   return 0;
}

// asynchronously write a block 
static int cache_aio_write( struct fs_core* core, struct syndicate_cache* cache, struct cache_block_future* f ) {
   
   // allow external clients to keep track of pending writes for this file
   fs_entry_cache_ongoing_writes_wlock( cache );
   
   int rc = aio_write( &f->aio );
   
   if( rc == 0 ) {
      // put one new block
      cache->ongoing_writes->insert( f );   
   }
   
   fs_entry_cache_ongoing_writes_unlock( cache );
   
   return rc;
}


// handle a completed write operation
void cache_aio_write_completion( sigval_t sigval ) {
   
   struct syndicate_cache_aio_write_args* wargs = (struct syndicate_cache_aio_write_args*)sigval.sival_ptr;
   
   struct syndicate_cache* cache = wargs->cache;
   struct cache_block_future* future = wargs->future;
   
   // successful completion?
   int write_rc = 0;
   int aio_rc = aio_error( &future->aio );
   if( aio_rc == 0 ) {
      // yup!
      write_rc = aio_return( &future->aio );
      
      if( write_rc == -1 ) {
         write_rc = -errno;
      }
      else {
         // sync and rewind file handle
         fdatasync( future->block_fd );
         lseek( future->block_fd, 0, SEEK_SET );
         
         //char prefix[21];
         //memset(prefix, 0, 21);
         //memcpy( prefix, future->block_data, MIN( core->blocking_factor, future->data_len ) );
         
         //dbprintf("wrote %d (of %zu) bytes to %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], prefix = '%s'\n",
         //         write_rc, future->data_len, future->key.file_id, future->key.file_version, future->key.block_id, future->key.block_version, prefix );
      }
   }
   else {
      write_rc = -aio_rc;
   }
   
   future->aio_rc = aio_rc;
   future->write_rc = write_rc;
   
   // enqueue for reaping
   fs_entry_cache_completed_wlock( cache );
   
   cache->completed->push_back( future );
   
   fs_entry_cache_completed_unlock( cache );
}


// start pending writes
// NOTE: we assume that only one thread calls this, for a given cache
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
   
   // safe to use pending as long as no one else performs the above swap
   
   // start pending writes
   for( block_buffer_t::iterator itr = pending->begin(); itr != pending->end(); itr++ ) {
      struct cache_block_future* f = *itr;
      struct cache_entry_key* c = &f->key;
      
      int rc = cache_aio_write( core, cache, f );
      if( rc < 0 ) {
         errorf("cache_aio_write( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ), rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, rc );
      }
   }
   
   pending->clear();
}


// reap completed writes
// NOTE: we assume that only one thread calls this at a time, for a given cache
void fs_entry_cache_complete_writes( struct fs_core* core, struct syndicate_cache* cache, cache_lru_t* write_lru ) {
   
   completion_buffer_t* completed = NULL;
   
   // get the current completed buffer, and switch to the other
   fs_entry_cache_completed_wlock( cache );
   
   completed = cache->completed;
   if( cache->completed == cache->completed_1 )
      cache->completed = cache->completed_2;
   else
      cache->completed = cache->completed_1;
   
   fs_entry_cache_completed_unlock( cache );
   
   // safe to use completed as long as no one else performs the above swap
   
   int write_count = 0;
   
   // reap completed writes
   for( completion_buffer_t::iterator itr = completed->begin(); itr != completed->end(); itr++ ) {
      struct cache_block_future* f = *itr;
      struct cache_entry_key* c = &f->key;
      
      // finished an aio write
      fs_entry_cache_ongoing_writes_wlock( cache );
      
      cache->ongoing_writes->erase( f );
      
      fs_entry_cache_ongoing_writes_unlock( cache );
      
      if( f->aio_rc != 0 ) {
         errorf("WARN: write aio %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, f->aio_rc );
         
         // clean up 
         fs_entry_cache_evict_block_internal( core, cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else if( f->write_rc < 0 ) {
         errorf("WARN: write %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, f->write_rc );
         
         // clean up 
         fs_entry_cache_evict_block_internal( core, cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else {
         // finished!
         if( write_lru ) {
            // log this as written
            write_lru->push_back( *c );
         }
         
         write_count ++;
      }
      
      bool detached = f->detached;
      
      // wake up anyone waiting on this
      sem_post( &f->sem_ongoing );
      
      // are we supposed to reap it?
      if( detached || !cache->running ) {
         fs_entry_cache_block_future_free( f );
      }
   }
   
   // successfully cached blocks
   __sync_fetch_and_add( &cache->num_blocks_written, write_count );
   
   if( write_count != 0 )
      dbprintf("Cache now has %d blocks\n", cache->num_blocks_written );
   
   completed->clear();
}


// evict blocks
// NOTE: we assume that only one thread calls this at a time, for a given cache
void fs_entry_cache_evict_blocks( struct fs_core* core, struct syndicate_cache* cache, cache_lru_t* new_writes ) {
   
   cache_lru_t* promotes = NULL;
   
   // swap promotes
   fs_entry_cache_promotes_wlock( cache );
   
   promotes = cache->promotes;
   if( cache->promotes == cache->promotes_1 )
      cache->promotes = cache->promotes_2;
   else
      cache->promotes = cache->promotes_1;
   
   fs_entry_cache_promotes_unlock( cache );
   
   // safe access to the promote buffer, as long as no one performs the above swap
   
   fs_entry_cache_lru_wlock( cache );
   
   // merge in the new writes
   if( new_writes )
      cache->cache_lru->splice( cache->cache_lru->end(), *new_writes );
   
   // process promotions
   // we can (probably) afford to be linear here, since we won't have millions of entries.
   // TODO: investigate boost biamp if not
   for( cache_lru_t::iterator pitr = promotes->begin(); pitr != promotes->end(); pitr++ ) {
      // search from the back first, since we might be hitting blocks that were recently read.
      cache_lru_t::iterator citr = cache->cache_lru->end();
      citr--;
      for( ; citr != cache->cache_lru->begin(); citr-- ) {
         
         if( cache_entry_key_comp::equal( *pitr, *citr ) ) {
            // promote this entry
            //struct cache_entry_key p = *citr;
            citr = cache->cache_lru->erase( citr );
         }
      }
   }
   
   cache->cache_lru->splice( cache->cache_lru->end(), *promotes );
   
   int num_blocks_written = cache->num_blocks_written;
   int blocks_removed = 0;
   
   // work to do?
   if( cache->cache_lru->size() > 0 && (unsigned)num_blocks_written > cache->soft_max_size ) {
      // start evicting
      do { 
         struct cache_entry_key c = cache->cache_lru->front();
         cache->cache_lru->pop_front();
         
         int rc = fs_entry_cache_evict_block_internal( core, cache, c.file_id, c.file_version, c.block_id, c.block_version );
         
         if( rc != 0 ) {
            // if it wasn't there, then it was already evicted.
            errorf("WARN: failed to evict %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], rc = %d\n", c.file_id, c.file_version, c.block_id, c.block_version, rc );
            
            if( rc == -ENOENT ) {
               // something removed it...
               blocks_removed ++;
            }
         }
         
         else {
            // successfully evicted a block
            dbprintf("Cache EVICT %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", c.file_id, c.file_version, c.block_id, c.block_version );
            blocks_removed ++;
         }
         
      } while( cache->cache_lru->size() > 0 && (unsigned)num_blocks_written - (unsigned)blocks_removed > cache->soft_max_size );
      
      // blocks evicted!
      __sync_fetch_and_sub( &cache->num_blocks_written, blocks_removed );
      
      dbprintf("Cache now has %d blocks\n", cache->num_blocks_written );
   }
   
   fs_entry_cache_lru_unlock( cache );
   
   // done with this
   promotes->clear();
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
      
      // wait for there to be blocks, if there are none
      if( cache->ongoing_writes->size() == 0 )
         sem_wait( &cache->sem_blocks_writing );
      
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
   // TODO: aio cancellations
   while( cache->ongoing_writes->size() > 0 ) {
      dbprintf("Waiting for %zu blocks to sync...\n", cache->ongoing_writes->size() );
      
      cache_lru_t new_writes;
      
      // reap completed writes
      fs_entry_cache_complete_writes( core, cache, &new_writes );
      
      // evict blocks 
      fs_entry_cache_evict_blocks( core, cache, &new_writes );
      
      sleep(1);
   }
   
   free( args );
   
   dbprintf("%s", "Cache writer thread exited\n" );
   
   return NULL;
}


// make a cache entry
static int cache_entry_key_init( struct cache_entry_key* c, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   c->file_id = file_id;
   c->file_version = file_version;
   c->block_id = block_id;
   c->block_version = block_version;
   return 0;
}

// add a block to the cache, to be written asynchronously.
// return a future that can be waited on.
// NOTE: the given data will be referenced!  Do NOT free it!
struct cache_block_future* fs_entry_cache_write_block_async( struct fs_core* core, struct syndicate_cache* cache,
                                                             uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version,
                                                             char* data, size_t data_len,
                                                             bool detached ) {
   
   if( !cache->running )
      return NULL;
   
   // reserve the right to cache this block
   sem_wait( &cache->sem_write_hard_limit );
   
   // create the block to cache
   int block_fd = fs_entry_cache_open_block( core, cache, file_id, file_version, block_id, block_version, O_CREAT | O_RDWR | O_TRUNC );
   if( block_fd < 0 ) {
      errorf("fs_entry_cache_open_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", file_id, file_version, block_id, block_version, block_fd );
      return NULL;
   }
   
   struct cache_block_future* f = CALLOC_LIST( struct cache_block_future, 1 );
   cache_block_future_init( core, cache, f, file_id, file_version, block_id, block_version, block_fd, data, data_len, detached );
   
   fs_entry_cache_pending_wlock( cache );
   
   cache->pending->push_back( f );
   
   // wake up the thread--we have another block
   sem_post( &cache->sem_blocks_writing );
   
   fs_entry_cache_pending_unlock( cache );
   
   return f;
}

// wait for a write to finish
int fs_entry_cache_block_future_wait( struct cache_block_future* f ) {
   return sem_wait( &f->sem_ongoing );
}

// extract the block file descriptor from a future.
// caller must close and clean up.
// NOTE: only call this after the future has finished!
int fs_entry_cache_block_future_release_fd( struct cache_block_future* f ) {
   int fd = f->block_fd;
   f->block_fd = -1;
   return fd;
}

// promote a cached block, so it doesn't get evicted
int fs_entry_cache_promote_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   int rc = 0;
   
   if( !cache->running )
      return -EAGAIN;
   
   struct cache_entry_key c;
   cache_entry_key_init( &c, file_id, file_version, block_id, block_version );
   
   fs_entry_cache_promotes_wlock( cache );
   
   cache->promotes->push_back( c );
   
   fs_entry_cache_promotes_unlock( cache );
   
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
   
   return nr;
}
