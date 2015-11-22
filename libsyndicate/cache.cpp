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

#include "libsyndicate/cache.h"
#include "libsyndicate/url.h"
#include "libsyndicate/storage.h"

struct md_cache_block_future {
   
   // ID of this chunk
   struct md_cache_entry_key key;
   
   // chunk of data to write
   char* block_data;
   size_t data_len;
   
   // fd to receive writes
   int block_fd;
   
   // asynchronous disk I/O structures
   struct aiocb aio;
   int aio_rc;
   int write_rc;
   
   sem_t sem_ongoing;
   uint64_t flags;      // cache future flags (detached, unshared, etc.)
   
   bool finalized;
};

// compare two cache records
// they're ordered by file id, then version, then block id, then block version
bool md_cache_entry_key_comp_func( const struct md_cache_entry_key& c1, const struct md_cache_entry_key& c2 ) {
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


void* md_cache_main_loop( void* arg );
void md_cache_aio_write_completion( sigval_t sigval );

// lock primitives for the pending buffer
int md_cache_pending_rlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->pending_lock );
}

int md_cache_pending_wlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->pending_lock );
}

int md_cache_pending_unlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->pending_lock );
}

// lock primitives for the completed writes buffer
int md_cache_completed_rlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->completed_lock );
}

int md_cache_completed_wlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->completed_lock );
}

int md_cache_completed_unlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->completed_lock );
}

// lock primitives for the lru buffer
int md_cache_lru_rlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->cache_lru_lock );
}

int md_cache_lru_wlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->cache_lru_lock );
}

int md_cache_lru_unlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->cache_lru_lock );
}


// lock primitives for the promotion buffer
int md_cache_promotes_rlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->promotes_lock );
}

int md_cache_promotes_wlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->promotes_lock );
}

int md_cache_promotes_unlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->promotes_lock );
}


// lock primitives on the ongoing writes buffer
int md_cache_ongoing_writes_rlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_rdlock( &cache->ongoing_writes_lock );
}

int md_cache_ongoing_writes_wlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_wrlock( &cache->ongoing_writes_lock );
}

int md_cache_ongoing_writes_unlock( struct md_syndicate_cache* cache ) {
   return pthread_rwlock_unlock( &cache->ongoing_writes_lock );
}


// make a cache entry
static int md_cache_entry_key_init( struct md_cache_entry_key* c, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   c->file_id = file_id;
   c->file_version = file_version;
   c->block_id = block_id;
   c->block_version = block_version;
   return 0;
}

// arguments to the cb below
struct md_cache_cb_add_lru_args {
   md_cache_lru_t* cache_lru;
   uint64_t file_id;
   int64_t file_version;
};

// callback to apply over a file's blocks.
// cls must be of type struct md_cache_cb_add_lru_args
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if we couldn't parse the block path 
static int md_cache_cb_add_lru( char const* block_path, void* cls ) {
   struct md_cache_cb_add_lru_args* args = (struct md_cache_cb_add_lru_args*)cls;
   
   md_cache_lru_t* cache_lru = args->cache_lru;
   
   uint64_t file_id = args->file_id;
   int64_t file_version = args->file_version;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   
   // scan path for block ID and block version
   char* block_path_basename = md_basename( block_path, NULL );
   if( block_path_basename == NULL ) {
      return -ENOMEM;
   }
   
   int rc = sscanf( block_path_basename, "%" PRIu64 ".%" PRId64, &block_id, &block_version );
   if( rc != 2 ) {
      SG_error("Unparsable block name '%s'\n", block_path_basename );
      rc = -EINVAL;
   }
   else {
      
      struct md_cache_entry_key lru_key;
      memset( &lru_key, 0, sizeof(lru_key) );
      
      lru_key.file_id = file_id;
      lru_key.file_version = file_version;
      lru_key.block_id = block_id;
      lru_key.block_version = block_version;
      
      rc = 0;
      
      try {
         cache_lru->push_back( lru_key );
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
      }
   }
   
   SG_safe_free( block_path_basename );
   
   return rc;
}

// clean up a future
// always succeeds
int md_cache_block_future_clean( struct md_cache_block_future* f ) {
   if( f->block_fd >= 0 ) {
      fsync( f->block_fd );
      close( f->block_fd );
      f->block_fd = -1;
   }
   
   if( (f->flags & SG_CACHE_FLAG_UNSHARED) != 0 ) {
      
      // we own this data
      SG_safe_free( f->block_data );
   }
   
   SG_safe_free( f->aio.aio_sigevent.sigev_value.sival_ptr );
   
   memset( &f->aio, 0, sizeof(f->aio) );
   
   sem_destroy( &f->sem_ongoing );
   
   return 0;
}

// free a future
// always succeeds
int md_cache_block_future_free( struct md_cache_block_future* f ) {
   if( f != NULL ) {
      md_cache_block_future_clean( f );
      SG_safe_free( f );
   }
   return 0;
}

// apply a function over a list of futures 
// always succeeds
static int md_cache_block_future_apply_all( vector<struct md_cache_block_future*>* futs, void (*func)( struct md_cache_block_future*, void* ), void* func_cls ) {
   
   for( vector<struct md_cache_block_future*>::iterator itr = futs->begin(); itr != futs->end(); itr++ ) {
      
      struct md_cache_block_future* f = *itr;
      
      (*func)( f, func_cls );
   }
   
   return 0;
}

// free cache futures with md_cache_block_future_free
// if close_fds is true, then this will close the cache block file descriptors 
// otherwise, it will leave them open, so the caller has to deal with them.
// this is useful for when the caller needs to do things with the cached data, even if the data gets evicted
// always succeeds
int md_cache_block_future_free_all( vector<struct md_cache_block_future*>* futs, bool close_fds ) {
   
   struct local {
      
      // free a cache block future, optionally releasing its file descriptor
      static void release_and_free( struct md_cache_block_future* fut, void* cls ) {
         
         bool* close_fds_ptr = (bool*)cls;
         
         if( !(*close_fds_ptr) ) {
            // release the file FD from the future, so we can use it later 
            md_cache_block_future_release_fd( fut );
         }
         md_cache_block_future_free( fut );
      }
   };
   
   md_cache_block_future_apply_all( futs, local::release_and_free, &close_fds );
   
   return 0;
}


// flush a cache write 
// return 0 on success
// return -EIO if the block failed to write 
// return negative if there was a problem with the semaphore (see md_download_sem_wait)
int md_cache_flush_write( struct md_cache_block_future* f ) {

   // wait for this block to finish 
   int rc = md_cache_block_future_wait( f );
   
   if( rc != 0 ) {
      
      SG_error("md_cache_block_future_wait rc = %d\n", rc );
      return rc;
   }
   
   // was there an IO error?
   if( md_cache_block_future_has_error( f ) ) {
      
      int aio_rc = md_cache_block_future_get_aio_error( f );
      int write_rc = md_cache_block_future_get_write_error( f );
      
      SG_error("Failed to flush %d, aio_rc = %d, write_rc = %d\n", f->block_fd, aio_rc, write_rc );
      
      return -EIO;
   }
   
   return 0;
}


// flush cache writes
// try to flush all writes, even if some fail 
// return 0 on success 
// return negative on error--the last error encountered
int md_cache_flush_writes( vector<struct md_cache_block_future*>* futs ) {
   
   struct local {
      
      // flush a block 
      static void flush_block( struct md_cache_block_future* fut, void* cls ) {
         int* worst_rc = (int*)cls;
         
         int rc = md_cache_flush_write( fut );
         
         if( rc != 0 ) {
            SG_error("md_cache_flush_write rc = %d\n", rc);
            
            *worst_rc = rc;
         }
      }
   };
   
   int worst_rc = 0;
   
   md_cache_block_future_apply_all( futs, local::flush_block, &worst_rc );
   
   return worst_rc;
}


// set up a file's cache directory.
// return 0 on success
// return -ENOMEM on OOM 
// return negative if we failed to create the directory to hold the data
static int md_cache_file_setup( struct md_syndicate_cache* cache, uint64_t file_id, int64_t version, mode_t mode ) {
   // it is possible for there to be a 0-sized non-directory here, to indicate the next version to be created.
   // if so, remove it

   int rc = 0;
   char* local_path = NULL;
   char* local_file_url = md_url_local_file_url( cache->conf->data_root, cache->conf->volume, file_id, version );
   
   if( local_file_url == NULL ) {
      return -ENOMEM;
   }
   
   local_path = SG_URL_LOCAL_PATH( local_file_url );

   rc = md_mkdirs3( local_path, mode | 0700 );
   
   SG_safe_free( local_file_url );
   
   return rc;
}


// is a block in a cache readable?  As in, has it been completely written to disk?
// return 0 on success
// return -EAGAIN if the block is still being written
int md_cache_is_block_readable( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   int rc = 0;
   
   struct md_cache_entry_key k;
   md_cache_entry_key_init( &k, file_id, file_version, block_id, block_version );
   
   // read through ongoing writes...
   md_cache_ongoing_writes_rlock( cache );
   
   for( md_cache_ongoing_writes_t::iterator itr = cache->ongoing_writes->begin(); itr != cache->ongoing_writes->end(); itr++ ) {
      
      struct md_cache_block_future* f = *itr;
      
      // is this block being written?
      if( md_cache_entry_key_comp::equal( f->key, k ) ) {
         rc = -EAGAIN;
         break;
      }
   }
   
   md_cache_ongoing_writes_unlock( cache );
   return rc;
}

// open a block in the cache
// return a file descriptor >= 0 on success
// return -ENOMEM if OOM
// return negative on error
int md_cache_open_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int flags ) {
   
   int rc = 0;
   int fd = 0;
   char* block_path = NULL;
   char* block_url = md_url_local_block_url( cache->conf->data_root, cache->conf->volume, file_id, file_version, block_id, block_version );
   if( block_url == NULL ) {
      
      return -ENOMEM;
   }
   
   block_path = SG_URL_LOCAL_PATH( block_url );
   
   // if we're creating the block, go ahead and create all the directories up to it.
   if( flags & O_CREAT ) {
      
      rc = md_cache_file_setup( cache, file_id, file_version, 0700 );
      if( rc != 0 ) {
         
         SG_error("md_cache_file_setup( %" PRIX64 ".%" PRId64 " ) rc = %d\n", file_id, file_version, rc );
         SG_safe_free( block_url );
         return rc;
      }
   }
   
   fd = open( block_path, flags, 0600 );
   if( fd < 0 ) {
      fd = -errno;
      SG_error("open(%s) rc = %d\n", block_path, fd );
   }
   
   SG_safe_free( block_url );
   return fd;
}


// stat a block in the cache (system use only)
// return 0 on success 
// return -ENOMEM if OOM
// return negative (from stat(2) errno) on error
int md_cache_stat_block_by_id( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct stat* sb ) {
   
   char* stat_path = NULL;
   int rc = 0;
   char* block_url = NULL;
   
   block_url = md_url_local_block_url( cache->conf->data_root, cache->conf->volume, file_id, file_version, block_id, block_version );
   if( block_url == NULL ) {
      return -ENOMEM;
   }
   
   stat_path = SG_URL_LOCAL_PATH( block_url );
   rc = stat( stat_path, sb );
   
   if( rc != 0 ) {
      rc = -errno;
   }
   
   SG_safe_free( block_url );
   
   return rc;
}

// delete a block in the cache
// return 0 on success
// return -ENOMEM on OOM 
// return negative (from unlink) on error
static int md_cache_evict_block_internal( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   
   char* block_path = NULL;
   int rc = 0;
   char* block_url = NULL;
   char* local_file_url = NULL;
   char* local_file_path = NULL;
   
   block_url = md_url_local_block_url( cache->conf->data_root, cache->conf->volume, file_id, file_version, block_id, block_version );
   if( block_url == NULL ) {
      return -ENOMEM;
   }
   
   block_path = SG_URL_LOCAL_PATH( block_url );
   rc = unlink( block_path );
   
   if( rc != 0 ) {
      rc = -errno;
   }
   
   if( rc == 0 || rc == -ENOENT ) {
      
      // let another block get queued
      sem_post( &cache->sem_write_hard_limit );
      
      local_file_url = md_url_local_file_url( cache->conf->data_root, cache->conf->volume, file_id, file_version );
      if( local_file_url == NULL ) {
         
         SG_safe_free( block_url );
         return -ENOMEM;
      }
      
      local_file_path = SG_URL_LOCAL_PATH( local_file_url );
      
      // remove the file's empty directories
      md_rmdirs( local_file_path );
      
      SG_safe_free( local_file_url );
   }
   
   SG_safe_free( block_url );
   
   return rc;
}

// delete a block in the cache, and decrement the number of blocks.
// for use with external clients of this module only.
// return 0 on success
// return negative on error (see md_cache_evict_block_internal)
int md_cache_evict_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   
   int rc = md_cache_evict_block_internal( cache, file_id, file_version, block_id, block_version );
   if( rc == 0 ) {
      __sync_fetch_and_sub( &cache->num_blocks_written, 1 );
   }
   
   return rc;
}


// schedule a block to be deleted.
// return 0 on success
// return -EAGAIN if the cache is not running
// return -ENOMEM if OOM
int md_cache_evict_block_async( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   int rc = 0;
   
   if( !cache->running ) {
      return -EAGAIN;
   }
   
   struct md_cache_entry_key c;
   md_cache_entry_key_init( &c, file_id, file_version, block_id, block_version );
   
   md_cache_promotes_wlock( cache );
   
   try {
      cache->evicts->push_back( c );
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   md_cache_promotes_unlock( cache );
   
   return rc;
}

// apply a function over a file's cached blocks
// keep applying it even if the callback fails on some of them
// return 0 on success
// return -ENOMEM on OOM
// return negative on opendir(2) failure 
// return non-zero if the block_func callback does not return 0
int md_cache_file_blocks_apply( char const* local_path, int (*block_func)( char const*, void* ), void* cls ) {
   
   struct dirent* result = NULL;
   char* block_path = NULL;
   int rc = 0;
   int worst_rc = 0;
   int dirent_sz = 0;
   
   DIR* dir = opendir( local_path );
   if( dir == NULL ) {
      int rc = -errno;
      return rc;
   }

   dirent_sz = offsetof(struct dirent, d_name) + pathconf(local_path, _PC_NAME_MAX) + 1;

   struct dirent* dent = SG_CALLOC( struct dirent, dirent_sz );
   if( dent == NULL ) {
      
      closedir( dir );
      return -ENOMEM;
   }
   
   do {
      
      readdir_r( dir, dent, &result );
      if( result != NULL ) {
         
         if( strcmp(result->d_name, ".") == 0 || strcmp(result->d_name, "..") == 0 ) {
            continue;
         }
         
         block_path = md_fullpath( local_path, result->d_name, NULL );
         if( block_path == NULL ) {
            
            worst_rc = -ENOMEM;
            break;
         }
         
         rc = (*block_func)( block_path, cls );
         if( rc != 0 ) {
            
            // could not unlink
            rc = -errno;
            SG_error( "block_func(%s) errno = %d\n", block_path, rc );
            worst_rc = rc;
         }
         
         SG_safe_free( block_path );
      }
      
   } while( result != NULL );

   closedir( dir );
   free( dent );
   
   return worst_rc;
}


// evict a file from the cache
// return 0 on success
// return -ENOMEM on OOM
// return negative if unlink(2) fails due to something besides -ENOENT
int md_cache_evict_file( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version ) {
   
   char* local_file_path = NULL;
   char* local_file_url = NULL;
   int rc = 0;
   
   struct local {
      // lambda function for deleting a block and evicting it 
      static int cache_evict_block( char const* block_path, void* cls ) {
         
         struct md_syndicate_cache* c = (struct md_syndicate_cache*)cls;
         
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
            SG_error("WARN: unlink( %s ) rc = %d\n", block_path, rc );
            
            // nevertheless, try to evict as much as possible
            rc = 0;
         }
         
         return rc;
      }
   };
   
   // path to the file...
   local_file_url = md_url_local_file_url( cache->conf->data_root, cache->conf->volume, file_id, file_version );
   if( local_file_url == NULL ) {
      return -ENOMEM;
   }
   
   local_file_path = SG_URL_LOCAL_PATH( local_file_url );
   
   rc = md_cache_file_blocks_apply( local_file_path, local::cache_evict_block, cache );
   
   if( rc == 0 ) {
      
      // remove this file's directories
      md_rmdirs( local_file_path );
   }
   
   SG_safe_free( local_file_url );
   return rc;
}


// reversion a file.
// move it into place, and then insert the new cache_entry_key records for it to the cache_lru list.
// don't bother removing the old cache_entry_key records; they will be removed from the cache_lru list automatically.
// NOTE: the corresponding fent structure should be write-locked for this, to make it atomic.
// return 0 on success
// return -ENOMEM on OOM 
// return negative if stat(2) on the new path fails for some reason besides -ENOENT
int md_cache_reversion_file( struct md_syndicate_cache* cache, uint64_t file_id, int64_t old_file_version, int64_t new_file_version ) {
   
   char* cur_local_url = md_url_local_file_url( cache->conf->data_root, cache->conf->volume, file_id, old_file_version );
   if( cur_local_url == NULL ) {
      return -ENOMEM;
   }
   
   char* new_local_url = md_url_local_file_url( cache->conf->data_root, cache->conf->volume, file_id, new_file_version );
   if( new_local_url == NULL ) {
      
      SG_safe_free( cur_local_url );
      return -ENOMEM;
   }

   char* cur_local_path = SG_URL_LOCAL_PATH( cur_local_url );
   char* new_local_path = SG_URL_LOCAL_PATH( new_local_url );
   
   // new path shouldn't exist, but old path should
   struct stat old_sb;
   struct stat new_sb;
   int rc = 0;
   
   rc = stat( cur_local_path, &old_sb );
   if( rc != 0 ) {
      
      rc = -errno;
      if( rc != -ENOENT ) {
         
         // problem 
         SG_error("Failed to stat %s, rc = %d\n", cur_local_path, rc );
         
         SG_safe_free( cur_local_url );
         SG_safe_free( new_local_url );
         
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
      
      SG_error("Failed to stat %s, rc = %d\n", new_local_path, rc );
      
      SG_safe_free( cur_local_url );
      SG_safe_free( new_local_url );
      
      return rc;
   }

   // move the file data over
   rc = rename( cur_local_path, new_local_path );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("rename(%s,%s) rc = %d\n", cur_local_path, new_local_path, rc );
   
      SG_safe_free( cur_local_url );
      SG_safe_free( new_local_url );
      
      return rc;
   }
   
   // insert the new records
   md_cache_lru_t lru;
   
   struct md_cache_cb_add_lru_args lru_args;
   lru_args.cache_lru = &lru;
   lru_args.file_id = file_id;
   lru_args.file_version = new_file_version;
   
   rc = md_cache_file_blocks_apply( new_local_path, md_cache_cb_add_lru, &lru_args );
   
   if( rc == 0 ) {
      // promote these blocks in the cache
      md_cache_promotes_wlock( cache );
      
      for( md_cache_lru_t::iterator itr = lru.begin(); itr != lru.end(); itr++ ) {
         try {
            cache->promotes->push_back( *itr );
         }
         catch( bad_alloc& ba ) {
            rc = -ENOMEM;
            break;
         }
      }
      md_cache_promotes_unlock( cache );
   }
   
   SG_safe_free( cur_local_url );
   SG_safe_free( new_local_url );
   
   return rc;
}


// initialize the cache 
// return 0 on success
// return -ENOMEM if OOM 
// return -EINVAL if soft_limit and hard_limit are both 0
int md_cache_init( struct md_syndicate_cache* cache, struct md_syndicate_conf* conf, size_t soft_limit, size_t hard_limit ) {
   
   int rc = 0;
   
   if( soft_limit == 0 && hard_limit == 0 ) {
      return -EINVAL;
   }
   
   memset( cache, 0, sizeof(struct md_syndicate_cache) );
   
   pthread_rwlock_t* locks[] = {
      &cache->pending_lock,
      &cache->completed_lock,
      &cache->cache_lru_lock,
      &cache->promotes_lock,
      &cache->ongoing_writes_lock,
      NULL
   };
   
   cache->conf = conf;
   
   for( int i = 0; locks[i] != NULL; i++ ) {
      rc = pthread_rwlock_init( locks[i], NULL );
      if( rc != 0 ) {
         
         rc = -errno;
         
         // free up and return 
         for( int j = 0; j < i; j++ ) {
            pthread_rwlock_destroy( locks[i] );
         }
         
         return rc;
      }
   }
   
   SG_debug("Soft limit: %zu blocks.  Hard limit: %zu blocks\n", soft_limit, hard_limit );
   
   cache->hard_max_size = hard_limit;
   cache->soft_max_size = soft_limit;
   
   sem_init( &cache->sem_write_hard_limit, 0, hard_limit );
   sem_init( &cache->sem_blocks_writing, 0, 0 );
   
   cache->pending_1 = SG_safe_new( md_cache_block_buffer_t() );
   cache->pending_2 = SG_safe_new( md_cache_block_buffer_t() );
   cache->pending = cache->pending_1;
   
   cache->completed_1 = SG_safe_new( md_cache_completion_buffer_t() );
   cache->completed_2 = SG_safe_new( md_cache_completion_buffer_t() );
   cache->completed = cache->completed_1;
   
   cache->cache_lru = SG_safe_new( md_cache_lru_t() );
   
   cache->promotes_1 = SG_safe_new( md_cache_lru_t() );
   cache->promotes_2 = SG_safe_new( md_cache_lru_t() );
   cache->promotes = cache->promotes_1;
   
   cache->evicts_1 = SG_safe_new( md_cache_lru_t() );
   cache->evicts_2 = SG_safe_new( md_cache_lru_t() );
   cache->evicts = cache->evicts_1;
   
   cache->ongoing_writes = SG_safe_new( md_cache_ongoing_writes_t() );
   
   // verify all alloc's succeeded
   if( cache->pending_1 == NULL || cache->pending_2 == NULL ||
       cache->completed_1 == NULL || cache->completed_2 == NULL ||
       cache->promotes_1 == NULL || cache->promotes_2 == NULL ||
       cache->evicts_1 == NULL || cache->evicts_2 == NULL ||
       cache->ongoing_writes == NULL ) {
      
      cache->running = false;
      md_cache_destroy( cache );
      return -ENOMEM;
   }
   
   return 0;
}

// start the cache thread
// return 0 on success
// return -1 if we failed to start the thread
int md_cache_start( struct md_syndicate_cache* cache ) {
   
   int rc = 0;
   
   // start the thread up 
   struct md_syndicate_cache_thread_args* args = SG_CALLOC( struct md_syndicate_cache_thread_args, 1 );
   args->cache = cache;
   
   cache->running = true;
   
   rc = md_start_thread( &cache->thread, md_cache_main_loop, (void*)args, false );
   if( rc < 0 ) {
      
      SG_error("md_start_thread rc = %d\n", rc );
      return -1;
   }
   
   return 0;
}

// stop the cache thread 
// always succeeds
int md_cache_stop( struct md_syndicate_cache* cache ) {
   
   cache->running = false;
   
   // wake up the writer
   sem_post( &cache->sem_blocks_writing );
   
   // wait for cache thread to finish 
   pthread_cancel( cache->thread );
   pthread_join( cache->thread, NULL );
   
   return 0;
}


// destroy the cache
// return 0 on success
// return -EINVAL if the cache is still running
int md_cache_destroy( struct md_syndicate_cache* cache ) {
   
   if( cache->running ) {
      // have to stop it first
      return -EINVAL;
   }
   
   cache->pending = NULL;
   cache->completed = NULL;
   
   md_cache_block_buffer_t* pendings[] = {
      cache->pending_1,
      cache->pending_2,
      NULL
   };
   
   for( int i = 0; pendings[i] != NULL; i++ ) {
      for( md_cache_block_buffer_t::iterator itr = pendings[i]->begin(); itr != pendings[i]->end(); itr++ ) {
         if( *itr != NULL ) {
            SG_safe_free( *itr );
         }
      }
      
      SG_safe_delete( pendings[i] );
   }
   
   md_cache_completion_buffer_t* completeds[] = {
      cache->completed_1,
      cache->completed_2,
      NULL
   };
   
   for( int i = 0; completeds[i] != NULL; i++ ) {
      for( md_cache_completion_buffer_t::iterator itr = completeds[i]->begin(); itr != completeds[i]->end(); itr++ ) {
         struct md_cache_block_future* f = *itr;
         md_cache_block_future_free( f );
      }
      
      SG_safe_delete( completeds[i] );
   }
   
   md_cache_lru_t* lrus[] = {
      cache->cache_lru,
      cache->promotes_1,
      cache->promotes_2,
      cache->evicts_1,
      cache->evicts_2,
      NULL
   };
   
   for( int i = 0; lrus[i] != NULL; i++ ) {
      SG_safe_delete( lrus[i] );
   }
   
   SG_safe_delete( cache->ongoing_writes );
   
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
// return 0 on success
// return -ENOMEM on OOM
int md_cache_block_future_init( struct md_syndicate_cache* cache, struct md_cache_block_future* f,
                                uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int block_fd,
                                char* data, size_t data_len,
                                uint64_t flags ) {
   
   // set up completion args
   struct md_syndicate_cache_aio_write_args* wargs = SG_CALLOC( struct md_syndicate_cache_aio_write_args, 1 );
   if( wargs == NULL ) {
      return -ENOMEM;
   }
   
   memset( f, 0, sizeof( struct md_cache_block_future ) );
   
   f->key.file_id = file_id;
   f->key.file_version = file_version;
   f->key.block_id = block_id;
   f->key.block_version = block_version;
   
   f->block_fd = block_fd;
   f->block_data = data;
   f->data_len = data_len;
   f->flags = flags;
   
   // fill in aio structure
   f->aio.aio_fildes = block_fd;
   f->aio.aio_buf = data;
   f->aio.aio_nbytes = data_len;
   f->aio.aio_offset = 0;
   
   // set up callback
   f->aio.aio_sigevent.sigev_notify = SIGEV_THREAD;
   f->aio.aio_sigevent.sigev_notify_function = md_cache_aio_write_completion;
   f->aio.aio_sigevent.sigev_notify_attributes = NULL;
   
   wargs->cache = cache;
   wargs->future = f;
   
   f->aio.aio_sigevent.sigev_value.sival_ptr = (void*)wargs;
   
   sem_init( &f->sem_ongoing, 0, 0 );
   
   return 0;
}

// add a block future to ongoing 
// cache->ongoing_lock must be write-locked
// return 0 on success
// return -ENOMEM on OOM
static int md_cache_add_ongoing( struct md_syndicate_cache* cache, struct md_cache_block_future* f ) {
   
   try {
      cache->ongoing_writes->insert( f );   
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}

// remove a block future from ongoing
// cache->ongoing_lock must be write-locked 
// return 0 on success
// return -ENOMEM on OOM 
static int md_cache_remove_ongoing( struct md_syndicate_cache* cache, struct md_cache_block_future* f ) {
   
   try {
      cache->ongoing_writes->erase( f );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}

// asynchronously write a block 
// return 0 on success
// return negative errno on aio_write(3) failure
static int md_cache_aio_write( struct md_syndicate_cache* cache, struct md_cache_block_future* f ) {
   
   // allow external clients to keep track of pending writes for this file
   md_cache_ongoing_writes_wlock( cache );
   
   int rc = aio_write( &f->aio );
   
   if( rc == 0 ) {
      // put one new block
      md_cache_add_ongoing( cache, f );
   }
   else {
      
      rc = -errno;
      SG_error("aio_write(%p) rc = %d\n", &f->aio, rc );
   }
   
   md_cache_ongoing_writes_unlock( cache );
   
   return rc;
}


// handle a completed write operation
// put error codes into future->aio_rc and future->write_rc
// always succeeds
void md_cache_aio_write_completion( sigval_t sigval ) {
   
   struct md_syndicate_cache_aio_write_args* wargs = (struct md_syndicate_cache_aio_write_args*)sigval.sival_ptr;
   
   struct md_syndicate_cache* cache = wargs->cache;
   struct md_cache_block_future* future = wargs->future;
   
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
         // rewind file handle, so other clients can access it
         lseek( future->block_fd, 0, SEEK_SET );
      }
   }
   else {
      write_rc = -aio_rc;
   }
   
   future->aio_rc = aio_rc;
   future->write_rc = write_rc;
   
   // enqueue for reaping
   md_cache_completed_wlock( cache );
   
   cache->completed->push_back( future );
   
   md_cache_completed_unlock( cache );
}


// start pending writes.  keep trying even if some fail to start
// NOTE: we assume that only one thread calls this, for a given cache
// return 0 on success
// return negative on failure (see md_cache_aio_write)
int md_cache_begin_writes( struct md_syndicate_cache* cache ) {
   
   int worst_rc = 0;
   
   // get the pending set, and switch the cache over to the other one
   md_cache_block_buffer_t* pending = NULL;
   
   md_cache_pending_wlock( cache );
   
   pending = cache->pending;
   if( cache->pending == cache->pending_1 ) {
      cache->pending = cache->pending_2;
   }
   else {
      cache->pending = cache->pending_1;
   }
   
   md_cache_pending_unlock( cache );
   
   // safe to use pending as long as no one else performs the above swap
   
   // start pending writes
   for( md_cache_block_buffer_t::iterator itr = pending->begin(); itr != pending->end(); itr++ ) {
      struct md_cache_block_future* f = *itr;
      struct md_cache_entry_key* c = &f->key;
      
      int rc = md_cache_aio_write( cache, f );
      if( rc < 0 ) {
         SG_error("md_cache_aio_write( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ), rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, rc );
         worst_rc = rc;
      }
   }
   
   pending->clear();
   
   return worst_rc;
}


// reap completed writes
// if a write failed, remove the data from the cache.
// NOTE: we assume that only one thread calls this at a time, for a given cache
// always succeeds
void md_cache_complete_writes( struct md_syndicate_cache* cache, md_cache_lru_t* write_lru ) {
   
   md_cache_completion_buffer_t* completed = NULL;
   
   // get the current completed buffer, and switch to the other
   md_cache_completed_wlock( cache );
   
   completed = cache->completed;
   if( cache->completed == cache->completed_1 ) {
      cache->completed = cache->completed_2;
   }
   else {
      cache->completed = cache->completed_1;
   }
   
   md_cache_completed_unlock( cache );
   
   // safe to use completed as long as no one else performs the above swap
   
   int write_count = 0;
   
   // reap completed writes
   for( md_cache_completion_buffer_t::iterator itr = completed->begin(); itr != completed->end(); itr++ ) {
      
      struct md_cache_block_future* f = *itr;
      struct md_cache_entry_key* c = &f->key;
      
      // finished an aio write
      md_cache_ongoing_writes_wlock( cache );
      
      md_cache_remove_ongoing( cache, f );
      
      md_cache_ongoing_writes_unlock( cache );
      
      if( f->aio_rc != 0 ) {
         SG_error("WARN: write aio %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, f->aio_rc );
         
         // clean up 
         md_cache_evict_block_internal( cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else if( f->write_rc < 0 ) {
         SG_error("WARN: write %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n", c->file_id, c->file_version, c->block_id, c->block_version, f->write_rc );
         
         // clean up 
         md_cache_evict_block_internal( cache, c->file_id, c->file_version, c->block_id, c->block_version );
      }
      else {
         // finished!
         if( write_lru ) {
            // log this as written
            write_lru->push_back( *c );
         }
         
         write_count ++;
      }
      
      // finalized!
      f->finalized = true;
      
      bool detached = ((f->flags & SG_CACHE_FLAG_DETACHED) != 0);
      
      // wake up anyone waiting on this
      sem_post( &f->sem_ongoing );
      
      // are we supposed to reap it?
      if( detached || !cache->running ) {
         md_cache_block_future_free( f );
      }
   }
   
   // successfully cached blocks
   __sync_fetch_and_add( &cache->num_blocks_written, write_count );
   
   if( write_count != 0 )
      SG_debug("Cache now has %d blocks\n", cache->num_blocks_written );
   
   completed->clear();
}



// promote blocks in a cache LRU
// return 0 on success
// return -ENOMEM on OOM
int md_cache_promote_blocks( md_cache_lru_t* cache_lru, md_cache_lru_t* promotes ) {
   
   try {
      // process block promotions
      // we can (probably) afford to be linear here, since we won't have millions of entries.
      // TODO: investigate boost biamp if not
      for( md_cache_lru_t::iterator pitr = promotes->begin(); pitr != promotes->end(); pitr++ ) {
         
         // search from the back first, since we might be hitting blocks that were recently read.
         md_cache_lru_t::iterator citr = cache_lru->end();
         citr--;
         for( ; citr != cache_lru->begin(); citr-- ) {
            
            if( md_cache_entry_key_comp::equal( *pitr, *citr ) ) {
               // promote this entry--take it out of the LRU and splice it at the end (below)
               citr = cache_lru->erase( citr );
            }
         }
      }
      
      // add the newly-promoted blocks to the end of the LRU (i.e. they are most-recently-used)
      cache_lru->splice( cache_lru->end(), *promotes );
      
      return 0;
   }
   catch( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
}


// demote blocks in a cache LRU 
// return 0 on success
// return -ENOMEM on OOM 
int md_cache_demote_blocks( md_cache_lru_t* cache_lru, md_cache_lru_t* demotes ) {
   
   try {
      // process block demotions
      // we can (probably) afford to be linear here, since we won't have millions of entries.
      // TODO: investigate boost biamp if not
      for( md_cache_lru_t::iterator pitr = demotes->begin(); pitr != demotes->end(); pitr++ ) {
         // search from the beginning, since we might be hitting blocks that are close to eviction anyway
         for( md_cache_lru_t::iterator citr = cache_lru->begin(); citr != cache_lru->end(); citr++ ) {   
            if( md_cache_entry_key_comp::equal( *pitr, *citr ) ) {
               // demote this entry--take it out of the LRU and splite it at the beginning (below)
               citr = cache_lru->erase( citr );
            }
         }
      }
      
      // add the newly-dmoted blocks to the beginning of the LRU (i.e. they are now the least-recently-used)
      cache_lru->splice( cache_lru->begin(), *demotes );
      
      return 0;
   }
   catch( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
}


// evict blocks, according to their LRU ordering and whether or not they are requested to be eagerly evicted
// NOTE: we assume that only one thread calls this at a time, for a given cache
// return 0 on success
// return the last eviction-related error on failure (i.e. due to bad I/O) (see md_cache_evict_block_internal)
int md_cache_evict_blocks( struct md_syndicate_cache* cache, md_cache_lru_t* new_writes ) {
   
   md_cache_lru_t* promotes = NULL;
   md_cache_lru_t* evicts = NULL;
   int worst_rc = 0;
   
   // swap promotes
   md_cache_promotes_wlock( cache );
   
   promotes = cache->promotes;
   if( cache->promotes == cache->promotes_1 ) {
      cache->promotes = cache->promotes_2;
   }
   else {
      cache->promotes = cache->promotes_1;
   }
   
   evicts = cache->evicts;
   if( cache->evicts == cache->evicts_1 ) {
      cache->evicts = cache->evicts_2;
   }
   else {
      cache->evicts = cache->evicts_1;
   }
   
   md_cache_promotes_unlock( cache );
   
   // safe access to the promote and evicts buffers, as long as no one performs the above swap
   
   md_cache_lru_wlock( cache );
   
   // merge in the new writes, as the most-recently-used
   if( new_writes ) {
      cache->cache_lru->splice( cache->cache_lru->end(), *new_writes );
   }
   
   // process promotions
   md_cache_promote_blocks( cache->cache_lru, promotes );
   
   // process demotions 
   md_cache_demote_blocks( cache->cache_lru, evicts );
   
   // NOTE: all blocks scheduled for eager eviction are at the beginning of cache_lru.
   // we will evict them here, even if the cache is not full.
   
   // see if we should start erasing blocks
   int num_blocks_written = cache->num_blocks_written;
   int blocks_removed = 0;
   int eager_evictions = evicts->size();        // number of blocks to eagerly evict
   
   // work to do?
   if( cache->cache_lru->size() > 0 && ((unsigned)num_blocks_written > cache->soft_max_size || eager_evictions > 0) ) {
      
      // start evicting
      do { 
         
         // least-recently-used block
         struct md_cache_entry_key c = cache->cache_lru->front();
         cache->cache_lru->pop_front();
         
         int rc = md_cache_evict_block_internal( cache, c.file_id, c.file_version, c.block_id, c.block_version );
         
         if( rc != 0 && rc != -ENOENT ) {
            
            SG_warn("Failed to evict %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "], rc = %d\n", c.file_id, c.file_version, c.block_id, c.block_version, rc );
            worst_rc = rc;
         }
         else {
            // successfully evicted a block
            SG_debug("Cache EVICT %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", c.file_id, c.file_version, c.block_id, c.block_version );
            blocks_removed ++;
            eager_evictions --;
         }
         
      } while( cache->cache_lru->size() > 0 && ((unsigned)num_blocks_written - (unsigned)blocks_removed > cache->soft_max_size || eager_evictions > 0) );
      
      // blocks evicted!
      __sync_fetch_and_sub( &cache->num_blocks_written, blocks_removed );
      
      SG_debug("Cache now has %d blocks\n", cache->num_blocks_written );
   }
   
   md_cache_lru_unlock( cache );
   
   // done with this
   promotes->clear();
   evicts->clear();
   
   return worst_rc;
}


// cache main loop.
// * start new writes
// * reap completed writes
// * evict blocks after the soft size limit has been exceeded
void* md_cache_main_loop( void* arg ) {
   struct md_syndicate_cache_thread_args* args = (struct md_syndicate_cache_thread_args*)arg;
   
   struct md_syndicate_cache* cache = args->cache;
   
   // cancel whenever by default
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   SG_debug("%s", "Cache writer thread strated\n" );
   
   while( cache->running ) {
      
      // wait for there to be blocks, if there are none
      if( cache->ongoing_writes->size() == 0 ) {
         sem_wait( &cache->sem_blocks_writing );
      }
      
      // waken up to die?
      if( !cache->running ) {
         break;
      }
      
      md_cache_lru_t new_writes;
      
      // don't get cancelled while doing this
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      // begin all pending writes
      md_cache_begin_writes( cache );
      
      // reap completed writes
      md_cache_complete_writes( cache, &new_writes );
      
      // can get cancelled now if needed
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
      
      // evict blocks 
      md_cache_evict_blocks( cache, &new_writes );
   }
   
   // wait for remaining writes to finish 
   // TODO: aio cancellations
   while( cache->ongoing_writes->size() > 0 ) {
      SG_debug("Waiting for %zu blocks to sync...\n", cache->ongoing_writes->size() );
      
      md_cache_lru_t new_writes;
      
      // don't get cancelled here
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      // reap completed writes
      md_cache_complete_writes( cache, &new_writes );
      
      // can get cancelled now if needed
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
      
      // evict blocks 
      md_cache_evict_blocks( cache, &new_writes );
      
      sleep(1);
   }
   
   SG_safe_free( args );
   
   SG_debug("%s", "Cache writer thread exited\n" );
   
   return NULL;
}

// add a block to the cache, to be written asynchronously.
// return a future that can be waited on 
// return NULL on error, and set *_rc to the error code
// *_rc can be:
// * -EAGAIN if the cache is not running
// * -ENOMEM if OOM
// * -EEXIST if the block already exists
// * negative if we failed to open the block (see md_cache_open_block)
// NOTE: the given data will be referenced!  Do NOT free it!
struct md_cache_block_future* md_cache_write_block_async( struct md_syndicate_cache* cache,
                                                          uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version,
                                                          char* data, size_t data_len,
                                                          uint64_t flags, int* _rc ) {
   
   *_rc = 0;
   
   if( !cache->running ) {
      *_rc = -EAGAIN;
      return NULL;
   }
   
   // reserve the right to cache this block
   sem_wait( &cache->sem_write_hard_limit );
   
   struct md_cache_block_future* f = SG_CALLOC( struct md_cache_block_future, 1 );
   if( f == NULL ) {
      
      *_rc = -ENOMEM;
      return NULL;
   }
   
   // create the block to cache
   int block_fd = md_cache_open_block( cache, file_id, file_version, block_id, block_version, O_CREAT | O_EXCL | O_RDWR | O_TRUNC );
   if( block_fd < 0 ) {
      
      *_rc = block_fd;
      SG_error("md_cache_open_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", file_id, file_version, block_id, block_version, block_fd );
      
      SG_safe_free( f );
      return NULL;
   }
   
   md_cache_block_future_init( cache, f, file_id, file_version, block_id, block_version, block_fd, data, data_len, flags );
   
   md_cache_pending_wlock( cache );
   
   try {
      cache->pending->push_back( f );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( f );
      f = NULL;
      
      *_rc = -ENOMEM;
   }
   
   // wake up the thread--we have another block
   sem_post( &cache->sem_blocks_writing );
   
   md_cache_pending_unlock( cache );
   
   return f;
}

// wait for a write to finish
// return 0 on success
// return negative if we couldn't wait on the semaphore (see md_download_sem_wait)
int md_cache_block_future_wait( struct md_cache_block_future* f ) {
   
   int rc = md_download_sem_wait( &f->sem_ongoing, -1 );
   if( rc != 0 ) {
      
      SG_error("md_download_sem_wait rc = %d\n", rc );
   }
   
   return rc;
}

// does a block future have an error?
// return 0 if no error 
// return -EAGAIN if the future is not yet finalized 
// return 1 if either aio_rc or write_rc has been set to indicate error
int md_cache_block_future_has_error( struct md_cache_block_future* f ) {
   
   if( !f->finalized ) {
      return -EAGAIN;
   }
   
   if( f->aio_rc != 0 || f->write_rc < 0 ) {
      return 1;
   }
   
   return 0;
}

// what's the aio rc?
// return -EAGAIN if f is not finalized
int md_cache_block_future_get_aio_error( struct md_cache_block_future* f ) {
   if( !f->finalized ) {
      return -EAGAIN;
   }
   
   return f->aio_rc;
}

// what's the write error?
// return -EAGAIN if the future is not finalized
int md_cache_block_future_get_write_error( struct md_cache_block_future* f ) {
   if( !f->finalized ) {
      return -EAGAIN;
   }
   
   return f->write_rc;
}

// get the block future's file descriptor 
int md_cache_block_future_get_fd( struct md_cache_block_future* f ) {
   return f->block_fd;
}

// get block future file ID
uint64_t md_cache_block_future_file_id( struct md_cache_block_future* fut ) {
   return fut->key.file_id;
}

// get block future file version 
int64_t md_cache_block_future_file_version( struct md_cache_block_future* fut ) {
   return fut->key.file_version;
}

// get block future block id
uint64_t md_cache_block_future_block_id( struct md_cache_block_future* fut ) {
   return fut->key.block_id;
}

// get block future block version
int64_t md_cache_block_future_block_version( struct md_cache_block_future* fut ) {
   return fut->key.block_version;
}

// extract the block file descriptor from a future, making it so the cache is no longer responsible for it.
// caller must close and clean up.
// NOTE: only call this after the future has finished!
// return the file descriptor (>= 0)
int md_cache_block_future_release_fd( struct md_cache_block_future* f ) {
   
   int fd = f->block_fd;
   f->block_fd = -1;
   return fd;
}

// extract the data from a future, and return it
// the caller must free it
char* md_cache_block_future_release_data( struct md_cache_block_future* f ) {
   char* ret = f->block_data;
   
   // data is no longer detached
   f->flags &= ~(SG_CACHE_FLAG_DETACHED);
   f->block_data = NULL;
   return ret;
}

// unshare data from a cache future 
// the cache future will free it, so the caller had better not 
// return 0 on success
// return -EINVAL if the future is finalized
int md_cache_block_future_unshare_data( struct md_cache_block_future* f ) {
   
   if( f->finalized ) {
      return -EINVAL;
   }
   
   f->flags |= SG_CACHE_FLAG_UNSHARED;
   return 0;
}

// promote a cached block, so it doesn't get evicted
// return 0 on success
// return -EAGAIN if the cache isn't running 
// return -ENOMEM on OOM
int md_cache_promote_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   int rc = 0;
   
   if( !cache->running ) {
      return -EAGAIN;
   }
   
   struct md_cache_entry_key c;
   md_cache_entry_key_init( &c, file_id, file_version, block_id, block_version );
   
   md_cache_promotes_wlock( cache );
   
   try {
      cache->promotes->push_back( c );
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   md_cache_promotes_unlock( cache );
   
   return rc;
}


// read a block from the cache, in its entirety
// return the number of bytes read on success (>= 0)
// return -errno if we couldn't fstat(2) the block 
// return -ENOMEM if OOM
ssize_t md_cache_read_block( int block_fd, char** buf ) {
   
   ssize_t nr = 0;
   struct stat sb;
   int rc = 0;
   ssize_t len = 0;
   char* block_buf = NULL;
   
   rc = fstat( block_fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fstat(%d) rc = %d\n", block_fd, rc );
      return rc;
   }
   
   len = sb.st_size;
   block_buf = SG_CALLOC( char, len );
   
   if( block_buf == NULL ) {
      
      return -ENOMEM;
   }
   
   nr = md_read_uninterrupted( block_fd, block_buf, len );
   
   *buf = block_buf;
   
   return nr;
}
