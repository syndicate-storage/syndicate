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
#include "core.h"
#include "driver.h"

// generate a file ID from a path 
uint64_t AG_cache_file_id( char const* path) {
   uint64_t file_id = (uint64_t)md_hash( path );
   return file_id;
}

// get a block from the cache 
int AG_cache_get_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version, char** block, size_t* block_len ) {
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( path );

   int fd = md_cache_open_block( state->cache, file_id, file_version, block_id, block_version, O_RDONLY );
   if( fd < 0 ) {
      
      if( fd != -ENOENT ) {
         errorf("md_cache_open_block(%s.%" PRId64 ".%" PRIu64 ".%" PRId64 ") rc = %d\n", path, file_version, block_id, block_version, fd );
      }
      else {
         dbprintf("CACHE MISS %s.%" PRId64 ".%" PRIu64 ".%" PRId64 "\n", path, file_version, block_id, block_version );
      }
      
      return fd;
   }
   
   uint64_t block_size = ms_client_get_volume_blocksize( state->ms );
   
   // sanity check: must be no bigger than a block
   struct stat sb;
   int rc = fstat( fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("fstat(%d) rc = %d\n", fd, rc );
      
      return rc;
   }
   
   if( (unsigned)sb.st_size > block_size ) {
      
      errorf("Block is %jd bytes, but our block size is %" PRIu64 "\n", sb.st_size, block_size );
      
      md_cache_evict_block( state->cache, file_id, file_version, block_id, block_version );
      
      return -ENOMEM;
   }
   
   ssize_t nr = md_cache_read_block( fd, block );
   
   if( nr < 0 ) {
      
      errorf("md_cache_read_block(%s.%" PRId64 ".%" PRIu64 ".%" PRId64 ") rc = %d\n", path, file_version, block_id, block_version, fd );
      
      close( fd );
      return (int)nr;
   }
   
   *block_len = (size_t)nr;
   
   close( fd );
   
   dbprintf("CACHE HIT %s.%" PRId64 ".%" PRIu64 ".%" PRId64 "\n", path, file_version, block_id, block_version );
   
   return 0;
}

// promote a block in the cache 
int AG_cache_promote_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( path );

   int rc = md_cache_promote_block( state->cache, file_id, file_version, block_id, block_version );
   if( rc != 0 ) {
      errorf("md_cache_promote_block(%s.%" PRId64 ".%" PRIu64 ".%" PRId64 ") rc = %d\n", path, file_version, block_id, block_version, rc );
   }
   
   return rc;
}

// put a block into the cache (asynchronously)
// NOTE: the cache takes ownership of the block!
int AG_cache_put_block_async( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version, char* block, size_t block_len ) {
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( path );

   int rc = 0;
   
   struct md_cache_block_future* block_fut = md_cache_write_block_async( state->cache, file_id, file_version, block_id, block_version, block, block_len, true, &rc );
   
   if( block_fut == NULL || rc != 0 ) {
      
      errorf("md_cache_write_block_async(%s.%" PRId64 ".%" PRIu64 ".%" PRId64 ") rc = %d\n", path, file_version, block_id, block_version, rc );
      return rc;
   }
   
   // NOTE: block_fut will get freed internally, since the write is detached from the caller
   return rc;
}

// evict a block
int AG_cache_evict_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( path );

   int rc = 0;
   
   rc = md_cache_evict_block( state->cache, file_id, file_version, block_id, block_version );
   
   if( rc != 0 ) {
      errorf("md_cache_evict_block(%s.%" PRId64 ".%" PRIu64 ".%" PRId64 ") rc = %d\n", path, file_version, block_id, block_version, rc );
   }
   
   return rc;
}

// make a cache path to a pubinfo, given the path to the associated map_info 
char* AG_cache_stat_path( char const* path ) {
   return md_fullpath( path, "stat", NULL );
}

// get a cached pubinfo 
int AG_cache_get_stat( struct AG_state* state, char const* path, int64_t file_version, struct AG_driver_publish_info* pubinfo ) {
   
   char* stat_path = AG_cache_stat_path( path );
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( stat_path );

   int fd = md_cache_open_block( state->cache, file_id, file_version, -1, -1, O_RDONLY );
   if( fd < 0 ) {
      
      if( fd != -ENOENT ) {
         errorf("md_cache_open_block(%s.%" PRId64 ".) rc = %d\n", stat_path, file_version, fd );
      }
      else {
         dbprintf("CACHE MISS %s.%" PRId64 "\n", stat_path, file_version );
      }
      
      free( stat_path );
      
      return fd;
   }
   
   // sanity check: must be no bigger than the structure
   struct stat sb;
   int rc = fstat( fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      errorf("fstat(%d) rc = %d\n", fd, rc );
    
      free( stat_path );
      
      return rc;
   }
   
   if( (unsigned)sb.st_size > sizeof(struct AG_driver_publish_info) ) {
      
      errorf("Chunk is %jd bytes, but our stat buffer size is %zu\n", sb.st_size, sizeof(struct AG_driver_publish_info) );
      
      md_cache_evict_block( state->cache, file_id, file_version, -1, -1 );
      
      free( stat_path );
      
      return -ENOMEM;
   }
   
   char* buf = NULL;
   ssize_t nr = md_cache_read_block( fd, &buf );
   
   if( nr < 0 || nr != sizeof(struct AG_driver_publish_info) ) {
      
      errorf("md_cache_read_block(%s.%" PRId64 ") rc = %d\n", stat_path, file_version, fd );
      
      close( fd );
      
      if( nr > 0 ) {
         // invalid cached data 
         md_cache_evict_block( state->cache, file_id, file_version, -1, -1 );
      }
      
      free( stat_path );
      return (int)nr;
   }
   
   memcpy( pubinfo, buf, sizeof(struct AG_driver_publish_info) );
   free( buf );
   
   close( fd );
   
   dbprintf("CACHE HIT %s.%" PRId64 ": { size = %zu, mtime_sec = %" PRId64 ", mtime_nsec = %" PRId32 " }\n", stat_path, file_version, pubinfo->size, pubinfo->mtime_sec, pubinfo->mtime_nsec );
   
   free( stat_path );
   return 0;
}


// promote a cached stat
int AG_cache_promote_stat( struct AG_state* state, char const* path, int64_t file_version ) {
   
   char* stat_path = AG_cache_stat_path( path );
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( stat_path );

   int rc = md_cache_promote_block( state->cache, file_id, file_version, -1, -1 );
   if( rc != 0 ) {
      errorf("md_cache_promote_block(%s.%" PRId64 ") rc = %d\n", stat_path, file_version, rc );
   }
   
   free( stat_path );
   return rc;
}

// cache a stat, asynchronously 
int AG_cache_put_stat_async( struct AG_state* state, char const* path, int64_t file_version, struct AG_driver_publish_info* pubinfo ) {

   char* stat_path = AG_cache_stat_path( path );
      
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( stat_path );

   int rc = 0;
   
   // duplicate...
   char* chunk = CALLOC_LIST( char, sizeof(struct AG_driver_publish_info) );
   memcpy( chunk, pubinfo, sizeof(struct AG_driver_publish_info) );
   
   struct md_cache_block_future* block_fut = md_cache_write_block_async( state->cache, file_id, file_version, -1, -1, chunk, sizeof(struct AG_driver_publish_info), true, &rc );
   
   if( block_fut == NULL || rc != 0 ) {
      
      errorf("md_cache_write_block_async(%s.%" PRId64 ") rc = %d\n", stat_path, file_version, rc );
      
      free( stat_path );
      return rc;
   }
   
   free( stat_path );
   
   // NOTE: block_fut will get freed internally, since the write is detached from the caller
   return rc;
}


// evict a cached status
int AG_cache_evict_stat( struct AG_state* state, char const* path, int64_t file_version ) {
   
   char* stat_path = AG_cache_stat_path( path );
   
   // convert path into a file_id 
   uint64_t file_id = AG_cache_file_id( stat_path );

   int rc = 0;
   
   rc = md_cache_evict_block( state->cache, file_id, file_version, -1, -1 );
   
   if( rc != 0 ) {
      errorf("md_cache_evict_block(%s.%" PRId64 ") rc = %d\n", stat_path, file_version, rc );
   }
   
   free( stat_path );
   
   return rc;
}


// evict a file's worth of blocks and stats
int AG_cache_evict_file( struct AG_state* state, char const* path, int64_t file_version ) {
   
   int rc = 0;
   
   uint64_t file_id = AG_cache_file_id( path );
   
   // evict all blocks
   rc = md_cache_evict_file( state->cache, file_id, file_version );
   if( rc != 0 ) {
      errorf("md_cache_evict_file(%s.%" PRId64 ") rc = %d\n", path, file_version, rc );
   }
   
   // evict the status
   rc = AG_cache_evict_stat( state, path, file_version );
   if( rc != 0 ) {
      errorf("AG_cache_evict_stat(%s.%" PRId64 ") rc = %d\n", path, file_version, rc );
   }
   
   return rc;
}