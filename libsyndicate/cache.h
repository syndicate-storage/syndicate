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

/*
 * Syndicate on-disk cache.
 * Features:
 * * synchronous, thread-safe reads
 * * asynchronous, thread-safe writes and evictions, via a "write future" abstraction
 * * soft and hard limits
 * * no locks held during I/O, promotion, or LRU eviction
 * * minimal dependency on Syndicate--it only needs its URL-generation code (in url.cpp) and configuration structure (in libsyndicate.h)
 */

#ifndef _LIBSYNDICATE_CACHE_H_
#define _LIBSYNDICATE_CACHE_H_

#include <set>
#include <list>
#include <string>
#include <locale>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <aio.h>

#include "libsyndicate/libsyndicate.h"

#define MD_CACHE_DEFAULT_SOFT_LIMIT        50000000        // 50 MB
#define MD_CACHE_DEFAULT_HARD_LIMIT       100000000        // 100 MB

#define SG_CACHE_FLAG_DETACHED          0x1             // caller won't wait for a future to finish (so the cache should reap it)
#define SG_CACHE_FLAG_UNSHARED          0x2             // cache can free the block data when it frees the block future--it's unshared from the caller

using namespace std;

// prototypes 
struct md_syndicate_conf;

struct md_cache_entry_key {
   uint64_t file_id;
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
};

// "lexigraphic" comparison between cache_entry_keys
bool md_cache_entry_key_comp_func( const struct md_cache_entry_key& c1, const struct md_cache_entry_key& c2 );

struct md_cache_entry_key_comp {
   
   bool operator()( const struct md_cache_entry_key& c1, const struct md_cache_entry_key& c2 ) {
      return md_cache_entry_key_comp_func( c1, c2 );
   }
   
   // equality test
   static bool equal( const struct md_cache_entry_key& c1, const struct md_cache_entry_key& c2 ) {
      return c1.file_id == c2.file_id && c1.file_version == c2.file_version && c1.block_id == c2.block_id && c1.block_version == c2.block_version;
   }
};

// ongoing cache write for a file
struct md_cache_block_future;

typedef list<struct md_cache_block_future*> md_cache_block_buffer_t;
typedef md_cache_block_buffer_t md_cache_completion_buffer_t;
typedef set<struct md_cache_block_future*> md_cache_ongoing_writes_t;
typedef list<struct md_cache_entry_key> md_cache_lru_t;

struct md_syndicate_cache {
   
   // size limits (in blocks, not bytes!)
   size_t hard_max_size;
   size_t soft_max_size;
   
   // reference to global configuration 
   struct md_syndicate_conf* conf;
   
   int num_blocks_written;                  // how many blocks have been successfully written to disk?
   
   // data to cache that is scheduled to be written to disk 
   md_cache_block_buffer_t* pending;
   pthread_rwlock_t pending_lock;
   
   // pending refers to one of these...
   md_cache_block_buffer_t* pending_1;
   md_cache_block_buffer_t* pending_2;
   
   // data that is being asynchronously written to disk
   md_cache_ongoing_writes_t* ongoing_writes;
   pthread_rwlock_t ongoing_writes_lock;
   
   // completed writes, to be reaped 
   md_cache_completion_buffer_t* completed;
   pthread_rwlock_t completed_lock;
   
   // completed refers to one of these
   md_cache_completion_buffer_t* completed_1;
   md_cache_completion_buffer_t* completed_2;
   
   // order in which blocks were added
   md_cache_lru_t* cache_lru;
   pthread_rwlock_t cache_lru_lock;
   
   // blocks to be promoted in the current lru 
   md_cache_lru_t* promotes;
   pthread_rwlock_t promotes_lock;
   
   // promotes refers to one of these
   md_cache_lru_t* promotes_1;
   md_cache_lru_t* promotes_2;
   
   // blocks to be evicted  (guarded by promotes_lock)
   md_cache_lru_t* evicts;
   
   // evicts refers to one of these 
   md_cache_lru_t* evicts_1;
   md_cache_lru_t* evicts_2;
   
   // thread for processing writes and evictions
   pthread_t thread;
   bool running;
   
   // semaphore to block writes once the hard limit is met
   sem_t sem_write_hard_limit;
   
   // semaphore to indicate that there is work to be done
   sem_t sem_blocks_writing;
   
};

// arguments to the main thread 
struct md_syndicate_cache_thread_args {
   struct md_syndicate_cache* cache;
};

// arguments to the write callback
struct md_syndicate_cache_aio_write_args {
   struct md_syndicate_cache* cache;
   struct md_cache_block_future* future;
};

extern "C" {

int md_cache_init( struct md_syndicate_cache* cache, struct md_syndicate_conf* conf, size_t soft_max_blocks, size_t hard_max_blocks );
int md_cache_start( struct md_syndicate_cache* cache );
int md_cache_stop( struct md_syndicate_cache* cache );
int md_cache_destroy( struct md_syndicate_cache* cache );

// asynchronous writes
struct md_cache_block_future* md_cache_write_block_async( struct md_syndicate_cache* cache,
                                                          uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version,
                                                          char* data, size_t data_len,
                                                          uint64_t flags, int* rc );

int md_cache_block_future_wait( struct md_cache_block_future* f );
int md_cache_block_future_free( struct md_cache_block_future* f );
int md_cache_block_future_release_fd( struct md_cache_block_future* f );
char* md_cache_block_future_release_data( struct md_cache_block_future* f );
int md_cache_block_future_unshare_data( struct md_cache_block_future* f );

// flushes
int md_cache_flush_write( struct md_cache_block_future* f );
int md_cache_flush_writes( vector<struct md_cache_block_future*>* futs );

// synchronous block I/O
int md_cache_is_block_readable( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
int md_cache_open_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int flags );
ssize_t md_cache_read_block( int block_fd, char** buf );

int md_cache_stat_block_by_id( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct stat* sb );

// allow external client to evict data
int md_cache_evict_file( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version );
int md_cache_evict_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
int md_cache_evict_block_async( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// allow external client to promote data in the cache (i.e. move it up the LRU)
int md_cache_promote_block( struct md_syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// allow external client to reversion a file 
int md_cache_reversion_file( struct md_syndicate_cache* cache, uint64_t file_id, int64_t old_file_version, int64_t new_file_version );

// allow external client to scan a file's cached blocks
int md_cache_file_blocks_apply( char const* local_path, int (*block_func)( char const*, void* ), void* cls );

// check a cache write future for I/O errors 
int md_cache_block_future_has_error( struct md_cache_block_future* f );
int md_cache_block_future_get_aio_error( struct md_cache_block_future* f );
int md_cache_block_future_get_write_error( struct md_cache_block_future* f );

// getters 
int md_cache_block_future_get_fd( struct md_cache_block_future* f );
uint64_t md_cache_block_future_file_id( struct md_cache_block_future* fut );
int64_t md_cache_block_future_file_version( struct md_cache_block_future* fut );
uint64_t md_cache_block_future_block_id( struct md_cache_block_future* fut );
int64_t md_cache_block_future_block_version( struct md_cache_block_future* fut );

// memory management 
int md_cache_block_future_free_all( vector<struct md_cache_block_future*>* futs, bool close_fds );

}

#endif