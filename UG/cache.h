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


#ifndef _CACHE_H_
#define _CACHE_H_

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
#include "libsyndicate/storage.h"
#include "log.h"
#include "fs.h"
#include "url.h"

#define CACHE_DEFAULT_SOFT_LIMIT        50000000        // 50 MB
#define CACHE_DEFAULT_HARD_LIMIT       100000000        // 100 MB

using namespace std;


struct cache_entry_key {
   uint64_t file_id;
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
};

// "lexigraphic" comparison between cache_entry_keys
bool cache_entry_key_comp_func( const struct cache_entry_key& c1, const struct cache_entry_key& c2 );

struct cache_entry_key_comp {
   
   bool operator()( const struct cache_entry_key& c1, const struct cache_entry_key& c2 ) {
      return cache_entry_key_comp_func( c1, c2 );
   }
   
   // equality test
   static bool equal( const struct cache_entry_key& c1, const struct cache_entry_key& c2 ) {
      return c1.file_id == c2.file_id && c1.file_version == c2.file_version && c1.block_id == c2.block_id && c1.block_version == c2.block_version;
   }
};

// ongoing cache write for a file
struct cache_block_future {
   struct cache_entry_key key;
   
   char* block_data;
   size_t data_len;
   
   int block_fd;
   
   struct aiocb aio;
   int aio_rc;
   int write_rc;
   
   sem_t sem_ongoing;
   bool detached;       // if true, reap this future once the write finishes
};


typedef list<struct cache_block_future*> block_buffer_t;
typedef block_buffer_t completion_buffer_t;
typedef set<struct cache_block_future*> ongoing_writes_t;
typedef list<struct cache_entry_key> cache_lru_t;

struct syndicate_cache {
   // size limits (in blocks, not bytes!)
   size_t hard_max_size;
   size_t soft_max_size;
   
   int num_blocks_written;                  // how many blocks have been successfully written to disk?
   
   // data to cache that is scheduled to be written to disk 
   block_buffer_t* pending;
   pthread_rwlock_t pending_lock;
   
   // pending refers to one of these...
   block_buffer_t* pending_1;
   block_buffer_t* pending_2;
   
   // data that is being asynchronously written to disk
   ongoing_writes_t* ongoing_writes;
   pthread_rwlock_t ongoing_writes_lock;
   
   // completed writes, to be reaped 
   completion_buffer_t* completed;
   pthread_rwlock_t completed_lock;
   
   // completed refers to one of these
   completion_buffer_t* completed_1;
   completion_buffer_t* completed_2;
   
   // order in which blocks were added
   cache_lru_t* cache_lru;
   pthread_rwlock_t cache_lru_lock;
   
   // blocks to be promoted in the current lru 
   cache_lru_t* promotes;
   pthread_rwlock_t promotes_lock;
   
   // promotes refers to one of these
   cache_lru_t* promotes_1;
   cache_lru_t* promotes_2;
   
   // thread for processing writes and evictions
   pthread_t thread;
   bool running;
   
   // semaphore to block writes once the hard limit is met
   sem_t sem_write_hard_limit;
   
   // semaphore to indicate that there is work to be done
   sem_t sem_blocks_writing;
   
};

// arguments to the main thread 
struct syndicate_cache_thread_args {
   struct fs_core* core;
   struct syndicate_cache* cache;
};

// arguments to the write callback
struct syndicate_cache_aio_write_args {
   struct fs_core* core;
   struct syndicate_cache* cache;
   struct cache_block_future* future;
};

int fs_entry_cache_init( struct fs_core* core, struct syndicate_cache* cache, size_t soft_max_size, size_t hard_max_size );
int fs_entry_cache_destroy( struct syndicate_cache* cache );

// asynchronous writes
struct cache_block_future* fs_entry_cache_write_block_async( struct fs_core* core, struct syndicate_cache* cache,
                                                             uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version,
                                                             char* data, size_t data_len,
                                                             bool detached );

int fs_entry_cache_block_future_wait( struct cache_block_future* f );
int fs_entry_cache_block_future_free( struct cache_block_future* f );
int fs_entry_cache_block_future_release_fd( struct cache_block_future* f );

// synchronous block I/O
int fs_entry_cache_open_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int flags );
ssize_t fs_entry_cache_read_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int block_fd, char* buf, size_t len );

int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct stat* sb );
int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, char const* fs_path, uint64_t block_id, int64_t block_version, struct stat* sb );

// allow external client to evict data
int fs_entry_cache_evict_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version );
int fs_entry_cache_evict_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// allow external client to promote data in the cache (i.e. move it up the LRU)
int fs_entry_cache_promote_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// allow external client to reversion a file 
int fs_entry_cache_reversion_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t old_file_version, int64_t new_file_version );

#endif