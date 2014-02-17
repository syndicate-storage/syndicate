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

#define MAX_CACHE_FDS 250               // don't starve the rest of the system of file descriptors!

using namespace std;


struct cache_entry_key {
   uint64_t file_id;
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
   size_t data_len;
};

struct cache_entry_key_comp {
   // "lexigraphic" comparison between cache_entry_keys
   bool operator()( const struct cache_entry_key& c1, const struct cache_entry_key& c2 ) {
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
};

// ongoing write
struct cache_write_ctx {
   struct aiocb aio;
   int aio_rc;
   int write_rc;
   
   struct cache_entry_key key;
};

typedef map<struct cache_entry_key, char*, struct cache_entry_key_comp> block_buffer_t;
typedef list<struct cache_write_ctx> completion_buffer_t;
typedef list<struct cache_entry_key> cache_lru_t;

struct syndicate_cache {
   // size limits (in blocks, not bytes!)
   size_t hard_max_size;
   size_t soft_max_size;
   
   int num_aio_writes;                      // how many ongoing aio writes are there?
   int num_blocks_written;                  // how many blocks have been successfully written to disk?
   
   block_buffer_t* pending;             // data to cache that is scheduled to be written to disk 
   pthread_rwlock_t pending_lock;
   
   // pending refers to one of these...
   block_buffer_t* pending_1;
   block_buffer_t* pending_2;
   
   // completed writes, to be reaped 
   completion_buffer_t* completed;
   pthread_rwlock_t completed_lock;
   
   // completed refers to one of these
   completion_buffer_t* completed_1;
   completion_buffer_t* completed_2;
   
   cache_lru_t* cache_lru;              // order in which blocks were added
   pthread_rwlock_t cache_lru_lock;
   
   pthread_t thread;
   bool running;
   
   // semaphore to block writes once the hard limit is met
   sem_t sem_write_hard_limit;
   
   // semaphore to count the number of pending write requests
   sem_t sem_blocks_pending;
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
   struct cache_write_ctx* write;
};

int fs_entry_cache_init( struct fs_core* core, struct syndicate_cache* cache, size_t soft_max_size, size_t hard_max_size );
int fs_entry_cache_destroy( struct syndicate_cache* cache );

// access on-disk cached data 
int fs_entry_cache_write_block_async( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, char* data, size_t data_len );
int fs_entry_cache_open_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int flags );
ssize_t fs_entry_cache_read_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, int block_fd, char* buf, size_t len );

int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct stat* sb );
int fs_entry_cache_stat_block( struct fs_core* core, struct syndicate_cache* cache, char const* fs_path, uint64_t block_id, int64_t block_version, struct stat* sb );

// allow external client to evict data
int fs_entry_cache_evict_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version );
int fs_entry_cache_evict_block( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// allow external client to reversion a file 
int fs_entry_cache_reversion_file( struct fs_core* core, struct syndicate_cache* cache, uint64_t file_id, int64_t old_file_version, int64_t new_file_version );

#endif