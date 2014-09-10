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

#ifndef _AG_CACHE_H_
#define _AG_CACHE_H_

#include "libsyndicate/cache.h"

#include "AG.h"

#define AG_CACHE_DEFAULT_SOFT_LIMIT 50000000L    // 50MB
#define AG_CACHE_DEFAULT_HARD_LIMIT 100000000L   // 100MB

struct AG_state;
struct AG_driver_publish_info;

int AG_cache_get_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version, char** block, size_t* block_len );
int AG_cache_promote_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version );
int AG_cache_put_block_async( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version, char* block, size_t block_len );
int AG_cache_evict_block( struct AG_state* state, char const* path, int64_t file_version, uint64_t block_id, int64_t block_version );

char* AG_cache_stat_path( char const* path );
uint64_t AG_cache_file_id( char const* path );

int AG_cache_get_stat( struct AG_state* state, char const* path, int64_t file_version, struct AG_driver_publish_info* pubinfo );
int AG_cache_promote_stat( struct AG_state* state, char const* path, int64_t file_version );
int AG_cache_put_stat_async( struct AG_state* state, char const* path, int64_t file_version, struct AG_driver_publish_info* pubinfo );
int AG_cache_evict_stat( struct AG_state* state, char const* path, int64_t file_version );

int AG_cache_evict_file( struct AG_state* state, char const* path, int64_t file_version );

#endif 