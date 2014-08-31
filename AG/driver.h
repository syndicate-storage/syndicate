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

#ifndef _AG_DRIVER_H_
#define _AG_DRIVER_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/closure.h"

#include "AG.h"

// prototype this 
struct AG_connection_context;

// types of callbacks
typedef int (*AG_driver_init_callback_t)( void** );
typedef int (*AG_driver_shutdown_callback_t)( void* );
typedef int (*AG_get_manifest_callback_t)( struct AG_connection_context*, struct AG_driver_publish_info*, void* );
typedef ssize_t (*AG_get_block_callback_t)( struct AG_connection_context*, uint64_t, char*, size_t, void* );
typedef int (*AG_connect_block_callback_t)( struct AG_connection_context*, void*, void** );
typedef int (*AG_connect_manifest_callback_t)( struct AG_connection_context*, void*, void** );
typedef int (*AG_cleanup_block_callback_t)( void* );
typedef int (*AG_cleanup_manifest_callback_t)( void* );
typedef int (*AG_stat_dataset_callback_t)( char const*, struct AG_map_info*, struct AG_driver_publish_info*, void* );
typedef int (*AG_reversion_callback_t)( char const*, struct AG_map_info*, void* );
typedef int (*AG_driver_event_callback_t)( char*, size_t, void* );
typedef char* (*AG_query_type_callback_t)(void);

// all the driver methods for a particular type of query
struct AG_driver {
   
   struct md_closure* closure;  // libsyndicate closure structure 
   
   char* query_type;            // which kind of query this driver handles
   void* driver_state;          // driver-specific state
   
   AG_driver_init_callback_t            driver_init_callback;
   AG_driver_shutdown_callback_t        driver_shutdown_callback;
   AG_get_manifest_callback_t           get_manifest_callback;
   AG_get_block_callback_t              get_block_callback;
   AG_connect_block_callback_t          connect_block_callback;
   AG_connect_manifest_callback_t       connect_manifest_callback;
   AG_cleanup_block_callback_t          cleanup_block_callback;
   AG_cleanup_manifest_callback_t       clenaup_manifst_callback;
   AG_stat_dataset_callback_t           stat_callback;
   AG_reversion_callback_t              reversion_callback;
   AG_query_type_callback_t             query_type_callback;
};

// information we need for publishing a dataset 
struct AG_driver_publish_info {
   off_t size;          // size of the dataset (or -1 if unbound)
   int64_t mtime_sec;   // modification time, seconds 
   int32_t mtime_nsec;  // modification time, nanoseconds
};

int AG_load_driver( struct md_syndicate_conf* conf, struct AG_driver* driver, char const* driver_path );
int AG_unload_driver( struct AG_driver* driver );
struct AG_driver* AG_lookup_driver( AG_driver_map_t* driver_map, char const* driver_query_type );
struct AG_driver* AG_driver_find_by_path( AG_fs_map_t* fs_map, char const* path, AG_driver_map_t* drivers, int* rc );

int AG_load_drivers( struct md_syndicate_conf* conf, AG_driver_map_t* driver_map, char const* driver_dir );
int AG_shutdown_drivers( AG_driver_map_t* driver_map );

// driver method wrappers
int AG_driver_init( struct AG_driver* driver );
int AG_driver_shutdown( struct AG_driver* driver );
int AG_driver_connect_block( struct AG_driver* driver, struct AG_connection_context* ctx );
int AG_driver_connect_manifest( struct AG_driver* driver, struct AG_connection_context* ctx );
ssize_t AG_driver_get_block( struct AG_driver* driver, struct AG_connection_context* ctx, uint64_t block_id, char* block_buf, size_t block_buf_len );
int AG_driver_get_manifest( struct AG_driver* driver, struct AG_connection_context* ctx, struct AG_driver_publish_info* pub_info );
int AG_driver_cleanup_block( struct AG_driver* driver, struct AG_connection_context* ctx );
int AG_driver_cleanup_manifest( struct AG_driver* driver, struct AG_connection_context* ctx );
int AG_driver_stat( struct AG_driver* driver, char const* path, struct AG_map_info* info, struct AG_driver_publish_info* pub_info );
int AG_driver_reversion( struct AG_driver* driver, char const* path, struct AG_map_info* info );
char* AG_driver_get_query_type( struct AG_driver* driver );
int AG_driver_handle_event( struct AG_driver* driver, char* event_payload, size_t payload_len );

extern "C" {

// stable driver-callable API 
   
// get config information loaded from the specfile
char* AG_driver_get_config_var( char const* config_varname );

// get information about the request
char* AG_driver_get_request_path( struct AG_connection_context* ctx );
char* AG_driver_get_query_string( struct AG_connection_context* ctx );
int64_t AG_driver_get_request_file_version( struct AG_connection_context* ctx );
uint64_t AG_driver_get_request_block_id( struct AG_connection_context* ctx );
int64_t AG_driver_get_request_block_version( struct AG_connection_context* ctx );
uint64_t AG_driver_get_block_size(void);

// set the connection's HTTP status
void AG_driver_set_HTTP_status( struct AG_connection_context* ctx, int http_status );

// register a driver signal handler
int AG_driver_set_signal_handler( int signum, sighandler_t sighandler );

// driver-initiated reversion request
int AG_driver_request_reversion( char const* path, struct AG_driver_publish_info* pub_info );

// cache API
// NOTE: the AG already caches driver-given blocks internally.
// This API is to allow the driver to cache additional data.
// In all four methods, "name" must be globally unique, and different from any name in the filesystem
int AG_driver_cache_get_chunk( char const* name, char** chunk, size_t* chunk_len );
int AG_driver_cache_promote_chunk( char const* name );
int AG_driver_cache_put_chunk_async( char const* name, char* chunk, size_t chunk_len );
int AG_driver_cache_evict_chunk( char const* name );

// map info API 
char* AG_driver_map_info_get_query_string( struct AG_map_info* mi );
int64_t AG_driver_map_info_get_file_version( struct AG_map_info* mi );

}

#endif // _AG_DRIVER_H_