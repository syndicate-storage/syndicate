/*
   Copyright 2015 The Trustees of Princeton University

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

// basic syndicate gateway implementation.
// UGs, RGs, and AGs will extend from this.

#ifndef _LIBSYNDICATE_GATEWAY_H_
#define _LIBSYNDICATE_GATEWAY_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/manifest.h"
#include "libsyndicate/closure.h"
#include "libsyndicate/cache.h"
#include "libsyndicate/workqueue.h"
#include "libsyndicate/ms/core.h"

#define SG_CLOSURE_HINT_NO_DELETE 1             // returned by the closure to signal the SG not to delete the resource

#define SG_GATEWAY_HINT_NO_MANIFEST_CACHE       0x1     // manifests are never cached
#define SG_GATEWAY_HINT_NO_BLOCK_CACHE          0x2     // blocks are never cached


// gateway request structure, for a block or a manifest
struct SG_request_data {
   uint64_t volume_id;
   uint64_t file_id;
   uint64_t coordinator_id;
   char* fs_path;
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
   struct timespec manifest_timestamp;
   uint64_t user_id;
};

// gateway chunk of data, with known length
struct SG_chunk {
   
   char* data;
   off_t len;
};

// Syndicte gateway implementation.
// This interface gets implemented by each gateway flavor,
// and allows it to react to other Syndicate gateways.
struct SG_gateway {
   
   void* cls;                           // gateway-specific state
   struct md_syndicate_conf* conf;      // gateway config
   struct md_closure* closure;          // gateway closure
   struct ms_client* ms;                // MS client
   struct md_syndicate_cache* cache;    // block and manifest cache
   struct md_HTTP* http;                // HTTP server
   struct md_downloader* dl;            // downloader
   struct md_wq* iowqs;                 // server I/O work queues
   int num_iowqs;                       // number of I/O work queues
   
   volatile bool running;               // set to true once brought up
   
   uint64_t hints;                      // hints on how to drive the implementation
   
   sem_t config_sem;                    // for signaling volume/cert reloads
   
   int first_arg_optind;                // index into argv of the first non-argument option
    
   // gateway init/shutdown 
   int (*impl_setup)( struct SG_gateway*, void** );
   void (*impl_shutdown)( struct SG_gateway*, void* );
   
   // stat an inode (for the server to know whether or not it can serve a file)
   int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* );
   
   // truncate file to a new size
   int (*impl_truncate)( struct SG_gateway*, struct SG_request_data*, uint64_t, void* );
   
   // rename a file 
   int (*impl_rename)( struct SG_gateway*, struct SG_request_data*, char const*, void* );
   
   // delete a file 
   int (*impl_detach)( struct SG_gateway*, struct SG_request_data*, void* );

   // get a block (on cache miss)
   // if it returns -ENOENT, the gateway responds with HTTP 404 to signal EOF
   int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* );
   
   // put a block 
   int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* );
   
   // delete a block
   int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* );
   
   // get manifest (on cache miss)
   int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* );
   
   // put manifest 
   int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* );
   
   // patch (update) a manifest
   int (*impl_patch_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* );
   
   // delete manifest 
   int (*impl_delete_manifest)( struct SG_gateway*, struct SG_request_data*, void* );
   
   // config change 
   int (*impl_config_change)( struct SG_gateway*, void* );
};

// Syndicate gateway closure method signatures
// connect to the cache, setting up the curl handle with a given URL
typedef int (*SG_closure_connect_cache_func)( struct SG_gateway*, CURL*, char const*, void* );

// transform a block to be put into Syndicate
typedef int (*SG_closure_put_block_func)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

// transform a serialized manifest to be put into Syndicate
typedef int (*SG_closure_put_manifest_func)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

// transform a block to be pulled into Syndicate
typedef ssize_t (*SG_closure_get_block_func)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

// transform a serialized manifest to be pulled into Syndicate
typedef int (*SG_closure_get_manifest_func)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

// do closure-specific things to begin changing a coordinator, before the request to the MS is sent.
typedef int (*SG_closure_chcoord_begin_func)( struct SG_gatewy*, struct SG_request_data*, uint64_t, void* );

// do closure-specific things to finish changing a coordinator, after the request to the MS is sent.
// pass in the MS's chcoord status code (non-zero indicates error), so the closure can act accordingly
typedef int (*SG_closure_chcoord_end_func)( struct SG_gateway*, struct SG_request_data*, int, void* );

// do closure-specific things to delete a block
// if this method returns SG_CLOSURE_HINT_NO_DELETE, then the block won't actually be deleted by the SG
// (but the closure will take full responsibility for doing so)
typedef int (*SG_closure_delete_block_func)( struct SG_gateway*, struct SG_request_data* );

// do closure-specific things to delete a manifest 
// if this method returns SG_CLOSURE_HINT_NO_DELETE, then the manifest won't actually be deleted by the SG
// (but the closure will take full responsibility for doing so)
typedef int (*SG_closure_delete_manifest_func)( struct SG_gateway*, struct SG_request_data* );

// do closure-specific things when a file is created
// if this method returns non-zero, the SG's creat operation will fail 
typedef int (*SG_closure_create_file_func)( struct SG_gateway*, struct SG_request_data* );

// do closure-specific things when a file is deleted 
// if this method returns non-zero, the SG's unlink operation will fail 
typedef int (*SG_closure_delete_file_func)( struct SG_gateway*, struct SG_request_data* );

// return a malloc'ed copy of this closure's name
typedef char* (*SG_closure_get_name_func)( void );


extern "C" {

// lifecycle
int SG_gateway_init( struct SG_gateway* gateway, uint64_t gateway_type, bool anonymous_client, int argc, char** argv );
int SG_gateway_init_opts( struct SG_gateway* gateway, struct md_opts* opts );
int SG_gateway_main( struct SG_gateway* gateway );
int SG_gateway_signal_main( struct SG_gateway* gateway );
int SG_gateway_shutdown( struct SG_gateway* gateway );
int SG_gateway_start_reload( struct SG_gateway* gateway );

// programming 
void SG_impl_setup( struct SG_gateway* gateway, int (*impl_setup)( struct SG_gateway*, void** ) );
void SG_impl_shutdown( struct SG_gateway* gateway, void (*impl_shutdown)( struct SG_gateway*, void* ) );
void SG_impl_stat( struct SG_gateway* gateway, int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) );
void SG_impl_truncate( struct SG_gateway* gateway, int (*impl_truncate)( struct SG_gateway*, struct SG_request_data*, uint64_t, void* ) );
void SG_impl_rename( struct SG_gateway* gateway, int (*impl_rename)( struct SG_gateway*, struct SG_request_data*, char const*, void* ) );
void SG_impl_detach( struct SG_gateway* gateway, int (*impl_detach)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_get_block( struct SG_gateway* gateway, int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) );
void SG_impl_put_block( struct SG_gateway* gateway, int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) );
void SG_impl_delete_block( struct SG_gateway* gateway, int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_get_manifest( struct SG_gateway* gateway, int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) );
void SG_impl_put_manifest( struct SG_gateway* gateway, int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) );
void SG_impl_patch_manifest( struct SG_gateway* gateway, int (*impl_patch_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) );
void SG_impl_delete_manifest( struct SG_gateway* gateway, int (*impl_delete_manifest)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_config_change( struct SG_gateway* gateway, int (*impl_config_change)( struct SG_gateway*, void* ) );

// request parsing
int SG_request_data_init( struct SG_request_data* reqdat );
int SG_request_data_init_block( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct SG_request_data* reqdat );
int SG_request_data_init_manifest( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, struct SG_request_data* reqdat );
int SG_request_data_parse( struct SG_request_data* reqdat, char const* url_path );
int SG_request_data_dup( struct SG_request_data* dest, struct SG_request_data* src );
bool SG_request_is_block( struct SG_request_data* reqdat );
bool SG_request_is_manifest( struct SG_request_data* reqdat );
void SG_request_data_free( struct SG_request_data* reqdat );

// getters for gateway fields 
void* SG_gateway_cls( struct SG_gateway* gateway );
struct md_syndicate_conf* SG_gateway_conf( struct SG_gateway* gateway );
struct md_closure* SG_gateway_closure( struct SG_gateway* gateway );
struct ms_client* SG_gateway_ms( struct SG_gateway* gateway );
struct md_syndicate_cache* SG_gateway_cache( struct SG_gateway* gateway );
struct md_HTTP* SG_gateway_HTTP( struct SG_gateway* gateway );
struct md_downloader* SG_gateway_dl( struct SG_gateway* gateway );
bool SG_gateway_running( struct SG_gateway* gateway );
uint64_t SG_gateway_impl_hints( struct SG_gateway* gateway );
uint64_t SG_gateway_id( struct SG_gateway* gateway );
uint64_t SG_gateway_user_id( struct SG_gateway* gateway );
EVP_PKEY* SG_gateway_private_key( struct SG_gateway* gateway );
int SG_gateway_first_arg_optind( struct SG_gateway* gateway );

// block memory management 
void SG_chunk_init( struct SG_chunk* chunk, char* data, off_t len );
int SG_chunk_dup( struct SG_chunk* dest, struct SG_chunk* src );
int SG_chunk_copy( struct SG_chunk* dest, struct SG_chunk* src );
void SG_chunk_free( struct SG_chunk* chunk );

// closure methods 
ssize_t SG_gateway_closure_get_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_block, struct SG_chunk* out_block );
int SG_gateway_closure_put_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_block, struct SG_chunk* out_block );
int SG_gateway_closure_put_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_manifest, struct SG_chunk* out_manifest );
int SG_gateway_closure_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_manifest, struct SG_chunk* out_manifest );
int SG_gateway_closure_connect_cache( struct SG_gateway* gateway, CURL* curl, char const* url );

// get blocks from the cache
int SG_gateway_cached_block_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_block );
int SG_gateway_cached_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block );

// get manifests from the cache
int SG_gateway_cached_manifest_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest );
int SG_gateway_cached_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* serialized_manifest );

// put blocks to the cache
int SG_gateway_cached_block_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_block, uint64_t cache_flags, struct md_cache_block_future** block_fut );
int SG_gateway_cached_block_put_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t cache_flags, struct md_cache_block_future** block_fut );

// put manifests to the cache 
int SG_gateway_cached_manifest_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest, uint64_t cache_flags, struct md_cache_block_future** block_fut );
int SG_gateway_cached_manifest_put_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* serialized_manifest, uint64_t cache_flags, struct md_cache_block_future** block_fut );

// run an I/O request 
int SG_gateway_io_start( struct SG_gateway* gateway, struct md_wreq* wreq );

// implementation 
int SG_gateway_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* mode );
int SG_gateway_impl_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size );
int SG_gateway_impl_rename( struct SG_gateway* gateway, struct SG_request_data* reqdat, char const* new_path );
int SG_gateway_impl_detach( struct SG_gateway* gateway, struct SG_request_data* reqdat );
int SG_gateway_impl_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block );
int SG_gateway_impl_block_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block );
int SG_gateway_impl_block_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat );
int SG_gateway_impl_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest );
int SG_gateway_impl_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest );
int SG_gateway_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta );
int SG_gateway_impl_manifest_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat );

}

#endif