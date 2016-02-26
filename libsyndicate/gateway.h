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

#ifndef _LIBSYNDICATE_GATEWAY_H_
#define _LIBSYNDICATE_GATEWAY_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/manifest.h"
#include "libsyndicate/driver.h"
#include "libsyndicate/cache.h"
#include "libsyndicate/workqueue.h"
#include "libsyndicate/ms/core.h"

// I/O hints for gateway requests
struct SG_IO_hints {
   int io_type;             // none, read, write, trunc
   uint64_t io_context;     // unique identifier that is consistent across a series of related reads or writes
   uint64_t offset;         // logical offset of the read/write
   uint64_t len;            // logical length of the read/write
};

// values for SG_IO_hints.io_type 
#define SG_IO_NONE  SG_messages::DriverRequest::NONE
#define SG_IO_READ  SG_messages::DriverRequest::READ
#define SG_IO_WRITE SG_messages::DriverRequest::WRITE
#define SG_IO_SYNC  SG_messages::DriverRequest::SYNC
#define SG_IO_DELETE SG_messages::DriverRequest::DELETE

// gateway request structure, for a block or a manifest or xattr info
struct SG_request_data {
   uint64_t user_id;                            // ID of the user running the requesting gateway
   uint64_t volume_id;                          // volume ID
   uint64_t file_id;                            // file ID (inode number)
   uint64_t coordinator_id;                     // gateway coordinating writes for this file 
   char* fs_path;                               // path to the file
   int64_t file_version;                        // file version 
   
   // if a block request...
   uint64_t block_id;                           // block ID                     
   int64_t block_version;                       // block version 
   
   // if a manifest request...
   struct timespec manifest_timestamp;          // manifest timestamp 
   
   // set to true if an xattr request 
   bool getxattr;
   bool listxattr;
   bool setxattr;
   bool removexattr;
   
   // getxattr/setxattr/removexattr
   char* xattr_name;
   char* xattr_value;
   size_t xattr_value_len;
   int64_t xattr_nonce;
   
   // internal hints to be given to the driver
   uint64_t io_thread_id;                        // I/O worker thread id handling this request
   struct SG_IO_hints io_hints;                  // I/O hints to be passed along to the driver

   // ID of the requesting gateway (optional) 
   uint64_t src_gateway_id;
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
   struct SG_driver* driver;            // gateway driver
   struct ms_client* ms;                // MS client
   struct md_syndicate_cache* cache;    // block and manifest cache
   struct md_HTTP* http;                // HTTP server
   struct md_downloader* dl;            // downloader
   struct md_wq* iowqs;                 // server I/O work queues
   int num_iowqs;                       // number of I/O work queues
   
   volatile bool running;               // set to true once brought up
   
   sem_t config_sem;                    // for signaling volume/cert reloads
   
   int first_arg_optind;                // index into argv of the first non-argument option
   bool foreground;                     // whether or not we'll run in the foreground
    
   // gateway init/shutdown 
   int (*impl_setup)( struct SG_gateway*, void** );
   void (*impl_shutdown)( struct SG_gateway*, void* );

   // connect to network caches 
   int (*impl_connect_cache)( struct SG_gateway*, CURL*, char const*, void* );
   
   // stat an inode (for the server to know whether or not it can serve a file)
   int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* );
   
   // stat a block inode (for the server to know whether or not it can serve a file)
   int (*impl_stat_block)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* );

   // truncate file to a new size
   int (*impl_truncate)( struct SG_gateway*, struct SG_request_data*, uint64_t, void* );
   
   // rename a file 
   int (*impl_rename)( struct SG_gateway*, struct SG_request_data*, char const*, void* );
   
   // delete a file 
   int (*impl_detach)( struct SG_gateway*, struct SG_request_data*, void* );

   // serialize a chunk
   int (*impl_serialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

   // deserialize a chunk 
   int (*impl_deserialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* );

   // get a block (on cache miss)
   // if it returns -ENOENT, the gateway responds with HTTP 404 to signal EOF
   int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t hints, void* );
   
   // put a block 
   int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t hints, void* );
   
   // delete a block
   int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* );
   
   // get manifest (on cache miss)
   int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, uint64_t hints, void* );
   
   // put manifest 
   int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t hints, void* );
   
   // patch (update) a manifest
   int (*impl_patch_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* );
   
   // delete manifest 
   int (*impl_delete_manifest)( struct SG_gateway*, struct SG_request_data*, void* );
   
   // get xattr
   int (*impl_getxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* );
   
   // list xattrs 
   int (*impl_listxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk** xattr_names, size_t* num_xattrs, void* );
   
   // set xattr 
   int (*impl_setxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* );
   
   // remove xattr 
   int (*impl_removexattr)( struct SG_gateway*, struct SG_request_data*, void* );
   
   // config change 
   int (*impl_config_change)( struct SG_gateway*, int, void* );
};

extern "C" {

// lifecycle
struct SG_gateway* SG_gateway_new(void);
int SG_gateway_init( struct SG_gateway* gateway, uint64_t gateway_type, int argc, char** argv, struct md_opts* overrides );
int SG_gateway_init_opts( struct SG_gateway* gateway, struct md_opts* opts );
int SG_gateway_main( struct SG_gateway* gateway );
int SG_gateway_signal_main( struct SG_gateway* gateway );
int SG_gateway_shutdown( struct SG_gateway* gateway );
int SG_gateway_start_reload( struct SG_gateway* gateway );

// programming a more specific gateway
void SG_impl_setup( struct SG_gateway* gateway, int (*impl_setup)( struct SG_gateway*, void** ) );
void SG_impl_shutdown( struct SG_gateway* gateway, void (*impl_shutdown)( struct SG_gateway*, void* ) );
void SG_impl_connect_cache( struct SG_gateway* gateway, int (*impl_connect_cache)( struct SG_gateway*, CURL*, char const*, void* ) );
void SG_impl_stat( struct SG_gateway* gateway, int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) );
void SG_impl_stat_block( struct SG_gateway* gateway, int (*impl_stat_block)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) );
void SG_impl_truncate( struct SG_gateway* gateway, int (*impl_truncate)( struct SG_gateway*, struct SG_request_data*, uint64_t, void* ) );
void SG_impl_rename( struct SG_gateway* gateway, int (*impl_rename)( struct SG_gateway*, struct SG_request_data*, char const*, void* ) );
void SG_impl_serialize( struct SG_gateway* gateway, int (*impl_serialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* ) );
void SG_impl_deserialize( struct SG_gateway* gateway, int (*impl_deserialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* ) );
void SG_impl_detach( struct SG_gateway* gateway, int (*impl_detach)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_get_block( struct SG_gateway* gateway, int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) );
void SG_impl_put_block( struct SG_gateway* gateway, int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) );
void SG_impl_delete_block( struct SG_gateway* gateway, int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_get_manifest( struct SG_gateway* gateway, int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, uint64_t, void* ) );
void SG_impl_put_manifest( struct SG_gateway* gateway, int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) );
void SG_impl_patch_manifest( struct SG_gateway* gateway, int (*impl_patch_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) );
void SG_impl_delete_manifest( struct SG_gateway* gateway, int (*impl_delete_manifest)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_getxattr( struct SG_gateway* gateway, int (*impl_getxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) );
void SG_impl_listxattr( struct SG_gateway* gateway, int (*impl_listxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk**, size_t*, void* ) );
void SG_impl_setxattr( struct SG_gateway* gateway, int (*impl_setxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) );
void SG_impl_removexattr( struct SG_gateway* gateway, int (*impl_removexattr)( struct SG_gateway*, struct SG_request_data*, void* ) );
void SG_impl_config_change( struct SG_gateway* gateway, int (*impl_config_change)( struct SG_gateway*, int, void* ) );

// request parsing
int SG_request_data_init( struct SG_request_data* reqdat );
int SG_request_data_init_common( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, struct SG_request_data* reqdat );
int SG_request_data_init_block( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct SG_request_data* reqdat );
int SG_request_data_init_manifest( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, struct SG_request_data* reqdat );
int SG_request_data_init_setxattr( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t xattr_nonce, char const* name, char const* value, size_t value_len, struct SG_request_data* reqdat );
int SG_request_data_init_removexattr( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t xattr_nonce, char const* name, struct SG_request_data* reqdat );
int SG_request_data_parse( struct SG_request_data* reqdat, char const* url_path );
int SG_request_data_dup( struct SG_request_data* dest, struct SG_request_data* src );
bool SG_request_is_block( struct SG_request_data* reqdat );
bool SG_request_is_manifest( struct SG_request_data* reqdat );
bool SG_request_is_getxattr( struct SG_request_data* reqdat );
bool SG_request_is_listxattr( struct SG_request_data* reqdat );
void SG_request_data_free( struct SG_request_data* reqdat );

// I/O hints
int SG_IO_hints_init( struct SG_IO_hints* io_hints, int io_type, uint64_t offset, uint64_t len );
int SG_request_data_get_IO_hints( struct SG_request_data* gateway, struct SG_IO_hints* hints );
int SG_request_data_set_IO_hints( struct SG_request_data* gateway, struct SG_IO_hints* hints );

// getters for gateway fields 
void* SG_gateway_cls( struct SG_gateway* gateway );
void SG_gateway_set_cls( struct SG_gateway* gateway, void* cls );
struct md_syndicate_conf* SG_gateway_conf( struct SG_gateway* gateway );
struct SG_driver* SG_gateway_driver( struct SG_gateway* gateway );
struct ms_client* SG_gateway_ms( struct SG_gateway* gateway );
struct md_syndicate_cache* SG_gateway_cache( struct SG_gateway* gateway );
struct md_HTTP* SG_gateway_HTTP( struct SG_gateway* gateway );
struct md_downloader* SG_gateway_dl( struct SG_gateway* gateway );
bool SG_gateway_running( struct SG_gateway* gateway );
uint64_t SG_gateway_id( struct SG_gateway* gateway );
uint64_t SG_gateway_user_id( struct SG_gateway* gateway );
EVP_PKEY* SG_gateway_public_key( struct SG_gateway* gateway );
EVP_PKEY* SG_gateway_private_key( struct SG_gateway* gateway );
int SG_gateway_first_arg_optind( struct SG_gateway* gateway );
bool SG_gateway_foreground( struct SG_gateway* gateway );

// block memory management 
void SG_chunk_init( struct SG_chunk* chunk, char* data, off_t len );
int SG_chunk_dup( struct SG_chunk* dest, struct SG_chunk* src );
int SG_chunk_copy( struct SG_chunk* dest, struct SG_chunk* src );
int SG_chunk_copy_or_dup( struct SG_chunk* dest, struct SG_chunk* src );
void SG_chunk_free( struct SG_chunk* chunk );

// driver methods 
int SG_gateway_driver_init( struct SG_gateway* gateway, struct SG_driver* driver );

// driver accessors
int SG_gateway_driver_get_config_text( struct SG_gateway* gateway, struct SG_chunk* config_data );
int SG_gateway_driver_get_mlocked_secrets_text( struct SG_gateway* gateway, struct SG_chunk* secrets_data );
int SG_gateway_driver_get_driver_text( struct SG_gateway* gateway, struct SG_chunk* driver_data );
int SG_gateway_driver_get_data( struct SG_gateway* gateway, char* data_name, struct SG_chunk* data );

// get blocks from the cache
int SG_gateway_cached_block_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_block );

// get manifests from the cache
int SG_gateway_cached_manifest_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest );

// put blocks to the cache
int SG_gateway_cached_block_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_block, uint64_t cache_flags, struct md_cache_block_future** block_fut );

// put manifests to the cache 
int SG_gateway_cached_manifest_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest, uint64_t cache_flags, struct md_cache_block_future** block_fut );

// run an I/O request 
int SG_gateway_io_start( struct SG_gateway* gateway, struct md_wreq* wreq );

// implementation 
int SG_gateway_impl_connect_cache( struct SG_gateway* gateway, CURL* curl, char const* url );
int SG_gateway_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* mode );
int SG_gateway_impl_stat_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* mode );
int SG_gateway_impl_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size );
int SG_gateway_impl_rename( struct SG_gateway* gateway, struct SG_request_data* reqdat, char const* new_path );
int SG_gateway_impl_detach( struct SG_gateway* gateway, struct SG_request_data* reqdat );
int SG_gateway_impl_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk );
int SG_gateway_impl_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk );
int SG_gateway_impl_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints );
int SG_gateway_impl_block_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints );
int SG_gateway_impl_block_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat );
int SG_gateway_impl_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints );
int SG_gateway_impl_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* manifest_chunk, uint64_t hints );
int SG_gateway_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta );
int SG_gateway_impl_manifest_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat );
int SG_gateway_impl_getxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* xattr_value );
int SG_gateway_impl_listxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk** xattr_names, size_t* num_xattrs );
int SG_gateway_impl_setxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* xattr_value );
int SG_gateway_impl_removexattr( struct SG_gateway* gateway, struct SG_request_data* reqdat );

// misc 
int SG_request_data_from_md_entry( struct SG_request_data* reqdat, char const* fs_path, struct md_entry* ent, uint64_t block_id, int64_t block_version );

}

#endif
