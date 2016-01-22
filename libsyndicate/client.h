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

// Syndicate Gateway client API 

#ifndef _LIBSYNDICATE_CLIENT_H_
#define _LIBSYNDICATE_CLIENT_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/manifest.h"
#include "libsyndicate/gateway.h"
#include "libsyndicate/download.h"

// maximum length of a gateway reply: 1MB
#define SG_CLIENT_MAX_REPLY_LEN         1024000

extern "C" {
    
// extra data to include in a write
struct SG_client_WRITE_data;

// set up a write reqeust 
struct SG_client_WRITE_data* SG_client_WRITE_data_new(void);
int SG_client_WRITE_data_init( struct SG_client_WRITE_data* dat );
int SG_client_WRITE_data_set_write_delta( struct SG_client_WRITE_data* dat, struct SG_manifest* write_delta );
int SG_client_WRITE_data_set_mtime( struct SG_client_WRITE_data* dat, struct timespec* mtime );
int SG_client_WRITE_data_set_mode( struct SG_client_WRITE_data* dat, mode_t mode );
int SG_client_WRITE_data_set_owner_id( struct SG_client_WRITE_data* dat, uint64_t owner_id );
int SG_client_WRITE_data_set_routing_info( struct SG_client_WRITE_data* dat, uint64_t volume_id, uint64_t coordinator_id, uint64_t file_id, int64_t file_version );
int SG_client_WRITE_data_merge( struct SG_client_WRITE_data* dat, struct md_entry* ent );

// GET operations
int SG_client_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_manifest* manifest );
int SG_client_get_block_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_get_block_finish( struct SG_gateway* gateway, struct SG_manifest* manifest, struct md_download_context* dlctx, uint64_t* block_id, struct SG_chunk* block );
int SG_client_get_block_cleanup_loop( struct md_download_loop* dlloop );
int SG_client_getxattr( struct SG_gateway* gateway, uint64_t gateway_id, char const* fs_path, uint64_t file_id, int64_t file_version, char const* xattr_name, uint64_t xattr_nonce, char** xattr_value, size_t* xattr_len );
int SG_client_listxattrs( struct SG_gateway* gateway, uint64_t gateway_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t xattr_nonce, char** xattr_list, size_t* xattr_list_len );

// signed blocks
int SG_client_serialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block_in, struct SG_chunk* block_out );
int SG_client_deserialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_chunk* block_in, struct SG_chunk* block_out );

// gateway-to-gateway requests.
int SG_client_request_WRITE_setup( struct SG_gateway* gateway, SG_messages::Request* request, char const* fs_path, struct SG_client_WRITE_data* write_data );
int SG_client_request_TRUNCATE_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, off_t new_size );
int SG_client_request_RENAME_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* new_path );
int SG_client_request_DETACH_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat );
int SG_client_request_PUTCHUNKS_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t chunk_info_len );
int SG_client_request_PUTCHUNKS_setup_ex( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t chunk_info_len, bool sign );
int SG_client_request_DELETECHUNKS_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t chunk_info_len );
int SG_client_request_DELETECHUNKS_setup_ex( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t chunk_info_len, bool sign );
int SG_client_request_SETXATTR_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, int flags );
int SG_client_request_REMOVEXATTR_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* xattr_name );

// gateway-to-gateway messaging.  The corresponding driver methods will be run 
int SG_client_request_send( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, SG_messages::Reply* reply );
int SG_client_request_send_async( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_request_send_finish( struct SG_gateway* gateway, struct md_download_context* dlctx, SG_messages::Reply* reply );

// low-level download logic 
int SG_client_download_async_start( struct SG_gateway* gateway, struct md_download_loop* dlloop, struct md_download_context* dlctx, uint64_t chunk_id, char* url, off_t max_size, void* cls );
int SG_client_download_async_wait( struct md_download_context* dlctx, char** chunk_buf, off_t* chunk_len, void** cls );
void SG_client_download_async_cleanup( struct md_download_context* dlctx );
void SG_client_download_async_cleanup_loop( struct md_download_loop* dlloop );

// signed block authentication 
int SG_client_block_sign( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block_data, struct SG_chunk* signed_block_data );
int SG_client_block_verify( struct SG_gateway* gateway, struct SG_chunk* signed_block, uint64_t* ret_data_offset );

// errors
bool SG_client_request_is_remote_unavailable( int error );

}

#endif 
