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

// GET operations
int SG_client_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_manifest* manifest );
int SG_client_get_block_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_get_block_finish( struct SG_gateway* gateway, struct SG_manifest* manifest, struct md_download_context* dlctx, uint64_t* block_id, struct SG_chunk* block );

// signed blocks
int SG_client_serialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block_in, struct SG_chunk* block_out );
int SG_client_deserialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_chunk* block_in, struct SG_chunk* block_out );

// request prep 
int SG_client_request_WRITE_setup( struct SG_gateway* gateway, SG_messages::Request* request, char const* fs_path, struct SG_manifest* write_delta, uint64_t* new_owner, mode_t* new_mode, struct timespec* mtime );
int SG_client_request_TRUNCATE_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, off_t new_size );
int SG_client_request_RENAME_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* new_path );
int SG_client_request_DETACH_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, ms::ms_reply* vacuum_ticket );
int SG_client_request_PUTBLOCK_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* block_info );
int SG_client_request_DELETEBLOCK_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* block_info, ms::ms_reply* vacuum_ticket );

// POST message 
int SG_client_request_send( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, SG_messages::Reply* reply );
int SG_client_request_send_async( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_request_send_cached_async( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_request_send_finish( struct SG_gateway* gateway, struct md_download_context* dlctx, SG_messages::Reply* reply );

// certificates 
int SG_client_cert_manifest_download( struct SG_gateway* gateway, uint64_t cert_version, struct SG_manifest* manifest );
int SG_client_cert_download_async( struct SG_gateway* gateway, struct SG_manifest* cert_manifest, uint64_t gateway_id, struct md_download_loop* dlloop, struct md_download_context* dlctx );
int SG_client_cert_download_finish( struct SG_gateway* gateway, struct md_download_context* dlctx, uint64_t* cert_gateway_id, struct ms_gateway_cert* cert );

// low-level download logic 
int SG_client_download_async_start( struct SG_gateway* gateway, struct md_download_loop* dlloop, struct md_download_context* dlctx, uint64_t chunk_id, char* url, off_t max_size, void* cls );
int SG_client_download_async_wait( struct md_download_context* dlctx, char** chunk_buf, off_t* chunk_len, void** cls );
void SG_client_download_async_cleanup( struct md_download_context* dlctx );
void SG_client_download_async_cleanup_loop( struct md_download_loop* dlloop );

// errors
bool SG_client_request_is_remote_unavailable( int error );

}

#endif 