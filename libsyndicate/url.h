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

#ifndef _LIBSYNDICATE_URL_H_
#define _LIBSYNDICATE_URL_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/ms/ms-client.h"

// URLs to manifest data in this UG
char* md_url_local_block_url( char const* data_root, uint64_t volume_id, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* md_url_public_block_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// URLs to block data in other gateways
char* md_url_UG_block_url( struct ms_client* ms, uint64_t UG_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* md_url_AG_block_url( struct ms_client* ms, uint64_t AG_id, char const* fs_path, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version );
char* md_url_RG_block_url( struct ms_client* ms, uint64_t RG_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version );

// generate a URL to a block
int md_url_make_block_url( struct ms_client* ms, char const* fs_path, uint64_t gateway_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version, char** url );

// URLs to file data in this gateway
char* md_url_local_file_url( char const* data_root, uint64_t volume_id, uint64_t file_id, int64_t file_version );
char* md_url_public_file_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version );

// URLs to manifest data in this gateway
char* md_url_public_manifest_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts );

// URLs to manifests in other gateways
char* md_url_UG_manifest_url( struct ms_client* ms, uint64_t ug_id, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts );
char* md_url_RG_manifest_url( struct ms_client* ms, uint64_t rg_id, uint64_t file_id, int64_t file_version, struct timespec* ts );
char* md_url_AG_manifest_url( struct ms_client* ms, uint64_t ag_id, char const* fs_path, uint64_t file_id, int64_t file_version, struct timespec* ts );

// generate a URL to a manifest
int md_url_make_manifest_url( struct ms_client* ms, char const* fs_path, uint64_t gateway_id, uint64_t file_id, int64_t file_version, struct timespec* ts, char** url );

// make a URL to the gateway's API server
int md_url_make_gateway_url( struct ms_client* client, uint64_t gateway_id, char** url );

#endif 
