/*
   Copyright 2013 The Trustees of Princeton University

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

#ifndef _URL_H_
#define _URL_H_

#include "fs_entry.h"

// URLs to manifest data in this UG
char* fs_entry_local_block_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_public_block_url( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );

// URLs to block data in other gateways
char* fs_entry_UG_block_url( struct fs_core* core, uint64_t gateway_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_RG_block_url( struct fs_core* core, uint64_t rg_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version );
char* fs_entry_AG_block_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version );

// generate a URL to a fent's block
int fs_entry_make_block_url( struct fs_core* core, char const* fs_path, uint64_t coordinator_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version, char** url );

// URLs to file data in this UG
char* fs_entry_local_file_url( struct fs_core* core, uint64_t file_id, int64_t file_version );
char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version );

// URLs to manifest data in this UG
char* fs_entry_public_manifest_url( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts );

// URLs to manifests in other gateways
char* fs_entry_UG_manifest_url( struct fs_core* core, uint64_t UG_id, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts );
char* fs_entry_RG_manifest_url( struct fs_core* core, uint64_t RG_id, uint64_t file_id, int64_t file_version, struct timespec* ts );
char* fs_entry_AG_manifest_url( struct fs_core* core, uint64_t AG_id, char const* fs_path, uint64_t file_id, int64_t file_version, struct timespec* ts );

// generate a URL to a fent's manifest
int fs_entry_make_manifest_url( struct fs_core* core, char const* fs_path, uint64_t coordinator_id, uint64_t file_id, int64_t file_version, struct timespec* ts, char** url );

#endif 
