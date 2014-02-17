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

char* fs_entry_block_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, bool local );
char* fs_entry_local_block_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_public_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_remote_block_url( struct fs_core* core, uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_replica_block_url( struct fs_core* core, char* RG_url, uint64_t volume_id, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version );
char* fs_entry_RG_block_url( struct fs_core* core, uint64_t rg_id, uint64_t volume_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version );
char* fs_entry_AG_block_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version );
char* fs_entry_block_url_path( struct fs_core* core, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version );

char* fs_entry_file_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, bool local );
char* fs_entry_local_file_url( struct fs_core* core, uint64_t file_id, int64_t file_version );
char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path, int64_t file_version );
char* fs_entry_manifest_url( struct fs_core* core, char const* gateway_base_url, uint64_t volume_id, char* fs_path, int64_t version, struct timespec* ts );
char* fs_entry_public_manifest_url( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts );
char* fs_entry_remote_manifest_url( struct fs_core* core, uint64_t UG_id, char const* fs_path, int64_t version, struct timespec* ts );
char* fs_entry_replica_manifest_url( struct fs_core* core, char const* RG_url, uint64_t volume_id, uint64_t file_id, int64_t version, struct timespec* ts );
char* fs_entry_manifest_url_path( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts );
char* fs_entry_RG_manifest_url( struct fs_core* core, uint64_t rg_id, uint64_t volume_id, uint64_t file_id, int64_t file_version, struct timespec* ts );
char* fs_entry_AG_manifest_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, int64_t file_version, struct timespec* ts );

#endif 
