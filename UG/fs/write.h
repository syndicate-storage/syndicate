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

#ifndef _WRITE_H_
#define _WRITE_H_

#include "fs_entry.h"
#include "consistency.h"
#include "network.h"

ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset );
ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset );

int fs_entry_remote_write( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t coordinator_id, Serialization::WriteMsg* write_msg );

struct cache_block_future* fs_entry_write_block_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_data, size_t len, bool evict_old_block, int* rc );

int fs_entry_revert_write( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, uint64_t original_size,
                           modification_map* new_block_info, modification_map* old_block_info, bool garbage_collect_manifest );

int fs_entry_garbage_collect_overwritten_data( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, modification_map* overwritten_blocks );

struct cache_block_future* fs_entry_write_one_block( struct fs_core* core, struct fs_file_handle* fh, uint64_t block_id, char* block, size_t num_affected_bytes,
                                                     modification_map* modified_blocks, modification_map* overwritten_blocks, int* ret );

int fs_entry_replicate_write( struct fs_core* core, struct fs_file_handle* fh, modification_map* modified_blocks );

int fs_entry_send_metadata_update( struct fs_core* core, struct fs_file_handle* fh );

int fs_entry_remote_write_or_coordinate( struct fs_core* core, struct fs_file_handle* fh, struct replica_snapshot* fent_old_snapshot, uint64_t start_id, uint64_t end_id );

int fs_entry_finish_writes( list<struct cache_block_future*>& block_futures, bool close_fds );

#endif