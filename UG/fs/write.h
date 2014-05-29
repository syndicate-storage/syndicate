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

struct fs_entry_partial_head {
   char const* buf_ptr;         // pointer to the start of the client's data that applies to this block
   
   uint64_t block_id;           // ID of the affected block
   int64_t block_version;       // (old) version of the affected block
   bool has_old_version;        // whether or not there is an old version of this block
   
   size_t write_offset;         // offset in the block where to start writing
   size_t write_len;            // number of bytes to apply to the block
};

struct fs_entry_partial_tail {
   char const* buf_ptr;         // pointer to the start of the client's data that applies to this block 
   
   uint64_t block_id;           // ID of the affected block 
   int64_t block_version;       // (old) version of the affected block
   bool has_old_version;        // whether ornot there is an old version of this block 
   
   size_t write_len;            // number of bytes to apply to the block
};

struct fs_entry_whole_block {
   char const* buf_ptr;         // pointer to the start of the client's data that applies to this block
   
   uint64_t block_id;           // ID of the affected block 
};

typedef vector<struct fs_entry_whole_block> fs_entry_whole_block_list_t;

// describes a write in terms of which blocks it affects, and how they are affected
struct fs_entry_write_vec {
   struct fs_entry_partial_head head;
   struct fs_entry_partial_tail tail;
   fs_entry_whole_block_list_t* overwritten;
   
   // range of the write
   uint64_t start_block_id;
   uint64_t end_block_id;
   
   // whether or not it has partial heads or tails
   bool has_head;
   bool has_tail;
};

ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset );
ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset );

struct cache_block_future* fs_entry_write_block_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char const* block, size_t block_len,
                                                       struct fs_entry_block_info* binfo_old, struct fs_entry_block_info* binfo_new, int* ret );

int fs_entry_read_partial_blocks( struct fs_core* core, struct fs_file_handle* fh, char const* fs_path, struct fs_entry* fent, struct fs_entry_read_context* read_ctx );

int fs_entry_put_write_holes( struct fs_core* core, struct fs_entry* fent, off_t offset );

int fs_entry_revert_write( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* fent_before_write, uint64_t new_size, modification_map* old_block_info );

int fs_entry_cache_evict_blocks_async( struct fs_core* core, struct fs_entry* fent, modification_map* blocks );

int fs_entry_update_bufferred_block_write( struct fs_core, struct fs_entry* fent, uint64_t block_id, char* block, size_t block_len );

int fs_entry_remote_write( struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t coordinator_id, Serialization::WriteMsg* write_msg );

#endif