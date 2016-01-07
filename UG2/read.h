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

#ifndef _UG_READ_H_
#define _UG_READ_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/client.h>

#include "block.h"
#include "inode.h"
#include "core.h"

extern "C" {
   
// set up a request for a block
int UG_read_setup_block_buffer( struct UG_inode* inode, uint64_t block_id, char* buf, uint64_t buf_len, UG_dirty_block_map_t* blocks );

// set up a request for unaligned blocks
int UG_read_unaligned_setup( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, size_t buf_len, off_t offset, UG_dirty_block_map_t* dirty_blocks, uint64_t* head_len, uint64_t* tail_len );

// set up a request for aligned blocks 
int UG_read_aligned_setup( struct UG_inode* inode, char* buf, size_t buf_len, off_t offset, uint64_t block_size, UG_dirty_block_map_t* dirty_blocks );

// get list of gateways to download from, starting with the coordinator
int UG_read_download_gateway_list( struct SG_gateway* gateway, uint64_t coordinator_id, uint64_t** gateway_ids, size_t* num_gateway_ids );

// read dirty blocks in RAM
int UG_read_dirty_blocks( struct SG_gateway* gateway, struct UG_inode* inode, UG_dirty_block_map_t* blocks, struct SG_manifest* absent );

// read locally-cached blocks 
int UG_read_blocks_local( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks, uint64_t offset, uint64_t len, struct SG_manifest* blocks_not_local );

// read individual blocks at once 
int UG_read_blocks( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* blocks, uint64_t offset, uint64_t len );

// read callback to fskit 
int UG_read_impl( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data );

}

#endif 
