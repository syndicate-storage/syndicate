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

#ifndef _UG_WRITE_H_
#define _UG_WRITE_H_

#include <fskit/fskit.h>

#include "block.h"
#include "inode.h"

// write callback to fskit 
int UG_write( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent, char* buf, size_t buf_len, off_t offset, void* handle_data );

// update the local manifest from a remote writer, and replicate it
int UG_write_patch_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct UG_inode* inode, struct SG_manifest* write_delta );

// update write timestamp 
int UG_write_timestamp_update( struct UG_inode* inode, struct timespec ts );

// update write nonce 
int UG_write_nonce_update( struct UG_inode* inode );

// merge dirty blocks into an inode
int UG_write_dirty_blocks_merge( struct SG_gateway* gateway, struct UG_inode* inode, int64_t old_file_version, off_t old_size, uint64_t block_size, UG_dirty_block_map_t* new_dirty_blocks, bool overwrite );

#endif
