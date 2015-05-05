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

#ifndef _UG_REPLICATION_H_
#define _UG_REPLICATION_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/client.h>
#include <libsyndicate/ms/file.h>
#include <libsyndicate/ms/vacuum.h>

#include "inode.h"

struct UG_state;

// snapshot of inode fields needed for replication and garbage collection 
struct UG_replica_context {
   
   char* fs_path;                       // path to the file to replicate
   
   struct md_entry inode_data;          // exported inode
   
   struct SG_manifest manifest;         // exported manifest to replicate
   
   UG_dirty_block_map_t* blocks;        // blocks to replicate
   
   uint64_t* affected_blocks;           // IDs of the blocks affected by this replica (to be sent to the MS)
   size_t num_affected_blocks;          // length of affected_blocks 
   
   struct UG_block_gateway_pair* chunk_queue;   // set of {gateway IDs} X {block IDs}
   size_t chunk_queue_len;              
   
   bool flushed_blocks;                 // if true, then the blocks have all been flushed to disk and can be replicated 
   
   bool sent_vacuum_log;                // if true, then we've told the MS about the manifest and blocks we're about to replicate 
   
   bool replicated_blocks;              // if true, then we've replicated blocks and manifests
   
   bool sent_ms_update;                 // if true, then we've sent the new inode metadata to the MS
};

// context setup/teardown
int UG_replica_context_init( struct UG_replica_context* rctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* manifest, struct timespec* old_manifest_timestamp, UG_dirty_block_map_t* flushed_blocks );
int UG_replica_context_free( struct UG_replica_context* rctx );

// send a manifest and a of dirty blocks to a given gateway
int UG_replicate( struct SG_gateway* gateway, struct UG_replica_context* rctx );

// setters 
UG_dirty_block_map_t* UG_replica_context_release_blocks( struct UG_replica_context* rctx );

#endif
