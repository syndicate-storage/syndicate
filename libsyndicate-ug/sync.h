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

#ifndef _UG_SYNC_H_
#define _UG_SYNC_H_

#include "libsyndicate/cache.h"
#include "inode.h"
#include "replication.h"
#include "vacuumer.h"

struct UG_sync_context {
   
   struct UG_replica_context* rctx;      // replication information 
   
   struct UG_vacuum_context* vctx;       // vacuum information 
   
   sem_t sem;                           // ensure all calls to sync(2) happen in order
};

extern "C" {
   
// sync blocks to cache   
int UG_sync_blocks_flush_async( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, UG_dirty_block_map_t* dirty_blocks );
int UG_sync_blocks_flush_finish( struct SG_gateway* gateway, struct UG_inode* inode, UG_dirty_block_map_t* dirty_blocks );

// set up and tear down a sync context 
int UG_sync_context_init( struct UG_sync_context* sctx, struct UG_replica_context* rctx );
int UG_sync_context_free( struct UG_sync_context* sctx );

// fskit sync
int UG_sync_fsync_ex( struct fskit_core* core, char const* path, struct fskit_entry* fent );
int UG_sync_fsync( struct fskit_core* core, struct fskit_route_metadata* route_metadata, struct fskit_entry* fent );

}

#endif
