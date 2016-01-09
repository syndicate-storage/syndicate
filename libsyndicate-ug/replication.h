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

struct UG_replica_context;

// replication hints
#define UG_REPLICA_HINT_NO_MS_UPDATE      0x1
#define UG_REPLICA_HINT_NO_RG_BLOCKS      0x2
#define UG_REPLICA_HINT_NO_MS_VACUUM      0x4

extern "C" {
   
// context setup/teardown
struct UG_replica_context* UG_replica_context_new();

int UG_replica_context_init( struct UG_replica_context* rctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* manifest, UG_dirty_block_map_t* flushed_blocks );
int UG_replica_context_free( struct UG_replica_context* rctx );

// send a manifest and a of dirty blocks to a given gateway
int UG_replicate( struct SG_gateway* gateway, struct UG_replica_context* rctx );

// control replication state 
int UG_replica_context_hint( struct UG_replica_context* rctx, uint64_t flags );

}

#endif
