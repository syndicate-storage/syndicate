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

#ifndef _SYNC_H_
#define _SYNC_H_

#include "replication.h"
#include "fs_entry.h"

#define SYNC_SUCCESS 0
#define SYNC_WAIT    1
#define SYNC_NOTHING 2

// synchronization context for a file.
// if the file is local, we:
//   * replicate blocks and manifests
//   * update the metadata on the MS
// if the file is remote, and the coordinator is online, we:
//   * replicate the blocks
//   * send the coordinator the block vector
// if the file is remote, but the coordinator has gone offline, we:
//   * replicate the blocks
//   * become the coordinator (i.e. once we discover the coordinator is offline)
//   * replicate the manifest
//   * send the metadata to the MS
// On receiving the new metadata, the coordinator replicates the manifest and metadata synchronously before acknowledging.
//
// Synchronization is a multi-step process, but the steps must go in order relative to the program order.
// If we replicate the block-set A before block-set B for file F, then we must replicate the metadata for block-set A before block-set B.
// Otherwise, the blocks available from the RGs might not match the metadata in the MS.
// To ensure this, we queue synchronization contexts, such that thread B yields to thread A if A's block-set replicated first (even if A went to sleep, and B wants to replicate metadata after its blocks).
struct sync_context {
   struct md_entry md_snapshot;                 // metadata to send to the MS
   struct replica_snapshot* fent_snapshot;      // snapshot of fs_entry's metadata fields
   
   modification_map* dirty_blocks;              // blocks that will be replicated
   modification_map* garbage_blocks;            // blocks that will be garbage-collected
   
   replica_list_t* replica_futures;             // blocks being replicated
   struct replica_context* manifest_fut;        // manifest future (NULL if not used).  Points to a future in replica_futures, if set
   
   sem_t sem;   // ensures proper ordering of block/metadata replication (see below)
};

// flush/resync data
int fs_entry_fsync( struct fs_core* core, struct fs_file_handle* fh );
int fs_entry_fdatasync( struct fs_core* core, struct fs_file_handle* fh );

// sync metadata 
int fs_entry_send_metadata_update( struct fs_core* core, struct fs_file_handle* fh );

// steps for syncing data; safe so long as fent remains locked throughout the begin and end operations.  Used in truncate()
int fs_entry_sync_data_begin( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t parent_id, char const* parent_name, struct sync_context* _sync_ctx );
int fs_entry_sync_data_finish( struct fs_core* core, struct sync_context* sync_ctx );

// undo a data sync (i.e. if it partially failed)
int fs_entry_sync_data_revert( struct fs_core* core, struct fs_entry* fent, struct sync_context* sync_ctx );

// stages of fsync that can be used without locking, since we guarantee that fsync A will complete before fsync B if A started before B.
// used in close() and fsync()
int fs_entry_fsync_begin_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx );
int fs_entry_fsync_end_data( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx, int begin_rc );
int fs_entry_fsync_metadata( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx );
int fs_entry_fsync_garbage_collect( struct fs_core* core, struct fs_entry* fent, struct sync_context* sync_ctx, bool gc_manifest );

int fs_entry_fsync_locked( struct fs_core* core, struct fs_file_handle* fh, struct sync_context* sync_ctx );

int fs_entry_sync_context_free( struct sync_context* sync_ctx );

#endif
