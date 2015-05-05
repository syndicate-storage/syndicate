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

#ifndef _UG_INODE_H_
#define _UG_INODE_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/manifest.h>
#include <libsyndicate/util.h>

#include <fskit/fskit.h>

#include "block.h"

// prototype...
struct UG_sync_context;

// queue for threads waiting to synchronize blocks
typedef queue< struct UG_sync_context* > UG_inode_fsync_queue_t;

// map block IDs to their versions, so we know which block to evict on close 
typedef map< uint64_t, int64_t > UG_inode_block_eviction_map_t;

// pair of block_id to gateway_id, for reading, replicating, and vacuuming
struct UG_block_gateway_pair {
   uint64_t gateway_id;
   uint64_t block_id;
};

// UG-specific inode information, for fskit
struct UG_inode {
   
   struct SG_manifest manifest;         // manifest of this file's blocks (includes coordinator_id and file_version)
   
   int64_t ms_write_nonce;      // last-known write nonce from the MS
   int64_t ms_xattr_nonce;      // last-known xattr nonce from the MS
   int64_t generation;          // last-known generation number of this file
   
   int64_t write_nonce;         // uncommited write nonce (initialized to ms_write_nonce; used to indicate dirty data)
   int64_t xattr_nonce;         // uncommitted xattr nonce
   
   struct timespec refresh_time;                // time of last refresh from the ms
   struct timespec manifest_refresh_time;       // time of last manifest refresh
   struct timespec children_refresh_time;       // if this is a directory, this is the time the children were last reloaded
   uint32_t max_read_freshness;         // how long since last refresh, in millis, this inode is to be considered fresh for reading
   uint32_t max_write_freshness;        // how long since last refresh, in millis, this inode is to be considered fresh for writing
   
   bool read_stale;     // if true, this file must be revalidated before the next read
   bool write_stale;    // if true, this file must be revalidated before the next write
   bool dirty;          // if true, then we need to flush data on fsync()
   
   int64_t ms_num_children;     // the number of children the MS says this inode has
   int64_t ms_capacity;         // maximum index number of a child in the MS
   
   bool vacuuming;              // if true, then we're currently vacuuming this file
   bool vacuumed;               // if true, then we've already tried to vacuum this file upon discovery (false means we should try again)
   
   UG_dirty_block_map_t* dirty_blocks;  // set of modified blocks that must be replicated, either on the next fsync() or last close()
   
   struct timespec old_manifest_modtime;// timestamp of the last-replicated manifest (used for vacuuming)
   struct SG_manifest replaced_blocks;  // set of blocks replaced by writes (contains only metadata; used for vacuuming)
   
   UG_inode_fsync_queue_t* sync_queue;  // queue of fsync requests on this inode
   
   struct fskit_entry* entry;           // the fskit entry that owns this inode 
   
   bool renaming;                       // if true, then this inode is in the process of getting renamed.  Concurrent renames will fail with EBUSY
   bool deleting;                       // if true, then this inode is in the process of being deleted.  Concurrent opens and stats will fail
};

#define UG_inode_volume_id( inode ) (inode).manifest.volume_id 
#define UG_inode_coordinator_id( inode ) (inode).manifest.coordinator_id 
#define UG_inode_file_id( inode ) (inode).manifest.file_id 
#define UG_inode_file_version( inode ) (inode).manifest.file_version
#define UG_inode_write_nonce( inode ) (inode).write_nonce
#define UG_inode_xattr_nonce( inode ) (inode).xattr_nonce
#define UG_inode_manifest( inode ) &(inode).manifest
#define UG_inode_replaced_blocks( inode ) &(inode).replaced_blocks
#define UG_inode_dirty_blocks( inode ) (inode).dirty_blocks
#define UG_inode_old_manifest_modtime( inode ) (inode).old_manifest_modtime
#define UG_inode_fskit_entry( inode ) (inode).entry
#define UG_inode_is_read_stale( inode, now ) ((inode).read_stale || md_timespec_diff_ms( now, &(inode).refresh_time ) > (inode).max_read_freshness)
#define UG_inode_renaming( inode ) (inode).renaming
#define UG_inode_deleting( inode ) (inode).deleting
#define UG_inode_ms_num_children( inode ) (inode).ms_num_children
#define UG_inode_ms_capacity( inode ) (inode).ms_capacity
#define UG_inode_max_read_freshness( inode ) (inode).max_read_freshness
#define UG_inode_max_write_freshness( inode ) (inode).max_write_freshness
#define UG_inode_generation( inode ) (inode).generation
#define UG_inode_refresh_time( inode ) (inode).refresh_time
#define UG_inode_manifest_refresh_time( inode ) (inode).manifest_refresh_time
#define UG_inode_sync_queue_len( inode ) (inode)->sync_queue->size()

#define UG_inode_set_write_nonce( inode, wn ) (inode)->write_nonce = wn
#define UG_inode_set_manifest_refresh_time( inode, ts ) (inode)->manifest_refresh_time = *(ts)
#define UG_inode_set_old_manifest_modtime( inode, ts ) (inode)->old_manifest_modtime = *(ts)
#define UG_inode_set_max_read_freshness( inode, rf ) (inode)->max_read_freshness = (rf)
#define UG_inode_set_max_write_freshness( inode, wf ) (inode)->max_write_freshness = (wf)
#define UG_inode_set_read_stale( inode, val ) (inode)->read_stale = (val)

// UG-specific file handle information, for fskit 
struct UG_file_handle {
   
   int flags;                           // open flags
   
   struct UG_inode* inode_ref;          // reference to the parent inode (i.e. so we can release dirty blocks)
   
   struct fskit_file_handle* handle_ref;        // refernece to the parent fskit file handle 
   
   UG_inode_block_eviction_map_t* evicts;       // non-dirty blocks to evict on close 
};


// initialization
int UG_inode_init( struct UG_inode* inode, struct fskit_entry* entry, uint64_t volume_id, uint64_t coordinator_id, int64_t file_version );
int UG_inode_init_from_protobuf( struct UG_inode* inode, struct fskit_entry* entry, ms::ms_entry* msent, SG_messages::Manifest* mmsg );
int UG_inode_init_from_export( struct UG_inode* inode, struct md_entry* inode_data, struct SG_manifest* manifest, struct fskit_entry* fent );

// free 
int UG_inode_free( struct UG_inode* inode );

// set up a file handle 
int UG_file_handle_init( struct UG_file_handle* fh, struct UG_inode* inode, int flags );

// free 
int UG_file_handle_free( struct UG_file_handle* fh );

// export an inode 
int UG_inode_export( struct md_entry* dest, struct UG_inode* src, uint64_t parent_id, char const* parent_name );
int UG_inode_export_fs( struct fskit_core* fs, char const* fs_path, struct md_entry* inode_data );

// sanity checks in import
int UG_inode_export_match_name( struct UG_inode* dest, struct md_entry* src );
int UG_inode_export_match_file_id( struct UG_inode* dest, struct md_entry* src );
int UG_inode_export_match_version( struct UG_inode* dest, struct md_entry* src );
int UG_inode_export_match_size( struct UG_inode* dest, struct md_entry* src );
int UG_inode_export_match_type( struct UG_inode* dest, struct md_entry* src );

// import an inode 
int UG_inode_import( struct UG_inode* dest, struct md_entry* src );

// import blocks 
int UG_inode_manifest_merge_blocks( struct SG_gateway* gateway, struct UG_inode* inode, struct SG_manifest* new_manifest );

// cache to dirty blocks
int UG_inode_dirty_block_cache( struct UG_inode* inode, struct UG_dirty_block* dirty_block );

// trim an inode's dirty blocks 
int UG_inode_dirty_blocks_trim( struct SG_gateway* gateway, char const* fs_path, struct UG_inode* inode, uint64_t* preserve, size_t preserve_len );

// get the modified dirty blocks from an inode 
int UG_inode_dirty_blocks_modified( struct UG_inode* inode, UG_dirty_block_map_t* modified );

// add a dirty block
int UG_inode_dirty_block_commit( struct SG_gateway* gateway, struct UG_inode* inode, struct UG_dirty_block* dirty_block );

// eviction hints 
int UG_file_handle_evict_add_hint( struct UG_file_handle* fh, uint64_t block_id, int64_t block_version );
int UG_file_handle_evict_blocks( struct UG_file_handle* fh );

// manifest 
int UG_inode_manifest_replace( struct UG_inode* inode, struct SG_manifest* manifest );

// truncate 
int UG_inode_truncate_find_removed( struct SG_gateway* gateway, struct UG_inode* inode, off_t new_size, struct SG_manifest* removed );
int UG_inode_truncate( struct SG_gateway* gateway, struct UG_inode* inode, off_t new_size, int64_t new_version );

// timestamps 
bool UG_inode_manifest_is_newer_than( struct SG_manifest* manifest, int64_t mtime_sec, int32_t mtime_nsec );

// extraction 
int UG_inode_dirty_blocks_extract_modified( struct UG_inode* inode, UG_dirty_block_map_t* modified );
int UG_inode_dirty_blocks_return( struct UG_inode* inode, UG_dirty_block_map_t* extracted );
int UG_inode_replaced_blocks_clear( struct UG_inode* inode, UG_dirty_block_map_t* dirty_blocks );

// sync
int UG_inode_sync_queue_push( struct UG_inode* inode, struct UG_sync_context* sync_context );
struct UG_sync_context* UG_inode_sync_queue_pop( struct UG_inode* inode );
int UG_inode_clear_replaced_blocks( struct UG_inode* inode );
UG_dirty_block_map_t* UG_inode_replace_dirty_blocks( struct UG_inode* inode, UG_dirty_block_map_t* new_dirty_blocks );

#endif
