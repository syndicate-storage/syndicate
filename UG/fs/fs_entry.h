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

#ifndef _FS_ENTRY_H_
#define _FS_ENTRY_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <map>
#include <set>
#include <list>
#include <algorithm>
#include <string>
#include <locale>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <curl/curl.h>
#include <openssl/sha.h>
#include <limits.h>
#include <sys/statvfs.h>
#include <sys/select.h>
#include <stdint.h>
#include <inttypes.h>
#include <math.h>

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/cache.h"
#include "libsyndicate/ms/ms-client.h"

using namespace std;

#define FTYPE_FILE 1
#define FTYPE_DIR  2
#define FTYPE_FIFO 3
#define FTYPE_DEAD 4

#define IS_READABLE( mode, node_user, node_vol, user, vol ) ((user) == SYS_USER || ((mode) & S_IROTH) || ((node_vol) == (vol) && ((mode) & S_IRGRP)) || ((node_user) == (user) && ((mode) & S_IRUSR)))
#define IS_DIR_READABLE( mode, node_user, node_vol, user, vol ) ((user) == SYS_USER || ((mode) & S_IXOTH) || ((node_vol) == (vol) && ((mode) & S_IXGRP)) || ((node_user) == (user) && ((mode) & S_IXUSR)))
#define IS_WRITEABLE( mode, node_user, node_vol, user, vol ) (((user) == SYS_USER || (mode) & S_IWOTH) || ((node_vol) == (vol) && ((mode) & S_IWGRP)) || ((node_user) == (user) && ((mode) & S_IWUSR)))
#define IS_EXECUTABLE( mode, node_user, node_vol, user, vol ) IS_DIR_READABLE( mode, node_user, node_vol, user, vol )

#define SYNDICATEFS_XATTR_URL       "user.syndicate_url"
#define SYNDICATEFS_MAX_FRESHNESS   "user.syndicate_max_freshness"

#define INVALID_BLOCK_ID (uint64_t)(-1)

typedef pair<long, struct fs_entry*> fs_dirent;
typedef vector<fs_dirent> fs_entry_set;

struct fs_entry_block_info {
   int64_t version;
   uint64_t gateway_id;
   unsigned char* hash;
   size_t hash_len;
   
   // already-opened block
   int block_fd;        // if >= 0, this is an FD that refers to the block on disk 
   char* block_buf;     // if non-NULL, this is the block itself in RAM (only applicable for bufferred blocks)
   size_t block_len;    // length of block_buf
   bool dirty;          // if true, then this block must be flushed to disk
};

typedef map<uint64_t, struct fs_entry_block_info> modification_map;
typedef map<string, string> xattr_cache_t;

// pre-declare these
class file_manifest;
struct syndicate_state;
struct fs_file_handle;
struct replica_snapshot;
struct sync_context;

typedef list<struct sync_context*> sync_context_list_t;     // queue of sync contexts

// Syndicate filesystem entry
struct fs_entry {
   char ftype;                // what type of file this is
   char* name;                // name of this file
   uint64_t file_id;          // Volume-wide unique ID for this file
   int64_t version;           // version of this file
   file_manifest* manifest;   // current file manifest

   uint64_t owner;               // User ID of the user that created this file.
   uint64_t coordinator;         // Gateway ID of the gateway that will coordinate writes on this file
   uint64_t volume;              // volume ID on which this file resides
   mode_t mode;               // access permissions
   off_t size;                // how big is this file's content?
   int link_count;            // how many other fs_entry structures refer to this file
   int open_count;            // how many file descriptors refer to this file
   int64_t generation;        // the generation number of this file (n, as in, the nth creat() in the parent directory)
   
   int64_t mtime_sec;         // modification time (seconds)
   int32_t mtime_nsec;        // modification time (nanoseconds)
   int64_t ctime_sec;         // creation time (seconds)
   int32_t ctime_nsec;        // creation time (nanoseconds)
   int64_t atime;             // access time (seconds)
   int64_t write_nonce;       // nonce generated at last write by the MS
   int64_t xattr_nonce;       // nonce generated on setxattr/removexattr by the MS
   int64_t ms_manifest_mtime_sec;       // latest-known manifest modification time from the MS
   int32_t ms_manifest_mtime_nsec;      // latest-known manifest modification time from the MS
   
   modification_map* bufferred_blocks;  // set of in-core blocks that have been either read recently, or modified recently.  Modified blocks will be flushed to on-disk cache.
   modification_map* dirty_blocks;      // set of disk-cached blocks that have been modified locally, and will need to be replicated on flush() or last close()
   modification_map* garbage_blocks;    // set of blocks as they were when the file was opened for the first time since the last synchronization (i.e. when open_count was last 0)
   
   struct timespec refresh_time;    // time of last refresh from the ms
   uint32_t max_read_freshness;     // how long since last refresh, in ms, this fs_entry is to be considered fresh for reading
   uint32_t max_write_freshness;    // how long since last refresh, in ms, this fs_entry is to be considered fresh for writing
   bool read_stale;
   bool write_stale;
   bool dirty;                      // if true, then we need to flush data on fsync()
   
   replica_snapshot* old_snapshot;      // snapshot of this fs_entry before dirtying it

   pthread_rwlock_t lock;     // lock to control access to this structure
   
   sync_context_list_t* sync_queue;     // queue of synchronization requests (from truncate() and fsync()), used to ensure that we send metadata to the MS in program order
   
   fs_entry_set* children;      // used only for directories--set of children
   int64_t ms_num_children;     // the number of children the MS says this entry has
   
   pthread_rwlock_t xattr_lock;
   xattr_cache_t* xattr_cache;  // cached xattrs
   
   bool created_in_session;     // if we're in client mode, then this is true of the file was created in this session
   bool deletion_in_progress;   // if true, we're in the process of deleting a file (which is not atomic)
   
   bool vacuuming;              // if true, then we're currently vacuuming this file
   bool vacuumed;               // if true, then we've already tried to vacuum this file upon discovery (false means we should try again)
};

#define IS_STREAM_FILE( fent ) ((fent).size < 0)

// Syndicate file handle
struct fs_file_handle {
   struct fs_entry* fent;     // reference to the fs_entry this handle represents
   char* path;                // path that was opened (used for revalidation purposes)
   uint64_t file_id;          // ID of the file opened
   uint64_t volume;           // which Volume this fent belongs to
   int open_count;            // how many processes have opened this handle
   int flags;                 // open flags
   bool dirty;                // set to true if there is dirty data 
   
   char* parent_name;         // name of parent directory
   uint64_t parent_id;        // ID of parent directory

   bool is_AG;                // whether or not this file is hosted by an AG (since it affects how we deal with unknown sizes and EOF)
   
   uint64_t block_id;         // ID of the block we're currently reading
   
   int64_t transfer_timeout_ms;   // how long the transfer is allowed to take (in milliseconds)

   pthread_rwlock_t lock;     // lock to control access to this structure
};

// Syndicate directory handle
struct fs_dir_handle {
   struct fs_entry* dent;     // reference to the fs_entry this handle represents
   char* path;                // path that was opened
   uint64_t file_id;          // ID of this dir
   int open_count;            // how many processes have opened this handle
   uint64_t volume;           // which Volume dent is in

   char* parent_name;         // name of parent directory
   uint64_t parent_id;        // ID of parent directory

   pthread_rwlock_t lock;     // lock to control access to this structure
};

// Syndicate directory entry
struct fs_dir_entry {
   char ftype;
   struct md_entry data;
};


struct fs_entry_view_change_cls {
   struct fs_core* core;
   uint64_t cert_version;
};

// Syndicate core information
struct fs_core {
   struct fs_entry* root;              // root FS entry
   struct md_syndicate_conf* conf;     // Syndicate configuration structure
   struct ms_client* ms;               // link to the MS
   struct md_closure* closure;         // UG storage closure
   struct md_syndicate_cache* cache;   // index over on-disk cache
   struct syndicate_state* state;   // state 
   struct fs_entry_view_change_cls* viewchange_cls;     // pass to view change callback
   
   uint64_t volume;                 // Volume we're bound to
   uint64_t gateway;                // gateway ID
   uint64_t blocking_factor;        // block size

   pthread_rwlock_t lock;     // lock to control access to this structure
   pthread_rwlock_t fs_lock;  // lock to create/remove entries in the filesystem
};

#define FS_ENTRY_LOCAL( core, fent ) (fent->coordinator == core->gateway)

// configuration
int fs_entry_set_config( struct md_syndicate_conf* conf );

// fs_core operations
int fs_core_init( struct fs_core* core, struct syndicate_state* state, struct md_syndicate_conf* conf, struct ms_client* client, struct md_syndicate_cache* cache,
                  uint64_t owner_id, uint64_t gateway_id, uint64_t volume, mode_t mode, uint64_t blocking_factor );
int fs_core_destroy(struct fs_core* core);

// destroy
int fs_destroy( struct fs_core* core );
int fs_unlink_children( struct fs_core* core, fs_entry_set* children, bool remove_data );

// fs locking
int fs_core_fs_rlock( struct fs_core* core );
int fs_core_fs_wlock( struct fs_core* core );
int fs_core_fs_unlock( struct fs_core* core );

// core locking
int fs_core_rlock( struct fs_core* core );
int fs_core_wlock( struct fs_core* core );
int fs_core_unlock( struct fs_core* core );

// fs_entry initialization
int fs_entry_init_file( struct fs_core* core, struct fs_entry* fent,
                        char const* name, uint64_t file_id, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec,
                        int64_t write_nonce, int64_t xattr_nonce );

int fs_entry_init_dir( struct fs_core* core, struct fs_entry* fent,
                       char const* name, uint64_t file_id, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, int64_t mtime_sec, int32_t mtime_nsec,
                       int64_t write_nonce, int64_t xattr_nonce );

int fs_entry_init_fifo( struct fs_core* core, struct fs_entry* fent,
                        char const* name, uint64_t file_id, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec,
                        int64_t write_nonce, int64_t xattr_nonce );

int fs_entry_init_md( struct fs_core* core, struct fs_entry* fent, struct md_entry* ent );

int64_t fs_entry_next_file_version(void);
int64_t fs_entry_next_block_version(void);

// fs_entry cleanup 
int fs_entry_destroy( struct fs_entry* fent, bool needlock );
int fs_entry_try_destroy( struct fs_core* core, struct fs_entry* fent );

// fs_file_handle cleanup
int fs_file_handle_destroy( struct fs_file_handle* fh );

// fs_dir_handle cleanup
void fs_dir_handle_destroy( struct fs_dir_handle* dh );

// fs_dir_entry cleanup
int fs_dir_entry_destroy( struct fs_dir_entry* dent );
int fs_dir_entry_destroy_all( struct fs_dir_entry** dents );

// cache xattrs 
int fs_entry_put_cached_xattr( struct fs_entry* fent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, int64_t last_known_xattr_nonce );
int fs_entry_get_cached_xattr( struct fs_entry* fent, char const* xattr_name, char** xattr_value, size_t* xattr_value_len );
int fs_entry_evict_cached_xattr( struct fs_entry* fent, char const* xattr_name );
int fs_entry_clear_cached_xattrs( struct fs_entry* fent, int64_t new_xattr_nonce );
int fs_entry_list_cached_xattrs( struct fs_entry* fent, char** xattr_list, size_t* xattr_list_len, int64_t last_known_xattr_nonce );
int fs_entry_cache_xattr_list( struct fs_entry* fent, xattr_cache_t* new_listing, int64_t last_known_xattr_nonce );

// dirty block handling 
int fs_entry_setup_garbage_blocks( struct fs_entry* fent );
int fs_entry_extract_dirty_blocks( struct fs_entry* fent, modification_map** dirty_blocks );
int fs_entry_copy_garbage_blocks( struct fs_entry* fent, modification_map** garbage_blocks );
int fs_entry_clear_garbage_blocks( struct fs_entry* fent );
int fs_entry_replace_dirty_blocks( struct fs_entry* fent, modification_map* dirty_blocks );

bool fs_entry_has_dirty_block( struct fs_entry* fent, uint64_t block_id );

bool fs_entry_has_dirty_blocks( struct fs_entry* fent );

// fs_entry locking
int fs_entry_rlock2( struct fs_entry* fent, char const* from_str, int lineno );
int fs_entry_wlock2( struct fs_entry* fent, char const* from_str, int lineno );
int fs_entry_unlock2( struct fs_entry* fent, char const* from_str, int lineno );

#define fs_entry_rlock( fent ) fs_entry_rlock2( fent, __FILE__, __LINE__ )
#define fs_entry_wlock( fent ) fs_entry_wlock2( fent, __FILE__, __LINE__ )
#define fs_entry_unlock( fent ) fs_entry_unlock2( fent, __FILE__, __LINE__ )

// fs_file_handle locking
int fs_file_handle_rlock( struct fs_file_handle* fh );
int fs_file_handle_wlock( struct fs_file_handle* fh );
int fs_file_handle_unlock( struct fs_file_handle* fh );

// fs_dir_handle locking
int fs_dir_handle_rlock( struct fs_dir_handle* dh );
int fs_dir_handle_wlock( struct fs_dir_handle* dh );
int fs_dir_handle_unlock( struct fs_dir_handle* dh );

// name hashing
long fs_entry_name_hash( char const* name );

// resolution
struct fs_entry* fs_entry_resolve_path( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err );
struct fs_entry* fs_entry_resolve_path_cls( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err, int (*ent_eval)( struct fs_entry*, void* ), void* cls );
struct fs_entry* fs_entry_resolve_path_and_parent_info( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err, uint64_t* parent_id, char** parent_name );
char* fs_entry_resolve_block( struct fs_core* core, struct fs_file_handle* fh, off_t offset );
uint64_t fs_entry_block_id( struct fs_core* core, off_t offset );
uint64_t fs_entry_block_id( size_t blocksize, off_t offset );

// operations on directory sets
void fs_entry_set_insert( fs_entry_set* set, char const* name, struct fs_entry* child );
void fs_entry_set_insert_hash( fs_entry_set* set, long hash, struct fs_entry* child );
struct fs_entry* fs_entry_set_find_name( fs_entry_set* set, char const* name );
struct fs_entry* fs_entry_set_find_hash( fs_entry_set* set, long hash );
bool fs_entry_set_remove( fs_entry_set* set, char const* name );
bool fs_entry_set_remove_hash( fs_entry_set* set, long nh );
bool fs_entry_set_replace( fs_entry_set* set, char const* name, struct fs_entry* replacement );
unsigned int fs_entry_set_count( fs_entry_set* set );
struct fs_entry* fs_entry_set_get( fs_entry_set::iterator* itr );
long fs_entry_set_get_name_hash( fs_entry_set::iterator* itr );
int64_t fs_entry_set_max_generation( fs_entry_set* children );

// conversion
int fs_entry_to_md_entry( struct fs_core* core, struct md_entry* dest, char const* fs_path, uint64_t owner, uint64_t volume );
int fs_entry_to_md_entry( struct fs_core* core, struct md_entry* dest, struct fs_entry* fent, uint64_t parent_id, char const* parent_name );

// versioning
int64_t fs_entry_next_version_number(void);
int fs_entry_update_modtime( struct fs_entry* fent );
int fs_entry_store_snapshot( struct fs_entry* fent, struct replica_snapshot* new_snapshot );

// misc
unsigned int fs_entry_num_children( struct fs_entry* fent );

// block info 
void fs_entry_block_info_replicate_init( struct fs_entry_block_info* binfo, int64_t version, unsigned char* hash, size_t hash_len, uint64_t gateway_id, int block_fd );
void fs_entry_block_info_garbage_init( struct fs_entry_block_info* binfo, int64_t version, unsigned char* hash, size_t hash_len, uint64_t gateway_id );
int fs_entry_block_info_free( struct fs_entry_block_info* binfo );
int fs_entry_block_info_free_ex( struct fs_entry_block_info* binfo, bool close_fd );

// bufferring 
int fs_entry_has_bufferred_block( struct fs_entry* fent, uint64_t block_id );
int fs_entry_read_bufferred_block( struct fs_entry* fent, uint64_t block_id, char* buf, off_t block_offset, size_t read_len );
int fs_entry_write_bufferred_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char const* buf, off_t block_offset, size_t write_len );
int fs_entry_replace_bufferred_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* buf, size_t buf_len, bool dirty );
int fs_entry_clear_bufferred_block( struct fs_entry* fent, uint64_t block_id );
int fs_entry_extract_bufferred_blocks( struct fs_entry* fent, modification_map* block_info );
int fs_entry_emplace_bufferred_blocks( struct fs_entry* fent, modification_map* block_info );
int fs_entry_hash_bufferred_block( struct fs_entry* fent, uint64_t block_id, unsigned char** block_hash, size_t* block_hash_len );

// syncing 
int fs_entry_sync_context_enqueue( struct fs_entry* fent, struct sync_context* ctx );
int fs_entry_sync_context_dequeue( struct fs_entry* fent, struct sync_context** ctx );
int fs_entry_sync_context_remove( struct fs_entry* fent, struct sync_context* ctx );
size_t fs_entry_sync_context_size( struct fs_entry* fent );
int fs_entry_sync_queue_apply( struct fs_entry* fent, void (*func)( struct sync_context*, void* ), void* cls );

// view change
int fs_entry_view_change_callback( struct ms_client* ms, void* cls );

// block state management
int fs_entry_list_block_ids( modification_map* m, uint64_t** block_ids, size_t* num_block_ids );

int fs_entry_setup_working_data( struct fs_core* core, struct fs_entry* fent );
int fs_entry_free_working_data( struct fs_entry* fent );

int fs_entry_free_modification_map( modification_map* m );
int fs_entry_free_modification_map_ex( modification_map* m, bool close_fds );

int fs_entry_merge_new_dirty_blocks( struct fs_entry* fent, modification_map* new_dirty_blocks );
int fs_entry_merge_old_dirty_blocks( struct fs_core* core, struct fs_entry* fent, uint64_t original_file_id, int64_t original_file_version, modification_map* old_dirty_blocks, modification_map* unmerged );

// cython compatibility
uint64_t fs_dir_entry_type( struct fs_dir_entry* dirent );
char* fs_dir_entry_name( struct fs_dir_entry* dirent );
uint64_t fs_dir_entry_file_id( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_mtime_sec( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_mtime_nsec( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_manifest_mtime_sec( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_manifest_mtime_nsec( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_ctime_sec( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_ctime_nsec( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_write_nonce( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_xattr_nonce( struct fs_dir_entry* dirent );
int64_t fs_dir_entry_version( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_max_read_freshness( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_max_write_freshness( struct fs_dir_entry* dirent );
uint64_t fs_dir_entry_owner( struct fs_dir_entry* dirent );
uint64_t fs_dir_entry_coordinator( struct fs_dir_entry* dirent );
uint64_t fs_dir_entry_volume( struct fs_dir_entry* dirent );
int32_t fs_dir_entry_mode( struct fs_dir_entry* dirent );
uint64_t fs_dir_entry_size( struct fs_dir_entry* dirent );

#endif
