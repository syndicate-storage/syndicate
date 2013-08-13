/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _FS_ENTRY_H_
#define _FS_ENTRY_H_

#define __STDC_FORMAT_MACROS

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

#include "libsyndicate.h"
#include "ms-client.h"

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

typedef map<uint64_t, int64_t> modification_map;

// pre-declare these
class Collator;
class file_manifest;
struct RG_channel;

// Syndicate filesystem entry
struct fs_entry {
   char ftype;                // what type of file this is
   char* name;                // name of this file
   char* url;                 // Base URL of this file (scheme://owner.hostname/path/to/file/name).  Does NOT include the version
   int64_t version;           // version of this file
   file_manifest* manifest;   // current file manifest

   uint64_t owner;               // User ID of the user that created this file.
   uint64_t coordinator;         // Gateway ID of the gateway that will coordinate writes on this file
   uint64_t volume;              // volume ID on which this file resides
   mode_t mode;               // access permissions
   off_t size;                // how big is this file's content?
   int link_count;            // how many other fs_entry structures refer to this file
   int open_count;            // how many processes are reading this file

   int64_t mtime_sec;         // modification time (seconds)
   int32_t mtime_nsec;        // modification time (nanoseconds)
   int64_t ctime_sec;         // creation time (seconds)
   int32_t ctime_nsec;        // creation time (nanoseconds)
   int64_t atime;             // access time
   
   struct timespec refresh_time;    // time of last refresh from the ms
   uint32_t max_read_freshness;     // how long since last refresh, in ms, this fs_entry is to be considered fresh for reading (negative means always fresh)
   uint32_t max_write_freshness;    // how long since last refresh, in ms, this fs_entry is to be considered fresh for writing (negative means always fresh)
   bool read_stale;
   bool write_stale;

   pthread_rwlock_t lock;     // lock to control access to this structure

   fs_entry_set* children;    // used only for directories--set of children
   
   bool write_locked;
};

#define IS_STREAM_FILE( fent ) ((fent).size < 0)

// Syndicate file handle
struct fs_file_handle {
   struct fs_entry* fent;     // reference to the fs_entry this handle represents
   uint64_t volume;           // which Volume this fent belongs to
   int open_count;            // how many processes have opened this handle
   int flags;                 // open flags
   char* path;                // the path that was opened
   bool dirty;                // set to true if a write has occurred on this file handle (but it wasn't flushed)

   pthread_rwlock_t lock;     // lock to control access to this structure
};

// Syndicate directory handle
struct fs_dir_handle {
   struct fs_entry* dent;     // reference to the fs_entry this handle represents
   char* path;                // the path that was opened
   int open_count;            // how many processes have opened this handle
   uint64_t volume;           // which Volume dent is in

   pthread_rwlock_t lock;     // lock to control access to this structure
};

// Syndicate directory entry
struct fs_dir_entry {
   char ftype;
   struct md_entry data;
};

// Syndicate core information
struct fs_core {
   struct fs_entry* root;              // root FS entry
   struct md_syndicate_conf* conf;     // Syndicate configuration structure
   unsigned long int num_files;        // how many files exist
   struct ms_client* ms;               // link to the MS
   Collator* col;                   // Collator interface
   uint64_t volume;                 // Volume we're bound to

   pthread_rwlock_t lock;     // lock to control access to this structure
   pthread_rwlock_t fs_lock;  // lock to create/remove entries in the filesystem
};

// configuration
int fs_entry_set_config( struct md_syndicate_conf* conf );

// fs_core operations
int fs_core_init( struct fs_core* core, struct md_syndicate_conf* conf );
int fs_core_destroy(struct fs_core* core);
int fs_core_use_ms( struct fs_core* core, struct ms_client* ms );
int fs_core_use_collator( struct fs_core* core, Collator* iop );

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
int fs_entry_init_file( struct fs_core* core, struct fs_entry* fent, char const* name, char const* url, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec );
int fs_entry_init_dir( struct fs_core* core, struct fs_entry* fent, char const* name, char const* url, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec );
int fs_entry_init_fifo( struct fs_core* core, struct fs_entry* fent, char const* name, char const* url, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec );
int fs_entry_init_md( struct fs_core* core, struct fs_entry* fent, struct md_entry* ent );

int64_t fs_entry_next_file_version(void);
int64_t fs_entry_next_block_version(void);

// fs_entry cleanup 
int fs_entry_destroy( struct fs_entry* fent, bool needlock );

// fs_file_handle cleanup
int fs_file_handle_destroy( struct fs_file_handle* fh );

// fs_dir_handle cleanup
void fs_dir_handle_destroy( struct fs_dir_handle* dh );

// fs_dir_entry cleanup
int fs_dir_entry_destroy( struct fs_dir_entry* dent );
int fs_dir_entry_destroy_all( struct fs_dir_entry** dents );


// fs_entry locking
int fs_entry_rlock( struct fs_entry* fent );
int fs_entry_wlock( struct fs_entry* fent );
int fs_entry_unlock( struct fs_entry* fent );

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
char* fs_entry_resolve_block( struct fs_core* core, struct fs_file_handle* fh, off_t offset );
uint64_t fs_entry_block_id( off_t offset, struct md_syndicate_conf* conf );

// operations on directory sets
void fs_entry_set_insert( fs_entry_set* set, char const* name, struct fs_entry* child );
void fs_entry_set_insert_hash( fs_entry_set* set, long hash, struct fs_entry* child );
struct fs_entry* fs_entry_set_find_name( fs_entry_set* set, char const* name );
struct fs_entry* fs_entry_set_find_hash( fs_entry_set* set, long hash );
bool fs_entry_set_remove( fs_entry_set* set, char const* name );
bool fs_entry_set_replace( fs_entry_set* set, char const* name, struct fs_entry* replacement );
unsigned int fs_entry_set_count( fs_entry_set* set );
struct fs_entry* fs_entry_set_get( fs_entry_set::iterator* itr );
long fs_entry_set_get_name_hash( fs_entry_set::iterator* itr );

// conversion
int fs_entry_to_md_entry( struct fs_core* core, char const* fs_path, uint64_t owner, uint64_t volume, struct md_entry* dest);
int fs_entry_to_md_entry( struct fs_core* core, char const* fs_path, struct fs_entry* fent, struct md_entry* dest );

// versioning
int64_t fs_entry_next_version_number(void);
int fs_entry_reversion_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t new_version );

#endif
