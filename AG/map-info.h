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

#ifndef _AG_MAP_INFO_H_
#define _AG_MAP_INFO_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/ms/ms-client.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/path.h"
#include "libsyndicate/ms/listdir.h"

#include "AG.h"
#include "driver.h"

#define AG_POPULATE_NO_DRIVER           0x1
#define AG_POPULATE_USE_MS_CACHE        0x2
#define AG_POPULATE_SKIP_DRIVER_INFO   0x4

using namespace std;

// descriptor of an AG's published entry
struct AG_map_info {
   
   // the following are extracted from the spec file
   struct AG_driver* driver;            // driver that handles queries on this entry (references an already-loaded driver somewhere)
   mode_t file_perm;                    // permission bits that this entry will have when published
   uint64_t reval_sec;                  // how often to refresh 
   int32_t type;                        // file or directory
   char* query_string;                  // specfile-given query string
   
   // cached MS fields; needed for manipulating the volume
   bool cache_valid;            // if true, then this data is fresh
   uint64_t file_id;            // ID of this file (obtained from the MS; initialized to 0)
   int64_t file_version;        // version of this file (obtained from the MS; initialized to 0)
   int64_t write_nonce;         // write nonce for this file (obtained from the MS; initialized to 0)
   uint64_t num_children;       // number of children the MS says this inode has 
   int64_t generation;          // generation number of this inode
   
   // cached driver fields
   bool driver_cache_valid;
   struct AG_driver_publish_info pubinfo;       // driver-given dataset information
   
   // generated at runtime 
   int64_t block_version;       // version all blocks will have.  Regenerated on publish/reversion.
   uint64_t refresh_deadline;           // when the next deadline to refresh is 
};


struct AG_path_filters {
   // generate a path with stale entries.
   // any path entries after the first encountered stale entries are stale.
   static bool is_stale( struct AG_map_info* mi, void* cls ) {
      
      return !mi->cache_valid;
   }
   
   // generate a path with fresh entries
   static bool is_fresh( struct AG_map_info* mi, void* cls ) {
      
      return mi->cache_valid;
   }
};

// static set of map info 
struct AG_fs {
   AG_fs_map_t* fs;
   pthread_rwlock_t fs_lock;
   
   struct ms_client* ms;                // immutable; always safe to reference as long as the AG_fs exists
};

// comparator for equality between map_info 
typedef bool (*AG_map_info_equality_func_t)( struct AG_map_info* mi1, struct AG_map_info* mi2 );

// init/shutdown
int AG_fs_init( struct AG_fs* ag_fs, AG_fs_map_t* fs_map, struct ms_client* ms );
int AG_fs_free( struct AG_fs* ag_fs );

// memory management 
void AG_map_info_init( struct AG_map_info* dest, int type, char const* query_string, mode_t file_perm, uint64_t reval_sec, struct AG_driver* driver );
void AG_map_info_dup( struct AG_map_info* dest, struct AG_map_info* source );
void AG_map_info_free( struct AG_map_info* mi );
int AG_fs_map_dup( AG_fs_map_t* dest, AG_fs_map_t* src );
void AG_fs_map_free(AG_fs_map_t *mi_map);

// consistency 
int AG_fs_rlock( struct AG_fs* ag_fs );
int AG_fs_wlock( struct AG_fs* ag_fs );
int AG_fs_unlock( struct AG_fs* ag_fs );
int AG_fs_refresh_path_metadata( struct AG_fs* ag_fs, char const* path, bool force_reload );

int AG_fs_make_coherent( struct AG_fs* ag_fs, char const* path, struct AG_map_info* ref_mi, struct AG_map_info* updated_mi );
int AG_map_info_make_coherent_with_MS_data( struct AG_map_info* mi, uint64_t file_id, int64_t file_version, int64_t write_nonce, uint64_t num_children, int64_t generation );
int AG_map_info_make_coherent_with_driver_data( struct AG_map_info* mi, size_t size, int64_t mtime_sec, int32_t mtime_nsec );
int AG_map_info_make_coherent_with_AG_data( struct AG_map_info* mi, int64_t block_version, uint64_t refresh_deadline );

int AG_copy_metadata_to_map_info( struct AG_map_info* mi, struct md_entry* ent );

int AG_map_info_copy_MS_data( struct AG_map_info* dest, struct AG_map_info* src );
int AG_map_info_copy_driver_data( struct AG_map_info* dest, struct AG_map_info* src );
int AG_map_info_copy_AG_data( struct AG_map_info* dest, struct AG_map_info* src );
int AG_fs_copy_cached_data( struct AG_fs* dest, struct AG_fs* src, int (*copy)( struct AG_map_info* dest, struct AG_map_info* src ) );

int AG_invalidate_cached_metadata( struct AG_map_info* mi );
int AG_invalidate_driver_metadata( struct AG_map_info* mi );
int AG_invalidate_metadata_all( AG_fs_map_t* fs_map, int (*invalidator)( struct AG_map_info* ) );

// validation 
int AG_validate_map_info( AG_fs_map_t* fs );

// tree operations
int AG_fs_map_transforms( AG_fs_map_t* old_fs, AG_fs_map_t* new_fs, AG_fs_map_t* to_publish, AG_fs_map_t* to_fresh, AG_fs_map_t* to_update, AG_fs_map_t* to_delete, AG_map_info_equality_func_t mi_equ );
int AG_fs_map_clone_path( AG_fs_map_t* fs_map, char const* path, AG_fs_map_t* path_data );
int AG_fs_map_merge_tree( AG_fs_map_t* fs_map, AG_fs_map_t* path_data, bool merge_new, AG_fs_map_t* not_merged );
int AG_fs_map_delete_tree( AG_fs_map_t* fs_map, AG_fs_map_t* to_delete );
int AG_fs_map_insert( struct AG_fs* ag_fs, char const* path, struct AG_map_info* mi );
int AG_fs_map_remove( struct AG_fs* ag_fs, char const* path, struct AG_map_info** ret_mi );

// path operations 
struct AG_map_info* AG_fs_lookup_path( struct AG_fs* ag_fs, char const* path );
struct AG_map_info* AG_fs_lookup_path_in_map( AG_fs_map_t* map_info, char const* path );
int AG_path_prefixes( char const* path, char*** ret_prefixes );
int AG_max_depth( AG_fs_map_t* map_infos );

// entry operations 
int64_t AG_map_info_make_deadline( int64_t reval_sec );
int AG_populate_md_entry( struct ms_client* ms, struct md_entry* entry, char const* path, struct AG_map_info* mi, struct AG_map_info* parent_mi, int flags, struct AG_driver_publish_info* opt_pubinfo );
void AG_populate_md_entry_from_AG_info( struct md_entry* entry, struct AG_map_info* mi, uint64_t volume_id, uint64_t owner_id, uint64_t gateway_id, char const* path_basename );
void AG_populate_md_entry_from_MS_info( struct md_entry* entry, uint64_t file_id, int64_t file_version, int64_t write_nonce );
void AG_populate_md_entry_from_driver_info( struct md_entry* entry, struct AG_driver_publish_info* pub_info );

// misc 
int AG_download_MS_fs_map( struct ms_client* ms, AG_fs_map_t* in_specfile, AG_fs_map_t* on_MS );
int AG_get_publish_info( char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pub_info );
int AG_get_publish_info_all( struct AG_state* state, AG_fs_map_t* fs_map );
int AG_get_publish_info_lowlevel( struct AG_state* state, char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pub_info );
int AG_dump_fs_map( AG_fs_map_t* fs_map );
int AG_map_info_get_root( struct ms_client* client, struct AG_map_info* root );
int AG_fs_count_children( AG_fs_map_t* fs_map, map<string, int>* child_counts );

#endif