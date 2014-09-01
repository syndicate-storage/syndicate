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
#include "libsyndicate/ms-client.h"

#include "AG.h"

using namespace std;

// prototypes
struct AG_driver;
struct AG_driver_publish_info;

// descriptor of an AG's published entry
struct AG_map_info {
   
   // the following are extracted from the spec file
   struct AG_driver* driver;            // driver that handles queries on this entry (references an already-loaded driver somewhere)
   mode_t file_perm;                    // permission bits that this entry will have when published
   uint64_t reval_sec;                  // how often to refresh 
   int32_t type;                        // file or directory
   char* query_string;                  // specfile-given query string
   
   // cached runtime fields; needed for manipulating the volume
   bool cache_valid;            // if true, then this data is fresh
   uint64_t file_id;            // ID of this file (obtained from the MS; initialized to 0)
   int64_t file_version;        // version of this file (obtained from the MS; initialized to 0)
   int64_t write_nonce;         // write nonce for this file (obtained from the MS; initialized to 0)
   
   // generated at runtime 
   int64_t block_version;       // version all blocks will have.  Regenerated on publish/reversion.
   uint64_t refresh_deadline;           // when the next deadline to refresh is 
};

// static set of map info 
struct AG_fs {
   AG_fs_map_t* fs;
   pthread_rwlock_t fs_lock;
   
   struct ms_client* ms;
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
bool AG_has_valid_cached_metadata( char const* path, struct AG_map_info* mi );
int AG_fs_make_coherent( struct AG_fs* ag_fs, char const* path, uint64_t file_id, int64_t file_version, int64_t block_version, int64_t write_nonce, int64_t reval_sec, struct AG_map_info* updated_mi );
int AG_fs_copy_cached_data( struct AG_fs* dest, struct AG_fs* src );

// validation 
int AG_validate_map_info( AG_fs_map_t* fs );

// tree operations
int AG_fs_map_transforms( AG_fs_map_t* old_fs, AG_fs_map_t* new_fs, AG_fs_map_t* to_publish, AG_fs_map_t* to_update, AG_fs_map_t* to_delete, AG_map_info_equality_func_t mi_equ );
int AG_fs_map_clone_path( AG_fs_map_t* fs_map, char const* path, AG_fs_map_t* path_data );
int AG_fs_map_merge_tree( AG_fs_map_t* fs_map, AG_fs_map_t* path_data, bool merge_new, AG_fs_map_t* not_merged );

// hierarchy management 
struct AG_map_info* AG_fs_lookup_path( struct AG_fs* ag_fs, char const* path );

int AG_fs_publish( struct AG_fs* ag_fs, char const* path, struct AG_map_info* mi );
int AG_fs_publish_map( struct AG_fs* ag_fs, AG_fs_map_t* to_publish, bool continue_if_exists );

int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* pubinfo );
int AG_fs_reversion_map( struct AG_fs* ag_fs, AG_fs_map_t* to_reversion, bool continue_on_failure );

int AG_fs_delete( struct AG_fs* ag_fs, char const* path );
int AG_fs_delete_map( struct AG_fs* ag_fs, AG_fs_map_t* to_delete, bool continue_on_failure );

// misc 
int AG_download_existing_fs_map( struct ms_client* ms, AG_fs_map_t** ret_existing_fs, bool fail_fast );
int AG_get_pub_info( struct AG_state* state, char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pubinfo );
int AG_dump_fs_map( AG_fs_map_t* fs_map );
int AG_map_info_get_root( struct ms_client* client, struct AG_map_info* root );

#endif