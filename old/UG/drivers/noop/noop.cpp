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

#include "noop.h"

// dummy init 
int closure_init( struct md_closure* closure, void** cls ) {
   SG_debug("%s: closure_init\n", DRIVER_NAME );
   return 0;
}

// dummy shutdown 
int closure_shutdown( void* cls ) {
   SG_debug("%s: closure_shutdown\n", DRIVER_NAME );
   return 0;
}

// dummy connect cache 
int connect_cache( struct fs_core* core, struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   SG_debug("%s: connect_cache\n", DRIVER_NAME );
   return ms_client_volume_connect_cache( core->ms, curl, url );
}

// dummy write_block_preup 
int write_block_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                       char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
 
   SG_debug("%s: write_block_preup\n", DRIVER_NAME );
   
   *out_data_len = in_data_len;
   *out_data = SG_CALLOC( char, in_data_len );
   
   memcpy( *out_data, in_data, in_data_len );
   
   return 0;
}

// dummy write_manifest_preup
int write_manifest_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                          char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
 
   SG_debug("%s: write_manifest_preup\n", DRIVER_NAME );
   
   *out_data_len = in_data_len;
   *out_data = SG_CALLOC( char, in_data_len );
   
   memcpy( *out_data, in_data, in_data_len );
   
   return 0;
}

// dummy read_block_postdown
ssize_t read_block_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                             char* in_data, size_t in_data_len, char* out_data, size_t out_data_len, void* cls ) {

   SG_debug("%s: read_block_postdown\n", DRIVER_NAME );
   
   ssize_t ret = MIN( out_data_len, in_data_len );
   
   memcpy( out_data, in_data, ret );
   
   return ret;
}

// dummy read_manifest_postdown 
int read_manifest_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                            char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls ) {
   
   SG_debug("%s: read_manifest_postdown\n", DRIVER_NAME );
   
   *out_data_len = in_data_len;
   *out_data = SG_CALLOC( char, in_data_len );
   
   memcpy( *out_data, in_data, in_data_len );
   
   return 0;
}

// dummy chcoord_begin 
int chcoord_begin( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coordinator_id, void* cls ) {
   
   SG_debug("%s: chcoord_begin\n", DRIVER_NAME );
   
   return 0;
}
   
// dummy chcoord_end 
int chcoord_end( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coodinator_id, int chcoord_status, void* cls ) {
   
   SG_debug("%s: chcoord_end\n", DRIVER_NAME );
   
   return 0;
}

// dummy garbage collect 
int garbage_collect( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct replica_snapshot* fent_snapshot, uint64_t* block_ids, int64_t* block_versions, size_t num_blocks ) {
   
   SG_debug("%s: garbage_collect\n", DRIVER_NAME );
   
   return 0;
}

// get name
char* get_driver_name(void) {
   
   SG_debug("%s: get_driver_name\n", DRIVER_NAME );
   
   return strdup(DRIVER_NAME);
}

// dummy create file 
int create_file( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent ) {
   
   SG_debug("%s: create file %s\n", DRIVER_NAME, fs_path );
   
   return 0;
}

// dummy delete file 
int delete_file( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent ) {
   
   SG_debug("%s: delete file %s\n", DRIVER_NAME, fs_path );
   
   return 0;
}

