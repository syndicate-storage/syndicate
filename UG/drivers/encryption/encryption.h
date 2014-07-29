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

/*
 * This closure lets the UGs in a Volume define shared secrets on a per-file basis,
 * enabling Volume-wide encryption.  The key for each file is stored as an extended 
 * attribute.  The MS will know all encryption keys, but UGs will only know keys 
 * for files they're allowed to access.
 */

#ifndef _UG_CLOSURE_ENCRYPTION_
#define _UG_CLOSURE_ENCRYPTION_

#include "libsyndicate/closure.h"
#include "libsyndicateUG/fs.h"

#define XATTR_ENCRYPT "encryption_key_and_iv"
#define ENTROPY_BYTES 64        // entropy to pad to the block before encrypting it
#define DRIVER_NAME "encryption"

// closure methods 
extern "C" {

int closure_init( struct md_closure* closure, void** cls );
int closure_shutdown( void* cls );

int connect_cache( struct fs_core* core, struct md_closure* closure, CURL* curl, char const* url, void* cls );

int write_block_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                       char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls );

int write_manifest_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                          char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls );

ssize_t read_block_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                             char* in_data, size_t in_data_len, char* out_data, size_t out_data_len, void* cls );

int read_manifest_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                            char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len, void* cls );

int chcoord_begin( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coordinator_id, void* cls );
int chcoord_end( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t new_coodinator_id, int chcoord_status, void* cls );

char* get_driver_name(void);
   
}

#endif 