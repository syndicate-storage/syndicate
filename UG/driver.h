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

#ifndef _DRIVER_H_
#define _DRIVER_H_

#include "libsyndicate.h"
#include "libsyndicate/closure.h"
#include "fs_entry.h"

#include <dlfcn.h>

// driver callback signatures
typedef int (*driver_connect_cache_func)( struct md_syndicate_conf*, CURL*, char const*, void* );
typedef int (*driver_write_block_preup_func)( char const*, struct fs_entry*, uint64_t, int64_t, char*, size_t, char**, size_t*, void* );
typedef int (*driver_write_manifest_preup_func)( char const*, struct fs_entry*, int64_t, int32_t, char*, size_t, char**, size_t*, void* );
typedef ssize_t (*driver_read_block_postdown_func)( char const*, struct fs_entry*, uint64_t, int64_t, char*, size_t, char*, size_t, void* );
typedef int (*driver_read_manifest_postdown_func)( char const*, struct fs_entry*, int64_t, int32_t, char*, size_t, char**, size_t*, void* );
typedef int (*driver_chcoord_begin_func)( char const*, struct fs_entry*, int64_t, void* );
typedef int (*driver_chcoord_end_func)( char const*, struct fs_entry*, int64_t, int, void* );

// for connecting to the cache providers
struct driver_connect_cache_cls {
   struct md_closure* driver;
   struct ms_client* client;
};

// for reading a manifest 
struct driver_read_manifest_postdown_cls {
   struct md_closure* driver;
   char const* fs_path;
   struct fs_entry* fent;
   int64_t mtime_sec;
   int32_t mtime_nsec;
};

// driver control API
int driver_init( struct fs_core* core, struct md_closure** driver );
int driver_reload( struct fs_core* core, struct md_closure* driver );
int driver_shutdown( struct md_closure* driver );

// UG calls these methods to access the driver...

int driver_connect_cache( struct md_syndicate_conf* conf, CURL* curl, char const* url, void* cls );

// called by read(), write(), and trunc()
int driver_write_block_preup( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                              char* in_block_data, size_t in_block_data_len, char** out_block_data, size_t* out_block_data_len );
int driver_write_manifest_preup( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                                 char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls );
ssize_t driver_read_block_postdown( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                                    char* in_block_data, size_t in_block_data_len, char* out_block_data, size_t out_block_data_len );
int driver_read_manifest_postdown( struct md_syndicate_conf* conf, char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls );

// called by chown()
int driver_chcoord_begin( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t replica_version );
int driver_chcoord_end( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t replica_version, int chcoord_status );

extern struct md_closure_callback_entry UG_CLOSURE_PROTOTYPE[];

#endif