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

#include "libsyndicate/libsyndicate.h"
#include "fs_entry.h"

#include <dlfcn.h>

#define DRIVER_TMPFILE_NAME "closure-XXXXXX"


// driver construct for the UG
struct storage_driver {
   void* driver;        // dynamic driver library handle
   char* so_path;       // path to the driver so file
   
   void* cls;           // driver-specific state
   
   int running;         // set to non-zero of this driver is initialized
   
   pthread_rwlock_t reload_lock;                // if write-locked, no method can be called here (i.e. the UG is reloading)
   
   int (*init)( struct fs_core* core, void** cls );
   int (*shutdown)( struct fs_core* core, void* cls );
   
   char* (*connect_cache_manifest)( struct fs_core* core, void* cls, CURL* curl, struct fs_entry* fent, char const* fs_path, int64_t mtime_sec, int32_t mtime_nsec );
   char* (*connect_cache_block)( struct fs_core* core, void* cls, CURL* curl, struct fs_entry* fent, char const* fs_path, uint64_t block_id, int64_t block_version );
   int (*write_preup)( struct fs_core* core, void* cls, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len );
   int (*read_postdown)( struct fs_core* core, void* cls, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len );
   int (*chcoord_begin)( struct fs_core* core, void* cls, struct fs_entry* fent );
   int (*chcoord_end)( struct fs_core* core, void* cls, struct fs_entry* fent, int chcoord_status );
};

// driver interface 
int driver_init( struct fs_core* core, struct storage_driver* driver );
int driver_reload( struct fs_core* core, struct storage_driver* driver );
int driver_shutdown( struct fs_core* core, struct storage_driver* driver );

// UG calls these methods to access the driver
char* driver_connect_cache_manifest( struct fs_core* core, struct storage_driver* driver, CURL* curl, struct fs_entry* fent, char const* fs_path, int64_t mtime_sec, int32_t mtime_nsec );
char* driver_connect_cache_block( struct fs_core* core, struct storage_driver* driver, CURL* curl, struct fs_entry* fent, char const* fs_path, uint64_t block_id, int64_t block_version );
int driver_write_preup( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len );
int driver_read_postdown( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* block_data, size_t block_data_len );
int driver_chcoord_begin( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent );
int driver_chcoord_end( struct fs_core* core, struct storage_driver* driver, struct fs_entry* fent, int chcoord_status );

#endif
