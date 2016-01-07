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


#ifndef _STAT_H_
#define _STAT_H_

#include "fs_entry.h"

// syndicatefs magic number
#define SYNDICATEFS_MAGIC  0x01191988

// read metadata
int fs_entry_stat( struct fs_core* core, char const* path, struct stat* sb, uint64_t user, uint64_t volume );
int fs_entry_stat_extended( struct fs_core* core, char const* path, struct stat* sb, bool* is_local, int64_t* version, uint64_t* coordinator_id, uint64_t user, uint64_t volume, bool revalidate );
int fs_entry_block_stat( struct fs_core* core, char const* path, uint64_t block_id, struct stat* sb );         // system use only
bool fs_entry_is_local( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, int* err );
bool fs_entry_is_block_local( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, uint64_t block_id );
int fs_entry_fstat( struct fs_core* core, struct fs_file_handle* fh, struct stat* sb );
int fs_entry_fstat_dir( struct fs_core* core, struct fs_dir_handle* dh, struct stat* sb );
int fs_entry_statfs( struct fs_core* core, char const* path, struct statvfs *statv, uint64_t user, uint64_t volume );
int fs_entry_access( struct fs_core* core, char const* path, int mode, uint64_t user, uint64_t volume );
int fs_entry_get_creation_time( struct fs_core* core, char const* fs_path, struct timespec* t );
int fs_entry_get_mod_time( struct fs_core* core, char const* fs_path, struct timespec* t );
int fs_entry_get_manifest_mod_time( struct fs_core* core, char const* fs_path, struct timespec* t );
int fs_entry_set_mod_time( struct fs_core* core, char const* fs_path, struct timespec* t );
int64_t fs_entry_get_version( struct fs_core* core, char const* fs_path );
int64_t fs_entry_get_block_version( struct fs_core* core, char* fs_path, uint64_t block_id );
uint64_t fs_entry_get_block_host( struct fs_core* core, char* fs_path, uint64_t block_id );
char* fs_entry_get_manifest_str( struct fs_core* core, char* fs_path );
ssize_t fs_entry_serialize_manifest( struct fs_core* core, char* fs_path, char** manifest_bits, bool sign );
ssize_t fs_entry_serialize_manifest( struct fs_core* core, struct fs_entry* fent, char** manifest_bits, bool sign );

// write metadata
int fs_entry_chown( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, uint64_t new_user );
int fs_entry_chmod( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, mode_t mode );
int fs_entry_utime( struct fs_core* core, char const* path, struct utimbuf* tb, uint64_t user, uint64_t volume );

#endif