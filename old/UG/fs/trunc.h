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

#ifndef _TRUNC_H_
#define _TRUNC_H_

#include "libsyndicate/cache.h"
#include "fs_entry.h"

int fs_entry_truncate( struct fs_core* core, char const* path, off_t size, uint64_t user, uint64_t volume );

int fs_entry_versioned_truncate(struct fs_core* core, const char* path, uint64_t file_id, uint64_t coordinator_id, off_t newsize,
                                int64_t known_version, uint64_t user, uint64_t volume, uint64_t gateway_id, bool check_file_id_and_coordinator_id );

int fs_entry_ftruncate( struct fs_core* core, struct fs_file_handle* fh, off_t size, uint64_t user, uint64_t volume );

int fs_entry_truncate_remote( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size );

int fs_entry_truncate_local( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t size, uint64_t parent_id, char const* parent_name );

#endif
