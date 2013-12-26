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

#ifndef _OPEN_H_
#define _OPEN_H_

#include "fs_entry.h"
#include "consistency.h"

// open files
struct fs_file_handle* fs_entry_create( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, mode_t mode, int* err );
struct fs_file_handle* fs_entry_open( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, int flags, mode_t mode, int* err );

// mknod 
int fs_entry_mknod( struct fs_core* core, char const* path, mode_t mode, dev_t dev, uint64_t user, uint64_t vol );

// filehandles
struct fs_file_handle* fs_file_handle_create( struct fs_core* core, struct fs_entry* ent, char const* opened_path );
int fs_file_handle_open( struct fs_file_handle* fh, int flags, mode_t mode );

#endif