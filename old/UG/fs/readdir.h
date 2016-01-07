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

#ifndef _READDIR_H_
#define _READDIR_H_

#include "fs_entry.h"

struct fs_dir_entry** fs_entry_readdir( struct fs_core* core, struct fs_dir_handle* dirh, int* err );
struct fs_dir_entry** fs_entry_readdir_lowlevel( struct fs_core* core, char const* fs_path, struct fs_entry* dent );

#endif