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

#ifndef _CONSISTENCY_H_
#define _CONSISTENCY_H_

#include "fs_entry.h"

#include "libsyndicate/ms/getattr.h"
#include "libsyndicate/ms/listdir.h"
#include "libsyndicate/ms/path.h"

// staleness processing
bool fs_entry_is_read_stale( struct fs_entry* fent );
int fs_entry_mark_read_stale( struct fs_entry* fent );
bool fs_entry_is_manifest_stale( struct fs_entry* fent );

// ensure every fs_entry along a given path is still considered fresh, re-downloading them if necessary
int fs_entry_revalidate_path( struct fs_core* core, char const* fs_path );

// ensure every child of a given fs_entry is fresh, re-downloading them if necessary 
int fs_entry_revalidate_children( struct fs_core* core, char const* fs_path );

// re-download a manifest if it is stale
int fs_entry_revalidate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent );

// load a manifest into an fs_entry
int fs_entry_revalidate_manifest_ex( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec, uint64_t* successful_gateway_id );

// revalidate all of a path and the fent at the end
int fs_entry_revalidate_metadata( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t* rg_id_ret );

// change/learn coordinator
int fs_entry_coordinate( struct fs_core* core, char const* fs_path, struct fs_entry* fent );

int fs_entry_reload_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

// extra information to be stored in path entries
struct fs_entry_getattr_cls {
   bool stale;                  // is this inode stale
   char* fs_path;               // absolute path to the inode 
   bool exists;                 // does this inode exist?
   bool modified;               // is the inode changed since the last refresh?
   struct md_entry ent;         // inode data
};

// extra information to be stored along with consistency processing
struct fs_entry_consistency_cls {
   struct fs_core* core;          // filesystem core reference (immutable)
   
   ms_path_t* path;                // path entries being revalidated (immutable)
   uint64_t file_id_begin_stale;   // inode number of the shallowest stale inode in the path (immutable)
   
   uint64_t file_id_remote_parent; // inode number of the deepest cached inode (MUTABLE)
   int remote_path_idx;            // index of the next path entry to reload (MUTABLE)
   
   struct timespec query_time;    // time when we started the query (to avoid clobberring files created during the revalidation) (immutable)
   int err;                       // error status during revalidation (MUTABLE)
};

#endif