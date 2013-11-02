/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _CONSISTENCY_H_
#define _CONSISTENCY_H_

#include "fs_entry.h"

// flush/resync data
int fs_entry_fsync( struct fs_core* core, struct fs_file_handle* fh );
int fs_entry_fdatasync( struct fs_core* core, struct fs_file_handle* fh );

// staleness
bool fs_entry_is_read_stale( struct fs_entry* fent );
int fs_entry_mark_read_stale( struct fs_entry* fent );
bool fs_entry_is_manifest_stale( struct fs_entry* fent );

// ensure every fs_entry along a given path is still considered fresh, re-downloading them if necessary
int fs_entry_revalidate_path( struct fs_core* core, uint64_t volume, char const* fs_path );
int fs_entry_revalidate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent );

// manifest
int fs_entry_reload_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

struct fs_entry_listing_cls {
   // extra information to be stored in path entries
   bool stale;
   char* fs_path;
   bool exists;
   struct ms_listing listing;
};

struct fs_entry_consistency_cls {
   struct fs_core* core;
   path_t* path;
   struct timespec query_time;
   int err;
   
   vector<uint64_t> reloaded;     // files/directories reloaded as part of a revalidation
};

#endif