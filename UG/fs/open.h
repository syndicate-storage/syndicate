/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
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