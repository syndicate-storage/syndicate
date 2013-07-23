/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _OPENDIR_H_
#define _OPENDIR_H_

#include "fs_entry.h"

// opendir
struct fs_dir_handle* fs_dir_handle_create( struct fs_entry* dir, char const* path );
int fs_dir_handle_open( struct fs_dir_handle* dirh );
struct fs_dir_handle* fs_entry_opendir( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, int* err );

#endif