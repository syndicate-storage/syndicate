/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _RENAME_H_
#define _RENAME_H_

#include "fs_entry.h"

// rename
int fs_entry_versioned_rename( struct fs_core* core, char const* old_path, char const* new_path, uint64_t user, uint64_t volume, int64_t version );
int fs_entry_remote_rename( struct fs_core* core, Serialization::WriteMsg* renameMsg );
int fs_entry_rename( struct fs_core* core, char const* old_path, char const* new_path, uint64_t user, uint64_t volume );

#endif