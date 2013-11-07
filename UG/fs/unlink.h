/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _UNLINK_H_
#define _UNLINK_H_

#include "fs_entry.h"

int fs_entry_detach( struct fs_core* core, char const* path, uint64_t user, uint64_t vol );
int fs_entry_versioned_unlink( struct fs_core* core, char const* path, uint64_t file_id, uint64_t coordinator_id, int64_t known_version, uint64_t owner, uint64_t volume, uint64_t gateway_id, bool check_file_id_and_coordinator_id );
int fs_entry_detach_lowlevel( struct fs_core* core, struct fs_entry* parent, struct fs_entry* child, bool remove_data );

#endif