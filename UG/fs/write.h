/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _WRITE_H_
#define _WRITE_H_

#include "fs_entry.h"
#include "consistency.h"

ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, char const* buf, size_t count, off_t offset );
ssize_t fs_entry_write( struct fs_core* core, struct fs_file_handle* fh, int source_fd, size_t count, off_t offset );

int fs_entry_remote_write( struct fs_core* core, char const* fs_path, Serialization::WriteMsg* write_msg );

int fs_entry_expand_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t new_size, modification_map* modified_blocks );
int fs_entry_prepare_write_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, char* block, size_t count, off_t offset, ssize_t num_written );
ssize_t fs_entry_fill_block( struct fs_core* core, struct fs_entry* fent, char* block, char const* buf, int source_fd, size_t count, off_t offset, ssize_t num_written );

#endif