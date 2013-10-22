/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _READ_H_
#define _READ_H_

#include "fs_entry.h"
#include "consistency.h"

ssize_t fs_entry_read_block( struct fs_core* core, struct fs_file_handle* fh, off_t offset, char* block_bits, size_t block_len );
ssize_t fs_entry_read_block( struct fs_core* core, char const* fs_path, uint64_t block_id, char* block_bits, size_t block_len );
ssize_t fs_entry_read( struct fs_core* core, struct fs_file_handle* fh, char* buf, size_t count, off_t offset );

ssize_t fs_entry_do_read_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len );

ssize_t fs_entry_read_local_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len );
ssize_t fs_entry_read_remote_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_bits, size_t block_len );

#endif