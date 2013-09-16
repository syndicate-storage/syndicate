/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _STORAGE_H_
#define _STORAGE_H_

#include "fs_entry.h"
#include "url.h"

#define SYNDICATE_COLLATE_TMPPATH "/tmp/syndicate-collate-XXXXXX"

// add/remove blocks
ssize_t fs_entry_put_block_data( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_data, off_t offset, size_t len, bool staging );
int fs_entry_remove_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, bool staging );

// read/write blocks
ssize_t fs_entry_get_block_local( struct fs_core* core, int fd, char* block, size_t block_len );
ssize_t fs_entry_write_block_data( struct fs_core* core, int fd, char* buf, size_t len );
ssize_t fs_entry_commit_block_data( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* buf, off_t offset, size_t len, bool staging );

// stat block
int fs_entry_block_stat( struct fs_core* core, char const* path, uint64_t block_id, struct stat* sb );

// reintegrate remotely-written blocks
int fs_entry_collate( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* bits, uint64_t block_len, uint64_t parent_id, char const* parent_name );
int fs_entry_release_staging( struct fs_core* core, Serialization::WriteMsg* accept_msg );

// manipulate local file data
int fs_entry_create_local_file( struct fs_core* core, uint64_t file_id, int64_t version, mode_t mode );
int fs_entry_move_local_file( char* path, char* new_path );
int fs_entry_clear_local_file( struct fs_core* core, uint64_t file_id, int64_t version );
int fs_entry_remove_local_file( struct fs_core* core, uint64_t file_id, int64_t version );
int fs_entry_reversion_local_file( struct fs_core* core, struct fs_entry* fent, uint64_t new_version );

// withdraw
int fs_entry_withdraw_dir( struct fs_core* core, char const* path );

#endif