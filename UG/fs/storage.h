/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _STORAGE_H_
#define _STORAGE_H_

#include "fs_entry.h"
#include "url.h"

#define SYNDICATE_COLLATE_TMPPATH "/tmp/syndicate-collate-XXXXXX"

// add/remove files
int fs_entry_publish_file( struct fs_core* core, char const* fs_path, uint64_t version, mode_t mode );

// add/remove blocks
//int64_t fs_entry_republish_block( struct fs_core* core, char const* data_root, char const* publish_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t minimum_block_version );
int fs_entry_put_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_data );
int fs_entry_remove_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id );

// read/write blocks
ssize_t fs_entry_get_block_local( struct fs_core* core, int fd, char* block );
ssize_t fs_entry_write_block( struct fs_core* core, int fd, char* buf );
int fs_entry_commit_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* buf );

// stat block
int fs_entry_block_stat( struct fs_core* core, char const* path, uint64_t block_id, struct stat* sb );

// reintegrate remotely-written blocks
int fs_entry_collate( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* bits );
int fs_entry_release_staging( struct fs_core* core, Serialization::WriteMsg* accept_msg );

// manipulate local data
int fs_entry_create_local_file( struct fs_core* core, char const* fs_path, int64_t version, mode_t mode );
int fs_entry_create_local_directory( struct fs_core* core, char const* fs_path );
int fs_entry_move_local_data( char* path, char* new_path );
int fs_entry_truncate_local_data( struct fs_core* core, char const* fs_path, int64_t version );
int fs_entry_remove_local_data( struct fs_core* core, char const* fs_path, int64_t version );
int fs_entry_remove_local_directory( struct fs_core* core, char const* fs_path );
int fs_entry_reversion_local_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t new_version );

// withdraw
int fs_entry_withdraw_dir( struct fs_core* core, char const* path );

#endif