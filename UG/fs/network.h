/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _NETWORK_H_
#define _NETWORK_H_

#include "fs_entry.h"
#include "serialization.pb.h"


int fs_entry_download_manifest( struct fs_core* core, uint64_t origin, char const* manifest_url, Serialization::ManifestMsg* mmsg );
ssize_t fs_entry_download_block( struct fs_core* core, char const* block_url, char** block_bits, size_t block_len );

int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type );
int fs_entry_prepare_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_id, uint64_t end_id, int64_t* versions );

int fs_entry_post_write( Serialization::WriteMsg* recvMsg, struct fs_core* core, uint64_t gateway_id, Serialization::WriteMsg* sendMsg );

int fs_entry_download_block_replica( struct fs_core* core, uint64_t volume_id, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, char** block_bits, size_t block_len, uint64_t* successful_RG_id );

int fs_entry_download_manifest_replica( struct fs_core* core, uint64_t origin,
                                        uint64_t volume_id, uint64_t file_id, int64_t file_version, int64_t mtime_sec, int32_t mtime_nsec,
                                        Serialization::ManifestMsg* mmsg, uint64_t* successful_RG_id );

#endif 