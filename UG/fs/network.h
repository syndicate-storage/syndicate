/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#ifndef _NETWORK_H_
#define _NETWORK_H_

#include "fs_entry.h"
#include "serialization.pb.h"


int fs_entry_download_cached( struct fs_core* core, char const* url, char** bits, ssize_t* ret_len );
int fs_entry_download_manifest( struct fs_core* core, char const* manifest_url, Serialization::ManifestMsg* mmsg );
ssize_t fs_entry_download_block( struct fs_core* core, char const* block_url, char* block_bits );

int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type );
int fs_entry_prepare_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, char* fs_path, struct fs_entry* fent, uint64_t start_id, uint64_t end_id, int64_t* versions );

int fs_entry_post_write( Serialization::WriteMsg* recvMsg, struct fs_core* core, char* url, Serialization::WriteMsg* sendMsg );

#endif 