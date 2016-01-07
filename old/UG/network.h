/*
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _NETWORK_H_
#define _NETWORK_H_

#include "fs_entry.h"
#include "libsyndicate/serialization.pb.h"
#include "libsyndicate/url.h"

// downloading
int fs_entry_download_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, char const* manifest_url, Serialization::ManifestMsg* mmsg );

int fs_entry_download_manifest_replica( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec,
                                        Serialization::ManifestMsg* mmsg, uint64_t* successful_RG_id );

// get a manifest
int fs_entry_get_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec,
                           Serialization::ManifestMsg* manifest_msg, uint64_t* successful_gateway_id );

// write messages
int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type );

int fs_entry_prepare_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, char const* fs_path, struct replica_snapshot* fent_snapshot, int64_t write_nonce, modification_map* dirty_blocks );

int fs_entry_prepare_truncate_message( Serialization::WriteMsg* truncate_msg, char const* fs_path, struct fs_entry* fent, uint64_t new_max_block );

int fs_entry_prepare_rename_message( Serialization::WriteMsg* rename_msg, char const* old_path, char const* new_path, struct fs_entry* old_fent, int64_t version );

int fs_entry_prepare_detach_message( Serialization::WriteMsg* detach_msg, char const* fs_path, struct fs_entry* fent, int64_t version );

// sending write messages
int fs_entry_post_write( Serialization::WriteMsg* recvMsg, struct fs_core* core, uint64_t gateway_id, Serialization::WriteMsg* sendMsg );

// coordination
int fs_entry_send_write_or_coordinate( struct fs_core* core, char const* fs_path, struct fs_entry* fent, Serialization::WriteMsg* write_msg, Serialization::WriteMsg* write_ack );

#endif 