/*
   Copyright 2014 The Trustees of Princeton University

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

#ifndef _MS_CLIENT_VACUUM_H_
#define _MS_CLIENT_VACUUM_H_

#include "libsyndicate/ms/core.h"

// vacuum entry 
struct ms_vacuum_entry {
   
   // NOTE: covered with the vacuum_signature in the ms_client_request sent to the MS
   uint64_t volume_id;
   uint64_t writer_id;  // id of the gateway that committed the data
   uint64_t file_id;
   int64_t file_version;
   int64_t manifest_mtime_sec;
   int32_t manifest_mtime_nsec;
   
   uint64_t* affected_blocks;
   size_t num_affected_blocks;
   
};


extern "C" {
   
// vacuum API 
int ms_client_vacuum_entry_init( struct ms_vacuum_entry* vreq, uint64_t volume_id, uint64_t writer_id, uint64_t file_id, int64_t file_version,
                                 int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, uint64_t* affected_blocks, size_t num_affected_blocks );

int ms_client_vacuum_entry_set_blocks( struct ms_vacuum_entry* vreq, uint64_t* affected_blocks, size_t num_affected_blocks );
int ms_client_vacuum_entry_free( struct ms_vacuum_entry* vreq );

int ms_client_sign_vacuum_ticket( struct ms_client* client, struct ms_vacuum_entry* ve, unsigned char** sig, size_t* sig_len );
int ms_client_verify_vacuum_ticket( struct ms_client* client, ms::ms_vacuum_ticket* vt );

int ms_client_peek_vacuum_log( struct ms_client* client, uint64_t volume_id, uint64_t file_id, struct ms_vacuum_entry* ve );
int ms_client_remove_vacuum_log_entry( struct ms_client* client, uint64_t volume_id, uint64_t writer_id, uint64_t file_id, uint64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec );
int ms_client_append_vacuum_log_entry( struct ms_client* client, struct ms_vacuum_entry* ve );

}

#endif
