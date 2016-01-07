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

#ifndef _LIBSYNDICATE_MS_GETATTR_
#define _LIBSYNDICATE_MS_GETATTR_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/path.h"
#include "libsyndicate/ms/url.h"

extern "C" {

int ms_client_getattr( struct ms_client* client, struct ms_path_ent* ms_ent, struct md_entry* ent_out );
int ms_client_getattr_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result );

int ms_client_getchild( struct ms_client* client, struct ms_path_ent* ms_ent, struct md_entry* ent_out );
int ms_client_getchild_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result );

int ms_client_getattr_request( struct ms_path_ent* ms_ent, uint64_t volume_id, uint64_t file_id, int64_t file_version, int64_t write_nonce, void* cls );
}

#endif
