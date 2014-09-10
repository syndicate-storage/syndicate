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

#ifndef _MS_CLIENT_URL_H_
#define _MS_CLIENT_URL_H_

#include "libsyndicate/ms/core.h"

extern "C" {

char* ms_client_url( char const* ms_url, uint64_t volume_id, char const* metadata_path );

char* ms_client_file_url( char const* ms_url, uint64_t volume_id );
char* ms_client_file_read_url( char const* ms_url, uint64_t volume_id, uint64_t file_id, int64_t version, int64_t write_nonce );

char* ms_client_getxattr_url( char const* ms_url, uint64_t volume_id, uint64_t file_id, char const* xattr_name );
char* ms_client_listxattr_url( char const* ms_url, uint64_t volume_id, uint64_t file_id );

char* ms_client_vacuum_url( char const* ms_url, uint64_t volume_id, uint64_t file_id );

char* ms_client_volume_url( char const* ms_url, uint64_t volume_id );
char* ms_client_volume_url_by_name( char const* ms_url, char const* name );

char* ms_client_public_key_register_url( char const* ms_url );
char* ms_client_openid_register_url( char const* ms_url, int gateway_type, char const* gateway_name, char const* username );

char* ms_client_openid_rpc_url( char const* ms_url );

char* ms_client_syndicate_pubkey_url( char const* ms_url );

char* ms_client_cert_url( char const* ms_url, uint64_t volume_id, uint64_t volume_cert_version, int gateway_type, uint64_t gateway_id, uint64_t gateway_cert_version );
char* ms_client_cert_manifest_url( char const* ms_url, uint64_t volume_id, uint64_t volume_cert_version );

}

#endif 