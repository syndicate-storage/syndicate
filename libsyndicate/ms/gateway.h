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

#ifndef _MS_CLIENT_GATEWAY_H_
#define _MS_CLIENT_GATEWAY_H_

#include "libsyndicate/ms/core.h"

extern "C" {

// peer verification
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len );

// get information about a specific gateway
int ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id );
uint64_t* ms_client_RG_ids( struct ms_client* client );
bool ms_client_is_AG( struct ms_client* client, uint64_t ag_id );
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id );
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* volume_id );
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, char** gateway_name );
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps );
int ms_client_gateway_key_pem( struct ms_client* client, char** buf, size_t* len );
int ms_client_get_closure_text( struct ms_client* client, char** closure_text, uint64_t* closure_len );
char** ms_client_RG_urls( struct ms_client* client, char const* scheme );
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t gateway_id );
char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t gateway_id );
char* ms_client_get_RG_content_url( struct ms_client* client, uint64_t gateway_id );

}

#endif 