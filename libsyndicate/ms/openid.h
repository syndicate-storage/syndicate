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

#ifndef _LIBSYNDICATE_OPENID_H_
#define _LIBSYNDICATE_OPENID_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/download.h"

/* DEPRECATED 
#define MS_OPENID_USERNAME_FIELD "openid_username"

#define MS_OPENID_MAX_RESPOSNE_LEN      102400          // 100KB

extern "C" {
   
int ms_client_openid_begin( CURL* curl, char const* username, char const* begin_url, ms::ms_openid_provider_reply* oid_reply, EVP_PKEY* syndicate_public_key );
int ms_client_openid_auth( CURL* curl, char const* username, char const* password, ms::ms_openid_provider_reply* oid_reply, char** return_to );
int ms_client_openid_complete( CURL* curl, char const* return_to_method, char const* return_to, char** response_body, size_t* response_body_len );

int ms_client_openid_session( CURL* curl, char const* openid_url, char const* username, char const* password, char** response_buf, size_t* response_len, EVP_PKEY* syndicate_public_key );

// generic access to MS RPC via OpenID
int ms_client_openid_auth_rpc( char const* ms_openid_url, char const* username, char const* password,
                               char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len,
                               char* syndicate_public_key_pem );

int ms_client_openid_rpc( char const* ms_openid_url, char const* username, char const* password,
                          char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len );

}
*/

#endif