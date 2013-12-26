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

#ifndef _HTTP_COMMON_H_
#define _HTTP_COMMON_H_

#include "libsyndicate.h"
#include "syndicate.h"

#define HTTP_REDIRECT_HANDLED 0
#define HTTP_REDIRECT_NOT_HANDLED 1
#define HTTP_REDIRECT_REMOTE 2

char* http_validate_url_path( struct md_HTTP* http, char* url, struct md_HTTP_response* resp );

bool http_file_exists( struct syndicate_state* state, char* url_path, struct stat* sb );
bool http_block_exists( struct syndicate_state* state, char* file_path, uint64_t block_id, struct stat* sb );

void http_io_error_resp( struct md_HTTP_response* resp, int err, char const* msg );

int http_make_redirect_response( struct md_HTTP_response* resp, char* new_url );
int http_make_default_headers( struct md_HTTP_response* resp, time_t last_modified, size_t size, bool cacheable );
int http_process_redirect( struct syndicate_state* state, char** redirect_url, struct stat* sb, struct gateway_request_data* reqdat );
int http_handle_redirect( struct syndicate_state* state, struct md_HTTP_response* resp, struct stat* sb, struct gateway_request_data* reqdat );

int http_POST_iterator(void *coninfo_cls, enum MHD_ValueKind kind,
                       const char *key,
                       const char *filename, const char *content_type,
                       const char *transfer_encoding, const char *data,
                       uint64_t off, size_t size);

int http_parse_request( struct md_HTTP* http_ctx, struct md_HTTP_response* resp, struct gateway_request_data* reqdat, char* url );

#endif