/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
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
int http_handle_redirect( struct md_HTTP_response* resp, struct syndicate_state* state, char* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, struct timespec* manifest_timestamp, struct stat* sb, bool staging );
int http_get_redirect_url( char** redirect_url, struct syndicate_state* state, char* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, struct timespec* manifest_timestamp, struct stat* sb, bool staging );

int http_POST_iterator(void *coninfo_cls, enum MHD_ValueKind kind,
                       const char *key,
                       const char *filename, const char *content_type,
                       const char *transfer_encoding, const char *data,
                       uint64_t off, size_t size);

int http_GET_preliminaries( struct md_HTTP_response* resp,
                            char* url,
                            struct md_HTTP* http_ctx,
                            char** fs_path,
                            int64_t* file_version,
                            uint64_t* block_id,
                            int64_t* block_version,
                            struct timespec* manifest_timestamp,
                            bool* staging );

#endif