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

#ifndef _MS_CLIENT_BENCHMARK_H_
#define _MS_CLIENT_BENCHMARK_H_

#include "libsyndicate/libsyndicate.h"


// benchmarking HTTP headers
#define HTTP_VOLUME_TIME   "X-Volume-Time"
#define HTTP_GATEWAY_TIME  "X-Gateway-Time"
#define HTTP_TOTAL_TIME    "X-Total-Time"
#define HTTP_RESOLVE_TIME  "X-Resolve-Time"
#define HTTP_CREATE_TIMES  "X-Create-Times"
#define HTTP_UPDATE_TIMES  "X-Update-Times"
#define HTTP_DELETE_TIMES  "X-Delete-Times"
#define HTTP_GETXATTR_TIMES "X-Getxattr-Times"
#define HTTP_SETXATTR_TIMES "X-Setattr-Times"
#define HTTP_LISTXATTRS_TIMES "X-Listattrs-Times"
#define HTTP_REMOVEXATTRS_TIMES "X-Removexattrs-Times"
#define HTTP_MS_LASTMOD    "X-MS-LastMod"

// benchmarking structure
struct ms_client_timing {
   uint64_t total_time;
   uint64_t volume_time;
   uint64_t ug_time;

   uint64_t* create_times;
   size_t num_create_times;

   uint64_t* update_times;
   size_t num_update_times;

   uint64_t* delete_times;
   size_t num_delete_times;

   uint64_t resolve_time;
};

extern "C" {

size_t ms_client_timing_header_func( void *ptr, size_t size, size_t nmemb, void *userdata);
int ms_client_timing_log( struct ms_client_timing* times );
int ms_client_timing_free( struct ms_client_timing* times );

}

#endif 