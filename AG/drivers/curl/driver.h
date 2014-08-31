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

#ifndef _AG_CURL_DRIVER_H_
#define _AG_CURL_DRIVER_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <string>
#include <sstream>

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/closure.h"
#include "libsyndicate/util.h"

#include "AG/driver.h"


#define AG_CURL_DRIVER_CONFIG_DEFAULT_MAX_BLOCKS 1024

// curl driver state 
struct curl_driver_state {
   
   char* cache_root;
};

// curl write context 
struct curl_write_context {
   
   char* buf;
   uint64_t buf_len;
   uint64_t num_written;
};

// curl connection context 
struct curl_connection_context {
   char* request_path;
   char* url;
   struct curl_driver_state* state;
};

extern "C" {

int driver_init( void** driver_state );
int driver_shutdown( void* driver_state );

int connect_dataset_block( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_connection_state );
int connect_dataset_manifest( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_connection_state );

int close_dataset_block( void* driver_connection_state );
int close_dataset_manifest( void* driver_connection_state );

int get_dataset_manifest_info( struct AG_connection_context* ag_ctx, struct AG_driver_publish_info* pub_info, void* driver_connection_state );
ssize_t get_dataset_block( struct AG_connection_context* ag_ctx, uint64_t block_id, char* block_buf, size_t buf_len, void* driver_connection_state );

int stat_dataset( char const* path, struct AG_map_info* map_info, struct AG_driver_publish_info* pub_info, void* driver_state );

int handle_event( char* event_payload, size_t event_payload_len, void* driver_state );

char* get_query_type(void);

}


#endif