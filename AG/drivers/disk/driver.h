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

#ifndef _AG_DISK_DRIVER_H_
#define _AG_DISK_DRIVER_H_

#include <map>
#include <string>
#include <sstream>

#include <sys/types.h>
#include <errno.h>

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/closure.h"
#include "AG/driver.h"

#define AG_CONFIG_DISK_DATASET_ROOT "dataset_root"

using namespace std;

// connection context for reading from disk
struct AG_disk_context {
   // input descriptor
   int fd;
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

int stat_dataset( char const* path, struct AG_map_info* ag_dataset_info, struct AG_driver_publish_info* pub_info, void* driver_state );

int handle_event( char* event_payload, size_t event_payload_len, void* driver_state );

char* get_query_type(void);

}

#endif //_AG_DISK_DRIVER_H_

