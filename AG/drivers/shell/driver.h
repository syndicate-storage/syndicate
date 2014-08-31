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

#ifndef _AG_SHELL_DRIVER_H_
#define _AG_SHELL_DRIVER_H_

#include <map>
#include <string>
#include <set>
#include <sstream>
#include <algorithm>

#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <math.h>

#include "libsyndicate/libsyndicate.h"
#include "AG/driver.h"
#include "proc-handler.h"

using namespace std;

// driver-wide state 
struct shell_driver_state {
   
   proc_table_t* running;                       // set of running processes
   pthread_rwlock_t running_lock;               // lock governing access to running
   
   cache_table_t* cache_table;                  // cached data for requests
   pthread_rwlock_t cache_lock;                 // lock governing access to cache
   
   char* storage_root;                          // root directory for storing stdout/stderr data and cached data
   
   bool is_running;                             // set to true if we're running the inotify thread
};

// process connection context
struct proc_connection_context {
    char* request_path;         // requested path
    char* shell_cmd;
    
    // pointer to global driver state 
    struct shell_driver_state*  state;
};

struct shell_driver_state* shell_driver_get_state();

extern "C" {
   
// AG driver interface
int driver_init( void** driver_state );
int driver_shutdown( void* driver_state );
int get_dataset_manifest_info( struct AG_connection_context* ag_ctx, struct AG_driver_publish_info* pubinfo, void* driver_conn_state );
ssize_t get_dataset_block( struct AG_connection_context* ag_ctx, uint64_t block_id, char* block_buf, size_t size, void* driver_conn_state );
int connect_dataset_block( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_conn_state );
int connect_dataset_manifest( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_conn_state );
int close_dataset_block( void* driver_conn_state );
int close_dataset_manifest( void* driver_conn_state );
int stat_dataset( char const* path, struct AG_map_info* mi, struct AG_driver_publish_info* pubinfo, void* driver_state );
int reversion_dataset( char const* path, struct AG_map_info* mi, void* driver_state );
int handle_event( char* event_buf, size_t event_len, void* driver_state );
char* get_query_type(void);

}

#endif //_AG_SHELL_DRIVER_H_

