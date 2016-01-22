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

#ifndef _EXEC_HANDLER_H_
#define _EXEC_HANDLER_H_

#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <sys/wait.h>

#include <string.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <set>

#include "libsyndicate/libsyndicate.h"

using namespace std;

// structure representing a running process
struct proc_table_entry {
   char*       request_path;           // path in the AG that this process is running for
   char*       stdout_path;            // path to this process's stdout on disk
   pid_t       pid;                    // shell process to wait on 
   bool        valid;                  // whether or not this structure is ready for usage
   
   pthread_rwlock_t pte_lock;          // lock governing access to this structure
}; 

// structure representing a block being served back from a running process
struct proc_block_status { 
    bool    in_progress;                
    bool    block_available;
    bool    no_file;
    bool    need_padding;
    off_t   written_so_far;
};


// map PIDs to running processes
typedef map<pid_t, struct proc_table_entry*> proc_table_t;

// map request paths to names of cached data
typedef map<string, string> cache_table_t;

void proc_sigchld_handler(int signum);

int shell_driver_state_init( struct shell_driver_state* state );
int shell_driver_state_start( struct shell_driver_state* state );
int shell_driver_state_stop( struct shell_driver_state* state );
int shell_driver_state_free( struct shell_driver_state* state );

int proc_ensure_has_data( struct shell_driver_state* state, struct proc_connection_context* ctx );
bool proc_is_generating_data( struct shell_driver_state* state, char const* request_path );
bool proc_finished_generating_data( struct shell_driver_state* state, char const* request_path );
int proc_stat_data( struct shell_driver_state* state, char const* request_path, struct stat* sb );
int proc_read_block_data( struct shell_driver_state* state, char const* request_path, uint64_t block_id, char* buf, ssize_t read_size );

int proc_evict_cache( struct shell_driver_state* state, char const* request_path );

struct shell_driver_state* shell_driver_get_state();
void shell_driver_set_state( struct shell_driver_state* state );

#endif //_EXEC_HANDLER_H_

