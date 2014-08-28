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

#ifndef _AG_CORE_H_
#define _AG_CORE_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/ms-client.h"
#include "libsyndicate/system.h"
#include "libsyndicate/storage.h"
#include "libsyndicate/opts.h"

#include "AG.h"

#include <getopt.h>
#include <ftw.h>
#include <dlfcn.h>

// prototypes
struct AG_event_listener;
struct AG_reversioner;
struct AG_driver;

// AG-specific options 
struct AG_opts {
   char* sock_path;
   char* logfile_path;
   char* driver_dir;
   char* spec_file_path;
};

// AG core state
struct AG_state {
   struct md_syndicate_conf* conf;
   
   struct ms_client* ms;
   struct md_HTTP* http;
   struct AG_event_listener* event_listener;
   struct AG_reversioner* reversioner;
   struct AG_fs* ag_fs;
   struct AG_opts ag_opts;
   AG_config_t* config;
   
   AG_driver_map_t* drivers;
   
   bool running;
   
   pthread_rwlock_t fs_lock;            // lock guarding access to ag_fs
   pthread_rwlock_t config_lock;        // lock guarding access to config 
   
   // thread for reloading/republishing the specfile
   pthread_t specfile_reload_thread;
   bool specfile_reload_thread_running;
   sem_t specfile_reload_sem;
   
   // lock for acquiring/releasing state references 
   pthread_rwlock_t state_lock;
   bool referenceable;   // set to true to allow AG_get_state() and AG_release_state() to be called
   
   // main thread waits on this semaphore to exit 
   sem_t running_sem;
};


int AG_state_init( struct AG_state* state, struct md_opts* opts, struct AG_opts* ag_opts, struct md_syndicate_conf* conf, struct ms_client* client );
int AG_state_free( struct AG_state* state );
int AG_start( struct AG_state* state );
int AG_stop( struct AG_state* state );

int AG_state_fs_rlock( struct AG_state* state );
int AG_state_fs_wlock( struct AG_state* state );
int AG_state_fs_unlock( struct AG_state* state );

// referencing state
struct AG_state* AG_get_state();
void AG_release_state( struct AG_state* state );

int AG_main( int argc, char** argv );

void AG_dump_config( AG_config_t* config );
char* AG_get_config_var( struct AG_state* state, char const* varname );


#define AG_DEFAULT_CONFIG_PATH "/etc/syndicate/syndicate-gateway-server.conf"

#endif

