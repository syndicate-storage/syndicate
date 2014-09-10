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

// simple interface for a Syndicate UG.

#ifndef _SYNDICATE_H_
#define _SYNDICATE_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/opts.h"
#include "libsyndicate/cache.h"
#include "stats.h"
#include "replication.h"
#include "fs.h"
#include "vacuumer.h"

#define UG_SHORTOPTS "al:L:F"

#define UG_CACHE_DEFAULT_SOFT_LIMIT        50000000        // 50 MB
#define UG_CACHE_DEFAULT_HARD_LIMIT       100000000        // 100 MB

// UG-specific command-line options 
struct UG_opts {
   
   uint64_t cache_soft_limit;
   uint64_t cache_hard_limit;
   bool anonymous;
   bool flush_replicas;
};
   
struct syndicate_state {
   FILE* logfile;
   
   struct ms_client* ms;   // metadata service client
   struct fs_core* core;   // core of the system
   struct md_syndicate_cache cache;        // local cache
   struct rg_client replication;            // replication context
   struct rg_client garbage_collector;      // garbage collector context
   struct fs_vacuumer vac;              // vacuumer
   struct md_downloader dl;             // downloader for this client
   struct UG_opts ug_opts;              // UG-specific command-line options

   // mounter info (since apparently FUSE doesn't do this right)
   int gid;
   int uid;

   // when was the filesystem started?
   time_t mounttime;

   // configuration
   struct md_syndicate_conf conf;

   // global running flag
   int running;

   // statistics
   Stats* stats;
};

extern "C" {
   
int syndicate_init( struct md_opts* opts, struct UG_opts* ug_opts );

void syndicate_set_running();
int syndicate_set_running_ex( struct syndicate_state* state, int running );

struct syndicate_state* syndicate_get_state();
struct md_syndicate_conf* syndicate_get_conf();

int syndicate_destroy( int wait_replicas );
int syndicate_destroy_ex( struct syndicate_state* state, int wait_replicas );

int UG_opts_init(void);
void UG_usage(void);
int UG_handle_opt( int opt_c, char* opt_s );
int UG_opts_get( struct UG_opts* opts );
}

int syndicate_setup_state( struct syndicate_state* state, struct ms_client* ms );

#endif
