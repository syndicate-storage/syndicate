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

// core state and control for Syndicate

#ifndef _STATE_H_
#define _STATE_H_

#include "libsyndicate/libsyndicate.h"
#include "stats.h"
#include "replication.h"
#include "fs.h"
#include "cache.h"

struct syndicate_state {
   FILE* logfile;
   FILE* replica_logfile;
   
   struct ms_client* ms;   // metadata service client
   struct fs_core* core;   // core of the system
   struct syndicate_cache cache;        // local cache
   struct syndicate_replication replication;            // replication context
   struct syndicate_replication garbage_collector;      // garbage collector context

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

int syndicate_init_state( struct syndicate_state* state, struct ms_client* ms );
int syndicate_set_running( struct syndicate_state* state, int running );
int syndicate_destroy_state( struct syndicate_state* state, int wait_replicas );

#endif