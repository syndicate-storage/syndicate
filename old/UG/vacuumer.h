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

#ifndef _VACUUMER_H_
#define _VACUUMER_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/download.h"
#include "libsyndicate/ms/ms-client.h"
#include "fs_entry.h"
#include "replication.h"

#define VACUUM_AGAIN 0
#define VACUUM_DONE 1
#define VACUUM_HEAD 2

#define VACUUM_TYPE_WRITE 1                // vacuum a write to a file
#define VACUUM_TYPE_LOG 2                  // only remove the vacuum log entry for a file

typedef map<struct replica_context*, int> completion_map_t;

struct fs_vacuumer_request {
   int type;                    // one of VACUUM_TYPE_*
   char* fs_path;
   struct replica_snapshot fent_snapshot;
};

struct fs_vacuumer_request_comp {
   
   bool operator()( const struct fs_vacuumer_request& v1, const struct fs_vacuumer_request& v2 ) {
      return memcmp( &v1, &v2, sizeof(struct fs_vacuumer_request) ) < 0 ? true : false;
   }
};

typedef set<struct fs_vacuumer_request, fs_vacuumer_request_comp> vacuum_set_t;

// collect un-garbage-collected data
struct fs_vacuumer {
   
   struct fs_core* core;
   
   // list of vacuum requests to process
   vacuum_set_t* vacuum_set;
   pthread_rwlock_t vacuum_set_lock;
   
   // list of vacuum requests to insert into the queue 
   vacuum_set_t* vacuum_pending;
   vacuum_set_t* vacuum_pending_1;
   vacuum_set_t* vacuum_pending_2;
   
   pthread_rwlock_t vacuum_pending_lock;
   
   pthread_t thread;
   bool running;
};

int fs_entry_vacuumer_init( struct fs_vacuumer* vac, struct fs_core* core );
int fs_entry_vacuumer_shutdown( struct fs_vacuumer* vac );

int fs_entry_vacuumer_start( struct fs_vacuumer* vac );
int fs_entry_vacuumer_stop( struct fs_vacuumer* vac );

// vacuum in the background 
int fs_entry_vacuumer_write_bg( struct fs_vacuumer* vac, char const* fs_path, struct replica_snapshot* snapshot );
int fs_entry_vacuumer_write_bg_fent( struct fs_vacuumer* vac, char const* fs_path, struct fs_entry* fent );
int fs_entry_vacuumer_log_entry_bg( struct fs_vacuumer* vac, char const* fs_path, struct replica_snapshot* snapshot );
int fs_entry_vacuumer_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent );

// update vacuum state 
bool fs_entry_vacuumer_is_vacuuming( struct fs_entry* fent );
bool fs_entry_vacuumer_is_vacuumed( struct fs_entry* fent );
#endif