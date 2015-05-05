/*
   Copyright 2015 The Trustees of Princeton University

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

#ifndef _UG_VACUUMER_H_
#define _UG_VACUUMER_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/client.h>
#include <libsyndicate/ms/vacuum.h>

#include "inode.h"

struct UG_state;

// state for vacuuming data
struct UG_vacuum_context {
   
   struct timespec old_manifest_timestamp;      // timestamp of the old manifest (to be vacuumed)
   struct SG_manifest old_blocks;               // old info for blocks to be vacuumed
   ms::ms_reply* vacuum_ticket;                 // ticket from the MS to pass along to the RG to prove that we can vacuum
   
   char* fs_path;                               // path to the inode
   struct md_entry inode_data;                  // exported inode
   
   struct UG_block_gateway_pair* vacuum_queue;  // list of {block IDs} X {gateway IDs} pairs to remove
   size_t vacuum_queue_len;
   
   bool have_old_manifest_timestamp;            // set to true if old_manifest_timestamp is set (otherwise, we'll get it from the MS)
   bool have_old_blocks;                        // set to true if old_blocks is set up and populated (otherwise, we'll get it from the RGs)
   bool vacuumed_old_blocks;                    // set to true if we vacuumed old blocks 
   
   int64_t delay;                               // delay delta for retry_deadline
   struct timespec retry_deadline;              // earliest time in the future when we can try this context again (if it failed) 
};

// queue of vacuum requests 
typedef queue<struct UG_vacuum_context*> UG_vacuum_queue_t;

// global vacuum state 
struct UG_vacuumer {
   
   pthread_t thread;
   
   UG_vacuum_queue_t* vacuum_queue;             // queue of vacuum requests to perform
   pthread_rwlock_t lock;                       // lock governing access to the vacuum queue 
   
   sem_t sem;                                   // used to wake up the vacuumer when there's work to be done
   
   volatile bool running;                       // is this thread running?
   
   struct SG_gateway* gateway;                  // parent gateway
};

// set up a vacuumer 
int UG_vacuumer_init( struct UG_vacuumer* vacuumer, struct SG_gateway* gateway );

// start vacuuming 
int UG_vacuumer_start( struct UG_vacuumer* vacuumer );

// stop vacuuming 
int UG_vacuumer_stop( struct UG_vacuumer* vacuumer );

// shut down a vacuumer 
int UG_vacuumer_shutdown( struct UG_vacuumer* vacuumer );

// set up a vacuum context 
int UG_vacuum_context_init( struct UG_vacuum_context* vctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* replaced_blocks );

// free up a vacuum context 
int UG_vacuum_context_free( struct UG_vacuum_context* vctx );

// restore a vacuum context's state to an inode 
int UG_vacuum_context_restore( struct UG_vacuum_context* vctx, struct UG_inode* inode );

// start vacuuming
int UG_vacuumer_enqueue( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx );

// synchronously vacuum
int UG_vacuum_run( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx );

#endif