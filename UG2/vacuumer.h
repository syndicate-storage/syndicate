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

struct UG_vacuum_context;
struct UG_vacuumer;

// queue of vacuum requests 
typedef queue<struct UG_vacuum_context*> UG_vacuum_queue_t;

extern "C" {

// allocators  
struct UG_vacuumer* UG_vacuumer_new();
struct UG_vacuum_context* UG_vacuum_context_new();

// set up a vacuumer 
int UG_vacuumer_init( struct UG_vacuumer* vacuumer, struct SG_gateway* gateway );

// start vacuuming 
int UG_vacuumer_start( struct UG_vacuumer* vacuumer );

// stop vacuuming 
int UG_vacuumer_quiesce( struct UG_vacuumer* vacuumer );
int UG_vacuumer_wait_all( struct UG_vacuumer* vacuumer );
int UG_vacuumer_stop( struct UG_vacuumer* vacuumer );

// shut down a vacuumer 
int UG_vacuumer_shutdown( struct UG_vacuumer* vacuumer );

// set up a vacuum context
int UG_vacuum_context_init( struct UG_vacuum_context* vctx, struct UG_state* ug, char const* fs_path, struct UG_inode* inode, struct SG_manifest* replaced_blocks );
int UG_vacuum_context_set_unlinking( struct UG_vacuum_context* vctx, bool unlinking );
int UG_vacuum_context_set_manifest_modtime( struct UG_vacuum_context* vctx, int64_t sec, int32_t nsec );

// free up a vacuum context 
int UG_vacuum_context_free( struct UG_vacuum_context* vctx );

// restore a vacuum context's state to an inode 
int UG_vacuum_context_restore( struct UG_vacuum_context* vctx, struct UG_inode* inode );

// start vacuuming
int UG_vacuumer_enqueue( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx );
int UG_vacuumer_enqueue_wait( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx );

// wait for a vacuum context to finish
int UG_vacuum_context_wait( struct UG_vacuum_context* vtcx );

// done vacuuming?
bool UG_vacuum_context_is_clean( struct UG_vacuum_context* vctx );

// synchronously vacuum
int UG_vacuum_run( struct UG_vacuumer* vacuumer, struct UG_vacuum_context* vctx );

}

#endif
