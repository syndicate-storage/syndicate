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

#ifndef _UG_CORE_H_
#define _UG_CORE_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/client.h>
#include <libsyndicate/opts.h>

#include <fskit/fskit.h>

#define UG_DEFAULT_DRIVER_EXEC_STR "/usr/local/lib/syndicate/ug-driver"

// prototypes...
struct UG_vacuumer;

// global UG state
struct UG_state;

extern "C" {
   
int UG_state_list_replica_gateway_ids( struct UG_state* state, uint64_t** replica_gateway_ids, size_t* num_replica_gateway_ids );
int UG_state_reload_replica_gateway_ids( struct UG_state* state );

int UG_state_rlock( struct UG_state* state );
int UG_state_wlock( struct UG_state* state );
int UG_state_unlock( struct UG_state* state );

// core init and shutdown 
struct UG_state* UG_init( int argc, char** argv, bool client );
int UG_start( struct UG_state* state );
int UG_main( struct UG_state* state );
int UG_shutdown( struct UG_state* state );

// getters 
struct SG_gateway* UG_state_gateway( struct UG_state* state );
struct fskit_core* UG_state_fs( struct UG_state* state );
struct UG_vacuumer* UG_state_vacuumer( struct UG_state* state );
uint64_t UG_state_owner_id( struct UG_state* state );
uint64_t UG_state_volume_id( struct UG_state* state );
struct md_wq* UG_state_wq( struct UG_state* state );
struct SG_driver* UG_state_driver( struct UG_state* state );

}

#endif 
