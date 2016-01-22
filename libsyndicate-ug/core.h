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

#ifndef UG_DEFAULT_DRIVER_EXEC_STR
#define UG_DEFAULT_DRIVER_EXEC_STR "/usr/local/lib/syndicate/ug-driver"
#endif

#define UG_RG_REQUEST_NOT_STARTED     0
#define UG_RG_REQUEST_IN_PROGRESS     1
#define UG_RG_REQUEST_SUCCESS         2

// prototypes...
struct UG_vacuumer;

// global UG state
struct UG_state;

// RG RPC state (for replication and vacuuming)
struct UG_RG_context;

extern "C" {
   
int UG_state_list_replica_gateway_ids( struct UG_state* state, uint64_t** replica_gateway_ids, size_t* num_replica_gateway_ids );
int UG_state_reload_replica_gateway_ids( struct UG_state* state );

struct UG_RG_context* UG_RG_context_new();
int UG_RG_context_init( struct UG_state* state, struct UG_RG_context* rctx );
int UG_RG_context_free( struct UG_RG_context* rctx );
uint64_t* UG_RG_context_RG_ids( struct UG_RG_context* rctx );
size_t UG_RG_context_num_RGs( struct UG_RG_context* rctx );
int UG_RG_context_get_status( struct UG_RG_context* rctx, int i );
int UG_RG_context_set_status( struct UG_RG_context* rctx, int i, int status );
int UG_RG_send_all( struct SG_gateway* gateway, struct UG_RG_context* rctx, SG_messages::Request* controlplane_request, struct SG_chunk* dataplane_request );

int UG_state_rlock( struct UG_state* state );
int UG_state_wlock( struct UG_state* state );
int UG_state_unlock( struct UG_state* state );

// core init and shutdown 
struct UG_state* UG_init( int argc, char** argv, bool client );
struct UG_state* UG_init_ex( int argc, char** argv, struct md_opts* overrides, void* cls );
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
void* UG_state_cls( struct UG_state* state );
int UG_state_stat_rh( struct UG_state* state );
int UG_state_creat_rh( struct UG_state* state );
int UG_state_mkdir_rh( struct UG_state* state );
int UG_state_open_rh( struct UG_state* state );
int UG_state_read_rh( struct UG_state* state );
int UG_state_write_rh( struct UG_state* state );
int UG_state_trunc_rh( struct UG_state* state );
int UG_state_close_rh( struct UG_state* state );
int UG_state_sync_rh( struct UG_state* state );
int UG_state_detach_rh( struct UG_state* state );
int UG_state_rename_rh( struct UG_state* state );

// setters 
void UG_state_set_cls( struct UG_state* state, void* cls );
int UG_state_set_stat_rh( struct UG_state* state, int rh );
int UG_state_set_creat_rh( struct UG_state* state, int rh );
int UG_state_set_mkdir_rh( struct UG_state* state, int rh );
int UG_state_set_open_rh( struct UG_state* state, int rh );
int UG_state_set_read_rh( struct UG_state* state, int rh );
int UG_state_set_write_rh( struct UG_state* state, int rh );
int UG_state_set_trunc_rh( struct UG_state* state, int rh );
int UG_state_set_close_rh( struct UG_state* state, int rh );
int UG_state_set_sync_rh( struct UG_state* state, int rh );
int UG_state_set_detach_rh( struct UG_state* state, int rh );
int UG_state_set_rename_rh( struct UG_state* state, int rh );


}

#endif 
