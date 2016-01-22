/*
   Copyright 2016 The Trustees of Princeton University

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

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/client.h>
#include <libsyndicate/opts.h>
#include <libsyndicate/proc.h>

#include <libsyndicate-ug/core.h>

#include <fskit/fskit.h>

#ifndef AG_DEFAULT_DRIVER_EXEC_STR
#define AG_DEFAULT_DRIVER_EXEC_STR "/usr/local/lib/syndicate/ag-driver"
#endif

extern "C" {

struct AG_state;

struct AG_state* AG_init( int argc, char** argv );
int AG_start( struct AG_state* state );
int AG_main( struct AG_state* state );
int AG_shutdown( struct AG_state* state );

struct SG_gateway* AG_state_gateway( struct AG_state* state );
struct UG_state* AG_state_ug( struct AG_state* state );
struct fskit_core* AG_state_fs( struct AG_state* state ); 

int AG_state_rlock( struct AG_state* state );
int AG_state_wlock( struct AG_state* state );
int AG_state_unlock( struct AG_state* state );

}
#endif
