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

#ifndef _SYNDICATE_RG_H_
#define _SYNDICATE_RG_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>
#include <libsyndicate/client.h>
#include <libsyndicate/opts.h>

#define RG_DEFAULT_EXEC       "/usr/local/lib/syndicate/rg-driver"

#define RG_ROLE_READ          0
#define RG_ROLE_WRITE         1
#define RG_ROLE_DELETE        2
#define RG_NUM_ROLES          3

#define SYNDICATE_UG 1
#define SYNDICATE_RG 2
#define SYNDICATE_AG 3

extern "C" {
   
struct RG_core;

char* RG_core_lookup_exec_str( struct RG_core* rg );
char* RG_core_get_exec_str( struct RG_core* rg );

int RG_core_rlock( struct RG_core* core );
int RG_core_wlock( struct RG_core* core );
int RG_core_unlock( struct RG_core* core );

struct SG_proc_group* RG_core_get_proc_group( struct RG_core* core, int role );
int RG_core_install_procs( struct RG_core* core, struct SG_proc_group** groups, char* exec_str );

struct SG_gateway* RG_core_gateway( struct RG_core* core );

}
#endif
