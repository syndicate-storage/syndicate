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

#ifndef _AG_PUBLISH_H_
#define _AG_PUBLISH_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/ms/ms-client.h"

#include "AG.h"

#include "map-info.h"

#define AG_REQUEST_USE_DRIVER               0x1
#define AG_REQUEST_DIRS_FIRST               0x2
#define AG_REQUEST_USE_DIRECTIVES           0x4

#define AG_REQUEST_DELETE_MAX_RETRIES           5

struct AG_request_stage;

typedef vector<struct AG_request_stage*> AG_request_stage_list_t;
typedef vector<struct ms_client_request> AG_request_list_t;

typedef int (*AG_request_stage_generator_t)( struct ms_client*, AG_fs_map_t*, AG_fs_map_t*, AG_request_stage_list_t* );

typedef map<uint64_t, char*> AG_request_parent_map_t;

typedef set<int> AG_operational_error_set_t;

// bundle of requests to execute at a given depth
struct AG_request_stage {
   
   int file_oper;                   // MS operation to perform for the files 
   int dir_oper;                    // MS operation to perform for the directories
   
   int flags;                       // operation flags (i.e. for xattrs)
   
   int depth;                           // what depth is this stage at?
   AG_request_list_t* file_reqs;        // requests over files
   AG_request_list_t* dir_reqs;         // requests over directories
   AG_request_parent_map_t* parents;        // map file and directory requests to their parents' paths
   
   bool dirs_first;                     // run directories first if true.  Otherwise run files first.
   
   struct ms_client_multi_result* results;       // results of operation
   
   int error;                           // set to non-zero if there was a failure
   AG_request_list_t* failed_reqs;      // points to either file_reqs or dir_reqs of one of them failed to process 
};

// hierarchy management 
int AG_fs_create_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_publish, AG_fs_map_t* mi_reference );
int AG_fs_update_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_update, AG_fs_map_t* mi_reference );
int AG_fs_delete_all( struct ms_client* client, AG_fs_map_t* dest, AG_fs_map_t* to_delete, AG_fs_map_t* mi_reference );

int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* pubinfo );

#endif