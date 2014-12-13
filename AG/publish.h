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
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/path.h"

#include <algorithm>

#include "AG.h"

#include "map-info.h"

#define AG_REQUEST_USE_DRIVER               0x1
#define AG_REQUEST_DIRS_FIRST               0x2
#define AG_REQUEST_USE_DIRECTIVES           0x4
#define AG_REQUEST_SKIP_IF_CACHE_VALID      0x8

#define AG_REQUEST_MAX_RETRIES           5

// hierarchy management 
int AG_fs_publish_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_publish );
int AG_fs_update_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_update );
int AG_fs_delete_all( struct ms_client* client, AG_fs_map_t* map_infos, AG_fs_map_t* to_delete );

// metadata generation 
int AG_fs_publish_generate_metadata( AG_fs_map_t* to_publish );

// one-off methods
int AG_fs_publish( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* pubinfo );
int AG_fs_reversion( struct AG_fs* ag_fs, char const* path, struct AG_driver_publish_info* pubinfo );
int AG_fs_delete( struct AG_fs* ag_fs, char const* path );

#endif