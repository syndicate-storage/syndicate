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

#ifndef _AG_REVERSIONER_H_
#define _AG_REVERSIONER_H_

#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include <set>

#include "AG.h"
#include "map-info.h"

using namespace std;

// prototypes
struct AG_state;

// pair a map_info to its path, so we can reversion it later
struct AG_path_map_info {
   
   char* path;
   
   struct AG_driver_publish_info* pubinfo;        // optionally sent on reversion
};

// comparator for AG_path_map_info
typedef struct {
   bool operator()(const struct AG_path_map_info& lmi, const struct AG_path_map_info& rmi) {
      // compare on path 
      return strcmp(lmi.path, rmi.path) < 0;
   }
} AG_map_info_deadline_comp;

// set of AG_path_map_info structures to track, ordered by deadline
typedef set<struct AG_path_map_info, AG_map_info_deadline_comp> AG_path_map_info_set_t;

struct AG_reversioner {

   // thread info
   bool running;
   pthread_t tid;
   
   // set of items to track
   pthread_mutex_t set_lock;
   AG_path_map_info_set_t* map_set;
   
   // reference to state
   struct AG_state* state;
   
   // semaphore that indicates that there is data available
   sem_t sem;
};

int AG_path_map_info_init( struct AG_path_map_info* pinfo, char const* path, struct AG_driver_publish_info* pubinfo );
int AG_path_map_info_dup( struct AG_path_map_info* new_pinfo, struct AG_path_map_info* old_pinfo );
int AG_path_map_info_free( struct AG_path_map_info* pinfo );

// init/start/stop/free
int AG_reversioner_init( struct AG_reversioner* reversioner, struct AG_state* state );
int AG_reversioner_start( struct AG_reversioner* reversioner );
int AG_reversioner_stop( struct AG_reversioner* reversioner );
int AG_reversioner_free( struct AG_reversioner* reversioner );

// map info 
int AG_reversioner_add_map_info( struct AG_reversioner* reversioner, char const* path, struct AG_driver_publish_info* pubinfo );
int AG_reversioner_add_map_infos( struct AG_reversioner* reversioner, AG_fs_map_t* map_infos );
int AG_reversioner_reversion_map_infos( struct AG_reversioner* reversioner );

#endif //_AG_REVERSIONER_H_

