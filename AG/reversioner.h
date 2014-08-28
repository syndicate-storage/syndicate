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
   struct AG_map_info mi;
   char* path;
};

// comparator for AG_map_info, by refresh deadline first, and then using the path hash to break ties
typedef struct {
   bool operator()(const struct AG_path_map_info& lmi, const struct AG_path_map_info& rmi) {
      if( lmi.mi.refresh_deadline != rmi.mi.refresh_deadline ) {
         return lmi.mi.refresh_deadline < rmi.mi.refresh_deadline;
      }
      else {
         return lmi.mi.path_hash < rmi.mi.path_hash;
      }
   }
} AG_map_info_deadline_comp;

// set of AG_path_map_info structures to track, ordered by deadline
typedef set<struct AG_path_map_info, AG_map_info_deadline_comp> AG_deadline_map_info_set_t;

struct AG_reversioner {

   // thread info
   bool running;
   pthread_t tid;
   
   // set of items to track
   pthread_mutex_t set_lock;
   AG_deadline_map_info_set_t* map_set;
   
   // reference to state
   struct AG_state* state;

};

int AG_path_map_info_init( struct AG_path_map_info* pinfo, char const* path, const struct AG_fs_map_info* mi );
int AG_path_map_info_dup( struct AG_path_map_info* new_pinfo, const struct AG_path_map_info* old_pinfo );
int AG_path_map_info_free( struct AG_path_map_info* pinfo );

// init/start/stop/free
int AG_reversioner_init( struct AG_reversioner* reversioner, struct AG_state* state );
int AG_reversioner_start( struct AG_reversioner* reversioner );
int AG_reversioner_stop( struct AG_reversioner* reversioner );
int AG_reversioner_free( struct AG_reversioner* reversioner );

// map info 
int AG_reversioner_add_map_info( struct AG_reversioner* reversioner, char const* path, struct AG_map_info* mi);
int AG_reversioner_remove_map_info( struct AG_reversioner* reversioner, struct AG_path_map_info* mi);
int AG_reversioner_reload_map_infos( struct AG_reversioner* reversioner, AG_fs_map_t* map_infos );
int AG_reversioner_invalidate_map_info( struct AG_reversioner* reversioner, int64_t* next_deadline );

#endif //_AG_REVERSIONER_H_

