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

#include "workqueue.h"
#include "core.h"
#include "map-info.h"
#include "publish.h"
#include "driver.h"

// pair a map_info to its path and the global state
struct AG_path_map_info {
   
   struct AG_state* global_state;              // reference to global AG state
   char* path;
   struct AG_driver_publish_info* pubinfo;        // optional
};

// allocate and set up a path map info 
static int AG_path_map_info_init( struct AG_path_map_info* pinfo, struct AG_state* global_state, char const* path, struct AG_driver_publish_info* pubinfo ) {
   
   memset( pinfo, 0, sizeof(struct AG_path_map_info) );
   
   pinfo->path = strdup(path);
   pinfo->global_state = global_state;
   
   if( pubinfo != NULL ) {
      pinfo->pubinfo = CALLOC_LIST( struct AG_driver_publish_info, 1 );
      memcpy( pinfo->pubinfo, pubinfo, sizeof(struct AG_driver_publish_info) );
   }
   
   return 0;
}

// free a path map info's memory 
static int AG_path_map_info_free( struct AG_path_map_info* pinfo ) {
   if( pinfo->path != NULL ) {
      free( pinfo->path );
      pinfo->path = NULL;
   }
   if( pinfo->pubinfo ) {
      free( pinfo->pubinfo );
      pinfo->pubinfo = NULL;
   }
   
   return 0;
}

// work queue method for publishing, within the global filesystem
static int AG_workqueue_work_publish( struct md_wreq* wreq, void* cls ) {
   
   struct AG_path_map_info* pinfo = (struct AG_path_map_info*)cls;
   int rc = 0;
   
   rc = AG_fs_publish( pinfo->global_state->ag_fs, pinfo->path, pinfo->pubinfo );
   
   if( rc != 0 ) {
      errorf("ERR: AG_fs_publish(%s) rc = %d\n", pinfo->path, rc );
   }
   
   AG_path_map_info_free( pinfo );
   free( pinfo );
   
   return rc;
}


// work queue method for reversioning.
static int AG_workqueue_work_reversion( struct md_wreq* wreq, void* cls ) {
   
   struct AG_path_map_info* pinfo = (struct AG_path_map_info*)cls;
   int rc = 0;
   
   rc = AG_fs_reversion( pinfo->global_state->ag_fs, pinfo->path, pinfo->pubinfo );
   
   if( rc != 0 ) {
      errorf("ERR: AG_fs_reversion(%s) rc = %d\n", pinfo->path, rc );
   }
   
   AG_path_map_info_free( pinfo );
   free( pinfo );
   
   return rc;
}

// work queue method for deletion.
static int AG_workqueue_work_delete( struct md_wreq* wreq, void* cls ) {
   
   struct AG_path_map_info* pinfo = (struct AG_path_map_info*)cls;
   int rc = 0;
   
   rc = AG_fs_delete( pinfo->global_state->ag_fs, pinfo->path );
   
   if( rc != 0 ) {
      errorf("ERR: AG_fs_delete(%s) rc = %d\n", pinfo->path, rc );
   }
   
   AG_path_map_info_free( pinfo );
   free( pinfo );
   
   return rc;
}

// add a request to perform an AG operation
static int AG_workqueue_add_operation( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo, md_wq_func_t op ) {
   
   struct AG_path_map_info* pinfo = CALLOC_LIST( struct AG_path_map_info, 1 );
   struct md_wreq wreq;
   
   struct AG_state* state = (struct AG_state*)md_wq_cls( wq );
   
   AG_path_map_info_init( pinfo, state, fs_path, pubinfo );
   
   md_wreq_init( &wreq, op, pinfo, 0 );
   
   return md_wq_add( wq, &wreq );
}

// add a publish request to the queue 
int AG_workqueue_add_publish( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo ) {
   
   return AG_workqueue_add_operation( wq, fs_path, pubinfo, AG_workqueue_work_publish );
}

// add a reversion request to the queue 
int AG_workqueue_add_reversion( struct md_wq* wq, char const* fs_path, struct AG_driver_publish_info* pubinfo ) {
   
   return AG_workqueue_add_operation( wq, fs_path, pubinfo, AG_workqueue_work_reversion );
}

// add a deletion request to the queue
int AG_workqueue_add_delete( struct md_wq* wq, char const* fs_path ) {
   
   return AG_workqueue_add_operation( wq, fs_path, NULL, AG_workqueue_work_delete );
}
