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

#include "reversioner.h"
#include "core.h"
#include "map-info.h"
#include "driver.h"

// allocate and set up a path map info 
int AG_path_map_info_init( struct AG_path_map_info* pinfo, char const* path, struct AG_driver_publish_info* pubinfo ) {
   
   memset( pinfo, 0, sizeof(struct AG_path_map_info) );
   
   pinfo->path = strdup(path);
   
   if( pubinfo != NULL ) {
      pinfo->pubinfo = CALLOC_LIST( struct AG_driver_publish_info, 1 );
      memcpy( pinfo->pubinfo, pubinfo, sizeof(struct AG_driver_publish_info) );
   }
   
   return 0;
}

// duplicate a path map info
int AG_path_map_info_dup( struct AG_path_map_info* new_pinfo, struct AG_path_map_info* old_pinfo ) {
   
   AG_path_map_info_init( new_pinfo, old_pinfo->path, old_pinfo->pubinfo );
   return 0;
}

// free a path map info's memory 
int AG_path_map_info_free( struct AG_path_map_info* pinfo ) {
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


// main reversioning loop 
void* AG_reversioner_main_loop(void *argc) {
   
    struct AG_reversioner* rv = (struct AG_reversioner*)argc;
    
    dbprintf("%s", "AG reversioner thread started\n");
    
    while (rv->running) {
   
      // wait for there to be data 
      sem_wait( &rv->sem );
      
      // refresh
      int rc = AG_reversioner_reversion_map_infos( rv );
      if( rc < 0 ) {
         errorf("AG_reversioner_reversion_map_infos rc = %d\n", rc );
      }
    }
    
    dbprintf("%s", "AG reversioner thread exit\n");
    
    return NULL;
}


// initialize a reversioner 
int AG_reversioner_init( struct AG_reversioner* reversioner, struct AG_state* state ) {
    reversioner->running = false;
    
    pthread_mutex_init(&reversioner->set_lock, NULL);
    
    reversioner->map_set = new AG_path_map_info_set_t();
    reversioner->state = state;
    
    sem_init( &reversioner->sem, 0, 0 );
    
    return 0;
}

// destroy a reversioner
int AG_reversioner_free( struct AG_reversioner* reversioner ) {
   
   pthread_mutex_destroy( &reversioner->set_lock );
   
   for( AG_path_map_info_set_t::iterator itr = reversioner->map_set->begin(); itr != reversioner->map_set->end(); itr++ ) {
      
      // NOTE: shallow copy; freeing minfo frees *itr's data
      struct AG_path_map_info minfo = *itr;
      
      AG_path_map_info_free( &minfo );
   }
   
   delete reversioner->map_set;
   
   sem_destroy( &reversioner->sem );
   
   return 0;
}


// start a reversioner
int AG_reversioner_start( struct AG_reversioner* reversioner ) {
    reversioner->running = true;
    
    reversioner->tid = md_start_thread( AG_reversioner_main_loop, reversioner, false );
    if( reversioner->tid < 0 ) {
       
       int errsv = -errno;
       errorf("Failed to start AG reversioning thread, errno = %d\n", errno );
       
       return errsv;
    }
    
    return 0;
}

// stop a reversioner
int AG_reversioner_stop( struct AG_reversioner* reversioner ) {
   
   if( reversioner->running ) {
      reversioner->running = false;
      
      pthread_cancel( reversioner->tid );
      pthread_join( reversioner->tid, NULL );
   }
   
   return 0;
}

// add a map info to a reversioner
// return EEXIST if it's already present.
int AG_reversioner_add_map_info( struct AG_reversioner* reversioner, char const* path, struct AG_driver_publish_info* pubinfo ) {
   
    struct AG_path_map_info pinfo;
    AG_path_map_info_init( &pinfo, path, pubinfo );
    
    pthread_mutex_lock(&reversioner->set_lock);
   
    if( reversioner->map_set->find( pinfo ) != reversioner->map_set->end() ) {
       pthread_mutex_unlock( &reversioner->set_lock );
       
       AG_path_map_info_free( &pinfo );
       return -EEXIST;
    }
    else {
      reversioner->map_set->insert(pinfo);
      
      if( reversioner->map_set->size() == 1 ) {
            
         // wake up the reversioner thread--it just got some work
         sem_post( &reversioner->sem );
      }
      
      pthread_mutex_unlock(&reversioner->set_lock);
    
      return 0;
    }
}


// add all map-infos and load new ones from an AG_fs_map_t 
int AG_reversioner_add_map_infos( struct AG_reversioner* reversioner, AG_fs_map_t* map_infos ) {
   
   pthread_mutex_lock( &reversioner->set_lock );
   
   bool wakeup = false;
   
   if( reversioner->map_set->size() == 0 ) {
      wakeup = true;
   }
   
   // load a path map info for each map info given 
   for( AG_fs_map_t::iterator itr = map_infos->begin(); itr != map_infos->end(); itr++ ) {
      
      char const* path = itr->first.c_str();
      
      struct AG_path_map_info pinfo;
      AG_path_map_info_init( &pinfo, path, NULL );
      
      reversioner->map_set->insert( pinfo );
   }
   
   pthread_mutex_unlock( &reversioner->set_lock );
   
   if( wakeup ) {
      
      // wake up the reversioner thread--it just got some work
      sem_post( &reversioner->sem );
   }
   
   return 0;
}


// reversion map infos
int AG_reversioner_reversion_map_infos( struct AG_reversioner* reversioner ) {

   int rc = 0;
   int worst_rc = 0;
   
   // swap out the list of entries to invalidate 
   AG_path_map_info_set_t* old_infos = NULL;
   AG_path_map_info_set_t* new_infos = new AG_path_map_info_set_t();
   
   pthread_mutex_lock(&reversioner->set_lock);
   
   old_infos = reversioner->map_set;
   reversioner->map_set = new_infos;
   
   pthread_mutex_unlock( &reversioner->set_lock );
   
   // reversion each of the stale map infos 
   for( AG_path_map_info_set_t::iterator itr = old_infos->begin(); itr != old_infos->end(); itr++ ) {
      
      struct AG_path_map_info pmi = *itr;
      
      dbprintf("Reversion %s, pubinfo = %p\n", pmi.path, pmi.pubinfo );
      
      // reversion
      AG_state_fs_rlock( reversioner->state );
      rc = AG_fs_reversion( reversioner->state->ag_fs, pmi.path, pmi.pubinfo );
      AG_state_fs_unlock( reversioner->state );
      
      if( rc != 0 ) {
         errorf("ERR: AG_fs_reversion(%s) rc = %d\n", pmi.path, rc );
         
         if( rc != -ENOENT ) {
            // some other error 
            worst_rc = -EREMOTEIO;
         }
      }
   }
   
   // free memory 
   for( AG_path_map_info_set_t::iterator itr = old_infos->begin(); itr != old_infos->end(); itr++ ) {
      
      struct AG_path_map_info pmi = *itr;
      
      AG_path_map_info_free( &pmi );
   }
   
   delete old_infos;
   
   return worst_rc;
}

