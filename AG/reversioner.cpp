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

// allocate and set up a path map info 
int AG_path_map_info_init( struct AG_path_map_info* pinfo, char const* path, const struct AG_map_info* mi ) {
   
   memset( pinfo, 0, sizeof(struct AG_path_map_info) );
   
   AG_map_info_dup( &pinfo->mi, mi );
   pinfo->path = strdup(path);
   
   return 0;
}

// duplicate a path map info
int AG_path_map_info_dup( struct AG_path_map_info* new_pinfo, const struct AG_path_map_info* old_pinfo ) {
   
   struct AG_map_info mi_dup;
   AG_map_info_dup( &mi_dup, &old_pinfo->mi );
   
   AG_path_map_info_init( new_pinfo, old_pinfo->path, &mi_dup );
   return 0;
}

// free a path map info's memory 
int AG_path_map_info_free( struct AG_path_map_info* pinfo ) {
   if( pinfo->path != NULL ) {
      free( pinfo->path );
      pinfo->path = NULL;
   }
   
   return 0;
}


// main reversioning loop 
void* AG_reversioner_main_loop(void *argc) {
   
    struct timespec request;
    struct timespec now;
    
    clock_gettime( CLOCK_MONOTONIC, &now );
    
    struct AG_reversioner* rv = (struct AG_reversioner*)argc;
    
    dbprintf("%s", "AG reversioner thread started\n");
    
    int64_t deadline = 1;
    
    while (rv->running) {
      
      clock_gettime( CLOCK_MONOTONIC, &now );
      
      // wait for a bit
      request.tv_sec = now.tv_sec + deadline;
      request.tv_nsec = 0;
      
      dbprintf("Next deadline in %ld second(s)\n", deadline );
      
      int rc = 0;
      
      while( true ) {
         
         // wait until the next deadline
         rc = clock_nanosleep( CLOCK_MONOTONIC, TIMER_ABSTIME, &request, NULL );
         if( rc == 0 ) {
            // success 
            break;
         }
         else {
            rc = -errno;
            if( rc == -EINTR ) {
               
               // try again 
               rc = 0;
               continue;
            }
            else {
               errorf("clock_nanosleep errno = %d\n", rc );
               break;
            }
         }
      }
      
      if( rc < 0 ) {
         // fail fast
         break;
      }
      
      // refresh
      rc = AG_reversioner_invalidate_map_info( rv, &deadline );
      if( rc < 0 ) {
         errorf("AG_reversioner_invalidate_map_info rc = %d\n", rc );
         
         if( deadline <= 0 ) {
            
            deadline = 1;
            errorf("WARN: waiting the minimal amount of time of %" PRId64 " seconds\n", deadline );
         }
      }
    }
    
    dbprintf("%s", "AG reversioner thread exit\n");
    
    return NULL;
}


// initialize a reversioner 
int AG_reversioner_init( struct AG_reversioner* reversioner, struct AG_state* state ) {
    reversioner->running = false;
    
    pthread_mutex_init(&reversioner->set_lock, NULL);
    
    reversioner->map_set = new AG_deadline_map_info_set_t();
    reversioner->state = state;
    
    return 0;
}

// destroy a reversioner
int AG_reversioner_free( struct AG_reversioner* reversioner ) {
   
   pthread_mutex_destroy( &reversioner->set_lock );
   
   for( AG_deadline_map_info_set_t::iterator itr = reversioner->map_set->begin(); itr != reversioner->map_set->end(); itr++ ) {
      
      // NOTE: shallow copy; freeing minfo frees *itr's data
      struct AG_path_map_info minfo = *itr;
      
      AG_path_map_info_free( &minfo );
   }
   
   delete reversioner->map_set;
   
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
int AG_reversioner_add_map_info( struct AG_reversioner* reversioner, char const* path, struct AG_map_info* mi) {
   
    struct AG_path_map_info pinfo;
    AG_path_map_info_init( &pinfo, path, mi );
    
    pthread_mutex_lock(&reversioner->set_lock);
   
    reversioner->map_set->insert(pinfo);
    
    pthread_mutex_unlock(&reversioner->set_lock);
    
    return 0;
}

// remove a map info from consideration 
int AG_reversioner_remove_map_info( struct AG_reversioner* reversioner, struct AG_path_map_info* pmi) {
    if (pmi == NULL ) {
      return -EINVAL;
    }
    
    bool has_pinfo = false;
    struct AG_path_map_info pinfo;
    
    memset( &pinfo, 0, sizeof(struct AG_path_map_info) );
    
    pthread_mutex_lock(&reversioner->set_lock);
    
    // get the elemnent out, if it exists
    AG_deadline_map_info_set_t::iterator itr = reversioner->map_set->find( *pmi );
    if( itr != reversioner->map_set->end() ) {
      
      pinfo = *itr;
      has_pinfo = true;
      reversioner->map_set->erase(*pmi);
    }
    
    pthread_mutex_unlock(&reversioner->set_lock);
    
    // clean up this element
    if( has_pinfo ) {
      AG_path_map_info_free( &pinfo );
    }
    
    return 0;
}


// replace all map-infos and load new ones from an AG_fs_map_t 
int AG_reversioner_reload_map_infos( struct AG_reversioner* reversioner, AG_fs_map_t* map_infos ) {
   
   pthread_mutex_lock( &reversioner->set_lock );
   
   // clear out everything we're tracking 
   AG_deadline_map_info_set_t* old_set = reversioner->map_set;
   
   reversioner->map_set = new AG_deadline_map_info_set_t();
   
   // load a path map info for each map info given 
   for( AG_fs_map_t::iterator itr = map_infos->begin(); itr != map_infos->end(); itr++ ) {
      
      char const* path = itr->first.c_str();
      struct AG_map_info* mi = itr->second;
      
      struct AG_path_map_info pinfo;
      AG_path_map_info_init( &pinfo, path, mi );
      
      reversioner->map_set->insert( pinfo );
   }
   
   pthread_mutex_unlock( &reversioner->set_lock );
   
   // free memory 
   for( AG_deadline_map_info_set_t::iterator itr = old_set->begin(); itr != old_set->end(); itr++ ) {
      
      struct AG_path_map_info pinfo = *itr;
      
      AG_path_map_info_free( &pinfo );
   }
   
   delete old_set;
   
   return 0;
}


// invalidate and reversion any stale map infos
// return the number of seconds to wait until the next item needs to be refreshed
int AG_reversioner_invalidate_map_info( struct AG_reversioner* reversioner, int64_t* ret_next_deadline ) {

   struct timespec now;
   int rc = 0;
   int worst_rc = 0;
   
   int64_t next_deadline = INT64_MAX;
   
   rc = clock_gettime( CLOCK_MONOTONIC, &now );
   if (rc != 0 ) {
      rc = -errno;
      errorf("clock_gettime rc = %d\n", rc );
      return -EAGAIN;
   }
   
   vector<struct AG_path_map_info> stale_info;
   
   pthread_mutex_lock(&reversioner->set_lock);
   
   // find the items to reversion--ones whose deadline has passed
   // (atomically) copy them out without doing any I/O
   for( AG_deadline_map_info_set_t::iterator itr = reversioner->map_set->begin(); itr != reversioner->map_set->end(); itr++ ) {
      
      struct AG_path_map_info pmi = *itr;
      
      if (pmi.mi.refresh_deadline >= (unsigned)now.tv_sec ) {
         
         // duplicate this one out 
         struct AG_path_map_info dup_pmi;
         AG_path_map_info_dup( &dup_pmi, &pmi );
         
         stale_info.push_back( dup_pmi );
      }
      else {
         // since this set is ordered by deadline, there is nothing else beyond here that is stale.
         break;
      }
   }
   
   // remove stale entries from the reversioner 
   for( unsigned int i = 0; i < stale_info.size(); i++ ) {
      
      AG_deadline_map_info_set_t::iterator itr = reversioner->map_set->find( stale_info[i] );
      
      if( itr != reversioner->map_set->end() ) {
         // free this, since it will be replaced 
         struct AG_path_map_info old = *itr;
         reversioner->map_set->erase( itr );
         AG_path_map_info_free( &old );
      }
   }
   
   // update deadlines for stale entries 
   for( unsigned int i = 0; i < stale_info.size(); i++ ) {
      
      struct AG_path_map_info* pmi = &stale_info[i];
      
      // advance the refresh deadline
      pmi->mi.refresh_deadline = now.tv_sec + pmi->mi.reval_sec;
      
      // put a duplicate back into the reversioner
      struct AG_path_map_info dup_pmi;
      AG_path_map_info_dup( &dup_pmi, pmi );
      
      reversioner->map_set->insert( dup_pmi );
   }
   
   // find the next deadline 
   if( reversioner->map_set->size() > 0 ) {
      next_deadline = reversioner->map_set->begin()->mi.refresh_deadline;
   }
   else {
      next_deadline = 1;
   }
   
   pthread_mutex_unlock( &reversioner->set_lock );
   
   // reversion each of the stale map infos 
   for( unsigned int i = 0; i < stale_info.size(); i++ ) {
      
      struct AG_path_map_info* pmi = &stale_info[i];
      
      dbprintf("Reversion %s\n", pmi->path );
      
      // reversion.  stop the reload thread from destroying state's fs
      AG_state_fs_rlock( reversioner->state );
      rc = AG_fs_reversion( reversioner->state->ag_fs, pmi->path );
      AG_state_fs_unlock( reversioner->state );
      
      if( rc != 0 ) {
         errorf("ERR: AG_fs_reversion(%s) rc = %d\n", pmi->path, rc );
         
         if( rc == -ENOENT ) {
            // clear this out and continue 
            AG_reversioner_remove_map_info( reversioner, pmi );
         }
         else {
            // some other error 
            worst_rc = -EREMOTEIO;
         }
      }
   }
   
   // free memory 
   for( unsigned int i = 0; i < stale_info.size(); i++ ) {
      AG_path_map_info_free( &stale_info[i] );
   }
   
   *ret_next_deadline = next_deadline;
   return worst_rc;
}

