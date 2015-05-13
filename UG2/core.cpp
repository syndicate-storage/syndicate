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

#include "core.h"
#include "impl.h"
#include "fs.h"
#include "vacuumer.h"

// global UG state
struct UG_state {
   
   struct SG_gateway gateway;          // reference to the gateway core (which, in turn, points to UG_state)
   
   uint64_t* replica_gateway_ids;       // IDs of replica gateways to replicate data to
   size_t num_replica_gateway_ids;
   
   struct fskit_core fs;               // filesystem core 
   
   struct UG_vacuumer vacuumer;         // vacuumer instance 
   
   pthread_rwlock_t lock;             // lock governing access to this structure
   
   int detach_rh;                       // route handle to the unlink()/rmdir() route
   
   bool running_thread;                        // if true, we've set up and started a thread to run the main loop ourselves 
   pthread_t thread;                    // the main loop thread
};

// create a duplicate listing of the replica gateway IDs 
// return 0 on success
// return -ENOMEM on OOM 
int UG_state_list_replica_gateway_ids( struct UG_state* state, uint64_t** replica_gateway_ids, size_t* num_replica_gateway_ids ) {
   
   uint64_t* ret = NULL;
   
   UG_state_rlock( state );
   
   ret = SG_CALLOC( uint64_t, state->num_replica_gateway_ids );
   if( ret == NULL ) {
      
      UG_state_unlock( state );
      return -ENOMEM;
   }
   
   memcpy( ret, state->replica_gateway_ids, sizeof(uint64_t) * state->num_replica_gateway_ids );
   
   *replica_gateway_ids = ret;
   *num_replica_gateway_ids = state->num_replica_gateway_ids;
   
   UG_state_unlock( state );
   
   return 0;
}


// reload the set of replica gateway IDs from the MS
// return 0 on success
// return -ENOMEM on OOM 
int UG_state_reload_replica_gateway_ids( struct UG_state* state ) {
   
   int rc = 0;
   
   uint64_t* replica_gateway_ids = NULL;
   size_t num_replica_gateway_ids = 0;
   
   struct SG_gateway* gateway = UG_state_gateway( state );
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // find all replica gateways
   rc = ms_client_get_gateways_by_type( ms, SYNDICATE_RG, &replica_gateway_ids, &num_replica_gateway_ids );
   if( rc != 0 ) {
      
      return rc;
   }
   
   UG_state_wlock( state );
   
   SG_safe_free( state->replica_gateway_ids );
   
   state->replica_gateway_ids = replica_gateway_ids;
   state->num_replica_gateway_ids = num_replica_gateway_ids;
   
   UG_state_unlock( state );
   
   return 0;
}


// read-lock state.  return 0 on success
int UG_state_rlock( struct UG_state* state ) {
   return pthread_rwlock_rdlock( &state->lock );
}

// write-lock state.  return 0 on success
int UG_state_wlock( struct UG_state* state ) {
   return pthread_rwlock_wrlock( &state->lock );
}

// unlock state. return 0 on success
int UG_state_unlock( struct UG_state* state ) {
   return pthread_rwlock_unlock( &state->lock );
}


// set up the UG 
// "client" means "anonymous read-only"
// return 0 on success
// return -errno on failure
struct UG_state* UG_init( int argc, char** argv, bool client ) {
   
   int rc = 0;
   
   struct UG_state state;
   memset( &state, 0, sizeof(struct UG_state) );
   
   // set up fskit library...
   rc = fskit_library_init();
   if( rc != 0 ) {
      
      fskit_error( "fskit_library_init rc = %d\n", rc );
      return NULL;
   }

   
   rc = pthread_rwlock_init( &state.lock, NULL );
   if( rc != 0 ) {
      
      fskit_library_shutdown();
      return NULL;
   }
   
   // set up gateway...
   rc = SG_gateway_init( &state.gateway, SYNDICATE_UG, client, argc, argv );
   if( rc < 0 ) {
      
      SG_error("SG_gateway_init rc = %d\n", rc );
      
      pthread_rwlock_destroy( &state.lock );
      fskit_library_shutdown();
      return NULL;
   }
   
   if( rc > 0 ) {
      
      // help was requested 
      md_common_usage( argv[0] );
      pthread_rwlock_destroy( &state.lock );
      fskit_library_shutdown();
      return NULL;
   }

   // set up fs...
   rc = fskit_core_init( &state.fs, &state.gateway );
   if( rc != 0 ) {
      
      SG_error("fskit_core_init rc = %d\n", rc );
      
      SG_gateway_shutdown( &state.gateway );
      pthread_rwlock_destroy( &state.lock );
      fskit_library_shutdown();
      return NULL;
   }
   
   // install methods 
   UG_impl_install_methods( &state.gateway );
   UG_fs_install_methods( &state.fs );
   
   // load replica gateways 
   rc = UG_state_reload_replica_gateway_ids( &state );
   if( rc != 0 ) {
      
      UG_shutdown( &state );
      return NULL;
   }
   
   // set up vacuumer 
   rc = UG_vacuumer_init( &state.vacuumer, &state.gateway );
   if( rc != 0 ) {
      
      UG_shutdown( &state );
      return NULL;
   }
   
   // start it 
   rc = UG_vacuumer_start( &state.vacuumer );
   if( rc != 0 ) {
   
      UG_shutdown( &state );
      return NULL;
   }
   
   struct UG_state* ret = SG_CALLOC( struct UG_state, 1 );
   if( ret == NULL ) {
      
      UG_shutdown( &state );
      return NULL;
   }
   
   memcpy( ret, &state, sizeof(struct UG_state) );
   
   return ret;
}


// main loop wrapper for pthreads
void* UG_main_pthread( void* arg ) {
   
   struct UG_state* state = (struct UG_state*)arg;
   
   int rc = UG_main( state );
   if( rc != 0 ) {
      
      SG_error("UG_main rc = %d\n", rc );
   }
   
   return NULL;
}

// run the UG in a separate thread.
// returns as soon as we start the new thread.
// return 0 on success
// return -EINVAL if we already started the UG
// return -ENOMEM on OOM 
// return -errno on failure to fork
int UG_start( struct UG_state* state ) {
   
   if( state->running_thread ) {
      return -EINVAL;
   }
   
   state->thread = md_start_thread( UG_main_pthread, state, false );
   if( (int)state->thread == -1 ) {
      
      return -EPERM;
   }
   
   state->running_thread = true;
   return 0;
}

// run the gateway in this thread.  return when the gateway shuts down.
// return 0 on success
// return -errno on failure to initialize, or due to runtime error
int UG_main( struct UG_state* state ) {
   
   int rc = 0;
   
   rc = SG_gateway_main( &state->gateway );
   
   return rc;
}


// shut down the UG, given a state bundle passed from UG_init
// always succeeds
int UG_shutdown( struct UG_state* state ) {
   
   int rc = 0;
   
   // are we running our own thread?  stop it if so.
   if( state->running_thread ) {
      
      SG_gateway_signal_main( UG_state_gateway( state ) );
      
      pthread_join( state->thread, NULL );
      state->running_thread = false;
   }
   
   // disconnect the filesystem
   UG_fs_uninstall_methods( &state->fs );
   
   // stop the vacuumer 
   UG_vacuumer_stop( &state->vacuumer );
   UG_vacuumer_shutdown( &state->vacuumer );
   
   // destroy the gateway 
   rc = SG_gateway_shutdown( &state->gateway );
   if( rc != 0 ) {
      SG_error("SG_gateway_shutdown rc = %d\n", rc );
   }
   
   // blow away all inode data
   rc = fskit_detach_all( &state->fs, "/", state->fs.root.children );
   if( rc != 0 ) {
      SG_error( "fskit_detach_all('/') rc = %d\n", rc );
   }

   // destroy the core
   rc = fskit_core_destroy( &state->fs, NULL );
   if( rc != 0 ) {
      SG_error( "fskit_core_destroy rc = %d\n", rc );
   }
   
   SG_safe_free( state->replica_gateway_ids );
   
   pthread_rwlock_destroy( &state->lock );
   
   memset( state, 0, sizeof(struct UG_state) );
   
   SG_safe_free( state );
   return 0;
}

// get a pointer to the gateway core 
struct SG_gateway* UG_state_gateway( struct UG_state* state ) {
   return &state->gateway;
}
   
// get a pointer to the filesystem core
struct fskit_core* UG_state_fs( struct UG_state* state ) {
   return &state->fs;
}

// get a pointer to the vacuumer core 
struct UG_vacuumer* UG_state_vacuumer( struct UG_state* state ) {
   return &state->vacuumer;
}

// get the owner ID of the gateway 
uint64_t UG_state_owner_id( struct UG_state* state ) {
   return SG_gateway_user_id( UG_state_gateway( state ) );
}

// get the volume ID of the gateway
uint64_t UG_state_volume_id( struct UG_state* state ) {
   return ms_client_get_volume_id( SG_gateway_ms( UG_state_gateway( state ) ) );
}

