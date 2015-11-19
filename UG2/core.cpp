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
#include "client.h"
#include "impl.h"
#include "fs.h"
#include "vacuumer.h"

#define UG_DRIVER_NUM_ROLES  3

char const* UG_DRIVER_ROLES[3] = {
   "cdn",
   "serialize",
   "deserialize"
};

// global UG state
struct UG_state {
   
   struct SG_gateway gateway;           // reference to the gateway core (which, in turn, points to UG_state)
   
   uint64_t* replica_gateway_ids;       // IDs of replica gateways to replicate data to
   size_t num_replica_gateway_ids;
   
   struct fskit_core* fs;               // filesystem core 
   
   struct UG_vacuumer vacuumer;         // vacuumer instance 
   
   pthread_rwlock_t lock;               // lock governing access to this structure
   
   int detach_rh;                       // route handle to the unlink()/rmdir() route
   
   bool running_thread;                 // if true, we've set up and started a thread to run the main loop ourselves 
   pthread_t thread;                    // the main loop thread
   
   struct md_wq* wq;                    // workqueue for deferred operations (like blowing away dead inodes)
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
// return a client on success
// return NULL on error
struct UG_state* UG_init( int argc, char** argv, bool client ) {
   
   int rc = 0;
   struct md_entry root_inode_data;
   struct fskit_entry* fs_root = NULL;
   struct UG_inode* root_inode = NULL;
   struct md_wq* wq = NULL;
   struct md_opts* overrides = md_opts_new( 1 );
   struct md_syndicate_conf* conf = NULL;
   
   if( overrides == NULL ) {
      return NULL;
   }
   
   md_opts_default( overrides );
   md_opts_set_client( overrides, client );
   md_opts_set_gateway_type( overrides, SYNDICATE_UG );
   md_opts_set_driver_params( overrides, UG_DEFAULT_DRIVER_EXEC_STR, UG_DRIVER_ROLES, UG_NUM_DRIVER_ROLES );
   
   struct UG_state* state = SG_CALLOC( struct UG_state, 1 );
   if( state == NULL ) {
      
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return NULL;
   }
   
   SG_debug("%s", "Activating filesystem\n");
   
   // set up fskit library...
   rc = fskit_library_init();
   if( rc != 0 ) {
      
      fskit_error( "fskit_library_init rc = %d\n", rc );
      SG_safe_free( state );
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return NULL;
   }
   
   rc = pthread_rwlock_init( &state->lock, NULL );
   if( rc != 0 ) {
      
      fskit_library_shutdown();
      SG_safe_free( state );
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return NULL;
   }
   
   SG_debug("%s", "Setting up gateway core\n");
   
   // set up gateway...
   rc = SG_gateway_init( &state->gateway, SYNDICATE_UG, argc, argv, overrides );
   
   md_opts_free( overrides );
   SG_safe_free( overrides );
   
   if( rc < 0 ) {
      
      SG_error("SG_gateway_init rc = %d\n", rc );
      
      pthread_rwlock_destroy( &state->lock );
      fskit_library_shutdown();
      SG_safe_free( state );
      return NULL;
   }
   
   if( rc > 0 ) {
      
      // help was requested 
      md_common_usage();
      pthread_rwlock_destroy( &state->lock );
      fskit_library_shutdown();
      SG_safe_free( state );
      return NULL;
   }
   
   // debugging?
   conf = SG_gateway_conf( &state->gateway );
   
   if( conf->debug_lock ) {
       
       fskit_set_debug_level( 2 );
   }
   else if( md_get_debug_level() != 0 ) {
       
       fskit_set_debug_level( 1 );
   }
   else {
       
       fskit_set_debug_level( 0 );
   }
   
   SG_debug("%s", "Setting up filesystem core\n");
   
   // set up fs...
   state->fs = fskit_core_new();
   if( state->fs == NULL ) {
      
      // OOM
      pthread_rwlock_destroy( &state->lock );
      fskit_library_shutdown();
      SG_safe_free( state );
      return NULL;
   }
   
   rc = fskit_core_init( state->fs, &state->gateway );
   if( rc != 0 ) {
      
      SG_error("fskit_core_init rc = %d\n", rc );
      
      SG_gateway_shutdown( &state->gateway );
      pthread_rwlock_destroy( &state->lock );
      fskit_library_shutdown();
      SG_safe_free( state->fs );
      SG_safe_free( state );
      return NULL;
   }
   
   // propagate UG to gateway 
   SG_gateway_set_cls( &state->gateway, state );
   
   SG_debug("%s", "Looking up volume root\n");
   
   // set up root inode
   rc = ms_client_get_volume_root( SG_gateway_ms( &state->gateway ), 0, 0, &root_inode_data );
   if( rc != 0 ) {
      
      SG_error("ms_client_get_volume_root() rc = %d\n", rc );
      UG_shutdown( state );
      
      return NULL;
   }
   
   root_inode = UG_inode_alloc( 1 );
   if( root_inode == NULL ) {
      
      // OOM 
      UG_shutdown( state );
      md_entry_free( &root_inode_data );
      return NULL;
   }
   
   SG_debug("%s", "Initializing root inode\n");
   
   // install root inode data
   fs_root = fskit_core_resolve_root( state->fs, true );
   if( fs_root == NULL ) {
      
      // something's seriously wrong 
      SG_error("fskit_entry_resolve_root rc = %p\n", fs_root );
      UG_shutdown( state );
      
      SG_safe_free( root_inode );
      md_entry_free( &root_inode_data );
      return NULL;
   }
   
   fskit_entry_set_owner( fs_root, root_inode_data.owner );
   fskit_entry_set_group( fs_root, root_inode_data.volume );
   
   rc = UG_inode_init_from_export( root_inode, &root_inode_data, fs_root );
   if( rc == 0 ) {
      
      UG_inode_bind_fskit_entry( root_inode, fs_root );
   }
   else {
      
      // OOM or invalid 
      SG_error("UG_inode_init_from_export('/') rc = %d\n", rc );
      
      fskit_entry_unlock( fs_root );
      UG_shutdown( state );
      SG_safe_free( root_inode );
      md_entry_free( &root_inode_data );
      
      return NULL;
   }
   
   //////////////////////////////////////////////////////
   /*
   char* root_str = NULL;
   rc = md_entry_to_string( &root_inode_data, &root_str );
   if( rc == 0 ) {
      SG_debug("root:\n%s\n", root_str );
      SG_safe_free( root_str );
   }
   rc = 0;
   */
   //////////////////////////////////////////////////////
   
   fskit_entry_unlock( fs_root );
   md_entry_free( &root_inode_data );
   
   SG_debug("%s", "Setting up filesystem callbacks\n");
   
   // install methods 
   UG_impl_install_methods( &state->gateway );
   UG_fs_install_methods( state->fs );
   
   // load replica gateways 
   rc = UG_state_reload_replica_gateway_ids( state );
   if( rc != 0 ) {
      
      UG_shutdown( state );
      return NULL;
   }
   
   SG_debug("%s", "Setting up deferred workqueue\n");
   
   // set up deferred workqueue 
   wq = md_wq_new( 1 );
   if( wq == NULL ) {
      
      UG_shutdown( state );
      return NULL;
   }
   
   state->wq = wq;
   
   SG_debug("%s", "Starting vacuumer\n");
   
   // set up vacuumer 
   rc = UG_vacuumer_init( &state->vacuumer, &state->gateway );
   if( rc != 0 ) {
      
      UG_shutdown( state );
      return NULL;
   }
   
   // start threads
   rc = UG_vacuumer_start( &state->vacuumer );
   if( rc != 0 ) {
   
      UG_shutdown( state );
      return NULL;
   }
  
   SG_debug("%s", "Starting deferred workqueue\n");
   
   rc = md_wq_start( state->wq );
   if( rc != 0 ) {
      
      UG_shutdown( state );
      return NULL;
   }

   return state;
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
   
   int rc = 0;
   
   if( state->running_thread ) {
      return -EINVAL;
   }
   
   rc = md_start_thread( &state->thread, UG_main_pthread, state, false );
   if( rc < 0 ) {
      
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
      
      SG_debug("%s", "Stopping main thread\n");
      
      SG_gateway_signal_main( UG_state_gateway( state ) );
      
      pthread_join( state->thread, NULL );
      state->running_thread = false;
   }
   
   SG_debug("%s", "Deactivating filesystem\n");
   
   // stop taking requests
   UG_fs_uninstall_methods( state->fs );
   
   SG_debug("%s", "Shut down vacuuming\n");
   
   // stop the vacuumer 
   UG_vacuumer_stop( &state->vacuumer );
   UG_vacuumer_shutdown( &state->vacuumer );
   
   // stop the deferred workqueue 
   if( state->wq != NULL ) {
      md_wq_stop( state->wq );
      md_wq_free( state->wq, NULL );
      SG_safe_free( state->wq );
   }
   
   // prepare to shutdown 
   UG_fs_install_shutdown_methods( state->fs );
   
   SG_debug("%s", "Gateway shutdown\n");
   
   // destroy the gateway 
   rc = SG_gateway_shutdown( &state->gateway );
   if( rc != 0 ) {
      SG_error("SG_gateway_shutdown rc = %d\n", rc );
   }
   
   SG_debug("%s", "Free all cached inodes\n");
   
   // blow away all inode data
   rc = fskit_detach_all( state->fs, "/" );
   if( rc != 0 ) {
      SG_error( "fskit_detach_all('/') rc = %d\n", rc );
   }
   
   // blow away root 
   // TODO
   
   SG_debug("%s", "Filesystem core shutdown\n");
   
   // destroy the core and its root inode
   rc = fskit_core_destroy( state->fs, NULL );
   if( rc != 0 ) {
      SG_error( "fskit_core_destroy rc = %d\n", rc );
   }
   
   SG_safe_free( state->replica_gateway_ids );
   SG_safe_free( state->fs );
   
   pthread_rwlock_destroy( &state->lock );
   
   memset( state, 0, sizeof(struct UG_state) );

   SG_debug("%s", "Library shutdown\n");
   
   SG_safe_free( state );
   fskit_library_shutdown();
   
   return 0;
}

// get a pointer to the gateway core 
struct SG_gateway* UG_state_gateway( struct UG_state* state ) {
   return &state->gateway;
}
   
// get a pointer to the filesystem core
struct fskit_core* UG_state_fs( struct UG_state* state ) {
   return state->fs;
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

// get the deferred workqueue 
struct md_wq* UG_state_wq( struct UG_state* state ) {
   return state->wq;
}

// get a malloc'ed copy of the driver exec string 
char* UG_state_get_exec_str( struct UG_state* state ) {
   return SG_strdup_or_null( state->exec_str );
}

// get a ref to the UG driver
// call only when at least read-locked 
struct SG_driver* UG_state_driver( struct UG_state* state ) {
   return state->driver;
}

// get a ref to the UG's driver roles 
// call only when at least read-locked 
char const** UG_state_driver_roles( struct UG_state* state ) {
   return state->roles;
}

// get the number of driver roles 
size_t UG_state_driver_num_roles( struct UG_state* state ) {
   return state->num_roles;
}

