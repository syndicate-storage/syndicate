/*
   Copyright 2016 The Trustees of Princeton University

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
#include "server.h"
#include "syndicate-ag.h"

#define AG_DEFAULT_DRIVER_EXEC_STR  "/usr/local/lib/syndicate/ag-driver"
#define AG_DRIVER_NUM_ROLES  4
char const* AG_DRIVER_ROLES[ AG_DRIVER_NUM_ROLES ] = {
   "serialize",
   "deserialize",
   "read",
   "crawl"
};

// core AG control structure
struct AG_state {

   struct UG_state* ug_core;
   pthread_rwlock_t lock;
};


// read-lock state.  return 0 on success
int AG_state_rlock( struct AG_state* state ) {
   return pthread_rwlock_rdlock( &state->lock );
}

// write-lock state.  return 0 on success
int AG_state_wlock( struct AG_state* state ) {
   return pthread_rwlock_wrlock( &state->lock );
}

// unlock state. return 0 on success
int AG_state_unlock( struct AG_state* state ) {
   return pthread_rwlock_unlock( &state->lock );
}

// set up the AG
// return a client on success
// return NULL on error
struct AG_state* AG_init( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct AG_state* ag = NULL;
   struct md_opts* overrides = md_opts_new( 1 );
   
   if( overrides == NULL ) {
      return NULL;
   }

   md_opts_default( overrides );
   md_opts_set_client( overrides, false );
   md_opts_set_gateway_type( overrides, SYNDICATE_AG );
   md_opts_set_driver_config( overrides, AG_DEFAULT_DRIVER_EXEC_STR, AG_DRIVER_ROLES, AG_DRIVER_NUM_ROLES );

   ag = SG_CALLOC( struct AG_state, 1 );
   if( ag == NULL ) {
      // OOM 
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return NULL;
   }

   // create UG core
   ug = UG_init_ex( argc, argv, overrides, ag );

   md_opts_free( overrides );
   SG_safe_free( overrides );

   if( ug == NULL ) {
      SG_error("%s", "UG_init failed\n");
      SG_safe_free( ag );
      return NULL;
   }

   ag->ug_core = ug;

   rc = pthread_rwlock_init( &ag->lock, NULL );
   if( rc != 0 ) {
      SG_error("pthread_rwlock_init rc = %d\n", rc );
      UG_shutdown( ug );
      SG_safe_free( ag );
      return NULL;
   }

   // add AG server-side behaviors
   AG_server_install_methods( AG_state_gateway( ag ) ); 

   return ag;
}


// run the AG's server-side logic
// return 0 on success
// return -EINVAL if we already started the UG
// return -ENOMEM on OOM 
// return -errno on failure to fork
int AG_main( struct AG_state* state ) {
   
   return UG_main( state->ug_core );
}

// shut down the AG, given a state bundle passed from UG_init
// always succeeds
int AG_shutdown( struct AG_state* state ) {
 
   int rc = 0; 
   rc = UG_shutdown( state->ug_core );
   if( rc != 0 ) {
      SG_error("UG_shutdown rc = %d\n", rc );
      return rc;
   }

   pthread_rwlock_destroy( &state->lock );
   memset( state, 0, sizeof(struct AG_state) );
   return 0;
}

// get a pointer to the gateway core 
struct SG_gateway* AG_state_gateway( struct AG_state* state ) {
   return UG_state_gateway( state->ug_core );
}
   
// get a pointer to the filesystem core
struct fskit_core* AG_state_fs( struct AG_state* state ) {
   return UG_state_fs( state->ug_core );
}

// get a pointer to the UG 
struct UG_state* AG_state_ug( struct AG_state* state ) {
   return state->ug_core;
}
