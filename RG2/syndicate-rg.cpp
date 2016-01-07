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

#include "syndicate-rg.h"

#include <libsyndicate/proc.h>
#include "server.h"

#include <signal.h>

#define RG_NUM_DRIVER_ROLES 3
char const* RG_DRIVER_ROLES[RG_NUM_DRIVER_ROLES] = {
   "read",
   "write",
   "delete"
};


struct RG_core {
   
   pthread_t thread;            // SG main loop
   pthread_rwlock_t lock;       // guard for this structure
   bool running;
   int main_rc;                 // result of SG main loop
   
   struct SG_gateway *gateway;  // gateway core
};

// global run flag 
bool g_running = true;

// global state 
struct RG_core g_core;

// rlock the core 
int RG_core_rlock( struct RG_core* rg ) {
   
   return pthread_rwlock_rdlock( &rg->lock );
}

// wlock the core 
int RG_core_wlock( struct RG_core* rg ) {
   
   return pthread_rwlock_wrlock( &rg->lock );
}

// unlock the core 
int RG_core_unlock( struct RG_core* rg ) {
   
   return pthread_rwlock_unlock( &rg->lock );
}

// get the core's gateway 
// NOTE: core must be at least read-locked 
struct SG_gateway* RG_core_gateway( struct RG_core* core ) {
   return core->gateway;
}

// set up RG
// return 0 on success
// return -errno on failure (see SG_gateway_init)
int RG_init( struct RG_core* rg, int argc, char** argv ) {
   
   int rc = 0;
   struct md_opts* overrides = md_opts_new( 1 );
   if( overrides == NULL ) {
      return -ENOMEM;
   }
   
   md_opts_set_client( overrides, false );
   md_opts_set_gateway_type( overrides, SYNDICATE_RG );
   md_opts_set_driver_config( overrides, RG_DEFAULT_EXEC, RG_DRIVER_ROLES, RG_NUM_DRIVER_ROLES ); 
   
   memset( rg, 0, sizeof(struct RG_core) );
   
   rc = pthread_rwlock_init( &rg->lock, NULL );
   if( rc != 0 ) {
      
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return rc;
   }
   
   rg->gateway = SG_CALLOC( struct SG_gateway, 1 );
   if( rg->gateway == NULL ) {
      
      pthread_rwlock_destroy( &rg->lock );
      md_opts_free( overrides );
      SG_safe_free( overrides );
      return -ENOMEM;
   }
   
   // core gateway...
   rc = SG_gateway_init( rg->gateway, SYNDICATE_RG, argc, argv, overrides );
   
   md_opts_free( overrides );
   SG_safe_free( overrides );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_init rc = %d\n", rc );
      
      SG_safe_free( rg->gateway );
      pthread_rwlock_destroy( &rg->lock );
      return rc;
   }
   
   // core methods...
   rc = RG_server_install_methods( rg->gateway, rg );
   if( rc != 0 ) {
      
      SG_error("RG_server_install_methods rc = %d\n", rc );
      
      SG_gateway_shutdown( rg->gateway );
      SG_safe_free( rg->gateway );
      pthread_rwlock_destroy( &rg->lock );
      return rc;
   }
   
   return rc;
}


// tear down RG
// return 0 on success
// return -errno on failure
int RG_shutdown( struct RG_core* rg ) {
   
   int rc = 0;
   
   if( rg->running ) {
      
      // ask the SG to die 
      SG_gateway_signal_main( rg->gateway );
      
      pthread_join( rg->thread, NULL );
      rg->running = false;
   }
   
   // shut down the core gateway
   rc = SG_gateway_shutdown( rg->gateway );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_shutdown rc = %d\n", rc );
   }
   
   SG_safe_free( rg->gateway );
   
   pthread_rwlock_destroy( &rg->lock );
   
   md_shutdown();
   
   memset( rg, 0, sizeof(struct RG_core) );
   return 0;
}


// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   
   // set up the RG
   rc = RG_init( &g_core, argc, argv );
   if( rc != 0 ) {
      
      SG_error("RG_init rc = %d\n", rc );
      exit(1);
   }
 
   // run the RG
   rc = SG_gateway_main( g_core.gateway );
   if( rc != 0 ) {

      SG_error("SG_gateway_main rc = %d\n", rc );
   }

   RG_shutdown( &g_core );
   
   // success!
   return 0;
}
