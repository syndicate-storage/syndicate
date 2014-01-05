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

#include "syndicate.h"


struct syndicate_state *global_state = NULL;

// initialize
int syndicate_init( char const* config_file,
                    int portnum,
                    char const* ms_url,
                    char const* volume_name,
                    char const* gateway_name,
                    char const* md_username,
                    char const* md_password,
                    char const* volume_pubkey_file,
                    char const* my_key_file,
                    char const* tls_key_file,
                    char const* tls_cert_file
                  ) {


   struct syndicate_state* state = CALLOC_LIST( struct syndicate_state, 1 );
   struct ms_client* ms = CALLOC_LIST( struct ms_client, 1 );

   // initialize library
   int rc = md_init( SYNDICATE_UG, config_file, &state->conf, ms, portnum, ms_url, volume_name, gateway_name, md_username, md_password, volume_pubkey_file, my_key_file, tls_key_file, tls_cert_file, NULL );
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      return rc;
   }
   
   // initialize state
   rc = syndicate_init_state( state, ms );
   if( rc != 0 ) {
      errorf("syndicate_init_state rc = %d\n", rc );
      return rc;
   }
   
   global_state = state;
   return 0;
}


// shutdown
int syndicate_destroy( int wait_replicas ) {

   syndicate_destroy_state( global_state, wait_replicas );
   
   free( global_state );
   global_state = NULL;

   dbprintf("%s", "library shutdown\n");
   md_shutdown();
   
   return 0;
}


// get state
struct syndicate_state* syndicate_get_state() {
   return global_state;
}

// get config
struct md_syndicate_conf* syndicate_get_conf() {
   return &global_state->conf;
}

void syndicate_finish_init( struct syndicate_state* state ) {
   syndicate_set_running( state, 1 );
}
