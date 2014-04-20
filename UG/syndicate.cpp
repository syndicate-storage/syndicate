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

// add extra information into global syndicate conf that isn't covered by the normal initialization step
void syndicate_add_extra_config( struct md_syndicate_conf* conf, struct syndicate_opts* opts ) {
   // add in extra information not covered by md_init
   conf->cache_soft_limit = opts->cache_soft_limit;
   conf->cache_hard_limit = opts->cache_hard_limit;
}

// initialize
int syndicate_init( struct syndicate_opts* opts ) {


   struct syndicate_state* state = CALLOC_LIST( struct syndicate_state, 1 );
   struct ms_client* ms = CALLOC_LIST( struct ms_client, 1 );
   
   // load config file
   md_default_conf( &state->conf, SYNDICATE_UG );
   
   // read the config file
   if( opts->config_file != NULL ) {
      int rc = md_read_conf( opts->config_file, &state->conf );
      if( rc != 0 ) {
         dbprintf("ERR: failed to read %s, rc = %d\n", opts->config_file, rc );
      }
   }
   
   // initialize library
   int rc = md_init( &state->conf, ms, opts->ms_url, opts->volume_name, opts->gateway_name, opts->username, opts->password, (char const*)opts->user_pkey_pem.ptr,
                                       opts->volume_pubkey_path, opts->gateway_pkey_path, opts->gateway_pkey_decryption_password,
                                       opts->tls_pkey_path, opts->tls_cert_path, opts->storage_root, opts->syndicate_pubkey_path );
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      return rc;
   }
   
   syndicate_add_extra_config( &state->conf, opts );
   
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

void syndicate_finish_init() {
   syndicate_set_running( global_state, 1 );
}
