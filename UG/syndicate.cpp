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


   // initialize Syndicate state
   global_state = CALLOC_LIST( struct syndicate_state, 1 );

   struct syndicate_state* state = global_state;
   state->ms = CALLOC_LIST( struct ms_client, 1 );


   int rc = md_init( SYNDICATE_UG, config_file, &state->conf, state->ms, portnum, ms_url, volume_name, gateway_name, md_username, md_password, volume_pubkey_file, my_key_file, tls_key_file, tls_cert_file );
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      return rc;
   }

   // get the volume
   uint64_t volume_id = ms_client_get_volume_id( state->ms );
   uint64_t blocking_factor = ms_client_get_volume_blocksize( state->ms );

   if( volume_id == 0 ) {
      errorf("Volume '%s' not found\n", volume_name);
      return -ENOENT;
   }
   
   // make the logfile
   state->logfile = log_init( state->conf.logfile_path );
   if( state->logfile == NULL ) {
      return -ENOMEM;
   }

   // start up stats gathering
   state->stats = new Stats( NULL );
   state->stats->use_conf( &state->conf );

   // get root info
   struct md_entry root;
   memset( &root, 0, sizeof(root) );

   rc = ms_client_get_volume_root( state->ms, &root );
   if( rc != 0 ) {
      errorf("ms_client_get_volume_root rc = %d\n", rc );
      return -ENODATA;
   }

   // sanity check
   if( root.volume != volume_id ) {
      errorf("Invalid root Volume %" PRIu64 "\n", root.volume );
      md_entry_free( &root );
      return -EINVAL;
   }

   // initialize the filesystem core
   struct fs_core* core = CALLOC_LIST( struct fs_core, 1 );
   fs_core_init( core, &state->conf, root.owner, root.coordinator, root.volume, root.mode, blocking_factor );

   md_entry_free( &root );

   fs_entry_set_config( &state->conf );

   state->core = core;
   state->col = new Collator( core );
   
   fs_core_use_collator( core, state->col );
   fs_core_use_ms( core, state->ms );

   // restore local files
   rc = fs_entry_restore_files( core );
   if( rc != 0 ) {
      errorf("fs_entry_restore_files rc = %d\n", rc );
      exit(1);
   }

   state->col->start();
   
   state->uid = getuid();
   state->gid = getgid();
   
   state->mounttime = currentTimeSeconds();

   // start up replication
   replication_init( state->ms, volume_id );

   return 0;
}


// shutdown
int syndicate_destroy() {

   struct syndicate_state* state = global_state;
   
   state->running = 0;

   dbprintf("%s", "stopping replication\n");
   replication_shutdown();
   
   dbprintf("%s", "destroy collation\n");
   delete state->col;

   dbprintf("%s", "destory MS client\n");
   ms_client_destroy( state->ms );
   free( state->ms );

   dbprintf("%s", "core filesystem shutdown\n");
   fs_destroy( state->core );
   free( state->core );
   
   string statistics_str = state->stats->dump();
   printf("Statistics: \n%s\n", statistics_str.c_str() );
   delete state->stats;

   dbprintf("%s", "log shutdown\n");

   log_shutdown( state->logfile );

   dbprintf("%s", "free configuration\n");
   md_free_conf( &state->conf );
   free( state );

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
   state->running = 1;
}
