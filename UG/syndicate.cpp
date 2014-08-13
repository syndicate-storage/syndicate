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
static void syndicate_add_extra_config( struct md_syndicate_conf* conf, struct syndicate_opts* opts ) {
   // add in extra information not covered by md_init
   conf->cache_soft_limit = opts->cache_soft_limit;
   conf->cache_hard_limit = opts->cache_hard_limit;
}

// finish initializing the state
int syndicate_setup_state( struct syndicate_state* state, struct ms_client* ms ) {
   
   int rc = 0;
   
   state->ms = ms;
   
   // get the volume
   uint64_t volume_id = ms_client_get_volume_id( state->ms );
   uint64_t block_size = ms_client_get_volume_blocksize( state->ms );

   if( volume_id == 0 ) {
      errorf("%s", "Volume not found\n");
      return -ENOENT;
   }
   
   // make the logfile
   state->logfile = log_init( state->conf.logfile_path );
   if( state->logfile == NULL ) {
      return -ENOMEM;
   }
   
   // start debugging
   fs_entry_set_config( &state->conf );

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

   // initialize the filesystem core (i.e. so it can reference all of the sub-components of the UG)
   // NOTE: cache isn't initialized yet, but it doesn't have to be.
   struct fs_core* core = CALLOC_LIST( struct fs_core, 1 );
   rc = fs_core_init( core, state, &state->conf, state->ms, &state->cache, root.owner, root.coordinator, root.volume, root.mode, block_size );
   
   md_entry_free( &root );
   
   if( rc != 0 ) {
      // something went wrong...
      errorf("fs_core_init rc = %d\n", rc );
      return rc;
   }
   
   // populate state with it (and other bits of info...)
   state->core = core;
   state->uid = getuid();
   state->gid = getgid();
   
   state->mounttime = currentTimeSeconds();
   
   // initialize the downloader 
   rc = md_downloader_init( &state->dl, "UG-downloader" );
   if( rc != 0 ) {
      errorf("md_downloader_init rc = %d\n", rc );
      return rc;
   }
   
   // start it up 
   rc = md_downloader_start( &state->dl );
   if( rc != 0 ) {
      errorf("md_downloader_start rc = %d\n", rc );
      return rc;
   }

   // initialize and start caching
   rc = fs_entry_cache_init( core, &state->cache, state->conf.cache_soft_limit / block_size, state->conf.cache_hard_limit / block_size );
   if( rc != 0 ) {
      errorf("fs_entry_cache_init rc = %d\n", rc );
      return rc;  
   }
   
   // start up replication
   fs_entry_replication_init( state, volume_id );
   
   // initialize vacuumer 
   rc = fs_entry_vacuumer_init( &state->vac, core );
   if( rc != 0 ) {
      errorf("fs_entry_vacuumer_init rc = %d\n", rc );
      return rc;
   }
   
   // start vacuumer 
   rc = fs_entry_vacuumer_start( &state->vac );
   if( rc != 0 ) {
      errorf("fs_entry_vacuumer_start rc = %d\n", rc );
      return rc;
   }

   return 0;
}

int syndicate_set_running_ex( struct syndicate_state* state, int running ) {
   state->running = running;
   return 0;
}

int syndicate_destroy_ex( struct syndicate_state* state, int wait_replicas ) {
   
   state->running = 0;
   
   dbprintf("%s", "stopping vacuumer\n");
   fs_entry_vacuumer_stop( &state->vac );
   
   dbprintf("%s", "stopping downloads\n");
   md_downloader_stop( &state->dl );
   
   dbprintf("%s", "shutting down downloader\n");
   md_downloader_shutdown( &state->dl );

   dbprintf("%s", "stopping replication\n");
   fs_entry_replication_shutdown( state, wait_replicas );
   
   dbprintf("%s", "shutting down vacuumer\n");
   fs_entry_vacuumer_shutdown( &state->vac );
   
   dbprintf("%s", "core filesystem shutdown\n");
   fs_destroy( state->core );
   free( state->core );
   
   dbprintf("%s", "destroy cache\n");
   fs_entry_cache_destroy( &state->cache );

   dbprintf("%s", "destory MS client\n");
   ms_client_destroy( state->ms );
   free( state->ms );

   if( state->stats != NULL ) {
      string statistics_str = state->stats->dump();
      printf("Statistics: \n%s\n", statistics_str.c_str() );
      delete state->stats;
      state->stats = NULL;
   }

   dbprintf("%s", "log shutdown\n");

   log_shutdown( state->logfile );

   dbprintf("%s", "free configuration\n");
   md_free_conf( &state->conf );
   
   return 0;
}


// initialize
int syndicate_init( struct syndicate_opts* opts ) {

   struct syndicate_state* state = CALLOC_LIST( struct syndicate_state, 1 );
   struct ms_client* ms = CALLOC_LIST( struct ms_client, 1 );
   
   // load config
   md_default_conf( &state->conf, SYNDICATE_UG );
   
   // read the config file, if given
   if( opts->config_file != NULL ) {
      int rc = md_read_conf( opts->config_file, &state->conf );
      if( rc != 0 ) {
         dbprintf("ERR: failed to read %s, rc = %d\n", opts->config_file, rc );
      }
   }
   
   // set debug level
   md_debug( &state->conf, opts->debug_level );
   
   // set hostname, if given 
   if( opts->hostname != NULL ) {
      md_set_hostname( &state->conf, opts->hostname );
   }
   
   // initialize library
   if( !opts->anonymous ) {
      dbprintf("%s", "Not anonymous; initializing as peer\n");
      
      int rc = md_init( &state->conf, ms, opts->ms_url, opts->volume_name, opts->gateway_name, opts->username, (char const*)opts->password.ptr, (char const*)opts->user_pkey_pem.ptr,
                                          opts->volume_pubkey_path, opts->gateway_pkey_path, (char const*)opts->gateway_pkey_decryption_password.ptr,
                                          opts->tls_pkey_path, opts->tls_cert_path, opts->storage_root, opts->syndicate_pubkey_path );
      if( rc != 0 ) {
         errorf("md_init rc = %d\n", rc );
         return rc;
      }
   }
   else {
      dbprintf("%s", "Anonymous; initializing as client\n");
      
      // load anything we need into RAM 
      char* volume_pubkey_pem = NULL;
      size_t volume_pubkey_pem_len = 0;
      
      char* syndicate_pubkey_pem = NULL;
      size_t syndicate_pubkey_pem_len = 0;
      
      // get the public keys into RAM 
      if( opts->volume_pubkey_path ) {
         volume_pubkey_pem = md_load_file_as_string( opts->volume_pubkey_path, &volume_pubkey_pem_len );
         if( volume_pubkey_pem == NULL ) {
            errorf("Failed to load %s\n", opts->volume_pubkey_path );
            return -ENOENT;
         }
      }
      
      if( opts->syndicate_pubkey_path ) {
         syndicate_pubkey_pem = md_load_file_as_string( opts->syndicate_pubkey_path, &syndicate_pubkey_pem_len );
         if( syndicate_pubkey_pem == NULL ) {
            errorf("Failed to load %s\n", opts->syndicate_pubkey_path );
            return -ENOENT;
         }
      }
      
      int rc = md_init_client( &state->conf, ms, opts->ms_url, opts->volume_name, NULL, NULL, NULL, NULL, volume_pubkey_pem, NULL, NULL, opts->storage_root, syndicate_pubkey_pem );
      
      if( rc != 0 ) {
         errorf("md_init_client rc = %d\n", rc );
         return rc;
      }
   }
   
   syndicate_add_extra_config( &state->conf, opts );
   
   // initialize state
   int rc = syndicate_setup_state( state, ms );
   if( rc != 0 ) {
      errorf("syndicate_init_state rc = %d\n", rc );
      return rc;
   }
   
   global_state = state;
   return 0;
}


// shutdown
int syndicate_destroy( int wait_replicas ) {

   syndicate_destroy_ex( global_state, wait_replicas );
   
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

void syndicate_set_running() {
   syndicate_set_running_ex( global_state, 1 );
}
