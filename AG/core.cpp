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


#include "core.h"
#include "cache.h"
#include "http.h"
#include "driver.h"
#include "events.h"
#include "publish.h"
#include "map-parser-xml.h"
#include "reversioner.h"

static struct AG_opts g_AG_opts;
static struct AG_state global_state;

// ref the global state
struct AG_state* AG_get_state() {
   if( !global_state.referenceable )
      return NULL;
   
   pthread_rwlock_rdlock( &global_state.state_lock );
   return &global_state;
}

// unref the global state
void AG_release_state( struct AG_state* state ) {
   pthread_rwlock_unlock( &state->state_lock );
}

// signal handler to handle dying
void AG_death_signal_handler( int signum ) {
   
   struct AG_state* state = &global_state;
   if( state->referenceable ) {
      // tell the main loop to proceed to shut down
      sem_post( &state->running_sem );
   }
}


// read-lock access to the AG_fs structure within the state
int AG_state_fs_rlock( struct AG_state* state ) {
   int rc = pthread_rwlock_rdlock( &state->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_rdlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// write-lock access to the AG_fs structure within the state
int AG_state_fs_wlock( struct AG_state* state ) {
   int rc = pthread_rwlock_wrlock( &state->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_wrlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// unlock access to the AG_fs structure within the state
int AG_state_fs_unlock( struct AG_state* state ) {
   int rc = pthread_rwlock_unlock( &state->fs_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_unlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// read-lock the AG_config structure within the state
int AG_state_config_rlock( struct AG_state* state ) {
   int rc = pthread_rwlock_rdlock( &state->config_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_rdlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// write-lock the AG_config structure within the state
int AG_state_config_wlock( struct AG_state* state ) {
   int rc = pthread_rwlock_wrlock( &state->config_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_wrlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// unlock the AG_config structure within the state
int AG_state_config_unlock( struct AG_state* state ) {
   int rc = pthread_rwlock_unlock( &state->config_lock );
   if( rc != 0 ) {
      errorf("pthread_rwlock_unlock(AG_state %p) rc = %d\n", state, rc );
   }
   return rc;
}

// get the specfile from the AG's cert (given by the MS)
int AG_get_spec_file_text( struct ms_client* client, char** out_specfile_text, size_t* out_specfile_text_len ) {
   
   // this will be embedded in the AG's driver text as a base64-encoded string.
   int rc = 0;
   char* specfile_text_json = NULL;
   size_t specfile_text_json_len = 0;
   
   char* specfile_text = NULL;
   size_t specfile_text_len = 0;
   
   // get the json
   rc = ms_client_get_closure_text( client, &specfile_text_json, &specfile_text_json_len );
   if( rc != 0 ) {
      errorf("ms_client_get_closure_text rc = %d\n", rc );
      return rc;
   }
   
   // extract from json 
   rc = md_closure_load_AG_specfile( specfile_text_json, specfile_text_json_len, &specfile_text, &specfile_text_len );
   free( specfile_text_json );
   
   if( rc != 0 ) {
      errorf("md_closure_load_AG_specfile rc = %d\n", rc );
      
      return rc;
   }
   
   // it came from the MS, so it's definitely compressed 
   char* decompressed_text = NULL;
   size_t decompressed_text_len = AG_MAX_SPECFILE_SIZE;
   
   int zrc = md_inflate( specfile_text, specfile_text_len, &decompressed_text, &decompressed_text_len );
   
   free( specfile_text );
   
   if( zrc != 0 ) {
      errorf("md_inflate(%zu bytes) rc = %d\n", specfile_text_len, zrc );
      
      return zrc;
   }
   
   *out_specfile_text = decompressed_text;
   *out_specfile_text_len = decompressed_text_len;
   
   return rc;
}

// get the specfile text, either from the MS certificate, or from an opt-defined location on disk.
// this does NOT try to decompress it
int AG_load_spec_file_text( struct AG_state* state, char** specfile_text, size_t* specfile_text_len ) {
   
   int rc = 0;
   
   if( state->ag_opts.spec_file_path != NULL ) {
      
      // read from disk
      size_t txt_len = 0;
      char* txt = load_file( state->ag_opts.spec_file_path, &txt_len );
      
      if( txt == NULL ) {
         errorf("Failed to load spec file text from %s\n", state->ag_opts.spec_file_path );
         rc = -ENODATA;
      }
      else {
         dbprintf("Loaded %zu-byte specfile from %s\n", *specfile_text_len, state->ag_opts.spec_file_path );
         
         *specfile_text = txt;
         *specfile_text_len = txt_len;
         return 0;
      }
   }
   else {
      // read from MS 
      rc = AG_get_spec_file_text( state->ms, specfile_text, specfile_text_len );
      
      if( rc != 0 ) {
         errorf("AG_get_spec_file_text rc = %d\n", rc );
      }
      else {
         dbprintf("Loaded %zu-byte specfile from the MS\n", *specfile_text_len );
      }
   }
   
   if( rc != 0 ) {
      // can't reload--didn't get the text
      errorf("Failed to get spec file text, rc = %d\n", rc );
   }
   
   return rc;
}


// generate the latest fs_map and config from the specfile 
// NOTE: state only needs .ms and .ag_opts to be initialized for this method to work.
int AG_reload_specfile( struct AG_state* state, AG_fs_map_t** new_fs, AG_config_t** new_config ) {
   
   dbprintf("%s", "Reloading AG spec file...\n");
   
   int rc = 0;
   char* new_specfile_text = NULL;
   size_t new_specfile_text_len = 0;
   
   // get the text 
   rc = AG_load_spec_file_text( state, &new_specfile_text, &new_specfile_text_len );
   if( rc != 0 ) {
      errorf("AG_load_spec_file_text rc = %d\n", rc );
      return rc;
   }
   
   // try to parse the text 
   rc = AG_parse_spec( state, new_specfile_text, new_specfile_text_len, new_fs, new_config );
   if( rc != 0 ) {
      errorf("AG_parse_spec rc = %d\n", rc );
   }
   
   free( new_specfile_text );
   
   return rc;
}


// synchronize the MS's copy of the AG's filesystem with what the AG itself has.
// that is, delete from the MS anything that is in old_fs exclusively, and publish to the 
// MS anything that is in new_fs exclusively.  Update entries that are in the intersection.
// neither AG_fs must be locked.
// This does not affect cache validation flags or driver pointers of either FS
int AG_resync( struct AG_fs* old_fs, struct AG_fs* new_fs, AG_map_info_equality_func_t mi_equ ) {
   
   int rc = 0;
   
   AG_fs_map_t to_delete;
   AG_fs_map_t to_publish;
   AG_fs_map_t to_update;
   
   // find the difference between old_fs's contents and new_fs's contents
   AG_fs_rlock( old_fs );
   AG_fs_rlock( new_fs );
   
   dbprintf("%s", "Old FS:\n");
   AG_dump_fs_map( old_fs->fs );
   
   dbprintf("%s", "New FS:\n");
   AG_dump_fs_map( new_fs->fs );
   
   rc = AG_fs_map_transforms( old_fs->fs, new_fs->fs, &to_publish, &to_update, &to_delete, mi_equ );
   
   AG_fs_unlock( new_fs );
   AG_fs_unlock( old_fs );
   
   if( rc != 0 ) {
      errorf("AG_fs_map_transforms rc = %d\n", rc );
      return rc;
   }
   
   dbprintf("%s", "Entries not on the MS that should be published:\n");
   AG_dump_fs_map( &to_publish );
   
   dbprintf("%s", "Entries in the MS that should be updated:\n");
   AG_dump_fs_map( &to_update );
   
   dbprintf("%s", "Entries in the MS that should be deleted:\n");
   AG_dump_fs_map( &to_delete );
   
   
   AG_fs_wlock( new_fs );
   
   // apply our changes to it.
   // add new entries, and delete old ones.
   int publish_rc = AG_fs_create_all( new_fs->ms, new_fs->fs, &to_publish, new_fs->fs );
   
   if( publish_rc != 0 ) {
      errorf("ERR: AG_fs_create_all rc = %d\n", publish_rc );
      
      AG_fs_unlock( new_fs );
      return publish_rc;
   }
   
   int update_rc = AG_fs_update_all( new_fs->ms, new_fs->fs, &to_update, new_fs->fs );
   if( update_rc != 0 ) {
      errorf("ERR: AG_fs_update_all rc = %d\n", update_rc );
      
      AG_fs_unlock( new_fs );
      return update_rc;
   }
   
   AG_fs_unlock( new_fs );
   
   AG_fs_wlock( old_fs );
   
   int delete_rc = AG_fs_delete_all( new_fs->ms, old_fs->fs, &to_delete, old_fs->fs );
   if( delete_rc != 0 ) {
      errorf("ERR: AG_fs_delete_all rc = %d\n", delete_rc );
      
      AG_fs_unlock( old_fs );
      return delete_rc;
   }
   
   AG_fs_unlock( old_fs );
   return 0;
      
   /*
   // apply our changes to it.
   // add new entries, and delete old ones.
   int publish_rc = AG_fs_publish_map( new_fs, &to_publish, true );
   if( publish_rc != 0 ) {
      errorf("WARN: AG_fs_publish_map rc = %d\n", publish_rc );
      return publish_rc;
   }
   
   int update_rc = AG_fs_reversion_map( new_fs, &to_update, true );
   if( update_rc != 0 ) {
      errorf("WARN: AG_fs_reversion_map rc = %d\n", update_rc );
      return update_rc;
   }
   
   int delete_rc = AG_fs_delete_map( old_fs, &to_delete, true );
   if( delete_rc != 0 ) {
      errorf("WARN: AG_fs_delete_map rc = %d\n", delete_rc );
      return delete_rc;
   }
   */
   
   return 0;
}

// get the latest specfile, and use it to publish new entries and withdraw now-old entries
int AG_reload( struct AG_state* state ) {
   
   int rc = 0;
   
   AG_fs_map_t* new_fs = NULL;
   AG_config_t* new_config = NULL;
   
   // get the new fs data 
   rc = AG_reload_specfile( state, &new_fs, &new_config );
   if( rc != 0 ) {
      errorf("AG_reload_specfile rc = %d\n", rc );
      return rc;
   }
   
   // verify its integrity 
   rc = AG_validate_map_info( new_fs );
   if( rc != 0 ) {
      errorf("AG_validate_map_info rc = %d\n", rc );
      
      AG_fs_map_free( new_fs );
      
      delete new_fs;
      delete new_config;
      return rc;
   }
   
   // wrap the new mapping into an AG_fs
   struct AG_fs* fs_clone = CALLOC_LIST( struct AG_fs, 1 );
   
   // clone the fs (but prevent another thread from replacing state->ag_fs)
   AG_state_fs_rlock( state );
   AG_fs_rlock( state->ag_fs );
   
   rc = AG_fs_init( fs_clone, new_fs, state->ms );
   
   if( rc == 0 ) {
      // copy all cached data from the current fs to the new fs (from the spec file),
      // since the current fs is coherent but the specfile-loaded mapping is not 
      AG_fs_copy_cached_data( fs_clone, state->ag_fs );
   }
   
   AG_fs_unlock( state->ag_fs );
   AG_state_fs_unlock( state );
   
   if( rc != 0 ) {
      errorf("AG_fs_map_dup rc = %d\n", rc );
      
      AG_fs_free( fs_clone );   // NOTE: frees new_fs 
      free( fs_clone );
      
      delete new_config;
      
      return rc;
   }
   
   
   // for reloading, an element is in both the old and new fsmaps if it has the same AG-specific metadata 
   struct AG_reload_comparator {
      static bool equ( struct AG_map_info* mi1, struct AG_map_info* mi2 ) {
         return (mi1->driver    == mi2->driver    &&
                 mi1->file_perm == mi2->file_perm &&
                 mi1->reval_sec == mi2->reval_sec &&
                 mi1->type      == mi2->type      &&
                 strcmp( mi1->query_string, mi2->query_string ) == 0 );
      }
   };
   
   // Evolve the current AG_fs into the one described by the specfile.
   rc = AG_resync( state->ag_fs, fs_clone, AG_reload_comparator::equ );
   if( rc != 0 ) {
      errorf("WARN: AG_resync rc = %d\n", rc );
   }
   
   // make the newly-loaded AG_fs the current AG_fs
   AG_state_fs_wlock( state );
   
   struct AG_fs* old_fs = state->ag_fs;
   
   // stop all other accesses to this old fs
   AG_fs_wlock( old_fs );
   
   
   AG_fs_rlock( fs_clone );
   
   // swap in the new one
   state->ag_fs = fs_clone;
   
   // clear out reversioner entries and load the new ones
   rc = AG_reversioner_add_map_infos( state->reversioner, state->ag_fs->fs );
   
   AG_fs_unlock( fs_clone );
   
   AG_state_fs_unlock( state );
   
   // swap the new config in 
   AG_state_config_wlock( state );
   
   AG_config_t* old_config = state->config;
   state->config = new_config;
   
   AG_state_config_unlock( state );
   
   // delete the old fs, all of its entries, and config
   AG_fs_unlock( old_fs );
   AG_fs_free( old_fs );
   free( old_fs );
   
   delete old_config;
   
   if( rc != 0 ) {
      errorf("WARN: AG_reversioner_reload_map_infos rc = %d\n", rc );
   }
   
   return 0;
}


// view-change reload thread (triggerred in response to volume change)
void* AG_reload_thread_main( void* arg ) {
   
   struct AG_state* state = (struct AG_state*)arg;
   
   dbprintf("%s\n", "Starting specfile reload thread\n");
   
   while( state->specfile_reload_thread_running ) {
      
      // wait to reload...
      sem_wait( &state->specfile_reload_sem );
      
      // were we simply told to exit?
      if( !state->specfile_reload_thread_running ) {
         break;
      }
      
      // do the reload 
      AG_reload( state );
   }
   
   
   dbprintf("%s\n", "Specfile reload thread exit\n");
   return NULL;
}


// view-change callback for the volume.
// just wake up the reload thread 
int AG_view_change_callback( struct ms_client* ms, void* arg ) {
   
   struct AG_state* state = (struct AG_state*)arg;
   
   sem_post( &state->specfile_reload_sem );
   
   return 0;
}


// terminate on command--send ourselves a SIGTERM
int AG_event_handler_terminate( char* event_payload, void* unused ) {
   
   dbprintf("%s\n", "EVENT: Terminate\n");
   
   pid_t my_pid = getpid();
   
   // tell ourselves to die 
   return kill( my_pid, SIGTERM );
}

// pass an event to the driver 
int AG_event_handler_driver_ioctl( char* event_payload, void* unused ) {
   
   dbprintf("%s\n", "EVENT: Driver ioctl\n");
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      // nothing to do 
      return -ENOTCONN;
   }
   
   char* query_type = NULL;
   char* payload = NULL;
   size_t payload_len = 0;
   
   int rc = 0;
   
   // parse the payload 
   rc = AG_parse_driver_ioctl( event_payload, &query_type, &payload, &payload_len );
   if( rc != 0 ) {
      errorf("AG_parse_driver_ioctl rc = %d\n", rc );
      return rc;
   }
   
   struct AG_driver* driver = AG_lookup_driver( state->drivers, query_type );
   if( driver == NULL ) {
      errorf("No such driver '%s'\n", query_type );
      return -EPERM;
   }
   
   // call the driver's event handler 
   rc = AG_driver_handle_event( driver, payload, payload_len );
   if( rc != 0 ) {
      errorf("AG_driver_handle_event( driver = '%s' ) rc = %d\n", query_type, rc );
   }
   
   return rc;
}


// state initialization 
// NOTE: ag_opts will get shallow-copied
// if this method fails, the caller should call AG_state_free on the given state
int AG_state_init( struct AG_state* state, struct md_opts* opts, struct AG_opts* ag_opts, struct md_syndicate_conf* conf, struct ms_client* client ) {
   
   int rc = 0;
   AG_fs_map_t* parsed_map = NULL;
   
   memset( state, 0, sizeof(struct AG_state));
   
   // basic init 
   state->ms = client;
   state->conf = conf;
   
   sem_init( &state->specfile_reload_sem, 0, 0 );
   sem_init( &state->running_sem, 0, 0 );
   
   memcpy( &state->ag_opts, ag_opts, sizeof(struct AG_opts) );
   
   pthread_rwlock_init( &state->fs_lock, NULL );
   pthread_rwlock_init( &state->state_lock, NULL );
   pthread_rwlock_init( &state->config_lock, NULL );
   
   // make the instance nonce
   char* tmp = CALLOC_LIST( char, 16 );
   rc = md_read_urandom( tmp, 16 );
   if( rc != 0 ) {
      errorf("md_read_urandom rc = %d\n", rc );
      free( tmp );
      return rc;
   }
   
   rc = Base64Encode( tmp, 16, &state->inst_nonce );
   free( tmp );
   
   if( rc != 0 ) {
      errorf("Base64Encode rc = %d\n", rc );
      return rc;
   }
   
   dbprintf("Initializing AG instance %s\n", state->inst_nonce );
   
   // initialize drivers 
   state->drivers = new AG_driver_map_t();
   
   rc = AG_load_drivers( state->conf, state->drivers, state->ag_opts.driver_dir );
   if( rc != 0 ) {
      errorf("AG_load_drivers(%s) rc = %d\n", state->ag_opts.driver_dir, rc );
      return rc;
   }
   
   // initialize the path-mapping 
   state->ag_fs = CALLOC_LIST( struct AG_fs, 1 );
   
   // get the new FS mapping and config 
   rc = AG_reload_specfile( state, &parsed_map, &state->config );
   if( rc != 0 ) {
      errorf("AG_reload_specfile rc = %d\n", rc );
      return rc;
   }
   
   // verify its integrity 
   rc = AG_validate_map_info( parsed_map );
   if( rc != 0 ) {
      errorf("AG_validate_map_info rc = %d\n", rc );
      
      AG_fs_map_free( parsed_map );
      delete parsed_map;
      
      return rc;
   }
   
   // pass in the newly-parsed FS map into the AG_fs (which takes ownership)
   rc = AG_fs_init( state->ag_fs, parsed_map, state->ms );
   
   if( rc != 0 ) {
      errorf("AG_fs_init rc = %d\n", rc );
      
      
      AG_fs_map_free( parsed_map );
      delete parsed_map;
      
      return rc;
   }
   
   dbprintf("Loaded the following file mapping into %p\n", state->ag_fs->fs );
   AG_dump_fs_map( state->ag_fs->fs );
   
   // initialize HTTP 
   state->http = CALLOC_LIST( struct md_HTTP, 1 );
   
   rc = AG_http_init( state->http, state->conf );
   if( rc != 0 ) {
      errorf("AG_http_init rc = %d\n", rc );
      return rc;
   }
   
   // initialize event listener 
   state->event_listener = CALLOC_LIST( struct AG_event_listener, 1 );
   
   rc = AG_event_listener_init( state->event_listener, ag_opts );
   if( rc != 0 ) {
      errorf("AG_event_listener_init rc = %d\n", rc );
      return rc;
   }
   
   // initialize reversioner 
   state->reversioner = CALLOC_LIST( struct AG_reversioner, 1 );
   
   rc = AG_reversioner_init( state->reversioner, state );
   if( rc != 0 ) {
      errorf("AG_reversioner_init rc = %d\n", rc );
      return rc;
   }
   
   // set up block cache 
   state->cache = CALLOC_LIST( struct md_syndicate_cache, 1 );
   
   uint64_t block_size = ms_client_get_volume_blocksize( client );
   rc = md_cache_init( state->cache, conf, ag_opts->cache_soft_limit / block_size, ag_opts->cache_hard_limit / block_size );
   
   if( rc != 0 ) {
      errorf("md_cache_init rc = %d\n", rc );
      return rc;
   }
                       
   // state can be referenced 
   state->referenceable = true;
   
   // specfile reload thread should be running 
   state->specfile_reload_thread_running = true;
   
   // set up event handlers 
   AG_add_event_handler( state->event_listener, AG_EVENT_TERMINATE_ID, AG_event_handler_terminate, NULL );
   AG_add_event_handler( state->event_listener, AG_EVENT_DRIVER_IOCTL_ID, AG_event_handler_driver_ioctl, NULL );
   
   // set up reload callback 
   ms_client_set_view_change_callback( state->ms, AG_view_change_callback, state );
   
                       
   return 0;
}

// start the AG 
int AG_start( struct AG_state* state ) {
   
   int rc = 0;
   
   // start event listener before reloading--the driver might need it
   dbprintf("%s", "Starting event listener\n");
   
   rc = AG_event_listener_start( state->event_listener );
   if( rc != 0 ) {
      errorf("AG_event_listener_start rc = %d\n", rc );
      
      return rc;
   }
   
   // start up the block cache 
   dbprintf("%s", "Starting block cache\n");
   
   rc = md_cache_start( state->cache );
   if( rc != 0) {
      errorf("md_cache_start rc = %d\n", rc );
      
      return rc;
   }
   
   dbprintf("%s", "(Re)synchronizing dataset\n");
   
   // get the list of entries that are already on the MS 
   AG_fs_map_t* on_MS = NULL;
   
   rc = AG_download_existing_fs_map( state->ms, &on_MS, true );
   if( rc != 0 ) {
      errorf("AG_download_existing_fs_map rc = %d\n", rc );
      
      return rc;
   }
   
   // wrap on_MS into an AG_fs 
   // NOTE: we don't care about drivers for this map; only consistency information
   struct AG_fs on_MS_fs;
   rc = AG_fs_init( &on_MS_fs, on_MS, state->ms );
   
   if( rc != 0 ) {
      errorf("AG_fs_init(on_MS) rc = %d\n", rc );
      
      AG_fs_map_free( on_MS );
      delete on_MS;
      
      return rc;
   }
   
   // copy all cached data from on_MS_fs to the new mapping (from the spec file),
   // since on_MS_fs is coherent but the specfile-loaded mapping is not 
   AG_fs_copy_cached_data( state->ag_fs, &on_MS_fs );
   
   // for initializing, an entry is common to both the specfile and the MS-hosted copy
   // if the MS entry's metadata reflects the entry's metadata in the specfile.
   struct AG_reload_comparator {
      static bool equ( struct AG_map_info* mi1, struct AG_map_info* mi2 ) {
         return (mi1->file_perm == mi2->file_perm &&
                 mi1->reval_sec == mi2->reval_sec &&
                 mi1->type      == mi2->type );
      }
   };
   
   // the AG_fs on the MS is the "old" mapping,
   // and the one we loaded from the spec file is
   // the "new" mapping.  Evolve the old into the new on the MS.
   rc = AG_resync( &on_MS_fs, state->ag_fs, AG_reload_comparator::equ );
   
   AG_fs_free( &on_MS_fs );
   
   if( rc != 0 ) {
      errorf("ERR: AG_resync rc = %d\n", rc );
      
      return rc;
   }
   
   // start HTTP 
   dbprintf("%s", "Starting HTTP server\n" );
   
   rc = md_start_HTTP( state->http, state->conf->portnum, state->conf );
   if( rc != 0 ) {
      errorf("ERR: md_start_HTTP rc = %d\n", rc );
      return rc;
   }
   
   // start performing invalidations
   AG_state_fs_rlock( state );
   AG_fs_rlock( state->ag_fs );
   
   dbprintf("%s", "Starting with the following FS map:\n");
   AG_dump_fs_map( state->ag_fs->fs );
   
   rc = 0;
   
   if( state->ag_opts.reversion_on_startup ) {
      dbprintf("%s", "Queuing all datasets for reversion\n");
      rc = AG_reversioner_add_map_infos( state->reversioner, state->ag_fs->fs );
   }
   
   AG_fs_unlock( state->ag_fs );
   AG_state_fs_unlock( state );
   
   if( rc != 0 ) {
      errorf("AG_reversioner_add_map_infos rc = %d\n", rc );
      return rc;
   }
   
   // start the reversioner 
   dbprintf("%s", "Starting reversioner\n");
   
   rc = AG_reversioner_start( state->reversioner );
   if( rc != 0 ) {
      errorf("AG_reversioner_start rc = %d\n", rc );
      return rc;
   }
   
   // start the reloader 
   dbprintf("%s", "Starting specfile reload thread\n");
   
   state->specfile_reload_thread = md_start_thread( AG_reload_thread_main, state, false );
   
   if( state->specfile_reload_thread < 0 ) {
      errorf("ERR: md_start_thread rc = %d\n", (int)state->specfile_reload_thread );
      return (int)state->specfile_reload_thread;
   }

   state->running = true;
   
   return 0;
}


// shut down the AG
int AG_stop( struct AG_state* state ) {
   
   dbprintf("%s", "Shutting down specfile reloader\n");
   
   // wake up the reload callback and tell it to exit
   state->specfile_reload_thread_running = false;
   AG_view_change_callback( state->ms, state );
   
   // de-register the viewchange callback
   ms_client_set_view_change_callback( state->ms, NULL, NULL );
   
   // join with the reload thread
   pthread_cancel( state->specfile_reload_thread );
   pthread_join( state->specfile_reload_thread, NULL );
   
   dbprintf("%s", "Shutting down HTTP server\n");
   md_stop_HTTP( state->http );
   
   dbprintf("%s", "Shutting down event listener\n");
   AG_event_listener_stop( state->event_listener );
   
   dbprintf("%s", "Shutting down reversioner\n");
   AG_reversioner_stop( state->reversioner );
   
   dbprintf("%s", "Shutting down block cache\n");
   md_cache_stop( state->cache );
   
   state->running = false;
   
   return 0;
}


// free state 
int AG_state_free( struct AG_state* state ) {
   
   if( state->running || state->specfile_reload_thread_running ) {
      // need to stop first 
      return -EINVAL;
   }
   
   dbprintf("Freeing AG instance %s\n", state->inst_nonce );
   
   // state can no longer be referenced 
   state->referenceable = false;
   
   // wait for all other threads to release state 
   pthread_rwlock_wrlock( &state->state_lock );
   
   if( state->http != NULL ) {
      md_free_HTTP( state->http );
      free( state->http );
      state->http = NULL;
   }
   
   if( state->event_listener != NULL ) {
      AG_event_listener_free( state->event_listener );
      free( state->event_listener );
      state->event_listener = NULL;
   }
   
   if( state->reversioner != NULL ) {
      AG_reversioner_free( state->reversioner );
      free( state->reversioner );
      state->reversioner = NULL;
   }
   
   if( state->drivers != NULL ) {
      AG_shutdown_drivers( state->drivers );
      delete state->drivers;
      state->drivers = NULL;
   }
   
   if( state->ag_fs != NULL ) {
      AG_fs_free( state->ag_fs );
      free( state->ag_fs );
      state->ag_fs = NULL;
   }
   
   if( state->config != NULL ) {
      delete state->config;
      state->config = NULL;
   }
   
   if( state->cache != NULL ) {
      md_cache_destroy( state->cache );
      free( state->cache );
      state->cache = NULL;
   }
   
   sem_destroy( &state->specfile_reload_sem );
   sem_destroy( &state->running_sem );
   
   // opts to free 
   char* opts_to_free[] = {
      state->ag_opts.sock_path,
      state->ag_opts.logfile_path,
      state->ag_opts.driver_dir,
      state->ag_opts.spec_file_path,
      NULL
   };
   
   for( int i = 0; opts_to_free[i] != NULL; i++ ) {
      if( opts_to_free[i] != NULL ) {
         free( opts_to_free[i] );
      }
   }
   
   if( state->inst_nonce ) {
      free( state->inst_nonce );
      state->inst_nonce = NULL;
   }
   
   pthread_rwlock_unlock( &state->state_lock );
   
   pthread_rwlock_destroy( &state->fs_lock );
   pthread_rwlock_destroy( &state->config_lock );
   pthread_rwlock_destroy( &state->state_lock );
   
   memset( &state->ag_opts, 0, sizeof(struct AG_opts) );
   memset( state, 0, sizeof(struct AG_state) );
   return 0;
}


// dump config to stdout 
void AG_dump_config( AG_config_t* config ) {
   
   dbprintf("Begin dump config %p\n", config );
   
   for( AG_config_t::iterator itr = config->begin(); itr != config->end(); itr++ ) {
      dbprintf("'%s' = '%s'\n", itr->first.c_str(), itr->second.c_str() );
   }
   
   dbprintf("End dump config %p\n", config );
}

// get a config variable 
char* AG_get_config_var( struct AG_state* state, char const* varname ) {
   
   char* ret = NULL;
   
   string varname_s(varname);
   
   AG_state_config_rlock( state );
   
   AG_config_t::iterator itr = state->config->find( varname_s );
   if( itr != state->config->end() ) {
      ret = strdup( itr->second.c_str() );
   }
   
   AG_state_config_unlock( state );
   
   return ret;
}


// AG-specific usage
static void AG_usage(void) {
   fprintf(stderr, "\n\
AG-specific options:\n\
   -e PATH\n\
            (Required) Path to a UNIX domain socket\n\
            over which to send/receive events.\n\
   -i PATH\n\
            Path to which to log runtime information, if not running\n\
            in the foreground.\n\
   -D DIR\n\
            Path to the directory that contains the storage drivers.\n\
   -s PATH\n\
            Path to an on-disk hierarchy spec file to be used to populate\n\
            this AG's volume.  If not supplied, the MS-served hierarchy spec\n\
            file will be used instead (the default).\n\
   -n\n\
            On start-up, queue all datasets for reversion.  This updates the\n\
            consistency information for each dataset on the MS, and invokes\n\
            each dataset driver's reversion method.\n\
   -l NUM\n\
            Soft size limit (in bytes) of the block cache.  Default is %ld\n\
   -L NUM\n\
            Hard size limit (in bytes) of the block cache.  Default is %ld\n\
\n", AG_CACHE_DEFAULT_SOFT_LIMIT, AG_CACHE_DEFAULT_HARD_LIMIT );
}

// clear global AG opts buffer
int AG_opts_init() {
   memset( &g_AG_opts, 0, sizeof(struct AG_opts));
   return 0;
}


// add default options
int AG_opts_add_defaults( struct md_syndicate_conf* conf, struct AG_opts* ag_opts ) {
   
   char* storage_root = conf->storage_root;
   
   // default values 
   if( ag_opts->sock_path == NULL ) {
      ag_opts->sock_path = md_fullpath( storage_root, "AG.socket", NULL );
   }
   
   if( ag_opts->logfile_path == NULL ) {
      ag_opts->logfile_path = md_fullpath( storage_root, "AG.log", NULL );
   }
   
   if( ag_opts->driver_dir == NULL ) {
      ag_opts->driver_dir = getcwd( NULL, 0 );     // look locally by default
   }
   
   if( ag_opts->cache_soft_limit == 0 ) {
      ag_opts->cache_soft_limit = AG_CACHE_DEFAULT_SOFT_LIMIT;
   }
   
   if( ag_opts->cache_hard_limit == 0 ) {
      ag_opts->cache_hard_limit = AG_CACHE_DEFAULT_HARD_LIMIT;
   }
   
   return 0;
}

// duplicate AG global opts buffer 
int AG_opts_get( struct AG_opts* opts ) {
   memcpy( opts, &g_AG_opts, sizeof(struct AG_opts) );
   
   // deep-copy dynamically-allocatd fields 
   opts->sock_path = strdup_or_null( g_AG_opts.sock_path );
   opts->logfile_path = strdup_or_null( g_AG_opts.logfile_path );
   opts->driver_dir = strdup_or_null( g_AG_opts.driver_dir );
   
   return 0;
}

// opts handler 
int AG_handle_opt( int opt_c, char* opt_s ) {
   
   int rc = 0;
   
   switch( opt_c ) {
      
      case 'e': {
         
         if( g_AG_opts.sock_path != NULL ) {
            free( g_AG_opts.sock_path );
         }
         
         g_AG_opts.sock_path = strdup( opt_s );
         
         break;
      }
      case 'i': {
         
         if( g_AG_opts.logfile_path != NULL ) {
            free( g_AG_opts.logfile_path );
         }
         
         g_AG_opts.logfile_path = strdup( opt_s );
         
         break;
      }
      case 'D': {
         
         if( g_AG_opts.driver_dir != NULL ) {
            free( g_AG_opts.driver_dir );
         }
         
         g_AG_opts.driver_dir = strdup( opt_s );
         
         break;
      }
      case 's': {
         
         if( g_AG_opts.spec_file_path != NULL ) {
            free( g_AG_opts.spec_file_path );
         }
         
         g_AG_opts.spec_file_path = strdup( opt_s );
         
         break;
      }
      case 'n': {
         
         g_AG_opts.reversion_on_startup = true;
         break;
      }
      case 'l': {
         
         long lim = 0;
         rc = md_opts_parse_long( opt_c, opt_s, &lim );
         if( rc == 0 ) {
            
            g_AG_opts.cache_soft_limit = (size_t)lim;
         }
         else {
            
            errorf("Failed to parse -l, rc = %d\n", rc );
            rc = -1;
         }
         break;
      }
      case 'L': {
         
         long lim = 0;
         rc = md_opts_parse_long( opt_c, opt_s, &lim );
         if( rc == 0 ) {
            
            g_AG_opts.cache_hard_limit = (size_t)lim;
         }
         else {
            
            errorf("Failed to parse -L, rc = %d\n", rc );
            rc = -1;
         }
         break;
      }
      default: {
         errorf("Unrecognized option '%c'\n", opt_c );
         rc = -1;
         break;
      }
   }
   return rc;
}

// main method
int AG_main( int argc, char** argv ) {
   curl_global_init(CURL_GLOBAL_ALL);

   // start up protocol buffers
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = 0;
   
   // register our death handlers 
   signal( SIGQUIT, AG_death_signal_handler );
   signal( SIGINT,  AG_death_signal_handler );
   signal( SIGTERM, AG_death_signal_handler );
   
   // get state 
   struct AG_state* state = &global_state;
   memset( state, 0, sizeof(struct AG_state) );
   
   // syndicate config and MS client
   struct md_syndicate_conf* conf = CALLOC_LIST( struct md_syndicate_conf, 1 );
   struct ms_client* ms = CALLOC_LIST( struct ms_client, 1 );
   
   // parse options
   struct md_opts opts;
   AG_opts_init();
   
   memset( &opts, 0, sizeof(struct md_opts));
   
   // get options
   rc = md_parse_opts( &opts, argc, argv, NULL, "e:l:D:s:n", AG_handle_opt );
   if( rc != 0 ) {
      md_common_usage( argv[0] );
      AG_usage();
      exit(1);
   }
   
   // enable debugging 
   md_debug( conf, opts.debug_level );
   
   // load config file
   md_default_conf( conf, SYNDICATE_AG );
   
   // read the config file
   if( opts.config_file != NULL ) {
      rc = md_read_conf( opts.config_file, conf );
      if( rc != 0 ) {
         errorf("ERR: md_read_conf(%s) rc = %d\n", opts.config_file, rc );
         exit(1);
      }
   }
   
   // initialize libsyndicate
   rc = md_init( conf, ms, opts.ms_url, opts.volume_name, opts.gateway_name, opts.username, (char const*)opts.password.ptr, (char const*)opts.user_pkey_pem.ptr,
                           opts.volume_pubkey_path, opts.gateway_pkey_path, (char const*)opts.gateway_pkey_decryption_password.ptr,
                           opts.tls_pkey_path, opts.tls_cert_path, opts.storage_root, opts.syndicate_pubkey_path );
   
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      exit(1);
   }
   
   // get back AG opts 
   struct AG_opts ag_opts;
   AG_opts_get( &ag_opts );
   
   // load default AG options, if we're missing some
   AG_opts_add_defaults( conf, &ag_opts );
   
   // initialize AG signal handling 
   rc = AG_signal_listener_init();
   
   if( rc != 0 ) {
      errorf("AG_signal_listener_init rc = %d\n", rc );
      exit(1);
   }
   
   // initialize AG state 
   rc = AG_state_init( state, &opts, &ag_opts, conf, ms );
   
   if( rc != 0 ) {
      errorf("AG_state_init rc = %d\n", rc );
      exit(1);
   }
   
   // start signal handlers 
   rc = AG_signal_listener_start();
   
   if( rc != 0 ) {
      errorf("AG_signal_listener_start rc = %d\n", rc );
      exit(1);
   }
   
   // start running 
   rc = AG_start( state );
   
   if( rc != 0 ) {
      errorf("AG_start rc = %d\n", rc );
   }
   
   else {
      // wait to die 
      while( true ) {
         rc = sem_wait( &state->running_sem );
         if( rc != 0 ) {
            
            rc = -errno;
            
            // ignore interruptions 
            if( rc == -EINTR ) {
               continue;
            }
         }
         else {
            // got woken up by a signal handler
            break;
         }
      }
   }
   
   // stop running 
   rc = AG_stop( state );
   
   if( rc != 0 ) {
      errorf("WARN: AG_stop rc = %d\n", rc );
   }
   
   // stop signal handlers and restore old ones 
   rc = AG_signal_listener_stop();
   if( rc != 0 ) {
      errorf("WARN: AG_signal_listener_stop rc = %d\n", rc );
   }
   
   // shut down AG
   rc = AG_state_free( state );
   
   if( rc != 0 ) {
      errorf("WARN: AG_state_free rc = %d\n", rc );
   }
   
   // shut down signal handlers 
   rc = AG_signal_listener_free();
   if( rc != 0 ) {
      errorf("WARN: AG_signal_listener_free rc = %d\n", rc );
   }
   
   // shutdown libsyndicate
   
   md_free_conf( conf );
   ms_client_destroy( ms );
   
   free( ms );
   free( conf );
   
   md_shutdown();
   
   return 0;
}


