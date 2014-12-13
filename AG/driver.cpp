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

#include "driver.h"
#include "events.h"
#include "map-info.h"
#include "core.h"
#include "cache.h"
#include "http.h"
#include "workqueue.h"

MD_CLOSURE_PROTOTYPE_BEGIN( AG_CLOSURE_PROTOTYPE )
   MD_CLOSURE_CALLBACK( "get_dataset_block" ),
   MD_CLOSURE_CALLBACK( "connect_dataset_block" ),
   MD_CLOSURE_CALLBACK( "close_dataset_block" ),
   MD_CLOSURE_CALLBACK( "stat_dataset" ),
   MD_CLOSURE_CALLBACK( "reversion_dataset" ),
   MD_CLOSURE_CALLBACK( "driver_init" ),
   MD_CLOSURE_CALLBACK( "driver_shutdown" ),
   MD_CLOSURE_CALLBACK( "get_query_type" ),
   MD_CLOSURE_CALLBACK( "handle_event" )
MD_CLOSURE_PROTOTYPE_END


// load a driver from a .so file.
// all methods must be present.
int AG_load_driver( struct md_syndicate_conf* conf, struct AG_driver* driver, char const* driver_path ) {
   
   int rc = 0;
   
   // make the closure
   driver->closure = CALLOC_LIST( struct md_closure, 1 );
   
   rc = md_closure_init_bin( conf, driver->closure, driver_path, AG_CLOSURE_PROTOTYPE, true );
   if( rc != 0 ) {
      errorf("md_closure_init_bin(%s) rc = %d\n", driver_path, rc );
      
      free( driver->closure );
      return rc;
   }
   
   // get the query type 
   char* qtype = AG_driver_get_query_type( driver );
   driver->query_type = qtype;
   
   return 0;
}

// unload a driver
// NOTE: you must call AG_driver_shutdown first
int AG_unload_driver( struct AG_driver* driver ) {
   
   int rc = 0;
   
   if( driver->query_type ) {
      free( driver->query_type );
      driver->query_type = NULL;
   }
   
   // destroy the closure 
   rc = md_closure_shutdown( driver->closure );
   if( rc != 0 ) {
      errorf("WARN: md_closure_shutdown rc = %d\n", rc );
   }
   
   free( driver->closure );
   
   memset( driver, 0, sizeof(struct AG_driver) );
   
   return 0;
}


// look up a driver 
struct AG_driver* AG_lookup_driver( AG_driver_map_t* driver_map, char const* driver_query_type ) {
   
   string driver_name( driver_query_type );
   
   // look up the driver 
   AG_driver_map_t::iterator itr = driver_map->find( driver_name );
   if( itr == driver_map->end() ) {
      errorf("No driver for '%s' loaded\n", driver_query_type );
      return NULL;
   }
   
   struct AG_driver* driver = itr->second;

   return driver;
}


// initialize the driver
int AG_driver_init( struct AG_driver* driver ) {
   
   void* driver_state = NULL;
   int ret = 0;
   
   if( md_closure_find_callback( driver->closure, "driver_init" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "driver_init", AG_driver_init_callback_t, &driver_state );
   }
   else {
      errorf("%s", "WARN: driver_init stub\n");
   }
   
   driver->driver_state = driver_state;
   
   return ret;
}

// shut down the driver
int AG_driver_shutdown( struct AG_driver* driver ) {
   
   int ret = 0;
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   if( md_closure_find_callback( driver->closure, "driver_shutdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "driver_shutdown", AG_driver_shutdown_callback_t, driver->driver_state );
   }
   else {
      errorf("%s", "WARN: driver_init stub\n");
   }
   
   if( ret == 0 ) {
      driver->driver_state = NULL;
   }
   
   return ret;
}


// load drivers
int AG_load_drivers( struct md_syndicate_conf* conf, AG_driver_map_t* driver_map, char const* driver_dir ) {
   
   dbprintf("Loading AG drivers from %s\n", driver_dir);
   
   // scan the directory looking for .so files 
   DIR *dirp = opendir(driver_dir);
   if (dirp == NULL) {
      int errsv = -errno;
      errorf("Failed to open %s, errno = %d\n", driver_dir, errsv);
      return errsv;
   }
   
   int rc = 0;
   ssize_t len = offsetof(struct dirent, d_name) + pathconf(driver_dir, _PC_NAME_MAX) + 1;
   
   struct dirent *dentry_dp = NULL;
   struct dirent *dentry = CALLOC_LIST( struct dirent, len );
   
   while(true) {
      
      rc = readdir_r(dirp, dentry, &dentry_dp);
      if( rc != 0 ) {
         errorf("readdir_r(%s) rc = %d\n", driver_dir, rc );
         break;
      }
      
      if( dentry_dp == NULL ) {
         // no more entries
         break;
      }
      
      // full path to this file 
      char* path = md_fullpath( driver_dir, dentry->d_name, NULL );
      
      // is this a regular file? does it end in .so?
      struct stat sb;
      rc = stat( path, &sb );
      
      if( rc != 0 ) {
         
         // stat error 
         rc = -errno;
         errorf("WARN: stat(%s) rc = %d\n", path, rc );
         
         free( path );
      
         continue;
      }
      
      if( !S_ISREG( sb.st_mode ) ) {
         
         free( path );
      
         // not a regular file 
         continue;
      }
      
      if( strlen( dentry->d_name ) < strlen(" .so") ) {
         
         free( path );
      
         // not a driver file 
         continue;
      }
      
      if( strcmp( dentry->d_name + strlen(dentry->d_name) - 3, ".so" ) != 0 ) {
         
         free( path );
      
         // not a driver file 
         continue;
      }
       
      dbprintf("Load driver %s\n", path );
      
      struct AG_driver* driver = CALLOC_LIST( struct AG_driver, 1 );
      
      // load this driver 
      rc = AG_load_driver( conf, driver, path );
      if( rc != 0 ) {
         
         errorf("WARN: AG_load_driver(%s) rc = %d\n", path, rc );
         free( path );
         free( driver );
         continue;
      }
      
      // get its query name 
      char* driver_query_type = AG_driver_get_query_type( driver );
      if( driver_query_type == NULL ) {
         
         errorf("Driver %s does not identify a supported query type.  Does it implement the get_query_type() method?\n", path );
         free( path );
         
         AG_unload_driver( driver );
         free( driver );
         continue;
      }
      
      dbprintf("Will use driver %s to handle '%s' queries\n", path, driver_query_type );
      
      // store it 
      string driver_query_type_s( driver_query_type );
      (*driver_map)[ driver_query_type_s ] = driver;
      
      free( path );
      free( driver_query_type );
   }
   
   // clean up
   closedir( dirp );
   free( dentry );
   
   return 0;
}


// shut down drivers 
int AG_shutdown_drivers( AG_driver_map_t* driver_map ) {
   
   dbprintf("%s", "Shutting down AG drivers...\n");
   
   int rc = 0;
   
   for( AG_driver_map_t::iterator itr = driver_map->begin(); itr != driver_map->end(); itr++ ) {
      
      struct AG_driver* driver = itr->second;
      
      char* query_type = AG_driver_get_query_type( driver );
      
      dbprintf("Shut down driver '%s'\n", query_type );
      
      free( query_type );
      
      // tell the driver to shutdown 
      rc = AG_driver_shutdown( driver );
      if( rc != 0 ) {
         errorf("WARN: AG_driver_shutdown(%s) rc = %d\n", driver->query_type, rc );
      }
      
      // unload the driver
      rc = AG_unload_driver( driver );
      if( rc != 0 ) {
         errorf("WARN: AG_unload_driver rc = %d\n", rc );
      }
      
      free( driver );
   }
   
   driver_map->clear();
   
   return 0;
}


// set up connection state for a block
int AG_driver_connect_block( struct AG_driver* driver, struct AG_connection_context* ctx ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   int ret = 0;
   void* connection_state = NULL;
   
   if( md_closure_find_callback( driver->closure, "connect_dataset_block" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "connect_dataset_block", AG_connect_block_callback_t, ctx, driver->driver_state, &connection_state );
   }
   else {
      errorf("%s", "WARN: connect_dataset_block stub\n");
   }
   
   if( ret == 0 ) {
      ctx->driver_connection_state = connection_state;
   }
   
   return ret;
}

// get a block 
ssize_t AG_driver_get_block( struct AG_driver* driver, struct AG_connection_context* ctx, uint64_t block_id, char* block_buf, size_t block_buf_len ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   ssize_t ret = 0;
   
   if( md_closure_find_callback( driver->closure, "get_dataset_block" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "get_dataset_block", AG_get_block_callback_t, ctx, block_id, block_buf, block_buf_len, ctx->driver_connection_state );
   }
   else {
      errorf("%s", "WARN: get_dataset_block stub\n");
   }
   
   return ret;
}

// clean up connection state for a block 
int AG_driver_cleanup_block( struct AG_driver* driver, struct AG_connection_context* ctx ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   int ret = 0;
   
   if( md_closure_find_callback( driver->closure, "close_dataset_block" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "close_dataset_block", AG_cleanup_block_callback_t, ctx->driver_connection_state );
   }
   else {
      errorf("%s", "WARN: close_dataset_block stub\n");
   }
   
   return ret;
}

// stat a dataset
int AG_driver_stat( struct AG_driver* driver, char const* path, struct AG_map_info* map_info, struct AG_driver_publish_info* pub_info ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   int ret = 0;
   
   if( md_closure_find_callback( driver->closure, "stat_dataset" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "stat_dataset", AG_stat_dataset_callback_t, path, map_info, pub_info, driver->driver_state );
   }
   else {
      errorf("%s", "WARN: stat_dataset stub\n");
   }
   
   return ret;
}


// indicate that we've reversioned a dataset
int AG_driver_reversion( struct AG_driver* driver, char const* path, struct AG_map_info* map_info ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   int ret = 0;
   
   if( md_closure_find_callback( driver->closure, "reversion_dataset" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "reversion_dataset", AG_reversion_callback_t, path, map_info, driver->driver_state );
   }
   else {
      errorf("%s", "WARN: reversion_dataset stub\n");
   }
   
   return ret;
}

// get the type of query this driver supports 
char* AG_driver_get_query_type( struct AG_driver* driver ) {
   
   if( driver == NULL ) {
      return NULL;
   }
   
   char* ret = NULL;
   
   if( md_closure_find_callback( driver->closure, "get_query_type" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "get_query_type", AG_query_type_callback_t );
   }
   else {
      errorf("%s", "WARN: get_query_type stub\n");
   }
   
   return ret;
}


// handle a driver-specific event 
int AG_driver_handle_event( struct AG_driver* driver, char* event_payload, size_t event_payload_len ) {
   
   if( driver == NULL ) {
      return -EINVAL;
   }
   
   int ret = 0;
   
   if( md_closure_find_callback( driver->closure, "handle_event" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver->closure, "handle_event", AG_driver_event_callback_t, event_payload, event_payload_len, driver->driver_state );
   }
   else {
      errorf("%s", "WARN: handle_event stub\n");
   }
   
   return ret;
}


// stable API for getting a config var 
char* AG_driver_get_config_var( char const* config_varname ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return NULL;
   }
   else {
      char* ret = AG_get_config_var( state, config_varname );
      AG_release_state( state );
      
      return ret;
   }
}

// stable API for getting the requeted path 
char* AG_driver_get_request_path( struct AG_connection_context* ctx ) {
   return strdup( ctx->reqdat.fs_path );
}

// stable API for getting the requested path's query string
char* AG_driver_get_query_string( struct AG_connection_context* ag_ctx ) {
   return strdup_or_null( ag_ctx->query_string );
}

// stable API for getting the query string from a map_info 
char* AG_driver_get_query_string_mi( struct AG_map_info* mi ) {
   return strdup_or_null( mi->query_string );
}

// stable API for getting the requested file version 
int64_t AG_driver_get_request_file_version( struct AG_connection_context* ctx ) {
   return ctx->reqdat.file_version;
}

// stable API for getting the requested block ID 
uint64_t AG_driver_get_request_block_id( struct AG_connection_context* ctx ) {
   return ctx->reqdat.block_id;
}

// stable API for getting the requested block version 
int64_t AG_driver_get_request_block_version( struct AG_connection_context* ctx ) {
   return ctx->reqdat.block_version;
}

// stable API for calculating the size (in bytes) of a block
uint64_t AG_driver_get_block_size(void) {
   struct AG_state* state = AG_get_state();
   
   if( state != NULL ) {
      uint64_t block_size = ms_client_get_volume_blocksize( state->ms );
      AG_release_state( state );
      
      return block_size;
   }
   else {
      return 0;
   }
}

// stable API for setting the HTTP status of a connection 
void AG_driver_set_HTTP_status( struct AG_connection_context* ctx, int http_status ) {
   ctx->http_status = http_status;
}

// stable API for setting a signal handler 
int AG_driver_set_signal_handler( int signum, sighandler_t sighandler ) {
   return AG_add_signal_handler( signum, sighandler );
}

// stable API to request that a dataset be republished 
int AG_driver_request_reversion( char const* path, struct AG_driver_publish_info* pubinfo ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   // enqueue it into the reversioner
   int rc = AG_workqueue_add_reversion( state->wq, path, pubinfo );
   if( rc != 0 ) {
      errorf("AG_workqueue_add_reversion(%s) rc = %d\n", path, rc );
   }
   
   AG_release_state( state );
   
   return rc;
}


// stable API to get a chunk of data from the cache 
int AG_driver_cache_get_chunk( char const* name, char** chunk, size_t* chunk_len ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   // ensure uniqueness
   char* chunk_name = md_fullpath( state->inst_nonce, name, NULL );
   
   int rc = AG_cache_get_block( state, chunk_name, -1, -1, -1, chunk, chunk_len );
   
   free( chunk_name );
   
   AG_release_state( state );
   
   return rc;
}

// stable API to promote a chunk in the cache
int AG_driver_cache_promote_chunk( char const* name ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   // ensure uniqueness
   char* chunk_name = md_fullpath( state->inst_nonce, name, NULL );
   
   int rc = AG_cache_promote_block( state, chunk_name, -1, -1, -1 );
   
   free( chunk_name );
   
   AG_release_state( state );
   
   return rc;
}

// stable API to put a chunk to the cache, asynchronously
int AG_driver_cache_put_chunk_async( char const* name, char* chunk, size_t chunk_len ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   // ensure uniqueness 
   char* chunk_name = md_fullpath( state->inst_nonce, name, NULL );
   
   int rc = AG_cache_put_block_async( state, chunk_name, -1, -1, -1, chunk, chunk_len );
   
   free( chunk_name );
   
   AG_release_state( state );
   
   return rc;
}

// stable API to evict a chunk from the cache
int AG_driver_cache_evict_chunk( char const* name ) {
   
   struct AG_state* state = AG_get_state();
   if( state == NULL ) {
      return -ENOTCONN;
   }
   
   // ensure uniqueness 
   char* chunk_name = md_fullpath( state->inst_nonce, name, NULL );
   
   int rc = AG_cache_evict_block( state, chunk_name, -1, -1, -1 );
   
   free( chunk_name );
   
   AG_release_state( state );
   
   return rc;
}

// stable API to get the query string from a map_info 
char* AG_driver_map_info_get_query_string( struct AG_map_info* mi ) {
   
   if( mi->query_string ) {
      return strdup( mi->query_string );
   }
   else {
      return NULL;
   }
}

// stable API to get the file version from a map_info
int64_t AG_driver_map_info_get_file_version( struct AG_map_info* mi ) {
   
   return mi->file_version;
}
