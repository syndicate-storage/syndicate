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

struct md_closure_callback_entry UG_CLOSURE_PROTOTYPE[] = {
   MD_CLOSURE_CALLBACK( "connect_cache" ),
   MD_CLOSURE_CALLBACK( "write_block_preup" ),
   MD_CLOSURE_CALLBACK( "write_manifest_preup" ),
   MD_CLOSURE_CALLBACK( "read_block_postdown" ),
   MD_CLOSURE_CALLBACK( "read_manifest_postdown" ),
   MD_CLOSURE_CALLBACK( "chcoord_begin" ),
   MD_CLOSURE_CALLBACK( "chcoord_end" )
};


// initialize the driver
int driver_init( struct fs_core* core, struct md_closure** driver ) {
   int rc = 0;
   
   // get the base64-encoded driver text 
   char* driver_text_b64 = NULL;
   size_t driver_text_len_b64 = 0;
   
   rc = ms_client_get_closure_text( core->ms, &driver_text_b64, &driver_text_len_b64 );
   if( rc != 0 ) {
      errorf("ms_client_get_closure_text rc = %d\n", rc );
      
      if( rc == -ENOENT ) {
         // no driver is fine
         rc = 0;
      }
   }
   else {
      // load it up!
      rc = md_install_binary_closure( core->conf, driver, UG_CLOSURE_PROTOTYPE, driver_text_b64, driver_text_len_b64 );
      
      free( driver_text_b64 );
      
      if( rc != 0 ) {
         errorf("md_install_binary_closure rc = %d\n", rc );
      }
   }
   
   return rc;
}


// reload the driver 
int driver_reload( struct fs_core* core, struct md_closure* driver ) {
   // get the binary closure text 
   // get the base64-encoded driver text 
   char* driver_text_b64 = NULL;
   size_t driver_text_len_b64 = 0;
   
   int rc = ms_client_get_closure_text( core->ms, &driver_text_b64, &driver_text_len_b64 );
   if( rc != 0 ) {
      errorf("ms_client_get_closure_text rc = %d\n", rc );
      rc = -ENOENT;
   }
   else {
      // convert to binary 
      char* driver_text = NULL;
      size_t driver_text_len = 0;
      
      rc = Base64Decode( driver_text_b64, driver_text_len_b64, &driver_text, &driver_text_len );
      
      free( driver_text_b64 );
      
      if( rc != 0 ) {
         errorf("Failed to decode driver, rc = %d\n", rc );
         rc = -EINVAL;
      }
      else {
         // load the code 
         rc = md_closure_reload( core->conf, driver, driver_text, driver_text_len );
         
         if( rc != 0 ) {
            errorf("md_closure_reload rc = %d\n", rc );
            rc = -ENODATA;
         }
      }
   }
   
   return rc;
}


// shut down the driver 
int driver_shutdown( struct md_closure* driver ) {
   return md_closure_shutdown( driver );
}

// connect to the cache (libsyndicate method)
int driver_connect_cache( struct md_syndicate_conf* conf, CURL* curl, char const* url, void* cls ) {
   struct driver_connect_cache_cls* driver_cls = (struct driver_connect_cache_cls*)cls;
   
   int ret = 0;
   
   if( md_closure_find_callback( driver_cls->driver, "connect_cache" ) != NULL ) { 
   
      // call our closure's connect_cache method
      MD_CLOSURE_CALL( ret, driver_cls->driver, "connect_cache", driver_connect_cache_func, conf, curl, url, driver_cls->driver->cls );
   
   }
   else {
      errorf("%s", "WARN: connect_cache stub\n");
      ms_client_connect_cache( conf, curl, url, driver_cls->client->volume->cache_closure );
      ret = 0;
   }
   
   return ret;
}

// process data before uploading 
int driver_write_block_preup( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                        char* in_block_data, size_t in_block_data_len, char** out_block_data, size_t* out_block_data_len ) {
   
   int ret = 0;
   
   if( md_closure_find_callback( driver, "write_block_preup" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver, "write_block_preup", driver_write_block_preup_func, fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len, driver->cls );
   }
   else {
      errorf("%s", "WARN: write_block_preup stub\n");
      
      *out_block_data_len = in_block_data_len;
      
      *out_block_data = CALLOC_LIST( char, in_block_data_len );
      memcpy( *out_block_data, in_block_data, in_block_data_len );
   }
   
   return ret;
}

int driver_write_manifest_preup( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                                 char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls ) {
 
   int ret = 0;
   
   if( md_closure_find_callback( driver, "write_manifest_preup" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, driver, "write_manifest_preup", driver_write_manifest_preup_func, fs_path, fent, mtime_sec, mtime_nsec, in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, user_cls );
   }
   else {
      errorf("%s", "WARN: write_manifest_preup stub\n");
      
      *out_manifest_data_len = in_manifest_data_len;
      
      *out_manifest_data = CALLOC_LIST( char, in_manifest_data_len );
      memcpy( *out_manifest_data, in_manifest_data, in_manifest_data_len );
   }
   
   return ret;
}


// process data after downloading 
ssize_t driver_read_block_postdown( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                                    char* in_block_data, size_t in_block_data_len, char* out_block_data, size_t out_block_data_len ) {
   
   ssize_t ret = 0;
   
   if( md_closure_find_callback( driver, "read_block_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver, "read_block_postdown", driver_read_block_postdown_func, fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len, driver->cls );
   }
   else {
      errorf("%s", "WARN: read_block_postdown stub\n");
      
      memcpy( out_block_data, in_block_data, MIN( in_block_data_len, out_block_data_len ) );
      ret = MIN( in_block_data_len, out_block_data_len );
   }
   
   return ret;
}


// process a manifest after downloading 
int driver_read_manifest_postdown( struct md_syndicate_conf* conf, char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls ) {
 
   int ret = 0;
   
   struct driver_read_manifest_postdown_cls* cls = (struct driver_read_manifest_postdown_cls*)user_cls;
   
   if( md_closure_find_callback( cls->driver, "read_manifest_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, cls->driver, "read_manifest_postdown", driver_read_manifest_postdown_func, cls->fs_path, cls->fent, cls->mtime_sec, cls->mtime_nsec,
                       in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, cls->driver->cls );
   }
   else {
      errorf("%s", "WARN: read_manifest_postdown stub\n");
      
      *out_manifest_data_len = in_manifest_data_len;
      
      *out_manifest_data = CALLOC_LIST( char, in_manifest_data_len );
      memcpy( *out_manifest_data, in_manifest_data, in_manifest_data_len );
   }
   
   return ret;
}


// begin changing coordinator.  This is called *before* the coordinator change request is sent.  fent->coordinator still refers to the old coordinator
int driver_chcoord_begin( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t replica_version ) {
   int ret = 0;
   
   if( md_closure_find_callback( driver, "chcoord_begin" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver, "chcoord_begin", driver_chcoord_begin_func, fs_path, fent, replica_version, driver->cls );
   }
   else {
      errorf("%s", "WARN: chcoord_begin stub\n");
   }
   
   return ret;
}

// end changing coordinator.  This is called *after* the coordintaor changes.
// chcoord_status is the MS's return code (0 for success, negative for error). 
// If chcoord_status == 0, the change succeeded
int driver_chcoord_end( struct md_closure* driver, char const* fs_path, struct fs_entry* fent, int64_t replica_version, int chcoord_status ) {
   int ret = 0;
   
   if( md_closure_find_callback( driver, "chcoord_end" ) != NULL ) {
      MD_CLOSURE_CALL( ret, driver, "chcoord_end", driver_chcoord_end_func, fs_path, fent, replica_version, chcoord_status, driver->cls );
   }
   else {
      errorf("%s", "WARN: chcoord_end stub\n");
   }
   return ret;
}

