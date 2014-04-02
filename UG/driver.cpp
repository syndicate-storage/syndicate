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

MD_CLOSURE_PROTOTYPE_BEGIN( UG_CLOSURE_PROTOTYPE )
   MD_CLOSURE_CALLBACK( "connect_cache" ),
   MD_CLOSURE_CALLBACK( "write_block_preup" ),
   MD_CLOSURE_CALLBACK( "write_manifest_preup" ),
   MD_CLOSURE_CALLBACK( "read_block_postdown" ),
   MD_CLOSURE_CALLBACK( "read_manifest_postdown" ),
   MD_CLOSURE_CALLBACK( "chcoord_begin" ),
   MD_CLOSURE_CALLBACK( "chcoord_end" )
MD_CLOSURE_PROTOTYPE_END


// initialize the closure
int driver_init( struct fs_core* core, struct md_closure** _ret ) {
   // get the closure text 
   char* closure_text = NULL;
   uint64_t closure_text_len = 0;
   
   struct md_closure* closure = CALLOC_LIST( struct md_closure, 1 );
   
   int rc = ms_client_get_closure_text( core->ms, &closure_text, &closure_text_len );
   if( rc != 0 ) {
      errorf("ms_client_get_closure_text rc = %d\n", rc );
      
      if( rc == -ENOENT ) {
         // dummy closure 
         *_ret = closure;
         return 0;
      }
      else {
         // error 
         return rc;
      }
   }
   
   rc = md_closure_init( core->ms, closure, UG_CLOSURE_PROTOTYPE, closure_text, closure_text_len, true );
   
   free( closure_text );
   
   if( rc != 0 ) {
      if( rc != -ENOENT ) {
         free( closure );
         closure = NULL;
         return rc;
      }
      else {
         // dummy closure, since none is given
         *_ret = closure;
         return 0;
      }
   }
   else {
      *_ret = closure;
      return 0;
   }
}


// reload the closure 
int driver_reload( struct fs_core* core, struct md_closure* closure ) {
   // get the closure text 
   char* closure_text = NULL;
   uint64_t closure_text_len = 0;
   
   int rc = ms_client_get_closure_text( core->ms, &closure_text, &closure_text_len );
   if( rc != 0 ) {
      errorf("ms_client_get_closure_text rc = %d\n", rc );
      return rc;
   }
   
   rc = md_closure_reload( core->ms, closure, closure_text, closure_text_len );
   
   free( closure_text );
   
   return rc;
}


// shut down the closure 
int driver_shutdown( struct md_closure* closure ) {
   return md_closure_shutdown( closure );
}

// connect to the cache (libsyndicate method)
int driver_connect_cache( struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   struct driver_connect_cache_cls* closure_cls = (struct driver_connect_cache_cls*)cls;
   
   int ret = 0;
   
   if( md_closure_find_callback( closure, "connect_cache" ) != NULL ) { 
   
      // call our closure's connect_cache method
      MD_CLOSURE_CALL( ret, closure, "connect_cache", driver_connect_cache_func, closure, curl, url, closure->cls );
   
   }
   else {
      errorf("%s", "WARN: connect_cache stub\n");
      ms_client_connect_cache( closure, curl, url, closure_cls->client->conf );
      ret = 0;
   }
   
   return ret;
}

// process data before uploading 
int driver_write_block_preup( struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                        char* in_block_data, size_t in_block_data_len, char** out_block_data, size_t* out_block_data_len ) {
   
   int ret = 0;
   
   if( md_closure_find_callback( closure, "write_block_preup" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "write_block_preup", driver_write_block_preup_func, closure, fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len, closure->cls );
   }
   else {
      errorf("%s", "WARN: write_block_preup stub\n");
      
      *out_block_data_len = in_block_data_len;
      
      *out_block_data = CALLOC_LIST( char, in_block_data_len );
      memcpy( *out_block_data, in_block_data, in_block_data_len );
   }
   
   return ret;
}

int driver_write_manifest_preup( struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec,
                                 char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len ) {
 
   int ret = 0;
   
   if( md_closure_find_callback( closure, "write_manifest_preup" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "write_manifest_preup", driver_write_manifest_preup_func, closure, fs_path, fent, mtime_sec, mtime_nsec, in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, closure->cls );
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
ssize_t driver_read_block_postdown( struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                                    char* in_block_data, size_t in_block_data_len, char* out_block_data, size_t out_block_data_len ) {
   
   ssize_t ret = 0;
   
   if( md_closure_find_callback( closure, "read_block_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "read_block_postdown", driver_read_block_postdown_func, closure, fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len, closure->cls );
   }
   else {
      errorf("%s", "WARN: read_block_postdown stub\n");
      
      memcpy( out_block_data, in_block_data, MIN( in_block_data_len, out_block_data_len ) );
      ret = MIN( in_block_data_len, out_block_data_len );
   }
   
   return ret;
}


// process a manifest after downloading (called by md_download_manifest())
int driver_read_manifest_postdown( struct md_closure* closure, char* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls ) {
 
   int ret = 0;
   
   struct driver_read_manifest_postdown_cls* cls = (struct driver_read_manifest_postdown_cls*)user_cls;
   
   if( md_closure_find_callback( closure, "read_manifest_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "read_manifest_postdown", driver_read_manifest_postdown_func, closure, cls->fs_path, cls->fent, cls->mtime_sec, cls->mtime_nsec,
                       in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, closure->cls );
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
int driver_chcoord_begin( struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t replica_version ) {
   int ret = 0;
   
   if( md_closure_find_callback( closure, "chcoord_begin" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "chcoord_begin", driver_chcoord_begin_func, closure, fs_path, fent, replica_version, closure->cls );
   }
   else {
      errorf("%s", "WARN: chcoord_begin stub\n");
   }
   
   return ret;
}

// end changing coordinator.  This is called *after* the coordintaor changes.
// chcoord_status is the MS's return code (0 for success, negative for error). 
// If chcoord_status == 0, the change succeeded
int driver_chcoord_end( struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t replica_version, int chcoord_status ) {
   int ret = 0;
   
   if( md_closure_find_callback( closure, "chcoord_end" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "chcoord_end", driver_chcoord_end_func, closure, fs_path, fent, replica_version, chcoord_status, closure->cls );
   }
   else {
      errorf("%s", "WARN: chcoord_end stub\n");
   }
   return ret;
}

