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
   MD_CLOSURE_CALLBACK( "chcoord_end" ),
   MD_CLOSURE_CALLBACK( "get_driver_name" ),
   MD_CLOSURE_CALLBACK( "garbage_collect" ),
   MD_CLOSURE_CALLBACK( "create_file" ),
   MD_CLOSURE_CALLBACK( "delete_file" )
MD_CLOSURE_PROTOTYPE_END


// initialize the closure
// if this fails due to there being no closure on file, a dummy closure will be used instead
int driver_init( struct fs_core* core, struct md_closure** _ret ) {
   // get the closure text 
   char* closure_text = NULL;
   uint64_t closure_text_len = 0;
   
   struct md_closure* closure = SG_CALLOC( struct md_closure, 1 );
   
   int rc = ms_client_get_closure_text( core->ms, &closure_text, &closure_text_len );
   if( rc != 0 ) {
      
      if( rc == -ENODATA && core->gateway == SG_GATEWAY_ANON ) {
         // no driver, since we're either anonymous or a client.
         // use the dummy closure 
         SG_debug("WARNING: ms_client_get_closure_text rc = %d, but this gateway is anonymous and/or in client mode.  Not treating this as an error.\n", rc );
         *_ret = closure;
         return 0;
      }
      
      // some other error
      SG_error("ms_client_get_closure_text rc = %d\n", rc );
      
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
   
   rc = md_closure_init( closure, core->conf, core->ms->my_pubkey, core->ms->my_key, UG_CLOSURE_PROTOTYPE, closure_text, closure_text_len, true, true );
   
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
      SG_error("ms_client_get_closure_text rc = %d\n", rc );
      return rc;
   }
   
   rc = md_closure_reload( closure, core->conf, core->ms->my_pubkey, core->ms->my_key, closure_text, closure_text_len );
   
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
      MD_CLOSURE_CALL( ret, closure, "connect_cache", driver_connect_cache_func, closure_cls->core, closure, curl, url, closure->cls );
   
   }
   else {
      SG_error("%s", "WARN: connect_cache stub\n");
      
      ms_client_volume_connect_cache( closure_cls->core->ms, curl, url );
      ret = 0;
   }
   
   return ret;
}

// process data before uploading 
int driver_write_block_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                              char const* in_block_data, size_t in_block_data_len, char** out_block_data, size_t* out_block_data_len ) {
   
   int ret = 0;
   
   if( md_closure_find_callback( closure, "write_block_preup" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "write_block_preup", driver_write_block_preup_func, core, closure,
                       fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len,
                       closure->cls );
   }
   else {
      SG_error("%s", "WARN: write_block_preup stub\n");
      
      *out_block_data_len = in_block_data_len;
      
      *out_block_data = SG_CALLOC( char, in_block_data_len );
      memcpy( *out_block_data, in_block_data, in_block_data_len );
   }
   
   return ret;
}

int driver_write_manifest_preup( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec,
                                 char const* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len ) {
 
   int ret = 0;
   
   if( md_closure_find_callback( closure, "write_manifest_preup" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "write_manifest_preup", driver_write_manifest_preup_func, core, closure, fs_path, fent, manifest_mtime_sec, manifest_mtime_nsec,
                                                                                               in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, closure->cls );
   }
   else {
      SG_error("%s", "WARN: write_manifest_preup stub\n");
      
      *out_manifest_data_len = in_manifest_data_len;
      
      *out_manifest_data = SG_CALLOC( char, in_manifest_data_len );
      memcpy( *out_manifest_data, in_manifest_data, in_manifest_data_len );
   }
   
   return ret;
}


// process data after downloading.
// NOTE: fent->version may not match the version of the data downloaded, since fent->version might have changed after the download started.
// If this is a problem, then use the as-of-yet-existing pre-read() method to snapshot the old versioning info.
ssize_t driver_read_block_postdown( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version,
                                    char const* in_block_data, size_t in_block_data_len, char* out_block_data, size_t out_block_data_len ) {
   
   ssize_t ret = 0;
   
   if( md_closure_find_callback( closure, "read_block_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "read_block_postdown", driver_read_block_postdown_func, core, closure,
                       fs_path, fent, block_id, block_version, in_block_data, in_block_data_len, out_block_data, out_block_data_len,
                       closure->cls );
   }
   else {
      SG_error("WARN: read_block_postdown stub (in buffer len = %zu, out buffer len = %zu)\n", in_block_data_len, out_block_data_len);
      
      memcpy( out_block_data, in_block_data, MIN( in_block_data_len, out_block_data_len ) );
      ret = MIN( in_block_data_len, out_block_data_len );
   }
   
   return ret;
}


// process a manifest after downloading (called by md_download_manifest())
int driver_read_manifest_postdown( struct md_closure* closure, char const* in_manifest_data, size_t in_manifest_data_len, char** out_manifest_data, size_t* out_manifest_data_len, void* user_cls ) {
 
   int ret = 0;
   
   struct driver_read_manifest_postdown_cls* cls = (struct driver_read_manifest_postdown_cls*)user_cls;
   
   if( md_closure_find_callback( closure, "read_manifest_postdown" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "read_manifest_postdown", driver_read_manifest_postdown_func, cls->core, closure, cls->fs_path, cls->fent, cls->manifest_mtime_sec, cls->manifest_mtime_nsec,
                       in_manifest_data, in_manifest_data_len, out_manifest_data, out_manifest_data_len, closure->cls );
   }
   else {
      SG_error("%s", "WARN: read_manifest_postdown stub\n");
      
      *out_manifest_data_len = in_manifest_data_len;
      
      *out_manifest_data = SG_CALLOC( char, in_manifest_data_len );
      memcpy( *out_manifest_data, in_manifest_data, in_manifest_data_len );
   }
   
   return ret;
}


// begin changing coordinator.  This is called *before* the coordinator change request is sent.  fent->coordinator still refers to the old coordinator
int driver_chcoord_begin( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t replica_version ) {
   int ret = 0;
   
   if( md_closure_find_callback( closure, "chcoord_begin" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "chcoord_begin", driver_chcoord_begin_func, core, closure, fs_path, fent, replica_version, closure->cls );
   }
   else {
      SG_error("%s", "WARN: chcoord_begin stub\n");
   }
   
   return ret;
}

// end changing coordinator.  This is called *after* the coordintaor changes.  fent->coordinator refers to the current coordinator
// chcoord_status is the MS's return code (0 for success, negative for error). 
// If chcoord_status == 0, the change succeeded
int driver_chcoord_end( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent, int64_t replica_version, int chcoord_status ) {
   int ret = 0;
   
   if( md_closure_find_callback( closure, "chcoord_end" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "chcoord_end", driver_chcoord_end_func, core, closure, fs_path, fent, replica_version, chcoord_status, closure->cls );
   }
   else {
      SG_error("%s", "WARN: chcoord_end stub\n");
   }
   return ret;
}

// tell the driver that we're garbage-collecting a write.  This is called *before* we actually garbage-collect anything.
// this method should return DRIVER_NOT_GARBAGE if the block is not to be garbage-collected.  The driver logic is responsible for garbage-collecting if so;
// otherwise a memory leak may occur.
// if the closure returns negative, then the garbage collection ends with an error in the UG.
int driver_garbage_collect( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct replica_snapshot* fent_snapshot, uint64_t* block_ids, int64_t* block_versions, size_t num_blocks ) {
   
   int ret = 0;
   
   if( md_closure_find_callback( closure, "garbage_collect" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "garbage_collect", driver_garbage_collect_func, core, closure, fs_path, fent_snapshot, block_ids, block_versions, num_blocks );
   }
   else {
      SG_error("%s", "WARN: garbage_collect stub\n");
   }
   
   return ret;
}


// get the driver name 
char* driver_get_name( struct fs_core* core, struct md_closure* closure ) {
   
   char* ret = NULL;
   
   if( md_closure_find_callback( closure, "get_driver_name" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "get_driver_name", driver_get_name_func );
   }
   else {
      SG_error("%s", "WARN: get_driver_name stub\n");
   }
   
   return ret;
}


// create a file 
int driver_create_file( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent ) {
   
   int ret = 0;
   
   if( md_closure_find_callback( closure, "create_file" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "create_file", driver_create_file_func, core, closure, fs_path, fent );
   }
   else {
      SG_error("%s", "WARN: create_file stub\n");
   }
   
   return ret;
}

// delete a file 
int driver_delete_file( struct fs_core* core, struct md_closure* closure, char const* fs_path, struct fs_entry* fent ) {
   
   int ret = 0;
   
   
   if( md_closure_find_callback( closure, "delete_file" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "delete_file", driver_delete_file_func, core, closure, fs_path, fent );
   }
   else {
      SG_error("%s", "WARN: delete_file stub\n");
   }
   
   return ret;
}
