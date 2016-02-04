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

#include "driver.h"
#include "impl.h"
#include "read.h"
#include "write.h"
#include "client.h"
#include "core.h"
#include "consistency.h"


// connect to the CDN
// return 0 on success
// return -ENOMEM on OOM 
static int UG_impl_connect_cache( struct SG_gateway* gateway, CURL* curl, char const* url, void* cls ) {

   int rc = 0;
   char* out_url = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );

   rc = UG_driver_cdn_url( ug, url, &out_url );
   if( rc != 0 ) {
      return rc;
   }

   // set up the curl handle
   curl_easy_setopt( curl, CURLOPT_URL, out_url );
   SG_safe_free( out_url );
   return 0; 
}


// update a file's manifest, in response to a remote call 
// return 0 on success
// return -ENOENT if not found
// return -ESTALE if not local
// return -errno on error 
// NOTE: the permissions will already have been checked by the server
static int UG_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta, void* cls ) {
   
   int rc = 0;
   int ref_rc = 0;
   struct fskit_entry* fent = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_inode* inode = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );

   rc = UG_consistency_path_ensure_fresh( gateway, reqdat->fs_path );
   if( rc != 0 ) {
      SG_error("UG_consistency_path_ensure_fresh('%s') rc = %d\n", reqdat->fs_path, rc );
      return rc;
   }

   rc = UG_consistency_manifest_ensure_fresh( gateway, reqdat->fs_path );
   if( rc != 0 ) {
      SG_error("UG_consistency_manifest_ensure_fresh('%s') rc = %d\n", reqdat->fs_path, rc );
      return rc;
   }
   
   // look up 
   fent = fskit_entry_resolve_path( fs, reqdat->fs_path, reqdat->user_id, volume_id, true, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   // must be coordinated by us 
   if( UG_inode_coordinator_id( inode ) != SG_gateway_id( gateway ) ) {
      
      fskit_entry_unlock( fent );
      return -ESTALE;
   }
   
   // update the manifest 
   fskit_entry_ref_entry( fent );
   rc = UG_write_patch_manifest( gateway, reqdat, inode, write_delta );
   
   fskit_entry_unlock( fent );

   ref_rc = fskit_entry_unref( fs, reqdat->fs_path, fent );
   if( ref_rc != 0 ) {
      SG_warn("fskit_entry_unref('%s') rc = %d\n", reqdat->fs_path, rc );
   }
   
   return rc;
}


// stat a file--build a manifest request, and set its mode
// return 0 on success 
// return -ESTALE if the inode is not local 
// return -ENOENT if we don't have it
// return -ENOMEM on OOM
// return -errno on error 
static int UG_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode, void* cls ) {
  
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct md_entry ent_info;

   rc = UG_stat_raw( ug, reqdat->fs_path, &ent_info );
   if( rc != 0 ) {
      
      SG_error("UG_stat_raw('%s') rc = %d\n", reqdat->fs_path, rc );
      return rc;
   }
   
   if( ent_info.coordinator != SG_gateway_id( gateway ) ) {
     
      // not ours 
      SG_error("Not the coordinator of '%s' (it is now %" PRIu64 ")\n", reqdat->fs_path, ent_info.coordinator );
      md_entry_free( &ent_info );
      return -ESTALE;
   }
  
   if( mode != NULL ) {
      *mode = ent_info.mode;
   }

   if( entity_info != NULL ) {
      
      rc = SG_request_data_init_manifest( gateway, reqdat->fs_path, ent_info.file_id, ent_info.version, ent_info.manifest_mtime_sec, ent_info.manifest_mtime_nsec, entity_info );
      if( rc != 0 ) {

         // OOM 
         md_entry_free( &ent_info );
         return -ENOMEM;
      }

      if( ent_info.type != MD_ENTRY_FILE ) {

         // not a file 
         md_entry_free( &ent_info );
         return -ENOENT;
      }
   }
   md_entry_free( &ent_info );

   return 0;
}


// stat a file's block--build a manifest request, and set its mode
// return 0 on success 
// return -ESTALE if the inode is not local 
// return -ENOENT if we don't have it
// return -ENOMEM on OOM
// return -errno on error 
static int UG_impl_stat_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode, void* cls ) {
  
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   int64_t block_version = 0;
   UG_handle_t* fi = NULL;
   struct fskit_entry* fent = NULL;
   struct UG_inode* inode = NULL;
   uint64_t file_id = 0;
   int64_t file_version = 0;
   int close_rc = 0;

   fi = UG_open( ug, reqdat->fs_path, O_RDONLY, &rc );
   if( fi == NULL ) {

      SG_error("UG_open('%s') rc = %d\n", reqdat->fs_path, rc );
      return rc;
   }

   fskit_file_handle_rlock( fi->fh );
   
   fent = fskit_file_handle_get_entry( fi->fh );
   if( fent == NULL ) {
      SG_error("BUG: no entry for handle %p\n", fi->fh );
      exit(1);
   }

   fskit_entry_rlock( fent );
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   if( inode == NULL ) {
      SG_error("BUG: no inode for entry %p\n", fent );
      exit(1);
   }

   if( UG_inode_coordinator_id( inode ) != SG_gateway_id( gateway ) ) {

      // not ours 
      SG_error("Not the coordinator of '%s' (it is now %" PRIu64 ")\n", reqdat->fs_path, UG_inode_coordinator_id( inode ) );
      fskit_entry_unlock( fent );
      fskit_file_handle_unlock( fi->fh );

      rc = UG_close( ug, fi );
      if( rc != 0 ) {

         SG_error("UG_close('%s') rc = %d\n", reqdat->fs_path, rc );
      }
      return rc;
   }

   file_id = UG_inode_file_id( inode );
   file_version = UG_inode_file_version( inode );

   if( mode != NULL ) {
      *mode = fskit_entry_get_mode( fent );
   }
   if( entity_info != NULL ) {
      rc = UG_getblockinfo( ug, reqdat->block_id, &block_version, NULL, fi );
   }

   fskit_entry_unlock( fent );
   fskit_file_handle_unlock( fi->fh );
   inode = NULL;

   if( rc != 0 ) {

      SG_error("UG_getblockinfo(%s[%" PRIu64 "]) rc = %d\n", reqdat->fs_path, reqdat->block_id, rc);
      goto UG_impl_stat_block_out;
   }

   rc = SG_request_data_init_block( gateway, reqdat->fs_path, file_id, file_version, reqdat->block_id, block_version, entity_info );
   if( rc != 0 ) {

      SG_error("SG_request_data_init_block rc = %d\n", rc );
      goto UG_impl_stat_block_out;
   }

UG_impl_stat_block_out:

   close_rc = UG_close( ug, fi );
   if( close_rc != 0 ) {

      SG_error("UG_close('%s') rc = %d\n", reqdat->fs_path, close_rc );
   }

   return rc;
}


// remote request to rename a file.
// there can be at most one ongoing rename at a given moment.
// return 0 on success 
// return -ENOMEM on OOM 
// return -EBUSY if the given path is being renamed already
// return -ESTALE if the node is not local
// return -errno on error 
static int UG_impl_rename( struct SG_gateway* gateway, struct SG_request_data* reqdat, char const* new_path, void* cls ) {
   
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   return UG_rename( ug, reqdat->fs_path, new_path );
}


// truncate a file 
// return 0 on success 
// return -errno on error 
static int UG_impl_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size, void* cls ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct ms_client* ms = (struct ms_client*)SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   // truncate locally.  The MS will be informed as part of the user route.
   rc = fskit_trunc( fs, reqdat->fs_path, reqdat->user_id, volume_id, new_size );
   if( rc != 0 ) {
      
      SG_error("fskit_trunc( '%s', %" PRIu64 ") rc = %d\n", reqdat->fs_path, new_size, rc);
   }
   
   return rc;
}

// detach a file 
// return 0 on success
// return -errno on error 
static int UG_impl_detach( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct ms_client* ms = (struct ms_client*)SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct stat sb;
   char const* method = NULL;
   
   // file or directory?
   rc = fskit_stat( fs, reqdat->fs_path, 0, 0, &sb );
   if( rc != 0 ) {
      
      return rc;
   }
   
   if( S_ISREG( sb.st_mode ) ) {
   
      // unlink locally.  The MS will be informed as part of the user route.
      method = "fskit_unlink";
      rc = fskit_unlink( fs, reqdat->fs_path, reqdat->user_id, volume_id );
   }
   else {
      
      // rmdir locally.  The MS will be informed as part of the user route 
      method = "fskit_rmdir";
      rc = fskit_rmdir( fs, reqdat->fs_path, reqdat->user_id, volume_id );
   }
   
   if( rc != 0 ) {
      
      SG_error("%s( '%s' ) rc = %d\n", method, reqdat->fs_path, rc);
   }
   
   return 0;
}


// on config reload, re-calculate the set of replica gateway IDs
// return 0 on success 
// return negative on error
static int UG_impl_config_change( struct SG_gateway* gateway, int driver_reload_rc, void* cls ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)cls;
   
   rc = UG_state_reload_replica_gateway_ids( ug );
   if( rc != 0 ) {
      
      SG_error("UG_state_reload_replica_gateway_ids rc = %d\n", rc );
   }
   
   return rc;
}


// set up the gateway's method implementation 
// always succeeds
int UG_impl_install_methods( struct SG_gateway* gateway ) {
   
   SG_impl_connect_cache( gateway, UG_impl_connect_cache );
   SG_impl_stat( gateway, UG_impl_stat );
   SG_impl_stat_block( gateway, UG_impl_stat_block );
   SG_impl_truncate( gateway, UG_impl_truncate );
   SG_impl_rename( gateway, UG_impl_rename );
   SG_impl_detach( gateway, UG_impl_detach );
   
   SG_impl_patch_manifest( gateway, UG_impl_manifest_patch );
   SG_impl_config_change( gateway, UG_impl_config_change );
   SG_impl_serialize( gateway, UG_driver_chunk_serialize );
   SG_impl_deserialize( gateway, UG_driver_chunk_deserialize );

   return 0;
}

