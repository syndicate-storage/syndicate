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


// update a file's manifest 
// return 0 on success
// return -ENOENT if not found
// return -ESTALE if not local
// return -errno on error 
// NOTE: the permissions will already have been checked by the server
static int UG_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta, void* cls ) {
   
   int rc = 0;
   struct fskit_entry* fent = NULL;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   
   struct fskit_core* fs = UG_state_fs( ug );
   struct UG_inode* inode = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
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
   rc = UG_write_patch_manifest( gateway, reqdat, inode, write_delta );
   
   fskit_entry_unlock( fent );
   
   return rc;
}


// stat a file 
// return 0 on success 
// return -ESTALE if the inode is not local 
// return -ENOENT if we don't have it
// return -ENOMEM on OOM
// return -errno on error 
static int UG_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode, void* cls ) {
   
   int rc = 0;
   struct UG_state* ug = (struct UG_state*)SG_gateway_cls( gateway );
   struct UG_inode* inode = NULL;
   struct fskit_core* fs = UG_state_fs( ug );
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   struct stat sb;
   
   struct fskit_entry* fent;
   
   fent = fskit_entry_resolve_path( fs, reqdat->fs_path, reqdat->user_id, volume_id, false, &rc );
   if( fent == NULL ) {
      
      return rc;
   }
   
   rc = fskit_fstat( fs, reqdat->fs_path, fent, &sb );
   
   if( rc != 0 ) {
      
      fskit_entry_unlock( fent );
      return rc;
   }
   
   inode = (struct UG_inode*)fskit_entry_get_user_data( fent );
   
   if( UG_inode_coordinator_id( inode ) != SG_gateway_id( gateway ) ) {
      
      fskit_entry_unlock( fent );
      return -ESTALE;
   }
   
   if( mode != NULL ) {
      
      *mode = sb.st_mode & 0777;
   }
   
   if( entity_info != NULL ) {
      
      entity_info->fs_path = SG_strdup_or_null( reqdat->fs_path );
      if( entity_info->fs_path == NULL ) {
         
         fskit_entry_unlock( fent );
         return -ENOMEM;
      }
      
      entity_info->volume_id = volume_id;
      entity_info->coordinator_id = UG_inode_coordinator_id( inode );
      entity_info->file_id = sb.st_ino;
      entity_info->file_version = UG_inode_file_version( inode );
      entity_info->xattr_nonce = UG_inode_xattr_nonce( inode );
   }
   
   fskit_entry_unlock( fent );
   return 0;
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
   SG_impl_truncate( gateway, UG_impl_truncate );
   SG_impl_rename( gateway, UG_impl_rename );
   SG_impl_detach( gateway, UG_impl_detach );
   
   // SG_impl_get_block( gateway, UG_impl_block_get );
   // SG_impl_get_manifest( gateway, UG_impl_manifest_get );
   SG_impl_patch_manifest( gateway, UG_impl_manifest_patch );
   SG_impl_config_change( gateway, UG_impl_config_change );
   SG_impl_serialize( gateway, UG_driver_chunk_serialize );
   SG_impl_deserialize( gateway, UG_driver_chunk_deserialize );

   return 0;
}

