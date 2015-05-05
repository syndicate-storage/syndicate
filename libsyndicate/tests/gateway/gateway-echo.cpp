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

#include "gateway-echo.h"


// setup 
int echo_setup( struct SG_gateway* gateway, void** cls ) {
   
   printf("\n===== echo_setup\n");
   
   return 0;
}

// shutdown 
void echo_shutdown( struct SG_gateway* gateway, void* cls ) {
   
   printf("\n===== echo_shutdown\n");
}

// stat 
int echo_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* file_info, mode_t* mode, void* cls ) {
   
   printf("\n===== echo_stat\n");
   
   // whatever was requested exists
   *mode = 0777;
   
   memcpy( file_info, reqdat, sizeof(struct SG_request_data) );
   
   // deep copy
   file_info->fs_path = SG_strdup_or_null( reqdat->fs_path );
   if( file_info->fs_path == NULL ) {
      
      if( reqdat->fs_path == NULL ) {
         
         SG_error("%s", "No fs_path given in request\n");
         return -EINVAL;
      }
      else {
         
         return -ENOMEM;
      }
   }
   
   // modulate manifest timestamp to every 20 seconds, to test redirect 
   file_info->manifest_timestamp.tv_sec = (file_info->manifest_timestamp.tv_sec / 20) * 20;
   file_info->manifest_timestamp.tv_nsec = 0;
   
   // keep block version consistent with manifest 
   file_info->block_version = 9876543210 + file_info->block_id;
   
   file_info->coordinator_id = SG_gateway_id( gateway );
   
   return 0;
}

// truncate 
int echo_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size, void* cls ) {
   
   printf("\n===== echo_truncate (new_size=%" PRIu64 ")\n", new_size);
   
   return 0;
}

// rename 
int echo_rename( struct SG_gateway* gateway, struct SG_request_data* reqdat, char const* new_path, void* cls ) {
   
   printf("\n===== echo_rename (new_path='%s')\n", new_path);
   
   return 0;
}

// detach 
int echo_detach( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {
   
   printf("\n===== echo_detach\n");
   
   return 0;
}

// get block 
int echo_get_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, void* cls ) {
   
   printf("\n===== echo_get_block\n");
   
   // return a fake block, from our fake manifest 
   // fill it with 'A', 'B, or 'C'
   if( reqdat->block_id > 3 ) {
      
      // no block 
      return -ENODATA;
   }
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t blocksize = ms_client_get_volume_blocksize( ms );
   
   block->data = SG_CALLOC( char, blocksize );
   if( block->data == NULL ) {
      
      return -ENOMEM;
   }
   
   block->len = blocksize;
   
   memset( block->data, 'A' + reqdat->block_id, block->len );
   
   return 0;
}

// put block 
int echo_put_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, void* cls ) {
   
   printf("\n===== echo_put_block\n");
   
   return 0;
}


// delete block 
int echo_delete_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {
   
   printf("\n===== echo_delete_block\n");
   
   return 0;
}

// get manifest 
int echo_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, void* cls ) {
   
   printf("\n===== echo_get_manifest\n");
   
   // make a fake manifest with three blocks
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t owner_id = SG_gateway_user_id( gateway );
   uint64_t gateway_id = SG_gateway_id( gateway );
   uint64_t file_id = 0x1234567890ABCDEF;
   uint64_t blocksize = ms_client_get_volume_blocksize( ms );
   int64_t file_version = 1234567890;
   int64_t block_version = 9876543210;
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   unsigned char* block_hash = NULL;
   
   char* buf = SG_CALLOC( char, block_size );
   if( buf == NULL ) {
      
      return -ENOMEM;
   }
   
   struct SG_manifest_block block;
   
   rc = SG_manifest_init( manifest, volume_id, gateway_id, file_id, file_version );
   if( rc != 0 ) {
      
      SG_error("SG_manifest_init rc = %d\n", rc );
      
      SG_manifest_block_free( &block );
      return rc;
   }
   
   for( int i = 0; i < 3; i++ ) {
   
      memset( &block, 0, sizeof( struct SG_manifest_block ) );
      
      // block 0 is filled with 'A'
      // block 1 is filled with 'B'
      // block 2 is filled with 'C'
      memset( buf, 'A' + i, block_size );
      
      // hash the block 
      block_hash = sha256_hash_data( buf, block_size );
      if( block_hash == NULL ) {
         
         SG_manifest_free( manifest );
         return -ENOMEM;
      }
      
      rc = SG_manifest_block_init( &block, i, block_version + i, block_hash, SG_BLOCK_HASH_LEN );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_block_init rc = %d\n", rc );
         SG_manifest_free( manifest );
         return rc;
      }
      
      SG_safe_free( block_hash );
      
      rc = SG_manifest_put_block( manifest, &block, true );
      
      SG_manifest_block_free( &block );
      
      if( rc != 0 ) {
         
         SG_error("SG_manifest_put_block rc = %d\n", rc );
         
         SG_manifest_free( manifest );
         return rc;
      }
   }
   
   SG_safe_free( buf );
   
   SG_manifest_set_owner_id( manifest, owner_id );
   SG_manifest_set_size( manifest, blocksize * 3 );
   SG_manifest_set_modtime( manifest, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec );
   
   SG_manifest_print( manifest );
   
   return 0;
}


// put manifest 
int echo_put_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, void* cls ) {
   
   printf("\n===== echo_put_manifest\n");
   
   return 0;
}

// patch manifest 
int echo_patch_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta, void* cls ) {
   
   printf("\n===== echo_patch_manifest\n");
   
   return 0;
}

// delete manifest 
int echo_delete_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {
   
   printf("\n==== echo_delete_manifest\n");
   
   return 0;
}

// config reload 
int echo_config_change( struct SG_gateway*, void* cls ) {
   
   printf("\n==== echo_config_change\n");
   
   return 0;
}

// entry point 
int main( int argc, char** argv ) {
   
   struct SG_gateway gateway;
   int rc = 0;
   
   memset( &gateway, 0, sizeof(struct SG_gateway) );
   
   // set up 
   SG_impl_setup( gateway, echo_setup );
   SG_impl_shutdown( gateway, echo_shutdown );
   SG_impl_stat( gateway, echo_stat );
   SG_impl_truncate( gateway, echo_truncate );
   SG_impl_rename( gateway, echo_rename );
   SG_impl_detach( gateway, echo_detach );
   SG_impl_get_block( gateway, echo_get_block );
   SG_impl_put_block( gateway, echo_put_block );
   SG_impl_delete_block( gateway, echo_delete_block );
   SG_impl_get_manifest( gateway, echo_get_manifest );
   SG_impl_put_manifest( gateway, echo_put_manifest );
   SG_impl_patch_manifest( gateway, echo_patch_manifest );
   SG_impl_delete_manifest( gateway, echo_delete_manifest );
   SG_impl_config_change( gateway, echo_config_change );
   
   // start up 
   rc = SG_gateway_init( &gateway, SYNDICATE_UG, false, argc, argv );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_init rc = %d\n", rc );
      exit(1);
   }
   
   SG_info("%s", "Initialized\n");
   
   // run 
   rc = SG_gateway_main( &gateway );
   
   SG_info("%s", "Shutting down\n");
   
   // clean up 
   rc = SG_gateway_shutdown( &gateway );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_shutdown rc = %d\n", rc );
   }
   
   return rc;
}

