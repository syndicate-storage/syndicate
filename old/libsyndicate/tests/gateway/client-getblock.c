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


#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/client.h"
#include "libsyndicate/ms/ms-client.h"

#include "common.h"

void usage( char const* progname ) {
   
   fprintf(stderr, "Usage: %s [SYNDICATE OPTIONS] GATEWAY_ID /path/to/file BLOCK_ID [BLOCK_ID]\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   int rc = 0;
   struct md_opts opts;
   int new_optind = 0;
   uint64_t remote_gateway_id = 0;
   uint64_t volume_id = 0;
   uint64_t file_id = 0x1234567890ABCDEF;
   int64_t file_version = 1234567890;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   
   int next_block_id = 0;
   struct timespec ts;
   
   clock_gettime( CLOCK_REALTIME, &ts );
   
   // make it every 20 seconds, to test caching 
   ts.tv_sec = (ts.tv_sec / 20) * 20;
   ts.tv_nsec = 0;
   
   struct md_download_loop dlloop;
   struct md_download_context* dlctx = NULL;
   
   struct ms_client* ms = NULL;
   
   struct SG_request_data reqdat;
   struct SG_manifest manifest;
   struct SG_chunk block_data;
   
   char* tmp = NULL;
   char* gateway_id_str = NULL;
   char* fs_path = NULL;
   
   // read opts, and find the end of the syndicate options 
   rc = common_parse_opts( &opts, argc, argv, &new_optind );
   if( rc != 0 ) {
      
      usage( argv[0] );
   }
   
   md_opts_free( &opts );
   
   if( new_optind + 3 > argc ) {
      
      printf("new_optind = %d, argc = %d\n", new_optind, argc );
      usage( argv[0] );
   }
   
   gateway_id_str = argv[new_optind];
   fs_path = argv[new_optind+1];
   
   next_block_id = new_optind+2;
   
   remote_gateway_id = strtoull( gateway_id_str, &tmp, 10 );
   if( *tmp != '\0' ) {
      
      usage( argv[0] );
   }
   
   // us
   struct SG_gateway gateway;
   
   memset( &gateway, 0, sizeof(struct SG_gateway) );
   
   // start up 
   rc = SG_gateway_init( &gateway, SYNDICATE_UG, true, argc, argv );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_init rc = %d\n", rc );
      exit(1);
   }
   
   SG_info("%s", "Initialized\n");
   
   // set up the request
   ms = SG_gateway_ms( &gateway );
   volume_id = ms_client_get_volume_id( ms );

   SG_request_data_init( &reqdat );
   reqdat.volume_id = volume_id;
   reqdat.file_id = file_id;
   reqdat.file_version = file_version;
   reqdat.fs_path = fs_path;
   reqdat.manifest_timestamp = ts;
   
   // get the manifest
   rc = SG_client_get_manifest( &gateway, &reqdat, remote_gateway_id, &manifest );
   
   if( rc != 0 ) {
      
      SG_error("SG_client_get_manifest rc = %d\n", rc );
      
      SG_gateway_shutdown( &gateway );
      exit(2);
   }
   
   // got the manifest! 
   printf("\nManifest for /%" PRIu64 "/%" PRIX64 "/manifest.%" PRId64 ".%ld:\n", volume_id, file_id, ts.tv_sec, ts.tv_nsec );
   SG_manifest_print( &manifest );
   
   printf("\n");
   
   // set up block downloads 
   rc = md_download_loop_init( &dlloop, gateway.dl, 6 );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_init rc = %d\n", rc );
      SG_gateway_shutdown( &gateway );
      
      exit(1);
   }
   
   // issue the requests
   do {
      
      // next block 
      while( next_block_id < argc ) {
         
         block_id = strtoull( argv[ next_block_id ], &tmp, 10 );
         if( *tmp != '\0' ) {
            
            usage( argv[0] );
         }
         
         SG_request_data_init( &reqdat );
         reqdat.volume_id = volume_id;
         reqdat.file_id = file_id;
         reqdat.file_version = file_version;
         reqdat.block_id = block_id;
         reqdat.block_version = block_version;
         reqdat.fs_path = fs_path;
         
         // next download 
         rc = md_download_loop_next( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // next block 
         rc = SG_client_get_block_async( &gateway, &reqdat, remote_gateway_id, &dlloop, dlctx );
         
         if( rc != 0 ) {
            
            SG_error("SG_client_get_block_async rc = %d\n", rc );
            break;
         }
         
         next_block_id++;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // run downloads 
      rc = md_download_loop_run( &dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // reap downloads 
      while( true ) {
         
         memset( &block_data, 0, sizeof(struct SG_chunk) );
         
         // find finished download 
         rc = md_download_loop_finished( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // out of finished downloads 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finished rc = %d\n", rc );
            break;
         }
         
         // get the block 
         rc = SG_client_get_block_finish( &gateway, &manifest, dlctx, &block_id, &block_data );
         if( rc != 0 ) {
            
            SG_error("SG_client_get_block_finish rc = %d\n", rc );
            break;
         }
         
         // got the block!
         char buf[11];
         memset( buf, 0, 11 );
         
         memcpy( buf, block_data.data, 10 );
         
         printf("Block %" PRIu64 ", data = '%s'...\n", block_id, buf );
         
         SG_chunk_free( &block_data );
      }
      
   } while( md_download_loop_running( &dlloop ) );
   
   if( rc != 0 ) {
      
      // clean up 
      md_download_loop_abort( &dlloop );
      SG_client_download_async_cleanup_loop( &dlloop );
   }
   
   md_download_loop_cleanup( &dlloop, NULL, NULL );
   md_download_loop_free( &dlloop );
   SG_manifest_free( &manifest );
   
   // done!
   SG_gateway_shutdown( &gateway );
   
   return 0;
}
   