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
   
   fprintf(stderr, "Usage: %s [SYNDICATE OPTIONS] GATEWAY_ID /path/to/file BLOCK_ID BLOCK_VERSION [BLOCK_ID BLOCK_VERSION...]\n", progname );
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
   struct timespec ts;
   
   uint64_t block_id = 0;
   int64_t block_version = 0;
   
   unsigned char fake_hash[32];
   
   clock_gettime( CLOCK_REALTIME, &ts );
   
   // make it every 20 seconds, to test caching 
   ts.tv_sec = (ts.tv_sec / 20) * 20;
   ts.tv_nsec = 0;
   
   struct ms_client* ms = NULL;
   
   struct SG_manifest write_delta;
   
   SG_messages::Request request;
   SG_messages::Reply reply;
   
   char* tmp = NULL;
   char* gateway_id_str = NULL;
   char* fs_path = NULL;
   
   // read opts, and find the end of the syndicate options 
   rc = common_parse_opts( &opts, argc, argv, &new_optind );
   if( rc != 0 ) {
      
      usage( argv[0] );
   }
   
   md_opts_free( &opts );
   
   // must have an even number of remaining options, beyond the file path and gateway id
   if( (argc - new_optind) % 2 != 0 || new_optind + 2 >= argc ) {
      
      printf("new_optind = %d, argc = %d\n", new_optind, argc );
      usage( argv[0] );
   }
   
   gateway_id_str = argv[new_optind];
   fs_path = argv[new_optind+1];
   
   remote_gateway_id = strtoull( gateway_id_str, &tmp, 10 );
   if( *tmp != '\0' ) {
      
      usage( argv[0] );
   }
   
   // us
   struct SG_gateway gateway;
   
   memset( &gateway, 0, sizeof(struct SG_gateway) );
   
   // start up 
   rc = SG_gateway_init( &gateway, SYNDICATE_UG, false, argc, argv );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_init rc = %d\n", rc );
      exit(1);
   }
   
   SG_info("%s", "Initialized\n");
   
   ms = SG_gateway_ms( &gateway );
   volume_id = ms_client_get_volume_id( ms );
   
   // create write request 
   rc = SG_manifest_init( &write_delta, volume_id, remote_gateway_id, file_id, file_version );
   if( rc != 0 ) {
      
      SG_error("SG_manifst_init( write_delta ) rc = %d\n", rc );
      exit(2);
   }
   
   // give it a modtime of mod-20 seconds 
   SG_manifest_set_modtime( &write_delta, ts.tv_sec, ts.tv_nsec );
   
   // coordinator is the remote gateway 
   SG_manifest_set_coordinator_id( &write_delta, remote_gateway_id );
   
   // fake hash...
   for( int i = 0; i < 32; i++ ) {
      fake_hash[i] = i;
   }
   
   // populate from argv 
   for( int i = new_optind + 2; i < argc; i += 2 ) {
      
      block_id = strtoull( argv[i], &tmp, 10 );
      if( *tmp != '\0' ) {
         
         SG_error("Failed to parse block ID '%s'\n", argv[i] );
         usage( argv[0] );
      }
      
      block_version = strtoll( argv[i+1], &tmp, 10 );
      if( *tmp != '\0' ) {
         
         SG_error("Failed to parse block version '%s'\n", argv[i] );
         usage( argv[0] );
      }
      
      // make a block 
      struct SG_manifest_block block;
      rc = SG_manifest_block_init( &block, block_id, block_version, fake_hash, 32 );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_block_init rc = %d\n", rc );
         exit(2);
      }
      
      // put this write 
      rc = SG_manifest_put_block( &write_delta, &block, true );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_put_block rc = %d\n", rc );
         exit(2);
      }
      
      SG_manifest_block_free( &block );
   }
   
   // generate the request
   rc = SG_client_request_WRITE_setup( &gateway, &request, fs_path, &write_delta );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_WRITE_setup rc = %d\n", rc );
      exit(2);
   }
   
   common_print_request( &request );
   
   // send it off 
   rc = SG_client_request_send( &gateway, remote_gateway_id, &request, NULL, &reply );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_send rc = %d\n", rc );
      exit(2);
   }
   
   // got a reply!
   // print it out
   printf("\n");
   common_print_reply( &reply );
   
   SG_gateway_shutdown( &gateway );
   
   return 0;
}
   