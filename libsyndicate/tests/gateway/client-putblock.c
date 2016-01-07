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
   
   fprintf(stderr, "Usage: %s [SYNDICATE OPTIONS] GATEWAY_ID /path/to/file BLOCK_ID BLOCK_FILL_PATTERN [BLOCK_ID BLOCK_FILL_PATTERN...]\n", progname );
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
   uint64_t block_size = 0;
   char* pattern_buf = NULL;
   char* block_buf = NULL;
   struct timespec ts;
   
   clock_gettime( CLOCK_REALTIME, &ts );
   
   // make it every 20 seconds, to test caching 
   ts.tv_sec = (ts.tv_sec / 20) * 20;
   ts.tv_nsec = 0;
   
   struct ms_client* ms = NULL;
   
   char* tmp = NULL;
   char* gateway_id_str = NULL;
   char* fs_path = NULL;
   struct SG_request_data reqdat;
   
   // read opts, and find the end of the syndicate options 
   rc = common_parse_opts( &opts, argc, argv, &new_optind );
   if( rc != 0 ) {
      
      usage( argv[0] );
   }
   
   md_opts_free( &opts );
   
   // need even number of subsequent args
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
   block_size = ms_client_get_block_size( ms );
   
   // block buffer!
   block_buf = SG_CALLOC( char, block_size );
   if( block_buf == NULL ) {
      
      exit( ENOMEM );
   }

   // make the file-wide request data
   SG_request_data_init( &reqdat );
   reqdat.volume_id = volume_id;
   reqdat.coordinator_id = remote_gateway_id;
   reqdat.file_id = file_id;
   reqdat.file_version = file_version;
   reqdat.fs_path = fs_path;
   
   for( int i = new_optind + 2; i < argc; i += 2 ) {
      
      SG_messages::Request request;
      struct SG_manifest_block block_info;
      unsigned char* block_hash = NULL;
      unsigned int j = 0;
      
      block_id = strtoull( argv[i], &tmp, 10 );
      if( *tmp != '\0' ) {
         
         usage(argv[0]);
      }
      
      pattern_buf = argv[i+1];
      
      // make the block 
      for( j = 0; j < block_size; j += strlen(pattern_buf) ) {
         
         memcpy( block_buf + strlen(pattern_buf) * j, pattern_buf, strlen(pattern_buf) );
      }
      
      if( j * strlen(pattern_buf) < block_size ) {
         
         memcpy( block_buf + strlen(pattern_buf) * j, pattern_buf, block_size - strlen(pattern_buf) * j );
      }
      
      // make block info 
      block_hash = sha256_hash_data( block_buf, block_size );
      if( block_hash == NULL ) {
         
         exit( ENOMEM );
      }
      
      rc = SG_manifest_block_init( &block_info, block_id, md_random64(), block_hash, SG_BLOCK_HASH_LEN );
      if( rc != 0 ) {
         
         SG_error("SG_manifest_block_init rc = %d\n", rc );
         exit( 255 );
      }
      
      SG_safe_free( block_hash );
      
      // make the request 
      rc = SG_client_request_BLOCK_setup( &gateway, &request, &reqdat, &block_info );
      if( rc != 0 ) {
         
         SG_error("SG_client_request_BLOCK_setup rc = %d\n", rc );
         exit(255);
      }
      
      SG_manifest_block_free( &block_info );
      
      // start the request 
      // TODO
   }
   
   
   // generate the request
   rc = SG_client_request_RENAME_setup( &gateway, &request, &reqdat, new_fs_path );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_RENAME_setup rc = %d\n", rc );
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
