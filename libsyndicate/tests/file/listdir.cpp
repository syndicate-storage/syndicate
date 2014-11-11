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

#include "listdir.h"

int usage( char const* prog_name ) {
   
   fprintf(stderr, "Usage: %s [SYNDICATE_OPTS] FILE_ID [FILE_ID]\n");
   return 0;
}

int main( int argc, char** argv ) {
   
   int rc = 0;
   struct syndicate_state state;
   struct md_opts opts;
   struct UG_opts ug_opts;
   int local_optind = 0;
   uint64_t file_id = 0;
   uint64_t parent_id = 0;
   ms_path_t path;
   struct ms_client_multi_result result;
   vector<uint64_t> file_ids;
   
   if( argc < 3 ) {
      usage( argv[0] );
      exit(1);
   }
   
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   memset( &opts, 0, sizeof(struct md_opts) );
   
   memset( &ug_opts, 0, sizeof(struct UG_opts) );
   
   // get options
   rc = md_parse_opts( &opts, argc, argv, &local_optind, NULL, NULL );
   if( rc != 0 ) {
      errorf("md_parse_opts rc = %d\n", rc );
      md_common_usage( argv[0] );
      usage( argv[0] );
      exit(1);
   }
   
   // connect to syndicate
   rc = syndicate_client_init( &state, &opts, &ug_opts );
   if( rc != 0 ) {
      errorf("syndicate_client_init rc = %d\n", rc );
      exit(1);
   }
   
   // get parent id and parse it
   rc = sscanf( argv[local_optind], "%" PRIX64, &parent_id );
   if( rc != 1 ) {
      errorf("failed to parse parent ID '%s'\n", argv[local_optind] );
      exit(1);
   }
   
   printf("\n\n\nBegin listdir\n");
   
   // get each name
   for( int i = local_optind; i < argc; i++ ) {
      
      // file ID 
      rc = sscanf( argv[i], "%" PRIX64, &file_id );
      if( rc != 1 ) {
         errorf("failed to parse file_id ID '%s'\n", argv[i] );
         exit(1);
      }
      
      printf("\n\n\n");
      
      rc = ms_client_listdir( state.ms, file_id, 0, 0, 100, &result );
         
      if( rc != 0 ) {
         errorf("ms_client_listdir rc = %d\n", rc );
         exit(1);
      }
      
      printf("\n\n\n");
         
      for( size_t i = 0; i < result.num_ents; i++ ) {
         
         if( result.ents[i].file_id != 0 ) {
            printf("Entry: %" PRIX64 " %s mode=%o version=%" PRId64 " write_nonce=%" PRId64 " generation=%d\n",
                  result.ents[i].file_id, result.ents[i].name, result.ents[i].mode, result.ents[i].version, result.ents[i].write_nonce, result.ents[i].generation );
         }
      }
      
      ms_client_multi_result_free( &result );
   }
   
   printf("\n\n\nEnd listdir\n\n\n");
   
   syndicate_client_shutdown( &state, 0 );
   
   return 0;
}
