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

#include "getattr.h"

int main( int argc, char** argv ) {
   
   int rc = 0;
   struct syndicate_state state;
   struct md_opts opts;
   struct UG_opts ug_opts;
   int local_optind = 0;
   uint64_t volume_id = 0;
   ms_path_t path;
   struct ms_client_multi_result result;
   
   md_opts_default( &opts );
   
   // get options
   rc = md_opts_parse( &opts, argc, argv, &local_optind, NULL, NULL );
   if( rc != 0 ) {
      SG_error("md_opts_parse rc = %d\n", rc );
      md_common_usage( argv[0] );
      exit(1);
   }
   
   memset( &ug_opts, 0, sizeof(struct UG_opts) );
   
   // connect to syndicate
   rc = syndicate_client_init( &state, &opts, &ug_opts );
   if( rc != 0 ) {
      SG_error("syndicate_client_init rc = %d\n", rc );
      exit(1);
   }
   
   // get volume ID
   volume_id = ms_client_get_volume_id( state.ms );
   
   
   printf("\n\n\nBegin getattr multi\n\n\n");
   
   // get each path and file ID
   for( int i = local_optind; i < argc; i++ ) {
      
      struct ms_path_ent path_ent;
      uint64_t file_id = 0;
         
      // file ID 
      rc = sscanf( argv[i], "%" PRIX64, &file_id );
      if( rc != 1 ) {
         SG_error("failed to parse file_id ID '%s'\n", argv[i] );
         exit(1);
      }
      
      printf("   getattr(%" PRIX64 ")\n", file_id );
      ms_client_make_path_ent( &path_ent, volume_id, 0, file_id, 0, 0, 0, 0, 0, NULL, NULL );
      path.push_back( path_ent );
   }
   
   printf("\n\n\n");
   
   // get all 
   rc = ms_client_getattr_multi( state.ms, &path, &result );
   
   if( rc != 0 ) {
      SG_error("ms_client_getattr_multi rc = %d\n", rc );
      exit(1);
   }
   
   printf("\n\n\n");
   
   for( unsigned int i = 0; i < result.num_ents; i++ ) {
      
      if( result.ents[i].file_id != 0 ) {
         printf("Entry: %" PRIX64 " %s mode=%o version=%" PRId64 " write_nonce=%" PRId64 " generation=%d\n",
                result.ents[i].file_id, result.ents[i].name, result.ents[i].mode, result.ents[i].version, result.ents[i].write_nonce, result.ents[i].generation );
      }
   }
   
   ms_client_multi_result_free( &result );
   
   printf("\n\n\nEnd getattr multi\n\n\n");
   
   syndicate_client_shutdown( &state, 0 );
   
   return 0;
}
