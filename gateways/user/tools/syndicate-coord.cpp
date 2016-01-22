/*
   Copyright 2016 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "syndicate-coord.h"

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   int path_optind = 0;
   struct tool_opts opts;
   struct md_entry ent_data;
   char* path = NULL;
   uint64_t new_coord = 0;
   
   memset( &opts, 0, sizeof(tool_opts) );
   
   rc = parse_args( argc, argv, &opts );
   if( rc != 0 ) {
      
      usage( argv[0], "file [file...]" );
      md_common_usage();
      exit(1);
   }
   
   // setup...
   ug = UG_init( argc, argv, opts.anonymous );
   if( ug == NULL ) {
      
      SG_error("%s", "UG_init failed\n" );
      exit(1);
   }
   
   gateway = UG_state_gateway( ug );
   
   // get the list of files to coordinate 
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0], "file [file...]" );
      md_common_usage();
      UG_shutdown( ug );
      exit(1);
   }
   
   for( int i = path_optind; i < argc; i++ ) {

      path = argv[i];

      // make sure this is a file...
      rc = UG_stat_raw( ug, path, &ent_data );
      if( rc != 0 ) {
         SG_error("UG_stat_raw('%s') rc = %d\n", path, rc );
         rc = 1;
         goto UG_coord_shutdown;
      }

      if( ent_data.type != MD_ENTRY_FILE ) {
         fprintf(stderr, "Not a file: %s\n", path );
         rc = 1;
         goto UG_coord_shutdown;
      }

      // if we're not the coordinator, become it 
      if( ent_data.coordinator != SG_gateway_id( gateway ) ) {

         SG_debug("Become the coordinator of '%s'\n", path );
         rc = UG_chcoord( ug, path, &new_coord );
         if( rc != 0 ) {
            fprintf(stderr, "chcoord '%s': %s\n", path, strerror(-rc) );
            rc = 1;
            goto UG_coord_shutdown;
         }
      }

      md_entry_free( &ent_data );
   }

   // proceed to handle requests
   SG_debug("%s", "Proceed to handle requests\n");

   rc = UG_main( ug );
   if( rc != 0 ) {
      fprintf(stderr, "UG_main: %s\n", strerror(-rc) );
      rc = 1;
   }

UG_coord_shutdown:
   UG_shutdown( ug );
   exit(rc);
}
