/*
   Copyright 2015 The Trustees of Princeton University

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

#include "syndicate-vacuum.h"

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   char* path = NULL;
   int path_optind = 0;
   struct tool_opts opts;
   struct UG_vacuum_context* vctx = NULL;
   
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
   
   // get the directory path 
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0], "file [file...]" );
      md_common_usage();
      UG_shutdown( ug );
      exit(1);
   }
   
   for( int i = path_optind; i < argc; i++ ) {
            
        path = argv[ i ];
        rc = UG_vacuum_begin( ug, path, &vctx );
        if( rc != 0 ) {
           fprintf(stderr, "Failed to vacuum '%s': %s\n", path, strerror(abs(rc)) );
           continue;
        }

        printf("Vacuuming %s\n", path );

        UG_vacuum_wait( vctx );
   }

   UG_shutdown( ug );
   exit(0);
}
