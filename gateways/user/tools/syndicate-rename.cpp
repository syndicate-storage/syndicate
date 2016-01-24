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

#include "syndicate-rename.h"

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   char* src_path = NULL;
   int path_optind = 0;
   char* dest_path = NULL;

   mode_t um = umask(0);
   umask( um );
   
   struct tool_opts opts;
   
   memset( &opts, 0, sizeof(tool_opts) );
   
   rc = parse_args( argc, argv, &opts );
   if( rc != 0 ) {
      
      usage( argv[0], "src_file dest_file" );
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
   
   // get the path...
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0], "src_file dest_file" );
      UG_shutdown( ug );
      exit(1);
   }
  
   // get the src path...
   src_path = argv[ path_optind ];
    
   // get the dest path...
   path_optind++;
   dest_path = argv[path_optind];

   // do the rename 
   rc = UG_rename( ug, src_path, dest_path );
   if( rc != 0 ) {
     SG_error("UG_rename(%s, %s) rc = %d\n", src_path, dest_path, rc );
   } 
   
   UG_shutdown( ug );

   if( rc != 0 ) {
      exit(1);
   }
   else {
      exit(0);
   }
}
