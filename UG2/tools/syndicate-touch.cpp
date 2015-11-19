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

#include "syndicate-touch.h"

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   char* path = NULL;
   int path_optind = 0;
   mode_t um = umask(0);
   umask( um );
   
   struct tool_opts opts;
   
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
   
   // get the path 
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0], "file [file...]");
      UG_shutdown( ug );
      exit(1);
   }
   
   for( int i = path_optind; i < argc; i++ ) {
        
       path = argv[ i ];
       
       // try to create
       UG_handle_t* fh = UG_create( ug, path, um & 0777, &rc );
       if( rc != 0 ) {
           
          if( rc != -EEXIST ) { 
              fprintf(stderr, "Failed to create '%s': %d %s\n", path, rc, strerror( abs(rc) ) );
              continue;
          }
          else {
            
             // already exists.
             // update timestamp
             struct timespec ts;
             struct utimbuf utime;

             clock_gettime( CLOCK_REALTIME, &ts );
             utime.actime = ts.tv_sec;
             utime.modtime = ts.tv_sec;

             rc = UG_utime( ug, path, &utime );
             if( rc != 0 ) {
                fprintf(stderr, "Failed to update timestamps on '%s': %s\n", path, strerror( abs(rc) ) );
                continue;
             }
          }
       }

       else {

          // created!
          rc = UG_close( ug, fh );
          if( rc != 0 ) {

             fprintf(stderr, "Failed to close '%s': %s\n", path, strerror( abs(rc) ) );
          }
       }
   }
   
   UG_shutdown( ug );
   exit(rc);
}
