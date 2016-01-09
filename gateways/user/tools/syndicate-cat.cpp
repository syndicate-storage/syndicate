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

#include "syndicate-put.h"

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   char* path = NULL;
   int path_optind = 0;
   char* buf = NULL;
   ssize_t nr = 0;
   int close_rc = 0;
   UG_handle_t* fh = NULL;

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
   
   // get the path list...
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0], "file [file...]" );
      UG_shutdown( ug );
      exit(1);
   }

   // make a read buffer (1 MB chunks should be fine) 
   buf = SG_CALLOC( char, 1024 * 1024 );
   if( buf == NULL ) {

      fprintf(stderr, "Out of memory\n");
      exit(1);
   }
 
   for( int i = path_optind; i < argc; i++ ) {

      path = argv[i];

      // try to open...
      fh = UG_open( ug, path, O_RDONLY, &rc );
      if( rc != 0 ) {

         fprintf(stderr, "Failed to open %s: %s\n", path, strerror(-rc));
         goto cat_end;
      }

      // try to read 
      nr = 0;
      while( 1 ) {
          nr = UG_read( ug, buf, 1024 * 1024, fh );
          if( nr < 0 ) {
    
             fprintf(stderr, "%s: read: %s\n", path, strerror(-nr));
             rc = nr;
             break;
          }
          if( nr == 0 ) {

             // EOF
             SG_debug("EOF on %s\n", path ); 
             rc = 0;
             break;
          }

          SG_debug("Read %zd bytes\n", nr );

          fwrite( buf, 1, nr, stdout );
          fflush( stdout );
      }

      // close up 
      close_rc = UG_close( ug, fh );
      if( close_rc < 0 ) {

         fprintf(stderr, "%s: close: %s\n", path, strerror(-close_rc));
         break;
      }

      if( rc != 0 ) {
         break;
      }
   }

cat_end:
   SG_safe_free(buf );
   UG_shutdown( ug );

   if( rc != 0 ) {
      exit(1);
   }
   else {
      exit(0);
   }
}
