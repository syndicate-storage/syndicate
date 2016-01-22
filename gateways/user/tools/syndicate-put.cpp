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

#define BUF_SIZE 4096

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   char* path = NULL;
   int path_optind = 0;
   char* file_path = NULL;
   int fd = 0;
   char buf[BUF_SIZE];
   ssize_t nr = 0;
   UG_handle_t* fh = NULL;

   mode_t um = umask(0);
   umask( um );
   
   struct tool_opts opts;
   
   memset( &opts, 0, sizeof(tool_opts) );
   
   rc = parse_args( argc, argv, &opts );
   if( rc != 0 ) {
      
      usage( argv[0], "local_file syndicate_file" );
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
      
      usage( argv[0], "local_file syndicate_file" );
      UG_shutdown( ug );
      exit(1);
   }
  
   // get the file path...
   file_path = argv[ path_optind ];
    
   // get the syndicate path...
   path_optind++;
   path = argv[path_optind];

   // get the file...
   fd = open( file_path, O_RDONLY );
   if( fd < 0 ) {
      rc = -errno;
      fprintf(stderr, "Failed to open '%s': %s\n", file_path, strerror(-rc));
      rc = 1;
      goto put_end;
   }
    
   // try to create
   fh = UG_create( ug, path, 0540, &rc );
   if( rc != 0 ) {
        
      if( rc != -EEXIST ) { 
          fprintf(stderr, "Failed to create '%s' (%d): %s\n", path, rc, strerror( abs(rc) ) );
          rc = 1;
          goto put_end;
      }
      else {
         
         // already exists.  open
         fh = UG_open( ug, path, O_WRONLY, &rc );
         if( rc != 0 ) {
            fprintf(stderr, "Failed to open '%s': %d %s\n", path, rc, strerror( abs(rc) ) );
            rc = 1;
            goto put_end;
         }
      }
   }

   // write the file
   while( 1 ) {
      nr = read( fd, buf, 4096 );
      if( nr == 0 ) {
         break;
      }
      if( nr < 0 ) {
         rc = -errno;
         fprintf(stderr, "Failed to read '%s': %s\n", file_path, strerror( abs(rc) ) );
         break;
      }

      rc = UG_write( ug, buf, nr, fh );
      if( rc < 0 ) {

         fprintf(stderr, "Failed to write '%s': %d %s\n", path, rc, strerror( abs(rc) ) );
         break;
      }
   }

   close( fd );

   if( rc < 0 ) {
      rc = 1;
      goto put_end;
   }

   // sync 
   rc = UG_fsync( ug, fh );
   if( rc < 0 ) {
         
      fprintf(stderr, "Failed to fsync '%s': %d %s\n", path, rc, strerror( abs(rc) ) );
      rc = 1;
      goto put_end;
   }

   // close 
   rc = UG_close( ug, fh );
   if( rc != 0 ) {
      fprintf(stderr, "Failed to close '%s': %d %s\n", path, rc, strerror( abs(rc) ) );
      rc = 1;
      goto put_end;
   } 

put_end:

   UG_shutdown( ug );

   if( rc != 0 ) {
      exit(1);
   }
   else {
      exit(0);
   }
}
