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

#include "syndicate-ls.h"

int usage( char const* progname ) {

   fprintf(stderr, "Usage: %s [syndicate options] /path/to/file/or/directory\n", progname );
   return 0;
}

// print a single entry 
int print_entry( struct md_entry* dirent ) {
   
   printf("%d %16" PRIX64 " %s\n", dirent->type, dirent->file_id, dirent->name );
   return 0;
}

// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state* ug = NULL;
   struct SG_gateway* gateway = NULL;
   UG_handle_t* dirh = NULL;
   UG_dir_listing_t dirents = NULL;
   char* path = NULL;
   int path_optind = 0;
   struct stat sb;
   char* basename = NULL;
   
   // setup...
   ug = UG_init( argc, argv, true );
   if( ug == NULL ) {
      
      SG_error("%s", "UG_init failed\n" );
      exit(1);
   }
   
   gateway = UG_state_gateway( ug );
   
   // get the directory path 
   path_optind = SG_gateway_first_arg_optind( gateway );
   if( path_optind == argc ) {
      
      usage( argv[0] );
      UG_shutdown( ug );
      exit(1);
   }
   
   path = argv[ path_optind ];
   
   // load up...
   rc = UG_stat( ug, path, &sb );
   if( rc != 0 ) {
      
      fprintf(stderr, "Failed to stat '%s': %s\n", path, strerror( rc ) );
      
      UG_shutdown( ug );
      exit(1);
   }
   
   if( S_ISREG( sb.st_mode ) ) {
      
      // regular file
      struct md_entry dirent;
      
      memset( &dirent, 0, sizeof(struct md_entry) );
      
      dirent.type = UG_TYPE_FILE;
      dirent.file_id = sb.st_ino;
      
      char* basename = fskit_basename( path, NULL );
      if( basename == NULL ) {
         
         // OOM 
         fprintf(stderr, "Out of memory\n");
         exit(2);
      }
      
      dirent.name = basename;
      
      print_entry( &dirent );
      
      SG_safe_free( basename );
      
      UG_shutdown( ug );
      exit(0);
   }
   else {
   
      // directory 
      dirh = UG_opendir( ug, path, &rc );
      if( dirh == NULL ) {
      
         fprintf(stderr, "Failed to open directory '%s': %s\n", path, strerror( rc ) );
         
         UG_shutdown( ug );
         exit(1);
      }
      
      while( true ) {
         
         rc = UG_readdir( ug, &dirents, LS_MAX_DIRENTS, dirh );
         if( rc != 0 ) {
            
            fprintf(stderr, "Failed to read directory '%s': %s\n", path, strerror( rc ) );
            
            rc = UG_closedir( ug, dirh );
            if( rc != 0 ) {
               
               fprintf(stderr, "Failed to close directory '%s': %s\n", path, strerror( rc ) );
            }
            
            UG_shutdown( ug );
            
            exit(1);
         }
         
         if( dirents[0] == NULL ) {
            
            // EOF 
            UG_free_dir_listing( dirents );
            break;
         }
         
         for( unsigned int i = 0; dirents[i] != NULL; i++ ) {
            
            print_entry( dirents[i] );
         }
         
         UG_free_dir_listing( dirents );
      }
      
      rc = UG_closedir( ug, dirh );
      if( rc != 0 ) {
         
         fprintf(stderr, "Failed to close directory '%s': %s\n", path, strerror( rc ) );
         rc = 1;
      }
      
      UG_shutdown( ug );
      
      exit(rc);
   }
}
