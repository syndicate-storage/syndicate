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

#include "common.h"

void usage( char* progname ) {
   printf("Usage %s [syndicate options] /path/to/dir\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;

   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   // arguments: creat [syndicate options] /path/to/file [data to write]
   if( test_optind < 0 )
      usage( argv[0] );
   
   if( test_optind >= argc )
      usage( argv[0] );
   
   // get path 
   char* path = argv[test_optind];

   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   int rc = 0;
   
   // open the directory 
   dbprintf("\n\n\nfs_entry_opendir( %s )\n\n\n", path );
   
   struct fs_dir_handle* fdh = fs_entry_opendir( state->core, path, SYS_USER, state->core->volume, &rc );
   
   if( rc != 0 ) {
      errorf("\n\n\fs_entry_opendir( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\fs_entry_opendir( %s ) rc = %d\n\n\n", path, rc );
   }
   
   // read the directory 
   dbprintf("\n\n\nfs_entry_readdir( %s )\n\n\n", path );
   
   struct fs_dir_entry** dirents = fs_entry_readdir( state->core, fdh, &rc );
   
   if( rc != 0 ) {
      errorf("\n\n\nfs_entry_readdir( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\nfs_entry_readdir( %s ) rc = %d\n\n", path, rc );
      
      // print them
      for( int i = 0; dirents[i] != NULL; i++ ) {
         printf("   type=%d name=%s\n", dirents[i]->ftype, dirents[i]->data.name );
      }
      
      printf("\n\n");
      
      // free them 
      fs_dir_entry_destroy_all( dirents );
      free( dirents );
   }
   
   // close the directory 
   rc = fs_entry_closedir( state->core, fdh );
   if( rc != 0 ) {
      errorf("\n\n\fs_entry_closedir( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\fs_entry_closedir( %s ) rc = %d\n\n\n", path, rc );
      free( fdh );
   }
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
