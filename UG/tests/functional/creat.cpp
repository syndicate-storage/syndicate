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
   printf("Usage %s [syndicate options] /path/to/file [data_to_write]\n", progname );
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
   
   char* path = argv[test_optind];
   
   // do we have data?
   char* data = NULL;
   if( test_optind + 1 < argc )
      data = argv[test_optind + 1];

   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // create the file
   int rc = 0;
   dbprintf("\n\n\nfs_entry_create( %s )\n\n\n", path );
   struct fs_file_handle* fh = fs_entry_create( state->core, path, SYS_USER, state->core->volume, 0755, &rc );
   
   if( fh == NULL || rc != 0 ) {
      errorf("\n\n\nfs_entry_create( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\nfs_entry_create( %s ) rc = %d\n\n\n", path, rc );
   }
   
   // write data, if we're supposed to 
   if( data != NULL ) {
      dbprintf("\n\n\nfs_entry_write( %s, %s )\n\n\n", path, data );
      rc = fs_entry_write( state->core, fh, data, strlen(data), 0 );
      
      if( rc != (signed)strlen(data) ) {
         errorf("\n\n\nfs_entry_write( %s ) rc = %d\n\n\n", path, rc );
         exit(1);
      }
      else {
         dbprintf("\n\n\nfs_entry_write( %s ) rc = %d\n\n\n", path, rc );
      }
      
      // fsync data 
      dbprintf("\n\n\nfs_entry_fsync( %s )\n\n\n", path );
      rc = fs_entry_fsync( state->core, fh );
      if( rc != 0 ) {
         errorf("\n\n\nfs_entry_fsync( %s ) rc = %d\n\n\n", path, rc );
         exit(1);
      }  
      else {
         dbprintf("\n\n\nfs_entry_fsync( %s ) rc = %d\n\n\n", path, rc );
      }
   }
   
   // close
   dbprintf("\n\n\nfs_entry_close( %s )\n\n\n", path );
   rc = fs_entry_close( state->core, fh );
   if( rc != 0 ) {
      errorf("\n\n\nfs_entry_close( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\nfs_entry_close( %s ) rc = %d\n\n\n", path, rc );
   }
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
