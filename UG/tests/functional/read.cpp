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
   printf("Usage %s [syndicate options] /path/to/file offset size [offset size]\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;

   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   // arguments: read [syndicate options] /path/to/file offset length [offset length...]
   if( test_optind < 0 )
      usage( argv[0] );
   
   if( test_optind >= argc )
      usage( argv[0] );
   
   char* path = argv[test_optind];
   
   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // open the file
   int rc = 0;
   dbprintf("\n\n\nfs_entry_open( %s )\n\n\n", path );
   struct fs_file_handle* fh = fs_entry_open( state->core, path, SYS_USER, state->core->volume, O_RDONLY, 0755, &rc );
   
   if( fh == NULL || rc != 0 ) {
      errorf("\n\n\nfs_entry_open( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      dbprintf("\n\n\nfs_entry_open( %s ) rc = %d\n\n\n", path, rc );
   }
   
   
   for( int i = test_optind + 1; i < argc; i++ ) {
      
      // get offset
      long offset = strtol( argv[i], NULL, 10 );
      
      i++;
      if( i >= argc ) {
         // must have [offset size] pairs
         usage( argv[0] );
      }
      
      // get size
      long size = strtol( argv[i], NULL, 10 );
      
      // make a read buffer
      char* buf = CALLOC_LIST( char, size );
      
      // get the data
      dbprintf("\n\n\nfs_entry_read( %s, %ld, %ld )\n\n\n", path, size, offset );
      ssize_t nr = fs_entry_read( state->core, fh, buf, size, offset );
      
      if( nr < 0 ) {
         errorf("\n\n\nfs_entry_read( %s ) rc = %d\n\n\n", path, nr );
         exit(1);
      }
      else {
         dbprintf("\n\n\nfs_entry_read( %s ) rc = %zd\n\nData: '%s'\n\n\n", path, nr, buf );
      }
      
      free( buf );
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
   
   free( fh );
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
