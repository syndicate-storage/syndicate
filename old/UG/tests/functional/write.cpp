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
   printf("Usage %s [syndicate options] /path/to/file offset data_to_write [offset data_to_write...] \n", progname );
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
   
   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // open the file
   int rc = 0;
   SG_debug("\n\n\nfs_entry_open( %s )\n\n\n", path );
   struct fs_file_handle* fh = fs_entry_open( state->core, path, SG_SYS_USER, state->core->volume, O_WRONLY, 0755, &rc );
   
   if( fh == NULL || rc != 0 ) {
      SG_error("\n\n\nfs_entry_open( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      SG_debug("\n\n\nfs_entry_open( %s ) rc = %d\n\n\n", path, rc );
   }
   
   // write data
   for( int i = test_optind + 1; i < argc; i++ ) {
      
      // get offset
      long offset = strtol( argv[i], NULL, 10 );
      
      i++;
      if( i >= argc ) {
         // must have [offset size] pairs
         usage( argv[0] );
      }
      
      // get data
      char* buf = argv[i];
      size_t size = strlen(buf);
      
      // write the data
      SG_debug("\n\n\nfs_entry_write( %s, %ld, %ld, '%s' )\n\n\n", path, size, offset, buf );
      ssize_t nw = fs_entry_write( state->core, fh, buf, size, offset );
      
      if( nw < 0 ) {
         SG_error("\n\n\nfs_entry_write( %s, %ld, %ld, '%s' ) rc = %d\n\n\n", path, size, offset, buf, nw );
         exit(1);
      }
      else {
         SG_debug("\n\n\nfs_entry_write( %s, %ld, %ld, '%s' ) rc = %zd\n\n\n", path, size, offset, buf, nw );
      }
   }
   
   // close
   SG_debug("\n\n\nfs_entry_close( %s )\n\n\n", path );
   rc = fs_entry_close( state->core, fh );
   if( rc != 0 ) {
      SG_error("\n\n\nfs_entry_close( %s ) rc = %d\n\n\n", path, rc );
      exit(1);
   }
   else {
      SG_debug("\n\n\nfs_entry_close( %s ) rc = %d\n\n\n", path, rc );
   }
   
   free( fh );
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
