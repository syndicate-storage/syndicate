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
   printf("Usage %s [syndicate options] /path/to/file\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;

   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   // arguments: listxattr [syndicate options] /path/to/file
   if( test_optind < 0 )
      usage( argv[0] );
   
   if( test_optind >= argc )
      usage( argv[0] );
   
   char* path = argv[test_optind];
   
   char xattr_listing[65536];
   memset( xattr_listing, 65536, 0 );
   
   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // get the xattr list size
   dbprintf("\n\n\nfs_entry_listxattr( %s )\n\n\n", path );
   
   ssize_t rc = fs_entry_listxattr( state->core, path, xattr_listing, 0, SYS_USER, 0 );
   if( rc < 0 || rc > 65535 ) {
      errorf("\n\n\nfs_entry_listxattr( %s ) rc = %zd\n\n\n", path, rc );
      syndicate_functional_test_shutdown( &syndicate_http );
      exit(1);
   }
   
   dbprintf("\n\n\nfs_entry_listxattr( %s, 0 ) rc = %zd\n\n\n", path, rc );
   
   // get the xattr list for real this time 
   memset( xattr_listing, 0, 65536 );
   
   rc = fs_entry_listxattr( state->core, path, xattr_listing, rc, SYS_USER, 0 );
   if( rc < 0 || rc > 65535 ) {
      errorf("\n\n\nfs_entry_listxattr( %s ) rc = %zd\n\n\n", path, rc );
      syndicate_functional_test_shutdown( &syndicate_http );
      exit(1);
   }
   
   dbprintf("\n\n\nfs_entry_listxattr( %s ) rc = %zd\n", path, rc );
   
   // tokenize and print the xattr listing 
   ssize_t off = 0;
   while( off < rc ) {
      printf("  %s\n", xattr_listing + off);
      
      off += strlen(xattr_listing + off);
      off += 1;
   }
   printf("\n");
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
