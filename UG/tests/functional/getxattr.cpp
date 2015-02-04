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
   printf("Usage %s [syndicate options] /path/to/file XATTR\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;

   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   // arguments: getxattr [syndicate options] /path/to/file xattr_name
   if( test_optind < 0 )
      usage( argv[0] );
   
   if( test_optind + 1 >= argc )
      usage( argv[0] );
   
   char* path = argv[test_optind];
   char* xattr_name = argv[test_optind+1];
   
   // stop Valgrind errors
   char xattr_value[65536];
   for( int i = 0; i <= 65536; i++ ) {
      xattr_value[i] = 0;
   }
   
   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // get the xattr size
   SG_debug("\n\n\nfs_entry_getxattr( %s, %s )\n\n\n", path, xattr_name );
   
   ssize_t rc = fs_entry_getxattr( state->core, path, xattr_name, xattr_value, 0, SYS_USER, 0 );
   if( rc < 0 || rc > 65535 ) {
      SG_error("\n\n\nfs_entry_getxattr( %s, %s ) rc = %zd\n\n\n", path, xattr_name, rc );
      syndicate_functional_test_shutdown( &syndicate_http );
      exit(1);
   }
   
   SG_debug("\n\n\nfs_entry_getxattr( %s, %s, 0 ) rc = %zd\n\n\n", path, xattr_name, rc );
   
   // get the xattr for real this time 
   rc = fs_entry_getxattr( state->core, path, xattr_name, xattr_value, rc, SYS_USER, 0 );
   if( rc < 0 || rc > 65535 ) {
      SG_error("\n\n\nfs_entry_getxattr( %s, %s ) rc = %zd\n\n\n", path, xattr_name, rc );
      syndicate_functional_test_shutdown( &syndicate_http );
      exit(1);
   }
   
   SG_debug("\n\n\nfs_entry_getxattr( %s, %s ) = '%s'\n\n\n", path, xattr_name, xattr_value );
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
