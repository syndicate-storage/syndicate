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
   printf("Usage %s [syndicate options] [create|replace|put] /path/to/file XATTR VALUE\n", progname );
   exit(1);
}

int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;

   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   // arguments: setxattr [syndicate options] [create|replace|put] /path/to/file xattr_name xattr_value
   if( test_optind < 0 )
      usage( argv[0] );
   
   if( test_optind + 3 >= argc )
      usage( argv[0] );
   
   char* command = argv[test_optind];
   char* path = argv[test_optind+1];
   char* xattr_name = argv[test_optind+2];
   char* xattr_value = argv[test_optind+3];
   
   int flags = 0;
   if( strcmp(command, "create") == 0 )
      flags = XATTR_CREATE;
   
   if( strcmp(command, "replace") == 0 )
      flags = XATTR_REPLACE;
   
   // get state 
   struct syndicate_state* state = syndicate_get_state();
   
   // get the xattr size
   dbprintf("\n\n\nfs_entry_setxattr( %s, %s, %s, flags=%s )\n\n\n", path, xattr_name, xattr_value, command );
   
   int rc = fs_entry_setxattr( state->core, path, xattr_name, xattr_value, strlen(xattr_value), flags, SYS_USER, 0 );
   if( rc < 0 ) {
      errorf("\n\n\nfs_entry_setxattr( %s, %s, %s, flags=%s ) rc = %d\n\n\n", path, xattr_name, xattr_value, command, rc );
      syndicate_functional_test_shutdown( &syndicate_http );
      exit(1);
   }
   
   dbprintf("\n\n\nfs_entry_setxattr( %s, %s, %s, flags=%s ) rc = %d\n\n\n", path, xattr_name, xattr_value, command, rc );
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   return 0;
}
