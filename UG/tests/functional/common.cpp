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

int syndicate_functional_test_init( int argc, char** argv, int* optind_out, struct md_HTTP* syndicate_http ) {
   
   struct syndicate_opts syn_opts;
   syndicate_default_opts( &syn_opts );
   
   int rc = 0;
   
   // get options
   rc = syndicate_parse_opts( &syn_opts, argc, argv, optind_out, NULL, NULL );
   if( rc != 0 ) {
      syndicate_common_usage( argv[0] );
      exit(1);
   }
   
   
   // start core services
   rc = syndicate_init( &syn_opts );
   if( rc != 0 ) {
      fprintf(stderr, "Syndicate failed to initialize\n");
      exit(1);
   }
   
   struct syndicate_state* state = syndicate_get_state();

   // start back-end HTTP server
   memset( syndicate_http, 0, sizeof(struct md_HTTP) );
   rc = server_init( state, syndicate_http );
   if( rc != 0 )
      exit(1);
   
   // we're now running
   syndicate_set_running();
   
   return 0;
}


int syndicate_functional_test_shutdown( struct md_HTTP* syndicate_http ) {
   
   server_shutdown( syndicate_http );
   
   syndicate_destroy( -1 );
   
   curl_global_cleanup();
   google::protobuf::ShutdownProtobufLibrary();
   
   return 0;
}