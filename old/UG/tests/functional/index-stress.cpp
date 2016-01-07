/*
   Copyright 2015 The Trustees of Princeton University

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

char* global_directory = NULL;
struct syndicate_state* global_state = NULL;
uint64_t global_num_files = 0;
int global_num_failures = 0;

void usage( char* progname ) {
   printf("Usage %s [syndicate options] /path/to/directory NUM_FILES NUM_THREADS\n", progname );
   exit(1);
}


void* create_main( void* arg ) {
   
   uint64_t* file_ids = (uint64_t*)arg;
   char path[4097];
   int rc = 0;
   
   
   for( uint64_t i = 0; i < global_num_files; i++ ) {
      
      memset( path, 0, 4097 );
      sprintf( path, "%s/file-%" PRIu64, global_directory, file_ids[i] );
      
      SG_debug("Create '%s'\n", path );
      
      struct fs_file_handle* fh = fs_entry_create( global_state->core, path, SG_SYS_USER, global_state->core->volume, 0755, &rc );
      
      if( fh == NULL || rc != 0 ) {
         
         SG_error("\n\n\nfs_entry_create('%s') rc = %d\n\n\n", path, rc );
         
         __sync_fetch_and_add( &global_num_failures, 1 );
         continue;
      }
      
      rc = fs_entry_close( global_state->core, fh );
      free( fh );
      
      if( rc != 0 ) {
      
         SG_error("\n\n\nfs_entry_close('%s') rc = %d\n\n\n", path, rc );
      }
   }
   
   return NULL;
}


int main( int argc, char** argv ) {
   
   struct md_HTTP syndicate_http;
   
   int test_optind = -1;
   uint64_t num_threads = 0;
   
   // set up the test 
   syndicate_functional_test_init( argc, argv, &test_optind, &syndicate_http );
   
   if( test_optind < 0 ) {
      usage( argv[0] );
   }
   
   if( test_optind + 2 >= argc ) {
      usage( argv[0] );
   }
   
   global_directory = argv[test_optind];
   global_state =  syndicate_get_state();
   
   global_num_files = (uint64_t)strtoull( argv[test_optind+1], 0, 10 );
   num_threads = (uint64_t)strtoull( argv[test_optind+2], 0, 10 );
   
   if( global_num_files == 0 || num_threads == 0 ) {
      usage( argv[0] );
   }
   
   // make work 
   uint64_t* work = SG_CALLOC( uint64_t, global_num_files * num_threads );
   
   // start up threads 
   pthread_t* threads = SG_CALLOC( pthread_t, num_threads );
   
   if( work == NULL || threads == NULL ) {
      exit( ENOMEM );
   }
   
   for( uint64_t i = 0; i < global_num_files * num_threads; i++ ) {
      work[i] = i;
   }
   
   for( uint64_t i = 0; i < num_threads; i++ ) {
      
      pthread_attr_t attrs;
      pthread_attr_init( &attrs );
      
      pthread_create( &threads[i], &attrs, create_main, work + (i * global_num_files) );
   }
   
   for( uint64_t i = 0; i < num_threads; i++ ) {
      
      pthread_join( threads[i], NULL );
   }
   
   // shut down the test 
   syndicate_functional_test_shutdown( &syndicate_http );
   
   printf("\n\nTotal failures: %d\n", global_num_failures );
   
   return 0;
}
