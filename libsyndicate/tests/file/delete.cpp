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

#include "delete.h"

#define CREATE_MODE_FILE        0660
#define CREATE_MODE_DIR         0750

int main( int argc, char** argv ) {
   
   int rc = 0;
   struct syndicate_state state;
   struct md_opts opts;
   struct UG_opts ug_opts;
   int local_optind = 0;
   struct ms_client_request* requests = NULL;
   size_t num_requests = 0;
   ms_client_results_list_t results;              // multi result
   
   md_default_opts( &opts );
   
   // get options
   rc = md_parse_opts( &opts, argc, argv, &local_optind, NULL, NULL );
   if( rc != 0 ) {
      errorf("md_parse_opts rc = %d\n", rc );
      md_common_usage( argv[0] );
      exit(1);
   }
   
   memset( &ug_opts, 0, sizeof(struct UG_opts) );
   
   // connect to syndicate
   rc = syndicate_client_init( &state, &opts, &ug_opts );
   if( rc != 0 ) {
      errorf("syndicate_client_init rc = %d\n", rc );
      exit(1);
   }
   
   if((argc - local_optind) <= 0 ) {
      errorf("Usage: %s [SYNDICATE OPTS] file_id [file_id...]\n", argv[0] );
      exit(1);
   }
   
   // set up requests 
   num_requests = argc - local_optind;
   requests = CALLOC_LIST( struct ms_client_request, num_requests );
   results = CALLOC_LIST( struct ms_client_request_result, num_requests );
   
   if( num_requests > 1 ) {
      printf("\n\n\nBegin delete multi\n\n\n");
   }
   else {
      printf("\n\n\nBegin delete single\n\n\n");
   }
   
   // format: file_id [file_id...]
   for( int i = local_optind; i < argc; i++ ) {
      
      uint64_t file_id = 0;
         
      // file_id ID 
      rc = sscanf( argv[i], "%" PRIX64, &file_id );
      if( rc != 1 ) {
         errorf("failed to parse file_id ID '%s'\n", argv[i] );
         exit(1);
      }
      
      printf("   delete(%" PRIX64 ")\n", file_id );
      
      struct md_entry* ent = CALLOC_LIST( struct md_entry, 1 );
      
      ent->file_id = file_id;
      
      ms_client_delete_request( state.ms, ent, &requests[i - local_optind] );
   }
   
   printf("\n\n\n");
   
   if( num_requests > 1 ) {
      
      // multi RPC 
      rc = ms_client_multi_run( state.ms, requests, results, num_requests );
      
      printf("\n\n\nms_client_multi_run(DELETE) rc = %d\n\n\n", rc );
      
      // print out 
      for( unsigned int i = 0; i < num_requests; i++ ) {
         
         struct md_entry* ent = results[i].ent;
         
         if( ent != NULL ) {
            printf("Entry (rc = %d, reply_error = %d): %" PRIX64 " %s mode=%o version=%" PRId64 " write_nonce=%" PRId64 " generation=%d\n",
                  results[i].rc, results[i].reply_error, ent->file_id, ent->name, ent->mode, ent->version, ent->write_nonce, ent->generation );
         }
         else {
            printf("Entry (rc = %d, reply_error = %d): %" PRIX64 " delete\n", results[i].rc, results[i].reply_error, results[i].file_id );
         }
         
         ms_client_request_result_free( &results[i] );
      }
      
      free( results );
   }
   else {
      
      // single RPC 
      rc = ms_client_delete( state.ms, requests[0].ent );
      
      printf("\n\n\nms_client_delete(%" PRIX64 ") rc = %d\n\n\n", requests[0].ent->file_id, rc );
      
      if( rc == 0 ) {
         printf("Entry %" PRIX64 " deleted\n", requests[0].ent->file_id );
      }
   }
   
   
   printf("\n\n\n");
   
   
   
   if( num_requests > 1 ) {
      printf("\n\n\nEnd delete multi\n\n\n");
   }
   else {
      printf("\n\n\nEnd delete single\n\n\n");
   }
   
   syndicate_client_shutdown( &state, 0 );
   
   for( unsigned int i = 0; i < num_requests; i++ ) {
      md_entry_free( requests[i].ent );
      free( requests[i].ent );
   }
   
   free( requests );
   
   return 0;
}