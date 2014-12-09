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

#include "create.h"

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
   
   if( (argc - local_optind) % 2 != 0 || (argc - local_optind) <= 0 ) {
      errorf("Usage: %s [SYNDICATE OPTS] parent_id name [parent_id name...]\n", argv[0] );
      exit(1);
   }
   
   // set up requests 
   num_requests = (argc - local_optind) / 2;
   requests = CALLOC_LIST( struct ms_client_request, num_requests );
   results = CALLOC_LIST( struct ms_client_request_result, num_requests );
   
   if( num_requests > 1 ) {
      printf("\n\n\nBegin create multi\n\n\n");
   }
   else {
      printf("\n\n\nBegin create single\n\n\n");
   }
   
   // format: parent_id name. If name ends in /, make a directory
   for( int i = local_optind; i < argc; i+=2 ) {
      
      uint64_t parent_id = 0;
      char* name = NULL;
      uint64_t file_id = 0;
         
      // parent ID 
      rc = sscanf( argv[i], "%" PRIX64, &parent_id );
      if( rc != 1 ) {
         errorf("failed to parse file_id ID '%s'\n", argv[i] );
         exit(1);
      }
      
      // name 
      name = strdup( argv[i+1] );
      
      printf("   create(%s) in %" PRIX64 "\n", name, parent_id );
      
      struct md_entry* ent = CALLOC_LIST( struct md_entry, 1 );
      
      ent->type = (name[strlen(name)-1] == '/' ? MD_ENTRY_DIR : MD_ENTRY_FILE );
      ent->file_id = ms_client_make_file_id();
      ent->name = name;
      ent->parent_id = parent_id;
      ent->mode = (ent->type == MD_ENTRY_DIR ? CREATE_MODE_DIR : CREATE_MODE_FILE );
      
      if( ent->type == MD_ENTRY_FILE ) {
         ms_client_create_request( state.ms, ent, &requests[(i - local_optind)/2] );
      }
      else {
         ms_client_mkdir_request( state.ms, ent, &requests[(i - local_optind)/2] );
      }
   }
   
   printf("\n\n\n");
   
   if( num_requests > 1 ) {
      
      // multi RPC 
      rc = ms_client_run_requests( state.ms, requests, results, num_requests );
      
      printf("\n\n\ms_client_run_requests(CREATE) rc = %d\n\n\n", rc );
      
      // print out 
      for( unsigned int i = 0; i < num_requests; i++ ) {
         
         struct md_entry* ent = results[i].ent;
         
         if( ent != NULL ) {
            printf("Entry (rc = %d, reply_error = %d): %" PRIX64 " %s mode=%o version=%" PRId64 " write_nonce=%" PRId64 " generation=%d\n",
                  results[i].rc, results[i].reply_error, ent->file_id, ent->name, ent->mode, ent->version, ent->write_nonce, ent->generation );
         }
         else {
            printf("Entry (rc = %d, reply_error = %d): %" PRIX64 " create failed\n", results[i].rc, results[i].reply_error, results[i].file_id );
         }
         
         ms_client_request_result_free( &results[i] );
      }
      
      free( results );
   }
   else {
      
      // single RPC 
      uint64_t file_id = requests[0].ent->file_id;
      int64_t write_nonce = 0;
      
      rc = ms_client_create( state.ms, &file_id, &write_nonce, requests[0].ent );
      
      printf("\n\n\nms_client_create(%" PRIX64 ") rc = %d\n\n\n", requests[0].ent->file_id, rc );
      
      if( rc == 0 ) {
         printf("Entry %s: file_id = %" PRIX64 ", write_nonce = %d\n", requests[0].ent->name, file_id, write_nonce );
      }
   }
   
   
   printf("\n\n\n");
   
   
   
   if( num_requests > 1 ) {
      printf("\n\n\nEnd create multi\n\n\n");
   }
   else {
      printf("\n\n\nEnd create single\n\n\n");
   }
   
   syndicate_client_shutdown( &state, 0 );
   
   for( unsigned int i = 0; i < num_requests; i++ ) {
      md_entry_free( requests[i].ent );
      free( requests[i].ent );
   }
   
   free( requests );
   
   return 0;
}