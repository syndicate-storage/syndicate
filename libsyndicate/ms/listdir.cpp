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

#include "listdir.h"

#include "libsyndicate/ms/path.h"

// per-download state
struct ms_client_get_dir_download_state {
   
   int64_t batch_id;
   char* url;
   char* auth_header;
};


// init a download state
// takes ownership of url 
// always succeeds 
static void ms_client_get_dir_download_state_init( struct ms_client_get_dir_download_state* dlstate, int64_t batch_id, char* url, char* auth_header ) {
   
   dlstate->batch_id = batch_id;
   dlstate->url = url;
   dlstate->auth_header = auth_header;
   return;
}

// free download state 
// always succeeds
static void ms_client_get_dir_download_state_free( struct ms_client_get_dir_download_state* dlstate ) {
   
   SG_safe_free( dlstate->url );
   SG_safe_free( dlstate->auth_header );
   SG_safe_free( dlstate );
   return;
}


// begin downloading metadata for a directory.
// if least_unknown_generation >= 0, then use the generation number to generate the URL 
// otherwise, use the batch (page) number
// return 0 on success 
// return -ENOMEM on OOM 
// return negative on failure to initialize or start the download
static int ms_client_get_dir_metadata_begin( struct ms_client* client, uint64_t parent_id, int64_t least_unknown_generation, int64_t batch_id, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   int rc = 0;
   CURL* curl = NULL;   
   char* url = NULL;
   char* auth_header = NULL;
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   struct ms_client_get_dir_download_state* dlstate = NULL;

   if( least_unknown_generation > 0 ) {
      
      // least unknown generation
      url = ms_client_file_listdir_url( client->url, volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), parent_id, -1, least_unknown_generation );
   }
   else {
      
      // page id
      url = ms_client_file_listdir_url( client->url, volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), parent_id, batch_id, -1 );
   }

   if( url == NULL ) {
      
      return -ENOMEM;
   }
   
   // set up download state 
   dlstate = SG_CALLOC( struct ms_client_get_dir_download_state, 1 );
   if( dlstate == NULL ) {
      
      SG_safe_free( url );
      return -ENOMEM;
   }
   
   // set up CURL 
   // TODO: connection pool 
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( dlstate );
      SG_safe_free( url );
      return -ENOMEM;
   }
   
   // generate auth header
   rc = ms_client_auth_header( client, url, &auth_header );
   if( rc != 0 ) {
      
      // failed!
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      SG_safe_free( dlstate );
      return -ENOMEM;
   }
   
   ms_client_init_curl_handle( client, curl, url, auth_header );
   
   // set up download 
   rc = md_download_context_init( dlctx, curl, MS_MAX_MSG_SIZE, dlstate );
   if( rc != 0 ) {
      
      SG_safe_free( dlstate );
      SG_safe_free( url );
      SG_safe_free( auth_header );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // watch the download 
   rc = md_download_loop_watch( dlloop, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_watch rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
     
      SG_safe_free( dlstate );
      SG_safe_free( url );
      SG_safe_free( auth_header );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // set up download state
   ms_client_get_dir_download_state_init( dlstate, batch_id, url, auth_header );
   
   // start download 
   rc = md_download_context_start( client->dl, dlctx );
   if( rc != 0 ) {
      
      md_download_context_free( dlctx, NULL );
      ms_client_get_dir_download_state_free( dlstate );
      dlstate = NULL;

      curl_easy_cleanup( curl );
      
      return rc;
   }
   
   return rc;
}


// finish up getting directory metadata, and free up the download handle
// return 0 on success, and set *batch_id to this download's batch
//   *ret_num_children to the number of children downloaded, and *max_gen to be the largest generation number seen.
// return -ENOMEM on OOM 
static int ms_client_get_dir_metadata_end( struct ms_client* client, uint64_t parent_id, struct md_download_context* dlctx, ms_client_dir_listing* dir_listing, int64_t* batch_id, size_t* ret_num_children, int64_t* max_gen ) {
   
   int rc = 0;
   int listing_error = 0;
   struct md_entry* children = NULL;
   size_t num_children = 0;
   CURL* curl = NULL;
   
   int64_t biggest_generation = 0;
   
   struct ms_client_get_dir_download_state* dlstate = (struct ms_client_get_dir_download_state*)md_download_context_get_cls( dlctx );
   md_download_context_set_cls( dlctx, NULL );

   // download status?
   rc = ms_client_download_parse_errors( dlctx );
   
   if( rc != 0 ) {
      
      if( rc != -EAGAIN) {
         // fatal 
         SG_error("ms_client_download_parse_errors( %p ) rc = %d\n", dlctx, rc );
      }
      
      // TODO: connection pool
      md_download_context_unref_free( dlctx, &curl );
      if( curl != NULL ) {
          curl_easy_cleanup( curl );
      }

      ms_client_get_dir_download_state_free( dlstate );
      dlstate = NULL;
      
      return rc;
   }
   
   // collect the data 
   rc = ms_client_listing_read_entries( client, dlctx, &children, &num_children, &listing_error );
   
   // done with the download
   // TODO: connection pool
   md_download_context_unref_free( dlctx, &curl );
   if( curl != NULL ) {
      curl_easy_cleanup( curl );
   }

   ms_client_get_dir_download_state_free( dlstate );
   dlstate = NULL;
   
   // did we get valid data?
   if( rc != 0 ) {
      
      SG_error("ms_client_listing_read_entries(%p) rc = %d\n", dlctx, rc );
      return rc;
   }
   
   if( listing_error != MS_LISTING_NEW ) {
      
      // somehow we didn't get data.  shouldn't happen in listdir
      SG_error("BUG: failed to get listing data for %" PRIX64 ", listing_error = %d\n", parent_id, listing_error );
      return -ENODATA;
   }
   
   // merge children in 
   for( unsigned int i = 0; i < num_children; i++ ) {
      
      uint64_t file_id = children[i].file_id;
      
      SG_debug("%p: %" PRIX64 "\n", dlctx, file_id );
      
      if( dir_listing->count( file_id ) > 0 ) {
         
         SG_warn("Duplicate child %" PRIX64 "\n", file_id );
         md_entry_free( &children[i] );
         continue;
      }
      
      try {
         
         (*dir_listing)[ file_id ] = children[i];
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
         break;
      }
      
      // generation?
      if( children[i].generation > biggest_generation ) {
         
         biggest_generation = children[i].generation;
      }
   }
   
   // NOTE: shallow free--we've copied the children into dir_listing
   SG_safe_free( children );
   
   *ret_num_children = num_children;
   *max_gen = biggest_generation;
   
   return 0;
}

// download metadata for a directory, in one of two ways:
// LISTDIR: fetch num_children entries in parallel by requesting disjoint ranges of them by index, in the range [0, dir_capacity].
// DIFFDIR: query by least unknown generation number until we have num_children entries, or the number of entries in a downloaded batch becomes 0 (i.e. no more entries known).
// in both cases, stop once the number of children is exceeded.
// if least_unknown_generation >= 0, then we will DIFFDIR.
// if dir_capacity >= 0, then we will LISTDIR.
// we can only do one or the other (both/neither are invalid arguments)
// return partial results, even on error 
// return 0 on success
// return -EINVAL for invalid arguments.
// return -ENOMEM on OOM 
// return negative on download failure, or corruption
static int ms_client_get_dir_metadata( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t least_unknown_generation, int64_t dir_capacity, struct ms_client_multi_result* results ) {
   
   int rc = 0;
   
   struct md_download_loop* dlloop = NULL;
   queue< int64_t > batch_queue;
   
   ms_client_dir_listing children;
   uint64_t num_children_downloaded = 0;
   
   int64_t max_known_generation = 0;
   
   struct md_download_context* dlctx = NULL;
   
   int64_t batch_id = 0;
   size_t num_children_fetched = 0;
   int64_t max_generation_fetched = 0;
   int query_count = 0;

   int i = 0;
   
   struct md_entry* ents = NULL;
   
   // sanity check 
   if( least_unknown_generation < 0 && dir_capacity < 0 ) {
      return -EINVAL;
   }
   
   if( least_unknown_generation >= 0 && dir_capacity >= 0 ) {
      return -EINVAL;
   }
   
   memset( results, 0, sizeof(struct ms_client_multi_result) );
   
   SG_debug("listdir %" PRIX64 ", num_children = %" PRId64 ", l.u.g. = %" PRId64 ", dir_capacity = %" PRId64 "\n", parent_id, num_children, least_unknown_generation, dir_capacity );
   
   try {
      if( least_unknown_generation >= 0 ) {
         
         // download from a generation offset 
         batch_queue.push( least_unknown_generation );
      }
      else {
         
         // get all batches in parallel
         for( int64_t batch_id = 0; batch_id * client->page_size < dir_capacity; batch_id++ ) {
            
            batch_queue.push( batch_id );
         }
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   // set up the md_download_loop
   dlloop = md_download_loop_new();
   if( dlloop == NULL ) {
      return -ENOMEM;
   }

   rc = md_download_loop_init( dlloop, client->dl, client->max_connections );
   if( rc != 0 ) {

      SG_safe_free( dlloop );
      return rc;
   }
   
   // run the downloads!
   do {
      
      while( batch_queue.size() > 0 ) {
         
         // next batch 
         int64_t next_batch = batch_queue.front();
         batch_queue.pop();
         
         query_count++;
         
         // next download 
         rc = md_download_loop_next( dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               // all downloads are running 
               rc = 0; 
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         else { 
             // GOGOGO!
             rc = ms_client_get_dir_metadata_begin( client, parent_id, least_unknown_generation, next_batch, dlloop, dlctx );
             if( rc != 0 ) {
            
                SG_error("ms_client_get_dir_metadata_begin( LUG=%" PRId64 ", batch=%" PRId64 " ) rc = %d\n", least_unknown_generation, next_batch, rc );
                break;
             }
         }
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // await next download 
      rc = md_download_loop_run( dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // process all completed downloads 
      while( true ) {
         
         // next completed download 
         rc = md_download_loop_finished( dlloop, &dlctx );
         if( rc != 0 ) {
            
            // out of downloads?
            if( rc == -EAGAIN ) {
              
               SG_debug("Out of downloads (rc = %d)\n", rc); 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finish rc = %d\n", rc );
            break;
         }
         
         // process it 
         rc = ms_client_get_dir_metadata_end( client, parent_id, dlctx, &children, &batch_id, &num_children_fetched, &max_generation_fetched );
         if( rc != 0 ) {
            
            SG_error("ms_client_get_dir_metadata_end rc = %d\n", rc );
            break;
         }
         
         num_children_downloaded += num_children_fetched;
         max_known_generation = MAX( max_generation_fetched, max_known_generation );

         // are we out of children to fetch?
         if( num_children_fetched == 0 ) {
           
            if( (unsigned)num_children_downloaded >= (unsigned)num_children ) { 
                SG_debug("Out of children (%" PRIu64 " fetched total)\n", num_children_downloaded );
            
                rc = MD_DOWNLOAD_FINISH;
                break;
            }
         }
         
         // do we need to switch over to LISTDIR?
         if( batch_queue.size() == 0 && num_children_downloaded < (unsigned)num_children ) {
            
            // yup
            SG_debug("Fetched %" PRIu64 " children (%" PRId64 " total); l.u.g. is now %" PRIu64 "\n", num_children_downloaded, num_children, max_known_generation + 1 );
            least_unknown_generation = max_known_generation + 1;
            batch_queue.push( least_unknown_generation );
         }
      }
      
      if( rc != 0 ) { 
         break;
      }
      
   } while( (batch_queue.size() > 0 || md_download_loop_running( dlloop )) && num_children_downloaded < (unsigned)num_children );
   
   if( rc != 0 ) {
      
      // download stopped prematurely
      md_download_loop_abort( dlloop );
   } 
   
   // free all ms_client_get_dir_download_state
   i = 0;
   for( dlctx = md_download_loop_next_initialized( dlloop, &i ); dlctx != NULL; dlctx = md_download_loop_next_initialized( dlloop, &i ) ) {
      
      if( dlctx == NULL ) {
         break;
      }
      
      struct ms_client_get_dir_download_state* dlstate = (struct ms_client_get_dir_download_state*)md_download_context_get_cls( dlctx );
      md_download_context_set_cls( dlctx, NULL );
     
      if( dlstate != NULL ) { 
          ms_client_get_dir_download_state_free( dlstate );
          dlstate = NULL;
      }
   }
   
   md_download_loop_cleanup( dlloop, NULL, NULL );
   md_download_loop_free( dlloop );
   SG_safe_free( dlloop );
   
   if( rc == MD_DOWNLOAD_FINISH ) {
      rc = 0;
   }
   
   // coalesce what we have into results
   ents = SG_CALLOC( struct md_entry, children.size() );
   if( ents == NULL ) {
      
      if( rc == 0 ) {
         rc = -ENOMEM;
      }
      
      // preserve download error, if need be
      return rc;
   }
   
   i = 0;
   for( ms_client_dir_listing::iterator itr = children.begin(); itr != children.end(); itr++ ) {
      
      ents[i] = itr->second;
      i++;
   }
   
   // populate results 
   results->ents = ents;
   results->reply_error = 0;
   results->num_processed = query_count;
   results->num_ents = children.size();
   
   return rc;
}


// list a directory, and put the data into results
// return 0 on success
// return negative on failure
// NOTE: even if this method fails, the caller should free the contents of results
int ms_client_listdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t dir_capacity, struct ms_client_multi_result* results ) {
   return ms_client_get_dir_metadata( client, parent_id, num_children, -1, dir_capacity, results );
}

// get new directory entries, and put the data into results 
// return 0 on success 
// return negative on failure 
// NOTE: even if this method fails, the caller should free the contents of results
int ms_client_diffdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t least_unknown_generation, struct ms_client_multi_result* results ) {
   return ms_client_get_dir_metadata( client, parent_id, num_children, least_unknown_generation, -1, results );
}
