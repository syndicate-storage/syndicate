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

#include "getattr.h"

#include "libsyndicate/ms/path.h"

// download state for metadata
struct ms_client_get_metadata_context {
   
   char* url;
   char* auth_header;
   int request_id;
}; 

static void ms_client_get_metadata_context_init( struct ms_client_get_metadata_context* dlstate, char* url, char* auth_header, int request_id ) {
   
   dlstate->url = url;
   dlstate->auth_header = auth_header;
   dlstate->request_id = request_id;
}

static void ms_client_get_metadata_context_free( struct ms_client_get_metadata_context* dlstate ) {
   
   SG_safe_free( dlstate->auth_header );
   SG_safe_free( dlstate->url );
   SG_safe_free( dlstate );
}


// begin downloading metadata 
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on failure to set up and start the download
static int ms_client_get_metadata_begin( struct ms_client* client, struct ms_path_ent* path_ent, int request_id, bool do_getchild, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   char* auth_header = NULL;
   char* url = NULL;
   CURL* curl = NULL;
   struct ms_client_get_metadata_context* dlstate = NULL;
   int rc = 0;
   
   // make the URL 
   if( !do_getchild ) {
      url = ms_client_file_getattr_url( client->url, path_ent->volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), path_ent->file_id, path_ent->version, path_ent->write_nonce );
   }
   else {
      url = ms_client_file_getchild_url( client->url, path_ent->volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), path_ent->parent_id, path_ent->name );
   }
   
   if( url == NULL ) {
      
      return -ENOMEM;
   }
   
   SG_debug("%s download %p = %d, url %s\n", (do_getchild ? "GETCHILD" : "GETATTR"), dlctx, request_id, url );
   
   // set up curl 
   // TODO connection pool
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( url );
      return -ENOMEM;
   }
   
   // generate auth header
   rc = ms_client_auth_header( client, url, &auth_header );
   if( rc != 0 ) {
      
      // failed!
      SG_safe_free( url );
      curl_easy_cleanup( curl );
      return -ENOMEM;
   }
   
   ms_client_init_curl_handle( client, curl, url, auth_header );
   
   // set up download state 
   dlstate = SG_CALLOC( struct ms_client_get_metadata_context, 1 );
   if( dlstate == NULL ) {
      
      // OOM 
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      SG_safe_free( auth_header );
      return -ENOMEM;
   }
   
   ms_client_get_metadata_context_init( dlstate, url, auth_header, request_id );
   
   // set up the download context 
   rc = md_download_context_init( dlctx, curl, MS_MAX_MSG_SIZE, dlstate );
   if( rc != 0 ) {
      
      SG_error("md_download_context_init( '%s' ) rc = %d\n", url, rc );
      
      curl_easy_cleanup( curl );
      ms_client_get_metadata_context_free( dlstate );
      return rc;
   }
   
   // watch the download 
   rc = md_download_loop_watch( dlloop, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_watch rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
      
      curl_easy_cleanup( curl );
      ms_client_get_metadata_context_free( dlstate );
      return rc;
   }
   
   // start the download 
   rc = md_download_context_start( client->dl, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_start( '%s' ) rc = %d\n", url, rc );
      
      md_download_context_free( dlctx, NULL );
      
      curl_easy_cleanup( curl );
      ms_client_get_metadata_context_free( dlstate );
      return rc;
   }
   
   return 0;
}


// finish up a metadata entry download, and free up the download handle
// return 0 on success, and put the batch's entries into the right places in result_ents, and set its *request_id
// return -ENOMEM on OOM 
// return -EBADMSG if we could not determine the listing status
static int ms_client_get_metadata_end( struct ms_client* client, ms_path_t* path, struct md_download_context* dlctx, struct md_entry* result_ents, int* request_id ) {
   
   int rc = 0;
   struct ms_client_get_metadata_context* dlstate = (struct ms_client_get_metadata_context*)md_download_context_get_cls( dlctx );
   CURL* curl = NULL; 
   int ent_idx = dlstate->request_id;
   struct md_entry ent;
   int listing_error = 0;
   
   // download status?
   rc = ms_client_download_parse_errors( dlctx );
   
   if( rc != 0 ) {
      
      if( rc != -EAGAIN ) {
         
         SG_error("ms_client_download_parse_errors( %p ) rc = %d\n", dlctx, rc );
      }
      
      md_download_context_set_cls( dlctx, NULL );

      // TODO connection pool
      md_download_context_unref_free( dlctx, &curl );
      if( curl != NULL ) {
         curl_easy_cleanup( curl );
      }

      ms_client_get_metadata_context_free( dlstate );
      return rc;
   }
   
   // success!
   memset( &ent, 0, sizeof(struct md_entry) );
   
   // get the entry from the result
   rc = ms_client_listing_read_entry( client, dlctx, &ent, &listing_error );
   
   // done with the download 
   // TODO connection pool
   md_download_context_set_cls( dlctx, NULL );
   md_download_context_unref_free( dlctx, &curl );
   if( curl != NULL ) {
      curl_easy_cleanup(curl);
   }

   ms_client_get_metadata_context_free( dlstate );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_listing_read_entry( %p ) rc = %d, listing_error = %d\n", dlctx, rc, listing_error);
      
      if( rc == -ENODATA ) {
         
         // failed because there was an MS-given error 
         rc = listing_error;
      }
      
      return rc;
   }
   
   // what's the listing status?
   if( listing_error == MS_LISTING_NONE ) {
      
      SG_warn("no data for %" PRIX64 "\n", path->at(ent_idx).file_id );
      
      memset( &result_ents[ent_idx], 0, sizeof(struct md_entry) );
      
      result_ents[ent_idx].file_id = path->at(ent_idx).file_id;
      result_ents[ent_idx].error = MS_LISTING_NONE;
   }
   else if( listing_error == MS_LISTING_NOCHANGE ) {
      
      SG_warn("no change in %" PRIX64 "\n", path->at(ent_idx).file_id );
      
      memset( &result_ents[ent_idx], 0, sizeof(struct md_entry) );
      
      result_ents[ent_idx].file_id = path->at(ent_idx).file_id;
      result_ents[ent_idx].error = MS_LISTING_NOCHANGE;
   }
   else if( listing_error == MS_LISTING_NEW ) {
      
      // got data! store it to the results buffer
      result_ents[ent_idx] = ent;
      result_ents[ent_idx].error = MS_LISTING_NEW;
   }
   else {
      
      SG_error("ms_client_listing_read_entry( %p ): unknown listing error %d\n", dlctx, listing_error );
      return -EBADMSG;
   }
   
   // succeeded!
   *request_id = ent_idx;
   return 0;
}


// download metadata for a set of entries 
// by default, this performs GETATTR.  If do_getchild is true, then this runs GETCHILD 
// return partial results, even in error.
// NOTE: for GETATTR, path[i].file_id, .volume_id, .version, and .write_nonce must be defined for each entry
// NOTE: for GETCHILD, path[i].parent_id, .volume_id, and .name must be defined for each entry
// return 0 on success, or if there are no path entries to download
// return -EINVAL if the path of entries to fetch data for (see above) is not well-formed
// return -ENOMEM on OOM
// return -ENODATA if we retried the maximum number of times to fetch an entry, but failed to do so
static int ms_client_get_metadata( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result, bool do_getchild ) {
   
   int rc = 0;
   struct md_entry* result_ents = NULL;
   struct md_download_loop* dlloop = NULL;
   queue<int> request_ids;
   int* attempts = NULL;
   int num_processed = 0;
   int request_id = 0;
   struct md_download_context* dlctx = NULL;
   
   if( path->size() == 0 ) {
      return 0;
   }
   
   if( !do_getchild ) {
      
      // validate GETATTR
      for( unsigned int i = 0; i < path->size(); i++ ) {
         
         if( path->at(i).volume_id == 0 ) {
            return -EINVAL;
         }
      }
   }
   else {
      
      // validate GETCHILD 
      for( unsigned int i = 0; i < path->size(); i++ ) {
         
         if( path->at(i).name == NULL ) {
            return -EINVAL;
         }
      }
   }
   
   // set up results 
   result_ents = SG_CALLOC( struct md_entry, path->size() );
   if( result_ents == NULL ) {
      
      return -ENOMEM;
   }
   
   // set up download loop 
   dlloop = md_download_loop_new();
   if( dlloop == NULL ) {
      
      SG_safe_free( result_ents );
      return -ENOMEM;
   }

   rc = md_download_loop_init( dlloop, client->dl, MIN( (unsigned)client->max_connections, path->size() ) );
   if( rc != 0 ) {
      
      SG_safe_free( dlloop );
      SG_safe_free( result_ents );
      return rc;
   }
   
   // set up attempt counts 
   attempts = SG_CALLOC( int, path->size() );
   if( attempts == NULL ) {
      
      SG_safe_free( result_ents );
      md_download_loop_free( dlloop );
      SG_safe_free( dlloop );
      return -ENOMEM;
   }
  
   try { 
       // prepare all entries!
       for( unsigned int i = 0; i < path->size(); i++ ) {
      
          request_ids.push( i );
       }
   }
   catch( bad_alloc& ba ) {
      SG_safe_free( result_ents );
      md_download_loop_free( dlloop );
      SG_safe_free( dlloop );
      return -ENOMEM;
   }
   
   // run the download loop!
   do {
      
      // start as many downloads as we can 
      while( request_ids.size() > 0 ) {
         
         // next download 
         rc = md_download_loop_next( dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               // pipe is full 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // this download 
         request_id = request_ids.front();
         request_ids.pop();
         
         // start the download 
         rc = ms_client_get_metadata_begin( client, &path->at(request_id), request_id, do_getchild, dlloop, dlctx );
         if( rc != 0 ) {
            
            SG_error("ms_client_get_metadata_begin( %p ) rc = %d\n", dlctx, rc );
            break;
         }
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // run the downloads 
      rc = md_download_loop_run( dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // process any finished downloads 
      while( true ) {
         
         struct md_download_context* dlctx = NULL;
         
         // next finished download 
         rc = md_download_loop_finished( dlloop, &dlctx );
         if( rc < 0 ) {
            
            if( rc == -EAGAIN) {
               // drained 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finished rc = %d\n", rc );
            break;
         }
         
         // process it 
         rc = ms_client_get_metadata_end( client, path, dlctx, result_ents, &request_id );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // try again 
               attempts[request_id]++;
               if( attempts[request_id] >= client->conf->max_metadata_read_retry ) {
                  
                  SG_error("Path entry %d attempted too many times\n", request_id );
                  rc = -ENODATA;
               }
               else {
                  rc = 0;
                  continue;
               }
            }
            
            SG_error("ms_client_get_metadata_end( %p ) rc = %d\n", dlctx, rc );
            break;
         }
         else {
            
            // success!
            num_processed++;
         }
      }
      
      if( rc != 0 ) {
         break;
      }
      
   } while( md_download_loop_running( dlloop ) );
   
   if( rc != 0 ) {
      
      SG_error("Abort download loop %p, rc = %d\n", dlloop, rc );
      
      md_download_loop_abort( dlloop );
      
      int i = 0;
      
      // free all ms_client_get_metadata_context
      for( dlctx = md_download_loop_next_initialized( dlloop, &i ); dlctx != NULL; dlctx = md_download_loop_next_initialized( dlloop, &i ) ) {
         
         if( dlctx == NULL ) {
            break;
         }
         
         struct ms_client_get_metadata_context* dlstate = (struct ms_client_get_metadata_context*)md_download_context_get_cls( dlctx );
         md_download_context_set_cls( dlctx, NULL );

         if( dlstate != NULL ) { 
             ms_client_get_metadata_context_free( dlstate );
             SG_safe_free( dlstate );
         }
      }
   }
   
   md_download_loop_cleanup( dlloop, NULL, NULL );
   md_download_loop_free( dlloop );
   SG_safe_free( dlloop );
   SG_safe_free( attempts );
   
   result->ents = result_ents;
   result->num_ents = path->size();
   result->num_processed = num_processed;
   
   if( rc == 0 ) {
      // success! convert to multi result
      result->reply_error = 0;
   }
   else {
      result->reply_error = rc;
      
      SG_error("md_download_all rc = %d\n", rc );
   }
   
   return rc;
}

// download multiple entries at once.
// result->ents will be in the same order as the corresponding element in path.
// path entries need:
// * file_id 
// * volume_id 
// * version 
// * write_nonce
// return 0 on success
// return negative on error
int ms_client_getattr_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result ) {
   return ms_client_get_metadata( client, path, result, false );
}


// download metadata for a single entry 
// ms_ent needs:
// * file_id 
// * volume_id 
// * version 
// * write_nonce 
// return 0 on success
// return -ENODATA on failure to communicate with the MS
// return -EACCES on permission error in the MS
// return -ENOENT if the entry doesn't exist
// return -EBADMSG if the MS replied invalid data
int ms_client_getattr( struct ms_client* client, struct ms_path_ent* ms_ent, struct md_entry* ent_out ) {
   
   ms_path_t path;
   int rc = 0;
   struct ms_client_multi_result result;
   
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   try {
      path.push_back( *ms_ent );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   rc = ms_client_get_metadata( client, &path, &result, false );
   if( rc != 0 ) {
      SG_error("ms_client_get_metadata(%" PRIX64 ") rc = %d\n", ms_ent->file_id, rc );
      ms_client_multi_result_free( &result );
      return -ENODATA;
   }

   if( result.reply_error != 0 ) {

      SG_error("MS replied %d\n", result.reply_error); 
      ms_client_multi_result_free( &result );
      ent_out->error = result.reply_error;
      return result.reply_error;
   }

   if( result.num_processed != 1 || result.num_ents != 1 ) {

      SG_error("Got back %d results (%zu entries), expected 1\n", result.num_processed, result.num_ents);
      ms_client_multi_result_free( &result );
      return -EBADMSG;
   }
  
   // gift result 
   memcpy( ent_out, &result.ents[0], sizeof(struct md_entry) );
   memset( &result.ents[0], 0, sizeof(struct md_entry) );
   
   ms_client_multi_result_free( &result );

   return 0;
}


// download multiple entries at once
// result->ents will be in the same order as the entries in path.
// retur 0 on success 
// return negative on error
int ms_client_getchild_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result ) {
   return ms_client_get_metadata( client, path, result, true );
}

// download metadata for a single entry 
// ms_ent needs:
// * parent_id
// * volume_id
// * name 
// return 0 on success 
// return negative on error
int ms_client_getchild( struct ms_client* client, struct ms_path_ent* ms_ent, struct md_entry* ent_out ) {
   
   ms_path_t path;
   int rc = 0;
   struct ms_client_multi_result result;

   if( ms_ent->name == NULL ) {
      return -EINVAL;
   }
   
   try {
      path.push_back( *ms_ent );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   rc = ms_client_get_metadata( client, &path, &result, true );
   if( rc != 0 ) {
      SG_error("ms_client_get_metadata(%s) rc = %d, MS reply %d\n", ms_ent->name, result.reply_error, rc );
      ent_out->error = result.reply_error;
      ms_client_multi_result_free( &result );
      return -ENODATA;
   }

   if( result.reply_error != 0 ) {

      SG_error("MS replied %d\n", result.reply_error); 
      ent_out->error = result.reply_error;
      ms_client_multi_result_free( &result );
      return result.reply_error;
   }

   if( result.num_processed != 1 || result.num_ents != 1 ) {

      SG_error("Got back %d results (%zu entries), expected 1\n", result.num_processed, result.num_ents);
      ms_client_multi_result_free( &result );
      return -EBADMSG;
   }
  
   // gift result 
   memcpy( ent_out, &result.ents[0], sizeof(struct md_entry) );
   memset( &result.ents[0], 0, sizeof(struct md_entry) );
   
   ms_client_multi_result_free( &result );
   return rc;
}

// set up an ms_ent to request attributes
// always succeeds
int ms_client_getattr_request( struct ms_path_ent* ms_ent, uint64_t volume_id, uint64_t file_id, int64_t file_version, int64_t write_nonce, void* cls ) {
   
   memset( ms_ent, 0, sizeof(struct ms_path_ent) );
   return ms_client_make_path_ent( ms_ent, volume_id, 0, file_id, file_version, write_nonce, 0, 0, 0, NULL, cls );
}
