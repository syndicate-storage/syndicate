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

// generate a URL for a batch to download 
char* ms_client_listdir_generate_batch_url( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_listdir_context* ctx = (struct ms_client_listdir_context*)cls;
   char* url = NULL;
   int next_batch = 0;
   
   pthread_mutex_lock( &ctx->lock );
   
   if( ctx->batches->size() > 0 ) {
   
      // next batch 
      next_batch = ctx->batches->front();
      ctx->batches->pop();
      
      // interpret batch as the page ID 
      url = ms_client_file_listdir_url( ctx->client->url, ctx->volume_id, ctx->parent_id, next_batch, -1 );
      
      // remember which download this was for
      (*ctx->downloading)[ dlctx ] = next_batch;
   }
   
   pthread_mutex_unlock( &ctx->lock );
   return url;
}


// listdir curl init'er 
static CURL* ms_client_listdir_curl_generator( void* cls ) {
   
   struct ms_client_listdir_context* ctx = (struct ms_client_listdir_context*)cls;
   
   // TODO: connection pool 
   CURL* curl = NULL;
   
   curl = curl_easy_init();
   
   ms_client_init_curl_handle( ctx->client, curl, NULL );
   
   return curl;
}


// retry a listdir 
int ms_client_listdir_batch_retry( struct ms_client_listdir_context* ctx, int batch_num ) {
   
   if( ctx->attempts->find( batch_num ) == ctx->attempts->end() ) {
      
      (*ctx->attempts)[batch_num] = 1;
   }
   else {
      
      (*ctx->attempts)[batch_num] ++;
      
      if( (*ctx->attempts)[batch_num] > ctx->client->conf->max_metadata_read_retry ) {
         
         // too many retries 
         errorf("Bailing out after trying batch %d %d times\n", batch_num, (*ctx->attempts)[batch_num] );
         return -ENODATA;
      }
   }
   
   ctx->batches->push( batch_num );
   
   return 0;
}

// cancel a download 
int ms_client_listdir_download_cancel( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_listdir_context* ctx = (struct ms_client_listdir_context*)cls;
   
   pthread_mutex_lock( &ctx->lock );
   
   // no longer downloading 
   ctx->downloading->erase( dlctx );
   
   pthread_mutex_unlock( &ctx->lock );
   
   return 0;
}

// postprocess a downloaded entry 
int ms_client_listdir_download_postprocess( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_listdir_context* ctx = (struct ms_client_listdir_context*)cls;
   int rc = 0;
   int this_batch = 0;
   int listing_error = 0;
   struct md_entry* children = NULL;
   size_t num_children = 0;
   
   pthread_mutex_lock( &ctx->lock );
   
   // which batch does this download refer to?
   ms_client_listdir_batch_set::iterator itr = ctx->downloading->find( dlctx );
   if( itr == ctx->downloading->end() ) {
      
      errorf("BUG: %p is not downloading\n", dlctx );
      pthread_mutex_unlock( &ctx->lock );
      
      return -EINVAL;
   }
   
   this_batch = itr->second;
   
   // no longer downloading 
   ctx->downloading->erase( itr );
   
   // download status?
   rc = ms_client_download_parse_errors( dlctx );
   
   if( rc == -EAGAIN ) {
      
      // try again 
      rc = ms_client_listdir_batch_retry( ctx, this_batch );
      
      if( rc != 0 ) {
         // can't retry
         
         pthread_mutex_unlock( &ctx->lock );
         return rc;
      }
   }
   else if( rc < 0 ) {
      
      // fatal error
      pthread_mutex_unlock( &ctx->lock );
      return rc;
   }
   
   // collect the data 
   rc = ms_client_listing_read_entries( ctx->client, dlctx, &children, &num_children, &listing_error );
   if( rc != 0 ) {
      
      errorf("ms_client_listing_read_entries(%p) rc = %d\n", dlctx, rc );
      
      ctx->listing_error = listing_error;
      pthread_mutex_unlock( &ctx->lock );
      return rc;
   }
   
   // merge them in 
   for( unsigned int i = 0; i < num_children; i++ ) {
      
      uint64_t file_id = children[i].file_id;
      
      dbprintf("%p: %" PRIX64 "\n", dlctx, file_id );
      
      if( ctx->children_ids->count( file_id ) > 0 ) {
         errorf("Duplicate child %" PRIX64 "\n", file_id );
         rc = -EBADMSG;
      }
      
      if( rc == 0 ) {
         ctx->children_ids->insert( file_id );
         ctx->children->push_back( children[i] );
      }
   }
   
   free( children );
   
   pthread_mutex_unlock( &ctx->lock );
   return rc;
}


// set up a listdir context 
int ms_client_listdir_context_init( struct ms_client_listdir_context* ctx, struct ms_client* client, uint64_t parent_id, int64_t version, int64_t write_nonce, int num_children ) {
   
   memset( ctx, 0, sizeof(struct ms_client_listdir_context) );
   
   ctx->client = client;
   ctx->parent_id = parent_id;
   ctx->volume_id = ms_client_get_volume_id( client );
   ctx->version = version;
   ctx->write_nonce = write_nonce;
   
   ctx->downloading = new ms_client_listdir_batch_set();
   ctx->children = new vector<struct md_entry>();
   ctx->attempts = new ms_client_listdir_attempt_set();
   ctx->batches = new queue<int>();
   ctx->children_ids = new set<uint64_t>();
   
   // one batch == one page
   size_t num_pages = num_children / client->page_size;
   for( unsigned int i = 0; i <= num_pages; i++ ) {
      ctx->batches->push( i );
   }
      
   pthread_mutex_init( &ctx->lock, NULL );
   return 0;
}

// free a listdir context 
int ms_client_listdir_context_free( struct ms_client_listdir_context* ctx ) {
   
   if( ctx->downloading != NULL ) {
      
      delete ctx->downloading;
      ctx->downloading = NULL;
   }
   
   if( ctx->children != NULL ) {
      
      delete ctx->children;
      ctx->children = NULL;
   }
   
   if( ctx->attempts != NULL ) {
      
      delete ctx->attempts;
      ctx->attempts = NULL;
   }
   
   if( ctx->batches != NULL ) {
      
      delete ctx->batches;
      ctx->batches = NULL;
   }
   
   if( ctx->children_ids ) {
      
      delete ctx->children_ids;
      ctx->children_ids = NULL;
   }
   
   return 0;
}

// download metadata for a directory.
// return partial results, even on error.
int ms_client_listdir( struct ms_client* client, uint64_t parent_id, int64_t version, int64_t write_nonce, int64_t num_children, struct ms_client_multi_result* results ) {
   
   int rc = 0;
   struct md_download_config dlconf;
   struct md_entry* ents = NULL;
   
   md_download_config_init( &dlconf );
   
   struct ms_client_listdir_context ctx;
   memset( &ctx, 0, sizeof(struct ms_client_listdir_context) );
   
   ms_client_listdir_context_init( &ctx, client, parent_id, version, write_nonce, num_children );
   
   // setup downloads 
   md_download_config_set_curl_generator( &dlconf, ms_client_listdir_curl_generator, &ctx );
   md_download_config_set_url_generator( &dlconf, ms_client_listdir_generate_batch_url, &ctx );
   md_download_config_set_postprocessor( &dlconf, ms_client_listdir_download_postprocess, &ctx );
   md_download_config_set_canceller( &dlconf, ms_client_listdir_download_cancel, &ctx );
   md_download_config_set_limits( &dlconf, client->max_connections, -1 );
   
   // run downloads 
   rc = md_download_all( &client->dl, client->conf, &dlconf );
   
   if( ctx.children->size() > 0 ) {
      
      ents = CALLOC_LIST( struct md_entry, ctx.children->size() );
      
      std::copy( ctx.children->begin(), ctx.children->end(), ents );
      
      results->ents = ents;
   }
   
   results->num_ents = ctx.children->size();
   results->num_processed = ctx.children->size();
   
   ctx.children->clear();
   delete ctx.children;
   ctx.children = NULL;
   
   if( rc == 0 ) {
      // success! convert to multi result
      results->reply_error = 0;
   }
   else {
      results->reply_error = ctx.listing_error;
      
      errorf("md_download_all rc = %d\n", rc );
   }
   
   ms_client_listdir_context_free( &ctx );
   return rc;
}
