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

// getattr/getchild attempt count 
// return 0 if we can retry
// return -ENODATA if we can't retry 
// the ctx must be locked
static int ms_client_getattr_retry( struct ms_client_getattr_context* ctx, int i ) {
   
   int num_attempts = 0;
   int rc = 0;
   
   try {
      ctx->attempts[i]++;
      num_attempts = ctx->attempts[i];
      
      if( num_attempts > ctx->client->conf->max_metadata_read_retry ) {
         // don't try again
         rc = -ENODATA;
      }
      else {
         // re-enqueue 
         ctx->to_download->push_back( i );
      }
   }
   catch( bad_alloc& ba ) {
      rc = -ENOMEM;
   }
   
   return rc;
}


// getchild/getattr curl init'er 
// return the curl handle on success
// return NULL on OOM
static CURL* ms_client_getattr_curl_generator( void* cls ) {
   
   struct ms_client_getattr_context* ctx = (struct ms_client_getattr_context*)cls;
   
   // TODO: connection pool 
   CURL* curl = NULL;
   
   curl = curl_easy_init();
   
   if( curl != NULL ) {
      ms_client_init_curl_handle( ctx->client, curl, NULL );
   }
   
   return curl;
}


// getattr download URL generator 
// return the new URL on success
// return NULL if we're out of URLs, or OOM
static char* ms_client_getattr_url_generator( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_getattr_context* ctx = (struct ms_client_getattr_context*)cls;
   char* url = NULL;
   struct ms_path_ent* path_ent = NULL;
   
   if( ctx->to_download->size() == 0 ) {
      return NULL;
   }
   
   int i = ctx->to_download->at(0);
   
   pthread_mutex_lock( &ctx->lock );
   
   if( ctx->attempts[i] > ctx->client->conf->max_metadata_read_retry ) {
      // don't try again 
      pthread_mutex_unlock( &ctx->lock );
      return NULL;
   }
   
   // next element 

   try {
      path_ent = &ctx->path->at(i);
      
      url = ms_client_file_getattr_url( ctx->client->url, path_ent->volume_id, path_ent->file_id, path_ent->version, path_ent->write_nonce );
      
      (*ctx->downloading)[ dlctx ] = i;
      
      ctx->to_download->erase( ctx->to_download->begin() );
      
      if( url != NULL ) {
         SG_debug("GETATTR %p = %d, url %s\n", dlctx, i, url );
      }
   }
   catch( bad_alloc& ba ) {
      // OOM 
      SG_safe_free( url );
   }
   
   pthread_mutex_unlock( &ctx->lock );
   return url;
}


// getchild download URL generator 
// return the URL on success
// return NULL if we're out of URLs, or OOM
static char* ms_client_getchild_url_generator( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_getattr_context* ctx = (struct ms_client_getattr_context*)cls;
   char* url = NULL;
   struct ms_path_ent* path_ent = NULL;
   
   if( ctx->to_download->size() == 0 ) {
      return NULL;
   }
   
   int i = ctx->to_download->at(0);
   
   pthread_mutex_lock( &ctx->lock );
   
   if( ctx->attempts[i] > ctx->client->conf->max_metadata_read_retry ) {
      // don't try again 
      pthread_mutex_unlock( &ctx->lock );
      return NULL;
   }
   
   try {
      // next element 
      path_ent = &ctx->path->at(i);
      
      url = ms_client_file_getchild_url( ctx->client->url, path_ent->volume_id, path_ent->parent_id, path_ent->name );
      
      (*ctx->downloading)[ dlctx ] = i;
      
      ctx->to_download->erase( ctx->to_download->begin() );
      
      if( url != NULL ) {
         SG_debug("GETCHILD %p = %d, url %s\n", dlctx, i, url );
      }
   }
   catch( bad_alloc& ba ) {
      // OOM 
      SG_safe_free( url );      
   }
   
   pthread_mutex_unlock( &ctx->lock );
   return url;
}


// getattr/getchild post-cancel processor 
// always succeeds
static int ms_client_getattr_download_cancel( struct md_download_context* dlctx, void* cls ) {
   struct ms_client_getattr_context* ctx = (struct ms_client_getattr_context*)cls;
   
   pthread_mutex_lock( &ctx->lock );
   
   // no longer downloading
   ctx->downloading->erase( dlctx );
   
   pthread_mutex_unlock( &ctx->lock );
   
   return 0;
}


// getattr/getchild post-download processor  
// return 0 on success
// return -EINVAL if we couldn't find out what download just finished (shouldn't happen)
// return -ENODATA if we've failed to download this entry too many times 
// return -ENOMEM on OOM
// return negative on other download failure
static int ms_client_getattr_download_postprocess( struct md_download_context* dlctx, void* cls ) {
   
   struct ms_client_getattr_context* ctx = (struct ms_client_getattr_context*)cls;
   
   int rc = 0;
   int ent_idx = 0;
   int listing_error = 0;
   struct md_entry ent;
   
   pthread_mutex_lock( &ctx->lock );
   
   // find the path ent 
   ms_client_getattr_downloading_set::iterator itr = ctx->downloading->find( dlctx );
   if( itr == ctx->downloading->end() ) {
      // bug 
      SG_error("BUG: no path entry for %p\n", dlctx );
      
      pthread_mutex_unlock( &ctx->lock );
      return -EINVAL;
   }
   else {
      SG_debug("Match %p = %d\n", dlctx, itr->second );
   }
   
   ent_idx = itr->second;
   
   // no longer downloading
   ctx->downloading->erase( itr );
   
   // download status?
   rc = ms_client_download_parse_errors( dlctx );
   
   if( rc == -EAGAIN ) {
      
      // try again 
      rc = ms_client_getattr_retry( ctx, ent_idx );
      
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
   
   // success!
   memset( &ent, 0, sizeof(struct md_entry) );
   
   // get the entry
   rc = ms_client_listing_read_entry( ctx->client, dlctx, &ent, &listing_error );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_listing_read_entry(%p) rc = %d, listing_error = %d\n", dlctx, rc, listing_error);
      
      ctx->listing_error = listing_error;
      pthread_mutex_unlock( &ctx->lock );
      return rc;
   }
   
   if( listing_error == MS_LISTING_NONE ) {
      
      SG_error("WARN: no data for %" PRIX64 "\n", ctx->path->at(ent_idx).file_id );
      
      ctx->results_buf[ent_idx].error = MS_LISTING_NONE;
   }
   else if( listing_error == MS_LISTING_NOCHANGE ) {
      
      SG_debug("WARN: no change in %" PRIX64 "\n", ctx->path->at(ent_idx).file_id );
      
      ctx->results_buf[ent_idx].error = MS_LISTING_NOCHANGE;
   }
   else if( listing_error == MS_LISTING_NEW ) {
      
      // got data! store it to the results buffer
      ctx->results_buf[ ent_idx ] = ent;
      ctx->results_buf[ ent_idx ].error = MS_LISTING_NEW;
   }
   else {
      
      SG_error("ms_client_listing_read_entry(%p): Unknown listing error %d\n", dlctx, listing_error );
      ctx->listing_error = listing_error;
      
      pthread_mutex_unlock( &ctx->lock );
      return -EBADMSG;
   }
   
   ctx->num_downloaded++;
   
   // succeeded!
   pthread_mutex_unlock( &ctx->lock );
   return rc;
}


// free a getattr/getchild context 
// always succeeds
static int ms_client_getattr_context_free( struct ms_client_getattr_context* ctx ) {
   
   SG_safe_delete( ctx->to_download );
   SG_safe_delete( ctx->downloading );
   SG_safe_free( ctx->attempts );
   
   pthread_mutex_destroy( &ctx->lock );
   
   memset( ctx, 0, sizeof(struct ms_client_getattr_context) );
   
   return 0;
}


// set up a getattr/getchild context 
// NOTE: there must be one entry in result_buf for each entry in path 
// return 0 on success
// return -ENOMEM on OOM
static int ms_client_getattr_context_init( struct ms_client_getattr_context* ctx, struct ms_client* client, ms_path_t* path, struct md_entry* result_buf ) {
   
   memset( ctx, 0, sizeof(struct ms_client_getattr_context) );
   
   int rc = pthread_mutex_init( &ctx->lock, NULL );
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   ctx->to_download = new (nothrow) vector<int>();
   ctx->attempts = SG_CALLOC( int, path->size() );
   ctx->downloading = new (nothrow) ms_client_getattr_downloading_set();
   
   if( ctx->to_download == NULL || ctx->attempts == NULL || ctx->downloading == NULL ) {
      
      ms_client_getattr_context_free( ctx );
      return -ENOMEM;
   }
   
   try {
      for( unsigned int i = 0; i < path->size(); i++ ) {
         
         // schedule each entry to be downloaded 
         ctx->to_download->push_back( i );
      }
   }
   catch( bad_alloc& ba ) {
      
      ms_client_getattr_context_free( ctx );
      return -ENOMEM;
   }
   
   ctx->client = client;
   ctx->path = path;
   ctx->results_buf = result_buf;
   
   
   return 0;
}


// download metadata for a set of entries. 
// do a single download if run_single is true; otherwise, use the multi-download interface
// return partial results, even on error.
// NOTE: path[i].file_id, .volume_id, .version, and .write_nonce must be defined for each entry
// return 0 on success, with result populated 
// return -ENOMEM on OOM
// return negative on error, with result partially populated
static int ms_client_getattr_lowlevel( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result, bool run_single ) {
   
   int rc = 0;
   struct md_download_config dlconf;
   md_download_config_init( &dlconf );
   
   memset( result, 0, sizeof(struct ms_client_multi_result) );
   
   struct ms_client_getattr_context ctx;
   memset( &ctx, 0, sizeof(struct ms_client_getattr_context) );
   
   // make result buffer (to be stuffed into result)
   struct md_entry* result_ents = SG_CALLOC( struct md_entry, path->size() );
   if( result_ents == NULL ) {
      return -ENOMEM;
   }
   
   ms_client_getattr_context_init( &ctx, client, path, result_ents );
   
   // setup downloads 
   md_download_config_set_curl_generator( &dlconf, ms_client_getattr_curl_generator, &ctx );
   md_download_config_set_url_generator( &dlconf, ms_client_getattr_url_generator, &ctx );
   md_download_config_set_postprocessor( &dlconf, ms_client_getattr_download_postprocess, &ctx );
   md_download_config_set_canceller( &dlconf, ms_client_getattr_download_cancel, &ctx );
   md_download_config_set_limits( &dlconf, path->size(), -1 );
   
   if( run_single ) {
      rc = md_download_single( client->conf, &dlconf );
   }
   else {
      rc = md_download_all( &client->dl, client->conf, &dlconf );
   }
   
   result->ents = result_ents;
   result->num_ents = path->size();
   result->num_processed = ctx.num_downloaded;
   
   if( rc == 0 ) {
      // success! convert to multi result
      result->reply_error = 0;
   }
   else {
      result->reply_error = ctx.listing_error;
      
      SG_error("md_download_all rc = %d\n", rc );
   }
   
   ms_client_getattr_context_free( &ctx );
   
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
   return ms_client_getattr_lowlevel( client, path, result, false );
}

// download metadata for a single entry 
// ms_ent needs:
// * file_id 
// * volume_id 
// * version 
// * write_nonce 
// return 0 on success 
// return negative on error
int ms_client_getattr( struct ms_client* client, struct ms_path_ent* ms_ent, struct ms_client_multi_result* result ) {
   
   ms_path_t path;
   path.push_back( *ms_ent );
   
   return ms_client_getattr_lowlevel( client, &path, result, true );
}


// download metadata for a set of entries. 
// do a single download if single is true; otherwise do a multi-download
// return partial results, even on error.
// NOTE: path[i].parent_id, .volume_id, and .name must be defined for each entry
// return 0 on success, and populate result 
// return -ENOMEM on OOM
// return negative on error, partially populating result 
static int ms_client_getchild_lowlevel( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result, bool single ) {
   
   int rc = 0;
   struct md_download_config dlconf;
   char const* method = NULL;
   md_download_config_init( &dlconf );
   
   struct ms_client_getattr_context ctx;
   memset( &ctx, 0, sizeof(struct ms_client_getattr_context) );
   
   // make result buffer (to be stuffed into result)
   struct md_entry* result_ents = SG_CALLOC( struct md_entry, path->size() );
   
   if( result_ents == NULL ) {
      return -ENOMEM;
   }
   
   ms_client_getattr_context_init( &ctx, client, path, result_ents );
   
   // setup downloads 
   md_download_config_set_curl_generator( &dlconf, ms_client_getattr_curl_generator, &ctx );
   md_download_config_set_url_generator( &dlconf, ms_client_getchild_url_generator, &ctx );
   md_download_config_set_postprocessor( &dlconf, ms_client_getattr_download_postprocess, &ctx );
   md_download_config_set_canceller( &dlconf, ms_client_getattr_download_cancel, &ctx );
   md_download_config_set_limits( &dlconf, path->size(), -1 );
   
   if( single ) {
      method = "md_download_single";
      rc = md_download_single( client->conf, &dlconf );
   }
   else {
      method = "md_download_all";
      rc = md_download_all( &client->dl, client->conf, &dlconf );
   }
   
   result->ents = result_ents;
   result->num_ents = path->size();
   result->num_processed = ctx.num_downloaded;
   
   if( rc == 0 ) {
      // success! convert to multi result
      result->reply_error = 0;
   }
   else {
      result->reply_error = ctx.listing_error;
      
      SG_error("%s rc = %d\n", method, rc );
   }
   
   ms_client_getattr_context_free( &ctx );
   
   return rc;
}


// download multiple entries at once
// result->ents will be in the same order as the entries in path.
// retur 0 on success 
// return negative on error
int ms_client_getchild_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result ) {
   return ms_client_getchild_lowlevel( client, path, result, false );
}

// download metadata for a single entry 
// ms_ent needs:
// * file_id 
// * volume_id 
// * version 
// * write_nonce 
// return 0 on success 
// return negative on error
int ms_client_getchild( struct ms_client* client, struct ms_path_ent* ms_ent, struct ms_client_multi_result* result ) {
   
   ms_path_t path;
   path.push_back( *ms_ent );
   
   return ms_client_getchild_lowlevel( client, &path, result, true );
}


