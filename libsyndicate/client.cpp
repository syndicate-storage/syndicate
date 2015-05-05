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

#include "libsyndicate/client.h"
#include "libsyndicate/server.h"

#include "libsyndicate/ms/gateway.h"
#include "libsyndicate/url.h"
#include "libsyndicate/ms/url.h"

static char const* SG_post_field_data = SG_SERVER_POST_FIELD_DATA_PLANE;
static char const* SG_post_field_control = SG_SERVER_POST_FIELD_CONTROL_PLANE;


// per-request state to be preserved for running multiple requests 
struct SG_client_request_cls {
   
   uint64_t chunk_id;                   // ID of the chunk we're transfering
   SG_messages::Request* message;       // the original control-plane message (if uploading)
   char* serialized_message;            // serialized control-plane message (if uploading)
   struct curl_httppost* form_begin;    // curl forms (if uploading)
   char* url;                           // target URL 
   
   void* cls;                           // user-given download state
};


// free a request cls.  always succeeds 
void SG_client_request_cls_free( struct SG_client_request_cls* cls ) {
   
   SG_safe_free( cls->url );
   SG_safe_free( cls->serialized_message );
   
   if( cls->form_begin != NULL ) {
      
      curl_formfree( cls->form_begin );
      cls->form_begin = NULL;
   }
}


// download a manifest (from the caches) using an initialized curl handle.  verify it came from remote_gateway_id and parse it.
// return 0 on success
// return -ENOMEM on OOM
// return -EAGAIN if the remote gateway is not known to us (i.e. we can't make a manifest url, and we should reload)
// return -EINVAL if we failed to parse the message 
// return -ETIMEDOUT if the request timed out 
// return -EREMOTEIO if the request failed with HTTP 500 or higher
// return between -499 and -400 if the request failed with an HTTP 400-level error code
// return -errno on socket- and recv-related errors
// NOTE: does *not* check if the manifest came from a different gateway than the one given here (remote_gateway_id)
static int SG_client_get_manifest_curl( struct ms_client* ms, CURL* curl, uint64_t remote_gateway_id, struct SG_manifest* manifest ) {
   
   int rc = 0;
   char* serialized_manifest = NULL;
   off_t serialized_manifest_len = 0;
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   SG_messages::Manifest mmsg;
   
   // download!
   rc = md_download_run( curl, SG_MAX_MANIFEST_LEN, &serialized_manifest, &serialized_manifest_len );
   if( rc != 0 ) {
      
      // download failed 
      SG_error("md_download_run rc = %d\n", rc );
      
      return rc;
   }
   
   // parse 
   rc = md_parse< SG_messages::Manifest >( &mmsg, serialized_manifest, serialized_manifest_len );
   SG_safe_free( serialized_manifest );
   
   if( rc != 0 ) {
      
      // failed to parse 
      SG_error("md_parse rc = %d\n", rc );
      
      return rc;
   }
   
   // is this message from that gateway?
   rc = ms_client_verify_gateway_message< SG_messages::Manifest >( ms, volume_id, remote_gateway_id, &mmsg );
   if( rc != 0 ) {
      
      SG_error("ms_client_verify_gateway_message( from=%" PRIu64 " ) rc = %d\n", remote_gateway_id, rc );
      
      return rc;
   }
   
   // deserialize 
   rc = SG_manifest_load_from_protobuf( manifest, &mmsg );
   if( rc != 0 ) {
      
      SG_error("SG_manifest_load_from_protobuf rc = %d\n", rc );
   }
   
   return rc;
}

// download a manifest (from the caches) from remote_gateway_id; verify it came from remote_gateway_id; parse it
// return 0 on success, and popuilate *manifest 
// return -ENOMEM on OOM
// return -EINVAL if reqdat doesn't refer to a manifest
// return -EINVAL if we failed to parse the message 
// return -EBADMSG if the manifest timestamp, volume, file version, or file ID doesn't match the received manifest
// return -EAGAIN if the remote gateway is not known to us (i.e. we can't make a manifest url, and we should reload)
// return -ETIMEDOUT if the request timed out 
// return -EREMOTEIO if the request failed with HTTP 500 or higher
// return between -499 and -400 if the request failed with an HTTP 400-level error code
// return -errno on socket- and recv-related errors
// return non-zero if the gateway's closure method to connect to the cache fails
// NOTE: does *not* check if the manifest came from a different gateway than the one given here (remote_gateway_id)
int SG_client_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_manifest* manifest ) {
   
   int rc = 0;
   char* manifest_url = NULL;
   CURL* curl = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   uint64_t remote_gateway_type = 0;
   
   SG_messages::Manifest mmsg;
   
   // sanity check 
   if( !SG_request_is_manifest( reqdat ) ) {
      
      return -EINVAL;
   }
   
   // sanity check--do we know of this gateway?
   remote_gateway_type = ms_client_get_gateway_type( ms, remote_gateway_id );
   if( remote_gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      // not present 
      SG_error("ms_client_get_gateway_type( %" PRIu64 " ) rc = -1\n", remote_gateway_id );
      
      // caller can reload and try again
      return -EAGAIN;
   }
   
   // generate URL 
   rc = md_url_make_manifest_url( ms, reqdat->fs_path, remote_gateway_id, reqdat->file_id, reqdat->file_version, &reqdat->manifest_timestamp, &manifest_url );
   if( rc != 0 ) {
      
      if( rc == -ENOENT ) {
         
         // gateway not found. 
         // caller can try to reload the cert bundle, if desired 
         rc = -EAGAIN;
      }
      
      return rc;
   }
   
   // TODO: connection pool 
   curl = curl_easy_init();
   
   if( curl == NULL ) {
      
      SG_safe_free( manifest_url );
      return -ENOMEM;
   }
   
   // set CURL url, just in case
   md_init_curl_handle( conf, curl, manifest_url, conf->connect_timeout );
   
   // connect to caches 
   rc = SG_gateway_closure_connect_cache( gateway, curl, manifest_url );
   
   if( rc != 0 ) {
      
      // failed 
      SG_error("SG_gateway_closure_connect_cache('%s') rc = %d\n", manifest_url, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( manifest_url );
      
      return rc;
   }
   
   rc = SG_client_get_manifest_curl( ms, curl, remote_gateway_id, manifest );
   if( rc != 0 ) {
      
      // failed 
      SG_error("SG_client_get_manifest_curl( '%s' ) rc = %d\n", manifest_url, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( manifest_url );
   
      return rc;
   }
   
   // is it the one we requested?
   if( SG_manifest_get_volume_id( manifest ) != volume_id ||
       SG_manifest_get_file_id( manifest ) != reqdat->file_id || 
       SG_manifest_get_file_version( manifest ) != reqdat->file_version || 
       SG_manifest_get_modtime_sec( manifest ) != reqdat->manifest_timestamp.tv_sec ||
       SG_manifest_get_modtime_nsec( manifest ) != reqdat->manifest_timestamp.tv_nsec ) {
      
      // failed 
      SG_error("manifest '%s' mismatch: expected volume=%" PRIu64 " file=%" PRIX64 ".%" PRId64 " timestamp=%ld.%ld, but got volume=%" PRIu64 " file=%" PRIX64 ".%" PRId64 " timestamp=%ld.%ld\n",
               reqdat->fs_path, volume_id, reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec,
               SG_manifest_get_volume_id( manifest ), SG_manifest_get_file_id( manifest ), SG_manifest_get_file_version( manifest ), (long)SG_manifest_get_modtime_sec( manifest ), (long)SG_manifest_get_modtime_nsec( manifest ) );
      
      curl_easy_cleanup( curl );
      SG_safe_free( manifest_url );
   
      return -EBADMSG;
   }
   
   curl_easy_cleanup( curl );
   SG_safe_free( manifest_url );
   
   return rc;
}


// set up and start a download context used for transferring data asynchronously 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_download_async_start( struct SG_gateway* gateway, struct md_download_loop* dlloop, struct md_download_context* dlctx, uint64_t chunk_id, char* url, off_t max_size, void* cls ) {
   
   int rc = 0;
   CURL* curl = NULL;
   uint64_t block_size = 0;
   
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   struct ms_client* ms = SG_gateway_ms( gateway );
   block_size = ms_client_get_volume_blocksize( ms );
   
   struct SG_client_request_cls* reqcls = SG_CALLOC( struct SG_client_request_cls, 1 );
   if( reqcls == NULL ) {
      
      return -ENOMEM;
   }
   
   // TODO: connection pool 
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( reqcls );
      return -ENOMEM;
   }
   
   md_init_curl_handle( conf, curl, url, conf->connect_timeout );
   
   // connect to caches 
   rc = SG_gateway_closure_connect_cache( gateway, curl, url );
   
   if( rc != 0 ) {
      
      // failed 
      SG_error("SG_gateway_closure_connect_cache('%s') rc = %d\n", url, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // set up download state 
   reqcls->url = url;
   reqcls->chunk_id = chunk_id;
   reqcls->cls = cls;
   
   // set up 
   rc = md_download_context_init( dlctx, curl, block_size * SG_MAX_BLOCK_LEN_MULTIPLER, reqcls );
   if( rc != 0 ) {
      
      // failed 
      SG_error("md_download_init('%s') rc = %d\n", url, rc );
      
      curl_easy_cleanup( curl );
      
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // reference it, to keep it around 
   md_download_context_ref( dlctx );
   
   // watch it 
   rc = md_download_loop_watch( dlloop, dlctx );
   if( rc != 0 ) {
      
      // failed 
      SG_error("md_download_loop_watch rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // start 
   rc = md_download_context_start( gateway->dl, dlctx );
   if( rc != 0 ) {
      
      // failed 
      SG_error("md_download_context_start('%s') rc = %d\n", url, rc );
      
      md_download_context_free( dlctx, NULL );
      
      // TODO: connection pool
      curl_easy_cleanup( curl );
      
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // started!
   return 0;
}


// clean up a download context used for transfering data asynchronously, including the associated state
// TODO: release the curl handle into the connection pool 
void SG_client_download_async_cleanup( struct md_download_context* dlctx ) {
   
   CURL* curl = NULL;
   
   struct SG_client_request_cls* reqcls = (struct SG_client_request_cls*)md_download_context_get_cls( dlctx );
   
   // clean up
   int free_rc = md_download_context_unref( dlctx );
   if( free_rc > 0 ) {
      
      SG_debug("Will free download context %p\n", dlctx );
      md_download_context_free( dlctx, &curl );
      
      if( curl != NULL ) {
         // TODO: connection pool 
         curl_easy_cleanup( curl );
      }
   }
   else {
      
      // shouldn't get here...
      SG_warn( "Download %p not fully unrefrenced\n", dlctx );
      
      // strip the request cls
      md_download_context_set_cls( dlctx, NULL );
   }
   
   SG_client_request_cls_free( reqcls );
   SG_safe_free( reqcls );
   
   return;
}


// clean up the state for each download in an (aborted) download loop
// NOTE: only use this in conjunction with SG_client_download_async_start
// always succeeds 
void SG_client_download_async_cleanup_loop( struct md_download_loop* dlloop ) {

   struct md_download_context* dlctx = NULL;
   int i = 0;
   
   // free all ms_client_get_metadata_context
   for( dlctx = md_download_loop_next_initialized( dlloop, &i ); dlctx != NULL; dlctx = md_download_loop_next_initialized( dlloop, &i ) ) {
      
      if( dlctx == NULL ) {
         break;
      }
      
      SG_client_download_async_cleanup( dlctx );
   }
}


// wait for a download to finish, get the buffer, and free the download handle
// return 0 on success 
// return -ENODATA if the download did not suceeed with HTTP 200
// return -errno if we failed to wait for the download, somehow 
// return -ENOMEM on OOM
int SG_client_download_async_wait( struct md_download_context* dlctx, uint64_t* chunk_id, char** chunk_buf, off_t* chunk_len, void** cls ) {
   
   int rc = 0;
   int http_status = 0;
   struct SG_client_request_cls* reqcls = (struct SG_client_request_cls*)md_download_context_get_cls( dlctx );
   
   // are we ready?
   if( !md_download_context_finalized( dlctx ) ) {
      
      // wait for it...
      rc = md_download_context_wait( dlctx, -1 );
      if( rc != 0 ) {
         
         SG_error("md_download_context_wait( %p ) rc = %d\n", dlctx, rc );
         
         SG_client_download_async_cleanup( dlctx );
         return rc;
      }
   }
   
   // do we even have data?
   if( !md_download_context_succeeded( dlctx, 200 ) ) {
      
      http_status = md_download_context_get_http_status( dlctx );
      
      SG_error("download %p finished with HTTP status %d\n", dlctx, http_status );
      
      SG_client_download_async_cleanup( dlctx );
      return -ENODATA;
   }
   
   // get the chunk from the download context 
   rc = md_download_context_get_buffer( dlctx, chunk_buf, chunk_len );
   if( rc != 0 ) {
      
      // OOM 
      SG_client_download_async_cleanup( dlctx );
      return rc;
   }
   
   // get the chunk ID from the download's closure
   *chunk_id = reqcls->chunk_id;
   
   if( cls != NULL ) {
      *cls = reqcls->cls;
   }
   
   // done!
   SG_client_download_async_cleanup( dlctx );
   
   return rc;
}


// begin downloading a block 
// NOTE: reqdat must be a block request
// return 0 on success, and set up *dlctx to refer to the downloading context
// return -ENOMEM if OOM
// return -ENOMEM if reqdat isn't a block request
// return -ENOENT if the remote gateway cannot be looked up 
int SG_client_get_block_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   int rc = 0;
   char* block_url = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   struct SG_request_data* reqdat_dup = NULL;
   
   // sanity check 
   if( !SG_request_is_block( reqdat ) ) {
      
      return -EINVAL;
   }
   
   // get block url 
   rc = md_url_make_block_url( ms, reqdat->fs_path, remote_gateway_id, reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, &block_url );
   if( rc != 0 ) {
      
      // failed to create block url 
      return rc;
   }
   
   // duplicate request data--we'll need it for SG_client_get_block_finish
   reqdat_dup = SG_CALLOC( struct SG_request_data, 1 );
   if( reqdat_dup == NULL ) {
      
      SG_safe_free( block_url );
      return -ENOMEM;
   }
   
   rc = SG_request_data_dup( reqdat_dup, reqdat );
   if( rc != 0 ) {
      
      SG_safe_free( block_url );
      SG_safe_free( reqdat_dup );
      return rc;
   }
   
   // GOGOO!
   rc = SG_client_download_async_start( gateway, dlloop, dlctx, reqdat->block_id, block_url, block_size * SG_MAX_BLOCK_LEN_MULTIPLER, reqdat_dup );
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_start('%s') rc = %d\n", block_url, rc );
      SG_safe_free( block_url );
      return rc;
   }
   
   return rc;
}



// log a block hash mismatch 
// always succeeds
static void SG_client_get_block_log_hash_mismatch( struct SG_manifest* manifest, uint64_t block_id, unsigned char* block_hash ) {

   // log it (takes a bit of effort to convert the hashes to printable strings...)
   unsigned char* expected_block_hash = NULL;
   size_t expected_block_hash_len = 0;
   
   char* expected_block_hash_str = NULL;
   char* actual_block_hash_str = NULL;
   
   bool logged = false;
   int rc = 0;
   
   rc = SG_manifest_get_block_hash( manifest, block_id, &expected_block_hash, &expected_block_hash_len );
   if( rc == 0 ) {
      
      expected_block_hash_str = md_data_printable( expected_block_hash, expected_block_hash_len );
      actual_block_hash_str = md_data_printable( block_hash, SG_BLOCK_HASH_LEN );
      
      SG_safe_free( expected_block_hash );
      
      if( expected_block_hash_str != NULL && actual_block_hash_str != NULL ) {
         
         SG_error("SG_manifest_block_hash_eq(%" PRIu64 "): expected '%s', got '%s'\n", block_id, expected_block_hash_str, actual_block_hash_str );
         
         logged = true;
      }
      
      SG_safe_free( expected_block_hash_str );
      SG_safe_free( actual_block_hash_str );
   }
   
   if( !logged ) {
      SG_error("SG_manifest_block_hash_eq(%" PRIu64 "): check failed\n", block_id );
   }
   
   return;
}
   


// parse a block from a download context, and use the manifest to verify it's integrity 
// if the block is still downloading, wait for it to finish (indefinitely). Otherwise, load right away.
// return 0 on success, and populate *block with its contents 
// return -ENOMEM on OOM 
// return -EINVAL if the request is not for a block
// return -ENODATA if the download context did not successfully finish
// return -EBADMSG if the block's authenticity could not be verified with the manifest
int SG_client_get_block_finish( struct SG_gateway* gateway, struct SG_manifest* manifest, struct md_download_context* dlctx, uint64_t* block_id, struct SG_chunk* block ) {
   
   int rc = 0;
   char* serialized_block_buf = NULL;
   off_t serialized_block_len = 0;
   
   struct SG_chunk serialized_block;
   
   char* block_buf = NULL;
   ssize_t block_len = 0;
   
   unsigned char* block_hash = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   struct SG_request_data* reqdat = NULL;
   
   // get the data; recover the original reqdat
   rc = SG_client_download_async_wait( dlctx, block_id, &serialized_block_buf, &serialized_block_len, (void**)&reqdat );
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_wait( %p ) rc = %d\n", dlctx, rc );
      
      return rc;
   }
   
   // get the hash 
   block_hash = sha256_hash_data( serialized_block_buf, serialized_block_len );
   if( block_hash == NULL ) {
      
      // OOM 
      SG_safe_free( serialized_block_buf );      
      
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      
      return -ENOMEM;
   }
   
   // compare to the hash in the manifest, verifying that it is actually present at the same time.
   rc = SG_manifest_block_hash_eq( manifest, *block_id, block_hash, SG_BLOCK_HASH_LEN );
   if( rc < 0 ) {
      
      // error 
      SG_error("SG_manifest_block_hash_eq( %" PRIu64 " ) rc = %d\n", *block_id, rc );
      
      SG_safe_free( serialized_block_buf );
      SG_safe_free( block_hash );
      
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      
      return rc;
   }
   else if( rc == 0 ) {
      
      // mismatch 
      SG_client_get_block_log_hash_mismatch( manifest, *block_id, block_hash );
      
      SG_safe_free( serialized_block_buf );
      SG_safe_free( block_hash );
      
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      
      return -EBADMSG;
   }
   
   SG_safe_free( block_hash );
   
   // hash matches!  run it through the driver 
   SG_chunk_init( &serialized_block, serialized_block_buf, serialized_block_len );
   
   block_buf = SG_CALLOC( char, block_size );
   if( block_buf == NULL ) {
      
      SG_chunk_free( &serialized_block );
      
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      return -ENOMEM;
   }
   
   block_len = block_size;
   
   // set up the actual block 
   SG_chunk_init( block, block_buf, block_len );
   
   // unserialize
   block_len = SG_gateway_closure_get_block( gateway, reqdat, &serialized_block, block );
   
   SG_chunk_free( &serialized_block );
   
   if( block_len < 0 || (unsigned)block_len != block_size ) {
      
      // failed 
      SG_error("SG_gateway_closure_get_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (size = %zu) ) rc = %zd\n", 
               reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, block->len, block_len );
      
      SG_chunk_free( block );
      
      if( block_len >= 0 && (unsigned)block_len != block_size ) {
         
         // not enough data given 
         rc = -ENODATA;
      }
   }
   else {
      
      rc = 0;
   }
   
   // done with this request
   SG_request_data_free( reqdat );
   SG_safe_free( reqdat );
   
   return rc;
}


// create a signed block 
// the wire format for which is:
// [ 0: 4 bytes $HEADER_SIZE ][ 4: $HEADER_SIZE block header ][ $HEADER_SIZE + 4: block data ]
// return 0 on success 
// return -ENOMEM on OOM
// return -EINVAL if reqdat isn't a block request, or if we failed to serialize and sign
int SG_client_serialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block_in, struct SG_chunk* block_out ) {
   
   int rc = 0;
   unsigned char* block_hash = NULL;
   char* serialized_header = NULL;
   size_t serialized_header_len = 0;
   
   char* block_buf = NULL;
   
   size_t total_length = 0;
   uint32_t header_len_htonl = 0;
   
   // sanity check 
   if( !SG_request_is_block( reqdat ) ) {
      
      return -EINVAL;
   }
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t gateway_id = SG_gateway_id( gateway );
   EVP_PKEY* gateway_private_key = SG_gateway_private_key( gateway );
   
   SG_messages::SignedBlockHeader hdr;
   
   hdr.set_volume_id( volume_id );
   hdr.set_file_id( reqdat->file_id );
   hdr.set_file_version( reqdat->file_version );
   hdr.set_block_id( reqdat->block_id );
   hdr.set_block_version( reqdat->block_version );
   hdr.set_gateway_id( gateway_id );
   
   block_hash = sha256_hash_data( block_in->data, block_in->len );
   if( block_hash == NULL ) {
      
      // OOM 
      return -ENOMEM;
   }
   
   try {
      
      hdr.set_block_hash( string( (char*)block_hash, SG_BLOCK_HASH_LEN ) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( block_hash );
      return -ENOMEM;
   }
   
   SG_safe_free( block_hash );
   
   // sign 
   rc = md_sign< SG_messages::SignedBlockHeader >( gateway_private_key, &hdr );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   // serialize 
   rc = md_serialize< SG_messages::SignedBlockHeader >( &hdr, &serialized_header, &serialized_header_len );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // calculate length and make the buffer 
   total_length = serialized_header_len + sizeof(uint32_t) + block_in->len;
   block_buf = SG_CALLOC( char, total_length );
   if( block_buf == NULL ) {
      
      // OOM 
      SG_safe_free( serialized_header );
      return -ENOMEM;
   }
   
   block_out->len = total_length;
   
   // make the buffer 
   header_len_htonl = htonl( serialized_header_len );
   
   memcpy( block_buf, &header_len_htonl, sizeof(uint32_t) );
   memcpy( block_buf + sizeof(uint32_t), serialized_header, serialized_header_len );
   memcpy( block_buf + sizeof(uint32_t) + serialized_header_len, block_in->data, block_in->len );
   
   SG_safe_free( serialized_header );
   
   block_out->data = block_buf;
   
   return 0;
}


// parse a signed block.
// return 0 on success, and put the block data into *block_out 
// return -EINVAL if the reqdat isn't a block request, or if we failed to deserialize due to a malformatted block_in 
// return -EBADMSG if the block didn't come from remote_gateway_id (i.e. the id or signature didn't match)
// return -ENOMEM if OOM
// return -EAGAIN if we don't know the gateway's public key (yet)
int SG_client_deserialize_signed_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t remote_gateway_id, struct SG_chunk* block_in, struct SG_chunk* block_out ) {

   int rc = 0;
   unsigned char* block_hash = NULL;
   
   uint32_t serialized_header_len = 0;
   char* serialized_header = NULL;
   char* serialized_block_buf = NULL;
   char* block_buf = NULL;

   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   SG_messages::SignedBlockHeader hdr;
   
   // must have at least 4 bytes in the block to get the size 
   memcpy( &serialized_header_len, block_in->data, sizeof(uint32_t) );
   
   serialized_header_len = ntohl( serialized_header_len );
   
   // header is variable-length but reasonably small...less than 4K for sure 
   if( serialized_header_len >= SG_messages::SignedBlockHeader::MAXIMUM_SIZE ) {
      
      return -EINVAL;
   }
   
   // whole block must fit...
   if( serialized_header_len + sizeof(uint32_t) + block_size >= (unsigned)block_in->len ) {
      
      return -EINVAL;
   }
   
   // safe to load
   serialized_header = block_in->data + sizeof(uint32_t);
   
   // load header
   rc = md_parse< SG_messages::SignedBlockHeader >( &hdr, serialized_header, serialized_header_len );
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      return rc;
   }
   
   // did it come from the expected origin?
   if( hdr.gateway_id() != remote_gateway_id ) {
      
      SG_error("Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", hdr.gateway_id(), remote_gateway_id );
      return -EBADMSG;
   }
   
   // verify header 
   rc = ms_client_verify_gateway_message< SG_messages::SignedBlockHeader >( ms, hdr.volume_id(), remote_gateway_id, &hdr );
   if( rc != 0 ) {
      
      SG_error("ms_client_verify_gateway_message( from=%" PRIu64 ") rc = %d\n", remote_gateway_id, rc );
      
      if( rc == -EINVAL ) {
         
         // hash mismatch 
         rc = -EBADMSG;
      }
      
      return rc;
   }
   
   // verify hash length 
   if( hdr.block_hash().size() != SG_BLOCK_HASH_LEN ) {
      
      SG_error("SignedBlockHeader hash length = %zu, expected %d\n", hdr.block_hash().size(), SG_BLOCK_HASH_LEN );
      return -EINVAL;
   }
   
   // start of data
   serialized_block_buf = block_in->data + sizeof(uint32_t) + serialized_header_len;
   
   // calculate data hash 
   block_hash = sha256_hash_data( serialized_block_buf, block_size );
   if( block_hash == NULL ) {
      
      // OOM 
      return -ENOMEM;
   }
   
   // verify hash 
   if( memcmp( block_hash, hdr.block_hash().data(), SG_BLOCK_HASH_LEN ) != 0 ) {
      
      // hash mismatch 
      SG_error("%" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] (%s): hash mismatch\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path );
      return -EBADMSG;
   }
   
   // hash is good; block is authentic 
   block_out->len = block_size;
   block_buf = SG_CALLOC( char, block_out->len );
   if( block_buf == NULL ) {
      
      // OOM 
      return -ENOMEM;
   }
   
   memcpy( block_buf, serialized_block_buf, block_out->len );
   block_out->data = block_buf;
   
   return 0;
}


// set up the common fields of a Request 
// return 0 on success 
// return -EINVAL if reqdat doesn't have file_id, file_version, coordinator_id, or fs_path set
// return -ENOMEM on OOM 
static int SG_client_request_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, uint64_t dest_gateway_id ) {
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t volume_version = ms_client_volume_version( ms );
   uint64_t cert_version = ms_client_cert_version( ms );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t gateway_id = SG_gateway_id( gateway );
   uint64_t user_id = SG_gateway_user_id( gateway );
   
   // sanity check...
   if( reqdat->coordinator_id == SG_INVALID_GATEWAY_ID || reqdat->file_id == SG_INVALID_FILE_ID || reqdat->fs_path == NULL ) {
      return -EINVAL;
   }
   
   try {
      
      request->set_volume_version( volume_version );
      request->set_cert_version( cert_version );
      request->set_volume_id( volume_id );
      request->set_coordinator_id( reqdat->coordinator_id );
      request->set_file_id( reqdat->file_id );
      request->set_file_version( reqdat->file_version );
      
      request->set_user_id( user_id );
      request->set_src_gateway_id( gateway_id );
      request->set_dest_gateway_id( dest_gateway_id );
      request->set_message_nonce( md_random64() );
      
      request->set_fs_path( string(reqdat->fs_path) );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}


// make a signed WRITE message--that is, send over new block information for a file, encoded as a manifest.
// the destination gateway is the coordinator ID in the manifest.
// write-delta must be non-NULL
// if new_owner and/or new_mode are non-NULL, they will be filled in as well
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if reqdat does not encode manifest data
int SG_client_request_WRITE_setup( struct SG_gateway* gateway, SG_messages::Request* request, char const* fs_path, struct SG_manifest* write_delta, uint64_t* new_owner, mode_t* new_mode, struct timespec* new_mtime ) {
   
   int rc = 0;
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   struct SG_request_data reqdat;
   memset( &reqdat, 0, sizeof(reqdat) );
   
   reqdat.coordinator_id = write_delta->coordinator_id;
   reqdat.fs_path = (char*)fs_path;
   reqdat.volume_id = write_delta->volume_id;
   reqdat.file_id = write_delta->file_id;
   reqdat.file_version = write_delta->file_version;
   
   rc = SG_client_request_setup( gateway, request, &reqdat, write_delta->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::WRITE );
   
   rc = SG_manifest_serialize_blocks_to_request_protobuf( write_delta, request );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_new_manifest_mtime_sec( write_delta->mtime_sec );
   request->set_new_manifest_mtime_nsec( write_delta->mtime_nsec );
   
   if( new_owner != NULL ) {
      request->set_new_owner_id( *new_owner );
   }
   
   if( new_mode != NULL ) {
      request->set_new_mode( *new_mode );
   }
   
   if( new_mtime != NULL ) {
      
      request->set_new_mtime_sec( new_mtime->tv_sec );
      request->set_new_mtime_nsec( new_mtime->tv_nsec );
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// make a signed TRUNCATE message
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_request_TRUNCATE_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, off_t new_size ) {
   
   int rc = 0;
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat, reqdat->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::TRUNCATE );
   request->set_new_size( new_size );
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// make a signed RENAME request 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_request_RENAME_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* new_path ) {
   
   int rc = 0;
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat, reqdat->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::RENAME );
   
   try {
      request->set_new_fs_path( string(new_path) );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// make a signed DETACH request, optionally with an MS-given vacuum ticket
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_request_DETACH_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, ms::ms_reply* vacuum_ticket ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat, reqdat->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::DETACH );
   
   // clone vcuum ticket, if given
   if( vacuum_ticket != NULL ) {
      
      ms::ms_reply* mutable_ticket = request->mutable_vacuum_ticket();
      mutable_ticket->CopyFrom( *vacuum_ticket );
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// make a signed PUTBLOCK request 
// return 0 on sucess 
// return -ENOMEM on OOM 
int SG_client_request_PUTBLOCK_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* block_info ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat, reqdat->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::PUTBLOCK );
   
   // add block information 
   SG_messages::ManifestBlock* mblock = NULL;
   
   try {
      
      mblock = request->add_blocks();
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   rc = SG_manifest_block_serialize_to_protobuf( block_info, mblock );
   if( rc != 0 ) {
      
      return rc;
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// make a signed DELETEBLOCK request, optionally with a vacuum ticket from the MS
// return 0 on sucess 
// return -ENOMEM on OOM 
int SG_client_request_DELETEBLOCK_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* block_info, ms::ms_reply* vacuum_ticket ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat, reqdat->coordinator_id );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::DELETEBLOCK );
   
   // clone vacuum ticket, if given
   if( vacuum_ticket != NULL ) {
      
      ms::ms_reply* mutable_ticket = request->mutable_vacuum_ticket();
      mutable_ticket->CopyFrom( *vacuum_ticket );
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// begin sending a request 
// serialize the given message, and set up a request cls.
// NOTE: the download takes ownership of control_plane--the caller should not manipulate it in any way while the download is proceeding
// return 0 on success, and set up *reqcls
// return -ENOMEM on OOM 
// return -EAGAIN if the destination gateway is not known to us, but could become known if we reloaded our volumeconfiguration
static int SG_client_request_begin( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, struct SG_client_request_cls* reqcls ) {
   
   int rc = 0;
   char* gateway_url = NULL;
   
   char* serialized_message = NULL;
   size_t serialized_message_len = 0;
   
   struct curl_httppost* form_begin = NULL;
   struct curl_httppost* form_end = NULL;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // look up gateway 
   rc = md_url_make_gateway_url( ms, dest_gateway_id, &gateway_url );
   if( rc != 0 ) {
      
      if( rc != -ENOMEM ) {
         
         // we don't know about this gateway.  try refreshing 
         rc = -EAGAIN;
      }
      return rc;
   }
   
   // serialize control plane
   rc = md_serialize< SG_messages::Request >( control_plane, &serialized_message, &serialized_message_len );
   if( rc != 0 ) {
      
      SG_safe_free( gateway_url );
      return rc;
   }
   
   // control-plane
   rc = curl_formadd( &form_begin, &form_end, CURLFORM_PTRNAME, SG_post_field_control,
                                              CURLFORM_CONTENTSLENGTH, serialized_message_len, 
                                              CURLFORM_PTRCONTENTS, serialized_message,
                                              CURLFORM_CONTENTTYPE, "application/octet-stream",
                                              CURLFORM_END );
   
   if( rc != 0 ) {
      
      SG_error("curl_formadd rc = %d\n", rc );
      
      SG_safe_free( gateway_url );
      SG_safe_free( serialized_message );
      
      return -ENOMEM;
   }
   
   // do we have a data plane?
   if( data_plane != NULL ) {
      
      rc = curl_formadd( &form_begin, &form_end, CURLFORM_PTRNAME, SG_post_field_data,
                                                 CURLFORM_CONTENTSLENGTH, data_plane->len,
                                                 CURLFORM_PTRCONTENTS, data_plane->data,
                                                 CURLFORM_CONTENTTYPE, "application/octet-stream",
                                                 CURLFORM_END );
      
      if( rc != 0 ) {
         
         SG_error("curl_formadd rc = %d\n", rc );
         
         curl_formfree( form_begin );
         SG_safe_free( gateway_url );
         SG_safe_free( serialized_message );
         
         return -ENOMEM;
      }
   }
   
   // success!
   memset( reqcls, 0, sizeof(struct SG_client_request_cls) );
   
   reqcls->url = gateway_url;
   reqcls->form_begin = form_begin;
   reqcls->serialized_message = serialized_message;
   reqcls->message = control_plane;
   
   return 0;
}


// finish processing a request 
// return 0 on success, and populate *reply
// return -EBADMSG if the reply could not be validated 
static int SG_client_request_end( struct SG_gateway* gateway, struct SG_chunk* serialized_reply, struct SG_client_request_cls* reqcls, SG_messages::Reply* reply ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   SG_messages::Request* control_plane = reqcls->message;
   
   // parse...
   rc = md_parse< SG_messages::Reply >( reply, serialized_reply->data, serialized_reply->len );
   
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      return rc;
   }
   
   // did it come from the request's destination?
   if( reply->gateway_id() != control_plane->dest_gateway_id() ) {
      
      SG_error("Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", control_plane->dest_gateway_id(), reply->gateway_id() );
      return -EBADMSG;
   }
   
   // verify message nonce 
   if( control_plane->message_nonce() != reply->message_nonce() ) {
      
      SG_error("Message nonce mismatch: expected %" PRIX64 ", got %" PRIX64 "\n", control_plane->message_nonce(), reply->message_nonce() );
   }
   
   // verify signature 
   rc = ms_client_verify_gateway_message< SG_messages::Reply >( ms, volume_id, control_plane->dest_gateway_id(), reply );
   if( rc != 0 ) {
      
      SG_error("ms_client_verify_gateway_message( from=%" PRIu64 " ) rc = %d\n", control_plane->dest_gateway_id(), rc );
      return -EBADMSG;
   }
   
   // done! 
   return 0;
}


// determine whether or not a call to SG_client_request_send or SG_client_request_send_finish indicates
// that the remote gateway is down.  That is, the error is one of the following:
// -EBADMSG, -ENODATA, -ETIMEDOUT, or between -400 and -499.
bool SG_client_request_is_remote_unavailable( int error ) {
   
   return (error == -EBADMSG || error == -ETIMEDOUT || (error >= -499 && error <= -400));
}

// send a message as a (control plane, data plane) pair, synchronously, to another gateway 
// return 0 on success 
// return -ENOMEM if OOM 
// return -EAGAIN if the request should be retried.  This could be because dest_gateway_id is not known to us, but could become known if we refreshed the volume config.
// return -ETIMEDOUT on connection timeout 
// return -EREMOTEIO if the HTTP status was >= 500 
// return between -499 and -400 if the request failed with an HTTP 400-level error code
// return -errno on socket- and recv-related errors
int SG_client_request_send( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, SG_messages::Reply* reply ) {
   
   int rc = 0;
   
   CURL* curl = NULL;
   
   struct SG_client_request_cls reqcls;
   struct SG_chunk serialized_reply;
   
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   // TODO: connection pool
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      return -ENOMEM;
   }
   
   rc = SG_client_request_begin( gateway, dest_gateway_id, control_plane, data_plane, &reqcls );
   if( rc != 0 ) {
      
      curl_easy_cleanup( curl );
      SG_error("SG_client_request_begin( %" PRIu64 " ) rc = %d\n", dest_gateway_id, rc );
      return rc;
   }
   
   // set up curl handle 
   md_init_curl_handle( conf, curl, reqcls.url, conf->connect_timeout );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, reqcls.form_begin );
   
   // run the transfer 
   rc = md_download_run( curl, SG_CLIENT_MAX_REPLY_LEN, &serialized_reply.data, &serialized_reply.len );
   
   if( rc != 0 ) {
      
      // failed 
      SG_error("md_download_run('%s') rc = %d\n", reqcls.url, rc );
      
      curl_easy_cleanup( curl );
      
      SG_client_request_cls_free( &reqcls );
      
      return rc;
   }
   
   // get the reply 
   rc = SG_client_request_end( gateway, &serialized_reply, &reqcls, reply );
   if( rc != 0 ) {
      
      // failed 
      SG_error("SG_client_request_end('%s') rc = %d\n", reqcls.url, rc );
   }
   
   curl_easy_cleanup( curl );
   SG_client_request_cls_free( &reqcls );
   SG_chunk_free( &serialized_reply );
   
   return rc;
}


// send a message asynchronously to another gateway 
// NOTE: the caller must NOT free *data_plane until freeing the download context!
// NOTE: the download context takes ownership of control_plane for the duration of the download!
// return 0 on success, and set up *dlctx as an upload future
// return -ENOMEM on OOM
int SG_client_request_send_async( struct SG_gateway* gateway, uint64_t dest_gateway_id, SG_messages::Request* control_plane, struct SG_chunk* data_plane, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   CURL* curl = NULL;
   struct md_downloader* dl = SG_gateway_dl( gateway );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   struct SG_client_request_cls* reqcls = SG_CALLOC( struct SG_client_request_cls, 1 );
   if( reqcls == NULL ) {
      
      return -ENOMEM;
   }
   
   // TODO: connection pool
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( reqcls );
      return -ENOMEM;
   }
   
   rc = SG_client_request_begin( gateway, dest_gateway_id, control_plane, data_plane, reqcls );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_begin( %" PRIu64 " ) rc = %d\n", dest_gateway_id, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( reqcls );
      return rc;
   }
   
   // set up curl handle 
   md_init_curl_handle( conf, curl, reqcls->url, conf->connect_timeout );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, reqcls->form_begin );
   
   // set up the download handle 
   rc = md_download_context_init( dlctx, curl, SG_CLIENT_MAX_REPLY_LEN, reqcls );
   if( rc != 0 ) {
      
      SG_error("md_download_context_init( %" PRIu64 " ) rc = %d\n", dest_gateway_id, rc );
      
      SG_client_request_cls_free( reqcls );
      SG_safe_free( reqcls );
      
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // keep this handle resident, even after finalization 
   md_download_context_ref( dlctx );
   
   // have the download loop watch this download 
   rc = md_download_loop_watch( dlloop, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_watch rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
      
      curl_easy_cleanup( curl );
      SG_client_request_cls_free( reqcls );
      SG_safe_free( reqcls );
   }
   
   // start the download 
   rc = md_download_context_start( dl, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_context_start( %" PRIu64 " ) rc = %d\n", dest_gateway_id, rc );
      
      md_download_context_free( dlctx, NULL );
      
      curl_easy_cleanup( curl );
      SG_client_request_cls_free( reqcls );
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   return 0;
}

// finish sending a message to another gateway
// return 0 on success, and set up *reply with the validated reply
// return -EINVAL if the download context was not used in a previous call to SG_client_request_send_async
// return -EBADMSG if the reply was invalid 
// return -ENODATA if the download did not succeed with HTTP 200
// return -ETIMEDOUT if the transfer did not complete in time
// NOTE: this frees up dlctx
int SG_client_request_send_finish( struct SG_gateway* gateway, struct md_download_context* dlctx, SG_messages::Reply* reply ) {
   
   int rc = 0;
   int http_status = 0;
   
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   struct SG_chunk serialized_reply;
   struct SG_client_request_cls* reqcls = NULL;
   
   // get request cls 
   reqcls = (struct SG_client_request_cls*)md_download_context_get_cls( dlctx );
   if( reqcls == NULL ) {
      
      // not a download 
      return -EINVAL;
   }
   
   // wait for this download to finish 
   rc = md_download_context_wait( dlctx, conf->transfer_timeout );
   if( rc != 0 ) {
   
      SG_error("md_download_context_wait( %p ) rc = %d\n", dlctx, rc );
      
      SG_client_download_async_cleanup( dlctx );
      return rc;
   }
   
   // succeeded?
   if( !md_download_context_succeeded( dlctx, 200 ) ) {
      
      // nope
      http_status = md_download_context_get_http_status( dlctx );
      
      SG_error("download %p finished with HTTP status %d\n", dlctx, http_status );
      
      SG_client_download_async_cleanup( dlctx );
      return -ENODATA;
   }
   
   // get data 
   rc = md_download_context_get_buffer( dlctx, &serialized_reply.data, &serialized_reply.len );
   if( rc != 0 ) {
      
      SG_error("md_download_context_get_buffer( %p ) rc = %d\n", dlctx, rc );
      
      SG_client_download_async_cleanup( dlctx );
      return rc;
   }
   
   // parse and validate 
   rc = SG_client_request_end( gateway, &serialized_reply, reqcls, reply );
   
   SG_chunk_free( &serialized_reply );
   
   if( rc != 0 ) {
      
      SG_error("SG_client_request_end( %p ) rc = %d\n", dlctx, rc );
      
      SG_client_download_async_cleanup( dlctx );
      return rc;
   }
   
   // clean up 
   SG_client_download_async_cleanup( dlctx );
   
   return rc;
}


// synchronously download a cert bundle manfest
// be sure to request our certificate
// return 0 on success 
// return -ENOMEM on OOM 
// return -EBADMSG if the downloaded manifest's file version does not match the cert version, or the coordinator ID is not the MS
// return negative on download failure
int SG_client_cert_manifest_download( struct SG_gateway* gateway, uint64_t cert_version, struct SG_manifest* manifest ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t gateway_id = SG_gateway_id( gateway );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   char* url = ms_client_cert_manifest_url( ms->url, volume_id, cert_version, gateway_id );
   
   if( url == NULL ) {
      return -ENOMEM;
   }
   
   // TODO: connection pool 
   CURL* curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( url );
      return -ENOMEM;
   }
   
   // set up CURL handle...
   md_init_curl_handle( conf, curl, url, conf->connect_timeout );
   
   // connect to caches 
   rc = SG_gateway_closure_connect_cache( gateway, curl, url );
   if( rc != 0 ) {
      
      // TODO: connection pool 
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      return rc;
   }
   
   // do the download 
   rc = SG_client_get_manifest_curl( ms, curl, 0, manifest );
   
   curl_easy_cleanup( curl );
   
   if( rc != 0 ) {
      
      SG_error("SG_client_get_manifest_curl( '%s' ) rc = %d\n", url, rc );
      SG_safe_free( url );
      
      return rc;
   }
   
   SG_safe_free( url );
   
   // verify that the manifest's coordinator is the MS
   if( manifest->coordinator_id != 0 ) {
      
      SG_error("Cert bundle has coordinator %" PRIu64 ", expected %d\n", manifest->coordinator_id, 0 );
      
      SG_manifest_free( manifest );
      return -EBADMSG;
   }
   
   // verify certificate version--it must be at least as new as ours
   if( (uint64_t)manifest->file_version < cert_version ) {
      
      SG_error("Cert bundle version mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", cert_version, (uint64_t)manifest->file_version );
      
      SG_manifest_free( manifest );
      return -EBADMSG;
   }
   
   return rc;
}


// begin downloading a certificate from the MS for a given gateway
// return 0 on success
// return -EINVAL if the cert was malformed 
// return negative on download error
int SG_client_cert_download_async( struct SG_gateway* gateway, struct SG_manifest* cert_manifest, uint64_t gateway_id, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   char* url = NULL;
   
   int64_t gateway_cert_version = 0;
   uint64_t gateway_type = ms->gateway_type;
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   uint64_t volume_cert_version = (uint64_t)cert_manifest->file_version;
   
   rc = SG_manifest_get_block_version( cert_manifest, gateway_id, &gateway_cert_version );
   if( rc != 0 ) {
      
      // not found 
      SG_error("SG_manifest_get_block_version( %" PRIu64 " ) rc = %d\n", gateway_id, rc );
      return rc;
   }
   
   // get the url to the cert 
   url = ms_client_cert_url( ms->url, volume_id, volume_cert_version, gateway_type, gateway_id, (uint64_t)gateway_cert_version );
   if( url == NULL ) {
      
      // OOM 
      return -ENOMEM;
   }
   
   // GOGOGO!
   rc = SG_client_download_async_start( gateway, dlloop, dlctx, gateway_id, url, SG_MAX_CERT_LEN, NULL );
   
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_start('%s') rc = %d\n", url, rc );
      SG_safe_free( url );
      
      return rc;
   }
   
   return rc;
}


// finish downloading a certificate from the MS.  Parse and validate it, and free up the download handle.
// return 0 on success, and fill in cert 
// return -errno if the download failed 
// return -EBADMSG if the message could not be parsed, or could not be verified
// return -ENODATA if the request did not succeed with HTTP 200
// return -ENOMEM on OOM
int SG_client_cert_download_finish( struct SG_gateway* gateway, struct md_download_context* dlctx, uint64_t* cert_gateway_id, struct ms_gateway_cert* cert ) {
   
   int rc = 0;
   
   ms::ms_gateway_cert certmsg;
   
   char* serialized_cert = NULL;
   off_t serialized_cert_len = 0;
   
   uint64_t gateway_id = SG_gateway_id( gateway );
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // get the data, and free up the handle
   rc = SG_client_download_async_wait( dlctx, cert_gateway_id, &serialized_cert, &serialized_cert_len, NULL );
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_wait( %p ) rc = %d\n", dlctx, rc );
      return rc;
   }
   
   // parse 
   rc = md_parse< ms::ms_gateway_cert >( &certmsg, serialized_cert, serialized_cert_len );
   
   SG_safe_free( serialized_cert );
   
   if( rc != 0 ) {
      
      SG_error("md_parse( %p ) rc = %d\n", dlctx, rc );
      return -EBADMSG;
   }
   
   // verify--did it come from this volume?
   // NOTE: have to rlock ms, so the volume public key doesn't disappear on us 
   // TODO: verify against user key as well!
   ms_client_config_rlock( ms );
   
   rc = md_verify< ms::ms_gateway_cert >( ms->volume->volume_public_key, &certmsg );
   
   ms_client_config_unlock( ms );
   
   if( rc != 0 ) {
      
      SG_error("md_verify( %p ) rc = %d\n", dlctx, rc );
      return EBADMSG;
   }
   
   // load the cert from the protobuf 
   rc = ms_client_gateway_cert_init( cert, gateway_id, &certmsg );
   if( rc != 0 ) {
      
      SG_error("ms_client_gateway_cert_init rc = %d\n", rc );
      return rc;
   }
   
   // got it!
   return 0;
}

