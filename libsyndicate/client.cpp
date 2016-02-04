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
#include "libsyndicate/proc.h"
#include "libsyndicate/ms/gateway.h"
#include "libsyndicate/url.h"
#include "libsyndicate/ms/url.h"

static char const* SG_post_field_data = SG_SERVER_POST_FIELD_DATA_PLANE;
static char const* SG_post_field_control = SG_SERVER_POST_FIELD_CONTROL_PLANE;

// extra data to include in a write
struct SG_client_WRITE_data {
    
    bool has_write_delta;
    struct SG_manifest* write_delta;
    
    bool has_mtime;
    struct timespec mtime;
    
    bool has_mode;
    mode_t mode;
    
    bool has_owner_id;
    uint64_t owner_id;
    
    // routing information--can be set separately, but will be imported from write_delta if not given 
    bool has_routing_information;
    uint64_t coordinator_id;
    uint64_t volume_id;
    uint64_t file_id;
    int64_t file_version;
};

// per-request state to be preserved for running multiple requests 
struct SG_client_request_cls {
   
   uint64_t chunk_id;                   // ID of the chunk we're transfering
   SG_messages::Request* message;       // the original control-plane message (if uploading)
   uint64_t dest_gateway_id;            // gateway that was supposed to receive the message
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


// download a manifest (from the caches) using an initialized curl handle.  verify it came from remote_gateway_id.
// return 0 on success
// return -ENOMEM on OOM
// return -EAGAIN if the remote gateway is not known to us (i.e. we can't make a manifest url, and we should reload)
// return -EINVAL if we failed to parse the message 
// return -ETIMEDOUT if the request timed out 
// return -EREMOTEIO if the request failed with HTTP 500 or higher
// return -ENOENT on HTTP 404
// return -EPERM on HTTP 400
// return -EACCES on HTTP 401 or 403
// return -ESTALE on HTTP 410
// return -EPROTO on any other HTTP 400-level error
// return -errno on socket- and recv-related errors
// NOTE: does *not* check if the manifest came from a different gateway than the one given here (remote_gateway_id)
static int SG_client_get_manifest_curl( struct SG_gateway* gateway, struct SG_request_data* reqdat, CURL* curl, uint64_t remote_gateway_id, struct SG_manifest* manifest ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   char* serialized_manifest = NULL;
   off_t serialized_manifest_len = 0;
   char* manifest_str = NULL;
   off_t manifest_strlen = 0;
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   SG_messages::Manifest mmsg;
   struct SG_chunk serialized_manifest_chunk;
   struct SG_chunk manifest_chunk;

   memset( &manifest_chunk, 0, sizeof(struct SG_chunk) );
   memset( &serialized_manifest_chunk, 0, sizeof(struct SG_chunk) );
   
   // download!
   rc = md_download_run( curl, SG_MAX_MANIFEST_LEN, &serialized_manifest, &serialized_manifest_len );
   if( rc != 0 ) {
      
      // download failed 
      SG_error("md_download_run rc = %d\n", rc );
      
      // translate HTTP-400-level errors 
      if( rc == -404 ) {
         rc = -ENOENT;
      }
      else if( rc == -400 ) {
         rc = -EPERM;
      }
      else if( rc == -401 || rc == -403 ) {
         rc = -EACCES;
      }
      else if( rc == -410 ) {
         rc = -ESTALE;
      }
      else if( rc >= -499 && rc <= -400 ) {
         rc = -EPROTO;
      }
      
      return rc;
   }
   
   // deserialize 
   serialized_manifest_chunk.data = serialized_manifest;
   serialized_manifest_chunk.len = serialized_manifest_len;
   
   rc = SG_gateway_impl_deserialize( gateway, reqdat, &serialized_manifest_chunk, &manifest_chunk );
   SG_safe_free( serialized_manifest );

   if( rc == -ENOSYS ) {

      SG_warn("%s", "No deserialize method defined\n");

      // no effect
      serialized_manifest_chunk.data = NULL;
      serialized_manifest_chunk.len = 0;

      manifest_chunk.data = serialized_manifest;
      manifest_chunk.len = serialized_manifest_len;
      rc = 0;
   }
   else if( rc != 0 ) {

      // error 
      SG_error("SG_gateway_impl_deserialize rc = %d\n", rc );
      return rc;
   }

   manifest_str = manifest_chunk.data;
   manifest_strlen = manifest_chunk.len;

   // parse 
   rc = md_parse< SG_messages::Manifest >( &mmsg, manifest_str, manifest_strlen );
   SG_safe_free( manifest_str );
   
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
// return -EPROTO on HTTP 400-level message
// return -errno on socket- and recv-related errors
// return non-zero if the gateway's driver method to connect to the cache fails
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
   rc = SG_gateway_impl_connect_cache( gateway, curl, manifest_url );
   if( rc == -ENOSYS ) {
      rc = 0;
   }
   else if( rc != 0 ) {
      
      // failed 
      SG_error("SG_gateway_impl_connect_cache('%s') rc = %d\n", manifest_url, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( manifest_url );
      
      return rc;
   }
   
   rc = SG_client_get_manifest_curl( gateway, reqdat, curl, remote_gateway_id, manifest );
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
   rc = SG_gateway_impl_connect_cache( gateway, curl, url );
   if( rc == -ENOSYS ) {
      rc = 0;
   }
   else if( rc != 0 ) {
      
      // failed 
      SG_error("SG_gateway_impl_connect_cache('%s') rc = %d\n", url, rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // set up download state 
   reqcls->url = url;
   reqcls->chunk_id = chunk_id;
   reqcls->cls = cls;
   
   // set up 
   rc = md_download_context_init( dlctx, curl, block_size * SG_MAX_BLOCK_LEN_MULTIPLIER, reqcls );
   if( rc != 0 ) {
      
      // failed 
      SG_error("md_download_init('%s') rc = %d\n", url, rc );
      
      curl_easy_cleanup( curl );
      
      SG_safe_free( reqcls );
      
      return rc;
   }
   
   // reference it, to keep it around despite the fate of the download loop struct
   // md_download_context_ref( dlctx );
   
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
   
      if( reqcls != NULL ) {
         SG_client_request_cls_free( reqcls );
         SG_safe_free( reqcls );
      }
   }
   
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

   if( reqcls == NULL ) {

      SG_error("FATAL BUG: not a download: %p\n", dlctx );
      exit(1);
   }
   
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
   
   // get the chunk ID from the download's driver
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
   rc = SG_client_download_async_start( gateway, dlloop, dlctx, reqdat->block_id, block_url, block_size * SG_MAX_BLOCK_LEN_MULTIPLIER, reqdat_dup );
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_start('%s') rc = %d\n", block_url, rc );
      SG_safe_free( block_url );
      return rc;
   }
   
   return rc;
}


// log a hash mismatch
// always succeeds
static void SG_client_log_hash_mismatch( unsigned char* expected_block_hash, unsigned char* block_hash ) {

   char* expected_block_hash_str = NULL;
   char* actual_block_hash_str = NULL;
   
   bool logged = false;

   expected_block_hash_str = md_data_printable( expected_block_hash, SG_BLOCK_HASH_LEN );
   actual_block_hash_str = md_data_printable( block_hash, SG_BLOCK_HASH_LEN );
   
   if( expected_block_hash_str != NULL && actual_block_hash_str != NULL ) {
      
      SG_error("Hash mismatch: expected '%s', got '%s'\n", expected_block_hash_str, actual_block_hash_str );
      
      logged = true;
   }
   
   SG_safe_free( expected_block_hash_str );
   SG_safe_free( actual_block_hash_str );
   
   if( !logged ) {
      SG_error("%s", "Hash mismatch: check failed\n" );
   }
}


// log a block hash mismatch from a manifest 
// always succeeds
static void SG_client_get_block_log_hash_mismatch( struct SG_manifest* manifest, uint64_t block_id, unsigned char* block_hash ) {

   // log it (takes a bit of effort to convert the hashes to printable strings...)
   unsigned char* expected_block_hash = NULL;
   size_t expected_block_hash_len = 0;
   int rc = 0;
   
   rc = SG_manifest_get_block_hash( manifest, block_id, &expected_block_hash, &expected_block_hash_len );
   if( rc == 0 ) {
      
      SG_client_log_hash_mismatch( expected_block_hash, block_hash );
      SG_safe_free( expected_block_hash );
   }
   else {
      SG_error("SG_manifest_block_hash_eq(%" PRIu64 "): check failed\n", block_id );
   }
   
   return;
}


// sign a serialized block: prepend a serialized signed block header 
// return 0 on success, and set *signed_chunk
// return -ENOMEM on OOM 
// return -EINVAL if reqdat is not for a block
// return -EPERM on signature failure
int SG_client_block_sign( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block_data, struct SG_chunk* signed_block_data ) {

   int rc = 0;
   SG_messages::SignedBlockHeader blkhdr;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   unsigned char block_hash[SHA256_DIGEST_LENGTH];
   char* hdr_buf = NULL;
   size_t hdr_buf_len = 0;

   char* full_block_data = NULL;
   uint32_t header_len_nbo = 0;      // header length in network byte order

   if( !SG_request_is_block( reqdat ) ) {
      return -EINVAL;
   }

   sha256_hash_buf( block_data->data, block_data->len, block_hash );

   try {
      blkhdr.set_volume_id( volume_id );
      blkhdr.set_file_id( reqdat->file_id );
      blkhdr.set_file_version( reqdat->file_version );
      blkhdr.set_block_id( reqdat->block_id );
      blkhdr.set_block_version( reqdat->block_version );
      blkhdr.set_block_hash( string((char const*)block_hash, SHA256_DIGEST_LENGTH) );
      blkhdr.set_gateway_id( SG_gateway_id( gateway ) );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   rc = md_sign< SG_messages::SignedBlockHeader >( SG_gateway_private_key( gateway ), &blkhdr ); 
   if( rc != 0 ) {
      SG_error("md_sign rc = %d\n", rc );
      return -EPERM;
   }

   // re-pack 
   rc = md_serialize< SG_messages::SignedBlockHeader >( &blkhdr, &hdr_buf, &hdr_buf_len );
   if( rc != 0 ) {
      SG_error("md_serialize rc = %d\n", rc );
      return rc;
   }

   full_block_data = SG_CALLOC( char, sizeof(uint32_t) + hdr_buf_len + block_data->len );
   if( full_block_data == NULL ) {

      SG_safe_free( hdr_buf );
      return -ENOMEM;
   }

   // format: htonl( header_size ) || header || data
   header_len_nbo = htonl( (uint32_t)hdr_buf_len );
   memcpy( full_block_data, &header_len_nbo, sizeof(uint32_t) );
   memcpy( full_block_data + sizeof(uint32_t), hdr_buf, hdr_buf_len );
   memcpy( full_block_data + sizeof(uint32_t) + hdr_buf_len, block_data->data, block_data->len );

   signed_block_data->data = full_block_data;
   signed_block_data->len = sizeof(uint32_t) + hdr_buf_len + block_data->len;

   SG_debug("Signed block: header = %zu bytes, payload = %zu bytes, total = %zu bytes, sig = %s\n", hdr_buf_len, block_data->len, signed_block_data->len, blkhdr.signature().c_str() );

   SG_safe_free( hdr_buf );
   return 0;
}


// verify the authenticity of a block that has a signed block header
// return 0 on success, and set *ret_data_offset to refer to the offset the signed block's buffer where the block data begins
// return -ENOMEM on OOM
// return -EPERM on signature verification failure 
// return -EBADMSG if the block doesn't have enough data
// return -EAGAIN if the cert was not found
int SG_client_block_verify( struct SG_gateway* gateway, struct SG_chunk* signed_block, uint64_t* ret_data_offset ) {

   int rc = 0;
   uint32_t hdr_len = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::SignedBlockHeader blkhdr;
   struct ms_gateway_cert* cert = NULL;
   EVP_PKEY* pubkey = NULL;
   unsigned char block_hash[SHA256_DIGEST_LENGTH];
   uint64_t data_offset = 0;
   uint64_t data_len = 0;

   if( (unsigned)signed_block->len < sizeof(uint32_t) ) {
      // can't even have the header length 
      return -EBADMSG;
   }

   memcpy( &hdr_len, signed_block->data, sizeof(uint32_t) );
   hdr_len = ntohl( hdr_len );

   if( (unsigned)signed_block->len < sizeof(uint32_t) + hdr_len ) {
      // can't have fit the header 
      SG_debug("Invalid header length %zu + %u\n", sizeof(uint32_t), hdr_len);
      return -EBADMSG;
   }

   // load header
   rc = md_parse< SG_messages::SignedBlockHeader >( &blkhdr, signed_block->data + sizeof(uint32_t), hdr_len );
   if( rc != 0 ) {
      // bad message 
      SG_debug("Unparseable data (offset %zu, length %u)\n", sizeof(uint32_t), hdr_len );
      return -EBADMSG;
   }

   // verify header 
   ms_client_config_rlock( ms );

   cert = ms_client_get_gateway_cert( ms, blkhdr.gateway_id() );
   if( cert == NULL ) {

      ms_client_config_unlock( ms );
      SG_error("Cert not found for %" PRIu64 "\n", blkhdr.gateway_id() );
      return -EAGAIN;
   }

   pubkey = ms_client_gateway_pubkey( cert );
   if( pubkey == NULL ) {

      // should never happen
      ms_client_config_unlock( ms );
      SG_error("BUG: no public key for cert of %" PRIu64 "\n", blkhdr.gateway_id() );
      exit(1);
   }

   rc = md_verify< SG_messages::SignedBlockHeader >( pubkey, &blkhdr );
   ms_client_config_unlock( ms );

   if( rc != 0 ) {
      SG_error("md_verify(from %" PRIu64 ") rc = %d\n", blkhdr.gateway_id(), rc );
      return -EPERM;
   }

   // verify block 
   data_offset = sizeof(uint32_t) + hdr_len;
   data_len = signed_block->len - data_offset;
   sha256_hash_buf( signed_block->data + data_offset, data_len, block_hash );

   rc = memcmp( block_hash, (unsigned char*)blkhdr.block_hash().data(), SHA256_DIGEST_LENGTH );
   if( rc != 0 ) {
      // hash mismatch 
      SG_client_log_hash_mismatch( (unsigned char*)blkhdr.block_hash().data(), block_hash );
      return -EPERM;
   }

   // success!
   *ret_data_offset = data_offset;
   return 0;
}

// authenticate a block's content, in one of two ways:
// * if the manifest has a hash for the block, then use the hash
// * otherwise, if the block has a signed block header, use the signed block header
// authentication fails if there is a hash mismatch, signature mismatch, or missing data.
// return 0 on success
// return -ENOMEM on OOM 
// return -EPERM if the data could not be authenticated (block not present, hash mismatch, etc.)
static int SG_client_block_authenticate( struct SG_gateway* gateway, struct SG_manifest* manifest, uint64_t block_id, struct SG_chunk* block_data, uint64_t* block_data_offset ) {

   int rc = 0;
   unsigned char* block_hash = NULL;

   
   // block exists?
   if( !SG_manifest_is_block_present( manifest, block_id ) ) {
      return -EPERM;
   }

   // hash exists?
   if( !SG_manifest_has_block_hash( manifest, block_id ) ) {

      // expect signed block header in the block data stream
      rc = SG_client_block_verify( gateway, block_data, block_data_offset );
      if( rc != 0 ) {
         SG_error("SG_client_block_verify(%" PRIu64 ") rc = %d\n", block_id, rc );
         return rc;
      }
   }
   else {

      // have hash 
      // get the hash 
      block_hash = sha256_hash_data( block_data->data, block_data->len );
      if( block_hash == NULL ) {
         
         // OOM 
         return -ENOMEM;
      }
     
      // compare to the hash in the manifest, verifying that it is actually present at the same time.
      rc = SG_manifest_block_hash_eq( manifest, block_id, block_hash, SG_BLOCK_HASH_LEN );
      if( rc < 0 ) {
         
         // error 
         SG_error("SG_manifest_block_hash_eq( %" PRIu64 " ) rc = %d\n", block_id, rc );
         
         SG_safe_free( block_hash );
         return rc;
      }
      else if( rc == 0 ) {
         
         // mismatch 
         SG_client_get_block_log_hash_mismatch( manifest, block_id, block_hash );
         SG_safe_free( block_hash );
         
         return -EPERM;
      }
   
      *block_data_offset = 0;
   }

   return 0;
} 

// parse a block from a download context, and use the manifest to verify it's integrity 
// if the block is still downloading, wait for it to finish (indefinitely). Otherwise, load right away.
// deserialize the block once we have it.
// return 0 on success, and populate *block with its contents 
// return -ENOMEM on OOM 
// return -EINVAL if the request is not for a block
// return -ENODATA if the download context did not successfully finish
// return -EBADMSG if the block's authenticity could not be verified with the manifest
int SG_client_get_block_finish( struct SG_gateway* gateway, struct SG_manifest* manifest, struct md_download_context* dlctx, uint64_t* block_id, struct SG_chunk* deserialized_block ) {
   
   int rc = 0;
   char* block_buf = NULL;
   off_t block_len = 0;
   uint64_t block_data_offset = 0;

   struct SG_request_data* reqdat = NULL;
   struct SG_chunk block_chunk;
   
   // get the data; recover the original reqdat
   rc = SG_client_download_async_wait( dlctx, block_id, &block_buf, &block_len, (void**)&reqdat );
   if( rc != 0 ) {
      
      SG_error("SG_client_download_async_wait( %p ) rc = %d\n", dlctx, rc );
      
      return rc;
   }
  
   block_chunk.data = block_buf;
   block_chunk.len = block_len;

   // authenticate the data 
   rc = SG_client_block_authenticate( gateway, manifest, *block_id, &block_chunk, &block_data_offset );
   if( rc < 0 ) {
      if( rc == -EPERM ) {

         SG_error("Failed to authenticate block %" PRIu64 "\n", *block_id );
         rc = -EBADMSG;
      }

      SG_safe_free( block_buf );
      memset( &block_chunk, 0, sizeof(struct SG_chunk) );

      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );

      return rc;
   }

   // does the actual block data start somewhere else?
   block_chunk.data = block_chunk.data + block_data_offset;
   block_chunk.len -= block_data_offset; 

   // deserialize
   rc = SG_gateway_impl_deserialize( gateway, reqdat, &block_chunk, deserialized_block );

   SG_safe_free( block_buf );
   memset( &block_chunk, 0, sizeof(struct SG_chunk) );
   SG_request_data_free( reqdat );
   SG_safe_free( reqdat );

   if( rc != 0 ) {
    
       SG_error("SG_gateway_impl_deserialize( %" PRIu64 " ) rc = %d\n", *block_id, rc );
   }

   return rc;
}


// clean up an aborted download loop used for getting blocks 
// return 0 on success 
int SG_client_get_block_cleanup_loop( struct md_download_loop* dlloop ) {

   struct md_download_context* dlctx = NULL;
   int i = 0;
    
   // free all request datas
   for( dlctx = md_download_loop_next_initialized( dlloop, &i ); dlctx != NULL; dlctx = md_download_loop_next_initialized( dlloop, &i ) ) {
      
      if( dlctx == NULL ) {
         break;
      }
      
      struct SG_request_data* reqdat = (struct SG_request_data*)md_download_context_get_cls( dlctx );
      md_download_context_set_cls( dlctx, NULL );

      if( reqdat != NULL ) { 
         SG_request_data_free( reqdat );
         SG_safe_free( reqdat );
      }
   }

   return 0;
}


// get an xattr by name 
// return 0 on success, and set *xattr_value and *xattr_value_len
// return -ENOMEM on OOM 
// return -ENOMEM on OOM
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request, or if we don't know about gateway_id
// return -EREMOTEIO if the HTTP error is >= 500 
// return -ENOATTR on HTTP 404
// return -EACCES on HTTP 401 or 403
// return -EPERM on HTTP 400
// return -ESTALE on HTTP 410
// return -EPROTO for any other HTTP 400-level error
int SG_client_getxattr( struct SG_gateway* gateway, uint64_t gateway_id, char const* fs_path, uint64_t file_id, int64_t file_version, char const* xattr_name, uint64_t xattr_nonce, char** xattr_value, size_t* xattr_len ) {
    
    int rc = 0;
    char* xattr_url = NULL;
    struct ms_client* ms = SG_gateway_ms( gateway );
    struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
    CURL* curl = NULL;
    SG_messages::Reply reply;
    struct ms_gateway_cert* gateway_cert = NULL;
    
    char* buf = NULL;
    off_t len = 0;
    
    ms_client_config_rlock( ms );
    
    gateway_cert = ms_client_get_gateway_cert( ms, gateway_id );
    
    ms_client_config_unlock( ms );
    
    // gateway exists?
    if( gateway_cert == NULL ) {
        return -EAGAIN;
    }
    
    gateway_cert = NULL;
    
    // TODO: connection pool 
    curl = curl_easy_init();
    if( curl == NULL ) {
        return -ENOMEM;
    }
    
    rc = md_url_make_getxattr_url( ms, fs_path, gateway_id, file_id, file_version, xattr_name, xattr_nonce, &xattr_url );
    if( rc != 0 ) {
        
        curl_easy_cleanup( curl );
        return rc;
    }
    
    md_init_curl_handle( conf, curl, xattr_url, conf->connect_timeout );
    
    rc = md_download_run( curl, SG_MAX_XATTR_LEN, &buf, &len );
    curl_easy_cleanup( curl );
    
    if( rc != 0 ) {
        
        SG_error("md_download_run('%s') rc = %d\n", xattr_url, rc );
        SG_safe_free( xattr_url );

        if( rc == -404 ) {
            rc = -ENOATTR;
        }
        else if( rc == -400 ) {
            rc = -EPERM;
        }
        else if( rc == -401 || rc == -403 ) {
            rc = -EACCES;
        }
        else if( rc == -410 ) {
            rc = -ESTALE;
        }

        else if( rc >= -499 && rc <= -400 ) {
            rc = -EPROTO;
        }
        
        return rc;
    }
    
    // parse reply 
    rc = md_parse< SG_messages::Reply >( &reply, buf, len );
    SG_safe_free( buf );
    
    if( rc != 0 ) {
        
        SG_error("md_parse('%s') rc = %d\n", xattr_url, rc );
        SG_safe_free( xattr_url );
        return rc;
    }
    
    SG_safe_free( xattr_url );
    
    ms_client_config_rlock( ms );
    
    gateway_cert = ms_client_get_gateway_cert( ms, gateway_id );
    if( gateway_cert == NULL ) {
        
        ms_client_config_unlock( ms );
        return -EAGAIN;
    }
    
    // verify reply 
    rc = md_verify< SG_messages::Reply >( ms_client_gateway_pubkey( gateway_cert ), &reply );
    ms_client_config_unlock( ms );
    
    if( rc != 0 ) {
        
        // invalid reply
        return rc;
    }
    
    // validate reply 
    if( !reply.has_xattr_value() ) {
        
        // invalid reply 
        return rc;
    }
    
    *xattr_value = SG_strdup_or_null( reply.xattr_value().c_str() );
    if( *xattr_value == NULL ) {
        
        // OOM 
        return -ENOMEM;
    }
    
    
    *xattr_len = reply.xattr_value().size();
    return 0;
}


// get a list of xattrs by name 
// return 0 on success, and set *xattr_value and *xattr_value_len
// return -ENOMEM on OOM 
// return -ENOMEM on OOM
// return -ETIMEDOUT if the tranfser could not complete in time 
// return -EAGAIN if we were signaled to retry the request 
// return -EREMOTEIO if the HTTP error is >= 500
// return -EPERM on HTTP 400
// return -EACCES if the HTTP error is 401 or 403 
// return _ENOATTR on HTTP 404
// return -ESTALE on HTTP 410
// return -EPROTO for any other HTTP 400-level error
int SG_client_listxattrs( struct SG_gateway* gateway, uint64_t gateway_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t xattr_nonce, char** xattr_list, size_t* xattr_list_len ) {
    
    int rc = 0;
    char* xattr_url = NULL;
    struct ms_client* ms = SG_gateway_ms( gateway );
    struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
    CURL* curl = NULL;
    SG_messages::Reply reply;
    struct ms_gateway_cert* gateway_cert = NULL;
    off_t off = 0;
    
    char* buf = NULL;
    off_t len = 0;
    
    ms_client_config_rlock( ms );
    
    gateway_cert = ms_client_get_gateway_cert( ms, gateway_id );
    
    ms_client_config_unlock( ms );
    
    // gateway exists?
    if( gateway_cert == NULL ) {
        return -EAGAIN;
    }
    
    gateway_cert = NULL;
    
    // TODO: connection pool 
    curl = curl_easy_init();
    if( curl == NULL ) {
        return -ENOMEM;
    }
    
    rc = md_url_make_listxattr_url( ms, fs_path, gateway_id, file_id, file_version, xattr_nonce, &xattr_url );
    if( rc != 0 ) {
        
        curl_easy_cleanup( curl );
        return rc;
    }
    
    md_init_curl_handle( conf, curl, xattr_url, conf->connect_timeout );
    
    rc = md_download_run( curl, SG_MAX_XATTR_LEN, &buf, &len );
    curl_easy_cleanup( curl );
    
    if( rc != 0 ) {
        
        SG_error("md_download_run('%s') rc = %d\n", xattr_url, rc );
        SG_safe_free( xattr_url );
        
        if( rc == -400 ) {
            rc = -EPERM;
        }
        else if( rc == -404 ) {
            rc = -ENOATTR;
        }
        else if( rc == -403 || rc == -401 ) {
            rc = -EACCES;
        }
        else if( rc == -410 ) {
            rc = -ESTALE;
        }
        else if( rc >= -499 && rc <= -400 ) {
            rc = -EPROTO;
        }
        
        return rc;
    }
    
    // parse reply 
    rc = md_parse< SG_messages::Reply >( &reply, buf, len );
    SG_safe_free( buf );
    
    if( rc != 0 ) {
        
        SG_error("md_parse('%s') rc = %d\n", xattr_url, rc );
        SG_safe_free( xattr_url );
        return rc;
    }
    
    SG_safe_free( xattr_url );
    
    ms_client_config_rlock( ms );
    
    gateway_cert = ms_client_get_gateway_cert( ms, gateway_id );
    if( gateway_cert == NULL ) {
        
        ms_client_config_unlock( ms );
        return -EAGAIN;
    }
    
    // verify reply 
    rc = md_verify< SG_messages::Reply >( ms_client_gateway_pubkey( gateway_cert ), &reply );
    ms_client_config_unlock( ms );
    
    if( rc != 0 ) {
        
        // invalid reply
        return rc;
    }
    
    if( reply.xattr_names_size() == 0 ) {
        
        // no xattrs 
        return 0;
    }
    
    // how many bytes?
    *xattr_list_len = 0;
    for( int i = 0; i < reply.xattr_names_size(); i++ ) {
        
        *xattr_list_len += reply.xattr_names(i).size() + 1;
    }
    
    *xattr_list = SG_CALLOC( char, *xattr_list_len );
    if( *xattr_list == NULL ) {
        
        // OOM 
        return -ENOMEM;
    }
    
    for( int i = 0; i < reply.xattr_names_size(); i++ ) {
        
        memcpy( *xattr_list + off, reply.xattr_names(i).c_str(), reply.xattr_names(i).size() );
        off += (reply.xattr_names(i).size() + 1);
    }
    
    return 0;
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
// return -ENOMEM on OOM 
static int SG_client_request_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat ) {
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t volume_version = ms_client_volume_version( ms );
   uint64_t cert_version = ms_client_cert_version( ms );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   uint64_t gateway_id = SG_gateway_id( gateway );
   uint64_t user_id = SG_gateway_user_id( gateway );
   
   // sanity check...
   if( reqdat->coordinator_id == SG_INVALID_GATEWAY_ID || reqdat->file_id == SG_INVALID_FILE_ID || reqdat->fs_path == NULL ) {
      SG_error("BUG: missing coordinator (%" PRIu64 "), file_id (%" PRIX64 "), or path (%s)\n", reqdat->coordinator_id, reqdat->file_id, reqdat->fs_path );
      exit(1);
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
      request->set_message_nonce( md_random64() );
      
      request->set_fs_path( string(reqdat->fs_path) );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}


// allocate a write request 
struct SG_client_WRITE_data* SG_client_WRITE_data_new(void) {
    return SG_CALLOC( struct SG_client_WRITE_data, 1 );
}

// set up a write reqeust 
int SG_client_WRITE_data_init( struct SG_client_WRITE_data* dat ) {
    memset( dat, 0, sizeof(struct SG_client_WRITE_data) );
    return 0;
}

// set write data manifest.
// NOTE: shallow copy 
int SG_client_WRITE_data_set_write_delta( struct SG_client_WRITE_data* dat, struct SG_manifest* write_delta ) {
    dat->write_delta = write_delta;
    dat->has_write_delta = true;
    return 0;
}

// set write data mtime 
int SG_client_WRITE_data_set_mtime( struct SG_client_WRITE_data* dat, struct timespec* mtime ) {
    dat->mtime = *mtime;
    dat->has_mtime = true;
    return 0;
}

// set write data mode 
int SG_client_WRITE_data_set_mode( struct SG_client_WRITE_data* dat, mode_t mode ) {
    dat->mode = mode;
    dat->has_mode = true;
    return 0;
}

// set write data owner ID
int SG_client_WRITE_data_set_owner_id( struct SG_client_WRITE_data* dat, uint64_t owner_id ) {
    dat->owner_id = owner_id;
    dat->has_owner_id = true;
    return 0;
}

// set routing info
int SG_client_WRITE_data_set_routing_info( struct SG_client_WRITE_data* dat, uint64_t volume_id, uint64_t coordinator_id, uint64_t file_id, int64_t file_version ) {
    
    dat->coordinator_id = coordinator_id;
    dat->file_id = file_id;
    dat->volume_id = volume_id;
    dat->file_version = file_version;
    dat->has_routing_information = true;
    return 0;
}

// merge data into an md_entry from a WRITE data struct 
int SG_client_WRITE_data_merge( struct SG_client_WRITE_data* dat, struct md_entry* ent ) {
    
    if( dat->has_owner_id ) {
        ent->owner = dat->owner_id;
    }
    if( dat->has_mtime ) {
        ent->mtime_sec = dat->mtime.tv_sec;
        ent->mtime_nsec= dat->mtime.tv_nsec;
    }
    if( dat->has_mode ) {
        ent->mode = dat->mode;
    }
    
    return 0;
}

// make a signed WRITE message--that is, send over new block information for a file, encoded as a manifest.
// the destination gateway is the coordinator ID in the manifest.
// write-delta must be non-NULL
// if new_owner and/or new_mode are non-NULL, they will be filled in as well
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if we don't have any routing information set in dat
int SG_client_request_WRITE_setup( struct SG_gateway* gateway, SG_messages::Request* request, char const* fs_path, struct SG_client_WRITE_data* dat ) {
   
   int rc = 0;
   
   // sanity check...
   if( !dat->has_routing_information ) {
       SG_error("BUG: no routing information for '%s'\n", fs_path);
       return -EINVAL;
   }
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   struct SG_request_data reqdat;
   memset( &reqdat, 0, sizeof(reqdat) );
   
   reqdat.coordinator_id = dat->coordinator_id;
   reqdat.fs_path = (char*)fs_path;
   reqdat.volume_id = dat->volume_id;
   reqdat.file_id = dat->file_id;
   reqdat.file_version = dat->file_version;
   
   rc = SG_client_request_setup( gateway, request, &reqdat );
   if( rc != 0 ) {
      
      SG_error("SG_client_request_setup('%s') rc = %d\n", fs_path, rc );
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::WRITE );
   
   if( dat->has_write_delta ) {
       
       // sending a manifest write delta
       rc = SG_manifest_serialize_blocks_to_request_protobuf( dat->write_delta, request );
       if( rc != 0 ) {
      
           SG_error("SG_manifest_serialize_blocks_to_request_protobuf('%s') rc = %d\n", fs_path, rc );
           return rc;
       }
   
       request->set_new_manifest_mtime_sec( dat->write_delta->mtime_sec );
       request->set_new_manifest_mtime_nsec( dat->write_delta->mtime_nsec );
   }
   
   if( dat->has_owner_id ) {
      request->set_new_owner_id( dat->owner_id );
   }
   
   if( dat->has_mode ) {
      request->set_new_mode( dat->mode );
   }
   
   if( dat->has_mtime ) {
      
      request->set_new_mtime_sec( dat->mtime.tv_sec );
      request->set_new_mtime_nsec( dat->mtime.tv_nsec );
   }
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// make a signed TRUNCATE message, from an initialized reqdat.
// the reqdat must be for a manifest
// return 0 on success 
// return -EINVAL if the reqdat is not for a manifest
// return -ENOMEM on OOM 
int SG_client_request_TRUNCATE_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, off_t new_size ) {
   
   int rc = 0;
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat );
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


// make a signed RENAME request, from an initialized reqdat.
// the reqdat must be for a manifest
// return 0 on success 
// return -EINVAL if teh reqdat is not for a manifest
// return -ENOMEM on OOM 
int SG_client_request_RENAME_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* new_path ) {
   
   int rc = 0;
   
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat );
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


// make a signed DETACH request from an initialized reqdat, optionally with an MS-given vacuum ticket
// the reqdat must be for a manifest
// return 0 on success 
// return -EINVAL if the reqdat is not for a manifest
// return -ENOMEM on OOM 
int SG_client_request_DETACH_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::DETACH );
   
   rc = md_sign< SG_messages::Request >( gateway_pkey, request );
   if( rc != 0 ) {
      
      SG_error("md_sign rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}


// make a PUTCHUNKS request, optionally signing it
// return 0 on sucess 
// return -ENOMEM on OOM 
int SG_client_request_PUTCHUNKS_setup_ex( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t num_chunk_info, bool sign ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::PUTCHUNKS );
  
   for( size_t i = 0; i < num_chunk_info; i++ ) {

       // add block information 
       SG_messages::ManifestBlock* mblock = NULL;
   
       try {
      
          mblock = request->add_blocks();
       }
       catch( bad_alloc& ba ) {
          return -ENOMEM;
       }
   
       rc = SG_manifest_block_serialize_to_protobuf( &chunk_info[i], mblock );
       if( rc != 0 ) {
      
          return rc;
       }
   }
   
   if( sign ) {
       rc = md_sign< SG_messages::Request >( gateway_pkey, request );
       if( rc != 0 ) {
      
          SG_error("md_sign rc = %d\n", rc );
          return rc;
       }
   }
   
   return rc;
}

// make a signed PUTCHUNKS request 
int SG_client_request_PUTCHUNKS_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t num_chunk_info ) {
   return SG_client_request_PUTCHUNKS_setup_ex( gateway, request, reqdat, chunk_info, num_chunk_info, true );
}


// make a DELETECHUNKS request, optionally signing it
// return 0 on sucess 
// return -ENOMEM on OOM 
int SG_client_request_DELETECHUNKS_setup_ex( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t num_chunk_info, bool sign ) {
   
   int rc = 0;
   EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
   // basics 
   rc = SG_client_request_setup( gateway, request, reqdat );
   if( rc != 0 ) {
      
      return rc;
   }
   
   request->set_request_type( SG_messages::Request::DELETECHUNKS );
   
   for( size_t i = 0; i < num_chunk_info; i++ ) {

       // add block information 
       SG_messages::ManifestBlock* mblock = NULL;
   
       try {
      
          mblock = request->add_blocks();
       }
       catch( bad_alloc& ba ) {
          return -ENOMEM;
       }
   
       rc = SG_manifest_block_serialize_to_protobuf( &chunk_info[i], mblock );
       if( rc != 0 ) {
      
          return rc;
       }
   }

   if( sign ) {
       rc = md_sign< SG_messages::Request >( gateway_pkey, request );
       if( rc != 0 ) {
      
          SG_error("md_sign rc = %d\n", rc );
          return rc;
       }
   }
   
   return rc;
}

// make a signed DELETECHUNKS request
int SG_client_request_DELETECHUNKS_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, struct SG_manifest_block* chunk_info, size_t num_chunk_info ) {
   return SG_client_request_DELETECHUNKS_setup_ex( gateway, request, reqdat, chunk_info, num_chunk_info, true );
}

// make a signed SETXATTR request
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_request_SETXATTR_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, int flags ) {
    
    int rc = 0;
    EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
   
    // basics 
    rc = SG_client_request_setup( gateway, request, reqdat );
    if( rc != 0 ) {
      
       return rc;
    }
    
    request->set_request_type( SG_messages::Request::SETXATTR );
    
    try {
        request->set_xattr_name( string(xattr_name) );
        request->set_xattr_value( string(xattr_value, xattr_value_len) );
        request->set_xattr_flags( flags );
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


// make a signed REMOVEXATTR request 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_client_request_REMOVEXATTR_setup( struct SG_gateway* gateway, SG_messages::Request* request, struct SG_request_data* reqdat, char const* xattr_name ) {
    
    int rc = 0;
    EVP_PKEY* gateway_pkey = SG_gateway_private_key( gateway );
    
    // basics 
    rc = SG_client_request_setup( gateway, request, reqdat );
    if( rc != 0 ) {
        
        return rc;
    }
    
    request->set_request_type( SG_messages::Request::REMOVEXATTR );
    
    try {
        request->set_xattr_name( string(xattr_name) );
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


// begin sending a request 
// serialize the given message, and set up a request cls.
// NOTE: the download takes ownership of control_plane--the caller should not manipulate it in any way while the download is proceeding
// NOTE: control_plane should be signed beforehand
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
   reqcls->dest_gateway_id = dest_gateway_id;
   
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
   if( reply->gateway_id() != reqcls->dest_gateway_id ) {
      
      SG_error("Gateway mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", reqcls->dest_gateway_id, reply->gateway_id() );
      return -EBADMSG;
   }
   
   // verify message nonce 
   if( control_plane->message_nonce() != reply->message_nonce() ) {
      
      SG_error("Message nonce mismatch: expected %" PRIX64 ", got %" PRIX64 "\n", control_plane->message_nonce(), reply->message_nonce() );
   }
   
   // verify signature 
   rc = ms_client_verify_gateway_message< SG_messages::Reply >( ms, volume_id, reqcls->dest_gateway_id, reply );
   if( rc != 0 ) {
      
      SG_error("ms_client_verify_gateway_message( from=%" PRIu64 " ) rc = %d\n", reqcls->dest_gateway_id, rc );
      return -EBADMSG;
   }
   
   // done! 
   return 0;
}


// determine whether or not a call to SG_client_request_send or SG_client_request_send_finish indicates
// that the remote gateway is down.  That is, the error is one of the following:
// -EBADMSG, -EPROTO, -ETIMEDOUT
bool SG_client_request_is_remote_unavailable( int error ) {
   
   return (error == -EBADMSG || error == -ETIMEDOUT || error == -EPROTO);
}

// send a message as a (control plane, data plane) pair, synchronously, to another gateway 
// return 0 on success 
// return -ENOMEM if OOM 
// return -EAGAIN if the request should be retried.  This could be because dest_gateway_id is not known to us, but could become known if we refreshed the volume config.
// return -ETIMEDOUT on connection timeout 
// return -EREMOTEIO if the HTTP status was >= 500 (indicates server-side I/O error)
// return -EACCES if HTTP status was 401 or 403
// return -EPERM on HTTP 400
// return -ESTALE on HTTP 410
// return -ENOENT on HTTP 404
// return -EPROTO if HTTP status was between 400 or 499, and not 401 or 403 (indicates a misconfiguration--they should never happen)
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
   
   curl_easy_setopt( curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL );    // force POST on redirect
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, reqcls.form_begin );
   
   // run the transfer 
   rc = md_download_run( curl, SG_CLIENT_MAX_REPLY_LEN, &serialized_reply.data, &serialized_reply.len );
   
   if( rc >= -499 && rc <= -400 ) {
      
      // 400-level HTTP error 
      SG_error("md_download_run('%s') HTTP status %d\n", reqcls.url, -rc );
      
      curl_easy_cleanup( curl );
      
      SG_client_request_cls_free( &reqcls );

      if( rc == -404 ) {
         rc = -ENOENT;
      }
      else if( rc == -403 || rc == -400 ) {
         rc = -EACCES;
      }
      else if( rc == -400 ) {
         rc = -EPERM;
      }
      else if( rc == -410 ) {
         rc = -ESTALE;
      }
      else {
         rc = -EPROTO;
      }

      return rc;
   }
   
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
   
   curl_easy_setopt( curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL );    // force POST on redirect
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
      SG_error("FATAL BUG: not a client download: %p\n", dlctx );
      exit(1);
   }
   
   // wait for this download to finish 
   rc = md_download_context_wait( dlctx, conf->transfer_timeout * 1000 );
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

