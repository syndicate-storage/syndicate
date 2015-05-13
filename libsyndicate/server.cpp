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

#include "libsyndicate/server.h"
#include "libsyndicate/gateway.h"
#include "libsyndicate/manifest.h"
#include "libsyndicate/url.h"

// connection initialization handler for embedded HTTP server
// return 0 on success 
// return -ENOMEM on OOM
// return -EINVAL if the request is not a valid URL
int SG_server_HTTP_connect( struct md_HTTP_connection_data* con_data, void** cls ) {
   
   struct SG_server_connection* sgcon = SG_CALLOC( struct SG_server_connection, 1 );
   if( sgcon == NULL ) {
      return -ENOMEM;
   }
   
   sgcon->gateway = (struct SG_gateway*)md_HTTP_cls( con_data->http );
   
   *cls = sgcon;
   
   return 0;
}


// stat a file given its request info, and set up an HTTP response with the appropriate failure code if it fails 
// return 0 if we handled the failure 
// return 1 if there was no failure to handle
// return negative on error 
static int SG_gateway_impl_stat_or_fail( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode ) {
   
   int rc = 0;
   
   // do the stat
   rc = SG_gateway_impl_stat( gateway, reqdat, entity_info, mode );
   if( rc != 0 ) {
      
      // not found or permission error?
      if( rc == -ENOENT || rc == -EACCES ) {
         
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      
      // not permitted? invalid?
      else if( rc == -EPERM || rc == -EINVAL ) {
         
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      
      // not defined?
      else if( rc == -ENOSYS ) {
         
         return md_HTTP_create_response_builtin( resp, 501 );
      }
      
      // some other error 
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   return 1;
}


// stat the requested entity, and verify that it has an appropriate mode.
// if not, generate the appropriate HTTP response
// return 0 if the request was handled 
// return 1 if not handled, but the request is sound (i.e. the caller should service it)
// return negative on error 
static int SG_server_stat_request( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, mode_t mode ) {
   
   int rc = 0;
   mode_t entity_mode = 0;
   
   // stat it
   rc = SG_gateway_impl_stat_or_fail( gateway, resp, reqdat, NULL, &entity_mode );
   if( rc <= 0 ) {
      
      // handled or error
      return rc;
   }
   
   // permission check 
   if( (entity_mode & mode) == 0 ) {
      
      // denied 
      return md_HTTP_create_response_builtin( resp, 403 );
   }
   
   // not handled, but okay for caller to handle
   return 1;
}


// sanity-check on inbound requests
// reject a request if the requested entity is not found, or does not have the requisite permissions
// redirect a request if it is to a stale version, or to a file we don't coordinate
// return 0 if handled 
// return 1 if not handled, but the request is sound (i.e. the caller should service it)
// return negative on error 
static int SG_server_redirect_request( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, mode_t mode ) {
   
   int rc = 0;
   struct SG_request_data entity_info;
   uint64_t gateway_id = SG_gateway_id( gateway );
   mode_t entity_mode = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   char* url = NULL;

   if( !SG_request_is_block( reqdat ) && !SG_request_is_manifest( reqdat ) ) {
      
      SG_error("%s", "Invalid request\n");
      
      // invalid request 
      return md_HTTP_create_response_builtin( resp, 400 );
   }
   
   if( gateway->impl_stat == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_stat is undefined\n");
      
      return md_HTTP_create_response_builtin( resp, 501 );
   }
   
   // stat the requested entity 
   rc = SG_gateway_impl_stat_or_fail( gateway, resp, reqdat, &entity_info, &entity_mode );
   if( rc <= 0 ) {
      
      // handled or error
      return rc;
   }
   
   rc = 0;
   
   // do we need to redirect?
   if( gateway_id != entity_info.coordinator_id ||               // coordinators do not match
       reqdat->file_version != entity_info.file_version ||       // file versions do not match
       (SG_request_is_block( reqdat ) && reqdat->block_version != entity_info.block_version) ||  // block request, and block versions do not match
       (SG_request_is_manifest( reqdat ) && (reqdat->manifest_timestamp.tv_sec < entity_info.manifest_timestamp.tv_sec ||       // manifest request, and timestamps are behind current
                                            (reqdat->manifest_timestamp.tv_sec == entity_info.manifest_timestamp.tv_sec && reqdat->manifest_timestamp.tv_nsec < entity_info.manifest_timestamp.tv_nsec)))) {
      
      // redirect 
      if( SG_request_is_block( reqdat ) ) {
         
         // block_request 
         if( gateway_id != entity_info.coordinator_id ) {
            
            SG_debug("REDIRECT: Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", entity_info.coordinator_id, gateway_id );
            
            // redirect block request to remote coordinator
            rc = md_url_make_block_url( ms, entity_info.fs_path, entity_info.coordinator_id, entity_info.file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, &url );
         }
         else {
            
            SG_debug("REDIRECT: Block/version mismatch: expected version=%" PRId64 ", block=%" PRIu64 ".%" PRId64 ", got version=%" PRId64 ", block=%" PRIu64 ".%" PRId64 "\n",
                     entity_info.file_version, entity_info.block_id, entity_info.block_version,
                     reqdat->file_version, reqdat->block_id, reqdat->block_version );
                     
            // redirect block request to newer local version 
            url = md_url_public_block_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, entity_info.block_id, entity_info.block_version );
            
            if( url == NULL ) {
               
               // OOM
               rc = -ENOMEM;
            }
         }
      }
      else {
         
         // manifest request 
         if( gateway_id != entity_info.coordinator_id ) {
            
            SG_debug("REDIRECT: Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", entity_info.coordinator_id, gateway_id );
            
            // redirect manifest request to remote coordinator
            rc = md_url_make_manifest_url( ms, entity_info.fs_path, entity_info.coordinator_id, entity_info.file_id, reqdat->file_version, &reqdat->manifest_timestamp, &url );
         }
         else {
            
            SG_debug("REDIRECT: Manifest/version mismatch: expected version=%" PRId64 ", ts=%" PRId64 ".%ld, got version=%" PRId64 ", ts=%" PRId64 ".%ld\n",
                     entity_info.file_version, entity_info.manifest_timestamp.tv_sec, entity_info.manifest_timestamp.tv_nsec,
                     reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec );
            
            // redirect manifest request to newer local version 
            url = md_url_public_manifest_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, &entity_info.manifest_timestamp );
            
            if( url == NULL ) {
               
               // OOM 
               rc = -ENOMEM;
            }
         }
      }
      
      if( url == NULL || rc != 0 ) {
         
         // failure 
         SG_safe_free( url );
         SG_request_data_free( &entity_info );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      // return 302 
      rc = md_HTTP_create_response_ram( resp, "text/plain", 302, "Redirect\n", strlen("Redirect\n") + 1 );
      if( rc != 0 ) {
         
         // failed to generate response 
         
         SG_safe_free( url );
         SG_request_data_free( &entity_info );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      rc = md_HTTP_header_add( resp, "Location", url );
      if( rc != 0 ) {
         
         // failed to generate header 
         SG_safe_free( url );
         SG_request_data_free( &entity_info );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      rc = md_HTTP_header_add( resp, "Cache-Control", "no-store" );
      if( rc != 0 ) {
         
         // failed to generate header 
         
         SG_safe_free( url );
         SG_request_data_free( &entity_info );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      // handled!
      SG_debug("Redirect to '%s'\n", url );
      
      SG_safe_free( url );
      SG_request_data_free( &entity_info );
      return 0;
   }
   
   // permission check 
   if( (entity_mode & mode) == 0 ) {
      
      // denied 
      SG_safe_free( url );
      SG_request_data_free( &entity_info );
      
      return md_HTTP_create_response_builtin( resp, 403 );
   }
   
   // request is sound, and refers to fresh data 
   SG_safe_free( url );
   SG_request_data_free( &entity_info );
      
   return 1;
}


// HTTP HEAD handler
// see if a block or manifest exists, and get redirected if need be
int SG_server_HTTP_HEAD_handler( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {

   struct SG_server_connection* sgcon = (struct SG_server_connection*)con_data->cls;
   
   struct SG_request_data reqdat;
   struct SG_gateway* gateway = sgcon->gateway;
   
   int rc = 0;
   
   // parse the request
   rc = SG_request_data_parse( &reqdat, con_data->url_path );
   if( rc != 0 ) {
      
      if( rc != -ENOMEM ) {
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      else {
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // redirect?  expect world-readable
   rc = SG_server_redirect_request( gateway, resp, &reqdat, 0004 );
   if( rc <= 0 ) {
      
      // handled, or error
      return rc;
   }
   
   SG_request_data_free( &reqdat );
   
   // this block or manifest is local and the requester knows the latest data
   return md_HTTP_create_response_builtin( resp, 200 );
}


// GET a block 
// try the cache first, then the implementation 
// return 0 on success
// return -ENOMEM on OOM
int SG_server_HTTP_GET_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   uint64_t impl_hints = SG_gateway_impl_hints( gateway );

   // block request 
   struct SG_chunk block;
   struct SG_chunk block_dup;
   struct md_cache_block_future* block_fut = NULL;
   
   // sanity check 
   if( gateway->impl_get_block == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_get_blocks is undefined\n");
      
      // not implemented 
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   if( (impl_hints & SG_GATEWAY_HINT_NO_BLOCK_CACHE) == 0 ) {
      
      // service from the cache?
      rc = SG_gateway_cached_block_get_raw( gateway, reqdat, &block );
      
      if( rc == 0 ) {
         
         // reply 
         return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, block.data, block.len );
      }
      else if( rc != -ENOENT ) {
         
         // error 
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }

   // cache miss 
   SG_debug("CACHE MISS %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "]\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version );
   
   // get from the implementation
   rc = SG_gateway_impl_block_get( gateway, reqdat, &block );
   if( rc < 0 ) {
      
      if( rc == -ENOENT ) {
         
         // not present (i.e. EOF)
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      
      // failure
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // duplicate the block--give one to the cache, and send one back 
   rc = SG_chunk_dup( &block_dup, &block );
   if( rc != 0 ) {
      
      // OOM 
      SG_chunk_free( &block );
      return md_HTTP_create_response_builtin( resp, 503 );
   }
   
   // apply the closure and cache this block, asynchronously 
   rc = SG_gateway_cached_block_put_async( gateway, reqdat, &block, SG_CACHE_FLAG_DETACHED | SG_CACHE_FLAG_UNSHARED, &block_fut );
   if( rc < 0 ) {
      
      // failure 
      SG_chunk_free( &block );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, block_dup.data, block_dup.len );
}



// GET a manifest 
// try the cache first, then the implementation 
// return 0 on success
// return -ENOMEM on OOM
int SG_server_HTTP_GET_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   uint64_t impl_hints = SG_gateway_impl_hints( gateway );

   // manifest request 
   struct SG_chunk raw_serialized_manifest;  // out of the cache 
   struct SG_chunk serialized_manifest;      // into the cache
   struct SG_chunk serialized_manifest_resp; // response
   
   struct SG_manifest manifest;              // from the implementation
   SG_messages::Manifest manifest_message;
   
   char* serialized_manifest_str;
   size_t serialized_manifest_len;
   
   struct md_cache_block_future* manifest_fut = NULL;
   
   EVP_PKEY* gateway_private_key = SG_gateway_private_key( gateway );
   
   // sanity check 
   if( gateway->impl_get_manifest == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_get_manifest is undefined\n");
      
      // not implemented 
      return md_HTTP_create_response_builtin( resp, 501 );
   }
   
   // manifest request 
   if( (impl_hints & SG_GATEWAY_HINT_NO_MANIFEST_CACHE) == 0 ) {
      
      // try the cache
      rc = SG_gateway_cached_manifest_get_raw( gateway, reqdat, &raw_serialized_manifest );
      
      if( rc == 0 ) {
         
         // reply 
         return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, raw_serialized_manifest.data, raw_serialized_manifest.len );
      }
      else if( rc != -ENOENT ) {
         
         // error 
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   SG_debug("CACHE MISS %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld]\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec );
   
   // cache miss 
   // get from the implementation 
   memset( &manifest, 0, sizeof(struct SG_manifest) );
   
   rc = SG_gateway_impl_manifest_get( gateway, reqdat, &manifest );
   if( rc != 0 ) {
      
      // failed 
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // serialize 
   rc = SG_manifest_serialize_to_protobuf( &manifest, &manifest_message );
   
   SG_manifest_free( &manifest );
   
   if( rc != 0 ) {
      
      // failed 
      SG_error("SG_manifest_serialize_to_protobuf( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // sign manifest 
   rc = md_sign< SG_messages::Manifest >( gateway_private_key, &manifest_message );
   if( rc != 0 ) {
      
      // failed to sign 
      SG_error("md_sign( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // serialize to string 
   rc = md_serialize< SG_messages::Manifest >( &manifest_message, &serialized_manifest_str, &serialized_manifest_len );
   if( rc != 0 ) {
      
      SG_error("md_serialize( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   SG_chunk_init( &serialized_manifest, serialized_manifest_str, serialized_manifest_len );
   
   // duplicate--send one back, and send the other to the cache
   rc = SG_chunk_dup( &serialized_manifest_resp, &serialized_manifest );
   if( rc != 0 ) {
      
      // OOM 
      SG_chunk_free( &serialized_manifest );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // apply the closure and cache (asynchronously)
   // cache takes ownership of the memory 
   rc = SG_gateway_cached_manifest_put_async( gateway, reqdat, &serialized_manifest, SG_CACHE_FLAG_DETACHED | SG_CACHE_FLAG_UNSHARED, &manifest_fut );
   if( rc != 0 ) {
      
      // failed 
      SG_chunk_free( &serialized_manifest );
      SG_chunk_free( &serialized_manifest_resp );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // reply with the signed, serialized manifest!
   return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, serialized_manifest_resp.data, serialized_manifest_resp.len );
}


// HTTP GET handler.
// dispatch the GET to the gateway's get_block or get_manifest, depending on the URL path.
// try the cache first, and then the implementation.
// return 0 on success, and populate *resp
// return -ENOMEM on OOM
// TODO: handle in an I/O work thread
int SG_server_HTTP_GET_handler( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {

   struct SG_server_connection* sgcon = (struct SG_server_connection*)con_data->cls;
   
   struct SG_request_data reqdat;
   struct SG_gateway* gateway = sgcon->gateway;
   
   int rc = 0;
   
   // parse the request
   rc = SG_request_data_parse( &reqdat, con_data->url_path );
   if( rc != 0 ) {
      
      if( rc != -ENOMEM ) {
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      else {
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // redirect? expect world-readable or volume-readable
   rc = SG_server_redirect_request( gateway, resp, &reqdat, 0044 );
   if( rc <= 0 ) {
      
      // handled, or error 
      SG_request_data_free( &reqdat );
      return rc;
   }
   
   // block request?
   if( SG_request_is_block( &reqdat ) ) {

      rc = SG_server_HTTP_GET_block( gateway, &reqdat, resp );
   }
   
   else if( SG_request_is_manifest( &reqdat ) ) {
      
      rc = SG_server_HTTP_GET_manifest( gateway, &reqdat, resp );
   }
   
   else {
      
      // bad request 
      rc = md_HTTP_create_response_builtin( resp, 400 );
   }
   
   SG_request_data_free( &reqdat );
   return rc;
}


// populate a reply message, signing it as well
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL on failure to serialize and sign
static int SG_server_reply_setup( struct SG_gateway* gateway, SG_messages::Reply* reply, uint64_t message_nonce, int error_code ) {
   
   int rc = 0;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   
   uint64_t gateway_id = ms->gateway_id;
   uint64_t gateway_type = ms->gateway_type;
   uint64_t volume_version = ms_client_volume_version( ms );
   uint64_t cert_version = ms_client_cert_version( ms );
   uint64_t user_id = conf->owner;
   
   EVP_PKEY* gateway_private_key = SG_gateway_private_key( gateway );
   
   try {
      
      reply->set_volume_version( volume_version );
      reply->set_cert_version( cert_version );
      reply->set_message_nonce( message_nonce );
      reply->set_error_code( error_code );
      
      reply->set_user_id( user_id );
      reply->set_gateway_id( gateway_id );
      reply->set_gateway_type( gateway_type );  
   }
   catch( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
   
   rc = md_sign< SG_messages::Reply >( gateway_private_key, reply );
   if( rc != 0 ) {
      
      return rc;
   }
   
   return 0;
}


// generate a populated, signed reply.
// serialize it and put it into a response 
// return 0 on success (indicates that we generated the HTTP response)
// return -ENOMEM on OOM 
static int SG_server_reply_serialize( struct SG_gateway* gateway, SG_messages::Reply* reply, struct md_HTTP_response* resp ) {
   
   char* serialized_reply = NULL;
   size_t serialized_reply_len = 0;
   int rc = 0;
   
   // serialize...
   rc = md_serialize< SG_messages::Reply >( reply, &serialized_reply, &serialized_reply_len );
   if( rc != 0 ) {
      
      // failed (invalid or OOM)
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // generate!
   return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, serialized_reply, serialized_reply_len );
}


// extract and verify a request's authenticity
// return 0 on success
// return -EINVAL if the message could not be parsed or verified 
// return -ENOMEM on OOM
// return -EAGAIN if we couldn't find the requester's certificate (i.e. we need to reload our config)
static int SG_request_message_parse( struct SG_gateway* gateway, SG_messages::Request* msg, char* msg_buf, size_t msg_sz ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // de-serialize 
   rc = md_parse< SG_messages::Request >( msg, msg_buf, msg_sz );
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      return rc;
   }
   
   // verify 
   rc = ms_client_verify_gateway_message< SG_messages::Request >( ms, msg->volume_id(), msg->src_gateway_id(), msg );
   if( rc != 0 ) {
      
      SG_error("ms_client_verify_gateway_message( from=%" PRIu64 " ) rc = %d\n", msg->src_gateway_id(), rc );
      return rc;
   }
   
   return 0;
}


// extract request info from the request message 
// return 0 on success
// return -ENOMEM if OOM
// NOTE: if request_msg encodes multiple blocks, only the first (block ID, block version) pair will be put into reqdat
static int SG_request_data_from_message( struct SG_request_data* reqdat, SG_messages::Request* request_msg ) {
   
   SG_request_data_init( reqdat );
   
   try {
      
      reqdat->fs_path = SG_strdup_or_null( request_msg->fs_path().c_str() );
   }
   catch( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
   
   reqdat->volume_id = request_msg->volume_id();
   reqdat->file_id = request_msg->file_id();
   reqdat->file_version = request_msg->file_version();
   reqdat->user_id = request_msg->user_id();
   
   if( request_msg->has_new_manifest_mtime_sec() && request_msg->has_new_manifest_mtime_nsec() ) {
      
      // manifest
      reqdat->manifest_timestamp.tv_sec = request_msg->new_manifest_mtime_sec();
      reqdat->manifest_timestamp.tv_nsec = request_msg->new_manifest_mtime_nsec();
   }
   else if( request_msg->blocks_size() > 0 ) {
      
      // put the first block in 
      reqdat->block_id = request_msg->blocks(0).block_id();
      reqdat->block_version = request_msg->blocks(0).block_version();
   }
   return 0;
}


// what are the capabilities required for a particular operation?
// return the bitwise OR of the cpability set 
// return (uint64_t)(-1) if the operation is not valid 
uint64_t SG_server_request_capabilities( uint64_t request_type ) {
   
   uint64_t caps_required = (uint64_t)(-1);         // all bits set
   
   switch( request_type ) {
      
      case SG_messages::Request::DETACH:
      case SG_messages::Request::RENAME: {
         
         caps_required = SG_CAP_WRITE_DATA | SG_CAP_WRITE_METADATA;
         break;
      }
      
      case SG_messages::Request::WRITE:
      case SG_messages::Request::TRUNCATE:
      case SG_messages::Request::DELETEBLOCK:
      case SG_messages::Request::PUTBLOCK: {
         
         caps_required = SG_CAP_WRITE_DATA;
         break;
      }
      
      default: {
         
         SG_error("Unknown request type %" PRIu64 "\n", request_type);
         break;
      }
   }
   
   return caps_required;
}


// verify that the sender of the given request has sufficiently capability for carrying out the requested operation.
// call this after parsing and validating the message.
// return 0 if so 
// return -EPERM if not permitted 
// return -EINVAL if the message is malformed, or came from outside the volume
// return -EAGAIN if the gateway is not known to us (indicates that we might have to reload our config)
int SG_server_check_capabilities( struct SG_gateway* gateway, SG_messages::Request* request ) {
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   uint64_t request_gateway_id = request->src_gateway_id();
   uint64_t request_user_id = request->user_id();
   uint64_t request_volume_id = request->volume_id();
   uint64_t request_type = request->request_type();
   
   uint64_t cert_user_id = (uint64_t)(-1);      // what user is running this gateway, according to our certs
   uint64_t cert_volume_id = (uint64_t)(-1);    // what volume this gateway is in, according to our certs 
   
   uint64_t required_caps = SG_server_request_capabilities( request_type );
   
   int rc = 0;
   
   // can only communicate with gateways in our volume
   if( request_volume_id != volume_id ) {
      
      SG_error("Invalid volume %" PRIu64 "; expected %" PRIu64 "\n", request_volume_id, volume_id );
      return -EINVAL;
   }
   
   // what volume is this gateway in?
   rc = ms_client_get_gateway_volume( ms, request_gateway_id, &cert_volume_id );
   if( volume_id != cert_volume_id ) {
      
      // volume mismatch
      SG_error("Invalid volume %" PRIu64 "; expected %" PRIu64 "\n", cert_volume_id, volume_id );
      return -EINVAL;
   }
   
   // what user runs this gateway?
   rc = ms_client_get_gateway_user( ms, request_gateway_id, &cert_user_id );
   if( request_user_id != cert_user_id ) {
      
      // user mismatch 
      SG_error("Invalid user %" PRIu64 "; expected %" PRIu64 "\n", cert_user_id, request_user_id );
      return -EINVAL;
   }
   
   // does this gatewy have the required capabilities?
   rc = ms_client_check_gateway_caps( ms, request_gateway_id, required_caps );
   if( rc != 0 ) {
      
      SG_error("ms_client_check_gateway_caps( %" PRIu64 ", %" PRIX64 " ) rc = %d\n", request_gateway_id, required_caps, rc );
      return rc;
   }
   
   // permitted
   return 0;
}

// start up an I/O request: suspend the connection and pass the request on to an I/O thread, for it to be dispatched asynchronously 
// reqdat and request_msg must be heap-allocated; the I/O subsystem will take ownership
// return 0 on success 
// return -ENOMEM on OOM 
int SG_server_HTTP_IO_start( struct SG_gateway* gateway, SG_server_IO_completion io_cb, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   struct SG_server_io* io = NULL;
   struct md_wreq wreq;
   
   memset( &wreq, 0, sizeof( struct md_wreq ) );
   
   io = SG_CALLOC( struct SG_server_io, 1 );
   if( io == NULL ) {
      
      return -ENOMEM;
   }
   
   io->gateway = gateway;
   io->reqdat = reqdat;
   io->request_msg = request_msg;
   io->con_data = con_data;
   io->resp = resp;
   io->io_completion = io_cb;
   
   // suspend the connection
   rc = md_HTTP_connection_suspend( con_data );
   if( rc != 0 ) {
   
      SG_error("md_HTTP_connection_suspend rc = %d\n", rc );
      
      SG_safe_free( io );
      return rc;
   }
   
   // enqueue the work
   rc = md_wreq_init( &wreq, SG_server_HTTP_IO_finish, io, 0 );
   if( rc != 0 ) {
      
      SG_error("md_wreq_init rc = %d\n", rc );
      
      md_HTTP_create_response_builtin( resp, 500 );
      md_HTTP_connection_resume( con_data, resp );
      SG_safe_free( io );
      return rc;
   }
   
   rc = SG_gateway_io_start( gateway, &wreq );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_io_start rc = %d\n", rc );
      
      md_HTTP_create_response_builtin( resp, 500 );
      md_HTTP_connection_resume( con_data, resp );
      SG_safe_free( io );
      return rc;
   }
   
   return 0;
}


// finish an I/O request: generate a response, resume the connection, and send it off
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on failure to generate a response
int SG_server_HTTP_IO_finish( struct md_wreq* wreq, void* cls ) {
   
   struct SG_server_io* io = (struct SG_server_io*)cls;
   int rc = 0;
   int io_rc = 0;
   SG_messages::Reply reply_msg;
   
   struct SG_gateway* gateway = io->gateway;
   struct md_HTTP_connection_data* con_data = io->con_data;
   struct md_HTTP_response* resp = io->resp;
   struct SG_request_data* reqdat = io->reqdat;
   SG_messages::Request* request_msg = io->request_msg;
   
   // run the operation
   io_rc = (*io->io_completion)( gateway, reqdat, request_msg, con_data );
   
   // generate response 
   rc = SG_server_reply_setup( gateway, &reply_msg, request_msg->message_nonce(), io_rc );
   if( rc != 0 ) {
      
      // failed to set up
      SG_error("SG_server_reply_setup rc = %d\n", rc );
      md_HTTP_create_response_builtin( resp, 500 );
   }
   
   else {
      
      rc = SG_server_reply_serialize( gateway, &reply_msg, resp );
      if( rc != 0 ) {
         
         // failed to serialize 
         SG_error("SG_server_reply_serialize rc = %d\n", rc );
         md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // resume the connection so we can send back the response
   rc = md_HTTP_connection_resume( con_data, resp );
   if( rc != 0 ) {
      
      SG_error("md_HTTP_connection_resume rc = %d\n", rc );
   }
   
   // done with these data
   SG_request_data_free( reqdat );
   SG_safe_free( reqdat );
   
   SG_safe_delete( request_msg );
   SG_safe_free( io );
   
   return rc;
}


// handle a WRITE request
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL if the WRITE fields in request_msg are missing or malformed
// return -ENOSYS if there is no impl_patch_manifest method defined
static int SG_server_HTTP_POST_WRITE( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   struct SG_manifest write_delta;
   
   // sanity check 
   if( gateway->impl_patch_manifest == NULL ) {
      
      return -ENOSYS;
   }
   
   // santiy check: must have >0 blocks 
   if( request_msg->blocks_size() == 0 ) {
      
      return -EINVAL;
   }
   
   memset( &write_delta, 0, sizeof( struct SG_manifest ) );
   
   rc = SG_manifest_init( &write_delta, reqdat->volume_id, request_msg->coordinator_id(), reqdat->file_id, reqdat->file_version );
   if( rc != 0 ) {
      
      // OOM
      return rc;
   }
   
   // construct the write delta 
   for( int i = 0; i < request_msg->blocks_size(); i++ ) {
      
      // next block 
      struct SG_manifest_block block;
      memset( &block, 0, sizeof( struct SG_manifest_block ) );
      
      // load block
      rc = SG_manifest_block_load_from_protobuf( &block, &request_msg->blocks(i) );
      if( rc != 0 ) {
         
         // OOM or invalid
         SG_manifest_free( &write_delta );
         return rc;
      }
      
      // insert block 
      rc = SG_manifest_put_block_nocopy( &write_delta, &block, true );
      if( rc != 0 ) {
         
         // OOM 
         SG_manifest_block_free( &block );
         SG_manifest_free( &write_delta );
         return rc;
      }
   }
   
   // apply the write delta 
   rc = SG_gateway_impl_manifest_patch( gateway, reqdat, &write_delta );
   
   SG_manifest_free( &write_delta );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_impl_manifest_patch( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
   }
   
   return rc;
}


// handle a TRUNCATE request
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the new size field is missing
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_TRUNCATE( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   uint64_t new_size = 0;
   
   // sanity check 
   if( gateway->impl_truncate == NULL ) {
      
      return -ENOSYS;
   }
   
   // sanity check--must have new_size 
   if( !request_msg->has_new_size() ) {
      
      return -EINVAL;
   }
   
   new_size = request_msg->new_size();
   
   // do the truncate 
   rc = SG_gateway_impl_truncate( gateway, reqdat, new_size );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_truncate( %" PRIX64 ".%" PRId64 " (%s), %" PRIu64 " ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, new_size, rc );
   }
   
   return rc;
}


// handle a RENAME request 
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the new path field is missing 
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_RENAME( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   char* new_path;      // guarantee NULL-terminated
   
   // sanity check 
   if( gateway->impl_rename == NULL ) {
      
      return -ENOSYS;
   }
   
   // sanity check--must have new path 
   if( !request_msg->has_new_fs_path() ) {
      
      return -EINVAL;
   }
   
   new_path = SG_CALLOC( char, request_msg->new_fs_path().size() + 1 );
   if( new_path == NULL ) {
      
      return -ENOMEM;
   }
   
   strncpy( new_path, request_msg->new_fs_path().c_str(), request_msg->new_fs_path().size() );
   
   // do the rename 
   rc = SG_gateway_impl_rename( gateway, reqdat, new_path );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_impl_rename( %" PRIX64 ".%" PRId64 " (%s), %s ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, new_path, rc );
   }
   
   SG_safe_free( new_path );
   
   return rc;
}

// handle a DETACH request 
// return 0 on success 
// return -ENOMEM on OOM 
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_DETACH( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   
   // sanity check 
   if( gateway->impl_detach == NULL ) {
      
      return -ENOSYS;
   }
   
   rc = SG_gateway_impl_detach( gateway, reqdat );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_detach( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
   }
   
   return rc;
}


// handle a DELETEBLOCK request: suspend the connection and handle it in an I/O thread.
// return 0 on success 
// return -ENOMEM on OOM 
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_DELETEBLOCK( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   
   // sanity check 
   if( gateway->impl_delete_block == NULL ) {
      
      return -ENOSYS;
   }
   
   if( !SG_request_is_block( reqdat ) ) {
      
      return -EINVAL;
   }
   
   rc = SG_gateway_impl_block_delete( gateway, reqdat );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_delete_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
   }
   
   return rc;
}

// handle a PUTBLOCK request: suspend the connection and handle it in an I/O thread.
// return 0 on success 
// return -ENOSYS if not implemented 
// return -EINVAL if the request does not contain block information
// return -EBADMSG if the block's hash does not match the hash given on the control plane
// return -errno on fchmod, mmap failure
static int SG_server_HTTP_POST_PUTBLOCK( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   int block_fd = 0;
   char* block_mmap = NULL;
   struct SG_chunk block;
   struct stat sb;
   uint64_t block_id = SG_INVALID_BLOCK_ID;
   int64_t block_version = -1;
   struct SG_manifest_block block_info;
   unsigned char* block_hash = NULL;
   
   // sanity check 
   if( gateway->impl_put_block == NULL ) {
      
      return -ENOSYS;
   }
   
   // sanity check: must have one block 
   if( request_msg->blocks_size() != 1 ) {
      
      return -EINVAL;
   }
   
   // get the block 
   rc = SG_manifest_block_load_from_protobuf( &block_info, &request_msg->blocks(0) );
   if( rc != 0 ) {
      
      SG_error("SG_manifest_block_load_from_protobuf rc = %d\n", rc );
      return rc;
   }
   
   // sanity check: hash required 
   if( block_info.hash == NULL ) {
      
      SG_error("%s", "SG_manifest_block.hash == NULL\n");
      
      SG_manifest_block_free( &block_info );
      return -EINVAL;
   }
   
   // get block ID and version 
   block_id = request_msg->blocks(0).block_id();
   block_version = request_msg->blocks(0).block_version();
   
   // fetch the block from the request 
   rc = md_HTTP_upload_get_field_tmpfile( con_data, SG_SERVER_POST_FIELD_DATA_PLANE, NULL, &block_fd );
   if( rc != 0 ) {
      
      SG_error("md_HTTP_upload_get_field_buffer( '%s' ) rc = %d\n", SG_SERVER_POST_FIELD_DATA_PLANE, rc );
      return rc;
   }
   
   // read-only...
   rc = fchmod( block_fd, 0400 );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fchmod rc = %d\n", rc );
      return rc;
   }
   
   rc = fstat( block_fd, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fstat rc = %d\n", rc );
      return rc;
   }
   
   // map the block into RAM 
   block_mmap = (char*)mmap( NULL, sb.st_size, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, block_fd, 0 );
   if( block_mmap == NULL ) {
      
      rc = -errno;
      SG_error("mmap rc = %d\n", rc );
      return rc;
   }
   
   // hash of block...
   block_hash = sha256_hash_data( block_mmap, sb.st_size );
   if( block_hash == NULL ) {
      
      rc = munmap( block_mmap, sb.st_size );
      if( rc != 0 ) {
         
         rc = -errno;
         SG_error("munmap rc = %d\n", rc );
      }
      
      SG_safe_free( block_hash );
      return -ENOMEM;
   }
   
   // integrity error?
   if( sha256_cmp( block_hash, block_info.hash ) != 0 ) {
      
      char expected[ 2*SG_BLOCK_HASH_LEN + 1 ];
      char actual[ 2*SG_BLOCK_HASH_LEN + 1 ];
      
      memset( expected, 0, 2*SG_BLOCK_HASH_LEN + 1 );
      memset( actual, 0, 2*SG_BLOCK_HASH_LEN + 1 );
      
      md_sprintf_data( expected, block_info.hash, block_info.hash_len );
      md_sprintf_data( actual, block_hash, SG_BLOCK_HASH_LEN );
      
      SG_error("%" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "]: expected '%s', got '%s'\n", reqdat->file_id, reqdat->file_version, block_id, block_version, expected, actual );
      
      // clean up 
      rc = munmap( block_mmap, sb.st_size );
      if( rc != 0 ) {
         
         rc = -errno;
         SG_error("munmap rc = %d\n", rc );
      }
      
      SG_safe_free( block_hash );
      return -EBADMSG;
   }
   
   SG_safe_free( block_hash );
   
   // set up a chunk 
   SG_chunk_init( &block, block_mmap, sb.st_size );
   
   // call into the implementation 
   rc = SG_gateway_impl_block_put( gateway, reqdat, &block );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_impl_block_put( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
   }
   
   int unmap_rc = munmap( block_mmap, sb.st_size );
   
   if( unmap_rc != 0 ) {
      
      unmap_rc = -errno;
      SG_error("munmap rc = %d\n", unmap_rc );
   }
   
   return rc;
}


// handle a POST.  Extract the message and let the implementation handle it asynchronously.  Suspend the connection
// return 0 on success
// return -ENOMEM on OOM 
int SG_server_HTTP_POST_finish( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   struct SG_server_connection* sgcon = (struct SG_server_connection*)con_data->cls;
   
   // unpack the connection and gateway info
   struct SG_request_data* reqdat = NULL;
   struct SG_gateway* gateway = sgcon->gateway;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   // request data
   SG_messages::Request* request_msg = SG_safe_new( SG_messages::Request );
   if( request_msg == NULL ) {
      
      return -ENOMEM;
   }
   
   // request information
   reqdat = SG_CALLOC( struct SG_request_data, 1 );
   if( reqdat == NULL ) {
      
      SG_safe_delete( request_msg );
      return -ENOMEM;
   }
   
   // serialized request that we got
   char* request_message_str = NULL;
   size_t request_message_len = 0;
   
   int rc = 0;
   
   // sanity check--we'll need to ensure certain methods are defined 
   if( gateway->impl_stat == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_stat is not defined\n");
      
      SG_safe_delete( request_msg );
      SG_safe_free( reqdat );
      
      return md_HTTP_create_response_builtin( resp, 501 );
   }
   
   // get the control-plane component of the data 
   rc = md_HTTP_upload_get_field_buffer( con_data, SG_SERVER_POST_FIELD_CONTROL_PLANE, &request_message_str, &request_message_len );
   if( rc != 0 ) {
      
      SG_safe_delete( request_msg );
      SG_safe_free( reqdat );
      
      // failed to process control-plane
      SG_error("md_HTTP_upload_get_field_buffer( '%s' ) rc = %d\n", SG_SERVER_POST_FIELD_CONTROL_PLANE, rc );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // parse and verify the request
   rc = SG_request_message_parse( gateway, request_msg, request_message_str, request_message_len );
   SG_safe_free( request_message_str );
   
   if( rc != 0 ) {
      
      SG_safe_delete( request_msg );
      SG_safe_free( reqdat );
      
      // failed to parse and verify control-plane message 
      SG_error("SG_request_message_parse( '%s' ) rc = %d\n", SG_SERVER_POST_FIELD_CONTROL_PLANE, rc );
      
      if( rc == -EAGAIN ) {
         
         // certificate is not on file.  Try to get it and have the requester try again 
         SG_gateway_start_reload( gateway );
         return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
      }
      else if( rc == -EBADMSG || rc == -EINVAL ) {
         
         // bad message
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      else {
         
         // some other error
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // request is legitimate
   // verify capabilities 
   rc = SG_server_check_capabilities( gateway, request_msg );
   if( rc != 0 ) {
      
      SG_safe_delete( request_msg );
      SG_safe_free( reqdat );
      
      if( rc == -EAGAIN ) {
         
         // only failed since we're missing the certificate.
         // try to get it and have the requester try again 
         SG_gateway_start_reload( gateway );
         return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
      }
      
      else {
         
         // not permitted 
         return md_HTTP_create_response_builtin( resp, 403 );
      }
   }
   
   // request is legitimate and allowed
   // look for the hint to reload the config
   rc = ms_client_need_reload( ms, volume_id, request_msg->volume_version(), request_msg->cert_version() );
   if( rc < 0 ) {
      
      // log, but mask 
      SG_warn( "ms_client_need_reload( %" PRIu64 ", %" PRIu64 ", %" PRIu64 " ) rc = %d\n", volume_id, request_msg->volume_version(), request_msg->cert_version(), rc );
      rc = 0;
   }
   else if( rc > 0 ) {
      
      SG_safe_delete( request_msg );
      SG_safe_free( reqdat );
      
      // yup, need a reload 
      SG_gateway_start_reload( gateway );
      return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
   }
   
   // extract request info from the request 
   rc = SG_request_data_from_message( reqdat, request_msg );
   if( rc != 0 ) {
      
      // failed 
      SG_safe_free( reqdat );
      SG_safe_delete( request_msg );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // dispatch operation 
   switch( request_msg->request_type() ) {
      
      case SG_messages::Request::WRITE: {
         
         // redirect? expect world-writeable or volume-writeable
         rc = SG_server_redirect_request( gateway, resp, reqdat, 0055 );
         if( rc <= 0 ) {
            
            if( rc < 0 ) {
               SG_error("SG_server_redirect_request rc = %d\n", rc );
            }
         }
         else {
            
            // start write 
            rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_WRITE, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( WRITE(%" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::TRUNCATE: {
         
         // accessible? expect world-writeable or volume-writeable
         rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
         if( rc <= 0 ) {
            
            if( rc < 0 ) {
               SG_error("SG_server_stat_request rc = %d\n", rc );
            }
         }
         else {
         
            // start truncate 
            rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_TRUNCATE, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_START( TRUNCATE( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
            }
         }
            
         break;
      }
      
      case SG_messages::Request::RENAME: {
         
         // accessible? expect world-writeable or volume-writeable
         rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
         if( rc <= 0 ) {
            
            if( rc < 0 ) {
               SG_error("SG_server_stat_request rc = %d\n", rc );
            }
         }
         else {
            
            // start rename 
            rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_RENAME, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( RENAME( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::DETACH: {
         
         // accessible? expect world-writeable or volume-writeable
         rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
         if( rc <= 0 ) {
            
            if( rc < 0 ) {
               SG_error("SG_server_stat_request rc = %d\n", rc );
            }
         }
         else {
               
            // start detach
            rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_DETACH, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( DETACH( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::DELETEBLOCK: {
         
         // accessible? expect world-writable or volume-writable 
         rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
         if( rc <= 0 ) {
            
            if( rc < 0 ) {
               SG_error("SG_server_stat_request rc = %d\n", rc );
            }
         }
         else {
            
            // start delete block request 
            rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_DELETEBLOCK, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( DELETEBLOCK( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s)) ) rc = %d\n",
                         reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::PUTBLOCK: {
         
         // block request 
         // NOTE: no request redirection here--it's possible for blocks to arrive out-of-order.  the implementation may choose to do so, however
         rc = SG_server_HTTP_IO_start( gateway, SG_server_HTTP_POST_PUTBLOCK, reqdat, request_msg, con_data, resp );
         if( rc != 0 ) {
            
            SG_error("SG_server_HTTP_IO_start( PUTBLOCK( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
         }
         
         break;
      }
      
      default: {
         
         SG_error("Unknown request type '%d'\n", request_msg->request_type() );
         
         rc = md_HTTP_create_response_builtin( resp, 501 );
         
         break;
      }
   }

   if( rc < 0 ) {
         
      // only clean up on error; otherwise the I/O completion logic will handle it
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      SG_safe_delete( request_msg );
   }
   
   return rc;
}


// clean up a connection
// if this was a block GET request, and we streamed data back, then close up shop
void SG_server_HTTP_cleanup( void *cls ) {
   
   struct SG_server_connection* sgcon = (struct SG_server_connection*)cls;
   
   SG_safe_free( sgcon );
}


// initialize an HTTP server with this server's methods
// always succeeds
int SG_server_HTTP_install_handlers( struct md_HTTP* http ) {
   
   md_HTTP_connect( *http, SG_server_HTTP_connect );
   md_HTTP_GET( *http, SG_server_HTTP_GET_handler );
   md_HTTP_HEAD( *http, SG_server_HTTP_HEAD_handler );
   md_HTTP_POST_finish( *http, SG_server_HTTP_POST_finish );
   md_HTTP_close( *http, SG_server_HTTP_cleanup );
   
   // install special field handlers 
   md_HTTP_post_field_handler( *http, SG_SERVER_POST_FIELD_CONTROL_PLANE, md_HTTP_post_field_handler_ram );
   md_HTTP_post_field_handler( *http, SG_SERVER_POST_FIELD_DATA_PLANE, md_HTTP_post_field_handler_disk );
   
   return 0;
}

