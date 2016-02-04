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


// translate a stat error into an HTTP response 
// return the result of md_HTTP_create_response_builtin 
static int SG_server_stat_fail_response( int rc, struct md_HTTP_response* resp ) {

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


// stat a file given its request info, and set up an HTTP response with the appropriate failure code if it fails 
// return 0 if we handled the failure 
// return 1 if there was no failure to handle
// return negative on error 
static int SG_gateway_impl_stat_or_fail( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode ) {
   
   int rc = 0;
   
   // do the stat
   rc = SG_gateway_impl_stat( gateway, reqdat, entity_info, mode );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_impl_stat rc = %d\n", rc ); 
      return SG_server_stat_fail_response( rc, resp );

   } 
   return 1;
}


// stat a file's block given its request info, and set up an HTTP response with the appropriate failure code if it fails 
// return 0 if we handled the failure 
// return 1 if there was no failure to handle
// return negative on error 
static int SG_gateway_impl_stat_block_or_fail( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, struct SG_request_data* entity_info, mode_t* mode ) {
   
   int rc = 0;
   
   // do the stat
   rc = SG_gateway_impl_stat_block( gateway, reqdat, entity_info, mode );
   if( rc != 0 ) {
     
      SG_error("SG_gateway_impl_stat_block rc = %d\n", rc ); 
      return SG_server_stat_fail_response( rc, resp );
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
      SG_debug("%" PRIu64 ": 0%o & 0%o == 0\n", reqdat->file_id, entity_mode, mode );
      return md_HTTP_create_response_builtin( resp, 403 );
   }
   
   // not handled, but okay for caller to handle
   return 1;
}


// early sanity-check on inbound requests:
// * accept a request if the gateway imposes no request rejection policy--i.e. all requests are considered sound.
// * reject a request if the requested entity is not found, or does not have the requisite permissions.
// * redirect a request if the request refers to a stale version of the entity.
// redirect a request if it is to a stale version, or to a file we don't coordinate
// return 0 if handled 
// return 1 if not handled, but the request is sound (i.e. the caller should service it)
// return negative on error 
static int SG_server_redirect_request( struct SG_gateway* gateway, struct md_HTTP_response* resp, struct SG_request_data* reqdat, mode_t mode ) {
   
   int rc = 0;
   struct SG_request_data entity_info;
   struct SG_request_data block_info;
   uint64_t gateway_id = SG_gateway_id( gateway );
   mode_t entity_mode = 0;
   mode_t block_mode = 0;
   bool set_block_mode = false;
   
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );

   memset( &entity_info, 0, sizeof(struct SG_request_data) );
   memset( &block_info, 0, sizeof(struct SG_request_data) );
   
   char* url = NULL;

   if( !SG_request_is_block( reqdat ) && !SG_request_is_manifest( reqdat ) && !SG_request_is_getxattr( reqdat ) && !SG_request_is_listxattr( reqdat ) ) {
      
      SG_error("%s", "Invalid request\n");
      
      // invalid request 
      return md_HTTP_create_response_builtin( resp, 400 );
   }
   
   if( gateway->impl_stat == NULL ) {
     
      // accept by default 
      return 1; 
   }
   
   // stat the requested entity 
   rc = SG_gateway_impl_stat_or_fail( gateway, resp, reqdat, &entity_info, &entity_mode );
   if( rc <= 0 ) {
      
      // handled or error
      return rc;
   }
   
   rc = 1;
   
   // redirect block?
   if( SG_request_is_block( reqdat ) ) {
      
      // block_request 
      if( gateway_id != entity_info.coordinator_id ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", entity_info.coordinator_id, gateway_id );
         
         // redirect block request to remote coordinator
         rc = md_url_make_block_url( ms, entity_info.fs_path, entity_info.coordinator_id, entity_info.file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, &url );
      }
      
      else if( reqdat->file_version != entity_info.file_version ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: File version mismatch: expected %" PRId64 ", got %" PRId64 "\n", entity_info.file_version, reqdat->file_version );
         
         // redirect block request to latest version 
         url = md_url_public_block_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, entity_info.block_id, entity_info.block_version );
         
         if( url == NULL ) {
            
            // OOM
            rc = -ENOMEM;
         }
      }

      if( gateway->impl_stat_block != NULL ) {

         rc = SG_gateway_impl_stat_block_or_fail( gateway, resp, reqdat, &block_info, &block_mode );
         if( rc <= 0 ) {
            
            // handled or error 
            return rc;
         }

         set_block_mode = true;
         rc = 1;
         if( reqdat->block_version != block_info.block_version ) {

            rc = 0;
            SG_debug("REDIRECT: Block/version mismatch: expected version=%" PRId64 ", block=%" PRIu64 ".%" PRId64 ", got version=%" PRId64 ", block=%" PRIu64 ".%" PRId64 "\n",
                     block_info.file_version, block_info.block_id, block_info.block_version,
                     reqdat->file_version, reqdat->block_id, reqdat->block_version );
                     
            // redirect block request to newer local version 
            url = md_url_public_block_url( conf->content_url, volume_id, block_info.fs_path, block_info.file_id, block_info.file_version, block_info.block_id, block_info.block_version );
            
            if( url == NULL ) {
               
               // OOM
               rc = -ENOMEM;
            }
         }

         SG_request_data_free( &block_info );
      }
   }

   else if( SG_request_is_manifest( reqdat ) ) {
      
      // manifest request 
      if( gateway_id != entity_info.coordinator_id ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", entity_info.coordinator_id, gateway_id );
         
         // redirect manifest request to remote coordinator
         rc = md_url_make_manifest_url( ms, entity_info.fs_path, entity_info.coordinator_id, entity_info.file_id, reqdat->file_version, &reqdat->manifest_timestamp, &url );
      }
      
      else if( reqdat->file_version != entity_info.file_version ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: File version mismatch: expected %" PRId64 ", got %" PRId64 "\n", entity_info.file_version, reqdat->file_version );
         
         // redirect manifest request to latest version 
         url = md_url_public_manifest_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, &entity_info.manifest_timestamp );
         
         if( url == NULL ) {
            
            // OOM 
            rc = -ENOMEM;
         }
      }
      
      else if(reqdat->manifest_timestamp.tv_sec < entity_info.manifest_timestamp.tv_sec ||       // manifest request, and timestamps are behind current
             (reqdat->manifest_timestamp.tv_sec == entity_info.manifest_timestamp.tv_sec && reqdat->manifest_timestamp.tv_nsec < entity_info.manifest_timestamp.tv_nsec) ) {
         
         rc = 0;
      
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
   
   else if( reqdat->xattr_name != NULL ) {
      
      // getxattr, setxattr, or removexattr request 
      if( gateway_id != entity_info.coordinator_id ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: Coordinator mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", entity_info.coordinator_id, gateway_id );
         
         // redirect getxattr request to remote coordinator
         rc = md_url_make_getxattr_url( ms, entity_info.fs_path, entity_info.coordinator_id, entity_info.file_id, reqdat->file_version, reqdat->xattr_name, reqdat->xattr_nonce, &url );
      }
      
      else if( reqdat->file_version != entity_info.file_version ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: File version mismatch: expected %" PRId64 ", got %" PRId64 "\n", entity_info.file_version, reqdat->file_version );
         
         // redirect getxattr request to latest version 
         url = md_url_public_getxattr_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, reqdat->xattr_name, reqdat->xattr_nonce );
         
         if( url == NULL ) {
            
            // OOM 
            rc = -ENOMEM;
         }
      }
      
      else if( reqdat->xattr_nonce != entity_info.xattr_nonce ) {
         
         rc = 0;
         
         SG_debug("REDIRECT: xattr nonce mismatch: expected %" PRId64 ", got %" PRId64 "\n", entity_info.xattr_nonce, reqdat->xattr_nonce );
         
         // redirect getxattr request to latest version 
         url = md_url_public_getxattr_url( conf->content_url, volume_id, entity_info.fs_path, entity_info.file_id, entity_info.file_version, reqdat->xattr_name, reqdat->xattr_nonce );
         
         if( url == NULL ) {
            
            // OOM 
            rc = -ENOMEM;
         }
      }
   }

   else if( !SG_request_is_listxattr( reqdat ) ) {
      
      // invalid request
      if( url != NULL ) {
         SG_safe_free( url );
      }
      
      SG_request_data_free( &entity_info );
      
      return md_HTTP_create_response_builtin( resp, 400 );
   }
   
   if( rc < 0 ) {
      
      // failure 
      if( url != NULL ) {
         SG_safe_free( url );
      }
      
      SG_request_data_free( &entity_info );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   if( rc == 0 ) {
      
      // will redirect
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
   
   else {
      
      // will not redirect
      // permission check 
      if( (entity_mode & mode) == 0 ) {
         
         // denied 
         SG_safe_free( url );
         SG_request_data_free( &entity_info );
         
         SG_debug("%" PRIu64 ": 0%o & 0%o == 0\n", reqdat->file_id, entity_mode, mode );
         return md_HTTP_create_response_builtin( resp, 403 );
      }

      if( set_block_mode && (block_mode & mode) == 0 ) {

         // denied 
         SG_safe_free( url );
         SG_request_data_free( &entity_info );

         SG_debug("%" PRIu64 "(block): 0%o & 0%o == 0\n", reqdat->file_id, block_mode, mode );
         return md_HTTP_create_response_builtin( resp, 403 );
      }
      
      // request is sound, and refers to fresh data 
      if( url != NULL ) {
         SG_safe_free( url );
      }
      SG_request_data_free( &entity_info );
      
      return 1;
   }
}


// populate a reply message
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL on failure to serialize and sign
static int SG_server_reply_populate( struct SG_gateway* gateway, SG_messages::Reply* reply, uint64_t message_nonce, int error_code ) {
   
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


// sign a reply message
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL on failure to serialize and sign
static int SG_server_reply_sign( struct SG_gateway* gateway, SG_messages::Reply* reply ) {
   
   int rc = 0;
   EVP_PKEY* gateway_private_key = SG_gateway_private_key( gateway );
   
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
     
      SG_error("SG_request_data_parse rc = %d\n", rc ); 
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


// GET an xattr
// return 0 on success 
// return -ENOMEM on OOM
int SG_server_HTTP_GET_getxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* ignored, struct md_HTTP_connection_data* ignored2, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   SG_messages::Reply reply;
   struct SG_chunk xattr_value;
   
   // getxattr request 
   SG_debug("GETXATTR %" PRIX64 ".%" PRId64 " (%s) %s.%" PRId64 "\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, reqdat->xattr_name, reqdat->xattr_nonce );
   
   // do the getxattr in the implementation
   rc = SG_gateway_impl_getxattr( gateway, reqdat, &xattr_value );
   if( rc < 0 ) {
      
      SG_error("SG_gateway_impl_getxattr rc = %d\n", rc );
      if( rc == -ENOENT ) {
         
         // not present 
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         
         // general failure 
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   else {
      
      // success!
      // put it into a reply 
      rc = SG_server_reply_populate( gateway, &reply, 0, 0 );
      if( rc != 0 ) {
         
         SG_error("SG_server_reply_populate rc = %d\n", rc );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      try {
         // add our xattr 
         reply.set_xattr_value( string(xattr_value.data, xattr_value.len) );
      }
      catch( bad_alloc& ba ) {
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      // serialize and send off
      rc = SG_server_reply_serialize( gateway, &reply, resp );
      if( rc != 0 ) {
         
         SG_error("SG_server_reply_serialize rc = %d\n", rc );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      else {
         
         return rc;
      }
   }
}


// GET the list of xattrs
// return 0 on success 
// return -ENOMEM on OOM
int SG_server_HTTP_GET_listxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* ignored, struct md_HTTP_connection_data* ignored2, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   SG_messages::Reply reply;
   struct SG_chunk* xattr_names = NULL;
   size_t num_xattrs = 0;
   
   // getxattr request 
   SG_debug("LISTXATTR %" PRIX64 ".%" PRId64 " (%s)\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path );
   
   // do the getxattr in the implementation
   rc = SG_gateway_impl_listxattr( gateway, reqdat, &xattr_names, &num_xattrs );
   if( rc < 0 ) {
      
      SG_error("SG_gateway_impl_listxattr rc = %d\n", rc );
      if( rc == -ENOENT ) {
         
         // not present 
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         
         // general failure 
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   else {
      
      // success!
      // put it into a reply 
      rc = SG_server_reply_populate( gateway, &reply, 0, 0 );
      if( rc != 0 ) {
         
         SG_error("SG_server_reply_populate rc = %d\n", rc );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      try {
         // add our xattrs
         for( size_t i = 0; i < num_xattrs; i++ ) {
            
            reply.add_xattr_names( string(xattr_names[i].data, xattr_names[i].len) );
         }
      }
      catch( bad_alloc& ba ) {
         
         for( size_t i = 0; i < num_xattrs; i++ ) {
            
            SG_chunk_free( &xattr_names[i] );
         }
         
         SG_safe_free( xattr_names );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      // serialize and send off
      rc = SG_server_reply_serialize( gateway, &reply, resp );
      
      // free memory
      for( size_t i = 0; i < num_xattrs; i++ ) {
            
         SG_chunk_free( &xattr_names[i] );
      }
      
      SG_safe_free( xattr_names );
      
      if( rc != 0 ) {
         
         SG_error("SG_server_reply_serialize rc = %d\n", rc );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      else {
         
         return rc;
      }
   }
}

// GET a block, as part of an I/O complection.
// try the cache first, then the implementation.
// on cache miss, run the block through the "put block" driver method and cache it for next time.
// return 0 on success
// return -ENOMEM on OOM
int SG_server_HTTP_GET_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* ignored, struct md_HTTP_connection_data* ignored2, struct md_HTTP_response* resp ) {
   
   int rc = 0;

   // block request 
   struct SG_chunk block;
   struct SG_chunk block_dup;
   struct md_cache_block_future* block_fut = NULL;
   struct SG_IO_hints io_hints;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   
   memset( &block, 0, sizeof( struct SG_chunk ) );
   
   // sanity check 
   if( gateway->impl_get_block == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_get_blocks is undefined\n");
      
      // not implemented 
      return md_HTTP_create_response_builtin( resp, 500 );
   }

   // get raw block from the cache?
   rc = SG_gateway_cached_block_get_raw( gateway, reqdat, &block );
   
   if( rc == 0 ) {
      
      // reply 
      return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, block.data, block.len );
   }
   else if( rc != -ENOENT ) {
      
      // error 
      SG_warn("SG_gateway_cached_block_get_raw( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, rc );
   }

   // cache miss 
   SG_debug("CACHE MISS %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "]\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version );
   
   // get raw block from the implementation, but don't deserialize
   SG_IO_hints_init( &io_hints, SG_IO_READ, reqdat->block_id * block_size, block_size );
   SG_request_data_set_IO_hints( reqdat, &io_hints );

   rc = SG_gateway_impl_block_get( gateway, reqdat, &block, 0 );
   if( rc < 0 ) {
      
      if( rc == -ENOENT ) {
         
         // not present (i.e. EOF)
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         
         // general failure
         SG_error("SG_gateway_cached_block_get_raw( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, rc );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // duplicate the block--give one to the cache, and send one back 
   rc = SG_chunk_dup( &block_dup, &block );
   if( rc != 0 ) {
      
      // OOM 
      SG_chunk_free( &block );
      return md_HTTP_create_response_builtin( resp, 503 );
   }
   
   // cache the raw block, asynchronously 
   rc = SG_gateway_cached_block_put_raw_async( gateway, reqdat, &block, SG_CACHE_FLAG_DETACHED | SG_CACHE_FLAG_UNSHARED, &block_fut );
   if( rc == -EEXIST ) {

      // this is okay--the block is already present
      rc = 0;
      SG_chunk_free( &block );
   }
   if( rc < 0 ) {
      
      // failure 
      SG_chunk_free( &block );
      SG_error("SG_gateway_cached_block_put_raw_async rc = %d\n", rc );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, block_dup.data, block_dup.len );
}



// GET a manifest, as part of an I/O completion
// try the cache first, then the implementation.
// on cache miss, run the serialized signed manifest through the "put manifest" driver method and cache it for next time.
// return 0 on success
// return -ENOMEM on OOM
int SG_server_HTTP_GET_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* ignored, struct md_HTTP_connection_data* ignored2, struct md_HTTP_response* resp ) {
   
   int rc = 0;

   // manifest request 
   struct SG_chunk raw_serialized_manifest;  // serialized manifest from out of the cache
   struct SG_chunk protobufed_manifest;      // unserialized manifest, as a protobuf str
   struct SG_chunk serialized_manifest;      // final manifest to cache
   struct SG_chunk serialized_manifest_resp; // response to send

   memset( &raw_serialized_manifest, 0, sizeof(struct SG_chunk) );
   memset( &protobufed_manifest, 0, sizeof(struct SG_chunk) );
   memset( &serialized_manifest, 0, sizeof(struct SG_chunk) );
   memset( &serialized_manifest_resp, 0, sizeof(struct SG_chunk) );
   
   struct SG_manifest manifest;              // from the implementation
   SG_messages::Manifest manifest_message;
   
   char* protobuf_manifest_str = NULL;
   size_t protobuf_manifest_len = 0;
   
   struct md_cache_block_future* manifest_fut = NULL;
   
   EVP_PKEY* gateway_private_key = SG_gateway_private_key( gateway );

   SG_IO_hints io_hints;
   
   // sanity check 
   if( gateway->impl_get_manifest == NULL ) {
      
      SG_error("%s", "BUG: gateway->impl_get_manifest is undefined\n");
      
      // not implemented 
      return md_HTTP_create_response_builtin( resp, 501 );
   }

   // try the cache
   rc = SG_gateway_cached_manifest_get_raw( gateway, reqdat, &raw_serialized_manifest );
   
   if( rc == 0 ) {
      
      // reply
      return md_HTTP_create_response_ram_nocopy( resp, "application/octet-stream", 200, raw_serialized_manifest.data, raw_serialized_manifest.len );
   }
   else if( rc != -ENOENT ) {
      
      // error 
      SG_warn("SG_gateway_cached_manifest_get_raw( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
   }
   
   SG_debug("CACHE MISS %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] (rc = %d)\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
   rc = 0;
   
   // cache miss 
   // get from the implementation 
   memset( &manifest, 0, sizeof(struct SG_manifest) );
   
   // get the manifest 
   SG_IO_hints_init( &io_hints, SG_IO_READ, 0, 0 );
   SG_request_data_set_IO_hints( reqdat, &io_hints );

   rc = SG_gateway_impl_manifest_get( gateway, reqdat, &manifest, 0 );
   if( rc != 0 ) {
     
      SG_error("SG_gateway_impl_manifest_get( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc ); 
      if( rc == -ENOENT ) {
         
         // not found 
         return md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
            
         // failed 
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // serialize to string
   rc = SG_manifest_serialize_to_protobuf( &manifest, &manifest_message );
   SG_manifest_free( &manifest );
   
   if( rc != 0 ) {
   
      // failed 
      SG_error("SG_manifest_serialize_to_protobuf( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }

   // sign manifest 
   rc = md_sign< SG_messages::Manifest >( gateway_private_key, &manifest_message );
   if( rc != 0 ) {
      
      // failed to sign 
      SG_error("md_sign( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // serialize to string (with signature) 
   rc = md_serialize< SG_messages::Manifest >( &manifest_message, &protobuf_manifest_str, &protobuf_manifest_len );
   if( rc != 0 ) {
      
      SG_error("md_serialize( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
      
      return md_HTTP_create_response_builtin( resp, 500 );
   }

   // feed through the gateway's serializer (if given)
   SG_chunk_init( &protobufed_manifest, protobuf_manifest_str, protobuf_manifest_len );
   rc = SG_gateway_impl_serialize( gateway, reqdat, &protobufed_manifest, &serialized_manifest );
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_serialize( %" PRIX64 ".%" PRId64 "[manifest %" PRId64 ".%ld] ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, rc );
   
      SG_chunk_free( &protobufed_manifest );      
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   else {

      // no need for the protobuf'ed form
      SG_chunk_free( &protobufed_manifest );
   }

   // duplicate--send one back, and send the other to the cache
   rc = SG_chunk_dup( &serialized_manifest_resp, &serialized_manifest );
   if( rc != 0 ) {
      
      // OOM 
      SG_chunk_free( &serialized_manifest );
      SG_error("SG_chunk_dup rc = %d\n", rc );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // cache (asynchronously)
   // cache takes ownership of the memory 
   rc = SG_gateway_cached_manifest_put_raw_async( gateway, reqdat, &serialized_manifest, SG_CACHE_FLAG_DETACHED | SG_CACHE_FLAG_UNSHARED, &manifest_fut );
   if( rc == -EEXIST ) {
      
      // this is okay--some other thread beat us to it 
      SG_chunk_free( &serialized_manifest );
      rc = 0;
   }
   if( rc != 0 ) {
      
      // failed 
      SG_chunk_free( &serialized_manifest );
      SG_chunk_free( &serialized_manifest_resp );

      SG_error("SG_gateway_cached_manifest_put_raw_async rc = %d\n", rc );
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
int SG_server_HTTP_GET_handler( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {

   struct SG_server_connection* sgcon = (struct SG_server_connection*)con_data->cls;
   
   struct SG_request_data* reqdat = NULL;
   struct SG_gateway* gateway = sgcon->gateway;
    
   int rc = 0;

   reqdat = SG_CALLOC( struct SG_request_data, 1 );
   if( reqdat == NULL ) {
      return -ENOMEM;
   }
   
   // parse the request
   rc = SG_request_data_parse( reqdat, con_data->url_path );
   if( rc != 0 ) {
         
      SG_error("SG_request_data_parse rc = %d\n", rc );
      
      SG_safe_free( reqdat );
      if( rc != -ENOMEM ) {
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      else {
         return md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   // redirect? expect world-readable or volume-readable
   rc = SG_server_redirect_request( gateway, resp, reqdat, 0044 );
   if( rc <= 0 ) {
      
      // handled, or error 
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      return rc;
   }
   
   // getxattr request?
   if( SG_request_is_getxattr( reqdat ) ) {
      rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_READ, SG_server_HTTP_GET_getxattr, reqdat, NULL, con_data, resp );
   }
   
   // listxattr request?
   else if( SG_request_is_listxattr( reqdat ) ) {
      rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_READ, SG_server_HTTP_GET_listxattr, reqdat, NULL, con_data, resp );
   }
   
   // block request?
   else if( SG_request_is_block( reqdat ) ) {

      rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_READ, SG_server_HTTP_GET_block, reqdat, NULL, con_data, resp );
   }
   
   else if( SG_request_is_manifest( reqdat ) ) {
      
      rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_READ, SG_server_HTTP_GET_manifest, reqdat, NULL, con_data, resp );
   }
   
   else {
      
      // bad request 
      rc = md_HTTP_create_response_builtin( resp, 400 );
      if( rc == 0 ) {
          rc = -ENOSYS;
      }
   }
   
   if( rc != 0 ) {
      // only do this in error; the I/O thread will clean up otherwise
      SG_request_data_free( reqdat );
      SG_safe_free( reqdat );
      SG_error("SG_server_HTTP_IO_start rc = %d\n", rc );
   }

   return rc;
}


// extract and verify a request's authenticity
// return 0 on success
// return -EINVAL if the message could not be parsed or verified 
// return -ENOMEM on OOM
// return -EAGAIN if we couldn't find the requester's certificate (i.e. we need to reload our config)
// return -EPERM if the message could not be validated, and will not ever be in the future.
static int SG_request_message_parse( struct SG_gateway* gateway, SG_messages::Request* msg, char* msg_buf, size_t msg_sz ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   
   // de-serialize 
   rc = md_parse< SG_messages::Request >( msg, msg_buf, msg_sz );
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      return rc;
   }
   
   // is this a request from a gateway, or a control-plane request from the command-line tool?
   if( msg->src_gateway_id() == SG_GATEWAY_TOOL ) {
      
      ms_client_config_rlock( ms );
      
      // from the admin tool.  verify that it's from the volume owner  
      rc = md_verify< SG_messages::Request >( ms->volume->volume_public_key, msg );
      
      ms_client_config_unlock( ms );
      
      if( rc != 0 ) {
         
         SG_error("Invalid admin message from %" PRIu64 "\n", msg->user_id() );
         return -EPERM;
      }
   }
   
   else {
      
      // from a gateway 
      rc = ms_client_verify_gateway_message< SG_messages::Request >( ms, msg->volume_id(), msg->src_gateway_id(), msg );
            
      if( rc != 0 ) {
            
          SG_error("ms_client_verify_gateway_message(from=%" PRIu64 ") rc = %d\n", msg->src_gateway_id(), rc );
          return -EPERM;
      }
   }
   
   return 0;
}


// extract request info from the request message 
// return 0 on success
// return -ENOMEM if OOM
// return -EINVAL if no message type could be discerned
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
   reqdat->src_gateway_id = request_msg->src_gateway_id();
   reqdat->coordinator_id = request_msg->coordinator_id();
   
   if( request_msg->request_type() == SG_messages::Request::SETXATTR ) {
      if( request_msg->has_xattr_name() && request_msg->has_xattr_value() ) {
         
         reqdat->xattr_name = SG_strdup_or_null( request_msg->xattr_name().c_str() );
         if( reqdat->xattr_name == NULL ) {
            
            return -ENOMEM;
         }
      }
      else {
         
         // invalid 
         SG_error("SETXATTR request on '%s' is missing xattr value\n", reqdat->fs_path );
         SG_request_data_free( reqdat );
         return -EINVAL;
      }
   }
   else if( request_msg->request_type() == SG_messages::Request::REMOVEXATTR ) {
      
      if( request_msg->has_xattr_name() ) {
         
         reqdat->xattr_name =  SG_strdup_or_null( request_msg->xattr_name().c_str() );
         if( reqdat->xattr_name == NULL ) {
            
            return -ENOMEM;
         }
      }  
      else {
         
         // invalid 
         SG_error("REMOVEXATTR request on '%s' is missing xattr name\n", reqdat->fs_path );
         SG_request_data_free( reqdat );
         return -EINVAL;
      }
   }
   else if( request_msg->has_new_manifest_mtime_sec() && request_msg->has_new_manifest_mtime_nsec() ) {
      
      // manifest
      reqdat->manifest_timestamp.tv_sec = request_msg->new_manifest_mtime_sec();
      reqdat->manifest_timestamp.tv_nsec = request_msg->new_manifest_mtime_nsec();
   }
   else if( request_msg->blocks_size() > 0 ) {
      
      // put the first block in 
      reqdat->block_id = request_msg->blocks(0).block_id();
      reqdat->block_version = request_msg->blocks(0).block_version();
   }
   else {
      
      return -EINVAL;
   }
   
   return 0;
}


// what are the capabilities required for a particular operation?
// return the bitwise OR of the cpability set 
// return (uint64_t)(-1) if the operation is not valid 
uint64_t SG_server_request_capabilities( uint64_t request_type ) {
   
   uint64_t caps_required = (uint64_t)(-1);         // all bits set
   
   switch( request_type ) {
      
      case SG_messages::Request::RELOAD: {
         
         // we only need a signature from the volume owner
         caps_required = 0;
         break;
      }
         
      case SG_messages::Request::SETXATTR:
      case SG_messages::Request::REMOVEXATTR: {
         
         caps_required = SG_CAP_WRITE_METADATA;
         break;
      }
      
      case SG_messages::Request::DETACH:
      case SG_messages::Request::RENAME: {
         
         caps_required = SG_CAP_WRITE_DATA | SG_CAP_WRITE_METADATA;
         break;
      }
      
      case SG_messages::Request::WRITE:
      case SG_messages::Request::TRUNCATE:
      case SG_messages::Request::DELETECHUNKS:
      case SG_messages::Request::PUTCHUNKS: {
         
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
int SG_server_HTTP_IO_start( struct SG_gateway* gateway, int type, SG_server_IO_completion io_cb, struct SG_request_data* reqdat, SG_messages::Request* request_msg,
                             struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   struct SG_server_io* io = NULL;
   struct md_wreq wreq;
   
   io = SG_CALLOC( struct SG_server_io, 1 );
   if( io == NULL ) {
      
      return -ENOMEM;
   }

   memset( &wreq, 0, sizeof(struct md_wreq) );
   
   io->gateway = gateway;
   io->reqdat = reqdat;
   io->request_msg = request_msg;
   io->con_data = con_data;
   io->resp = resp;
   io->io_completion = io_cb;
   io->io_type = type;
   
   // suspend the connection
   rc = md_HTTP_connection_suspend( con_data );
   if( rc != 0 ) {
   
      SG_error("md_HTTP_connection_suspend rc = %d\n", rc );
      
      SG_safe_free( io );
      return rc;
   }
   
   // enqueue the work
   // TODO: this needlessly constrains the order in which I/O happens.
   // what we really want is to "select()" on outstanding I/O requests, and collect results as we get them.
    
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


// finish an I/O request: generate a response, resume the connection, and send it off.
// return 0 on success 
// return -ENOMEM on OOM 
// return -errno on failure to generate a response
// TODO: allow the I/O completion callback to populate reply_msg.ent_out
int SG_server_HTTP_IO_finish( struct md_wreq* wreq, void* cls ) {
   
   struct SG_server_io* io = (struct SG_server_io*)cls;
   int rc = 0;
   int io_rc = 0;
   SG_messages::Reply reply_msg;
   
   int io_type = io->io_type;
   struct SG_gateway* gateway = io->gateway;
   struct md_HTTP_connection_data* con_data = io->con_data;
   struct md_HTTP_response* resp = io->resp;
   struct SG_request_data* reqdat = io->reqdat;
   SG_messages::Request* request_msg = io->request_msg;
   
   // what kind of response do we expect?
   if( io_type == SG_SERVER_IO_WRITE ) {
      
      // run the operation
      io_rc = (*io->io_completion)( gateway, reqdat, request_msg, con_data, NULL );
      
      // generate response 
      rc = SG_server_reply_populate( gateway, &reply_msg, request_msg->message_nonce(), io_rc );
      if( rc != 0 ) {
         
         // failed to set up
         SG_error("SG_server_reply_populate rc = %d\n", rc );
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
      
      else {
         
         // sign it
         rc = SG_server_reply_sign( gateway, &reply_msg );
         if( rc != 0 ) {
            
            // failed to sign 
            SG_error("SG_server_reply_sign rc = %d\n", rc );
            rc = md_HTTP_create_response_builtin( resp, 500 );
         }
         
         else {
            
            // serialize it
            rc = SG_server_reply_serialize( gateway, &reply_msg, resp );
            if( rc != 0 ) {
               
               // failed to serialize 
               SG_error("SG_server_reply_serialize rc = %d\n", rc );
               rc = md_HTTP_create_response_builtin( resp, 500 );
            }
         }
      }
   }
   else {
      
      // run the operation directly; the IO completion callback will generate the response.
      io_rc = (*io->io_completion)( gateway, reqdat, NULL, NULL, resp );
      if( io_rc != 0 ) {

         // failed
         SG_error("io_completion rc = %d\n", io_rc );
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   if( rc != 0 ) {
      
      // failed to create a response
      // TODO: have a static built-in response for this case
      SG_error("%s", "Out of memory\n");
      exit(1);
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


// handle a WRITE request--run a manifest through the implementation's "patch manifest" callback.
// this is called as part of an IO completion.
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL if the WRITE fields in request_msg are missing or malformed
// return -ENOSYS if there is no impl_patch_manifest method defined
static int SG_server_HTTP_POST_WRITE( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   struct SG_manifest write_delta;
   
   // sanity check 
   if( gateway->impl_patch_manifest == NULL ) {
     
      return -ENOSYS;
   }
   
   // santiy check: must have >0 blocks 
   if( request_msg->blocks_size() == 0 ) {
     
      SG_error("FATAL: request has %d blocks\n", request_msg->blocks_size() ); 
      return -EINVAL;
   }
   
   memset( &write_delta, 0, sizeof( struct SG_manifest ) );
   
   rc = SG_manifest_init( &write_delta, reqdat->volume_id, request_msg->coordinator_id(), reqdat->file_id, reqdat->file_version );
   if( rc != 0 ) {
      
      // OOM or invalid
      SG_error("FATAL: SG_manifest_init() rc = %d\n", rc );
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
         SG_error("FATAL: SG_manifest_load_from_protobuf rc = %d\n", rc );
         SG_manifest_free( &write_delta );
         return rc;
      }
      
      // insert block 
      rc = SG_manifest_put_block_nocopy( &write_delta, &block, true );
      if( rc != 0 ) {
         
         // OOM or invalid
         SG_error("FATAL: SG_manifest_put_block_nocopy rc = %d\n", rc );
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
   
   // TODO: reply ent_out
   
   return rc;
}


// handle a TRUNCATE request: feed the request to the implementation's "truncate" callback.
// this is called as part of an IO completion.
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the new size field is missing
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_TRUNCATE( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
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


// handle a RENAME request: feed the request to the implementation's "rename" callback.
// this is called as part of an IO completion.
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL if the new path field is missing 
// return -ENOSYS if not implemented
static int SG_server_HTTP_POST_RENAME( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
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

// handle a DETACH request: feed the request to the implementation's "detach" callback
// This is called as part of an IO completion.
// return 0 on success 
// return -ENOMEM on OOM 
// return -ENOSYS if not implemented
// return -EINVAL if this isn't a manifest request
static int SG_server_HTTP_POST_DETACH( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   
   // sanity check 
   if( gateway->impl_detach == NULL ) {
      
      return -ENOSYS;
   }
   
   // sanity check: manifest request?
   if( !SG_request_is_manifest( reqdat ) ) {
      
      return -EINVAL;
   }
   
   // yup 
   rc = SG_gateway_impl_detach( gateway, reqdat );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_detach( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
   }
   
   return rc;
}


// handle a DELETECHUNKS request: feed the request's serialized manifests and blocks into the implementation's "delete manifest" and "delete block" callbacks.
// this is called as part of an IO completion.
// return 0 on success, and evict the block from the cache.
// return -ENOMEM on OOM 
// return -ENOSYS if not implemented
// return -EINVAL if the request is not a block request
static int SG_server_HTTP_POST_DELETECHUNKS( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   int chunk_type = 0; 
   struct SG_manifest_block chunk_info;
   struct SG_IO_hints io_hints;
   int64_t io_context = md_random64();
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );

   // sanity check 
   if( gateway->impl_delete_block == NULL || gateway->impl_delete_manifest == NULL ) {
      
      return -ENOSYS;
   }
   
   // sanity check: need some optional fields
   for( int i = 0; i < request_msg->blocks_size(); i++ ) {

      if( !request_msg->blocks(i).has_chunk_type() ) {
         SG_error("Chunk %d is missing 'chunk_type'\n", i );
         return -EINVAL;
      }

      chunk_type = request_msg->blocks(i).chunk_type();
      if( chunk_type != SG_messages::ManifestBlock::BLOCK && chunk_type != SG_messages::ManifestBlock::MANIFEST ) {
         SG_error("Chunk %d is neither a BLOCK or MANIFEST chunk\n", i );
         return -EINVAL;
      }
   }

   // process each one 
   for( int i = 0; i < request_msg->blocks_size(); i++ ) {

      chunk_type = request_msg->blocks(i).chunk_type();

      rc = SG_manifest_block_load_from_protobuf( &chunk_info, &request_msg->blocks(i) );
      if( rc != 0 ) {

         SG_error("SG_manifest_block_load_from_protobuf rc = %d\n", rc );
         rc = -ENODATA;
         goto SG_server_HTTP_POST_DELETECHUNKS_finish;
      }

      if( chunk_type == SG_messages::ManifestBlock::MANIFEST ) {

         // delete manifest 
         // fill in request details....
         reqdat->manifest_timestamp.tv_sec = chunk_info.block_id;
         reqdat->manifest_timestamp.tv_nsec = chunk_info.block_version;
         reqdat->block_id = SG_INVALID_BLOCK_ID;
         reqdat->block_version = 0;
         
         SG_IO_hints_init( &io_hints, SG_IO_DELETE, 0, 0 );
         io_hints.io_context = io_context;
         SG_request_data_set_IO_hints( reqdat, &io_hints );

         rc = SG_gateway_impl_manifest_delete( gateway, reqdat );
         SG_manifest_block_free( &chunk_info );

         if( rc != 0 ) {

            SG_error("SG_gateway_impl_manifest_delete(%d) rc = %d\n", i, rc );
            rc = -ENODATA;
            goto SG_server_HTTP_POST_DELETECHUNKS_finish;
         }
      }
      else {

         // delete block 
         // fill in request details....
         reqdat->manifest_timestamp.tv_sec = -1;
         reqdat->manifest_timestamp.tv_nsec = -1;
         reqdat->block_id = request_msg->blocks(i).block_id();
         reqdat->block_version = request_msg->blocks(i).block_version();
    
         SG_IO_hints_init( &io_hints, SG_IO_DELETE, block_size * reqdat->block_id, block_size );
         io_hints.io_context = io_context;
         SG_request_data_set_IO_hints( reqdat, &io_hints );

         rc = SG_gateway_impl_block_delete( gateway, reqdat );
         SG_manifest_block_free( &chunk_info );

         if( rc != 0 ) {

            SG_error("SG_gateway_impl_block_delete(%d) rc = %d\n", i, rc );
            rc = -ENODATA;
            goto SG_server_HTTP_POST_DELETECHUNKS_finish;
         }
      }
   }

SG_server_HTTP_POST_DELETECHUNKS_finish:

   return rc;
}


// handle a PUTCHUNKS request: deserialize each chunk into a block or manifest, and feed them into the implementation's "put block" and "put manifest" callbacks.
// this is called as part of an IO completion.
// return 0 on success
// return -ENOSYS if not implemented 
// return -EINVAL if the request does not contain block information
// return -EBADMSG if the block's hash does not match the hash given on the control plane
// return -ENODATA if we failed to load the data into the gateway
// return -EPERM on internal error
// return -errno on fchmod, lseek, mmap failure
static int SG_server_HTTP_POST_PUTCHUNKS( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   int chunks_fd = 0;
   struct stat sb;
   int chunk_type = 0;
   char* chunks_mmap = NULL;
   struct SG_chunk chunk;
   struct SG_manifest_block chunk_info;
   unsigned char chunk_hash[SG_BLOCK_HASH_LEN];
   uint64_t offset = 0;
   uint64_t size = 0;
   struct SG_chunk deserialized_chunk;
   struct SG_IO_hints io_hints;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t blocksize = ms_client_get_volume_blocksize( ms );
   int64_t io_context = md_random64();
   
   // sanity check 
   if( gateway->impl_put_block == NULL || gateway->impl_put_manifest == NULL ) {
      
      return -ENOSYS;
   }

   // sanity check: need chunk type, offset, hash, and size for each block 
   for( int i = 0; i < request_msg->blocks_size(); i++ ) {
      
      if( !request_msg->blocks(i).has_chunk_type() ) {
         SG_error("Chunk %d is missing 'chunk_type'\n", i);
         return -EINVAL;
      }

      if( !request_msg->blocks(i).has_offset() ) {
         SG_error("Chunk %d is missing 'offset'\n", i);
         return -EINVAL;
      }

      if( !request_msg->blocks(i).has_size() ) {
         SG_error("Chunk %d is missing 'size'\n", i);
         return -EINVAL;
      }

      if( !request_msg->blocks(i).has_hash() ) {
         SG_error("Chunk %d is missing 'hash'\n", i);
         return -EINVAL;
      }

      chunk_type = request_msg->blocks(i).chunk_type();
      if( chunk_type != SG_messages::ManifestBlock::BLOCK && chunk_type != SG_messages::ManifestBlock::MANIFEST ) {
         SG_error("Chunk type %d is not BLOCK or MANIFEST\n", chunk_type );
         return -EINVAL;
      }
   }
   
   // fetch the serialized chunks from the request 
   rc = md_HTTP_upload_get_field_tmpfile( con_data, SG_SERVER_POST_FIELD_DATA_PLANE, NULL, &chunks_fd );
   if( rc != 0 ) {
      
      SG_error("md_HTTP_upload_get_field_buffer( '%s' ) rc = %d\n", SG_SERVER_POST_FIELD_DATA_PLANE, rc );
      return -EPERM;
   }
   
   // read-only...
   rc = fchmod( chunks_fd, 0400 );
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("fchmod rc = %d\n", rc );
      return -EPERM;
   }

   rc = fstat( chunks_fd, &sb );
   if( rc != 0 ) {

      rc = -errno;
      SG_error("fstat rc = %d\n", rc );
      return -EPERM;
   }

   // integrity/authenticity check
   for( int i = 0; i < request_msg->blocks_size(); i++ ) {

       // get the chunk information
       rc = SG_manifest_block_load_from_protobuf( &chunk_info, &request_msg->blocks(i) );
       if( rc != 0 ) {
      
          SG_error("SG_manifest_block_load_from_protobuf rc = %d\n", rc );
          return -EPERM;
       }
   
       // offset and size of chunk
       offset = request_msg->blocks(i).offset();
       size = request_msg->blocks(i).size();

       rc = lseek( chunks_fd, offset, SEEK_SET );
       if( rc < 0 ) {
          rc = -errno;
          SG_error("lseek(%d) rc = %d\n", chunks_fd, rc );

          SG_manifest_block_free( &chunk_info );
          return -ENODATA;
       }

       // integrity/authenticity check on each chunk 
       sha256_fd_buf( chunks_fd, size, chunk_hash );
       if( sha256_cmp( chunk_hash, chunk_info.hash ) != 0 ) {
 
            char expected[ 2*SG_BLOCK_HASH_LEN + 1 ];
            char actual[ 2*SG_BLOCK_HASH_LEN + 1 ];
            
            memset( expected, 0, 2*SG_BLOCK_HASH_LEN + 1 );
            memset( actual, 0, 2*SG_BLOCK_HASH_LEN + 1 );
            
            md_sprintf_data( expected, chunk_info.hash, chunk_info.hash_len );
            md_sprintf_data( actual, chunk_hash, SG_BLOCK_HASH_LEN );
            
            SG_error("%" PRIX64 ".%" PRId64 "[chunk %d] (%" PRIu64 "): expected '%s', got '%s'\n", reqdat->file_id, reqdat->file_version, i, size, expected, actual );
            
            SG_manifest_block_free( &chunk_info );
            return -EBADMSG;
       }

       SG_manifest_block_free( &chunk_info );
   }
   
   lseek( chunks_fd, 0, SEEK_SET );

   // it all checks out.
   // feed manifests and blocks to the driver
   chunks_mmap = (char*)mmap( NULL, sb.st_size, PROT_READ, MAP_PRIVATE, chunks_fd, 0 );
   if( chunks_mmap == MAP_FAILED ) {

      rc = -errno;
      SG_error("mmap rc = %d\n", rc );

      rc = -ENODATA;
      goto SG_server_HTTP_POST_PUTCHUNKS_finish;
   }

   for( int i = 0; i < request_msg->blocks_size(); i++ ) {

      // type, offset and size of chunk
      chunk_type = request_msg->blocks(i).chunk_type();
      offset = request_msg->blocks(i).offset();
      size = request_msg->blocks(i).size();

      SG_debug("Load chunk %d [offset %" PRIu64 ", size %" PRIu64 "]\n", i, offset, size );

      // set up a chunk 
      SG_chunk_init( &chunk, chunks_mmap + offset, size );

      // deserialize 
      rc = SG_gateway_impl_deserialize( gateway, reqdat, &chunk, &deserialized_chunk );
      if( rc != 0 ) {

         SG_error("SG_gateway_impl_deserialize(%d) rc = %d\n", i, rc );
         rc = -ENODATA;
         goto SG_server_HTTP_POST_PUTCHUNKS_finish;
      }

      if( chunk_type == SG_messages::ManifestBlock::BLOCK ) {

         // block 
         // fill in request details....
         reqdat->manifest_timestamp.tv_sec = -1;
         reqdat->manifest_timestamp.tv_nsec = -1;
         reqdat->block_id = request_msg->blocks(i).block_id();
         reqdat->block_version = request_msg->blocks(i).block_version();

         SG_IO_hints_init( &io_hints, SG_IO_WRITE, reqdat->block_id * blocksize, blocksize );
         io_hints.io_context = io_context;
         SG_request_data_set_IO_hints( reqdat, &io_hints );

         // pass along
         rc = SG_gateway_impl_block_put( gateway, reqdat, &deserialized_chunk, 0 );
         SG_chunk_free( &deserialized_chunk );

         if( rc != 0 ) {
            
            SG_error("SG_gateway_impl_block_put(chunk %d) rc = %d\n", i, rc );
            rc = -ENODATA;
            goto SG_server_HTTP_POST_PUTCHUNKS_finish;
         }
      }
      else {

         // manifest
         // fill in request details
         reqdat->manifest_timestamp.tv_sec = request_msg->blocks(i).block_id(); 
         reqdat->manifest_timestamp.tv_nsec = request_msg->blocks(i).block_version();
         reqdat->block_id = SG_INVALID_BLOCK_ID;
         reqdat->block_version = 0;
         
         SG_IO_hints_init( &io_hints, SG_IO_WRITE, 0, 0 );
         io_hints.io_context = io_context;
         SG_request_data_set_IO_hints( reqdat, &io_hints );

         // put into the gateway 
         rc = SG_gateway_impl_manifest_put( gateway, reqdat, &deserialized_chunk, 0 );
         SG_chunk_free( &deserialized_chunk );

         if( rc != 0 ) {

            SG_error("SG_gateway_impl_manifest_put(chunk %d) rc = %d\n", i, rc );
            rc = -ENODATA;
            goto SG_server_HTTP_POST_PUTCHUNKS_finish;
         }
      }

      memset( &chunk, 0, sizeof(struct SG_chunk) );
   }

SG_server_HTTP_POST_PUTCHUNKS_finish:

   if( chunks_mmap != MAP_FAILED ) {
       
       int unmap_rc = munmap( chunks_mmap, size );
       if( unmap_rc != 0 ) {

           unmap_rc = -errno;
           SG_error("munmap rc = %d\n", unmap_rc );
       }
   }
    
   return rc;
}


// handle a SETXATTR request: feed the request into the implementation's "setxattr" callback.
// this is called as part of an IO completion.
// NOTE: reqdat must be a getxattr request
// return 0 on success
// return -ENOSYS if not implemented 
// return -EINVAL if the request does not contain xattr information
// return -EAGAIN if stale
static int SG_server_HTTP_POST_SETXATTR( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   struct SG_chunk xattr_value;
   
   // sanity check 
   if( gateway->impl_setxattr == NULL ) {
      
      return -ENOSYS;
   }
   
   if( reqdat->xattr_name == NULL ) {
      
      return -EINVAL;
   }
   
   memset( &xattr_value, 0, sizeof(struct SG_chunk) );
   SG_chunk_init( &xattr_value, (char*)request_msg->xattr_value().data(), request_msg->xattr_value().size() );
   
   rc = SG_gateway_impl_setxattr( gateway, reqdat, &xattr_value );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_setxattr( %" PRIX64 ".%" PRId64 ".%s (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->xattr_name, reqdat->fs_path, rc );
   }
   
   return rc;
}


// handle a REMOVEXATTR request: feed the request into the implementation's "removexattr" callback.
// this is called as part of an IO completion.
// NOTE: reqdat must be a getxattr request
// return 0 on success
// return -ENOSYS if not implemented 
// return -EINVAL if the request does not contain xattr information
static int SG_server_HTTP_POST_REMOVEXATTR( struct SG_gateway* gateway, struct SG_request_data* reqdat, SG_messages::Request* request_msg, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* ignored ) {
   
   int rc = 0;
   
   // sanity check 
   if( gateway->impl_removexattr == NULL ) {
      
      return -ENOSYS;
   }
   
   if( reqdat->xattr_name == NULL ) {
      
      return -EINVAL;
   }
   
   rc = SG_gateway_impl_removexattr( gateway, reqdat );
   
   if( rc != 0 ) {
   
      SG_error("SG_gateway_impl_removexattr( %" PRIX64 ".%" PRId64 ".%s (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->xattr_name, reqdat->fs_path, rc );
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
   
   // serialized request that we got
   char* request_message_str = NULL;
   size_t request_message_len = 0;
   
   int rc = 0;
   
   // get the control-plane component of the data 
   rc = md_HTTP_upload_get_field_buffer( con_data, SG_SERVER_POST_FIELD_CONTROL_PLANE, &request_message_str, &request_message_len );
   if( rc != 0 ) {
      
      SG_safe_delete( request_msg );
      
      // failed to process control-plane
      SG_error("md_HTTP_upload_get_field_buffer( '%s' ) rc = %d\n", SG_SERVER_POST_FIELD_CONTROL_PLANE, rc );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // parse and verify the request
   rc = SG_request_message_parse( gateway, request_msg, request_message_str, request_message_len );
   SG_safe_free( request_message_str );
   
   if( rc != 0 ) {
      
      SG_safe_delete( request_msg );
      
      // failed to parse and verify control-plane message 
      SG_error("SG_request_message_parse('%s') rc = %d\n", SG_SERVER_POST_FIELD_CONTROL_PLANE, rc );
      
      if( rc == -EAGAIN ) {
         
         // certificate is not on file.  Try to get it and have the requester try again 
         SG_gateway_start_reload( gateway );
         return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
      }
      else if( rc == -EBADMSG || rc == -EINVAL ) {
         
         // bad message
         return md_HTTP_create_response_builtin( resp, 400 );
      }
      else if( rc == -EPERM ) {
         
         // invalid caller 
         return md_HTTP_create_response_builtin( resp, 401 );
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
      
      if( rc == -EAGAIN ) {
         
         // only failed since we're missing the certificate.
         // try to get it and have the requester try again 
         SG_gateway_start_reload( gateway );
         return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
      }
      
      else {
         
         // not permitted 
         SG_debug("%s", "Gateway does not have sufficient capabilities\n" );
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
      
      // yup, need a reload 
      SG_gateway_start_reload( gateway );
      return md_HTTP_create_response_builtin( resp, SG_HTTP_TRYAGAIN );
   }
   
   // request information
   reqdat = SG_CALLOC( struct SG_request_data, 1 );
   if( reqdat == NULL ) {
      
      // OOM!
      SG_safe_delete( request_msg );
      return md_HTTP_create_response_builtin( resp, 500 );
   }
   
   // extract request info from the request 
   rc = SG_request_data_from_message( reqdat, request_msg );
   if( rc != 0 ) {
      
      // failed 
      SG_safe_free( reqdat );
      SG_safe_delete( request_msg );
      SG_error("SG_request_data_from_message rc = %d\n", rc ); 
      return md_HTTP_create_response_builtin( resp, 500 );
   }
  
   SG_debug("Got message type %d\n", (int)request_msg->request_type() );

   // dispatch operation 
   switch( request_msg->request_type() ) {
      
      case SG_messages::Request::WRITE: {
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         // requires reqdat to be a manifest 
         if( !SG_request_is_manifest( reqdat ) ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a manifest request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }

         if( gateway->impl_stat == NULL ) {
      
            SG_error("%s", "BUG: gateway->impl_stat is not defined\n");
            SG_safe_delete( request_msg );
            SG_request_data_free( reqdat );
            SG_safe_free( reqdat );
            return md_HTTP_create_response_builtin( resp, 501 );
         }
         
         else {
            // accessible? expect world-writeable or volume-writeable
            rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
            if( rc <= 0 ) {
               
               if( rc < 0 ) {
                  SG_error("SG_server_stat_request rc = %d\n", rc );
               }
            }
            else {
               
               // start write 
               rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_WRITE, reqdat, request_msg, con_data, resp );
               if( rc != 0 ) {
                  
                  SG_error("SG_server_HTTP_IO_start( WRITE(%" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
               }
            }
         }
         
         break;
      }
      
      case SG_messages::Request::TRUNCATE: {
         
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         // requires reqdat to be a manifest 
         if( !SG_request_is_manifest( reqdat ) ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a manifest request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }

         if( gateway->impl_stat == NULL ) {
      
            SG_error("%s", "BUG: gateway->impl_stat is not defined\n");
            SG_safe_delete( request_msg );
            return md_HTTP_create_response_builtin( resp, 501 );
         }
         
         else {
            // accessible? expect world-writeable or volume-writeable
            rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
            if( rc <= 0 ) {
               
               if( rc < 0 ) {
                  SG_error("SG_server_stat_request rc = %d\n", rc );
               }
            }
            else {
            
               // start truncate 
               rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_TRUNCATE, reqdat, request_msg, con_data, resp );
               if( rc != 0 ) {
                  
                  SG_error("SG_server_HTTP_IO_START( TRUNCATE( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
               }
            }
         }
         break;
      }
      
      case SG_messages::Request::RENAME: {
         
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         // requires reqdat to be a manifest 
         if( !SG_request_is_manifest( reqdat ) ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a manifest request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }
         
         if( gateway->impl_stat == NULL ) {
      
            SG_error("%s", "BUG: gateway->impl_stat is not defined\n");
            SG_safe_delete( request_msg );
            return md_HTTP_create_response_builtin( resp, 501 );
         }
         else {
               
            // accessible? expect world-writeable or volume-writeable
            rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
            if( rc <= 0 ) {
               
               if( rc < 0 ) {
                  SG_error("SG_server_stat_request rc = %d\n", rc );
               }
            }
            else {
               
               // start rename 
               rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_RENAME, reqdat, request_msg, con_data, resp );
               if( rc != 0 ) {
                  
                  SG_error("SG_server_HTTP_IO_start( RENAME( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
               }
            }
         }
         
         break;
      }
      
      case SG_messages::Request::DETACH: {
          
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         // requires reqdat to be a manifest 
         if( !SG_request_is_manifest( reqdat ) ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a manifest request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }
         
         if( gateway->impl_stat == NULL ) {
      
            SG_error("%s", "BUG: gateway->impl_stat is not defined\n");
            SG_safe_delete( request_msg );
            return md_HTTP_create_response_builtin( resp, 501 );
         }
         else {
            
            // accessible? expect world-writeable or volume-writeable
            rc = SG_server_stat_request( gateway, resp, reqdat, 0055 );
            if( rc <= 0 ) {
               
               if( rc < 0 ) {
                  SG_error("SG_server_stat_request rc = %d\n", rc );
               }
            }
            else {
                  
               // start detach
               rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_DETACH, reqdat, request_msg, con_data, resp );
               if( rc != 0 ) {
                  
                  SG_error("SG_server_HTTP_IO_start( DETACH( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
               }
            }
         }
         
         break;
      }
      
      case SG_messages::Request::DELETECHUNKS: {
         
         // start delete-chunks request 
         rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_DELETECHUNKS, reqdat, request_msg, con_data, resp );
         if( rc != 0 ) {
            
            SG_error("SG_server_HTTP_IO_start( DELETECHUNKS( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n",
                     reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
         }
      
         break;
      }
      
      case SG_messages::Request::PUTCHUNKS: {
         
         // start put-chunks request 
         rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_PUTCHUNKS, reqdat, request_msg, con_data, resp );
         if( rc != 0 ) {
            
            SG_error("SG_server_HTTP_IO_start( PUTCHUNKS( %" PRIX64 ".%" PRId64 " (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
         }
         
         break;
      }
      
      case SG_messages::Request::SETXATTR: {
          
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         if( reqdat->xattr_name == NULL || !request_msg->has_xattr_value() ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a block request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }
         else {
            
            rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_SETXATTR, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( SETXATTR( %" PRIX64 ".%" PRId64 ".%s (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->xattr_name, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::REMOVEXATTR: {
          
         // are we the coordinator?
         if( SG_gateway_id( gateway ) != reqdat->coordinator_id ) {
             // redirect 
             SG_error("Not the coordinator of %" PRIX64 "\n", reqdat->file_id );
             SG_safe_delete( request_msg );
             SG_request_data_free( reqdat );
             SG_safe_free( reqdat );
             return md_HTTP_create_response_builtin( resp, 410 );
         }

         if( reqdat->xattr_name == NULL ) {
            
            SG_error("Request on '%s' (/%" PRIX64 "/%" PRId64 ") is not a block request\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
            rc = -EINVAL;
         }
         else {
            
            rc = SG_server_HTTP_IO_start( gateway, SG_SERVER_IO_WRITE, SG_server_HTTP_POST_REMOVEXATTR, reqdat, request_msg, con_data, resp );
            if( rc != 0 ) {
               
               SG_error("SG_server_HTTP_IO_start( REMOVEXATTR( %" PRIX64 ".%" PRId64 ".%s (%s)) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->xattr_name, reqdat->fs_path, rc );
            }
         }
         
         break;
      }
      
      case SG_messages::Request::RELOAD: {
         
         // TODO 
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


