/*
   Copyright 2013 The Trustees of Princeton University

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


#include "libsyndicate/httpd.h"

char const MD_HTTP_NOMSG[128] = "\n";
char const MD_HTTP_200_MSG[128] = "OK\n";
char const MD_HTTP_400_MSG[128] = "Bad Request\n";
char const MD_HTTP_401_MSG[128] = "Invalid authorization credentials\n";
char const MD_HTTP_403_MSG[128] = "Credentials required\n";
char const MD_HTTP_404_MSG[128] = "Not found\n";
char const MD_HTTP_409_MSG[128] = "Operation conflict\n";
char const MD_HTTP_413_MSG[128] = "Requested entry too big\n";
char const MD_HTTP_422_MSG[128] = "Unprocessable entry\n";
char const MD_HTTP_500_MSG[128] = "Internal Server Error\n";
char const MD_HTTP_501_MSG[128] = "Not implemented\n";
char const MD_HTTP_504_MSG[128] = "Remote Server Timeout\n";

char const MD_HTTP_DEFAULT_MSG[128] = "RESPONSE\n";


// respond to a request
static int md_HTTP_default_send_response( struct MHD_Connection* connection, int status_code, char* data ) {
   char const* page = NULL;
   struct MHD_Response* response = NULL;
   
   if( data == NULL ) {
      // use a built-in status message
      switch( status_code ) {
         case MHD_HTTP_BAD_REQUEST:
            page = MD_HTTP_400_MSG;
            break;
         
         case MHD_HTTP_INTERNAL_SERVER_ERROR:
            page = MD_HTTP_500_MSG;
            break;
         
         case MHD_HTTP_UNAUTHORIZED:
            page = MD_HTTP_401_MSG;
            break;
            
         default:
            page = MD_HTTP_DEFAULT_MSG;
            break;
      }
      response = MHD_create_response_from_buffer( strlen(page), (void*)page, MHD_RESPMEM_PERSISTENT );
   }
   else {
      // use the given status message
      response = MHD_create_response_from_buffer( strlen(data), (void*)data, MHD_RESPMEM_MUST_FREE );
   }
   
   if( !response )
      return MHD_NO;
      
   // this is always a text/plain type
   MHD_add_response_header( response, "Content-Type", "text/plain" );
   int ret = MHD_queue_response( connection, status_code, response );
   MHD_destroy_response( response );
   
   return ret;
}

// make a RAM response which we'll defensively copy
int md_create_HTTP_response_ram( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_COPY );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make a RAM response which we'll keep a pointer to and free later
int md_create_HTTP_response_ram_nocopy( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_FREE );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make a RAM response which is persistent
int md_create_HTTP_response_ram_static( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_PERSISTENT );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make an FD-based response
int md_create_HTTP_response_fd( struct md_HTTP_response* resp, char const* mimetype, int status, int fd, off_t offset, size_t size ) {
   resp->resp = MHD_create_response_from_fd_at_offset( size, fd, offset );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make a callback response
int md_create_HTTP_response_stream( struct md_HTTP_response* resp, char const* mimetype, int status, uint64_t size, size_t blk_size, md_HTTP_stream_callback scb, void* cls, md_HTTP_free_cls_callback fcb ) {
   resp->resp = MHD_create_response_from_callback( size, blk_size, scb, cls, fcb );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// give back a user-callback-created response
static int md_HTTP_send_response( struct MHD_Connection* connection, struct md_HTTP_response* resp ) {
   int ret = MHD_queue_response( connection, resp->status, resp->resp );
   MHD_destroy_response( resp->resp );
   
   return ret;
}


// free an HTTP response
void md_free_HTTP_response( struct md_HTTP_response* resp ) {
   return;
}


// find an http header value
char const* md_find_HTTP_header( struct md_HTTP_header** headers, char const* header ) {
   for( int i = 0; headers[i] != NULL; i++ ) {
      if( strcasecmp( headers[i]->header, header ) == 0 ) {
         return headers[i]->value;
      }
   }
   return NULL;
}

// add a header
int md_HTTP_add_header( struct md_HTTP_response* resp, char const* header, char const* value ) {
   if( resp->resp != NULL ) {
      MHD_add_response_header( resp->resp, header, value );
   }
   return 0;
}


// get the user's data out of a syndicate-managed connection data structure
void* md_cls_get( void* cls ) {
   struct md_HTTP_connection_data* dat = (struct md_HTTP_connection_data*)cls;
   return dat->cls;
}

// set the status of a syndicate-managed connection data structure
void md_cls_set_status( void* cls, int status ) {
   struct md_HTTP_connection_data* dat = (struct md_HTTP_connection_data*)cls;
   dat->status = status;
}

// set the response of a syndicate-managed connection data structure
struct md_HTTP_response* md_cls_set_response( void* cls, struct md_HTTP_response* resp ) {
   struct md_HTTP_connection_data* dat = (struct md_HTTP_connection_data*)cls;
   struct md_HTTP_response* ret = dat->resp;
   dat->resp = resp;
   return ret;
}


// create an http header
void md_create_HTTP_header( struct md_HTTP_header* header, char const* h, char const* v ) {
   header->header = strdup( h );
   header->value = strdup( v );
}

// free an HTTP header
void md_free_HTTP_header( struct md_HTTP_header* header ) {
   if( header->header ) {
      free( header->header );
   }
   if( header->value ) {
      free( header->value );
   }
   memset( header, 0, sizeof(struct md_HTTP_header) );
}


// accumulate inbound headers
static int md_accumulate_headers( void* cls, enum MHD_ValueKind kind, char const* key, char const* value ) {
   vector<struct md_HTTP_header*> *header_list = (vector<struct md_HTTP_header*> *)cls;
   
   struct md_HTTP_header* hdr = (struct md_HTTP_header*)calloc( sizeof(struct md_HTTP_header), 1 );
   md_create_HTTP_header( hdr, key, value );
   
   header_list->push_back( hdr );
   return MHD_YES;
}

// free a list of headers
static void md_free_headers( struct md_HTTP_header** headers ) {
   for( unsigned int i = 0; headers[i] != NULL; i++ ) {
      md_free_HTTP_header( headers[i] );
      free( headers[i] );
   }
   free( headers );
}

// short message upload handler, for accumulating smaller messages into RAM via a response buffer
int md_response_buffer_upload_iterator(void *coninfo_cls, enum MHD_ValueKind kind,
                                       const char *key,
                                       const char *filename, const char *content_type,
                                       const char *transfer_encoding, const char *data,
                                       uint64_t off, size_t size) {


   dbprintf( "upload %zu bytes\n", size );

   struct md_HTTP_connection_data *md_con_data = (struct md_HTTP_connection_data*)coninfo_cls;
   response_buffer_t* rb = md_con_data->rb;

   // add a copy of the data
   char* data_dup = CALLOC_LIST( char, size );
   memcpy( data_dup, data, size );

   rb->push_back( buffer_segment_t( data_dup, size ) );

   return MHD_YES;
}



// HTTP connection handler
static int md_HTTP_connection_handler( void* cls, struct MHD_Connection* connection, 
                                       char const* url, 
                                       char const* method, 
                                       char const* version, 
                                       char const* upload_data, size_t* upload_size, 
                                       void** con_cls ) {
   
   struct md_HTTP* http_ctx = (struct md_HTTP*)cls;
   struct md_HTTP_connection_data* con_data = (struct md_HTTP_connection_data*)(*con_cls);

   // need to create connection data?
   if( con_data == NULL ) {

      // verify that the URL starts with '/'
      if( strlen(url) > 0 && url[0] != '/' ) {
         errorf( "malformed URL %s\n", url );
         return md_HTTP_default_send_response( connection, MHD_HTTP_BAD_REQUEST, NULL );
      }
      
      con_data = CALLOC_LIST( struct md_HTTP_connection_data, 1 );
      if( !con_data ) {
         errorf("%s\n", "out of memory" );
         return md_HTTP_default_send_response( connection, MHD_HTTP_INTERNAL_SERVER_ERROR, NULL );
      }

      struct MHD_PostProcessor* pp = NULL;
      int mode = MD_HTTP_UNKNOWN;

      if( strcmp( method, "GET" ) == 0 ) {
         if( http_ctx->HTTP_GET_handler )
            mode = MD_HTTP_GET;
      }
      else if( strcmp( method, "HEAD" ) == 0 ) {
         if( http_ctx->HTTP_HEAD_handler )
            mode = MD_HTTP_HEAD;
      }
      else if( strcmp( method, "POST" ) == 0 ) {

         if( http_ctx->HTTP_POST_iterator ) {

            char const* encoding = MHD_lookup_connection_value( connection, MHD_HEADER_KIND, MHD_HTTP_HEADER_CONTENT_TYPE );
            if( encoding != NULL &&
               (strncasecmp( encoding, MHD_HTTP_POST_ENCODING_FORM_URLENCODED, strlen( MHD_HTTP_POST_ENCODING_FORM_URLENCODED ) ) == 0 ||
                strncasecmp( encoding, MHD_HTTP_POST_ENCODING_MULTIPART_FORMDATA, strlen( MHD_HTTP_POST_ENCODING_MULTIPART_FORMDATA ) ) == 0 )) {

               pp = MHD_create_post_processor( connection, 4096, http_ctx->HTTP_POST_iterator, con_data );
               if( pp == NULL ) {
                  errorf( "failed to create POST processor for %s\n", method);
                  free( con_data );
                  return md_HTTP_default_send_response( connection, 400, NULL );
               }
            }
            else {
               con_data->offset = 0;
            }

            mode = MD_HTTP_POST;
         }
      }
      else if( strcmp( method, "PUT" ) == 0 ) {

         if( http_ctx->HTTP_PUT_iterator ) {

            char const* encoding = MHD_lookup_connection_value( connection, MHD_HEADER_KIND, MHD_HTTP_HEADER_CONTENT_TYPE );
            if( strncasecmp( encoding, MHD_HTTP_POST_ENCODING_FORM_URLENCODED, strlen( MHD_HTTP_POST_ENCODING_FORM_URLENCODED ) ) == 0 ||
                strncasecmp( encoding, MHD_HTTP_POST_ENCODING_MULTIPART_FORMDATA, strlen( MHD_HTTP_POST_ENCODING_MULTIPART_FORMDATA ) ) == 0 ) {

               pp = MHD_create_post_processor( connection, 4096, http_ctx->HTTP_PUT_iterator, con_data );
               if( pp == NULL ) {
                  errorf( "failed to create POST processor for %s\n", method);
                  free( con_data );
                  return md_HTTP_default_send_response( connection, 400, NULL );
               }
            }
            else {
               con_data->offset = 0;
            }
            mode = MD_HTTP_PUT;
         }
      }
      else if( strcmp( method, "DELETE" ) == 0 ) {
         if( http_ctx->HTTP_DELETE_handler )
            mode = MD_HTTP_DELETE;
      }

      if( mode == MD_HTTP_UNKNOWN ) {
         // unsupported method
         struct md_HTTP_response* resp = CALLOC_LIST(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
         free( con_data );
         
         errorf("unknown HTTP method '%s'\n", method);
         
         return md_HTTP_send_response( connection, resp );
      }
      
      // build up con_data from what we know
      con_data->conf = http_ctx->conf;
      con_data->http = http_ctx;
      con_data->url_path = md_flatten_path( url );
      con_data->version = strdup(version);
      con_data->query_string = (char*)index( con_data->url_path, '?' );
      con_data->rb = new response_buffer_t();
      con_data->remote_host = MHD_lookup_connection_value( connection, MHD_HEADER_KIND, MHD_HTTP_HEADER_HOST );
      con_data->method = method;
      con_data->mode = mode;
      con_data->cls = NULL;
      con_data->status = 200;
      con_data->pp = pp;
      con_data->ms = http_ctx->ms;

      char const* content_length_str = MHD_lookup_connection_value( connection, MHD_HEADER_KIND, MHD_HTTP_HEADER_CONTENT_LENGTH );
      if( content_length_str != NULL )
         con_data->content_length = strtol( content_length_str, NULL, 10 );
      else
         con_data->content_length = 0;

      // split query string on '?'
      if( con_data->query_string != NULL ) {
         char* p = con_data->query_string;
         *p = 0;
         con_data->query_string = p + 1;
      }

      // get headers
      vector<struct md_HTTP_header*> headers_vec;
      MHD_get_connection_values( connection, MHD_HEADER_KIND, md_accumulate_headers, (void*)&headers_vec );

      // convert to list
      struct md_HTTP_header** headers = CALLOC_LIST( struct md_HTTP_header*, headers_vec.size() + 1 );
      for( unsigned int i = 0; i < headers_vec.size(); i++ ) {
         headers[i] = headers_vec.at(i);
      }

      con_data->headers = headers;

      dbprintf("%s %s, query=%s, requester=%s\n", method, con_data->url_path, con_data->query_string, con_data->remote_host );
      
      // perform connection setup
      if( http_ctx->HTTP_connect != NULL ) {
         con_data->cls = (*http_ctx->HTTP_connect)( con_data );

         if( con_data->status >= 300 ) {
            // not going to serve data
            errorf("connect status = %d\n", con_data->status );
            return md_HTTP_send_response( connection, con_data->resp );
         }
      }

      *con_cls = con_data;
      
      return MHD_YES;
   }

   // POST
   if( con_data->mode == MD_HTTP_POST ) {

      if( *upload_size != 0 && con_data->status > 0 ) {
         if( con_data->pp ) {
            dbprintf( "POST %s, postprocess %zu bytes\n", con_data->url_path, *upload_size );
            MHD_post_process( con_data->pp, upload_data, *upload_size );
            *upload_size = 0;
            return MHD_YES;
         }
         else {
            dbprintf( "POST %s, raw %zu bytes\n", con_data->url_path, *upload_size );
            int rc = (*http_ctx->HTTP_POST_iterator)( con_data, MHD_POSTDATA_KIND, NULL, NULL, NULL, NULL, upload_data, con_data->offset, *upload_size );
            con_data->offset += *upload_size;
            *upload_size = 0;
            return rc;
         }
      }
      else {
         (*http_ctx->HTTP_POST_finish)( con_data );
         dbprintf( "POST finished (%s)\n", url);
         
         if( con_data->resp == NULL ) {
            return md_HTTP_default_send_response( connection, 500, NULL );
         }
         else {
            return md_HTTP_send_response( connection, con_data->resp );
         }
      }
   }
   
   // PUT
   else if( con_data->mode == MD_HTTP_PUT ) {

      if( *upload_size != 0 && con_data->status > 0 ) {
         if( con_data->pp ) {
            dbprintf( "PUT %s, postprocess %zu bytes\n", con_data->url_path, *upload_size );
            MHD_post_process( con_data->pp, upload_data, *upload_size );
            *upload_size = 0;
            return MHD_YES;
         }
         else {
            dbprintf( "PUT %s, raw %zu bytes\n", con_data->url_path, *upload_size );
            int rc = (*http_ctx->HTTP_PUT_iterator)( con_data, MHD_POSTDATA_KIND, NULL, NULL, NULL, NULL, upload_data, con_data->offset, *upload_size );
            con_data->offset += *upload_size;
            *upload_size = 0;
            return rc;
         }
      }
      else {
         
         (*http_ctx->HTTP_PUT_finish)( con_data );
         dbprintf( "PUT finished (%s)\n", url);
         
         if( con_data->resp == NULL ) {
            return md_HTTP_default_send_response( connection, 500, NULL );
         }
         else {
            return md_HTTP_send_response( connection, con_data->resp );
         }
      }
   }

   // DELETE
   else if( con_data->mode == MD_HTTP_DELETE ) {
      struct md_HTTP_response* resp = (*http_ctx->HTTP_DELETE_handler)( con_data, 0 );

      if( resp == NULL ) {
         resp = CALLOC_LIST(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, (char*)"text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      }

      con_data->resp = resp;
      return md_HTTP_send_response( connection, con_data->resp );
   }
   
   // GET
   else if( con_data->mode == MD_HTTP_GET ) {
      
      struct md_HTTP_response* resp = (*http_ctx->HTTP_GET_handler)( con_data );
      if( resp == NULL ) {
         resp = CALLOC_LIST(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, (char*)"text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      }

      con_data->resp = resp;

      return md_HTTP_send_response( connection, resp );
   }
   
   // HEAD
   else if( con_data->mode == MD_HTTP_HEAD ) {
      
      struct md_HTTP_response* resp = (*http_ctx->HTTP_HEAD_handler)( con_data );
      if( resp == NULL ) {
         resp = CALLOC_LIST(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, (char*)"text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      }

      con_data->resp = resp;

      return md_HTTP_send_response( connection, resp );
   }

   return md_HTTP_default_send_response( connection, MHD_HTTP_BAD_REQUEST, NULL );
}


// free a connection state
void md_HTTP_free_connection_data( struct md_HTTP_connection_data* con_data ) {
   if( con_data->pp ) {
      MHD_destroy_post_processor( con_data->pp );
   }
   if( con_data->resp ) {
      md_free_HTTP_response( con_data->resp );
      free( con_data->resp );
   }
   if( con_data->url_path ) {
      free( con_data->url_path );
      con_data->url_path = NULL;
   }
   if( con_data->query_string ) {
      free( con_data->query_string );
      con_data->query_string = NULL;
   }
   if( con_data->version ) {
      free( con_data->version );
      con_data->version = NULL;
   }
   if( con_data->headers ) {
      md_free_headers( con_data->headers );
      con_data->headers = NULL;
   }
   if( con_data->rb ) {
      response_buffer_free( con_data->rb );
      delete con_data->rb;
      con_data->rb = NULL;
   }
}

// default cleanup handler
// calls user-supplied cleanup handler as well
void md_HTTP_cleanup( void *cls, struct MHD_Connection *connection, void **con_cls, enum MHD_RequestTerminationCode term ) {
   struct md_HTTP* http = (struct md_HTTP*)cls;
   
   struct md_HTTP_connection_data* con_data = NULL;
   if( con_cls ) {
      con_data = (struct md_HTTP_connection_data*)(*con_cls);
   }
   
   if( http->HTTP_cleanup && con_data) {
      (*http->HTTP_cleanup)( connection, con_data->cls, term );
      con_data->cls = NULL;
   }
   if( con_data ) {
      md_HTTP_free_connection_data( con_data );
      free( con_data );
   }
}

// set fields in an HTTP structure
int md_HTTP_init( struct md_HTTP* http, int server_type, struct md_syndicate_conf* conf, struct ms_client* client ) {
   memset( http, 0, sizeof(struct md_HTTP) );
   http->conf = conf;
   http->server_type = server_type;
   http->ms = client;
   return 0;
}


// start the HTTP thread
int md_start_HTTP( struct md_HTTP* http, int portnum ) {
   
   struct md_syndicate_conf* conf = http->conf;
   
   pthread_rwlock_init( &http->lock, NULL );
   
   if( conf->server_cert && conf->server_key ) {

      http->server_pkey = conf->server_key;
      http->server_cert = conf->server_cert;
      
      // SSL enabled
      if( http->server_type == MHD_USE_THREAD_PER_CONNECTION ) {
         http->http_daemon = MHD_start_daemon(  http->server_type | MHD_USE_SSL, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                                MHD_OPTION_HTTPS_MEM_KEY, conf->server_key, 
                                                MHD_OPTION_HTTPS_MEM_CERT, conf->server_cert,
                                                MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                                MHD_OPTION_HTTPS_PRIORITIES, SYNDICATE_GNUTLS_CIPHER_SUITES,
                                                MHD_OPTION_END );
      }
      else {
         http->http_daemon = MHD_start_daemon(  http->server_type | MHD_USE_SSL, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                                MHD_OPTION_HTTPS_MEM_KEY, conf->server_key, 
                                                MHD_OPTION_HTTPS_MEM_CERT, conf->server_cert, 
                                                MHD_OPTION_THREAD_POOL_SIZE, conf->num_http_threads,
                                                MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                                MHD_OPTION_HTTPS_PRIORITIES, SYNDICATE_GNUTLS_CIPHER_SUITES,
                                                MHD_OPTION_END );
      }
   
      if( http->http_daemon )
         dbprintf( "Started HTTP server with SSL enabled (cert = %s, pkey = %s) on port %d\n", conf->server_cert_path, conf->server_key_path, portnum);
   }
   else {
      // SSL disabled
      if( http->server_type == MHD_USE_THREAD_PER_CONNECTION ) {
         http->http_daemon = MHD_start_daemon(  http->server_type, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                                MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                                MHD_OPTION_END );
      }
      else {
         http->http_daemon = MHD_start_daemon(  http->server_type, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                                MHD_OPTION_THREAD_POOL_SIZE, conf->num_http_threads,
                                                MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                                MHD_OPTION_END );
      }
      
      if( http->http_daemon )
         dbprintf( "Started HTTP server on port %d\n", portnum);
   }
   
   if( http->http_daemon == NULL ) {
      pthread_rwlock_destroy( &http->lock );
      return -1;
   }
   
   return 0;
}

// stop the HTTP thread
int md_stop_HTTP( struct md_HTTP* http ) {
   MHD_stop_daemon( http->http_daemon );
   http->http_daemon = NULL;
   return 0;
}

// free the HTTP server
int md_free_HTTP( struct md_HTTP* http ) {
   pthread_rwlock_destroy( &http->lock );
   return 0;
}

int md_HTTP_rlock( struct md_HTTP* http ) {
   return pthread_rwlock_rdlock( &http->lock );
}

int md_HTTP_wlock( struct md_HTTP* http ) {
   return pthread_rwlock_wrlock( &http->lock );
}

int md_HTTP_unlock( struct md_HTTP* http ) {
   return pthread_rwlock_unlock( &http->lock );
}

