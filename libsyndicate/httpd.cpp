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
char const MD_HTTP_302_MSG[128] = "Redirect\n";
char const MD_HTTP_400_MSG[128] = "Bad Request\n";
char const MD_HTTP_401_MSG[128] = "Invalid authorization credentials\n";
char const MD_HTTP_403_MSG[128] = "Credentials required\n";
char const MD_HTTP_404_MSG[128] = "Not found\n";
char const MD_HTTP_409_MSG[128] = "Operation conflict\n";
char const MD_HTTP_413_MSG[128] = "Requested entry too big\n";
char const MD_HTTP_422_MSG[128] = "Unprocessable entry\n";
char const MD_HTTP_500_MSG[128] = "Internal Server Error\n";
char const MD_HTTP_501_MSG[128] = "Not implemented\n";
char const MD_HTTP_502_MSG[128] = "Bad gateway\n";
char const MD_HTTP_503_MSG[128] = "Service unavailable\n";
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

// make a RAM response which MHD will defensively copy
int md_create_HTTP_response_ram( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_COPY );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make a RAM response which MHD keep a pointer to and free later
int md_create_HTTP_response_ram_nocopy( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_FREE );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make a RAM response which MHD should not copy, but the caller will not free
int md_create_HTTP_response_ram_static( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_PERSISTENT );
   resp->status = status;
   MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   return 0;
}

// make an file-descriptor-based response
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


   SG_debug( "upload %zu bytes\n", size );

   struct md_HTTP_connection_data *md_con_data = (struct md_HTTP_connection_data*)coninfo_cls;
   md_response_buffer_t* rb = md_con_data->rb;

   // add a copy of the data
   char* data_dup = SG_CALLOC( char, size );
   memcpy( data_dup, data, size );

   rb->push_back( md_buffer_segment_t( data_dup, size ) );

   return MHD_YES;
}

// convert a sockaddr to a string containing the hostname and port number
static int md_sockaddr_to_hostname_and_port( struct sockaddr* addr, char** buf ) {
   socklen_t addr_len = 0;
   switch( addr->sa_family ) {
      case AF_INET:
         addr_len = sizeof(struct sockaddr_in);
         break;
         
      case AF_INET6:
         addr_len = sizeof(struct sockaddr_in6);
         break;
      
      default:
         SG_error("Address is not IPv4 or IPv6 (%d)\n", addr->sa_family);
         return -EINVAL;
   }
   
   *buf = SG_CALLOC( char, HOST_NAME_MAX + 10 );
   char portbuf[10];
   
   // prefix with :
   portbuf[0] = ':';
   
   // write hostname to buf, and portnum to portbuf + 1 (i.e. preserve the colon)
   int rc = getnameinfo( addr, addr_len, *buf, HOST_NAME_MAX + 1, portbuf + 1, 10, NI_NUMERICSERV );
   if( rc != 0 ) {
      SG_error("getnameinfo rc = %d (%s)\n", rc, gai_strerror(rc) );
      rc = -ENODATA;
   }
   
   // append port
   strcat( *buf, portbuf );
   
   return rc;
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
         SG_error( "malformed URL %s\n", url );
         return md_HTTP_default_send_response( connection, MHD_HTTP_BAD_REQUEST, NULL );
      }
      
      con_data = SG_CALLOC( struct md_HTTP_connection_data, 1 );
      if( !con_data ) {
         SG_error("%s\n", "out of memory" );
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
                  SG_error( "failed to create POST processor for %s\n", method);
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
                  SG_error( "failed to create POST processor for %s\n", method);
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
         struct md_HTTP_response* resp = SG_CALLOC(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
         free( con_data );
         
         SG_error("unknown HTTP method '%s'\n", method);
         
         return md_HTTP_send_response( connection, resp );
      }
      
      // get remote host 
      const union MHD_ConnectionInfo* con_info = MHD_get_connection_info( connection, MHD_CONNECTION_INFO_CLIENT_ADDRESS );
      if( con_info == NULL ) {
         // internal error
         struct md_HTTP_response* resp = SG_CALLOC(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
         free( con_data );
         
         SG_error("No connection info from daemon on '%s'\n", method);
         
         return md_HTTP_send_response( connection, resp );
      }
      
      struct sockaddr* client_addr = con_info->client_addr;
      char* remote_host = NULL;
      
      int rc = md_sockaddr_to_hostname_and_port( client_addr, &remote_host );
      if( rc != 0 ) {
         
         struct md_HTTP_response* resp = SG_CALLOC(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
         free( con_data );
         
         SG_error("md_sockaddr_to_hostname_and_port rc = %d\n", rc );
         
         return md_HTTP_send_response( connection, resp );
      }
         
      
      // build up con_data from what we know
      con_data->http = http_ctx;
      con_data->url_path = md_flatten_path( url );
      con_data->version = strdup(version);
      con_data->query_string = (char*)index( con_data->url_path, '?' );
      con_data->rb = new md_response_buffer_t();
      con_data->remote_host = remote_host;
      con_data->method = method;
      con_data->mode = mode;
      con_data->cls = NULL;
      con_data->status = 200;
      con_data->pp = pp;

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
      struct md_HTTP_header** headers = SG_CALLOC( struct md_HTTP_header*, headers_vec.size() + 1 );
      for( unsigned int i = 0; i < headers_vec.size(); i++ ) {
         headers[i] = headers_vec.at(i);
      }

      con_data->headers = headers;

      SG_debug("%s %s, query=%s, requester=%s\n", method, con_data->url_path, con_data->query_string, con_data->remote_host );
      
      *con_cls = con_data;
      
      // perform connection setup
      if( http_ctx->HTTP_connect != NULL ) {
         con_data->cls = (*http_ctx->HTTP_connect)( con_data );

         if( con_data->status >= 300 ) {
            // not going to serve data
            SG_error("connect status = %d\n", con_data->status );
            
            if( con_data->resp == NULL ) {
               md_create_HTTP_response_ram_static( con_data->resp, "text/plain", con_data->status, "connection error", strlen("connection error") + 1 );
            }
            
            return md_HTTP_send_response( connection, con_data->resp );
         }
      }

      return MHD_YES;
   }

   // POST
   if( con_data->mode == MD_HTTP_POST ) {

      if( *upload_size != 0 && con_data->status > 0 ) {
         if( con_data->pp ) {
            SG_debug( "POST %s, postprocess %zu bytes\n", con_data->url_path, *upload_size );
            MHD_post_process( con_data->pp, upload_data, *upload_size );
            *upload_size = 0;
            return MHD_YES;
         }
         else {
            SG_debug( "POST %s, raw %zu bytes\n", con_data->url_path, *upload_size );
            int rc = (*http_ctx->HTTP_POST_iterator)( con_data, MHD_POSTDATA_KIND, NULL, NULL, NULL, NULL, upload_data, con_data->offset, *upload_size );
            con_data->offset += *upload_size;
            *upload_size = 0;
            return rc;
         }
      }
      else {
         (*http_ctx->HTTP_POST_finish)( con_data );
         SG_debug( "POST finished (%s)\n", url);
         
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
            SG_debug( "PUT %s, postprocess %zu bytes\n", con_data->url_path, *upload_size );
            MHD_post_process( con_data->pp, upload_data, *upload_size );
            *upload_size = 0;
            return MHD_YES;
         }
         else {
            SG_debug( "PUT %s, raw %zu bytes\n", con_data->url_path, *upload_size );
            int rc = (*http_ctx->HTTP_PUT_iterator)( con_data, MHD_POSTDATA_KIND, NULL, NULL, NULL, NULL, upload_data, con_data->offset, *upload_size );
            con_data->offset += *upload_size;
            *upload_size = 0;
            return rc;
         }
      }
      else {
         
         (*http_ctx->HTTP_PUT_finish)( con_data );
         SG_debug( "PUT finished (%s)\n", url);
         
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
         resp = SG_CALLOC(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, (char*)"text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      }

      con_data->resp = resp;
      return md_HTTP_send_response( connection, con_data->resp );
   }
   
   // GET
   else if( con_data->mode == MD_HTTP_GET ) {
      
      struct md_HTTP_response* resp = (*http_ctx->HTTP_GET_handler)( con_data );
      if( resp == NULL ) {
         resp = SG_CALLOC(struct md_HTTP_response, 1);
         md_create_HTTP_response_ram_static( resp, (char*)"text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      }

      con_data->resp = resp;

      return md_HTTP_send_response( connection, resp );
   }
   
   // HEAD
   else if( con_data->mode == MD_HTTP_HEAD ) {
      
      struct md_HTTP_response* resp = (*http_ctx->HTTP_HEAD_handler)( con_data );
      if( resp == NULL ) {
         resp = SG_CALLOC(struct md_HTTP_response, 1);
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
   if( con_data->remote_host ) {
      free( con_data->remote_host );
      con_data->remote_host = NULL;
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
      md_response_buffer_free( con_data->rb );
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
int md_HTTP_init( struct md_HTTP* http, int server_type ) {
   memset( http, 0, sizeof(struct md_HTTP) );
   http->server_type = server_type;
   return 0;
}


// start the HTTP thread
int md_start_HTTP( struct md_HTTP* http, int portnum, struct md_syndicate_conf* conf ) {
   
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
         SG_debug( "Started HTTP server with SSL enabled (cert = %s, pkey = %s) on port %d\n", conf->server_cert_path, conf->server_key_path, portnum);
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
         SG_debug( "Started HTTP server on port %d\n", portnum);
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

static int md_parse_uint64( char* id_str, char const* fmt, uint64_t* out ) {
   uint64_t ret = 0;
   int rc = sscanf( id_str, fmt, &ret );
   if( rc == 0 ) {
      return -EINVAL;
   }
   else {
      *out = ret;
   }
   
   return 0;
}

static int md_parse_manifest_timestamp( char* _manifest_str, struct timespec* manifest_timestamp ) {
   long tv_sec = -1;
   long tv_nsec = -1;
   
   int num_read = sscanf( _manifest_str, "manifest.%ld.%ld", &tv_sec, &tv_nsec );
   if( num_read != 2 ) {
      return -EINVAL;
   }
   
   if( tv_sec < 0 || tv_nsec < 0 ) {
      return -EINVAL;
   }
   
   manifest_timestamp->tv_sec = tv_sec;
   manifest_timestamp->tv_nsec = tv_nsec;

   return 0;
}


static int md_parse_block_id_and_version( char* _block_id_version_str, uint64_t* _block_id, int64_t* _block_version ) {
   uint64_t block_id = INVALID_BLOCK_ID;
   int64_t block_version = 0;
   
   int num_read = sscanf( _block_id_version_str, "%" PRIu64 ".%" PRId64, &block_id, &block_version );
   if( num_read != 2 ) {
      return -EINVAL;
   }
   
   *_block_id = block_id;
   *_block_version = block_version;

   return 0;
}


// parse the file ID and version
// format is path.file_id.version 
// get "file_id" and "version"
static int md_parse_file_id_and_version( char* _name_id_and_version_str, uint64_t* _file_id, int64_t* _file_version ) {
   
   // scan back for the second-to-last '.'
   char* ptr = _name_id_and_version_str + strlen(_name_id_and_version_str);
   
   int num_periods = 0;
   while( num_periods < 2 ) {
      if( *ptr == '.' ) {
         num_periods ++;
      }
      ptr--;
      
      if( ptr < _name_id_and_version_str ) {
         break;
      }
   }
   ptr++;
   
   if( ptr == _name_id_and_version_str && num_periods < 2 ) {
      return -EINVAL;
   }
   
   uint64_t file_id = INVALID_FILE_ID;
   int64_t file_version = -1;
   
   int num_read = sscanf( ptr, ".%" PRIX64 ".%" PRId64, &file_id, &file_version );
   if( num_read != 2 ) {
      return -EINVAL;
   }
   
   *_file_id = file_id;
   *_file_version = file_version;
   
   return 0;
}

// clear the file ID from a string in the format of file_path.file_id 
static int md_clear_file_id( char* name_and_id ) {
   char* file_id_ptr = rindex( name_and_id, '.' );
   if( file_id_ptr == NULL ) {
      return -EINVAL;
   }
   
   *file_id_ptr = '\0';
   return 0;
}

// parse a URL in the format of:
// /$PREFIX/$volume_id/$file_path.$file_id.$file_version/($block_id.$block_version || manifest.$mtime_sec.$mtime_nsec)
int md_HTTP_parse_url_path( char const* _url_path, uint64_t* _volume_id, char** _file_path, uint64_t* _file_id, int64_t* _file_version, uint64_t* _block_id, int64_t* _block_version, struct timespec* _manifest_ts ) {
   char* url_path = strdup( _url_path );

   // temporary values
   uint64_t volume_id = INVALID_VOLUME_ID;
   char* file_path = NULL;
   uint64_t file_id = INVALID_FILE_ID;
   int64_t file_version = -1;
   uint64_t block_id = INVALID_BLOCK_ID;
   int64_t block_version = -1;
   struct timespec manifest_timestamp;
   manifest_timestamp.tv_sec = -1;
   manifest_timestamp.tv_nsec = -1;
   int rc = 0;


   int num_parts = 0;
   char* prefix = NULL;
   char* volume_id_str = NULL;

   bool is_manifest = false;
   int file_name_id_and_version_part = 0;
   int manifest_part = 0;
   int block_id_and_version_part = 0;
   size_t file_path_len = 0;

   char** parts = NULL;
   char* tmp = NULL;
   char* cursor = NULL;

   
   // break url_path into tokens, by /
   int num_seps = 0;
   for( unsigned int i = 0; i < strlen(url_path); i++ ) {
      if( url_path[i] == '/' ) {
         num_seps++;
         while( url_path[i] == '/' && i < strlen(url_path) ) {
            i++;
         }
      }
   }

   // minimum number of parts: data prefix, volume_id, path.file_id.file_version, (block.version || manifest.tv_sec.tv_nsec)
   if( num_seps < 4 ) {
      rc = -EINVAL;
      SG_error("num_seps = %d\n", num_seps );
      goto _md_HTTP_parse_url_path_finish;
   }

   num_parts = num_seps;
   parts = SG_CALLOC( char*, num_seps + 1 );
   tmp = NULL;
   cursor = url_path;
   
   for( int i = 0; i < num_seps; i++ ) {
      char* tok = strtok_r( cursor, "/", &tmp );
      cursor = NULL;

      if( tok == NULL ) {
         break;
      }

      parts[i] = tok;
   }
   
   prefix = parts[0];
   volume_id_str = parts[1];
   file_name_id_and_version_part = num_parts-2;
   manifest_part = num_parts-1;
   block_id_and_version_part = num_parts-1;

   if( strcmp(prefix, SYNDICATE_DATA_PREFIX) != 0 ) {
      // invalid prefix
      free( parts );
      rc = -EINVAL;
      SG_error("prefix = '%s'\n", prefix);
      goto _md_HTTP_parse_url_path_finish;
   }

   // volume ID?
   rc = md_parse_uint64( volume_id_str, "%" PRIu64, &volume_id );
   if( rc < 0 ) {
      free( parts );
      rc = -EINVAL;
      SG_error("could not parse '%s'\n", volume_id_str);
      goto _md_HTTP_parse_url_path_finish;
   }
   
   // is this a manifest request?
   if( strncmp( parts[manifest_part], "manifest", strlen("manifest") ) == 0 ) {
      rc = md_parse_manifest_timestamp( parts[manifest_part], &manifest_timestamp );
      if( rc == 0 ) {
         // success!
         is_manifest = true;
      }
   }

   if( !is_manifest ) {
      // not a manifest request, so we must have a block ID and block version 
      rc = md_parse_block_id_and_version( parts[block_id_and_version_part], &block_id, &block_version );
      if( rc != 0 ) {
         // invalid request--neither a manifest nor a block ID
         SG_error("could not parse '%s'\n", parts[block_id_and_version_part]);
         free( parts );
         rc = -EINVAL;
         goto _md_HTTP_parse_url_path_finish;
      }
   }
   
   // parse file ID and version
   rc = md_parse_file_id_and_version( parts[file_name_id_and_version_part], &file_id, &file_version );
   if( rc != 0 ) {
      // invalid 
      SG_error("could not parse ID and/or version of '%s'\n", parts[file_name_id_and_version_part] );
      free( parts );
      rc = -EINVAL;
      goto _md_HTTP_parse_url_path_finish;
   }
   
   // clear file version
   md_clear_version( parts[file_name_id_and_version_part] );
   
   // clear file ID 
   md_clear_file_id( parts[file_name_id_and_version_part] );

   // assemble the path
   for( int i = 2; i <= file_name_id_and_version_part; i++ ) {
      file_path_len += strlen(parts[i]) + 2;
   }

   file_path = SG_CALLOC( char, file_path_len + 1 );
   for( int i = 2; i <= file_name_id_and_version_part; i++ ) {
      strcat( file_path, "/" );
      strcat( file_path, parts[i] );
   }

   *_volume_id = volume_id;
   *_file_path = file_path;
   *_file_id = file_id;
   *_file_version = file_version;
   *_block_id = block_id;
   *_block_version = block_version;
   _manifest_ts->tv_sec = manifest_timestamp.tv_sec;
   _manifest_ts->tv_nsec = manifest_timestamp.tv_nsec;

   /*
   if( manifest_timestamp.tv_sec > 0 || manifest_timestamp.tv_nsec > 0 ) {
      SG_debug("Path is /%" PRIu64 "/%s.%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld\n", volume_id, file_path, file_id, file_version, manifest_timestamp.tv_sec, manifest_timestamp.tv_nsec );
   }
   else {
      SG_debug("Path is /%" PRIu64 "/%s.%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 "\n", volume_id, file_path, file_id, file_version, block_id, block_version );
   }
   */
   
   free( parts );

_md_HTTP_parse_url_path_finish:

   free( url_path );

   return rc;
}


// free a gateway_request_data
void md_gateway_request_data_free( struct md_gateway_request_data* reqdat ) {
   if( reqdat->fs_path ) {
      free( reqdat->fs_path );
   }
   memset( reqdat, 0, sizeof(struct md_gateway_request_data) );
}

