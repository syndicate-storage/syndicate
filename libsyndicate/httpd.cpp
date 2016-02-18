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


// find the text to return for a particular status code 
// return the default response if not found.
static char const* md_HTTP_response_builtin_text( int status ) {
   
   char const* page = NULL;

   // use a built-in status message
   switch( status ) {
      
      case 200: {
         page = MD_HTTP_200_MSG;
         break;
      }
      
      case 400: {
         page = MD_HTTP_400_MSG;
         break;
      }
      
      case 401: {
         page = MD_HTTP_401_MSG;
         break;
      }
      
      case 403: {
         page = MD_HTTP_403_MSG;
         break;
      }
      
      case 404: {
         page = MD_HTTP_404_MSG;
         break;
      }
      
      case 409: {
         page = MD_HTTP_409_MSG;
         break;
      }
      
      case 413: {
         page = MD_HTTP_413_MSG;
         break;
      }
      
      case 422: {
         page = MD_HTTP_422_MSG;
         break;
      }
      
      case 500: {
         page = MD_HTTP_500_MSG;
         break;
      }
      
      case 501: {
         page = MD_HTTP_501_MSG;
         break;
      }
      
      case 502: { 
         page = MD_HTTP_502_MSG;
         break;
      }
      
      case 503: {
         page = MD_HTTP_503_MSG;
         break;
      }
      
      case 504: {
         page = MD_HTTP_504_MSG;
         break;
      }
      
      default: {
         page = MD_HTTP_DEFAULT_MSG;
         break;
      }
   }
   
   return page;
}


// respond to a request with data (if non-NULL), or with a built-in response.
// either way, md_HTTP takes over responsibility for the data (i.e. the caller should NOT free it)
// return MHD_YES on success
// return MHD_NO on failure (i.e. OOM, or no data given when the status code doesn't match a built-in page)
static int md_HTTP_default_send_response( struct MHD_Connection* connection, int status_code, char* data ) {
   
   char const* page = NULL;
   struct MHD_Response* response = NULL;
   int rc = 0;
   
   if( data == NULL ) {
      
      page = md_HTTP_response_builtin_text( status_code );
      response = MHD_create_response_from_buffer( strlen(page), (void*)page, MHD_RESPMEM_PERSISTENT );
   }
   else {
      // use the given status message
      response = MHD_create_response_from_buffer( strlen(data), (void*)data, MHD_RESPMEM_MUST_FREE );
   }
   
   if( response == NULL ) {
      
      // no response allocated, or no data given
      return MHD_NO;
   }
   
   // this is always a text/plain type
   rc = MHD_add_response_header( response, "Content-Type", "text/plain" );
   if( rc != MHD_YES ) {
      
      // OOM 
      MHD_destroy_response( response );
      return MHD_NO;
   }
   
   rc = MHD_queue_response( connection, status_code, response );
   if( rc != MHD_YES ) {
      
      // OOM or related 
      MHD_destroy_response( response );
      return MHD_NO;
   }
   
   // unref
   MHD_destroy_response( response );
   
   return MHD_YES;
}

// make a built-in response (static RAM) from our built-in messages 
// return 0 on success 
// return -ENOMEM on OOM
int md_HTTP_create_response_builtin( struct md_HTTP_response* resp, int status ) {
   
   char const* page = NULL;

   page = md_HTTP_response_builtin_text( status );
   resp->resp = MHD_create_response_from_buffer( strlen(page), (void*)page, MHD_RESPMEM_PERSISTENT );
   
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// make a RAM response which MHD will defensively copy
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_create_response_ram( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   
   int rc = 0;
   
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_COPY );
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   rc = MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   if( rc != MHD_YES ) {
      
      MHD_destroy_response( resp->resp );
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// make a RAM response which MHD keep a pointer to and free later
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_create_response_ram_nocopy( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   
   int rc = 0;
   
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_MUST_FREE );
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   rc = MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   if( rc != MHD_YES ) {
      
      MHD_destroy_response( resp->resp );
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// make a RAM response which MHD should not copy, but the caller will not free
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_create_response_ram_static( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len ) {
   
   int rc = 0;
   
   resp->resp = MHD_create_response_from_buffer( len, (void*)data, MHD_RESPMEM_PERSISTENT );
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   rc = MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   if( rc != MHD_YES ) {
      
      MHD_destroy_response( resp->resp );
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// make an file-descriptor-based response
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_create_response_fd( struct md_HTTP_response* resp, char const* mimetype, int status, int fd, off_t offset, size_t size ) {
   
   int rc = 0;
   
   resp->resp = MHD_create_response_from_fd_at_offset64( size, fd, offset );
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   rc = MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   if( rc != MHD_YES ) {
      
      MHD_destroy_response( resp->resp );
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// make a callback response
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_create_response_stream( struct md_HTTP_response* resp, char const* mimetype, int status, uint64_t size, size_t blk_size, md_HTTP_stream_callback scb, void* cls, md_HTTP_free_cls_callback fcb ) {
   
   int rc = 0;
   
   resp->resp = MHD_create_response_from_callback( size, blk_size, scb, cls, fcb );
   if( resp->resp == NULL ) {
      return -ENOMEM;
   }
   
   rc = MHD_add_response_header( resp->resp, "Content-Type", mimetype );
   if( rc != MHD_YES ) {
      
      MHD_destroy_response( resp->resp );
      return -ENOMEM;
   }
   
   resp->status = status;
   return 0;
}

// give back a user-callback-created response
static int md_HTTP_send_response( struct MHD_Connection* connection, struct md_HTTP_response* resp ) {
   
   int rc = 0;
   
   rc = MHD_queue_response( connection, resp->status, resp->resp );
   
   SG_debug("connection %p: HTTP %d\n", connection, resp->status );
   
   // safe to unref 
   MHD_destroy_response( resp->resp );
   md_HTTP_response_free( resp );
   SG_safe_free( resp );
   
   return rc;
}


// free an HTTP response
void md_HTTP_response_free( struct md_HTTP_response* resp ) {
   return;
}


// find an http header value
// return a const char* pointer to the header on success
// return NULL on not found
char const* md_HTTP_header_lookup( struct md_HTTP_header** headers, char const* header ) {
   for( int i = 0; headers[i] != NULL; i++ ) {
      
      if( strcasecmp( headers[i]->header, header ) == 0 ) {
         return headers[i]->value;
      }
   }
   return NULL;
}

// add a header
// return 0 on success
// return -ENOMEM on error
int md_HTTP_header_add( struct md_HTTP_response* resp, char const* header, char const* value ) {
   if( resp->resp != NULL ) {
      
      int rc = MHD_add_response_header( resp->resp, header, value );
      if( rc == MHD_NO ) {
         return -ENOMEM;
      }
   }
   return 0;
}

// create an http header
// return 0 on success
// return -ENOMEM on OOM
int md_HTTP_header_create( struct md_HTTP_header* header, char const* h, char const* v ) {
   
   header->header = SG_strdup_or_null( h );
   if( header->header == NULL ) {
      return -ENOMEM;
   }
   
   header->value = SG_strdup_or_null( v );
   if( header->value == NULL ) {
      
      SG_safe_free( header->header );
      return -ENOMEM;
   }
   
   return 0;
}

// free an HTTP header
// always succeeds
void md_HTTP_header_free( struct md_HTTP_header* header ) {
   
   if( header->header != NULL ) {
      SG_safe_free( header->header );
   }
   if( header->value != NULL ) {
      SG_safe_free( header->value );
   }
   
   memset( header, 0, sizeof(struct md_HTTP_header) );
}


// accumulate inbound headers callback
// return MHD_YES on success
// return MHD_NO on failure
// NOTE: if we fail on allocating memory, free the whole header list and set the first entry to (struct md_HTTP_header*)(-ENOMEM)
static int md_accumulate_headers( void* cls, enum MHD_ValueKind kind, char const* key, char const* value ) {
   
   int rc = 0;
   int i = 0;
   struct md_HTTP_header** headers = (struct md_HTTP_header**)cls;
   
   struct md_HTTP_header* hdr = SG_CALLOC( struct md_HTTP_header, 1 );
   if( hdr == NULL ) {
      
      // OOM 
      for( i = 0; headers[i] != NULL; i++ ) {
         md_HTTP_header_free( headers[i] );
      }
      
      headers[0] = (struct md_HTTP_header*)(-ENOMEM);
      
      return MHD_NO;
   }
   
   rc = md_HTTP_header_create( hdr, key, value );
   if( rc != 0 ) {
      
      // OOM 
      for( i = 0; headers[i] != NULL; i++ ) {
         md_HTTP_header_free( headers[i] );
      }
      
      headers[0] = (struct md_HTTP_header*)(-ENOMEM);
      return MHD_NO;
   }
   
   // insert
   for( i = 0; headers[i] != NULL; i++ );
   headers[i] = hdr;
   
   return MHD_YES;
}

// free a list of headers
static void md_free_headers( struct md_HTTP_header** headers ) {
   
   for( unsigned int i = 0; headers[i] != NULL; i++ ) {
      
      md_HTTP_header_free( headers[i] );
      SG_safe_free( headers[i] );
   }
   
   SG_safe_free( headers );
}


// multiplex uploads by POST field (key), routing them to individual field handlers.
// "*" is the catch-all field handler, if a more specific match cannot be found.
// return MHD_YES on success
// return MHD_NO on OOM or field handler error
int md_HTTP_post_upload_iterator( void *coninfo_cls, enum MHD_ValueKind kind,
                                  const char *key,
                                  const char *filename, const char *content_type,
                                  const char *transfer_encoding, const char *data,
                                  uint64_t off, size_t size) {

   SG_debug( "field '%s': upload %zu bytes\n", key, size );

   struct md_HTTP_connection_data *md_con_data = (struct md_HTTP_connection_data*)coninfo_cls;
   struct md_HTTP* http = md_con_data->http;
   int rc = MHD_YES;
   
   SG_HTTP_post_field_handler_t handler = NULL;
   struct SG_HTTP_post_field* field = NULL;
   
   try {
      
      string field_name(key);
      
      SG_HTTP_post_field_handler_map_t::iterator itr = http->upload_field_handlers->find( field_name );
      if( itr != http->upload_field_handlers->end() ) {
         
         handler = itr->second;
      }
      else {
         
         // is there a generic handler?
         field_name = string("*");
         
         itr = http->upload_field_handlers->find( field_name );
         if( itr != http->upload_field_handlers->end() ) {
            
            handler = itr->second;
         }
      }
      
      // find the matching field
      SG_HTTP_post_field_map_t::iterator field_itr = md_con_data->post_fields->find( field_name );
      if( field_itr != md_con_data->post_fields->end() ) {
         
         field = &field_itr->second;
      }
      else {
         
         // no field data; ignore
         SG_warn("No field data for '%s'\n", key );
         return MHD_YES;
      }
   }
   catch( bad_alloc& ba ) {
      
      // OOM 
      rc = MHD_NO;
   }
   
   if( rc == MHD_YES && handler != NULL ) {
   
      rc = (*handler)( key, filename, data, off, size, field );
      
      if( rc != 0 ) {
         
         SG_error("Field handler for '%s': rc = %d\n", key, rc );
         rc = MHD_NO;
      }
      else {
         rc = MHD_YES;
      }
   }
   
   return rc;
}

// convert a sockaddr to a string containing the hostname and port number
// return 0 on success
// return -ENODATA on getnameinfo() failure 
// return -ENOMEM on OOM
static int md_sockaddr_to_hostname_and_port( struct sockaddr* addr, char** buf ) {
   
   socklen_t addr_len = 0;
   switch( addr->sa_family ) {
      
      case AF_INET: {
         
         addr_len = sizeof(struct sockaddr_in);
         break;
      }
      
      case AF_INET6: {
         
         addr_len = sizeof(struct sockaddr_in6);
         break;
      }
      
      default: {
         SG_error("Address is not IPv4 or IPv6 (%d)\n", addr->sa_family);
         return -EINVAL;
      }
   }
   
   *buf = SG_CALLOC( char, HOST_NAME_MAX + 10 );
   if( *buf == NULL ) {
      return -ENOMEM;
   }
   
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


// field handler for holding data in a response buffer (RAM)
// return 0 on success
// return -EINVAL if cls is NULL
// return -ENOMEM on OOM 
// return -EOVERFLOW if the message is too big
int md_HTTP_post_field_handler_ram( char const* field_name, char const* filename, char const* data, off_t offset, size_t len, void* cls ) {
   
   struct SG_HTTP_post_field* field = (struct SG_HTTP_post_field*)cls;
   char* data_dup = NULL;
   
   // sanity checks
   if( field == NULL ) {
      return -EINVAL;
   }
   
   if( field->rb == NULL ) {
      return -EINVAL;
   }

   // cap size 
   if( len + field->num_written > field->max_size ) {
      return -EOVERFLOW;
   }

   // store data
   data_dup = SG_CALLOC( char, len );
   if( data_dup == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( data_dup, data, len );
   
   try {
      field->rb->push_back( md_buffer_segment_t( data_dup, len ) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( data_dup );
      return -ENOMEM;
   }
   
   return 0;
}


// field handler for holding data in a temporary file (disk)
// return 0 on success
// return -errno on write error
// return -EOVERFLOW if the message is too big
int md_HTTP_post_field_handler_disk( char const* field_name, char const* filename, char const* data, off_t offset, size_t len, void* cls ) {
   
   struct SG_HTTP_post_field* field = (struct SG_HTTP_post_field*)cls;
   ssize_t nw = 0;
   
   // sanity checks
   if( field == NULL ) {
      return -EINVAL;
   }
   
   if( field->tmpfd < 0 || field->tmpfd_path == NULL ) {
      return -EINVAL;
   }

   // cap size 
   if( len + field->num_written > field->max_size ) {
      return -EOVERFLOW;
   }

   // store data 
   nw = md_write_uninterrupted( field->tmpfd, data, len );
   if( nw < 0 || (size_t)nw != len ) {
      
      SG_error("md_write_uninterrupted('%s' (%d), %zu) rc = %zd\n", field->tmpfd_path, field->tmpfd, len, nw );
      return (int)nw;
   }
   
   return 0;
}


// get an uploaded field's contents from RAM.
// the field must have been uploaded by md_HTTP_post_field_handler_ram
// return 0 on success, and set *buf and *buflen to the contents and length (non-null-terminated)
// return -ENOMEM on OOM
// return -EINVAL if the field was not uploaded to RAM 
// return -ENOENT if there is no such field 
int md_HTTP_upload_get_field_buffer( struct md_HTTP_connection_data* con_data, char const* field_name, char** buf, size_t* buflen ) {
  
   struct SG_HTTP_post_field* field = NULL;
   
   try {
      
      SG_HTTP_post_field_map_t::iterator itr = con_data->post_fields->find( string(field_name) );
      if( itr != con_data->post_fields->end() ) {
         
         field = &itr->second;
      }
      else {
         
         // not found 
         return -ENOENT;
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   // have ram buffer?
   if( field->rb == NULL ) {
      return -EINVAL;
   }
   
   // get a copy 
   *buf = md_response_buffer_to_string( field->rb );
   *buflen = md_response_buffer_size( field->rb );
   
   if( *buf == NULL ) {
      
      return -ENOMEM;
   }
   
   return 0;
}

// get an uploaded field's temporary file path and descriptor.
// the field must have been uploaded by md_HTTP_post_field_handler_disk
// return 0 on success, and set *path and *fd to the file path and file descriptor, respectively (if they are not NULL)
// return -ENOMEM on OOM 
// return -EINVAL if the field was not uploaded to disk 
// return -ENOENT if there is no such field
int md_HTTP_upload_get_field_tmpfile( struct md_HTTP_connection_data* con_data, char const* field_name, char** path, int* fd ) {
   
   struct SG_HTTP_post_field* field = NULL;
   
   try {
      
      SG_HTTP_post_field_map_t::iterator itr = con_data->post_fields->find( string(field_name) );
      if( itr != con_data->post_fields->end() ) {
         
         field = &itr->second;
         
      }
      else {
         
         // not found 
         return -ENOENT;
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
      
   // have disk buffer?
   if( field->tmpfd < 0 || field->tmpfd_path == NULL ) {
      return -EINVAL;
   }
   
   // get copies 
   if( path != NULL ) {
      *path = SG_strdup_or_null( field->tmpfd_path );
         
      if( *path == NULL ) {
         
         return -ENOMEM;
      }
   }
   
   if( fd != NULL ) {
      *fd = field->tmpfd;
   }
   
   return 0;
}


// free a field 
// always succeeds
static int md_HTTP_post_field_free( struct SG_HTTP_post_field* field ) {
   
   if( field->tmpfd >= 0 ) {
      
      close( field->tmpfd );
      field->tmpfd = -1;
   }
   
   if( field->tmpfd_path != NULL ) {
      
      SG_safe_free( field->tmpfd_path );
   }
   
   if( field->rb ) {
      
      md_response_buffer_free( field->rb );
      SG_safe_delete( field->rb );
   }
   
   return 0;
}

// free a field map 
// always succeeds
static int md_HTTP_post_field_map_free( SG_HTTP_post_field_map_t* fields ) {
   
   for( SG_HTTP_post_field_map_t::iterator itr = fields->begin(); itr != fields->end(); itr++ ) {
      
      md_HTTP_post_field_free( &itr->second );
   }
   
   fields->clear();
   
   return 0;
}


// unlink all temporary files in a field map 
// always succeeds
static int md_HTTP_post_field_unlink_tmpfiles( SG_HTTP_post_field_map_t* fields ) {
   
   for( SG_HTTP_post_field_map_t::iterator itr = fields->begin(); itr != fields->end(); itr++ ) {
      
      if( itr->second.tmpfd_path != NULL ) {
         
         int rc = unlink( itr->second.tmpfd_path );
         if( rc != 0 ) {
            
            rc = -errno;
            if( rc != -ENOENT ) {
               
               SG_warn("unlink('%s') rc = %d\n", itr->second.tmpfd_path, rc );
            }
         }
      }
   }
   
   return 0;
}

// set up a post processor 
// return 0 on success
// return -ENODATA if we failed to set one up 
// return -ENOMEM if OOM
static int md_HTTP_connection_setup_upload( struct md_HTTP* http_ctx, struct md_HTTP_connection_data* con_data, struct MHD_Connection* connection ) {
   
   struct MHD_PostProcessor* pp = NULL;
   SG_HTTP_post_field_map_t* field_data = NULL;
   int rc = 0;
   
   pp = MHD_create_post_processor( connection, 4096, md_HTTP_post_upload_iterator, con_data );
   if( pp == NULL ) {
      
      SG_error( "%s", "failed to create POST processor\n");
      
      return -ENODATA;
   }
   
   // new field holders
   field_data = SG_safe_new( SG_HTTP_post_field_map_t() );
   if( field_data == NULL ) {
      
      MHD_destroy_post_processor( pp );
      return -ENOMEM;
   }
   
   // create field holders for each field we expect to handle
   for( SG_HTTP_post_field_handler_map_t::iterator itr = http_ctx->upload_field_handlers->begin(); itr != http_ctx->upload_field_handlers->end(); itr++ ) {
      
      char const* field_name = itr->first.c_str();
      SG_HTTP_post_field_handler_t field_handler = itr->second;
      struct SG_HTTP_post_field field;
      
      memset( &field, 0, sizeof(struct SG_HTTP_post_field) );
      field.tmpfd = -1;
      
      // hold field in RAM?
      if( field_handler == md_HTTP_post_field_handler_ram ) {
         
         // set up a RAM field 
         field.rb = SG_safe_new( md_response_buffer_t() );
         if( field.rb == NULL ) {
            
            // out of memory 
            rc = -ENOMEM;
            break;
         }

         field.max_size = http_ctx->max_ram_upload_size;
      }
      
      // hold on disk?
      else if( field_handler == md_HTTP_post_field_handler_disk ) {
         
         // set up a disk field 
         field.tmpfd_path = SG_strdup_or_null( SG_HTTP_TMPFILE_FORMAT );
         if( field.tmpfd_path == NULL ) {
            
            // out of memory 
            rc = -ENOMEM;
            break;
         }
         
         field.tmpfd = mkstemp( field.tmpfd_path );
         if( field.tmpfd < 0 ) {
            
            // mkstemp error 
            rc = -errno;
            SG_error( "mkstemp('%s') rc = %d\n", field.tmpfd_path, rc );
            
            SG_safe_free( field.tmpfd_path );
            break;
         }

         field.max_size = http_ctx->max_disk_upload_size;
      }
      
      try {
         
         (*field_data)[ string(field_name) ] = field;
      }
      catch( bad_alloc& ba ) {
         
         md_HTTP_post_field_free( &field );
         rc = -ENOMEM;
         break;
      }
   }
   
   if( rc != 0 ) {
      
      // abort 
      md_HTTP_post_field_map_free( field_data );
      SG_safe_delete( field_data );
      MHD_destroy_post_processor( pp );
   }
   else {
      
      // success!
      con_data->pp = pp;
      con_data->post_fields = field_data;
   }
   
   return rc;
}


// convert the string representation of an HTTP method to a nuermic one 
// return the numeric mode ( > 0) on success
// return -EINVAL if not recognized 
static int md_HTTP_parse_method( char const* method ) {
   
   if( strcmp(method, "HEAD" ) == 0 ) {
      return MD_HTTP_HEAD;
   }
   else if( strcmp( method, "GET" ) == 0 ) {
      return MD_HTTP_GET;
   }
   else if( strcmp(method, "POST") == 0 ) {
      return MD_HTTP_POST;
   }
   else if( strcmp(method, "PUT") == 0 ) {
      return MD_HTTP_PUT;
   }
   else if( strcmp(method, "DELETE") == 0 ) {
      return MD_HTTP_DELETE;
   }
   
   return -EINVAL;
}


// is a method supported by our server?
// return 0 if so 
// return -ENOSYS if not 
static int md_HTTP_is_supported( struct md_HTTP* http_ctx, int method ) {
   
   if( method == MD_HTTP_GET && http_ctx->HTTP_GET_handler != NULL ) {
      return 0;
   }
   else if( method == MD_HTTP_HEAD && http_ctx->HTTP_HEAD_handler != NULL ) {
      return 0;
   }
   else if( method == MD_HTTP_POST && http_ctx->HTTP_POST_finish != NULL ) {
      return 0;
   }
   else if( method == MD_HTTP_PUT && http_ctx->HTTP_PUT_finish != NULL ) {
      return 0;
   }
   else if( method == MD_HTTP_DELETE && http_ctx->HTTP_DELETE_handler != NULL ) {
      return 0;
   }
   
   return -ENOSYS;
}


// parse the HTTP version 
// return X*10 + Y for version X.Y on success
// return -EINVAL on parse error
static int md_HTTP_parse_version( char const* http_version ) {
   
   int major = 0;
   int minor = 0;
   
   int rc = sscanf( http_version, "%d.%d", &major, &minor );
   if( rc != 2 ) {
      return -EINVAL;
   }
   
   return major * 10 + minor;
}


// open a new HTTP connection
// set up the given connection data, and call the http context's HTTP_connect method if given.
// return 0 on success
// return -EINVAL if we got a malformed URL, no headers were given, or we got a malformed Content-Length header
// return -ENOMEM on OOM 
// return -ENODATA if the caller is POST'ing or PUT'ing and we failed to create a post processor context 
// return -ENOSYS if the HTTP method is not supported
// return -ENOTCONN if we failed to look up the remote host 
// return -ECONNREFUSED if the caller's HTTP_connect method failed
static int md_HTTP_connection_setup( struct md_HTTP* http_ctx, struct md_HTTP_connection_data* con_data, struct MHD_Connection* connection, char const* url, char const* method, char const* version ) {

   // opening a new connection
   int mode = MD_HTTP_UNKNOWN;
   const union MHD_ConnectionInfo* con_info = NULL;
   char* remote_host = NULL;
   int rc = 0;
   char const* content_length_str = NULL;
   char* tmp = NULL;
   int num_headers = 0;
   struct md_HTTP_header** headers = NULL;
   long content_length = 0;
   char* url_path = NULL;
   
   // verify that the URL starts with '/'
   if( strlen(url) > 0 && url[0] != '/' ) {
      
      SG_error( "malformed URL '%s'\n", url );
      return -EINVAL;
   }
   
   // what's the method?
   mode = md_HTTP_parse_method( method );
   if( mode < 0 ) {
      
      SG_error("Unrecognized HTTP method '%s'\n", method );
      return -EINVAL;
   }
   
   // is it supported?
   if( md_HTTP_is_supported( http_ctx, mode ) != 0 ) {
      
      SG_error("Unsupported HTTP method '%s'\n", method );
      return -ENOSYS;
   }
   
   // get remote host info 
   con_info = MHD_get_connection_info( connection, MHD_CONNECTION_INFO_CLIENT_ADDRESS );
   if( con_info == NULL ) {
      
      
      SG_error("No connection info on '%s'\n", method);
      
      // internal error
      return -ENOTCONN;
   }
   
   // look up host:port string
   rc = md_sockaddr_to_hostname_and_port( con_info->client_addr, &remote_host );
   if( rc != 0 ) {
      
      SG_error("md_sockaddr_to_hostname_and_port rc = %d\n", rc );
      
      return -ENOTCONN;
   }
   
   // build up con_data from what we know
   // content length...
   content_length_str = MHD_lookup_connection_value( connection, MHD_HEADER_KIND, MHD_HTTP_HEADER_CONTENT_LENGTH );
   if( content_length_str != NULL ) {
      
      content_length = strtol( content_length_str, &tmp, 10 );
      if( (tmp != NULL && *tmp != '\0') || content_length == LONG_MAX ) {
         
         // invalid content length 
         SG_safe_free( remote_host );
         return -EINVAL;
      }
   }
   else {
      
      content_length = 0;
   }
   
   // get headers
   num_headers = MHD_get_connection_values( connection, MHD_HEADER_KIND, NULL, NULL );
   if( num_headers <= 0 ) {
      
      SG_error("%s: No headers\n", method );
      
      SG_safe_free( remote_host );
      
      return -EINVAL;
   }
   
   headers = SG_CALLOC( struct md_HTTP_header*, num_headers + 1 );
   if( headers == NULL ) {
      
      // OOM 
      SG_safe_free( remote_host );
      
      return -ENOMEM;
   }
   
   MHD_get_connection_values( connection, MHD_HEADER_KIND, md_accumulate_headers, headers );
   
   // check for error 
   if( (intptr_t)headers[0] == -ENOMEM ) {
      
      // OOM 
      SG_safe_free( headers );
      SG_safe_free( remote_host );
      
      return -ENOMEM;
   }
   
   // URL
   url_path = md_flatten_path( url );
   
   if( url_path == NULL ) {
      
      // OOM
      SG_safe_free( url_path );
      SG_safe_free( remote_host );
      
      md_free_headers( headers );
      
      return -ENOMEM;
   }

   SG_debug("%s", "Begin Headers:\n");
   for( int i = 0; headers[i] != NULL; i++ ) {
      SG_debug("%s: %s\n", headers[i]->header, headers[i]->value);
   }
   SG_debug("%s", "End Headers\n");
   
   // uploading?
   if( mode == MD_HTTP_POST || mode == MD_HTTP_PUT ) {
      
      rc = md_HTTP_connection_setup_upload( http_ctx, con_data, connection );
      if( rc != 0 ) {
         
         SG_error("md_HTTP_connection_setup_upload( %s '%s' ) rc = %d\n", method, url, rc );
         
         SG_safe_free( remote_host );
         SG_safe_free( url_path );
         
         md_free_headers( headers );
         return rc;
      }
   }
   
   // Got everything!
   
   // HTTP version
   con_data->version = md_HTTP_parse_version( version );
   if( con_data->version < 0 ) {
      con_data->version = 0;
   }
   
   // connection data
   con_data->http = http_ctx;
   con_data->headers = headers;
   con_data->url_path = url_path;
   con_data->query_string = (char*)strchr( con_data->url_path, '?' );
   con_data->remote_host = remote_host;
   con_data->mode = mode;
   con_data->cls = NULL;
   con_data->status = 200;
   con_data->content_length = content_length;
   con_data->connection = connection;
   con_data->suspended = false;

   // split query string off of url_path
   if( con_data->query_string != NULL ) {
      char* p = con_data->query_string;
      *p = 0;
      con_data->query_string = p + 1;
   }
   
   SG_debug("connection %p: %s %s, query=%s, remote_host=%s\n", connection, method, con_data->url_path, con_data->query_string, con_data->remote_host );
   
   // perform connection setup
   if( http_ctx->HTTP_connect != NULL ) {
      
      // do caller-given connection setup 
      rc = (*http_ctx->HTTP_connect)( con_data, &con_data->cls );

      if( rc != 0 ) {
         
         // not going to serve data
         SG_error("HTTP_connect('%s', '%s') rc = %d\n", url, con_data->remote_host, rc );
         
         SG_safe_free( con_data->url_path );
         SG_safe_free( remote_host );
         
         md_free_headers( headers );
         con_data->headers = NULL;
         
         return -ECONNREFUSED;
      }
   }
   
   return 0;
}


// handle an HTTP method.
// return 0 on success, and allocate and fill in *resp
// return -ENOMEM on OOM 
static int md_HTTP_do_method( char const* method_name, SG_HTTP_method_t method, struct MHD_Connection* connection, struct md_HTTP_connection_data* con_data, struct md_HTTP_response** ret_resp ) {

   struct md_HTTP_response* resp = NULL;
   int rc = 0;
   
   resp = SG_CALLOC( struct md_HTTP_response, 1 );
   if( resp == NULL ) {
      
      // OOM 
      return -ENOMEM;
   }
   
   rc = (*method)( con_data, resp );
   if( rc != 0 ) {
      
      // failed to generate a response 
      SG_error("%s('%s') rc = %d\n", method_name, con_data->url_path, rc );
      SG_safe_free( resp );
      
      return rc;
   }
   else {
      
      // generated a response!
      *ret_resp = resp;
      return 0;
   }
}


// dispatch an HTTP method 
// this finishes up the connection handler's work 
static int md_HTTP_dispatch_method( struct md_HTTP* http_ctx, struct md_HTTP_connection_data* con_data ) {
   
   int rc = 0;
   char const* method = NULL;
   SG_HTTP_method_t cb = NULL;
   struct md_HTTP_response* resp = NULL;
   
   switch( con_data->mode ) {
      
      case MD_HTTP_GET: {
         
         method = "GET";
         cb = http_ctx->HTTP_GET_handler;
         break;
      }
      
      case MD_HTTP_HEAD: {
         
         method = "HEAD";
         cb = http_ctx->HTTP_HEAD_handler;
         break;
      }
      
      case MD_HTTP_POST: {
         
         method = "POST";
         cb = http_ctx->HTTP_POST_finish;
         break;
      }
      
      case MD_HTTP_PUT: {
         
         method = "PUT";
         cb = http_ctx->HTTP_PUT_finish;
         break;
      }
      
      case MD_HTTP_DELETE: {
         
         method = "DELETE";
         cb = http_ctx->HTTP_DELETE_handler;
         break;
      }
      
      default: {
         
         return md_HTTP_default_send_response( con_data->connection, 501, NULL );
      }
   }

   rc = md_HTTP_do_method( method, cb, con_data->connection, con_data, &resp );
   if( rc != 0 ) {
      
      // failed to generate a response
      return md_HTTP_default_send_response( con_data->connection, 500, NULL );
   }
   else {
      
      // were we suspended?
      if( con_data->suspended ) {
         
         // don't send a response 
         return MHD_YES;
      }
      
      // generated a response!
      return md_HTTP_send_response( con_data->connection, resp );
   }
}

// HTTP connection handler, fed into libmicrohttpd
// return MHD_YES on successful handing 
// return MHD_NO if the connection should be terminated
static int md_HTTP_connection_handler( void* cls, struct MHD_Connection* connection, 
                                       char const* url, 
                                       char const* method, 
                                       char const* version, 
                                       char const* upload_data, size_t* upload_size, 
                                       void** con_cls ) {
   
   struct md_HTTP* http_ctx = (struct md_HTTP*)cls;
   struct md_HTTP_connection_data* con_data = (struct md_HTTP_connection_data*)(*con_cls);
   int rc = 0;
   
   // need to create connection data?
   if( con_data == NULL ) {

      // opening a new connection
      con_data = SG_CALLOC( struct md_HTTP_connection_data, 1 );
      if( con_data == NULL ) {
         
         // OOM
         return md_HTTP_default_send_response( connection, 500, NULL );
      }

      rc = md_HTTP_connection_setup( http_ctx, con_data, connection, url, method, version );
      
      if( rc != 0 ) {
         
         SG_error("md_HTTP_connection_setup('%s') rc = %d\n", url, rc );
         
         SG_safe_free( con_data );
         
         if( rc == -ENOSYS ) {
            return md_HTTP_default_send_response( connection, 501, NULL );
         }
         else if( rc == -EINVAL || rc == -ENODATA ) {
            return md_HTTP_default_send_response( connection, 400, NULL );
         }
         else {
            return md_HTTP_default_send_response( connection, 500, NULL );
         }
      }
      
      *con_cls = con_data;

      return MHD_YES;
   }
   
   // are we suspended? then don't block 
   if( con_data->suspended ) {
      return MHD_YES;
   }
   
   // were we resumed and given a response? if so, send it
   if( con_data->resume_resp != NULL ) {
      
      struct md_HTTP_response* resp = con_data->resume_resp;
      con_data->resume_resp = NULL;
      
      return md_HTTP_send_response( connection, resp );
   }
   
   // need to receive data via an iterator?
   if( con_data->mode == MD_HTTP_POST || con_data->mode == MD_HTTP_PUT ) {
      
      if( *upload_size != 0 ) {
         
         // have data!
         // send through the postprocessor 
         SG_debug( "%s %s, postprocess %zu bytes\n", method, con_data->url_path, *upload_size );
         
         rc = MHD_post_process( con_data->pp, upload_data, *upload_size );
         
         if( rc == MHD_NO ) {
            
            // failed (parse error, OOM, etc)
            return md_HTTP_default_send_response( connection, 500, NULL );
         }
         else {
            
            // succeeded
            *upload_size = 0;
            return MHD_YES;
         }
      }
      else {
         
         // got all data.  process it and dispatch the response
         return md_HTTP_dispatch_method( http_ctx, con_data );
      }
   }
   else {
      
      // no data to receive--just dispatch to the handler 
      return md_HTTP_dispatch_method( http_ctx, con_data );
   }
}


// suspend a connection.
// must be resumed later.
// return 0 on success
// return -EINVAL if already suspended
int md_HTTP_connection_suspend( struct md_HTTP_connection_data* con_data ) {
   
   if( con_data->suspended ) {
      return -EINVAL;
   }
   
   MHD_suspend_connection( con_data->connection );

   con_data->suspended = true;
   
   SG_debug("Suspend connection %p\n", con_data->connection );
   
   return 0;
}


// resume a connection 
// return 0 on success
// return -EINVAL if already resumed 
int md_HTTP_connection_resume( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   if( !con_data->suspended ) {
      return -EINVAL;
   }
   
   // send this response back once we resume 
   con_data->resume_resp = resp;
   
   // send it back
   MHD_resume_connection( con_data->connection );
   con_data->suspended = false;
      
   SG_debug("Resume connection %p\n", con_data->connection );
      
   return 0;
}


// free a connection state
void md_HTTP_free_connection_data( struct md_HTTP_connection_data* con_data ) {
   
   if( con_data->pp != NULL ) {
      
      MHD_destroy_post_processor( con_data->pp );
      con_data->pp = NULL;
   }
   
   if( con_data->url_path != NULL ) {
      
      SG_safe_free( con_data->url_path );
   }
   
   if( con_data->remote_host != NULL ) {
      
      SG_safe_free( con_data->remote_host );
   }
   
   if( con_data->headers != NULL ) {
      
      md_free_headers( con_data->headers );
      con_data->headers = NULL;
   }
   
   if( con_data->post_fields != NULL ) {
      
      // remove temprary files
      md_HTTP_post_field_unlink_tmpfiles( con_data->post_fields );
      md_HTTP_post_field_map_free( con_data->post_fields );
      SG_safe_delete( con_data->post_fields );
   }
   
   memset( con_data, 0, sizeof( struct md_HTTP_connection_data ) );
}


// default cleanup handler
// calls user-supplied cleanup handler as well
void md_HTTP_cleanup( void *cls, struct MHD_Connection *connection, void **con_cls, enum MHD_RequestTerminationCode term ) {
   
   struct md_HTTP* http = (struct md_HTTP*)cls;
   struct md_HTTP_connection_data* con_data = NULL;
   
   if( con_cls != NULL ) {
      
      con_data = (struct md_HTTP_connection_data*)(*con_cls);
   }
   
   if( http->HTTP_cleanup != NULL && con_data != NULL ) {
      
      // do the clean up
      (*http->HTTP_cleanup)( con_data->cls );
      con_data->cls = NULL;
   }
   if( con_data != NULL ) {
      
      md_HTTP_free_connection_data( con_data );
      SG_safe_free( con_data );
      
      *con_cls = NULL;
   }
}


// set fields in an HTTP structure
int md_HTTP_init( struct md_HTTP* http, int server_type, void* server_cls ) {
   memset( http, 0, sizeof(struct md_HTTP) );
   
   http->upload_field_handlers = SG_safe_new( SG_HTTP_post_field_handler_map_t() );
   if( http->upload_field_handlers == NULL ) {
      return -ENOMEM;
   }
   
   http->server_type = server_type;
   http->server_cls = server_cls;
   
   http->max_ram_upload_size = 1024*1024;   // 1MB default
   http->max_disk_upload_size = 100 * 1024 * 1024;  // 100MB default
   return 0;
}


// set HTTP limits 
int md_HTTP_set_limits( struct md_HTTP* http, uint64_t max_ram_upload_size, uint64_t max_disk_upload_size ) {
   http->max_ram_upload_size = max_ram_upload_size;
   http->max_disk_upload_size = max_disk_upload_size;
   return 0;
}


// start the HTTP thread
// return 0 on success
// return -EPERM on failure to start
int md_HTTP_start( struct md_HTTP* http, int portnum ) {
   
   int rc = 0;
   int num_http_threads = sysconf( _SC_NPROCESSORS_CONF );

   if( (http->server_type & MHD_USE_THREAD_PER_CONNECTION) != 0 ) {
      http->http_daemon = MHD_start_daemon(  http->server_type | MHD_USE_DEBUG, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                             MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                             MHD_OPTION_END );
   }
   else {
      http->http_daemon = MHD_start_daemon(  http->server_type | MHD_USE_SUSPEND_RESUME | MHD_USE_PIPE_FOR_SHUTDOWN | MHD_USE_DEBUG, portnum, NULL, NULL, &md_HTTP_connection_handler, http,
                                             MHD_OPTION_THREAD_POOL_SIZE, num_http_threads,
                                             MHD_OPTION_NOTIFY_COMPLETED, md_HTTP_cleanup, http,
                                             MHD_OPTION_END );
   }
   
   if( http->http_daemon != NULL ) {
      SG_debug( "Started HTTP server on port %d\n", portnum);
   }
   else {
      rc = -EPERM;
   }
   
   http->running = true;
   
   return rc;
}

// stop the HTTP thread
int md_HTTP_stop( struct md_HTTP* http ) {
   MHD_stop_daemon( http->http_daemon );
   http->http_daemon = NULL;
   
   http->running = false;
   return 0;
}

// free the HTTP server
// always succeeds
int md_HTTP_free( struct md_HTTP* http ) {
   
   if( http->upload_field_handlers != NULL ) {
      SG_safe_delete( http->upload_field_handlers );
   }
   
   return 0;
}

// parse a uint64_t 
// return 0 on success, and set *out
// return -EINVAL if it couldn't be parsed
int md_parse_uint64( char* id_str, char const* fmt, uint64_t* out ) {
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

// parse a manifest timestamp 
// return 0 on success, and set *manifest_timestamp
// return -EINVAL if we fail
int md_parse_manifest_timestamp( char* _manifest_str, struct timespec* manifest_timestamp ) {
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

// parse a string in the form of $BLOCK_ID.$BLOCK_VERSION 
// return 0 on success, and set *_block_id and *_block_version 
// return -EINVAL on failure
int md_parse_block_id_and_version( char* _block_id_version_str, uint64_t* _block_id, int64_t* _block_version ) {
   uint64_t block_id = SG_INVALID_BLOCK_ID;
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
// return 0 on success, and set *_file_id and *_file_version 
// return -EINVAL on failure
int md_parse_file_id_and_version( char* _name_id_and_version_str, uint64_t* _file_id, int64_t* _file_version ) {
   
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
   
   uint64_t file_id = SG_INVALID_FILE_ID;
   int64_t file_version = -1;
   
   int num_read = sscanf( ptr, ".%" PRIX64 ".%" PRId64, &file_id, &file_version );
   if( num_read != 2 ) {
      return -EINVAL;
   }
   
   *_file_id = file_id;
   *_file_version = file_version;
   
   return 0;
}


// get the HTTP server's closure 
void* md_HTTP_cls( struct md_HTTP* http ) {
   return http->server_cls;
}
