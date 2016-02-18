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

// HTTP daemon code

#ifndef _LIBSYNDICATE_HTTPD_H_
#define _LIBSYNDICATE_HTTPD_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util.h"


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <vector>
#include <map>
#include <dirent.h>
#include <utime.h>
#include <fstream>
#include <string>
#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <attr/xattr.h>
#include <pthread.h>
#include <pwd.h>
#include <math.h>
#include <locale>
#include <signal.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <inttypes.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#include <endian.h>
#include <sys/resource.h>

using namespace std;

#include "libsyndicate/libsyndicate.h"
#include "microhttpd.h"

// Support old versions of microhttpd (< 0.9.43)
#if MHD_VERSION < 0x00094300
#define MHD_create_response_from_fd_at_offset64         MHD_create_response_from_fd_at_offset
#endif

#define SG_HTTP_TMPFILE_FORMAT          "/tmp/.syndicate-upload.XXXXXX"

// HTTP headers
struct md_HTTP_header {
   char* header;
   char* value;
};


// ssize_t (*)(void* cls, uint64_t pos, char* buf, size_t max)
typedef MHD_ContentReaderCallback md_HTTP_stream_callback;

// void (*)(void* cls)
typedef MHD_ContentReaderFreeCallback md_HTTP_free_cls_callback;

// HTTP stream response
struct md_HTTP_stream_response {
   md_HTTP_stream_callback scb;
   md_HTTP_free_cls_callback fcb;
   
   void* cls;
   size_t blk_size;
   uint64_t size;
};
   

// HTTP response (to be populated by handlers)
struct md_HTTP_response {
   int status;
   struct MHD_Response* resp;
};

struct md_HTTP;

// HTTP post field data 
struct SG_HTTP_post_field {
   md_response_buffer_t* rb;    // for RAM upload 
   
   int tmpfd;                   // temporary file 
   char* tmpfd_path;

   uint64_t num_written;        // number of bytes accepted
   uint64_t max_size;           // maximum allowed size 
};

typedef map<string, struct SG_HTTP_post_field> SG_HTTP_post_field_map_t;

// HTTP connection data
struct md_HTTP_connection_data {
   
   struct md_HTTP* http;                // HTTP server reference
   struct MHD_PostProcessor* pp;        // HTTP post processor 
   struct md_HTTP_header** headers;     // headers from the request 
   char* remote_host;                   // remote peer (host:port string)
   uint64_t content_length;             // length of the content returned
   
   int status;                // HTTP status
   int mode;                  // numerical alias for the operation
   off_t offset;              // if there isn't a post-processor, then this stores the offset 
   
   void* cls;                 // user-supplied closure
   int version;               // HTTP version
   char* url_path;            // path requested
   char* query_string;        // url's query string (points to data alloc'ed to url_path)
   
   SG_HTTP_post_field_map_t* post_fields;       // upload data keyed by field
   
   struct MHD_Connection* connection;   // reference to HTTP connection 
   volatile bool suspended;             // if true, then this connection is suspended
   struct md_HTTP_response* resume_resp;        // if set, this is the response to send back immediately following a connection resume
};

typedef int (*SG_HTTP_method_t)( struct md_HTTP_connection_data*, struct md_HTTP_response* );
typedef int (*SG_HTTP_post_field_handler_t)( char const*, char const*, char const*, off_t, size_t, void* );

typedef map<string, SG_HTTP_post_field_handler_t> SG_HTTP_post_field_handler_map_t;

// HTTP callbacks and control code
struct md_HTTP {
   
   struct MHD_Daemon* http_daemon;
   int server_type;   // one of the MHD options
   
   bool running;

   // connection setup/cleanup
   int  (*HTTP_connect)( struct md_HTTP_connection_data* md_con_data, void** cls );
   void (*HTTP_cleanup)( void* cls );
   
   // associated server-wide state to make available to connections
   void* server_cls;
   
   // method handlers
   SG_HTTP_method_t HTTP_HEAD_handler;
   SG_HTTP_method_t HTTP_GET_handler;
   SG_HTTP_method_t HTTP_POST_finish;
   SG_HTTP_method_t HTTP_PUT_finish;
   SG_HTTP_method_t HTTP_DELETE_handler;
   
   // upload field handlers
   SG_HTTP_post_field_handler_map_t* upload_field_handlers;

   // maximum RAM upload size per connection
   uint64_t max_ram_upload_size;

   // maximum disk upload size per connection 
   uint64_t max_disk_upload_size;
};

extern char const MD_HTTP_NOMSG[128];
extern char const MD_HTTP_200_MSG[128];
extern char const MD_HTTP_302_MSG[128];
extern char const MD_HTTP_400_MSG[128];
extern char const MD_HTTP_401_MSG[128];
extern char const MD_HTTP_403_MSG[128];
extern char const MD_HTTP_404_MSG[128];
extern char const MD_HTTP_409_MSG[128];
extern char const MD_HTTP_413_MSG[128];
extern char const MD_HTTP_422_MSG[128];
extern char const MD_HTTP_500_MSG[128];
extern char const MD_HTTP_501_MSG[128];
extern char const MD_HTTP_502_MSG[128];
extern char const MD_HTTP_503_MSG[128];
extern char const MD_HTTP_504_MSG[128];

extern char const MD_HTTP_DEFAULT_MSG[128];

#define SG_HTTP_TRYAGAIN            503
#define SG_HTTP_TRYAGAIN_MSG        MD_HTTP_503_MSG

#define MD_HTTP_TYPE_STATEMACHINE   MHD_USE_SELECT_INTERNALLY
#define MD_HTTP_TYPE_THREAD         MHD_USE_THREAD_PER_CONNECTION

#define MD_MIN_POST_DATA 4096

#define md_HTTP_connect( http, callback ) (http).HTTP_connect = (callback)
#define md_HTTP_GET( http, callback ) (http).HTTP_GET_handler = (callback)
#define md_HTTP_HEAD( http, callback ) (http).HTTP_HEAD_handler = (callback)
#define md_HTTP_upload_iterator( http, callback ) (http).HTTP_upload_iterator = (callback)
#define md_HTTP_POST_finish( http, callback ) (http).HTTP_POST_finish = (callback)
#define md_HTTP_PUT_finish( http, callback ) (http).HTTP_PUT_finish = (callback)
#define md_HTTP_DELETE( http, callback ) (http).HTTP_DELETE_handler = (callback)
#define md_HTTP_close( http, callback ) (http).HTTP_cleanup = (callback)
#define md_HTTP_server_type( http, type ) (http).server_type = type
#define md_HTTP_post_field_handler( http, field, callback ) (*(http).upload_field_handlers)[ string(field) ] = (callback)

// use STRONG encryption, with perfect forward security
// Use ephemeral Diffie-Hellman for key exchange (RSA or ECC)
// Use at least 256-bit MACs
// Use at least 256-bit keys for data encryption.
#define SYNDICATE_GNUTLS_CIPHER_SUITES "PFS:-ARCFOUR-128:-3DES-CBC:-AES-128-CBC:-AES-256-CBC:-CAMELLIA-128-CBC:-MD5:-SHA1"

// types of responses
#define HTTP_RESPONSE_RAM              1
#define HTTP_RESPONSE_RAM_NOCOPY       2
#define HTTP_RESPONSE_RAM_STATIC       3
#define HTTP_RESPONSE_FILE             4
#define HTTP_RESPONSE_FD               5
#define HTTP_RESPONSE_CALLBACK         6

// mode for connection data
#define MD_HTTP_GET      0
#define MD_HTTP_POST     1
#define MD_HTTP_PUT      2
#define MD_HTTP_HEAD     3
#define MD_HTTP_DELETE   4

#define MD_HTTP_UNKNOWN  -1

extern "C" {

// POST field handlers 
int md_HTTP_post_field_handler_ram( char const* field_name, char const* filename, char const* data, off_t offset, size_t len, void* cls );
int md_HTTP_post_field_handler_disk( char const* field_name, char const* filename, char const* data, off_t offset, size_t len, void* cls );

// HTTP server responses
int md_HTTP_create_response_ram( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_HTTP_create_response_ram_nocopy( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_HTTP_create_response_ram_static( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_HTTP_create_response_fd( struct md_HTTP_response* resp, char const* mimetype, int status, int fd, off_t offset, size_t size );
int md_HTTP_create_response_stream( struct md_HTTP_response* resp, char const* mimetype, int status, uint64_t size, size_t blk_size, md_HTTP_stream_callback scb, void* cls, md_HTTP_free_cls_callback fcb );
int md_HTTP_create_response_builtin( struct md_HTTP_response* resp, int status );
void md_HTTP_response_free( struct md_HTTP_response* resp );

// suspend/resume connection 
int md_HTTP_connection_suspend( struct md_HTTP_connection_data* con_data );
int md_HTTP_connection_resume( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );

// init/start/stop/shutdown
int md_HTTP_init( struct md_HTTP* http, int server_type, void* server_cls );
int md_HTTP_set_limits( struct md_HTTP* http, uint64_t max_ram_upload_size, uint64_t max_disk_upload_size );
int md_HTTP_start( struct md_HTTP* http, int portnum );
int md_HTTP_stop( struct md_HTTP* http );
int md_HTTP_free( struct md_HTTP* http );

// headers
int md_HTTP_header_create( struct md_HTTP_header* header, char const* h, char const* value );
void md_HTTP_header_free( struct md_HTTP_header* header );
char const* md_HTTP_header_lookup( struct md_HTTP_header** headers, char const* header );
int md_HTTP_header_add( struct md_HTTP_response* resp, char const* header, char const* value );

// path parsing 
int md_parse_uint64( char* id_str, char const* fmt, uint64_t* out );
int md_parse_manifest_timestamp( char* _manifest_str, struct timespec* manifest_timestamp );
int md_parse_block_id_and_version( char* _block_id_version_str, uint64_t* _block_id, int64_t* _block_version );
int md_parse_file_id_and_version( char* _name_id_and_version_str, uint64_t* _file_id, int64_t* _file_version );

// memory management
void md_HTTP_free_connection_data( struct md_HTTP_connection_data* con_data );

// get uploaded data 
int md_HTTP_upload_get_field_buffer( struct md_HTTP_connection_data* con_data, char const* field_name, char** buf, size_t* buflen );
int md_HTTP_upload_get_field_tmpfile( struct md_HTTP_connection_data* con_data, char const* field_name, char** path, int* fd );

// getters 
void* md_HTTP_cls( struct md_HTTP* http );

}

#endif
