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


// HTTP handler args
struct md_HTTP_handler_args {
   struct md_syndicate_conf* conf;
   struct md_user_entry** users;
};


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

// HTTP connection data
struct md_HTTP_connection_data {
   struct md_HTTP* http;
   struct md_syndicate_conf* conf;
   struct ms_client* ms;
   struct MHD_PostProcessor* pp;
   struct md_HTTP_response* resp;
   struct md_HTTP_header** headers;
   char* remote_host;
   char const* method;
   size_t content_length;
   
   int status;
   int mode;
   off_t offset;              // if there isn't a post-processor, then this stores the offset 
   
   void* cls;                 // user-supplied closure
   char* version;             // HTTP version
   char* url_path;            // path requested
   char* query_string;        // url's query string
   response_buffer_t* rb;     // response buffer for small messages
};

// gateway request structure
struct md_gateway_request_data {
   uint64_t volume_id;
   uint64_t file_id;
   char* fs_path;
   int64_t file_version;
   uint64_t block_id;
   int64_t block_version;
   struct timespec manifest_timestamp;
};

// HTTP callbacks and control code
struct md_HTTP {

   pthread_rwlock_t lock;
   
   struct MHD_Daemon* http_daemon;
   int server_type;   // one of the MHD options

   char* server_cert;
   char* server_pkey;
   
   void*                     (*HTTP_connect)( struct md_HTTP_connection_data* md_con_data );
   uint64_t                  (*HTTP_authenticate)( struct md_HTTP_connection_data* md_con_data, char* username, char* password );
   struct md_HTTP_response*  (*HTTP_HEAD_handler)( struct md_HTTP_connection_data* md_con_data );
   struct md_HTTP_response*  (*HTTP_GET_handler)( struct md_HTTP_connection_data* md_con_data );
   int                       (*HTTP_POST_iterator)(void *coninfo_cls, enum MHD_ValueKind kind, 
                                                   char const *key,
                                                   char const *filename, char const *content_type,
                                                   char const *transfer_encoding, char const *data, 
                                                   uint64_t off, size_t size);
   void                      (*HTTP_POST_finish)( struct md_HTTP_connection_data* md_con_data );
   int                       (*HTTP_PUT_iterator)(void *coninfo_cls, enum MHD_ValueKind kind,
                                                  char const *key,
                                                  char const *filename, char const *content_type,
                                                  char const *transfer_encoding, char const *data,
                                                  uint64_t off, size_t size);
   void                      (*HTTP_PUT_finish)( struct md_HTTP_connection_data* md_con_data );
   struct md_HTTP_response*  (*HTTP_DELETE_handler)( struct md_HTTP_connection_data* md_con_data, int depth );
   void                      (*HTTP_cleanup)(struct MHD_Connection *connection, void *con_cls, enum MHD_RequestTerminationCode term);
};

extern char const MD_HTTP_NOMSG[128];
extern char const MD_HTTP_200_MSG[128];
extern char const MD_HTTP_400_MSG[128];
extern char const MD_HTTP_401_MSG[128];
extern char const MD_HTTP_403_MSG[128];
extern char const MD_HTTP_404_MSG[128];
extern char const MD_HTTP_409_MSG[128];
extern char const MD_HTTP_413_MSG[128];
extern char const MD_HTTP_422_MSG[128];
extern char const MD_HTTP_500_MSG[128];
extern char const MD_HTTP_501_MSG[128];
extern char const MD_HTTP_504_MSG[128];

extern char const MD_HTTP_DEFAULT_MSG[128];

#define MD_HTTP_TYPE_STATEMACHINE   MHD_USE_SELECT_INTERNALLY
#define MD_HTTP_TYPE_THREAD         MHD_USE_THREAD_PER_CONNECTION

#define MD_MIN_POST_DATA 4096

#define md_HTTP_connect( http, callback ) (http).HTTP_connect = (callback)
#define md_HTTP_authenticate( http, callback ) (http.HTTP_authenticate) = (callback)
#define md_HTTP_GET( http, callback ) (http).HTTP_GET_handler = (callback)
#define md_HTTP_HEAD( http, callback ) (http).HTTP_HEAD_handler = (callback)
#define md_HTTP_POST_iterator( http, callback ) (http).HTTP_POST_iterator = (callback)
#define md_HTTP_POST_finish( http, callback ) (http).HTTP_POST_finish = (callback)
#define md_HTTP_PUT_iterator( http, callback ) (http).HTTP_PUT_iterator = (callback)
#define md_HTTP_PUT_finish( http, callback ) (http).HTTP_PUT_finish = (callback)
#define md_HTTP_DELETE( http, callback ) (http).HTTP_DELETE_handler = (callback)
#define md_HTTP_close( http, callback ) (http).HTTP_cleanup = (callback)
#define md_HTTP_server_type( http, type ) (http).server_type = type

// use STRONG encryption, with perfect forward security
// Use ephemeral Diffie-Hellman for key exchange (RSA or ECC)
// Use at least 256-bit MACs
// Use at least 256-bit keys for data encryption.
#define SYNDICATE_GNUTLS_CIPHER_SUITES "PFS:-ARCFOUR-128:-3DES-CBC:-AES-128-CBC:-AES-256-CBC:-CAMELLIA-128-CBC:-MD5:-SHA1"

// authentication (can be OR'ed together)
#define HTTP_AUTHENTICATE_NONE        0
#define HTTP_AUTHENTICATE_READ        1
#define HTTP_AUTHENTICATE_WRITE       2
#define HTTP_AUTHENTICATE_READWRITE   3

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

// basic helpers
int md_HTTP_rlock( struct md_HTTP* http );
int md_HTTP_wlock( struct md_HTTP* http );
int md_HTTP_unlock( struct md_HTTP* http );

// default callbacks and helpers
int md_response_buffer_upload_iterator(void *coninfo_cls, enum MHD_ValueKind kind,
                                       const char *key,
                                       const char *filename, const char *content_type,
                                       const char *transfer_encoding, const char *data,
                                       uint64_t off, size_t size);

// HTTP server responses
int md_create_HTTP_response_ram( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_create_HTTP_response_ram_nocopy( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_create_HTTP_response_ram_static( struct md_HTTP_response* resp, char const* mimetype, int status, char const* data, int len );
int md_create_HTTP_response_fd( struct md_HTTP_response* resp, char const* mimetype, int status, int fd, off_t offset, size_t size );
int md_create_HTTP_response_stream( struct md_HTTP_response* resp, char const* mimetype, int status, uint64_t size, size_t blk_size, md_HTTP_stream_callback scb, void* cls, md_HTTP_free_cls_callback fcb );
void md_free_HTTP_response( struct md_HTTP_response* resp );

// get/set closure
void* md_cls_get( void* cls );
void md_cls_set_status( void* cls, int status );
struct md_HTTP_response* md_cls_set_response( void* cls, struct md_HTTP_response* resp );

// init/start/stop/shutdown
int md_HTTP_init( struct md_HTTP* http, int server_type );
int md_start_HTTP( struct md_HTTP* http, int portnum, struct md_syndicate_conf* conf );
int md_stop_HTTP( struct md_HTTP* http );
int md_free_HTTP( struct md_HTTP* http );

// headers
void md_create_HTTP_header( struct md_HTTP_header* header, char const* h, char const* value );
void md_free_HTTP_header( struct md_HTTP_header* header );
char const* md_find_HTTP_header( struct md_HTTP_header** headers, char const* header );
int md_HTTP_add_header( struct md_HTTP_response* resp, char const* header, char const* value );

// path parsing 
int md_HTTP_parse_url_path( char const* _url_path, uint64_t* _volume_id, char** _file_path, uint64_t* _file_id, int64_t* _file_version, uint64_t* _block_id, int64_t* _block_version, struct timespec* _manifest_ts );

// memory management
void md_HTTP_free_connection_data( struct md_HTTP_connection_data* con_data );
void md_gateway_request_data_free( struct md_gateway_request_data* reqdat );

#endif