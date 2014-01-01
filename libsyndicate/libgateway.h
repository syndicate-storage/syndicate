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

#ifndef _LIBGATEWAY_H_
#define _LIBGATEWAY_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/ms-client.h"
#include "libsyndicate/system.h"

#include <getopt.h>
#include <ftw.h>
#include <dlfcn.h>

#define RMAP_CTRL_FLAG	    0x01
#define STOP_CTRL_FLAG	    0x02

#define AG_DEFAULT_BLOCK_SIZE  61440

#define REQUEST_IS_MANIFEST( reqdat ) ((reqdat).manifest_timestamp.tv_sec > 0)

extern struct md_syndicate_conf *global_conf;

struct gateway_context {
   char const* hostname;
   char const* username;
   char const* method;
   struct md_gateway_request_data reqdat;
   
   size_t size;         // for PUT, this is the length of the uploaded data.  for GET, this is the expected length of the data to be fetched
   time_t last_mod;     // for GET, this is the last-mod time of the file to be served
   char** args;
   int err;
   int http_status;
   ms::ms_gateway_request_info *block_info;
};


// connection data
struct gateway_connection_data {
   response_buffer_t* rb;

   int err;                               // error code
   bool has_gateway_md;                   // do we have the gateway information?

   struct gateway_context ctx;            // gateway constext
   void* user_cls;                        // driver-supplied
};


BEGIN_EXTERN_C

int AG_main( int argc, char** argv );
int gateway_main( int gateway_type, int argc, char** argv );
int start_gateway_service( struct md_syndicate_conf *conf, struct ms_client *client, char* logfile, char* pidfile, bool make_daemon );

// NOTE: on GET, the passed method should set the size field in the given gateway_context structure.
// NOTE: the passed method should return NULL on error, and non-NULL on success
void gateway_connect_func( void* (*connect_func)( struct gateway_context* ) );
void gateway_put_func( ssize_t (*put_func)(struct gateway_context*, char const* data, size_t len, void* usercls) );
void gateway_get_func( ssize_t (*get_func)(struct gateway_context*, char* buf, size_t len, void* usercls) );
void gateway_delete_func( int (*delete_func)(struct gateway_context*, void* usercls) );
void gateway_cleanup_func( void (*cleanup_func)(void* usercls) );
void gateway_metadata_func( int (*metadata_func)(struct gateway_context*, ms::ms_gateway_request_info* info, void* usercls) );
void gateway_publish_func( int (*publish_func)(struct gateway_context*, struct ms_client*, char* dataset ) );
void gateway_controller_func( int (*controller_func)(pid_t pid, int ctrl_flag));

int gateway_key_value( char* arg, char* key, char* value );

int gateway_sign_manifest( EVP_PKEY* pkey, Serialization::ManifestMsg* mmsg );
int gateway_verify_manifest( EVP_PKEY* pkey, Serialization::ManifestMsg* mmsg );

int gateway_sign_blockinfo( EVP_PKEY* pkey, ms::ms_gateway_request_info* blkinfo );

int load_AG_driver( char *lib );

END_EXTERN_C

#define GATEWAY_DEFAULT_CONFIG "/etc/syndicate/syndicate-gateway-server.conf"

#endif

