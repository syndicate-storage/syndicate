/*
   Copyright 2014 The Trustees of Princeton University

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

#ifndef _AG_HTTP_H_
#define _AG_HTTP_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/url.h"

#include "AG.h"

#define AG_REQUEST_BLOCK        1
#define AG_REQUEST_MANIFEST     2

// prototypes 
struct AG_driver;
struct AG_map_info;
struct AG_driver_publish_info;

// connection context to an AG
struct AG_connection_context {
   char const* hostname;
   char const* method;
   
   int request_type;            // AG_REQUEST_BLOCK or AG_REQUEST_MANIFEST
   struct md_gateway_request_data reqdat;
   
   size_t size;         // this is the expected length of the data to be fetched
   char** args;
   int err;
   int http_status;
   
   struct AG_driver* driver;            // driver used to handle this connection
   char* query_string;                  // query string associated with the requested path
   void* driver_connection_state;      // driver-supplied connection state
};


// connection data
struct AG_connection_data {
   md_response_buffer_t* rb;

   int err;                               // error code
   struct AG_map_info* mi;                // looked-up map info
   
   struct AG_connection_context ctx;      // AG connection context
   void* user_cls;                        // driver-supplied
   
   struct AG_driver_publish_info* pubinfo;       // if this is a manifest request, this is filled in with the results of stat_dataset()
};

int AG_http_init( struct md_HTTP* http, struct md_syndicate_conf* conf );

#define AG_IS_MANIFEST_REQUEST( agctx ) ((agctx).reqdat.manifest_timestamp.tv_sec > 0)
#define AG_IS_BLOCK_REQUEST( agctx ) (!AG_IS_MANIFEST_REQUEST(agctx))

#endif
