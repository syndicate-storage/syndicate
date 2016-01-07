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

#ifndef _LIBSYNDICATE_SERVER_H_
#define _LIBSYNDICATE_SERVER_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"
#include "libsyndicate/gateway.h"

#define SG_SERVER_POST_FIELD_CONTROL_PLANE      "control-plane"
#define SG_SERVER_POST_FIELD_DATA_PLANE         "data-plane"

#define SG_SERVER_IO_READ                       1       // I/O completion will take a name and return a record 
#define SG_SERVER_IO_WRITE                      2       // I/O completion will take a record and return a status code

// server connection state
struct SG_server_connection {
   
   struct SG_gateway* gateway;
};

typedef int (*SG_server_IO_completion)( struct SG_gateway*, struct SG_request_data*, SG_messages::Request*, struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );

// server I/O request completion context 
struct SG_server_io {
   
   int io_type;                                 // "read" or "write" I/O
   struct SG_gateway* gateway;
   struct SG_request_data* reqdat;
   SG_messages::Request* request_msg;
   struct md_HTTP_connection_data* con_data;
   struct md_HTTP_response* resp;
   
   SG_server_IO_completion io_completion;
};

extern "C" {

// Syndicate Gateway HTTP server definition 
int SG_server_HTTP_connect( struct md_HTTP_connection_data* con_data, void** cls );
int SG_server_HTTP_HEAD_handler( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );
int SG_server_HTTP_GET_handler( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );
int SG_server_HTTP_POST_finish( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );
void SG_server_HTTP_cleanup( void *con_cls );

// populate an HTTP server with our Syndicate Gateway methods 
int SG_server_HTTP_install_handlers( struct md_HTTP* http );

// server I/O workqueue interfacing
int SG_server_HTTP_IO_start( struct SG_gateway* gateway, int type, SG_server_IO_completion io_cb, struct SG_request_data* reqdat, SG_messages::Request* request_msg,
                             struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp );
int SG_server_HTTP_IO_finish( struct md_wreq* wreq, void* cls );

}

#endif 