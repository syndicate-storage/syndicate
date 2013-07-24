/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _LIBGATEWAY_H_
#define _LIBGATEWAY_H_

#include "libsyndicate.h"
#include <getopt.h>
#include <ftw.h>
#include <dlfcn.h>

extern struct md_syndicate_conf *global_conf;

struct gateway_context {
   char const* url_path;
   char const* hostname;
   char const* username;
   char const* method;
   size_t size;         // for PUT, this is the length of the uploaded data.  for GET, this is the expected length of the data to be fetched
   time_t last_mod;     // for GET, this is the last-mod time of the file to be served
   char** args;
   int err;
   int http_status;
};


// connection data
struct gateway_connection_data {
   response_buffer_t* rb;
   struct md_user_entry *user;
   ms::ms_gateway_blockinfo *block_info;

   int err;                               // error code
   bool has_gateway_md;                   // do we have the gateway information?

   struct gateway_context ctx;            // gateway constext
   void* user_cls;                        // driver-supplied
};


BEGIN_EXTERN_C

int RG_main( int argc, char** argv );
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
void gateway_metadata_func( int (*metadata_func)(struct gateway_context*, ms::ms_gateway_blockinfo* info, void* usercls) );
void gateway_publish_func( int (*publish_func)(struct gateway_context*, struct ms_client*, char* dataset ) );

int gateway_key_value( char* arg, char* key, char* value );

int load_AG_driver( char *lib );
int unload_AG_driver( );

END_EXTERN_C

#define GATEWAY_DEFAULT_CONFIG "/etc/syndicate/syndicate-gateway-server.conf"

#endif

