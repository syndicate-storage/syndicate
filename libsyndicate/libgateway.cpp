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


#include "libgateway.h"

static bool gateway_running = true;
static bool allow_overwrite = false;

// gloabl config
struct md_syndicate_conf *global_conf = NULL;

// global ms
struct ms_client *global_ms = NULL;

// gateway driver
static void* driver = NULL;

// callbacks to be filled in by an RG implementation
static ssize_t (*put_callback)( struct gateway_context*, char const* data, size_t len, void* usercls ) = NULL;
static ssize_t (*get_callback)( struct gateway_context*, char* data, size_t len, void* usercls ) = NULL;
static int (*delete_callback)( struct gateway_context*, void* usercls ) = NULL;
static void* (*connect_callback)( struct gateway_context* ) = NULL;
static void (*cleanup_callback)( void* usercls ) = NULL;
static int (*metadata_callback)( struct gateway_context*, ms::ms_gateway_request_info*, void* ) = NULL;
static int (*publish_callback)( struct gateway_context*, ms_client*, char* ) = NULL;
int (*controller_callback)(pid_t pid, int ctrl_flag);

// set the PUT callback
void gateway_put_func( ssize_t (*put_func)(struct gateway_context*, char const* data, size_t len, void* usercls) ) {
   put_callback = put_func;
}

// set the GET callback
void gateway_get_func( ssize_t (*get_func)(struct gateway_context*, char* buf, size_t len, void* usercls) ) {
   get_callback = get_func;
}

// set the CGI argument parser callback
void gateway_connect_func( void* (*connect_func)( struct gateway_context* ) ) {
   connect_callback = connect_func;
}

// set the delete func
void gateway_delete_func( int (*delete_func)(struct gateway_context*, void* usercls) ) {
   delete_callback = delete_func;
}

// set up the cleanup func
void gateway_cleanup_func( void (*cleanup_func)(void* usercls) ) {
   cleanup_callback = cleanup_func;
}

// set the metadata get callback
void gateway_metadata_func( int (*metadata_func)(struct gateway_context* ctx, ms::ms_gateway_request_info*, void*) ) {
   metadata_callback = metadata_func;
}


// set the publish callback
void gateway_publish_func( int (*publish_func)(struct gateway_context*, ms_client*, char*) ){
   publish_callback = publish_func;
}

// set controller callback
void gateway_controller_func( int (*controller_func)(pid_t pid, int ctrl_flag)) {
    controller_callback = controller_func;
}

// free a gateway_request_data
void gateway_request_data_free( struct gateway_request_data* reqdat ) {
   if( reqdat->fs_path ) {
      free( reqdat->fs_path );
   }
   memset( reqdat, 0, sizeof(struct gateway_request_data) );
}

// get the HTTP status
int get_http_status( struct gateway_context* ctx, int default_value ) {
   int ret = default_value;
   if( ctx->http_status != 0 ) {
      ret = ctx->http_status;
   }
   return ret;
}

// separate a CGI argument into its key and value
int gateway_key_value( char* arg, char* key, char* value ) {
   int eq_off = 0;
   int i;
   for( i = 0; i < (int)strlen(arg); i++ ) {
      if( arg[i] == '=' )
         break;
   }
   if( i == (int)strlen(arg) )
      return -1;
   
   eq_off = i;
   
   if( key )
      strncpy( key, arg, eq_off );
   if( value )
      strncpy( value, arg + eq_off + 1, strlen(arg) - eq_off - 1 );
   return eq_off;
}

static char const* CONNECT_ERROR = "CONNECT ERROR";

// connection handler
static void* gateway_HTTP_connect( struct md_HTTP_connection_data* md_con_data ) {
   
   struct gateway_request_data reqdat;
   memset( &reqdat, 0, sizeof(reqdat) );
   
   int rc = md_HTTP_parse_url_path( md_con_data->url_path, &reqdat.volume_id, &reqdat.fs_path, &reqdat.file_version, &reqdat.block_id, &reqdat.block_version, &reqdat.manifest_timestamp, &reqdat.staging );
   if( rc != 0 ) {
      errorf( "failed to parse '%s', rc = %d\n", md_con_data->url_path, rc );
      
      return NULL;
   }
   
   if( reqdat.staging ) {
      errorf("Invalid request: '%s' cannot be staging\n", md_con_data->url_path );
      gateway_request_data_free( &reqdat );
      return NULL;
   }
   
   
   struct gateway_connection_data* con_data = CALLOC_LIST( struct gateway_connection_data, 1 );
   
   con_data->user = md_con_data->user;
   con_data->rb = new response_buffer_t();
   con_data->has_gateway_md = false;
   con_data->err = 0;

   if( md_con_data->query_string ) {
      char* args_str = strdup( md_con_data->query_string );
      con_data->ctx.args = md_parse_cgi_args( args_str );
   }
   
   if( md_con_data->user != NULL ) {
      con_data->ctx.username = md_con_data->user->username;
   }
   
   memcpy( &con_data->ctx.reqdat, &reqdat, sizeof(reqdat) );
   con_data->ctx.block_info = new ms::ms_gateway_request_info();
   con_data->ctx.hostname = md_con_data->remote_host;
   con_data->ctx.method = md_con_data->method;
   con_data->ctx.size = global_conf->ag_block_size;
   con_data->ctx.err = 0;
   con_data->ctx.http_status = 0;
   
   md_con_data->status = 200;
   
   if( connect_callback ) {

      void* cls = (*connect_callback)( &con_data->ctx );
      if( cls == NULL ) {
         md_con_data->status = -abs( get_http_status( &con_data->ctx, 500 ) );
      }

      con_data->user_cls = cls;
   }

   if( md_con_data->status == 200 ) {
      // sanity checks
      if( strcmp( con_data->ctx.method, "POST" ) == 0 && put_callback == NULL ) {
         md_con_data->status = -501;
      }

      if( strcmp( con_data->ctx.method, "GET" ) == 0 && get_callback == NULL ) {
         md_con_data->status = -501;
      }

      if( strcmp( con_data->ctx.method, "DELETE" ) == 0 && delete_callback == NULL ) {
         md_con_data->status = -501;
      }
   }

   if( md_con_data->status != 200 ) {
      md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );
      md_create_HTTP_response_ram_static( md_con_data->resp, "text/plain", abs(md_con_data->status), CONNECT_ERROR, strlen(CONNECT_ERROR) + 1 );
   }
   
   return (void*)con_data;
}


// POST metadata handler
// NOTE: we need to ensure that the "metadata" section comes before the "data" section
static int gateway_POST_iterator(void *coninfo_cls, enum MHD_ValueKind kind,
                                 const char *key,
                                 const char *filename, const char *content_type,
                                 const char *transfer_encoding, const char *data,
                                 uint64_t off, size_t size) {


   struct md_HTTP_connection_data* md_con_data = (struct md_HTTP_connection_data*)coninfo_cls;
   struct gateway_connection_data *con_data = (struct gateway_connection_data*)md_con_data->cls;

   if( md_con_data->status < -1 ) {
      return MHD_NO;
   }

   md_con_data->status = 200;

   dbprintf( "[%p] got data for '%s'\n", con_data, key );

   if( strcmp( key, "metadata" ) == 0 ) {
      if( !con_data->has_gateway_md ) {
         // haven't started getting data yet
         char* data_dup = CALLOC_LIST( char, size );
         memcpy( data_dup, data, size );
         con_data->rb->push_back( buffer_segment_t( data_dup, size ) );
      }
      else {
         // driver in progress--no more metadata allowed
         errorf("%s", " cannot accept metadata now\n");
         md_con_data->status = -400;
         return MHD_NO;
      }
   }
   else if( strcmp( key, "data" ) == 0 ) {
      // this is file block data
      if( size > 0 ) {

         dbprintf( "[%p] data; size = %zu, off = %" PRIu64 "\n", con_data, size, off );

         if( !con_data->has_gateway_md ) {
            // attempt to parse our metadata before doing anything with it
            char* buf = response_buffer_to_string( con_data->rb );
            size_t buf_len = response_buffer_size( con_data->rb );

            bool parse_rc = con_data->ctx.block_info->ParseFromString( string(buf, buf_len) );
            free( buf );

            if( !parse_rc ) {
               // failed to parse
               errorf("%s", "failed to parse metadata\n");
               md_con_data->status = -400;
               con_data->err = -EINVAL;
               return MHD_NO;
            }
            else {
               // got data that was useful!
               con_data->has_gateway_md = true;
            }
         }

         if( con_data->has_gateway_md && con_data->err == 0 && md_con_data->status == 200 ) {
            // feed the data to the callback
            if( put_callback ) {
               ssize_t num_put = (*put_callback)( &con_data->ctx, data, size, con_data->user_cls );
               if( num_put != (signed)size ) {
                  errorf( "user PUT returned %zd\n", num_put );

                  md_con_data->status = -abs( get_http_status( &con_data->ctx, 500 ) );
                  con_data->err = num_put;
                  return MHD_NO;
               }
            }
            else {
               con_data->err = -ENOSYS;
               md_con_data->status = -501;
               return MHD_NO;
            }
         }
      }
   }
   else {
      errorf( "unknown field '%s'\n", key );
      md_con_data->status = -400;
      return MHD_NO;
   }

   return MHD_YES;
}

// finish posting
static void gateway_POST_finish( struct md_HTTP_connection_data* md_con_data ) {
   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   if( md_con_data->status < -1 || rpc->err != 0 ) {
      // problem
      char buf[15];
      sprintf(buf, "%d", rpc->err );
      md_create_HTTP_response_ram( md_con_data->resp, "text/plain", abs(md_con_data->status), buf, strlen(buf) + 1 );
   }
   else {
      // we're good
      md_create_HTTP_response_ram_static( md_con_data->resp, "text/plain", 200, MD_HTTP_200_MSG, strlen(MD_HTTP_200_MSG) + 1 );
   }
   md_HTTP_add_header( md_con_data->resp, "Connection", "keep-alive" );
   return;
}


// MHD callback for streaming data from the gateway server implementation
static ssize_t gateway_HTTP_read_callback( void* cls, uint64_t pos, char* buf, size_t max ) {
   struct gateway_connection_data* rpc = (struct gateway_connection_data*)cls;

   ssize_t ret = -1;
   if( get_callback ) {
      // TODO: encrypt this as we send it back
      ret = (*get_callback)( &rpc->ctx, buf, max, rpc->user_cls );
      if( ret == 0 ) {
         // NOTE: return 0 can indicate to try again!
         errorf("get_callback returned %zd\n", ret);
         ret = -1;
      }
   }
   return ret;
}


// make a last-modified header
static void add_last_mod_header( struct md_HTTP_response* resp ) {

   char hdr_buf[200];
   memset( hdr_buf, 0, 200 );

   struct timespec now;
   clock_gettime( CLOCK_REALTIME, &now );
   
   time_t now_sec = now.tv_sec;
   struct tm utc_time;
   
   gmtime_r( &now_sec, &utc_time );
   
   strftime( hdr_buf, 200, "%a, %d %b %Y %H:%M:%S GMT", &utc_time );

   md_HTTP_add_header( resp, "Last-Modified", hdr_buf );
}


// gateway GET handler
static char const* GATEWAY_GET_INVALID = "Invalid\n";

static struct md_HTTP_response* gateway_GET_handler( struct md_HTTP_connection_data* md_con_data ) {
   //struct md_HTTP* http = md_con_data->http;

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      // shouldn't happen
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }

   if( get_callback ) {
      int http_status = get_http_status( &rpc->ctx, 200 );
      md_create_HTTP_response_stream( resp, "application/octet-stream", http_status, rpc->ctx.size, global_conf->ag_block_size, gateway_HTTP_read_callback, rpc, NULL );
      add_last_mod_header( resp );
   }
   else {
      md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
   }
   
   md_HTTP_add_header( resp, "Connection", "keep-alive" );
   return resp;
}


// populate an ms_block_info structure with defaults
static int gateway_default_blockinfo( char const* url_path, struct gateway_connection_data* rpc, ms::ms_gateway_request_info* info ) {
   
   uint64_t volume_id = 0;
   int64_t file_version = 0;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   char* file_path = NULL;
   struct timespec manifest_timestamp;
   manifest_timestamp.tv_sec = 0;
   manifest_timestamp.tv_nsec = 0;
   bool staging = false;
   
   int rc = md_HTTP_parse_url_path( url_path, &volume_id, &file_path, &file_version, &block_id, &block_version, &manifest_timestamp, &staging );
   if( rc != 0 ) {
      errorf( "failed to parse '%s', rc = %d\n", url_path, rc );
      free( file_path );
      return -EINVAL;
   }
   
   // populate this structure, and then ask the driver to add its stuff on top of it
   info->set_size( global_conf->ag_block_size );
   
   info->set_volume( volume_id );
   info->set_file_id( (uint64_t)(-1) );
   info->set_file_version( file_version );
   info->set_block_id( block_id );
   info->set_block_version( block_version );
   info->set_file_mtime_sec( 0 );
   info->set_file_mtime_nsec( 0 );
   
   // no block hash or signature yet...
   info->set_hash( string("") );
   info->set_signature( string("") );
   
   // block ownership info
   info->set_owner( global_ms->owner_id );
   info->set_writer( global_ms->gateway_id );
   
   return 0;
}


// gateway HEAD handler
static struct md_HTTP_response* gateway_HEAD_handler( struct md_HTTP_connection_data* md_con_data ) {
   
   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }
   
   // do we have metadata for this?
   int rc = 0;
   ms::ms_gateway_request_info info;
   
   if( metadata_callback == NULL ) {
      md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
      rc = -501;
   }
   else {
      rc = gateway_default_blockinfo( md_con_data->url_path, rpc, &info );
      
      if( rc == 0 ) {   
         rc = (*metadata_callback)( &rpc->ctx, &info, rpc->user_cls );
      }
      
      if( rc != 0 ) {
         int http_status = get_http_status( &rpc->ctx, 404 );

         char const* msg = "Unable to read metadata";
         
         md_create_HTTP_response_ram_static( resp, "text/plain", http_status, msg, strlen(msg) + 1 );
      }
      else {
         // sign this...
         rc = gateway_sign_blockinfo( global_ms->my_key, &info );
         if( rc != 0 ) {
            md_create_HTTP_response_ram_static( resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
         }
         else {
            // serialize and return 
            string info_str;
            bool src = info.SerializeToString( &info_str );
            if( !src ) {
               errorf( "could not serialize metadata for %s\n", md_con_data->url_path );
               md_create_HTTP_response_ram_static( resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
            }
            else {
               int http_status = get_http_status( &rpc->ctx, 200 );
               md_create_HTTP_response_ram( resp, "text/plain", http_status, info_str.c_str(), info_str.size() );
               add_last_mod_header( resp );
            }
         }
      }
   }

   md_HTTP_add_header( md_con_data->resp, "Connection", "keep-alive" );
   return resp;
}


// DELETE handler
static struct md_HTTP_response* gateway_DELETE_handler( struct md_HTTP_connection_data* md_con_data, int depth ) {

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }

   if( delete_callback ) {
      // delete our record
      int rc = (*delete_callback)( &rpc->ctx, rpc->user_cls );
      if( rc != 0 ) {
         errorf( "DELETE callback rc = %d\n", rc );

         char buf[15];
         sprintf(buf, "%d", rc );
         int http_status = get_http_status( &rpc->ctx, 500 );
         
         md_create_HTTP_response_ram( resp, "text/plain", http_status, buf, strlen(buf) + 1 );
      }
   }
   else {
      md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
   }

   md_HTTP_add_header( md_con_data->resp, "Connection", "keep-alive" );
   return resp;
}


// clean up
static void gateway_cleanup( struct MHD_Connection *connection, void *user_cls, enum MHD_RequestTerminationCode term) {

   struct gateway_connection_data *con_data = (struct gateway_connection_data*)(user_cls);
   if( con_data == NULL )
      return;

   // free memory
   if( con_data->rb ) {
      response_buffer_free( con_data->rb );
      delete con_data->rb;
   }

   if( con_data->ctx.block_info ) {
      delete con_data->ctx.block_info;
   }

   if( con_data->ctx.args )
      free( con_data->ctx.args );

   if( cleanup_callback ) {
      (*cleanup_callback)( con_data->user_cls );
   }
   
   free( con_data );
}


// sign a manifest message
int gateway_sign_manifest( EVP_PKEY* pkey, Serialization::ManifestMsg* mmsg ) {
   return md_sign< Serialization::ManifestMsg >( pkey, mmsg );
}


// sign a block info message
int gateway_sign_blockinfo( EVP_PKEY* pkey, ms::ms_gateway_request_info* blkinfo ) {
   return md_sign< ms::ms_gateway_request_info >( pkey, blkinfo );
}

// verify a manifest received from a gateway
int gateway_verify_manifest( EVP_PKEY* pkey, Serialization::ManifestMsg* mmsg ) {
   return md_verify< Serialization::ManifestMsg >( pkey, mmsg );
}


// start up server
static int gateway_init( struct md_HTTP* http, struct md_syndicate_conf* conf, struct ms_client* ms ) {
   
   //md_path_locks_create( &gateway_md_locks );

   md_HTTP_init( http, MHD_USE_SELECT_INTERNALLY | MHD_USE_POLL | MHD_USE_DEBUG, conf, ms );
   md_HTTP_auth_mode( *http, conf->http_authentication_mode );
   md_HTTP_connect( *http, gateway_HTTP_connect );
   md_HTTP_GET( *http, gateway_GET_handler );
   md_HTTP_HEAD( *http, gateway_HEAD_handler );
   md_HTTP_POST_iterator( *http, gateway_POST_iterator );
   md_HTTP_POST_finish( *http, gateway_POST_finish );
   md_HTTP_DELETE( *http, gateway_DELETE_handler );
   md_HTTP_close( *http, gateway_cleanup );

   md_mkdirs( conf->gateway_metadata_root );

   md_connect_timeout( conf->metadata_connect_timeout );
   md_signals( 0 );        // no signals

   int rc = md_start_HTTP( http, conf->portnum );
   if( rc != 0 ) {
      errorf("ERR: rc = %d when starting HTTP thread\n", rc );
   }
   
   return rc;
}


// shut down the HTTP module
int gateway_shutdown( struct md_HTTP* http ) {
   md_stop_HTTP( http );

   return 0;
}

// signal handler for SIGINT, SIGTERM
static void die_handler(int param) {
   if( gateway_running ) {
      gateway_running = false;
   }
}

// set up signal handler
static void setup_signals() {
   signal( SIGTERM, die_handler );
   signal( SIGINT, die_handler );
}

static void gateway_usage( char* name, struct option* opts, int exitcode ) {
   fprintf(stderr, "Usage: %s [ARGS]\n", name );
   for( int i = 0; opts[i].name != NULL ; i++ ) {
      
      // extract the help text
      char const* help_text = opts[i].name + strlen(opts[i].name) + 1;

      if( opts[i].has_arg == required_argument )
         fprintf(stderr, "\t[-%c|--%s] ARG\t\t%s\n", opts[i].val, opts[i].name, help_text );
      else
         fprintf(stderr, "\t[-%c|--%s]    \t\t%s\n", opts[i].val, opts[i].name, help_text );
   }

   exit(exitcode);
}


int AG_main( int argc, char** argv ) {
   return gateway_main( SYNDICATE_AG, argc, argv );
}

// main method
// usage: driver SERVER_IP SERVER_PORT
int gateway_main( int gateway_type, int argc, char** argv ) {
   curl_global_init(CURL_GLOBAL_ALL);

   // start up protocol buffers
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = 0;
   char* config_file = strdup( GATEWAY_DEFAULT_CONFIG );

   //Initialize global config struct
   global_conf = ( struct md_syndicate_conf* )
		malloc( sizeof ( struct md_syndicate_conf ) );
                
   global_ms = CALLOC_LIST( struct ms_client, 1 );

   // process command-line options
   bool make_daemon = true;
   char* logfile = NULL;
   char* pidfile = NULL;
   char* metadata_url = NULL;
   int portnum = 0;
   char* portnum_str = NULL;
   char* username = NULL;
   char* password = NULL;
   char* volume_name = NULL;
   char* dataset = NULL;
   char* gw_driver = NULL;
   char* gateway_name = NULL;
   bool pub_mode = false;
   char* volume_pubkey_path = NULL;
   char* gateway_pkey_path = NULL;
   char* tls_pkey_path = NULL;
   char* tls_cert_path = NULL;
   pid_t gateway_daemon_pid = 0;
   bool rmap = false;
   bool stop = false;
   
   static struct option gateway_options[] = {
      {"config-file\0Gateway configuration file path",      required_argument,   0, 'c'},
      {"volume-name\0Name of the volume to join",           required_argument,   0, 'v'},
      {"username\0User authentication identity",             required_argument,   0, 'u'},
      {"password\0User authentication secret",               required_argument,   0, 'p'},
      {"port\0Syndicate port number",                       required_argument,   0, 'P'},
      {"MS\0Metadata Service URL",                          required_argument,   0, 'm'},
      {"foreground\0Run in the foreground",                 no_argument,         0, 'f'},
      {"overwrite\0Overwrite previous upload on conflict",  no_argument,         0, 'w'},
      {"logfile\0Path to the log file",                     required_argument,   0, 'l'},
      {"pidfile\0Path to the PID file",                     required_argument,   0, 'i'},
      {"dataset\0Path to dataset",                     	    required_argument,   0, 'd'},
      {"gw-driver\0Gateway driver",                         required_argument,   0, 'D'},
      {"gateway-name\0Name of this gateway",                required_argument,   0, 'g'},
      {"volume-pubkey\0Volume public key path (PEM)",       required_argument,   0, 'V'},
      {"gateway-pkey\0Gateway private key path (PEM)",      required_argument,   0, 'G'},
      {"tls-pkey\0Server TLS private key path (PEM)",       required_argument,   0, 'S'},
      {"tls-cert\0Server TLS certificate path (PEM)",       required_argument,   0, 'C'},
      {"stop\0Stop the gateway daemon",                     required_argument,   0, 't'},
      {"remap\0Remap file mapping",			    required_argument,   0, 'r'},
      {"help\0Print this message",                          no_argument,         0, 'h'},
      {0, 0, 0, 0}
   };

   int opt_index = 0;
   int c = 0;
   while((c = getopt_long(argc, argv, "c:v:u:p:P:m:fwl:i:d:D:hg:V:G:S:C:t:r:", gateway_options, &opt_index)) != -1) {
      switch( c ) {
         case 'v': {
            volume_name = optarg;
            break;
         }
         case 'c': {
            config_file = optarg;
            break;
         }
         case 'u': {
            username = optarg;
            break;
         }
         case 'p': {
            password = optarg;
            break;
         }
         case 'P': {
            portnum_str = optarg;
            break;
         }
         case 'm': {
            metadata_url = optarg;
            break;
         }
         case 'f': {
            make_daemon = false;
            break;
         }
         case 'w': {
            allow_overwrite = true;
            break;
         }
         case 'l': {
            logfile = optarg;
            break;
         }
         case 'i': {
            pidfile = optarg;
            break;
         }
         case 'd': {
            dataset = optarg;
	    pub_mode = true;
            break;
         }
         case 'D': {
            gw_driver = optarg;
            break;
         }
         case 'h': {
            gateway_usage( argv[0], gateway_options, 0 );
            break;
         }
         case 'g': {
            gateway_name = optarg;
            break;
         }
         case 'V': {
            volume_pubkey_path = optarg;
            break;
         }
         case 'G': {
            gateway_pkey_path = optarg;
            break;
         }
         case 'S': {
            tls_pkey_path = optarg;
            break;
         }
         case 'C': {
            tls_cert_path = optarg;
            break;
         }
         case 't': {
            gateway_daemon_pid = strtol(optarg, NULL, 10);
	    stop = true;
            break;
         }
         case 'r': {
            gateway_daemon_pid = strtol(optarg, NULL, 10);
	    rmap = true;
            break;
         }
         default: {
            break;
         }
      }
   }

   if (gateway_daemon_pid) {
       int flag = 0;
       struct md_syndicate_conf controller_conf;
       if (config_file != NULL) {
	   md_read_conf(config_file, &controller_conf); 
       }

       // load AG driver
       if ( controller_conf.ag_driver && gateway_type == SYNDICATE_AG) {
	   if (gw_driver != NULL)
	       controller_conf.ag_driver = gw_driver;
	   if (controller_conf.ag_driver == NULL) {
	       cerr<<"AG controller is unable to load the AG driver."<<endl;
	       exit(1);
	   }
	   if ( load_AG_driver( controller_conf.ag_driver ) < 0) {
	       cerr<<"AG controller is unable to load the AG driver."<<endl;
	       exit(1);
	   }
       }
       if (stop) {
	   flag |= STOP_CTRL_FLAG;
       }
       if (rmap) {
	   flag |= RMAP_CTRL_FLAG;
       }
       if ( controller_callback(gateway_daemon_pid, flag) < 0) {
	   cerr<<"Controller Failed"<<endl;
	   exit(1);
       }
       exit(0);
   }
   
   if( portnum_str != NULL ) {
      portnum = strtol( portnum_str, NULL, 10 );

      if( portnum == 0 ) {
         fprintf(stderr, "Invalid port number\n");
         gateway_usage( argv[0], gateway_options, 1 );
      }
   }

   // set up Syndicate
   struct ms_client client;
   struct md_syndicate_conf conf;
 
   rc = md_init( gateway_type, config_file, &conf, &client, portnum, metadata_url, volume_name, gateway_name, username, password, volume_pubkey_path, gateway_pkey_path, tls_pkey_path, tls_cert_path );
   if( rc != 0 ) {
      exit(1);
   }
   // override ag_driver provided in conf file if driver is 
   // specified as a command line argument.
   if ( gw_driver ) {
       if (gateway_type == SYNDICATE_AG)
	   conf.ag_driver = gw_driver;
   }
   
   // get the block size from the ms cert
   conf.ag_block_size = ms_client_get_AG_blocksize( &client, client.gateway_id );
   dbprintf("blocksize will be %" PRIu64 "\n", conf.ag_block_size );
   
   memcpy( global_conf, &conf, sizeof( struct md_syndicate_conf ) );
   memcpy( global_ms, &client, sizeof( struct ms_client ) );
   
   // load AG driver
   if ( conf.ag_driver && gateway_type == SYNDICATE_AG) {
      if ( load_AG_driver( conf.ag_driver ) < 0)
	 exit(1);
   }
   if (pub_mode) {
       if ( publish_callback ) {
	   if ( ( rc = publish_callback( NULL, &client, dataset ) ) !=0 )
	       errorf("publish_callback rc = %d\n", rc);
       }
       else {
	   errorf("%s\n", "AG Publisher mode is not implemented...");
           exit(1);
       }
   }
   if ( (rc = start_gateway_service( &conf, &client, logfile, pidfile, make_daemon )) ) {
       errorf( "start_gateway_service rc = %d\n", rc);	
   } 

   return 0;
}

int start_gateway_service( struct md_syndicate_conf *conf, struct ms_client *client, char* logfile, char* pidfile, bool make_daemon ) {
   int rc = 0;
   // clean up stale records
   // overwrite mandated by config?
   if( conf->replica_overwrite )
      allow_overwrite = true;
   
   // need to daemonize?
   if( make_daemon ) {
      FILE* log = NULL;
      rc = md_daemonize( logfile, pidfile, &log );
      if( rc < 0 ) {
         errorf( "md_daemonize rc = %d\n", rc );
         exit(1);
      }
   }

   // start gateway server
   struct md_HTTP http;

   rc = gateway_init( &http, conf, client );
   if( rc != 0 ) {
      return rc;
   }

   setup_signals();

   while( gateway_running ) {
      sleep(1);
   }

   gateway_shutdown( &http );


   // shut down protobufs
   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}


/**
 * open the diver library and setup following callbacks
 * get_callback,
 * connect_callback,
 * cleanup_callback,
 * metadata_callback,
 * publish_callback
 **/
int load_AG_driver( char *lib ) 
{
   // open library
   driver = dlopen( lib, RTLD_LAZY );
   if ( driver == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -EINVAL;
   }

   // setup callbacks
   *(void **) (&get_callback) = dlsym( driver, "get_dataset" );
   if ( get_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   *(void **) (&connect_callback) = dlsym( driver, "connect_dataset" );
   if ( connect_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   *(void **) (&cleanup_callback) = dlsym( driver, "cleanup_dataset" );
   if ( cleanup_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   *(void **) (&metadata_callback) = dlsym( driver, "metadata_dataset" );
   if ( metadata_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   *(void **) (&publish_callback) = dlsym( driver, "publish_dataset" );
   if ( publish_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   *(void **) (&controller_callback) = dlsym( driver, "controller" );
   if ( controller_callback == NULL ) {
      errorf( "load_AG_gateway_driver = %s\n", dlerror() );
      return -ENXIO;
   }
   return 0;
}

int unload_AG_driver( )
{
   if (driver == NULL) {
      return -1;
   }
   dlclose( driver );
   return 0;
}

