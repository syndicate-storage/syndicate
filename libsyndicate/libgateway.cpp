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


#include "libsyndicate/libgateway.h"

static bool gateway_running = true;
static bool allow_overwrite = false;

// gloabl config
struct md_syndicate_conf *global_conf = NULL;

// global ms
struct ms_client *global_ms = NULL;

// gateway driver
static void* driver = NULL;

// callbacks to be filled in by an AG implementation
static ssize_t (*get_callback)( struct gateway_context*, char* data, size_t len, void* usercls ) = NULL;
static void* (*connect_callback)( struct gateway_context* ) = NULL;
static void (*cleanup_callback)( void* usercls ) = NULL;
static int (*metadata_callback)( struct gateway_context*, ms::ms_gateway_request_info*, void* ) = NULL;
static int (*publish_callback)( struct gateway_context*, ms_client*, char* ) = NULL;
int (*controller_callback)(pid_t pid, int ctrl_flag);

// set the GET callback
void gateway_get_func( ssize_t (*get_func)(struct gateway_context*, char* buf, size_t len, void* usercls) ) {
   get_callback = get_func;
}

// set the CGI argument parser callback
void gateway_connect_func( void* (*connect_func)( struct gateway_context* ) ) {
   connect_callback = connect_func;
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

// free a gateway_connection_data
static void gateway_connection_data_free( struct gateway_connection_data* con_data ) {
   
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


// generate a signed manifest from an md_entry
int gateway_manifest( struct md_entry* ent, Serialization::ManifestMsg* mmsg ) {
   // populate the manifest
   mmsg->set_volume_id( ent->volume );
   mmsg->set_coordinator_id( global_ms->gateway_id );        // we're always the coordinator
   mmsg->set_owner_id( ent->owner );
   mmsg->set_file_id( ent->file_id );
   mmsg->set_size( ent->size );
   mmsg->set_file_version( ent->version );
   mmsg->set_mtime_sec( ent->mtime_sec );
   mmsg->set_mtime_nsec( ent->mtime_nsec );

   uint64_t blocking_factor = global_conf->ag_block_size;
   uint64_t num_blocks = ent->size / blocking_factor;
   if( ent->size % blocking_factor != 0 )
      num_blocks++;

   Serialization::BlockURLSetMsg *bbmsg = mmsg->add_block_url_set();
   bbmsg->set_start_id( 0 );
   bbmsg->set_end_id( num_blocks );
   bbmsg->set_gateway_id( global_ms->gateway_id );   // we're always the coordinator

   for( uint64_t i = 0; i < num_blocks; i++ ) {
      bbmsg->add_block_versions( 1 );
   }
   
   // sign the message
   int rc = gateway_sign_manifest( global_ms->my_key, mmsg );
   if( rc != 0 ) {
      errorf("gateway_sign_manifest rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}

static char const* CONNECT_ERROR = "CONNECT ERROR";

// connection handler
static void* gateway_HTTP_connect( struct md_HTTP_connection_data* md_con_data ) {
   
   // sanity check...
   if( strcmp(md_con_data->method, "GET") != 0 ) {
      md_con_data->status = 501;
      return NULL;
   }
   
   struct md_gateway_request_data reqdat;
   memset( &reqdat, 0, sizeof(reqdat) );
   
   int rc = md_HTTP_parse_url_path( md_con_data->url_path, &reqdat.volume_id, &reqdat.fs_path, &reqdat.file_version, &reqdat.block_id, &reqdat.block_version, &reqdat.manifest_timestamp );
   if( rc != 0 ) {
      errorf( "failed to parse '%s', rc = %d\n", md_con_data->url_path, rc );
      
      return NULL;
   }
   
   struct gateway_connection_data* con_data = CALLOC_LIST( struct gateway_connection_data, 1 );
   
   con_data->rb = new response_buffer_t();
   con_data->has_gateway_md = false;
   con_data->err = 0;

   if( md_con_data->query_string ) {
      char* args_str = strdup( md_con_data->query_string );
      con_data->ctx.args = md_parse_cgi_args( args_str );
   }
   
   memcpy( &con_data->ctx.reqdat, &reqdat, sizeof(reqdat) );
   con_data->ctx.block_info = new ms::ms_gateway_request_info();
   con_data->ctx.hostname = md_con_data->remote_host;           // WARNING: not a copy; don't free this!
   con_data->ctx.method = md_con_data->method;
   con_data->ctx.size = global_conf->ag_block_size;
   con_data->ctx.err = 0;
   con_data->ctx.http_status = 0;
   
   md_con_data->status = 200;
   
   if( connect_callback ) {

      void* cls = (*connect_callback)( &con_data->ctx );
      if( cls == NULL ) {
         md_con_data->status = get_http_status( &con_data->ctx, 500 );
      }

      con_data->user_cls = cls;
   }

   if( md_con_data->status != 200 ) {
      // error occurred above... make an error response for it.
      
      md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );
      md_create_HTTP_response_ram_static( md_con_data->resp, "text/plain", abs(md_con_data->status), CONNECT_ERROR, strlen(CONNECT_ERROR) + 1 );
      
      gateway_connection_data_free( con_data );
      con_data = NULL;
   }
   
   return (void*)con_data;
}

// MHD callback for streaming data from the gateway server implementation
static ssize_t gateway_HTTP_read_callback( void* cls, uint64_t pos, char* buf, size_t max ) {
   struct gateway_connection_data* rpc = (struct gateway_connection_data*)cls;

   ssize_t ret = -1;
   if( get_callback ) {
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
   
   int rc = md_HTTP_parse_url_path( url_path, &volume_id, &file_path, &file_version, &block_id, &block_version, &manifest_timestamp );
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

// clean up
static void gateway_cleanup( struct MHD_Connection *connection, void *user_cls, enum MHD_RequestTerminationCode term) {

   struct gateway_connection_data *con_data = (struct gateway_connection_data*)(user_cls);
   
   gateway_connection_data_free( con_data );
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
   
   md_HTTP_init( http, MHD_USE_SELECT_INTERNALLY | MHD_USE_POLL | MHD_USE_DEBUG, conf, ms );
   md_HTTP_connect( *http, gateway_HTTP_connect );
   md_HTTP_GET( *http, gateway_GET_handler );
   md_HTTP_close( *http, gateway_cleanup );

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

// main method
// usage: driver SERVER_IP SERVER_PORT
int AG_main( int argc, char** argv ) {
   curl_global_init(CURL_GLOBAL_ALL);

   // start up protocol buffers
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = 0;
   char* config_file = NULL;

   //Initialize global config struct
   global_conf = CALLOC_LIST( struct md_syndicate_conf, 1 );
   global_ms = CALLOC_LIST( struct ms_client, 1 );
   
   // process command-line options
   bool make_daemon = true;
   char* logfile = NULL;
   char* pidfile = NULL;
   char* metadata_url = NULL;
   char* username = NULL;
   char* password = NULL;
   char* volume_name = NULL;
   char* dataset = NULL;
   char* gw_driver = NULL;
   char* gateway_name = NULL;
   bool pub_mode = false;
   char* volume_pubkey_path = NULL;
   char* gateway_pkey_path = NULL;
   char* gateway_pkey_decryption_password = NULL;
   char* tls_pkey_path = NULL;
   char* tls_cert_path = NULL;
   char* syndicate_pubkey_path = NULL;
   pid_t gateway_daemon_pid = 0;
   bool rmap = false;
   bool stop = false;
   
   static struct option gateway_options[] = {
      {"config-file\0Gateway configuration file path",      required_argument,   0, 'c'},
      {"volume-name\0Name of the volume to join",           required_argument,   0, 'v'},
      {"username\0User authentication identity",             required_argument,   0, 'u'},
      {"password\0User authentication secret",               required_argument,   0, 'p'},
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
      {"gateway-pkey-password\0Gateway private key decryption password", required_argument, 0, 'K'},
      {"tls-pkey\0Server TLS private key path (PEM)",       required_argument,   0, 'T'},
      {"tls-cert\0Server TLS certificate path (PEM)",       required_argument,   0, 'C'},
      {"syndicate-pubkey\0Syndicate public key path (PEM)", required_argument,   0, 'S'},
      {"stop\0Stop the gateway daemon",                     required_argument,   0, 't'},
      {"remap\0Remap file mapping",			    required_argument,   0, 'r'},
      {"help\0Print this message",                          no_argument,         0, 'h'},
      {0, 0, 0, 0}
   };

   int opt_index = 0;
   int c = 0;
   while((c = getopt_long(argc, argv, "c:v:u:p:P:m:fwl:i:d:D:hg:V:G:S:T:C:t:r:K:", gateway_options, &opt_index)) != -1) {
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
         case 'T': {
            tls_pkey_path = optarg;
            break;
         }
         case 'S': {
            syndicate_pubkey_path = optarg;
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
         case 'K': {
            gateway_pkey_decryption_password = optarg;
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
       if ( controller_conf.ag_driver ) {
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
   
   // load config file
   md_default_conf( global_conf, SYNDICATE_AG );
   
   // read the config file
   if( config_file != NULL ) {
      rc = md_read_conf( config_file, global_conf );
      if( rc != 0 ) {
         errorf("WARN: failed to read %s, rc = %d\n", config_file, rc );
         return rc;
      }
   }
   
   rc = md_init( global_conf, global_ms, metadata_url, volume_name, gateway_name, username, password, volume_pubkey_path, gateway_pkey_path, gateway_pkey_decryption_password, tls_pkey_path, tls_cert_path, NULL, syndicate_pubkey_path );
   if( rc != 0 ) {
      exit(1);
   }
   
   // override ag_driver provided in conf file if driver is 
   // specified as a command line argument.
   if ( gw_driver ) {
       global_conf->ag_driver = gw_driver;
   }
   
   // get the block size from the ms cert
   global_conf->ag_block_size = ms_client_get_AG_blocksize( global_ms, global_ms->gateway_id );
   dbprintf("blocksize will be %" PRIu64 "\n", global_conf->ag_block_size );
   
   // load AG driver
   if ( global_conf->ag_driver ) {
      dbprintf("Load driver %s\n", global_conf->ag_driver );
      if ( load_AG_driver( global_conf->ag_driver ) < 0)
         exit(1);
   }
   else {
      errorf("%s", "No driver given!  Pass -D\n");
      exit(1);
   }
      
   if (pub_mode) {
       if ( publish_callback ) {
	   if ( ( rc = publish_callback( NULL, global_ms, dataset ) ) !=0 )
	       errorf("publish_callback rc = %d\n", rc);
       }
       else {
	   errorf("%s\n", "AG Publisher mode is not implemented...");
           exit(1);
       }
   }
   if ( (rc = start_gateway_service( global_conf, global_ms, logfile, pidfile, make_daemon )) ) {
       errorf( "start_gateway_service rc = %d\n", rc);	
   } 

   return 0;
}


// turn into a deamon
int daemonize( char* logfile_path, char* pidfile_path, FILE** logfile ) {

   FILE* log = NULL;
   int pid_fd = -1;
   
   if( logfile_path ) {
      log = fopen( logfile_path, "a" );
   }
   if( pidfile_path ) {
      pid_fd = open( pidfile_path, O_CREAT | O_EXCL | O_WRONLY, 0644 );
      if( pid_fd < 0 ) {
         // specified a PID file, and we couldn't make it.  someone else is running
         int errsv = -errno;
         errorf( "Failed to create PID file %s (error %d)\n", pidfile_path, errsv );
         return errsv;
      }
   }
   
   pid_t pid, sid;

   pid = fork();
   if (pid < 0) {
      int rc = -errno;
      errorf( "Failed to fork (errno = %d)\n", -errno);
      return rc;
   }

   if (pid > 0) {
      exit(0);
   }

   // child process 
   // umask(0);

   sid = setsid();
   if( sid < 0 ) {
      int rc = -errno;
      errorf("setsid errno = %d\n", rc );
      return rc;
   }

   if( chdir("/") < 0 ) {
      int rc = -errno;
      errorf("chdir errno = %d\n", rc );
      return rc;
   }

   close( STDIN_FILENO );
   close( STDOUT_FILENO );
   close( STDERR_FILENO );

   if( log ) {
      int log_fileno = fileno( log );

      if( dup2( log_fileno, STDOUT_FILENO ) < 0 ) {
         int errsv = -errno;
         errorf( "dup2 errno = %d\n", errsv);
         return errsv;
      }
      if( dup2( log_fileno, STDERR_FILENO ) < 0 ) {
         int errsv = -errno;
         errorf( "dup2 errno = %d\n", errsv);
         return errsv;
      }

      if( logfile )
         *logfile = log;
      else
         fclose( log );
   }
   else {
      int null_fileno = open("/dev/null", O_WRONLY);
      dup2( null_fileno, STDOUT_FILENO );
      dup2( null_fileno, STDERR_FILENO );
   }

   if( pid_fd >= 0 ) {
      char buf[10];
      sprintf(buf, "%d", getpid() );
      write( pid_fd, buf, strlen(buf) );
      fsync( pid_fd );
      close( pid_fd );
   }
   
   struct passwd* pwd;
   int ret = 0;
   
   // switch to "daemon" user, if possible
   pwd = getpwnam( "daemon" );
   if( pwd != NULL ) {
      setuid( pwd->pw_uid );
      dbprintf( "became user '%s'\n", "daemon" );
      ret = 0;
   }
   else {
      dbprintf( "could not become '%s'\n", "daemon" );
      ret = 1;
   }
   
   return ret;
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
      rc = daemonize( logfile, pidfile, &log );
      if( rc < 0 ) {
         errorf( "daemonize rc = %d\n", rc );
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

