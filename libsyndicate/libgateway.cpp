/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/



#include "libgateway.h"

static bool gateway_running = true;
static bool allow_overwrite = false;
static md_path_locks gateway_md_locks;

// session ID for written data
static int64_t SESSION_ID = 0;

// gateway driver
static void* driver = NULL;

// callbacks to be filled in by an RG implementation
static ssize_t (*put_callback)( struct gateway_context*, char const* data, size_t len, void* usercls ) = NULL;
static ssize_t (*get_callback)( struct gateway_context*, char* data, size_t len, void* usercls ) = NULL;
static int (*delete_callback)( struct gateway_context*, void* usercls ) = NULL;
static void* (*connect_callback)( struct gateway_context* ) = NULL;
static void (*cleanup_callback)( void* usercls ) = NULL;
static int (*metadata_callback)( struct gateway_context*, ms::ms_gateway_blockinfo*, void* ) = NULL;
static int (*publish_callback)( struct gateway_context*, ms_client*, char* ) = NULL;

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
void gateway_metadata_func( int (*metadata_func)(struct gateway_context* ctx, ms::ms_gateway_blockinfo*, void*) ) {
   metadata_callback = metadata_func;
}


// set the publish callback
void gateway_publish_func( int (*publish_func)(struct gateway_context*, ms_client*, char*) ){
   publish_callback = publish_func;
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

// get the full path of a file chunk to gatewayte
// NOTE: fp should be at least PATH_MAX bytes long
static char* gateway_fullpath( char const* base, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, char* fp ) {
   snprintf(fp, PATH_MAX, "%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, base, fs_path, file_version, block_id, block_version );
   return fp;
}

// store a gateway's metadata.
// return -EEXIST if it already exists and we can't overwrite (i.e. replace == False)
static int gateway_store_metadata( struct md_syndicate_conf* conf, ms::ms_gateway_blockinfo* info, bool replace ) {

   info->set_write_time( currentTimeSeconds() );
   info->set_session_id( SESSION_ID );

   string info_str;
   bool src = info->SerializeToString( &info_str );
   if( !src ) {
      errorf("%s", " could not serialize\n");
      return -EINVAL;
   }

   char gateway_dir[PATH_MAX];
   char gateway_fp[PATH_MAX];
   memset( gateway_fp, 0, PATH_MAX );
   memset( gateway_dir, 0, PATH_MAX );

   gateway_fullpath( conf->gateway_metadata_root, info->fs_path().c_str(), info->file_version(), info->block_id(), info->block_version(), gateway_fp );
   md_dirname( gateway_fp, gateway_dir );

   int rc = md_mkdirs( gateway_dir );
   if( rc != 0 ) {
      errorf( "md_mkdirs rc = %d\n", rc );
      return rc;
   }


   if( allow_overwrite ) {
      // we can overwrite, but we need to make sure that this is atomic
      md_lock_path( &gateway_md_locks, info->fs_path().c_str() );
      unlink( gateway_fp );
   }

   int fd = -1;

   if( replace ) {
      fd = open( gateway_fp, O_WRONLY|O_TRUNC|O_SYNC, 0644 );
   }
   else {
      fd = open( gateway_fp, O_CREAT|O_EXCL|O_WRONLY|O_TRUNC|O_SYNC, 0644 );
   }

   if( allow_overwrite )
      // release lock
      md_unlock_path( &gateway_md_locks, info->fs_path().c_str() );

   if( fd < 0 ) {
      int errsv = -errno;
      errorf( "open errno = %d\n", errsv );
      return errsv;
   }

   size_t num_written = 0;
   while( num_written < info_str.size() ) {
      ssize_t nw = write( fd, info_str.c_str() + num_written, info_str.size() - num_written );
      if( nw < 0 ) {
         int errsv = -errno;
         errorf( "write errno = %d\n", errsv );

         close( fd );
         unlink( gateway_fp );
         return rc;
      }

      num_written += nw;
   }

   close( fd );

   return 0;
}


// get a gateway's metadata
// NOTE: path should be /fs_path.file_version/block_id.block_version
// return 0 on success
// return -ESTALE if the requested metadata was from a different session
// return negative errno for other errors
static int gateway_get_metadata( struct md_syndicate_conf* conf, char const* path, ms::ms_gateway_blockinfo* info ) {
   char* fullpath = md_fullpath( conf->gateway_metadata_root, path, NULL );

   if( allow_overwrite )
      md_lock_path( &gateway_md_locks, path );

   int fd = open( fullpath, O_RDONLY );

   if( allow_overwrite )
      md_unlock_path( &gateway_md_locks, path );

   if( fd < 0 ) {
      int errsv = -errno;
      errorf( "open(%s) errno = %d\n", fullpath, errsv );
      free( fullpath );
      return errsv;
   }

   response_buffer_t rb;

   char buf[32768];
   while( true ) {
      ssize_t nr = read( fd, buf, 32768 );

      if( nr > 0 ) {
         char* buf_cp = CALLOC_LIST( char, nr );
         memcpy( buf_cp, buf, nr );

         rb.push_back( buffer_segment_t( buf_cp, nr ) );
      }
      else if( nr == 0 ) {
         // EOF
         break;
      }
      else {
         // error
         int errsv = -errno;
         errorf( "read errno = %d\n", errsv );

         close(fd);
         response_buffer_free( &rb );
         free( fullpath );
         return errsv;
      }
   }

   size_t len = response_buffer_size( &rb );
   char* data = response_buffer_to_string( &rb );

   string info_str( data, len );

   free( data );
   response_buffer_free( &rb );
   free( fullpath );
   close( fd );

   bool src = info->ParseFromString( info_str );
   if( !src ) {
      errorf("%s", " failed to parse\n");
      return -ENODATA;
   }

   // verify that this data has been committed
   if( info->progress() != ms::ms_gateway_blockinfo::COMMITTED && info->session_id() != SESSION_ID ) {
      // we were in the process of backing something up earlier, and it failed
      errorf("uncommitted data for %s\n", path);
      return -ESTALE;
   }

   return 0;
}


// delete a gateway's metadata
static int gateway_delete_metadata( struct md_syndicate_conf* conf, char const* path ) {
   char* fullpath = md_fullpath( conf->gateway_metadata_root, path, NULL );

   int rc = unlink( fullpath );
   if( rc != 0 ) {
      rc = -errno;
      errorf( "unlink errno = %d\n", rc );
      free( fullpath );
      return rc;
   }

   // remove directories (will fail when we reach an unempty directory
   char* fullpath_dir = md_dirname( fullpath, NULL );
   md_rmdirs( fullpath_dir );
   free( fullpath_dir );
   free( fullpath );

   return 0;
}


static char const* CONNECT_ERROR = "CONNECT ERROR";

// connection handler
static void* gateway_HTTP_connect( struct md_HTTP_connection_data* md_con_data ) {
   struct gateway_connection_data* con_data = CALLOC_LIST( struct gateway_connection_data, 1 );
   
   con_data->user = md_con_data->user;
   con_data->rb = new response_buffer_t();
   con_data->block_info = new ms::ms_gateway_blockinfo();
   con_data->has_gateway_md = false;
   con_data->err = 0;

   if( md_con_data->query_string ) {
      char* args_str = strdup( md_con_data->query_string );
      con_data->ctx.args = md_parse_cgi_args( args_str );
   }
   
   con_data->ctx.url_path = md_con_data->url_path;
   con_data->ctx.username = md_con_data->user->username;
   con_data->ctx.hostname = md_con_data->remote_host;
   con_data->ctx.method = md_con_data->method;
   con_data->ctx.size = md_con_data->conf->blocking_factor;
   con_data->ctx.err = 0;
   
   md_con_data->status = 200;
   
   if( connect_callback ) {

      void* cls = (*connect_callback)( &con_data->ctx );
      if( cls == NULL ) {
         md_con_data->status = -500;
         if( con_data->ctx.err != 0 )
            md_con_data->status = con_data->ctx.err;
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

            bool parse_rc = con_data->block_info->ParseFromString( string(buf, buf_len) );
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

               // put metadata for this block
               con_data->block_info->set_progress( ms::ms_gateway_blockinfo::STARTED );
               int rc = gateway_store_metadata( md_con_data->conf, con_data->block_info, false );
               if( rc != 0 ) {
                  // failed to record
                  con_data->err = rc;
                  if( rc == -EEXIST )
                     md_con_data->status = -409;
                  else
                     md_con_data->status = -500;

                  errorf( "gateway_store_metadata rc = %d\n", rc );
                  return MHD_NO;
               }
            }
         }

         if( con_data->has_gateway_md && con_data->err == 0 && md_con_data->status == 200 ) {
            // feed the data to the callback
            if( put_callback ) {
               ssize_t num_put = (*put_callback)( &con_data->ctx, data, size, con_data->user_cls );
               if( num_put != (signed)size ) {
                  errorf( "user PUT returned %zd\n", num_put );
                  md_con_data->status = -500;
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

      // clean up
      gateway_delete_metadata( md_con_data->conf, rpc->ctx.url_path );
   }
   else {
      // we're good
      md_create_HTTP_response_ram_static( md_con_data->resp, "text/plain", 200, MD_HTTP_200_MSG, strlen(MD_HTTP_200_MSG) + 1 );

      // commit this
      rpc->block_info->set_progress( ms::ms_gateway_blockinfo::COMMITTED );
      gateway_store_metadata( md_con_data->conf, rpc->block_info, true );
   }
   md_HTTP_add_header( md_con_data->resp, "Connection", "keep-alive" );
   return;
}


// MHD callback for streaming data from the gateway server implementation
static ssize_t gateway_HTTP_read_callback( void* cls, uint64_t pos, char* buf, size_t max ) {
   struct gateway_connection_data* rpc = (struct gateway_connection_data*)cls;

   ssize_t ret = -1;
   if( get_callback ) {
      ret = (*get_callback)( &rpc->ctx, buf, max, rpc->user_cls );
      if( ret == 0 )
         ret = -1;
   }
   return ret;
}


// make a last-modified header
static void add_last_mod_header( struct md_HTTP_response* resp, time_t date ) {

   char hdr_buf[200];
   memset( hdr_buf, 0, 200 );

   struct tm utc_time;
   gmtime_r( &date, &utc_time );
   strftime( hdr_buf, 200, "%a, %d %b %Y %H:%M:%S GMT", &utc_time );

   md_HTTP_add_header( resp, "Last-Modified", hdr_buf );
}

// gateway GET handler
static char const* GATEWAY_GET_INVALID = "Invalid\n";

static struct md_HTTP_response* gateway_GET_handler( struct md_HTTP_connection_data* md_con_data ) {
   struct md_HTTP* http = md_con_data->http;

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      // shouldn't happen
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }

   // first, get the metadata
   int rc = 0;
   time_t last_mod = 0;
   ms::ms_gateway_blockinfo info;

   // get the metadata for this object
   if( metadata_callback == NULL ) {   
      rc = gateway_get_metadata( http->conf, rpc->ctx.url_path, &info );
   }
   else {
      rc = (*metadata_callback)( &rpc->ctx, &info, rpc->user_cls );
   }

   
   if( rc != 0 ) {
      // no metadata for this entry
      md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
   }
   else {
      if( info.progress() == ms::ms_gateway_blockinfo::COMMITTED ) {
         // we have data
         last_mod = info.write_time();
      }
      else {
         // we don't have data yet to serve
         md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
         rc = -404;
      }
   }
   
   if( rc == 0 ) {
      // have entry! start reading
      if( get_callback ) {
         md_create_HTTP_response_stream( resp, "application/octet-stream", 200, rpc->ctx.size, 4096, gateway_HTTP_read_callback, rpc, NULL );
         add_last_mod_header( resp, last_mod );
      }
      else {
         md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
      }
   }
   md_HTTP_add_header( resp, "Connection", "keep-alive" );
   return resp;
}


// gateway HEAD handler
// HEAD to /$DRIVER_BIN/$DRIVER/$PATH.$FILE_VERSION/$BLOCK_ID.$BLOCK_VERSION?$QUERY_STRING
static struct md_HTTP_response* gateway_HEAD_handler( struct md_HTTP_connection_data* md_con_data ) {
   struct md_HTTP* http = md_con_data->http;

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }
   
   // do we have metadata for this?
   int rc = 0;
   ms::ms_gateway_blockinfo info;

   if( metadata_callback == NULL ) {
      rc = gateway_get_metadata( http->conf, rpc->ctx.url_path, &info );
   }
   else {
      rc = (*metadata_callback)( &rpc->ctx, &info, rpc->user_cls );
   }

   if( rc != 0 ) {
      // error
      md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
   }
   else {
      string info_str;
      bool src = info.SerializeToString( &info_str );
      if( !src ) {
         errorf( "could not serialize metadata for %s\n", rpc->ctx.url_path );
         md_create_HTTP_response_ram_static( resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
         rc = -500;
      }
      else {
         md_create_HTTP_response_ram( resp, "text/plain", 200, info_str.c_str(), info_str.size() );
         add_last_mod_header( resp, info.write_time() );
      }
   }

   md_HTTP_add_header( md_con_data->resp, "Connection", "keep-alive" );
   return resp;
}


// DELETE handler
static struct md_HTTP_response* gateway_DELETE_handler( struct md_HTTP_connection_data* md_con_data, int depth ) {
   struct md_HTTP* http = md_con_data->http;

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_connection_data* rpc = (struct gateway_connection_data*)md_con_data->cls;

   if( rpc == NULL ) {
      md_create_HTTP_response_ram_static( resp, "text/plain", md_con_data->status, GATEWAY_GET_INVALID, strlen(GATEWAY_GET_INVALID) + 1);
      return resp;
   }

   // do we have metadata for this?
   ms::ms_gateway_blockinfo info;
   int rc = gateway_get_metadata( http->conf, rpc->ctx.url_path, &info );
   if( rc != 0 ) {
      // error
      md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
   }
   else {
      if( delete_callback ) {
         // delete our record
         rc = gateway_delete_metadata( http->conf, rpc->ctx.url_path );
         if( rc != 0 ) {
            errorf( "gateway_delete_metadata rc = %d\n", rc );

            char buf[15];
            sprintf(buf, "%d", rc );
            md_create_HTTP_response_ram( resp, "text/plain", 500, buf, strlen(buf) + 1 );
         }
         else {
            rc = (*delete_callback)( &rpc->ctx, rpc->user_cls );
            if( rc != 0 ) {
               errorf( "DELETE callback rc = %d\n", rc );

               char buf[15];
               sprintf(buf, "%d", rc );
               md_create_HTTP_response_ram( resp, "text/plain", 500, buf, strlen(buf) + 1 );
            }
         }
      }
      else {
         md_create_HTTP_response_ram_static( resp, "text/plain", 501, MD_HTTP_501_MSG, strlen(MD_HTTP_501_MSG) + 1 );
      }
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

   if( con_data->block_info ) {
      delete con_data->block_info;
   }

   if( con_data->ctx.args )
      free( con_data->ctx.args );

   if( cleanup_callback ) {
      (*cleanup_callback)( con_data->user_cls );
   }
   
   free( con_data );
}

// accumulate a list of regular file paths in the gateway metadata directory
static vector<char*> __accumulate_reg_paths_buf;

static int accumulate_reg_paths( char const* fpath, const struct stat *sb, int tflag ) {
   if( tflag == FTW_F ) {
      __accumulate_reg_paths_buf.push_back( strdup( fpath ) );
   }
   return 0;
}

// clean up all uncommitted transactions
// NOTE: NOT THREAD-SAFE
static void cleanup_stale_transactions( struct md_syndicate_conf* conf ) {
   __accumulate_reg_paths_buf.clear();
   
   int rc = ftw( conf->gateway_metadata_root, accumulate_reg_paths, 1000 );
   
   if( rc == 0 ) {
      for( unsigned int i = 0; i < __accumulate_reg_paths_buf.size(); i++ ) {
         char* path = __accumulate_reg_paths_buf[i];
         
         ms::ms_gateway_blockinfo info;
         int rc = gateway_get_metadata( conf, path, &info );
         if( rc == -ESTALE ) {
            // this is a stale entry
            errorf("Removing uncommitted metadata record for %s\n", path );
            unlink( path );
         }
      }
   }

   for( unsigned int i = 0; i < __accumulate_reg_paths_buf.size(); i++ ) {
      free( __accumulate_reg_paths_buf[i] );
   }
   __accumulate_reg_paths_buf.clear();
}


// start up server
static int gateway_init( struct md_HTTP* http, struct md_syndicate_conf* conf, struct md_user_entry** users ) {
   
   md_path_locks_create( &gateway_md_locks );

   md_HTTP_init( http, MHD_USE_SELECT_INTERNALLY | MHD_USE_POLL | MHD_USE_DEBUG, conf, users );
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

   md_path_locks_free( &gateway_md_locks );
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
      char const* help_text = opts[i].name + strlen(opts[i].name);

      if( opts[i].has_arg == required_argument )
         fprintf(stderr, "\t[-%c|--%s] ARG\t\t%s\n", opts[i].val, opts[i].name, help_text );
      else
         fprintf(stderr, "\t[-%c|--%s]    \t\t%s\n", opts[i].val, opts[i].name, help_text );
   }

   exit(exitcode);
}


static void gateway_init_session_id() {
   // make a session id
   int random_fd = open("/dev/urandom", O_RDONLY);
   char random_bits[8];
   read( random_fd, random_bits, 8 );
   close( random_fd );
   memcpy( &SESSION_ID, random_bits, 8 );
}


int RG_main( int argc, char** argv ) {
   return gateway_main( SYNDICATE_RG, argc, argv );
}

int AG_main( int argc, char** argv ) {
   return gateway_main( SYNDICATE_AG, argc, argv );
}

// main method
// usage: driver SERVER_IP SERVER_PORT
int gateway_main( int gateway_type, int argc, char** argv ) {
   curl_global_init(CURL_GLOBAL_ALL);

   gateway_init_session_id();
   
   // start up protocol buffers
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = 0;
   char* config_file = strdup( GATEWAY_DEFAULT_CONFIG );

   //Initialize global config struct
   global_conf = ( struct md_syndicate_conf* )
		malloc( sizeof ( struct md_syndicate_conf ) );

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
   char* volume_secret = NULL;
   char* dataset = NULL;
   bool pub_mode = false;
   
   static struct option gateway_options[] = {
      {"config-file\0Gateway configuration file path",      required_argument,   0, 'c'},
      {"volume-name\0Name of the volume to join",           required_argument,   0, 'v'},
      {"volume-secret\0Volume authentication secret",       required_argument,   0, 'S'},
      {"username\0Gateway authentication identity",         required_argument,   0, 'u'},
      {"password\0Gateway authentication secret",           required_argument,   0, 'p'},
      {"port\0Syndicate port number",                       required_argument,   0, 'P'},
      {"MS\0Metadata Service URL",                          required_argument,   0, 'm'},
      {"foreground\0Run in the foreground",                 no_argument,         0, 'f'},
      {"overwrite\0Overwrite previous upload on conflict",  no_argument,         0, 'w'},
      {"logfile\0Path to the log file",                     required_argument,   0, 'l'},
      {"pidfile\0Path to the PID file",                     required_argument,   0, 'i'},
      {"dataset\0Path to dataset",                     	    required_argument,   0, 'd'},
      {"help\0Print this message",                          no_argument,         0, 'h'},
      {0, 0, 0, 0}
   };

   int opt_index = 0;
   int c = 0;
   while((c = getopt_long(argc, argv, "c:v:S:u:p:P:m:fwl:i:d:h", gateway_options, &opt_index)) != -1) {
      switch( c ) {
         case 'v': {
            volume_name = optarg;
            break;
         }
         case 'c': {
            config_file = optarg;
            break;
         }
         case 'S': {
            volume_secret = optarg;
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
         case 'h': {
            gateway_usage( argv[0], gateway_options, 0 );
            break;
         }
         default: {
            break;
         }
      }
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
   struct md_user_entry** users = NULL;
   struct md_syndicate_conf conf;
   
   rc = md_init( gateway_type, config_file, &conf, &client, &users, portnum, metadata_url, volume_name, volume_secret, username, password );
   if( rc != 0 ) {
      exit(1);
   }

   // copy conf to global_conf
   memcpy( global_conf, &conf, sizeof( struct md_syndicate_conf ) );
   // load AG driver
   if ( conf.ag_driver ) {
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
   if ( (rc = start_gateway_service( &conf, &client, users, logfile, pidfile, make_daemon )) ) {
       errorf( "start_gateway_service rc = %d\n", rc);	
   } 

   return 0;
}

int start_gateway_service( struct md_syndicate_conf *conf, ms_client *client, md_user_entry** users,
			   char* logfile, char* pidfile, bool make_daemon ) {
   int rc = 0;
   // clean up stale records
   cleanup_stale_transactions( conf );

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

   rc = gateway_init( &http, conf, users );
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

