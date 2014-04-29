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

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/storage.h"
#include "libsyndicate/ms-client.h"

static int _signals = 1;

// initialize server information
static int md_init_server_info( struct md_syndicate_conf* c ) {
   
   int rc = 0;
   
   if( !c->is_client ) {
   #ifndef _SYNDICATE_NACL_
      // get hostname
      struct addrinfo hints;
      memset( &hints, 0, sizeof(hints) );
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_flags = AI_CANONNAME;
      hints.ai_protocol = 0;
      hints.ai_canonname = NULL;
      hints.ai_addr = NULL;
      hints.ai_next = NULL;

      struct addrinfo *result = NULL;
      char hostname[HOST_NAME_MAX+1];
      gethostname( hostname, HOST_NAME_MAX );

      rc = getaddrinfo( hostname, NULL, &hints, &result );
      if( rc != 0 ) {
         // could not get addr info
         errorf("getaddrinfo: %s\n", gai_strerror( rc ) );
         return -abs(rc);
      }
      
      // now reverse-lookup ourselves
      char hn[HOST_NAME_MAX+1];
      rc = getnameinfo( result->ai_addr, result->ai_addrlen, hn, HOST_NAME_MAX, NULL, 0, NI_NAMEREQD );
      if( rc != 0 ) {
         errorf("getnameinfo: %s\n", gai_strerror( rc ) );
         return -abs(rc);
      }
      
      dbprintf("canonical hostname is %s\n", hn);

      c->hostname = strdup(hn);
   #else
      c->hostname = strdup("localhost");
   #endif
         
   }
   else {
      // fill in defaults, but they won't be used except for registration
      c->hostname = strdup("localhost");
      c->server_key = NULL;
      c->server_cert = NULL;
   }
   
   return rc;
}

// initialize all global data structures
static int md_runtime_init( struct md_syndicate_conf* c, char const* key_password ) {

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   int rc = md_crypt_init();
   if( rc != 0 ) {
      errorf("md_crypt_init rc = %d\n", rc );
      return rc;
   }
   
   // get the umask
   mode_t um = get_umask();
   c->usermask = um;
   
   rc = md_init_local_storage( c );
   if( rc != 0 ) {
      errorf("md_init_local_storage(%s) rc = %d\n", c->storage_root, rc );
      return rc;
   }
   
   rc = md_init_server_info( c );
   if( rc != 0 ) {
      errorf("md_init_server_info() rc = %d\n", rc );
      return rc;
   }
   
   // load gateway public/private key
   if( c->gateway_key_path != NULL ) {
      c->gateway_key = md_load_file_as_string( c->gateway_key_path, &c->gateway_key_len );

      if( c->gateway_key == NULL ) {
         errorf("Could not read Gateway key %s\n", c->gateway_key_path );
         rc = -ENOENT;
      }
      
      if( key_password ) {
         // need to decrypt
         char* unencrypted_key = NULL;
         size_t unencrypted_key_len = 0;
         
         rc = md_password_unseal( c->gateway_key, c->gateway_key_len, key_password, strlen(key_password), &unencrypted_key, &unencrypted_key_len );
         if( rc != 0 ) {
            errorf("md_password_unseal rc = %d\n", rc );
            
            free( c->gateway_key );
            c->gateway_key = NULL;
         }
         else {
            free( c->gateway_key );
            c->gateway_key = unencrypted_key;
         }
      }
   }
   
   // load volume public key, if given
   if( c->volume_pubkey_path != NULL ) {
      c->volume_pubkey = md_load_file_as_string( c->volume_pubkey_path, &c->volume_pubkey_len );
      if( c->volume_pubkey == NULL ) {
         errorf("Failed to load public key from %s\n", c->volume_pubkey_path );
         return -ENOENT;
      }
      
   }

   // load TLS credentials
   if( c->server_key_path != NULL && c->server_cert_path != NULL ) {
      
      c->server_key = md_load_file_as_string( c->server_key_path, &c->server_key_len );
      c->server_cert = md_load_file_as_string( c->server_cert_path, &c->server_cert_len );

      if( c->server_key == NULL ) {
         errorf( "Could not read TLS private key %s\n", c->server_key_path );
         rc = -ENOENT;
      }
      if( c->server_cert == NULL ) {
         errorf( "Could not read TLS certificate %s\n", c->server_cert_path );
         rc = -ENOENT;
      }
   }
   
   // load syndicate public key, if given 
   if( c->syndicate_pubkey_path != NULL ) {
      c->syndicate_pubkey = md_load_file_as_string( c->syndicate_pubkey_path, &c->syndicate_pubkey_len );
      if( c->syndicate_pubkey == NULL ) {
         errorf("Failed to load public key from %s\n", c->syndicate_pubkey_path );
         return -ENOENT;
      }
   }
   
   return rc;
}

// if level >= 1, this turns on debug messages.
// if level >= 2, this turns on locking debug messages
int md_debug( struct md_syndicate_conf* conf, int level ) {
   int prev = get_debug_level();
   set_debug_level( level );
   
   conf->debug_lock = false;
   if( level >= 2 )
      conf->debug_lock = true;
   
   return prev;
}

// if level >= 1, this turns on error messages
int md_error( struct md_syndicate_conf* conf, int level ) {
   int prev = get_error_level();
   set_error_level( level );
   return prev;
}


// shut down the library.
// free all global data structures
int md_shutdown() {
   
   // shut down protobufs
   google::protobuf::ShutdownProtobufLibrary();

   md_crypt_shutdown();
   return 0;
}


// read a configuration line, with the following syntax:
// KEY = "VALUE_1" "VALUE_2" ... "VALUE_N".  Arbitrarily many spaces are allowed.  
// Quotes within quotes are allowed, as long as they are escaped
// return number of values on success (0 for comments and empty lines), negative on syntax/parse error.
// Pass NULL for key and values if you want them allocated for you (otherwise, they must be big enough)
// key and value will remain NULL if the line was a comment.
int md_read_conf_line( char* line, char** key, char*** values ) {
   char* line_cpy = strdup( line );
   
   // split on '='
   char* key_half = strtok( line_cpy, "=" );
   if( key_half == NULL ) {
      // ensure that this is a comment or an empty line
      char* key_str = strtok( line_cpy, " \n" );
      if( key_str == NULL ) {
         free( line_cpy );    // empty line
         return 0;
      }
      else if( key_str[0] == COMMENT_KEY ) {
         free( line_cpy );    // comment
         return 0;
      }
      else {
         free( line_cpy );    // bad line
         return -1;
      }
   }
   
   char* value_half = line_cpy + strlen(key_half) + 1;
   
   char* key_str = strtok( key_half, " \t" );
   
   // if this is a comment, then skip it
   if( key_str[0] == COMMENT_KEY ) {
      free( line_cpy );
      return 0;     
   }
   
   // if this is a newline, then skip it
   if( key_str[0] == '\n' || key_str[0] == '\r' ) {
      free( line_cpy );
      return 0;      // just a blank line
   }
   
   // read each value
   vector<char*> val_list;
   char* val_str = strtok( value_half, " \n" );
   
   while( 1 ) {
      if( val_str == NULL )
         // no more values
         break;
      
      // got a value string
      if( val_str[0] != '"' ) {
         // needs to be in quotes!
         free( line_cpy );
         return -3;
      }
      
      // advance beyond the first '"'
      val_str++;
      
      // scan until we find an unescaped "
      int escaped = 0;
      int end = -1;
      for( int i = 0; i < (signed)strlen(val_str); i++ ) {
         if( val_str[i] == '\\' ) {
            escaped = 1;
            continue;
         }
         
         if( escaped ) {
            escaped = 0;
            continue;
         }
         
         if( val_str[i] == '"' ) {
            end = i;
            break;
         }
      }
      
      if( end == -1 ) {
         // the string didn't end in a "
         free( line_cpy );
         return -4;
      }
      
      val_str[end] = 0;
      val_list.push_back( val_str );
      
      // next value
      val_str = strtok( NULL, " \n" );
   }
   
   // get the key
   if( *key == NULL ) {
      *key = (char*)calloc( strlen(key_str) + 1, 1 );
   }
   
   strcpy( *key, key_str );
   
   // get the values
   if( *values == NULL ) {
      *values = (char**)calloc( sizeof(char*) * (val_list.size() + 1), 1 );
   }
   
   for( int i = 0; i < (signed)val_list.size(); i++ ) {
      (*values)[i] = strdup( val_list.at(i) );
   }
   
   free( line_cpy );
   
   return val_list.size();
}


// read an int value 
int md_conf_parse_long( char* value, long* ret, char* buf, int line_cnt ) {
   char *end = NULL;
   long val = strtol( value, &end, 10 );
   if( end[0] != '\0' ) {
      errorf( "WARN: ignoring bad config line %d: %s\n", line_cnt, buf );
      return -1;
   }
   else {
      *ret = val;
   }
   return 0;
}


// read the configuration file and populate a md_syndicate_conf structure
int md_read_conf( char const* conf_path, struct md_syndicate_conf* conf ) {

   FILE* fd = fopen( conf_path, "r" );
   if( fd == NULL ) {
      errorf( "could not read %s\n", conf_path);
      return -1;
   }
   
   memset( conf, 0, sizeof( struct md_syndicate_conf ) );
   
   char buf[4096];    // big enough?
   
   char* eof = NULL;
   int line_cnt = 0;
   
   do {
      memset( buf, 0, sizeof(char) * 4096 );
      eof = fgets( buf, 4096, fd );
      if( eof == NULL )
         break;
         
      line_cnt++;
      
      char* key = NULL;
      char** values = NULL;
      int num_values = md_read_conf_line( buf, &key, &values );
      if( num_values <= 0 ) {
         //dbprintf("read_conf: ignoring malformed line %d\n", line_cnt );
         continue;
      }
      if( key == NULL || values == NULL ) {
         continue;      // comment or empty line
      }
      
      // have key, values.
      // what to do?
      if( strcmp( key, DEFAULT_READ_FRESHNESS_KEY ) == 0 ) {
         // pull time interval
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->default_read_freshness = val;
         }
      }
      
      else if( strcmp( key, DEFAULT_WRITE_FRESHNESS_KEY ) == 0 ) {
         // pull time interval
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->default_write_freshness = val;
         }
      }
      
      else if( strcmp( key, CONNECT_TIMEOUT_KEY ) == 0 ) {
         // read timeout
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->connect_timeout = val;
         }
      }
      
      else if( strcmp( key, MS_USERNAME_KEY ) == 0 ) {
         // metadata server username
         conf->ms_username = strdup( values[0] );
      }
      
      else if( strcmp( key, MS_PASSWORD_KEY ) == 0 ) {
         // metadata server password
         conf->ms_password = strdup( values[0] );
      }
      
      else if( strcmp( key, VIEW_RELOAD_FREQ_KEY ) == 0 ) {
         // view reload frequency
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->view_reload_freq = val;
         }
      }
      
      else if( strcmp( key, VERIFY_PEER_KEY ) == 0 ) {
         // verify peer?
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->verify_peer = (val != 0);
         }
      }
      
      else if( strcmp( key, METADATA_URL_KEY ) == 0 ) {
         // metadata publisher URL
         conf->metadata_url = strdup( values[0] );
      }
      
      else if( strcmp( key, LOGFILE_PATH_KEY ) == 0 ) {
         // logfile path
         conf->logfile_path = strdup( values[0] );
      }
      
      else if( strcmp( key, CDN_PREFIX_KEY ) == 0 ) {
         // cdn prefix
         conf->cdn_prefix = strdup( values[0] );
      }

      else if( strcmp( key, PROXY_URL_KEY ) == 0 ) {
         // proxy URL
         conf->proxy_url = strdup( values[0] );
      }
      
      else if( strcmp( key, GATHER_STATS_KEY ) == 0 ) {
         // gather statistics?
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->gather_stats = (val != 0);
         }
      }

      else if( strcmp( key, NUM_HTTP_THREADS_KEY ) == 0 ) {
         // how big is the HTTP threadpool?
         conf->num_http_threads = (unsigned int)strtol( values[0], NULL, 10 );
      }
      
      else if( strcmp( key, DATA_ROOT_KEY ) == 0 ) {
         // data root
         conf->data_root = strdup( values[0] );
         if( conf->data_root[ strlen(conf->data_root)-1 ] != '/' ) {
            char* tmp = md_prepend( conf->data_root, "/", NULL );
            free( conf->data_root );
            conf->data_root = tmp;
         }
      }

      else if( strcmp( key, SSL_PKEY_KEY ) == 0 ) {
         // server private key
         conf->server_key_path = strdup( values[0] );
      }
      
      else if( strcmp( key, SSL_CERT_KEY ) == 0 ) {
         // server certificate
         conf->server_cert_path = strdup( values[0] );
      }

      else if( strcmp( key, GATEWAY_KEY_KEY ) == 0 ) {
         // user-given public/private key
         conf->gateway_key_path = strdup( values[0] );
      }
      
      else if( strcmp( key, SYNDICATE_PUBKEY_KEY ) == 0 ) {
         // user-given syndicate public key 
         conf->syndicate_pubkey_path = strdup( values[0] );
      }

      else if( strcmp( key, PORTNUM_KEY ) == 0 ) {
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            if( val > 0 && val <= 65534 )
               conf->portnum = val;
            else
               errorf("WARN: invalid port number %ld in line %d\n", val, line_cnt );
         }
      }

      else if( strcmp( key, CONTENT_URL_KEY ) == 0 ) {
         // public content URL
         conf->content_url = strdup( values[0] );
         if( conf->content_url[ strlen(conf->content_url)-1 ] != '/' ) {
            char* tmp = md_prepend( conf->content_url, "/", NULL );
            free( conf->content_url );
            conf->content_url = tmp;
         }
      }
      
      else if( strcmp( key, GATEWAY_NAME_KEY ) == 0 ) {
         // gateway name
         conf->gateway_name = strdup( values[0] );
      }
      
      else if( strcmp( key, DEBUG_KEY ) == 0 ) {
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            md_debug( conf, (int)val );
         }
      }
      
      else if( strcmp( key, LOCAL_STORAGE_DRIVERS_KEY ) == 0 ) {
         conf->local_sd_dir = strdup( values[0] );
      }
      else if( strcmp( key, HTTPD_PORTNUM_KEY ) == 0 ) {
         conf->httpd_portnum = strtol( values[0], NULL, 10 );
      }

      else if( strcmp( key, TRANSFER_TIMEOUT_KEY ) == 0 ) {
         conf->transfer_timeout = strtol( values[0], NULL, 10 );
      }

      else if( strcmp( key, AG_GATEWAY_DRIVER_KEY ) == 0 ) {
         conf->ag_driver = strdup(values[0]);
      }
      
      else if( strcmp( key, AG_BLOCK_SIZE_KEY ) == 0 ) {
         long val = 0;
         int rc = md_conf_parse_long( values[0], &val, buf, line_cnt );
         if( rc == 0 ) {
            conf->ag_block_size = val;
         }
      }

      else {
         errorf( "WARN: unrecognized key '%s'\n", key );
      }
      
      // clean up
      free( key );
      for( int i = 0; i < num_values; i++ ) {
         free( values[i] );
      }
      free(values);
      
   } while( eof != NULL );
   
   fclose( fd );

   return 0;
}


// free all memory associated with a server configuration
int md_free_conf( struct md_syndicate_conf* conf ) {
   void* to_free[] = {
      (void*)conf->metadata_url,
      (void*)conf->logfile_path,
      (void*)conf->content_url,
      (void*)conf->data_root,
      (void*)conf->cdn_prefix,
      (void*)conf->proxy_url,
      (void*)conf->ms_username,
      (void*)conf->ms_password,
      (void*)conf->server_key,
      (void*)conf->server_cert,
      (void*)conf->server_key_path,
      (void*)conf->server_cert_path,
      (void*)conf->gateway_key,
      (void*)conf->gateway_key_path,
      (void*)conf->gateway_name,
      (void*)conf->volume_name,
      (void*)conf->volume_pubkey,
      (void*)conf->syndicate_pubkey,
      (void*)conf->user_pkey,
      (void*)conf
   };

   for( int i = 0; to_free[i] != conf; i++ ) {
      free( to_free[i] );
   }
   
   memset( conf, 0, sizeof(struct md_syndicate_conf) );
      
   return 0;
}



// destroy an md entry
void md_entry_free( struct md_entry* ent ) {
   if( ent->name ) {
      free( ent->name );
      ent->name = NULL;
   }
   if( ent->parent_name ) {
      free( ent->parent_name );
      ent->parent_name = NULL;
   }
}


// destroy a bunch of md_entries
void md_entry_free_all( struct md_entry** ents ) {
   for( int i = 0; ents[i] != NULL; i++ ) {
      md_entry_free( ents[i] );
      free( ents[i] );
   }
}

// duplicate an md_entry.  Make a new one if given NULL
struct md_entry* md_entry_dup( struct md_entry* src ) {
   struct md_entry* ret = (struct md_entry*)calloc( sizeof(struct md_entry), 1 );
   md_entry_dup2( src, ret );
   return ret;
}


// duplicate an md_entry.  Make a new one if given NULL
void md_entry_dup2( struct md_entry* src, struct md_entry* ret ) {
   // copy non-pointers
   memcpy( ret, src, sizeof(md_entry) );

   if( src->name ) {
      ret->name = strdup( src->name );
   }

   if( src->parent_name ) {
      ret->parent_name = strdup( src->parent_name );
   }
}

// concatenate root (a directory path) with path (a relative path)
char* md_fullpath( char const* root, char const* path, char* dest ) {
   char delim = 0;
   int path_off = 0;
   
   int len = strlen(path) + strlen(root) + 2;
   
   if( strlen(root) > 0 ) {
      size_t root_delim_off = strlen(root) - 1;
      if( root[root_delim_off] != '/' && path[0] != '/' ) {
         len++;
         delim = '/';
      }
      else if( root[root_delim_off] == '/' && path[0] == '/' ) {
         path_off = 1;
      }
   }

   if( dest == NULL )
      dest = (char*)calloc( len, 1 );
   
   memset(dest, 0, len);
   
   strcpy( dest, root );
   if( delim != 0 ) {
      dest[strlen(dest)] = '/';
   }
   strcat( dest, path + path_off );
   
   return dest;
}


// write the directory name of a path to dest.
// if a well-formed path is given, then a string ending in a / is returned
char* md_dirname( char const* path, char* dest ) {
   
   if( dest == NULL ) {
      dest = (char*)calloc( strlen(path) + 1, 1 );
   }
   
   // is this root?
   if( strlen(path) == 0 || strcmp( path, "/" ) == 0 ) {
      strcpy( dest, "/" );
      return dest;
   }
   
   int delim_i = strlen(path);
   if( path[delim_i] == '/' ) {
      delim_i--;
   }
   
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' )
         break;
   }
   
   if( delim_i == 0 && path[0] == '/' ) {
      delim_i = 1;
   }
   
   strncpy( dest, path, delim_i );
   dest[delim_i+1] = '\0';
   return dest;
}

// find the depth of a node in a path.
// the depth of / is 0
// the depth of /foo/bar/baz/ is 3
// the depth of /foo/bar/baz is also 3
int md_depth( char const* path ) {
   int i = strlen(path) - 1;
   
   if( i <= 0 )
      return 0;
   
   if( path[i] == '/' )
      i--;
   
   int depth = 0;
   for( ; i >= 0; i-- )
      if( path[i] == '/' )
         depth++;
   
   return depth;
}


// find the integer offset into a path where the directory name begins
// the inex will be at the last '/'
int md_dirname_end( char const* path ) {
   int delim_i = strlen(path) - 1;
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' )
         break;
   }
   
   return delim_i;
}


// write the basename of a path to dest.
char* md_basename( char const* path, char* dest ) {
   int delim_i = strlen(path) - 1;
   if( delim_i <= 0 ) {
      if( dest == NULL )
         dest = strdup("/");
      else
         strcpy(dest, "/");
      return dest;
   }
   if( path[delim_i] == '/' ) {
      // this path ends with '/', so skip over it if it isn't /
      if( delim_i > 0 ) {
         delim_i--;
      }
   }
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' )
         break;
   }
   delim_i++;
   
   if( dest == NULL ) {
      dest = (char*)calloc( strlen(path) - delim_i + 1, 1 );
   }
   else {
      memset( dest, 0, strlen(path) - delim_i + 1 );
   }
   strncpy( dest, path + delim_i, strlen(path) - delim_i );
   return dest;
}


// find the integer offset into a path where the basename begins.
// the index will be right after the '/'
int md_basename_begin( char const* path ) {
   int delim_i = strlen(path) - 1;
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' )
         break;
   }
   
   return delim_i + 1;
}


// prepend a prefix to a string
char* md_prepend( char const* prefix, char const* str, char* output ) {
   if( output == NULL ) {
      output = (char*)calloc( strlen(prefix) + strlen(str) + 1, 1 );
   }
   sprintf(output, "%s%s", prefix, str );
   return output;
}


// hash a path
long md_hash( char const* path ) {
   locale loc;
   const collate<char>& coll = use_facet<collate<char> >(loc);
   return coll.hash( path, path + strlen(path) );
}


// split a path into its components
int md_path_split( char const* path, vector<char*>* result ) {
   char* tmp = NULL;
   char* path_copy = strdup( path );
   char* ptr = path_copy;

   // does the path start with /?
   if( *ptr == '/' ) {
      result->push_back( strdup("/") );
      ptr++;
   }

   // parse through this path
   while( 1 ) {
      char* next_tok = strtok_r( ptr, "/", &tmp );
      ptr = NULL;

      if( next_tok == NULL )
         break;

      result->push_back( strdup(next_tok) );
   }

   free( path_copy );
   return 0;
}

// make sure paths don't end in /, unless they're root.
void md_sanitize_path( char* path ) {
   if( strcmp( path, "/" ) != 0 ) {
      size_t len = strlen(path);
      if( len > 0 ) {
         if( path[len-1] == '/' ) {
            path[len-1] = '\0';
         }
      }
   }
}


// given a URL, is it hosted locally?
bool md_is_locally_hosted( struct md_syndicate_conf* conf, char const* url ) {
   char* url_host = md_url_hostname( url );
   int url_port = md_portnum_from_url( url );

   char* local_host = md_url_hostname( conf->content_url );
   int local_port = conf->portnum;

   bool ret = false;

   if( strcmp( local_host, url_host ) == 0 ) {
      if( (local_port <= 0 && url_port <= 0) || local_port == url_port ) {
         ret = true;
      }
   }

   free( local_host );
   free( url_host );
   return ret;
}

// start a thread
pthread_t md_start_thread( void* (*thread_func)(void*), void* arg, bool detach ) {

   // start up a thread to listen for connections
   pthread_attr_t attrs;
   pthread_t listen_thread;
   int rc;
   
   rc = pthread_attr_init( &attrs );
   if( rc != 0 ) {
      errorf( "pthread_attr_init rc = %d\n", rc);
      return (pthread_t)(-1);   // problem
   }

   if( detach ) {
      rc = pthread_attr_setdetachstate( &attrs, PTHREAD_CREATE_DETACHED );    // make a detached thread--we won't join with it
      if( rc != 0 ) {
         errorf( "pthread_attr_setdetachstate rc = %d\n", rc );
         return (pthread_t)(-1);
      }
   }
   
   rc = pthread_create( &listen_thread, &attrs, thread_func, arg );
   if( rc != 0 ) {
      errorf( "pthread_create rc = %d\n", rc );
      return (pthread_t)(-1);
   }
   
   return listen_thread;
}

// download data to a buffer
size_t md_default_get_callback_ram(void *stream, size_t size, size_t count, void *user_data) {
   struct md_download_buf* dlbuf = (struct md_download_buf*)user_data;
   
   size_t realsize = size * count;
   
   int new_size = realsize + dlbuf->len;
   
   if( dlbuf->data_len > 0 ) {
      // have an upper bound on how much data to copy
      if( dlbuf->len + realsize > (size_t)dlbuf->data_len ) {
         realsize = dlbuf->data_len - dlbuf->len;
      }
      dbprintf("receive %zu to offset %zd of %zd\n", realsize, dlbuf->len, dlbuf->data_len);
      memcpy( dlbuf->data + dlbuf->len, stream, realsize );
      dlbuf->len += realsize;
      return realsize;
   }
   else {
      // expand   
      char* new_buf = (char*)realloc( dlbuf->data, new_size );
      if( new_buf == NULL ) {
         free( dlbuf->data );
         dlbuf->data = NULL;
         dbprintf("out of memory for %p\n", user_data );
         return 0;      // out of memory
      }

      else {
         dlbuf->data = new_buf;
         memcpy( dlbuf->data + dlbuf->len, stream, realsize );
         dlbuf->len = new_size;
         return realsize;
      }
   }
   
   return 0;
}

// download data to a response buffer
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   response_buffer_t* rb = (response_buffer_t*)user_data;

   size_t realsize = size * count;
   char* buf = CALLOC_LIST( char, realsize );
   memcpy( buf, stream, realsize );
   
   rb->push_back( buffer_segment_t( buf, realsize ) );

   return realsize;
}


// download to a bound response buffer
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data ) {
   struct md_bound_response_buffer* brb = (struct md_bound_response_buffer*)user_data;
   
   //dbprintf("size = %zu, count = %zu, max_size = %ld, size = %ld\n", size, count, brb->max_size, brb->size );
   
   off_t realsize = size * count;
   if( brb->max_size >= 0 && (off_t)(brb->size + realsize) > brb->max_size ) {
      realsize = brb->max_size - brb->size;
   }
   
   char* buf = CALLOC_LIST( char, realsize );
   memcpy( buf, stream, realsize );
   
   brb->rb->push_back( buffer_segment_t( buf, realsize ) );
   brb->size += realsize;
   
   return realsize;
}


// download via a continuation
size_t md_get_callback_driver_continuation( void* stream, size_t size, size_t count, void* user_data ) {
   struct md_download_continuation* cont = (struct md_download_continuation*)user_data;
   
   size_t processed = md_get_callback_bound_response_buffer( stream, size, count, (void*)&cont->brb );
   if( processed != size * count ) {
      // not all processed
      return processed;
   }
   
   // call the continuation 
   if( cont->cont ) {
      processed = (*cont->cont)( stream, size, count, cont->user_cls );
   }
   
   return processed;
}


// download data to disk
size_t md_default_get_callback_disk(void *stream, size_t size, size_t count, void* user_data) {
   int* fd = (int*)user_data;
   
   ssize_t num_written = write( *fd, stream, size*count );
   if( num_written < 0 )
      num_written = 0;
   
   return (size_t)num_written;
}


// no signals?
int md_signals( int use_signals ) {
   int tmp = _signals;
   _signals = use_signals;
   return tmp;
}

// download straight from an existing curl handle
off_t md_download_file2( CURL* curl_h, char** buf, off_t max_len ) {
   struct md_download_buf dlbuf;
   dlbuf.len = 0;
   
   if( max_len > 0 ) {
      dbprintf("Download max %zd\n", max_len );
      dlbuf.data = CALLOC_LIST( char, max_len );
      dlbuf.data_len = max_len;
   }
   else {
      dlbuf.data = CALLOC_LIST( char, 1 );
      dlbuf.data_len = -1;
   }

   if( dlbuf.data == NULL ) {
      return -1;
   }

   curl_easy_setopt( curl_h, CURLOPT_WRITEDATA, (void*)&dlbuf );
   curl_easy_setopt( curl_h, CURLOPT_WRITEFUNCTION, md_default_get_callback_ram );
   int rc = curl_easy_perform( curl_h );

   if( rc != 0 ) {
      dbprintf("curl_easy_perform rc = %d\n", rc);
      free( dlbuf.data );
      dlbuf.data = NULL;
      return -abs(rc);
   }

   *buf = dlbuf.data;
   return dlbuf.len;
}

off_t md_download_file( CURL* curl_h, char** buf ) {
   return md_download_file2( curl_h, buf, -1 );
}

// initialize a download continuation 
int md_download_continuation_init( struct md_download_continuation* cont, off_t max_len, md_download_continuation_func cont_func, void* cls ) {
   memset( cont, 0, sizeof(struct md_download_continuation) );
   
   cont->brb.max_size = max_len;
   cont->brb.size = 0;
   cont->brb.rb = new response_buffer_t();
   
   cont->user_cls = cls;
   cont->cont = cont_func;
   
   return 0;
}

// free a continuation 
void md_download_continuation_free( struct md_download_continuation* cont ) {
   if( cont->brb.rb ) {
      response_buffer_free( cont->brb.rb );
      delete cont->brb.rb;
      cont->brb.rb = NULL;
   }
}


// download data using a continuation
// the curl handle must already have been initialized.
// return 0 on success
// return negative on irrecoverable error
int md_download_with_continuation( CURL* curl, char** bits, off_t* ret_len, int* _status_code, struct md_download_continuation* cont ) {
   
   off_t len = 0;
   long status_code = 0;
   int rc = 0;

   char* tmpbuf = NULL;
   
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)cont );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_driver_continuation );
   
   rc = curl_easy_perform( curl );
   
   if( rc != 0 ) {
      long err = 0;
      
      // get the errno
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      // get the effective URL 
      char* url = NULL;
      curl_easy_getinfo( curl, CURLINFO_EFFECTIVE_URL, &url );
      
      errorf("curl_easy_perform(%p, %s) rc = %d, err = %ld\n", curl, url, rc, err );
      
      rc = -EREMOTEIO;
      *_status_code = 0;
   }
   else {
      
      curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &status_code );

      *_status_code = status_code;
      
      if( status_code == 200 ) {
         // everything was okay
         
         tmpbuf = response_buffer_to_string( cont->brb.rb );
         len = response_buffer_size( cont->brb.rb );
         
         *bits = tmpbuf;
         *ret_len = len;
      }
      else {
            
         // get the effective URL 
         char* url = NULL;
         curl_easy_getinfo( curl, CURLINFO_EFFECTIVE_URL, &url );
         
         // HTTP protocol error
         errorf("curl_easy_perform(%p, %s) HTTP status = %ld\n", curl, url, status_code );
         
         *bits = NULL;
         *ret_len = 0;
         rc = -EREMOTEIO;
      }
   }
   
   return rc;
}

// download data from one or more CDNs.
// cache_func will initialize the curl handle.
// return 0 on success.
// return negative on error.
// fill in the HTTP satus code
int md_download_from_caches( struct md_closure* closure, CURL* curl, char const* base_url, char** bits, off_t* ret_len,
                             off_t max_len, int* status_code, md_cache_connector_func cache_func, void* cache_func_cls ) {
   int rc = 0;

   if( cache_func ) {
      rc = (*cache_func)( closure, curl, base_url, cache_func_cls );
      if( rc != 0 ) {
         errorf("cache connector function rc = %d\n", rc );
         return rc;
      }
   }
   else {
      // need a cache connector...
      return -EINVAL;
   }
   
   // set up our continuation...
   struct md_download_continuation download_cont;
   md_download_continuation_init( &download_cont, max_len, NULL, NULL );
   
   rc = md_download_with_continuation( curl, bits, ret_len, status_code, &download_cont );

   md_download_continuation_free( &download_cont );
   
   return rc;
}


// download data from one or more CDNs, and then fall back to a direct download if that fails.
// return 0 on success.
// return negative on error.
// fill in the HTTP status code 
int md_download( struct md_syndicate_conf* conf, struct md_closure* closure, CURL* curl, char const* base_url, char** bits, off_t* ret_len, off_t max_len, int* status_code, md_cache_connector_func cache_func, void* cache_func_cls ) {
   int rc = 0;
   
   if( cache_func ) {
      rc = md_download_from_caches( closure, curl, base_url, bits, ret_len, max_len, status_code, cache_func, cache_func_cls );
      if( rc == 0 ) {
         return 0;
      }
      else {
         errorf("WARN: md_download_from_caches(%s) rc = %d\n", base_url, rc);
      }
   }
   
   // download directly if we get here...
   md_init_curl_handle( conf, curl, base_url, conf->connect_timeout );
   
   struct md_download_continuation download_cont;
   md_download_continuation_init( &download_cont, max_len, NULL, NULL );
   
   rc = md_download_with_continuation( curl, bits, ret_len, status_code, &download_cont );

   md_download_continuation_free( &download_cont );
   
   if( rc != 0 ) {
      errorf("md_download_with_continuation(%s) rc = %d, HTTP status = %d\n", base_url, rc, *status_code );
   }
   
   return rc;
}


// translate an HTTP status code into the approprate error code.
// return the code if no error could be determined.
static int md_HTTP_status_code_to_error_code( int status_code ) {
   if( status_code == GATEWAY_HTTP_TRYAGAIN )
      return -EAGAIN;
   
   if( status_code == GATEWAY_HTTP_EOF )
      return 0;
   
   return status_code;
}

// download a manifest and parse it.
// Do not attempt to check it for errors, or verify its authenticity
int md_download_manifest( struct md_syndicate_conf* conf, struct md_closure* closure, CURL* curl, char const* manifest_url, Serialization::ManifestMsg* mmsg,
                          md_cache_connector_func cache_func, void* cache_func_cls,
                          md_manifest_processor_func manifest_func, void* manifest_func_cls ) {

   char* manifest_data = NULL;
   int status_code = 0;
   off_t manifest_data_len = 0;
   int rc = 0;

   rc = md_download( conf, closure, curl, manifest_url, &manifest_data, &manifest_data_len, SYNDICATE_MAX_MANIFEST_LEN, &status_code, cache_func, cache_func_cls );
   
   if( rc != 0 ) {
      errorf( "md_download(%s) rc = %d\n", manifest_url, rc );
      return rc;
   }
   
   if( status_code != 200 ) {
      // bad HTTP status
      errorf( "md_download(%s) HTTP status %d\n", manifest_url, status_code );
      
      int err = md_HTTP_status_code_to_error_code( status_code );
      if( err == 0 || err == status_code )
         return -EREMOTEIO;
      else
         return err;
   }
   
   // process the manifest...
   if( manifest_func ) {
      char* processed_manifest_data = NULL;
      size_t processed_manifest_data_len = 0;
      
      rc = (*manifest_func)( closure, manifest_data, manifest_data_len, &processed_manifest_data, &processed_manifest_data_len, manifest_func_cls );
      if( rc != 0 ) {
         errorf("manifest_func rc = %d\n", rc );
         
         free( manifest_data );
         return rc;
      }
      
      if( processed_manifest_data != NULL && processed_manifest_data != manifest_data ) {
         // driver transformed the data
         free( manifest_data );
      
         manifest_data = processed_manifest_data;
         manifest_data_len = processed_manifest_data_len;
      }
   }

   rc = md_parse< Serialization::ManifestMsg >( mmsg, manifest_data, manifest_data_len );
   if( rc != 0 ) {
      errorf("md_parse rc = %d\n", rc );
      return -ENODATA;
   }
   
   if( manifest_data )
      free( manifest_data );

   return rc;
}


// download a block 
off_t md_download_block( struct md_syndicate_conf* conf, struct md_closure* closure, CURL* curl, char const* block_url, char** block_bits, size_t block_len, md_cache_connector_func cache_func, void* cache_func_cls ) {

   ssize_t nr = 0;
   char* block_buf = NULL;
   int status_code = 0;
   
   dbprintf("fetch at most %zu bytes from '%s'\n", block_len, block_url );
   
   int ret = md_download( conf, closure, curl, block_url, &block_buf, &nr, block_len, &status_code, cache_func, cache_func_cls );
   
   if( ret != 0 ) {
      errorf("md_download(%s) ret = %d\n", block_url, ret );
      return ret;
   }
   
   if( status_code != 200 ) {
      errorf("md_download(%s) HTTP status %d\n", block_url, status_code );
      if( block_buf ) {
         free( block_buf );
      }
      int err = md_HTTP_status_code_to_error_code( status_code );
      if( err == status_code ) {
         return -EREMOTEIO;
      }
      else {
         return -abs(err);
      }
   }
   
   // got back data!
   *block_bits = block_buf;
   
   return nr;
}


// parse a query string into a list of CGI arguments
// NOTE: this modifies args_str
char** md_parse_cgi_args( char* args_str ) {
   int num_args = 1;
   for( unsigned int i = 0; i < strlen(args_str); i++ ) {
      if( args_str[i] == '&' )
         num_args++;
   }

   char** cgi_args = (char**)calloc( sizeof(char*) * (num_args+1), 1 );
   int off = 0;
   for( int i = 0; i < num_args - 1; i++ ) {
      cgi_args[i] = args_str + off;
      
      unsigned int j;
      for( j = off+1; j < strlen(args_str); j++ ) {
         if( args_str[j] == '&' )
            break;
      }
      
      args_str[j] = '\0';
      off = j+1;
   }
   cgi_args[ num_args - 1 ] = args_str + off;
   return cgi_args;
}


// get the scheme out of a URL
char* md_url_scheme( char const* _url ) {
   char* url = strdup( _url );

   // find ://
   char* host_port = strstr(url, "://" );
   if( host_port == NULL ) {
      free( url );
      return NULL;
   }
   else {
      // careful...pointer arithmetic 
      int len = 0;
      char* tmp = url;
      while( tmp != host_port ) {
         tmp++;
         len++;
      }

      char* scheme = CALLOC_LIST( char, len + 1 );
      strncpy( scheme, _url, len );

      free( url );
      return scheme;
   }
      
}

// get the hostname out of a URL
char* md_url_hostname( char const* _url ) {
   char* url = strdup( _url );
   
   // find :// separator
   char* host_port = strstr( url, "://" );
   if( host_port == NULL )
      host_port = url;
   else
      host_port += 3;
   
   // consume the string to find / or :
   char* tmp = NULL;
   char* ret = strtok_r( host_port, ":/", &tmp );
   if( ret == NULL )
      // no : or /
      ret = host_port;
   
   ret = strdup( ret );
   free( url );
   return ret;
}

// locate the path from the url
char* md_path_from_url( char const* url ) {
   // find the ://, if given
   char* off = strstr( (char*)url, "://" );
   if( !off ) {
      off = (char*)url;
   }
   else {
      off += 3;         // advance to hostname
   }
   
   // find the next /
   off = strstr( off, "/" );
   char* ret = NULL;
   if( !off ) {
      // just a URL; no '/''s
      ret = strdup( "/" );
   }
   else {
      ret = strdup( off );
   }
   
   return ret;
}


// get the FS path from a URL
char* md_fs_path_from_url( char const* url ) {
   char* ret = NULL;

   // extract the prefixes
   char const* prefixes[] = {
      SYNDICATE_DATA_PREFIX,
      NULL
   };

   char const* url_path = url;

   for( int i = 0; prefixes[i] != NULL; i++ ) {
      char const* start = strstr( url_path, prefixes[i] );
      if( start != NULL ) {
         url_path = start + strlen(prefixes[i]);

         if( url_path[0] != '/' )
            url_path--;

         break;
      }
   }

   // if no prefies, then just advance to the path
   if( url_path == url ) {
      url_path = md_path_from_url( url );
   }

   ret = strdup( url_path );
   md_clear_version( ret );

   return ret;
}

// strip the path from he url 
char* md_url_strip_path( char const* url ) {
   char* ret = strdup( url );
   
   // find the ://, if given
   char* off = strstr( (char*)ret, "://" );
   if( !off ) {
      off = (char*)ret;
   }
   else {
      off += 3;         // advance to hostname
   }

   // find the next /
   off = strstr( off, "/" );
   if( off ) {
      *off = '\0';
   }

   return ret;
}

// locate the port number from the url
int md_portnum_from_url( char const* url ) {
   // find the ://, if given
   char* off = strstr( (char*)url, "://" );
   if( !off ) {
      off = (char*)url;
   }
   else {
      off += 3;         // advance to hostname
   }
   
   // find the next :
   off = strstr( off, ":" );
   if( !off ) {
      // no port number given
      return -1;
   }
   else {
      off++;
      long ret = strtol( off, NULL, 10 );
      return (int)ret;
   }
}

// strip the protocol from a url
char* md_strip_protocol( char const* url ) {
   char* off = strstr( (char*)url, "://" );
   if( !off ) {
      return strdup( url );
   }
   else {
      return strdup( off + 3 );
   }
}


// flatten a path.  That is, remove /./, /[/]*, etc, but don't resolve ..
char* md_flatten_path( char const* path ) {
   size_t len = strlen(path);
   char* ret = CALLOC_LIST( char, len + 1 );
 
   unsigned int i = 0;
   int off = 0;
   
   while( i < len ) {
      
      // case something/[/]*/something
      if( path[i] == '/' ) {
         if( off == 0 || (off > 0 && ret[off-1] != '/') ) {
            ret[off] = path[i];
            off++;
         }
         
         i++;
         while( i < len && path[i] == '/' ) {
            i++;
         }
      }
      else if( path[i] == '.' ) {
         // case "./somethong"
         if( off == 0 && i + 1 < len && path[i+1] == '/' ) {
            i++;
         }
         // case "something/./something"
         else if( off > 0 && ret[off-1] == '/' && i + 1 < len && path[i+1] == '/' ) {
            i+=2;
         }
         // case "something/."
         else if( off > 0 && ret[off-1] == '/' && i + 1 == len ) {
            i++;
         }
         else {
            ret[off] = path[i];
            i++;
            off++;
         }
      }
      else {
         ret[off] = path[i];
         i++;
         off++;
      }
   }
   
   return ret;
}


// convert the URL into the CDN-ified form
char* md_cdn_url( char const* cdn_prefix, char const* url ) {
   // fix the URL so it is prefixed by the hostname and CDN, instead of being file://path or http://hostname/path
   char* host_path = md_strip_protocol( url );
   if( cdn_prefix == NULL || strlen(cdn_prefix) == 0 ) {
      // no prefix given
      cdn_prefix = (char*)"http://";
   }
   char* update_url = md_fullpath( cdn_prefix, host_path, NULL );
   free( host_path );
   return update_url;
}


// split a url into the url+path and query string
int md_split_url_qs( char const* url, char** url_and_path, char** qs ) {
   if( strstr( url, "?" ) != NULL ) {
      char* url2 = strdup( url );
      char* qs_start = strstr( url2, "?" );
      *qs_start = '\0';

      *qs = strdup( qs_start + 1 );
      *url_and_path = url2;
      return 0;
   }
   else {
      return -EINVAL;
   }
}


// get the offset at which the value starts in a header
off_t md_header_value_offset( char* header_buf, size_t header_len, char const* header_name ) {

   if( strlen(header_name) >= header_len )
      return -1;      // header is too short

   if( strncasecmp(header_buf, header_name, MIN( header_len, strlen(header_name) ) ) != 0 )
      return -1;      // not found

   size_t off = strlen(header_name);

   // find :
   while( off < header_len ) {
      if( header_buf[off] == ':' )
         break;

      off++;
   }

   if( off == header_len )
      return -1;      // no value

   off++;

   // find value
   while( off < header_len ) {
      if( header_buf[off] != ' ' )
         break;

      off++;
   }

   if( off == header_len )
      return -1;      // no value

   return off;
}

// parse one value in a header
uint64_t md_parse_header_uint64( char* hdr, off_t offset, size_t size ) {
   char* value = hdr + offset;
   size_t value_len = size - offset;

   char* value_str = CALLOC_LIST( char, value_len + 1 );
   
   strncpy( value_str, value, value_len );

   uint64_t data = (uint64_t)strtoll( value_str, NULL, 10 );

   free( value_str );
   
   return data;
}

// read a csv of values
uint64_t* md_parse_header_uint64v( char* hdr, off_t offset, size_t size, size_t* ret_len ) {
   char* value = hdr + offset;
   size_t value_len = size - offset;

   char* value_str = (char*)alloca( value_len + 1 );
   strcpy( value_str, value );

   // how many commas?
   int num_values = 1;
   for( size_t i = offset; i < size; i++ ) {
      if( hdr[i] == ',' )
         num_values++;
   }
   
   char* tmp = value_str;
   char* tmp2 = NULL;
   
   uint64_t* ret = CALLOC_LIST( uint64_t, num_values );
   int i = 0;
   
   while( 1 ) {
      char* tok = strtok_r( tmp, ", \r\n", &tmp2 );
      if( tok == NULL )
         break;

      tmp = NULL;

      uint64_t data = (uint64_t)strtoll( value_str, NULL, 10 );
      ret[i] = data;
      i++;
   }

   *ret_len = num_values;
   return ret;
}




// Read the path version from a given path.
// The path version is the number attached to the end of the path by a period.
// returns a nonnegative number on success.
// returns negative on error.
int64_t md_path_version( char const* path ) {
   // find the last .
   int i;
   bool valid = true;
   for( i = strlen(path)-1; i >= 0; i-- ) {
      if( path[i] == '.' )
         break;
      
      if( path[i] < '0' || path[i] > '9' ) {
         valid = false;
         break;
      }
   }
   if( i <= 0 )
      return (int64_t)(-1);     // no version can be found
   
   if( !valid )
      return (int64_t)(-2);     // no version in the name
   
   char *end;
   int64_t version = strtoll( path + i + 1, &end, 10 );
   if( version == 0 && *end != '\0' )
      return (int64_t)(-3);     // could not parse the version

   return version;
}

// Get the offset into a path where the version begins.
// Returns nonnegative on success.
// returns negative on error.
int md_path_version_offset( char const* path ) {
   int i;
   bool valid = true;
   for( i = strlen(path)-1; i >= 0; i-- ) {
      if( path[i] == '.' )
         break;
      if( path[i] < '0' || path[i] > '9' ) {
         valid = false;
         break;
      }
   }
   
   if( !valid ) {
      return -2;
   }
   
   if( i <= 0 ) {
      return -1;
   }
   
   char *end;
   long version = strtol( path + i + 1, &end, 10 );
   if( version == 0 && *end != '\0' )
      return -3;     // could not parse the version
   
   return i;  
}

// given two paths, determine if one is the versioned form of the other
bool md_is_versioned_form( char const* vanilla_path, char const* versioned_path ) {
   // if this isn't a versioned path, then no
   int version_offset = md_path_version_offset( versioned_path );
   if( version_offset <= 0 )
      return false;
   
   if( strlen(vanilla_path) > (unsigned)version_offset )
      return false;
   
   if( strncmp( vanilla_path, versioned_path, version_offset ) != 0 )
      return false;
   
   return true;
}



// clear the version of a path
char* md_clear_version( char* path ) {
   int off = md_path_version_offset( path );
   if( off > 0 ) {
      path[off] = '\0';
   }
   return path;
}

// serialize updates

// iterator for lists
struct md_metadata_update_list_data {
   uint64_t next;
   struct md_update** updates;
};

static struct md_update* md_metadata_update_list_iterator( void* arg ) {
   struct md_metadata_update_list_data* itrdata = (struct md_metadata_update_list_data*)arg;

   struct md_update* ret = itrdata->updates[ itrdata->next ];
   itrdata->next++;
   return ret;
}

ssize_t md_metadata_update_text( struct md_syndicate_conf* conf, char** buf, struct md_update** updates ) {
   struct md_metadata_update_list_data itrdata;
   itrdata.updates = updates;
   itrdata.next = 0;

   return md_metadata_update_text3( conf, buf, md_metadata_update_list_iterator, &itrdata );
}

// iterator for vectors
struct md_metadata_update_vector_iterator_data {
   uint64_t next;
   vector<struct md_update>* updates;
};

static struct md_update* md_metadata_update_vector_iterator( void* arg ) {
   struct md_metadata_update_vector_iterator_data* itrdata = (struct md_metadata_update_vector_iterator_data*)arg;

   if( itrdata->next >= itrdata->updates->size() )
      return NULL;

   struct md_update* up = &itrdata->updates->at( itrdata->next );
   itrdata->next++;
   return up;
}

ssize_t md_metadata_update_text2( struct md_syndicate_conf* conf, char** buf, vector<struct md_update>* updates ) {
   struct md_metadata_update_vector_iterator_data itrdata;
   itrdata.updates = updates;
   itrdata.next = 0;

   return md_metadata_update_text3( conf, buf, md_metadata_update_vector_iterator, &itrdata );
}

// convert an md_entry to an ms_entry
int md_entry_to_ms_entry( ms::ms_entry* msent, struct md_entry* ent ) {

   if( ent->parent_id != (uint64_t)(-1) )
      msent->set_parent_id( ent->parent_id );

   if( ent->parent_name != NULL )
      msent->set_parent_name( string(ent->parent_name) );


   msent->set_file_id( ent->file_id );
   msent->set_type( ent->type == MD_ENTRY_FILE ? ms::ms_entry::MS_ENTRY_TYPE_FILE : ms::ms_entry::MS_ENTRY_TYPE_DIR );
   msent->set_owner( ent->owner );
   msent->set_coordinator( ent->coordinator );
   msent->set_volume( ent->volume );
   msent->set_mode( ent->mode );
   msent->set_ctime_sec( ent->ctime_sec );
   msent->set_ctime_nsec( ent->ctime_nsec );
   msent->set_mtime_sec( ent->mtime_sec );
   msent->set_mtime_nsec( ent->mtime_nsec );
   msent->set_version( ent->version );
   msent->set_size( ent->size );
   msent->set_max_read_freshness( ent->max_read_freshness );
   msent->set_max_write_freshness( ent->max_write_freshness );
   msent->set_name( string( ent->name ) );
   msent->set_write_nonce( ent->write_nonce );
   msent->set_xattr_nonce( ent->xattr_nonce );
   return 0;
}


// convert ms_entry to md_entry
int ms_entry_to_md_entry( const ms::ms_entry& msent, struct md_entry* ent ) {
   memset( ent, 0, sizeof(struct md_entry) );

   ent->type = msent.type() == ms::ms_entry::MS_ENTRY_TYPE_FILE ? MD_ENTRY_FILE : MD_ENTRY_DIR;
   ent->file_id = msent.file_id();
   ent->owner = msent.owner();
   ent->coordinator = msent.coordinator();
   ent->volume = msent.volume();
   ent->mode = msent.mode();
   ent->mtime_sec = msent.mtime_sec();
   ent->mtime_nsec = msent.mtime_nsec();
   ent->ctime_sec = msent.ctime_sec();
   ent->ctime_nsec = msent.ctime_nsec();
   ent->max_read_freshness = (uint64_t)msent.max_read_freshness();
   ent->max_write_freshness = (uint64_t)msent.max_write_freshness();
   ent->version = msent.version();
   ent->size = msent.size();
   ent->name = strdup( msent.name().c_str() );
   ent->write_nonce = msent.write_nonce();
   ent->xattr_nonce = msent.xattr_nonce();
   
   if( msent.has_parent_id() )
      ent->parent_id = msent.parent_id();
   else
      ent->parent_id = -1;

   if( msent.has_parent_name() )
      ent->parent_name = strdup( msent.parent_name().c_str() );
   else
      ent->parent_name = NULL;
   
   return 0;
}



// iterate through a set of updates and serialize them
ssize_t md_metadata_update_text3( struct md_syndicate_conf* conf, char** buf, struct md_update* (*iterator)( void* ), void* arg ) {

   ms::ms_updates updates;

   while( 1 ) {
      struct md_update* update = (*iterator)( arg );
      if( update == NULL )
         break;

      ms::ms_update* ms_up = updates.add_updates();

      ms_up->set_type( update->op );

      ms::ms_entry* ms_ent = ms_up->mutable_entry();

      md_entry_to_ms_entry( ms_ent, &update->ent );
   }

   string text;
   
   bool valid = updates.SerializeToString( &text );
   if( !valid ) {
      return -1;
   }

   *buf = CALLOC_LIST( char, text.size() + 1 );
   memcpy( *buf, text.data(), text.size() );

   return (ssize_t)text.size();
}

// default callback function to be used when uploading to a server
size_t md_default_upload_callback(void *ptr, size_t size, size_t nmemb, void *userp) {
   size_t realsize = size * nmemb;
   struct md_upload_buf* post = (struct md_upload_buf*)userp;
   size_t writesize = min( post->len - post->offset, (int)realsize );
   
   if( post->offset >= post->len )
      return 0;
   
   memcpy( ptr, post->text + post->offset, writesize );
   post->offset += realsize;
   
   if( post->offset >= post->len ) {
      return writesize;
   }
   else {
      return realsize;
   }  
}


// free a metadata update
void md_update_free( struct md_update* update ) {
   md_entry_free( &update->ent );
   
   if( update->xattr_name )
      free( update->xattr_name );
      
   if( update->xattr_value )
      free( update->xattr_value );

   memset( update, 0, sizeof(struct md_update) );
}


// duplicate an update
void md_update_dup2( struct md_update* src, struct md_update* dest ) {
   dest->op = src->op;
   dest->flags = src->flags;
   dest->error = src->error;
   
   if( src->xattr_name ) {
      dest->xattr_name = strdup( src->xattr_name );
   }
   else {
      dest->xattr_name = NULL;
   }
   
   if( src->xattr_value ) {
      dest->xattr_value = CALLOC_LIST( char, src->xattr_value_len );
      memcpy( dest->xattr_value, src->xattr_value, src->xattr_value_len );
      dest->xattr_value_len = src->xattr_value_len;
   }
   else {
      dest->xattr_value = NULL;
   }
   md_entry_dup2( &src->ent, &dest->ent );
}


// free a download buffer
void md_free_download_buf( struct md_download_buf* buf ) {
   if( buf->data ) {
      free( buf->data );
      buf->data = NULL;
   }
   buf->len = 0;
}


// initialze a curl handle
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl_h, char const* url, time_t query_timeout ) {
   md_init_curl_handle2( curl_h, url, query_timeout, conf->verify_peer );
}

// initialze a curl handle
void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer ) {
   curl_easy_setopt( curl_h, CURLOPT_NOPROGRESS, 1L );   // no progress bar
   curl_easy_setopt( curl_h, CURLOPT_USERAGENT, "Syndicate-Gateway/1.0");
   curl_easy_setopt( curl_h, CURLOPT_URL, url );
   curl_easy_setopt( curl_h, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl_h, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( curl_h, CURLOPT_CONNECTTIMEOUT, query_timeout );
   curl_easy_setopt( curl_h, CURLOPT_FILETIME, 1L );
   
   if( url != NULL && strncasecmp( url, "https", 5 ) == 0 ) {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_ALL );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, ssl_verify_peer ? 1L : 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 2L );
   }
   else {
      curl_easy_setopt( curl_h, CURLOPT_USE_SSL, CURLUSESSL_NONE );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, 0L );
      curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 0L );
   }
   
   //curl_easy_setopt( curl_h, CURLOPT_VERBOSE, 1L );
}

// flatten a response buffer into a byte string
char* response_buffer_to_string( response_buffer_t* rb ) {
   size_t total_len = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      total_len += (*rb)[i].second;
   }

   char* msg_buf = CALLOC_LIST( char, total_len );
   size_t offset = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      memcpy( msg_buf + offset, (*rb)[i].first, (*rb)[i].second );
      offset += (*rb)[i].second;
   }

   return msg_buf;
}

// free a response buffer
void response_buffer_free( response_buffer_t* rb ) {
   if( rb == NULL )
      return;
      
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      if( rb->at(i).first != NULL ) {
         free( rb->at(i).first );
         rb->at(i).first = NULL;
      }
      rb->at(i).second = 0;
   }
   rb->clear();
}

// size of a response buffer
off_t response_buffer_size( response_buffer_t* rb ) {
   off_t ret = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      ret += rb->at(i).second;
   }
   return ret;
}


static bool _already_inited = false;

// basic Syndicate initialization
int md_init_begin( struct md_syndicate_conf* conf,
                   char const* ms_url,
                   char const* volume_name,
                   char const* gateway_name,
                   char const* oid_username,
                   char const* oid_password,
                   char const* user_pkey_pem,
                   char const* gateway_key_path,
                   char const* tls_pkey_file,
                   char const* tls_cert_file,
                   char const* syndicate_pubkey_path
                 ) {
   
 
   int rc = 0;
   
   if( !_already_inited ) {
   #ifndef _SYNDICATE_NACL_
      // before we load anything, disable core dumps (i.e. to keep private keys from leaking)
      
      struct rlimit rlim;
      getrlimit( RLIMIT_CORE, &rlim );
      rlim.rlim_max = 0;
      rlim.rlim_cur = 0;
      
      rc = setrlimit( RLIMIT_CORE, &rlim );
      if( rc != 0 ) {
         rc = -errno;
         errorf("Failed to disable core dumps, rc = %d\n", rc );
         return rc;
      }
   #endif
      
      // need a Volume name
      if( volume_name == NULL ) {
         errorf("%s", "ERR: missing Volume name\n");
         return -EINVAL;
      }

      rc = util_init();
      if( rc != 0 ) {
         errorf("util_init rc = %d\n", rc );
         return rc;
      }
      
      _already_inited = true;
   }
   
   // populate the config
   MD_SYNDICATE_CONF_OPT( *conf, volume_name, volume_name );
   MD_SYNDICATE_CONF_OPT( *conf, metadata_url, ms_url );
   MD_SYNDICATE_CONF_OPT( *conf, ms_username, oid_username );
   MD_SYNDICATE_CONF_OPT( *conf, ms_password, oid_password );
   MD_SYNDICATE_CONF_OPT( *conf, gateway_name, gateway_name );
   MD_SYNDICATE_CONF_OPT( *conf, server_cert_path, tls_cert_file );
   MD_SYNDICATE_CONF_OPT( *conf, server_key_path, tls_pkey_file );
   MD_SYNDICATE_CONF_OPT( *conf, gateway_key_path, gateway_key_path );
   MD_SYNDICATE_CONF_OPT( *conf, syndicate_pubkey_path, syndicate_pubkey_path );
   MD_SYNDICATE_CONF_OPT( *conf, user_pkey, user_pkey_pem );
   
   if( user_pkey_pem != NULL ) {
      conf->user_pkey_len = strlen(user_pkey_pem);
   }
   
   return rc;
}


// finish initialization
int md_init_finish( struct md_syndicate_conf* conf, struct ms_client* client, char const* key_password ) {
   
   int rc = 0;
   
   // validate the config
   rc = md_check_conf( conf );
   if( rc != 0 ) {
      errorf("ERR: md_check_conf rc = %d\n", rc );
      return rc;
   }
   
   // setup the client
   rc = ms_client_init( client, conf->gateway_type, conf );
   if( rc != 0 ) {
      errorf("ms_client_init rc = %d\n", rc );
      return rc;
   }
   
   // attempt public-key authentication 
   if( conf->user_pkey != NULL ) {
      rc = ms_client_public_key_gateway_register( client, conf->gateway_name, conf->ms_username, conf->user_pkey, conf->volume_pubkey, key_password );
      if( rc != 0 ) {
         errorf("ms_client_public_key_register rc = %d\n", rc );
         
         ms_client_destroy( client );
         
         return rc;
      }
   }
   
   // attempt OpenID authentication
   else if( conf->gateway_name != NULL && conf->ms_username != NULL && conf->ms_password != NULL ) {
      // register the gateway via OpenID
      rc = ms_client_openid_gateway_register( client, conf->gateway_name, conf->ms_username, conf->ms_password, conf->volume_pubkey, key_password );
      if( rc != 0 ) {
         errorf("ms_client_gateway_register rc = %d\n", rc );
         
         ms_client_destroy( client );
         
         return rc;
      }
   }
   else {
      // anonymous register.
      // (force client mode)
      if( !conf->is_client ) {
         errorf("%s", "ERROR: no authentication tokens provided, but not in client mode.\n");
         return -EINVAL;
      }
      
      conf->gateway_name = strdup("<anonymous>");
      conf->ms_username = strdup("<anonymous>");
      conf->ms_password = strdup("<anonymous>");
      conf->owner = USER_ANON;
      conf->gateway = GATEWAY_ANON;
      rc = ms_client_anonymous_gateway_register( client, conf->volume_name, conf->volume_pubkey );
      
      if( rc != 0 ) {
         errorf("ms_client_anonymous_gateway_register(%s) rc = %d\n", conf->volume_name, rc );
         
         ms_client_destroy( client );
         
         return -ENOTCONN;
      }  
   }

   // verify that we bound to the right volume
   
   char* ms_volume_name = ms_client_get_volume_name( client );

   if( ms_volume_name == NULL ) {
      errorf("%s", "This gateway does not appear to be bound to any volumes!\n");
      return -EINVAL;
   }
   
   if( strcmp(ms_volume_name, conf->volume_name) != 0 ) {
      errorf("ERR: This UG is not registered to Volume '%s'\n", conf->volume_name );
      free( ms_volume_name );
      return rc;
   }
   
   // get the portnum
   conf->portnum = ms_client_get_portnum( client );
   
   // FIXME: DRY this up
   ms_client_wlock( client );
   conf->owner = client->owner_id;
   conf->gateway = client->gateway_id;
   ms_client_unlock( client );
   
   // create a public url, now that we know the port number
   conf->content_url = CALLOC_LIST( char, strlen(conf->hostname) + 20 );
   sprintf(conf->content_url, "http://%s:%d/", conf->hostname, conf->portnum );
   dbprintf("content URL is %s\n", conf->content_url );
   
   free( ms_volume_name );
   
   return rc;
}
   
   
// initialize Syndicate
int md_init( struct md_syndicate_conf* conf,
             struct ms_client* client,
             char const* ms_url,
             char const* volume_name,
             char const* gateway_name,
             char const* oid_username,
             char const* oid_password,
             char const* user_pkey_pem,
             char const* volume_pubkey_file,
             char const* gateway_key_path,
             char const* gateway_key_password,
             char const* tls_pkey_file,
             char const* tls_cert_file,
             char const* storage_root,
             char const* syndicate_pubkey_path
           ) {

   int rc = md_init_begin( conf, ms_url, volume_name, gateway_name, oid_username, oid_password, user_pkey_pem, gateway_key_path, tls_pkey_file, tls_cert_file, syndicate_pubkey_path );

   if( rc != 0 ) {
      errorf("md_init_begin() rc = %d\n", rc );
      return rc;
   }
   
   conf->is_client = false;
   
   MD_SYNDICATE_CONF_OPT( *conf, storage_root, storage_root );
   

   // set up libsyndicate runtime information
   rc = md_runtime_init( conf, gateway_key_password );
   if( rc != 0 ) {
      errorf("md_runtime_init() rc = %d\n", rc );
      return rc;
   }
   
   return md_init_finish( conf, client, gateway_key_password );
}


// initialize syndicate as a client only
int md_init_client( struct md_syndicate_conf* conf,
                    struct ms_client* client,
                    char const* ms_url,
                    char const* volume_name, 
                    char const* gateway_name,
                    char const* oid_username,
                    char const* oid_password,
                    char const* user_pkey_pem,
                    char const* volume_pubkey_pem,
                    char const* gateway_key_pem,
                    char const* gateway_key_password,
                    char const* storage_root,
                    char const* syndicate_pubkey_pem
                  ) {
   
   
   int rc = md_init_begin( conf, ms_url, volume_name, gateway_name, oid_username, oid_password, user_pkey_pem, NULL, NULL, NULL, NULL );

   if( rc != 0 ) {
      errorf("md_init_begin() rc = %d\n", rc );
      return rc;
   }
   
   conf->is_client = true;
   
   MD_SYNDICATE_CONF_OPT( *conf, storage_root, storage_root );
   MD_SYNDICATE_CONF_OPT( *conf, gateway_key, gateway_key_pem );
   MD_SYNDICATE_CONF_OPT( *conf, volume_pubkey, volume_pubkey_pem );
   MD_SYNDICATE_CONF_OPT( *conf, syndicate_pubkey, syndicate_pubkey_pem );
   MD_SYNDICATE_CONF_OPT( *conf, user_pkey, user_pkey_pem );
   
   if( conf->gateway_key ) {
      conf->gateway_key_len = strlen(conf->gateway_key);
   }
   if( conf->volume_pubkey ) {
      conf->volume_pubkey_len = strlen(conf->volume_pubkey);
   }
   if( conf->syndicate_pubkey ) {
      conf->syndicate_pubkey_len = strlen(conf->syndicate_pubkey);
   }
   if( conf->user_pkey ) {
      conf->user_pkey_len = strlen(conf->user_pkey);
   }
   
   // set up libsyndicate runtime information
   rc = md_runtime_init( conf, gateway_key_password );
   if( rc != 0 ) {
      errorf("md_runtime_init() rc = %d\n", rc );
      return rc;
   }
   
   return md_init_finish( conf, client, gateway_key_password );   
}



// default configuration
int md_default_conf( struct md_syndicate_conf* conf, int gateway_type ) {

   memset( conf, 0, sizeof(struct md_syndicate_conf) );
   
   conf->default_read_freshness = 5000;
   conf->default_write_freshness = 0;
   conf->gather_stats = false;
   conf->httpd_portnum = 44444;

#ifndef _DEVELOPMENT
   conf->verify_peer = true;
#else
   conf->verify_peer = false;
#endif
   
   conf->num_http_threads = 1;
   
   conf->ag_block_size = 0;
   
   conf->debug_lock = false;

   conf->connect_timeout = 10;
   conf->replica_connect_timeout = 10;
   
   conf->portnum = -1;
   conf->transfer_timeout = 300;

   conf->owner = getuid();
   conf->usermask = 0377;

   conf->view_reload_freq = 3600;  // once an hour at minimum
   
   conf->gateway_type = gateway_type;

   return 0;
}


// check a configuration structure to see that it has everything we need.
// print warnings too
int md_check_conf( struct md_syndicate_conf* conf ) {
   char const* warn_fmt = "WARN: missing configuration parameter: %s\n";
   char const* err_fmt = "ERR: missing configuration parameter: %s\n";

   // universal configuration warnings and errors
   int rc = 0;
   if( conf->logfile_path == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, LOGFILE_PATH_KEY );
   }
   if( conf->cdn_prefix == NULL ) {
      fprintf(stderr, warn_fmt, CDN_PREFIX_KEY );
   }
   if( conf->proxy_url == NULL ) {
      fprintf(stderr, warn_fmt, PROXY_URL_KEY );
   }
   if( conf->metadata_url == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, METADATA_URL_KEY );
   }
   if( conf->ms_username == NULL ) {
      fprintf(stderr, warn_fmt, MS_USERNAME_KEY );
   }
   if( conf->ms_password == NULL ) {
      fprintf(stderr, warn_fmt, MS_PASSWORD_KEY );
   }
   if( conf->gateway_name == NULL ) {
      fprintf(stderr, warn_fmt, GATEWAY_NAME_KEY );
   }
   if( conf->gateway_key == NULL ) {
      fprintf(stderr, warn_fmt, GATEWAY_KEY_KEY );
   }
   
   if( conf->gateway_type == SYNDICATE_UG ) {
      // UG-specific warnings and errors
      if( conf->data_root == NULL ) {
         rc = -EINVAL;
         fprintf(stderr, err_fmt, DATA_ROOT_KEY );
      }
   }

   else {
      // RG/AG-specific warnings and errors
      if( conf->server_key == NULL ) {
         fprintf(stderr, warn_fmt, SSL_PKEY_KEY );
      }
      if( conf->server_cert == NULL ) {
         fprintf(stderr, warn_fmt, SSL_CERT_KEY );
      }
   }

   return rc;
}
