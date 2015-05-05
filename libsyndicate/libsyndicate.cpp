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
#include "libsyndicate/opts.h"
#include "libsyndicate/ms/ms-client.h"
#include "libsyndicate/cache.h"

#define INI_MAX_LINE 4096
#define INI_STOP_ON_FIRST_ERROR 1

#include "ini.h"

// stacktrace for uncaught C++ exceptions 
void md_uncaught_exception_handler(void) {
   
   SG_error("%s", "UNCAUGHT EXCEPTION!  Stack trace follows");
   
   void *trace_elems[32];
   int trace_elem_count(backtrace( trace_elems, 32 ));
   
   char **stack_syms(backtrace_symbols( trace_elems, trace_elem_count ));
   
   for( int i = 0 ; i < trace_elem_count ; i++ ) {
      
      SG_error("        %s\n", stack_syms[i] );
   }
   
   SG_safe_free( stack_syms );
   
   exit(1);
}

// set the hostname 
// return 0 on success
// return -ENOMEM if OOM
int md_set_hostname( struct md_syndicate_conf* conf, char const* hostname ) {
   
   char* new_hostname = SG_strdup_or_null( hostname );
   if( new_hostname == NULL ) {
      return -ENOMEM;
   }
   
   if( conf->hostname ) {
      SG_safe_free( conf->hostname );
   }
   
   conf->hostname = new_hostname;
   return 0;
}

// get the hostname 
// return a duplicate of the hostname string on success
// return NULL on OOM, or if no host is defined
char* md_get_hostname( struct md_syndicate_conf* conf ) {
   
   return SG_strdup_or_null( conf->hostname );
}

// initialize server information
// return 0 on success
// return -ENOMEM on OOM 
// return gai_error if we failed to get the address or hostname information
static int md_init_server_info( struct md_syndicate_conf* c ) {
   
   int rc = 0;
   
   char* new_hostname = SG_CALLOC( char, HOST_NAME_MAX + 1);
   if( new_hostname == NULL ) {
      return -ENOMEM;
   }
   
   if( !c->is_client ) {
      
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
         SG_error("getaddrinfo(%s): %s\n", hostname, gai_strerror( rc ) );
         freeaddrinfo( result );
         return -abs(rc);
      }
      
      // now reverse-lookup ourselves
      rc = getnameinfo( result->ai_addr, result->ai_addrlen, new_hostname, HOST_NAME_MAX, NULL, 0, NI_NAMEREQD );
      if( rc != 0 ) {
         SG_error("getnameinfo: %s\n", gai_strerror( rc ) );
         freeaddrinfo( result );
         
         SG_safe_free( new_hostname );
         return -abs(rc);
      }
      
      SG_debug("canonical hostname is %s\n", new_hostname);

      
      c->hostname = new_hostname;
      
      freeaddrinfo( result );
   }
   else {
      // fill in defaults, but they won't be used except for registration
      strcpy( new_hostname, "localhost" );
      c->hostname = new_hostname;
      c->server_key = NULL;
      c->server_cert = NULL;
   }
   
   return rc;
}

// initialize fields in the config that cannot be loaded from command line options alone
// (hence 'runtime_init' in the name).
// This includes loading all files, setting up local storage (if needed), setting up networking (if needed).
// return 0 on success
// return -ENONET if we couldn't load a file requested by the config 
// return negative if we couldn't initialize local storage, setup crypto, setup networking, or load a sensitive file securely.
// NOTE: if this fails, the caller must free the md_syndicate_conf structure's fields
static int md_runtime_init( struct md_syndicate_conf* c, char const* key_password ) {

   int rc = 0;
   
   GOOGLE_PROTOBUF_VERIFY_VERSION;
   rc = curl_global_init( CURL_GLOBAL_ALL );
   
   if( rc != 0 ) {
      SG_error("curl_global_init rc = %d\n", rc );
      return rc;
   }
   
   rc = md_crypt_init();
   if( rc != 0 ) {
      SG_error("md_crypt_init rc = %d\n", rc );
      return rc;
   }
   
   // get the umask
   mode_t um = md_get_umask();
   c->usermask = um;

   rc = md_init_local_storage( c );
   if( rc != 0 ) {
      
      SG_error("md_init_local_storage(%s) rc = %d\n", c->storage_root, rc );
      return rc;
   }
   
   SG_debug("Store local data at %s\n", c->storage_root );
   
   if( c->hostname == NULL ) {
      
      rc = md_init_server_info( c );
      if( rc != 0 ) {
         
         SG_error("md_init_server_info() rc = %d\n", rc );
         return rc;
      }
      
      SG_debug("Serve data as %s\n", c->hostname );
   }
   
   // load gateway public/private key
   if( c->gateway_key_path != NULL ) {
      
      // securely load the gateway key
      struct mlock_buf gateway_key_mlock_buf;
      memset( &gateway_key_mlock_buf, 0, sizeof(gateway_key_mlock_buf) );
      
      int rc = md_load_secret_as_string( &gateway_key_mlock_buf, c->gateway_key_path );
      if( rc != 0 ) {
         
         SG_error("Could not read Gateway key %s, rc = %d\n", c->gateway_key_path, rc );
         return rc;
      }
      
      char* enc_gateway_key = (char*)gateway_key_mlock_buf.ptr;
      size_t enc_gateway_key_len = gateway_key_mlock_buf.len;
      
      if( key_password != NULL ) {
         
         // need to decrypt
         char* unencrypted_key = NULL;
         size_t unencrypted_key_len = 0;
         
         // NOTE: unencrypted_key will be mlock'ed
         rc = md_password_unseal_mlocked( enc_gateway_key, enc_gateway_key_len, key_password, strlen(key_password), &unencrypted_key, &unencrypted_key_len );
         
         // NOTE: gateway_key_mlock_buf is the same as enc_gateway_key 
         mlock_free( &gateway_key_mlock_buf );
         
         if( rc != 0 ) {
            SG_error("md_password_unseal rc = %d\n", rc );
            
            c->gateway_key = NULL;
            c->gateway_key_len = 0;
         }
         else {
            
            // NOTE: unencrypted_key is mlock'ed, so it will be safe to put it back into an mlock_buf and mlock_free it.
            c->gateway_key = unencrypted_key;
            c->gateway_key_len = 0;
         }
      }
      else {
         
         // not encrypted; just initialize.
         // NOTE: the key is mlock'ed, so it will be safe to put it back into an mlock_buf and mlock_free it later.
         c->gateway_key = enc_gateway_key;
         c->gateway_key_len = enc_gateway_key_len;
      }
   }
   
   // load volume public key, if given
   if( c->volume_pubkey_path != NULL ) {
      
      c->volume_pubkey = md_load_file_as_string( c->volume_pubkey_path, &c->volume_pubkey_len );
      if( c->volume_pubkey == NULL ) {
         
         SG_error("Failed to load public key from %s\n", c->volume_pubkey_path );
         return -ENOENT;
      }
      
   }

   // load TLS credentials
   if( c->server_key_path != NULL && c->server_cert_path != NULL ) {
      
      // load the signed public key (i.e. the cert)
      c->server_cert = md_load_file_as_string( c->server_cert_path, &c->server_cert_len );
      
      if( c->server_cert == NULL ) {
         
         SG_error( "Could not read TLS certificate %s\n", c->server_cert_path );
         rc = -ENOENT;
      }
      else {
         // securely load the private key 
         struct mlock_buf server_key_mlock_buf;
         int rc = md_load_secret_as_string( &server_key_mlock_buf, c->server_key_path );
         if( rc != 0 ) {
            
            SG_error("Could not read TLS private key %s\n", c->server_key_path );
            SG_safe_free( c->server_cert );
            c->server_cert = NULL;
            return rc;
         }
         
         // NOTE: server_key will be mlock'ed
         c->server_key = (char*)server_key_mlock_buf.ptr;
         c->server_key_len = server_key_mlock_buf.len;
      }
   }
   
   // load syndicate public key, if given 
   if( c->syndicate_pubkey_path != NULL ) {
      
      c->syndicate_pubkey = md_load_file_as_string( c->syndicate_pubkey_path, &c->syndicate_pubkey_len );
      if( c->syndicate_pubkey == NULL ) {
         
         SG_error("Failed to load public key from %s\n", c->syndicate_pubkey_path );
         return -ENOENT;
      }
   }
   
   return rc;
}

// if level >= 1, this turns on debug messages.
// if level >= 2, this turns on locking debug messages
int md_debug( struct md_syndicate_conf* conf, int level ) {
   md_set_debug_level( level );
   
   conf->debug_lock = false;
   if( level > SG_MAX_VERBOSITY ) {
      
      // debug locks as well
      conf->debug_lock = true;
   }
   
   return 0;
}

// if level >= 1, this turns on error messages
// always succeeds
int md_error( struct md_syndicate_conf* conf, int level ) {
   md_set_error_level( level );
   return 0;
}


// shut down the library.
// free all global data structures
// always succeeds
int md_shutdown() {
   
   // shut down protobufs
   google::protobuf::ShutdownProtobufLibrary();
   
   md_crypt_shutdown();
   
   curl_global_cleanup();
   return 0;
}

// read an long value 
// return 0 on success, and set *ret to the parsed value 
// return -EINVAL if it could not be parsed
long md_conf_parse_long( char const* value, long* ret ) {
   char *end = NULL;
   long val = strtol( value, &end, 10 );
   if( end == value ) {
      SG_error( "bad config line: '%s'\n", value );
      return -EINVAL;
   }
   else {
      *ret = val;
   }
   return 0;
}

// ini parser callback 
// return 1 on success
// return <= 0 on failure
static int md_conf_ini_parser( void* userdata, char const* section, char const* key, char const* value ) {
   
   struct md_syndicate_conf* conf = (struct md_syndicate_conf*)userdata;
   int rc = 0;
   long val = 0;
   
   if( strcmp(section, "syndicate") == 0 ) {
      
      // have key, value.
      // what to do?
      if( strcmp( key, SG_CONFIG_DEFAULT_READ_FRESHNESS ) == 0 ) {
         // pull time interval
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->default_read_freshness = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_DEFAULT_WRITE_FRESHNESS ) == 0 ) {
         // pull time interval
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->default_write_freshness = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_CONNECT_TIMEOUT ) == 0 ) {
         // read timeout
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->connect_timeout = val;
         }
         else { 
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MS_USERNAME ) == 0 ) {
         // metadata server username
         conf->ms_username = SG_strdup_or_null( value );
         if( conf->ms_username == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MS_PASSWORD ) == 0 ) {
         // metadata server password
         conf->ms_password = SG_strdup_or_null( value );
         if( conf->ms_password == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_RELOAD_FREQUENCY ) == 0 ) {
         // view reload frequency
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->config_reload_freq = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_TLS_VERIFY_PEER ) == 0 ) {
         // verify peer?
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->verify_peer = (val != 0);
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MS_URL ) == 0 ) {
         // metadata publisher URL
         conf->metadata_url = SG_strdup_or_null( value );
         if( conf->metadata_url == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_LOGFILE_PATH ) == 0 ) {
         // logfile path
         conf->logfile_path = SG_strdup_or_null( value );
         if( conf->logfile_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_GATHER_STATS ) == 0 ) {
         // gather statistics?
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->gather_stats = (val != 0);
         }
         else {
            return -EINVAL;
         }
      }

      else if( strcmp( key, SG_CONFIG_NUM_HTTP_THREADS ) == 0 ) {
         // how big is the HTTP threadpool?
         val = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->num_http_threads = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_STORAGE_ROOT ) == 0 ) {
         // storage root
         size_t len = strlen( value );
         if( len == 0 ) {
            return -EINVAL;
         }
         
         if( value[len-1] != '/' ) {
            // must end in /
            conf->storage_root = SG_CALLOC( char, len+2 );
            if( conf->storage_root == NULL ) {
               return -ENOMEM;
            }
            
            sprintf( conf->storage_root, "%s/", value );
         }
         else {
            
            conf->storage_root = SG_strdup_or_null( value );
            if( conf->storage_root == NULL ) {
               return -ENOMEM;
            }
         }
      }

      else if( strcmp( key, SG_CONFIG_TLS_PKEY_PATH ) == 0 ) {
         // server private key
         conf->server_key_path = SG_strdup_or_null( value );
         if( conf->server_key_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_TLS_CERT_PATH ) == 0 ) {
         // server certificate
         conf->server_cert_path = SG_strdup_or_null( value );
         if( conf->server_cert_path == NULL ) {
            return -ENOMEM;
         }
      }

      else if( strcmp( key, SG_CONFIG_GATEWAY_PKEY_PATH ) == 0 ) {
         // user-given public/private key
         conf->gateway_key_path = SG_strdup_or_null( value );
         if( conf->gateway_key_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_SYNDICATE_PUBKEY_PATH ) == 0 ) {
         // user-given syndicate public key 
         conf->syndicate_pubkey_path = SG_strdup_or_null( value );
         if( conf->syndicate_pubkey_path == NULL ) {
            return -ENOMEM;
         }
      }

      else if( strcmp( key, SG_CONFIG_PORTNUM ) == 0 ) {
         
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            if( val > 0 && val <= 65534 ) {
               conf->portnum = val;
            }
            else {
               SG_error("Invalid port number %ld\n", val );
               return -EINVAL;
            }
         }
         else {
            return -EINVAL;
         }
      }

      else if( strcmp( key, SG_CONFIG_PUBLIC_URL ) == 0 ) {
         
         // public content URL
         size_t len = strlen( value );
         if( len == 0 ) {
            return -EINVAL;
         }
         
         if( value[len-1] != '/' ) {
            // must end in /
            conf->content_url = SG_CALLOC( char, len+2 );
            if( conf->content_url == NULL ) {
               return -ENOMEM;
            }
            
            sprintf( conf->content_url, "%s/", value );
         }
         else {
            
            conf->content_url = SG_strdup_or_null( value );
            if( conf->content_url == NULL ) {
               return -ENOMEM;
            }
         }
      }
      
      else if( strcmp(key, SG_CONFIG_VOLUME_NAME ) == 0 ) {
         // volume name 
         conf->volume_name = SG_strdup_or_null( value );
         if( conf->volume_name == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_GATEWAY_NAME ) == 0 ) {
         // gateway name
         conf->gateway_name = SG_strdup_or_null( value );
         if( conf->gateway_name == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_DEBUG_LEVEL ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            md_debug( conf, (int)val );
         }
         else {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_LOCAL_DRIVERS_DIR ) == 0 ) {
         conf->local_sd_dir = SG_strdup_or_null( value );
         if( conf->local_sd_dir == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_TRANSFER_TIMEOUT ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->transfer_timeout = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_CACHE_SOFT_LIMIT ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->cache_soft_limit = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_CACHE_HARD_LIMIT ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->cache_hard_limit = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_NUM_IOWQS ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->num_iowqs = val;
         }
         else {
            return -EINVAL;
         }
      }
      else {
         SG_error( "Unrecognized key '%s'\n", key );
         return -EINVAL;
      }
   }
   
   return 1;
}


// read the configuration file and populate a md_syndicate_conf structure
int md_read_conf( char const* conf_path, struct md_syndicate_conf* conf ) {
   
   int rc = 0;
   
   FILE* f = fopen( conf_path, "r" );
   if( f == NULL ) {
      
      rc = -errno;
      SG_error("fopen('%s') rc = %d\n", conf_path, rc );
      return rc;
   }
   
   rc = ini_parse_file( f, md_conf_ini_parser, conf );
   if( rc != 0 ) {
      SG_error("ini_parse_file('%s') rc = %d\n", conf_path, rc );
   }
   
   fclose( f );
   
   return rc;
}


// free all memory associated with a server configuration
int md_free_conf( struct md_syndicate_conf* conf ) {
   void* to_free[] = {
      (void*)conf->metadata_url,
      (void*)conf->logfile_path,
      (void*)conf->content_url,
      (void*)conf->data_root,
      (void*)conf->ms_username,
      (void*)conf->ms_password,
      (void*)conf->server_cert,
      (void*)conf->server_key_path,
      (void*)conf->server_cert_path,
      (void*)conf->gateway_key_path,
      (void*)conf->gateway_name,
      (void*)conf->volume_name,
      (void*)conf->volume_pubkey,
      (void*)conf->syndicate_pubkey,
      (void*)conf->local_sd_dir,
      (void*)conf->hostname,
      (void*)conf->storage_root,
      (void*)conf
   };
   
   // things that are mlock'ed, and need to be munlock'ed
   void* mlocked[] = {
      (void*)conf->server_key,   
      (void*)conf->gateway_key,
      (void*)conf->user_pkey,
      (void*)conf
   };
   
   size_t mlocked_len[] = {
      conf->server_key_len,
      conf->gateway_key_len,
      conf->user_pkey_len,
      0
   };

   // munlock first 
   for( int i = 0; mlocked[i] != conf; i++ ) {
      struct mlock_buf tmp;
      
      if( mlocked[i] != NULL ) {
         tmp.ptr = mlocked[i];
         tmp.len = mlocked_len[i];
      
         mlock_free( &tmp );
      }
   }
   
   // free the rest
   for( int i = 0; to_free[i] != conf; i++ ) {
      if( to_free[i] != NULL ) {
         free( to_free[i] );
      }
   }
   
   memset( conf, 0, sizeof(struct md_syndicate_conf) );
      
   return 0;
}



// destroy an md entry
void md_entry_free( struct md_entry* ent ) {
   if( ent->name ) {
      SG_safe_free( ent->name );
   }
   if( ent->parent_name ) {
      SG_safe_free( ent->name );
   }
}


// destroy a bunch of md_entries
void md_entry_free_all( struct md_entry** ents ) {
   for( int i = 0; ents[i] != NULL; i++ ) {
      md_entry_free( ents[i] );
      SG_safe_free( ents[i] );
   }
}

// duplicate an md_entry.
// return a calloc'ed duplicate on success
// return NULL on error
struct md_entry* md_entry_dup( struct md_entry* src ) {
   
   int rc = 0;
   struct md_entry* ret = SG_CALLOC( struct md_entry, 1 );
   
   if( ret == NULL ) {
      return NULL;
   }
   
   rc = md_entry_dup2( src, ret );
   if( rc != 0 ) {
      
      // OOM
      md_entry_free( ret );
      SG_safe_free( ret );
   }
   
   return ret;
}


// duplicate an md_entry into a given ret 
// return 0 on success
// return -ENOMEM if out of memory
int md_entry_dup2( struct md_entry* src, struct md_entry* ret ) {
   
   // copy non-pointers
   char* new_name = SG_strdup_or_null( src->name );
   char* new_parent_name = SG_strdup_or_null( src->parent_name );
   
   if( (src->name != NULL && new_name == NULL) || (src->parent_name != NULL && new_parent_name == NULL) ) {
      
      // OOM
      SG_safe_free( new_name );
      SG_safe_free( new_parent_name );
      return -ENOMEM;
   }
   
   
   // copy everything else 
   memcpy( ret, src, sizeof(md_entry) );
   
   src->name = new_name;
   src->parent_name = new_parent_name;
   
   return 0;
}

// concatenate two paths.
// fill in dest with the result.
// if dest is NULL, then allocate and return a buffer containing the path
// return the path on success
// return NULL on OOM
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

   if( dest == NULL ) {
      dest = SG_CALLOC( char, len );
      if( dest == NULL ) {
         return NULL;
      }
   }
   
   memset(dest, 0, len);
   
   strcpy( dest, root );
   if( delim != 0 ) {
      dest[strlen(dest)] = '/';
   }
   strcat( dest, path + path_off );
   
   return dest;
}


// generate the directory name of a path.
// if dest is not NULL, write the path to dest.
// otherwise, malloc and return the dirname
// if a well-formed path is given, then a string ending in a / is returned
// return the directory on success
// return NULL on OOM
char* md_dirname( char const* path, char* dest ) {
   
   if( dest == NULL ) {
      dest = SG_CALLOC( char, strlen(path) + 1 );
      if( dest == NULL ) {
         return NULL;
      }
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
      if( path[delim_i] == '/' ) {
         break;
      }
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
// the paths must be normalized, and not include ..
// return the depth on success
int md_depth( char const* path ) {
   int i = strlen(path) - 1;
   
   if( i <= 0 ) {
      return 0;
   }
   
   if( path[i] == '/' ) {
      i--;
   }
   
   int depth = 0;
   for( ; i >= 0; i-- ) {
      if( path[i] == '/' ) {
         depth++;
      }
   }
   
   return depth;
}


// find the integer offset into a path where the directory name begins
// return the index of the last '/'
// return -1 if there is no '/' in path
int md_dirname_end( char const* path ) {
   
   int delim_i = strlen(path) - 1;
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' ) {
         break;
      }
   }
   
   if( delim_i == 0 && path[delim_i] != '/' ) {
      delim_i = -1;
   }
   
   return delim_i;
}


// find the basename of a path.
// if dest is not NULL, write it to dest
// otherwise, allocate the basename
// return the basename on success
// return NULL on OOM
char* md_basename( char const* path, char* dest ) {
   int delim_i = strlen(path) - 1;
   if( delim_i <= 0 ) {
      if( dest == NULL ) {
         dest = SG_strdup_or_null("/");
         if( dest == NULL ) {
            return NULL;
         }
      }
      else {
         strcpy(dest, "/");
      }
      return dest;
   }
   if( path[delim_i] == '/' ) {
      // this path ends with '/', so skip over it if it isn't /
      if( delim_i > 0 ) {
         delim_i--;
      }
   }
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' ) {
         break;
      }
   }
   delim_i++;
   
   if( dest == NULL ) {
      dest = SG_CALLOC( char, strlen(path) - delim_i + 1 );
      if( dest == NULL ) {
         return NULL;
      }
   }
   else {
      memset( dest, 0, strlen(path) - delim_i + 1 );
   }
   
   strncpy( dest, path + delim_i, strlen(path) - delim_i );
   return dest;
}


// find the integer offset into a path where the basename begins.
// return the index of the basename
// return -1 if there is no '/'
int md_basename_begin( char const* path ) {
   
   int delim_i = strlen(path) - 1;
   for( ; delim_i >= 0; delim_i-- ) {
      if( path[delim_i] == '/' )
         break;
   }
   
   if( delim_i == 0 && path[delim_i] == '/' ) {
      return -1;
   }
   
   return delim_i + 1;
}


// prepend a prefix to a string
// put the resulting string in output, if output is non-NULL 
// otherwise, allocate and return the prepended string
// return NULL on OOM
char* md_prepend( char const* prefix, char const* str, char* output ) {
   if( output == NULL ) {
      output = SG_CALLOC( char, strlen(prefix) + strlen(str) + 1 );
      if( output == NULL ) {
         return NULL;
      }
   }
   sprintf(output, "%s%s", prefix, str );
   return output;
}


// hash a path
// return the hash as a long on success
long md_hash( char const* path ) {
   locale loc;
   const collate<char>& coll = use_facet<collate<char> >(loc);
   return coll.hash( path, path + strlen(path) );
}


// split a path into its components.
// each component will be duplicated, so the caller must free the strings in results
// return 0 on success
// return -ENOMEM if OOM, in which case the values in result are undefined
int md_path_split( char const* path, vector<char*>* result ) {
   char* tmp = NULL;
   char* path_copy = SG_strdup_or_null( path );
   
   if( path_copy == NULL ) {
      return -ENOMEM;
   }
   
   char* ptr = path_copy;

   // does the path start with /?
   if( *ptr == '/' ) {
      
      char* d = SG_strdup_or_null("/");
      if( d == NULL ) {
         
         SG_safe_free( path_copy );
         return -ENOMEM;
      }
      
      result->push_back( d );
      ptr++;
   }

   // parse through this path
   while( 1 ) {
      
      char* next_tok = strtok_r( ptr, "/", &tmp );
      ptr = NULL;

      if( next_tok == NULL ) {
         break;
      }
      
      char* d = SG_strdup_or_null( next_tok );
      if( d == NULL ) {
         
         SG_safe_free( path_copy );
         return -ENOMEM;
      }
      
      result->push_back( next_tok );
   }

   SG_safe_free( path_copy );
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

// start a thread
// return 0 on success
// return -1 on failure
pthread_t md_start_thread( void* (*thread_func)(void*), void* arg, bool detach ) {

   // start up a thread to listen for connections
   pthread_attr_t attrs;
   pthread_t listen_thread;
   int rc;
   
   rc = pthread_attr_init( &attrs );
   if( rc != 0 ) {
      SG_error( "pthread_attr_init rc = %d\n", rc);
      return (pthread_t)(-1);   // problem
   }

   if( detach ) {
      rc = pthread_attr_setdetachstate( &attrs, PTHREAD_CREATE_DETACHED );    // make a detached thread--we won't join with it
      if( rc != 0 ) {
         SG_error( "pthread_attr_setdetachstate rc = %d\n", rc );
         return (pthread_t)(-1);
      }
   }
   
   rc = pthread_create( &listen_thread, &attrs, thread_func, arg );
   if( rc != 0 ) {
      SG_error( "pthread_create rc = %d\n", rc );
      return (pthread_t)(-1);
   }
   
   return listen_thread;
}

// parse a query string into a list of CGI arguments
// NOTE: this modifies args_str
// return a NULL-terminated list of strings on success.  each string points to args_str
// return NULL on OOM (in which case args_str is not modified
char** md_parse_cgi_args( char* args_str ) {
   int num_args = 1;
   for( unsigned int i = 0; i < strlen(args_str); i++ ) {
      if( args_str[i] == '&' ) {
         
         while( args_str[i] == '&' && i < strlen(args_str) ) {
            i++;
         }
         num_args++;
      }
   }

   char** cgi_args = SG_CALLOC( char*, num_args+1 );
   if( cgi_args == NULL ) {
      return NULL;
   }
   
   int off = 0;
   
   for( int i = 0; i < num_args - 1; i++ ) {
      cgi_args[i] = args_str + off;
      
      unsigned int j;
      for( j = off+1; j < strlen(args_str); j++ ) {
         if( args_str[j] == '&' ) {
            
            while( args_str[j] == '&' && j < strlen(args_str) ) {
               args_str[j] = '\0';
               j++;
            }
            
            break;
         }
      }
      
      off = j+1;
   }
   
   cgi_args[ num_args - 1 ] = args_str + off;
   
   return cgi_args;
}


// locate the path from the url
// return the path in a malloc'ed buffer on success
// return NULL on OOM
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
      ret = SG_strdup_or_null( "/" );
   }
   else {
      ret = SG_strdup_or_null( off );
   }
   
   return ret;
}


// flatten a path.  That is, remove /./, /[/]*, etc, but don't resolve ..
// return the flattened URL on success 
// return NULL on OOM 
char* md_flatten_path( char const* path ) {
   
   size_t len = strlen(path);
   char* ret = SG_CALLOC( char, len + 1 );
   if( ret == NULL ) {
      return NULL;
   }
 
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


// split a url into the url+path and query string
// return 0 on success, set *url_and_path and *qs to calloc'ed strings with the url/path and query string, respectively.
//   if there is no query string, set *qs to NULL
// return -ENOMEM if OOM
int md_split_url_qs( char const* url, char** url_and_path, char** qs ) {
   
   if( strstr( url, "?" ) != NULL ) {
      
      // have query string
      size_t url_path_len = strcspn( url, "?" );
      
      char* ret_url = SG_CALLOC( char, url_path_len + 1 );
      if( ret_url == NULL ) {
         return -ENOMEM;
      }
      
      char* ret_qs = SG_CALLOC( char, strlen(url) - url_path_len + 1 );
      if( ret_qs == NULL ) {
         
         SG_safe_free( ret_url );
         return -ENOMEM;
      }
      
      strncpy( ret_url, url, url_path_len );
      strcpy( ret_qs, strstr( url, "?" ) + 1 );
      
      *url_and_path = ret_url;
      *qs = ret_qs;
      
      return 0;
   }
   else {
      
      char* ret_url = SG_strdup_or_null( url );
      if( ret_url == NULL ) {
         return -ENOMEM;
      }
      
      *qs = NULL;
      *url_and_path = ret_url;
      return 0;
   }
}


// get the offset at which the value starts in a header
// return >= 0 on success 
// return -1 if not found
off_t md_header_value_offset( char* header_buf, size_t header_len, char const* header_name ) {

   size_t off = 0;
   
   if( strlen(header_name) >= header_len ) {
      return -1;      // header is too short
   }
   if( strncasecmp(header_buf, header_name, MIN( header_len, strlen(header_name) ) ) != 0 ) {
      return -1;      // not found
   }
   
   off = strlen(header_name);
   
   // find :
   while( off < header_len ) {
      if( header_buf[off] == ':' ) {
         break;
      }
      off++;
   }

   if( off == header_len ) {
      return -1;      // no value
   }
   off++;

   // find value
   while( off < header_len ) {
      if( header_buf[off] != ' ' ) {
         break;
      }
      off++;
   }

   if( off == header_len ) {
      return -1;      // no value
   }
   return off;
}


// parse an accumulated null-terminated header buffer, and find the first instance of the given header name
// return 0 on success, and put the Location into *location_url as a null-terminated string
// return -ENOENT if not found
// return -ENOMEM if OOM 
int md_parse_header( char* header_buf, char const* header_name, char** header_value ) {
   
   size_t span = 0;
   char* location = strcasestr( header_buf, header_name );
   
   if( location == NULL ) {
      return -ENOENT;
   }
   
   location += strlen(header_name);
   span = strspn( location, ": " );
   
   location += span;
   
   // location points to the header value 
   // advance to EOL
   span = strcspn( location, "\r\n\0" );
   
   char* ret = SG_CALLOC( char, span + 1 );
   if( ret == NULL ) {
      return -ENOMEM;
   }
   
   strncpy( ret, location, span );
   *header_value = ret;
   
   return 0;
}


// parse one value in a header (excluding UINT64_MAX)
// return UINT64_MAX on error
uint64_t md_parse_header_uint64( char* hdr, off_t offset, size_t size ) {
   char* value = hdr + offset;
   size_t value_len = size - offset;

   char* value_str = SG_CALLOC( char, value_len + 1 );
   if( value_str == NULL ) {
      return UINT64_MAX;
   }
   
   strncpy( value_str, value, value_len );

   uint64_t data = 0;
   int rc = sscanf( value_str, "%" PRIu64, &data );
   if( rc != 1 ) {
      
      data = UINT64_MAX;
   }
   
   free( value_str );
   
   return data;
}

// read a csv of values
// place UINT64_MAX in an element on failure to parse
// return NULL on OOM
uint64_t* md_parse_header_uint64v( char* hdr, off_t offset, size_t size, size_t* ret_len ) {
   
   char* value = hdr + offset;
   size_t value_len = size - offset;
   
   char* value_str = SG_CALLOC( char, value_len + 1 );
   if( value_str == NULL ) {
      return NULL;
   }
   
   strcpy( value_str, value );

   // how many commas?
   int num_values = 1;
   for( size_t i = offset; i < size; i++ ) {
      if( hdr[i] == ',' ) {
         num_values++;
      }
   }
   
   char* tmp = value_str;
   char* tmp2 = NULL;
   
   uint64_t* ret = SG_CALLOC( uint64_t, num_values );
   if( ret == NULL ) {
   
      SG_safe_free( value_str );
      return NULL;
   }
   
   int i = 0;
   
   while( 1 ) {
      char* tok = strtok_r( tmp, ", \r\n", &tmp2 );
      if( tok == NULL ) {
         break;
      }
      
      tmp = NULL;
      
      uint64_t data = (uint64_t)(-1);
      sscanf( value_str, "%" PRIu64, &data );
      
      ret[i] = data;
      i++;
   }

   SG_safe_free( value_str );
   *ret_len = num_values;
   return ret;
}

// Get the offset into a path where the version begins (delimited by a .)
// Returns nonnegative on success.
// returns negative on error.
int md_path_version_offset( char const* path ) {
   int i;
   bool valid = true;
   bool sign = false;
   for( i = strlen(path)-1; i >= 0; i-- ) {
      if( path[i] == '.' ) {
         break;
      }
      if( path[i] == '-' && !sign ) {
         sign = true;
         continue;
      }
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
   if( version == 0 && end == path ) {
      return -3;     // could not parse the version
   }
   
   return i;  
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
// return the number of bytes serialized on success, and allocate a buffer and put it into *buf 
// return -ENOMEM if OOM 
// return -EINVAL if we couldn't serialize
ssize_t md_metadata_update_text( struct md_syndicate_conf* conf, char** buf, struct md_update** updates ) {
   
   int rc = 0;
   ms::ms_updates ms_updates;

   for( int i = 0; updates[i] != NULL; i++ ) {
      
      struct md_update* update = updates[i];
      ms::ms_update* ms_up = NULL;
      ms::ms_entry* ms_ent = NULL;
      
      try {
         ms_up = ms_updates.add_updates();
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
         break;
      }
      
      ms_up->set_type( update->op );

      try {
         ms_ent = ms_up->mutable_entry();
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
         break;
      }

      rc = md_entry_to_ms_entry( ms_ent, &update->ent );
      if( rc != 0 ) {
         break;
      }
   }
   
   if( rc != 0 ) {
      return rc;
   }

   string text;
   
   try {
      
      bool valid = ms_updates.SerializeToString( &text );
      if( !valid ) {
         return -EINVAL;
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   *buf = SG_CALLOC( char, text.size() + 1 );
   if( *buf == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( *buf, text.data(), text.size() );

   return (ssize_t)text.size();
}

// convert an md_entry to an ms_entry
// return 0 on success
// return -ENOMEM on OOM
int md_entry_to_ms_entry( ms::ms_entry* msent, struct md_entry* ent ) {
   
   try {
         
      if( ent->parent_id != (uint64_t)(-1) ) {
         msent->set_parent_id( ent->parent_id );
      }
      
      if( ent->parent_name != NULL ) {
         msent->set_parent_name( string(ent->parent_name) );
      }
      else {
         msent->set_parent_name( string("") );
      }

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
      msent->set_manifest_mtime_sec( ent->manifest_mtime_sec );
      msent->set_manifest_mtime_nsec( ent->manifest_mtime_nsec );
      msent->set_version( ent->version );
      msent->set_size( ent->size );
      msent->set_max_read_freshness( ent->max_read_freshness );
      msent->set_max_write_freshness( ent->max_write_freshness );
      msent->set_write_nonce( ent->write_nonce );
      msent->set_xattr_nonce( ent->xattr_nonce );
      msent->set_generation( ent->generation );
      msent->set_capacity( ent->capacity );
      
      if( ent->name != NULL ) {
         msent->set_name( string( ent->name ) );
      }
      else {
         msent->set_name( string("") );
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}


// convert ms_entry to md_entry
// return 0 on success
// return negative on error
int ms_entry_to_md_entry( const ms::ms_entry& msent, struct md_entry* ent ) {
   memset( ent, 0, sizeof(struct md_entry) );

   ent->name = SG_strdup_or_null( msent.name().c_str() );
   if( ent->name == NULL ) {
      return -ENOMEM;
   }
   
   if( msent.has_parent_name() ) {
      ent->parent_name = SG_strdup_or_null( msent.parent_name().c_str() );
      if( ent->parent_name == NULL ) {
         
         SG_safe_free( ent->name );
         return -ENOMEM;
      }
   }
   else {
      ent->parent_name = NULL;
   }
   
   ent->type = msent.type() == ms::ms_entry::MS_ENTRY_TYPE_FILE ? MD_ENTRY_FILE : MD_ENTRY_DIR;
   ent->file_id = msent.file_id();
   ent->owner = msent.owner();
   ent->coordinator = msent.coordinator();
   ent->volume = msent.volume();
   ent->mode = msent.mode();
   ent->mtime_sec = msent.mtime_sec();
   ent->mtime_nsec = msent.mtime_nsec();
   ent->manifest_mtime_sec = msent.manifest_mtime_sec();
   ent->manifest_mtime_nsec = msent.manifest_mtime_nsec();
   ent->ctime_sec = msent.ctime_sec();
   ent->ctime_nsec = msent.ctime_nsec();
   ent->max_read_freshness = (uint64_t)msent.max_read_freshness();
   ent->max_write_freshness = (uint64_t)msent.max_write_freshness();
   ent->version = msent.version();
   ent->size = msent.size();
   ent->write_nonce = msent.write_nonce();
   ent->xattr_nonce = msent.xattr_nonce();
   ent->generation = msent.generation();
   ent->num_children = msent.num_children();
   ent->capacity = msent.capacity();
   
   if( msent.has_parent_id() ) {
      ent->parent_id = msent.parent_id();
   }
   else {
      ent->parent_id = -1;
   }

   return 0;
}

// free a metadata update
void md_update_free( struct md_update* update ) {
   md_entry_free( &update->ent );
   
   if( update->xattr_name ) {
      SG_safe_free( update->xattr_name );
   }
   
   if( update->xattr_value ) {
      SG_safe_free( update->xattr_value );
   }

   if( update->affected_blocks ) {
      SG_safe_free( update->affected_blocks );
   }
   
   memset( update, 0, sizeof(struct md_update) );
}


// duplicate an update
// return 0 on success
// return -ENOMEM on OOM
int md_update_dup2( struct md_update* src, struct md_update* dest ) {
   
   dest->op = src->op;
   dest->flags = src->flags;
   dest->error = src->error;
   
   
   char* xattr_name = NULL;
   char* xattr_value = NULL;
   
   if( src->xattr_name ) {
      xattr_name = SG_strdup_or_null( src->xattr_name );
      if( xattr_name == NULL ) {
         
         return -ENOMEM;
      }
   }
   
   if( src->xattr_value ) {
      
      xattr_value = SG_CALLOC( char, src->xattr_value_len );
      if( xattr_value == NULL ) {
         
         SG_safe_free( xattr_name );
         return -ENOMEM;
      }
      
      memcpy( dest->xattr_value, src->xattr_value, src->xattr_value_len );
      dest->xattr_value_len = src->xattr_value_len;
   }
   
   dest->xattr_name = xattr_name;
   dest->xattr_value = xattr_value;
   
   return 0;
}



// basic Syndicate initialization
// return 0 on success
// return -ENOMEM if OOM.  If this happens, the caller should free the config
int md_init_begin( struct md_syndicate_conf* conf,
                   char const* ms_url,
                   char const* volume_name,
                   char const* gateway_name,
                   char const* ms_username,
                   char const* ms_password,
                   char const* user_pkey_pem,           // NOTE: should be mlock'ed
                   char const* volume_pubkey_path,
                   char const* gateway_key_path,
                   char const* tls_pkey_file,
                   char const* tls_cert_file,
                   char const* syndicate_pubkey_path
                 ) {
   
   int rc = 0;
   
   // before we load anything, disable core dumps (i.e. to keep private keys from leaking)
   bool disable_core_dumps = true;
   
#ifdef _DEVELOPMENT
   // for development, keep user's core dump setting to facilitate debugging
   disable_core_dumps = false;
#endif
   
   if( disable_core_dumps ) {
      
      struct rlimit rlim;
      getrlimit( RLIMIT_CORE, &rlim );
      rlim.rlim_max = 0;
      rlim.rlim_cur = 0;
      
      rc = setrlimit( RLIMIT_CORE, &rlim );
      if( rc != 0 ) {
         rc = -errno;
         SG_error("Failed to disable core dumps, rc = %d\n", rc );
         return rc;
      }
   }
   
   // need a Volume name
   if( volume_name == NULL ) {
      SG_error("%s", "missing Volume name\n");
      return -EINVAL;
   }

   rc = md_util_init();
   if( rc != 0 ) {
      SG_error("md_util_init rc = %d\n", rc );
      return rc;
   }
   
   // populate the config
   rc = 0;
   
   MD_SYNDICATE_CONF_OPT( *conf, volume_name, volume_name, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, volume_pubkey_path, volume_pubkey_path, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, metadata_url, ms_url, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, ms_username, ms_username, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, ms_password, ms_password, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, gateway_name, gateway_name, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, server_cert_path, tls_cert_file, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, server_key_path, tls_pkey_file, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, gateway_key_path, gateway_key_path, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, syndicate_pubkey_path, syndicate_pubkey_path, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   if( user_pkey_pem != NULL ) {
      // special case: duplicate an mlock'ed buffer
      conf->user_pkey_len = strlen(user_pkey_pem);
      
      struct mlock_buf tmp;
      int rc = mlock_dup( &tmp, user_pkey_pem, conf->user_pkey_len + 1 );
      if( rc != 0 ) {
         
         SG_error("mlock_dup rc = %d\n", rc );
         return -ENOMEM;
      }
      
      conf->user_pkey = (char*)tmp.ptr;
   }
   
   return rc;
}


// finish initialization
// set up the ms_client and register the gateway.
// NOTE: key_password should be mlock'ed
// if this fails, the caller should free conf
// return 0 on success
// return -ENOMEM if OOM
// return negative on failure (-ENODATA, -ENOTCONN) to register
int md_init_finish( struct md_syndicate_conf* conf, struct ms_client* client, char const* key_password ) {
   
   int rc = 0;
   
   // validate the config
   rc = md_check_conf( conf, (key_password != NULL) );
   if( rc != 0 ) {
      
      SG_error("md_check_conf rc = %d\n", rc );
      return rc;
   }
   
   // setup the client
   rc = ms_client_init( client, conf->gateway_type, conf );
   if( rc != 0 ) {
      
      SG_error("ms_client_init rc = %d\n", rc );
      return rc;
   }
   
   // attempt public-key authentication 
   if( conf->user_pkey != NULL ) {
      
      rc = ms_client_public_key_gateway_register( client, conf->gateway_name, conf->ms_username, conf->user_pkey, conf->volume_pubkey, key_password );
      if( rc != 0 ) {
         
         SG_error("ms_client_public_key_register rc = %d\n", rc );
         
         ms_client_destroy( client );
         
         return rc;
      }
   }
   
   // attempt OpenID authentication
   else if( conf->gateway_name != NULL && conf->ms_username != NULL && conf->ms_password != NULL ) {
      
      // register the gateway via OpenID
      rc = ms_client_openid_gateway_register( client, conf->gateway_name, conf->ms_username, conf->ms_password, conf->volume_pubkey, key_password );
      if( rc != 0 ) {
         
         SG_error("ms_client_gateway_register rc = %d\n", rc );
         
         ms_client_destroy( client );
         
         return rc;
      }
   }
   else {
      
      // anonymous register.
      // (force client mode)
      if( !conf->is_client ) {
         SG_error("%s", "ERROR: no authentication tokens provided, but not in client mode.\n");
         return -EINVAL;
      }
      
      conf->gateway_name = SG_strdup_or_null("<anonymous>");
      if( conf->gateway_name == NULL ) {
         return -ENOMEM;
      }
      
      conf->ms_username = SG_strdup_or_null("<anonymous>");
      if( conf->ms_username == NULL ) {
         return -ENOMEM;
      }
      
      conf->ms_password = SG_strdup_or_null("<anonymous>");
      if( conf->ms_password == NULL ) {
         return -ENOMEM;
      }
      
      conf->owner = SG_USER_ANON;
      conf->gateway = SG_GATEWAY_ANON;
      
      rc = ms_client_anonymous_gateway_register( client, conf->volume_name, conf->volume_pubkey );
      
      if( rc != 0 ) {
         SG_error("ms_client_anonymous_gateway_register(%s) rc = %d\n", conf->volume_name, rc );
         
         ms_client_destroy( client );
         
         return -ENOTCONN;
      }  
   }
   
   // verify that we bound to the right volume
   char* ms_volume_name = ms_client_get_volume_name( client );

   if( ms_volume_name == NULL ) {
      
      SG_error("%s", "Not bound to volume\n");
      return -EINVAL;
   }
   
   if( strcmp(ms_volume_name, conf->volume_name) != 0 ) {
      
      SG_error("Specified volume '%s', but MS says registered to volume '%s'\n", conf->volume_name, ms_volume_name );
      SG_safe_free( ms_volume_name );
      return -EINVAL;
   }
   
   SG_safe_free( ms_volume_name );
   
   // get the portnum
   conf->portnum = ms_client_get_portnum( client );
   
   // FIXME: DRY this up
   ms_client_wlock( client );
   
   conf->owner = client->owner_id;
   conf->gateway = client->gateway_id;

   conf->volume = ms_client_get_volume_id( client );
   ms_client_unlock( client );
   
   if( conf->content_url == NULL ) {
      
      // create a public url, now that we know the port number
      conf->content_url = SG_CALLOC( char, strlen(conf->hostname) + 20 );
      if( conf->content_url == NULL ) {
         
         return -ENOMEM;
      }
      sprintf(conf->content_url, "http://%s:%d/", conf->hostname, conf->portnum );
   }
   
   SG_debug("Running as Gateway %" PRIu64 "\n", conf->gateway );
   SG_debug("content URL is %s\n", conf->content_url );
   
   return rc;
}
   
   
// initialize Syndicate
// return 0 on success 
// if this fails, the caller should shut down the library and free conf
static int md_init_common( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts, bool is_client ) {
   
   char const* ms_url = opts->ms_url;
   char const* volume_name = opts->volume_name;
   char const* gateway_name = opts->gateway_name;
   char const* oid_username = opts->username;
   char const* oid_password = (char const*)opts->password.ptr;
   char const* user_pkey_pem = (char const*)opts->user_pkey_pem.ptr;
   char const* volume_pubkey_path = opts->volume_pubkey_path;
   char const* volume_pubkey_pem = opts->volume_pubkey_pem;
   char const* gateway_key_path = opts->gateway_pkey_path;
   char const* gateway_key_pem = (char const*)opts->gateway_pkey_pem.ptr;
   char const* gateway_key_password = (char const*)opts->gateway_pkey_decryption_password.ptr;
   char const* storage_root = opts->storage_root;
   char const* syndicate_pubkey_path = opts->syndicate_pubkey_path;
   char const* syndicate_pubkey_pem = opts->syndicate_pubkey_pem;
   char const* tls_pkey_path = opts->tls_pkey_path;
   char const* tls_cert_path = opts->tls_cert_path;
   int rc = 0;
   
   // early exception handling 
   set_terminate( md_uncaught_exception_handler );
   
   // early debugging 
   md_set_debug_level( opts->debug_level );
   md_set_error_level( SG_MAX_VERBOSITY );
   
   rc = md_init_begin( conf, ms_url, volume_name, gateway_name, oid_username, oid_password, user_pkey_pem, volume_pubkey_path, gateway_key_path, tls_pkey_path, tls_cert_path, syndicate_pubkey_path );
   
   if( rc != 0 ) {
      
      SG_error("md_init_begin() rc = %d\n", rc );
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, storage_root, storage_root, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, volume_pubkey, volume_pubkey_pem, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, syndicate_pubkey, syndicate_pubkey_pem, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   conf->is_client = is_client;
   
   if( conf->volume_pubkey ) {
      conf->volume_pubkey_len = strlen(conf->volume_pubkey);
   }
   if( conf->syndicate_pubkey ) {
      conf->syndicate_pubkey_len = strlen(conf->syndicate_pubkey);
   }
   
   if( gateway_key_pem != NULL ) {
      // special case: duplicate an mlock'ed buffer
      conf->gateway_key_len = strlen(gateway_key_pem);
      
      struct mlock_buf tmp;
      rc = mlock_dup( &tmp, gateway_key_pem, conf->gateway_key_len + 1 );
      if( rc != 0 ) {
         
         SG_error("mlock_dup rc = %d\n", rc );
         return rc;
      }
      
      conf->gateway_key = (char*)tmp.ptr;
   }
   
   // set up libsyndicate runtime information
   rc = md_runtime_init( conf, gateway_key_password );
   if( rc != 0 ) {
      SG_error("md_runtime_init() rc = %d\n", rc );
      return rc;
   }
   
   return md_init_finish( conf, client, gateway_key_password );
}


// initialize syndicate as a client only
int md_init_client( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts ) {
   return md_init_common( conf, client, opts, true );
}

// initialize syndicate as a full gateway 
int md_init( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts ) {
   return md_init_common( conf, client, opts, false );
}


// default configuration
// return 0 on success
// return -ENOMEM on OOM
int md_default_conf( struct md_syndicate_conf* conf, uint64_t gateway_type ) {

   memset( conf, 0, sizeof(struct md_syndicate_conf) );
   
   conf->default_read_freshness = 5000;
   conf->default_write_freshness = 0;
   conf->gather_stats = false;

#ifndef _DEVELOPMENT
   conf->verify_peer = true;
#else
   conf->verify_peer = false;
#endif
   
   conf->num_http_threads = sysconf( _SC_NPROCESSORS_CONF );
   conf->num_iowqs = 4 * sysconf( _SC_NPROCESSORS_CONF );       // I/O doesn't take much CPU...
   
   conf->debug_lock = false;

   conf->connect_timeout = 600;
   
   conf->portnum = -1;
   conf->transfer_timeout = 600;

   conf->owner = 0;
   conf->usermask = 0377;

   conf->config_reload_freq = 3600;  // once an hour at minimum
   
   conf->max_metadata_read_retry = 3;
   conf->max_metadata_write_retry = 3;
   conf->max_read_retry = 3;
   conf->max_write_retry = 3;
   
   conf->gateway_type = gateway_type;
   
   conf->cache_soft_limit = MD_CACHE_DEFAULT_SOFT_LIMIT;
   conf->cache_hard_limit = MD_CACHE_DEFAULT_HARD_LIMIT;
   
   return 0;
}


// check a configuration structure to see that it has everything we need.
// print warnings too
int md_check_conf( struct md_syndicate_conf* conf, bool have_key_password ) {
   
   // char const* warn_fmt = "WARN: missing configuration parameter: %s\n";
   char const* err_fmt = "ERR: missing configuration parameter: %s\n";

   // universal configuration warnings and errors
   int rc = 0;
   if( conf->metadata_url == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_MS_URL );
   }
   if( conf->ms_username == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_MS_USERNAME );
   }
   if( conf->ms_password == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_MS_PASSWORD );
   }
   if( conf->gateway_name == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_GATEWAY_NAME );
   }
   if( conf->gateway_key == NULL && !conf->is_client && !have_key_password ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_GATEWAY_PKEY_PATH );
   }
   if( conf->volume_name == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, SG_CONFIG_VOLUME_NAME );
   }
   
   return rc;
}
