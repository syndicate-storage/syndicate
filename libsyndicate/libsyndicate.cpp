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
#include "libsyndicate/private/opts.h"
#include "libsyndicate/ms/ms-client.h"
#include "libsyndicate/cache.h"
#include "libsyndicate/proc.h"

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
   }
   
   return rc;
}


// look up a gateway cert by ID
// return a pointer to the cert on success
// return NULL if not found 
struct ms_gateway_cert* md_gateway_cert_find( ms_cert_bundle* gateway_certs, uint64_t gateway_id ) {
   
   ms_cert_bundle::iterator itr = gateway_certs->find( gateway_id );
   if( itr != gateway_certs->end() ) {
      
      return itr->second;
   }
   else {
      
      return NULL;
   }
}

// load a cert bundle, given the name of the gateway and volume
// return 0 on success, and populate gateway_certs
// return -ENOMEM on OOM
// return -ENOENT on missing certificate 
// sets conf->cert_bundle_version
static int md_gateway_certs_load( struct md_syndicate_conf* conf, ms_cert_bundle* gateway_certs ) {

   int rc = 0;
   SG_messages::Manifest cert_bundle;

   rc = md_cert_bundle_load( conf->certs_path, conf->volume_name, &cert_bundle );
   if( rc != 0 ) {
      SG_error("md_cert_bundle_load('%s', '%s') rc = %d\n", conf->certs_path, conf->volume_name, rc );
      return rc;
   }

   // store config information 
   conf->cert_bundle_version = cert_bundle.mtime_sec();

   // each gateway
   for( int i = 1; i < cert_bundle.blocks_size(); i++ ) {

      uint64_t gateway_id = cert_bundle.blocks(i).block_id();
      char gateway_id_buf[100];
      ms::ms_gateway_cert* gateway_cert = NULL;
      struct ms_gateway_cert* loaded_cert = NULL;

      sprintf( gateway_id_buf, "%" PRIu64, gateway_id );

      gateway_cert = SG_safe_new( ms::ms_gateway_cert() );
      if( gateway_cert == NULL ) {
         rc = -ENOMEM;
         goto md_gateway_certs_load_out;
      }
      
      rc = md_gateway_cert_load( conf->certs_path, gateway_id_buf, gateway_cert );
      if( rc != 0 ) {
         SG_error("md_gateway_cert_load('%s', %s) rc = %d\n", conf->certs_path, gateway_id_buf, rc );
         goto md_gateway_certs_load_out;
      }

      loaded_cert = SG_CALLOC( struct ms_gateway_cert, 1 );
      if( loaded_cert == NULL ) {
         rc = -ENOMEM;
         goto md_gateway_certs_load_out;
         return -ENOMEM;
      }

      rc = ms_client_gateway_cert_init( loaded_cert, conf->gateway, gateway_cert );
      if( rc != 0 ) {
         SG_error("ms_gateway_cert_init('%s') rc = %d\n", gateway_cert->name().c_str(), rc );
         goto md_gateway_certs_load_out;
      }

      rc = ms_client_cert_bundle_put( gateway_certs, loaded_cert );
      if( rc != 0 ) {
         SG_error("ms_client_cert_bundle_put('%s') rc = %d\n", gateway_cert->name().c_str(), rc );
         goto md_gateway_certs_load_out;
      }
   } 

md_gateway_certs_load_out:
   if( rc != 0 ) {
      ms_client_cert_bundle_free( gateway_certs );
   }

   return rc;
}


// get the name of our syndicate instance 
// return 0 on success, and set *ret_syndicate_name 
// return -ENOMEM on OOM 
// return -EINVAL on invalid syndicate host
static int md_syndicate_name( struct md_syndicate_conf* conf, char** ret_syndicate_name ) {
 
    int rc = 0;
    char* ms_host = NULL;
    int ms_port = -1;
    char* syndicate_name = NULL;
   
    rc = md_parse_hostname_portnum( conf->metadata_url, &ms_host, &ms_port );
    if( rc != 0 ) {
       return rc;
    }

    // name of syndicate instance 
    syndicate_name = SG_CALLOC( char, strlen(ms_host) + 10 );
    if( syndicate_name == NULL ) {
        
        SG_safe_free( ms_host );
        return -ENOMEM;
    }
    
    if( ms_port <= 0 ) {
        // guess from protocol 
        if( strstr( conf->metadata_url, "https://" ) != NULL ) {
            ms_port = 443;
        }
        else if( strstr( conf->metadata_url, "http://" ) != NULL ) {
            ms_port = 80;
        }
        else {
            
            SG_error("Invalid URL '%s'\n", conf->metadata_url );
            SG_safe_free( ms_host );
            return -EINVAL;
        }
    }
   
    sprintf( syndicate_name, "%s:%d", ms_host, ms_port );    
    SG_safe_free( ms_host );
    
    *ret_syndicate_name = syndicate_name;
    return 0; 
}


// Make sure that all the certs we need for this volume are downloaded and fresh.
// Use our helper programs to go and get them.
// The security of this method is dependent on having a secure way to get and validate *user* certificates.
// Once we can be guaranteed that we have the right user certs, then the right volume and gateway certs will follow.
// Return 0 on success, and populate conf with all volume, user, and gateway runtime information obtained from the certs.
// return -EPERM on validation error 
// return -ENOMEM on OOM
int md_certs_reload( struct md_syndicate_conf* conf, EVP_PKEY** syndicate_pubkey, ms::ms_user_cert* user_cert, ms::ms_user_cert* volume_owner_cert, ms::ms_volume_metadata* volume_cert, ms_cert_bundle* gateway_certs ) {

   int rc = 0;
   int exit_status = 0;
   char* syndicate_name = NULL;
   char* syndicate_pubkey_pem = NULL;
   size_t syndicate_pubkey_pem_len = 0;
   ms::ms_gateway_cert gateway_cert;

   // use our certificate helper to go refresh the cached certificates 
   char* certs_reload_args[] = {
      conf->certs_reload_helper,
      conf->config_file_path,
      conf->ms_username,
      conf->volume_name,
      conf->gateway_name,
      (char*)NULL
   };

   // go fetch 
   rc = SG_proc_subprocess( conf->certs_reload_helper, certs_reload_args, conf->helper_env, NULL, 0, NULL, 0, 0, &exit_status );
   if( rc != 0 ) {

      SG_error("SG_proc_subprocess('%s') rc = %d\n", conf->certs_reload_helper, rc );
      return rc;
   }

   if( exit_status != 0 ) {
      SG_error("Subprocess '%s' exit code %d\n", conf->certs_reload_helper, exit_status );
      return -EPERM;
   }

   // load syndicate public key 
   rc = md_syndicate_name( conf, &syndicate_name );
   if( rc != 0 ) {
      SG_error("md_syndicate_name('%s') rc = %d\n", conf->metadata_url, rc );
      return rc;
   }

   rc = md_syndicate_pubkey_load( conf->syndicate_path, syndicate_name, &syndicate_pubkey_pem, &syndicate_pubkey_pem_len );
   if( rc != 0 ) {
      SG_error("md_syndicate_pubkey_load('%s/%s') rc = %d\n", conf->syndicate_path, syndicate_name, rc );
      SG_safe_free( syndicate_name );
      return rc;
   }
   else {
      SG_safe_free( syndicate_name );
   }

   rc = md_load_pubkey( syndicate_pubkey, syndicate_pubkey_pem, syndicate_pubkey_pem_len );
   SG_safe_free( syndicate_pubkey_pem );

   if( rc != 0 ) {
      SG_error("%s", "Failed to parse Syndicate public key\n");
      return -EPERM;
   }

   // load our gateway cert
   rc = md_gateway_cert_load( conf->certs_path, conf->gateway_name, &gateway_cert );
   if( rc != 0 ) {
      SG_error("%s", "Failed to load our gateway certificate\n");
      return -EPERM;
   }

   conf->gateway = gateway_cert.gateway_id();

   // load our volume cert 
   rc = md_volume_cert_load( conf->certs_path, conf->volume_name, volume_cert );
   if( rc != 0 ) {
      SG_error("%s", "Failed to load our volume certificate\n");
      return -EPERM;
   }

   // sanity checks... 
   if( volume_cert->volume_id() != gateway_cert.volume_id() ) {
      SG_error("Volume/gateway cert mismatch: volume ID %" PRIu64 " != %" PRIu64 "\n", volume_cert->volume_id(), gateway_cert.volume_id() );
      return -EPERM; 
   }

   // load our volume owner cert 
   rc = md_user_cert_load( conf->certs_path, volume_cert->owner_email().c_str(), volume_owner_cert );
   if( rc != 0 ) {
      SG_error("md_user_cert_load('%s', '%s') rc = %d\n", conf->certs_path, volume_cert->owner_email().c_str(), rc );
      return -EPERM;
   }

   // sanity checks... 
   if( volume_cert->owner_id() != volume_owner_cert->user_id() ) {
      SG_error("Volume/owner cert mismatch: volume owner ID %" PRIu64 " != %" PRIu64 "\n", volume_cert->owner_id(), volume_owner_cert->user_id() );
      return -EPERM;
   }

   // load our user cert 
   rc = md_user_cert_load( conf->certs_path, conf->ms_username, user_cert );
   if( rc != 0 ) {
      SG_error("md_user_cert_load('%s', '%s') rc = %d\n", conf->certs_path, conf->ms_username, rc );
      return -EPERM;
   }

   // propagate config info
   conf->owner = gateway_cert.owner_id(); 
   conf->volume = volume_cert->volume_id();
   conf->gateway = gateway_cert.gateway_id();
   conf->gateway_type = gateway_cert.gateway_type();
   conf->volume_version = volume_cert->volume_version();
   conf->gateway_version = gateway_cert.version();
   conf->blocksize = volume_cert->blocksize();
   conf->portnum = gateway_cert.port();

   // load each gateway certificate
   rc = md_gateway_certs_load( conf, gateway_certs );
   if( rc != 0 ) {
      SG_error("md_gateway_certs_load rc = %d\n", rc);
      return -EPERM;
   }

   return 0;
}


// reload the gateway driver, validating it against the hash we have on file.
// return 0 on success, and store the driver to the given cert.
// return -EPERM on validation error 
// return -ENOMEM on OOM 
// return -ENOENT if there is no driver
int md_driver_reload( struct md_syndicate_conf* conf, struct ms_gateway_cert* cert ) {
   
   int rc = 0;
   char* driver_text = NULL;
   size_t driver_text_len = 0;
   int exit_status = 0;
   char driver_hash_str[ 2*SHA256_DIGEST_LENGTH + 1 ];
   
   // get the hash 
   sha256_printable_buf( cert->driver_hash, driver_hash_str );

   // driver_reload args 
   char* driver_reload_args[] = {
      conf->driver_reload_helper,
      conf->config_file_path,
      conf->volume_name,
      conf->gateway_name,
      (char*)NULL
   };

   // go fetch 
   rc = SG_proc_subprocess( conf->driver_reload_helper, driver_reload_args, conf->helper_env, NULL, 0, NULL, 0, 0, &exit_status );
   if( rc != 0 ) {

      SG_error("SG_proc_subprocess('%s') rc = %d\n", conf->driver_reload_helper, rc );
      return rc;
   }

   if( exit_status != 0 ) {
      SG_error("Subprocess '%s' exit code %d\n", conf->driver_reload_helper, exit_status );
      return -EPERM;
   }

   // load up
   rc = md_driver_load( conf->certs_path, driver_hash_str, &driver_text, &driver_text_len );
   if( rc != 0 ) {
      SG_error("md_driver_load('%s', '%s') rc = %d\n", conf->certs_path, driver_hash_str, rc );
      return rc; 
   }

   ms_client_gateway_cert_set_driver( cert, driver_text, driver_text_len );
   return rc;
}


// initialize fields in the config that cannot be loaded from command line options alone.
// (hence 'runtime_init' in the name).
// This includes downloading and loading all files, setting up local storage (if needed), setting up networking (if needed).
// return 0 on success, and populate the volume certificate and gateway certificates
// return -ENONET if we couldn't load a file requested by the config 
// return negative if we couldn't initialize local storage, setup crypto, setup networking, or load a sensitive file securely.
// NOTE: if this fails, the caller must free the md_syndicate_conf structure's fields
static int md_runtime_init( struct md_syndicate_conf* c, EVP_PKEY** syndicate_pubkey, ms::ms_volume_metadata* volume_cert, ms_cert_bundle* gateway_certs ) {

   int rc = 0;
   struct mlock_buf gateway_pkey;
   
   ms::ms_user_cert user_cert;
   ms::ms_user_cert volume_owner_cert;
   struct ms_gateway_cert* gateway_cert = NULL;         // our cert
   
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

   // set up local storage directories
   rc = md_init_local_storage( c );
   if( rc != 0 ) {
      
      SG_error("md_init_local_storage rc = %d\n", rc );
      return rc;
   }
   
   SG_debug("Store local data at %s\n", c->data_root );
   
   if( c->hostname == NULL ) {
      
      // find our hostname
      rc = md_init_server_info( c );
      if( rc != 0 ) {
         
         SG_error("md_init_server_info() rc = %d\n", rc );
         return rc;
      }
      
      SG_debug("Serve data as %s\n", c->hostname );
   }
   
   // go fetch or revalidate our certs, and re-load versioning information
   rc = md_certs_reload( c, syndicate_pubkey, &user_cert, &volume_owner_cert, volume_cert, gateway_certs );
   if( rc != 0 ) {
      
      SG_error("md_certs_load rc = %d\n", rc );
      return rc;
   }
   
   // go fetch or revalidate our driver 
   gateway_cert = md_gateway_cert_find( gateway_certs, c->gateway );
   if( gateway_cert == NULL ) {
      
      SG_error("BUG: no cert on file for us (%" PRIu64 ")\n", c->gateway );
      return -EPERM;
   }
   
   rc = md_driver_reload( c, gateway_cert );
   if( rc != 0 && rc != -ENOENT ) {
      
      SG_error("md_driver_reload rc = %d\n", rc );
      return rc;
   }
   
   // success!
   if( !c->is_client ) {
      
      // load gateway private key, if we need to 
      SG_debug("Load private key for '%s' in '%s'\n", c->gateway_name, c->gateways_path );

      memset( &gateway_pkey, 0, sizeof(struct mlock_buf) );
      rc = md_gateway_private_key_load( c->gateways_path, c->gateway_name, &gateway_pkey );
      if( rc != 0 ) {
      
         SG_error("md_gateway_private_key_load('%s') rc = %d\n", c->gateway_name, rc );
         return rc;
      }
      
      // load it up 
      c->gateway_key = (char*)gateway_pkey.ptr;
      c->gateway_key_len = gateway_pkey.len;
      
      memset( &gateway_pkey, 0, sizeof(struct mlock_buf) );
      
      // non-anonymous gateway has a user public key
      if( c->user_pubkey_pem != NULL ) {
         SG_safe_free( c->user_pubkey_pem );
      }
      
      c->user_pubkey_pem = SG_strdup_or_null( user_cert.public_key().c_str() );
      c->user_pubkey_pem_len = user_cert.public_key().size();
      
      if( c->user_pubkey_pem == NULL ) {
         return -ENOMEM;
      }
      
      if( c->user_pubkey != NULL ) {
         EVP_PKEY_free( c->user_pubkey );
      }
      
      rc = md_load_pubkey( &c->user_pubkey, c->user_pubkey_pem, c->user_pubkey_pem_len );
      if( rc < 0 ) {
         
         SG_error("md_load_pubkey('%s') rc = %d\n", c->ms_username, rc );
         return rc;
      }
      
      c->owner = user_cert.user_id();
   }
   else {
      
      // anonymous gateway
      c->owner = SG_USER_ANON;
   }
   
   // every volume has a public key
   if( c->volume_pubkey_pem != NULL ) {
      SG_safe_free( c->volume_pubkey_pem );
   }
   
   c->volume_pubkey_pem = SG_strdup_or_null( volume_owner_cert.public_key().c_str() );
   c->volume_pubkey_pem_len = volume_owner_cert.public_key().size();
   
   if( c->volume_pubkey_pem == NULL ) {
      return -ENOMEM;
   }
   
   if( c->volume_pubkey != NULL ) {
      EVP_PKEY_free( c->volume_pubkey );
      c->volume_pubkey = NULL;
   }
   
   rc = md_load_pubkey( &c->volume_pubkey, c->volume_pubkey_pem, c->volume_pubkey_pem_len );
   if( rc < 0 ) {
      
      SG_error("md_load_pubkey('%s') rc = %d\n", c->volume_name, rc );
      return rc;
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
      
      if( strcmp(key, SG_CONFIG_VOLUMES_PATH) == 0) {
         
         // path to volume information 
         if( conf->volumes_path != NULL ) {
             SG_safe_free( conf->volumes_path );
         }

         conf->volumes_path = SG_strdup_or_null( value );
         if( conf->volumes_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_GATEWAYS_PATH) == 0 ) {
         
         // path to gateway information 
         if( conf->gateways_path != NULL ) {
             SG_safe_free( conf->gateways_path );
         }

         conf->gateways_path = SG_strdup_or_null( value );
         if( conf->gateways_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_USERS_PATH) == 0 ) {
         
         // path to user information 
         if( conf->users_path != NULL ) {
             SG_safe_free( conf->users_path );
         }

         conf->users_path = SG_strdup_or_null( value );
         if( conf->users_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_DRIVERS_PATH) == 0 ) {
         
         // path to driver storage 
         if( conf->drivers_path != NULL ) {
             SG_safe_free( conf->drivers_path );
         }

         conf->drivers_path = SG_strdup_or_null( value );
         if( conf->drivers_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_SYNDICATE_PATH ) == 0 ) {
          
         // path to syndicate pubkeys 
         if( conf->syndicate_path != NULL ) {
             SG_safe_free( conf->syndicate_path );
         }

         conf->syndicate_path = SG_strdup_or_null( value );
         if( conf->syndicate_path == NULL ) {
            return -ENOMEM;
         }
      }

      else if( strcmp( key, SG_CONFIG_CERTS_ROOT ) == 0 ) {

         // path to certs cache root
         if( conf->certs_root != NULL ) {
             SG_safe_free( conf->certs_root );
         }

         conf->certs_root = SG_strdup_or_null( value );
         if( conf->certs_root == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MS_URL ) == 0 ) {
         // metadata publisher URL
         if( conf->metadata_url != NULL ) {
            SG_safe_free( conf->metadata_url );
         }

         conf->metadata_url = SG_strdup_or_null( value );
         if( conf->metadata_url == NULL ) {
            return -ENOMEM;
         }
      }
           
      else if( strcmp( key, SG_CONFIG_DATA_ROOT ) == 0 ) {
         // data root
         size_t len = strlen( value );
         if( len == 0 ) {
            return -EINVAL;
         }

         if( conf->data_root != NULL ) {
            SG_safe_free( conf->data_root );
         }
         
         if( value[len-1] != '/' ) {
            // must end in /
            conf->data_root = SG_CALLOC( char, len+2 );
            if( conf->data_root == NULL ) {
               return -ENOMEM;
            }
            
            sprintf( conf->data_root, "%s/", value );
         }
         else {
            
            conf->data_root = SG_strdup_or_null( value );
            if( conf->data_root == NULL ) {
               return -ENOMEM;
            }
         }
      }
      
      else if( strcmp( key, SG_CONFIG_LOGS_PATH ) == 0 ) {
         // logfile path
         if( conf->logs_path != NULL ) {
            SG_safe_free( conf->logs_path );
         }

         conf->logs_path = SG_strdup_or_null( value );
         if( conf->logs_path == NULL ) {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MS_USERNAME ) == 0 ) {
         // metadata server username
         if( conf->ms_username != NULL ) {
            SG_safe_free( conf->ms_username );
         }

         conf->ms_username = SG_strdup_or_null( value );
         if( conf->ms_username == NULL ) {
            return -ENOMEM;
         }
      }
      
      else {
         
         SG_error("Unrecognized option '%s' in section '%s'\n", key, section);
         return 0;
      }
   }
   
   else if( strcmp(section, "helpers") == 0 ) {
       
      // helpers section 
      if( strcmp( key, SG_CONFIG_ENVAR ) == 0 ) {
         
         // environment variable to feed into the helpers 
         if( conf->helper_env == NULL ) {
             
             conf->helper_env = SG_CALLOC( char*, 2 );
             if( conf->helper_env == NULL ) {
                 return -ENOMEM;
             }
             
             char* value_dup = strdup( value );
             if( value_dup == NULL ) {
                 return -ENOMEM;
             }
             
             conf->helper_env[0] = value_dup;
             conf->num_helper_envs = 1;
             conf->max_helper_envs = 1;
         }
         else {
             
             if( conf->num_helper_envs >= conf->max_helper_envs ) {
                 
                 conf->max_helper_envs *= 2;
                 char** new_helper_env = (char**)realloc( conf->helper_env, sizeof(char*) * conf->max_helper_envs );
                 if( new_helper_env == NULL ) {
                     return -ENOMEM;
                 }
                 
                 conf->helper_env = new_helper_env;
             }
        
             char* value_dup = strdup( value );
             if( value_dup == NULL ) {
                 return -ENOMEM;
             }
             
             conf->helper_env[ conf->num_helper_envs ] = value_dup;
             conf->num_helper_envs++;
         }
      }

      else if( strcmp( key, SG_CONFIG_CERTS_RELOAD_HELPER ) == 0 ) {

         if( conf->certs_reload_helper != NULL ) {
            SG_safe_free( conf->certs_reload_helper );
         }

         conf->certs_reload_helper = SG_strdup_or_null( value );
         if( conf->certs_reload_helper == NULL ) {
            return -ENOMEM;
         }

         rc = access( conf->certs_reload_helper, X_OK );
         if( rc < 0 ) {

            rc = -errno;
            SG_error("Cannot access '%s' as an executable, rc = %d\n", conf->certs_reload_helper, rc );
            return rc;
         }
      }

      else if( strcmp( key, SG_CONFIG_DRIVER_RELOAD_HELPER ) == 0 ) {

         if( conf->driver_reload_helper != NULL ) {
            SG_safe_free( conf->driver_reload_helper );
         }

         conf->driver_reload_helper = SG_strdup_or_null( value );
         if( conf->driver_reload_helper == NULL ) {
            return -ENOMEM;
         }

         rc = access( conf->driver_reload_helper, X_OK );
         if( rc < 0 ) {

            rc = -errno;
            SG_error("Cannot access '%s' as an executable, rc = %d\n", conf->driver_reload_helper, rc );
            return rc;
         }
      }
   }
   
   else if( strcmp(section, "gateway") == 0 ) {
      
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
         if( strcasecmp( value, "true" ) == 0 || strcasecmp( value, "yes" ) == 0 || strcasecmp( value, "y" ) == 0 ) {
            conf->verify_peer = 1;
         }
         else if( strcasecmp( value, "false" ) == 0 || strcasecmp( value, "no" ) == 0 || strcasecmp( value, "n" ) == 0 ) {
            conf->verify_peer = 0;
         }
         else {
                
            rc = md_conf_parse_long( value, &val );
            if( rc == 0 ) {
                conf->verify_peer = (val != 0);
            }
            else {
                return -EINVAL;
            }
         }
      }
      
      else if( strcmp( key, SG_CONFIG_GATHER_STATS ) == 0 ) {
         // gather statistics?
         if( strcasecmp( value, "true" ) == 0 || strcasecmp( value, "yes" ) == 0 || strcasecmp( value, "y" ) == 0 ) {
            conf->gather_stats = 1;
         }
         else if( strcasecmp( value, "false" ) == 0 || strcasecmp( value, "no" ) == 0 || strcasecmp( value, "n" ) == 0 ) {
            conf->gather_stats = 0;
         }
         else {
            
            rc = md_conf_parse_long( value, &val );
            if( rc == 0 ) {
                conf->gather_stats = (val != 0);
            }
            else {
                return -EINVAL;
            }
         }
      }
      
      else if( strcmp( key, SG_CONFIG_PUBLIC_URL ) == 0 ) {
         
         // public content URL
         size_t len = strlen( value );
         if( len == 0 ) {
            return -EINVAL;
         }
         
         if( conf->content_url != NULL ) {
            SG_safe_free( conf->content_url );
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
      
      else if( strcmp( key, SG_CONFIG_DEBUG_LEVEL ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            md_debug( conf, (int)val );
         }
         else {
            return -ENOMEM;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_DEBUG_LOCK ) == 0 ) {
         
         if( strcasecmp( value, "true" ) == 0 || strcasecmp( value, "yes" ) == 0 || strcasecmp( value, "y" ) == 0 ) {
            conf->debug_lock = 1;
         }
         else if( strcasecmp( value, "false" ) == 0 || strcasecmp( value, "no" ) == 0 || strcasecmp( value, "n" ) == 0 ) {
            conf->debug_lock = 0;
         }
         else {
            
            rc = md_conf_parse_long( value, &val );
            if( rc == 0 ) {
               conf->debug_lock = (val != 0 );
            }
            else {
               return -EINVAL;
            }
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
      
      else if( strcmp( key, SG_CONFIG_MAX_READ_RETRY ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->max_read_retry = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MAX_WRITE_RETRY ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->max_write_retry = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MAX_METADATA_READ_RETRY ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->max_metadata_read_retry = val;
         }
         else {
            return -EINVAL;
         }
      }
      
      else if( strcmp( key, SG_CONFIG_MAX_METADATA_WRITE_RETRY ) == 0 ) {
         rc = md_conf_parse_long( value, &val );
         if( rc == 0 ) {
            conf->max_metadata_write_retry = val;
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
   
   // store expanded config file path 
   char* expanded_path = NULL;
   size_t expanded_path_len = 0;
    
   rc = md_expand_path( conf_path, &expanded_path, &expanded_path_len );
   if( rc != 0 ) {
        
       SG_error("md_expand_path('%s') rc = %d\n", conf_path, rc );
       return rc;
   } 
   
   SG_debug("Read config from '%s'\n", expanded_path );

   FILE* f = fopen( expanded_path, "r" );
   if( f == NULL ) {
      
      rc = -errno;
      SG_error("fopen('%s') rc = %d\n", expanded_path, rc );
      
      SG_safe_free( expanded_path );
      return rc;
   }
   
   rc = ini_parse_file( f, md_conf_ini_parser, conf );
   if( rc != 0 ) {
      SG_error("ini_parse_file('%s') rc = %d\n", expanded_path, rc );
   }
   
   fclose( f );
   
   if( rc == 0 ) {
      
      if( conf->config_file_path != NULL ) {
         SG_safe_free( conf->config_file_path );
      }
      
      conf->config_file_path = expanded_path;
   }
   else {
       
      SG_safe_free( expanded_path );
   }
   
   return rc;
}


// free all memory associated with a server configuration
int md_free_conf( struct md_syndicate_conf* conf ) {     
   
   void* to_free[] = {
      (void*)conf->config_file_path,
      (void*)conf->volumes_path,
      (void*)conf->gateways_path,
      (void*)conf->users_path,
      (void*)conf->drivers_path,
      (void*)conf->metadata_url,
      (void*)conf->logs_path,
      (void*)conf->content_url,
      (void*)conf->data_root,
      (void*)conf->certs_root,
      (void*)conf->certs_path,
      (void*)conf->syndicate_path,
      (void*)conf->ms_username,
      (void*)conf->certs_reload_helper,
      (void*)conf->driver_reload_helper,
      (void*)conf->user_pubkey_pem,
      (void*)conf->gateway_name,
      (void*)conf->volume_name,
      (void*)conf->volume_pubkey_pem,
      (void*)conf->hostname,
      (void*)conf->driver_exec_path,
      (void*)conf
   };
   
   // things that are mlock'ed, and need to be munlock'ed
   void* mlocked[] = {  
      (void*)conf->gateway_key,
      (void*)conf
   };
   
   size_t mlocked_len[] = {
      conf->gateway_key_len,
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
   
   if( conf->volume_pubkey != NULL ) {
      EVP_PKEY_free( conf->volume_pubkey );
   }
   
   if( conf->user_pubkey != NULL ) {
      EVP_PKEY_free( conf->user_pubkey );
   }
   
   if( conf->helper_env != NULL ) {
      SG_FREE_LIST( conf->helper_env, free );
   }

   if( conf->driver_roles != NULL ) {
      SG_FREE_LISTV( conf->driver_roles, conf->num_driver_roles, free );
   }
   
   memset( conf, 0, sizeof(struct md_syndicate_conf) );
      
   return 0;
}


// set the driver parameters 
// return 0 on success, and populate *conf
// return -ENOMEM on OOM
int md_conf_set_driver_params( struct md_syndicate_conf* conf, char const* driver_exec_path, char const** driver_roles, size_t num_roles ) {
   
   char* driver_exec_path_dup = SG_strdup_or_null( driver_exec_path );
   char** driver_roles_dup = SG_CALLOC( char*, num_roles );

   if( driver_roles_dup == NULL || (driver_exec_path_dup == NULL && driver_exec_path != NULL) ) {
      SG_safe_free( driver_roles_dup );
      SG_safe_free( driver_exec_path_dup );
      return -ENOMEM;
   }

   for( size_t i = 0; i < num_roles; i++ ) {
      driver_roles_dup[i] = SG_strdup_or_null( driver_roles[i] );
      if( driver_roles_dup[i] == NULL && driver_roles[i] != NULL ) {
            
         SG_FREE_LISTV( driver_roles_dup, num_roles, free );
         SG_safe_free( driver_exec_path_dup );
         return -ENOMEM;
      }
   }
   
   if( conf->driver_exec_path != NULL ) {
      SG_safe_free( conf->driver_exec_path );
   }

   if( conf->driver_roles != NULL ) {
      SG_FREE_LISTV( conf->driver_roles, num_roles, free );
   }

   conf->driver_exec_path = driver_exec_path_dup;
   conf->driver_roles = driver_roles_dup;
   conf->num_driver_roles = num_roles;

   return 0;
}


// destroy an md entry
void md_entry_free( struct md_entry* ent ) {
   if( ent->name != NULL ) {
      SG_safe_free( ent->name );
   }
   if( ent->xattr_hash != NULL ) {
      SG_safe_free( ent->xattr_hash );
   }
   if( ent->ent_sig != NULL ) {
      SG_safe_free( ent->ent_sig );
   }
   
   memset( ent, 0, sizeof(struct md_entry) );
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
   unsigned char* new_ent_sig = NULL;
   unsigned char* xattr_hash = NULL;
   
   if( (src->name != NULL && new_name == NULL) ) {
      
      // OOM
      SG_safe_free( new_name );
      return -ENOMEM;
   }
   
   // copy everything else 
   memcpy( ret, src, sizeof(md_entry) );
   
   if( src->xattr_hash != NULL ) {
      
      xattr_hash = SG_CALLOC( unsigned char, SHA256_DIGEST_LENGTH );
      if( xattr_hash == NULL ) {
         SG_safe_free( new_name );
         return -ENOMEM;
      }
      
      memcpy( xattr_hash, src->xattr_hash, SHA256_DIGEST_LENGTH );
   }
   
   if( src->ent_sig != NULL ) {
      
       new_ent_sig = SG_CALLOC( unsigned char, src->ent_sig_len );
       if( new_ent_sig == NULL ) {
          SG_safe_free( xattr_hash );
          SG_safe_free( new_name );
          return -ENOMEM;
       }
       
       memcpy( new_ent_sig, src->ent_sig, src->ent_sig_len );
   }
   
   ret->name = new_name;
   ret->ent_sig = new_ent_sig;
   ret->ent_sig_len = src->ent_sig_len;
   ret->xattr_hash = xattr_hash;
   
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
int md_start_thread( pthread_t* th, void* (*thread_func)(void*), void* arg, bool detach ) {

   // start up a thread to listen for connections
   pthread_attr_t attrs;
   int rc;
   
   rc = pthread_attr_init( &attrs );
   if( rc != 0 ) {
      SG_error( "pthread_attr_init rc = %d\n", rc);
      return -1;   // problem
   }

   if( detach ) {
      rc = pthread_attr_setdetachstate( &attrs, PTHREAD_CREATE_DETACHED );    // make a detached thread--we won't join with it
      if( rc != 0 ) {
         SG_error( "pthread_attr_setdetachstate rc = %d\n", rc );
         return -1;
      }
   }
   
   rc = pthread_create( th, &attrs, thread_func, arg );
   if( rc != 0 ) {
      SG_error( "pthread_create rc = %d\n", rc );
      return -1;
   }
   
   return rc;;
}

// extract hostname and port number from a URL 
int md_parse_hostname_portnum( char const* url, char** hostname, int* portnum ) {
    
   char const* host_ptr = NULL;
   char const* port_ptr = NULL;
   char* tmp = NULL;
   
   // advance past ://
   host_ptr = strstr( url, "://" );
   if( host_ptr == NULL ) {
       host_ptr = url;
   }
   
   host_ptr += 3;
   
   // advance to :
   port_ptr = strstr( host_ptr, ":" );
   if( port_ptr == NULL ) {
       
       // no port 
       *portnum = -1;
   }
   else {
       
       port_ptr++;
   }
   
   size_t hostname_len = ((uint64_t)(port_ptr) - (uint64_t)(host_ptr)) / sizeof(char);
   if( hostname_len > 0 ) {
      // skip ':'
      hostname_len--;
   } 
   *hostname = SG_CALLOC( char, hostname_len + 1 );
   if( *hostname == NULL ) {
       return -ENOMEM;
   }
   
   strncpy( *hostname, host_ptr, hostname_len );
   *portnum = strtol( port_ptr, &tmp, 10 );
   
   if( tmp == port_ptr ) {
       // invalid port number 
       *portnum = -1;
   }
   
   return 0;
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

// remove the last token of a string by setting 
// the last instance of delim to '\0'
// return a pointer to the chomped string if delim was found.
// return NULL otherwise
char* md_rchomp( char* str, char delim ) {
   char* ptr = strrchr( str, delim );
   if( ptr == NULL ) {
      return NULL;
   }
   
   *ptr = '\0';
   return (ptr + 1);
}


// convert an md_entry to an ms_entry
// return 0 on success
// return -ENOMEM on OOM
int md_entry_to_ms_entry( ms::ms_entry* msent, struct md_entry* ent ) {
   
   try {
         
      if( ent->parent_id != (uint64_t)(-1) ) {
         msent->set_parent_id( ent->parent_id );
      }
      
      if( ent->xattr_hash != NULL ) {
         msent->set_xattr_hash( string((char*)ent->xattr_hash, SHA256_DIGEST_LENGTH) );
      }
      
      if( ent->ent_sig != NULL ) {
         msent->set_signature( string((char*)ent->ent_sig, ent->ent_sig_len) );
      }
      else {
         msent->set_signature( string("") );
      }
      
      if( ent->name != NULL ) {
         msent->set_name( string( ent->name ) );
      }
      else {
         msent->set_name( string("") );
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
      msent->set_num_children( ent->num_children );
      msent->set_capacity( ent->capacity );
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
   
   if( msent.has_xattr_hash() ) {
      ent->xattr_hash = SG_CALLOC( unsigned char, msent.xattr_hash().size() );
      if( ent->xattr_hash == NULL ) {
         
         md_entry_free( ent );
         return -ENOMEM;
      }
      
      memcpy( ent->xattr_hash, msent.xattr_hash().data(), msent.xattr_hash().size() );
   }
   
   if( msent.has_signature() ) {
      ent->ent_sig = SG_CALLOC( unsigned char, msent.signature().size() );
      if( ent->ent_sig == NULL ) {
         
         md_entry_free( ent );
         return -ENOMEM;
      }
      
      memcpy( ent->ent_sig, msent.signature().data(), msent.signature().size() );
      ent->ent_sig_len = msent.signature().size();
   }
   
   if( msent.has_parent_id() ) {
      ent->parent_id = msent.parent_id();
   }
   else {
      ent->parent_id = SG_INVALID_FILE_ID;
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

   return 0;
}


// verify the MS's signature over an ms_entry
// return 0 if the given gateway is known to us, and matches the signature and user
// return -EPERM if the gateway signature is invalid or missing
// return -ENOMEM on OOM 
static int ms_entry_verify_ms_signature( struct ms_client* ms, ms::ms_entry* msent ) {
   
   // NOTE: derived from md_verify template function; see if we can't use it here too (address ms_signature vs signature field)
   if( !msent->has_ms_signature() ) {
      SG_error("%s\n", "missing MS signature");
      return -EINVAL;
   }
   
   // get the signature
   size_t sigb64_len = msent->ms_signature().size();
   int rc = 0;
   
   if( sigb64_len == 0 ) {
      // malformed message
      SG_error("%s\n", "invalid signature length");
      return -EINVAL;
   }
   
   char* sigb64 = SG_CALLOC( char, sigb64_len + 1 );
   if( sigb64 == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( sigb64, msent->ms_signature().data(), sigb64_len );
   
   try {
      msent->set_ms_signature( "" );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( sigb64 );
      return -ENOMEM;
   }
   
   string bits;
   try {
      msent->SerializeToString( &bits );
   }
   catch( exception e ) {
      try {
         // revert
         msent->set_ms_signature( string(sigb64) );
         rc = -EINVAL;
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
      }
      
      free( sigb64 );
      return rc;
   }
   
   // verify the signature
   rc = md_verify_signature( ms->syndicate_pubkey, bits.data(), bits.size(), sigb64, sigb64_len );
   
   // revert
   try {
      msent->set_ms_signature( string(sigb64) );
   }
   catch( bad_alloc& ba ) {
      SG_safe_free( sigb64 );
      return -ENOMEM;
   }
   
   SG_safe_free( sigb64 );

   if( rc != 0 ) {
      SG_error("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}

// verify an ms_entry: the coordinator must have signed it
// return 0 if the given gateway is known to us, and matches the signature and user
// return -EAGAIN if the gateway is not on file 
// return -EPERM if the gateway signature is invalid, or the gateway is not owned by the same user
// return -ENOMEM if OOM
int ms_entry_verify( struct ms_client* ms, ms::ms_entry* msent ) {
   
   int rc = 0;
   struct ms_gateway_cert* cert = NULL;
   EVP_PKEY* pubkey = NULL;
   
   // preserve but clear fields set by the MS 
   uint64_t num_children = msent->num_children();
   uint64_t generation = msent->generation();
   uint64_t capacity = msent->capacity();
   
   // if root, preserve but clear these fields set by the MS
   // (in order to verify volume owner signature)
   int64_t write_nonce = msent->write_nonce();
   int64_t xattr_nonce = msent->xattr_nonce();
   string ms_signature = msent->ms_signature();
   string xattr_hash;
   bool has_xattr_hash = false;
   
   if( msent->has_xattr_hash() ) {
      xattr_hash = msent->xattr_hash();
      has_xattr_hash = true;
   }
   
   // check against the coordinator's key, and make sure 
   // the coordinator is owned by the user the entry claims to come from.
   ms_client_config_rlock( ms );
   
   if( msent->type() == MD_ENTRY_FILE || (msent->type() == MD_ENTRY_DIR && msent->file_id() != 0) ) {
       
       // file
       SG_debug("Check signature of %" PRIX64 " from gateway %" PRIu64 "\n", msent->file_id(), msent->coordinator() );
       
       cert = ms_client_get_gateway_cert( ms, msent->coordinator() );
       if( cert == NULL ) {
                
           ms_client_config_unlock( ms );
           return -EAGAIN;
       }
       
       pubkey = ms_client_gateway_pubkey( cert );
   }
   else {
       
       // root directory
       SG_debug("Check signature of %" PRIX64 " from volume owner\n", msent->file_id());
       pubkey = ms->volume->volume_public_key;
   }
   
   if( msent->type() == MD_ENTRY_DIR ) {
       
       // restore original directory values
       // xattrs are trusted for directories only if the client trusts the MS.
       msent->set_write_nonce( 1 );
       msent->set_xattr_nonce( 1 );
       msent->clear_xattr_hash();
   }
   
   // default initial values, put in place by the file/directory creator
   msent->set_num_children( 0 );
   msent->set_generation( 1 );
   msent->set_capacity( 16 );
   msent->clear_ms_signature();

   SG_debug("%s", "Verify:\n");
   msent->PrintDebugString();
   
   rc = md_verify< ms::ms_entry >( pubkey, msent );
    
   // restore
   msent->set_num_children( num_children );
   msent->set_generation( generation );
   msent->set_capacity( capacity );
   msent->set_ms_signature( ms_signature );
   
   if( msent->type() == MD_ENTRY_DIR ) {
       
       // restore
       msent->set_write_nonce( write_nonce );
       msent->set_xattr_nonce( xattr_nonce );
       
       if( has_xattr_hash ) {
          msent->set_xattr_hash( xattr_hash );
       }
   }
   
   if( rc != 0 ) {
       
       SG_error("md_verify< ms::ms_entry >( %" PRIX64 " ) rc = %d\n", msent->file_id(), rc );
       ms_client_config_unlock( ms );
       return rc;
   }
   
   // check owner 
   if( cert != NULL ) {
        if( msent->owner() != cert->user_id ) {
                
            SG_error("Entry %" PRIX64 " claims to come from gateway %" PRIu64 ", which is not owned by user %" PRIu64 "\n", msent->file_id(), cert->gateway_id, cert->user_id );
            rc = -EPERM;
        }
   }
   else {
       
       // must be volume owner
       if( msent->owner() != ms->volume->volume_owner_id ) {
           
           SG_error("Entry %" PRIX64 " claims to come from user %" PRIu64 ", but it is owned by user %" PRIu64 "\n", msent->file_id(), msent->owner(), ms->volume->volume_owner_id );
           rc = -EPERM;
       }
   }
   
   // if directory, check the MS's signature over the MS-maintained fields
   if( rc == 0 && msent->type() == MD_ENTRY_DIR ) {
      
       rc = ms_entry_verify_ms_signature( ms, msent );
       if( rc != 0 ) {
           
           SG_error("ms_entry_verify_ms_signature( %" PRIX64 " ) rc = %d\n", msent->file_id(), rc );
       }
   }
        
   ms_client_config_unlock( ms );
   
   return rc;
}


// sign an md_entry 
// return 0 on success, and fill in *sig and *sig_len 
// return -ENOMEM on OOM 
int md_entry_sign2( EVP_PKEY* privkey, struct md_entry* ent, unsigned char** sig, size_t* sig_len, char const* file, int lineno ) {
   
   ms::ms_entry msent;
   int rc = 0;
   
   rc = md_entry_to_ms_entry( &msent, ent );
   if( rc != 0 ) {
      return rc;
   }
   
   // NOTE: these three fields have to be the same for directories; the MS will sign the values it fills in here.
   msent.set_num_children( 0 );
   msent.set_generation( 1 );
   msent.set_capacity( 16 ); 

   // NOTE: xattrs are trusted on directories, but not so for files
   if( ent->type == MD_ENTRY_DIR ) {
       msent.set_xattr_nonce( 1 );
       msent.clear_xattr_hash();
   }
  
   SG_debug("from %s:%d, sign:\n", file, lineno);
   msent.PrintDebugString();

   rc = md_sign< ms::ms_entry >( privkey, &msent );
   if( rc != 0 ) {
      
      return rc;
   }
   
   *sig_len = msent.signature().size();
   *sig = SG_CALLOC( unsigned char, msent.signature().size() );
   if( *sig == NULL ) {
      
      return -ENOMEM;
   }
  
   memcpy( *sig, msent.signature().data(), msent.signature().size() );
   SG_debug("Signature is %s\n", msent.signature().c_str() ); 
   return 0;
}


// expand a path, with wordexp 
// return 0 on success, and set *expanded and *expanded_len 
// return -ENOMEM on OOM
// return -EINVAL on failure to parse
int md_expand_path( char const* path, char** expanded, size_t* expanded_len ) {
    
    int rc = 0;
    wordexp_t wp;
    
    rc = wordexp( path, &wp, WRDE_UNDEF );
    if( rc != 0 ) {
        SG_error("wordexp('%s') rc = %d\n", path, rc );
        return -EINVAL;
    }
    
    // join all pieces 
    *expanded_len = 0;
    for( unsigned int i = 0; i < wp.we_wordc; i++ ) {
        *expanded_len += strlen(wp.we_wordv[i]);
    }
    
    *expanded_len += 1;
    
    *expanded = SG_CALLOC( char, *expanded_len );
    if( *expanded == NULL ) {
        return -ENOMEM;
    }
    
    for( unsigned int i = 0; i < wp.we_wordc; i++ ) {
        
        strcat( *expanded, wp.we_wordv[i] );
    }
    
    wordfree( &wp );
    
    return 0;
}
   
// initialize Syndicate
// return 0 on success 
// if this fails, the caller should shut down the library and free conf
static int md_init_common( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts, bool is_client ) {
   
   char const* ms_url = opts->ms_url;
   char const* volume_name = opts->volume_name;
   char const* gateway_name = opts->gateway_name;
   char const* username = opts->username;
   char const* config_path = opts->config_file;
   char* certs_path = NULL;
   char* expanded_path = NULL;
   size_t expanded_path_len = 0;
  
   if( config_path == NULL ) {
       
       config_path = conf->config_file_path;
       if( config_path == NULL ) {
           
           config_path = SG_DEFAULT_CONFIG_PATH;
       }
   }
       
   ms::ms_volume_metadata* volume_cert = NULL;
   ms_cert_bundle* gateway_certs = NULL;
   ms_cert_bundle* old_gateway_certs = NULL;
   EVP_PKEY* syndicate_pubkey = NULL;
   
   // before we load anything, disable core dumps (i.e. to keep private keys from leaking)
   bool disable_core_dumps = true;
   
   int rc = 0;
   
   // early exception handling 
   set_terminate( md_uncaught_exception_handler );
   
   // early debugging 
   if( opts->debug_level > md_get_debug_level() ) {
       md_set_debug_level( opts->debug_level );
   }
   
   md_set_error_level( SG_MAX_VERBOSITY );
   
   conf->is_client = is_client;
   
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
   
   rc = md_util_init();
   if( rc != 0 ) {
      SG_error("md_util_init rc = %d\n", rc );
      return rc;
   }
   
   // populate the config with command-line opts
   rc = 0;
   
   MD_SYNDICATE_CONF_OPT( *conf, volume_name, volume_name, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, metadata_url, ms_url, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, ms_username, username, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, gateway_name, gateway_name, rc );
   if( rc != 0 ) {
      return rc;
   }
   
   rc = md_expand_path( config_path, &expanded_path, &expanded_path_len );
   if( rc != 0 ) {
      return rc;
   }
   
   MD_SYNDICATE_CONF_OPT( *conf, config_file_path, expanded_path, rc );
   SG_safe_free( expanded_path );
   if( rc != 0 ) {
      return rc;
   }

   certs_path = SG_CALLOC( char, strlen(conf->certs_root) + 1 + strlen(volume_name) + 1 + strlen(gateway_name) + 2 );
   if( certs_path == NULL ) {
      return -ENOMEM;
   }

   sprintf(certs_path, "%s/%s/%s", conf->certs_root, volume_name, gateway_name );
   MD_SYNDICATE_CONF_OPT( *conf, certs_path, certs_path, rc );
   SG_safe_free( certs_path );
   if( rc != 0 ) {
      return rc;
   } 

   SG_debug("Certs path: '%s'\n", conf->certs_path );

   // allocate certs...
   volume_cert = SG_safe_new( ms::ms_volume_metadata );
   if( volume_cert == NULL ) {
      return -ENOMEM;
   }
   
   gateway_certs = SG_safe_new( ms_cert_bundle );
   if( gateway_certs == NULL ) {
      
      SG_safe_delete( volume_cert );
      return -ENOMEM;
   }
   
   // set up runtime information, and get our certs
   rc = md_runtime_init( conf, &syndicate_pubkey, volume_cert, gateway_certs );
   if( rc != 0 ) {
      SG_error("md_runtime_init() rc = %d\n", rc );
      
      SG_safe_delete( volume_cert );
      SG_safe_delete( gateway_certs );
      return rc;
   }
   
   // validate the config
   rc = md_check_conf( conf );
   if( rc != 0 ) {
      
      SG_error("md_check_conf rc = %d\n", rc );
      EVP_PKEY_free( syndicate_pubkey );
      SG_safe_delete( volume_cert );
      SG_safe_delete( gateway_certs );
      return rc;
   }
   
   // setup the MS client
   rc = ms_client_init( client, conf, syndicate_pubkey, volume_cert );
   if( rc != 0 ) {
      
      SG_error("ms_client_init rc = %d\n", rc );
      EVP_PKEY_free( syndicate_pubkey );
      SG_safe_delete( volume_cert );
      SG_safe_delete( gateway_certs );
      return rc;
   }
   
   // pass along the gateway certs...
   old_gateway_certs = ms_client_swap_gateway_certs( client, gateway_certs );
   if( old_gateway_certs != NULL ) {
      
      ms_client_cert_bundle_free( old_gateway_certs );
      SG_safe_delete( old_gateway_certs );
   }
   
   // fill in defaults
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
// exit on error (will be due to memory exhaustion)
int md_default_conf( struct md_syndicate_conf* conf ) {

   size_t path_len = 0;
   int rc = 0;

   memset( conf, 0, sizeof(struct md_syndicate_conf) );
  
   rc = md_expand_path( SG_DEFAULT_VOLUMES_PATH, &conf->volumes_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_GATEWAYS_PATH, &conf->gateways_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_USERS_PATH, &conf->users_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_LOGS_PATH, &conf->logs_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_DRIVERS_PATH, &conf->drivers_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_DATA_ROOT, &conf->data_root, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_SYNDICATE_PATH, &conf->syndicate_path, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   rc = md_expand_path( SG_DEFAULT_CERTS_ROOT, &conf->certs_root, &path_len );
   if( rc != 0 ) {
      exit(1);
   }

   conf->default_read_freshness = 5000;
   conf->default_write_freshness = 0;
   conf->gather_stats = false;

#ifndef _DEVELOPMENT
   conf->verify_peer = true;
#else
   conf->verify_peer = false;
#endif
   
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
   
   conf->gateway_version = -1;
   conf->cert_bundle_version = -1;
   conf->volume_version = -1;
   
   conf->cache_soft_limit = MD_CACHE_DEFAULT_SOFT_LIMIT;
   conf->cache_hard_limit = MD_CACHE_DEFAULT_HARD_LIMIT;

   conf->certs_reload_helper = SG_strdup_or_die( SG_DEFAULT_CERTS_RELOAD_HELPER );
   conf->driver_reload_helper = SG_strdup_or_die( SG_DEFAULT_DRIVER_RELOAD_HELPER );

   // create a NULL terminated empty array (in length 1)
   conf->helper_env = SG_CALLOC( char*, 1 );
   if( conf->helper_env == NULL ) {
      exit(1);
   }

   return 0;
}


// check a configuration structure to see that it has everything we need.
// print warnings too
int md_check_conf( struct md_syndicate_conf* conf ) {
   
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
   if( conf->gateway_name == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "gateway name" );
   }
   if( conf->gateway_key == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "gateway private key" );
   }
   if( conf->user_pubkey == NULL && !conf->is_client ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "user certificate");
   }
   if( conf->volume_pubkey == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "volume owner certificate");
   }
   if( conf->volume_name == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "volume name" );
   }
   
   if( conf->drivers_path == NULL ) {
      rc = -EINVAL;
      fprintf(stderr, err_fmt, "drivers path");
   }
    
   return rc;
}


// convert an md_entry to string 
int md_entry_to_string( struct md_entry* ent, char** data ) {
    
    int rc = 0;
    size_t buf_size = 0;
    char* buf = NULL;
    char* sigb64 = NULL;
    char xattr_hash_buf[ 2*SHA256_DIGEST_LENGTH + 1 ];
    
    buf_size += strlen("type:     \n") + 16 + 1;
    buf_size += strlen("name:     \n" ) + SG_strlen_or_zero( ent->name );
    buf_size += strlen("file_id:  \n" ) + 16 + 1;      // file ID 
    buf_size += strlen("ctime_s:  \n" ) + 21 + 1;      // ctime_sec 
    buf_size += strlen("ctime_ns: \n" ) + 21 + 1;      // ctime_nsec 
    buf_size += strlen("mtime_s:  \n" ) + 11 + 1;      // mtime_sec
    buf_size += strlen("mtime_ns: \n" ) + 11 + 1;      // mtime_nsec
    buf_size += strlen("mmtim_s:  \n" ) + 21 + 1;      // manifest_mtime_sec 
    buf_size += strlen("mmtim_ns: \n" ) + 11 + 1;      // manifest_mtime_nsec
    buf_size += strlen("write_n:  \n" ) + 21 + 1;      // write nonce 
    buf_size += strlen("xattr_n:  \n" ) + 21 + 1;      // xattr nonce 
    buf_size += strlen("version:  \n" ) + 21 + 1;      // version
    buf_size += strlen("max_read: \n" ) + 11 + 1;      // max read freshness 
    buf_size += strlen("max_wrte: \n" ) + 11 + 1;      // max write freshness
    buf_size += strlen("owner:    \n" ) + 21 + 1;      // owner ID 
    buf_size += strlen("coord:    \n" ) + 21 + 1;      // coordinator 
    buf_size += strlen("volume:   \n" ) + 21 + 1;      // volume ID 
    buf_size += strlen("mode:     \n" ) + 5 + 1;       // mode 
    buf_size += strlen("size:     \n" ) + 21 + 1;      // size 
    buf_size += strlen("error:    \n" ) + 11 + 1;      // error code 
    buf_size += strlen("gen:      \n" ) + 21 + 1;      // generation 
    buf_size += strlen("num_chld: \n" ) + 21 + 1;      // number of children 
    buf_size += strlen("capacity: \n" ) + 21 + 1;      // capacity 
    buf_size += strlen("sig:      \n" ) + (ent->ent_sig_len * 4) / 3 + 2;     // signature (base64-encoded)
    buf_size += strlen("parent:   \n" ) + 16 + 1;      // parent 
    buf_size += strlen("xattr_h:  \n" ) + 2*SHA256_DIGEST_LENGTH + 1;
    buf_size += 1;
    
    buf = SG_CALLOC( char, buf_size );
    if( buf == NULL ) {
        return -ENOMEM;
    }
    
    if( ent->ent_sig != NULL ) {
        
        rc = md_base64_encode( (char const*)ent->ent_sig, ent->ent_sig_len, &sigb64 );
        if( rc != 0 ) {
            SG_safe_free( buf );
            return rc;
        }
    }
    
    if( ent->xattr_hash != NULL ) {
        
        sha256_printable_buf( ent->xattr_hash, xattr_hash_buf );
    }
    else {
        
        memset( xattr_hash_buf, 0, 2*SHA256_DIGEST_LENGTH + 1 );
    }
    
    snprintf(buf, buf_size, 
             "type:     %X\n"
             "name:     %s\n"
             "file_id:  %" PRIX64 "\n"
             "ctime_s:  %" PRId64 "\n"
             "ctime_ns: %" PRId32 "\n"
             "mtime_s:  %" PRId64 "\n"
             "mtime_ns: %" PRId32 "\n"
             "mmtim_s:  %" PRId64 "\n"
             "mmtim_ns: %" PRId32 "\n"
             "write_n:  %" PRId64 "\n"
             "xattr_n:  %" PRId64 "\n"
             "max_read: %" PRId32 "\n"
             "max_wrte: %" PRId32 "\n"
             "owner:    %" PRIu64 "\n"
             "coord:    %" PRIu64 "\n"
             "volume:   %" PRIu64 "\n"
             "mode:     %o\n"
             "size:     %" PRIu64 "\n"
             "error:    %" PRId32 "\n"
             "gen:      %" PRId64 "\n"
             "num_chld: %" PRId64 "\n"
             "capacity: %" PRId64 "\n"
             "sig:      %s\n" 
             "parent:   %" PRIX64 "\n"
             "xattr_h:  %s\n",
             ent->type,
             ent->name,
             ent->file_id,
             ent->ctime_sec,
             ent->ctime_nsec,
             ent->mtime_sec,
             ent->mtime_nsec,
             ent->manifest_mtime_sec,
             ent->manifest_mtime_nsec,
             ent->write_nonce,
             ent->xattr_nonce,
             ent->max_read_freshness,
             ent->max_write_freshness,
             ent->owner,
             ent->coordinator,
             ent->volume,
             ent->mode,
             ent->size,
             ent->error,
             ent->generation,
             ent->num_children,
             ent->capacity,
             sigb64,
             ent->parent_id,
             xattr_hash_buf );
    
    SG_safe_free( sigb64 );
    *data = buf;
    return 0;
}


// get data root directory
char* md_conf_get_data_root( struct md_syndicate_conf* conf ) {
    return conf->data_root;
}

