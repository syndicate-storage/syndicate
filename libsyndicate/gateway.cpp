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

#include "libsyndicate/gateway.h"
#include "libsyndicate/server.h"
#include "libsyndicate/opts.h"
#include "libsyndicate/private/opts.h"
#include "libsyndicate/client.h"
#include "libsyndicate/util.h"

#include "libsyndicate/ms/gateway.h"
#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/cert.h"

// gateway for which we are running the main() loop
static struct SG_gateway* g_main_gateway = NULL;

// alloc a gateway 
struct SG_gateway* SG_gateway_new(void) {
   return SG_CALLOC( struct SG_gateway, 1 );
}

// initialize SG IO hints 
int SG_IO_hints_init( struct SG_IO_hints* io_hints, int io_type, uint64_t offset, uint64_t len ) {

   memset( io_hints, 0, sizeof(struct SG_IO_hints));
   
   io_hints->io_type = io_type;
   io_hints->io_context = md_random64();
   io_hints->offset = offset;
   io_hints->len = len;
   return 0;
}

// initialize an empty request data structure 
// always succeeds 
int SG_request_data_init( struct SG_request_data* reqdat ) {
   
   memset( reqdat, 0, sizeof( struct SG_request_data) );
   
   reqdat->volume_id = SG_INVALID_VOLUME_ID;
   reqdat->block_id = SG_INVALID_BLOCK_ID;
   reqdat->file_id = SG_INVALID_FILE_ID;
   reqdat->coordinator_id = SG_INVALID_GATEWAY_ID;
   
   reqdat->manifest_timestamp.tv_sec = -1;
   reqdat->manifest_timestamp.tv_nsec = -1;
   
   reqdat->user_id = SG_INVALID_USER_ID;
   reqdat->io_hints.io_type = SG_IO_NONE;

   return 0;
}


// init common fields of a request data 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_request_data_init_common( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, struct SG_request_data* reqdat ) {

   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   char* fs_path_dup = SG_strdup_or_null( fs_path );
   
   if( fs_path_dup == NULL && fs_path != NULL ) {
      return -ENOMEM;
   }
   
   SG_request_data_init( reqdat );
   
   reqdat->fs_path = fs_path_dup;
   reqdat->volume_id = volume_id;
   reqdat->file_id = file_id;
   reqdat->coordinator_id = SG_gateway_id( gateway );
   reqdat->file_version = file_version;
   reqdat->user_id = SG_gateway_user_id( gateway );

   return 0;
}


// initialize a request data structure for a block 
// return 0 on success 
// return -ENOMEM on OOM
int SG_request_data_init_block( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct SG_request_data* reqdat ) {

   int rc = SG_request_data_init_common( gateway, fs_path, file_id, file_version, reqdat );
   if( rc != 0 ) {
      return rc;
   }

   reqdat->block_id = block_id;
   reqdat->block_version = block_version;
   
   return 0;
}


// initialize a reqeust data structure for a manifest 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_request_data_init_manifest( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, struct SG_request_data* reqdat ) {

   int rc = SG_request_data_init_common( gateway, fs_path, file_id, file_version, reqdat );
   if( rc != 0 ) {
      return rc;
   }
   
   reqdat->manifest_timestamp.tv_sec = manifest_mtime_sec;
   reqdat->manifest_timestamp.tv_nsec = manifest_mtime_nsec;
   
   return 0;
}


// initialize a request data structure for setting an xattr 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_request_data_init_setxattr( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t xattr_nonce, char const* name, char const* value, size_t value_len, struct SG_request_data* reqdat ) {

   if( name == NULL || value == NULL ) {
      return -EINVAL;
   }
   
   char* name_dup = SG_strdup_or_null( name );
   if( name_dup == NULL ) {
      return -ENOMEM;
   }
   
   char* value_dup = SG_CALLOC( char, value_len );
   if( value_dup == NULL ) {
      SG_safe_free( name_dup );
      return -ENOMEM;
   }
   
   int rc = SG_request_data_init_common( gateway, fs_path, file_id, file_version, reqdat );
   if( rc != 0 ) {
      SG_safe_free( name_dup );
      SG_safe_free( value_dup );
      return -ENOMEM;
   }
   
   reqdat->setxattr = true;
   reqdat->xattr_name = name_dup;
   reqdat->xattr_value = value_dup;
   reqdat->xattr_value_len = value_len;
   
   return 0;
}



// initialize a request data structure for removing an xattr 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_request_data_init_removexattr( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t xattr_nonce, char const* name, struct SG_request_data* reqdat ) {

   if( name == NULL ) {
      return -EINVAL;
   }
   
   char* name_dup = SG_strdup_or_null( name );
   if( name_dup == NULL ) {
      return -ENOMEM;
   }
   
   int rc = SG_request_data_init_common( gateway, fs_path, file_id, file_version, reqdat );
   if( rc != 0 ) {
      SG_safe_free( name_dup );
      return -ENOMEM;
   }
   
   reqdat->removexattr = true;
   reqdat->xattr_name = name_dup;
   
   return 0;
}


// parse an SG request from a URL.
// return 0 on success
// return -EINVAL if the URL is malformed 
// return -ENOMEM if OOM
int SG_request_data_parse( struct SG_request_data* reqdat, char const* _url_path ) {
   
   memset( reqdat, 0, sizeof(struct SG_request_data) );
  
   // sanity check 
   if( strlen(_url_path) < 5 ) {
      return -EINVAL;
   }

   if( strstr( _url_path, "/../" ) != NULL || strcmp( _url_path + strlen(_url_path) - 3, "/.." ) == 0 ) {
      return -EINVAL;
   }
   
   char* url_path = SG_strdup_or_null( _url_path );
   if( url_path == NULL ) {
      
      return -ENOMEM;
   }

   // temporary values
   uint64_t volume_id = SG_INVALID_VOLUME_ID;
   char* file_path = NULL;
   uint64_t file_id = SG_INVALID_FILE_ID;
   int64_t file_version = -1;
   uint64_t block_id = SG_INVALID_BLOCK_ID;
   int64_t block_version = -1;
   struct timespec manifest_timestamp;
   manifest_timestamp.tv_sec = -1;
   manifest_timestamp.tv_nsec = -1;
   int rc = 0;

   int num_parts = 0;
   char* prefix = NULL;
   char* volume_id_str = NULL;

   bool is_manifest = false;
   int file_name_id_and_version_part = 0;
   int xattr_name_and_nonce_part = 0;
   int manifest_part = 0;
   int block_id_and_version_part = 0;
   size_t file_path_len = 0;

   char** parts = NULL;
   char* tmp = NULL;
   char* cursor = NULL;
   char* xattr_name = NULL;
   char* xattr_nonce_str = NULL;
   int64_t xattr_nonce = 0;
   
   bool is_getxattr = false;
   bool is_listxattr = false;

   // break url_path into tokens, by /
   int num_seps = 0;
   for( unsigned int i = 0; i < strlen(url_path); i++ ) {
      if( url_path[i] == '/' ) {
         num_seps++;
         while( url_path[i] == '/' && i < strlen(url_path) ) {
            i++;
         }
      }
   }

   // minimum number of parts: data prefix, volume_id, path.file_id.file_version, (block.version || manifest.tv_sec.tv_nsec)
   if( num_seps < 4 ) {
      rc = -EINVAL;
      
      SG_error("num_seps = %d\n", num_seps );
      SG_safe_free( url_path );
      return rc;
   }

   num_parts = num_seps;
   parts = SG_CALLOC( char*, num_seps + 1 );
   
   if( parts == NULL ) {
      rc = -ENOMEM;
      SG_safe_free( url_path );
      
      return rc;
   }
   
   tmp = NULL;
   cursor = url_path;
   
   for( int i = 0; i < num_seps; i++ ) {
      char* tok = strtok_r( cursor, "/", &tmp );
      cursor = NULL;

      if( tok == NULL ) {
         break;
      }

      parts[i] = tok;
   }
   
   prefix = parts[0];
   volume_id_str = parts[1];
   file_name_id_and_version_part = num_parts-2;
   manifest_part = num_parts-1;
   block_id_and_version_part = num_parts-1;

   if( strcmp(prefix, SG_DATA_PREFIX) != 0 ) {
      
      if( strcmp( prefix, SG_GETXATTR_PREFIX ) == 0 ) {
         
         is_getxattr = true;
         
         // basename of the path is the xattr name and xattr nonce
         xattr_name_and_nonce_part = file_name_id_and_version_part;
         file_name_id_and_version_part--;
         
         if( file_name_id_and_version_part < 0 ) {
            SG_error("Invalid URL path '%s'\n", url_path );
            
            rc = -EINVAL;
            goto SG_request_data_parse_end;
         }
         
         // parse name and nonce 
         xattr_name = parts[xattr_name_and_nonce_part];
         xattr_nonce_str = md_rchomp( parts[xattr_name_and_nonce_part], '.' );
         
         if( xattr_nonce_str == NULL ) {
            SG_error("Invalid getxattr string '%s'\n", parts[xattr_name_and_nonce_part]);
            
            rc = -EINVAL;
            goto SG_request_data_parse_end;
         }
         
         xattr_nonce = (int64_t)strtoll( xattr_nonce_str, &tmp, 10 );
         if( xattr_nonce == 0 && *tmp != '\0') {
            SG_error("Invalid getxattr nonce '%s'\n", xattr_nonce_str );
            
            rc = -EINVAL;
            goto SG_request_data_parse_end;
         }
      }
      else if( strcmp( prefix, SG_LISTXATTR_PREFIX ) == 0 ) {
         
         is_listxattr = true;
      }
      else {
            
         // invalid prefix
         SG_error("Invalid URL prefix = '%s'\n", prefix);
         
         rc = -EINVAL;
         goto SG_request_data_parse_end;
      }
   }

   // volume ID?
   rc = md_parse_uint64( volume_id_str, "%" PRIu64, &volume_id );
   if( rc < 0 ) {
      
      SG_error("could not parse '%s'\n", volume_id_str);
      
      rc = -EINVAL;
      goto SG_request_data_parse_end;
   }
   
   // is this a manifest request?
   if( strncmp( parts[manifest_part], "manifest", strlen("manifest") ) == 0 ) {
      rc = md_parse_manifest_timestamp( parts[manifest_part], &manifest_timestamp );
      if( rc == 0 ) {
         // success!
         is_manifest = true;
      }
      else {
         
         SG_error("md_parse_manifest_timestamp('%s') rc = %d\n", parts[manifest_part], rc );
         
         rc = -EINVAL;
         goto SG_request_data_parse_end;
      }
   }

   if( !is_manifest && !is_getxattr && !is_listxattr ) {
      
      // not a manifest request, so we must have a block ID and block version 
      rc = md_parse_block_id_and_version( parts[block_id_and_version_part], &block_id, &block_version );
      if( rc != 0 ) {
         // invalid request--neither a manifest nor a block ID
         SG_error("could not parse '%s'\n", parts[block_id_and_version_part]);
         
         rc = -EINVAL;
         goto SG_request_data_parse_end;
      }
   }
   
   // parse file ID and version
   rc = md_parse_file_id_and_version( parts[file_name_id_and_version_part], &file_id, &file_version );
   if( rc != 0 ) {
      // invalid 
      SG_error("could not parse ID and/or version of '%s'\n", parts[file_name_id_and_version_part] );
      
      rc = -EINVAL;
      goto SG_request_data_parse_end;
   }
   
   // clear file version
   tmp = md_rchomp( parts[file_name_id_and_version_part], '.' );
   if( tmp == NULL ) {
      
      // invalid 
      SG_error("No file version in '%s'\n", parts[file_name_id_and_version_part]);
      
      rc = -EINVAL;
      goto SG_request_data_parse_end;
   }
   
   // clear file ID 
   tmp = md_rchomp( parts[file_name_id_and_version_part], '.' );
   if( tmp == NULL ) {
      
      // invalid 
      SG_error("No file ID in '%s'\n", parts[file_name_id_and_version_part]);
      
      rc = -EINVAL;
      goto SG_request_data_parse_end;
   }
   
   // assemble the path
   for( int i = 2; i <= file_name_id_and_version_part; i++ ) {
      file_path_len += strlen(parts[i]) + 2;
   }

   file_path = SG_CALLOC( char, file_path_len + 1 );
   
   if( file_path == NULL ) {
      
      SG_safe_free( parts );
      SG_safe_free( url_path );
      
      rc = -ENOMEM;
      return rc;
   }
   
   for( int i = 2; i <= file_name_id_and_version_part; i++ ) {
      strcat( file_path, "/" );
      strcat( file_path, parts[i] );
   }
   
   reqdat->volume_id = volume_id;
   reqdat->fs_path = file_path;
   reqdat->file_id = file_id;
   reqdat->file_version = file_version;
   reqdat->block_id = block_id;
   reqdat->block_version = block_version;
   reqdat->manifest_timestamp = manifest_timestamp;
   reqdat->getxattr = is_getxattr;
   reqdat->listxattr = is_listxattr;
   reqdat->xattr_name = xattr_name;
   reqdat->xattr_nonce = xattr_nonce;
   
SG_request_data_parse_end:
   SG_safe_free( parts );
   SG_safe_free( url_path );

   return rc;
}


// duplicate an SG_request_data's fields 
// return 0 on success
// return -ENOMEM on OOM 
int SG_request_data_dup( struct SG_request_data* dest, struct SG_request_data* src ) {
   
   SG_request_data_init( dest );
   
   char* fs_path = SG_strdup_or_null( src->fs_path );
   if( fs_path == NULL ) {
      
      return -ENOMEM;
   }
   
   memcpy( dest, src, sizeof(struct SG_request_data) );
   
   // deep copy
   dest->fs_path = fs_path;
   return 0;
}


// is this a request for a block?
// return true if so
// return false if not 
bool SG_request_is_block( struct SG_request_data* reqdat ) {
  
   return (reqdat->block_id != SG_INVALID_BLOCK_ID);
}

// is this a request for a manifest?
// return true if so 
// return false if not 
bool SG_request_is_manifest( struct SG_request_data* reqdat ) {
   
   return (reqdat->block_id == SG_INVALID_BLOCK_ID && !reqdat->getxattr && !reqdat->listxattr &&
          !reqdat->removexattr && !reqdat->setxattr);
}


// is this a request for an xattr?
// return true if so 
// return false if not 
bool SG_request_is_getxattr( struct SG_request_data* reqdat ) {

   return reqdat->getxattr;
}

// is this a request for an xattr list?
// return true if so 
// return false if not 
bool SG_request_is_listxattr( struct SG_request_data* reqdat ) {
   
   return reqdat->listxattr;
}


// free a gateway_request_data
void SG_request_data_free( struct SG_request_data* reqdat ) {
   if( reqdat->fs_path != NULL ) {
      SG_safe_free( reqdat->fs_path );
   }
   if( reqdat->xattr_name != NULL ) {
      SG_safe_free( reqdat->xattr_name );
   }
   if( reqdat->xattr_value != NULL ) {
      SG_safe_free( reqdat->xattr_value );
   }
   memset( reqdat, 0, sizeof(struct SG_request_data) );
}

// get IO hints
int SG_request_data_get_IO_hints( struct SG_request_data* reqdat, struct SG_IO_hints* hints ) {
   *hints = reqdat->io_hints;
   return 0;
}


// set IO hints
int SG_request_data_set_IO_hints( struct SG_request_data* reqdat, struct SG_IO_hints* hints ) {
   reqdat->io_hints = *hints;
   return 0;
}


// merge opts and config--opts overriding the config
// return 0 on success
// return -ENOMEM on OOM 
static int SG_config_merge_opts( struct md_syndicate_conf* conf, struct md_opts* opts ) {
   
   // set maximum of command-line and file-given debug levels
   md_set_debug_level( MAX( opts->debug_level, md_get_debug_level() ) );
   conf->is_client = opts->client;

   // pass on driver parameters 
   int rc = md_conf_set_driver_params( conf, opts->driver_exec_str, opts->driver_roles, opts->num_driver_roles );
   
   return rc;
}


// initialize the gateway's internal driver, common to all gateways
// if this fails due to there being no driver for this gateway, a dummy driver will be used instead
// return 0 on success, and set *_ret to a newly-allocated driver
// return -ENOENT if there is no driver for this gateway
// return -ENOMEM on OOM
// return -errno on failure to initialize the driver
static int SG_gateway_driver_init_internal( struct ms_client* ms, struct md_syndicate_conf* conf, struct SG_driver* driver, int num_instances ) {
   
   // get the driver text 
   char* driver_text = NULL;
   uint64_t driver_text_len = 0;
   int rc = 0;
   
   // get the driver text
   rc = ms_client_gateway_get_driver_text( ms, &driver_text, &driver_text_len );
   if( rc != 0 ) {
      
      // TODO: anonymous gateway drivers
      if( rc == -EAGAIN ) {
         // no driver loaded at boot-time; use stub
         
         if( conf->is_client ) {
            SG_warn("%s", "Anonymous gateway; using stub driver\n");
         }
         else {
            SG_warn("%s", "No driver loaded\n");
         }
         
         return -ENOENT;
      }
      
      
      // some other error
      SG_error("ms_client_gateway_get_driver_text rc = %d\n", rc );
      return rc;
   }
   
   // create the driver
   rc = SG_driver_init( driver, conf, ms->gateway_pubkey, ms->gateway_key, conf->driver_exec_path, conf->driver_roles, conf->num_driver_roles, num_instances, driver_text, driver_text_len );
   
   SG_safe_free( driver_text );
   
   return rc;
}


// initialize a custom driver for the specific type of gateway
// if this fails due to there being no driver for this gateway, a dummy driver will be used instead
// return 0 on success, and set *_ret to a newly-allocated driver
// return -ENOENT if there is no driver for this gateway
// return -ENOMEM on OOM
// return -errno on failure to initialize the driver
int SG_gateway_driver_init( struct SG_gateway* gateway, struct SG_driver* driver ) {
   return SG_gateway_driver_init_internal( SG_gateway_ms( gateway ), SG_gateway_conf( gateway ), driver, gateway->num_iowqs );
}


// get driver data for this gateway 
// return 0 on success, and populate driver_data with the raw text of the given field
// return -ENONET if the data requested is not available 
// return -ENOMEM on OOM 
int SG_gateway_driver_get_data( struct SG_gateway* gateway, char const* data_name, struct SG_chunk* driver_data ) {
   
   int rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   char* driver_text = NULL;
   size_t driver_len = 0;
   char* ret_data = NULL;
   size_t ret_data_len = 0;
   
   rc = ms_client_gateway_get_driver_text( ms, &driver_text, &driver_len );
   if( rc != 0 ) {
      
      SG_error("ms_client_gateway_get_driver_text rc = %d\n", rc );
      return rc;
   }

   // look up the value 
   rc = SG_driver_get_string( driver_text, driver_len, data_name, &ret_data, &ret_data_len );
   SG_safe_free( driver_text );
   
   if( rc != 0 ) {
   
      SG_error("SG_driver_get_string('%s') rc = %d\n", data_name, rc );
      return rc;
   }
   
   SG_chunk_init( driver_data, ret_data, ret_data_len );
   return 0;
}


// get the base64-decoded configuration text for this gateway.
// return 0 on success, and populate the given config_data with the decoded configuration text 
// return -ENOENT if there is no config 
// return -ENOMEM on OOM 
int SG_gateway_driver_get_config_text( struct SG_gateway* gateway, struct SG_chunk* config_data ) {
  
   int rc = 0;
   struct SG_chunk config_b64;
   size_t len = 0;
   
   // look up new information 
   rc = SG_gateway_driver_get_data( gateway, "config", &config_b64 );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_driver_get_data('config') rc = %d\n", rc );
      return rc;
   }
   
   // decode!
   rc = md_base64_decode( config_b64.data, config_b64.len, &config_data->data, &len );
   config_data->len = len;
   SG_chunk_free( &config_b64 );
   
   if( rc != 0 ) {
      
      SG_error("md_base64_decode('config') rc = %d\n", rc );
      
      if( rc != -ENOMEM ) {
         rc = -EINVAL;
      }
      
      return rc;
   }
   
   return rc;
}


// get the decrypted, decoded, mlock'ed secrets text for this gateway.
// return 0 on success, and populate the given secrets_data with the decoded secrets text 
// return -ENOENT if there are no secrets 
// return -ENOMEM on OOM 
int SG_gateway_driver_get_mlocked_secrets_text( struct SG_gateway* gateway, struct SG_chunk* secrets_data ) {
   
   int rc = 0;
   struct SG_chunk secrets_b64;
   char* secrets_str = NULL;
   size_t secrets_len = 0;
   
   // look up new information 
   rc = SG_gateway_driver_get_data( gateway, "secrets", &secrets_b64 );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_driver_get_data('secrets') rc = %d\n", rc );
      return rc;
   }
   
   // decrypt--it should have been encrypted by this gateway's key 
   // NOTE: secrets_str will have been mlock'ed
   rc = SG_driver_decrypt_secrets( SG_gateway_public_key( gateway ), SG_gateway_private_key( gateway ), &secrets_str, &secrets_len, secrets_b64.data, secrets_b64.len );
   SG_chunk_free( &secrets_b64 );
   
   if( rc != 0 ) {
      
      SG_error("SG_driver_decrypt_secrets rc = %d\n", rc );
   }
   
   SG_chunk_init( secrets_data, secrets_str, secrets_len );
   return rc;
}


// get the decoded driver text for this gateway.
// return 0 on success, and populate the given driver_data with the decoded driver image 
// return -ENOENT if there is no driver file 
// return -ENOMEM on OOM 
int SG_gateway_driver_get_driver_text( struct SG_gateway* gateway, struct SG_chunk* driver_data ) {
   
   int rc = 0;
   struct SG_chunk driver_b64;
   size_t driver_len = 0;
   
   // look up the info 
   rc = SG_gateway_driver_get_data( gateway, "driver", &driver_b64 );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_driver_get_data('driver') rc = %d\n", rc );
      return rc;
   }
   
   // decode!
   rc = md_base64_decode( driver_b64.data, driver_b64.len, &driver_data->data, &driver_len );
   driver_data->len = driver_len;
   SG_chunk_free( &driver_b64 );
   
   if( rc != 0 ) {
      
      SG_error("md_base64_decode('driver') rc = %d\n", rc );
      
      if( rc != -ENOMEM ) {
         rc = -EINVAL;
      }
      
      return rc;
   }
   
   return rc;
}


// initialize and start the gateway, using a parsed options structure 
// return 0 on success
// return -ENOMEM on OOM 
// return -ENOENT if a file was not found 
// return negative if libsyndicate fails to initialize 
int SG_gateway_init_opts( struct SG_gateway* gateway, struct md_opts* opts ) {
   
   int rc = 0;
   struct ms_client* ms = SG_CALLOC( struct ms_client, 1 );
   struct md_syndicate_conf* conf = SG_CALLOC( struct md_syndicate_conf, 1 );
   struct md_syndicate_cache* cache = SG_CALLOC( struct md_syndicate_cache, 1 );
   struct md_HTTP* http = SG_CALLOC( struct md_HTTP, 1 );
   struct SG_driver* driver = SG_driver_alloc();
   struct md_downloader* dl = md_downloader_new();
   struct md_wq* iowqs = NULL;
   
   sem_t config_sem;
   
   bool md_inited = false;
   bool ms_inited = false;
   bool config_inited = false;
   bool cache_inited = false;
   bool http_inited = false;
   bool driver_inited = false;
   bool dl_inited = false;
   
   uint64_t block_size = 0;   
   int first_arg_optind = -1;
   
   int num_iowqs = 0;
   int max_num_iowqs = 1; // 4 * sysconf( _SC_NPROCESSORS_CONF );       // I/O doesn't take much CPU...
   
   if( ms == NULL || conf == NULL || cache == NULL || http == NULL || dl == NULL ) {
      
      rc = -ENOMEM;
      goto SG_gateway_init_error;
   }
   
   // load config
   md_default_conf( conf );
   
   // set debug level
   md_set_debug_level( opts->debug_level );
   
   // read the config file, if given
   if( opts->config_file != NULL ) {
      
      rc = md_read_conf( opts->config_file, conf );
      if( rc != 0 ) {
         SG_error("md_read_conf('%s'), rc = %d\n", opts->config_file, rc );
         
         goto SG_gateway_init_error;
      }
   }
  
   // fold in gateway implementation options 
   rc = SG_config_merge_opts( conf, opts );
   if( rc != 0 ) {
      
      SG_error("SG_config_merge_opts rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }

   // validity 
   if( opts->gateway_name == NULL ) {

      SG_error("%s", "No gateway name defined\n");
      rc = -EINVAL;
      goto SG_gateway_init_error;
   }

   if( opts->volume_name == NULL ) {

      SG_error("%s", "No volume name defined\n");
      rc = -EINVAL;
      goto SG_gateway_init_error;
   }
  
   // allocate I/O work queues 
   iowqs = SG_CALLOC( struct md_wq, max_num_iowqs );
   
   if( iowqs == NULL ) {
      
      // OOM 
      rc = -ENOMEM;
      goto SG_gateway_init_error;
   }
   
   // initialize library
   if( !opts->client ) {
      
      if( opts->username == NULL ) {
         
         SG_error("%s", "No username given\n");
         rc = -EINVAL;
         goto SG_gateway_init_error;
      }

      // initialize peer
      SG_debug("%s", "Not anonymous; initializing as peer\n");
      
      rc = md_init( conf, ms, opts );
      if( rc != 0 ) {
         
         goto SG_gateway_init_error;
      }
   }
   else {
      
      // initialize client
      SG_debug("%s", "Anonymous; initializing as client\n");
      
      rc = md_init_client( conf, ms, opts );
      if( rc != 0 ) {
         
         goto SG_gateway_init_error;
      }
   }
   
   // advance!
   md_inited = true;
   ms_inited = true;
   
   // initialize config reload 
   sem_init( &config_sem, 0, 0 );
   
   // advance!
   config_inited = true;
   
   // initialize workqueues 
   for( num_iowqs = 0; num_iowqs < max_num_iowqs; num_iowqs++ ) {
      
      rc = md_wq_init( &iowqs[num_iowqs], gateway );
      if( rc != 0 ) {
         
         SG_error("md_wq_init( iowq[%d] ) rc = %d\n", num_iowqs, rc );
         
         goto SG_gateway_init_error;
      }
   }
   
   // get block size, now that the MS client is initialized 
   block_size = ms_client_get_volume_blocksize( ms );
   
   // initialize cache
   rc = md_cache_init( cache, conf, conf->cache_soft_limit / block_size, conf->cache_hard_limit / block_size );
   if( rc != 0 ) {
      
      SG_error("md_cache_init rc = %d\n", rc );
   
      goto SG_gateway_init_error;
   }
   
   // advance!
   cache_inited = true;
   
   // if we're a peer, initialize HTTP server, making the gateway available to connections
   if( !conf->is_client ) {
         
      rc = md_HTTP_init( http, MD_HTTP_TYPE_STATEMACHINE | MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY | MHD_USE_DEBUG, gateway );
      if( rc != 0 ) {
         
         SG_error("md_HTTP_init rc = %d\n", rc );
         
         goto SG_gateway_init_error;
      }
     
      md_HTTP_set_limits( http, block_size * SG_MAX_BLOCK_LEN_MULTIPLIER, 100 * block_size * SG_MAX_BLOCK_LEN_MULTIPLIER );

      // set up HTTP server methods 
      SG_server_HTTP_install_handlers( http );

      // advance!
      http_inited = true;
   }
   else {
      
      // won't need the HTTP server 
      SG_safe_free( http );
   }
    
   // load driver 
   if( !opts->ignore_driver ) {
      
      rc = SG_gateway_driver_init_internal( ms, conf, driver, 1 );
      if( rc != 0 && rc != -ENOENT ) {
         
         SG_error("SG_gateway_driver_init_internal rc = %d\n", rc );
         
         goto SG_gateway_init_error;
      }
      
      // advance 
      driver_inited = true;
   }
   
   // set up the downloader 
   rc = md_downloader_init( dl, "gateway" );
   if( rc != 0 ) {
      
      SG_error("md_downloader_init('gateway') rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   // advance!
   dl_inited = true;
   
   // start workqueues 
   for( int i = 0; i < num_iowqs; i++ ) {
      
      rc = md_wq_start( &iowqs[i] );
      if( rc != 0 ) {
         
         SG_error("md_wq_start( iowqs[%d] ) rc = %d\n", i, rc );
         
         goto SG_gateway_init_error;
      }
   }
   
   // start cache 
   rc = md_cache_start( cache );
   if( rc != 0 ) {
      
      SG_error("md_cache_start rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   // start downloader 
   rc = md_downloader_start( dl );
   if( rc != 0 ) {
      
      SG_error("md_downloader_start rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   // don't die on SIGPIPE 
   signal( SIGPIPE, SIG_IGN );

   // start driver
   if( driver_inited ) { 

      rc = SG_driver_procs_start( driver ); 
      if( rc != 0 ) {

         SG_error("SG_driver_procs_start('%s') rc = %d\n", opts->driver_exec_str, rc );
         goto SG_gateway_init_error;
      }
   }
   
   // initialize gateway runtime, so we can start HTTP and get certificates
   gateway->ms = ms;
   gateway->conf = conf;
   gateway->cache = cache;
   gateway->http = http;
   gateway->driver = driver;
   gateway->dl = dl;
   gateway->config_sem = config_sem;
   gateway->iowqs = iowqs;
   gateway->num_iowqs = max_num_iowqs;
   gateway->first_arg_optind = first_arg_optind;
   gateway->foreground = opts->foreground;
   
   if( gateway->http != NULL ) {
      
      // start HTTP server 
      rc = md_HTTP_start( gateway->http, ms_client_get_portnum( ms ) );
      if( rc != 0 ) {
         
         SG_error("md_HTTP_start rc = %d\n", rc );
         
         goto SG_gateway_init_error;
      }
   }
   
   // success!
   gateway->running = true;
   
   // initialize gateway-specific bits
   if( gateway->impl_setup != NULL ) {
      
      rc = (*gateway->impl_setup)( gateway, &gateway->cls );
      if( rc != 0 ) {
         
         SG_error("gateway->impl_setup rc = %d\n", rc );
         
         gateway->running = false;
         memset( gateway, 0, sizeof(struct SG_gateway) );
         
         goto SG_gateway_init_error;
      }
   }

   return rc;
   
   // error handler
SG_gateway_init_error:   
   
   if( dl_inited ) {
      
      if( md_downloader_is_running( dl ) ) {
         md_downloader_stop( dl );
      }
      
      md_downloader_shutdown( dl );
   }
   
   SG_safe_free( dl );
   
   if( http_inited ) {
      
      if( http->running ) {
         md_HTTP_stop( http );
      }
      
      md_HTTP_free( http );
   }
   
   SG_safe_free( http );
   
   if( cache_inited ) {
      
      if( cache->running ) {
         md_cache_stop( cache );
      }
      
      md_cache_destroy( cache );
   }
   
   SG_safe_free( cache );
   
   if( driver_inited ) {
      SG_driver_shutdown( driver );
   }
   
   SG_safe_free( driver );
   
   if( config_inited ) {
      sem_destroy( &config_sem );
   }
   
   if( ms_inited ) {
      ms_client_destroy( ms );
   }
   
   if( iowqs != NULL ) {
      for( int i = 0; i < num_iowqs; i++ ) {
        
          md_wq_stop( &iowqs[i] );
          md_wq_free( &iowqs[i], NULL );
      }
      
      SG_safe_free( iowqs );
   }
   
   SG_safe_free( ms );
   
   md_free_conf( conf );
   SG_safe_free( conf );
   
   if( md_inited ) {
      md_shutdown();
   }
   
   return rc;
}


// initialize and start the gateway, parsing argc and argv in the process.
// forwards on to SG_gateway_init_opts from the parsed options.
// loads and initializes the driver, starts up the cache, reloads the certificates, starts up the HTTP server, starts up the download infrastructure
// return 0 on success
// return -ENOMEM of OOM
// return -ENOENT if a file was not found 
// return negative if libsyndicate fails to initialize
// return 1 if the user wanted help
int SG_gateway_init( struct SG_gateway* gateway, uint64_t gateway_type, int argc, char** argv, struct md_opts* overrides ) {
   
   int rc = 0;
   struct md_opts opts;
   int first_arg_optind = 0;
   
   rc = md_opts_default( &opts );
   if( rc != 0 ) {
       // OOM
       return rc;
   }
   
   // get options
   rc = md_opts_parse( &opts, argc, argv, &first_arg_optind, NULL, NULL );
   if( rc != 0 ) {
      
      if( rc < 0 ) {
            
         SG_error( "md_opts_parse rc = %d\n", rc );
      }
      
      return rc;
   }
   
   // become process group leader, so we can talk to all processes
   // started by the driver and other infrastructure 
   rc = setpgrp();
   if( rc != 0 ) {
      
      rc = -errno;
      SG_error("setpgrp rc = %d\n", rc );
      return rc;
   }
   
   md_opts_set_client( &opts, md_opts_get_client( overrides ) );
   md_opts_set_gateway_type( &opts, md_opts_get_gateway_type( overrides ) );
   md_opts_set_ignore_driver( &opts, md_opts_get_ignore_driver( overrides ) );
   md_opts_set_driver_config( &opts, overrides->driver_exec_str, overrides->driver_roles, overrides->num_driver_roles );
   
   // initialize the gateway 
   rc = SG_gateway_init_opts( gateway, &opts );
   
   md_opts_free( &opts );
   
   if( rc == 0 ) {
      
      gateway->first_arg_optind = first_arg_optind;
   }
   
   return rc;
}


// set the gateway's client-given state 
// always succeeds 
void SG_gateway_set_cls( struct SG_gateway* gateway, void* cls ) {
   gateway->cls = cls;
}


// signal the main loop to exit 
// always succeeds
int SG_gateway_signal_main( struct SG_gateway* gateway ) {
   
   gateway->running = false;
   sem_post( &gateway->config_sem );
   
   return 0;
}


// shut the gateway down 
// return 0 on success
// return -EINVAL if the gateway was already stopped
int SG_gateway_shutdown( struct SG_gateway* gateway ) {
   
   gateway->running = false;
   
   // do the gateway shutdown 
   if( gateway->impl_shutdown != NULL ) {
      
      (*gateway->impl_shutdown)( gateway, gateway->cls );
   }
  
   if( gateway->dl != NULL ) { 
       md_downloader_stop( gateway->dl );
       md_downloader_shutdown( gateway->dl );
       SG_safe_free( gateway->dl );
   }
      
   if( gateway->http != NULL ) {
      md_HTTP_stop( gateway->http );
      md_HTTP_free( gateway->http );
      SG_safe_free( gateway->http );
   }
   
   if( gateway->cache != NULL ) {
       md_cache_stop( gateway->cache );
       md_cache_destroy( gateway->cache );
       SG_safe_free( gateway->cache );
   }
   
   if( gateway->driver != NULL ) {
      SG_driver_shutdown( gateway->driver );
      SG_safe_free( gateway->driver );
   }
   
   if( gateway->ms != NULL ) {
       ms_client_destroy( gateway->ms );
       SG_safe_free( gateway->ms );
   }
   
   if( gateway->iowqs != NULL ) {
       for( int i = 0; i < gateway->num_iowqs; i++ ) {
      
          md_wq_stop( &gateway->iowqs[i] );
          md_wq_free( &gateway->iowqs[i], NULL );
       }
   
       SG_safe_free( gateway->iowqs );
   }

   if( gateway->conf != NULL ) {
       md_free_conf( gateway->conf );
       SG_safe_free( gateway->conf );
   }

   sem_destroy( &gateway->config_sem );
   
   memset( gateway, 0, sizeof(struct SG_gateway) );
   
   md_shutdown();
   
   return 0;
}


// terminal signal handler to stop the gateway running.
// shut down the running gateway at most once.
// always succeeds 
static void SG_gateway_term( int signum, siginfo_t* siginfo, void* context ) {
   
   SG_gateway_signal_main( g_main_gateway );
}


// main loop
// periodically reload the volume and certificates
// return 0 on success 
int SG_gateway_main( struct SG_gateway* gateway ) {
   
   int rc = 0;
   
   // we're running main for this gateway 
   g_main_gateway = gateway;
   
   // set up signal handlers, so we can shut ourselves down 
   struct sigaction sigact;
   memset( &sigact, 0, sizeof(struct sigaction) );
   
   sigact.sa_sigaction = SG_gateway_term;
   
   // use sa_sigaction, not sa_handler
   sigact.sa_flags = SA_SIGINFO;
   
   // handle the usual terminal cases 
   sigaction( SIGQUIT, &sigact, NULL );
   sigaction( SIGTERM, &sigact, NULL );
   sigaction( SIGINT, &sigact, NULL );
   
   SG_debug("%s", "Entering main loop\n");
   
   while( gateway->running ) {

      struct timespec now;
      struct timespec reload_deadline;
      struct ms_volume* old_volume = NULL;
      
      ms::ms_user_cert user_cert;
      ms::ms_user_cert volume_owner_cert;
      ms_cert_bundle* gateway_certs = NULL;
      ms_cert_bundle* old_gateway_certs = NULL;
      ms::ms_volume_metadata* volume_cert = NULL;
      EVP_PKEY* syndicate_pubkey = NULL;
      EVP_PKEY* old_syndicate_pubkey = NULL;
      
      struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
      struct ms_client* ms = SG_gateway_ms( gateway );

      struct ms_gateway_cert* old_gateway_cert;
      unsigned char old_driver_hash[SHA256_DIGEST_LENGTH];
      unsigned char new_driver_hash[SHA256_DIGEST_LENGTH];
      char old_driver_hash_str[2*SHA256_DIGEST_LENGTH + 1];
      char new_driver_hash_str[2*SHA256_DIGEST_LENGTH + 1];
      struct ms_gateway_cert* new_gateway_cert = NULL;
      char* new_driver_text = NULL;
      size_t new_driver_text_len = 0;
      
      clock_gettime( CLOCK_REALTIME, &now );
      
      reload_deadline.tv_sec = now.tv_sec + conf->config_reload_freq;
      reload_deadline.tv_nsec = 0;
      
      if( reload_deadline.tv_sec == now.tv_sec ) {
         
         // avoid flapping
         SG_warn("%s", "Waiting for manditory 1 second between volume reload checks\n");
         reload_deadline.tv_sec ++;
      }
      
      SG_info("Next reload at %ld (in %ld seconds)\n", reload_deadline.tv_sec, reload_deadline.tv_sec - now.tv_sec );
      
      // wait to be signaled to reload 
      while( reload_deadline.tv_sec > now.tv_sec ) {
         
         clock_gettime( CLOCK_REALTIME, &now );
         
         rc = sem_timedwait( &gateway->config_sem, &reload_deadline );
         
         // signaled to die?
         if( !gateway->running ) {
            rc = 0;
            break;
         }
         
         if( rc != 0 ) {
            rc = -errno;
            
            if( rc == -EINTR ) {
               continue;
            }
            else if( rc == -ETIMEDOUT ) {
               break;
            }
            else {
               SG_error("sem_timedwait errno = %d\n", rc);
               return rc;
            }
         }
         else {
            // got woken up 
            break;
         }
      }
      
      // signaled to die?
      if( !gateway->running ) {
         break;
      }

      // find old cert 
      old_gateway_cert = ms_client_get_gateway_cert( ms, conf->gateway );
      if( old_gateway_cert == NULL ) {
         SG_error("BUG: no gateway on file for us (%" PRIu64 ")\n", conf->gateway );
         rc = -ENOTCONN;
         break;
      }

      // find old driver hash 
      rc = ms_client_gateway_driver_hash_buf( old_gateway_cert, old_driver_hash );
      if( rc == -ENOENT ) {
         rc = 0;
         memset( old_driver_hash, 0, SHA256_DIGEST_LENGTH );
      }
      
      // fetch new certs 
      volume_cert = SG_safe_new( ms::ms_volume_metadata );
      if( volume_cert == NULL ) {
         rc = -ENOMEM;
         break;
      }
      
      gateway_certs = SG_safe_new( ms_cert_bundle );
      if( gateway_certs == NULL ) {
         
         SG_safe_delete( volume_cert );
         rc = -ENOMEM;
         break;
      }
      
      rc = md_certs_reload( conf, &syndicate_pubkey, &user_cert, &volume_owner_cert, volume_cert, gateway_certs );
      if( rc != 0 ) {
         
         SG_error("md_certs_reload rc = %d\n", rc );
         
         ms_client_cert_bundle_free( gateway_certs );
         SG_safe_delete( gateway_certs );
         SG_safe_delete( volume_cert );
         if( syndicate_pubkey != NULL ) {
            EVP_PKEY_free( syndicate_pubkey );
            syndicate_pubkey = NULL;
         }
         
         rc = 0;
         continue;
      }
      
      // install new syndicate pubkey 
      old_syndicate_pubkey = ms_client_swap_syndicate_pubkey( ms, syndicate_pubkey );
      if( old_syndicate_pubkey != NULL ) {
          
         EVP_PKEY_free( old_syndicate_pubkey );
      }
      
      // install new volume state
      old_volume = ms_client_swap_volume_cert( ms, volume_cert );
      if( old_volume != NULL ) {
         
         ms_client_volume_free( old_volume );
         SG_safe_free( old_volume );
      }
      
      // install new certs
      old_gateway_certs = ms_client_swap_gateway_certs( ms, gateway_certs );
      if( old_gateway_certs != NULL ) {
         
         ms_client_cert_bundle_free( old_gateway_certs );
         SG_safe_delete( old_gateway_certs );
      }
     
      // go fetch or revalidate our new driver, if it changed
      new_gateway_cert = md_gateway_cert_find( gateway_certs, conf->gateway );
      if( new_gateway_cert == NULL ) {
      
         SG_error("No cert on file for us (%" PRIu64 ")\n", conf->gateway );
         rc = -ENOTCONN;
         break;
      }

      rc = ms_client_gateway_driver_hash_buf( new_gateway_cert, new_driver_hash );
      if( rc == -ENOENT ) {
         rc = 0;
         memset( new_driver_hash, 0, SHA256_DIGEST_LENGTH );
      }

      // did the driver change?
      if( memcmp( old_driver_hash, new_driver_hash, SHA256_DIGEST_LENGTH ) == 0 ) {

         // nope--no driver change
         // no need to reload
         SG_debug("%s", "driver did not change\n");
         continue;
      }

      sha256_printable_buf( old_driver_hash, old_driver_hash_str );
      sha256_printable_buf( new_driver_hash, new_driver_hash_str );
      SG_debug("Driver changed from %s to %s; reloading\n", old_driver_hash_str, new_driver_hash_str );
      
      // driver changed. go re-download
      rc = md_driver_reload( conf, new_gateway_cert );
      if( rc != 0 && rc != -ENOENT ) {
      
         SG_error("md_driver_reload rc = %d\n", rc );
         rc = -ENOTCONN;
         break;
      }
      
      rc = ms_client_gateway_get_driver_text( ms, &new_driver_text, &new_driver_text_len );
      if( rc != 0 ) {
         SG_error("ms_client_gateway_get_driver_text rc = %d\n", rc );
      }
      if( rc == -ENOMEM ) {
         // have a driver, but couldn't get to it 
         break;
      }

      if( rc == 0 ) {

         // reload workers
         rc = SG_driver_reload( SG_gateway_driver( gateway ), ms_client_my_pubkey( ms ), ms_client_my_privkey( ms ), new_driver_text, new_driver_text_len );
         SG_safe_free( new_driver_text ); 
         if( gateway->impl_config_change != NULL ) {
         
            // gateway-specific config reload 
            rc = (*gateway->impl_config_change)( gateway, rc, gateway->cls );
            if( rc != 0 ) {
            
               SG_warn( "gateway->impl_config_change rc = %d\n", rc );
            }
         }
      }
     
      if( rc != 0 ) {

         // failed to load the driver
         // default behavior is to abort
         SG_error("FATAL: aborting on failure to reload the driver (rc = %d)\n", rc ); 
         break;
      }
     
      rc = 0;
   }

   SG_debug("%s", "Leaving main loop\n");
   
   return rc;
}



// begin to reload--wake up the main loop 
int SG_gateway_start_reload( struct SG_gateway* gateway ) {
   
   sem_post( &gateway->config_sem );
   return 0;
}

// set the gateway implementation setup routine 
void SG_impl_setup( struct SG_gateway* gateway, int (*impl_setup)( struct SG_gateway*, void** ) ) {
   gateway->impl_setup = impl_setup;
}

// set the gateway implementation shutdown routine 
void SG_impl_shutdown( struct SG_gateway* gateway, void (*impl_shutdown)( struct SG_gateway*, void* ) ) {
   gateway->impl_shutdown = impl_shutdown;
}

// set the gateway implementation connect_cache routine 
void SG_impl_connect_cache( struct SG_gateway* gateway, int (*impl_connect_cache)( struct SG_gateway*, CURL*, char const*, void* ) ) {
   gateway->impl_connect_cache = impl_connect_cache;
}

// set the gateway implementation stat routine 
void SG_impl_stat( struct SG_gateway* gateway, int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) ) {
   gateway->impl_stat = impl_stat;
}

// set the gateway implementation stat-block routine 
void SG_impl_stat_block( struct SG_gateway* gateway, int (*impl_stat_block)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) ) {
   gateway->impl_stat_block = impl_stat_block;
}

// set the gateway implementation truncate routine 
void SG_impl_truncate( struct SG_gateway* gateway, int (*impl_truncate)( struct SG_gateway*, struct SG_request_data*, uint64_t, void* ) ) {
   gateway->impl_truncate = impl_truncate;
}

// set the gateway implementation rename routine 
void SG_impl_rename( struct SG_gateway* gateway, int (*impl_rename)( struct SG_gateway*, struct SG_request_data*, char const*, void* ) ) {
   gateway->impl_rename = impl_rename;
}

// set the gateway implementation detach routine 
void SG_impl_detach( struct SG_gateway* gateway, int (*impl_detach)( struct SG_gateway*, struct SG_request_data*, void* ) ) {
   gateway->impl_detach = impl_detach;
}

// set the gateway implementation to serialize a chunk 
void SG_impl_serialize( struct SG_gateway* gateway, int (*impl_serialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* ) ) {
   gateway->impl_serialize = impl_serialize;
}

// set the gateway implementation to deserialize a chunk 
void SG_impl_deserialize( struct SG_gateway* gateway, int (*impl_deserialize)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, struct SG_chunk*, void* ) ) {
   gateway->impl_deserialize = impl_deserialize;
}

// set the gateway implementation get_block routine 
void SG_impl_get_block( struct SG_gateway* gateway, int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) ) {
   gateway->impl_get_block = impl_get_block;
}

// set the gateway implementation put_block routine 
void SG_impl_put_block( struct SG_gateway* gateway, int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) ) {
   gateway->impl_put_block = impl_put_block;
}

// set the gateway implementation delete_block routine 
void SG_impl_delete_block( struct SG_gateway* gateway, int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* ) ) {
   gateway->impl_delete_block = impl_delete_block;
}

// set the gateway implementation get_manifest routine
void SG_impl_get_manifest( struct SG_gateway* gateway, int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, uint64_t, void* ) ) {
   gateway->impl_get_manifest = impl_get_manifest;
}

// set the gateway implementation put_manifest routine 
void SG_impl_put_manifest( struct SG_gateway* gateway, int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, uint64_t, void* ) ) {
   gateway->impl_put_manifest = impl_put_manifest;
}

// set the gateway implementation patch_manifest routine 
void SG_impl_patch_manifest( struct SG_gateway* gateway, int (*impl_patch_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) ) {
   gateway->impl_patch_manifest = impl_patch_manifest;
}

// set the gateway implementation delete_manifest routine 
void SG_impl_delete_manifest( struct SG_gateway* gateway, int (*impl_delete_manifest)( struct SG_gateway*, struct SG_request_data*, void* ) ) {
   gateway->impl_delete_manifest = impl_delete_manifest;
}

// set the gateway implementation getxattr routine 
void SG_impl_getxattr( struct SG_gateway* gateway, int (*impl_getxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) ) {
   gateway->impl_getxattr = impl_getxattr;
}

// set the gateway implementation listxattr routine 
void SG_impl_listxattr( struct SG_gateway* gateway, int (*impl_listxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk**, size_t*, void* ) ) {
   gateway->impl_listxattr = impl_listxattr;
}

// set the gateway implementation setxattr routine 
void SG_impl_setxattr( struct SG_gateway* gateway, int (*impl_setxattr)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) ) {
   gateway->impl_setxattr = impl_setxattr;
}

// set the gateway implementatio removexattr routine 
void SG_impl_removexattr( struct SG_gateway* gateway, int (*impl_removexattr)( struct SG_gateway*, struct SG_request_data*, void* ) ) {
   gateway->impl_removexattr = impl_removexattr;
}

// set the gateway implementation config_change routine 
void SG_impl_config_change( struct SG_gateway* gateway, int (*impl_config_change)( struct SG_gateway*, int, void* ) ) {
   gateway->impl_config_change = impl_config_change;
}

// get the gateway's gatewa-specific driver 
void* SG_gateway_cls( struct SG_gateway* gateway ) {
   return gateway->cls;
}

// get the gateway's config 
struct md_syndicate_conf* SG_gateway_conf( struct SG_gateway* gateway ) {
   return gateway->conf;
}

// get the gateway's driver 
struct SG_driver* SG_gateway_driver( struct SG_gateway* gateway ) {
   return gateway->driver;
}

// get the gateway's MS client 
struct ms_client* SG_gateway_ms( struct SG_gateway* gateway ) {
   return gateway->ms;
}

// get the gateway's cache 
struct md_syndicate_cache* SG_gateway_cache( struct SG_gateway* gateway ) {
   return gateway->cache;
}

// get the gateway's HTTP server
struct md_HTTP* SG_gateway_HTTP( struct SG_gateway* gateway ) {
   return gateway->http;
}

// get the gateway's downloader 
struct md_downloader* SG_gateway_dl( struct SG_gateway* gateway ) {
   return gateway->dl;
}

// is the gateway running?
bool SG_gateway_running( struct SG_gateway* gateway ) {
   return gateway->running;
}

// get gateway ID 
uint64_t SG_gateway_id( struct SG_gateway* gateway ) {
   return gateway->ms->gateway_id;
}

// get gateway user ID 
uint64_t SG_gateway_user_id( struct SG_gateway* gateway ) {
   return gateway->ms->owner_id;
}

// get gateway private key 
EVP_PKEY* SG_gateway_private_key( struct SG_gateway* gateway ) {
   return gateway->ms->gateway_key;
}


// get gateway public key 
EVP_PKEY* SG_gateway_public_key( struct SG_gateway* gateway ) {
   return gateway->ms->gateway_pubkey;
}

// get the first non-opt argument index
int SG_gateway_first_arg_optind( struct SG_gateway* gateway ) {
   return gateway->first_arg_optind;
}

// running in the foreground?
bool SG_gateway_foreground( struct SG_gateway* gateway ) {
   return gateway->foreground;
}

// set up a chunk
void SG_chunk_init( struct SG_chunk* chunk, char* data, off_t len ) {
   chunk->data = data;
   chunk->len = len;
}

// duplicate a chunk
// if dest's data is already allocated, try to expand it. 
// return 0 on success 
// return -ENOMEM on OOM
// either way, set dest->len to the required size 
int SG_chunk_dup( struct SG_chunk* dest, struct SG_chunk* src ) {
  
   dest->data = SG_CALLOC( char, src->len );
   if( dest->data == NULL ) {
      return -ENOMEM;
   }
   
   dest->len = src->len;
   memcpy( dest->data, src->data, src->len );
   
   return 0;
}


// copy or duplicate a chunk
// only copy if we have space; otherwise duplicate
// return 0 on success
// return -ENOMEM on OOM
// return -ERANGE if there's not enough space to copy 
int SG_chunk_copy_or_dup( struct SG_chunk* dest, struct SG_chunk* src ) {
   if( dest->data != NULL ) {
      if( dest->len < src->len ) {

         // too small
         return -ERANGE;
      }

      memcpy( dest->data, src->data, src->len );
      dest->len = src->len;
      return 0;
   }
   else {
      return SG_chunk_dup( dest, src );
   }
}



// copy one chunk's data to another 
// return 0 on success
// return -EINVAL if dest isn't big enough
int SG_chunk_copy( struct SG_chunk* dest, struct SG_chunk* src ) {
   
   if( dest->len < src->len ) {
      return -EINVAL;
   }
   
   memcpy( dest->data, src->data, src->len );
   dest->len = src->len;
   
   return 0;
}

// free a chunk 
void SG_chunk_free( struct SG_chunk* chunk ) {
   SG_safe_free( chunk->data );
   chunk->len = 0;
}


// fetch a block or serialized manifest from the on-disk cache 
// return 0 on success, and set *buf and *buf_len to the contents of the cached chunk
// return -ENOENT if not found 
// return -ENOMEM if OOM 
// return -errno if failed to read 
static int SG_gateway_cache_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t block_id_or_manifest_mtime_sec, int64_t block_version_or_manifest_mtime_nsec, struct SG_chunk* chunk ) {
   
   char* chunk_buf = NULL;
   ssize_t chunk_len = 0;
   int block_fd = 0;
   
   // stored on disk?
   block_fd = md_cache_open_block( gateway->cache, reqdat->file_id, reqdat->file_version, block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, O_RDONLY );
   
   if( block_fd < 0 ) {
      
      SG_warn("md_cache_open_block( %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
              reqdat->file_id, reqdat->file_version, SG_request_is_block( reqdat ) ? "block" : "manifest", block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path, block_fd );
      
      return block_fd;
   }
   
   // chunk opened.
   // read it 
   chunk_len = md_cache_read_block( block_fd, &chunk_buf );
   if( chunk_len < 0 ) {
      
      SG_error("md_cache_read_block( %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, SG_request_is_block( reqdat ) ? "block" : "manifest", block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path, (int)chunk_len );
      
      return (int)chunk_len;
   }
   
   close( block_fd );
   
   // success! promote!
   md_cache_promote_block( gateway->cache, reqdat->file_id, reqdat->file_version, block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec );
   
   SG_debug("CACHE HIT on %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s)\n",
            reqdat->file_id, reqdat->file_version, SG_request_is_block( reqdat ) ? "block" : "manifest", block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path );
   
   SG_chunk_init( chunk, chunk_buf, chunk_len );
   return 0;
}


// asynchronously put a driver-transformed chunk of data directly into the cache.
// chunk must persist until the future completes, unless the cache is going to get its own copy
// (indicated with SG_GATEWAY_CACHE_UNSHARED) or the caller is gifting it (SG_GATEWAY_CACHE_DETACHED)
// return 0 on success, and set *cache_fut to a newly-allocated future, which the caller can wait on
// return negative on failure to begin writing the chunk.
static int SG_gateway_cache_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t block_id_or_manifest_mtime_sec, int64_t block_version_or_manifest_mtime_nsec,
                                           struct SG_chunk* chunk, uint64_t cache_flags, struct md_cache_block_future** cache_fut ) {
   
   int rc = 0;
   struct md_cache_block_future* f = NULL;
   
   // cache the new chunk.  Get back the future (caller will manage it).
   f = md_cache_write_block_async( gateway->cache, reqdat->file_id, reqdat->file_version, block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, chunk->data, chunk->len, cache_flags, &rc );
   
   if( f == NULL ) {
      
      SG_error("md_cache_write_block_async( %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, (SG_request_is_block( reqdat ) ? "block" : "manifest"), block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path, rc );
               
      return rc;
   }
   
   // for debugging...
   char prefix[21];
   memset( prefix, 0, 21 );
   memcpy( prefix, chunk->data, MIN( 20, chunk->len ) );
   
   SG_debug("CACHE PUT %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s) %zu bytes, data: '%s'...\n",
            reqdat->file_id, reqdat->file_version, (SG_request_is_block( reqdat ) ? "block" : "manifest"), block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path, chunk->len, prefix );
   
   *cache_fut = f;
   
   return rc;
}

// read from the on-disk block cache.
// do not apply the driver, since the caller may just want to deal with the data as-is
// return 0 on success, and set *buf and *buf_len to the contents of the obtained block
// return -ENOENT if not hit.
// return -EINVAL if the request data structure isn't for a block.
// return -ENOMEM if OOM
// return negative on error
int SG_gateway_cached_block_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* chunk ) {
   
   int rc = 0;
   
   // sanity check 
   if( !SG_request_is_block( reqdat ) ) {
      return -EINVAL;
   }
   
   // lookaside: if this block is being written, then we can't read it 
   rc = md_cache_is_block_readable( gateway->cache, reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version );
   if( rc == -EAGAIN ) {
      
      // not available in the cache 
      return -ENOENT;
   }
   
   // check cache 
   rc = SG_gateway_cache_get_raw( gateway, reqdat, reqdat->block_id, reqdat->block_version, chunk );
   if( rc != 0 ) {
      
      // not available in the cache 
      return rc;
   }
   
   return rc;
}

// get a manifest from the cache, without processing it
// return 0 on success, and set *manifest_buf and *manifest_buf_len to the allocated buffer and its length 
// return -ENOMEM if OOM 
// return negative on I/O error
int SG_gateway_cached_manifest_get_raw( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest ) {
   
   int rc = 0;
   
   // sanity check 
   if( !SG_request_is_manifest( reqdat ) ) {
      SG_error("Not a manifest request: %p\n", reqdat);
      return -EINVAL;
   }
   
   // lookaside: if this manifest is being written, then we can't read it 
   rc = md_cache_is_block_readable( gateway->cache, reqdat->file_id, reqdat->file_version, (uint64_t)reqdat->manifest_timestamp.tv_sec, (int64_t)reqdat->manifest_timestamp.tv_nsec );
   if( rc == -EAGAIN ) {
      
      // not available in the cache 
      SG_error("Chunk is not readable at this time: %p\n", reqdat);
      return -ENOENT;
   }
   else if( rc != 0 ) {

      SG_error("md_cache_is_block_readable rc = %d\n", rc );
      return rc;
   }
   
   // check cache disk 
   rc = SG_gateway_cache_get_raw( gateway, reqdat, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, raw_serialized_manifest );
   if( rc != 0 ) {
      
      // not available in the cache 
      SG_error("Chunk is not in the cache (rc = %d)\n", rc);
      return rc;
   }
   
   return rc;
}


// Put a block directly into the cache 
// return 0 on success, and set *cache_fut to the the future the caller can wait on 
// return -EINVAL if this isn't a block request 
// return negative on I/O error
int SG_gateway_cached_block_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t cache_flags, struct md_cache_block_future** cache_fut ) {
   
   if( !SG_request_is_block( reqdat ) ) {
      return -EINVAL;
   }
   
   return SG_gateway_cache_put_raw_async( gateway, reqdat, reqdat->block_id, reqdat->block_version, block, cache_flags, cache_fut );
}
   

// asynchronously put a serialized manifest directly into the cache 
// return 0 on success, and set *manifest_fut to the newly-allocated cache future, which the caller can wait on to complete the write 
// return -EINVAL if this isn't a manifest request 
// return negative on I/O error 
int SG_gateway_cached_manifest_put_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* raw_serialized_manifest, uint64_t cache_flags, struct md_cache_block_future** manifest_fut ) {
   
   if( !SG_request_is_manifest( reqdat ) ) {
      return -EINVAL;
   }
   
   return SG_gateway_cache_put_raw_async( gateway, reqdat, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, raw_serialized_manifest, cache_flags, manifest_fut );
}


// start an I/O request on one of the gateway's I/O work queues
// return 0 on success 
// return negative on error
// NOTE: wreq and all of its data must be heap-allocated.  The gateway will take ownership.
int SG_gateway_io_start( struct SG_gateway* gateway, struct md_wreq* wreq ) {
   
   int rc = 0;
   int wq_num = md_random64() % gateway->num_iowqs;     // NOTE: this is slightly biased
   
   rc = md_wq_add( &gateway->iowqs[wq_num], wreq );
   return rc;
}


// get thread worker ID
uint64_t SG_gateway_io_thread_id( struct SG_gateway* gateway ) {
   
   union {
      pthread_t t;
      uint64_t i;
   } io_thread_id_data;

   io_thread_id_data.i = 0;
   io_thread_id_data.t = pthread_self();
   
   return io_thread_id_data.i;
}


// connect to the network caches of this volume, given the URL to the requested chunk
// return 0 on success, and program the CURL handle
// return -ENOSYS if not defined
// return non-zero on implementation error
int SG_gateway_impl_connect_cache( struct SG_gateway* gateway, CURL* curl, char const* url ) {

   int rc = 0;

   if( gateway->impl_connect_cache != NULL ) {

      rc = (*gateway->impl_connect_cache)( gateway, curl, url, gateway->cls );
      if( rc != 0) {

         SG_error("gateway->impl_connect_cache('%s') rc = %d\n", url, rc );
      }

      return rc;
   }
   else {

      return -ENOSYS;
   }
}


// stat a file, filling in what we know into the out_reqdat structure 
// return 0 on success, populating *out_reqdat 
// return -ENOSYS if not defined
// return non-zero on implementation error 
int SG_gateway_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* out_mode ) {
   
   int rc = 0;
   
   if( gateway->impl_stat != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_stat)( gateway, reqdat, out_reqdat, out_mode, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_stat( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// stat a file's block, filling in what we know into the out_reqdat structure 
// return 0 on success, populating *out_reqdat 
// return -ENOSYS if not defined
// return non-zero on implementation error 
int SG_gateway_impl_stat_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* out_mode ) {
   
   int rc = 0;
   
   if( gateway->impl_stat_block != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_stat_block)( gateway, reqdat, out_reqdat, out_mode, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_stat_block( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// truncate a file.
// the implementation MUST reversion the file to complete the operation 
// return 0 on success
// return -ENOSYS if not defined 
// return non-zero on implementation error 
int SG_gateway_impl_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size ) {
   
   int rc = 0;
   
   if( gateway->impl_truncate != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_truncate)( gateway, reqdat, new_size, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_truncate( %" PRIX64 ".%" PRId64 " (%s), %" PRIu64 " ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, new_size, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// rename a file 
// the implementation MUST inform the MS of the rename
// return 0 on success 
// return -ENOSYS if not defined 
// return non-zero on implementation error 
int SG_gateway_impl_rename( struct SG_gateway* gateway, struct SG_request_data* reqdat, char const* new_path ) {
   
   int rc = 0;
   
   if( gateway->impl_rename != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_rename)( gateway, reqdat, new_path, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_rename( %" PRIX64 ".%" PRId64 " (%s), %s ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, new_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// detach a file 
// the implementation MUST inform the MS of the detach 
// return 0 on success 
// return -ENOSYS if not defined 
// return non-zero on implementation error 
int SG_gateway_impl_detach( struct SG_gateway* gateway, struct SG_request_data* reqdat ) {
   
   int rc = 0;
   
   if( gateway->impl_detach != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_detach)( gateway, reqdat, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_detach( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// serialize a chunk, making it suitable for storage and transmission
// return 0 on success
// return -ENOSYS if not defined
// return non-zero on error 
int SG_gateway_impl_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk ) {

   int rc = 0;
   
   if( gateway->impl_serialize != NULL ) {

      rc = (*gateway->impl_serialize)( gateway, reqdat, in_chunk, out_chunk, gateway->cls );
      if( rc != 0 ) {

         SG_error("gateway->impl_serialize( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }

      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// deserialize a chunk, making it suitable for consumption by a client program
// *out_chunk may be allocated already (i.e. if the chunk's length is known).  The implementation must accomodate this possibility.
// return 0 on success
// return -ENOSYS if not defined
// return non-zero on error 
int SG_gateway_impl_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk ) {

   int rc = 0;
   if( gateway->impl_deserialize != NULL ) {

      rc = (*gateway->impl_deserialize)( gateway, reqdat, in_chunk, out_chunk, gateway->cls );
      if( rc != 0 ) {

         SG_error("gateway->impl_deserialize( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }

      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// get a manifest from the implementation
// return 0 on success, and populate *manifest 
// return -ENOSYS if not defined
// return non-zero on implementation-specific failure 
int SG_gateway_impl_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints ) {
   
   int rc = 0;
   
   if( gateway->impl_get_manifest != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_get_manifest)( gateway, reqdat, manifest, hints, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_get_manifest( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
                   reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// put a protobuf'ed manifest into the implementation 
// return 0 on success
// return -ENOSYS if not implemented
// return non-zero on implementation-specific error 
int SG_gateway_impl_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* chunk, uint64_t hints ) {
   
   int rc = 0;
   
   if( gateway->impl_put_manifest != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_put_manifest)( gateway, reqdat, chunk, hints, gateway->cls );
      if( rc != 0 ) {
         
         SG_error("gateway->impl_put_manifest( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
                   reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// patch a manifest 
// the gateway MUST inform the MS of the new manifest information 
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta ) {
   
   int rc = 0;
   
   if( gateway->impl_patch_manifest != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_patch_manifest)( gateway, reqdat, write_delta, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_patch_manifest( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// delete a manifest 
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_manifest_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat) {
   
   int rc = 0;
   
   if( gateway->impl_delete_manifest != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_delete_manifest)( gateway, reqdat, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_delete_manifest( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// get a block from the implementation, directly.
// fill in the given block with data.
// return 0 on success, and populate *block with new data
// return -ENOSYS if not defined
// return non-zero on implementation-specific error 
int SG_gateway_impl_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints ) {
   
   int rc = 0;
   
   if( gateway->impl_get_block != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_get_block)( gateway, reqdat, block, hints, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_get_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
                  reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// put a block into the implementation 
// return 0 on success 
// return non-zero on implementation error 
int SG_gateway_impl_block_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints ) {
   
   int rc = 0;
   
   if( gateway->impl_put_block != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_put_block)( gateway, reqdat, block, hints, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_put_block( %" PRIX64 ".%" PRId64 "[%" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
                   reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// delete a block in the implementation 
// return 0 on success 
// return non-zero on implementation error 
int SG_gateway_impl_block_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat ) {
   
   int rc = 0;
   
   if( gateway->impl_delete_block != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_delete_block)( gateway, reqdat, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_delete_block( %" PRIX64 ".%" PRId64 " [%" PRIu64 ".%" PRId64 "] (%s) rc = %d\n", 
                  reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// get an xattr
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_getxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* xattr_value ) {
   
   int rc = 0;
   
   if( gateway->impl_getxattr != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_getxattr)( gateway, reqdat, xattr_value, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_getxattr( %" PRIX64 ".%" PRId64 " (%s) %s.%" PRId64 " ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, reqdat->xattr_name, reqdat->xattr_nonce, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// list xattrs 
// return 0 on success 
// return non-zero on implementation error 
int SG_gateway_impl_listxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk** xattr_names, size_t* num_xattrs ) {
   
   int rc = 0;
   
   if( gateway->impl_listxattr != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_listxattr)( gateway, reqdat, xattr_names, num_xattrs, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_listxattr( %" PRIX64 ".%" PRId64 " (%s) ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// set an xattr
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_setxattr( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* xattr_value ) {
   
   int rc = 0;
   
   if( gateway->impl_setxattr != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_setxattr)( gateway, reqdat, xattr_value, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_setxattr( %" PRIX64 ".%" PRId64 " (%s) %s.%" PRId64 " ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, reqdat->xattr_name, reqdat->xattr_nonce, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// remove an xattr
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_removexattr( struct SG_gateway* gateway, struct SG_request_data* reqdat ) {
   
   int rc = 0;
   
   if( gateway->impl_removexattr != NULL ) {
      
      reqdat->io_thread_id = SG_gateway_io_thread_id( gateway );
      rc = (*gateway->impl_removexattr)( gateway, reqdat, gateway->cls );
      
      if( rc != 0 ) {
         
         SG_error("gateway->impl_removexattr( %" PRIX64 ".%" PRId64 " (%s) %s.%" PRId64 " ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->fs_path, reqdat->xattr_name, reqdat->xattr_nonce, rc );
      }
      
      return rc;
   }
   else {
      
      return -ENOSYS;
   }
}


// convert an md_entry into an SG_request_data 
// return 0 on success
// return -ENOMEM on OOM 
int SG_request_data_from_md_entry( struct SG_request_data* reqdat, char const* fs_path, struct md_entry* ent, uint64_t block_id, int64_t block_version ) {
   
   memset( reqdat, 0, sizeof(struct SG_request_data) );
   
   reqdat->user_id = ent->owner;
   reqdat->volume_id = ent->volume;
   reqdat->file_id = ent->file_id;
   reqdat->coordinator_id = ent->coordinator;
   reqdat->fs_path = SG_strdup_or_null( fs_path );
   reqdat->file_version = ent->version;
   reqdat->block_id = block_id;
   reqdat->block_version = block_version;
   reqdat->manifest_timestamp.tv_sec = ent->manifest_mtime_sec;
   reqdat->manifest_timestamp.tv_nsec = ent->manifest_mtime_nsec;
   reqdat->getxattr = false;
   reqdat->listxattr = false;
   reqdat->xattr_name = NULL;
   reqdat->xattr_nonce = ent->xattr_nonce;
   
   if( reqdat->fs_path == NULL ) {
      return -ENOMEM;
   }
   
   return 0;
}
