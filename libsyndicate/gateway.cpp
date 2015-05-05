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
#include "libsyndicate/client.h"

#include "libsyndicate/ms/gateway.h"
#include "libsyndicate/ms/core.h"

MD_CLOSURE_PROTOTYPE_BEGIN( SG_CLOSURE_PROTOTYPE )
   MD_CLOSURE_CALLBACK( "SG_driver_name" ),
   MD_CLOSURE_CALLBACK( "SG_connect_cache" ),
   MD_CLOSURE_CALLBACK( "SG_put_block" ),
   MD_CLOSURE_CALLBACK( "SG_put_manifest" ),
   MD_CLOSURE_CALLBACK( "SG_get_block" ),
   MD_CLOSURE_CALLBACK( "SG_get_manifest" ),
   MD_CLOSURE_CALLBACK( "SG_delete_block" ),
   MD_CLOSURE_CALLBACK( "SG_delete_manifest" ),
   MD_CLOSURE_CALLBACK( "SG_chcoord_begin" ),
   MD_CLOSURE_CALLBACK( "SG_chcoord_end" ),
   MD_CLOSURE_CALLBACK( "SG_create_file" ),
   MD_CLOSURE_CALLBACK( "SG_delete_file" )
MD_CLOSURE_PROTOTYPE_END

// gateway for which we are running the main() loop
static struct SG_gateway* g_main_gateway = NULL;

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
   
   return 0;
}


// initialize a request data structure for a block 
// return 0 on success 
// return -ENOMEM on OOM
int SG_request_data_init_block( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, struct SG_request_data* reqdat ) {

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
   reqdat->block_id = block_id;
   reqdat->block_version = block_version;
   
   return 0;
}


// initialize a reqeust data structure for a manifest 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_request_data_init_manifest( struct SG_gateway* gateway, char const* fs_path, uint64_t file_id, int64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, struct SG_request_data* reqdat ) {

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
   reqdat->manifest_timestamp.tv_sec = manifest_mtime_sec;
   reqdat->manifest_timestamp.tv_nsec = manifest_mtime_nsec;
   
   return 0;
}


// parse an SG request from a URL.
// use md_HTTP_parse_url_path
// return 0 on success
// return -EINVAL if the URL is malformed 
// return -ENOMEM if OOM
int SG_request_data_parse( struct SG_request_data* reqdat, char const* url_path ) {
   
   memset( reqdat, 0, sizeof(struct SG_request_data) );
   return md_HTTP_parse_url_path( url_path, &reqdat->volume_id, &reqdat->fs_path, &reqdat->file_id, &reqdat->file_version, &reqdat->block_id, &reqdat->block_version, &reqdat->manifest_timestamp );
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
   
   return (reqdat->manifest_timestamp.tv_sec > 0 && reqdat->manifest_timestamp.tv_nsec >= 0);
}

// free a gateway_request_data
void SG_request_data_free( struct SG_request_data* reqdat ) {
   if( reqdat->fs_path != NULL ) {
      SG_safe_free( reqdat->fs_path );
   }
   memset( reqdat, 0, sizeof(struct SG_request_data) );
}

// merge opts and config 
// return 0 on success
// return -ENOMEM on OOM 
static int SG_config_merge_opts( struct md_syndicate_conf* conf, struct md_opts* opts ) {
   
   // set maximum of command-line and file-given debug levels
   md_set_debug_level( MAX( opts->debug_level, md_get_debug_level() ) );
   
   // opts overrides config for hostname
   if( opts->hostname != NULL ) {
      int rc = md_set_hostname( conf, opts->hostname );
      if( rc != 0 ) {
         
         SG_error("md_set_hostname('%s') rc = %d\n", opts->hostname, rc );
         return rc;
      }
   }
   
   // opts overrides config for cache limits 
   if( opts->cache_hard_limit != 0 ) {
      conf->cache_hard_limit = opts->cache_hard_limit;
   }
   
   if( opts->cache_soft_limit != 0 ) {
      conf->cache_soft_limit = opts->cache_soft_limit;
   }
   
   return 0;
}



// initialize the closure
// if this fails due to there being no closure for this gateway, a dummy closure will be used instead
// return 0 on success, and set *_ret to a newly-allocated closure
// return -ENOENT if there is no closure for this gateway
// return -ENOMEM on OOM
// return -errno on failure to initialize the closure
int SG_gateway_closure_init( struct ms_client* ms, struct md_syndicate_conf* conf, struct md_closure* closure ) {
   
   // get the closure text 
   char* closure_text = NULL;
   uint64_t closure_text_len = 0;
   int rc = 0;
   
   // get the closure text
   rc = ms_client_get_closure_text( ms, &closure_text, &closure_text_len );
   if( rc != 0 ) {
      
      // TODO: anonymous gateway closures
      if( rc == -ENODATA && conf->is_client ) {
         // stub closure 
         SG_warn("%s", "Anonymous gateway; using stub closure\n");
         return -ENOENT;
      }
         
      // some other error
      SG_error("ms_client_get_closure_text rc = %d\n", rc );
      return rc;
   }
   
   // create the closure
   rc = md_closure_init( closure, conf, ms->gateway_pubkey, ms->gateway_key, SG_CLOSURE_PROTOTYPE, closure_text, closure_text_len, true );
   
   SG_safe_free( closure_text );
   
   return rc;
}


// reload a gateway's certificates
// call this after refreshing a volume.
// * download the manifest, then download the certificates in parallel 
// * calculate the difference between the client's current certificates and the new ones.
// * expire the old ones
// * trust the new ones
// return 0 on success
// return -ENOMEM on OOM 
// return -ESTALE if the volume cert is too old at the time of refresh 
// return -errno on failure to download and parse a certificate or certificate manifest
int SG_gateway_reload_certs( struct SG_gateway* gateway, uint64_t cert_version ) {
   
   uint64_t volume_id = 0;
   int rc = 0;
   struct SG_manifest manifest;
   struct SG_manifest to_download;
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct ms_gateway_cert* cert = NULL;
   struct md_download_loop dlloop;
   struct SG_manifest_block* cert_block = NULL;
   ms_cert_bundle* new_cert_bundle = NULL;
   ms_cert_bundle* old_cert_bundle = NULL;
   struct md_download_context* dlctx = NULL;
   uint64_t cert_gateway_id = 0;
   
   if( rc != 0 ) {
      return rc;
   }
   
   // get the certificate manifest...
   rc = SG_client_cert_manifest_download( gateway, cert_version, &manifest );
   if( rc != 0 ) {
      
      SG_error("SG_client_cert_manifest_download( %" PRIu64 " ) rc = %d\n", volume_id, rc );
      return rc;
   }
   
   SG_debug("Got cert manifest with %" PRIu64 " certificates\n", SG_manifest_get_block_count( &manifest ) );
   
   // revoke old certs from the MS client
   rc = ms_client_revoke_certs( ms, &manifest );
   if( rc != 0 ) { 
      
      SG_error("ms_client_revoke_certs(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      
      SG_manifest_free( &manifest );
      return rc;
   }
   
   // manifest of revoked certificates 
   rc = SG_manifest_init( &to_download, manifest.volume_id, manifest.coordinator_id, manifest.file_id, manifest.file_version );
   if( rc != 0 ) {
      
      SG_error("SG_manifest_init rc = %d\n", rc );
      
      SG_manifest_free( &manifest );
      return rc;
   }
   
   // find all certs that need to be downloaded, and put them into to_download
   for( SG_manifest_block_map_t::iterator itr = manifest.blocks->begin(); itr != manifest.blocks->end(); itr++ ) {
      
      cert_block = &itr->second;
      
      // revoked cert info
      uint64_t gateway_id = cert_block->block_id;
      uint64_t gateway_cert_version = (uint64_t)cert_block->block_version;
      
      struct SG_manifest_block revoked_cert_info;
      
      // was this certificate revoked?
      cert = ms_client_get_gateway_cert( ms, gateway_id );
      if( cert == NULL ) {
         
         // yup 
         rc = SG_manifest_block_init( &revoked_cert_info, gateway_id, gateway_cert_version, NULL, 0 );
         if( rc != 0 ) {
            
            SG_error("SG_manifest_block_init rc = %d\n", rc );
            
            SG_manifest_free( &manifest );
            SG_manifest_free( &to_download );
            return rc;
         }
         
         rc = SG_manifest_put_block( &to_download, &revoked_cert_info, true );
         if( rc != 0 ) {
            
            SG_error("SG_manifest_put_block rc = %d\n", rc );
            
            SG_manifest_free( &manifest );
            SG_manifest_free( &to_download );
            return rc;
         }
      }
   }
   
   // new cert bundle 
   new_cert_bundle = SG_safe_new( ms_cert_bundle() );
   if( new_cert_bundle == NULL ) {
      
      // OOM
      SG_manifest_free( &manifest );
      SG_manifest_free( &to_download );
      
      return -ENOMEM;
   }
   
   // prepare to download certs
   rc = md_download_loop_init( &dlloop, gateway->dl, MIN( (unsigned)ms->max_connections, to_download.blocks->size() ) );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_init rc = %d\n", rc );
      
      SG_manifest_free( &manifest );
      SG_manifest_free( &to_download );
      SG_safe_delete( new_cert_bundle );
      return rc;
   }
   
   SG_manifest_block_map_t::iterator itr = to_download.blocks->begin();
   
   // download each cert 
   do {
      
      // start as many downloads as we can 
      while( itr != to_download.blocks->end() ) {
         
         // next cert download 
         uint64_t gateway_id = itr->second.block_id;
         
         rc = md_download_loop_next( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // start it 
         rc = SG_client_cert_download_async( gateway, &to_download, gateway_id, &dlloop, dlctx );
         if( rc != 0 ) {
            
            SG_error("SG_client_cert_download_async( %" PRIu64 " ) rc = %d\n", gateway_id, rc );
            break;
         }
         
         // next block
         itr++;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // wait for at least one of the downloads to finish 
      rc = md_download_loop_run( &dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // find the finished downloads.  check at least once.
      while( true ) {
         
         struct ms_gateway_cert* cert = NULL;
      
         rc = md_download_loop_finished( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // out of finished downloads 
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finished rc = %d\n", rc );
            break;
         }
         
         // allocate the cert 
         cert = SG_CALLOC( struct ms_gateway_cert, 1 );
         if( cert == NULL ) {
            
            // OOM 
            rc = -ENOMEM;
            break;
         }
         
         // process it, and free up the download handle
         rc = SG_client_cert_download_finish( gateway, dlctx, &cert_gateway_id, cert );
         if( rc != 0 ) {
            
            SG_error("SG_client_cert_download_finish rc = %d\n", rc );
            
            SG_safe_free( cert );
            break;
         }
         
         // insert it 
         try {
            
            (*new_cert_bundle)[ cert->gateway_id ] = cert;
         }
         catch( bad_alloc& ba ) {
            
            SG_safe_free( cert );
            rc = -ENOMEM;
            break;
         }  
      }
      
      if( rc != 0 ) {
         break;
      }
      
   } while( md_download_loop_running( &dlloop ) );
   
   // failure?
   if( rc != 0 ) {
      
      md_download_loop_abort( &dlloop );
      SG_client_download_async_cleanup_loop( &dlloop );
      
      // free all 
      ms_client_cert_bundle_free( new_cert_bundle );
      SG_safe_delete( new_cert_bundle );
   }
   else {
      
      // have new cert bundle!
      old_cert_bundle = ms_client_reload_certs( ms, new_cert_bundle, manifest.file_version );
      
      ms_client_cert_bundle_free( old_cert_bundle );
      SG_safe_delete( old_cert_bundle );
   }
   
   md_download_loop_cleanup( &dlloop, NULL, NULL );
   md_download_loop_free( &dlloop );
   SG_manifest_free( &manifest );
   SG_manifest_free( &to_download );
   
   return rc;
}


// initialize and start the gateway 
// return -ENOMEM of OOM
// return -ENOENT if a file was not found 
// return negative if libsyndicate fails to initialize
// return 1 if the user wanted help
int SG_gateway_init( struct SG_gateway* gateway, uint64_t gateway_type, bool anonymous, int argc, char** argv ) {
   
   int rc = 0;
   struct md_opts opts;
   struct ms_client* ms = SG_CALLOC( struct ms_client, 1 );
   struct md_syndicate_conf* conf = SG_CALLOC( struct md_syndicate_conf, 1 );
   struct md_syndicate_cache* cache = SG_CALLOC( struct md_syndicate_cache, 1 );
   struct md_HTTP* http = SG_CALLOC( struct md_HTTP, 1 );
   struct md_closure* closure = SG_CALLOC( struct md_closure, 1 );
   struct md_downloader* dl = SG_CALLOC( struct md_downloader, 1 );
   struct md_wq* iowqs = NULL;
   int num_iowqs = 0;
   
   sem_t config_sem;
   
   bool opts_inited = false;
   bool md_inited = false;
   bool ms_inited = false;
   bool config_inited = false;
   bool cache_inited = false;
   bool http_inited = false;
   bool closure_inited = false;
   bool dl_inited = false;
   
   uint64_t block_size = 0;
   uint64_t cert_version = 0;
   
   int first_arg_optind = 0;
   
   if( ms == NULL || conf == NULL || cache == NULL || http == NULL || dl == NULL ) {
      
      rc = -ENOMEM;
      goto SG_gateway_init_error;
   }
   
   // load config
   md_default_conf( conf, gateway_type );
   memset( &opts, 0, sizeof(struct md_opts));
   
   // get options
   rc = md_opts_parse( &opts, argc, argv, &first_arg_optind, NULL, NULL );
   if( rc != 0 ) {
      
      goto SG_gateway_init_error;
   }
   
   // advance!
   opts_inited = true;
   
   // set debug level
   md_set_debug_level( opts.debug_level );
   
   // read the config file, if given
   if( opts.config_file != NULL ) {
      
      rc = md_read_conf( opts.config_file, conf );
      if( rc != 0 ) {
         SG_error("md_read_conf('%s'), rc = %d\n", opts.config_file, rc );
         
         goto SG_gateway_init_error;
      }
   }
   
   rc = SG_config_merge_opts( conf, &opts );
   if( rc != 0 ) {
      
      SG_error("SG_config_merge_opts rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   // allocate I/O work queues 
   iowqs = SG_CALLOC( struct md_wq, conf->num_iowqs );
   
   if( iowqs == NULL ) {
      
      // OOM 
      rc = -ENOMEM;
      goto SG_gateway_init_error;
   }
   
   // initialize library
   if( !anonymous ) {
      
      // initialize peer
      SG_debug("%s", "Not anonymous; initializing as peer\n");
      
      rc = md_init( conf, ms, &opts );
      if( rc != 0 ) {
         
         goto SG_gateway_init_error;
      }
   }
   else {
      
      // initialize client
      SG_debug("%s", "Anonymous; initializing as client\n");
      
      rc = md_init_client( conf, ms, &opts );
      if( rc != 0 ) {
         
         goto SG_gateway_init_error;
      }
   }
   
   md_opts_free( &opts );
   opts_inited = false;
   
   // advance!
   md_inited = true;
   ms_inited = true;
   
   // initialize config reload 
   sem_init( &config_sem, 0, 0 );
   
   // advance!
   config_inited = true;
   
   // initialize workqueues 
   for( num_iowqs = 0; num_iowqs < conf->num_iowqs; num_iowqs++ ) {
      
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
      
      // advance!
      http_inited = true;
      
      // set up HTTP server methods 
      SG_server_HTTP_install_handlers( http );
   }
   else {
      
      // won't need the HTTP server 
      SG_safe_free( http );
   }
   
   // load closure 
   rc = SG_gateway_closure_init( ms, conf, closure );
   if( rc != 0 && rc != -ENOENT ) {
      
      SG_error("SG_gateway_closure_init rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   // advance 
   closure_inited = true;
   
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
   
   // initialize gateway runtime, so we can start HTTP and get certificates
   gateway->ms = ms;
   gateway->conf = conf;
   gateway->cache = cache;
   gateway->http = http;
   gateway->closure = closure;
   gateway->dl = dl;
   gateway->config_sem = config_sem;
   gateway->iowqs = iowqs;
   gateway->num_iowqs = num_iowqs;
   gateway->first_arg_optind = first_arg_optind;
   
   // get certificates 
   cert_version = ms_client_cert_version( ms );
   
   rc = SG_gateway_reload_certs( gateway, cert_version );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_reload_certs rc = %d\n", rc );
      
      goto SG_gateway_init_error;
   }
   
   if( gateway->http != NULL ) {
      
      // start HTTP server 
      rc = md_HTTP_start( gateway->http, ms_client_get_portnum( ms ), gateway->conf );
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
      
      if( dl->running ) {
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
   
   if( closure_inited ) {
      md_closure_shutdown( closure );
   }
   
   SG_safe_free( closure );
   
   if( config_inited ) {
      sem_destroy( &config_sem );
   }
   
   if( ms_inited ) {
      ms_client_destroy( ms );
   }
   
   for( int i = 0; i < num_iowqs; i++ ) {
      
      md_wq_stop( &iowqs[i] );
      md_wq_free( &iowqs[i], NULL );
   }
   
   SG_safe_free( iowqs );
   
   SG_safe_free( ms );
   
   md_free_conf( conf );
   SG_safe_free( conf );
   
   if( md_inited ) {
      md_shutdown();
   }
   
   if( opts_inited ) {
      md_opts_free( &opts );
   }
      
   return rc;
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
   
   md_downloader_stop( gateway->dl );
   md_downloader_shutdown( gateway->dl );
   SG_safe_free( gateway->dl );
      
   if( gateway->http != NULL ) {
      md_HTTP_stop( gateway->http );
      md_HTTP_free( gateway->http );
      SG_safe_free( gateway->http );
   }
   
   md_cache_stop( gateway->cache );
   md_cache_destroy( gateway->cache );
   SG_safe_free( gateway->cache );
   
   md_closure_shutdown( gateway->closure );
   SG_safe_free( gateway->closure );
   
   ms_client_destroy( gateway->ms );
   SG_safe_free( gateway->ms );
   
   for( int i = 0; i < gateway->num_iowqs; i++ ) {
      
      md_wq_stop( &gateway->iowqs[i] );
      md_wq_free( &gateway->iowqs[i], NULL );
   }
   
   SG_safe_free( gateway->iowqs );
   
   md_free_conf( gateway->conf );
   SG_safe_free( gateway->conf );
   
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
      uint64_t new_cert_version = 0;
      
      struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
      struct ms_client* ms = SG_gateway_ms( gateway );
      
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
      
      // reload Volume metadata
      SG_debug("%s", "Reload volume\n" );

      rc = ms_client_reload_volume( ms, &new_cert_version );

      if( rc != 0 ) {
         
         SG_warn("ms_client_reload_volume rc = %d\n", rc);
      }
      
      else {
         
         // reload certificates
         rc = SG_gateway_reload_certs( gateway, new_cert_version );
         if( rc != 0 ) {
            
            SG_warn("SG_gateway_reload_certs rc = %d\n", rc );
         }
         
         else if( gateway->impl_config_change != NULL ) {
            
            // gateway-specific config reload 
            rc = (*gateway->impl_config_change)( gateway, gateway->cls );
            if( rc != 0 ) {
               
               SG_warn( "gateway->impl_config_change rc = %d\n", rc );
            }
         }
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

// set the gateway implementation stat routine 
void SG_impl_stat( struct SG_gateway* gateway, int (*impl_stat)( struct SG_gateway*, struct SG_request_data*, struct SG_request_data*, mode_t*, void* ) ) {
   gateway->impl_stat = impl_stat;
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

// set the gateway implemenetation get_block routine 
void SG_impl_get_block( struct SG_gateway* gateway, int (*impl_get_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) ) {
   gateway->impl_get_block = impl_get_block;
}

// set the gateway implementation put_block routine 
void SG_impl_put_block( struct SG_gateway* gateway, int (*impl_put_block)( struct SG_gateway*, struct SG_request_data*, struct SG_chunk*, void* ) ) {
   gateway->impl_put_block = impl_put_block;
}

// set the gateway implementation delete_block routine 
void SG_impl_delete_block( struct SG_gateway* gateway, int (*impl_delete_block)( struct SG_gateway*, struct SG_request_data*, void* ) ) {
   gateway->impl_delete_block = impl_delete_block;
}

// set the gateway implementation get_manifest routine
void SG_impl_get_manifest( struct SG_gateway* gateway, int (*impl_get_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) ) {
   gateway->impl_get_manifest = impl_get_manifest;
}

// set the gateway implementation put_manifest routine 
void SG_impl_put_manifest( struct SG_gateway* gateway, int (*impl_put_manifest)( struct SG_gateway*, struct SG_request_data*, struct SG_manifest*, void* ) ) {
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

// set the gateway implementation config_change routine 
void SG_impl_config_change( struct SG_gateway* gateway, int (*impl_config_change)( struct SG_gateway*, void* ) ) {
   gateway->impl_config_change = impl_config_change;
}

// get the gateway's gatewa-specific closure 
void* SG_gateway_cls( struct SG_gateway* gateway ) {
   return gateway->cls;
}

// get the gateway's config 
struct md_syndicate_conf* SG_gateway_conf( struct SG_gateway* gateway ) {
   return gateway->conf;
}

// get the gateway's closure 
struct md_closure* SG_gateway_closure( struct SG_gateway* gateway ) {
   return gateway->closure;
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

// get gateway implementation hints 
uint64_t SG_gateway_impl_hints( struct SG_gateway* gateway ) {
   return gateway->hints;
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

// get the first non-opt argument index
int SG_gateway_first_arg_optind( struct SG_gateway* gateway ) {
   return gateway->first_arg_optind;
}

// set up a chunk
void SG_chunk_init( struct SG_chunk* chunk, char* data, off_t len ) {
   chunk->data = data;
   chunk->len = len;
}

// duplicate a chunk 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_chunk_dup( struct SG_chunk* dest, struct SG_chunk* src ) {
   
   dest->data = SG_CALLOC( char, src->len );
   if( dest->data == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( dest->data, src->data, src->len );
   dest->len = src->len;
   
   return 0;
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


// set up a CURL handle for connecting to the CDNs, using the gateway'c closure 
// return 0 on success, or if the method is not defined
int SG_gateway_closure_connect_cache( struct SG_gateway* gateway, CURL* curl, char const* url ) {
   
   int rc = 0;
   struct md_closure* closure = SG_gateway_closure( gateway );
   
   if( md_closure_find_callback( closure, "SG_connect_cache" ) != NULL ) {
      
      MD_CLOSURE_CALL( rc, closure, "SG_connect_cache", SG_closure_connect_cache_func, gateway, curl, url, gateway->cls );
      
      if( rc != 0 ) {
      
         SG_error("SG_connect_cache( '%s' ) rc = %d\n", url, rc );
      }
      
      return rc;
   }
   else {
      
      // stub 
      SG_warn("'SG_connect_cache' stub ('%s')\n", url );
      return 0;
   }
}

// transform a cached or downloaded block with the SG_get_block closure method
// return the number of bytes copied on success, filling in out_block->data with up to out_block->len bytes using the closure method
// return negative if this method fails
// NOTE: out_block must be allocated *before* calling this method!
ssize_t SG_gateway_closure_get_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_block, struct SG_chunk* out_block ) {
   
   ssize_t ret = 0;
   struct md_closure* closure = SG_gateway_closure( gateway );
   
   if( md_closure_find_callback( closure, "SG_get_block" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "SG_get_block", SG_closure_get_block_func, gateway, reqdat, in_block, out_block, closure->cls );
      
      if( ret < 0 ) {
         
         SG_error("SG_get_block( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %zd\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, ret );
      }
   }
   else {
      
      SG_warn("'SG_get_block' stub (in = %zu, out = %zu)\n", in_block->len, out_block->len);
      
      ret = MIN( in_block->len, out_block->len );
      
      memcpy( out_block->data, in_block->data, ret );
      out_block->len = ret;
   }
   
   return ret;
}


// transform a block into a block suitable for public access, using the SG_put_block method 
// return 0 on success, and set *out_block_data and *out_block_data_len to the allocated, transformed blocks.
// return -ENOMEM if OOM
// return non-zero on closure method failure
// NOTE: it is acceptable to set *out_block_data to in_block_data and *out_block_data_len to in_block_data_len to allow for zero-copy data transfers
int SG_gateway_closure_put_block( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_block, struct SG_chunk* out_block ) {
   
   int ret = 0;
   struct md_closure* closure = SG_gateway_closure( gateway );
   
   if( md_closure_find_callback( closure, "SG_put_block" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "SG_put_block", SG_closure_put_block_func, gateway, reqdat, in_block, out_block, closure->cls );
      
      if( ret < 0 ) {
         
         SG_error("SG_put_block( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, ret );
      }
   }
   else {
      
      SG_warn("'SG_put_block' stub (in = %zu)\n", in_block->len );
      
      // zero-copy
      SG_chunk_init( out_block, in_block->data, in_block->len );
   }
   
   return ret;
}


// transform a cached or downloaded serialized manifest with the SG_read_manifest closure method
// return 0 on success, setting *out_manifest_data and *out_manifest_data_len to a malloc'ed buffer containing the new manifest
// return -ENOMEM if OOM
// return negative if this method fails
// NOTE: it is acceptable to set *out_manifest's fields to *in_manifest's fields
int SG_gateway_closure_get_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_manifest, struct SG_chunk* out_manifest ) {
   
   int ret = 0;
   struct md_closure* closure = SG_gateway_closure( gateway );
   
   if( md_closure_find_callback( closure, "SG_get_manifest" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "SG_get_manifest", SG_closure_get_manifest_func, gateway, reqdat, in_manifest, out_manifest, closure->cls );
      
      if( ret != 0 ) {
         
         SG_error("SG_get_manifest( %" PRIX64 ".%" PRId64 "[manifest.%" PRId64 ".%ld] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, ret );
      }
   }
   else {
      
      SG_warn("'SG_get_manifest' stub (in = %zu)\n", in_manifest->len );
      
      // zero-copy 
      SG_chunk_init( out_manifest, in_manifest->data, in_manifest->len );
   }
   
   return ret;
}


// transform a serialized manifest into a manifest suitable for public access 
// return 0 on success 
// return -ENOMEM if OOM 
// return on-zero if the closure method fails 
// NOTE: it is acceptable to set *out_manifest_data to in_manifest_data and *out_manifest_data_len to in_manifest_data_len to allow for zero-copy data transfers
int SG_gateway_closure_put_manifest( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_manifest, struct SG_chunk* out_manifest ) {

   int ret = 0;
   struct md_closure* closure = SG_gateway_closure( gateway );
   
   if( md_closure_find_callback( closure, "SG_put_manifest" ) != NULL ) {
      
      MD_CLOSURE_CALL( ret, closure, "SG_put_manifest", SG_closure_put_manifest_func, gateway, reqdat, in_manifest, out_manifest, closure->cls );
      
      if( ret != 0 ) {
         
         SG_error("SG_put_manifest( %" PRIX64 ".%" PRId64 "[manifest.%" PRId64 ".%ld] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, ret );
      }
   }
   else {
      
      SG_warn("'SG_put_manifest' stub (in = %zu)\n", in_manifest->len );
      
      // zero-copy 
      SG_chunk_init( out_manifest, in_manifest->data, in_manifest->len );
   }
   
   return ret;
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
   
   // TODO: check RAM?
   
   // stored on disk?
   block_fd = md_cache_open_block( gateway->cache, reqdat->file_id, reqdat->file_version, block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, O_RDONLY );
   
   if( block_fd < 0 ) {
      
      if( block_fd != -ENOENT ) {
         
         SG_warn("md_cache_open_block( %" PRIX64 ".%" PRId64 "[%s %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
                 reqdat->file_id, reqdat->file_version, SG_request_is_block( reqdat ) ? "block" : "manifest", block_id_or_manifest_mtime_sec, block_version_or_manifest_mtime_nsec, reqdat->fs_path, block_fd );
      }
      
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


// asynchronously put a closure-transformed chunk of data directly into the cache.
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


// read the block from the on-disk block cache 
// if found, transform the block with the caller's SG_get_block driver method 
// return the number of bytes copied on success, and copy at most buf_len bytes into buf
// return -ENOENT if the block is not cached locally 
// return -EINVAL if hte request data structure isn't for a block 
// return -ENOMEM on OOM 
// return negative on other error
// NOTE: chunk must be allocated to at least the volume blocksize *before* calling this method
int SG_gateway_cached_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* chunk ) { 
   
   struct SG_chunk cached_chunk;
   
   int rc = 0;
   
   // get the block from the cache 
   rc = SG_gateway_cached_block_get_raw( gateway, reqdat, &cached_chunk );
   if( rc != 0 ) {
      
      if( rc != -ENOENT ) {
         
         SG_error("SG_gateway_cached_block_get_raw( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
                  reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   
   // transform the block 
   rc = SG_gateway_closure_get_block( gateway, reqdat, &cached_chunk, chunk );
   
   if( rc != 0 ) {
      
      SG_chunk_free( &cached_chunk );
      
      SG_error("SG_gateway_closure_get_block( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] (%s) ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->fs_path, rc );
      
      return rc;
      
   }
   
   // if not zero-copied, then clean up 
   if( cached_chunk.data != chunk->data ) {
      
      SG_chunk_free( &cached_chunk );
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
      return -EINVAL;
   }
   
   // lookaside: if this manifest is being written, then we can't read it 
   rc = md_cache_is_block_readable( gateway->cache, reqdat->file_id, reqdat->file_version, (uint64_t)reqdat->manifest_timestamp.tv_sec, (int64_t)reqdat->manifest_timestamp.tv_nsec );
   if( rc == -EAGAIN ) {
      
      // not available in the cache 
      return -ENOENT;
   }
   
   // check cache disk 
   rc = SG_gateway_cache_get_raw( gateway, reqdat, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, raw_serialized_manifest );
   if( rc != 0 ) {
      
      // not available in the cache 
      return rc;
   }
   
   return rc;
}


// get a manifest from the cache, transforming it with the gateway's closure
// return 0 on success, and set *manifest_buf and *manifest_buf_len to the allocated buffer and its length 
// return -ENOMEM if OOM
// return negative on I/O error
int SG_gateway_cached_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* serialized_manifest ) {
   
   struct SG_chunk raw_serialized_manifest;
   
   int rc = 0;
   
   // get the block from the cache 
   rc = SG_gateway_cached_manifest_get_raw( gateway, reqdat, &raw_serialized_manifest );
   if( rc != 0 ) {
      
      if( rc != -ENOENT ) {
         
         SG_error("SG_gateway_cached_manifest_get_raw( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
                   reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      }
      
      return rc;
   }
   
   // transform the manifest 
   rc = SG_gateway_closure_get_manifest( gateway, reqdat, &raw_serialized_manifest, serialized_manifest );
   
   if( rc != 0 ) {
      
      SG_error("SG_gateway_closure_get_manifest( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
                reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      
      SG_chunk_free( &raw_serialized_manifest );
      
      return rc;
   }
   
   // if not zero-copied, then clean up 
   if( raw_serialized_manifest.data != serialized_manifest->data ) {
      
      SG_chunk_free( &raw_serialized_manifest );
   }
   
   return rc;
}


// Put a block directly into the cache 
// return 0 on success, and set *cache_fut to the the future the caller can wait on 
// return -EINVAL if this isn't a block request 
// return negative on I/O error
int SG_gateway_cache_put_block_raw_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t cache_flags, struct md_cache_block_future** cache_fut ) {
   
   if( !SG_request_is_block( reqdat ) ) {
      return -EINVAL;
   }
   
   return SG_gateway_cache_put_raw_async( gateway, reqdat, reqdat->block_id, reqdat->block_version, block, cache_flags, cache_fut );
}
   

// asynchronously put a block into the cache.
// transform the block using the SG_put_block callback before writing it 
// return 0 on success, and set *block_fut to the newly-allocated cache future, which the caller can wait on to complete the write 
// return negative on failure to write the block, or failure to transform the data 
int SG_gateway_cached_block_put_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_block, uint64_t cache_flags, struct md_cache_block_future** block_fut ) {
   
   int rc = 0;
   struct SG_chunk cache_block;
   
   // transform the block 
   rc = SG_gateway_closure_put_block( gateway, reqdat, in_block, &cache_block );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_closure_put_block( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, rc );
      return rc;
   }
   
   // did we create our own copy of the block data?
   // if so, have the cache free it once it's done
   if( cache_block.data != in_block->data ) {
      
      cache_flags |= SG_CACHE_FLAG_UNSHARED;
   }
   
   // put the block 
   rc = SG_gateway_cache_put_block_raw_async( gateway, reqdat, &cache_block, cache_flags, block_fut );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_cached_block_put_raw_async( %" PRIX64 ".%" PRId64 "[block %" PRIu64 ".%" PRId64 "] ) rc = %d\n", reqdat->file_id, reqdat->file_version, reqdat->block_id, reqdat->block_version, rc );
      
      if( cache_block.data != in_block->data ) {
         SG_chunk_free( &cache_block );
      }
   }
   
   return rc;
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


// asynchronously put a serialized manifest into the cache 
// transform the manifest using the SG_put_manifest callback before writing it.
// return 0 on success, and set *manifest_fut to the newly-allocated cache future, which the caller can wait on to complete the write 
// return negative on failure to write the manifest, or failure to transform the data 
int SG_gateway_cached_manifest_put_async( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* serialized_manifest, uint64_t cache_flags, struct md_cache_block_future** manifest_fut ) {
   
   int rc = 0;
   
   struct SG_chunk raw_serialized_manifest;
   
   bool duplicated = false;
   
   // transform the block 
   rc = SG_gateway_closure_put_manifest( gateway, reqdat, serialized_manifest, &raw_serialized_manifest );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_closure_put_manifest( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      
      return rc;
   }
   
   // did we create our own copy of the manifest data?
   // if so, have the cache free it once it's done
   if( raw_serialized_manifest.data != serialized_manifest->data ) {
      
      cache_flags |= SG_CACHE_FLAG_UNSHARED;
      
      duplicated = true;
   }
   
   // put the manifest 
   rc = SG_gateway_cached_manifest_put_raw_async( gateway, reqdat, &raw_serialized_manifest, cache_flags, manifest_fut );
   if( rc != 0 ) {
      
      SG_error("SG_gateway_cached_manifest_put_raw_async( %" PRIX64 ".%" PRId64 "[manifest %" PRIu64 ".%ld] (%s) ) rc = %d\n",
               reqdat->file_id, reqdat->file_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->fs_path, rc );
      
      if( duplicated ) {
         
         // only free this if it was generated from the serialized manifest
         SG_chunk_free( &raw_serialized_manifest );
      }
   }
   
   return rc;
}


// start an I/O request on one of the gateway's I/O work queues
// return 0 on success 
// return negative on error
// NOTE: wreq must be heap-allocated.  The gateway will take ownership.
int SG_gateway_io_start( struct SG_gateway* gateway, struct md_wreq* wreq ) {
   
   int rc = 0;
   int wq_num = md_random64() % gateway->num_iowqs;     // NOTE: this is slightly biased
   
   rc = md_wq_add( &gateway->iowqs[wq_num], wreq );
   
   return rc;
}


// stat a file, filling in what we know into the out_reqdat structure 
// return 0 on success, populating *out_reqdat 
// return -ENOSYS if not defined
// return non-zero on implementation error 
int SG_gateway_impl_stat( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_request_data* out_reqdat, mode_t* out_mode ) {
   
   int rc = 0;
   
   if( gateway->impl_stat != NULL ) {
      
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

// truncate a file.
// the implementation MUST reversion the file to complete the operation 
// return 0 on success
// return -ENOSYS if not defined 
// return non-zero on implementation error 
int SG_gateway_impl_truncate( struct SG_gateway* gateway, struct SG_request_data* reqdat, uint64_t new_size ) {
   
   int rc = 0;
   
   if( gateway->impl_truncate != NULL ) {
      
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


// get a manifest from the implementation
// return 0 on success, and populate *manifest 
// return -ENOSYS if not defined
// return non-zero on implementation-specific failure 
int SG_gateway_impl_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest ) {
   
   int rc = 0;
   
   if( gateway->impl_get_manifest != NULL ) {
      
      rc = (*gateway->impl_get_manifest)( gateway, reqdat, manifest, gateway->cls );
      
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


// put a manifest into the implementation 
// return 0 on success
// return -ENOSYS if not implemented
// return non-zero on implementation-specific error 
int SG_gateway_impl_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest ) {
   
   int rc = 0;
   
   if( gateway->impl_put_manifest != NULL ) {
      
      rc = (*gateway->impl_put_manifest)( gateway, reqdat, manifest, gateway->cls );
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


// get a block from the implementation, directly.
// fill in the given block with data.
// return 0 on success, and populate *block with new data
// return -ENOSYS if not defined
// return non-zero on implementation-specific error 
int SG_gateway_impl_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block ) {
   
   int rc = 0;
   
   if( gateway->impl_get_block != NULL ) {
      
      rc = (*gateway->impl_get_block)( gateway, reqdat, block, gateway->cls );
      
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
int SG_gateway_impl_block_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block ) {
   
   int rc = 0;
   
   if( gateway->impl_put_block != NULL ) {
      
      rc = (*gateway->impl_put_block)( gateway, reqdat, block, gateway->cls );
      
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

// patch a manifest 
// the gateway MUST inform the MS of the new manifest information 
// return 0 on success
// return non-zero on implemetation error
int SG_gateway_impl_manifest_patch( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* write_delta ) {
   
   int rc = 0;
   
   if( gateway->impl_patch_manifest != NULL ) {
      
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
