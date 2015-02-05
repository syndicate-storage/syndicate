/*
   Copyright 2014 The Trustees of Princeton University

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

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/benchmark.h"
#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/openid.h"


MD_CLOSURE_PROTOTYPE_BEGIN( MS_CLIENT_CACHE_CLOSURE_PROTOTYPE )
   MD_CLOSURE_CALLBACK( "connect_cache" )
MD_CLOSURE_PROTOTYPE_END

// prototypes...
static void* ms_client_config_change_thread( void* arg );

// verify that a given key has our desired security parameters
int ms_client_verify_key( EVP_PKEY* key ) {
   
   RSA* ref_rsa = EVP_PKEY_get1_RSA( key );
   int size = 0;
   
   if( ref_rsa == NULL ) {
      // not an RSA key
      SG_error("Not an RSA key: %p\n", key);
      return -EINVAL;
   }

   size = RSA_size( ref_rsa );
   if( size * 8 != SG_RSA_KEY_SIZE ) {
      
      // not the right size
      SG_error("Invalid RSA size %d\n", size * 8 );
      return -EINVAL;
   }
   return 0;
}


// convert a gateway type into a human readable name for it.
int ms_client_gateway_type_str( int gateway_type, char* gateway_type_str ) {
   if( gateway_type == SYNDICATE_UG ) {
      sprintf( gateway_type_str, "UG" );
   }
   
   else if( gateway_type == SYNDICATE_RG ) {
      sprintf( gateway_type_str, "RG" );
   }
   
   else if( gateway_type == SYNDICATE_AG ) {
      sprintf( gateway_type_str, "AG" );
   }
   
   else {
      return -EINVAL;
   }
   return 0;
}


// set up secure CURL handle 
int ms_client_init_curl_handle( struct ms_client* client, CURL* curl, char const* url ) {
   
   md_init_curl_handle( client->conf, curl, url, client->conf->connect_timeout);
   curl_easy_setopt( curl, CURLOPT_USE_SSL, 1L );
   curl_easy_setopt( curl, CURLOPT_SSL_VERIFYPEER, (client->conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( curl, CURLOPT_SSL_VERIFYHOST, 2L );
   curl_easy_setopt( curl, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( curl, CURLOPT_SSL_CIPHER_LIST, MS_CIPHER_SUITES );
   
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl, CURLOPT_MAXREDIRS, 10L );
   curl_easy_setopt( curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
   curl_easy_setopt( curl, CURLOPT_USERPWD, client->userpass );
   
   curl_easy_setopt( curl, CURLOPT_TIMEOUT, client->ms_transfer_timeout );
   
   
   return 0;
}


// download from the caches, using the cache closure
// return 0 on success 
// return negative on error (from closure call)
int ms_client_connect_cache_impl( struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   
   int ret = 0;
   struct md_syndicate_conf* conf = (struct md_syndicate_conf*)cls;
   
   if( md_closure_find_callback( closure, "connect_cache" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "connect_cache", md_cache_connector_func, closure, curl, url, closure->cls );
   }
   else {
      
      // download manually...
      SG_warn("%s", "connect_cache stub\n" );
      md_init_curl_handle( conf, curl, url, conf->connect_timeout );
   }
   
   return ret;
}

// default connect cache (for external consumption)
int ms_client_volume_connect_cache( struct ms_client* client, CURL* curl, char const* url ) {
   
   ms_client_config_rlock( client );
   
   int rc = ms_client_connect_cache_impl( client->volume->cache_closure, curl, url, client->conf );
   
   ms_client_config_unlock( client );
   
   return rc;
}

// start internal threads (only safe to do so after we have a private key)
// client must be write-locked!
int ms_client_start_threads( struct ms_client* client ) {
   
   SG_info("%s\n", "Starting MS client threads" );
   
   if( client->running ) {
      return -EALREADY;
   }
   
   client->running = true;
   
   client->view_thread_running = true;

   client->view_thread = md_start_thread( ms_client_config_change_thread, client, false );
   if( client->view_thread < 0 ) {
      client->running = false;
      return -errno;  
   }
   
   return 0;
}

// stop internal threads
// NOTE: not thread-safe; client must be write-locked
int ms_client_stop_threads( struct ms_client* client ) {
   
   SG_info("%s\n", "Stopping MS client threads" );
   
   // shut down the uploader and view threads
   bool was_running = client->running;
   
   client->running = false;

   client->view_thread_running = false;
   
   if( was_running ) {
      
      pthread_cancel( client->view_thread );
      
      SG_info("%s", "wait for view change thread to finish...\n");
      
      pthread_join( client->view_thread, NULL );
   }
   
   return 0;
}

// load up a key, storing its OpenSSL EVP_PKEY form and optionally a duplicate of its PEM-encoded value (if key_pem_dup is not NULL)
// return 0 on success 
// return non-zero on error
int ms_client_try_load_key( struct md_syndicate_conf* conf, EVP_PKEY** key, char** key_pem_dup, char const* key_pem, bool is_public ) {
   
   int rc = 0;
   char const* method = NULL;
   
   if( key_pem != NULL ) {
      
      // we were given a key.  Load it
      if( is_public ) {
         rc = md_load_pubkey( key, key_pem );
         method = "md_load_pubkey";
      }
      else {
         rc = md_load_privkey( key, key_pem );
         method = "md_load_privkey";
      }
      
      if( rc != 0 ) {
         SG_error("%s rc = %d\n", method, rc );
         return rc;
      }
      
      rc = ms_client_verify_key( *key );
      if( rc != 0 ) {
         SG_error("ms_client_verify_key rc = %d\n", rc );
         return rc;
      }

      // hold onto the PEM form.
      // if private, mlock it
      if( key_pem_dup ) {
         
         struct mlock_buf pkey_buf;
         memset( &pkey_buf, 0, sizeof(pkey_buf) );
         
         rc = mlock_dup( &pkey_buf, key_pem, strlen(key_pem) + 1 );
         if( rc != 0 ) {
            
            SG_error("mlock_dup rc = %d\n", rc );
            return rc;
         }
         
         *key_pem_dup = (char*)pkey_buf.ptr;
      }
   }
   else {
      SG_warn("%s\n", "No key given" );
   }
   
   return 0;
}
   

// create an MS client context
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf ) {

   int rc = 0;
   
   if( conf == NULL ) {
      return -EINVAL;
   }
   
   memset( client, 0, sizeof(struct ms_client) );
   
   // get MS url 
   client->url = SG_strdup_or_null( conf->metadata_url );
   if( client->url == NULL ) {
      return -ENOMEM;
   }
   
   // set up downloader so we can register
   md_downloader_init( &client->dl, "ms-client" );
   
   rc = md_downloader_start( &client->dl );
   if( rc != 0 ) {
      
      SG_safe_free( client->url );
      SG_error("Failed to start downloader, rc = %d\n", rc );
      return rc;
   }

   client->gateway_type = gateway_type;
   
   // clear the / at the end...
   if( strrchr( client->url, '/' ) != NULL ) {
      
      md_strrstrip( client->url, "/" );
   }
   
   client->userpass = NULL;

   pthread_rwlock_init( &client->lock, NULL );
   pthread_rwlock_init( &client->config_lock, NULL );

   client->conf = conf;

   // uploader thread 
   sem_init( &client->uploader_sem, 0, 0 );
   
   rc = ms_client_try_load_key( conf, &client->my_key, &client->my_key_pem, conf->gateway_key, false );
   if( rc != 0 ) {
      SG_error("ms_client_try_load_key rc = %d\n", rc );
      
      SG_safe_free( client->url );
      
      return rc;
   }
   
   if( client->my_key != NULL ) {
      
      // if we loaded the private key, derive the public key from it
      rc = md_public_key_from_private_key( &client->my_pubkey, client->my_key );
      
      if( rc != 0 || client->my_pubkey == NULL ) {
         
         SG_error("md_public_key_from_private_key( %p ) rc = %d\n", client->my_key, rc );
         
         SG_safe_free( client->url );
         return rc;
      }
   }
   
   // NOTE: ms_client_try_load_key will mlock a private key 
   client->my_key_pem_mlocked = true;
   
   rc = ms_client_try_load_key( conf, &client->syndicate_public_key, &client->syndicate_public_key_pem, conf->syndicate_pubkey, true );
   if( rc != 0 ) {
      
      SG_error("ms_client_try_load_key rc = %d\n", rc );
      
      SG_safe_free( client->url );
      return rc;
   }
   
   client->config_change_callback = ms_client_config_change_callback_default;
   client->config_change_callback_cls = NULL;
   
   client->inited = true;               // safe to destroy later
   
   return rc;
}


// destroy an MS client context 
int ms_client_destroy( struct ms_client* client ) {
   if( client == NULL ) {
      SG_warn("client is %p\n", client);
      return 0;
   }
   
   if( !client->inited ) {
      SG_warn("client->inited = %d\n", client->inited );
      return 0;
   }
   
   ms_client_stop_threads( client );
   
   md_downloader_stop( &client->dl );
   
   ms_client_wlock( client );
   
   client->inited = false;

   // clean up view
   pthread_rwlock_wrlock( &client->config_lock );

   if( client->volume != NULL ) {
      ms_volume_free( client->volume );
      SG_safe_free( client->volume );
   }
   
   pthread_rwlock_unlock( &client->config_lock );
   pthread_rwlock_destroy( &client->config_lock );
   
   sem_destroy( &client->uploader_sem );
   
   // clean up our state
   SG_safe_free( client->userpass );
   SG_safe_free( client->url );
   SG_safe_free( client->session_password );
   
   if( client->my_key ) {
      EVP_PKEY_free( client->my_key );
      client->my_key = NULL;
   }
   if( client->my_pubkey ) {
      EVP_PKEY_free( client->my_pubkey );
      client->my_pubkey = NULL;
   }
   if( client->my_key_pem ) {
      // NOTE: was mlock'ed
      if( client->my_key_pem_mlocked ) {
         munlock( client->my_key_pem, client->my_key_pem_len );
      }
      SG_safe_free( client->my_key_pem );
   }
   
   SG_safe_free( client->syndicate_public_key_pem );
   
   if( client->syndicate_public_key ) {
      EVP_PKEY_free( client->syndicate_public_key );
      client->syndicate_public_key = NULL;
   }
   
   md_downloader_shutdown( &client->dl );
   
   ms_client_unlock( client );
   pthread_rwlock_destroy( &client->lock );
 
   SG_info("%s", "MS client shutdown\n");
   
   return 0;
}

// synchronously download metadata from the MS
// return 0 on success
// return negative on error
int ms_client_download( struct ms_client* client, char const* url, char** buf, off_t* buflen ) {
   
   int rc = 0;
   int curl_rc = 0;
   CURL* curl = NULL;
   struct ms_client_timing timing;
   long curl_errno = 0;
   long http_status = 0;
   
   memset( &timing, 0, sizeof(struct ms_client_timing) );
   
   // connect (TODO: connection pool)
   curl = curl_easy_init();  
   ms_client_init_curl_handle( client, curl, url );
   
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, &timing );
   
   // run 
   curl_rc = md_download_file( curl, buf, buflen );
   
   // check download status
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &curl_errno );
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_status );
   
   rc = ms_client_download_interpret_errors( url, http_status, curl_rc, curl_errno );
   
   curl_easy_cleanup( curl );
   
   if( rc != 0 ) {
      SG_error("download error: curl rc = %d, http status = %ld, errno = %ld, rc = %d\n", curl_rc, http_status, curl_errno, rc );
   }
   else {
      ms_client_timing_log( &timing );
   }
   
   ms_client_timing_free( &timing );
   return rc;
}

// read-lock a client context 
int ms_client_rlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //SG_debug("ms_client_rlock(%p) from %s:%d\n", client, from_str, lineno);
   return pthread_rwlock_rdlock( &client->lock );
}

// write-lock a client context 
int ms_client_wlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //SG_debug("ms_client_wlock(%p) from %s:%d\n", client, from_str, lineno);
   return pthread_rwlock_wrlock( &client->lock );
}

// unlock a client context 
int ms_client_unlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //SG_debug("ms_client_unlock(%p) from %s:%d\n", client, from_str, lineno);
   return pthread_rwlock_unlock( &client->lock );
}

// read-lock a client context's view
int ms_client_config_rlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //SG_debug("ms_client_config_rlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_rdlock( &client->config_lock );
}

// write-lock a client context's view
int ms_client_config_wlock2( struct ms_client* client, char const* from_str, int lineno  ) {
   //SG_debug("ms_client_config_wlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_wrlock( &client->config_lock );
}

// unlock a client context's view
int ms_client_config_unlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //SG_debug("ms_client_config_unlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_unlock( &client->config_lock );
}

// get the current volume version
uint64_t ms_client_volume_version( struct ms_client* client ) {
   ms_client_config_rlock( client );

   uint64_t ret = client->volume->volume_version;
   ms_client_config_unlock( client );
   return ret;
}


// get the current cert version
uint64_t ms_client_cert_version( struct ms_client* client ) {
   ms_client_config_rlock( client );

   uint64_t ret = client->volume->volume_cert_version;
   ms_client_config_unlock( client );
   return ret;
}


// get the Volume ID
uint64_t ms_client_get_volume_id( struct ms_client* client ) {
   ms_client_config_rlock( client );

   uint64_t ret = client->volume->volume_id;

   ms_client_config_unlock( client );
   return ret;
}

// get the Volume name
char* ms_client_get_volume_name( struct ms_client* client ) {
   ms_client_config_rlock( client );

   char* ret = SG_strdup_or_null( client->volume->name );
   
   ms_client_config_unlock( client );
   return ret;
}

// get the hostname in the cert
char* ms_client_get_hostname( struct ms_client* client ) {
   ms_client_config_rlock( client );
   
   char* hostname = NULL;
   
   // get our cert...
   if( client->volume != NULL ) {
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ client->conf->gateway_type ]->find( client->gateway_id );
      if( itr != cert_bundles[ client->conf->gateway_type ]->end() ) {
         hostname = SG_strdup_or_null( itr->second->hostname );
      }
   }
   
   ms_client_config_unlock( client );
   
   return hostname;
}

// get the port num
int ms_client_get_portnum( struct ms_client* client ) {
   ms_client_config_rlock( client );
   
   int ret = client->portnum;
   
   ms_client_config_unlock( client );
   return ret;
}


// get the blocking factor
uint64_t ms_client_get_volume_blocksize( struct ms_client* client ) {
   ms_client_config_rlock( client );

   uint64_t ret = client->volume->blocksize;

   ms_client_config_unlock( client );
   return ret;
}


// get a root structure 
int ms_client_get_volume_root( struct ms_client* client, struct md_entry* root ) {
   int rc = 0;

   ms_client_config_rlock( client );

   if( client->volume->root == NULL ) {
      ms_client_config_unlock( client );
      return -ENODATA;
   }
   
   memset( root, 0, sizeof(struct md_entry) );
   
   rc = md_entry_dup2( client->volume->root, root );

   ms_client_config_unlock( client );

   return rc;
}


// is an MS operation an async operation?
int ms_client_is_async_operation( int oper ) {
   
   return (oper == ms::ms_update::UPDATE_ASYNC || oper == ms::ms_update::CREATE_ASYNC || oper == ms::ms_update::DELETE_ASYNC );
}

// process a gateway message's header, in order to detect when we have stale metadata.
// if we have stale metadata, then wake up the reloader thread and synchronize our volume metadata
int ms_client_process_header( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version ) {
   int rc = 0;
   
   ms_client_config_rlock( client );
   
   if( client->volume->volume_id != volume_id ) {
      return -EINVAL;
   }
   
   // wake up volume reload thread, if there is new configuration information for us
   if( client->volume->volume_version < volume_version ) {
      sem_post( &client->uploader_sem );
   }
   
   if( client->volume->volume_cert_version < cert_version ) {
      sem_post( &client->uploader_sem );
   }
   
   ms_client_config_unlock( client );
   return rc;
}

// synchronous method to GET data
int ms_client_read( struct ms_client* client, char const* url, ms::ms_reply* reply ) {
   
   char* buf = NULL;
   off_t buflen = 0;
   int rc = 0;
   
   rc = ms_client_download( client, url, &buf, &buflen );
   
   if( rc != 0 ) {
      SG_error("ms_client_download('%s') rc = %d\n", url, rc );
      
      return rc;
   }
   
   // parse and verify
   rc = ms_client_parse_reply( client, reply, buf, buflen, true );
   
   free( buf );
   buf = NULL;
   
   if( rc != 0 ) {
      SG_error("ms_client_parse_reply rc = %d\n", rc );
   }
   
   return rc;
}

// set the configuration change callback
int ms_client_set_config_change_callback( struct ms_client* client, ms_client_config_change_callback clb, void* cls ) {
   ms_client_config_wlock( client );
   
   client->config_change_callback = clb;
   client->config_change_callback_cls = cls;
   
   ms_client_config_unlock( client );
   
   return 0;
}

// set the user cls
void* ms_client_set_config_change_callback_cls( struct ms_client* client, void* cls ) {
   ms_client_config_wlock( client );
   
   void* ret = client->config_change_callback_cls;
   client->config_change_callback_cls = cls;
   
   ms_client_config_unlock( client );
   
   return ret;
}
   

// wake up the reload thread
int ms_client_start_config_reload( struct ms_client* client ) {
   int rc = 0;
   
   ms_client_config_wlock( client );

   sem_post( &client->uploader_sem );
   
   ms_client_config_unlock( client );
   return rc;
}


// defaut view change callback
int ms_client_config_change_callback_default( struct ms_client* client, void* cls ) {
   SG_warn("%s", "WARN: stub Volume view change callback\n");
   return 0;
}

// volume config change thread body, for synchronizing gateway configuration
static void* ms_client_config_change_thread( void* arg ) {
   
   struct ms_client* client = (struct ms_client*)arg;
   
   int rc = 0;

   // since we don't hold any resources between downloads, simply cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   SG_info("%s", "View thread starting up\n");

   while( client->running ) {

      struct timespec now;
      clock_gettime( CLOCK_REALTIME, &now );
      
      struct timespec reload_deadline;
      reload_deadline.tv_sec = now.tv_sec + (client->conf->view_reload_freq);
      reload_deadline.tv_nsec = 0;
      
      // if somehow we wait 0 seconds, then set to 1 second 
      if( reload_deadline.tv_sec == now.tv_sec ) {
         SG_warn("%s", "Waiting for manditory 1 second between volume reload checks\n");
         reload_deadline.tv_sec ++;
      }
      
      SG_info("Reload Volume in at most %ld seconds (at %ld)\n", reload_deadline.tv_sec - now.tv_sec, reload_deadline.tv_sec );
      
      // wait to be signaled to reload 
      while( reload_deadline.tv_sec > now.tv_sec ) {
         
         clock_gettime( CLOCK_REALTIME, &now );
         
         rc = sem_timedwait( &client->uploader_sem, &reload_deadline );
         
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
               return NULL;
            }
         }
         else {
            // got woken up by client
            break;
         }
      }
      
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      // reload Volume metadata
      SG_debug("%s", "Begin reload Volume metadata\n" );

      rc = ms_client_reload_volume( client );

      SG_debug("End reload Volume metadata, rc = %d\n", rc);
      
      if( rc == 0 ) {
         ms_client_rlock( client );
         
         if( client->config_change_callback != NULL ) {
            rc = (*client->config_change_callback)( client, client->config_change_callback_cls );
            if( rc != 0 ) {
               SG_error("WARN: view change callback rc = %d\n", rc );
            }
         }
         
         ms_client_unlock( client );
      }
      
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
   }

   SG_debug("%s", "View thread shutting down\n");
   
   return NULL;
}
