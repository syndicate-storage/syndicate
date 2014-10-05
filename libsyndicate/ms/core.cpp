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
static void* ms_client_view_thread( void* arg );

// verify that a given key has our desired security parameters
int ms_client_verify_key( EVP_PKEY* key ) {
   RSA* ref_rsa = EVP_PKEY_get1_RSA( key );
   if( ref_rsa == NULL ) {
      // not an RSA key
      errorf("%s", "Not an RSA key\n");
      return -EINVAL;
   }

   int size = RSA_size( ref_rsa );
   if( size * 8 != RSA_KEY_SIZE ) {
      // not the right size
      errorf("Invalid RSA size %d\n", size * 8 );
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
int ms_client_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl, char const* url ) {
   md_init_curl_handle( conf, curl, url, conf->connect_timeout);
   curl_easy_setopt( curl, CURLOPT_USE_SSL, 1L );
   curl_easy_setopt( curl, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( curl, CURLOPT_SSL_VERIFYHOST, 2L );
   curl_easy_setopt( curl, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( curl, CURLOPT_SSL_CIPHER_LIST, MS_CIPHER_SUITES );
   return 0;
}


// download from the caches...
int ms_client_connect_cache_impl( struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   
   int ret = -1;
   struct md_syndicate_conf* conf = (struct md_syndicate_conf*)cls;
   
   if( md_closure_find_callback( closure, "connect_cache" ) != NULL ) {
      MD_CLOSURE_CALL( ret, closure, "connect_cache", md_cache_connector_func, closure, curl, url, closure->cls );
   }
   else {
      
      // download manually...
      errorf("%s", "WARN: connect_cache stub\n" );
      md_init_curl_handle( conf, curl, url, conf->connect_timeout );
      ret = 0;
   }
   
   return ret;
}

// default connect cache (for external consumption)
int ms_client_volume_connect_cache( struct ms_client* client, CURL* curl, char const* url ) {
   
   ms_client_view_rlock( client );
   
   int rc = ms_client_connect_cache_impl( client->volume->cache_closure, curl, url, client->conf );
   
   ms_client_view_unlock( client );
   
   return rc;
}

// start internal threads (only safe to do so after we have a private key)
// client must be write-locked!
int ms_client_start_threads( struct ms_client* client ) {
   
   dbprintf("%s\n", "Starting MS client threads" );
   
   if( client->running ) {
      return -EALREADY;
   }
   
   client->running = true;
   
   client->view_thread_running = true;

   client->view_thread = md_start_thread( ms_client_view_thread, client, false );
   if( client->view_thread < 0 ) {
      client->running = false;
      return -errno;  
   }
   
   return 0;
}

// stop internal threads
int ms_client_stop_threads( struct ms_client* client ) {
   
   dbprintf("%s\n", "Stopping MS client threads" );
   
   // shut down the uploader and view threads
   bool was_running = client->running;
   
   client->running = false;

   client->view_thread_running = false;
   
   if( was_running ) {
      
      pthread_cancel( client->view_thread );
      
      dbprintf("%s", "wait for view change thread to finish...\n");
      
      if( client->view_thread != 0 ) {
         pthread_join( client->view_thread, NULL );
      }
   }
   
   return 0;
}

// load up a key, storing its OpenSSL form and optionally a duplicate of its PEM-encoded value
int ms_client_try_load_key( struct md_syndicate_conf* conf, EVP_PKEY** key, char** key_pem_dup, char const* key_pem, bool is_public ) {
   int rc = 0;
   
   if( key_pem != NULL ) {
      // we were given a key.  Load it
      char const* method = NULL;
      if( is_public ) {
         rc = md_load_pubkey( key, key_pem );
         method = "md_load_pubkey";
      }
      else {
         rc = md_load_privkey( key, key_pem );
         method = "md_load_privkey";
      }
      
      if( rc != 0 ) {
         errorf("%s rc = %d\n", method, rc );
         return rc;
      }
      
      rc = ms_client_verify_key( *key );
      if( rc != 0 ) {
         errorf("ms_client_verify_key rc = %d\n", rc );
         return rc;
      }

      // hold onto the PEM form.
      // if private, mlock it
      if( key_pem_dup ) {
         struct mlock_buf pkey_buf;
         memset( &pkey_buf, 0, sizeof(pkey_buf) );
         
         rc = mlock_dup( &pkey_buf, key_pem, strlen(key_pem) + 1 );
         if( rc != 0 ) {
            errorf("mlock_dup rc = %d\n", rc );
            return rc;
         }
         
         *key_pem_dup = (char*)pkey_buf.ptr;
      }
   }
   else {
      dbprintf("%s\n", "WARN: No key given" );
   }
   
   return 0;
}
   

// create an MS client context
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf ) {

   if( conf == NULL ) {
      return -EINVAL;
   }
   
   memset( client, 0, sizeof(struct ms_client) );
   
   // set up downloader so we can register
   md_downloader_init( &client->dl, "ms-client" );
   
   int rc = md_downloader_start( &client->dl );
   if( rc != 0 ) {
      client->running = false;
      errorf("Failed to start downloader, rc = %d\n", rc );
      return rc;
   }

   client->gateway_type = gateway_type;
   
   client->url = strdup( conf->metadata_url );
   
   // clear the / at the end...
   if( client->url[ strlen(client->url)-1 ] == '/' ) {
      client->url[ strlen(client->url)-1 ] = 0;
   }
   
   client->userpass = NULL;

   pthread_rwlock_init( &client->lock, NULL );
   pthread_rwlock_init( &client->view_lock, NULL );

   client->conf = conf;

   // uploader thread 
   sem_init( &client->uploader_sem, 0, 0 );
   
   rc = ms_client_try_load_key( conf, &client->my_key, &client->my_key_pem, conf->gateway_key, false );
   if( rc != 0 ) {
      errorf("ms_client_try_load_key rc = %d\n", rc );
      return rc;
   }
   if( client->my_key != NULL ) {
      // if we loaded the private key, derive the public key from it
      rc = md_public_key_from_private_key( &client->my_pubkey, client->my_key );
      if( rc != 0 || client->my_pubkey == NULL ) {
         errorf("md_public_key_from_private_key( %p ) rc = %d\n", client->my_key, rc );
         return rc;
      }
   }
   
   // NOTE: ms_client_try_load_key will mlock a private key 
   client->my_key_pem_mlocked = true;
   
   rc = ms_client_try_load_key( conf, &client->syndicate_public_key, &client->syndicate_public_key_pem, conf->syndicate_pubkey, true );
   if( rc != 0 ) {
      errorf("ms_client_try_load_key rc = %d\n", rc );
      return rc;
   }
   
   client->view_change_callback = ms_client_view_change_callback_default;
   client->view_change_callback_cls = NULL;
   
   client->inited = true;               // safe to destroy later
   
   return rc;
}


// destroy an MS client context 
int ms_client_destroy( struct ms_client* client ) {
   if( client == NULL ) {
      errorf("WARN: client is %p\n", client);
      return 0;
   }
   
   if( !client->inited ) {
      errorf("WARN: client->inited = %d\n", client->inited );
      return 0;
   }
   
   ms_client_stop_threads( client );
   
   md_downloader_stop( &client->dl );
   
   ms_client_wlock( client );
   
   client->inited = false;

   // clean up view
   pthread_rwlock_wrlock( &client->view_lock );

   ms_volume_free( client->volume );
   free( client->volume );
   
   pthread_rwlock_unlock( &client->view_lock );
   pthread_rwlock_destroy( &client->view_lock );
   
   sem_destroy( &client->uploader_sem );
   
   // clean up our state
   if( client->userpass ) {
      free( client->userpass );
   }
   
   if( client->url ) {
      free( client->url );
   }
   if( client->session_password ) {
      free( client->session_password );
   }
   if( client->my_key ) {
      EVP_PKEY_free( client->my_key );
   }
   if( client->my_pubkey ) {
      EVP_PKEY_free( client->my_pubkey );
   }
   if( client->my_key_pem ) {
      // NOTE: was mlock'ed
      if( client->my_key_pem_mlocked ) {
         munlock( client->my_key_pem, client->my_key_pem_len );
      }
      free( client->my_key_pem );
   }
   if( client->syndicate_public_key_pem ) {
      free( client->syndicate_public_key_pem );
   }
   if( client->syndicate_public_key ) {
      EVP_PKEY_free( client->syndicate_public_key );
   }
   
   md_downloader_shutdown( &client->dl );
   
   ms_client_unlock( client );
   pthread_rwlock_destroy( &client->lock );
 
   dbprintf("%s", "MS client shutdown\n");
   
   return 0;
}


// open a metadata connection to the MS.  Preserve all connection state in dlctx.  Optionally have opt_dlset track dlctx (if it's not NULL), so the caller can wait on batches of dlctx instances
// return 0 on success, negative on error
// if opt_dlset is not NULL, then add the resulting download context to it.  NOTE: opt_dlset must NOT be freed until ms_client_download_end() is called for this download context!
int ms_client_download_begin( struct ms_client* client, char const* url, struct curl_slist* headers, struct md_download_context* dlctx, struct md_download_set* opt_dlset, struct ms_client_timing* times ) {
   
   // set up a cURL handle to the MS 
   // TODO: conection pool
   CURL* curl = curl_easy_init();
   ms_client_init_curl_handle( client->conf, curl, url );
   
   int rc = md_download_context_init( dlctx, curl, NULL, NULL, -1 );
   if( rc != 0 ) {
      errorf("md_download_context_init(%s) rc = %d\n", url, rc );
      
      // TODO: connection pool 
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   curl_easy_setopt( curl, CURLOPT_URL, url );
   curl_easy_setopt( curl, CURLOPT_HTTPHEADER, headers );
   
   if( times != NULL ) {
      // benchmark
      curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
      curl_easy_setopt( curl, CURLOPT_WRITEHEADER, times );   
   }
   
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
   curl_easy_setopt( curl, CURLOPT_USERPWD, client->userpass );
   
   if( opt_dlset != NULL ) {
      md_download_set_add( opt_dlset, dlctx );
   }
   
   ms_client_rlock( client );
   
   // start to download
   rc = md_download_context_start( &client->dl, dlctx, NULL, url );
   
   ms_client_unlock( client );
   
   if( rc != 0 ) {
      errorf("md_download_context_start(%s) rc = %d\n", url, rc );
      
      // TODO: connection pool 
      if( opt_dlset != NULL ) {
         md_download_set_clear( opt_dlset, dlctx );
      }
      
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      return rc;
      
   }
      
   return 0;
}

// shut down a metadata connection to the MS.  Free up the state in dlctx.
// this method returns:
// * -EAGAIN if we got no data
// * the negative errno if errno was set by a connection disruption
// * the positive HTTP status if it's an error status (i.e. >= 400)
// * the positive error code returned by libcurl (i.e. < 100)
// * the positive HTTP status (between 200 and 399) if we succeeded
int ms_client_download_end( struct ms_client* client, struct md_download_context* dlctx, char** response_buf, size_t* response_buf_len ) {
   
   // wait for the download to finish 
   int rc = md_download_context_wait( dlctx, client->conf->transfer_timeout * 1000 );
   if( rc != 0 ) {
      
      dbprintf("md_download_context_wait(%p) rc = %d\n", dlctx, rc );
      
      // timed out.  cancel 
      md_download_context_cancel( &client->dl, dlctx );
      
      // if we were given a download set, then clear it 
      md_download_context_clear_set( dlctx );
      
      // TODO: connection pool 
      CURL* curl = NULL;
      md_download_context_free( dlctx, &curl );
      
      if( curl != NULL ) {
         curl_easy_cleanup( curl );
      }
      
      return rc;
   }
   
   // get data 
   char* url = NULL;
   int os_errno = md_download_context_get_errno( dlctx );
   int http_status = md_download_context_get_http_status( dlctx );
   int curl_rc = md_download_context_get_curl_rc( dlctx );
   md_download_context_get_effective_url( dlctx, &url );
   
   // curl error?
   if( curl_rc != 0 || http_status != 200 ) {
      
      errorf("Download of %p %s errno = %d, CURL rc = %d, HTTP status = %d\n", dlctx, url, os_errno, curl_rc, http_status );
      
      if( os_errno != 0 ) {
         rc = -abs(os_errno);
      }
      else if( http_status >= 400 ) {
         rc = http_status;
      }
      else if( curl_rc == CURLE_GOT_NOTHING ) {
         // got disconnected; try again
         rc = -EAGAIN;
      }
      else {
         rc = curl_rc;
      }
   }
   else {
      // get the response 
      off_t len = 0;
      rc = md_download_context_get_buffer( dlctx, response_buf, &len );
      
      if( rc != 0 ) {
         errorf("md_download_context_get_buffer(%p url=%s) rc = %d\n", dlctx, url, rc );
      }
      else {
         *response_buf_len = len;
      }
   }
   
   // TODO: connection pool 
   CURL* curl = NULL;
   
   // if we were given a download set, then clear it 
   md_download_context_clear_set( dlctx );
   
   // clean up 
   md_download_context_free( dlctx, &curl );
   
   if( curl != NULL ) {
      curl_easy_cleanup( curl );
   }
   
   if( url != NULL ) {
      free( url );
   }
   
   if( rc == 0 ) {
      rc = http_status;
   }
   
   return rc;
}

// begin uploading to the MS.
// dlctx serves as the state to be preserved between ms_client_upload_begin and ms_client_upload_end.  Do not free it!
// if opt_dlset is given, add dlctx to it (in which case, opt_dlset cannot be freed until calling ms_client_upload_end() with this dlctx)
// return 0 on success; negative on failure
int ms_client_upload_begin( struct ms_client* client, char const* url, struct curl_httppost* forms, struct md_download_context* dlctx, struct md_download_set* opt_dlset, struct ms_client_timing* timing ) {
   
   int rc = 0;
   
   // set up a cURL handle to the MS 
   // TODO: conection pool
   CURL* curl = curl_easy_init();
   ms_client_init_curl_handle( client->conf, curl, url );
   
   rc = md_download_context_init( dlctx, curl, NULL, NULL, -1 );
   if( rc != 0 ) {
      errorf("md_download_context_init(%s) rc = %d\n", url, rc );
      
      // TODO: connection pool 
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L);
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, forms );
   
   curl_easy_setopt( curl, CURLOPT_URL, url );
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
   curl_easy_setopt( curl, CURLOPT_USERPWD, client->userpass );
   
   if( timing != NULL ) {
      curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
      curl_easy_setopt( curl, CURLOPT_WRITEHEADER, timing );      
   }
   
   // if there's a download set, add the download context to it
   if( opt_dlset != NULL ) {
      md_download_set_add( opt_dlset, dlctx );
   }
   
   ms_client_rlock( client );
   
   // start to download
   rc = md_download_context_start( &client->dl, dlctx, NULL, url );
   
   ms_client_unlock( client );
   
   if( rc != 0 ) {
      errorf("md_download_context_start(%s) rc = %d\n", url, rc );
      
      // TODO: connection pool 
      if( opt_dlset != NULL ) {
         md_download_set_clear( opt_dlset, dlctx );
      }
      
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      return rc;
      
   }
   
   return rc;
}


// finish uploading from the MS.  Get back the HTTP status and response buffer.  Free up the state in dlctx.
// return the HTTP response on success; negative on error
int ms_client_upload_end( struct ms_client* client, struct md_download_context* dlctx, char** buf, size_t* buflen ) {
   
   // logically, this is the same as ending a download 
   int rc = ms_client_download_end( client, dlctx, buf, buflen );
   if( rc != 200 ) {
      errorf("ms_client_download_end(%p) rc = %d\n", dlctx, rc );
   }
   return rc;
}


// synchronously download metadata from the MS
// return the HTTP response on success; negative on error
int ms_client_download( struct ms_client* client, char const* url, char** buf, size_t* buflen ) {
   
   struct md_download_context dlctx;
   memset( &dlctx, 0, sizeof(struct md_download_context) );
   
   struct ms_client_timing times;
   memset( &times, 0, sizeof(struct ms_client_timing) );
   
   // start downloading
   int rc = ms_client_download_begin( client, url, NULL, &dlctx, NULL, &times );
   if( rc != 0 ) {
      errorf("ms_client_download_begin(%s) rc = %d\n", url, rc );
      return rc;
   }
   
   // finish downloading 
   int http_response = ms_client_download_end( client, &dlctx, buf, buflen );

   if( http_response < 0 ) {
      errorf("ms_client_download_end rc = %d\n", http_response );
      
      ms_client_timing_free( &times );
      return http_response;
   }
   
   if( http_response != 200 ) {
      errorf("ms_client_download_end HTTP response = %d\n", http_response );

      if( http_response == 0 ) {
         // really bad--MS bug
         errorf("%s", "!!! likely an MS bug !!!\n");
         http_response = 500;
      }

      ms_client_timing_free( &times );
      return -http_response;
   }
   
   ms_client_timing_log( &times );
   ms_client_timing_free( &times );
   
   return http_response;
}


// set up a download
int ms_client_network_context_download_init( struct ms_client_network_context* nctx, char const* url, struct curl_slist* headers, struct md_download_set* dlset ) {
   
   memset( nctx, 0, sizeof(struct ms_client_network_context) );
   
   nctx->headers = headers;
   
   nctx->dlctx = CALLOC_LIST( struct md_download_context, 1 );
   nctx->timing = CALLOC_LIST( struct ms_client_timing, 1 );
   
   nctx->upload = false;
   nctx->url = strdup( url );
   
   nctx->ended = false;
   
   nctx->dlset = dlset;
   
   return 0;
}

// set up an upload 
int ms_client_network_context_upload_init( struct ms_client_network_context* nctx, char const* url, struct curl_httppost* forms, struct md_download_set* dlset ) {
   
   memset( nctx, 0, sizeof(struct ms_client_network_context) );
   
   nctx->forms = forms;
   
   nctx->dlctx = CALLOC_LIST( struct md_download_context, 1 );
   nctx->timing = CALLOC_LIST( struct ms_client_timing, 1 );
   
   nctx->upload = true;
   nctx->url = strdup( url );
   
   nctx->ended = false;
   
   nctx->dlset = dlset;
   
   return 0;
}

// cancel a running download
int ms_client_network_context_cancel( struct ms_client* client, struct ms_client_network_context* nctx ) {
   if( nctx->dlctx != NULL ) {
      int rc = md_download_context_cancel( &client->dl, nctx->dlctx );
      
      if( rc == 0 ) {
         
         // safe to free
         CURL* curl = NULL;
         
         if( nctx->dlset != NULL ) {
            // remove ourselves from the download set
            md_download_set_clear( nctx->dlset, nctx->dlctx );
         }
         
         md_download_context_free( nctx->dlctx, &curl );
         
         // TODO: connection pool 
         if( curl != NULL ) {
            curl_easy_cleanup( curl );
         }
         
         free( nctx->dlctx );
         
         nctx->ended = true;
      }
      return rc;
   }
   else {
      return 0;
   }
}

// free a network context 
int ms_client_network_context_free( struct ms_client_network_context* nctx ) {
   
   
   if( nctx->dlctx != NULL ) {
      
      if( !nctx->ended ) {
         
         if( !md_download_context_finalized( nctx->dlctx ) ) {
            // not finalized.  cancel or wait for it to complete first.
            return -EINVAL;
         }
         
         CURL* curl = NULL;
         
         if( nctx->dlset != NULL ) {
            // remove ourselves from the download set
            md_download_set_clear( nctx->dlset, nctx->dlctx );
         }
         
         md_download_context_free( nctx->dlctx, &curl );
         
         // TODO: connection pool 
         if( curl != NULL ) {
            curl_easy_cleanup( curl );
         }
         
         free( nctx->dlctx );
      }
      
      nctx->dlctx = NULL;
   }
   
   if( nctx->headers != NULL ) {
      curl_slist_free_all( nctx->headers );
      nctx->headers = NULL;
   }
   
   if( nctx->forms != NULL ) {
      curl_formfree( nctx->forms );
      nctx->forms = NULL;
   }
   
   if( nctx->timing != NULL ) {
      ms_client_timing_free( nctx->timing );
      free( nctx->timing );
      nctx->timing = NULL;
   }
   
   if( nctx->url ) {
      free( nctx->url );
      nctx->url = NULL;
   }
   
   nctx->dlset = NULL;
   
   return 0;
}

// start a context running 
int ms_client_network_context_begin( struct ms_client* client, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   char const* method = NULL;
   
   if( nctx->upload ) {
      
      method = "ms_client_upload_begin";
      rc = ms_client_upload_begin( client, nctx->url, nctx->forms, nctx->dlctx, nctx->dlset, nctx->timing );
   }
   else {
      
      method = "ms_client_download_begin";
      rc = ms_client_download_begin( client, nctx->url, nctx->headers, nctx->dlctx, nctx->dlset, nctx->timing );
   }
   
   if( rc != 0 ) {
      errorf("%s(%s) rc = %d\n", method, nctx->url, rc );
   }
   else {
      nctx->started = true;
      nctx->ended = false;
   }
   
   return rc;
}


// wait for and finish a network context
// return the HTTP status on successful HTTP-level communication (could be an error, though)
// return -errno on OS-level error (or if the connection timed out)
// return positive error code (< 200) on CURL error
int ms_client_network_context_end( struct ms_client* client, struct ms_client_network_context* nctx, char** result_buf, size_t* result_len ) {
   
   int http_status_or_error_code = 0;
   char const* method = NULL;
   
   if( nctx->upload ) {
      
      method = "ms_client_upload_end";
      http_status_or_error_code = ms_client_upload_end( client, nctx->dlctx, result_buf, result_len );
   }
   else {
      
      method = "ms_client_download_end";
      http_status_or_error_code = ms_client_download_end( client, nctx->dlctx, result_buf, result_len );
   }
   
   if( http_status_or_error_code != 200 ) {
      errorf("%s(%s) rc = %d\n", method, nctx->url, http_status_or_error_code );
   }
   
   if( nctx->dlctx != NULL ) {
      // done with this 
      free( nctx->dlctx );
      nctx->dlctx = NULL;
   }
   
   nctx->started = false;
   nctx->ended = true;
   
   return http_status_or_error_code;
}


// store app-specific context data to a network context 
void ms_client_network_context_set_cls( struct ms_client_network_context* nctx, void* cls ) {
   nctx->cls = cls;
}

// get app-specific context data out of a network context 
void* ms_client_network_context_get_cls( struct ms_client_network_context* nctx ) {
   return nctx->cls;
}

// do a one-off RPC call via OpenID
// rpc_type can be "json" or "xml"
int ms_client_openid_auth_rpc( char const* ms_openid_url, char const* username, char const* password,
                               char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len,
                               char* syndicate_public_key_pem ) {
   
   CURL* curl = curl_easy_init();
   
   EVP_PKEY* pubkey = NULL;
   int rc = 0;
   
   if( syndicate_public_key_pem != NULL ) {
      rc = md_load_pubkey( &pubkey, syndicate_public_key_pem );
   
      if( rc != 0 ) {
         errorf("Failed to load Syndicate public key, md_load_pubkey rc = %d\n", rc );
         return -EINVAL;
      }
   }
   
   // TODO: elegant way to avoid hard constants?
   md_init_curl_handle2( curl, NULL, 30, true );
   
   char* ms_openid_url_begin = CALLOC_LIST( char, strlen(ms_openid_url) + strlen("/begin") + 1 );
   sprintf(ms_openid_url_begin, "%s/begin", ms_openid_url );
   
   rc = ms_client_openid_session( curl, ms_openid_url_begin, username, password, response_buf, response_len, pubkey );
   
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   free( ms_openid_url_begin );
   
   if( pubkey )
      EVP_PKEY_free( pubkey );
   
   if( rc != 0 ) {
      errorf("ms_client_openid_session(%s) rc = %d\n", ms_openid_url, rc );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // set the body contents
   struct md_upload_buf upload;
   memset( &upload, 0, sizeof(upload) );
      
   upload.text = request_buf;
   upload.len = request_len;
   upload.offset = 0;
   
   curl_easy_setopt(curl, CURLOPT_POST, 1L );
   curl_easy_setopt(curl, CURLOPT_URL, ms_openid_url);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_buf);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, request_len);
   
   struct curl_slist* headers = NULL;
   
   // what kind of RPC?
   if( strcasecmp( rpc_type, "json" ) == 0 ) {
      // json rpc 
      headers = curl_slist_append( headers, "content-type: application/json" );
   }
   else if( strcasecmp( rpc_type, "xml" ) == 0 ) {
      // xml rpc
      headers = curl_slist_append( headers, "content-type: application/xml" );
   }
   
   if( headers != NULL ) {
      curl_easy_setopt( curl, CURLOPT_HTTPHEADER, headers );
   }
   
   char* tmp_response = NULL;
   ssize_t tmp_response_len = md_download_file( curl, &tmp_response );
   if( tmp_response_len < 0 ) {
      errorf("md_download_file(%s) rc = %zd\n", ms_openid_url, tmp_response_len );
      rc = -ENODATA;
   }
   
   // clear headers
   curl_easy_setopt( curl, CURLOPT_HTTPHEADER, NULL );
   
   if( headers ) {
      curl_slist_free_all( headers );
   }
   
   curl_easy_cleanup( curl );
   
   if( rc == 0 ) {
      *response_buf = tmp_response;
      *response_len = tmp_response_len;
   }
   
   return rc;
}

// OpenID RPC, but don't verify 
int ms_client_openid_rpc( char const* ms_openid_url, char const* username, char const* password,
                          char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len ) {
   
   errorf("%s", "WARN: will not verify RPC result from Syndicate MS\n");
   return ms_client_openid_auth_rpc( ms_openid_url, username, password, rpc_type, request_buf, request_len, response_buf, response_len, NULL );
}

// read-lock a client context 
int ms_client_rlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //dbprintf("ms_client_rlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_rdlock( &client->lock );
}

// write-lock a client context 
int ms_client_wlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //dbprintf("ms_client_wlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_wrlock( &client->lock );
}

// unlock a client context 
int ms_client_unlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //dbprintf("ms_client_unlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_unlock( &client->lock );
}

// read-lock a client context's view
int ms_client_view_rlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //dbprintf("ms_client_view_rlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_rdlock( &client->view_lock );
}

// write-lock a client context's view
int ms_client_view_wlock2( struct ms_client* client, char const* from_str, int lineno  ) {
   //dbprintf("ms_client_view_wlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_wrlock( &client->view_lock );
}

// unlock a client context's view
int ms_client_view_unlock2( struct ms_client* client, char const* from_str, int lineno ) {
   //dbprintf("ms_client_view_unlock %p (from %s:%d)\n", client, from_str, lineno);
   return pthread_rwlock_unlock( &client->view_lock );
}

// get the current volume version
uint64_t ms_client_volume_version( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t ret = client->volume->volume_version;
   ms_client_view_unlock( client );
   return ret;
}


// get the current cert version
uint64_t ms_client_cert_version( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t ret = client->volume->volume_cert_version;
   ms_client_view_unlock( client );
   return ret;
}


// get the Volume ID
uint64_t ms_client_get_volume_id( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t ret = client->volume->volume_id;

   ms_client_view_unlock( client );
   return ret;
}

// get the Volume name
char* ms_client_get_volume_name( struct ms_client* client ) {
   ms_client_view_rlock( client );

   char* ret = strdup( client->volume->name );
   
   ms_client_view_unlock( client );
   return ret;
}

// get the hostname in the cert
char* ms_client_get_hostname( struct ms_client* client ) {
   ms_client_view_rlock( client );
   
   char* hostname = NULL;
   
   // get our cert...
   if( client->volume != NULL ) {
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ client->conf->gateway_type ]->find( client->gateway_id );
      if( itr != cert_bundles[ client->conf->gateway_type ]->end() ) {
         hostname = strdup( itr->second->hostname );
      }
   }
   
   ms_client_view_unlock( client );
   
   return hostname;
}

// get the port num
int ms_client_get_portnum( struct ms_client* client ) {
   ms_client_view_rlock( client );
   
   int ret = client->portnum;
   
   ms_client_view_unlock( client );
   return ret;
}


// get the blocking factor
uint64_t ms_client_get_volume_blocksize( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t ret = client->volume->blocksize;

   ms_client_view_unlock( client );
   return ret;
}


// get a root structure 
int ms_client_get_volume_root( struct ms_client* client, struct md_entry* root ) {
   int rc = 0;

   ms_client_view_rlock( client );

   if( client->volume->root == NULL ) {
      ms_client_view_unlock( client );
      return -ENODATA;
   }
   
   memset( root, 0, sizeof(struct md_entry) );
   md_entry_dup2( client->volume->root, root );

   ms_client_view_unlock( client );

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
   
   ms_client_view_rlock( client );
   
   if( client->volume->volume_id != volume_id ) {
      return -EINVAL;
   }
   
   if( client->volume->volume_version < volume_version ) {
      sem_post( &client->uploader_sem );
   }
   
   if( client->volume->volume_cert_version < cert_version ) {
      sem_post( &client->uploader_sem );
   }
   
   ms_client_view_unlock( client );
   return rc;
}


// asynchronously start fetching data from the MS 
// client cannot be locked
int ms_client_read_begin( struct ms_client* client, char const* url, struct ms_client_network_context* nctx, struct md_download_set* dlset ) {
   
   // set up the download 
   ms_client_network_context_download_init( nctx, url, NULL, dlset );
   
   // start downloading 
   int rc = ms_client_network_context_begin( client, nctx );
   if( rc != 0 ) {
      errorf("ms_client_network_context_begin(%s) rc = %d\n", url, rc );
      
      ms_client_network_context_free( nctx );
      return rc;
   }
   
   return rc;
}
 
 
// wait for an asynchronously-started MS read to finish.  Parse and verify the MS reply
// client cannot be locked 
int ms_client_read_end( struct ms_client* client, uint64_t volume_id, ms::ms_reply* reply, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   size_t len = 0;
   char* buf = NULL;
   
   int http_response = ms_client_network_context_end( client, nctx, &buf, &len );
   
   if( http_response <= 0 ) {
      errorf("ms_client_network_context_end rc = %d\n", http_response );
      
      if( buf != NULL ) {
         free( buf );
      }
      
      ms_client_network_context_free( nctx );
      return http_response;
   }
   
   if( http_response == 200 ) {
      // success!
      // parse and verify
      rc = ms_client_parse_reply( client, reply, buf, len, true );
      if( rc != 0 ) {
         errorf("ms_client_read rc = %d\n", rc );
         free( buf );
         
         ms_client_network_context_free( nctx );
         return -ENODATA;
      }
      
      free( buf );
      
      // check errors
      int err = reply->error();
      if( err != 0 ) {
         errorf("MS reply error %d\n", err );
         
         
         ms_client_network_context_free( nctx );
         return err;
      }
      
      else {
         // extract versioning information from the reply
         ms_client_process_header( client, volume_id, reply->volume_version(), reply->cert_version() );
      
         ms_client_network_context_free( nctx );
         return 0;
      }
   }
   else {
      // error 
      errorf("ms_client_download_end rc = %d\n", http_response );
      
      if( http_response == 0 ) {
         errorf("%s", "MS bug: HTTP response is zero!\n");
         
         http_response = -EIO;
      }
      
      if( buf != NULL ) {
         free( buf );
      }
      
      ms_client_network_context_free( nctx );
      return -http_response;
   }
}

// synchronous wrapper around read_begin and read_end 
int ms_client_read( struct ms_client* client, uint64_t volume_id, char const* url, ms::ms_reply* reply ) {
   
   int rc = 0;
   struct ms_client_network_context nctx;
   memset( &nctx, 0, sizeof(struct ms_client_network_context) );
   
   // start reading
   rc = ms_client_read_begin( client, url, &nctx, NULL );
   if( rc != 0 ) {
      errorf("ms_client_read_begin(%s) rc = %d\n", url, rc );
      return rc;
   }
   
   // finish reading
   rc = ms_client_read_end( client, volume_id, reply, &nctx );
   if( rc != 0) {
      errorf("ms_client_read_end(%s) rc = %d\n", url, rc );
   }
   
   return rc;
}

// set the volume view change callback
int ms_client_set_view_change_callback( struct ms_client* client, ms_client_view_change_callback clb, void* cls ) {
   ms_client_view_wlock( client );
   
   client->view_change_callback = clb;
   client->view_change_callback_cls = cls;
   
   ms_client_view_unlock( client );
   
   return 0;
}

// set the user cls
void* ms_client_set_view_change_callback_cls( struct ms_client* client, void* cls ) {
   ms_client_view_wlock( client );
   
   void* ret = client->view_change_callback_cls;
   client->view_change_callback_cls = cls;
   
   ms_client_view_unlock( client );
   
   return ret;
}
   

// wake up the reload thread
int ms_client_sched_volume_reload( struct ms_client* client ) {
   int rc = 0;
   
   ms_client_view_wlock( client );

   sem_post( &client->uploader_sem );
   
   ms_client_view_unlock( client );
   return rc;
}


// defaut view change callback
int ms_client_view_change_callback_default( struct ms_client* client, void* cls ) {
   dbprintf("%s", "WARN: stub Volume view change callback\n");
   return 0;
}

// volume view thread body, for synchronizing Volume metadata
static void* ms_client_view_thread( void* arg ) {
   
   struct ms_client* client = (struct ms_client*)arg;
   
   int rc = 0;

   // since we don't hold any resources between downloads, simply cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   dbprintf("%s", "View thread starting up\n");

   while( client->running ) {

      struct timespec now;
      clock_gettime( CLOCK_REALTIME, &now );
      
      struct timespec reload_deadline;
      reload_deadline.tv_sec = now.tv_sec + (client->conf->view_reload_freq);
      reload_deadline.tv_nsec = 0;
      
      // if somehow we wait 0 seconds, then set to 1 second 
      if( reload_deadline.tv_sec == now.tv_sec ) {
         errorf("%s", "WARN: waiting for manditory 1 second between volume reload checks\n");
         reload_deadline.tv_sec ++;
      }
      
      dbprintf("Reload Volume in at most %ld seconds (at %ld)\n", reload_deadline.tv_sec - now.tv_sec, reload_deadline.tv_sec );
      
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
               errorf("sem_timedwait errno = %d\n", rc);
               return NULL;
            }
         }
      }
      
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      // reload Volume metadata
      dbprintf("%s", "Begin reload Volume metadata\n" );

      rc = ms_client_reload_volume( client );

      dbprintf("End reload Volume metadata, rc = %d\n", rc);
      
      if( rc == 0 ) {
         ms_client_rlock( client );
         
         if( client->view_change_callback != NULL ) {
            rc = (*client->view_change_callback)( client, client->view_change_callback_cls );
            if( rc != 0 ) {
               errorf("WARN: view change callback rc = %d\n", rc );
            }
         }
         
         ms_client_unlock( client );
      }
      
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
   }

   dbprintf("%s", "View thread shutting down\n");
   
   return NULL;
}