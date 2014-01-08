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

#include "libsyndicate/ms-client.h"

static int ms_client_view_change_callback_default( struct ms_client* client, void* cls );

static void* ms_client_uploader_thread( void* arg );
static void* ms_client_view_thread( void* arg );
static void ms_client_uploader_signal( struct ms_client* client );
int ms_client_load_volume_metadata( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem );
static size_t ms_client_header_func( void *ptr, size_t size, size_t nmemb, void *userdata);
char* ms_client_cert_url( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, int gateway_type, uint64_t gateway_id, uint64_t gateway_cert_version );

int ms_client_parse_reply( struct ms_client* client, ms::ms_reply* src, char const* buf, size_t buf_len, bool verify );

static void ms_client_cert_bundles( struct ms_volume* volume, ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1] ) {
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   memset( cert_bundles, 0, sizeof(cert_bundles[0]) * (MS_NUM_CERT_BUNDLES + 1) );
   cert_bundles[SYNDICATE_UG] = volume->UG_certs;
   cert_bundles[SYNDICATE_AG] = volume->AG_certs;
   cert_bundles[SYNDICATE_RG] = volume->RG_certs;
   return;
}  

static void print_timings( uint64_t* timings, size_t num_timings, char const* hdr ) {
   if( num_timings > 0 ) {
      for( size_t i = 0; i < num_timings; i++ ) {
         DATA( hdr, (double)(timings[i]) / 1e9 );
      }
   }
}


static void ms_client_gateway_cert_free( struct ms_gateway_cert* cert ) {
   if( cert->hostname ) {
      free( cert->hostname );
      cert->hostname = NULL;
   }

   if( cert->name ) {
      free( cert->name );
      cert->name = NULL;
   }

   if( cert->pubkey ) {
      EVP_PKEY_free( cert->pubkey );
      cert->pubkey = NULL;
   }
   
   if( cert->closure_text ) {
      free( cert->closure_text );
      cert->closure_text = NULL;
   }
}

static void ms_volume_free( struct ms_volume* vol ) {
   if( vol == NULL )
      return;
   
   dbprintf("Destroy Volume '%s'\n", vol->name );
   
   if( vol->volume_public_key ) {
      EVP_PKEY_free( vol->volume_public_key );
      vol->volume_public_key = NULL;
   }
   
   ms_cert_bundle* all_certs[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, all_certs );

   for( int i = 1; all_certs[i] != NULL; i++ ) {
      ms_cert_bundle* certs = all_certs[i];
      
      for( ms_cert_bundle::iterator itr = certs->begin(); itr != certs->end(); itr++ ) {
         ms_client_gateway_cert_free( itr->second );
         free( itr->second );
      }
   }
   
   delete vol->UG_certs;
   delete vol->RG_certs;
   delete vol->AG_certs;
   
   vol->UG_certs = NULL;
   vol->RG_certs = NULL;
   vol->AG_certs = NULL;
   
   if( vol->name ) {
      free( vol->name );
      vol->name = NULL;
   }

   if( vol->root ) {
      md_entry_free( vol->root );
      free( vol->root );
   }

   memset( vol, 0, sizeof(struct ms_volume) );
}

// verify that a given key has our desired security parameters
static int ms_client_verify_key( EVP_PKEY* key ) {
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

static long ms_client_hash( uint64_t volume_id, uint64_t file_id ) {
   locale loc;
   const collate<char>& coll = use_facet<collate<char> >(loc);

   char hashable[100];
   sprintf(hashable, "%" PRIu64 "%" PRIu64, volume_id, file_id );
   long ret = coll.hash( hashable, hashable + strlen(hashable) );

   return ret;
}


static int ms_client_gateway_type_str( int gateway_type, char* gateway_type_str ) {
   if( gateway_type == SYNDICATE_UG )
      sprintf( gateway_type_str, "UG" );

   else if( gateway_type == SYNDICATE_RG )
      sprintf( gateway_type_str, "RG" );

   else if( gateway_type == SYNDICATE_AG )
      sprintf( gateway_type_str, "AG" );

   else
      return -EINVAL;

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
      

// create an MS client context
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf ) {

   memset( client, 0, sizeof(struct ms_client) );

   client->gateway_type = gateway_type;
   
   // configure the HTTPS streams to the MS
   client->ms_read = curl_easy_init();
   client->ms_write = curl_easy_init();
   client->ms_view = curl_easy_init();
   
   client->url = strdup( conf->metadata_url );
   
   // clear the / at the end...
   if( client->url[ strlen(client->url)-1 ] == '/' )
      client->url[ strlen(client->url)-1 ] = 0;

   // will change URL once we know the Volume ID
   ms_client_init_curl_handle( conf, client->ms_read, "https://localhost" );
   ms_client_init_curl_handle( conf, client->ms_write, "https://localhost" );
   ms_client_init_curl_handle( conf, client->ms_view, "https://localhost" );
   
   curl_easy_setopt( client->ms_write, CURLOPT_POST, 1L);
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );

   curl_easy_setopt( client->ms_read, CURLOPT_HEADERFUNCTION, ms_client_header_func );
   curl_easy_setopt( client->ms_write, CURLOPT_HEADERFUNCTION, ms_client_header_func );

   curl_easy_setopt( client->ms_read, CURLOPT_WRITEHEADER, &client->read_times );
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEHEADER, &client->write_times );
   
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
   curl_easy_setopt( client->ms_write, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
   curl_easy_setopt( client->ms_view, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );

   client->userpass = NULL;

   pthread_rwlock_init( &client->lock, NULL );
   pthread_rwlock_init( &client->view_lock, NULL );

   client->updates = new update_set();
   client->conf = conf;
   client->deadlines = new deadline_queue();

   // uploader thread
   pthread_mutex_init( &client->uploader_lock, NULL );
   pthread_cond_init( &client->uploader_cv, NULL );

   int rc = 0;

   if( conf->gateway_key != NULL ) {
      // we were given Gateway keys.  Load them
      rc = md_load_privkey( &client->my_key, conf->gateway_key );
      if( rc != 0 ) {
         errorf("md_load_privkey rc = %d\n", rc );
         return rc;
      }
   }
   else {
      errorf("%s: Missing public key\n", conf->gateway_name );
      return -EINVAL;
   }
   
   rc = ms_client_verify_key( client->my_key );
   if( rc != 0 ) {
      errorf("ms_client_verify_key rc = %d\n", rc );
      return rc;
   }

   client->view_change_callback = ms_client_view_change_callback_default;
   client->view_change_callback_cls = NULL;

   client->running = true;
   
   client->uploader_thread = md_start_thread( ms_client_uploader_thread, client, false );
   if( client->uploader_thread < 0 ) {
      return -errno;
   }

   client->view_thread = md_start_thread( ms_client_view_thread, client, false );
   if( client->view_thread < 0 ) {
      return -errno;  
   }

   return rc;
}


// destroy an MS client context 
int ms_client_destroy( struct ms_client* client ) {
   if( client == NULL )
      return 0;
   
   // shut down the uploader thread
   client->running = false;

   ms_client_uploader_signal( client );
   pthread_cancel( client->view_thread );

   dbprintf("%s", "wait for write uploads to finish...\n");

   pthread_join( client->uploader_thread, NULL );

   dbprintf("%s", "wait for view change thread to finish...\n");
   
   pthread_join( client->view_thread, NULL );

   ms_client_wlock( client );

   pthread_mutex_destroy( &client->uploader_lock );
   pthread_cond_destroy( &client->uploader_cv );


   // clean up CURL
   curl_easy_cleanup( client->ms_read );
   curl_easy_cleanup( client->ms_write );
   curl_easy_cleanup( client->ms_view );

   // clean up view
   pthread_rwlock_wrlock( &client->view_lock );

   ms_volume_free( client->volume );
   free( client->volume );
   
   pthread_rwlock_unlock( &client->view_lock );
   pthread_rwlock_destroy( &client->view_lock );
   
   // clean up our state
   if( client->userpass )
      free( client->userpass );

   if( client->updates ) {
      for( update_set::iterator itr = client->updates->begin(); itr != client->updates->end(); itr++ ) {
         md_update_free( &itr->second );
         memset( &itr->second, 0, sizeof(struct md_update) );
      }
      client->updates->clear();
      delete client->updates;
   }

   if( client->deadlines ) {
      client->deadlines->clear();
      delete client->deadlines;
   }

   if( client->url )
      free( client->url );

   if( client->session_password )
      free( client->session_password );

   if( client->my_key )
      EVP_PKEY_free( client->my_key );
   
   ms_client_unlock( client );
   pthread_rwlock_destroy( &client->lock );

   // free OpenSSL memory
   ERR_free_strings();
   
   dbprintf("%s", "MS client shutdown\n");
   
   return 0;
}


char* ms_client_url( struct ms_client* client, uint64_t volume_id, char const* metadata_path ) {
   char volume_id_str[50];
   sprintf(volume_id_str, "%" PRIu64, volume_id);

   char* volume_md_path = md_fullpath( metadata_path, volume_id_str, NULL );

   ms_client_rlock( client );
   char* url = md_fullpath( client->url, volume_md_path, NULL );
   ms_client_unlock( client );

   free( volume_md_path );

   return url;
}

// POST for a file
char* ms_client_file_url( struct ms_client* client, uint64_t volume_id ) {
   
   char volume_id_str[50];
   sprintf( volume_id_str, "%" PRIu64, volume_id );

   char* volume_file_path = CALLOC_LIST( char, strlen(client->url) + 1 + strlen("/FILE/") + 1 + strlen(volume_id_str) + 1 );

   ms_client_rlock( client );
   sprintf( volume_file_path, "%s/FILE/%s", client->url, volume_id_str);
   ms_client_unlock( client );
   
   return volume_file_path;
}

// GET for a file
char* ms_client_file_url( struct ms_client* client, uint64_t volume_id, uint64_t file_id, int64_t version, int64_t write_nonce ) {

   char volume_id_str[50];
   sprintf( volume_id_str, "%" PRIu64, volume_id );

   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char version_str[50];
   sprintf( version_str, "%" PRId64, version );

   char write_nonce_str[60];
   sprintf( write_nonce_str, "%" PRId64, write_nonce );

   char* volume_file_path = CALLOC_LIST( char, strlen(client->url) + 1 + strlen("/FILE/") + 1 + strlen(volume_id_str) + 1 + strlen(file_id_str) + 1 + strlen(version_str) + 1 + strlen(write_nonce_str) + 1 );

   ms_client_rlock( client );
   sprintf( volume_file_path, "%s/FILE/%s/%s/%s/%s", client->url, volume_id_str, file_id_str, version_str, write_nonce_str );
   ms_client_unlock( client );

   return volume_file_path;
}

char* ms_client_volume_url( struct ms_client* client, uint64_t volume_id ) {
   char buf[50];
   sprintf(buf, "%" PRIu64, volume_id );

   char* volume_md_path = md_fullpath( "/VOLUME/", buf, NULL );

   ms_client_rlock( client );
   char* url = md_fullpath( client->url, volume_md_path, NULL );
   ms_client_unlock( client );

   free( volume_md_path );

   return url;
}


char* ms_client_volume_url_by_name( struct ms_client* client, char const* name ) {
   char* volume_md_path = md_fullpath( "/VOLUME/", name, NULL );

   ms_client_rlock( client );
   char* url = md_fullpath( client->url, volume_md_path, NULL );
   ms_client_unlock( client );

   free( volume_md_path );

   return url;
}

char* ms_client_openid_register_url( struct ms_client* client ) {
   // build the /REGISTER/ url

   char gateway_type_str[10];
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   ms_client_rlock( client );

   char* url = CALLOC_LIST( char, strlen(client->url) + 1 +
                                  strlen("/OPENID/") + 1 +
                                  strlen(client->conf->gateway_name) + 1 +
                                  strlen(client->conf->ms_username) + 1 +
                                  strlen(gateway_type_str) + 1 +
                                  strlen("/begin") + 1);

   sprintf(url, "%s/OPENID/%s/%s/%s/begin", client->url, gateway_type_str, client->conf->gateway_name, client->conf->ms_username );

   ms_client_unlock( client );

   return url;
}

static int ms_client_view_change_callback_default( struct ms_client* client, void* cls ) {
   dbprintf("%s", "Volume has changed!\n");
   return 0;
}

// view thread body, for synchronizing Volume metadata
static void* ms_client_view_thread( void* arg ) {
   struct ms_client* client = (struct ms_client*)arg;
   
   // how often do we reload?
   uint64_t view_reload_freq = client->conf->view_reload_freq;
   struct timespec sleep_time;

   struct timespec remaining;

   client->view_thread_running = true;

   // since we don't hold any resources between downloads, simply cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   dbprintf("%s", "View thread starting up\n");

   while( client->running ) {

      uint64_t now_ms = currentTimeMillis();
      uint64_t wakeup_ms = now_ms + view_reload_freq * 1000;
      
      bool do_early_reload = false;
      
      // wait for next reload
      while( now_ms < wakeup_ms ) {
         
         sleep_time.tv_sec = 1;
         sleep_time.tv_nsec = 0;

         int rc = nanosleep( &sleep_time, &remaining );
         int errsv = errno;
         
         now_ms = currentTimeMillis();
         
         if( rc < 0 ) {
            if( errsv == EINTR ) {
               errorf("errsv = %d\n", errsv);
            }
            else {
               errorf("nanosleep errno = %d\n", errsv );
               client->view_thread_running = false;
               return NULL;
            }
         }

         // hint to reload now?
         ms_client_view_wlock( client );
         
         do_early_reload = client->early_reload;
         
         ms_client_view_unlock( client );

         if( do_early_reload ) {
            break;
         }
         
         if( !client->running ) {
            break;
         }
      }

      if( !client->running ) {
         break;
      }
      
      if( do_early_reload ) {

         // reload Volume metadata
         pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
         
         dbprintf("%s", "Begin reload Volume metadata\n" );

         int rc = ms_client_reload_volume( client );

         dbprintf("End reload Volume metadata, rc = %d\n", rc);
         
         if( rc == 0 ) {
            ms_client_rlock( client );
            
            if( client->view_change_callback != NULL ) {
               rc = (*client->view_change_callback)( client, client->view_change_callback_cls );
            }
            
            ms_client_unlock( client );
         }

         pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
         
         ms_client_view_wlock( client );
         
         client->early_reload = false;
         
         ms_client_view_unlock( client );
      }
   }

   dbprintf("%s", "View thread shutting down\n");
   
   client->view_thread_running = false;
   return NULL;
}



// get the offset at which the value starts
static off_t ms_client_find_header_value( char* header_buf, size_t header_len, char const* header_name ) {

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

// parse one value
static uint64_t ms_client_read_one_value( char* hdr, off_t offset, size_t size ) {
   char* value = hdr + offset;
   size_t value_len = size - offset;

   char* value_str = CALLOC_LIST( char, value_len + 1 );
   
   strncpy( value_str, value, value_len );

   uint64_t data = (uint64_t)strtoll( value_str, NULL, 10 );

   free( value_str );
   
   return data;
}

// read a csv of values
static uint64_t* ms_client_read_multi_values( char* hdr, off_t offset, size_t size, size_t* ret_len ) {
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

// header parser
static size_t ms_client_header_func( void *ptr, size_t size, size_t nmemb, void *userdata) {
   struct ms_client_timing* times = (struct ms_client_timing*)userdata;

   size_t len = size * nmemb;
   char* data = (char*)ptr;

   char* data_str = CALLOC_LIST( char, len + 1 );
   strncpy( data_str, data, len );

   //dbprintf("header: %s\n", data_str );

   // is this one of our headers?  Find each of them
   
   off_t off = ms_client_find_header_value( data_str, len, HTTP_VOLUME_TIME );
   if( off > 0 ) {
      times->volume_time = ms_client_read_one_value( data_str, off, len );
      free( data_str );
      return len;
   }
   
   off = ms_client_find_header_value( data_str, len, HTTP_GATEWAY_TIME );
   if( off > 0 ) {
      times->ug_time = ms_client_read_one_value( data_str, off, len );
      free( data_str );
      return len;
   }

   off = ms_client_find_header_value( data_str, len, HTTP_TOTAL_TIME );
   if( off > 0 ) {
      times->total_time = ms_client_read_one_value( data_str, off, len );
      free( data_str );
      return len;
   }

   off = ms_client_find_header_value( data_str, len, HTTP_RESOLVE_TIME );
   if( off > 0 ) {
      times->resolve_time = ms_client_read_one_value( data_str, off, len );
      free( data_str );
      return len;
   }

   off = ms_client_find_header_value( data_str, len, HTTP_CREATE_TIMES );
   if( off > 0 ) {
      times->create_times = ms_client_read_multi_values( data_str, off, len, &times->num_create_times );
      free( data_str );
      return len;
   }

   off = ms_client_find_header_value( data_str, len, HTTP_UPDATE_TIMES );
   if( off > 0 ) {
      times->update_times = ms_client_read_multi_values( data_str, off, len, &times->num_update_times );
      free( data_str );
      return len;
   }

   off = ms_client_find_header_value( data_str, len, HTTP_DELETE_TIMES );
   if( off > 0 ) {
      times->delete_times = ms_client_read_multi_values( data_str, off, len, &times->num_delete_times );
      free( data_str );
      return len;
   }

   free( data_str );
   return len;
}


// redirect parser
static size_t ms_client_redirect_header_func( void *ptr, size_t size, size_t nmemb, void *userdata) {
   response_buffer_t* rb = (response_buffer_t*)userdata;

   size_t len = size * nmemb;

   char* data = (char*)ptr;

   // only get one Location header
   if( rb->size() > 0 )
      return len;

   char* data_str = CALLOC_LIST( char, len + 1 );
   strncpy( data_str, data, len );

   off_t off = ms_client_find_header_value( data_str, len, "Location" );
   if( off > 0 ) {

      char* value = data_str + off;
      size_t value_len = len - off;
      
      char* value_str = CALLOC_LIST(char, value_len );
      strncpy( value_str, value, value_len - 2 );     // strip off '\n\r'

      rb->push_back( buffer_segment_t( value_str, value_len ) );
   }

   free( data_str );
   
   return len;
}


// put the uplodaer to sleep
static void ms_client_uploader_wait( struct ms_client* client ) {

   // don't wait if we're not running
   if( !client->running )
      return;
   
   pthread_mutex_lock( &client->uploader_lock );

   if( !client->more_work ) {
      pthread_cond_wait( &client->uploader_cv, &client->uploader_lock );
   }

   client->more_work = false;
   
   pthread_mutex_unlock( &client->uploader_lock );
}

// wake up the uploader
static void ms_client_uploader_signal( struct ms_client* client ) {
   pthread_mutex_lock( &client->uploader_lock );

   if( !client->more_work ) 
      pthread_cond_signal( &client->uploader_cv );
   else
      client->more_work = true;
   
   pthread_mutex_unlock( &client->uploader_lock );
}


// uploader thread body
static void* ms_client_uploader_thread( void* arg ) {
   block_all_signals();
   struct ms_client* client = (struct ms_client*)arg;

   client->uploader_running = true;
   while( client->running ) {

      ms_client_rlock( client );
      bool has_volume = (client->volume != NULL && client->volume->volume_id != 0);
      ms_client_unlock( client );

      if( !has_volume ) {
         // nothing to do
         struct timespec ts;
         ts.tv_sec = 1;
         ts.tv_nsec = 0;

         struct timespec rem;
         nanosleep( &ts, &rem );
         continue;
      }
      
      ms_client_rlock( client );

      int64_t until_next_deadline = 0;
      bool process = false;      // set to true if we will upload 
      
      if( client->deadlines->size() > 0 ) {
         uint64_t now_ms = currentTimeMillis();
         until_next_deadline = (int64_t)(client->deadlines->begin()->first - now_ms);
         process = true;

         dbprintf("%zu pending deadlines\n", client->deadlines->size() );
      }

      ms_client_unlock( client );
      
      if( process ) {
         // wait until the deadline expires
         if( until_next_deadline > 0 ) {
            dbprintf("%p sleep %" PRIu64" \n", client, until_next_deadline * 1000L );

            // interruptable sleep, by client->running
            for( int64_t i = 0; i < until_next_deadline && client->running; i += 1000 ) {
               sleep( 1 );
            }
            usleep( (until_next_deadline % 1000) * 1000 );
         }

         if( client->running ) {
            int rc = ms_client_sync_updates( client, 0 );
            dbprintf("%p sync'ed updates, rc = %d\n", client, rc );

            if( rc < 0 ) {
               errorf("WARN: ms_entry_sync_updates rc = %d\n", rc );
            }
         }
      }
      else {
         // nothing to do; wake me up when there is something
         ms_client_uploader_wait( client );
      }
   }

   client->uploader_running = false;
   return NULL;
}


// exponential back-off
static void ms_client_wlock_backoff( struct ms_client* client, bool* downloading ) {
   // if another download is pending, wait, using random exponential back-off
   ms_client_wlock( client );
   uint64_t delay = random() % 1000;
   while( *downloading ) {
      ms_client_unlock( client );

      dbprintf("sleep for %" PRIu64 "\n", delay);
      
      usleep( delay );
      delay += random() % 1000;
      delay <<= 1;

      ms_client_wlock( client );
   }
}


int ms_client_begin_downloading( struct ms_client* client, char const* url, struct curl_slist* headers ) {
   ms_client_wlock_backoff( client, &client->downloading );

   client->downloading = true;

   curl_easy_setopt( client->ms_read, CURLOPT_URL, url );
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, headers );

   ms_client_unlock( client );

   return 0;
}


int ms_client_end_downloading( struct ms_client* client ) {
   long http_response = 0;

   ms_client_wlock( client );
   
   DATA( HTTP_VOLUME_TIME, (double)client->read_times.volume_time / 1e9 );
   DATA( HTTP_GATEWAY_TIME, (double)client->read_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->read_times.total_time / 1e9 );

   // not downloading anymore
   client->downloading = false;

   curl_easy_setopt( client->ms_read, CURLOPT_URL, NULL );
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, NULL );
   curl_easy_getinfo( client->ms_read, CURLINFO_RESPONSE_CODE, &http_response );

   ms_client_unlock( client );

   return (int)(http_response);
}


int ms_client_begin_downloading_view( struct ms_client* client, char const* url, struct curl_slist* headers ) {
   ms_client_wlock_backoff( client, &client->downloading_view );

   client->downloading_view = true;

   curl_easy_setopt( client->ms_view, CURLOPT_FOLLOWLOCATION, 1L );
   curl_easy_setopt( client->ms_view, CURLOPT_URL, url );
   curl_easy_setopt( client->ms_view, CURLOPT_HTTPHEADER, headers );

   ms_client_unlock( client );

   return 0;
}


int ms_client_end_downloading_view( struct ms_client* client ) {
   long http_response = 0;

   ms_client_wlock( client );

   DATA( HTTP_VOLUME_TIME, (double)client->read_times.volume_time / 1e9 );
   DATA( HTTP_GATEWAY_TIME, (double)client->read_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->read_times.total_time / 1e9 );

   // not downloading anymore
   client->downloading_view = false;

   curl_easy_setopt( client->ms_view, CURLOPT_URL, NULL );
   curl_easy_setopt( client->ms_view, CURLOPT_HTTPHEADER, NULL );
   curl_easy_getinfo( client->ms_view, CURLINFO_RESPONSE_CODE, &http_response );

   ms_client_unlock( client );

   return (int)(http_response);
}

int ms_client_begin_uploading( struct ms_client* client, char const* url, response_buffer_t* rb, struct curl_httppost* forms ) {
   // lock, but back off if someone else is uploading
   ms_client_wlock_backoff( client, &client->uploading );

   client->uploading = true;

   curl_easy_setopt( client->ms_write, CURLOPT_URL, url );
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEDATA, (void*)rb );
   curl_easy_setopt( client->ms_write, CURLOPT_HTTPPOST, forms );

   memset( &client->write_times, 0, sizeof(client->write_times) );

   ms_client_unlock( client );

   return 0;
}


int ms_client_end_uploading( struct ms_client* client ) {
   // not uploading anymore, reaquire
   ms_client_wlock( client );

   print_timings( client->write_times.create_times, client->write_times.num_create_times, HTTP_CREATE_TIMES );
   print_timings( client->write_times.update_times, client->write_times.num_update_times, HTTP_UPDATE_TIMES );
   print_timings( client->write_times.delete_times, client->write_times.num_delete_times, HTTP_DELETE_TIMES );

   if( client->write_times.create_times ) {
      free( client->write_times.create_times );
      client->write_times.create_times = NULL;
   }

   if( client->write_times.update_times ) {
      free( client->write_times.update_times );
      client->write_times.update_times = NULL;
   }

   if( client->write_times.delete_times ) {
      free( client->write_times.delete_times );
      client->write_times.delete_times = NULL;
   }

   DATA( HTTP_VOLUME_TIME, (double)client->write_times.volume_time / 1e9 );
   DATA( HTTP_GATEWAY_TIME, (double)client->write_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->write_times.total_time / 1e9 );

   client->uploading = false;


   // get the results
   long http_response = 0;
   curl_easy_getinfo( client->ms_write, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( client->ms_write, CURLOPT_URL, NULL );
   curl_easy_setopt( client->ms_write, CURLOPT_HTTPPOST, NULL );

   ms_client_unlock( client );

   return http_response;
}


// download metadata from the MS for a Volume.
// metadata_path should be an absolute directory path (like /VOLUME/, or /UG/, or /RG/)
// returns the HTTP response on success, or negative on error
int ms_client_download_volume_metadata( struct ms_client* client, char const* url, char** buf, ssize_t* buflen ) {
   
   char* bits = NULL;
   ssize_t len = 0;
   int http_response = 0;

   ms_client_begin_downloading_view( client, url, NULL );

   // do the download
   memset( &client->read_times, 0, sizeof(client->read_times) );
   len = md_download_file5( client->ms_view, &bits );

   http_response = ms_client_end_downloading_view( client );

   if( len < 0 ) {
      errorf("md_download_file5(%s) rc = %zd\n", url, len );
      return (int)len;
   }

   if( http_response != 200 ) {
      errorf("md_download_file5(%s) HTTP status = %d\n", url, http_response );

      if( http_response == 0 ) {
         // really bad--MS bug
         errorf("%s", "!!! likely an MS bug !!!\n");
         http_response = 500;
      }

      return -http_response;
   }

   *buf = bits;
   *buflen = len;
   
   return 0;
}

// download a cert bundle manifest
int ms_client_download_cert_bundle_manifest( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, Serialization::ManifestMsg* mmsg ) {
   char* url = CALLOC_LIST( char, strlen(client->url) + 1 + strlen("/CERT/") + 1 + 21 + 1 + strlen("manifest.") + 21 + 1 );
   sprintf(url, "%s/CERT/%" PRIu64 "/manifest.%" PRIu64, client->url, volume_id, volume_cert_version );
   
   ms_client_begin_downloading_view( client, url, NULL );
   
   int rc = md_download_manifest( client->conf, client->ms_view, url, mmsg );
   
   int http_response = ms_client_end_downloading_view( client );
   
   if( rc != 0 ) {
      errorf("md_download_manifest(%s) rc = %d\n", url, rc );
      free( url );
      return rc;
   }
   
   if( http_response != 200 ) {
      errorf("md_download_manifest(%s) HTTP status = %d\n", url, http_response );
      
      if( http_response == 0 ) {
         // really bad--MS bug
         errorf("%s", "!!! likely an MS bug !!!\n");
         http_response = 500;
      }
      
      free( url );
      return -http_response;
   }
   
   free( url );
   return rc;
}


// calculate which certs are new, and which are stale, given a manifest of them.
// If we're a UG or RG, then only process certs for writer UGs and our own cert
// If we're an AG, only process our own cert
// client must be read-locked at least 
int ms_client_make_cert_diff( struct ms_client* client, struct ms_volume* vol, Serialization::ManifestMsg* mmsg, ms_cert_diff* certdiff ) {
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   set< uint64_t > present;
   
   char gateway_type_str[5];
   
   // find new certs...
   for( int64_t i = 0; i < mmsg->size(); i++ ) {
      const Serialization::BlockURLSetMsg& cert_block = mmsg->block_url_set(i);
      
      // extract gateway metadata, according to serialization.proto
      uint64_t gateway_id = cert_block.gateway_id();
      uint64_t gateway_type = cert_block.start_id();
      uint64_t cert_version = cert_block.block_versions(0);
      
      ms_client_gateway_type_str( gateway_type, gateway_type_str );
      
      ms_cert_bundle* existing_bundle = cert_bundles[gateway_type];
      
      ms_cert_bundle::iterator itr = existing_bundle->find( gateway_id );
      if( itr != existing_bundle->end() ) {
         // found!
         // need to reload it?
         if( itr->second->version < cert_version ) {
            // new certificate for this gateway!
            struct ms_cert_diff_entry diffent;
            
            diffent.gateway_type = gateway_type;
            diffent.gateway_id = gateway_id;
            diffent.cert_version = cert_version;
            
            dbprintf("new cert: (gateway_type=%s, gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type_str, gateway_id, cert_version );
            
            certdiff->new_certs->push_back( diffent );
         }
      }
      else {
         // certificate exists remotely but not locally
         struct ms_cert_diff_entry diffent;
         
         diffent.gateway_type = gateway_type;
         diffent.gateway_id = gateway_id;
         diffent.cert_version = cert_version;
         
         dbprintf("new cert: (gateway_type=%s, gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type_str, gateway_id, cert_version );
         
         certdiff->new_certs->push_back( diffent );
      }
      
      present.insert( gateway_id );
   }
   
   // find old certs...
   for( int i = 0; cert_bundles[i] != NULL; i++ ) {
      ms_cert_bundle* cert_bundle = cert_bundles[i];
      
      for( ms_cert_bundle::iterator itr = cert_bundle->begin(); itr != cert_bundle->end(); itr++ ) {
         if( present.count( itr->first ) == 0 ) {
            // absent
            struct ms_cert_diff_entry diffent;
         
            diffent.gateway_type = itr->second->gateway_type;
            diffent.gateway_id = itr->second->gateway_id;
            diffent.cert_version = itr->second->version;
            
            ms_client_gateway_type_str( diffent.cert_version, gateway_type_str );
            dbprintf("old cert: (gateway_type=%s, gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type_str, diffent.gateway_id, diffent.cert_version );
            
            certdiff->old_certs->push_back( diffent );
         }
      }
   }
   
   return 0;
}

// get a certificate URL
char* ms_client_cert_url( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, int gateway_type, uint64_t gateway_id, uint64_t gateway_cert_version ) {
   char type_str[5];
   ms_client_gateway_type_str( gateway_type, type_str );
   
   char* url = CALLOC_LIST( char, strlen(client->url) + 1 + strlen("/CERT/") + 1 + 21 + 1 + 21 + 1 + strlen(type_str) + 1 + 21 + 1 + 21 + 1 );
   sprintf( url, "%s/CERT/%" PRIu64 "/%" PRIu64 "/%s/%" PRIu64 "/%" PRIu64, client->url, volume_id, volume_cert_version, type_str, gateway_id, gateway_cert_version );
   
   return url;
}

// given a cert diff, calculate the set of certificate URLs
int ms_client_cert_urls( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, ms_cert_diff_list* new_certs, char*** cert_urls_buf ) {
   vector<char*> cert_urls;
   
   for( unsigned int i = 0; i < new_certs->size(); i++ ) {
      struct ms_cert_diff_entry* diffent = &new_certs->at(i);
      
      char* url = ms_client_cert_url( client, volume_id, volume_cert_version, diffent->gateway_type, diffent->gateway_id, diffent->cert_version );
      
      cert_urls.push_back( url );
   }
   
   char** ret = CALLOC_LIST( char*, cert_urls.size() + 1 );
   for( unsigned int i = 0; i < cert_urls.size(); i++ ) {
      ret[i] = cert_urls[i];
   }
   
   *cert_urls_buf = ret;
   return 0;
}


// download a certificate
int ms_client_download_cert( struct ms_client* client, CURL* curl, char const* url, ms::ms_gateway_cert* ms_cert ) {
   
   char* buf = NULL;
   ssize_t buf_len = 0;
   int http_status = 0;
   
   int rc = md_download_cached( client->conf, curl, url, &buf, &buf_len, MS_MAX_CERT_SIZE, &http_status );
   
   if( rc != 0 ) {
      errorf("md_download_cached(%s) rc = %d\n", url, rc );
      return rc;
   }
   
   // parse the cert...
   bool valid = false;
   
   try {
      valid = ms_cert->ParseFromString( string(buf, buf_len) );
   }
   catch( exception e ) {
      errorf("failed to parse certificate from %s\n", url );
      return -EINVAL;
   }
   
   rc = 0;
   
   if( !valid )
      rc = -EINVAL;
   
   free( buf );
   
   return rc;
}


// given a cert diff list, revoke the contained certificates
int ms_client_revoke_certs( struct ms_client* client, struct ms_volume* vol, ms_cert_diff_list* certdiff ) {
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   for( unsigned int i = 0; i < certdiff->size(); i++ ) {
      struct ms_cert_diff_entry* diffent = &(certdiff->at(i));
      
      ms_cert_bundle::iterator itr = cert_bundles[diffent->gateway_type]->find( diffent->gateway_type );
      if( itr != cert_bundles[diffent->gateway_type]->end() ) {
         // revoke!
         ms_client_gateway_cert_free( itr->second );
         free( itr->second );
         cert_bundles[diffent->gateway_type]->erase( itr );
      }
      else {
         errorf("WARN: No certificate for gateway %" PRIu64 " (type %d)\n", diffent->gateway_id, diffent->gateway_type );
      }
   }
   
   return 0;
}


// find all expired certs
int ms_client_find_expired_certs( struct ms_client* client, struct ms_volume* vol, ms_cert_diff_list* expired ) {
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   for( unsigned int i = 0; cert_bundles[i] != NULL; i++ ) {
      ms_cert_bundle* cert_bundle = cert_bundles[i];
      
      for( ms_cert_bundle::iterator itr = cert_bundle->begin(); itr != cert_bundle->end(); itr++ ) {
         struct ms_gateway_cert* cert = itr->second;
         
         if( cert->expires > 0 && cert->expires < (uint64_t)currentTimeSeconds() ) {
            dbprintf("Certificate for Gateway %" PRIu64 " (type %d) expired at %" PRId64 "\n", cert->gateway_id, cert->gateway_type, cert->expires );
            
            struct ms_cert_diff_entry diffent;
            
            diffent.gateway_type = cert->gateway_type;
            diffent.gateway_id = cert->gateway_id;
            diffent.cert_version = cert->version;
            
            expired->push_back( diffent );
         }
      }
   }
   
   return 0;
}


// reload a Volume's certificates
int ms_client_reload_certs( struct ms_client* client ) {
   
   // get the certificate manifest...
   Serialization::ManifestMsg mmsg;
   
   ms_client_view_rlock( client );
   
   uint64_t volume_id = client->volume->volume_id;
   uint64_t volume_cert_version = client->volume->volume_cert_version;
   
   ms_client_view_unlock( client );
   
   int rc = ms_client_download_cert_bundle_manifest( client, volume_id, volume_cert_version, &mmsg );
   if( rc != 0 ) {
      errorf("ms_client_download_cert_bundle_manifest(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   dbprintf("Got cert manifest with %" PRIu64 " certificates\n", mmsg.size() );
   
   // lock Volume data to calculate the certs we need...
   ms_client_view_wlock( client );
   
   // get the old and new certs...
   struct ms_cert_diff certdiff;
   
   rc = ms_client_make_cert_diff( client, client->volume, &mmsg, &certdiff );
   if( rc != 0 ) {
      ms_client_view_unlock( client );
      errorf("ms_client_make_cert_diff(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // revoke old certs
   rc = ms_client_revoke_certs( client, client->volume, certdiff.old_certs );
   if( rc != 0 ){
      ms_client_view_unlock( client );
      errorf("ms_client_revoke_certs(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // get the URLs for the new certs...
   char** cert_urls = NULL;
   rc = ms_client_cert_urls( client, volume_id, volume_cert_version, certdiff.new_certs, &cert_urls );
   if( rc != 0 ) {
      ms_client_view_unlock( client );
      errorf("ms_client_cert_urls(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // unlock Volume data, so we can download without locking the view-change threads
   ms_client_view_unlock( client );
   
   // what's our gateway id?
   ms_client_rlock( client );
   uint64_t my_gateway_id = client->gateway_id;
   ms_client_unlock( client );
   
   // get the new certs...
   CURL* curl = curl_easy_init();
   
   for( int i = 0; cert_urls[i] != NULL; i++ ) {
      
      md_init_curl_handle( client->conf, curl, cert_urls[i], client->conf->connect_timeout );
      
      ms::ms_gateway_cert ms_cert;
      
      dbprintf("Get certificate %s\n", cert_urls[i] );
      
      rc = ms_client_download_cert( client, curl, cert_urls[i], &ms_cert );
      if( rc != 0 ) {
         errorf("ms_client_download_cert(%s) rc = %d\n", cert_urls[i], rc );
         continue;
      }
      
      // lock Volume data...
      ms_client_view_wlock( client );
      
      if( client->volume->volume_cert_version > volume_cert_version ) {
         // moved on
         volume_cert_version = client->volume->volume_cert_version;
         
         ms_client_view_unlock( client );
         curl_easy_cleanup( curl );
         
         errorf("Volume cert version %" PRIu64 " is too old (expected greater than %" PRIu64 ")\n", volume_cert_version, client->volume->volume_cert_version );
         return 0;
      }
      
      // advance the volume cert version
      client->volume->volume_cert_version = volume_cert_version;
      
      // check signature with Volume public key
      rc = md_verify< ms::ms_gateway_cert >( client->volume->volume_public_key, &ms_cert );
      if( rc != 0 ) {
         ms_client_view_unlock( client );
         
         errorf("Signature verification failed for certificate at %s\n", cert_urls[i] );
         continue;
      }
      
      // load!
      struct ms_gateway_cert* new_cert = CALLOC_LIST( struct ms_gateway_cert, 1 );
      rc = ms_client_load_cert( my_gateway_id, new_cert, &ms_cert );
      if( rc != 0 ) {
         ms_client_view_unlock( client );
         
         errorf("ms_client_load_cert(%s) rc = %d\n", cert_urls[i], rc );
         free( new_cert );
         continue;
      }
      
      // load this cert in, if it is newer
      // clear the old one, if needed.
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ new_cert->gateway_type ]->find( new_cert->gateway_id );
      if( itr != cert_bundles[ new_cert->gateway_type ]->end() ) {
         // verify that this certificate is newer (otherwise reject it)
         struct ms_gateway_cert* old_cert = itr->second;
         if( old_cert->version >= new_cert->version ) {
            if( old_cert->version > new_cert->version ) {
               // tried to load an old cert
               errorf("Downloaded certificate for Gateway %s (ID %" PRIu64 ") with old version %" PRIu64 "; expected greater than %" PRIu64 "\n", old_cert->name, old_cert->gateway_id, new_cert->version, old_cert->version);
            }
            
            ms_client_gateway_cert_free( new_cert );
            free( new_cert );
         }
         else {
            // old cert--revoke
            ms_client_gateway_cert_free( itr->second );
            free( itr->second );
            cert_bundles[ new_cert->gateway_type ]->erase( itr );
         }
      }
      dbprintf("Trusting new certificate for Gateway %s (ID %" PRIu64 ")\n", new_cert->name, new_cert->gateway_id);
      (*cert_bundles[ new_cert->gateway_type ])[ new_cert->gateway_id ] = new_cert;
      
      ms_client_view_unlock( client );
   }
   
   curl_easy_cleanup( curl );
   
   for( int i = 0; cert_urls[i] != NULL; i++ ) {
      free( cert_urls[i] );
   }
   free( cert_urls );
   
   return 0;
}


// download a volume's metadata by name
int ms_client_download_volume_by_name( struct ms_client* client, char const* volume_name, struct ms_volume* vol, char const* volume_pubkey_pem ) {
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   ssize_t len = 0;
   int rc = 0;

   char* volume_url = ms_client_volume_url_by_name( client, volume_name );

   rc = ms_client_download_volume_metadata( client, volume_url, &buf, &len );

   free( volume_url );
   
   if( rc != 0 ) {
      errorf("ms_client_download_volume_metadata rc = %d\n", rc );

      return rc;
   }

   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      errorf("Invalid data for Volume %s\n", volume_name );
      return -EINVAL;
   }
   
   rc = ms_client_load_volume_metadata( vol, &volume_md, volume_pubkey_pem );
   if( rc != 0 ) {
      errorf("ms_client_load_volume_metadata rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// reload volume metadata
// client must NOT be locked.
int ms_client_reload_volume( struct ms_client* client ) {
   int rc = 0;
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   ssize_t len = 0;

   ms_client_view_rlock( client );
 
   struct ms_volume* vol = client->volume;
   
   if( vol == NULL ) {
      errorf("%s", "ERR: unbound from Volume!\n" );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // get the Volume ID for later
   uint64_t volume_id = vol->volume_id;
   
   char* volume_url = ms_client_volume_url( client, vol->volume_id );

   ms_client_view_unlock( client );

   rc = ms_client_download_volume_metadata( client, volume_url, &buf, &len );

   free( volume_url );
   
   if( rc != 0 ) {
      errorf("ms_client_download_volume_metadata rc = %d\n", rc );

      return rc;
   }

   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      errorf("Invalid data for Volume %" PRIu64 "\n", volume_id );
      return -EINVAL;
   }
   
   ms_client_view_wlock( client );

   // re-find the Volume
   vol = client->volume;
   if( vol == NULL ) {
      errorf("%s", "ERR: unbound from Volume!" );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   uint64_t old_version = vol->volume_version;
   uint64_t old_cert_version = vol->volume_cert_version;
   
   // get the new versions, and make sure they've advanced.
   uint64_t new_version = volume_md.volume_version();
   uint64_t new_cert_version = volume_md.cert_version();
   
   if( new_version < old_version ) {
      errorf("Invalid volume version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_version, new_version );
      ms_client_view_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_cert_version < old_cert_version ) {
      errorf("Invalid certificate version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_cert_version, new_cert_version );
      ms_client_view_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_version > old_version ) {
      // have new data--load it in
      rc = ms_client_load_volume_metadata( vol, &volume_md, NULL );
   }
   else {
      rc = 0;
   }
   
   ms_client_view_unlock( client );
   
   if( rc != 0 ) {
      errorf("ms_client_load_volume_metadata(%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // do we need to download the UGs and/or RGs as well?
   dbprintf("Volume  version %" PRIu64 " --> %" PRIu64 "\n", old_version, new_version );
   dbprintf("Cert    version %" PRIu64 " --> %" PRIu64 "\n", old_cert_version, new_cert_version );

   // load new certificate information, if we have any
   if( new_cert_version > old_cert_version ) {
      rc = ms_client_reload_certs( client );
      if( rc != 0 ) {
         errorf("ms_client_reload_certs rc = %d\n", rc );

         return rc;
      }
   }
   return 0;
}

// verify that a message came from a UG with the given ID (needed by libsyndicate python wrapper)
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len ) {
   ms_client_view_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      errorf("Message from outside the Volume (%" PRIu64 ")\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // only UGs can send messages...
   ms_cert_bundle::iterator itr = client->volume->UG_certs->find( gateway_id );
   if( itr == client->volume->UG_certs->end() ) {
      // not found here--probably means we need to reload our certs
      
      dbprintf("WARN: No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      client->early_reload = true;
      ms_client_view_unlock( client );
      return -EAGAIN;
   }
   
   int rc = md_verify_signature( itr->second->pubkey, msg, msg_len, sigb64, sigb64_len );
   
   ms_client_view_unlock( client );
   
   return rc;
}


// (re)load a gateway certificate.
// If my_gateway_id matches the ID in the cert, then load the closure as well (since we'll need it)
int ms_client_load_cert( uint64_t my_gateway_id, struct ms_gateway_cert* cert, const ms::ms_gateway_cert* ms_cert ) {
   cert->user_id = ms_cert->owner_id();
   cert->gateway_id = ms_cert->gateway_id();
   cert->gateway_type = ms_cert->gateway_type();
   cert->name = strdup( ms_cert->name().c_str() );
   cert->hostname = strdup( ms_cert->host().c_str() );
   cert->portnum = ms_cert->port();
   cert->blocksize = ms_cert->blocksize();
   cert->version = ms_cert->version();
   cert->caps = ms_cert->caps();
   cert->volume_id = ms_cert->volume_id();
   
   // NOTE: closure information is base64-encoded
   if( my_gateway_id == cert->gateway_id && ms_cert->closure_text().size() > 0 ) {
      cert->closure_text_len = ms_cert->closure_text().size();
      cert->closure_text = CALLOC_LIST( char, cert->closure_text_len + 1 );
      memcpy( cert->closure_text, ms_cert->closure_text().c_str(), cert->closure_text_len );
   }
   else {
      cert->closure_text = NULL;
      cert->closure_text_len = 0;
   }
   
   // validate... 
   if( !VALID_GATEWAY_TYPE( cert->gateway_type ) ) {
      errorf("Invalid gateway type %d\n", cert->gateway_type );
      return -EINVAL;
   }

   int rc = 0;
   
   if( strcmp( ms_cert->public_key().c_str(), "NONE" ) == 0 ) {
      // no public key for this gateway on the MS
      dbprintf("WARN: No public key for Gateway %s\n", cert->name );
      cert->pubkey = NULL;
   }
   else {
      int rc = md_load_pubkey( &cert->pubkey, ms_cert->public_key().c_str() );
      if( rc != 0 ) {
         errorf("md_load_pubkey(Gateway %s) rc = %d\n", cert->name, rc );
      }
   }
   
   if( rc == 0 ) {
      char gateway_type_str[5];
      ms_client_gateway_type_str( cert->gateway_type, gateway_type_str );
      
      dbprintf("Loaded cert (user_id=%" PRIu64 ", gateway_type=%s, gateway_id=%" PRIu64 ", gateway_name=%s, hostname=%s, portnum=%d, blocksize=%" PRIu64 ", version=%" PRIu64 ", caps=%" PRIX64 ")\n",
               cert->user_id, gateway_type_str, cert->gateway_id, cert->name, cert->hostname, cert->portnum, cert->blocksize, cert->version, cert->caps );
   }
   
   return rc;
}


// populate a Volume structure with the volume metadata
int ms_client_load_volume_metadata( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem ) {

   int rc = 0;
   
   // get the new public key
   if( vol->reload_volume_key || vol->volume_public_key == NULL || volume_pubkey_pem != NULL ) {
      vol->reload_volume_key = false;
      
      // trust it this time, but not in the future
      if( volume_pubkey_pem != NULL )
         rc = md_load_pubkey( &vol->volume_public_key, volume_pubkey_pem );
      else
         rc = md_load_pubkey( &vol->volume_public_key, volume_md->volume_public_key().c_str() );
      
      if( rc != 0 ) {
         errorf("md_load_pubkey rc = %d\n", rc );
         return -ENOTCONN;
      }
   }

   if( vol->volume_public_key == NULL ) {
      errorf("%s", "unable to verify integrity of metadata for Volume!  No public key given!\n");
      return -ENOTCONN;
   }

   // verify metadata
   rc = md_verify<ms::ms_volume_metadata>( vol->volume_public_key, volume_md );
   if( rc != 0 ) {
      errorf("Signature verification failed on Volume %s (%" PRIu64 "), rc = %d\n", volume_md->name().c_str(), volume_md->volume_id(), rc );
      return rc;
   }

   // sanity check 
   if( vol->name ) {
      char* new_name = strdup( volume_md->name().c_str() );
      if( strcmp( new_name, vol->name ) != 0 ) {
         errorf("Invalid Volume metadata: tried to change name from '%s' to '%s'\n", vol->name, new_name );
         free( new_name );
         return -EINVAL;
      }
      free( new_name );
   }
   
   struct md_entry* root = NULL;
   if( volume_md->has_root() ) {
      root = CALLOC_LIST( struct md_entry, 1 );
      ms_entry_to_md_entry( volume_md->root(), root );
   }
   
   if( vol->UG_certs == NULL )
      vol->UG_certs = new ms_cert_bundle();
   
   if( vol->RG_certs == NULL )
      vol->RG_certs = new ms_cert_bundle();
   
   if( vol->AG_certs == NULL )
      vol->AG_certs = new ms_cert_bundle();

   vol->volume_cert_version = volume_md->cert_version();
   vol->volume_id = volume_md->volume_id();
   vol->volume_owner_id = volume_md->owner_id();
   vol->blocksize = volume_md->blocksize();
   vol->volume_version = volume_md->volume_version();
   vol->num_files = volume_md->num_files();
   
   if( vol->root ) {
      md_entry_free( vol->root );
      free( vol->root );
   }
   
   vol->root = root;

   if( vol->name == NULL )
      vol->name = strdup( volume_md->name().c_str() );

   return 0;
}


// load a registration message
int ms_client_load_registration_metadata( struct ms_client* client, ms::ms_registration_metadata* registration_md, char const* volume_pubkey_pem ) {

   int rc = 0;

   struct ms_gateway_cert cert;
   memset( &cert, 0, sizeof(cert) );

   // load cert
   const ms::ms_gateway_cert& my_cert = registration_md->cert();
   rc = ms_client_load_cert( 0, &cert, &my_cert );
   if( rc != 0 ) {
      errorf("ms_client_load_cert rc = %d\n", rc );
      return rc;
   }

   ms_client_rlock( client );

   // verify that our host and port match the MS's record
   if( strcmp( cert.hostname, client->conf->hostname ) != 0 || cert.portnum != client->conf->portnum ) {
      // wrong host
      errorf("ERR: This gateway is running on %s:%d, but the MS says it should be running on %s:%d.  Please update the Gateway record on the MS.\n", client->conf->hostname, client->conf->portnum, cert.hostname, cert.portnum );
      ms_client_unlock( client );

      ms_client_gateway_cert_free( &cert );
      return -ENOTCONN;
   }

   ms_client_unlock( client );

   struct ms_volume* volume = CALLOC_LIST( struct ms_volume, 1 );
   
   dbprintf("Registered as Gateway %s (%" PRIu64 ")\n", cert.name, cert.gateway_id );
   
   volume->reload_volume_key = true;         // get the public key

   ms::ms_volume_metadata* vol_md = registration_md->mutable_volume();

   // load the Volume information
   rc = ms_client_load_volume_metadata( volume, vol_md, volume_pubkey_pem );
   if( rc != 0 ) {
      errorf("ms_client_load_volume_metadata(%s) rc = %d\n", vol_md->name().c_str(), rc );
      
      ms_volume_free( volume );
      free( volume );
      ms_client_gateway_cert_free( &cert );
      return rc;
   }

   dbprintf("Volume ID %" PRIu64 ": '%s', version: %" PRIu64 ", certs: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version, volume->volume_cert_version );

   ms_client_view_wlock( client );
   client->volume = volume;
   ms_client_view_unlock( client );
   

   ms_client_wlock( client );
   
   // new session password
   curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, NULL );
   curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, NULL );
   curl_easy_setopt( client->ms_view, CURLOPT_USERPWD, NULL );

   if( client->userpass ) {
      free( client->userpass );
   }

   // userpass format:
   // ${gateway_type}_${gateway_id}:${password}
   char gateway_type_str[5];
   char gateway_id_str[50];

   if( client->session_password )
      free( client->session_password );
   
   client->session_password = strdup( registration_md->session_password().c_str() );
   client->session_expires = registration_md->session_expires();
   
   sprintf(gateway_id_str, "%" PRIu64, cert.gateway_id );
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   client->userpass = CALLOC_LIST( char, strlen(gateway_id_str) + 1 + strlen(gateway_type_str) + 1 + strlen(client->session_password) + 1 );
   sprintf( client->userpass, "%s_%s:%s", gateway_type_str, gateway_id_str, client->session_password );

   curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_view, CURLOPT_USERPWD, client->userpass );

   client->owner_id = cert.user_id;
   client->gateway_id = cert.gateway_id;
   
   // sanity check...
   if( client->session_expires > 0 && client->session_expires < currentTimeSeconds() ) {
      errorf("Session password expired at %" PRId64 "\n", client->session_expires );
      rc = -EINVAL;
   }
   
   ms_client_unlock( client );

   ms_client_gateway_cert_free( &cert );

   return rc;
}

// dummy CURL read
size_t ms_client_dummy_read( void *ptr, size_t size, size_t nmemb, void *userdata ) {
   return size * nmemb;
}

// dummy CURL write
size_t ms_client_dummy_write( char *ptr, size_t size, size_t nmemb, void *userdata) {
   return size * nmemb;
}

// read an OpenID reply from the MS
int ms_client_load_openid_reply( ms::ms_openid_provider_reply* oid_reply, char* openid_redirect_reply_bits, size_t openid_redirect_reply_bits_len ) {
   // get back the OpenID provider reply
   string openid_redirect_reply_bits_str = string( openid_redirect_reply_bits, openid_redirect_reply_bits_len );

   bool valid = oid_reply->ParseFromString( openid_redirect_reply_bits_str );
   if( !valid ) {
      errorf("%s", "Invalid MS OpenID provider reply\n");
      return -EINVAL;
   }

   return 0;
}


// begin the registration process.  Ask to be securely redirected from the MS to the OpenID provider
int ms_client_begin_register( CURL* curl, char const* username, char const* register_url, ms::ms_openid_provider_reply* oid_reply ) {

   // url-encode the username
   char* username_encoded = url_encode( username, strlen(username) );
   
   // post arguments
   char* post = CALLOC_LIST( char, strlen("openid_username") + 1 + strlen(username_encoded) + 1 );
   sprintf( post, "openid_username=%s", username_encoded );
   
   free( username_encoded );

   response_buffer_t rb;      // will hold the OpenID provider reply
   response_buffer_t header_rb;
   
   curl_easy_setopt( curl, CURLOPT_URL, register_url );
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, post );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)&rb );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );

   int rc = curl_easy_perform( curl );

   long http_response = 0;
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, NULL );

   free( post );

   if( rc != 0 ) {
      long err = 0;
   
      // get the errno
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      return err;
   }

   if( http_response != 200 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      return -http_response;
   }

   if( rb.size() == 0 ) {
      errorf("%s", "no response\n");
      response_buffer_free( &rb );
      return -ENODATA;
   }

   char* response = response_buffer_to_string( &rb );
   size_t len = response_buffer_size( &rb );

   rc = ms_client_load_openid_reply( oid_reply, response, len );

   free( response );
   response_buffer_free( &rb );

   return rc;
}


int ms_client_split_url_qs( char const* url, char** url_and_path, char** qs ) {
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

int ms_client_set_method( CURL* curl, char const* method, char const* url, char const* qs ) {

   curl_easy_setopt( curl, CURLOPT_URL, url );
   
   if( strcmp(method, "POST") == 0 ) {
      curl_easy_setopt( curl, CURLOPT_POST, 1L );

      if( qs )
         curl_easy_setopt( curl, CURLOPT_POSTFIELDS, qs );
   }
   else if( strcmp(method, "GET") == 0 ) {
      curl_easy_setopt( curl, CURLOPT_HTTPGET, 1L );
   }
   else {
      errorf("Invalid HTTP method '%s'\n", method );
      return -EINVAL;
   }
   return 0;
}

// authenticate to the OpenID provider.
// populate the return_to URL
// client must be read-locked
int ms_client_auth_op( char const* ms_username, char const* ms_password, CURL* curl, ms::ms_openid_provider_reply* oid_reply, char** return_to_method, char** return_to ) {

   char* post = NULL;
   char const* openid_redirect_url = oid_reply->redirect_url().c_str();
   long http_response = 0;
   
   // how we ask the OID provider to challenge us
   char const* challenge_method = oid_reply->challenge_method().c_str();

   // how we respond to the OID provider challenge
   char const* response_method = oid_reply->response_method().c_str();

   // how we redirect to the OID RP
   char const* redirect_method = oid_reply->redirect_method().c_str();

   dbprintf("%s challenge to %s\n", challenge_method, openid_redirect_url );

   response_buffer_t header_rb;
   
   // inform the OpenID provider that we have been redirected by the RP by fetching the authentication page.
   // The OpenID provider may then redirect us back.
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );     // catch 302
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );
   curl_easy_setopt( curl, CURLOPT_READFUNCTION, ms_client_dummy_read );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, ms_client_dummy_write );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_READDATA, NULL );
   //curl_easy_setopt( curl, CURLOPT_VERBOSE, 1L );

   char* url_and_path = NULL;
   char* url_qs = NULL;
   int rc = ms_client_split_url_qs( openid_redirect_url, &url_and_path, &url_qs );
   if( rc != 0 ) {
      // no query string
      url_and_path = strdup( openid_redirect_url );
   }
   
   rc = ms_client_set_method( curl, challenge_method, url_and_path, url_qs );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", challenge_method, rc );
      return rc;
   }

   rc = curl_easy_perform( curl );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );

   free( url_and_path );
   if( url_qs )
      free( url_qs );

   if( rc != 0 ) {
      
      long err = 0;
      
      // get the errno
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      
      response_buffer_free( &header_rb );
      return -abs(rc);
   }

   if( http_response != 200 && http_response != 302 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      response_buffer_free( &header_rb );
      return -http_response;
   }

   if( http_response == 302 ) {
      // authenticated already; we're being sent back
      char* url = response_buffer_to_string( &header_rb );

      dbprintf("return to %s", url );
      
      *return_to = url;
      *return_to_method = strdup( redirect_method );
      
      response_buffer_free( &header_rb );

      return 0;
   }

   response_buffer_free( &header_rb );

   // authenticate to the OpenID provider
   char const* extra_args = oid_reply->extra_args().c_str();
   char const* username_field = oid_reply->username_field().c_str();
   char const* password_field = oid_reply->password_field().c_str();
   char const* auth_handler = oid_reply->auth_handler().c_str();

   char* username_urlencoded = url_encode( ms_username, strlen(ms_username) );
   char* password_urlencoded = url_encode( ms_password, strlen(ms_password) );
   post = CALLOC_LIST( char, strlen(username_field) + 1 + strlen(username_urlencoded) + 1 +
                             strlen(password_field) + 1 + strlen(password_urlencoded) + 1 +
                             strlen(extra_args) + 1);

   sprintf(post, "%s=%s&%s=%s&%s", username_field, username_urlencoded, password_field, password_urlencoded, extra_args );

   free( username_urlencoded );
   free( password_urlencoded );

   dbprintf("%s authenticate to %s?%s\n", response_method, auth_handler, post );

   rc = ms_client_set_method( curl, response_method, auth_handler, post );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", response_method, rc );
      return rc;
   }

   // send the authentication request
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );
   curl_easy_setopt( curl, CURLOPT_READFUNCTION, ms_client_dummy_read );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, ms_client_dummy_write );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_READDATA, NULL );

   rc = curl_easy_perform( curl );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );

   free( post );

   if( rc != 0 ) {
      long err = 0;
      
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      response_buffer_free( &header_rb );
      return -abs(rc);
   }

   if( http_response != 302 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      response_buffer_free( &header_rb );
      return -http_response;
   }

   // authenticated! we're being sent back
   char* url = response_buffer_to_string( &header_rb );
   response_buffer_free( &header_rb );
   
   *return_to = url;
   *return_to_method = strdup( redirect_method );
   return 0;
}

// complete the registration
// client must not be locked
int ms_client_complete_register( CURL* curl, char const* return_to_method, char const* return_to, ms::ms_registration_metadata* registration_md ) {

   dbprintf("%s return to %s\n", return_to_method, return_to );

   char* return_to_url_and_path = NULL;
   char* return_to_qs = NULL;

   char* bits = NULL;
   ssize_t len = 0;
   long http_response = 0;

   int rc = ms_client_split_url_qs( return_to, &return_to_url_and_path, &return_to_qs );
   if( rc != 0 ) {
      // no qs
      return_to_url_and_path = strdup( return_to );
   }
   
   rc = ms_client_set_method( curl, return_to_method, return_to_url_and_path, return_to_qs );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", return_to_method, rc );
      free( return_to_url_and_path );
      if( return_to_qs )
         free( return_to_qs );
      
      return rc;
   }

   // get the registration data
   len = md_download_file5( curl, &bits );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );

   if( len < 0 ) {
      errorf("md_download_file5 rc = %zd\n", len );
      free( return_to_url_and_path );
      if( return_to_qs )
         free( return_to_qs );
      return (int)len;
   }

   if( http_response != 200 ) {
      errorf("md_download_file5 HTTP status = %ld\n", http_response );
      free( return_to_url_and_path );
      if( return_to_qs )
         free( return_to_qs );
      return -abs( (int)http_response );
   }

   // got the data
   bool valid = registration_md->ParseFromString( string(bits, len) );
   free( bits );

   free( return_to_url_and_path );
   if( return_to_qs )
      free( return_to_qs );

   if( !valid ) {
      errorf( "%s", "invalid registration metadata\n" );
      return -EINVAL;
   }

   return 0;
}
   
   

// register this gateway with the MS, using the SyndicateUser's OpenID username and password
int ms_client_openid_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password, char const* volume_pubkey_pem ) {

   int rc = 0;
   char* register_url = ms_client_openid_register_url( client );

   CURL* curl = curl_easy_init();
   
   ms::ms_openid_provider_reply oid_reply;
   ms::ms_registration_metadata registration_md;

   dbprintf("register at %s\n", register_url );

   // enable the cookie parser
   curl_easy_setopt( curl, CURLOPT_COOKIEFILE, "/COOKIE" );
   
   ms_client_rlock( client );
   
   md_init_curl_handle( client->conf, curl, NULL, client->conf->connect_timeout );

   // get info for the OpenID provider
   rc = ms_client_begin_register( curl, username, register_url, &oid_reply );

   free( register_url );
   
   if( rc != 0 ) {
      errorf("ms_client_begin_register rc = %d\n", rc);
      ms_client_unlock( client );
      curl_easy_cleanup( curl );
      return rc;
   }

   // authenticate to the OpenID provider
   char* return_to = NULL;
   char* return_to_method = NULL;
   rc = ms_client_auth_op( client->conf->ms_username, client->conf->ms_password, curl, &oid_reply, &return_to_method, &return_to );
   if( rc != 0 ) {
      errorf("ms_client_auth_op rc = %d\n", rc);
      ms_client_unlock( client );
      curl_easy_cleanup( curl );
      return rc;
   }

   // complete the registration 
   rc = ms_client_complete_register( curl, return_to_method, return_to, &registration_md );
   if( rc != 0 ) {
      errorf("ms_client_complete_register rc = %d\n", rc);
      ms_client_unlock( client );
      curl_easy_cleanup( curl );
      return rc;
   }

   ms_client_unlock( client );

   free( return_to );
   free( return_to_method );

   curl_easy_cleanup( curl );
   
   // load up the registration information, including our set of Volumes
   rc = ms_client_load_registration_metadata( client, &registration_md, volume_pubkey_pem );
   if( rc != 0 ) {
      errorf("ms_client_load_registration_metadata rc = %d\n", rc );
      return rc;
   }
   
   // load the certificate bundle   
   rc = ms_client_reload_certs( client );
   
   return rc;
}


// anonymously register with a (public) volume
int ms_client_anonymous_gateway_register( struct ms_client* client, char const* volume_name, char const* volume_public_key_pem ) {
   int rc = 0;

   struct ms_gateway_cert cert;
   memset( &cert, 0, sizeof(cert) );

   struct ms_volume* volume = CALLOC_LIST( struct ms_volume, 1 );
   
   if( volume_public_key_pem != NULL ) {
      // attempt to load the public key 
      rc = md_load_pubkey( &volume->volume_public_key, volume_public_key_pem );
      if( rc != 0 ) {
         errorf("md_load_pubkey rc = %d\n", rc );
         return -EINVAL;
      }
   }
   else {  
      volume->reload_volume_key = true;         // get the public key
   }

   rc = ms_client_download_volume_by_name( client, volume_name, volume, volume_public_key_pem );
   if( rc != 0 ) {
      errorf("ms_client_download_volume_by_name(%s) rc = %d\n", volume_name, rc );
      ms_volume_free( volume );
      free( volume );
      return -ENOTCONN;
   }
   
   dbprintf("Volume ID %" PRIu64 ": '%s', version: %" PRIu64 ", certs: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version, volume->volume_cert_version );

   ms_client_view_wlock( client );
   client->volume = volume;
   ms_client_view_unlock( client );

   ms_client_wlock( client );
   
   // fill in sane defaults
   curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, NULL );
   curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, NULL );
   curl_easy_setopt( client->ms_view, CURLOPT_USERPWD, NULL );

   if( client->session_password )
      free( client->session_password );
   
   client->session_password = NULL;
   client->session_expires = -1;
   client->gateway_type = client->conf->gateway_type;
   client->owner_id = client->conf->owner;
   client->gateway_id = client->conf->gateway;
   
   ms_client_unlock( client );
   
   // load the certificate bundle   
   rc = ms_client_reload_certs( client );

   return rc;
}



// read-lock a client context 
int ms_client_rlock( struct ms_client* client ) {
   //dbprintf("ms_client_rlock %p\n", client);
   return pthread_rwlock_rdlock( &client->lock );
}

// write-lock a client context 
int ms_client_wlock( struct ms_client* client ) {
   //dbprintf("ms_client_wlock %p\n", client);
   return pthread_rwlock_wrlock( &client->lock );
}

// unlock a client context 
int ms_client_unlock( struct ms_client* client ) {
   //dbprintf("ms_client_unlock %p\n", client);
   return pthread_rwlock_unlock( &client->lock );
}

// read-lock a client context's view
int ms_client_view_rlock( struct ms_client* client ) {
   //dbprintf("ms_client_view_rlock %p\n", client);
   return pthread_rwlock_rdlock( &client->view_lock );
}

// write-lock a client context's view
int ms_client_view_wlock( struct ms_client* client ) {
   //dbprintf("ms_client_view_wlock %p\n", client);
   return pthread_rwlock_wrlock( &client->view_lock );
}

// unlock a client context's view
int ms_client_view_unlock( struct ms_client* client ) {
   //dbprintf("ms_client_view_unlock %p\n", client);
   return pthread_rwlock_unlock( &client->view_lock );
}


// put back an update to the client context, but only if it is the most recent one
// return 0 on success
// return -EEXIST if a more recent update already exists
static inline int ms_client_put_update( update_set* updates, deadline_queue* deadlines, long path_hash, struct md_update* update, uint64_t deadline ) {
   bool replace = true;
   int rc = 0;
   
   if( updates ) {
      if( updates->count( path_hash ) == 0 ) {
         memcpy( &((*updates)[ path_hash ]), update, sizeof(struct md_update) );
      }
      else {
         struct md_update* current_update = &((*updates)[path_hash]);
         if( current_update->ent.mtime_sec > update->ent.mtime_sec || (current_update->ent.mtime_sec == update->ent.mtime_sec && current_update->ent.mtime_nsec > update->ent.mtime_nsec) ) {
            // current update is more recent
            replace = false;
            rc = -EEXIST;
         }
         else {
            md_update_free( &((*updates)[ path_hash ]) );
            memcpy( &((*updates)[ path_hash ]), update, sizeof(struct md_update) );
         }
      }
   }
   if( deadlines && replace ) {
      (*deadlines)[deadline] = path_hash;
   }
   
   return rc;
}


// add or replace an existing update in the client context
// return 0 on success 
int ms_client_queue_update( struct ms_client* client, struct md_entry* update, uint64_t deadline_ms, uint64_t deadline_delta ) {

   int rc = 0;

   if( deadline_ms < (uint64_t)currentTimeMillis() ) {
      rc = ms_client_update( client, update );
      return rc;
   }

   ms_client_wlock( client );

   long path_hash = ms_client_hash( update->volume, update->file_id );

   if( client->updates->count( path_hash ) == 0 ) {
      // not yet added
      struct md_update* up = &(*client->updates)[path_hash];
      up->op = ms::ms_update::UPDATE;
      md_entry_dup2( update, &up->ent );

      (*client->deadlines)[deadline_ms] = path_hash;
   }
   else {
      // find the associated deadline
      deadline_queue::iterator itr = client->deadlines->find( path_hash );
      uint64_t new_deadline = 0;
      bool found = false;
      
      if( itr != client->deadlines->end() ) {
         uint64_t deadline = itr->first;

         found = true;
         new_deadline = deadline + deadline_delta;
      }
      else {
         new_deadline = deadline_ms;
      }

      // free up the memory of this update
      md_update_free( &((*client->updates)[ path_hash ]) );

      if( new_deadline <= (uint64_t)currentTimeMillis() ) {
         // deadline is now
         if( found )
            client->deadlines->erase( itr );

         client->updates->erase( path_hash );
         
         ms_client_unlock( client );

         rc = ms_client_update( client, update );
         return rc;
      }
      else {
         struct md_update* up = &(*client->updates)[path_hash];
         up->op = ms::ms_update::UPDATE;
         md_entry_dup2( update, &up->ent );

         if( found )
            client->deadlines->erase( itr );

         (*client->deadlines)[new_deadline] = path_hash;
      }
   }

   // wake up the uploader
   ms_client_uploader_signal( client );
   
   ms_client_unlock( client );
   return rc;
}


// remove an update and put it into a caller-supplied buffer.
// client must be write-locked first
int ms_client_remove_update( struct ms_client* client, long path_hash, struct md_update* old_update, uint64_t* deadline ) {
   int rc = 0;
   if( client->updates->count( path_hash ) != 0 ) {
      // clear it out
      if( old_update ) {
         memcpy( old_update, &((*client->updates)[path_hash]), sizeof(struct md_update) );
      }
      else {
         //dbprintf("clearing update(path=%s, url=%s)\n", (*client->updates)[path_hash].ent.path, (*client->updates)[path_hash].ent.url );
         md_update_free( &((*client->updates)[ path_hash ]) );
      }

      client->updates->erase( path_hash );

      // search deadlines
      for( deadline_queue::iterator itr = client->deadlines->begin(); itr != client->deadlines->end(); itr++ ) {
         if( itr->second == path_hash ) {
            if( deadline )
               *deadline = itr->first;

            client->deadlines->erase( itr );
            break;
         }
      }
   }
   else {
      rc = -ENOENT;
   }
   return rc;
}


// clear an existing udate
// return 0 on success
// return -ENOENT on failure 
int ms_client_clear_update( struct ms_client* client, uint64_t volume_id, uint64_t file_id ) {
   int rc = 0;

   ms_client_wlock( client );

   rc = ms_client_remove_update( client, ms_client_hash( volume_id, file_id ), NULL, NULL );

   ms_client_unlock( client );
   return rc;
}


// post data
static int ms_client_send( struct ms_client* client, char const* url, char const* data, size_t len, ms::ms_reply* reply, bool verify ) {
   struct curl_httppost *post = NULL, *last = NULL;
   int rc = 0;
   response_buffer_t* rb = new response_buffer_t();

   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, data, CURLFORM_BUFFERLENGTH, len, CURLFORM_END );

   ms_client_begin_uploading( client, url, rb, post );
   
   // do the upload
   struct timespec ts, ts2;
   BEGIN_TIMING_DATA( ts );

   rc = curl_easy_perform( client->ms_write );

   END_TIMING_DATA( ts, ts2, "MS send" );

   int http_response = ms_client_end_uploading( client );
   
   // what happened?
   if( rc != 0 ) {
      // curl failed
      errorf( "curl_easy_perform rc = %d\n", rc );
   }
   else if( http_response == 200 ) {
      // got something!
      rc = 0;
      if( rb->size() > 0 ) {
         // this should be an ms_reply structure
         
         char* ret = response_buffer_to_string( rb );
         size_t ret_len = response_buffer_size( rb );
         
         rc = ms_client_parse_reply( client, reply, ret, ret_len, verify );
         if( rc != 0 ) {
            // failed to parse--bad message
            errorf("ms_client_parse_reply rc = %d\n", rc );
            rc = -EBADMSG;
         }
         else {
            // check for errors
            rc = reply->error();
            if( rc != 0 ) {
               errorf("MS reply error %d\n", rc );
            }
         }
         
         free( ret );
      }
      else {
         // no response...
         rc = -ENODATA;
      }
   }
   else {
      // protocol error
      errorf("curl_easy_perform HTTP status %d\n", http_response );
      rc = -EREMOTEIO;
   }

   response_buffer_free( rb );
   delete rb;
   
   curl_formfree( post );
   
   return rc;
}


// convert an update_set into a protobuf
static int ms_client_serialize_update_set( update_set* updates, ms::ms_updates* ms_updates ) {
   // populate the protobuf
   for( update_set::iterator itr = updates->begin(); itr != updates->end(); itr++ ) {

      struct md_update* update = &itr->second;
      
      ms::ms_update* ms_up = ms_updates->add_updates();

      ms_up->set_type( update->op );

      ms::ms_entry* ms_ent = ms_up->mutable_entry();

      md_entry_to_ms_entry( ms_ent, &update->ent );
      
      // if this is a RENAME, then add the 'dest' argument
      if( update->op == ms::ms_update::RENAME ) {
         ms::ms_entry* dest_ent = ms_up->mutable_dest();
         md_entry_to_ms_entry( dest_ent, &update->dest );
      }
   }

   ms_updates->set_signature( string("") );
   return 0;
}


// convert an update set to a string
ssize_t ms_client_update_set_to_string( ms::ms_updates* ms_updates, char** update_text ) {
   string update_bits;
   bool valid;

   try {
      valid = ms_updates->SerializeToString( &update_bits );
   }
   catch( exception e ) {
      errorf("%s", "failed to serialize update set\n");
      return -EINVAL;
   }

   if( !valid ) {
      errorf("%s", "failed ot serialize update set\n");
      return -EINVAL;
   }

   *update_text = CALLOC_LIST( char, update_bits.size() + 1 );
   memcpy( *update_text, update_bits.data(), update_bits.size() );
   return (ssize_t)update_bits.size();
}


// sign an update set
static int ms_client_sign_updates( EVP_PKEY* pkey, ms::ms_updates* ms_updates ) {
   return md_sign<ms::ms_updates>( pkey, ms_updates );
}


// post a record on the MS, synchronously.
// Get back a new file ID and/or coordinator ID, depending on the operation
static int ms_client_post( struct ms_client* client, uint64_t* file_id, uint64_t* coordinator_id, uint64_t volume_id, int op, struct md_entry* ent, struct md_entry* dest ) {
   
   // pack the first entry
   struct md_update up;
   memset( &up, 0, sizeof(up) );
   up.op = op;
   memcpy( &up.ent, ent, sizeof(struct md_entry) );

   update_set updates;
   
   // do we have a second entry?  We need one for rename
   if( op == ms::ms_update::RENAME ) {
      if( dest != NULL ) {
         memcpy( &up.dest, dest, sizeof(struct md_entry) );
      }
      else {
         return -EINVAL;
      }
   }
   
   updates[ ms_client_hash( ent->volume, ent->file_id ) ] = up;

   ms::ms_updates ms_updates;
   ms_client_serialize_update_set( &updates, &ms_updates );

   // sign it
   int rc = ms_client_sign_updates( client->my_key, &ms_updates );
   if( rc != 0 ) {
      errorf("ms_client_sign_updates rc = %d\n", rc );
      return rc;
   }

   // make it a string
   char* update_text = NULL;
   ssize_t update_text_len = ms_client_update_set_to_string( &ms_updates, &update_text );

   if( update_text_len < 0 ) {
      errorf("ms_client_update_set_to_string rc = %zd\n", update_text_len );
      return (int)update_text_len;
   }

   char* file_url = ms_client_file_url( client, volume_id );

   // do we need to verify the authenticity of the reply?  That is, will the reply contain 
   // information beyond a positive or negative ACK?
   bool need_verify = (op == ms::ms_update::CREATE || op == ms::ms_update::CHOWN);
   
   ms::ms_reply reply;
   
   // send it off
   rc = ms_client_send( client, file_url, update_text, update_text_len, &reply, need_verify );
   
   free( update_text );

   free( file_url );
   
   if( rc == 0 ) {
      // success
      
      // did the caller expect a file ID?
      if( file_id ) {
         if( reply.listing().entries_size() > 0 ) {
            *file_id = reply.listing().entries(0).file_id();
         }
         else {
            rc = -ENODATA;
         }
      }
      
      // did the caller expect a coordinator ID?
      if( coordinator_id ) {
         if( reply.listing().entries_size() > 0 ) {
            *coordinator_id = reply.listing().entries(0).coordinator();
         }
         else {
            rc = -ENODATA;
         }
      }
   }
      
   return rc;
}


uint64_t ms_client_make_file_id() {
   uint64_t lower = CMWC4096();
   uint64_t upper = CMWC4096();
   
   uint64_t ret = (upper) << 32 | lower;
   return ret;
}

// create a file record on the MS, synchronously
int ms_client_create( struct ms_client* client, uint64_t* file_id_ret, struct md_entry* ent ) {
   ent->type = MD_ENTRY_FILE;
   
   uint64_t file_id = ms_client_make_file_id();
   uint64_t old_file_id = ent->file_id;
   ent->file_id = file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", file_id );
   
   int rc = ms_client_post( client, &file_id, NULL, ent->volume, ms::ms_update::CREATE, ent, NULL );
   
   if( rc == 0 ) {
      *file_id_ret = file_id;
      dbprintf("output file_id: %" PRIX64 "\n", file_id );
   }

   ent->file_id = old_file_id;
   
   return rc;
}


// make a directory on the MS, synchronously
int ms_client_mkdir( struct ms_client* client, uint64_t* file_id_ret, struct md_entry* ent ) {   
   ent->type = MD_ENTRY_DIR;
   
   uint64_t file_id = ms_client_make_file_id();
   uint64_t old_file_id = ent->file_id;
   ent->file_id = file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", file_id );
   int rc = ms_client_post( client, &file_id, NULL, ent->volume, ms::ms_update::CREATE, ent, NULL );
   
   if( rc == 0 ) {
      *file_id_ret = file_id;
      dbprintf("output file_id: %" PRIX64 "\n", file_id );
   }

   ent->file_id = old_file_id;
   
   return rc;
}

// delete a record on the MS, synchronously
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, NULL, NULL, ent->volume, ms::ms_update::DELETE, ent, NULL );
}

// update a record on the MS, synchronously
int ms_client_update( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, NULL, NULL, ent->volume, ms::ms_update::UPDATE, ent, NULL );
}

// change coordinator ownership of a file on the MS, synchronously
int ms_client_coordinate( struct ms_client* client, uint64_t* new_coordinator, struct md_entry* ent ) {
   return ms_client_post( client, NULL, new_coordinator, ent->volume, ms::ms_update::CHOWN, ent, NULL );
}

// rename from src to dest 
int ms_client_rename( struct ms_client* client, struct md_entry* src, struct md_entry* dest ) {
   if( src->volume != dest->volume )
      return -EXDEV;
      
   return ms_client_post( client, NULL, NULL, src->volume, ms::ms_update::RENAME, src, dest );
}

// send a batch of updates.
// client must NOT be locked in any way.
static int ms_client_send_updates( struct ms_client* client, update_set* all_updates ) {

   int rc = 0;
   
   // don't do anything if we have nothing to do
   if( all_updates->size() == 0 ) {
      // nothing to do
      return 0;
   }

   // group updates by volume
   map< uint64_t, update_set > updates_by_volume;

   for( update_set::iterator itr = all_updates->begin(); itr != all_updates->end(); itr++ ) {
      updates_by_volume[ itr->second.ent.volume ][ itr->first ] = itr->second;
   }

   // send by Volume
   // TODO: do this in parallel?
   for( map< uint64_t, update_set >::iterator itr = updates_by_volume.begin(); itr != updates_by_volume.end(); itr++ ) {
      update_set* updates = &itr->second;
      
      // pack the updates into a protobuf
      ms::ms_updates ms_updates;
      ms_client_serialize_update_set( updates, &ms_updates );

      // sign it
      rc = ms_client_sign_updates( client->my_key, &ms_updates );
      if( rc != 0 ) {
         errorf("ms_client_sign_updates rc = %d\n", rc );
         return rc;
      }

      // make it a string
      char* update_text = NULL;
      ssize_t update_text_len = ms_client_update_set_to_string( &ms_updates, &update_text );

      if( update_text_len < 0 ) {
         errorf("ms_client_update_set_to_string rc = %zd\n", update_text_len );
         return (int)update_text_len;
      }

      // which Volumes are we sending off to?
      char* file_url = ms_client_file_url( client, itr->first );

      // send it off
      ms::ms_reply reply;
      rc = ms_client_send( client, file_url, update_text, update_text_len, &reply, false );

      free( update_text );

      if( rc != 0 ) {
         errorf("ms_client_send(%s) rc = %d\n", file_url, rc );
         free( file_url );
         return rc;
      }

      free( file_url );
   }
   
   return rc;
}


// post a pending update to the MS for a specific file, removing it from the update queue
// return 0 on success
// return >0 if there was a CURL error (return value is the CURL error code)
// return -EREMOTEIO if the MS's response could not be interpreted
// return <-100 if there was an unexpected HTTP status code
// return -ENOENT if there is no pending update 
int ms_client_sync_update( struct ms_client* client, uint64_t volume_id, uint64_t file_id ) {

   struct md_update update;
   int rc = 0;
   
   long path_hash = ms_client_hash( volume_id, file_id );
   uint64_t old_deadline = 0;

   ms_client_wlock( client );
   
   if( client->updates->count( path_hash ) != 0 ) {
      // clear it out
      rc = ms_client_remove_update( client, path_hash, &update, &old_deadline );  
   }
   else {
      ms_client_unlock( client );
      return 0;
   }

   ms_client_unlock( client );

   if( rc != 0 ) {
      // error
      return rc;
   }

   update_set updates;
   memcpy( &(updates[ path_hash ]), &update, sizeof(struct md_update) );
   
   rc = ms_client_send_updates( client, &updates );

   // if we failed, then put the updates back and try again later
   if( rc != 0 ) {
      errorf("ms_client_send_updates(/%" PRIu64 "/%" PRIu64 "/%" PRIX64 ") rc = %d\n", volume_id, client->gateway_id, file_id, rc);
   }

   md_update_free( &update );
   
   return rc;
}


// post all updates to the MS that are older $freshness_ms milliseconds old
// return 0 on success.
// returns >0 if there was a CURL error (return value is the CURL error code)
// returns -EREMOTEIO if the MS's response could not be interpreted
// returns <-100 if there was an unexpected HTTP status code (return value is negative HTTP status code)
int ms_client_sync_updates( struct ms_client* client, uint64_t freshness_ms ) {
   int rc = 0;

   update_set updates;
   map<long, uint64_t> old_deadlines;

   // get all the updates that are older than $freshness milliseconds from the present
   ms_client_wlock( client );

   uint64_t deadline_ms = currentTimeMillis() - freshness_ms;
   for( deadline_queue::iterator itr = client->deadlines->begin(); itr != client->deadlines->end(); ) {
      uint64_t deadline = itr->first;
      if( deadline > deadline_ms ) {
         // all deadlines after this are in the future
         break;
      }

      // get the associated update
      long path_hash = itr->second;

      struct md_update old_up;
      memset( &old_up, 0, sizeof(old_up) );
      
      if( client->updates->count( path_hash ) != 0 ) {
         memcpy( &old_up, &(*client->updates)[path_hash], sizeof(struct md_update) );
         
         updates[path_hash] = old_up;
         old_deadlines[path_hash] = deadline;
      }

      deadline_queue::iterator old_itr = itr;

      itr++;
      
      client->updates->erase( path_hash );
      client->deadlines->erase( old_itr );
   }

   // done with the client for now
   ms_client_unlock( client );

   if( updates.size() > 0 ) {
      rc = ms_client_send_updates( client, &updates );
   }

   // if we failed, then put the updates back and try again later
   if( rc != 0 ) {
      ms_client_wlock( client );
      
      for( update_set::iterator itr = updates.begin(); itr != updates.end(); itr++ ) {
         int put_rc = ms_client_put_update( client->updates, client->deadlines, itr->first, &itr->second, old_deadlines[itr->first] );
         if( put_rc == -EEXIST ) {
            // this update can be freed, since a later write overtook it
            md_update_free( &itr->second );
         }
      }

      ms_client_unlock( client );
   }
         
   // if we succeeded, free the posted updates (they're no longer present in the client context)
   else {
      for( update_set::iterator itr = updates.begin(); itr != updates.end(); itr++ ) {
         md_update_free( &itr->second );
      }
   }
   
   return rc;
}


// parse an MS reply
int ms_client_parse_reply( struct ms_client* client, ms::ms_reply* src, char const* buf, size_t buf_len, bool verify ) {

   ms_client_view_rlock( client );
   
   int rc = md_parse< ms::ms_reply >( src, buf, buf_len );
   if( rc != 0 ) {
      ms_client_view_unlock( client );
      
      errorf("md_parse ms_reply failed, rc = %d\n", rc );
      
      return rc;
   }
   
   if( verify ) {
      // verify integrity and authenticity
      rc = md_verify< ms::ms_reply >( client->volume->volume_public_key, src );
      if( rc != 0 ) {
         
         ms_client_view_unlock( client );
         
         errorf("md_verify ms_reply failed, rc = %d\n", rc );
         
         return rc;
      }
   }
   
   ms_client_view_unlock( client );
   
   return 0;
}
   
// parse an MS listing
int ms_client_parse_listing( struct ms_listing* dst, ms::ms_reply* reply ) {
   
   const ms::ms_listing& src = reply->listing();
   
   memset( dst, 0, sizeof(struct ms_listing) );
   
   if( src.status() != ms::ms_listing::NONE ) {
      dst->status = (src.status() == ms::ms_listing::NEW ? MS_LISTING_NEW : MS_LISTING_NOCHANGE);
   }
   else {
      dst->status = MS_LISTING_NONE;
   }

   if( dst->status == MS_LISTING_NEW ) {
      dst->type = src.ftype();
      dst->entries = new vector<struct md_entry>();
      
      for( int i = 0; i < src.entries_size(); i++ ) {
         struct md_entry ent;
         ms_entry_to_md_entry( src.entries(i), &ent );

         dst->entries->push_back( ent );
      }
   }

   return 0;
}


// free an MS listing
void ms_client_free_listing( struct ms_listing* listing ) {
   if( listing->entries ) {
      for( unsigned int i = 0; i < listing->entries->size(); i++ ) {
         md_entry_free( &listing->entries->at(i) );
      }

      delete listing->entries;
      listing->entries = NULL;
   }
}


// free an MS response
void ms_client_free_response( ms_response_t* ms_response ) {
   for( ms_response_t::iterator itr = ms_response->begin(); itr != ms_response->end(); itr++ ) {
      ms_client_free_listing( &itr->second );
   }
}


// initialize a download context
int ms_client_init_download( struct ms_client* client, struct ms_download_context* download, uint64_t volume_id, uint64_t file_id, int64_t file_version, int64_t write_nonce ) {
   download->url = ms_client_file_url( client, volume_id, file_id, file_version, write_nonce );
   download->curl = curl_easy_init();
   download->rb = new response_buffer_t();

   md_init_curl_handle( client->conf, download->curl, download->url, client->conf->connect_timeout );
   
   curl_easy_setopt( download->curl, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( download->curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );
   curl_easy_setopt( download->curl, CURLOPT_WRITEDATA, download->rb );

   return 0;
}


// free a download context
void ms_client_free_download( struct ms_download_context* download ) {
   if( download->url ) {
      free( download->url );
      download->url = NULL;
   }
   
   if( download->rb ) {
      response_buffer_free( download->rb );
      delete download->rb;
      download->rb = NULL;
   }

   if( download->curl ) {
      curl_easy_cleanup( download->curl );
      download->curl = NULL;
   }
}


// build a path ent
int ms_client_make_path_ent( struct ms_path_ent* path_ent, uint64_t volume_id, uint64_t file_id, int64_t version, int64_t write_nonce, char const* name, void* cls ) {
   // build up the ms_path as we traverse our cached path
   path_ent->volume_id = volume_id;
   path_ent->file_id = file_id;
   path_ent->version = version;
   path_ent->write_nonce = write_nonce;
   path_ent->name = strdup( name );
   path_ent->cls = cls;
   return 0;
}

// free a path entry
void ms_client_free_path_ent( struct ms_path_ent* path_ent, void (*free_cls)( void* ) ) {
   if( path_ent->name ) {
      free( path_ent->name );
      path_ent->name = NULL;
   }
   if( path_ent->cls && free_cls ) {
      (*free_cls)( path_ent->cls );
      path_ent->cls = NULL;
   }

   memset( path_ent, 0, sizeof(struct ms_path_ent) );
}

// free a path
void ms_client_free_path( path_t* path, void (*free_cls)(void*) ) {
   for( unsigned int i = 0; i < path->size(); i++ ) {
      ms_client_free_path_ent( &path->at(i), free_cls );
   }
}

// download many things at once
int ms_client_perform_multi_download( struct ms_client* client, struct ms_download_context* downloads, unsigned int num_downloads ) {
   int still_running = 0;
   CURLMsg* msg = NULL;
   int msgs_left = 0;
   int rc = 0;

   CURLM* curl_multi = curl_multi_init();
   
   // populate the curl handle
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      curl_multi_add_handle( curl_multi, downloads[i].curl );
   }

   // start the action
   curl_multi_perform( curl_multi, &still_running );
   
   do {
      struct timeval timeout;

      fd_set fdread;
      fd_set fdwrite;
      fd_set fdexcep;
      
      FD_ZERO( &fdread );
      FD_ZERO( &fdwrite );
      FD_ZERO( &fdexcep );

      int maxfd = -1;

      long curl_timeo = -1;

      // check back at most once a second
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;

      rc = curl_multi_timeout( curl_multi, &curl_timeo );
      if( rc != 0 ) {
         errorf("curl_multi_timeout rc = %d\n", rc);
         rc = -ENODATA;
         break;
      }
      
      if( curl_timeo >= 0 ) {
         timeout.tv_sec = curl_timeo / 1000;
         if( timeout.tv_sec > 1 ) {
            timeout.tv_sec = 1;
         }
         else {
            timeout.tv_usec = (curl_timeo % 1000) * 1000;
         }
      }

      // get fd set
      rc = curl_multi_fdset( curl_multi, &fdread, &fdwrite, &fdexcep, &maxfd );
      if( rc != 0 ) {
         errorf("curl_multi_fdset rc = %d\n", rc );
         rc = -ENODATA;
         break;
      }

      rc = select( maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout );
      if( rc < 0 ) {
         rc = -errno;
         errorf("select errno = %d\n", rc );
         rc = -ENODATA;
         break;
      }
      
      rc = 0;

      curl_multi_perform( curl_multi, &still_running );
      
   } while( still_running > 0 );
   
   if( rc == 0 ) {
      // how did the transfers go?
      do {
         msg = curl_multi_info_read( curl_multi, &msgs_left );

         if( msg == NULL )
            break;

         if( msg->msg == CURLMSG_DONE ) {
            // status of this handle...
            bool found = false;
            unsigned int i = 0;
            
            for( i = 0; i < num_downloads; i++ ) {
               if( msg->easy_handle == downloads[i].curl ) {
                  found = true;
                  break;
               }
            }

            if( found ) {
               // check status
               if( msg->data.result != 0 ) {
                  // curl error
                  errorf("Download %s rc = %d\n", downloads[i].url, msg->data.result);
                  rc = -ENODATA;
               }
               
               // check HTTP code
               long http_status = 0;
               int crc = curl_easy_getinfo( downloads[i].curl, CURLINFO_RESPONSE_CODE, &http_status );
               if( crc != 0 ) {
                  errorf("curl_easy_getinfo rc = %d\n", rc );
                  rc = -ENODATA;
               }
               
               if( http_status != 200 ) {
                  errorf("MS HTTP response code %ld for %s\n", http_status, downloads[i].url );
                  if( http_status == 404 ) {
                     rc = -ENOENT;
                  }
                  else if( http_status == 403 ) {
                     rc = -EACCES;
                  }
                  else {
                     rc = -EREMOTEIO;
                     
                  }
               }
            }
         }
      } while( msg != NULL );
   }
   
   // clean up
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      curl_multi_remove_handle( curl_multi, downloads[i].curl );
   }

   curl_multi_cleanup( curl_multi );

   return rc;
}


// get a set of metadata entries.
// on succes, populate ms_response with ms_listing structures for each path entry that needed to be downloaded, as indicated by the stale flag.
int ms_client_get_listings( struct ms_client* client, path_t* path, ms_response_t* ms_response ) {

   unsigned int num_downloads = path->size();

   if( num_downloads == 0 ) {
      // everything's fresh
      return 0;
   }
   
   // fetch concurrently--set up downloads
   struct ms_download_context* path_downloads = CALLOC_LIST( struct ms_download_context, num_downloads );
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      struct ms_path_ent* path_ent = &path->at(i);
      ms_client_init_download( client, &path_downloads[i], path_ent->volume_id, path_ent->file_id, path_ent->version, path_ent->write_nonce );
   }

   struct timespec ts, ts2;
   
   BEGIN_TIMING_DATA( ts );
   
   int rc = ms_client_perform_multi_download( client, path_downloads, num_downloads );

   END_TIMING_DATA( ts, ts2, "MS recv" );
   
   if( rc != 0 ) {
      errorf("ms_client_perform_multi_download rc = %d\n", rc );

      // free memory
      for( unsigned int i = 0; i < num_downloads; i++ ) {
         ms_client_free_download( &path_downloads[i] );
      }
      free( path_downloads );
      
      return rc;
   }

   // got data! parse it
   unsigned int di = 0;
   for( unsigned int i = 0; i < path->size(); i++ ) {
      
      char* buf = response_buffer_to_string( path_downloads[di].rb );
      size_t buf_len = response_buffer_size( path_downloads[di].rb );
      
      // parse and verify
      ms::ms_reply reply;
      rc = ms_client_parse_reply( client, &reply, buf, buf_len, true );
      if( rc != 0 ) {
         errorf("ms_client_parse_reply(%s) rc = %d\n", path_downloads[di].url, rc );
         rc = -EIO;
         ms_client_free_response( ms_response );
         free( buf );
         break;
      }
      
      // check errors
      int err = reply.error();
      if( err != 0 ) {
         errorf("MS reply error %d from %s\n", err, path_downloads[di].url );
         rc = err;
         ms_client_free_response( ms_response );
         free( buf );
         break;
      }
      
      // extract versioning information from the reply
      uint64_t volume_id = ms_client_get_volume_id( client );
      ms_client_process_header( client, volume_id, reply.volume_version(), reply.cert_version() );
      
      // get the listing
      struct ms_listing listing;
      
      rc = ms_client_parse_listing( &listing, &reply );
      
      free( buf );
      
      if( rc != 0 ) {
         errorf("ms_client_parse_listing(%s) rc = %d\n", path_downloads[di].url, rc );
         rc = -EIO;
         ms_client_free_response( ms_response );
         break;
      }
      
      // save
      (*ms_response)[ path->at(i).file_id ] = listing;
      di++;
   }

   // free memory
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      ms_client_free_download( &path_downloads[i] );
   }
   free( path_downloads );

   return rc;
}


// get a list of RG ids
uint64_t* ms_client_RG_ids( struct ms_client* client ) {
   ms_client_view_rlock( client );
   
   uint64_t* ret = CALLOC_LIST( uint64_t, client->volume->RG_certs->size() + 1 );
   int i = 0;
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      ret[i] = rg_cert->gateway_id;
      
      i++;
   }
   
   ms_client_view_unlock( client );
   return ret;
}


// get a copy of the RG URLs
char** ms_client_RG_urls( struct ms_client* client, char const* scheme ) {
   ms_client_view_rlock( client );

   char** urls = CALLOC_LIST( char*, client->volume->RG_certs->size() + 1 );
   int i = 0;
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      urls[i] = CALLOC_LIST( char, strlen(scheme) + strlen(rg_cert->hostname) + 1 + 7 + 1 + strlen(SYNDICATE_DATA_PREFIX) + 2 );
      sprintf( urls[i], "%s%s:%d/%s/", scheme, rg_cert->hostname, rg_cert->portnum, SYNDICATE_DATA_PREFIX );
      
      i++;
   }

   ms_client_view_unlock( client );

   return urls;
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

// get the blocking factor
uint64_t ms_client_get_volume_blocksize( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t ret = client->volume->blocksize;

   ms_client_view_unlock( client );
   return ret;
}

// get the type of gateway
int ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id ) {
   ms_client_view_rlock( client );
   
   int ret = -ENOENT;
   
   if( client->volume->UG_certs->count( g_id ) != 0 )
      ret = SYNDICATE_UG;
   
   else if( client->volume->RG_certs->count( g_id ) != 0 )
      ret = SYNDICATE_RG;
   
   else if( client->volume->AG_certs->count( g_id ) != 0 ) 
      ret = SYNDICATE_AG;
   
   ms_client_view_unlock( client );
   return ret;
}

// is this ID an AG ID?
bool ms_client_is_AG( struct ms_client* client, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   bool ret = false;
   
   if( client->volume->AG_certs->count( ag_id ) != 0 )
      ret = true;

   ms_client_view_unlock( client );

   return ret;
}

// get an AG's block size
uint64_t ms_client_get_AG_blocksize( struct ms_client* client, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   uint64_t ret = 0;
   
   ms_cert_bundle::iterator itr = client->volume->AG_certs->find( ag_id );
   if( itr != client->volume->AG_certs->end() ) {
      ret = itr->second->blocksize;
   }
   
   ms_client_view_unlock( client );

   if( ret == 0 ) {
      errorf("No such AG %" PRIu64 "\n", ag_id );
   }

   return ret;
}

char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->AG_certs->find( ag_id );
   if( itr != client->volume->AG_certs->end() ) {
      ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
      sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   }

   ms_client_view_unlock( client );

   if( ret == NULL ) {
      errorf("No such AG %" PRIu64 "\n", ag_id );
   }

   return ret;
}


char* ms_client_get_RG_content_url( struct ms_client* client, uint64_t rg_id ) {
   ms_client_view_rlock( client );

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->RG_certs->find( rg_id );
   if( itr != client->volume->RG_certs->end() ) {
      ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
      sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   }

   ms_client_view_unlock( client );

   if( ret == NULL ) {
      errorf("No such RG %" PRIu64 "\n", rg_id );
   }

   return ret;
}

uint64_t ms_client_get_num_files( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t num_files = client->volume->num_files;

   ms_client_view_unlock( client );

   return num_files;
}


// get a UG url
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t gateway_id ) {
   ms_client_view_rlock( client );

   // is this us?
   if( gateway_id == client->gateway_id ) {
      char* ret = strdup( client->conf->content_url );
      ms_client_view_unlock( client );
      return ret;
   }

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->UG_certs->find( gateway_id );
   if( itr == client->volume->UG_certs->end() ) {
      errorf("No such Gateway %" PRIu64 "\n", gateway_id );
      ms_client_view_unlock( client );
      return NULL;
   }
   
   ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
   sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   
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


// check a gateway's capabilities (as a bit mask)
// return 0 if all the capabilites are allowed, or -EPERM if at least one is not.
// return -EAGAIN if the gateway is not known.
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps ) {
   
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      client->early_reload = true;      // writing this is okay, since regardless of thread scheduling the view thread will read it eventually
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   int ret = ((cert->caps & caps) == caps ? 0 : -EPERM);
   
   ms_client_view_unlock( client );
   
   return ret;
}


// get a gateway's user 
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id ) {
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      client->early_reload = true;      // writing this is okay, since regardless of thread scheduling the view thread will read it eventually
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   *user_id = cert->user_id;
   
   ms_client_view_unlock( client );
   
   return 0;
}


// get a gateway's volume
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id ) {
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      client->early_reload = true;      // writing this is okay, since regardless of thread scheduling the view thread will read it eventually
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   *user_id = cert->volume_id;
   
   ms_client_view_unlock( client );
   
   return 0;
}


// get a copy of the closure text for this gateway
int ms_client_get_closure_text( struct ms_client* client, char** closure_text, uint64_t* closure_len ) {
   // find my cert
   ms_client_view_rlock( client );
   
   struct ms_volume* vol = client->volume;
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ client->gateway_type ]->find( client->gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // something's seriously wrong here...
      ms_client_view_unlock( client );
      return -ENOTCONN;
   }
   
   struct ms_gateway_cert* my_cert = itr->second;
   
   int ret = 0;
   
   if( my_cert->closure_text != NULL ) {
      *closure_text = CALLOC_LIST( char, my_cert->closure_text_len );
      memcpy( *closure_text, my_cert->closure_text, my_cert->closure_text_len );
      *closure_len = my_cert->closure_text_len;
   }
   else {
      ret = -ENOENT;
   }
   
   ms_client_view_unlock( client );
   
   return ret;
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
   

// schedule a Volume reload
int ms_client_sched_volume_reload( struct ms_client* client ) {
   int rc = 0;
   
   ms_client_view_wlock( client );

   client->early_reload = true;
   
   ms_client_view_unlock( client );
   return rc;
}


// extract versioning information from the reply
int ms_client_process_header( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version ) {
   int rc = 0;
   
   ms_client_view_rlock( client );
   
   if( client->volume->volume_id != volume_id )
      return -EINVAL;
   
   if( client->volume->volume_version < volume_version )
      client->early_reload = true;
   
   if( client->volume->volume_cert_version < cert_version )
      client->early_reload = true;
   
   ms_client_view_unlock( client );
   return rc;
}

