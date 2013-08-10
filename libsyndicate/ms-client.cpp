/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "ms-client.h"

static void* ms_client_uploader_thread( void* arg );
static void* ms_client_view_thread( void* arg );
static void ms_client_uploader_signal( struct ms_client* client );
static size_t ms_client_header_func( void *ptr, size_t size, size_t nmemb, void *userdata);

static void print_timings( uint64_t* timings, size_t num_timings, char const* hdr ) {
   if( num_timings > 0 ) {
      for( size_t i = 0; i < num_timings; i++ ) {
         DATA( hdr, (double)(timings[i]) / 1e9 );
      }
   }
}


static void UG_cred_free( struct UG_cred* cred ) {
   if( cred->hostname ) {
      free( cred->hostname );
      cred->hostname = NULL;
   }

   if( cred->name ) {
      free( cred->name );
      cred->name = NULL;
   }

   if( cred->pubkey ) {
      EVP_PKEY_free( cred->pubkey );
      cred->pubkey = NULL;
   }
}

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
      

// create an MS client context
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf ) {

   memset( client, 0, sizeof(struct ms_client) );

   client->gateway_type = gateway_type;
   
   // configure the HTTPS streams to the MS
   client->ms_read = curl_easy_init();
   client->ms_write = curl_easy_init();
   client->ms_view = curl_easy_init();

   client->url = strdup( conf->metadata_url );

   // will change URL once we know the Volume ID
   md_init_curl_handle( client->ms_read, "https://localhost", conf->metadata_connect_timeout);
   md_init_curl_handle( client->ms_write, "https://localhost", conf->metadata_connect_timeout);
   md_init_curl_handle( client->ms_view, "https://localhost", conf->metadata_connect_timeout);

   curl_easy_setopt( client->ms_write, CURLOPT_POST, 1L);
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );

   curl_easy_setopt( client->ms_read, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( client->ms_write, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( client->ms_view, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );

   curl_easy_setopt( client->ms_read, CURLOPT_HEADERFUNCTION, ms_client_header_func );
   curl_easy_setopt( client->ms_write, CURLOPT_HEADERFUNCTION, ms_client_header_func );

   curl_easy_setopt( client->ms_read, CURLOPT_WRITEHEADER, &client->read_times );
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEHEADER, &client->write_times );
   
   curl_easy_setopt( client->ms_read, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( client->ms_write, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( client->ms_view, CURLOPT_NOSIGNAL, 1L );

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
   client->running = true;
   pthread_mutex_init( &client->uploader_lock, NULL );
   pthread_cond_init( &client->uploader_cv, NULL );

   int rc = 0;

   client->registered = false;

   // if we were given a Volume public key in advance, load it
   if( conf->volume_public_key != NULL ) {
      rc = ms_client_load_volume_pubkey( client, conf->volume_public_key );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_pubkey rc = %d\n", rc );
         return rc;
      }

      rc = ms_client_verify_key( client->volume_public_key );
      if( rc != 0 ) {
         errorf("ms_client_verify_key rc = %d\n", rc );
         return rc;
      }
      
      client->reload_volume_key = false;
   }

   if( conf->gateway_key != NULL ) {
      // we were given Gateway keys.  Load them
      rc = ms_client_load_privkey( &client->my_key, conf->gateway_key );
      if( rc != 0 ) {
         errorf("ms_client_load_privkey rc = %d\n", rc );
         return rc;
      }

      rc = ms_client_verify_key( client->my_key );
      if( rc != 0 ) {
         errorf("ms_client_verify_key rc = %d\n", rc );
         return rc;
      }
   }
   else {
      // generate our public/private key pairs
      rc = ms_client_generate_key( &client->my_key );
      if( rc != 0 ) {
         errorf("ms_client_generate_key rc = %d\n", rc );
         ms_client_unlock( client );
         return rc;
      }
   }


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

   // shut down the uploader thread
   client->running = false;

   ms_client_uploader_signal( client );

   dbprintf("%s", "wait for write uploads to finish...\n");
   
   while( client->uploader_running ) {
      sleep(1);
   }

   dbprintf("%s", "wait for view change thread to finish...\n");

   pthread_cancel( client->view_thread);

   while( client->view_thread_running ) {
      sleep(1);
   }

   pthread_join( client->uploader_thread, NULL );
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

   if( client->RG_urls != NULL ) {
      for( int i = 0; client->RG_urls[i] != NULL; i++ ) {
         free( client->RG_urls[i] );
         client->RG_urls[i] = NULL;
      }
      free( client->RG_urls );
   }

   if( client->UG_creds != NULL ) {
      for( int i = 0; client->UG_creds[i] != NULL; i++ ) {
         UG_cred_free( client->UG_creds[i] );
         free( client->UG_creds[i] );
      }
      free( client->UG_creds );
   }
   
   pthread_rwlock_unlock( &client->view_lock );
   pthread_rwlock_destroy( &client->view_lock );
   
   // clean up our state
   if( client->userpass )
      free( client->userpass );

   for( update_set::iterator itr = client->updates->begin(); itr != client->updates->end(); itr++ ) {
      md_update_free( &itr->second );
      memset( &itr->second, 0, sizeof(struct md_update) );
   }

   client->deadlines->clear();
   client->updates->clear();

   if( client->url )
      free( client->url );

   if( client->view_url )
      free( client->view_url );

   if( client->file_url )
      free( client->file_url );

   if( client->session_password )
      free( client->session_password );

   if( client->volume_public_key )
      EVP_PKEY_free( client->volume_public_key );

   if( client->my_key )
      EVP_PKEY_free( client->my_key );
      
   delete client->updates;
   delete client->deadlines;
   
   ms_client_unlock( client );
   pthread_rwlock_destroy( &client->lock );

   // free OpenSSL memory
   ERR_free_strings();
   
   return 0;
}


char* ms_client_url( struct ms_client* client, char const* metadata_path ) {
   char volume_id[50];
   sprintf(volume_id, "%" PRIu64, client->volume_id);

   char* volume_md_path = md_fullpath( metadata_path, volume_id, NULL );

   ms_client_rlock( client );
   char* url = md_fullpath( client->url, volume_md_path, NULL );
   ms_client_unlock( client );

   free( volume_md_path );

   return url;
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


// view thread body, for synchronizing Volume metadata (including UG and RG lists)
static void* ms_client_view_thread( void* arg ) {
   struct ms_client* client = (struct ms_client*)arg;
   
   // how often do we reload?
   uint64_t view_reload_freq = client->conf->view_reload_freq;
   struct timespec sleep_time;
   
   sleep_time.tv_sec = view_reload_freq / 1000;
   sleep_time.tv_nsec = (view_reload_freq % 1000) * 1000;

   struct timespec remaining;

   client->view_thread_running = true;

   // since we don't hold any resources between downloads, simply cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   dbprintf("%s", "View thread starting up\n");
   
   while( client->running ) {

      uint64_t now_ms = currentTimeMillis();
      uint64_t wakeup_ms = now_ms + view_reload_freq * 1000;
      
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
         bool early_reload = client->early_reload;
         client->early_reload = false;
         ms_client_view_unlock( client );

         if( early_reload ) {
            break;
         }
      }

      if( !client->running )
         break;

      if( !client->registered )
         continue;      // nothing we can do

      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      // reload Volume metadata
      dbprintf("%s", "Begin reload Volume metadata\n");
      
      int rc = ms_client_get_volume_metadata_curl( client, client->ms_view );
      
      dbprintf("End reload Volume metadata, rc = %d\n", rc);

      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
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

      if( !client->registered ) {
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


int ms_client_begin_uploading( struct ms_client* client, response_buffer_t* rb, struct curl_httppost* forms ) {
   // lock, but back off if someone else is uploading
   ms_client_wlock_backoff( client, &client->uploading );

   client->uploading = true;

   curl_easy_setopt( client->ms_write, CURLOPT_URL, client->file_url );
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

char* ms_client_register_url( struct ms_client* client ) {
   // build the /REGISTER/ url

   char gateway_type_str[10];
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );
   
   ms_client_rlock( client );

   char* url = CALLOC_LIST( char, strlen(client->url) + 1 +
                                  strlen("/REGISTER/") + 1 +
                                  strlen(client->conf->volume_name) + 1 +
                                  strlen(client->conf->gateway_name) + 1 +
                                  strlen(client->conf->ms_username) + 1 +
                                  strlen(gateway_type_str) + 1 +
                                  strlen("/begin") + 1);

   sprintf(url, "%s/REGISTER/%s/%s/%s/%s/begin", client->url, client->conf->volume_name, gateway_type_str, client->conf->gateway_name, client->conf->ms_username );
   
   ms_client_unlock( client );

   return url;
}


// download metadata from the MS for a Volume.
// metadata_path should be an absolute directory path (like /VOLUME/, or /UG/, or /RG/)
// returns the HTTP response on success, or negative on error
int ms_client_download_volume_metadata( struct ms_client* client, char const* url, char** buf, ssize_t* buflen ) {
   
   char* bits = NULL;
   ssize_t len = 0;
   int http_response = 0;

   ms_client_begin_downloading( client, url, NULL );

   // do the download
   memset( &client->read_times, 0, sizeof(client->read_times) );
   len = md_download_file5( client->ms_read, &bits );

   http_response = ms_client_end_downloading( client );

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


// get one or all UG records.
// if gateway_id == 0, then get all records.
// otherwise, get the specific record.
int ms_client_get_UGs( struct ms_client* client, uint64_t gateway_id, struct UG_cred*** ug_listing, uint64_t* ug_version ) {
   ms::ms_volume_UGs volume_ugs;

   char* bits = NULL;
   ssize_t len = 0;
   char* UG_url = NULL;

   if( gateway_id == 0 ) {
      UG_url = ms_client_url( client, "/UG/" );
   }
   else {
      char* tmp = ms_client_url( client, "/UG/" );
      char buf[50];
      sprintf(buf, "%" PRIu64, gateway_id );
      UG_url = md_fullpath( tmp, buf, NULL );
   }

   int rc = ms_client_download_volume_metadata( client, UG_url, &bits, &len );

   if( rc < 0 ) {
      errorf("ms_client_download_volume_metadata( %s ) rc = %d\n", UG_url, rc );
      free( UG_url );
      return rc;
   }

   free( UG_url );

   // got the data
   bool valid = volume_ugs.ParseFromString( string(bits, len) );
   free( bits );

   if( !valid ) {
      errorf("%s", "invalid UG metadata\n" );
      return -EINVAL;
   }

   // verify the data
   rc = ms_client_verify_UGs( client, &volume_ugs );
   if( rc != 0 ) {
      errorf("ms_client_verify_UGs rc = %d\n", rc );
      return rc;
   }

   struct UG_cred** ugs = CALLOC_LIST( struct UG_cred*, volume_ugs.ug_creds_size() + 1 );
   for( int i = 0; i < volume_ugs.ug_creds_size(); i++ ) {
      struct UG_cred* uent = CALLOC_LIST( struct UG_cred, 1 );

      const ms::ms_volume_gateway_cred& ug_cred = volume_ugs.ug_creds(i);

      ms_client_load_cred( uent, &ug_cred );

      ugs[i] = uent;

      dbprintf("UG: id = %" PRIu64 ", owner = %" PRIu64 ", name = %s\n", uent->gateway_id, uent->user_id, uent->name );
   }

   *ug_listing = ugs;
   *ug_version = volume_ugs.ug_version();
   return 0;
}


// get UG metadata
int ms_client_reload_UGs( struct ms_client* client ) {

   struct UG_cred** ugs = NULL;
   uint64_t ug_version = 0;
   int rc = 0;

   rc = ms_client_get_UGs( client, 0, &ugs, &ug_version );
   if( rc != 0 ) {
      errorf("ms_client_get_UGs rc = %d\n", rc);
      return rc;
   }
   
   ms_client_view_wlock( client );

   struct UG_cred** old_users = client->UG_creds;
   client->UG_creds = ugs;
   client->UG_version = ug_version;

   ms_client_view_unlock( client );

   if( old_users ) {
      for( int i = 0; old_users[i] != NULL; i++ ) {
         UG_cred_free( old_users[i] );
         free( old_users[i] );
      }
      free( old_users );
   }

   return 0;
}


// get RG metadata
int ms_client_reload_RGs( struct ms_client* client ) {
   ms::ms_volume_RGs volume_rgs;

   char* bits = NULL;
   ssize_t len = 0;

   char* RG_url = ms_client_url( client, "/RG/" );

   int rc = ms_client_download_volume_metadata( client, RG_url, &bits, &len );

   free( RG_url );
   
   if( rc < 0 ) {
      errorf("ms_client_download_volume_metadata( /RG/ ) rc = %d\n", rc );
      return rc;
   }

   // got the data
   bool valid = volume_rgs.ParseFromString( string(bits, len) );
   free( bits );

   if( valid ) {

      // verify the data
      rc = ms_client_verify_RGs( client, &volume_rgs );
      if( rc != 0 ) {
         errorf("ms_client_verify_RGs rc = %d\n", rc );
         return rc;
      }

      // sanity check--the number of hosts and ports must be the same
      if( volume_rgs.rg_hosts_size() != volume_rgs.rg_ports_size() )
         valid = false;

      // sanity check---the ports must be between 1 and 65535
      for( int i = 0; i < volume_rgs.rg_ports_size(); i++ ) {
         if( volume_rgs.rg_ports(i) < 1 || volume_rgs.rg_ports(i) > 65534 ) {
            valid = false;
            break;
         }
      }
   }

   if( !valid ) {
      errorf("%s", "invalid RG metadata from %s\n" );
      return -EINVAL;
   }

   char** rg_urls = CALLOC_LIST( char*, volume_rgs.rg_hosts_size() + 1 );
   for( int i = 0; i < volume_rgs.rg_hosts_size(); i++ ) {
      char* tmp = md_prepend( "https://", volume_rgs.rg_hosts(i).c_str(), NULL );
      
      char portbuf[20];
      sprintf(portbuf, ":%d", volume_rgs.rg_ports(i) );
              
      char* url = md_prepend( tmp, portbuf, NULL );
      rg_urls[i] = url;

      dbprintf("RG: %s\n", url );
      
      free( tmp );
   }

   uint64_t RG_version = volume_rgs.rg_version();
   
   ms_client_view_wlock( client );

   char** old_rgs = client->RG_urls;
   client->RG_urls = rg_urls;
   client->RG_version = RG_version;

   ms_client_view_unlock( client );

   if( old_rgs ) {
      for( int i = 0; old_rgs[i] != NULL; i++ ) {
         free( old_rgs[i] );
      }
      free( old_rgs );
   }

   return 0;
}


// verify the signature of the Volume metadata.
// client must be read-locked
int ms_client_verify_volume_metadata( struct ms_client* client, ms::ms_volume_metadata* volume_md ) {
   // get the signature
   size_t sigb64_len = volume_md->signature().size();
   char* sigb64 = CALLOC_LIST( char, sigb64_len + 1 );
   memcpy( sigb64, volume_md->signature().data(), sigb64_len );
   
   volume_md->set_signature( "" );

   string volume_md_data;
   try {
      volume_md->SerializeToString( &volume_md_data );
   }
   catch( exception e ) {
      return -EINVAL;
   }

   // verify the signature
   int rc = md_verify_signature( client->volume_public_key, volume_md_data.data(), volume_md_data.size(), sigb64, sigb64_len );
   free( sigb64 );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}

// verify the signature of the UG metadata
// client must be read-locked
int ms_client_verify_UGs( struct ms_client* client, ms::ms_volume_UGs* ugs ) {
   // get the signature
   size_t sig_len = ugs->signature().size();
   char* sig = CALLOC_LIST( char, sig_len );
   memcpy( sig, ugs->signature().data(), sig_len );

   ugs->set_signature( "" );

   string ug_md_data;
   try {
      ugs->SerializeToString( &ug_md_data );
   }
   catch( exception e ) {
      free( sig );
      return -EINVAL;
   }

   // verify the signature
   int rc = md_verify_signature( client->volume_public_key, ug_md_data.data(), ug_md_data.size(), sig, sig_len );
   free( sig );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}

// verify the signature of RG metadata
// client must be read-locked
int ms_client_verify_RGs( struct ms_client* client, ms::ms_volume_RGs* rgs ) {
   // get the signature
   size_t sig_len = rgs->signature().size();
   char* sig = CALLOC_LIST( char, sig_len );
   memcpy( sig, rgs->signature().data(), sig_len );

   rgs->set_signature( "" );

   string rg_md_data;
   try {
      rgs->SerializeToString( &rg_md_data );
   }
   catch( exception e ) {
      return -EINVAL;
   }

   // verify the signature
   int rc = md_verify_signature( client->volume_public_key, rg_md_data.data(), rg_md_data.size(), sig, sig_len );
   free( sig );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}


// verify that a message came from a gateway with the given ID.
// This WILL read-lock the client
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t user_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len ) {
   ms_client_view_rlock( client );

   bool early_reload = false;
   
   // find the gateway
   for( int i = 0; client->UG_creds[i] != NULL; i++ ) {
      if( client->UG_creds[i]->gateway_id == gateway_id && client->UG_creds[i]->user_id == user_id ) {
         if( client->UG_creds[i]->pubkey == NULL ) {
            dbprintf("WARN: No public key for Gateway %s\n", client->UG_creds[i]->name );

            // do an early reload--see if there is new volume metadata
            early_reload = true;
            continue;
         }
         
         int rc = md_verify_signature( client->UG_creds[i]->pubkey, msg, msg_len, sigb64, sigb64_len );

         ms_client_view_unlock( client );

         if( early_reload ) {
            ms_client_view_wlock( client );
            client->early_reload = true;
            ms_client_view_unlock( client );
         }

         return rc;
      }
   }

   ms_client_view_unlock( client );

   if( early_reload ) {
      ms_client_view_wlock( client );
      client->early_reload = true;
      ms_client_view_unlock( client );
   }

   return -ENOENT;
}


// load a PEM-encoded (RSA) public key into an EVP key
int ms_client_load_pubkey( EVP_PKEY** key, char const* pubkey_str ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)pubkey_str, strlen(pubkey_str) );

   EVP_PKEY* public_key = PEM_read_bio_PUBKEY( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( public_key == NULL ) {
      // invalid public key
      errorf("%s", "ERR: failed to read public key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = public_key;
   
   return 0;
}


// load a PEM-encoded (RSA) private key into an EVP key
int ms_client_load_privkey( EVP_PKEY** key, char const* privkey_str ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)privkey_str, strlen(privkey_str) );

   EVP_PKEY* privkey = PEM_read_bio_PrivateKey( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( privkey == NULL ) {
      // invalid public key
      errorf("%s", "ERR: failed to read private key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = privkey;

   return 0;
}

// generate RSA public/private key pair
int ms_client_generate_key( EVP_PKEY** key ) {

   dbprintf("%s", "Generating public/private key...\n");
   
   EVP_PKEY_CTX *ctx;
   EVP_PKEY *pkey = NULL;
   ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
   if (!ctx) {
      md_openssl_error();
      return -1;
   }

   int rc = EVP_PKEY_keygen_init( ctx );
   if( rc <= 0 ) {
      md_openssl_error();
      return rc;
   }

   rc = EVP_PKEY_CTX_set_rsa_keygen_bits( ctx, RSA_KEY_SIZE );
   if( rc <= 0 ) {
      md_openssl_error();
      return rc;
   }

   rc = EVP_PKEY_keygen( ctx, &pkey );
   if( rc <= 0 ) {
      md_openssl_error();
      return rc;
   }

   *key = pkey;
   return 0;
}


// dump a key to memory
long ms_client_dump_pubkey( EVP_PKEY* pkey, char** buf ) {
   BIO* mbuf = BIO_new( BIO_s_mem() );
   
   int rc = PEM_write_bio_PUBKEY( mbuf, pkey );
   if( rc <= 0 ) {
      errorf("PEM_write_bio_PUBKEY rc = %d\n", rc );
      md_openssl_error();
      return -EINVAL;
   }

   (void) BIO_flush( mbuf );
   
   char* tmp = NULL;
   long sz = BIO_get_mem_data( mbuf, &tmp );

   *buf = CALLOC_LIST( char, sz );
   memcpy( *buf, tmp, sz );

   BIO_free( mbuf );
   
   return sz;
}

// (re)load the Volume public key.
// client must be write-locked
int ms_client_load_volume_pubkey( struct ms_client* client, char const* volume_pubkey_str ) {
   if( client->volume_public_key ) {
      EVP_PKEY_free( client->volume_public_key );
      client->volume_public_key = NULL;
   }

   return ms_client_load_pubkey( &client->volume_public_key, volume_pubkey_str );
}


// (re)load a credential
int ms_client_load_cred( struct UG_cred* cred, const ms::ms_volume_gateway_cred* ms_cred ) {
   cred->user_id = ms_cred->owner_id();
   cred->gateway_id = ms_cred->gateway_id();
   cred->name = strdup( ms_cred->name().c_str() );
   cred->hostname = strdup( ms_cred->host().c_str() );
   cred->portnum = ms_cred->port();

   if( strcmp( ms_cred->public_key().c_str(), "NONE" ) == 0 ) {
      // no public key for this gateway on the MS
      dbprintf("WARN: No public key for Gateway %s\n", cred->name );
      cred->pubkey = NULL;
      return 0;
   }
   else {
      int rc = ms_client_load_pubkey( &cred->pubkey, ms_cred->public_key().c_str() );
      if( rc != 0 ) {
         errorf("ms_client_load_pubkey(Gateway %s) rc = %d\n", cred->name, rc );
      }
      return rc;
   }
}


// populate the client structure with the volume metadata
int ms_client_load_volume_metadata( struct ms_client* client, ms::ms_volume_metadata* volume_md ) {

   int rc = 0;

   struct UG_cred cred;
   memset( &cred, 0, sizeof(cred) );


   ms_client_wlock( client );

   // get the new public key, if desired

   if( client->reload_volume_key || client->conf->volume_public_key == NULL ) {
      // trust new public keys
      rc = ms_client_load_volume_pubkey( client, volume_md->volume_public_key().c_str() );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_pubkey rc = %d\n", rc );

         ms_client_unlock( client );
         return -ENOTCONN;
      }
   }

   // verify metadata
   rc = ms_client_verify_volume_metadata( client, volume_md );
   if( rc != 0 ) {
      errorf("ms_client_verify_volume_metadata rc = %d\n", rc );
      ms_client_unlock( client );
      return rc;
   }

   const ms::ms_volume_gateway_cred& my_cred = volume_md->cred();
   rc = ms_client_load_cred( &cred, &my_cred );
   if( rc != 0 ) {
      errorf("ms_client_load_cred rc = %d\n", rc );
      ms_client_unlock( client );
      return rc;
   }
   
   // verify that our host and port match the MS's record
   if( strcmp( cred.hostname, client->conf->hostname ) != 0 || cred.portnum != client->conf->portnum ) {
      // wrong host
      errorf("ERR: This UG is running on %s:%d, but the MS says it should be running on %s:%d.  Please log into the MS and update the UG record.\n", client->conf->hostname, client->conf->portnum, cred.hostname, cred.portnum );
      ms_client_unlock( client );

      return -ENOTCONN;
   }

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
   
   client->session_password = strdup( volume_md->session_password().c_str() );
   
   sprintf(gateway_id_str, "%" PRIu64, cred.gateway_id );
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   client->userpass = CALLOC_LIST( char, strlen(gateway_id_str) + 1 + strlen(gateway_type_str) + 1 + strlen(client->session_password) + 1 );
   sprintf( client->userpass, "%s_%s:%s", gateway_type_str, gateway_id_str, client->session_password );

   curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_view, CURLOPT_USERPWD, client->userpass );

   client->owner_id = volume_md->cred().owner_id();
   client->gateway_id = volume_md->cred().gateway_id();
   client->volume_owner_id = volume_md->owner_id();
   client->volume_id = volume_md->volume_id();
   client->blocksize = volume_md->blocksize();

   if( client->file_url == NULL ) {
      // build the /FILE/ url
      char* tmp = md_fullpath( client->url, "/FILE/", NULL );
      char buf[50];
      sprintf(buf, "%" PRIu64 "/", client->volume_id );

      client->file_url = md_fullpath( tmp, buf, NULL );
      free( tmp );
   }


   ms_client_unlock( client );

   uint64_t version = volume_md->volume_version();
   uint64_t UG_version = volume_md->ug_version();
   uint64_t RG_version = volume_md->rg_version();

   ms_client_view_wlock( client );

   if( client->view_url == NULL ) {
      client->view_url = ms_client_volume_url( client, volume_md->volume_id() );
      curl_easy_setopt( client->ms_view, CURLOPT_URL, client->view_url );
   }
      
   client->volume_version = version;
   client->UG_version = UG_version;
   client->RG_version = RG_version;
   client->session_timeout = volume_md->session_timeout();

   ms_client_view_unlock( client );
   UG_cred_free( &cred );

   return rc;
}


// process volume metadata--load it into the client, and also reload the RGs and UGs if needed.
int ms_client_process_volume_metadata( struct ms_client* client, ms::ms_volume_metadata* volume_md ) {

   int rc = 0;
   
   ms_client_view_rlock( client );

   // preserve session information
   uint64_t prev_volume_version = client->volume_version;
   uint64_t prev_UG_version = client->UG_version;
   uint64_t prev_RG_version = client->RG_version;

   ms_client_view_unlock( client );

   rc = ms_client_load_volume_metadata( client, volume_md );
   if( rc != 0 ) {
      errorf("ms_client_load_volume_metadata rc = %d\n", rc );
      return rc;
   }

   // reload UGs and RGs if needed
   if( client->volume_version != prev_volume_version ) {
      dbprintf("Volume version %" PRIu64 " --> %" PRIu64 "\n", prev_volume_version, client->volume_version );
   }

   if( client->UG_version != prev_UG_version ) {
      rc = ms_client_reload_UGs( client );
      if( rc != 0 ) {
         errorf("ms_client_reload_UGs rc = %d\n", rc );
         return rc;
      }
   }

   if( client->RG_version != prev_RG_version ) {
      rc = ms_client_reload_RGs( client );
      if( rc != 0 ) {
         errorf("ms_client_reload_RGs rc = %d\n", rc );
         return rc;
      }
   }

   return 0;
}


// get volume metadata, given a Volume anem
int ms_client_get_volume_metadata_curl( struct ms_client* client, CURL* curl ) {
   
   char* bits = NULL;
   ssize_t len = 0;
   long http_response = 0;
   ms::ms_volume_metadata volume_md;
   
   len = md_download_file5( curl, &bits );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );

   if( len < 0 ) {
      errorf("md_download_file5 rc = %zd\n", len );
      return (int)len;
   }

   if( http_response != 200 ) {
      errorf("md_download_file5 HTTP status = %ld\n", http_response );
      return -abs( (int)http_response );
   }

   // got the data
   bool valid = volume_md.ParseFromString( string(bits, len) );
   free( bits );

   if( !valid ) {
      errorf( "%s", "invalid volume metadata\n" );
      return -EINVAL;
   }

   // process the metadata
   return ms_client_process_volume_metadata( client, &volume_md );
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
// client must be read-locked
int ms_client_begin_register( struct ms_client* client, CURL* curl, char const* username, char const* register_url, ms::ms_openid_provider_reply* oid_reply ) {

   // extract our public key bits
   char* key_bits = NULL;
   long keylen = ms_client_dump_pubkey( client->my_key, &key_bits );
   if( keylen <= 0 ) {
      errorf("ms_client_load_pubkey rc = %ld\n", keylen );
      md_openssl_error();
      return (int)keylen;
   }

   // Base64 encode the key (which will also be url-encoded)
   char* key_bits_encoded = NULL;
   int rc = Base64Encode( key_bits, keylen, &key_bits_encoded );
   if( rc != 0 ) {
      errorf("Base64Encode rc = %d\n", rc );
      free( key_bits );
      return -EINVAL;
   }

   // url-encode the username
   char* username_encoded = url_encode( username, strlen(username) );
   
   free( key_bits );

   // post arguments
   char* post = CALLOC_LIST( char, strlen("syndicatepubkey=") + strlen(key_bits_encoded) + 1 + strlen("openid_username") + 1 + strlen(username_encoded) + 1 );
   sprintf( post, "openid_username=%s&syndicatepubkey=%s", username_encoded, key_bits_encoded );
   
   free( key_bits_encoded );
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

   rc = curl_easy_perform( curl );

   long http_response = 0;
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, NULL );

   free( post );

   if( rc != 0 ) {
      errorf("curl_easy_perform rc = %d\n", rc );
      return -abs(rc);
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
int ms_client_auth_op( struct ms_client* client, CURL* curl, ms::ms_openid_provider_reply* oid_reply, char** return_to_method, char** return_to ) {

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
      errorf("curl_easy_perform rc = %d\n", rc );
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
      response_buffer_free( &header_rb );

      return http_response;
   }

   response_buffer_free( &header_rb );

   // authenticate to the OpenID provider
   char const* extra_args = oid_reply->extra_args().c_str();
   char const* username_field = oid_reply->username_field().c_str();
   char const* password_field = oid_reply->password_field().c_str();
   char const* auth_handler = oid_reply->auth_handler().c_str();

   char* username_urlencoded = url_encode( client->conf->ms_username, strlen(client->conf->ms_username) );
   char* password_urlencoded = url_encode( client->conf->ms_password, strlen(client->conf->ms_password) );
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
      errorf("curl_easy_perform rc = %d\n", rc );
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
int ms_client_complete_register( struct ms_client* client, CURL* curl, char const* return_to_method, char const* return_to ) {

   dbprintf("%s return to %s\n", return_to_method, return_to );

   char* return_to_url_and_path = NULL;
   char* return_to_qs = NULL;

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
   
   rc = ms_client_get_volume_metadata_curl( client, curl );

   free( return_to_url_and_path );
   if( return_to_qs )
      free( return_to_qs );
   
   if( rc != 0 ) {
      errorf("ms_client_get_volume_metadata_curl rc = %d\n", rc );
      return rc;
   }

   return 0;
}
   
   

// register this gateway with the MS, using the SyndicateUser's OpenID username and password
int ms_client_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password ) {

   int rc = 0;
   char* register_url = ms_client_register_url( client );

   dbprintf("register at %s\n", register_url );

   CURL* curl = curl_easy_init();

   // enable the cookie parser
   curl_easy_setopt( curl, CURLOPT_COOKIEFILE, "/COOKIE" );
   
   ms_client_rlock( client );
   
   md_init_curl_handle( curl, NULL, client->conf->metadata_connect_timeout );

   // load the response
   ms::ms_openid_provider_reply oid_reply;

   // get info for the OpenID provider
   rc = ms_client_begin_register( client, curl, username, register_url, &oid_reply );

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
   rc = ms_client_auth_op( client, curl, &oid_reply, &return_to_method, &return_to );
   if( rc != 0 ) {
      errorf("ms_client_auth_op rc = %d\n", rc);
      ms_client_unlock( client );
      curl_easy_cleanup( curl );
      return rc;
   }

   ms_client_unlock( client );
   
   // complete the registration 
   rc = ms_client_complete_register( client, curl, return_to_method, return_to );
   if( rc != 0 ) {
      errorf("ms_client_complete_register rc = %d\n", rc);
      ms_client_unlock( client );
      curl_easy_cleanup( curl );
      return rc;
   }

   // success!
   ms_client_wlock( client );
   client->registered = true;
   ms_client_unlock( client );
   
   curl_easy_cleanup( curl );
   free( return_to );
   free( return_to_method );
   
   return rc;
}

// read-lock a client context 
int ms_client_rlock( struct ms_client* client ) {
   dbprintf("ms_client_rlock %p\n", client);
   return pthread_rwlock_rdlock( &client->lock );
}

// write-lock a client context 
int ms_client_wlock( struct ms_client* client ) {
   dbprintf("ms_client_wlock %p\n", client);
   return pthread_rwlock_wrlock( &client->lock );
}

// unlock a client context 
int ms_client_unlock( struct ms_client* client ) {
   dbprintf("ms_client_unlock %p\n", client);
   return pthread_rwlock_unlock( &client->lock );
}

// read-lock a client context's view
int ms_client_view_rlock( struct ms_client* client ) {
   dbprintf("ms_client_view_rlock %p\n", client);
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
int ms_client_queue_update( struct ms_client* client, char const* path, struct md_entry* update, uint64_t deadline_ms, uint64_t deadline_delta ) {

   int rc = 0;

   ms_client_wlock( client );

   long path_hash = md_hash( path );

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
      
      if( itr != client->deadlines->end() ) {
         uint64_t deadline = itr->first;
         client->deadlines->erase( itr );

         new_deadline = deadline + deadline_delta;
      }
      else {
         new_deadline = deadline_ms;
      }

      // free up the memory of this update
      md_update_free( &((*client->updates)[ path_hash ]) );

      struct md_update* up = &(*client->updates)[path_hash];
      up->op = ms::ms_update::UPDATE;
      md_entry_dup2( update, &up->ent );

      (*client->deadlines)[new_deadline] = path_hash;
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
         dbprintf("clearing update(path=%s, url=%s)\n", (*client->updates)[path_hash].ent.path, (*client->updates)[path_hash].ent.url );
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
int ms_client_clear_update( struct ms_client* client, char const* path ) {
   int rc = 0;

   ms_client_wlock( client );

   rc = ms_client_remove_update( client, md_hash( path ), NULL, NULL );

   ms_client_unlock( client );
   return rc;
}

// post data
static int ms_client_send( struct ms_client* client, char const* data, size_t len ) {
   struct curl_httppost *post = NULL, *last = NULL;
   int rc = 0;
   response_buffer_t* rb = new response_buffer_t();

   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, data, CURLFORM_BUFFERLENGTH, len, CURLFORM_END );

   ms_client_begin_uploading( client, rb, post );
   
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
      // we're good!
      rc = 0;
   }
   else if( http_response == 202 ) {
      // not OK--the MS returned an error code
      if( rb->size() > 0 ) {
         char* ret = response_buffer_to_string( rb );

         rc = strtol( ret, NULL, 10 );
         if( rc == 0 )
            rc = -EREMOTEIO;

         free( ret );
      }
   }
   else {
      // some other HTTP code
      rc = -http_response;
      
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
   ms_updates->set_signature( string("") );

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
      errorf("%s", "failed to serialize update set\n");
      return -EINVAL;
   }

   // sign this message
   char* sigb64 = NULL;
   size_t sigb64_len = 0;

   int rc = md_sign_message( pkey, update_bits.data(), update_bits.size(), &sigb64, &sigb64_len );
   if( rc != 0 ) {
      errorf("md_sign_message rc = %d\n", rc );
      return rc;
   }

   ms_updates->set_signature( string(sigb64, sigb64_len) );
   free( sigb64 );
   return 0;
}


// post a record on the MS, synchronously
static int ms_client_post( struct ms_client* client, int op, struct md_entry* ent ) {
   struct md_update up;
   up.op = op;
   memcpy( &up.ent, ent, sizeof(struct md_entry) );

   update_set updates;
   updates[ md_hash( ent->path ) ] = up;

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

   // send it off
   rc = ms_client_send( client, update_text, update_text_len );
   
   free( update_text );
   
   return rc;
}

// create a file record on the MS, synchronously
int ms_client_create( struct ms_client* client, struct md_entry* ent ) {
   ent->type = MD_ENTRY_FILE;
   return ms_client_post( client, ms::ms_update::CREATE, ent );
}

int ms_client_mkdir( struct ms_client* client, struct md_entry* ent ) {
   ent->type = MD_ENTRY_DIR;
   return ms_client_post( client, ms::ms_update::CREATE, ent );
}

// delete a record on the MS, synchronously
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, ms::ms_update::DELETE, ent );
}

// update a record on the MS, synchronously
int ms_client_update( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, ms::ms_update::UPDATE, ent );
}


// send a batch of updates.
// client must NOT be locked in any way.
static int ms_client_send_updates( struct ms_client* client, update_set* updates ) {

   int rc = 0;
   
   // don't do anything if we have nothing to do
   if( updates->size() == 0 ) {
      // nothing to do
      return 0;
   }

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

   // send it off
   rc = ms_client_send( client, update_text, update_text_len );

   free( update_text );
   return rc;
}

// post a pending update to the MS for a specific file, removing it from the update queue
// return 0 on success
// return >0 if there was a CURL error (return value is the CURL error code)
// return -EREMOTEIO if the MS's response could not be interpreted
// return <-100 if there was an unexpected HTTP status code
// return -ENOENT if there is no pending update 
int ms_client_sync_update( struct ms_client* client, char const* path ) {

   struct md_update update;
   int rc = 0;
   
   long path_hash = md_hash( path );
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
      errorf("ms_client_send_updates(%s) rc = %d\n", path, rc);

      ms_client_wlock( client );
      int replace_rc = ms_client_put_update( client->updates, client->deadlines, path_hash, &update, old_deadline );
      ms_client_unlock( client );

      if( replace_rc == -EEXIST ) {
         // this update can be freed
         md_update_free( &update );
      }
   }

   // free memory if it is no longer needed
   else {
      md_update_free( &update );
   }

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
            // this update can be freed, since we didn't put it back
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



// given a URL, convert it into a local URL if necessary
static char* ms_client_convert_url( struct md_syndicate_conf* conf, char const* url ) {
   char* ret = NULL;

   if( md_is_locally_hosted( conf, url ) ) {

      char* fs_path = md_fs_path_from_url( url );
      
      char* full_path = md_fullpath( conf->data_root, fs_path, NULL );
      
      ret = md_prepend( SYNDICATEFS_LOCAL_PROTO, full_path, NULL );
      
      free( full_path );
      free( fs_path );
   }
   else {
      ret = strdup( url );
   }

   return ret;
}


// get a set of metadata entries.
// on success, populate result with the list of filesystem i-node metadatas on the path from / to the last entry
int ms_client_resolve_path( struct ms_client* client, char const* path, vector<struct md_entry>* result_dirs, vector<struct md_entry>* result_base, struct timespec* lastmod, int* md_rc ) {
   // calculate the URL of the entry
   char* md_url = md_fullpath( client->file_url, path, NULL );

   ssize_t len = 0;
   char* md_bits = NULL;

   struct md_syndicate_conf* conf = client->conf;

   // buffer the lastmod
   char buf[256];
   sprintf(buf, "%s: %ld.%ld", HTTP_MS_LASTMOD, lastmod->tv_sec, lastmod->tv_nsec );

   // add our lastmod header
   struct curl_slist *lastmod_header = NULL;
   lastmod_header = curl_slist_append( lastmod_header, buf );

   ms_client_begin_downloading( client, md_url, lastmod_header );

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   memset( &client->read_times, 0, sizeof(client->read_times) );
   len = md_download_file5( client->ms_read, &md_bits );

   END_TIMING_DATA( ts, ts2, "MS recv" );

   int http_response = ms_client_end_downloading( client );

   curl_slist_free_all( lastmod_header );

   // print out timing information
   ms_client_rlock( client );
   DATA( HTTP_RESOLVE_TIME, (double)client->read_times.resolve_time / 1e9 );
   ms_client_unlock( client );
   
   int rc = 0;

   if( len > 0 && http_response == 200 ) {
      // got data!  it should be in the form of an md_freshness message
      ms::ms_reply resp;
      
      bool valid = resp.ParseFromString( string( md_bits, len ) );
      if( !valid ) {
         rc = -EIO;
      }
      else {

         rc = 0;
         
         // got data back!
         for( int i = 0; i < resp.entries_dir_size(); i++ ) {
            const ms::ms_entry& entry = resp.entries_dir(i);
            
            // create an md_entry from this
            struct md_entry ent;
            memset( &ent, 0, sizeof(ent));
            
            int rc = ms_entry_to_md_entry( entry, &ent );
            if( rc != 0 ) {
               errorf("ms_entry_to_md_entry(%s) rc = %d\n", entry.path().c_str(), rc );
               break;
            }

            // fix up the URL
            char* fixed_url = ms_client_convert_url( conf, ent.url );
            free( ent.url );
            ent.url = fixed_url;
            
            result_dirs->push_back( ent );
         }

         for( int i = 0; rc == 0 && i < resp.entries_base_size(); i++ ) {
            const ms::ms_entry& entry = resp.entries_base(i);

            // create an md_entry from this
            struct md_entry ent;
            memset( &ent, 0, sizeof(ent));
            
            int rc = ms_entry_to_md_entry( entry, &ent );
            if( rc != 0 ) {
               errorf("ms_entry_to_md_entry(%s) rc = %d\n", entry.path().c_str(), rc );
               break;
            }

            // fix up the URL
            char* fixed_url = ms_client_convert_url( conf, ent.url );
            free( ent.url );
            ent.url = fixed_url;
            
            result_base->push_back( ent );
         }
      }

      if( md_rc != NULL ) {
         *md_rc = resp.error();
      }
   }
   else if( len > 0 && http_response == 202 ) {
      // got an error message
      rc = strtol( md_bits, NULL, 10 );
      if( rc == 0 )
         rc = -EREMOTEIO;
   }
   else {
      if( len < 0 )
         rc = len;
      else
         rc = -http_response;    // some other HTTP error
   }

   if( md_bits )
      free( md_bits );
   
   free( md_url );
   return rc;
}


// attempt to become the acting owner of an MSEntry if we can't reach its owner
int ms_client_claim( struct ms_client* client, char const* path ) {
   // TODO
   return -EACCES;
}

// authenticate a remote UG
uint64_t ms_client_authenticate( struct ms_client* client, struct md_HTTP_connection_data* data, char* username, char* password ) {
   uint64_t uid = MD_INVALID_UID;

   // there must be a password, otherwise this is a guest (and can only access world-readable stuff)
   if( password == NULL )
      return MD_GUEST_UID;
   
   char* signature = sha256_hash_printable( password, strlen(password) );
   
   ms_client_view_rlock( client );

   for( int i = 0; client->UG_creds[i] != NULL; i++ ) {
      
   }
   
   ms_client_view_unlock( client );
   
   return uid;
}


// get a copy of the RG URLs
char** ms_client_RG_urls_copy( struct ms_client* client ) {
   ms_client_view_rlock( client );
   
   char** urls = CALLOC_LIST( char*, client->num_RG_urls + 1 );
   for( int i = 0; i < client->num_RG_urls; i++ ) {
      urls[i] = strdup( client->RG_urls[i] );
   }

   ms_client_view_unlock( client );

   return urls;
}

// get the current view version
uint64_t ms_client_volume_version( struct ms_client* client ) {
   ms_client_view_rlock( client );
   uint64_t ret = client->volume_version;
   ms_client_view_unlock( client );
   return ret;
}

// get the current UG version
uint64_t ms_client_UG_version( struct ms_client* client ) {
   ms_client_view_rlock( client );
   uint64_t ret = client->UG_version;
   ms_client_view_unlock( client );
   return ret;
}

// get the current RG version
uint64_t ms_client_RG_version( struct ms_client* client ) {
   ms_client_view_rlock( client );
   uint64_t ret = client->RG_version;
   ms_client_view_unlock( client );
   return ret;
}


