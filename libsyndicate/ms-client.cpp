/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "ms-client.h"

static void* ms_client_uploader_thread( void* arg );
static void* ms_client_view_thread( void* arg );
static void ms_client_uploader_signal( struct ms_client* client );
int ms_client_load_volume_metadata( struct ms_volume* vol, ms::ms_volume_metadata* volume_md );
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

static void ms_volume_free( struct ms_volume* vol ) {
   if( vol->volume_public_key ) {
      EVP_PKEY_free( vol->volume_public_key );
      vol->volume_public_key = NULL;
   }

   if( vol->UG_creds ) {
      for( int i = 0; vol->UG_creds[i] != NULL; i++ ) {
         UG_cred_free( vol->UG_creds[i] );
         free( vol->UG_creds[i] );
      }
      free( vol->UG_creds );
      vol->UG_creds = NULL;
   }

   if( vol->RG_urls ) {
      for( int i = 0; i < vol->num_RG_urls; i++ ) {
         free( vol->RG_urls[i] );
      }
      free( vol->RG_urls );
   }

   if( vol->AG_infos ) {
      for( int i = 0; i < vol->num_AGs; i++ ) {
         free( vol->AG_infos[i]->hostname );
         free( vol->AG_infos[i] );
      }
      free( vol->AG_infos );
   }
   
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
   pthread_mutex_init( &client->uploader_lock, NULL );
   pthread_cond_init( &client->uploader_cv, NULL );

   int rc = 0;

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

   for( int i = 0; i < client->num_volumes; i++ ) {
      ms_volume_free( client->volumes[i] );
      free( client->volumes[i] );
   }
   free( client->volumes );
   
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

   if( client->session_password )
      free( client->session_password );

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
char* ms_client_file_url( struct ms_client* client, uint64_t volume_id, uint64_t file_id, int64_t version, int64_t tv_sec, int32_t tv_nsec ) {

   char volume_id_str[50];
   sprintf( volume_id_str, "%" PRIu64, volume_id );

   char file_id_str[50];
   sprintf( file_id_str, "%" PRIX64, file_id );

   char version_str[50];
   sprintf( version_str, "%" PRId64, version );

   char timespec_str[60];
   sprintf( timespec_str, "%" PRId64 "/%d", tv_sec, tv_nsec );

   char* volume_file_path = CALLOC_LIST( char, strlen(client->url) + 1 + strlen("/FILE/") + 1 + strlen(volume_id_str) + 1 + strlen(file_id_str) + 1 + strlen(version_str) + 1 + strlen(timespec_str) + 1 );

   ms_client_rlock( client );
   sprintf( volume_file_path, "%s/FILE/%s/%s/%s/%s", client->url, volume_id_str, file_id_str, version_str, timespec_str );
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


char* ms_client_register_url( struct ms_client* client ) {
   // build the /REGISTER/ url

   char gateway_type_str[10];
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   ms_client_rlock( client );

   char* url = CALLOC_LIST( char, strlen(client->url) + 1 +
                                  strlen("/REGISTER/") + 1 +
                                  strlen(client->conf->gateway_name) + 1 +
                                  strlen(client->conf->ms_username) + 1 +
                                  strlen(gateway_type_str) + 1 +
                                  strlen("/begin") + 1);

   sprintf(url, "%s/REGISTER/%s/%s/%s/begin", client->url, gateway_type_str, client->conf->gateway_name, client->conf->ms_username );

   ms_client_unlock( client );

   return url;
}


// view thread body, for synchronizing Volume metadata (including UG and RG lists)
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

   bool* early_reload = NULL;
   uint64_t* volumes = NULL;
   
   while( client->running ) {

      int num_volumes = 0;
      uint64_t now_ms = currentTimeMillis();
      uint64_t wakeup_ms = now_ms + view_reload_freq * 1000;
      
      // wait for next reload
      while( now_ms < wakeup_ms ) {
         
         if( early_reload ) {
            free( early_reload );
            early_reload = NULL;
         }

         if( volumes ) {
            free( volumes );
            volumes = NULL;
         }

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

         bool do_early_reload = false;
         
         // hint to reload now?
         ms_client_view_wlock( client );
         
         num_volumes = client->num_volumes;
         if( num_volumes > 0 ) {
            num_volumes = client->num_volumes;
            early_reload = CALLOC_LIST( bool, num_volumes );
            volumes = CALLOC_LIST( uint64_t, num_volumes );

            for( int i = 0; i < num_volumes; i++ ) {

               volumes[i] = client->volumes[i]->volume_id;
               
               if( client->volumes[i]->early_reload ) {
                  early_reload[i] = true;
                  client->volumes[i]->early_reload = false;
                  do_early_reload = true;
               }
            }
         }

         ms_client_view_unlock( client );

         if( do_early_reload ) {
            break;
         }
         
         if( !client->running ) {
            break;
         }
      }

      if( !client->running || num_volumes == 0 ) {
         if( early_reload ) {
            free( early_reload );
            early_reload = NULL;
         }

         if( volumes ) {
            free( volumes );
            volumes = NULL;
         }
         
         if( !client->running )
            break;
         else
            continue;
      }
      if( num_volumes > 0 ) {
         pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );

         for( int i = 0; i < num_volumes; i++ ) {
            if( !early_reload[i] )
               continue;

            // reload Volume metadata
            dbprintf("Begin reload Volume %" PRIu64 " metadata\n", volumes[i] );

            int rc = ms_client_reload_volume( client, NULL, volumes[i] );

            dbprintf("End reload Volume %" PRIu64 " metadata, rc = %d\n", volumes[i], rc);
         }

         pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
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
      int num_volumes = client->num_volumes;
      ms_client_unlock( client );

      if( num_volumes == 0 ) {
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
   ms_client_wlock_backoff( client, &client->downloading );

   client->downloading = true;

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
   client->downloading = false;

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


// look up a Volume
// return a pointer (reference)
// client must be at least read-locked
struct ms_volume* ms_client_find_volume( struct ms_client* client, uint64_t volume ) {
   struct ms_volume* vol = NULL;

   for( int i = 0; i < client->num_volumes; i++ ) {
      if( client->volumes[i]->volume_id == volume ) {
         vol = client->volumes[i];
         break;
      }
   }

   return vol;
}

// look up a Volume by name
// return a pointer (reference)
// client must be read-locked
struct ms_volume* ms_client_find_volume( struct ms_client* client, char const* name ) {
   struct ms_volume* vol = NULL;

   for( int i = 0; i < client->num_volumes; i++ ) {
      if( strcmp( client->volumes[i]->name, name ) == 0 ) {
         vol = client->volumes[i];
         break;
      }
   }

   return vol;
}


// replace a Volume's UGs
// return -ENOENT if it doesn't exist
// free the old Volume
// client must be view-write-locked
int ms_client_replace_volume( struct ms_client* client, struct ms_volume* vol ) {
   for( int i = 0; i < client->num_volumes; i++ ) {
      if( client->volumes[i]->volume_id == vol->volume_id ) {

         struct ms_volume* old = client->volumes[i];
         client->volumes[i] = vol;

         ms_volume_free( old );
         free( old );
         return 0;
      }
   }
   return -ENOENT;
}

// add a Volume
// return a pointer (reference) to it
// NOT THREAD SAFE--client must be read-locked first
struct ms_volume* ms_client_add_volume( struct ms_client* client ) {
   struct ms_volume* vol = NULL;

   for( int i = 0; i < client->num_volumes; i++ ) {
      if( client->volumes[i]->volume_id == 0 ) {
         vol = client->volumes[i];
         break;
      }
   }

   if( vol )
      return vol;

   // allocate a new one
   struct ms_volume** new_vols = CALLOC_LIST( struct ms_volume*, 2 * client->num_volumes );
   memcpy( new_vols, client->volumes, sizeof(struct ms_volume*) * client->num_volumes );

   free( client->volumes );
   client->volumes = new_vols;
   
   vol = client->volumes[ client->num_volumes ];

   client->num_volumes *= 2;

   return vol;
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


// load a Volume's UGs from data
int ms_client_load_volume_UGs( struct ms_volume* vol, const ms::ms_volume_UGs* volume_ugs ) {
   
   struct UG_cred** ugs = CALLOC_LIST( struct UG_cred*, volume_ugs->ug_creds_size() + 1 );
   for( int i = 0; i < volume_ugs->ug_creds_size(); i++ ) {
      struct UG_cred* uent = CALLOC_LIST( struct UG_cred, 1 );

      const ms::ms_volume_gateway_cred& ug_cred = volume_ugs->ug_creds(i);

      ms_client_load_cred( uent, &ug_cred );

      ugs[i] = uent;

      dbprintf("UG: Volume = %" PRIu64 ", id = %" PRIu64 ", owner = %" PRIu64 ", name = %s\n", vol->volume_id, uent->gateway_id, uent->user_id, uent->name );
   }

   struct UG_cred** old_ugs = vol->UG_creds;
   int old_ug_len = vol->num_UG_creds;
   
   vol->UG_creds = ugs;
   vol->UG_version = volume_ugs->ug_version();
   vol->num_UG_creds = volume_ugs->ug_creds_size();

   for( int i = 0; i < old_ug_len; i++ ) {
      UG_cred_free( old_ugs[i] );
      free( old_ugs[i] );
   }
   free( old_ugs );

   return 0;
}


// load a Volume's RGs from data
// no validation is done; this should be done earlier
int ms_client_load_volume_RGs( struct ms_volume* volume, const ms::ms_volume_RGs* volume_rgs ) {

   char** rg_urls = CALLOC_LIST( char*, volume_rgs->rg_hosts_size() + 1 );
   for( int i = 0; i < volume_rgs->rg_hosts_size(); i++ ) {
      char* tmp = md_prepend( "https://", volume_rgs->rg_hosts(i).c_str(), NULL );

      char portbuf[20];
      sprintf(portbuf, ":%d", volume_rgs->rg_ports(i) );

      char* url = md_prepend( tmp, portbuf, NULL );
      rg_urls[i] = url;

      dbprintf("RG: Volume = %" PRIu64 ", URL = %s\n", volume->volume_id, url );

      free( tmp );
   }

   uint64_t RG_version = volume_rgs->rg_version();

   char** old_urls = volume->RG_urls;
   int num_old_urls = volume->num_RG_urls;

   volume->RG_urls = rg_urls;
   volume->num_RG_urls = volume_rgs->rg_hosts_size();
   volume->RG_version = RG_version;

   for( int i = 0; i < num_old_urls; i++ ) {
      free( old_urls[i] );
   }
   free( old_urls );

   return 0;
}


// load a Volume's AGs from data
// no validation is done; this should be done earlier
int ms_client_load_volume_AGs( struct ms_volume* volume, const ms::ms_volume_AGs* volume_ags ) {
   struct AG_info** ags = CALLOC_LIST( struct AG_info*, volume_ags->ag_ids_size() + 1 );

   for( int i = 0; i < volume_ags->ag_ids_size(); i++ ) {
      ags[i] = CALLOC_LIST( struct AG_info, 1 );
      ags[i]->gateway_id = volume_ags->ag_ids(i);
      ags[i]->blocksize = volume_ags->ag_blocksizes(i);
      ags[i]->hostname = strdup( volume_ags->hostnames(i).c_str() );
      ags[i]->portnum = volume_ags->portnums(i);
   }

   uint64_t AG_version = volume_ags->ag_version();

   struct AG_info** old_ags = volume->AG_infos;
   int num_old_ags = volume->num_AGs;

   volume->AG_version = AG_version;
   volume->AG_infos = ags;
   volume->num_AGs = volume_ags->ag_ids_size();

   for( int i = 0; i < num_old_ags; i++ ) {
      free( old_ags[i] );
   }
   free( old_ags );

   return 0;
}


// Get the UG records from a Volume
int ms_client_reload_UGs( struct ms_client* client, uint64_t volume_id ) {

   ms::ms_volume_UGs volume_ugs;

   char* bits = NULL;
   ssize_t len = 0;

   ms_client_view_rlock( client );
   
   // verify that the Volume exists
   struct ms_volume* vol = ms_client_find_volume( client, volume_id );

   if( vol == NULL ) {
      // not part of this volume anymore...
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   vol = NULL;

   char* UG_url = ms_client_url( client, volume_id, "/UG/" );

   ms_client_view_unlock( client );

   int rc = ms_client_download_volume_metadata( client, UG_url, &bits, &len );

   if( rc < 0 ) {
      errorf("ms_client_download_volume_metadata( %s ) rc = %d\n", UG_url, rc );
      free( UG_url );
      free( vol );
      return rc;
   }

   free( UG_url );

   // got the data
   bool valid = volume_ugs.ParseFromString( string(bits, len) );
   free( bits );

   if( !valid ) {
      errorf("%s", "invalid UG metadata\n" );
      free( vol );
      return -EINVAL;
   }

   ms_client_view_wlock( client );
   
   vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      // not part of this Volume anymore
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // verify the data
   rc = ms_client_verify_UGs( vol->volume_public_key, &volume_ugs );
   if( rc != 0 ) {
      errorf("ms_client_verify_UGs rc = %d\n", rc );
      ms_client_view_unlock( client );
      return rc;
   }

   // load the Volume
   rc = ms_client_load_volume_UGs( vol, &volume_ugs );
   ms_client_view_unlock( client );
   
   if( rc != 0 ) {
      errorf("ms_client_load_volume_UGs rc = %d\n", rc );
      free( vol );
      return rc;
   }

   return 0;
}

// get RG metadata for a Volume
int ms_client_reload_RGs( struct ms_client* client, uint64_t volume_id ) {
   ms::ms_volume_RGs volume_rgs;

   char* bits = NULL;
   ssize_t len = 0;

   ms_client_view_rlock( client );

   // verify that the Volume exists
   struct ms_volume* vol = ms_client_find_volume( client, volume_id );

   if( vol == NULL ) {
      // not part of this volume anymore...
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   vol = NULL;
   
   char* RG_url = ms_client_url( client, volume_id, "/RG/" );

   ms_client_view_unlock( client );

   int rc = ms_client_download_volume_metadata( client, RG_url, &bits, &len );

   free( RG_url );
   
   if( rc < 0 ) {
      errorf("ms_client_download_volume_metadata( /RG/ ) rc = %d\n", rc );
      return rc;
   }

   // got the data
   bool valid = volume_rgs.ParseFromString( string(bits, len) );
   free( bits );

   if( !valid ) {
      errorf("%s", "Failed to load RGs\n");
      return -EINVAL;
   }

   ms_client_view_wlock( client );

   vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // verify the data
   rc = ms_client_verify_RGs( vol->volume_public_key, &volume_rgs );
   if( rc != 0 ) {
      errorf("ms_client_verify_RGs rc = %d\n", rc );
      ms_client_view_unlock( client );
      return rc;
   }

   // sanity check--the number of hosts and ports must be the same
   if( volume_rgs.rg_hosts_size() != volume_rgs.rg_ports_size() ) {
      errorf("%s", "RG message disagrees on number of hosts and ports\n");
      ms_client_view_unlock( client );
      return rc;
   }

   // sanity check---the ports must be between 1 and 65535
   for( int i = 0; i < volume_rgs.rg_ports_size(); i++ ) {
      if( volume_rgs.rg_ports(i) < 1 || volume_rgs.rg_ports(i) > 65534 ) {
         errorf("RG message has invalid port number %d\n", volume_rgs.rg_ports(i));
         ms_client_view_unlock( client );
         return rc;
      }
   }

   // load the Volume
   rc = ms_client_load_volume_RGs( vol, &volume_rgs );
   ms_client_view_unlock( client );
   
   if( rc != 0 ) {
      errorf("ms_client_load_volume_RGs rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// get AG metadata for a Volume
int ms_client_reload_AGs( struct ms_client* client, uint64_t volume_id ) {
   ms::ms_volume_AGs volume_ags;

   char* bits = NULL;
   ssize_t len = 0;

   ms_client_view_rlock( client );

   // verify that the Volume exists
   struct ms_volume* vol = ms_client_find_volume( client, volume_id );

   if( vol == NULL ) {
      // not part of this volume anymore...
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   vol = NULL;

   char* AG_url = ms_client_url( client, volume_id, "/AG/" );

   ms_client_view_unlock( client );

   int rc = ms_client_download_volume_metadata( client, AG_url, &bits, &len );

   free( AG_url );

   if( rc < 0 ) {
      errorf("ms_client_download_volume_metadata( /AG/ ) rc = %d\n", rc );
      return rc;
   }

   // got the data
   bool valid = volume_ags.ParseFromString( string(bits, len) );
   free( bits );

   if( !valid ) {
      errorf("%s", "Failed to load AGs\n");
      return -EINVAL;
   }

   ms_client_view_wlock( client );

   vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // verify the data
   rc = ms_client_verify_AGs( vol->volume_public_key, &volume_ags );
   if( rc != 0 ) {
      errorf("ms_client_verify_AGs rc = %d\n", rc );
      ms_client_view_unlock( client );
      return rc;
   }

   // sanity check--the number of hosts and ports must be the same
   if( volume_ags.ag_ids_size() != volume_ags.ag_blocksizes_size() ) {
      errorf("%s", "AG message disagrees on number of IDs and blocksizes\n");
      ms_client_view_unlock( client );
      return rc;
   }

   // sanity check---the ports must be between 1 and 65535
   for( int i = 0; i < volume_ags.portnums_size(); i++ ) {
      if( volume_ags.portnums(i) < 1 || volume_ags.portnums(i) > 65534 ) {
         errorf("AG message has invalid port number %d\n", volume_ags.portnums(i));
         ms_client_view_unlock( client );
         return rc;
      }
   }

   // load the Volume
   rc = ms_client_load_volume_AGs( vol, &volume_ags );
   ms_client_view_unlock( client );

   if( rc != 0 ) {
      errorf("ms_client_load_volume_AGs rc = %d\n", rc );
      return rc;
   }

   return 0;
}



// reload a Volume, which already exists
// give either volume_name or volume_id (volume_name preferred), but you don't need both.
// client must NOT be locked
int ms_client_reload_volume( struct ms_client* client, char const* volume_name, uint64_t volume_id ) {
   int rc = 0;
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   ssize_t len = 0;
   struct ms_volume* vol = NULL;

   ms_client_view_rlock( client );
 
   if( volume_name != NULL ) {
      // look up by name
      vol = ms_client_find_volume( client, volume_name );
   }
   else if( volume_id > 0 ) {
      // look up by ID
      vol = ms_client_find_volume( client, volume_id );
   }

   if( vol == NULL ) {
      errorf("No such volume '%s' or %" PRIu64 "\n", volume_name, volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // get the Volume ID for later
   volume_id = vol->volume_id;
   
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
      errorf("Invalid data for Volume '%s' or %" PRIu64 "\n", volume_name, volume_id );
      return -EINVAL;
   }
   
   ms_client_view_wlock( client );

   // re-find the Volume
   vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("ERR: unbound from Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   uint64_t old_version = vol->volume_version;
   uint64_t old_UG_version = vol->UG_version;
   uint64_t old_RG_version = vol->RG_version;
   uint64_t old_AG_version = vol->AG_version;
   
   rc = ms_client_load_volume_metadata( vol, &volume_md );

   uint64_t new_version = vol->volume_version;
   uint64_t new_UG_version = vol->UG_version;
   uint64_t new_RG_version = vol->RG_version;
   uint64_t new_AG_version = vol->AG_version;
   
   ms_client_view_unlock( client );
   
   if( rc != 0 ) {
      errorf("ms_client_load_volume_metadata('%s' or %" PRIu64 ") rc = %d\n", volume_name, volume_id, rc );
      return rc;
   }
   
   // do we need to download the UGs and/or RGs as well?
   dbprintf("Volume version %" PRIu64 " --> %" PRIu64 "\n", old_version, new_version );
   dbprintf("UG     version %" PRIu64 " --> %" PRIu64 "\n", old_UG_version, new_UG_version );
   dbprintf("RG     version %" PRIu64 " --> %" PRIu64 "\n", old_RG_version, new_RG_version );
   dbprintf("AG     version %" PRIu64 " --> %" PRIu64 "\n", old_AG_version, new_AG_version );

   if( new_UG_version != old_UG_version && !volume_md.has_ugs() ) {
      // version change, and no UG information given 
      rc = ms_client_reload_UGs( client, volume_id );
      if( rc != 0 ) {
         errorf("ms_client_reload_UGs rc = %d\n", rc );

         return rc;
      }
   }

   if( new_RG_version != old_RG_version && !volume_md.has_rgs() ) {
      // version change, and no RG information given
      rc = ms_client_reload_RGs( client, volume_id );
      if( rc != 0 ) {
         errorf("ms_client_reload_RGs rc = %d\n", rc );

         return rc;
      }
   }

   if( new_AG_version != old_AG_version && !volume_md.has_ags() ) {
      // version change, and no AG information given
      rc = ms_client_reload_AGs( client, volume_id );
      if( rc != 0 ) {
         errorf("ms_client_reload_AGs rc = %d\n", rc );

         return rc;
      }
   }
   
   return 0;
}


// verify the signature of the Volume metadata.
// client must be read-locked
int ms_client_verify_volume_metadata( EVP_PKEY* volume_public_key, ms::ms_volume_metadata* volume_md ) {
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
   int rc = md_verify_signature( volume_public_key, volume_md_data.data(), volume_md_data.size(), sigb64, sigb64_len );
   free( sigb64 );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}

// verify the signature of the UG metadata
// client must be read-locked
int ms_client_verify_UGs( EVP_PKEY* public_key, ms::ms_volume_UGs* ugs ) {
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
   int rc = md_verify_signature( public_key, ug_md_data.data(), ug_md_data.size(), sig, sig_len );
   free( sig );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}

// verify the signature of RG metadata
// client must be read-locked
int ms_client_verify_RGs( EVP_PKEY* public_key, ms::ms_volume_RGs* rgs ) {
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
   int rc = md_verify_signature( public_key, rg_md_data.data(), rg_md_data.size(), sig, sig_len );
   free( sig );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}


// verify the signature of AG metadata
// client must be read-locked
int ms_client_verify_AGs( EVP_PKEY* public_key, ms::ms_volume_AGs* ags ) {
   // get the signature
   size_t sig_len = ags->signature().size();
   char* sig = CALLOC_LIST( char, sig_len );
   memcpy( sig, ags->signature().data(), sig_len );

   ags->set_signature( "" );

   string ag_md_data;
   try {
      ags->SerializeToString( &ag_md_data );
   }
   catch( exception e ) {
      return -EINVAL;
   }

   // verify the signature
   int rc = md_verify_signature( public_key, ag_md_data.data(), ag_md_data.size(), sig, sig_len );
   free( sig );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}


// verify that a message came from a gateway with the given ID.
// this will write-lock the client view
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t user_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len ) {
   ms_client_view_wlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      dbprintf("WARN: No such volume %" PRIu64 "\n", volume_id );
      client->early_reload = true;
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // find the gateway
   for( int i = 0; vol->UG_creds[i] != NULL; i++ ) {
      if( vol->UG_creds[i]->gateway_id == gateway_id && vol->UG_creds[i]->user_id == user_id ) {
         if( vol->UG_creds[i]->pubkey == NULL ) {
            dbprintf("WARN: No public key for Gateway %s\n", vol->UG_creds[i]->name );

            // ask the MS for the public key
            vol->early_reload = true;
               
            ms_client_view_unlock( client );
            return -EAGAIN;
         }
         
         int rc = md_verify_signature( vol->UG_creds[i]->pubkey, msg, msg_len, sigb64, sigb64_len );

         ms_client_view_unlock( client );

         return rc;
      }
   }

   ms_client_view_unlock( client );

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


// populate a Volume structure with the volume metadata
int ms_client_load_volume_metadata( struct ms_volume* vol, ms::ms_volume_metadata* volume_md ) {

   int rc = 0;
   
   // get the new public key
   if( vol->reload_volume_key || vol->volume_public_key == NULL ) {
      vol->reload_volume_key = false;
      
      // trust it this time, but not in the future
      rc = ms_client_load_pubkey( &vol->volume_public_key, volume_md->volume_public_key().c_str() );
      if( rc != 0 ) {
         errorf("ms_client_load_pubkey rc = %d\n", rc );
         return -ENOTCONN;
      }
   }

   if( vol->volume_public_key == NULL ) {
      errorf("%s", "unable to verify integrity of metadata for Volume!  No public key given!\n");
      return -ENOTCONN;
   }

   // verify metadata
   rc = ms_client_verify_volume_metadata( vol->volume_public_key, volume_md );
   if( rc != 0 ) {
      errorf("ms_client_verify_volume_metadata rc = %d\n", rc );
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

   // load UG and RG information, if needed, but don't add it to the Volume until successfully given
   // NOTE: since the whole volume_md structure is signed, there's no need to check the signatures of the UGs or RGs
   struct ms_volume vol_UGs;
   struct ms_volume vol_RGs;
   struct ms_volume vol_AGs;

   struct md_entry* root = NULL;

   memset( &vol_UGs, 0, sizeof(struct ms_volume) );
   memset( &vol_RGs, 0, sizeof(struct ms_volume) );
   memset( &vol_AGs, 0, sizeof(struct ms_volume) );

   bool loaded_ugs = false;
   bool loaded_rgs = false;
   bool loaded_ags = false;

   // if root is given, then load it
   if( volume_md->has_root() ) {
      root = CALLOC_LIST( struct md_entry, 1 );
      ms_entry_to_md_entry( volume_md->root(), root );
   }

   // if UG information is given, then load it
   if( volume_md->has_ugs() ) {
      const ms::ms_volume_UGs& ugs = volume_md->ugs();

      rc = ms_client_load_volume_UGs( &vol_UGs, &ugs );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_UGs rc = %d\n", rc );
         return rc;
      }

      loaded_ugs = true;
   }

   // if RG information is given, then load it
   if( volume_md->has_rgs() ) {
      const ms::ms_volume_RGs& rgs = volume_md->rgs();

      rc = ms_client_load_volume_RGs( &vol_RGs, &rgs );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_RGs rc = %d\n", rc );

         if( loaded_ugs ) {
            ms_volume_free( &vol_UGs );
         }
         
         return rc;
      }

      loaded_rgs = true;
   }

   // if AG information is given, then load it
   if( volume_md->has_ags() ) {
      const ms::ms_volume_AGs& ags = volume_md->ags();

      rc = ms_client_load_volume_AGs( &vol_AGs, &ags );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_AGs rc = %d\n", rc );

         if( loaded_ugs ) {
            ms_volume_free( &vol_UGs );
         }
         if( loaded_rgs ) {
            ms_volume_free( &vol_RGs );
         }

         return rc;
      }

      loaded_ags = true;
   }

   if( loaded_ugs ) {
      // got UG information
      vol->num_UG_creds = vol_UGs.num_UG_creds;
      vol->UG_creds = vol_UGs.UG_creds;
      vol->UG_version = vol_UGs.UG_version;
   }
   else {
      vol->UG_version = volume_md->ug_version();
   }

   if( loaded_rgs ) {
      // got RG information
      vol->num_RG_urls = vol_RGs.num_RG_urls;
      vol->RG_urls = vol_RGs.RG_urls;
      vol->RG_version = vol_RGs.RG_version;
   }
   else {
      vol->RG_version = volume_md->rg_version();
   }

   if( loaded_ags ) {
      // got AG information
      vol->num_AGs = vol_AGs.num_AGs;
      vol->AG_infos = vol_AGs.AG_infos;
      vol->AG_version = vol_AGs.AG_version;
   }
   else {
      vol->AG_version = volume_md->ag_version();
   }

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


static void ms_client_free_volumes( struct ms_volume** volumes, int num_volumes ) {
   for( int i = 0; i < num_volumes; i++ ) {
      ms_volume_free( volumes[i] );
      free( volumes[i] );
   }
}

// load a registration message
int ms_client_load_registration_metadata( struct ms_client* client, ms::ms_registration_metadata* registration_md ) {

   int rc = 0;

   struct UG_cred cred;
   memset( &cred, 0, sizeof(cred) );

   // load cred
   const ms::ms_volume_gateway_cred& my_cred = registration_md->cred();
   rc = ms_client_load_cred( &cred, &my_cred );
   if( rc != 0 ) {
      errorf("ms_client_load_cred rc = %d\n", rc );
      return rc;
   }

   ms_client_rlock( client );

   // verify that our host and port match the MS's record
   if( strcmp( cred.hostname, client->conf->hostname ) != 0 || cred.portnum != client->conf->portnum ) {
      // wrong host
      errorf("ERR: This UG is running on %s:%d, but the MS says it should be running on %s:%d.  Please log into the MS and update the UG record.\n", client->conf->hostname, client->conf->portnum, cred.hostname, cred.portnum );
      ms_client_unlock( client );

      UG_cred_free( &cred );
      return -ENOTCONN;
   }

   ms_client_unlock( client );

   // allocate volumes
   struct ms_volume** volumes = CALLOC_LIST( struct ms_volume*, registration_md->volumes_size() );
   int num_volumes = registration_md->volumes_size();

   
   dbprintf("Registered as Gateway %" PRIu64 "\n", cred.gateway_id );
   
   if( num_volumes == 1 ) {
      dbprintf("Bound to %d volume\n", num_volumes);
   }
   else {
      dbprintf("Bound to %d volume\n", num_volumes);
   }

   
   for( int i = 0; i < num_volumes; i++ ) {
      struct ms_volume* vol = CALLOC_LIST( struct ms_volume, 1 );
      vol->reload_volume_key = true;         // get the public key

      ms::ms_volume_metadata* vol_md = registration_md->mutable_volumes(i);

      // load the Volume information
      rc = ms_client_load_volume_metadata( vol, vol_md );
      if( rc != 0 ) {
         errorf("ms_client_load_volume_metadata(%s) rc = %d\n", vol_md->name().c_str(), rc );
         
         ms_client_free_volumes( volumes, i );
         free( volumes );
         UG_cred_free( &cred );
         return rc;
      }

      volumes[i] = vol;

      dbprintf("Volume %" PRIu64 ": '%s', version: %" PRIu64 ", UGs: %" PRIu64 ", RGs: %" PRIu64 "\n", vol->volume_id, vol->name, vol->volume_version, vol->UG_version, vol->RG_version );
   }

   ms_client_view_wlock( client );
   client->volumes = volumes;
   client->num_volumes = num_volumes;
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
   client->session_timeout = registration_md->session_timeout();
   
   sprintf(gateway_id_str, "%" PRIu64, cred.gateway_id );
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   client->userpass = CALLOC_LIST( char, strlen(gateway_id_str) + 1 + strlen(gateway_type_str) + 1 + strlen(client->session_password) + 1 );
   sprintf( client->userpass, "%s_%s:%s", gateway_type_str, gateway_id_str, client->session_password );

   curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( client->ms_view, CURLOPT_USERPWD, client->userpass );

   client->owner_id = registration_md->cred().owner_id();
   client->gateway_id = registration_md->cred().gateway_id();

   ms_client_unlock( client );

   UG_cred_free( &cred );

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
// client must be read-locked
int ms_client_begin_register( EVP_PKEY* my_key, CURL* curl, char const* username, char const* register_url, ms::ms_openid_provider_reply* oid_reply ) {

   // extract our public key bits
   char* key_bits = NULL;
   long keylen = ms_client_dump_pubkey( my_key, &key_bits );
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
int ms_client_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password ) {

   int rc = 0;
   char* register_url = ms_client_register_url( client );

   CURL* curl = curl_easy_init();
   
   ms::ms_openid_provider_reply oid_reply;
   ms::ms_registration_metadata registration_md;

   dbprintf("register at %s\n", register_url );

   // enable the cookie parser
   curl_easy_setopt( curl, CURLOPT_COOKIEFILE, "/COOKIE" );
   
   ms_client_rlock( client );
   
   md_init_curl_handle( curl, NULL, client->conf->metadata_connect_timeout );

   // get info for the OpenID provider
   rc = ms_client_begin_register( client->my_key, curl, username, register_url, &oid_reply );

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
   rc = ms_client_load_registration_metadata( client, &registration_md );
   if( rc != 0 ) {
      errorf("ms_client_load_registration_metadata rc = %d\n", rc );
      return rc;
   }
   
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

   if( deadline_ms == 0 ) {
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

      if( new_deadline >= (uint64_t)currentTimeMillis() ) {
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
static int ms_client_send( struct ms_client* client, uint64_t* file_id, char const* url, char const* data, size_t len ) {
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
      // we're good!
      rc = 0;
      if( rb->size() > 0 && file_id ) {
         // this should be the File ID
         char* ret = response_buffer_to_string( rb );
         size_t ret_len = response_buffer_size( rb );
         
         // force null-termination and length
         char buf[21];
         memset( buf, 0, 21 );
         strncpy( buf, ret, 20 );
         
         uint64_t fid = 0;
         int cnt = sscanf( buf, "%" PRIu64, &fid );
         
         if( cnt == 0 || fid == 0 ) {
            // critical error
            char* errbuf = CALLOC_LIST( char, ret_len + 1 );
            strncpy( errbuf, ret, ret_len );
            errorf("Invalid HTTP 200 response: '%s'\n", errbuf );
            rc = -EREMOTEIO;
            free( errbuf );
         }
         else {
            *file_id = fid;
         }
         
         free( ret );
      }
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
static int ms_client_post( struct ms_client* client, uint64_t* file_id, uint64_t volume_id, int op, struct md_entry* ent ) {
   struct md_update up;
   up.op = op;
   memcpy( &up.ent, ent, sizeof(struct md_entry) );

   update_set updates;
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

   // send it off
   rc = ms_client_send( client, file_id, file_url, update_text, update_text_len );
   
   free( update_text );

   free( file_url );
      
   return rc;
}


static uint64_t ms_client_make_file_id() {
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
   
   int rc = ms_client_post( client, &file_id, ent->volume, ms::ms_update::CREATE, ent );
   
   if( rc == 0 ) {
      *file_id_ret = file_id;
      dbprintf("output file_id: %" PRIX64 "\n", file_id );
   }

   ent->file_id = old_file_id;
   
   return rc;
}

int ms_client_mkdir( struct ms_client* client, uint64_t* file_id_ret, struct md_entry* ent ) {   
   ent->type = MD_ENTRY_DIR;
   
   uint64_t file_id = ms_client_make_file_id();
   uint64_t old_file_id = ent->file_id;
   ent->file_id = file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", file_id );
   int rc = ms_client_post( client, &file_id, ent->volume, ms::ms_update::CREATE, ent );
   
   if( rc == 0 ) {
      *file_id_ret = file_id;
      dbprintf("output file_id: %" PRIX64 "\n", file_id );
   }

   ent->file_id = old_file_id;
   
   return rc;
}

// delete a record on the MS, synchronously
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, NULL, ent->volume, ms::ms_update::DELETE, ent );
}

// update a record on the MS, synchronously
int ms_client_update( struct ms_client* client, struct md_entry* ent ) {
   return ms_client_post( client, NULL, ent->volume, ms::ms_update::UPDATE, ent );
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
      rc = ms_client_send( client, NULL, file_url, update_text, update_text_len );

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
int ms_client_parse_reply( ms::ms_reply* src, char const* buf, size_t buf_len ) {

   // de-serialize
   bool valid = src->ParseFromString( string(buf, buf_len) );
   if( !valid ) {
      errorf("%s", "Invalid data\n" );
      return -EINVAL;
   }
   
   if( src->error() != 0 ) {
      errorf("MS error: %d\n", src->error() );
      return -EREMOTEIO;
   }
   
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
int ms_client_init_download( struct ms_client* client, struct ms_download_context* download, uint64_t volume_id, uint64_t file_id, int64_t file_version, int64_t tv_sec, int32_t tv_nsec ) {
   download->url = ms_client_file_url( client, volume_id, file_id, file_version, tv_sec, tv_nsec );
   download->curl = curl_easy_init();
   download->rb = new response_buffer_t();

   md_init_curl_handle( download->curl, download->url, client->conf->metadata_connect_timeout );
   
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
int ms_client_make_path_ent( struct ms_path_ent* path_ent, uint64_t file_id, int64_t version, int64_t mtime_sec, int32_t mtime_nsec, char const* name, void* cls ) {
   // build up the ms_path as we traverse our cached path
   path_ent->file_id = file_id;
   path_ent->version = version;
   path_ent->mtime.tv_sec = mtime_sec;
   path_ent->mtime.tv_nsec = mtime_nsec;
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

   memset( path_ent, 0, sizeof(path_ent) );
}

// free a path
void ms_client_free_path( path_t* path, void (*free_cls)(void*) ) {
   for( unsigned int i = 0; i < path->size(); i++ ) {
      ms_client_free_path_ent( &path->at(i), free_cls );
   }
}

// get many downloads at once
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
               else if( http_status != 200 ) {
                  errorf("MS HTTP response code %ld for %s\n", http_status, downloads[i].url );
                  if( http_status == 404 ) {
                     rc = -ENOENT;
                  }
                  else if( http_status == 403 ) {
                     rc = -EACCES;
                  }
                  else if( http_status == 202 ) {
                     // parse response
                     char* buf = response_buffer_to_string( downloads[i].rb );
                     char* tmp = NULL;
                     long resp = strtol( buf, &tmp, 10 );
                     if( tmp == buf ) {
                        errorf("%s", "Inintelligable response from MS\n");
                        rc = -EREMOTEIO;
                     }
                     else {
                        rc = -abs(resp);
                     }
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
int ms_client_get_listings( struct ms_client* client, uint64_t volume_id, path_t* path, ms_response_t* ms_response ) {

   unsigned int num_downloads = path->size();

   if( num_downloads == 0 ) {
      // everything's fresh
      return 0;
   }
   
   // fetch concurrently--set up downloads
   struct ms_download_context* path_downloads = CALLOC_LIST( struct ms_download_context, num_downloads );
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      struct ms_path_ent* path_ent = &path->at(i);
      ms_client_init_download( client, &path_downloads[i], volume_id, path_ent->file_id, path_ent->version, path_ent->mtime.tv_sec, path_ent->mtime.tv_nsec );
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
      
      // parse
      ms::ms_reply reply;
      rc = ms_client_parse_reply( &reply, buf, buf_len );
      if( rc != 0 ) {
         errorf("ms_client_parse_reply(%s) rc = %d\n", path_downloads[di].url, rc );
         rc = -EIO;
         ms_client_free_response( ms_response );
         free( buf );
         break;
      }
      
      // extract versioning information from the reply
      ms_client_process_header( client, volume_id, reply.volume_version(), reply.ug_version(), reply.rg_version(), reply.ag_version() );
      
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


// attempt to become the acting owner of an MSEntry if we can't reach its owner
int ms_client_claim( struct ms_client* client, char const* path ) {
   // TODO
   return -EACCES;
}

// get a copy of the RG URLs
char** ms_client_RG_urls_copy( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return NULL;
   }
   
   char** urls = CALLOC_LIST( char*, vol->num_RG_urls + 1 );
   for( int i = 0; i < vol->num_RG_urls; i++ ) {
      urls[i] = strdup( vol->RG_urls[i] );
   }

   ms_client_view_unlock( client );

   return urls;
}

// get the current view version
uint64_t ms_client_volume_version( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }
   
   uint64_t ret = vol->volume_version;
   ms_client_view_unlock( client );
   return ret;
}

// get the current UG version
uint64_t ms_client_UG_version( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }
   
   uint64_t ret = vol->UG_version;
   ms_client_view_unlock( client );
   return ret;
}

// get the current RG version
uint64_t ms_client_RG_version( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }
   
   uint64_t ret = vol->RG_version;
   ms_client_view_unlock( client );
   return ret;
}

// get the current AG version
uint64_t ms_client_AG_version( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }
   
   uint64_t ret = vol->AG_version;
   ms_client_view_unlock( client );
   return ret;
}


// get the Volume ID
uint64_t ms_client_get_volume_id( struct ms_client* client, int i ) {
   ms_client_view_rlock( client );

   if( i < 0 || i >= client->num_volumes ) {
      ms_client_view_unlock( client );
      return -EINVAL;
   }
   
   uint64_t ret = client->volumes[i]->volume_id;

   ms_client_view_unlock( client );
   return ret;
}

// get the Volume name
char* ms_client_get_volume_name( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return NULL;
   }

   char* ret = strdup( vol->name );
   
   ms_client_view_unlock( client );
   return ret;
}

// get the blocking factor
uint64_t ms_client_get_volume_blocksize( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }

   uint64_t ret = vol->blocksize;

   ms_client_view_unlock( client );
   return ret;
}


// is this ID an AG ID?
bool ms_client_is_AG( struct ms_client* client, uint64_t volume_id, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }

   bool ret = false;
   
   // find this gateway
   for( int i = 0; i < vol->num_AGs; i++ ) {
      if( vol->AG_infos[i]->gateway_id == ag_id ) {
         ret = true;
         break;
      }
   }

   ms_client_view_unlock( client );

   return ret;
}

// get an AG's block size
uint64_t ms_client_get_AG_blocksize( struct ms_client* client, uint64_t volume_id, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }

   uint64_t ret = 0;
   
   // find this gateway
   for( int i = 0; i < vol->num_AGs; i++ ) {
      if( vol->AG_infos[i]->gateway_id == ag_id ) {
         ret = vol->AG_infos[i]->blocksize;
         break;
      }
   }

   ms_client_view_unlock( client );

   if( ret == 0 ) {
      errorf("No such AG %" PRIu64 "\n", ag_id );
   }

   return ret;
}

char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t volume_id, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }

   char* ret = NULL;

   // find this gateway
   for( int i = 0; i < vol->num_AGs; i++ ) {
      if( vol->AG_infos[i]->gateway_id == ag_id ) {
         ret = CALLOC_LIST( char, strlen("http://") + strlen(vol->AG_infos[i]->hostname) + 1 + 7 + 1 );
         sprintf( ret, "http://%s:%d/", vol->AG_infos[i]->hostname, vol->AG_infos[i]->portnum );
         break;
      }
   }

   ms_client_view_unlock( client );

   if( ret == NULL ) {
      errorf("No such AG %" PRIu64 "\n", ag_id );
   }

   return ret;
}


uint64_t ms_client_get_num_files( struct ms_client* client, uint64_t volume_id ) {
   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return 0;
   }

   uint64_t num_files = vol->num_files;

   ms_client_view_unlock( client );

   return num_files;
}


// get the number of Volumes
int ms_client_get_num_volumes( struct ms_client* client ) {
   ms_client_view_rlock( client );
   int ret = client->num_volumes;
   ms_client_view_unlock( client );
   return ret;
}

// get a UG url
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id ) {
   ms_client_view_rlock( client );

   // is this us?
   if( gateway_id == client->gateway_id ) {
      char* ret = strdup( client->conf->content_url );
      ms_client_view_unlock( client );
      return ret;
   }

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return NULL;
   }

   char* ret = NULL;

   for( int i = 0; i < vol->num_UG_creds; i++ ) {
      if( vol->UG_creds[i]->gateway_id == gateway_id ) {
         // build the URL
         ret = CALLOC_LIST( char, strlen("http://") + strlen(vol->UG_creds[i]->hostname) + 1 + 7 + 1 );
         sprintf( ret, "http://%s:%d/", vol->UG_creds[i]->hostname, vol->UG_creds[i]->portnum );
         break;
      }
   }

   if( ret == NULL ) {
      errorf("No such Gateway %" PRIu64 "\n", gateway_id );
   }

   ms_client_view_unlock( client );
   
   return ret;
}

// get a root structure 
int ms_client_get_volume_root( struct ms_client* client, uint64_t volume_id, struct md_entry* root ) {
   int rc = 0;

   ms_client_view_rlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   if( vol->root == NULL ) {
      ms_client_view_unlock( client );
      return -ENODATA;
   }
   
   memset( root, 0, sizeof(struct md_entry) );
   md_entry_dup2( vol->root, root );

   ms_client_view_unlock( client );

   return rc;
}

// schedule a Volume reload
int ms_client_sched_volume_reload( struct ms_client* client, uint64_t volume_id ) {
   int rc = 0;
   
   ms_client_view_wlock( client );

   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   vol->early_reload = true;
   
   ms_client_view_unlock( client );
   return rc;
}


// extract versioning information from the reply
int ms_client_process_header( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t ug_version, uint64_t rg_version, uint64_t ag_version ) {
   int rc = 0;
   
   ms_client_view_rlock( client );
   
   struct ms_volume* vol = ms_client_find_volume( client, volume_id );
   if( vol == NULL ) {
      errorf("No such Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // TODO: more efficient reload process
   if( vol->volume_version < volume_version )
      vol->early_reload = true;
   
   if( vol->UG_version < ug_version )
      vol->early_reload = true;
   
   if( vol->RG_version < rg_version )
      vol->early_reload = true;
   
   if( vol->AG_version < ag_version )
      vol->early_reload = true;
   
   ms_client_view_unlock( client );
   return rc;
}
   


