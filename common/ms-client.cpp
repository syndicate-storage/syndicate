/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "ms-client.h"

static void* ms_client_uploader_thread( void* arg );
static void ms_client_uploader_signal( struct ms_client* client );
static size_t ms_client_header_func( void *ptr, size_t size, size_t nmemb, void *userdata);

// create an MS client context
int ms_client_init( struct ms_client* client, struct md_syndicate_conf* conf, char const* volume_name, char const* username, char const* passwd ) {

   memset( client, 0, sizeof(struct ms_client) );
   
   // configure the HTTP(s) streams to the MS
   client->ms_read = curl_easy_init();
   client->ms_write = curl_easy_init();

   client->url = strdup( conf->metadata_url );
   char* tmp = md_fullpath( client->url, "/FILE/", NULL);
   char* tmp2 = md_fullpath( tmp, volume_name, NULL );
   client->file_url = md_fullpath( tmp2, "/", NULL );    // must end in /
   free( tmp );
   free( tmp2 );
   
   md_init_curl_handle( client->ms_read, client->file_url, conf->metadata_connect_timeout);
   md_init_curl_handle( client->ms_write, client->file_url, conf->metadata_connect_timeout );

   curl_easy_setopt( client->ms_write, CURLOPT_POST, 1L);
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );

   curl_easy_setopt( client->ms_read, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( client->ms_write, CURLOPT_SSL_VERIFYPEER, (conf->verify_peer ? 1L : 0L) );

   curl_easy_setopt( client->ms_read, CURLOPT_HEADERFUNCTION, ms_client_header_func );
   curl_easy_setopt( client->ms_write, CURLOPT_HEADERFUNCTION, ms_client_header_func );

   curl_easy_setopt( client->ms_read, CURLOPT_WRITEHEADER, &client->read_times );
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEHEADER, &client->write_times );
   
   curl_easy_setopt( client->ms_read, CURLOPT_NOSIGNAL, 1L );
   curl_easy_setopt( client->ms_write, CURLOPT_NOSIGNAL, 1L );

   client->userpass = NULL;
   
   if( username && passwd ) {
      // we have authentication tokens!
      curl_easy_setopt( client->ms_read, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
      curl_easy_setopt( client->ms_write, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC );
      
      client->userpass = CALLOC_LIST( char, strlen(username) + 1 + strlen(passwd) + 1 );
      sprintf( client->userpass, "%s:%s", username, passwd );

      curl_easy_setopt( client->ms_read, CURLOPT_USERPWD, client->userpass );
      curl_easy_setopt( client->ms_write, CURLOPT_USERPWD, client->userpass );
   }

   pthread_rwlock_init( &client->lock, NULL );

   client->updates = new update_set();
   client->conf = conf;
   client->deadlines = new deadline_queue();

   // uploader thread
   client->running = true;
   pthread_mutex_init( &client->uploader_lock, NULL );
   pthread_cond_init( &client->uploader_cv, NULL );

   client->uploader_thread = md_start_thread( ms_client_uploader_thread, client, false );
   if( client->uploader_thread < 0 ) {
      return client->uploader_thread;
   }

   return 0;
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

   pthread_join( client->uploader_thread, NULL );

   ms_client_wlock( client );

   pthread_mutex_destroy( &client->uploader_lock );
   pthread_cond_destroy( &client->uploader_cv );


   // clean up CURL
   curl_easy_cleanup( client->ms_read );
   curl_easy_cleanup( client->ms_write );


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

   if( client->file_url )
      free( client->file_url );

   if( client->volume_secret )
      free( client->volume_secret );
   
   delete client->updates;
   delete client->deadlines;
   
   ms_client_unlock( client );
   pthread_rwlock_destroy( &client->lock );

   return 0;
}

// get the offset at which the value starts
static off_t ms_client_find_header_value( char* header_buf, size_t header_len, char const* header_name ) {

   if( strlen(header_name) >= header_len )
      return -1;      // header is too short

   if( strncmp(header_buf, header_name, strlen(header_name) ) != 0 )
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

   char* value_str = (char*)alloca( value_len + 1 );
   strcpy( value_str, value );

   uint64_t data = (uint64_t)strtoll( value_str, NULL, 10 );
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

   // is this one of our headers?  Find each of them
   
   off_t off = ms_client_find_header_value( data, len, HTTP_VOLUME_TIME );
   if( off > 0 ) {
      times->volume_time = ms_client_read_one_value( data, off, size );
      return len;
   }
   
   off = ms_client_find_header_value( data, len, HTTP_UG_TIME );
   if( off > 0 ) {
      times->ug_time = ms_client_read_one_value( data, off, size );
      return len;
   }

   off = ms_client_find_header_value( data, len, HTTP_TOTAL_TIME );
   if( off > 0 ) {
      times->total_time = ms_client_read_one_value( data, off, size );
      return len;
   }

   off = ms_client_find_header_value( data, len, HTTP_RESOLVE_TIME );
   if( off > 0 ) {
      times->resolve_time = ms_client_read_one_value( data, off, size );
      return len;
   }

   off = ms_client_find_header_value( data, len, HTTP_CREATE_TIMES );
   if( off > 0 ) {
      times->create_times = ms_client_read_multi_values( data, off, size, &times->num_create_times );
      return len;
   }

   off = ms_client_find_header_value( data, len, HTTP_UPDATE_TIMES );
   if( off > 0 ) {
      times->update_times = ms_client_read_multi_values( data, off, size, &times->num_update_times );
      return len;
   }

   off = ms_client_find_header_value( data, len, HTTP_DELETE_TIMES );
   if( off > 0 ) {
      times->delete_times = ms_client_read_multi_values( data, off, size, &times->num_delete_times );
      return len;
   }

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
   struct ms_client* client = (struct ms_client*)arg;

   client->uploader_running = true;
   while( client->running ) {
      
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

// get volume metadata
int ms_client_get_volume_metadata( struct ms_client* client, char const* volume_name, char const* password, uint64_t* version, uid_t* my_owner_id, uid_t* volume_owner_id, gid_t* volume_id, char*** replica_urls, uint64_t* blocksize, struct md_user_entry*** user_gateways ) {

   // sanity check
   if( volume_name == NULL) {
      errorf("%s", "Missing volume name\n");
      return -EINVAL;
   }

   if( password == NULL ) {
      errorf("%s", "Missing volume secret\n");
      return -EINVAL;
   }
   
   ms::ms_volume_metadata volume_md;

   char* volume_md_path = md_fullpath( "/VOLUME/", volume_name, NULL );
   char* volume_url = md_fullpath( client->url, volume_md_path, NULL );

   free( volume_md_path );

   char* bits = NULL;
   ssize_t len = 0;
   long http_response = 0;

   // custom volume secret header
   char* volume_secret_header = CALLOC_LIST( char, strlen(HTTP_VOLUME_SECRET) + 2 + strlen(password) + 1 );
   sprintf(volume_secret_header, "%s: %s", HTTP_VOLUME_SECRET, password);
   
   struct curl_slist *headers = NULL;
   headers = curl_slist_append( headers, volume_secret_header );
   free( volume_secret_header );
   
   // synchronously fetch the entry
   ms_client_wlock_backoff( client, &client->downloading );
   
   client->downloading = true;
   
   curl_easy_setopt( client->ms_read, CURLOPT_URL, volume_url );
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, headers );

   ms_client_unlock( client );

   // do the download
   memset( &client->read_times, 0, sizeof(client->read_times) );
   len = md_download_file5( client->ms_read, &bits );

   ms_client_wlock( client );

   DATA( HTTP_VOLUME_TIME, (double)client->read_times.volume_time / 1e9 );
   DATA( HTTP_UG_TIME, (double)client->read_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->read_times.total_time / 1e9 );

   // not downloading anymore
   client->downloading = false;
   
   curl_easy_setopt( client->ms_read, CURLOPT_URL, NULL );
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, NULL );
   curl_easy_getinfo( client->ms_read, CURLINFO_RESPONSE_CODE, &http_response );

   ms_client_unlock( client );

   curl_slist_free_all( headers );

   if( len < 0 ) {
      errorf("md_download_file5(%s) rc = %zd\n", volume_url, len );
      free( volume_url );
      return (int)len;
   }

   if( http_response != 200 ) {
      errorf("bad MS HTTP response %ld\n", http_response);
      free( volume_url );
      return -http_response;
   }
   
   // got the data
   bool valid = volume_md.ParseFromString( string(bits, len) );
   free( bits );
   
   if( !valid ) {
      errorf( "invalid volume metadata from %s\n", volume_url );
      free( volume_url );
      return -EINVAL;
   }

   free( volume_url );

   *replica_urls = CALLOC_LIST( char*, volume_md.replica_urls_size() + 1 );
   
   // extract
   for( int i = 0; i < volume_md.replica_urls_size(); i++ ) {
      (*replica_urls)[i] = strdup( volume_md.replica_urls(i).c_str() );
   }

   *version = volume_md.volume_version();
   *my_owner_id = volume_md.requester_id();
   *volume_owner_id = volume_md.owner_id();
   *volume_id = volume_md.volume_id();
   *blocksize = volume_md.blocksize();

   ms_client_wlock( client );
   client->volume_version = volume_md.volume_version();
   client->volume_secret = strdup( password );     // so we can ask again if the volume metadata version changes
   ms_client_unlock( client );

   struct md_user_entry** users = CALLOC_LIST( struct md_user_entry*, volume_md.user_gateway_creds_size() + 1 );
   for( int i = 0; i < volume_md.user_gateway_creds_size(); i++ ) {
      struct md_user_entry* uent = CALLOC_LIST( struct md_user_entry, 1 );
      
      uent->uid = volume_md.user_gateway_creds(i).owner_id();
      uent->username = strdup( volume_md.user_gateway_creds(i).username().c_str() );
      uent->password_hash = strdup( volume_md.user_gateway_creds(i).password_hash().c_str() );

      users[i] = uent;
   }

   *user_gateways = users;

   return 0;
}


// read-lock a client context 
int ms_client_rlock( struct ms_client* client ) {
   return pthread_rwlock_rdlock( &client->lock );
}

// write-lock a client context 
int ms_client_wlock( struct ms_client* client ) {
   return pthread_rwlock_wrlock( &client->lock );
}

// unlock a client context 
int ms_client_unlock( struct ms_client* client ) {
   return pthread_rwlock_unlock( &client->lock );
}

// put an update to the client context
// return 0 on success
// return -EEXIST if the update already exists
static inline int ms_client_put_update( update_set* updates, deadline_queue* deadlines, long path_hash, struct md_update* update, uint64_t deadline ) {
   if( updates ) {
      memcpy( &((*updates)[ path_hash ]), update, sizeof(struct md_update) );
   }
   if( deadlines )
      (*deadlines)[deadline] = path_hash;
   
   return 0;
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
      up->timestamp = currentTimeMillis();
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
      up->timestamp = currentTimeMillis();
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

static void print_timings( uint64_t* timings, size_t num_timings, char const* hdr ) {
   if( num_timings > 0 ) {
      for( size_t i = 0; i < num_timings; i++ ) {
         DATA( hdr, (double)(timings[i]) / 1e9 );
      }
   }
}


// post data
// NOTE: client must be write-locked before sending this!
static int ms_client_send( struct ms_client* client, char const* data, size_t len ) {
   struct curl_httppost *post = NULL, *last = NULL;
   int rc = 0;
   response_buffer_t* rb = new response_buffer_t();

   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, data, CURLFORM_BUFFERLENGTH, len, CURLFORM_END );

   // lock, but back off if someone else is uploading
   ms_client_wlock_backoff( client, &client->uploading );

   client->uploading = true;

   curl_easy_setopt( client->ms_write, CURLOPT_URL, client->file_url );
   curl_easy_setopt( client->ms_write, CURLOPT_WRITEDATA, (void*)rb );
   curl_easy_setopt( client->ms_write, CURLOPT_HTTPPOST, post );
   
   ms_client_unlock( client );

   // do the upload
   struct timespec ts, ts2;
   BEGIN_TIMING_DATA( ts );
   
   memset( &client->write_times, 0, sizeof(client->write_times) );
   rc = curl_easy_perform( client->ms_write );

   END_TIMING_DATA( ts, ts2, "MS send" );
   
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
   DATA( HTTP_UG_TIME, (double)client->write_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->write_times.total_time / 1e9 );
   
   client->uploading = false;

   
   // get the results
   int http_response = 0;
   curl_easy_getinfo( client->ms_write, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( client->ms_write, CURLOPT_URL, NULL );
   curl_easy_setopt( client->ms_write, CURLOPT_HTTPPOST, NULL );

   // what happened?
   if( rc != 0 ) {
      // curl failed
      errorf( "curl_easy_perform rc = %d\n", rc );
      
      response_buffer_free( rb );
   }
   else if( http_response == 200 ) {
      // we're good!
      response_buffer_free( rb );
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
      response_buffer_free( rb );
   }
   else {
      // some other HTTP code
      rc = -http_response;
      response_buffer_free( rb );
   }

   delete rb;
   curl_formfree( post );
   
   return rc;
}


// post a record on the MS, synchronously
static int ms_client_post( struct ms_client* client, int op, struct md_entry* ent ) {
   struct md_update up;
   up.op = op;
   up.timestamp = currentTimeMillis();
   memcpy( &up.ent, ent, sizeof(struct md_entry) );
   
   // send the update
   struct md_update* update_list[] = {
      &up,
      NULL
   };

   char* update_text = NULL;
   ssize_t update_text_len = md_metadata_update_text( client->conf, &update_text, update_list );

   if( update_text_len < 0 ) {
      errorf("md_metadata_update_text rc = %zd\n", update_text_len );
      return (int)update_text_len;
   }

   ms_client_wlock( client );
   int rc = ms_client_send( client, update_text, update_text_len );
   ms_client_unlock( client );

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


// iterator data and method for serializing an update_set's worth of updates
struct update_set_iterator_data {
   update_set* updates;
   update_set::iterator itr;
};

static struct md_update* update_set_iterator( void* arg ) {
   struct update_set_iterator_data* itrdata = (struct update_set_iterator_data*)arg;

   if( itrdata->itr == itrdata->updates->end() )
      return NULL;

   struct md_update* ret = &itrdata->itr->second;

   dbprintf("update(path=%s, url=%s)\n", ret->ent.path, ret->ent.url);
   
   itrdata->itr++;
   return ret;
}

static ssize_t serialize_update_set( struct md_syndicate_conf* conf, update_set* updates, char** buf ) {
   struct update_set_iterator_data itrdata;
   itrdata.updates = updates;
   itrdata.itr = updates->begin();

   return md_metadata_update_text3( conf, buf, update_set_iterator, &itrdata );
}



// NOTE: client must be write-locked before calling this!
static int ms_client_send_updates( struct ms_client* client, update_set* updates ) {

   int rc = 0;
   
   // don't do anything if we have nothing to do
   if( updates->size() == 0 ) {
      // nothing to do
      return 0;
   }

   // otherwise, upload the updates.
   char* update_text = NULL;
   ssize_t update_text_len = serialize_update_set( client->conf, updates, &update_text );

   if( update_text_len < 0 ) {
      errorf("serialize_update_set rc = %zd\n", update_text_len );
      return (int)update_text_len;
   }

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
   
   ms_client_wlock( client );

   long path_hash = md_hash( path );
   uint64_t old_deadline = 0;

   if( client->updates->count( path_hash ) != 0 ) {
      // clear it out
      rc = ms_client_remove_update( client, path_hash, &update, &old_deadline );
   }
   else {
      rc = -ENOENT;
   }

   if( rc != 0 ) {
      // error
      ms_client_unlock( client );
      return rc;
   }

   update_set updates;
   memcpy( &(updates[ path_hash ]), &update, sizeof(struct md_update) );
   
   rc = ms_client_send_updates( client, &updates );
   
   // if we failed, then put the updates back and try again later
   if( rc != 0 ) {
      ms_client_put_update( client->updates, client->deadlines, path_hash, &update, old_deadline );
   }

   // done with the client
   ms_client_unlock( client );

   // free memory if it is no longer needed
   if( rc == 0 ) {
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

   if( updates.size() > 0 ) {
      rc = ms_client_send_updates( client, &updates );
   }
   
   // if we failed, then put the updates back and try again later
   if( rc != 0 ) {
      for( update_set::iterator itr = updates.begin(); itr != updates.end(); itr++ ) {
         ms_client_put_update( client->updates, client->deadlines, itr->first, &itr->second, old_deadlines[itr->first] );
      }
   }
         
   // done with the client
   ms_client_unlock( client );
   
   // if we succeeded, free the posted updates (they're no longer present in the client context)
   if( rc == 0 ) {
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

   char* md_bits = NULL;
   long http_response = 0;

   struct md_syndicate_conf* conf = client->conf;

   // buffer the lastmod
   char buf[256];
   sprintf(buf, "%s: %ld.%ld", HTTP_MS_LASTMOD, lastmod->tv_sec, lastmod->tv_nsec );
   
   // synchronously fetch the entry, backing off if someone else is downloading
   ms_client_wlock_backoff( client, &client->downloading );

   client->downloading = true;
   curl_easy_setopt( client->ms_read, CURLOPT_URL, md_url );

   // add our lastmod header
   struct curl_slist *lastmod_header = NULL;
   lastmod_header = curl_slist_append( lastmod_header, buf );

   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, lastmod_header );
                     
   ms_client_unlock( client );

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   memset( &client->read_times, 0, sizeof(client->read_times) );
   ssize_t len = md_download_file5( client->ms_read, &md_bits );

   END_TIMING_DATA( ts, ts2, "MS recv" );

   ms_client_wlock( client );

   // remove header
   curl_easy_setopt( client->ms_read, CURLOPT_HTTPHEADER, NULL );

   curl_slist_free_all( lastmod_header );
   
   // parse MS timing headers
   DATA( HTTP_RESOLVE_TIME, (double)client->read_times.resolve_time / 1e9 );
   DATA( HTTP_RESOLVE_TIME, (double)client->read_times.resolve_time / 1e9 );
   DATA( HTTP_VOLUME_TIME, (double)client->read_times.volume_time / 1e9 );
   DATA( HTTP_UG_TIME, (double)client->read_times.ug_time / 1e9 );
   DATA( HTTP_TOTAL_TIME, (double)client->read_times.total_time / 1e9 );

   // not downloading anymore
   client->downloading = false;
   
   // get CURL status
   curl_easy_getinfo( client->ms_read, CURLINFO_RESPONSE_CODE, &http_response );
   
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
            
            int rc = ms_entry_to_md_entry( conf, entry, &ent );
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
            
            int rc = ms_entry_to_md_entry( conf, entry, &ent );
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

