#include "validator.h"

pthread_t validate_thread;
bool running = true;

// ping a gateway to verify that this entry still exists
bool validate( char* path, void* arg ) {

   // skip directories
   if( MD_ENTRY_PATH_ISDIR( path ) )
      return true;

   
   // get the metadata entry 
   struct md_entry ent;
   memset( &ent, 0, sizeof(ent) );

   int status = 0;
   
   int rc = md_read_entry( "/", path, &ent );
   if( rc != 0 ) {
      errorf( "could not read %s\n", path );
      return false;
   }

   // read it
   CURL* curl_h = (CURL*)arg;

   curl_easy_setopt( curl_h, CURLOPT_URL, ent.url );
   rc = curl_easy_perform( curl_h );
   if( rc != 0 ) {
      // error
      errorf( "could not stat %s, rc = %d\n", ent.url, rc );
      status = -ENOENT;
   }

   if( status != 0 ) {
      // remove from the metadata
      md_remove_mc_entry( (char*)"/", &ent );
   }

   md_entry_free( &ent );

   // don't process this entry; we're done
   return false;
}

void* validator_run( void* arg ) {
   // continuously walk the master copy and verify that each gateway still has the files it claims it has
   struct md_syndicate_conf* conf = (struct md_syndicate_conf*)arg;
   
   while( running ) {
      CURL* curl_h = curl_easy_init();

      curl_easy_setopt( curl_h, CURLOPT_NOPROGRESS, 1L );   // no progress bar
      curl_easy_setopt( curl_h, CURLOPT_NOSIGNAL, 1L );
      curl_easy_setopt( curl_h, CURLOPT_USERAGENT, "Syndicate-agent/1.0");
      curl_easy_setopt( curl_h, CURLOPT_FOLLOWLOCATION, 1L );
      curl_easy_setopt( curl_h, CURLOPT_FILETIME, 1L );
      curl_easy_setopt( curl_h, CURLOPT_CONNECTTIMEOUT, conf->metadata_connect_timeout );
      curl_easy_setopt( curl_h, CURLOPT_TIMEOUT, conf->transfer_timeout );
      curl_easy_setopt( curl_h, CURLOPT_NOBODY, 1L );

      // walk the master copy
      struct md_entry** dumb = md_walk_fs_dir( conf->master_copy_root, true, false, validate, curl_h, NULL, NULL );
      md_entry_free_all( dumb );
      free( dumb );

      curl_easy_cleanup( curl_h );

      sleep(60);
   }

   return NULL;
}

int validator_init( struct md_syndicate_conf* conf ) {
   dbprintf("%s", "Starting validator thread\n");
   validate_thread = md_start_thread( validator_run, conf, false );
   return 0;
}

int validator_shutdown() {
   dbprintf("%s", "Stopping validator thread\n");
   running = false;
   pthread_cancel( validate_thread );
   pthread_join( validate_thread, NULL );
   return 0;
}
