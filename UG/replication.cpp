/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "replication.h"

int fs_entry_replicate_wait_and_free( vector<struct replica_context*>* rctxs, struct timespec* timeout );

static struct syndicate_replication global_replication;

// set up a replica context
int replica_context_init( struct replica_context* rctx, int type, int op, FILE* block_data, char* manifest_data, off_t data_len, struct curl_httppost* form_data, uint64_t file_id, bool sync ) {
   memset( rctx, 0, sizeof(struct replica_context) );
   
   if( type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
      rctx->data = manifest_data;
   }
   else if( type == REPLICA_CONTEXT_TYPE_BLOCK ) {
      rctx->file = block_data;
   }
   else {
      return -EINVAL;
   }
   
   rctx->curls = new vector<CURL*>();
   rctx->type = type;
   rctx->op = op;
   rctx->size = data_len;
   rctx->form_data = form_data;
   rctx->sync = sync;
   rctx->file_id = file_id;
   
   sem_init( &rctx->processing_lock, 0, 1 );
   
   return 0;
}


// free a replica context
int replica_context_free( struct replica_context* rctx ) {
   dbprintf("free replica %p\n", rctx);
   if( rctx->type == REPLICA_CONTEXT_TYPE_BLOCK ) {
      if( rctx->file ) {
         fclose( rctx->file );
         rctx->file = NULL;
      }
   }
   else if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
      if( rctx->data ) {
         free( rctx->data );
         rctx->data = NULL;
      }
   }
   
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      if( rctx->curls->at(i) != NULL ) {
         curl_easy_cleanup( rctx->curls->at(i) );
      }
   }
   
   delete rctx->curls;
   
   if( rctx->form_data ) {
      curl_formfree( rctx->form_data );
      rctx->form_data = NULL;
   }
   
   sem_destroy( &rctx->processing_lock );
   
   memset( rctx, 0, sizeof( struct replica_context ) );
   
   return 0;
}


// create metadata form field for posting a request info structure
int replica_add_metadata_form( struct curl_httppost** form_data, struct curl_httppost** last, ms::ms_gateway_request_info* replica_info ) {
   // serialize to string
   string replica_info_str;
   
   bool src = replica_info->SerializeToString( &replica_info_str );
   if( !src ) {
      errorf("%s", "Failed to serialize data\n" );
      return -EINVAL;
   }
   
   // build up the form to submit to the RG
   curl_formadd( form_data, last,  CURLFORM_COPYNAME, "metadata",
                                   CURLFORM_CONTENTSLENGTH, replica_info_str.size(),
                                   CURLFORM_COPYCONTENTS, replica_info_str.c_str(),
                                   CURLFORM_CONTENTTYPE, "application/octet-stream",
                                   CURLFORM_END );

   return 0;
}


// create data form field for posting manifest data
int replica_add_data_form( struct curl_httppost** form_data, struct curl_httppost** last, char const* data, size_t len ) {
   curl_formadd( form_data, last, CURLFORM_COPYNAME, "data",
                                    CURLFORM_PTRCONTENTS, data,
                                    CURLFORM_CONTENTSLENGTH, len,
                                    CURLFORM_CONTENTTYPE, "application/octet-stream",
                                    CURLFORM_END );
   
   return 0;
}


// create data form field for posting block data
int replica_add_data_form( struct curl_httppost** form_data, struct curl_httppost** last, FILE* f, off_t size ) {
   curl_formadd( form_data, last, CURLFORM_COPYNAME, "data",
                                    CURLFORM_FILENAME, "block",         // make this look like a file upload
                                    CURLFORM_STREAM, f,
                                    CURLFORM_CONTENTSLENGTH, (long)size,
                                    CURLFORM_CONTENTTYPE, "application/octet-stream",
                                    CURLFORM_END );
   
   return 0;
}

// populate a ms_gateway_request_info structure with data
int replica_populate_request( ms::ms_gateway_request_info* replica_info, struct fs_core* core, int request_type, struct fs_entry* fent, uint64_t block_id, uint64_t block_version, off_t size, unsigned char const* hash, size_t hash_len ) {
   
   replica_info->set_type( request_type );
   replica_info->set_file_version( fent->version );
   replica_info->set_block_id( block_id );
   replica_info->set_block_version( block_version );
   replica_info->set_size( size );
   replica_info->set_file_mtime_sec( fent->mtime_sec );
   replica_info->set_file_mtime_nsec( fent->mtime_nsec );
   replica_info->set_file_id( fent->file_id );
   replica_info->set_owner( fent->owner );
   replica_info->set_writer( core->gateway );
   replica_info->set_volume( fent->volume );
   
   
   char* b64hash = NULL;
   int rc = Base64Encode( (char*)hash, hash_len, &b64hash );
   if( rc != 0 ) {
      errorf("Base64Encode rc = %d\n", rc );
      return -EINVAL;
   }
   
   replica_info->set_hash( string(b64hash) );
   
   free( b64hash );
   
   return 0;
}


// create a manifest replica context
// fent must be at least read-locked
int replica_context_manifest( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, bool sync ) {
   
   // get the manifest data
   char* manifest_data = NULL;
   ssize_t manifest_data_len = 0;
   int rc = 0;
   
   manifest_data_len = fs_entry_serialize_manifest( core, fent, &manifest_data, false );
   if( manifest_data_len < 0 ) {
      errorf("fs_entry_serialize_manifest(%" PRIX64 ") rc = %zd\n", fent->file_id, manifest_data_len);
      return -EINVAL;
   }
   
   // hash the manifest
   unsigned char* hash = sha256_hash_data( manifest_data, manifest_data_len );
   size_t hash_len = sha256_len();
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, core, ms::ms_gateway_request_info::MANIFEST, fent, 0, 0, manifest_data_len, hash, hash_len );
   
   free( hash );
   
   if( rc != 0 ) {
      errorf("replica_populate_request rc = %d\n", rc );
      free( manifest_data );
      return -EINVAL;
   }
   
   rc = md_sign< ms::ms_gateway_request_info >( core->ms->my_key, &replica_info );
   if( rc != 0 ) {
      errorf("md_sign rc = %d\n", rc );
      free( manifest_data );
      return -EINVAL;
   }
   
   // build up the form to submit to the RG
   struct curl_httppost* form_data = NULL;
   struct curl_httppost* last = NULL;
   
   // metadata form
   rc = replica_add_metadata_form( &form_data, &last, &replica_info );
   if( rc != 0 ) {
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      free( manifest_data );
      return -EINVAL;
   }
   
   // data form
   rc = replica_add_data_form( &form_data, &last, manifest_data, manifest_data_len );
   if( rc != 0 ) {
      errorf("replica_add_data_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      free( manifest_data );
      
      return -EINVAL;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_POST, NULL, manifest_data, manifest_data_len, form_data, fent->file_id, sync );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      free( manifest_data );
      
      return -EINVAL;
   }
   
   return 0;
}


// create a block replica context
// fent must be read-locked
int replica_context_block( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, uint64_t block_id, struct fs_entry_block_info* block_info, bool sync ) {
   
   // attempt to open the file
   char* local_block_url = NULL;
   bool staging = !FS_ENTRY_LOCAL( core, fent );

   if( staging ) {
      local_block_url = fs_entry_local_staging_block_url( core, fent->file_id, fent->version, block_id, block_info->version );
   }
   else {
      local_block_url = fs_entry_local_block_url( core, fent->file_id, fent->version, block_id, block_info->version );
   }

   char* local_path = GET_PATH( local_block_url );
   
   FILE* f = fopen( local_path, "r" );
   if( f == NULL ) {
      int errsv = -errno;
      errorf( "fopen(%s) errno = %d\n", local_path, errsv );
      free( local_block_url );
      return errsv;
   }
   
   free( local_block_url );

   // stat this to get its size
   struct stat sb;
   int rc = fstat( fileno(f), &sb );
   if( rc != 0 ) {
      int errsv = -errno;
      errorf( "fstat errno = %d\n", errsv );
      fclose( f );
      
      return errsv;
   }

   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, core, ms::ms_gateway_request_info::BLOCK, fent, block_id, block_info->version, sb.st_size, block_info->hash, block_info->hash_len );
   if( rc != 0 ) {
      errorf("replica_populate_request rc = %d\n", rc );
      fclose( f );
      
      return -EINVAL;
   }
   
   rc = md_sign< ms::ms_gateway_request_info >( core->ms->my_key, &replica_info );
   if( rc != 0 ) {
      errorf("md_sign rc = %d\n", rc );
      fclose( f );
      return -EINVAL;
   }

   // build request
   struct curl_httppost* last = NULL;
   struct curl_httppost* form_data = NULL;

   rc = replica_add_metadata_form( &form_data, &last, &replica_info );
   if( rc != 0 ) {
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      fclose( f );
      
      return rc;
   }
   
   rc = replica_add_data_form( &form_data, &last, f, sb.st_size );
   if( rc != 0 ) {
      errorf("replica_add_data_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      fclose( f );
      
      return rc;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_POST, f, NULL, sb.st_size, form_data, fent->file_id, sync );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      fclose( f );
      
      return -EINVAL;
   }
   
   return 0;
}


// delete a manifest
int replica_context_delete_manifest( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, bool sync ) {
   int rc = 0;
   
   // put random bits into the hash field, for some cryptographic padding
   unsigned char fake_hash[256];
   for( unsigned int i = 0; i < (256 / sizeof(uint32_t)); i++ ) {
      uint32_t random_bits = CMWC4096();
      memcpy( fake_hash + (i * sizeof(uint32_t)), &random_bits, sizeof(uint32_t) );
   }
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, core, ms::ms_gateway_request_info::MANIFEST, fent, 0, 0, 0, fake_hash, 256 );
   if( rc != 0 ) {
      errorf("replica_populate_request rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_sign< ms::ms_gateway_request_info >( core->ms->my_key, &replica_info );
   if( rc != 0 ) {
      errorf("md_sign rc = %d\n", rc );
      return -EINVAL;
   }

   // build request
   struct curl_httppost* last = NULL;
   struct curl_httppost* form_data = NULL;

   rc = replica_add_metadata_form( &form_data, &last, &replica_info );
   if( rc != 0 ) {
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      return rc;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_DELETE, NULL, NULL, 0, form_data, fent->file_id, sync );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      
      return -EINVAL;
   }
   
   return 0;
}


// delete a block
int replica_context_delete_block( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, uint64_t block_id, int64_t block_version, bool sync ) {
   int rc = 0;
   
   // put random bits into the hash field, for some cryptographic padding
   unsigned char fake_hash[256];
   for( unsigned int i = 0; i < (256 / sizeof(uint32_t)); i++ ) {
      uint32_t random_bits = CMWC4096();
      memcpy( fake_hash + (i * sizeof(uint32_t)), &random_bits, sizeof(uint32_t) );
   }
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, core, ms::ms_gateway_request_info::BLOCK, fent, block_id, block_version, 0, fake_hash, 256 );
   if( rc != 0 ) {
      errorf("replica_populate_request rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_sign< ms::ms_gateway_request_info >( core->ms->my_key, &replica_info );
   if( rc != 0 ) {
      errorf("md_sign rc = %d\n", rc );
      return -EINVAL;
   }

   // build request
   struct curl_httppost* last = NULL;
   struct curl_httppost* form_data = NULL;

   rc = replica_add_metadata_form( &form_data, &last, &replica_info );
   if( rc != 0 ) {
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      return rc;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_DELETE, NULL, NULL, 0, form_data, fent->file_id, sync );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      
      return -EINVAL;
   }
   
   return 0;
}


static int old_still_running = -1;

// process curl
// synrp must be locked
int replica_multi_upload( struct syndicate_replication* synrp ) {
   int rc = 0;
   int still_running = 0;
   
   // process downloads
   struct timeval timeout;
   memset( &timeout, 0, sizeof(timeout) );
   
   fd_set fdread;
   fd_set fdwrite;
   fd_set fdexcep;
   int maxfd = -1;

   long curl_timeout = -1;

   FD_ZERO(&fdread);
   FD_ZERO(&fdwrite);
   FD_ZERO(&fdexcep);
   
   // how long until we should call curl_multi_perform?
   rc = curl_multi_timeout( synrp->running, &curl_timeout);
   if( rc != 0 ) {
      errorf("curl_multi_timeout rc = %d\n", rc );
      return -1;
   }
   
   if( curl_timeout < 0 ) {
      // no timeout given; wait a default amount
      timeout.tv_sec = 0;
      timeout.tv_usec = 10000;
   }
   else {
      timeout.tv_sec = 0;
      timeout.tv_usec = (curl_timeout % 1000) * 1000;
   }
   
   // get FDs
   rc = curl_multi_fdset( synrp->running, &fdread, &fdwrite, &fdexcep, &maxfd);
   
   if( rc != 0 ) {
      errorf("curl_multi_fdset rc = %d\n", rc );
      return -1;
   }
   
   // find out which FDs are ready
   rc = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);
   
   if( rc < 0 ) {
      // we have a problem
      errorf("select rc = %d, errno = %d\n", rc, -errno );
      return -1;
   }
   
   // let CURL do its thing
   
   do {
      rc = curl_multi_perform( synrp->running, &still_running );
      
      if( old_still_running <= 0 )
         old_still_running = still_running;
      
      if( old_still_running > 0 ) {
         dbprintf("still running = %d\n", still_running );
      }
      old_still_running = still_running;
      
      if( rc == CURLM_OK )
         break;
      
      dbprintf("%s\n", "tick!");
   } while( rc != CURLM_CALL_MULTI_PERFORM );
   
   // process messages
   return 0;
}


static int replica_remove_upload_context( struct syndicate_replication* synrp, CURL* curl ) {
   
   replica_upload_set::iterator itr = synrp->uploads->find( curl );
   if( itr != synrp->uploads->end() ) {
      
      struct replica_context* rctx = itr->second;
      
      int rc = 0;
      
      curl_multi_remove_handle( synrp->running, itr->first );
      synrp->uploads->erase( itr );
      
      // clear this curl
      int still_processing = 0;
      
      for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
         if( rctx->curls->at(i) == curl ) {
            curl_easy_cleanup( rctx->curls->at(i) );
            rctx->curls->at(i) = NULL;
         }
         
         if( rctx->curls->at(i) != NULL )
            still_processing ++;
      }
      
      // have all of this context's CURL handles finished?
      if( still_processing == 0 ) {      
         dbprintf("Finished %p\n", rctx );
         sem_post( &rctx->processing_lock );
      }
      
      return rc;
   }
   
   return -ENOENT;
}

// how did the transfers go?
// NOTE: synrp must be locked
int replica_process_responses( struct syndicate_replication* synrp ) {
   CURLMsg* msg = NULL;
   int msgs_left = 0;
   int rc = 0;

   do {
      msg = curl_multi_info_read( synrp->running, &msgs_left );

      if( msg == NULL )
         break;

      if( msg->msg == CURLMSG_DONE ) {
         // status of this handle...
         
         replica_upload_set::iterator itr = synrp->uploads->find( msg->easy_handle );
         if( itr != synrp->uploads->end() ) {
            
            struct replica_context* rctx = itr->second;
            
            // check status
            if( msg->data.result != 0 ) {
               // curl error
               rctx->error = -ENODATA;
               errorf("RG curl status %d\n", msg->data.result );
            }
            
            // check HTTP code
            long http_status = 0;
            int crc = curl_easy_getinfo( msg->easy_handle, CURLINFO_RESPONSE_CODE, &http_status );
            if( crc != 0 ) {
               rctx->error = -ENODATA;
            }
            else if( http_status != 200 ) {
               errorf("RG HTTP response code %ld\n", http_status );
               if( http_status == 404 ) {
                  rctx->error = -ENOENT;
               }
               else if( http_status == 403 ) {
                  rctx->error = -EACCES;
               }
               else {
                  rctx->error = -EREMOTEIO;
               }
            }
            
            dbprintf("Finished replicating %p (%s of %" PRIX64 ")\n", rctx, (rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ? "manifest" : "block"), rctx->file_id );
            
            replica_remove_upload_context( synrp, msg->easy_handle );
         }
      }
   } while( msg != NULL );
   
   return rc;
}


// main Replica SG upload thread
void* replica_main( void* arg ) {
   struct syndicate_replication* synrp = (struct syndicate_replication*)arg;
   
   int rc = 0;
   
   dbprintf("%s", "thread started\n");
   
   while( synrp->active ) {
      // run CURL for a bit
      pthread_mutex_lock( &synrp->running_lock );
      
      // do we have pending?
      if( synrp->has_pending ) {
         pthread_mutex_lock( &synrp->pending_lock );
         
         for( replica_upload_set::iterator itr = synrp->pending_uploads->begin(); itr != synrp->pending_uploads->end(); itr++ ) {
            (*synrp->uploads)[ itr->first ] = itr->second;
            curl_multi_add_handle( synrp->running, itr->first );
         }
         
         synrp->pending_uploads->clear();
         
         synrp->has_pending = false;
         pthread_mutex_unlock( &synrp->pending_lock );
      }
      
      rc = replica_multi_upload( synrp );
      
      if( rc != 0 ) {
         errorf("replica_multi_upload rc = %d\n", rc );
         pthread_mutex_unlock( &synrp->running_lock );
         break;
      }
      
      // find out what finished uploading
      rc = replica_process_responses( synrp );
      
      pthread_mutex_unlock( &synrp->running_lock );
      
      if( rc != 0 ) {
         errorf("replica_process_responses rc = %d\n", rc );
         break;
      }
   }
   
   dbprintf("%s", "thread shutdown\n" );
   
   return NULL;
}


// begin downloading
int replica_begin( struct syndicate_replication* rp, struct replica_context* rctx ) {
   
   // acquire this lock, since we're processing it now...
   sem_wait( &rctx->processing_lock );
   
   // find all RG urls
   char** rg_urls = ms_client_RG_urls( rp->ms, rp->ms->conf->verify_peer ? "https://" : "http://" );
   
   pthread_mutex_lock( &rp->pending_lock );
   
   for( int i = 0; rg_urls[i] != NULL; i++ ) {
      
      CURL* curl = curl_easy_init();
      rctx->curls->push_back( curl );
      
      dbprintf("%s %p (%s) to %s\n", (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx, (rctx->type == REPLICA_CONTEXT_TYPE_BLOCK ? "block" : "manifest"), rg_urls[i] );
      md_init_curl_handle( curl, rg_urls[i], rp->ms->conf->replica_connect_timeout );
      
      // prepare upload
      curl_easy_setopt( curl, CURLOPT_POST, 1L );
      curl_easy_setopt( curl, CURLOPT_HTTPPOST, rctx->form_data );
      
      // if we're deleting, change the method
      if( rctx->op == REPLICA_DELETE ) {
         curl_easy_setopt( curl, CURLOPT_CUSTOMREQUEST, "DELETE" );
      }
      
      //curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
      
      (*rp->pending_uploads)[ curl ] = rctx;
   }
   
   rp->has_pending = true;
   pthread_mutex_unlock( &rp->pending_lock );
   
   for( int i = 0; rg_urls[i] != NULL; i++ ) {
      free( rg_urls[i] );
   }
   free( rg_urls );
   
   return 0;
}


// wait for a (synchronous) replica context to finish.
// if the deadline (ts) is passed, remove the handle.
int replica_wait_and_remove( struct syndicate_replication* rp, struct replica_context* rctx, struct timespec* ts ) {
   if( ts == NULL ) {
      sem_wait( &rctx->processing_lock );
      
      // removed by replica_process_responses
      return 0;
   }
   else {
      struct timespec abs_ts;
      clock_gettime( CLOCK_REALTIME, &abs_ts );
      abs_ts.tv_sec += ts->tv_sec;
      abs_ts.tv_nsec += ts->tv_nsec;
      
      int rc = sem_timedwait( &rctx->processing_lock, &abs_ts );
      if( rc != 0 ) {
         rc = -errno;
         
         // Remove the associated handle ourselves
         pthread_mutex_lock( &rp->running_lock );
         
         for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
            replica_remove_upload_context( rp, rctx->curls->at(i) );
         }
         
         pthread_mutex_unlock( &rp->running_lock );
      }
      
      return rc;
   }
}


// initalize a syndicate replication instance
int replica_init_replication( struct syndicate_replication* rp, struct ms_client* client, uint64_t volume_id ) {
   pthread_mutex_init( &rp->running_lock, NULL );
   pthread_mutex_init( &rp->pending_lock, NULL );
   
   rp->running = curl_multi_init();
   
   rp->uploads = new replica_upload_set();
   rp->pending_uploads = new replica_upload_set();
   
   rp->active = true;
   
   rp->upload_thread = md_start_thread( replica_main, rp, false );
   if( rp->upload_thread < 0 ) {
      errorf("md_start_thread rc = %lu\n", rp->upload_thread );
      return rp->upload_thread;
   }
   
   rp->ms = client;
   rp->volume_id = volume_id;
   
   return 0;
}


// shut down a syndicate replication instance
int replica_shutdown_replication( struct syndicate_replication* rp ) {
   rp->active = false;
   
   pthread_cancel( rp->upload_thread );
   pthread_join( rp->upload_thread, NULL );
   
   int need_running_unlock = pthread_mutex_trylock( &rp->running_lock );
   
   for( replica_upload_set::iterator itr = rp->uploads->begin(); itr != rp->uploads->end(); itr++ ) {
      curl_multi_remove_handle( rp->running, itr->first );
      
      replica_context_free( itr->second );
      
      free( itr->second );
   }
   
   int need_pending_unlock = pthread_mutex_trylock( &rp->pending_lock );
   
   for( replica_upload_set::iterator itr = rp->pending_uploads->begin(); itr != rp->pending_uploads->end(); itr++ ) {
      
      replica_context_free( itr->second );
      
      free( itr->second );
   }
   
   delete rp->uploads;
   delete rp->pending_uploads;
   
   rp->uploads = NULL;
   rp->pending_uploads = NULL;
   
   if( need_running_unlock )
      pthread_mutex_unlock( &rp->pending_lock );
   
   if( need_pending_unlock )
      pthread_mutex_unlock( &rp->running_lock );
   
   pthread_mutex_destroy( &rp->pending_lock );
   pthread_mutex_destroy( &rp->running_lock );
   
   curl_multi_cleanup( rp->running );
   
   return 0;
}


// start up replication
int replication_init(struct ms_client* client, uint64_t volume_id) {
   int rc = replica_init_replication( &global_replication, client, volume_id );
   if( rc != 0 ) {
      errorf("replica_init_replication rc = %d\n", rc );
      return -ENOSYS;
   }
   
   return 0;
}


// shut down replication
int replication_shutdown() {
   int rc = replica_shutdown_replication( &global_replication );
   if( rc != 0 ) {
      errorf("replica_shutdown_replication rc = %d\n", rc );
      return -ENOSYS;
   }
   return 0;
}


// run a manifest replication
int replica_run_manifest_context( struct fs_core* core, struct replica_context* manifest_rctx, bool sync, struct fs_file_handle* fh ) {
   int rc = 0;
   
   // proceed to replicate
   rc = replica_begin( &global_replication, manifest_rctx );
   
   if( rc != 0 ) {
      errorf("replica_begin(%p) rc = %d\n", manifest_rctx, rc );
      
      replica_context_free( manifest_rctx );
      free( manifest_rctx );
      return rc;
   }
   
   // wait for this to finish?
   if( sync ) {
      
      struct timespec *tsp = NULL;
      if( core->conf->transfer_timeout > 0 ) {
         struct timespec ts;
         ts.tv_sec = core->conf->transfer_timeout;
         ts.tv_nsec = 0;
         tsp = &ts;
      }
      
      rc = replica_wait_and_remove( &global_replication, manifest_rctx, tsp );
         
      if( rc != 0 ) {
         errorf("replica_wait rc = %d\n", rc );
      }
      
      rc = manifest_rctx->error;
      if( rc != 0 ) {
         errorf("manifest replication rc = %d\n", rc );
      }
      
      replica_context_free( manifest_rctx );
      free( manifest_rctx );
      
      return rc;
   }
   else if( fh ) {
      // wait for a call to fs_entry_replicate_wait
      fh->rctxs->push_back( manifest_rctx );
   }
   
   return rc;
}


// run a set of block replications
int replica_run_block_contexts( struct fs_core* core, vector<struct replica_context*>* block_rctxs, bool sync, struct fs_file_handle* fh ) {
   
   int rc = 0;
   vector<struct replica_context*> running;
   
   // kick of the replicas
   for( unsigned int i = 0; i < block_rctxs->size(); i++ ) {
      rc = replica_begin( &global_replication, block_rctxs->at(i) );
      if( rc != 0 ) {
         errorf("replica_begin(%p) rc = %d\n", block_rctxs->at(i), rc );
      }
      else {
         running.push_back( block_rctxs->at(i) );
      }
   }
   
   // wait for them all to finish?
   if( sync ) {
      struct timespec *tsp = NULL;
      if( core->conf->transfer_timeout > 0 ) {
         struct timespec ts;
         ts.tv_sec = core->conf->transfer_timeout;
         ts.tv_nsec = 0;
         tsp = &ts;
      }
      
      rc = fs_entry_replicate_wait_and_free( &running, tsp );
      if( rc != 0 ) {
         errorf("fs_entry_replicate_wait rc = %d\n", rc );
      }
   }
   
   else {
      if( fh ) {
         // wait for a later call to fs_entry_replicate_wait
         for( unsigned int i = 0; i < running.size(); i++ ) {
            fh->rctxs->push_back( running[i] );
         }
      }
   }
   
   return rc;
}


// replicate a manifest
// fent must be write-locked
int fs_entry_replicate_manifest( struct fs_core* core, struct fs_entry* fent, bool sync, struct fs_file_handle* fh ) {
   struct replica_context* manifest_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   int rc = replica_context_manifest( core, manifest_rctx, fent, sync );
   if( rc != 0 ) {
      errorf("replica_context_manifest rc = %d\n", rc );
      free( manifest_rctx );
      return rc;
   }
   
   rc = replica_run_manifest_context( core, manifest_rctx, sync, fh );
   
   return rc;
}


// replicate a sequence of modified blocks
// fent must be write-locked
int fs_entry_replicate_blocks( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, bool sync, struct fs_file_handle* fh ) {
   vector<struct replica_context*> block_rctxs;
   int rc = 0;
   
   for( modification_map::iterator itr = modified_blocks->begin(); itr != modified_blocks->end(); itr++ ) {
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* block_info = &itr->second;
      
      struct replica_context* block_rctx = CALLOC_LIST( struct replica_context, 1 );
      
      rc = replica_context_block( core, block_rctx, fent, block_id, block_info, sync );
      if( rc != 0 ) {
         errorf("replica_context_block rc = %d\n", rc );
         free( block_rctx );
      }
      
      block_rctxs.push_back( block_rctx );
   }
   
   rc = replica_run_block_contexts( core, &block_rctxs, sync, fh );
   
   return rc;
}


// delete a manifest replica.
int fs_entry_delete_manifest_replicas( struct fs_core* core, struct fs_entry* fent, bool sync, struct fs_file_handle* fh ) {
   struct replica_context* manifest_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   int rc = replica_context_delete_manifest( core, manifest_rctx, fent, sync );
   if( rc != 0 ) {
      errorf("replica_context_delete_manifest rc = %d\n", rc );
      free( manifest_rctx );
      return rc;
   }
   
   rc = replica_run_manifest_context( core, manifest_rctx, sync, fh );
   
   return rc;
}


// delete block replicas
int fs_entry_delete_block_replicas( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, bool sync, struct fs_file_handle* fh ) {
   vector<struct replica_context*> block_rctxs;
   int rc = 0;
   
   for( modification_map::iterator itr = modified_blocks->begin(); itr != modified_blocks->end(); itr++ ) {
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* block_info = &itr->second;
      
      struct replica_context* block_rctx = CALLOC_LIST( struct replica_context, 1 );
      
      rc = replica_context_delete_block( core, block_rctx, fent, block_id, block_info->version, sync );
      if( rc != 0 ) {
         errorf("replica_context_block rc = %d\n", rc );
         free( block_rctx );
      }
      
      block_rctxs.push_back( block_rctx );
   }
   
   rc = replica_run_block_contexts( core, &block_rctxs, sync, fh );
   
   return rc;
}


// wait for all replication to finish
int fs_entry_replicate_wait_and_free( vector<struct replica_context*>* rctxs, struct timespec* timeout ) {
   int rc = 0;
   int worst_rc = 0;
   
   // set deadlines
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      if( rctxs->at(i) == NULL )
         continue;
      
      if( timeout != NULL ) {
         dbprintf("wait %ld.%ld seconds for replica %p\n", timeout->tv_sec, timeout->tv_nsec, rctxs->at(i) );
         
         rctxs->at(i)->deadline.tv_sec = timeout->tv_sec;
         rctxs->at(i)->deadline.tv_nsec = timeout->tv_nsec;
      }
      else {
         dbprintf("wait for replica %p\n", rctxs->at(i) );
      }
   }
   
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      
      if( rctxs->at(i) == NULL )
         continue;
      
      // wait for this replica to finish...
      rc = replica_wait_and_remove( &global_replication, rctxs->at(i), &rctxs->at(i)->deadline );
      
      if( rc != 0 ) {
         errorf("replica_wait rc = %d\n", rc );
         worst_rc = -EIO;
      }
      
      dbprintf("replica %p finished\n", rctxs->at(i) );
      
      rc = rctxs->at(i)->error;
      if( rc != 0 ) {
         errorf("replica error %d\n", rc );
         worst_rc = rc;
      }
      
      replica_context_free( rctxs->at(i) );
      free( rctxs->at(i) );
         
      (*rctxs)[i] = NULL;
   }
   return worst_rc;
}


// wait for all replications to finish
// fh must be write-locked
int fs_entry_replicate_wait( struct fs_file_handle* fh ) {
   struct timespec *tsp = NULL;
   
   if( fh->transfer_timeout_ms > 0 ) {
      struct timespec ts;
      ts.tv_sec = fh->transfer_timeout_ms / 1000L;
      ts.tv_nsec = ((fh->transfer_timeout_ms) % 1000L) * 1000000L;
      tsp = &ts;
   }
   
   int rc = fs_entry_replicate_wait_and_free( fh->rctxs, tsp );
   
   fh->rctxs->clear();
   return rc;
}

