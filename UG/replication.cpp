/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "replication.h"

static ReplicaUploader* rutp;


// free an upload state machine
void RG_upload_destroy( struct RG_upload* rup ) {
   if( rup->file ) {
      fclose( rup->file );
   }
   if( rup->path ) {
      free( rup->path );
   }
   if( rup->form_data ) {
      curl_formfree( rup->form_data );
   }
   if( rup->data ) {
      free( rup->data );
   }
   if( rup->sync ) {
      pthread_cond_destroy( &rup->sync_cv );
      pthread_mutex_destroy( &rup->sync_lock );
   }
}


/*
// debugging purposes
static int replica_trace( CURL* curl_h, curl_infotype type, char* data, size_t size, void* user ) {
   if( type != CURLINFO_TEXT ) {
      FILE* f = (FILE*)user;
      fwrite( data, 1, size, f );
      fflush( f );
   }
   return 0;
}
*/


// curl read function for uploading
size_t replica_curl_read( void* ptr, size_t size, size_t nmemb, void* userdata ) {
   FILE* file = (FILE*)userdata;
   if( file ) {
      size_t sz = fread( ptr, size, nmemb, file );
      return sz * size;
   }
   else {
      errorf("%s", " no userdata\n");
      return CURL_READFUNC_ABORT;
   }
}


// replicate a manifest
int RG_upload_init_manifest( struct RG_upload* rup, struct ms_client* ms, char* manifest_data, size_t manifest_data_len, char const* fs_path, int64_t file_version, int64_t mtime_sec, int32_t mtime_nsec, bool sync ) {
   memset( rup, 0, sizeof(struct RG_upload) );
   
   // build an update
   ms::ms_gateway_blockinfo replica_info;
   replica_info.set_fs_path( string(fs_path) );
   replica_info.set_file_version( file_version );
   replica_info.set_block_id( 0 );
   replica_info.set_block_version( 0 );
   replica_info.set_blocking_factor( ms->conf->blocking_factor );
   replica_info.set_file_mtime_sec( mtime_sec );
   replica_info.set_file_mtime_nsec( mtime_nsec );

   string replica_info_str;
   bool src = replica_info.SerializeToString( &replica_info_str );
   if( !src ) {
      errorf("%s", " failed to serialize\n");
      return -EINVAL;
   }

   char* fs_fullpath = fs_entry_manifest_path( "", fs_path, file_version, mtime_sec, mtime_nsec );
   
   rup->path = fs_fullpath;
   struct curl_httppost* last = NULL;
   rup->form_data = NULL;

   curl_formadd( &rup->form_data, &last, CURLFORM_COPYNAME, "metadata",
                                          CURLFORM_CONTENTSLENGTH, replica_info_str.size(),
                                          CURLFORM_COPYCONTENTS, replica_info_str.c_str(),
                                          CURLFORM_END );

   curl_formadd( &rup->form_data, &last, CURLFORM_COPYNAME, "data",
                                          CURLFORM_PTRCONTENTS, manifest_data,
                                          CURLFORM_CONTENTSLENGTH, manifest_data_len,
                                          CURLFORM_END );

   rup->data = manifest_data;

   rup->sync = sync;
   
   if( sync ) {
      pthread_cond_init( &rup->sync_cv, NULL );
      pthread_mutex_init( &rup->sync_lock, NULL );
   }
   
   return 0;
   
}
                                   
// replicate a block
int RG_upload_init_block( struct RG_upload* rup, struct ms_client* ms, char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, int64_t mtime_sec, int32_t mtime_nsec, bool sync ) {

   memset( rup, 0, sizeof(struct RG_upload) );

   // attempt to open the file
   char* local_path = fs_entry_local_block_path( data_root, fs_path, file_version, block_id, block_version );

   if( rup->file == NULL ) {
      // stream data from file
      rup->file = fopen( local_path, "r" );
      if( rup->file == NULL ) {
         int errsv = -errno;
         errorf( "fopen(%s) errno = %d\n", local_path, errsv );
         free( local_path );
         return errsv;
      }

      // stat this to get its size
      struct stat sb;
      int rc = fstat( fileno(rup->file), &sb );
      if( rc != 0 ) {
         int errsv = -errno;
         errorf( "fstat errno = %d\n", errsv );
         free( local_path );
         fclose( rup->file );
         rup->file = NULL;
         return errsv;
      }

      // build an update
      ms::ms_gateway_blockinfo replica_info;
      replica_info.set_fs_path( string(fs_path) );
      replica_info.set_file_version( file_version );
      replica_info.set_block_id( block_id );
      replica_info.set_block_version( block_version );
      replica_info.set_blocking_factor( ms->conf->blocking_factor );
      replica_info.set_file_mtime_sec( mtime_sec );
      replica_info.set_file_mtime_nsec( mtime_nsec );

      string replica_info_str;
      bool src = replica_info.SerializeToString( &replica_info_str );
      if( !src ) {
         errorf("%s", " failed to serialize\n");
         free( local_path );
         fclose( rup->file );
         rup->file = NULL;
         return -EINVAL;
      }

      char* fs_fullpath = fs_entry_local_block_path( "", fs_path, file_version, block_id, block_version );
      rup->path = fs_fullpath;
      struct curl_httppost* last = NULL;
      rup->form_data = NULL;

      curl_formadd( &rup->form_data, &last, CURLFORM_COPYNAME, "metadata",
                                             CURLFORM_CONTENTSLENGTH, replica_info_str.size(),
                                             CURLFORM_COPYCONTENTS, replica_info_str.c_str(),
                                             CURLFORM_END );
      
      curl_formadd( &rup->form_data, &last, CURLFORM_COPYNAME, "data",
                                             CURLFORM_STREAM, rup->file,
                                             CURLFORM_FILENAME, rup->path,
                                             CURLFORM_CONTENTSLENGTH, (long)sb.st_size,
                                             CURLFORM_END );
      free( local_path );

      rup->sync = sync;

      if( sync ) {
         pthread_cond_init( &rup->sync_cv, NULL );
         pthread_mutex_init( &rup->sync_lock, NULL );
      }

   
      return 0;
   }
   else {
      free( local_path );
      return -EINVAL;
   }
}

// reference an upload
int RG_upload_ref( struct RG_upload* rup ) {
   return __sync_fetch_and_add( &rup->running, 1 );
}

int RG_upload_unref( struct RG_upload* rup ) {
   return __sync_fetch_and_sub( &rup->running, 1 );
}

// make an uploader
ReplicaUploader::ReplicaUploader( struct ms_client* ms ) :
   CURLTransfer( 1 ) {

   this->ms = ms;
   this->num_RGs = 0;
   this->RGs = NULL;
   this->headers = NULL;
   pthread_mutex_init( &this->download_lock, NULL );

   // get the set of replica_urls
   char** replica_urls = ms_client_RG_urls_copy( ms );
   
   if( replica_urls != NULL ) {
      
      SIZE_LIST( &this->num_RGs, replica_urls );

      this->RGs = CALLOC_LIST( struct RG_channel, num_RGs );
      this->headers = CALLOC_LIST( struct curl_slist*, num_RGs );
      
      for( int i = 0; replica_urls[i] != NULL; i++ ) {

         this->RGs[i].curl_h = curl_easy_init();
         this->RGs[i].pending = new upload_list();
         this->RGs[i].url = strdup( replica_urls[i] );
         
         pthread_mutex_init( &this->RGs[i].pending_lock, NULL );
         
         md_init_curl_handle( this->RGs[i].curl_h, replica_urls[i], ms->conf->metadata_connect_timeout );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_POST, 1L );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_SSL_VERIFYPEER, (ms->conf->verify_peer ? 1L : 0L) );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_READFUNCTION, replica_curl_read );

         // disable Expect: 100-continue
         this->headers[i] = curl_slist_append(this->headers[i], "Expect");
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_HTTPHEADER, this->headers[i] );
         
         /*
         FILE* f = fopen( "/tmp/replica.trace", "w" );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_DEBUGFUNCTION, replica_trace );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_DEBUGDATA, f );
         curl_easy_setopt( this->RGs[i].curl_h, CURLOPT_VERBOSE, 1L );
         */
         
         this->add_curl_easy_handle( 0, this->RGs[i].curl_h );
      }
   }
}

// free memory 
ReplicaUploader::~ReplicaUploader() {
   this->running = false;

   if( !this->stopped ) {
      dbprintf("%s", "Waiting for threads to die...\n");
      while( !this->stopped ) {
         sleep(1);
      }
   }
   
   pthread_join( this->thread, NULL );

   // no need to hold downloading lock, since the thread has just joined

   for( int i = 0; i < this->num_RGs; i++ ) {
      this->remove_curl_easy_handle( 0, this->RGs[i].curl_h );
      curl_easy_cleanup( this->RGs[i].curl_h );

      for( unsigned int j = 0; j < this->RGs[i].pending->size(); j++ ) {
         struct RG_upload* rup = (*this->RGs[i].pending)[j];
         int refs = RG_upload_unref( rup );
         if( refs == 0 ) {
            RG_upload_destroy( rup );
            free( rup );
            (*this->RGs[i].pending)[j] = NULL;
         }
      }
      
      delete this->RGs[i].pending;
      pthread_mutex_destroy( &this->RGs[i].pending_lock );
   }

   if( this->RGs )
      free( this->RGs );

   for( int i = 0; i < this->num_RGs; i++ ) {
      curl_slist_free_all( this->headers[i] );
   }
   free( this->headers );

   pthread_mutex_destroy( &this->download_lock );
}

// get the next ready handle
// NOTE: must have locks first!
struct RG_channel* ReplicaUploader::next_ready_RG( int* err ) {

   CURLMsg* msg = NULL;
   *err = 0;
   struct RG_channel* rsc = NULL;

   do {
      msg = this->get_next_curl_msg( 0 );
      if( msg ) {
         if( msg->msg == CURLMSG_DONE ) {
            for( int i = 0; i < this->num_RGs; i++ ) {

               if( msg->easy_handle == this->RGs[i].curl_h ) {
                  rsc = &this->RGs[i];
                  break;
               }
            }
         }
      }
   } while( msg != NULL && rsc == NULL );

   if( rsc != NULL ) {
      // ensure that the download finished successfully
      if( msg->data.result != 0 ) {
         *err = msg->data.result;
      }
   }
   return rsc;
}

// start replicating on a channel
void ReplicaUploader::enqueue_replica( struct RG_channel* rsc, struct RG_upload* ru ) {
   RG_upload_ref( ru );
   
   pthread_mutex_lock( &rsc->pending_lock );
   rsc->pending->push_back( ru );
   pthread_mutex_unlock( &rsc->pending_lock );
}

// finish a replica
void ReplicaUploader::finish_replica( struct RG_channel* rsc, int status ) {
   pthread_mutex_lock( &this->download_lock );
   
   pthread_mutex_lock( &rsc->pending_lock );
   curl_easy_setopt( rsc->curl_h, CURLOPT_HTTPPOST, NULL );

   // remove the head
   struct RG_upload* ru = (*rsc->pending)[0];
   rsc->pending->erase( rsc->pending->begin() );

   pthread_mutex_unlock( &rsc->pending_lock );
   pthread_mutex_unlock( &this->download_lock );

   int num_running = 0;
   if( status != 0 ) {
      ru->error = status;
      ru->running = 0;
   }
   else {
      num_running = RG_upload_unref( ru );
   }
   
   if( num_running == 0 ) {
      // done replicating...signal waiting threads 
      if( ru->sync ) {
         pthread_cond_signal( &ru->sync_cv );
      }

      dbprintf("Replicated %s, status = %d\n", ru->path, ru->error );
   }
}

// start the next replica
int ReplicaUploader::start_next_replica( struct RG_channel* rsc ) {
   pthread_mutex_lock( &this->download_lock );
   pthread_mutex_lock( &rsc->pending_lock );

   if( rsc->pending->size() > 0 ) {
      struct RG_upload* ru = (*rsc->pending)[0];

      curl_easy_setopt( rsc->curl_h, CURLOPT_HTTPPOST, ru->form_data );
   }

   pthread_mutex_unlock( &rsc->pending_lock );
   pthread_mutex_unlock( &this->download_lock );
   
   return 0;
}


// add a replica for downloading
void ReplicaUploader::add_replica( struct RG_upload* rup ) {
   // verify that the volume version has not changed
   uint64_t curr_version = ms_client_volume_version( this->ms );
   if( curr_version != this->volume_version ) {
      // view change occurred.  Regenerate the set of RG channels
      // TODO...
   }
   
   for( int i = 0; i < this->num_RGs; i++ ) {
      this->enqueue_replica( &this->RGs[i], rup );
   }
}


// main loop for our replicator
void* ReplicaUploader::thread_main( void* arg ) {
   ReplicaUploader* ctx = (ReplicaUploader*)arg;

   ctx->stopped = false;
   
   dbprintf("%s", " started\n");
   while( ctx->running ) {
      int rc = 0;

      pthread_mutex_lock( &ctx->download_lock );
      
      // do download/upload
      rc = ctx->process_curl( 0 );
      if( rc != 0 ) {
         errorf( "process_curl rc = %d\n", rc );
         rc = 0;
      }

      pthread_mutex_unlock( &ctx->download_lock );
      
      // accumulate finished replicas 
      vector<struct RG_channel*> rscs;
      struct RG_channel* rsc = NULL;
      
      do {
         rsc = ctx->next_ready_RG( &rc );

         if( rsc ) {
            ctx->finish_replica( rsc, rc );
            rscs.push_back( rsc );
         }
      } while( rsc != NULL );
      
      // start up the next replicas
      for( vector<struct RG_channel*>::iterator itr = rscs.begin(); itr != rscs.end(); itr++ ) {
         ctx->start_next_replica( *itr );
      }
   }

   dbprintf("%s", " exited\n");
   ctx->stopped = true;
   return NULL;
}


int ReplicaUploader::start() {
   this->running = true;
   this->thread = md_start_thread( ReplicaUploader::thread_main, this, false );
   if( this->thread < 0 )
      return this->thread;
   else
      return 0;
}

int ReplicaUploader::cancel() {
   this->running = false;
   return 0;
}


// start up replication
int replication_init( struct ms_client* ms ) {
   rutp = new ReplicaUploader( ms );
   rutp->start();
   return 0;
}

// shut down replication
int replication_shutdown() {
   rutp->cancel();
   delete rutp;
   return 0;
}


// replicate a sequence of modified blocks, and the associated manifest
// fh must be write-locked
// fh->fent must be read-locked
int fs_entry_replicate_write( struct fs_core* core, struct fs_file_handle* fh, modification_map* modified_blocks, bool sync ) {
   
   char* fs_path = fh->path;
   struct fs_entry* fent = fh->fent;
   
   // don't even bother if there are no replica servers
   if( rutp->get_num_RGs() == 0 )
      return 0;
   
   // replicate the manifest
   char* manifest_data = NULL;
   int rc = 0;
   ssize_t manifest_len = fs_entry_serialize_manifest( core, fent, &manifest_data );

   vector<struct RG_upload*> replicas;

   if( manifest_len > 0 ) {
      struct RG_upload* manifest_rup = CALLOC_LIST( struct RG_upload, 1 );
      RG_upload_init_manifest( manifest_rup, core->ms, manifest_data, manifest_len, fs_path, fent->version, fent->mtime_sec, fent->mtime_nsec, sync );
      
      replicas.push_back( manifest_rup );
   }
   else {
      errorf( "fs_entry_serialize_manifest(%s) rc = %zd\n", fs_path, manifest_len );
      return rc;
   }
   
   // start replicating all modified blocks.  They could be local or staging, so we'll need to be careful 
   for( modification_map::iterator itr = modified_blocks->begin(); itr != modified_blocks->end(); itr++ ) {
      char* data_root = core->conf->data_root;

      if( !URL_LOCAL( fent->url ) ) {
         data_root = core->conf->staging_root;
      }

      struct RG_upload* block_rup = CALLOC_LIST( struct RG_upload, 1 );
      RG_upload_init_block( block_rup, core->ms, data_root, fs_path, fent->version, itr->first, itr->second, fent->mtime_sec, fent->mtime_nsec, sync );
      
      replicas.push_back( block_rup );
   }

   // if we're supposed to wait, then wait
   if( sync ) {
      // TODO
   }
   else {
      // TODO
   }

   return rc;
}

int fs_entry_replicate_wait( struct fs_file_handle* fh ) {
   // TODO
   return 0;
}
