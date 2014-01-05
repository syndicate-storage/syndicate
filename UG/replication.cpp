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

#include "replication.h"
#include "state.h"

int fs_entry_replicate_wait_and_free( struct syndicate_replication* synrp, vector<struct replica_context*>* rctxs, struct timespec* timeout );

// populate a replica_snapshot
int fs_entry_replica_snapshot( struct fs_core* core, struct fs_entry* snapshot_fent, uint64_t block_id, int64_t block_version, struct replica_snapshot* snapshot ) {
   snapshot->file_id = snapshot_fent->file_id;
   snapshot->file_version = snapshot_fent->version;
   snapshot->block_id = block_id;
   snapshot->block_version = block_version;
   snapshot->writer_id = core->gateway;
   snapshot->owner_id = snapshot_fent->owner;
   snapshot->mtime_sec = snapshot_fent->mtime_sec;
   snapshot->mtime_nsec = snapshot_fent->mtime_nsec;
   snapshot->volume_id = snapshot_fent->volume;
   snapshot->size = snapshot_fent->size;
   snapshot->max_write_freshness = snapshot_fent->max_write_freshness;
   return 0;
}

// revert a snapshot to a fent
// fent must be write-locked
int fs_entry_replica_snapshot_restore( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* snapshot ) {
   
   fent->version = snapshot->file_version;
   fent->coordinator = snapshot->writer_id;
   fent->owner = snapshot->owner_id;
   fent->mtime_sec = snapshot->mtime_sec;
   fent->mtime_nsec = snapshot->mtime_nsec;
   fent->size = snapshot->size;
   
   return 0;
}


// set up a replica context
int replica_context_init( struct replica_context* rctx,
                          struct replica_snapshot* snapshot,
                          int type,
                          int op,
                          FILE* block_data,
                          char* manifest_data,
                          off_t data_len,
                          struct curl_httppost* form_data,
                          bool sync,
                          bool free_on_processed
                        ) {
   
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
   rctx->free_on_processed = free_on_processed;
   
   memcpy( &rctx->snapshot, snapshot, sizeof(struct replica_snapshot) );
   
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
int replica_populate_request( ms::ms_gateway_request_info* replica_info, int request_type, struct replica_snapshot* snapshot, off_t size, unsigned char const* hash, size_t hash_len ) {
   
   replica_info->set_type( request_type );
   replica_info->set_file_version( snapshot->file_version );
   replica_info->set_block_id( snapshot->block_id );
   replica_info->set_block_version( snapshot->block_version );
   replica_info->set_size( size );
   replica_info->set_file_mtime_sec( snapshot->mtime_sec );
   replica_info->set_file_mtime_nsec( snapshot->mtime_nsec );
   replica_info->set_file_id( snapshot->file_id );
   replica_info->set_owner( snapshot->owner_id );
   replica_info->set_writer( snapshot->writer_id );
   replica_info->set_volume( snapshot->volume_id );
   
   
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
int replica_context_manifest( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, bool replicate_sync ) {
   
   // get the manifest data
   char* manifest_data = NULL;
   ssize_t manifest_data_len = 0;
   int rc = 0;
   
   manifest_data_len = fs_entry_serialize_manifest( core, fent, &manifest_data, true );
   if( manifest_data_len < 0 ) {
      errorf("fs_entry_serialize_manifest(%" PRIX64 ") rc = %zd\n", fent->file_id, manifest_data_len);
      return -EINVAL;
   }
   
   // snapshot this fent
   struct replica_snapshot snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &snapshot );
   
   // TODO: encrypt manifest string
   
   // hash the manifest
   unsigned char* hash = sha256_hash_data( manifest_data, manifest_data_len );
   size_t hash_len = sha256_len();
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::MANIFEST, &snapshot, manifest_data_len, hash, hash_len );
   
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
   rc = replica_context_init( rctx, &snapshot, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_POST, NULL, manifest_data, manifest_data_len, form_data, replicate_sync, false );
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
int replica_context_block( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, uint64_t block_id, struct fs_entry_block_info* block_info, bool replicate_sync ) {
   
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
   
   // snapshot this fent
   struct replica_snapshot snapshot;
   fs_entry_replica_snapshot( core, fent, block_id, block_info->version, &snapshot );

   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::BLOCK, &snapshot, sb.st_size, block_info->hash, block_info->hash_len );
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
   rc = replica_context_init( rctx, &snapshot, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_POST, f, NULL, sb.st_size, form_data, replicate_sync, false );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      fclose( f );
      
      return -EINVAL;
   }
   
   return 0;
}


// garbage-collect a manifest
int replica_context_garbage_manifest( struct fs_core* core, struct replica_context* rctx, struct replica_snapshot* snapshot ) {
   int rc = 0;
   
   // put random bits into the hash field, for some cryptographic padding
   unsigned char fake_hash[256];
   for( unsigned int i = 0; i < (256 / sizeof(uint32_t)); i++ ) {
      uint32_t random_bits = CMWC4096();
      memcpy( fake_hash + (i * sizeof(uint32_t)), &random_bits, sizeof(uint32_t) );
   }
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::MANIFEST, snapshot, 0, fake_hash, 256 );
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
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", snapshot->file_id, rc );
      
      return rc;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, snapshot, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_DELETE, NULL, NULL, 0, form_data, false, true );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", snapshot->file_id, rc );
      
      curl_formfree( form_data );
      
      return -EINVAL;
   }
   
   return 0;
}


// garbage-collect a block
int replica_context_garbage_block( struct fs_core* core, struct replica_context* rctx, struct replica_snapshot* snapshot ) {
   int rc = 0;
   
   // put random bits into the hash field, for some cryptographic padding
   unsigned char fake_hash[256];
   for( unsigned int i = 0; i < (256 / sizeof(uint32_t)); i++ ) {
      uint32_t random_bits = CMWC4096();
      memcpy( fake_hash + (i * sizeof(uint32_t)), &random_bits, sizeof(uint32_t) );
   }
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::BLOCK, snapshot, 0, fake_hash, 256 );
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
      errorf("replica_add_metadata_form( %" PRIX64 " ) rc = %d\n", snapshot->file_id, rc );
      
      return rc;
   }
   
   // set up the replica context
   rc = replica_context_init( rctx, snapshot, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_DELETE, NULL, NULL, 0, form_data, false, true );
   
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", snapshot->file_id, rc );
      
      curl_formfree( form_data );
      
      return -EINVAL;
   }
   
   return 0;
}


// add a CURL handle to our running uploads
// synrp->uploads should be locked
static void replica_insert_upload( struct syndicate_replication* synrp, CURL* curl, struct replica_context* rctx ) {
   // add to running 
   dbprintf("%s: running: %p\n", synrp->process_name, rctx );
   
   (*synrp->uploads)[ curl ] = rctx;
   curl_multi_add_handle( synrp->running, curl );
}

// add a replica context's CURL handles to our running uploads
// synrp->uploads should be locked
static void replica_insert_uploads( struct syndicate_replication* synrp, struct replica_context* rctx ) {
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      replica_insert_upload( synrp, rctx->curls->at(i), rctx );
   }
}

// add a CURL handle to our penduing uploads
// synrp->pending should be locked
static void replica_insert_pending_upload( struct syndicate_replication* synrp, CURL* curl, struct replica_context* rctx ) {
   if( !synrp->accepting )
      return;
   
   // add to running 
   dbprintf("%s: pending: %p\n", synrp->process_name, rctx );
   
   (*synrp->pending_uploads)[ curl ] = rctx;
}

// add a replica context's CURL handles to our running uploads
// synrp->uploads should be locked
static void replica_insert_pending_uploads( struct syndicate_replication* synrp, struct replica_context* rctx ) {
   if( !synrp->accepting )
      return;
   
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      replica_insert_pending_upload( synrp, rctx->curls->at(i), rctx );
   }
}

// connect a replica context to the RGs, and begin processing it
int replica_context_connect( struct syndicate_replication* rp, struct replica_context* rctx ) {
   
   // replicate everywhere
   uint64_t* rg_ids = ms_client_RG_ids( rp->ms );

   // acquire this lock, since we're processing it now...
   sem_wait( &rctx->processing_lock );
   
   int rc = 0;
   size_t num_rgs = 0;
   
   for( int i = 0; rg_ids[i] != 0; i++ ) {
      
      char* rg_base_url = ms_client_get_RG_content_url( rp->ms, rg_ids[i] );
      
      CURL* curl = curl_easy_init();
      rctx->curls->push_back( curl );
      
      dbprintf("%s: %s %p (%s) to %s\n", rp->process_name, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx, (rctx->type == REPLICA_CONTEXT_TYPE_BLOCK ? "block" : "manifest"), rg_base_url );
      md_init_curl_handle( rp->conf, curl, rg_base_url, rp->ms->conf->replica_connect_timeout );
      
      // prepare upload
      curl_easy_setopt( curl, CURLOPT_POST, 1L );
      curl_easy_setopt( curl, CURLOPT_HTTPPOST, rctx->form_data );
      
      // if we're deleting, change the method
      if( rctx->op == REPLICA_DELETE ) {
         curl_easy_setopt( curl, CURLOPT_CUSTOMREQUEST, "DELETE" );
      }
      
      num_rgs += 1;
      
      free( rg_base_url );
   }
   
   if( num_rgs > 0 ) {
      rp->has_pending = true;
   }
   else {
      // no replica gateways!
      errorf("%s: No RGs are known to us!\n", rp->process_name);
      rc = -EHOSTDOWN;
   }
   
   free( rg_ids );
   
   return rc;
}


static int old_still_running = -1;

// process curl
// NOTE: synrp must be locked
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
      errorf("%s: curl_multi_timeout rc = %d\n", synrp->process_name, rc );
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
      errorf("%s: curl_multi_fdset rc = %d\n", synrp->process_name, rc );
      return -1;
   }
   
   // find out which FDs are ready
   rc = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);
   
   if( rc < 0 ) {
      // we have a problem
      errorf("%s: select rc = %d, errno = %d\n", synrp->process_name, rc, -errno );
      return -1;
   }
   
   // let CURL do its thing
   
   do {
      rc = curl_multi_perform( synrp->running, &still_running );
      
      if( old_still_running <= 0 )
         old_still_running = still_running;
      
      if( old_still_running > 0 ) {
         dbprintf("%s: still running = %d\n", synrp->process_name, still_running );
      }
      old_still_running = still_running;
      
      if( rc == CURLM_OK )
         break;
      
   } while( rc != CURLM_CALL_MULTI_PERFORM );
   
   // process messages
   return 0;
}


// erase a running replica context, given an iterator to synrp->uploads
// NOTE: running_lock must be held by the caller!
static int replica_erase_upload_context( struct syndicate_replication* synrp, const replica_upload_set::iterator& itr ) {
   
   struct replica_context* rctx = itr->second;
   
   int rc = 0;
   
   CURL* curl = itr->first;
   
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
      dbprintf("%s: Finished %p\n", synrp->process_name, rctx );
      
      // record this before posting, since the waiting thread can free rctx
      bool free_on_processed = rctx->free_on_processed;
      
      sem_post( &rctx->processing_lock );
      
      if( free_on_processed ) {
         // destroy this
         replica_context_free( rctx );
         free( rctx );
      }
   }
   
   return rc;
}


// remove a running replica context
// NOTE: running_lock must be held by the caller!
static int replica_remove_upload_context( struct syndicate_replication* synrp, CURL* curl ) {
   
   replica_upload_set::iterator itr = synrp->uploads->find( curl );
   if( itr != synrp->uploads->end() ) {
      
      return replica_erase_upload_context( synrp, itr );
   }
   
   return -ENOENT;
}


// does a replica context match a set of snapshot attributes?
static bool replica_context_snapshot_match( struct replica_context* rctx, struct replica_snapshot* snapshot ) {
   return (rctx->snapshot.volume_id == snapshot->volume_id &&
          rctx->snapshot.file_id == snapshot->file_id &&
          rctx->snapshot.file_version == snapshot->file_version &&
          rctx->snapshot.block_id == snapshot->block_id &&
          rctx->snapshot.block_version == snapshot->block_version &&
          rctx->snapshot.mtime_sec == snapshot->mtime_sec &&
          rctx->snapshot.mtime_nsec == snapshot->mtime_nsec);
}


// cancel a replica context, based on known attributes
static int replica_cancel_contexts( struct syndicate_replication* synrp, struct replica_snapshot* snapshot ) {
   int num_erased = 0;
   
   set<struct replica_context*> to_free;
   
   // search replicas that will be started in the future
   pthread_mutex_lock( &synrp->write_delayed_lock );
   
   for( replica_delay_queue::iterator itr = synrp->write_delayed->begin(); itr != synrp->write_delayed->end(); itr++ ) {
      struct replica_context* rctx = itr->second;
      
      replica_delay_queue::iterator to_erase = itr;
      itr++;
      
      if( replica_context_snapshot_match( rctx, snapshot ) ) {
         
         to_free.insert( rctx );
         
         synrp->write_delayed->erase( to_erase );
         num_erased++;
      }
   }
   
   pthread_mutex_unlock( &synrp->write_delayed_lock );
   
   // search replicas that are about to start
   pthread_mutex_lock( &synrp->pending_lock );
   
   for( replica_upload_set::iterator itr = synrp->pending_uploads->begin(); itr != synrp->pending_uploads->end();  ) {
      struct replica_context* rctx = itr->second;
      
      replica_upload_set::iterator to_erase = itr;
      itr++;
      
      if( replica_context_snapshot_match( rctx, snapshot ) ) {
         
         to_free.insert( rctx );
         
         synrp->pending_uploads->erase( to_erase );
         num_erased++;
      }
   }
   
   pthread_mutex_unlock( &synrp->pending_lock );
   
   // free memory
   for( set<struct replica_context*>::iterator itr = to_free.begin(); itr != to_free.end(); itr++ ) {
      struct replica_context* rctx = *itr;
      
      replica_context_free( rctx );
      free( rctx );
   }
   
   // schedule this replica to be stopped by the main loop
   pthread_mutex_lock( &synrp->cancel_lock );
   
   synrp->pending_cancels->push_back( *snapshot );
   
   synrp->has_cancels = true;
   
   pthread_mutex_unlock( &synrp->cancel_lock );
   
   return num_erased;
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
               errorf("%s: RG curl status %d\n", synrp->process_name, msg->data.result );
            }
            
            // check HTTP code
            long http_status = 0;
            int crc = curl_easy_getinfo( msg->easy_handle, CURLINFO_RESPONSE_CODE, &http_status );
            if( crc != 0 ) {
               rctx->error = -ENODATA;
            }
            else if( http_status != 200 ) {
               errorf("%s: RG HTTP response code %ld for %p\n", synrp->process_name, http_status, rctx );
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
            
            dbprintf("%s: Finished %p (%s of %" PRIX64 ")\n", synrp->process_name, rctx, (rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ? "manifest" : "block"), rctx->snapshot.file_id );
            
            replica_remove_upload_context( synrp, msg->easy_handle );
         }
      }
   } while( msg != NULL );
   
   return rc;
}



// thread body for processing the work of a syndicate_replication instance
void* replica_main( void* arg ) {
   struct syndicate_replication* synrp = (struct syndicate_replication*)arg;
   
   int rc = 0;
   
   dbprintf("%s: thread started\n", synrp->process_name);
   
   while( synrp->active ) {
      // run CURL for a bit
      pthread_mutex_lock( &synrp->running_lock );
      
      int total_to_upload = 0;
      
      // do we have delayed replicas?
      if( synrp->has_write_delayed ) {
         pthread_mutex_lock( &synrp->write_delayed_lock );
         
         double now = now_ns();
         
         for( replica_delay_queue::iterator itr = synrp->write_delayed->begin(); itr != synrp->write_delayed->end(); itr++ ) {
            if( itr->first > now ) {
               if( synrp->accepting ) {
                  // we're still running, so wait till the
                  // future deadline 
                  total_to_upload ++;
                  break;
               }
            }
            
            struct replica_context* rctx = itr->second;
            
            int rc = replica_context_connect( synrp, rctx );
            if( rc != 0 ) {
               errorf("%s: replica_context_connect(%p) rc = %d\n", synrp->process_name, itr->second, rc );
            }
            else {
               replica_insert_uploads( synrp, rctx );
            }
         }
         
         if( synrp->write_delayed->size() == 0 )
            synrp->has_write_delayed = false;
         
         pthread_mutex_unlock( &synrp->write_delayed_lock );
      }
      
      
      // do we have replicas that are waiting to be started?
      if( synrp->has_pending ) {
         pthread_mutex_lock( &synrp->pending_lock );
         
         for( replica_upload_set::iterator itr = synrp->pending_uploads->begin(); itr != synrp->pending_uploads->end(); itr++ ) {
            replica_insert_upload( synrp, itr->first, itr->second );
         }
         
         synrp->pending_uploads->clear();
         
         synrp->has_pending = false;
         pthread_mutex_unlock( &synrp->pending_lock );
      }
      
      // do we have replicas to cancel?
      if( synrp->has_cancels ) {
         pthread_mutex_lock( &synrp->cancel_lock );
         
         for( unsigned int i = 0; i < synrp->pending_cancels->size(); i++ ) {
            struct replica_snapshot* snapshot = &synrp->pending_cancels->at(i);
            
            for( replica_upload_set::iterator itr = synrp->uploads->begin(); itr != synrp->uploads->end();  ) {
               replica_upload_set::iterator may_cancel = itr;
               itr++;
               
               if( replica_context_snapshot_match( may_cancel->second, snapshot ) ) {
                  
                  dbprintf("%s: cancel: %p\n", synrp->process_name, may_cancel->second );
                  
                  // cancel this running upload
                  replica_erase_upload_context( synrp, may_cancel );
               }
            }
         }
         
         synrp->pending_cancels->clear();
         
         synrp->has_cancels = false;
         pthread_mutex_unlock( &synrp->cancel_lock );
      }
      
      // do we have replicas that have expired (timed out)?
      if( synrp->has_expires ) {
         pthread_mutex_lock( &synrp->expire_lock );
         
         for( replica_expire_set::iterator itr = synrp->pending_expires->begin(); itr != synrp->pending_expires->end(); itr++ ) {
            
            CURL* rctx_curl = *itr;
            
            replica_upload_set::iterator rctx_itr = synrp->uploads->find( rctx_curl );
            if( rctx_itr != synrp->uploads->end() ) {
               dbprintf("%s: expire: %p\n", synrp->process_name, rctx_itr->second );
            }
            
            // remove this expired upload
            replica_remove_upload_context( synrp, rctx_curl );
         }
         
         synrp->pending_expires->clear();
         
         synrp->has_expires = false;
         pthread_mutex_unlock( &synrp->expire_lock );
      }
      
      synrp->num_uploads = synrp->uploads->size() + total_to_upload;
      
      // upload data
      rc = replica_multi_upload( synrp );
      
      if( rc != 0 ) {
         errorf("%s: replica_multi_upload rc = %d\n", synrp->process_name, rc );
         pthread_mutex_unlock( &synrp->running_lock );
         break;
      }
      
      // find out what finished uploading
      rc = replica_process_responses( synrp );
      
      pthread_mutex_unlock( &synrp->running_lock );
      
      if( rc != 0 ) {
         errorf("%s: replica_process_responses rc = %d\n", synrp->process_name, rc );
         break;
      }
   }
   
   dbprintf("%s: thread shutdown\n", synrp->process_name );
   
   return NULL;
}


// begin uploading
int replica_begin( struct syndicate_replication* rp, struct replica_context* rctx, double when ) {
   
   if( !rp->accepting )
      return -ENOTCONN;
   
   int rc = 0;
   
   if( when <= 0.0 ) {
      rc = replica_context_connect( rp, rctx );
      if( rc != 0 ) {
         errorf("%s: replica_connect_context(%p) rc = %d\n", rp->process_name, rctx, rc );
      }
      else {
         pthread_mutex_lock( &rp->pending_lock );
         
         replica_insert_pending_uploads( rp, rctx );
         
         rp->has_pending = true;
         pthread_mutex_unlock( &rp->pending_lock );
      }
   }
   else {
      // begin later
      pthread_mutex_lock( &rp->write_delayed_lock );
      
      // check for duplicates...
      replica_delay_queue::iterator itr = rp->write_delayed->find( when );
      while( itr != rp->write_delayed->end() ) {
         when += 1e-9;
         itr = rp->write_delayed->find( when );
      }
      
      // write after a certain amount of time has passed
      (*rp->write_delayed)[ when ] = rctx;
      rp->has_write_delayed = true;
      pthread_mutex_unlock( &rp->write_delayed_lock );
   }
   
   return rc;
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
         
         // remove these handles on the next loop iteration
         pthread_mutex_lock( &rp->expire_lock );
         
         for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
            rp->pending_expires->insert( rctx->curls->at(i) );
         }
         
         rp->has_expires = true;
         
         pthread_mutex_unlock( &rp->expire_lock );
      }
      
      return rc;
   }
}


// initalize a syndicate replication instance
int replica_init_replication( struct syndicate_replication* rp, char const* name, struct md_syndicate_conf* conf, struct ms_client* client, uint64_t volume_id ) {
   pthread_mutex_init( &rp->running_lock, NULL );
   pthread_mutex_init( &rp->pending_lock, NULL );
   pthread_mutex_init( &rp->cancel_lock, NULL );
   pthread_mutex_init( &rp->expire_lock, NULL );
   pthread_mutex_init( &rp->write_delayed_lock, NULL );
   
   rp->running = curl_multi_init();
   rp->process_name = strdup( name );
   
   rp->uploads = new replica_upload_set();
   rp->pending_uploads = new replica_upload_set();
   rp->pending_cancels = new replica_cancel_list();
   rp->pending_expires = new replica_expire_set();
   rp->write_delayed = new replica_delay_queue();
   
   rp->accepting = true;
   rp->active = true;
   
   rp->ms = client;
   rp->conf = conf;
   rp->volume_id = volume_id;
   
   rp->upload_thread = md_start_thread( replica_main, rp, false );
   if( rp->upload_thread < 0 ) {
      errorf("%s: md_start_thread rc = %lu\n", rp->process_name, rp->upload_thread );
      return rp->upload_thread;
   }
   
   return 0;
}


// shut down a syndicate replication instance
int replica_shutdown_replication( struct syndicate_replication* rp, int wait_replicas ) {
   rp->accepting = false;
   
   if( wait_replicas > 0 ) {
      dbprintf("Wait %d seconds for all replicas to finish in %s\n", wait_replicas, rp->process_name );
      sleep(wait_replicas);
   }
   else if( wait_replicas < 0 ) {
      dbprintf("Wait for all replicas to finish in %s\n", rp->process_name);
      
      do {
         dbprintf("remaining: %d\n", rp->num_uploads );
         if( rp->num_uploads == 0 )
            break;
         
         sleep(1);
      } while( rp->num_uploads != 0 );
   }
   
   rp->active = false;
   
   // NOTE: we don't care of pthread_cancel fails...
   pthread_cancel( rp->upload_thread );
   pthread_join( rp->upload_thread, NULL );
   
   int need_running_unlock = pthread_mutex_trylock( &rp->running_lock );
   
   if( rp->uploads != NULL ) {
      dbprintf("free %zu uploads for %s\n", rp->uploads->size(), rp->process_name);
      for( replica_upload_set::iterator itr = rp->uploads->begin(); itr != rp->uploads->end(); itr++ ) {
         curl_multi_remove_handle( rp->running, itr->first );
         
         replica_context_free( itr->second );
         
         free( itr->second );
      }
   }
   
   int need_pending_unlock = pthread_mutex_trylock( &rp->pending_lock );
   
   if( rp->pending_uploads != NULL ) {
      dbprintf("free %zu pending for %s\n", rp->pending_uploads->size(), rp->process_name);
      for( replica_upload_set::iterator itr = rp->pending_uploads->begin(); itr != rp->pending_uploads->end(); itr++ ) {
         
         replica_context_free( itr->second );
         
         free( itr->second );
      }
   }
   
   int need_write_delayed_lock = pthread_mutex_trylock( &rp->write_delayed_lock );
   
   if( rp->write_delayed != NULL ) {
      dbprintf("free %zu delayed for %s\n", rp->write_delayed->size(), rp->process_name );
      for( replica_delay_queue::iterator itr = rp->write_delayed->begin(); itr != rp->write_delayed->end(); itr++ ) {
         
         replica_context_free( itr->second );
         
         free( itr->second );
      }
   }
   
   if( rp->pending_cancels != NULL ) {
      dbprintf("free %zu pending cancels for %s\n", rp->pending_cancels->size(), rp->process_name );
      rp->pending_cancels->clear();
   }
   
   if( rp->uploads )
      delete rp->uploads;
   
   if( rp->pending_uploads )
      delete rp->pending_uploads;
   
   if( rp->pending_cancels )
      delete rp->pending_cancels;
   
   if( rp->pending_expires )
      delete rp->pending_expires;
   
   if( rp->write_delayed )
      delete rp->write_delayed;
   
   rp->uploads = NULL;
   rp->pending_uploads = NULL;
   rp->pending_cancels = NULL;
   rp->pending_expires = NULL;
   rp->write_delayed = NULL;
   
   if( need_running_unlock )
      pthread_mutex_unlock( &rp->pending_lock );
   
   if( need_pending_unlock )
      pthread_mutex_unlock( &rp->running_lock );
   
   if( need_write_delayed_lock )
      pthread_mutex_unlock( &rp->write_delayed_lock );
   
   pthread_mutex_destroy( &rp->pending_lock );
   pthread_mutex_destroy( &rp->running_lock );
   pthread_mutex_destroy( &rp->cancel_lock );
   pthread_mutex_destroy( &rp->expire_lock );
   pthread_mutex_destroy( &rp->write_delayed_lock );
   
   if( rp->running )
      curl_multi_cleanup( rp->running );
   
   rp->running = NULL;
   
   if( rp->process_name ) {
      dbprintf("destroyed %s\n", rp->process_name );
   
      free( rp->process_name );
      rp->process_name = NULL;
   }
   
   return 0;
}


// start up replication
int replication_init( struct syndicate_state* state, uint64_t volume_id) {
   int rc = replica_init_replication( &state->replication, "replication", &state->conf, state->ms, volume_id );
   if( rc != 0 ) {
      errorf("replication: replica_init_replication rc = %d\n", rc );
      return -ENOSYS;
   }
   
   rc = replica_init_replication( &state->garbage_collector, "garbage collector", &state->conf, state->ms, volume_id );
   if( rc != 0 ) {
      errorf("garbage collector: replica_init_replication rc = %d\n", rc );
      return -ENOSYS;
   }
   
   return 0;
}


// shut down replication
int replication_shutdown( struct syndicate_state* state, int wait_replicas ) {
   int rc = replica_shutdown_replication( &state->replication, wait_replicas );
   if( rc != 0 ) {
      errorf("%s: replica_shutdown_replication rc = %d\n", state->replication.process_name, rc );
      return -ENOSYS;
   }
   
   rc = replica_shutdown_replication( &state->garbage_collector, wait_replicas );
   if( rc != 0 ) {
      errorf("%s: replica_shutdown_replication rc = %d\n", state->garbage_collector.process_name, rc );
      return -ENOSYS;
   }
   
   return 0;
}


// run a manifest replication
int replica_run_manifest_context( struct fs_core* core, struct syndicate_replication* synrp, struct replica_context* manifest_rctx, bool sync, vector<struct replica_context*>* rctxs, double start_time ) {
   int rc = 0;
   
   // proceed to replicate
   rc = replica_begin( synrp, manifest_rctx, start_time );
   
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
      
      rc = replica_wait_and_remove( synrp, manifest_rctx, tsp );
         
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
   else if( rctxs ) {
      // wait for a call to fs_entry_replicate_wait
      rctxs->push_back( manifest_rctx );
   }
   
   return rc;
}


// run a set of block replications
int replica_run_block_contexts( struct fs_core* core, struct syndicate_replication* synrp, vector<struct replica_context*>* block_rctxs, bool sync, vector<struct replica_context*>* rctxs, double start_time ) {
   
   int rc = 0;
   vector<struct replica_context*> running;
   
   // kick of the replicas
   for( unsigned int i = 0; i < block_rctxs->size(); i++ ) {
      rc = replica_begin( synrp, block_rctxs->at(i), start_time );
      if( rc != 0 ) {
         errorf("replica_begin(%p) rc = %d\n", block_rctxs->at(i), rc );
      }
      else {
         running.push_back( block_rctxs->at(i) );
      }
   }
   
   if( running.size() > 0 ) {
      // wait for them all to finish?
      if( sync ) {
         struct timespec *tsp = NULL;
         if( core->conf->transfer_timeout > 0 ) {
            struct timespec ts;
            ts.tv_sec = core->conf->transfer_timeout;
            ts.tv_nsec = 0;
            tsp = &ts;
         }
         
         rc = fs_entry_replicate_wait_and_free( synrp, &running, tsp );
         if( rc != 0 ) {
            errorf("fs_entry_replicate_wait_and_free rc = %d\n", rc );
         }
      }
      
      else {
         if( rctxs ) {
            // wait for a later call to fs_entry_replicate_wait
            for( unsigned int i = 0; i < running.size(); i++ ) {
               rctxs->push_back( running[i] );
            }
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
   
   vector<struct replica_context*>* rctxs = NULL;
   
   if( fh )
      rctxs = fh->rctxs;
   
   rc = replica_run_manifest_context( core, &core->state->replication, manifest_rctx, sync, rctxs, -1.0 );
   
   return rc;
}


// replicate a sequence of modified blocks.
// in modified_blocks, we only need the version, hash, and hash_len fields for each block.
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
      else {
         block_rctxs.push_back( block_rctx );
      }
   }
   
   vector<struct replica_context*>* rctxs = NULL;
   
   if( fh )
      rctxs = fh->rctxs;
   
   rc = replica_run_block_contexts( core, &core->state->replication, &block_rctxs, sync, rctxs, -1.0 );
   
   return rc;
}

// garbage-collect a manifest replica.
int fs_entry_garbage_collect_manifest( struct fs_core* core, struct replica_snapshot* snapshot ) {
   struct replica_context* manifest_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   int rc = replica_context_garbage_manifest( core, manifest_rctx, snapshot );
   if( rc != 0 ) {
      errorf("replica_context_garbage_manifest rc = %d\n", rc );
      free( manifest_rctx );
      return rc;
   }
   
   // if there are any pending uploads for this same manifest, stop them
   replica_cancel_contexts( &core->state->replication, snapshot );
   
   struct timespec write_ttl;
   write_ttl.tv_sec = snapshot->max_write_freshness;
   write_ttl.tv_nsec = 0;
   
   double start_time = timespec_to_double( &write_ttl );
   
   rc = replica_run_manifest_context( core, &core->state->garbage_collector, manifest_rctx, false, NULL, start_time );
      
   return rc;
}


// garbage-collect blocks
// in modified_blocks, we only need the version field for each block.
int fs_entry_garbage_collect_blocks( struct fs_core* core, struct replica_snapshot* snapshot, modification_map* modified_blocks ) {
   vector<struct replica_context*> block_rctxs;
   int rc = 0;
   
   for( modification_map::iterator itr = modified_blocks->begin(); itr != modified_blocks->end(); itr++ ) {
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* block_info = &itr->second;
      
      // make a block-specific snapshot 
      struct replica_snapshot block_snapshot;
      memcpy( &block_snapshot, snapshot, sizeof(struct replica_snapshot) );
      
      block_snapshot.block_id = block_id;
      block_snapshot.block_version = block_info->version;
      
      struct replica_context* block_rctx = CALLOC_LIST( struct replica_context, 1 );
      
      rc = replica_context_garbage_block( core, block_rctx, &block_snapshot );
      if( rc != 0 ) {
         errorf("replica_context_garbage_block rc = %d\n", rc );
         free( block_rctx );
      }
      
      // if there are any pending uploads for this block, then simply stop them.
      replica_cancel_contexts( &core->state->replication, &block_snapshot );
      
      // garbage collect!
      block_rctxs.push_back( block_rctx );
   }
   
   struct timespec write_ttl;
   write_ttl.tv_sec = snapshot->max_write_freshness;
   write_ttl.tv_nsec = 0;
   
   double start_time = timespec_to_double( &write_ttl );
   
   rc = replica_run_block_contexts( core, &core->state->garbage_collector, &block_rctxs, false, NULL, start_time );
   
   return rc;
}


// wait for all replication to finish
int fs_entry_replicate_wait_and_free( struct syndicate_replication* synrp, vector<struct replica_context*>* rctxs, struct timespec* timeout ) {
   int rc = 0;
   int worst_rc = 0;
   
   // set deadlines
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      if( rctxs->at(i) == NULL )
         continue;
      
      if( timeout != NULL ) {
         dbprintf("wait %ld.%ld seconds for replica %p\n", (long)timeout->tv_sec, (long)timeout->tv_nsec, rctxs->at(i) );
         
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
      rc = replica_wait_and_remove( synrp, rctxs->at(i), &rctxs->at(i)->deadline );
      
      if( rc != 0 ) {
         errorf("replica_wait_and_remove rc = %d\n", rc );
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
int fs_entry_replicate_wait( struct fs_core* core, struct fs_file_handle* fh ) {
   struct timespec *tsp = NULL;
   
   if( fh->transfer_timeout_ms > 0 ) {
      struct timespec ts;
      ts.tv_sec = fh->transfer_timeout_ms / 1000L;
      ts.tv_nsec = ((fh->transfer_timeout_ms) % 1000L) * 1000000L;
      tsp = &ts;
   }
   
   int rc = fs_entry_replicate_wait_and_free( &core->state->replication, fh->rctxs, tsp );
   
   fh->rctxs->clear();
   return rc;
}

// make a "fake" file handle that has just enough data in it for us to process
int fs_entry_replica_file_handle( struct fs_core* core, struct fs_entry* fent, struct fs_file_handle* fh ) {
   memset( fh, 0, sizeof( struct fs_file_handle ) );
   
   fh->rctxs = new vector<struct replica_context*>();
   fh->transfer_timeout_ms = core->conf->transfer_timeout * 1000L;
   
   return 0;
}

// clean up a "fake" file handle
int fs_entry_free_replica_file_handle( struct fs_file_handle* fh ) {
   if( fh->rctxs ) {
      delete fh->rctxs;
   }
   
   return 0;
}


// garbage collect a file's data
// fent must be read-locked
int fs_entry_garbage_collect_file( struct fs_core* core, struct fs_entry* fent ) {
   if( !FS_ENTRY_LOCAL( core, fent ) )
      return -EINVAL;
   
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );

   int rc = 0;
   
   // garbage-collect manifest
   rc = fs_entry_garbage_collect_manifest( core, &fent_snapshot );
   if( rc != 0 ) {
      errorf( "fs_entry_garbage_collect_manifest(%s) rc = %d\n", fent->name, rc );
      rc = 0;
   }
   
   // garbage-collect each block
   uint64_t num_blocks = fent->manifest->get_num_blocks();
   modification_map block_infos;
   
   for( uint64_t i = 0; i < num_blocks; i++ ) {
      
      // acquire block info 
      struct fs_entry_block_info binfo;
      memset( &binfo, 0, sizeof(binfo) );
      
      binfo.version = fent->manifest->get_block_version( i );
      
      block_infos[ i ] = binfo;
   }
   
   rc = fs_entry_garbage_collect_blocks( core, &fent_snapshot, &block_infos );
   if( rc != 0 ) {
      errorf( "fs_entry_garbage_collect_blocks(%s) rc = %d\n", fent->name, rc );
      rc = 0;
   }
 
   return rc;
}

