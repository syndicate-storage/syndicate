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
#include "syndicate.h"
#include "driver.h"

int fs_entry_replica_wait_and_free( struct rg_client* synrp, replica_list_t* rctxs, struct timespec* timeout );

// populate a replica_snapshot
int fs_entry_replica_snapshot( struct fs_core* core, struct fs_entry* snapshot_fent, uint64_t block_id, int64_t block_version, struct replica_snapshot* snapshot ) {
   snapshot->file_id = snapshot_fent->file_id;
   snapshot->file_version = snapshot_fent->version;
   snapshot->block_id = block_id;
   snapshot->block_version = block_version;
   snapshot->writer_id = core->gateway;
   snapshot->coordinator_id = snapshot_fent->coordinator;
   snapshot->owner_id = snapshot_fent->owner;
   snapshot->fent_mtime_sec = snapshot_fent->mtime_sec;
   snapshot->fent_mtime_nsec = snapshot_fent->mtime_nsec;
   snapshot->volume_id = snapshot_fent->volume;
   snapshot->size = snapshot_fent->size;
   snapshot->max_write_freshness = snapshot_fent->max_write_freshness;
   
   struct timespec ts;
   snapshot_fent->manifest->get_modtime( &ts );
   
   snapshot->manifest_mtime_sec = ts.tv_sec;
   snapshot->manifest_mtime_nsec = ts.tv_nsec;
   snapshot->manifest_initialized = snapshot_fent->manifest->is_initialized();
   return 0;
}

// revert a snapshot to a fent (doesn't affect whether or not the manifest was initialized)
// fent must be write-locked
int fs_entry_replica_snapshot_restore( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* snapshot ) {
   
   fent->version = snapshot->file_version;
   fent->coordinator = snapshot->coordinator_id;
   fent->owner = snapshot->owner_id;
   fent->mtime_sec = snapshot->fent_mtime_sec;
   fent->mtime_nsec = snapshot->fent_mtime_nsec;
   fent->size = snapshot->size;
   fent->manifest->set_modtime( snapshot->manifest_mtime_sec, snapshot->manifest_mtime_nsec );
   
   return 0;
}


// set up a replica context
int replica_context_init( struct replica_context* rctx,
                          struct replica_snapshot* snapshot,
                          int type,
                          int op,
                          FILE* block_data,             // non-NULL if replicating a block.  It will NOT be duplicated, so the caller should ensure it will stick around.
                          char* manifest_data,          // non-NULL if replicating a manifest.  It will NOT be duplicated, so the caller should ensure it will stick around.
                          off_t data_len,               // number of bytes we're sending
                          unsigned char* hash,          // hash of data we're posting
                          size_t hash_len,              // length of hash
                          struct curl_httppost* form_data,
                          bool free_on_processed        // if true, free this structure once its removed from the rg_client
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
   
   // TODO: connection pool
   rctx->curls = new vector<CURL*>();
   rctx->type = type;
   rctx->op = op;
   rctx->size = data_len;
   rctx->form_data = form_data;
   rctx->free_on_processed = free_on_processed;
   
   if( hash ) {
      rctx->hash_len = hash_len;
      rctx->hash = CALLOC_LIST( unsigned char, hash_len );
      memcpy( rctx->hash, hash, hash_len );
   }
   
   memcpy( &rctx->snapshot, snapshot, sizeof(struct replica_snapshot) );
   
   sem_init( &rctx->processing_lock, 0, 1 );
   
   return 0;
}


// free a replica context
int fs_entry_replica_context_free( struct replica_context* rctx ) {
   
   if( rctx->hash ) {
      free( rctx->hash );
      rctx->hash = NULL;
   }
   
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      if( rctx->curls->at(i) != NULL ) {
         
         if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
            dbprintf("curl_easy_cleanup %s %" PRIX64 "/manifest.%" PRId64 ".%d\n",
                     (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
         }
         else {
            dbprintf("curl_easy_cleanup %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
                     (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
         }
         
         curl_easy_cleanup( rctx->curls->at(i) );
         rctx->curls->at(i) = NULL;
      }
   }
   
   delete rctx->curls;
   
   if( rctx->form_data ) {
      curl_formfree( rctx->form_data );
      rctx->form_data = NULL;
   }
   
   if( rctx->type == REPLICA_CONTEXT_TYPE_BLOCK ) {
      
      dbprintf("free %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
               rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
      
      
      if( rctx->file ) {
         fclose( rctx->file );
         rctx->file = NULL;
      }
   }
   else if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
      dbprintf("free %p %s %" PRIX64 "/manifest.%" PRId64 ".%d\n",
               rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
      
      
      if( rctx->data ) {
         free( rctx->data );
         rctx->data = NULL;
      }
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
                                   CURLFORM_COPYCONTENTS, replica_info_str.data(),
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
int replica_add_file_form( struct curl_httppost** form_data, struct curl_httppost** last, FILE* f, off_t size ) {
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
   replica_info->set_file_mtime_sec( snapshot->manifest_mtime_sec );
   replica_info->set_file_mtime_nsec( snapshot->manifest_mtime_nsec );
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
int replica_context_manifest( struct fs_core* core, struct replica_context* rctx, char const* fs_path, struct fs_entry* fent ) {
   
   // get the manifest data
   char* in_manifest_data = NULL;
   ssize_t in_manifest_data_len = 0;
   int rc = 0;
   
   // generate a signed serialized manifest
   in_manifest_data_len = fs_entry_serialize_manifest( core, fent, &in_manifest_data, true );
   if( in_manifest_data_len < 0 ) {
      errorf("fs_entry_serialize_manifest(%" PRIX64 ") rc = %zd\n", fent->file_id, in_manifest_data_len);
      return -EINVAL;
   }
   
   // pre-upload driver processing 
   char* manifest_data = NULL;
   size_t manifest_data_len = 0;
   
   struct timespec manifest_ts;
   fent->manifest->get_modtime( &manifest_ts );
   
   rc = driver_write_manifest_preup( core, core->closure, fs_path, fent, manifest_ts.tv_sec, manifest_ts.tv_nsec, in_manifest_data, in_manifest_data_len, &manifest_data, &manifest_data_len );
   free( in_manifest_data );
   
   if( rc != 0 ) {
      errorf("driver_write_manifest_preup(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      return rc;
   }
   
   // snapshot this fent
   struct replica_snapshot snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &snapshot );
   
   // hash the manifest
   unsigned char* hash = sha256_hash_data( manifest_data, manifest_data_len );
   size_t hash_len = sha256_len();
   
   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::MANIFEST, &snapshot, manifest_data_len, hash, hash_len );
   
   if( rc != 0 ) {
      errorf("replica_populate_request rc = %d\n", rc );
      free( manifest_data );
      free( hash );
      
      return -EINVAL;
   }
   
   // sign the metadata portion
   rc = md_sign< ms::ms_gateway_request_info >( core->ms->my_key, &replica_info );
   if( rc != 0 ) {
      errorf("md_sign rc = %d\n", rc );
      free( manifest_data );
      free( hash );
      
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
      free( hash );
   
      return -EINVAL;
   }
   
   // data form
   rc = replica_add_data_form( &form_data, &last, manifest_data, manifest_data_len );
   if( rc != 0 ) {
      errorf("replica_add_data_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      free( manifest_data );
      free( hash );
   
      return -EINVAL;
   }
   
   // set up the replica context
   // caller will free it.
   rc = replica_context_init( rctx, &snapshot, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_POST, NULL, manifest_data, manifest_data_len, hash, hash_len, form_data, false );
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      free( manifest_data );
      free( hash );
      
      return -EINVAL;
   }
   
   ////////////////////////////////////////////////////////
   
   char* manifest_str = fent->manifest->serialize_str();
   char* manifest_hash = sha256_printable( hash );
   
   dbprintf("Manifest:\n%s\n", manifest_str);
   dbprintf("Manifest hash: %s\n", manifest_hash );
   dbprintf("Manifest length: %zu\n", manifest_data_len );
   
   free( manifest_str );
   free( manifest_hash );
   
   
   ////////////////////////////////////////////////////////
   
   free( hash );
   
   return 0;
}


// create a block replica context
// fent must be read-locked
int replica_context_block( struct fs_core* core, struct replica_context* rctx, struct fs_entry* fent, uint64_t block_id, int64_t block_version, unsigned char* hash, size_t hash_len, int block_fd ) {
   
   FILE* f = fdopen( block_fd, "r" );
   if( f == NULL ) {
      int errsv = -errno;
      errorf( "fdopen(%d) errno = %d\n", block_fd, errsv );
      return errsv;
   }

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
   fs_entry_replica_snapshot( core, fent, block_id, block_version, &snapshot );

   // build an update
   ms::ms_gateway_request_info replica_info;
   rc = replica_populate_request( &replica_info, ms::ms_gateway_request_info::BLOCK, &snapshot, sb.st_size, hash, hash_len );
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
   
   rc = replica_add_file_form( &form_data, &last, f, sb.st_size );
   if( rc != 0 ) {
      errorf("replica_add_file_form( %" PRIX64 " ) rc = %d\n", fent->file_id, rc );
      
      curl_formfree( form_data );
      fclose( f );
      
      return rc;
   }
   
   // set up the replica context.
   // caller will free it.
   rc = replica_context_init( rctx, &snapshot, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_POST, f, NULL, sb.st_size, hash, hash_len, form_data, false );
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
   memset( fake_hash, 0, 256 );
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
   // we'll free it internally.
   rc = replica_context_init( rctx, snapshot, REPLICA_CONTEXT_TYPE_MANIFEST, REPLICA_DELETE, NULL, NULL, 0, NULL, 0, form_data, true );
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
   
   // put random bits into the hash field, for some salting
   unsigned char fake_hash[256];
   memset( fake_hash, 0, 256 );
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
   // we'll free it internally
   rc = replica_context_init( rctx, snapshot, REPLICA_CONTEXT_TYPE_BLOCK, REPLICA_DELETE, NULL, NULL, 0, NULL, 0, form_data, true );
   
   if( rc != 0 ) {
      errorf("replica_context_init(%" PRIX64 ") rc = %d\n", snapshot->file_id, rc );
      
      curl_formfree( form_data );
      
      return -EINVAL;
   }
   
   return 0;
}


// add a CURL handle to our running uploads
// synrp->uploads should be locked
static void replica_insert_upload( struct rg_client* synrp, CURL* curl, struct replica_context* rctx ) {
   // add to running 

   if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
      dbprintf("%s: running %p %" PRIX64 "/manifest.%" PRId64 ".%d\n", synrp->process_name, rctx, rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
   }
   else {
      dbprintf("%s: running %p %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", synrp->process_name, rctx, rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
   }
   
   (*synrp->uploads)[ curl ] = rctx;
   curl_multi_add_handle( synrp->running, curl );
}

// add a CURL handle to our penduing uploads
// synrp->pending should be locked
static void replica_insert_pending_upload( struct rg_client* synrp, CURL* curl, struct replica_context* rctx ) {
   if( !synrp->accepting )
      return;
   
   // add to running 
   if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
      dbprintf("%s: pending %p %" PRIX64 "/manifest.%" PRId64 ".%d\n", synrp->process_name, rctx, rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
   }
   else {
      dbprintf("%s: pending %p %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", synrp->process_name, rctx, rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
   }
   
   
   (*synrp->pending_uploads)[ curl ] = rctx;
}

// add a replica context's CURL handles to our running uploads
// synrp->uploads should be locked
static void replica_insert_pending_uploads( struct rg_client* synrp, struct replica_context* rctx ) {
   if( !synrp->accepting )
      return;
   
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      replica_insert_pending_upload( synrp, rctx->curls->at(i), rctx );
   }
}

// connect a replica context to the RGs, and begin processing it
int replica_context_connect( struct rg_client* rp, struct replica_context* rctx ) {
   
   // replicate everywhere
   uint64_t* rg_ids = ms_client_RG_ids( rp->ms );

   // acquire this lock, since we're processing it now...
   sem_wait( &rctx->processing_lock );
   
   int rc = 0;
   size_t num_rgs = 0;
   
   for( int i = 0; rg_ids[i] != 0; i++ ) {
      
      char* rg_base_url = ms_client_get_RG_content_url( rp->ms, rg_ids[i] );
      
      CURL* curl = curl_easy_init();
      
      if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
         dbprintf("%s: Connect %p %s %" PRIX64 "/manifest.%" PRId64 ".%d\n", rp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"),
                  rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
      }
      else {
         dbprintf("%s: Connect %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", rp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"),
                  rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
      }
      
      md_init_curl_handle( rp->conf, curl, rg_base_url, rp->conf->replica_connect_timeout );
      
      // prepare upload
      curl_easy_setopt( curl, CURLOPT_POST, 1L );
      curl_easy_setopt( curl, CURLOPT_HTTPPOST, rctx->form_data );
      
      // if we're deleting, change the method
      if( rctx->op == REPLICA_DELETE ) {
         curl_easy_setopt( curl, CURLOPT_CUSTOMREQUEST, "DELETE" );
      }
      
      num_rgs += 1;
      
      free( rg_base_url );
      
      rctx->curls->push_back( curl );
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
int replica_multi_upload( struct rg_client* synrp ) {
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
   for( int i = 0; i < maxfd+1; i++ ) {
      if( FD_ISSET(i, &fdread) ) {
         int frc = fcntl( i, F_GETFL );
         if( frc < 0 ) {
            frc = -errno;
            errorf("fcntl %d (fdread) errno %d\n", i, frc );
         }
      }
   }
   
   for( int i = 0; i < maxfd+1; i++ ) {
      if( FD_ISSET( i, &fdwrite ) ) {
         int frc = fcntl( i, F_GETFL );
         if( frc < 0 ) {
            frc = -errno;
            errorf("fcntl %d (fdwrite) errno %d\n", i, frc );
         }
      }
   }
   
   for( int i = 0; i < maxfd+1; i++ ) {
      if( FD_ISSET( i, &fdexcep ) ) {
         int frc = fcntl( i, F_GETFL );
         if( frc < 0 ) {
            frc = -errno;
            errorf("fcntl %d (fdexcep) errno %d\n", i, frc );
         }
      }
   }
   
   rc = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);
   
   if( rc < 0 ) {
      // probably due to removing a handle
      errorf("%s: select rc = %d, errno = %d\n", synrp->process_name, rc, -errno );
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
static int replica_erase_upload_context( struct rg_client* synrp, const replica_upload_set::iterator& itr ) {
   
   struct replica_context* rctx = itr->second;
   
   int rc = 0;
   
   CURL* curl = itr->first;
   
   curl_multi_remove_handle( synrp->running, curl );
   synrp->uploads->erase( itr );
   
   // clear this curl
   int still_processing = 0;
   
   for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
      if( rctx->curls->at(i) == curl ) {
         // destroy this upload, and erase it from this context's curl handles
         if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
            dbprintf("%s: curl_easy_cleanup %p's curl %p: %s %" PRIX64 "/manifest.%" PRId64 ".%d\n",
                     synrp->process_name, rctx, curl, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
         }  
         else {
            dbprintf("%s: curl_easy_cleanup %p's curl %p: %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
                     synrp->process_name, rctx, curl, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
         }
         
         curl_easy_cleanup( rctx->curls->at(i) );
         (*rctx->curls)[i] = NULL;
      }
      
      else if( rctx->curls->at(i) != NULL )
         still_processing ++;
   }
   
   // have all of this context's CURL handles finished?
   // and, are we supposed to free this replica context when this happens?
   if( still_processing == 0 ) {
      
      if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
         dbprintf("%s: Unqueued %p %s %" PRIX64 "/manifest.%" PRId64 ".%d\n",
                  synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
      }
      else {
         dbprintf("%s: Unqueued %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
                  synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
      }
      
      bool free_on_processed = rctx->free_on_processed;
      
      // signal anyone waiting for this to have finished
      sem_post( &rctx->processing_lock );
      
      if( free_on_processed ) {
         // destroy this
         fs_entry_replica_context_free( rctx );
         free( rctx );
      }
   }   
   
   return rc;
}


// does a replica context match a set of snapshot attributes?
static bool replica_context_snapshot_match( struct replica_context* rctx, struct replica_snapshot* snapshot ) {
   return (rctx->snapshot.volume_id == snapshot->volume_id &&
          rctx->snapshot.file_id == snapshot->file_id &&
          rctx->snapshot.file_version == snapshot->file_version &&
          rctx->snapshot.block_id == snapshot->block_id &&
          rctx->snapshot.block_version == snapshot->block_version &&
          rctx->snapshot.fent_mtime_sec == snapshot->fent_mtime_sec &&
          rctx->snapshot.fent_mtime_nsec == snapshot->fent_mtime_nsec &&
          rctx->snapshot.manifest_mtime_sec == snapshot->manifest_mtime_sec &&
          rctx->snapshot.manifest_mtime_nsec == snapshot->manifest_mtime_nsec);
}


// cancel a replica context, based on known attributes
static int replica_cancel_contexts( struct rg_client* synrp, struct replica_snapshot* snapshot ) {
   int num_erased = 0;
   
   set<struct replica_context*> to_free;
   
   // search replicas that are about to start
   pthread_mutex_lock( &synrp->pending_lock );
   
   // clear out any pending instances of this replica_snapshot
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
      
      fs_entry_replica_context_free( rctx );
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
int replica_process_responses( struct rg_client* synrp ) {
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
            }
            
            // check HTTP code
            long http_status = 0;
            int crc = curl_easy_getinfo( msg->easy_handle, CURLINFO_RESPONSE_CODE, &http_status );
            if( crc != 0 && rctx->error == 0 ) {
               rctx->error = -ENODATA;
            }
            else if( http_status != 200 ) {
               
               if( http_status == 404 ) {
                  rctx->error = -ENOENT;
               }
               else if( http_status == 403 ) {
                  rctx->error = -EACCES;
               }
               else if( http_status == 503 ) {
                  rctx->error = -EAGAIN;
               }
               else {
                  rctx->error = -EREMOTEIO;
               }
            }
            
         
            if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
               dbprintf("%s: Finished %p %s %" PRIX64 "/manifest.%" PRId64 ".%d, rc = %d, curl rc = %d, HTTP status = %ld\n", 
                        synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"),
                        rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec, rctx->error, msg->data.result, http_status );
            }
            else {
               dbprintf("%s: Finished %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "], rc = %d, curl rc = %d, HTTP status = %ld\n",
                        synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version, rctx->error, msg->data.result, http_status );
            }
            
            replica_erase_upload_context( synrp, itr );
         }
      }
   } while( msg != NULL );
   
   return rc;
}


// start up any pending replicas 
static void replica_start_pending( struct rg_client* synrp ) {
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
}

// cancel any replicas 
static void replica_process_cancels( struct rg_client* synrp ) {
   if( synrp->has_cancels ) {
      pthread_mutex_lock( &synrp->cancel_lock );
      
      for( unsigned int i = 0; i < synrp->pending_cancels->size(); i++ ) {
         
         struct replica_snapshot* snapshot = &synrp->pending_cancels->at(i);
         
         for( replica_upload_set::iterator itr = synrp->uploads->begin(); itr != synrp->uploads->end();  ) {
            replica_upload_set::iterator may_cancel_itr = itr;
            
            struct replica_context* rctx = may_cancel_itr->second;
            itr++;
            
            if( replica_context_snapshot_match( rctx, snapshot ) ) {
               
               if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
                  dbprintf("%s: Cancel %s %" PRIX64 "/manifest.%" PRId64 ".%d\n",
                           synrp->process_name, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
               }
               else {
                  dbprintf("%s: Cancel %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n",
                           synrp->process_name, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
               }
               
               // remove this running upload, freeing it if the caller didn't intend to do so.
               replica_erase_upload_context( synrp, may_cancel_itr );
            }
         }
      }
      
      synrp->pending_cancels->clear();
      
      synrp->has_cancels = false;
      pthread_mutex_unlock( &synrp->cancel_lock );
   }
}

// expire any timed-out requests 
static void replica_expire_requests( struct rg_client* synrp ) {
   // do we have replicas that have expired (timed out)?
   if( synrp->has_expires ) {
      pthread_mutex_lock( &synrp->expire_lock );
      
      for( replica_expire_set::iterator itr = synrp->pending_expires->begin(); itr != synrp->pending_expires->end(); itr++ ) {
         
         CURL* rctx_curl = *itr;
         
         replica_upload_set::iterator rctx_itr = synrp->uploads->find( rctx_curl );
         if( rctx_itr != synrp->uploads->end() ) {
            
            struct replica_context* rctx = rctx_itr->second;
            if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
               dbprintf("%s: Expire %" PRIX64 "/manifest.%" PRId64 ".%d\n", synrp->process_name, rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec );
            }
            else {
               dbprintf("%s: Expire %" PRIX64 "[%" PRIu64 ".%" PRId64 "]\n", synrp->process_name, rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version );
            }

            // remove this expired upload, freeing it if the caller didn't intend to do so.
            replica_erase_upload_context( synrp, rctx_itr );
         }
      }
      
      synrp->pending_expires->clear();
      
      synrp->has_expires = false;
      pthread_mutex_unlock( &synrp->expire_lock );
   }
}


// thread body for processing the work of a rg_client instance
void* replica_main( void* arg ) {
   struct rg_client* synrp = (struct rg_client*)arg;
   
   int rc = 0;
   
   dbprintf("%s: thread started\n", synrp->process_name);
   
   while( synrp->active ) {
      // run CURL for a bit
      pthread_mutex_lock( &synrp->running_lock );
      
      // start up any pending requests
      replica_start_pending( synrp );
      
      // process any requests for cancellations
      replica_process_cancels( synrp );
      
      // expire timed-out requests
      replica_expire_requests( synrp );
      
      // update our statistic
      synrp->num_uploads = synrp->uploads->size();
      
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


// begin uploading data.
// if when > 0, don't start immediately; start after $when seconds have passed
int replica_begin( struct rg_client* rp, struct replica_context* rctx) {
   
   if( !rp->accepting )
      return -ENOTCONN;
   
   int rc = replica_context_connect( rp, rctx );
   if( rc != 0 ) {
      
      if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
         errorf("%s: replica_context_connect( %" PRIX64 "/manifest.%" PRId64 ".%d ) rc = %d\n", rp->process_name, rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec, rc );
      }
      else {
         errorf("%s: replica_context_connect( %" PRIX64 "[%" PRIu64 ".%" PRId64 "] ) rc = %d\n", rp->process_name, rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version, rc );
      }
   
   }
   else {
      pthread_mutex_lock( &rp->pending_lock );
      
      replica_insert_pending_uploads( rp, rctx );
      
      rp->has_pending = true;
      pthread_mutex_unlock( &rp->pending_lock );
   }

   return rc;
}


// wait for a (synchronous) replica context to finish.
// if the deadline (ts) is passed, remove the handle.
int replica_wait_and_remove( struct rg_client* rp, struct replica_context* rctx, struct timespec* ts ) {
   
   // how many milliseconds to wait?
   int64_t ts_millis = 0;
   
   if( ts != NULL )
      ts_millis = (ts->tv_sec * 1000L) + (ts->tv_nsec / 1000000L);
   
   // wait (uninterrupted)
   int rc = md_download_sem_wait( &rctx->processing_lock, ts_millis );
   
   if( rc != 0 && ts_millis > 0 ) {
   
      // remove these handles on the next loop iteration
      pthread_mutex_lock( &rp->expire_lock );
      
      for( unsigned int i = 0; i < rctx->curls->size(); i++ ) {
         rp->pending_expires->insert( rctx->curls->at(i) );
      }
      
      rp->has_expires = true;
      
      pthread_mutex_unlock( &rp->expire_lock );
      
      // wait for the cancellation to be procesed
      rc = md_download_sem_wait( &rctx->processing_lock, 0 );
   }
   
   return rc;
}


// initalize a syndicate replication instance
int replica_init_replication( struct rg_client* rp, char const* name, struct md_syndicate_conf* conf, struct ms_client* client, uint64_t volume_id ) {
   pthread_mutex_init( &rp->running_lock, NULL );
   pthread_mutex_init( &rp->pending_lock, NULL );
   pthread_mutex_init( &rp->cancel_lock, NULL );
   pthread_mutex_init( &rp->expire_lock, NULL );
   
   rp->running = curl_multi_init();
   rp->process_name = strdup( name );
   
   rp->uploads = new replica_upload_set();
   rp->pending_uploads = new replica_upload_set();
   rp->pending_cancels = new replica_cancel_list();
   rp->pending_expires = new replica_expire_set();
   
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
int replica_shutdown_replication( struct rg_client* rp, int wait_replicas ) {
   rp->accepting = false;
   
   if( wait_replicas > 0 ) {
      dbprintf("Wait %d seconds for all replicas to finish in %s\n", wait_replicas, rp->process_name );
      sleep(wait_replicas);
   }
   else if( wait_replicas < 0 ) {
      dbprintf("Wait for all replicas to finish in %s\n", rp->process_name);
      
      do {
         dbprintf("%s: remaining: %d\n", rp->process_name, rp->num_uploads );
         
         if( rp->num_uploads > 0 ) {
            // log which ones are remaining 
            pthread_mutex_lock( &rp->running_lock );
            
            for( replica_upload_set::iterator itr = rp->uploads->begin(); itr != rp->uploads->end(); itr++ ) {
               
               // extract info 
               struct replica_context* rctx = itr->second;
               
               dbprintf("  replica %p\n", rctx );
            }
            
            pthread_mutex_unlock( &rp->running_lock );
         }
         
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
         
         fs_entry_replica_context_free( itr->second );
         
         free( itr->second );
      }
   }
   
   int need_pending_unlock = pthread_mutex_trylock( &rp->pending_lock );
   
   if( rp->pending_uploads != NULL ) {
      dbprintf("free %zu pending for %s\n", rp->pending_uploads->size(), rp->process_name);
      for( replica_upload_set::iterator itr = rp->pending_uploads->begin(); itr != rp->pending_uploads->end(); itr++ ) {
         
         fs_entry_replica_context_free( itr->second );
         
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
   
   rp->uploads = NULL;
   rp->pending_uploads = NULL;
   rp->pending_cancels = NULL;
   rp->pending_expires = NULL;
   
   if( need_running_unlock )
      pthread_mutex_unlock( &rp->pending_lock );
   
   if( need_pending_unlock )
      pthread_mutex_unlock( &rp->running_lock );
   
   pthread_mutex_destroy( &rp->pending_lock );
   pthread_mutex_destroy( &rp->running_lock );
   pthread_mutex_destroy( &rp->cancel_lock );
   pthread_mutex_destroy( &rp->expire_lock );
   
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

// start a set of block replications
// if block_futures is not NULL, this method inserts pointers to the generated contexts into it.
// the caller can then call replica_wait (or the like) to block until all contexts have been processed.
// in doing so, the caller must free them.
int replica_start_block_contexts( struct fs_core* core, struct rg_client* synrp, replica_list_t* block_rctxs, replica_list_t* block_futures ) {
   
   int rc = 0;
   replica_list_t running;
   
   // sanity check: if rctxs is NULL (i.e. caller won't wait to free them),
   // then the given contexts must have free_on_processed set.
   if( block_futures == NULL ) {
      for( unsigned int i = 0; i < block_rctxs->size(); i++ ) {
         if( !block_rctxs->at(i)->free_on_processed ) {
            errorf("%s", "Invalid argument: caller will not free replica context, but replica context not marked to be freed internally.  This will lead to a memory leak!\n");
            return -EINVAL;
         }
      }
   }
   
   // kick off the replicas
   for( unsigned int i = 0; i < block_rctxs->size(); i++ ) {
      
      dbprintf("begin block %p\n", block_rctxs->at(i) );
      
      rc = replica_begin( synrp, block_rctxs->at(i) );
      if( rc != 0 ) {
         errorf("replica_begin(block %p) rc = %d\n", block_rctxs->at(i), rc );
      }
      else {
         running.push_back( block_rctxs->at(i) );
      }
   }
   
   if( running.size() > 0 && block_futures ) {
      if( block_futures ) {
         // let the caller keep track of these replica_contexts as well.
         for( unsigned int i = 0; i < running.size(); i++ ) {
            block_futures->push_back( running[i] );
         }
      }
   }
   
   return rc;
}

// replicate a single block, asynchronously.
// fent must be at least read-locked.
struct replica_context* fs_entry_replicate_block_async( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, struct fs_entry_block_info* binfo, int* ret ) {
   
   int rc = 0;
   struct replica_context* block_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   rc = replica_context_block( core, block_rctx, fent, block_id, binfo->version, binfo->hash, binfo->hash_len, binfo->block_fd );
   if( rc != 0 ) {
      errorf("replica_context_block rc = %d\n", rc );
      free( block_rctx );
      block_rctx = NULL;
   }
   else {
      // begin replicating
      dbprintf("begin block %p\n", block_rctx );
      rc = replica_begin( &core->state->replication, block_rctx );
      if( rc != 0 ) {
         errorf("replica_begin( block %p ) rc = %d\n", block_rctx, rc );
         free( block_rctx );
         block_rctx = NULL;
      }
   }
   
   *ret = rc;
   return block_rctx;
}

// replicate a single manifest, asynchronously.
// fent must be at least read-locked
struct replica_context* fs_entry_replicate_manifest_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int* ret ) {
   
   if( fent->manifest == NULL || !fent->manifest->is_initialized() ) {
      dbprintf("Manifest for %" PRIX64 " is not initialized, so not replicating\n", fent->file_id );
      return 0;
   }
   
   struct replica_context* manifest_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   int rc = replica_context_manifest( core, manifest_rctx, fs_path, fent );
   if( rc != 0 ) {
      errorf("replica_context_manifest rc = %d\n", rc );
      free( manifest_rctx );
      manifest_rctx = NULL;
   }
   else {
      // begin replicating 
      dbprintf("begin manifest %p\n", manifest_rctx );
      rc = replica_begin( &core->state->replication, manifest_rctx );
      if( rc != 0 ) {
         errorf("replica_begin( manifest %p ) rc = %d\n", manifest_rctx, rc );
         free( manifest_rctx );
         manifest_rctx = NULL;
      }
   }
   
   *ret = rc;
   return manifest_rctx;
}


// replicate a manifest, synchronously
// fent must be read-locked
int fs_entry_replicate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   
   int rc = 0;
   struct replica_context* manifest_future = fs_entry_replicate_manifest_async( core, fs_path, fent, &rc );
   
   if( manifest_future == NULL || rc != 0 ) {
      errorf("fs_entry_replicate_manifest_async( %s %" PRIX64 " ) rc = %d\n", fs_path, fent->file_id, rc );
      return rc;
   }
   
   // wait for it to finish 
   rc = fs_entry_replica_wait( core, manifest_future, 0 );
   if( rc != 0 ) {
      errorf("fs_entry_replica_wait( %s %" PRIX64 " ) rc = %d\n", fs_path, fent->file_id, rc );
      
      fs_entry_replica_context_free( manifest_future );
      return rc;
   }
   
   // success!
   fs_entry_replica_context_free( manifest_future );
   
   return rc;
}

// replicate a sequence of modified blocks, asynchronously
// in modified_blocks, we need the version, hash, hash_len, and block_fd fields for each block.
// caller must free block_futures, even if the method returns in error
// fent must be read-locked
int fs_entry_replicate_blocks_async( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, replica_list_t* block_futures ) {
   replica_list_t block_rctxs;
   int rc = 0;
   
   for( modification_map::iterator itr = modified_blocks->begin(); itr != modified_blocks->end(); itr++ ) {
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* block_info = &itr->second;
      
      struct replica_context* block_rctx = CALLOC_LIST( struct replica_context, 1 );
      
      rc = replica_context_block( core, block_rctx, fent, block_id, block_info->version, block_info->hash, block_info->hash_len, block_info->block_fd );
      if( rc != 0 ) {
         errorf("replica_context_block rc = %d\n", rc );
         free( block_rctx );
      }
      else {
         block_rctxs.push_back( block_rctx );
      }
   }
   
   if( rc == 0 ) {
      rc = replica_start_block_contexts( core, &core->state->replication, &block_rctxs, block_futures );
   }
   else {
      fs_entry_replica_list_free( &block_rctxs );
   }
   
   return rc;
}


// replicate a sequence of modified blocks, synchronously 
// in modified_blocks, we need the version, hash, hash_len, and block_fd fields for each block.
// fent must be read-locked 
int fs_entry_replicate_blocks( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks ) {
   
   int rc = 0;
   replica_list_t block_futs;
   uint64_t file_id = fent->file_id;
   
   rc = fs_entry_replicate_blocks_async( core, fent, modified_blocks, &block_futs );
   
   if( rc != 0 ) {
      errorf("fs_entry_replicate_blocks_async( %" PRIX64 " ) rc = %d\n", file_id, rc );
      return rc;
   }
   
   // wait for them to finish 
   rc = fs_entry_replica_wait_all( core, &block_futs, 0 );
   if( rc != 0 ) {
      errorf("fs_entry_replica_wait_all( %" PRIX64 " ) rc = %d\n", file_id, rc );
      
      fs_entry_replica_list_free( &block_futs );
      return rc;
   }
   
   fs_entry_replica_list_free( &block_futs );
   return rc;
}

// garbage-collect a manifest replica.
int fs_entry_garbage_collect_manifest( struct fs_core* core, struct replica_snapshot* snapshot ) {
   // NOTE: don't need to do anything if the manifest wasn't initialized 
   if( !snapshot->manifest_initialized ) {
      dbprintf("Manifest for %" PRIX64 " was not initialized, so not garbage-collecting\n", snapshot->file_id );
      return 0;
   }
   
   struct replica_context* manifest_rctx = CALLOC_LIST( struct replica_context, 1 );
   
   int rc = replica_context_garbage_manifest( core, manifest_rctx, snapshot );
   if( rc != 0 ) {
      errorf("replica_context_garbage_manifest rc = %d\n", rc );
      free( manifest_rctx );
      return rc;
   }
   
   // if there are any pending uploads for this same manifest, stop them
   replica_cancel_contexts( &core->state->replication, snapshot );
   
   // proceed to replicate
   rc = replica_begin( &core->state->garbage_collector, manifest_rctx );
   
   if( rc != 0 ) {
      errorf("replica_begin(%p) rc = %d\n", manifest_rctx, rc );
      
      fs_entry_replica_context_free( manifest_rctx );
      free( manifest_rctx );
      return rc;
   }
   
   // will free once processed internally
   return rc;
}


// garbage-collect blocks
// in modified_blocks, we only need the version field for each block.
int fs_entry_garbage_collect_blocks( struct fs_core* core, struct replica_snapshot* snapshot, modification_map* modified_blocks ) {
   replica_list_t block_rctxs;
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
         
         fs_entry_replica_list_free( &block_rctxs );
         return rc;
      }
      else {
         // if there are any pending uploads for this block, then simply stop them.
         replica_cancel_contexts( &core->state->replication, &block_snapshot );
      
         // garbage collect!
         block_rctxs.push_back( block_rctx );
      }
   }
   
   rc = replica_start_block_contexts( core, &core->state->garbage_collector, &block_rctxs, NULL );
   
   if( rc != 0 ) {
      errorf("replica_start_block_contexts rc = %d\n", rc );
      
      for( replica_list_t::iterator itr = block_rctxs.begin(); itr != block_rctxs.end(); itr++ ) {
         fs_entry_replica_context_free( *itr );
      }
   }
   
   return rc;
}


// wait for all replication to finish, and then free them if they're internal to us.
int fs_entry_replica_wait_and_free( struct rg_client* synrp, replica_list_t* rctxs, struct timespec* timeout ) {
   int rc = 0;
   int worst_rc = 0;
   
   // set deadlines
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      if( rctxs->at(i) == NULL )
         continue;
      
      if( timeout != NULL ) {
         
         struct replica_context* rctx = rctxs->at(i);
         
         if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
            dbprintf("%s: wait %ld.%ld seconds for %p %s %" PRIX64 "/manifest.%" PRId64 ".%d rc = %d\n",
                     synrp->process_name, (long)timeout->tv_sec, (long)timeout->tv_nsec,
                     rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec, rc );
         }
         else {
            dbprintf("%s: wait %ld.%ld seconds for %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "] rc = %d\n",
                     synrp->process_name, (long)timeout->tv_sec, (long)timeout->tv_nsec,
                     rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version, rc );
         }
         
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
      
      struct replica_context* rctx = rctxs->at(i);
      int rrc = rctx->error;
      if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
         dbprintf("%s: wait and remove %p %s %" PRIX64 "/manifest.%" PRId64 ".%d rc = %d, rctx rc = %d\n",
                  synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec, rc, rrc );
      }
      else {
         dbprintf("%s: wait and remove %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "] rc = %d, rctx rc = %d\n",
                  synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version, rc, rrc );
      }
      
      if( rc != 0 ) {
         // waiting failed, somehow
         worst_rc = -EIO;
      }
      
      if( rrc != 0 ) {
         // more specific from rctx
         worst_rc = rrc;
      }
      
      if( rctx->free_on_processed ) {
         // caller will not free the replica_contexts--they're internal to this module.
         if( rctx->type == REPLICA_CONTEXT_TYPE_MANIFEST ) {
            dbprintf("%s: free %p %s %" PRIX64 "/manifest.%" PRId64 ".%d rc = %d, rctx rc = %d\n",
                     synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.manifest_mtime_sec, rctx->snapshot.manifest_mtime_nsec, rc, rrc );
         }
         else {
            dbprintf("%s: free %p %s %" PRIX64 "[%" PRIu64 ".%" PRId64 "] rc = %d, rctx rc = %d\n",
                     synrp->process_name, rctx, (rctx->op == REPLICA_POST ? "POST" : "DELETE"), rctx->snapshot.file_id, rctx->snapshot.block_id, rctx->snapshot.block_version, rc, rrc );
         }
         
         fs_entry_replica_context_free( rctxs->at(i) );
         free( rctxs->at(i) );
         
         (*rctxs)[i] = NULL;
      }
   }
   return worst_rc;
}


// wait for all replica contexts to finish
int fs_entry_replica_wait_all( struct fs_core* core, replica_list_t* rctxs, uint64_t transfer_timeout_ms ) {
   struct timespec *tsp = NULL;
   
   if( transfer_timeout_ms > 0 ) {
      struct timespec ts;
      ts.tv_sec = transfer_timeout_ms / 1000L;
      ts.tv_nsec = ((transfer_timeout_ms) % 1000L) * 1000000L;
      tsp = &ts;
   }
   
   int rc = fs_entry_replica_wait_and_free( &core->state->replication, rctxs, tsp );
   
   return rc;
}


// wait for a single replica context to finish 
int fs_entry_replica_wait( struct fs_core* core, struct replica_context* rctx, uint64_t transfer_timeout_ms ) {
   replica_list_t rlist;
   rlist.push_back( rctx );
   
   return fs_entry_replica_wait_all( core, &rlist, transfer_timeout_ms );
}

// free up all replica contexts following a replication
int fs_entry_replica_list_free( replica_list_t* rctxs ) {
   
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      
      if( rctxs->at(i) == NULL )
         continue;
      
      fs_entry_replica_context_free( rctxs->at(i) );
      free( rctxs->at(i) );
      
      (*rctxs)[i] = NULL;
   }
   
   rctxs->clear();
   
   return 0;
}


// extract failed block replica futures and put them into a modification map.
// this includes preserving the file descriptor (by dup-ing it and closing the old one)
// free successful futures--both the data each future contains, as well as the future itself.  As in, this has the same semantics as fs_entry_replica_list_free for successful blocks
int fs_entry_extract_block_info_from_failed_block_replicas( replica_list_t* rctxs, modification_map* dirty_blocks ) {
   
   for( unsigned int i = 0; i < rctxs->size(); i++ ) {
      
      // skip NULL
      if( rctxs->at(i) == NULL )
         continue;
      
      struct replica_context* rctx = rctxs->at(i);
      
      // not a block replica? just free it
      if( rctx->type != REPLICA_CONTEXT_TYPE_BLOCK ) {
         fs_entry_replica_context_free( rctx );
         free( rctx );
         (*rctxs)[i] = NULL;
         continue;
      }
      
      // should never happen, but check anyway
      if( rctx->file == NULL ) {
         errorf("%s", "BUG: replica context for block has a NULL file\n");
         continue;
      }
      
      // error?
      if( rctx->error != 0 ) {
         // this block is still dirty 
            
         // duplicate the file (so it doesn't get erased by the cache when we free the rctx)
         int newfd = dup( fileno( rctx->file ) );
         if( newfd < 0 ) {
            int errsv = -errno;
            
            errorf("dup errno = %d\n", errsv );
            return errsv;
         }
         
         lseek( newfd, 0, SEEK_SET );
         
         // block info 
         struct fs_entry_block_info binfo;
         memset( &binfo, 0, sizeof(struct fs_entry_block_info) );
         
         // duplicate the hash 
         unsigned char* hash_dup = CALLOC_LIST( unsigned char, rctx->hash_len );
         memcpy( hash_dup, rctx->hash, rctx->hash_len );
         
         // init the block info, with the separate hash (passed in, not copied)
         fs_entry_block_info_replicate_init( &binfo, rctx->snapshot.file_version, hash_dup, rctx->hash_len, rctx->snapshot.coordinator_id, newfd );
         
         (*dirty_blocks)[ rctx->snapshot.block_id ] = binfo;
      }
      
      // free the context 
      fs_entry_replica_context_free( rctx );
      free( rctx );
      (*rctxs)[i] = NULL;
   }
   
   rctxs->clear();
   
   return 0;
}


// garbage collect a file's data
// fent must be read-locked
int fs_entry_garbage_collect_file( struct fs_core* core, struct fs_entry* fent ) {
   if( !FS_ENTRY_LOCAL( core, fent ) )
      return -EINVAL;
   
   // anything to collect?
   if( fent->size == 0 )
      return 0;
   
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

