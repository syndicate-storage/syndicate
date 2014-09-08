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

#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/benchmark.h"
#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/url.h"

// hash a volume and file ID together to create a unique identifier for it
static long ms_client_hash( uint64_t volume_id, uint64_t file_id ) {
   locale loc;
   const collate<char>& coll = use_facet<collate<char> >(loc);

   char hashable[100];
   sprintf(hashable, "%" PRIu64 "%" PRIu64, volume_id, file_id );
   long ret = coll.hash( hashable, hashable + strlen(hashable) );

   return ret;
}


// post file data
static int ms_client_send( struct ms_client* client, char const* url, char const* data, size_t len, ms::ms_reply* reply, bool verify ) {
   
   struct curl_httppost *post = NULL, *last = NULL;
   int rc = 0;
   int http_response = 0;
   struct md_download_context dlctx;
   struct ms_client_timing times;
   char* buf = NULL;
   size_t buflen = 0;
   
   memset( &dlctx, 0, sizeof(struct md_download_context) );
   memset( &times, 0, sizeof(struct ms_client_timing) );
   
   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, data, CURLFORM_BUFFERLENGTH, len, CURLFORM_END );

   // do the upload
   struct timespec ts, ts2;
   BEGIN_TIMING_DATA( ts );

   rc = ms_client_upload_begin( client, url, post, &dlctx, &times );
   if( rc != 0 ) {
      errorf("ms_client_upload_begin(%s) rc = %d\n", url, rc );
      
      ms_client_timing_free( &times );
      curl_formfree( post );
      return rc;
   }
   
   // wait for it...
   http_response = ms_client_upload_end( client, &dlctx, &buf, &buflen );
   
   if( http_response != 200 ) {
      errorf("ms_client_upload_end(%s) rc = %d\n", url, http_response );
      
      ms_client_timing_free( &times );
      curl_formfree( post );
      return http_response;
   }
   
   ms_client_timing_log( &times );
   
   END_TIMING_DATA( ts, ts2, "MS send" );
   
   ms_client_timing_free( &times );
   
   // what happened?
   if( http_response == 200 ) {
      // got something!
      if( buflen > 0 ) {
         
         // this should be an ms_reply structure
         rc = ms_client_parse_reply( client, reply, buf, buflen, verify );
         
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
      }
      else {
         // no response...
         rc = -ENODATA;
      }
   }
   
   if( buf != NULL ) {
      free( buf );
   }
   
   curl_formfree( post );
   
   return rc;
}


// fill serializable char* fields in an ent, if they aren't there already.  Emit warnings if they aren't 
static int ms_client_md_entry_sanity_check( struct md_entry* ent ) {
   if( ent->name == NULL ) {
      errorf("WARNING: entry %" PRIX64 " name field is NULL\n", ent->file_id );
      ent->name = strdup("");
   }
   
   if( ent->parent_name == NULL ) {
      errorf("WARNING: entry %" PRIX64 " parent_name field is NULL\n", ent->file_id );
      ent->parent_name = strdup("");
   }
   
   return 0;
}

// convert an update_set into a protobuf
static int ms_client_update_set_serialize( ms_client_update_set* updates, ms::ms_updates* ms_updates ) {
   // populate the protobuf
   for( ms_client_update_set::iterator itr = updates->begin(); itr != updates->end(); itr++ ) {

      struct md_update* update = &itr->second;
      
      ms_client_md_entry_sanity_check( &update->ent );
      
      ms::ms_update* ms_up = ms_updates->add_updates();

      ms_up->set_type( update->op );

      ms::ms_entry* ms_ent = ms_up->mutable_entry();

      md_entry_to_ms_entry( ms_ent, &update->ent );
      
      // if this an UPDATE, then add the affected blocks 
      if( update->op == ms::ms_update::UPDATE ) {
         if( update->affected_blocks != NULL ) {
            for( size_t i = 0; i < update->num_affected_blocks; i++ ) {
               ms_up->add_affected_blocks( update->affected_blocks[i] );
            }
         }
      }
      
      // if this is a RENAME, then add the 'dest' argument
      else if( update->op == ms::ms_update::RENAME ) {
         ms::ms_entry* dest_ent = ms_up->mutable_dest();
         md_entry_to_ms_entry( dest_ent, &update->dest );
      }
      
      // if this is a SETXATTR, then set the flags, attr name, and attr value
      else if( update->op == ms::ms_update::SETXATTR ) {
         // sanity check...
         if( update->xattr_name == NULL || update->xattr_value == NULL ) {
            return -EINVAL;
         }
         
         // set flags 
         ms_up->set_xattr_create( (update->flags & XATTR_CREATE) ? true : false );
         ms_up->set_xattr_replace( (update->flags & XATTR_REPLACE) ? true : false );
       
         // set names
         ms_up->set_xattr_name( string(update->xattr_name) );
         ms_up->set_xattr_value( string(update->xattr_value, update->xattr_value_len) );
         
         // set requesting user 
         ms_up->set_xattr_owner( update->xattr_owner );
         ms_up->set_xattr_mode( update->xattr_mode );
      }
      
      // if this is a REMOVEXATTR, then set the attr name
      else if( update->op == ms::ms_update::REMOVEXATTR ) {
         // sanity check ...
         if( update->xattr_name == NULL )
            return -EINVAL;
         
         ms_up->set_xattr_name( string(update->xattr_name) );
      }
      
      // if this is a CHOWNXATTR, then set the attr name and owner 
      else if( update->op == ms::ms_update::CHOWNXATTR ) {
         if( update->xattr_name == NULL )
            return -EINVAL;
         
         ms_up->set_xattr_name( string(update->xattr_name) );
         ms_up->set_xattr_owner( update->xattr_owner );
      }
      
      // if this is a CHMODXATTR, then set the attr name and mode 
      else if( update->op == ms::ms_update::CHMODXATTR ) {
         if( update->xattr_name == NULL )
            return -EINVAL;
         
         ms_up->set_xattr_name( string(update->xattr_name) );
         ms_up->set_xattr_mode( update->xattr_mode );
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
   if( pkey == NULL ) {
      errorf("%s\n", "Private key is NULL!");
      return -EINVAL;
   }
   return md_sign<ms::ms_updates>( pkey, ms_updates );
}


// populate an ms_update 
// NOTE: ths is a shallow copy of ent and affected_blocks.  The caller should NOT free them; they'll be freed internally
int ms_client_populate_update( struct md_update* up, int op, int flags, struct md_entry* ent ) {
   memset( up, 0, sizeof(struct md_update) );
   up->op = op;
   up->flags = flags;
   up->affected_blocks = NULL;
   up->num_affected_blocks = 0;
   
   memcpy( &up->ent, ent, sizeof(struct md_entry) );
   return 0;
}

// add an update to an update set
static int ms_client_add_update( ms_client_update_set* updates, struct md_update* up ) {
   (*updates)[ ms_client_hash( up->ent.volume, up->ent.file_id ) ] = *up;
   return 0;
}

// post a record on the MS, synchronously.
int ms_client_file_post( struct ms_client* client, struct md_update* up, ms::ms_reply* reply ) {
   
   // put it into the update set
   ms_client_update_set updates;
   ms_client_add_update( &updates, up );
   
   return ms_client_send_updates( client, &updates, reply, true );
}

// random 64-bit number
uint64_t ms_client_make_file_id() {
   return (uint64_t)md_random64();
}


// create a file record on the MS, synchronously
int ms_client_create( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {
   
   ent->type = MD_ENTRY_FILE;
   
   uint64_t file_id = ms_client_make_file_id();
   uint64_t old_file_id = ent->file_id;
   ent->file_id = file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", file_id );
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::CREATE, 0, ent );
   
   // reply buffer
   ms::ms_reply reply;
   
   int rc = ms_client_file_post( client, &up, &reply );
   
   if( rc == 0 ) {
      // did we get back a file_id and write_nonce?
      if( reply.listing().entries_size() > 0 ) {
         
         *file_id_ret = reply.listing().entries(0).file_id();
         *write_nonce_ret = reply.listing().entries(0).write_nonce();
         
         dbprintf("output file_id: %" PRIX64 " write_nonce: %" PRId64 "\n", *file_id_ret, *write_nonce_ret );
      }
      else {
         rc = -ENODATA;
      }
   }

   ent->file_id = old_file_id;
   
   return rc;
}


// make a directory on the MS, synchronously
int ms_client_mkdir( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {   
   ent->type = MD_ENTRY_DIR;
   
   uint64_t file_id = ms_client_make_file_id();
   uint64_t old_file_id = ent->file_id;
   ent->file_id = file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", file_id );
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::CREATE, 0, ent );

   // put it into the update set
   ms_client_update_set updates;
   ms_client_add_update( &updates, &up );
   
   // reply buffer
   ms::ms_reply reply;
   
   int rc = ms_client_file_post( client, &up, &reply );
   
   if( rc == 0 ) {
      // did we get back a file_id and write_nonce?
      if( reply.listing().entries_size() > 0 ) {
         
         *file_id_ret = reply.listing().entries(0).file_id();
         *write_nonce_ret = reply.listing().entries(0).write_nonce();
         
         dbprintf("output file_id: %" PRIX64 " write_nonce: %" PRId64 "\n", *file_id_ret, *write_nonce_ret );
      }
      else {
         rc = -ENODATA;
      }
   }

   ent->file_id = old_file_id;
   
   return rc;
}

// delete a record on the MS, synchronously
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::DELETE, 0, ent );
   
   return ms_client_file_post( client, &up, NULL );
}

// update a record on the MS, synchronously, due to a write()
int ms_client_update_write( struct ms_client* client, int64_t* write_nonce_ret, struct md_entry* ent, uint64_t* in_affected_blocks, size_t num_affected_blocks ) {
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::UPDATE, 0, ent );
   
   uint64_t* affected_blocks = NULL;
   
   // add affected blocks 
   if( in_affected_blocks != NULL ) {
      
      affected_blocks = CALLOC_LIST( uint64_t, num_affected_blocks );
      memcpy( affected_blocks, in_affected_blocks, num_affected_blocks * sizeof(uint64_t) );
      
      up.affected_blocks = affected_blocks;
   }
   
   up.num_affected_blocks = num_affected_blocks;
   
   ms::ms_reply reply;
   
   int rc = ms_client_file_post( client, &up, &reply );
   
   if( rc == 0 ) {
      // did we get back a write_nonce?
      if( reply.listing().entries_size() > 0 ) {
         
         *write_nonce_ret = reply.listing().entries(0).write_nonce();
         
         dbprintf("updated write_nonce: %" PRId64 "\n", *write_nonce_ret );
      }
      else {
         rc = -ENODATA;
      }
   }
   
   // clean up
   if( affected_blocks != NULL ) {
      free( affected_blocks );
      up.affected_blocks = NULL;
      up.num_affected_blocks = 0;
   }
   
   return rc;
}

// update a record on the MS, synchronously, NOT due to a write()
int ms_client_update( struct ms_client* client, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_update_write( client, write_nonce_ret, ent, NULL, 0 );
}

// change coordinator ownership of a file on the MS, synchronously
int ms_client_coordinate( struct ms_client* client, uint64_t* new_coordinator, int64_t* write_nonce, struct md_entry* ent ) {
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::CHCOORD, 0, ent );
   
   // reply buffer 
   ms::ms_reply reply;
   
   int rc = ms_client_file_post( client, &up, &reply );
   
   if( rc == 0 ) {
      // got back a coordinator?
      if( reply.listing().entries_size() > 0 ) {
         
         *new_coordinator = reply.listing().entries(0).coordinator();
         *write_nonce = reply.listing().entries(0).write_nonce();
         
         dbprintf("New coordinator of %" PRIX64 " is %" PRIu64 ", write_nonce = %" PRId64 "\n", ent->file_id, *new_coordinator, *write_nonce );
      }
      else {
         rc = -ENODATA;
      }
   }
   
   return rc;
}

// rename from src to dest 
int ms_client_rename( struct ms_client* client, int64_t* write_nonce, struct md_entry* src, struct md_entry* dest ) {
   if( src->volume != dest->volume )
      return -EXDEV;
   
   if( dest == NULL )
      return -EINVAL;
   
   // generate our update
   struct md_update up;
   ms_client_populate_update( &up, ms::ms_update::RENAME, 0, src );
   memcpy( &up.dest, dest, sizeof(struct md_entry) );

   ms::ms_reply reply;
   
   int rc = ms_client_file_post( client, &up, &reply );
   
   if( rc == 0 ) {
      // got back a write nonce??
      if( reply.listing().entries_size() > 0 ) {
         
         *write_nonce = reply.listing().entries(0).write_nonce();
         
         dbprintf("New write_nonce of %" PRIx64 " is %" PRId64 "\n", src->file_id, *write_nonce );
      }
      else {
         rc = -ENODATA;
      }
   }
   
   return rc;
}

// send a batch of updates.
// client must NOT be locked in any way.
int ms_client_send_updates( struct ms_client* client, ms_client_update_set* all_updates, ms::ms_reply* reply, bool verify_response ) {

   int rc = 0;
   
   // don't do anything if we have nothing to do
   if( all_updates->size() == 0 ) {
      // nothing to do
      return 0;
   }

   // pack the updates into a protobuf
   ms::ms_updates ms_updates;
   ms_client_update_set_serialize( all_updates, &ms_updates );

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
   
   uint64_t volume_id = ms_client_get_volume_id( client );

   // which Volumes are we sending off to?
   char* file_url = ms_client_file_url( client->url, volume_id );

   // send it off
   if( reply != NULL ) {
      rc = ms_client_send( client, file_url, update_text, update_text_len, reply, verify_response );
   }
   else {
      // internal reply buffer 
      ms::ms_reply internal_reply;
      rc = ms_client_send( client, file_url, update_text, update_text_len, &internal_reply, verify_response );
   }

   free( update_text );

   if( rc != 0 ) {
      errorf("ms_client_send(%s) rc = %d\n", file_url, rc );
      free( file_url );
      return rc;
   }

   free( file_url );
   
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
void ms_client_free_path( ms_path_t* path, void (*free_cls)(void*) ) {
   for( unsigned int i = 0; i < path->size(); i++ ) {
      ms_client_free_path_ent( &path->at(i), free_cls );
   }
}


// free all downloads 
static int ms_client_free_path_downloads( struct ms_client* client, struct md_download_context* path_downloads, unsigned int num_downloads ) {
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      if( !md_download_context_finalized( &path_downloads[i] ) ) {
         // wait for it...
         md_download_context_wait( &path_downloads[i], -1 );
      }      

      CURL* old_handle = NULL;
      md_download_context_free( &path_downloads[i], &old_handle );
      
      if( old_handle != NULL ) {
         curl_easy_cleanup( old_handle );
      }
      memset( &path_downloads[i], 0, sizeof(struct md_download_context) );
   }
   
   return 0;
}


// cancel all running downloads
static int ms_client_cancel_path_downloads( struct ms_client* client, struct md_download_context* path_downloads, unsigned int num_downloads ) {
   
   // cancel all downloads
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      if( !md_download_context_finalized( &path_downloads[i] ) ) {
         // cancel this 
         md_download_context_cancel( &client->dl, &path_downloads[i] );
      }
   }
   
   return 0;
}


// set up a path download 
static int ms_client_set_up_path_downloads( struct ms_client* client, ms_path_t* path, struct md_download_context** ret_path_downloads ) {
   
   unsigned int num_downloads = path->size();
   
   // fetch all downloads concurrently 
   struct md_download_context* path_downloads = CALLOC_LIST( struct md_download_context, num_downloads );
   
   int rc = 0;
   
   // set up downloads
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      // TODO: use a connection pool for the MS
      struct ms_path_ent* path_ent = &path->at(i);
      
      CURL* curl_handle = curl_easy_init();
      
      // NOTE: no cache driver for the MS, so we'll do this manually 
      char* url = ms_client_file_read_url( client->url, path_ent->volume_id, path_ent->file_id, path_ent->version, path_ent->write_nonce );
      
      md_init_curl_handle( client->conf, curl_handle, url, client->conf->connect_timeout );
      curl_easy_setopt( curl_handle, CURLOPT_USERPWD, client->userpass );
      curl_easy_setopt( curl_handle, CURLOPT_URL, url );
      
      free( url );
      
      rc = md_download_context_init( &path_downloads[i], curl_handle, NULL, NULL, -1 );
      if( rc != 0 ) {
         break;
      }
   }
   
   if( rc != 0 ) {
      // something failed 
      ms_client_free_path_downloads( client, path_downloads, num_downloads );
      free( path_downloads );
   }
   else {
      *ret_path_downloads = path_downloads;
   }
   
   return rc;
}


// run a set of downloads
// retry ones that time out, up to conf->max_metadata_read_retry times.
static int ms_client_run_path_downloads( struct ms_client* client, struct md_download_context* path_downloads, unsigned int num_downloads ) {
   
   int rc = 0;
   int num_running_downloads = num_downloads;
   
   // associate an attempt counter with each download, to handle timeouts
   map< struct md_download_context*, int > attempts;
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      attempts[ &path_downloads[i] ] = 0;
   }
   
   
   // set up a download set to track these downloads 
   struct md_download_set path_download_set;
   
   md_download_set_init( &path_download_set );
   
   // add all downloads to the download set 
   for( unsigned int i = 0; i < num_downloads; i++ ) {
         
      rc = md_download_set_add( &path_download_set, &path_downloads[i] );
      if( rc != 0 ) {
         errorf("md_download_set_add rc = %d\n", rc );
         
         md_download_set_free( &path_download_set );
         return rc;
      }
   }
   
   while( num_running_downloads > 0 ) {
   
      // wait for a download to complete 
      rc = md_download_context_wait_any( &path_download_set, -1 );
      
      if( rc != 0 ) {
         errorf("md_download_context_wait_any rc = %d\n", rc);
         break;
      }
      
      // re-tally this
      num_running_downloads = 0;
      
      vector<struct md_download_context*> succeeded;
      
      // find the one(s) that finished...
      for( md_download_set_iterator itr = md_download_set_begin( &path_download_set ); itr != md_download_set_end( &path_download_set ); itr++ ) {
         
         struct md_download_context* dlctx = md_download_set_iterator_get_context( itr );
         
         if( dlctx == NULL ) {
            continue;
         }
         if( !md_download_context_finalized( dlctx ) ) {
            num_running_downloads++;
            continue;
         }

         // process this finalized dlctx
         char* final_url = NULL;
         int http_status = md_download_context_get_http_status( dlctx );
         int os_err = md_download_context_get_errno( dlctx );
         int curl_rc = md_download_context_get_curl_rc( dlctx );
         md_download_context_get_effective_url( dlctx, &final_url );
         
         // serious MS error?
         if( http_status >= 500 ) {
            errorf("Path download %s HTTP status %d\n", final_url, http_status );
            
            rc = -EREMOTEIO;
            
            free( final_url );
            break;
         }
         
         // timed out?
         else if( curl_rc == CURLE_OPERATION_TIMEDOUT || os_err == -ETIMEDOUT ) {
            
            errorf("Path download %s timed out (curl_rc = %d, errno = %d, attempt %d)\n", final_url, curl_rc, os_err, attempts[dlctx] + 1);
            
            attempts[dlctx] ++;
            
            // try again?
            if( attempts[dlctx] < client->conf->max_metadata_read_retry ) {
               
               md_download_context_reset( dlctx, NULL );
               
               rc = md_download_context_start( &client->dl, dlctx, NULL, NULL );
               if( rc != 0 ) {
                  // shouldn't happen, but just in case 
                  errorf("md_download_context_start(%p) rc = %d\n", dlctx, rc );
                  
                  free( final_url );
                  break;
               }
            }
            else {
               // download failed, and we tried as many times as we could
               rc = -ENODATA;
               free( final_url );
               break;
            }
         }
         
         // some other error?
         else if( http_status != 200 || curl_rc != 0 ) {
            
            errorf("Path download %s failed, HTTP status = %d, cURL rc = %d, errno = %d\n", final_url, http_status, curl_rc, os_err );
            
            if( os_err != 0 ) {
               rc = os_err;
            }
            else {
               rc = -EREMOTEIO;
            }
            
            free( final_url );
            break;
         }
         
         // succeeded!
         free( final_url );
         succeeded.push_back( dlctx );
      }
      
      // clear the ones that succeeded 
      for( unsigned int i = 0; i < succeeded.size(); i++ ) {
         
         md_download_set_clear( &path_download_set, succeeded[i] );
      }
   }
   
   md_download_set_free( &path_download_set );
   
   // all downloads finished 
   return rc;
}


// run the path downloads in the download set, retrying any that fail due to timeouts
// on success, put the finalized download contexts into ret_path_downloads
static int ms_client_download_path_listing( struct ms_client* client, ms_path_t* path, struct md_download_context** ret_path_downloads ) {
   
   int rc = 0;
   unsigned int num_downloads = path->size();
   
   // initialize a download set to track these downloads
   struct md_download_context* path_downloads = NULL;
   
   rc = ms_client_set_up_path_downloads( client, path, &path_downloads );
   if( rc != 0 ) {
      errorf("ms_client_set_up_path_downloads rc = %d\n", rc );
      return rc;
   }
   
   // start downloads on the MS downloader
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      rc = md_download_context_start( &client->dl, &path_downloads[i], NULL, NULL );
      if( rc != 0 ) {
         // shouldn't happen, but just in case 
         errorf("md_download_context_start(%p (%" PRIX64 ")) rc = %d\n", &path_downloads[i], path->at(i).file_id, rc );
         break;
      }
   }
   
   // process all downloads 
   rc = ms_client_run_path_downloads( client, path_downloads, num_downloads );
   
   if( rc != 0 ) {
      // cancel everything 
      ms_client_cancel_path_downloads( client, path_downloads, num_downloads );
      ms_client_free_path_downloads( client, path_downloads, num_downloads );
      free( path_downloads );
   }
   else {
      *ret_path_downloads = path_downloads;
   }
   return rc;
}

// get a set of metadata entries.
// on succes, populate ms_response with ms_listing structures for each path entry that needed to be downloaded, as indicated by the stale flag.
int ms_client_get_listings( struct ms_client* client, ms_path_t* path, ms_response_t* ms_response ) {

   unsigned int num_downloads = path->size();

   if( num_downloads == 0 ) {
      // everything's fresh
      return 0;
   }
   
   struct md_download_context* path_downloads = NULL;
   struct timespec ts, ts2;
   
   BEGIN_TIMING_DATA( ts );
   
   int rc = ms_client_download_path_listing( client, path, &path_downloads );
   
   END_TIMING_DATA( ts, ts2, "MS recv" );
   
   if( rc != 0 ) {
      errorf("ms_client_perform_multi_download rc = %d\n", rc );
      
      return rc;
   }

   // got data! parse it
   unsigned int di = 0;
   for( unsigned int i = 0; i < path->size(); i++ ) {
      
      // get the buffer 
      char* buf = NULL;
      off_t buf_len = 0;
      
      rc = md_download_context_get_buffer( &path_downloads[i], &buf, &buf_len );
      if( rc != 0 ) {
         
         errorf("md_download_context_get_buffer rc = %d\n", rc );
         rc = -EIO;
         break;
      }
      
      // parse and verify
      ms::ms_reply reply;
      rc = ms_client_parse_reply( client, &reply, buf, buf_len, true );
      if( rc != 0 ) {
         errorf("ms_client_parse_reply rc = %d\n", rc );
         rc = -EIO;
         ms_client_free_response( ms_response );
         free( buf );
         break;
      }
      
      // verify that we have the listing 
      if( !reply.has_listing() ) {
         errorf("%s", "MS reply does not contain a listing\n" );
         rc = -ENODATA;
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
         errorf("ms_client_parse_listing rc = %d\n", rc );
         rc = -EIO;
         ms_client_free_response( ms_response );
         break;
      }
      
      // sanity check: listing[0], if given, must match the ith path element's file ID 
      if( listing.entries != NULL && listing.entries->size() != 0 ) {
         if( listing.entries->at(0).file_id != path->at(i).file_id ) {
            errorf("Invalid MS listing: requested listing of %" PRIX64 ", got listing of %" PRIX64 "\n", path->at(i).file_id, listing.entries->at(0).file_id );
            rc = -EBADMSG;
            ms_client_free_response( ms_response );
            break;
         }
      }
      
      // save
      (*ms_response)[ path->at(i).file_id ] = listing;
      di++;
   }

   ms_client_free_path_downloads( client, path_downloads, num_downloads );
   free( path_downloads );

   return rc;
}
