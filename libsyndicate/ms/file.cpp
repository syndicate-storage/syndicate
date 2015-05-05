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
#include "libsyndicate/download.h"

// state per download
struct ms_client_request_batch {
   
   int request_id;
   
   struct ms_client_timing* timing;
   
   char* serialized_updates;
   ssize_t serialized_updates_len;
   
   int* ops;                            // list of operations that this request entails
   uint64_t* file_ids;                  // inodes we're operating on (in correspondence with ops)
   int num_ops;                         // how many operations this context represents
   
   int result_offset;                   // offset into client result buffer where the results of these requests will be stored
   
   struct curl_httppost* forms;
   struct curl_slist* headers;
   
   char* url;
};

// convert an update_set into a protobuf
// return 0 on success
// return -EINVAL if the an update in updates is invalid
// return -ENOMEM if we're out of memory
static int ms_client_update_set_serialize( ms_client_update_set* updates, ms::ms_updates* ms_updates ) {
   
   try {
      // populate the protobuf
      for( ms_client_update_set::iterator itr = updates->begin(); itr != updates->end(); itr++ ) {

         struct md_update* update = &itr->second;
         
         // verify that we have a valid update type...
         if( update->op <= 0 || update->op >= ms::ms_update::NUM_UPDATE_TYPES ) {
            
            SG_error("Invalid update type %d\n", update->op );
            return -EINVAL;
         }
         
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
            if( update->xattr_name == NULL ) {
               return -EINVAL;
            }
            
            ms_up->set_xattr_name( string(update->xattr_name) );
         }
         
         // if this is a CHOWNXATTR, then set the attr name and owner 
         else if( update->op == ms::ms_update::CHOWNXATTR ) {
            if( update->xattr_name == NULL ) {
               return -EINVAL;
            }
            
            ms_up->set_xattr_name( string(update->xattr_name) );
            ms_up->set_xattr_owner( update->xattr_owner );
         }
         
         // if this is a CHMODXATTR, then set the attr name and mode 
         else if( update->op == ms::ms_update::CHMODXATTR ) {
            if( update->xattr_name == NULL ) {
               return -EINVAL;
            }
               
            ms_up->set_xattr_name( string(update->xattr_name) );
            ms_up->set_xattr_mode( update->xattr_mode );
         }
      }

      ms_updates->set_signature( string("") );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}


// convert an update set to a string
// return the number of bytes on success, and set *update_text 
// return -EINVAL if we failed to serialize
// return -ENOMEM if OOM
ssize_t ms_client_update_set_to_string( ms::ms_updates* ms_updates, char** update_text ) {
   
   string update_bits;
   bool valid;

   try {
      valid = ms_updates->SerializeToString( &update_bits );
   }
   catch( exception e ) {
      SG_error("%s", "failed to serialize update set\n");
      return -EINVAL;
   }

   if( !valid ) {
      SG_error("%s", "failed ot serialize update set\n");
      return -EINVAL;
   }

   *update_text = SG_CALLOC( char, update_bits.size() + 1 );
   if( *update_text == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( *update_text, update_bits.data(), update_bits.size() );
   return (ssize_t)update_bits.size();
}


// sign an update set
// return 0 on success
// return -EINVAL if pkey or ms_updates is NULL
// return -EINVAL if we can't sign 
// return -ENOMEM if OOM
static int ms_client_sign_updates( EVP_PKEY* pkey, ms::ms_updates* ms_updates ) {
   if( pkey == NULL || ms_updates == NULL ) {
      return -EINVAL;
   }
   return md_sign<ms::ms_updates>( pkey, ms_updates );
}


// populate an ms_update with an md_entry and associated fields
// always succeeds
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
// return 0 on success 
// return -ENOMEM on OOM
static int ms_client_add_update( ms_client_update_set* updates, struct md_update* up ) {
   
   try {
      (*updates)[ up->ent.file_id ] = *up;
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}

// generate the next file ID
uint64_t ms_client_make_file_id() {
   return (uint64_t)md_random64();
}


// allocate a multi-result
// return 0 on success 
// return -ENOMEM on OOM 
int ms_client_multi_result_init( struct ms_client_multi_result* result, size_t num_ents ) {
   
   memset( result, 0, sizeof(struct ms_client_multi_result) );
   
   result->ents = SG_CALLOC( struct md_entry, num_ents );
   if( result->ents == NULL ) {
      
      return -ENOMEM;
   }
   
   result->num_ents = num_ents;
   return 0;
}

// free a multi-result 
int ms_client_multi_result_free( struct ms_client_multi_result* result ) {
   
   if( result->ents != NULL ) {
      
      for( unsigned int i = 0; i < result->num_ents; i++ ) {
         md_entry_free( &result->ents[i] );
      }
      
      SG_safe_free( result->ents );
      result->num_ents = 0;
   }
   
   memset( result, 0, sizeof(struct ms_client_multi_result) );
   
   return 0;
}

// handle errors from a download 
// return 0 if there was no error
// return -EPERM if the download was cancelled, or is not finalized
// return -EAGAIN if the download should be retried 
// return negative if the download encountered a fatal error
int ms_client_download_parse_errors( struct md_download_context* dlctx ) {
   
   int rc = 0;
   int http_status = 0;
   int os_err = 0;
   int curl_rc = 0;
   
   // were we cancelled? or were we not started? or are we not finished?
   if( md_download_context_cancelled( dlctx ) || md_download_context_pending( dlctx ) || !md_download_context_finalized( dlctx ) ) {
      return -EPERM;
   }
   
   // download status?
   http_status = md_download_context_get_http_status( dlctx );
   os_err = md_download_context_get_errno( dlctx );
   curl_rc = md_download_context_get_curl_rc( dlctx );
   
   rc = md_download_interpret_errors( http_status, curl_rc, os_err );
   
   return rc;
}

// verify that a reply listing has no duplicate entries 
// return 0 if no duplicates
// return -EBADMSG if there are duplicates
// return -ENOMEM if OOM
static int ms_client_batch_verify_no_duplicate_listing( ms::ms_reply* reply ) {
      
   int rc = 0;

   // verify no duplicate IDs
   set<uint64_t> ids;
   
   try {
      for( int i = 0; i < reply->listing().entries_size(); i++ ) {
         
         string name = reply->listing().entries(i).name();
         uint64_t id = reply->listing().entries(i).file_id();
         
         if( ids.count(id) ) {
            SG_error("Duplicate entry '%s' (%" PRIX64 ")\n", name.c_str(), id );
            rc = -EBADMSG;
         }
         
         ids.insert( id );
      }
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return rc;
}


// verify that a reply has an appropriate response for each request (i.e. every successful request that expects an entry from the MS *has* an entry),
// return 0 if we succeeded
// return -EINVAL if the request context doesn't have the same number of requests as the reply
// return -EBADMSG if we're missing an entry, or have too many
static int ms_client_batch_verify_matching_responses( ms::ms_reply* reply, struct ms_client_request_batch* batch ) {
   
   int rc = 0;
   
   if( reply->errors_size() != batch->num_ops ) {
      SG_error("MS replied %d errors; expected %d\n", reply->errors_size(), batch->num_ops );
      return -EINVAL;
   }
   
   // verify that each request has a matching response
   int entry_count = 0;
   for( int i = 0; i < reply->errors_size(); i++ ) {
      
      int ms_rc = reply->errors(i);
      int op = batch->ops[i];
      
      if( MS_CLIENT_OP_RETURNS_ENTRY( op ) && ms_rc == 0 ) {
         
         // make sure we have an entry for this 
         if( entry_count >= reply->listing().entries_size() ) {
            
            // not enough
            SG_error("MS did not return an entry for operation %d (op #%d)\n", op, entry_count );
            rc = -EBADMSG;
            break;
         }
         else {
            
            // accounted 
            entry_count++;
         }
      }
   }
   
   // too many/not enough entries?
   if( rc == 0 && entry_count != reply->listing().entries_size() ) {
      SG_error("MS returned too many entries (expected %d, got %d)\n", entry_count, reply->listing().entries_size() );
      return -EBADMSG;
   }
   
   return rc;
}

// extract results from a reply.
// only process results if the MS gave back a reply for each request sent.
// make sure the MS gave back appropriate data (no duplicate entires).
// return 0 on success
// return -EBADMSG if the reply was malformed (i.e. missing fields, duplicate entries, etc.)
// return -ENODATA if we're missing replies
// return -EPERM if the MS reply error was nonzero (the error will be set in *reply_error)
// return -ENOMEM if OOM
static int ms_client_parse_results( ms::ms_reply* reply, struct ms_client_request_batch* batch, struct ms_client_request_result* results, int* reply_error ) {
   
   int rc = 0;
   int num_items_processed = 0;
   
   *reply_error = reply->error();
   
   if( *reply_error != 0 ) {
      return -EPERM;
   }
   
   num_items_processed = reply->errors_size();
   
   // sanity check--did anything get through at all?
   if( num_items_processed == 0 ) {
      SG_error("MS processed %d items\n", num_items_processed );
      
      return -ENODATA;
   }
   
   if( num_items_processed != batch->num_ops ) {
      SG_error("Partial data: Requested %d items, but %d were processed\n", batch->num_ops, num_items_processed );
      
      return -ENODATA;
   }
   
   if( reply->has_listing() ) {
      
      // verify no duplicate IDs
      rc = ms_client_batch_verify_no_duplicate_listing( reply );
      if( rc != 0 ) {
         
         // invalid message
         SG_error("ms_client_batch_verify_no_duplicate_listing rc = %d\n", rc );
         return rc;
      }
      
      // verify that each request has a matching response
      rc = ms_client_batch_verify_matching_responses( reply, batch );
      if( rc != 0 ) {
         
         // invalid message 
         SG_error("ms_client_batch_verify_matching_responses rc = %d\n", rc );
         return -EBADMSG;
      }
      
      // store at least partial results
      int k = 0;        // indexes reply->listing().entries
      for( int i = 0; i < num_items_processed; i++ ) {
         
         int ms_rc = reply->errors(i);
         int op = batch->ops[i];
         
         struct md_entry* ent = NULL;
         
         if( MS_CLIENT_OP_RETURNS_ENTRY( op ) && ms_rc == 0 ) {
            
            if( k >= reply->listing().entries_size() ) {
               // out of entries
               break;
            }
            
            // next entry in this batch
            ent = SG_CALLOC( struct md_entry, 1 );
            if( ent == NULL ) {
               rc = -ENOMEM;
               break;
            }
            
            rc = ms_entry_to_md_entry( reply->listing().entries(k), ent );
            if( rc != 0 ) {
               break;
            }
            
            SG_debug("%s (at %d + %d): output file_id: %" PRIX64 ", write_nonce: %" PRId64 ", coordinator_id: %" PRIu64 "\n", ent->name, batch->result_offset, i, ent->file_id, ent->write_nonce, ent->coordinator );
            
            k++;
         
            // verify the returned entry matches this request 
            if( ent->file_id != batch->file_ids[i] ) {
               SG_error("Mismatched file ID at %d: %" PRIX64 " != %" PRIX64 "\n", i, ent->file_id, batch->file_ids[i] );
               
               rc = -EBADMSG;
               break;
            }
         }
         
         // bundle the entry with its return code
         // put the result in place
         struct ms_client_request_result result;
         memset( &result, 0, sizeof(struct ms_client_request_result) );
         
         result.file_id = batch->file_ids[i];
         result.ent = ent;
         result.rc = ms_rc;
         result.reply_error = *reply_error;
         
         results[ batch->result_offset + i ] = result;
         
         ent = NULL;
      }
   }
   
   return rc;
}

// generate data to upload and HTTP forms wrapping it.
// return 0 on success, and set *serialized_text and *serialized_text_len to the serialized buffer holding all of the updates, and set *ret_post and *ret_last to a CURL form structure 
//   that maps the 'ms-metadata-updates' field to the serialized data.
// return positive error if we failed to generate forms (see curl error codes)
// return negative on error (result of either ms_client_update_set_serialize, ms_client_sign_updates, ms_client_update_set_to_string)
static int ms_client_request_serialize( struct ms_client* client, ms_client_update_set* all_updates, char** serialized_text, size_t* serialized_text_len, struct curl_httppost** ret_post, struct curl_httppost** ret_last ) {
   
   int rc = 0;
   
   // pack the updates into a protobuf
   ms::ms_updates ms_updates;
   rc = ms_client_update_set_serialize( all_updates, &ms_updates );
   if( rc != 0 ) {
      
      SG_error("ms_client_update_set_serialize rc = %d\n", rc );
      return rc;
   }

   // sign it
   rc = ms_client_sign_updates( client->gateway_key, &ms_updates );
   if( rc != 0 ) {
      
      SG_error("ms_client_sign_updates rc = %d\n", rc );
      return rc;
   }

   // make it a string
   char* update_text = NULL;
   ssize_t update_text_len = ms_client_update_set_to_string( &ms_updates, &update_text );

   if( update_text_len < 0 ) {
      
      SG_error("ms_client_update_set_to_string rc = %zd\n", update_text_len );
      return (int)update_text_len;
   }
   
   // generate curl headers 
   struct curl_httppost *post = NULL, *last = NULL;
   
   // send as multipart/form-data file
   rc = curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, update_text, CURLFORM_BUFFERLENGTH, update_text_len, CURLFORM_END );
   if( rc != 0 ) {
      
      return rc;
   }
   
   *ret_post = post;
   *ret_last = last;
   *serialized_text = update_text;
   *serialized_text_len = update_text_len;
   
   return 0;
}


// set up a request context 
// return 0 on success
// return -ENOMEM on OOM
// return negative on error (see ms_client_request_serialize)
static int ms_client_request_batch_init( struct ms_client* client, struct ms_client_request_batch* batch, ms_client_update_set* all_updates, int64_t result_offset ) {
   
   int rc = 0;
   struct curl_httppost *post = NULL, *last = NULL;
   char* serialized_text = NULL;
   size_t serialized_text_len = 0;
   
   memset( batch, 0, sizeof(struct ms_client_request_batch) );
   
   rc = ms_client_request_serialize( client, all_updates, &serialized_text, &serialized_text_len, &post, &last );
   if( rc != 0 ) {
      
      SG_error("ms_client_request_serialize rc = %d\n", rc );
      return rc;
   }
   
   // record order of operations on files 
   batch->ops = SG_CALLOC( int, batch->num_ops );
   batch->file_ids = SG_CALLOC( uint64_t, batch->num_ops );
   
   // benchmark data 
   batch->timing = SG_CALLOC( struct ms_client_timing, 1 );
   
   if( batch->ops == NULL || batch->file_ids == NULL || batch->timing == NULL ) {
      
      SG_safe_free( batch->ops );
      SG_safe_free( batch->file_ids );
      SG_safe_free( batch->timing );
      
      SG_safe_free( serialized_text );
      curl_formfree( post );
      return -ENOMEM;
   }
   
   batch->serialized_updates = serialized_text;
   batch->serialized_updates_len = serialized_text_len;
   batch->forms = post;
   batch->num_ops = all_updates->size();
   batch->result_offset = result_offset; 
   
   int i = 0;
   for( ms_client_update_set::iterator itr = all_updates->begin(); itr != all_updates->end(); itr++ ) {
      batch->ops[i] = itr->second.op;
      batch->file_ids[i] = itr->second.ent.file_id;
      i++;
   }
   
   return 0;
}


// free a request context 
// always succeeds
static int ms_client_request_batch_free( struct ms_client_request_batch* batch ) {
   
   if( batch->timing != NULL ) {
      ms_client_timing_free( batch->timing );
      SG_safe_free( batch->timing );
   }
   
   if( batch->headers != NULL ) {
      curl_slist_free_all( batch->headers );
      batch->headers = NULL;
   }
   
   if( batch->forms != NULL ) {
      curl_formfree( batch->forms );
      batch->forms = NULL;
   }
   
   SG_safe_free( batch->serialized_updates );
   SG_safe_free( batch->ops );
   SG_safe_free( batch->file_ids );
   SG_safe_free( batch->url );
   
   return 0;
}


// convert an update into a request 
// NOTE: this is a shallow copy!  do NOT free request
// always succeeds
static int ms_client_request_to_update( struct ms_client_request* request, struct md_update* up ) {

   // generate our update
   memset( up, 0, sizeof(struct md_update) );
   
   ms_client_populate_update( up, request->op, request->flags, request->ent );
   
   // affected blocks? shallow-copy them over so we can serialize
   if( request->affected_blocks != NULL ) {
      
      up->affected_blocks = request->affected_blocks;
      up->num_affected_blocks = request->num_affected_blocks;
   }
   
   // destination?  i.e. due to a RENAME?
   // shallow-copy it over
   if( request->dest != NULL ) {
      up->dest = *(request->dest);
   }
   
   return 0;
}


// set up all request batches from the requests
// return 0 on success, and set *request_batches and *num_request_batches
// return -ENOMEM on OOM 
static int ms_client_request_batch_setup( struct ms_client* client, struct ms_client_request* requests, size_t num_requests, struct ms_client_request_batch** request_batches, size_t* num_request_batches ) {
   
   // send requests as batches
   struct ms_client_request_batch* batches = NULL;
   size_t num_batches = num_requests / client->max_request_batch;
   
   if( (num_requests % client->max_request_batch) != 0 ) {
      num_batches++;
   }
   
   int rc = 0;
   int i = 0;   // index requests
   int k = 0;   // index request_batches
   
   while( (unsigned)i < num_requests ) {
      
      // next batch 
      ms_client_update_set updates;
      
      int offset = i;
      
      for( int j = 0; j < client->max_request_batch && (unsigned)i < num_requests; j++ ) {
         
         // generate our update (NOTE: shallow copy)
         struct md_update up;
         rc = ms_client_request_to_update( &requests[i], &up );
         
         if( rc != 0 ) {
            
            SG_error("ms_client_request_to_update( %" PRIX64 " ) rc = %d\n", requests[i].ent->file_id, rc );
            break;
         }
         
         rc = ms_client_add_update( &updates, &up );
         if( rc != 0 ) {
            
            SG_error("ms_client_add_update( %" PRIX64 " ) rc = %d\n", up.ent.file_id, rc );
            break;
         }
         
         // next request
         i++;
      }
      
      if( rc != 0 ) {
         break;
      }
      
      // stuff it into the next request 
      rc = ms_client_request_batch_init( client, &batches[k], &updates, offset );
      
      if( rc != 0 ) {
         
         SG_error("ms_client_request_batch_init( batch=%d ) rc = %d\n", k, rc );
         break;
      }
      
      k++;
   }
   
   if( rc != 0 ) {
      
      // failure; clean up 
      for( int i = 0; i < k; i++ ) {
         
         ms_client_request_batch_free( &batches[i] );
      }
      
      SG_safe_free( batches );
   }
   else {
      SG_debug("%zu requests / %d requests per batch = %zu uploads\n", num_requests, client->max_request_batch, num_batches );
      
      *request_batches = batches;
      *num_request_batches = k;
   }
   
   return rc;
}


// begin processing a batch of requests
// return 0 on success, and set up dlctx 
// return -ENOMEM on OOM
int ms_client_run_request_batch_begin( struct ms_client* client, struct ms_client_request_batch* batch, int request_id, struct md_download_loop* dlloop, struct md_download_context* dlctx ) {
   
   int rc = 0;
   char* url = NULL;
   uint64_t volume_id = ms_client_get_volume_id( client );
   CURL* curl = NULL;
   
   // TODO: connection pool 
   curl = curl_easy_init();
   if( curl == NULL ) {
      return -ENOMEM;
   }
   
   // make our URL
   url = ms_client_file_url( client->url, volume_id );
   if( url == NULL ) {
      
      curl_easy_cleanup( curl );
      return -ENOMEM;
   }
   
   ms_client_init_curl_handle( client, curl, url );
   
   // program this download context
   curl_easy_setopt( curl, CURLOPT_POST, 1L);
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, batch->forms );
   
   if( batch->timing != NULL ) {
      curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
      curl_easy_setopt( curl, CURLOPT_WRITEHEADER, batch->timing );      
   }
   
   batch->url = url;
   batch->request_id = request_id;
   
   // set up the download 
   rc = md_download_context_init( dlctx, curl, MS_MAX_MSG_SIZE, batch );
   if( rc != 0 ) {
   
      SG_error("md_download_context_init rc = %d\n", rc );
      
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      return rc;
   }
   
   // watch the download 
   rc = md_download_loop_watch( dlloop, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_loop_watch rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      
      return rc;
   }
   
   // start the download 
   rc = md_download_context_start( &client->dl, dlctx );
   if( rc != 0 ) {
      
      SG_error("md_download_context_start rc = %d\n", rc );
      
      md_download_context_free( dlctx, NULL );
      curl_easy_cleanup( curl );
      SG_safe_free( url );
      
      return rc;
   }
   
   return 0;
}


// finish a batch of requests, and free up the download handle
// return 0 on success 
// return -ENOMEM on OOM 
// return -EBADMSG if the data could not be parsed or verified
// return -ENOMEM if OOM
int ms_client_run_request_batch_end( struct ms_client* client, struct md_download_context* dlctx, struct ms_client_request_result* results, int* request_id ) {
   
   int rc = 0;
   struct ms_client_request_batch* batch = NULL;
   char* buf = NULL;
   off_t buf_len = 0;
   ms::ms_reply reply;
   int reply_error = 0;
   CURL* curl = NULL;
   
   // need to finalize?
   if( !md_download_context_finalized( dlctx ) ) {
      
      rc = md_download_context_wait( dlctx, -1 );
      if( rc != 0 ) {
         
         md_download_context_free( dlctx, &curl );
         curl_easy_cleanup( curl );
         
         SG_error("md_download_context_wait( %p ) rc = %d\n", dlctx, rc );
         return rc;
      }
   }
   
   // process download errors
   rc = ms_client_download_parse_errors( dlctx );
   if( rc != 0 ) {
      
      if( rc != -EAGAIN ) {
         // fatal error 
         SG_error("ms_client_download_parse_errors( %p ) rc = %d\n", dlctx, rc );
      }
      
      md_download_context_free( dlctx, &curl );
      curl_easy_cleanup( curl );
      
      return rc;
   }
   
   // extract batch
   batch = (struct ms_client_request_batch*)md_download_context_get_cls( dlctx );
   *request_id = batch->request_id;
   
   // get the buffer 
   rc = md_download_context_get_buffer( dlctx, &buf, &buf_len );
   if( rc != 0 || buf == NULL ) {
      
      SG_error("md_download_context_get_buffer( %p ) rc = %d\n", dlctx, rc );
      
      if( rc != -ENOMEM ) {
         rc = -EBADMSG;
      }
      
      md_download_context_free( dlctx, &curl );
      curl_easy_cleanup( curl );
      
      return rc;
   }
   
   // parse and verify
   rc = ms_client_parse_reply( client, &reply, buf, buf_len );
   
   SG_safe_free( buf );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_parse_reply rc = %d\n", rc );
      rc = -EBADMSG;
      
      md_download_context_free( dlctx, &curl );
      curl_easy_cleanup( curl );
      
      return rc;
   }
   
   // extract results from this batch
   rc = ms_client_parse_results( &reply, batch, results, &reply_error );
   if( rc != 0 ) {
      
      // failed to process
      SG_warn("ms_client_parse_results rc = %d, reply error = %d\n", rc, reply_error );
      
      if( rc == -EPERM ) {
         // mask this--the actual reply error is contained in the results 
         rc = 0;
      }
   }
   
   md_download_context_free( dlctx, &curl );
   curl_easy_cleanup( curl );
      
   return rc;
}


// run multiple batch requests, using a download config.
// request[i]'s result will be stored to results[i]
// NOTE: requests and results must be the same length
// return 0 on success, or if num_requests == 0
// return negative on error (see ms_client_multi_context_init, md_download_all)
int ms_client_run_requests( struct ms_client* client, struct ms_client_request* requests, struct ms_client_request_result* results, size_t num_requests ) {
   
   // sanity check 
   if( num_requests == 0 ) {
      return 0;
   }
   
   int rc = 0;
   struct md_download_loop dlloop;
   int* request_attempts = NULL;
   queue<int> request_queue;
   
   struct md_download_context* dlctx = NULL;
   
   struct ms_client_request_batch* batches = NULL;
   size_t num_batches = 0;
   
   // set up download loop 
   rc = md_download_loop_init( &dlloop, &client->dl, MIN( client->max_connections, (signed)(num_requests / client->max_request_batch) + 1 ) );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // set up attempt counts 
   request_attempts = SG_CALLOC( int, num_requests );
   if( request_attempts == NULL ) {
      
      md_download_loop_free( &dlloop );
      return -ENOMEM;
   }
   
   // set up batches 
   rc = ms_client_request_batch_setup( client, requests, num_requests, &batches, &num_batches );
   if( rc != 0 ) {
      
      // probably OOM 
      md_download_loop_free( &dlloop );
      SG_safe_free( request_attempts );
      
      return rc;
   }
   
   // queue all batches
   for( unsigned int i = 0; i < num_batches; i++ ) {
      
      request_queue.push( i );
   }
   
   // run the loop!
   do {
      
      // batch id
      int i = 0;
      
      // fill up the download loop
      while( request_queue.size() > 0 ) {
         
         // next request...
         i = request_queue.front();
         request_queue.pop();
         
         // next available download slot
         rc = md_download_loop_next( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            // filled the pipe?
            if( rc == -EAGAIN ) {
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_next rc = %d\n", rc );
            break;
         }
         
         // set up this download, and enqueue it on the client's downloader
         rc = ms_client_run_request_batch_begin( client, batches, i, &dlloop, dlctx );
         if( rc != 0 ) {
            
            SG_error("ms_client_run_request_begin( batch=%d ) rc = %d\n", i, rc );
            break;
         }
      }
      
      // failed to start?
      if( rc != 0 ) {
         break;
      }
      
      // run downloads 
      rc = md_download_loop_run( &dlloop );
      if( rc != 0 ) {
         
         SG_error("md_download_loop_run rc = %d\n", rc );
         break;
      }
      
      // finish up downloads 
      while( true ) {
         
         // find finished download
         rc = md_download_loop_finished( &dlloop, &dlctx );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // out of finished downloads
               rc = 0;
               break;
            }
            
            SG_error("md_download_loop_finish rc = %d\n", rc );
            break;
         }
         
         // process finished batch.  i will index requests
         rc = ms_client_run_request_batch_end( client, dlctx, results, &i );
         if( rc != 0 ) {
            
            if( rc == -EAGAIN ) {
               
               // try this one again 
               request_attempts[i]++;
               
               // too many attempts?
               if( request_attempts[i] >= client->conf->max_metadata_read_retry ) {
                  
                  SG_error("Retried batch %d too many times\n", i );
                  rc = -ENODATA;
                  break;
               }
               else {
                  
                  // try again 
                  request_queue.push( i );
                  rc = 0;
               }
            }
            else {
               
               // failed to finish the request batch
               SG_error("ms_client_run_request_batch_end rc = %d\n", rc );
               break;
            }
         }
      }
      
      if( rc != 0 ) {
         break;
      }
   
   } while( md_download_loop_running( &dlloop ) );
   
   if( rc != 0 ) {
      // stop all downloads
      md_download_loop_abort( &dlloop );
   }
   
   // clean up
   md_download_loop_cleanup( &dlloop, NULL, NULL );
   md_download_loop_free( &dlloop );
   SG_safe_free( request_attempts );
   
   for( unsigned int i = 0; i < num_batches; i++ ) {
      
      ms_client_request_batch_free( &batches[i] );
   }
   
   SG_safe_free( batches );
      
   return rc;
}

// initialize a create request.
// NOTE: this shallow-copies the data; do not free ent
// always succeeds
int ms_client_create_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::CREATE;
   
   return 0;
}

// initialize a create-async request.
// NOTE: this shallow-copies the data; do not free ent
// always succeeds
int ms_client_create_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::CREATE_ASYNC;
   
   return 0;
}

// initialize a mkdir request 
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_mkdir_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   return ms_client_create_request( client, ent, request );
}

// initialize a mkdir-async request 
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_mkdir_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   return ms_client_create_async_request( client, ent, request );
}

// initialize an update request (but not one for writes)
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_update_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   return ms_client_update_write_request( client, ent, NULL, 0, request );
}

// initialize an update-async request (but not one for writes)
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_update_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::UPDATE_ASYNC;
   request->affected_blocks = NULL;
   request->num_affected_blocks = 0;
   
   return 0;
}

// initialize an update request for a write 
// NOTE: this shallow-copies the data; do not free ent, affected_blocks
// always succeeds
int ms_client_update_write_request( struct ms_client* client, struct md_entry* ent, uint64_t* affected_blocks, size_t num_affected_blocks, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::UPDATE;
   request->affected_blocks = affected_blocks;
   request->num_affected_blocks = num_affected_blocks;
   
   return 0;
}


// initialize a coordinate request
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_coordinate_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::CHCOORD;
   
   return 0;
}


// initialize a rename request 
// NOTE: this shallow-copies the data; do not free src or dest 
// always succeeds
int ms_client_rename_request( struct ms_client* client, struct md_entry* src, struct md_entry* dest, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = src;
   request->dest = dest;
   
   return 0;
}

// initialize a delete request 
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_delete_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::DELETE;
   
   return 0;
}

// initialize a delete-async request
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_delete_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_update::DELETE_ASYNC;
   
   return 0;
}

// set the cls for a request 
// always succeeds
int ms_client_request_set_cls( struct ms_client_request* request, void* cls ) {
 
   request->cls = cls;
   return 0;
}

// free a single request result
// always succeeds
int ms_client_request_result_free( struct ms_client_request_result* result ) {
   
   if( result->ent != NULL ) {
      md_entry_free( result->ent );
      SG_safe_free( result->ent );
   }
   
   memset( result, 0, sizeof(struct ms_client_request_result) );
   
   return 0;
}

// free a list of results, as well as the list itself
// always succeeds
int ms_client_request_result_free_all( struct ms_client_request_result* results, size_t num_results ) {
   
   for( unsigned int i = 0; i < num_results; i++ ) {
      ms_client_request_result_free( &results[i] );
   }
   SG_safe_free( results );
   
   return 0;
}

// perform a single operation on the MS, synchronously, given the single update to send
// return 0 on success, which means that we successfully got a response from the MS.  The response will be stored to result (which can encode an error from the MS, albeit successfully transferred).
// return negative on lower-level errors, like protocol, transport, marshalling problems.
// return -ENOMEM on OOM
static int ms_client_single_rpc_lowlevel( struct ms_client* client, struct md_update* up, struct ms_client_request_result* result ) {
   
   int rc = 0;
   ms_client_update_set updates;
   ms::ms_updates ms_updates;
   CURL* curl = NULL;
   struct curl_httppost *post = NULL, *last = NULL;
   struct ms_client_timing timing;
   char* url = NULL;
   char* serialized_text = NULL;
   size_t serialized_text_len = 0;
   char* buf = NULL;
   off_t buflen = 0;
   ms::ms_reply reply;
   struct md_entry* ent = NULL;
   
   memset( &timing, 0, sizeof(struct ms_client_timing) );
   
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   // generate our update
   ms_client_add_update( &updates, up );
   
   rc = ms_client_request_serialize( client, &updates, &serialized_text, &serialized_text_len, &post, &last );
   if( rc != 0 ) {
      
      SG_error("ms_client_request_serialize rc = %d\n", rc );
      return rc;
   }
   
   // connect (TODO: connection pool)
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( serialized_text );
      curl_formfree( post );
      return -ENOMEM;
   }
   
   url = ms_client_file_url( client->url, volume_id );
   if( url == NULL ) {
      
      curl_easy_cleanup( curl );
      SG_safe_free( serialized_text );
      curl_formfree( post );
      return -ENOMEM;
   }
                             
   ms_client_init_curl_handle( client, curl, url );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L);
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, post );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, &timing );
   
   // run 
   rc = md_download_run( curl, MS_MAX_MSG_SIZE, &buf, &buflen );
   
   curl_easy_cleanup( curl );
   curl_formfree( post );
   SG_safe_free( serialized_text );
   SG_safe_free( url );
   
   if( rc != 0 ) {
      SG_error("md_download_run rc = %d\n", rc );
      
      ms_client_timing_free( &timing );
      return rc;
   }
   
   // parse and verify
   rc = ms_client_parse_reply( client, &reply, buf, buflen );
   
   SG_safe_free( buf );
   
   if( rc != 0 ) {
      SG_error("ms_client_parse_reply rc = %d\n", rc );
      
      ms_client_timing_free( &timing );
      return rc;
   }
   
   result->reply_error = reply.error();
   
   // ensure we got meaningful data
   if( result->reply_error != 0 ) {
      SG_error("MS RPC error code %d\n", result->reply_error );
      
      ms_client_timing_free( &timing );
      return 0;
   }
   
   if( reply.errors_size() != 1 ) {
      SG_error("MS replied %d error codes (expected 1)\n", reply.errors_size() );
      
      ms_client_timing_free( &timing );
      return -EBADMSG;
   }
   
   if( reply.errors(0) != 0 ) {
      SG_error("MS operation %d error %d\n", up->op, reply.errors(0) );
   }
   
   else if( MS_CLIENT_OP_RETURNS_ENTRY( up->op ) ) {
      
      if( !reply.has_listing() ) {
         SG_error("%s", "MS replied 0 entries (expected 1)\n" );
         
         ms_client_timing_free( &timing );
         return -EBADMSG;
      }
      
      if( reply.listing().entries_size() != 1 ) {
         SG_error("MS replied %d entries (expected 1)\n", reply.listing().entries_size() );
         
         ms_client_timing_free( &timing );
         return -EBADMSG;
      }
      
      // get the entry 
      ent = SG_CALLOC( struct md_entry, 1 );
      if( ent == NULL ) {
         
         ms_client_timing_free( &timing );
         return -ENOMEM;
      }
      
      rc = ms_entry_to_md_entry( reply.listing().entries(0), ent );
      if( rc != 0 ) {
         
         ms_client_timing_free( &timing );
         return rc;
      }
   }
   
   result->rc = reply.errors(0);
   result->ent = ent;
   
   ms_client_timing_log( &timing );
   ms_client_timing_free( &timing );
   
   return 0;
}

// perform a single RPC, using the request/request_result structures
// return the result of the RPC 
// return 0 on success 
// return negative on error
int ms_client_single_rpc( struct ms_client* client, struct ms_client_request* request, struct ms_client_request_result* result ) {
   
   struct md_update up;
   memset( &up, 0, sizeof(struct md_update) );
   
   ms_client_request_to_update( request, &up );
   
   return ms_client_single_rpc_lowlevel( client, &up, result );
}


// perform one update rpc with the MS, synchronously.  Don't bother with the result--just return an error, if one occurred
// return 0 on success
// return negative negative on error (be it protocol, formatting, or RPC error)
int ms_client_update_rpc( struct ms_client* client, struct md_update* up ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   rc = ms_client_single_rpc_lowlevel( client, up, &result );
   
   if( rc != 0 ) {
      ms_client_request_result_free( &result );
      return rc;
   }
   
   if( result.reply_error != 0 ) {
      rc = result.reply_error;
   }
   
   else if( result.rc != 0 ) {
      rc = result.rc;
   }
   
   ms_client_request_result_free( &result );
   return rc;
}


// create a single file or directory record on the MS, synchronously
// assert that the entry's type (MD_ENTRY_FILE or MD_ENTRY_DIRECTORY) matches a given type (return -EINVAL if it doesn't)
// NOTE: ent will be modified internally, so don't call this method or access this ent while this method is running
// return 0 on success
// return negative on error
static int ms_client_create_or_mkdir( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, int type, struct md_entry* ent ) {
   
   // sanity check 
   if( ent->type != type ) {
      SG_error("Entry '%s' has type %d; expected type %d\n", ent->name, ent->type, type );
      return EINVAL;
   }
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // remember the old file ID
   uint64_t old_file_id = ent->file_id;
   uint64_t new_file_id = ms_client_make_file_id();
   
   // request a particular file ID
   ent->file_id = new_file_id;
   
   SG_debug("desired file_id: %" PRIX64 "\n", ent->file_id );
   
   // populate the request 
   req.ent = ent;
   req.op = ms::ms_update::CREATE;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, &req, &result );
   
   // restore 
   ent->file_id = old_file_id;
   
   if( rc != 0 ) {
      
      SG_error("ms_client_single_rpc(CREATE) rc = %d\n", rc );
   }
   else {
      
      // get data
      if( result.reply_error != 0 ) {
         
         SG_error("MS reply error %d\n", result.reply_error );
         rc = result.reply_error;
      }
      else if( result.rc != 0 ) {
         
         SG_error("MS file_create rc = %d\n", result.rc );
         rc = result.rc;
      }
      
      else {
         
         // got data!  make sure it matches (the MS should have went with our file ID)
         if( new_file_id != result.ent->file_id ) {
            
            SG_error("MS returned invalid data: expected file ID %" PRIX64 ", but got %" PRIX64 "\n", new_file_id, result.ent->file_id );
            rc = -EBADMSG;
         }
         
         else {
            
            *file_id_ret = result.ent->file_id;
            *write_nonce_ret = result.ent->write_nonce;
         }
      }
   }
   
   ms_client_request_result_free( &result );
   
   return rc;
}


// create a single file on the MS, synchronously.
// unlike ms_client_create_or_mkdir, this only works for files
// return 0 on success
// return negative on error
int ms_client_create( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, file_id_ret, write_nonce_ret, MD_ENTRY_FILE, ent );
}

// create a single directory on the MS, synchronously 
// unlike ms_client_create_or_mkdir, this only works for diretories 
// return 0 on success 
// return negative on error
int ms_client_mkdir( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, file_id_ret, write_nonce_ret, MD_ENTRY_DIR, ent );
}


// delete a record from the MS, synchronously
// return 0 on success 
// return negative on error
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // populate the request 
   req.ent = ent;
   req.op = ms::ms_update::DELETE;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, &req, &result );
   if( rc != 0 ) {
      
      SG_error("ms_client_single_rpc(DELETE) rc = %d\n", rc );
   }
   else {
      if( result.reply_error != 0 ) {
         
         SG_error("MS reply error %d\n", result.reply_error );
         rc = result.reply_error;
      }
      else if( result.rc != 0 ) {
         
         SG_error("MS file_delete rc = %d\n", result.rc );
         rc = result.rc;
      }
   }
   
   ms_client_request_result_free( &result );
   
   return rc;
}


// update a record on the MS, synchronously.
// if in_affected_blocks is not NULL, send the affected blocks to the MS as part of the update (i.e. so we can vacuum them later)
// in_affected_blocks will be referenced, but not duplicated, so don't free it.
// return 0 on success 
// return negative on error
int ms_client_update_write( struct ms_client* client, int64_t* write_nonce, struct md_entry* ent, uint64_t* in_affected_blocks, size_t num_affected_blocks ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // populate the request 
   req.ent = ent;
   req.op = ms::ms_update::UPDATE;
   req.affected_blocks = in_affected_blocks;
   req.num_affected_blocks = num_affected_blocks;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, &req, &result );
   if( rc != 0 ) {
      
      SG_error("ms_client_single_rpc(UPDATE) rc = %d\n", rc );
   }
   else {
      if( result.reply_error != 0 ) {
         
         SG_error("MS reply error %d\n", result.reply_error );
         rc = result.reply_error;
      }
      else if( result.rc != 0 ) {
         
         SG_error("MS file_update rc = %d\n", result.rc );
         rc = result.rc;
      }
      else {
         *write_nonce = result.ent->write_nonce;
      }
   }
   
   ms_client_request_result_free( &result );
   
   return 0;
}


// update a record on the MS, synchronously, NOT due to a write()
// return 0 on success 
// return negative on error
int ms_client_update( struct ms_client* client, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_update_write( client, write_nonce_ret, ent, NULL, 0 );
}

// change coordinator ownership of a file on the MS, synchronously
// return 0 on success, and give back the write nonce and new coordinator ID of the file
// return negative on error
int ms_client_coordinate( struct ms_client* client, uint64_t* new_coordinator, int64_t* write_nonce, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // populate the request 
   req.ent = ent;
   req.op = ms::ms_update::CHCOORD;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, &req, &result );
   if( rc != 0 ) {
      
      SG_error("ms_client_single_rpc(CHCOORD %" PRIX64 ") rc = %d\n", ent->file_id, rc );
   }
   else {
      if( result.reply_error != 0 ) {
         
         SG_error("MS reply error %d\n", result.reply_error );
         rc = result.reply_error;
      }
      else if( result.rc != 0 ) {
         
         SG_error("MS chcoord(%" PRIX64 ") rc = %d\n", ent->file_id, result.rc );
         rc = result.rc;
      }
      else {
         
         // got data too!
         *write_nonce = result.ent->write_nonce;
         *new_coordinator = result.ent->coordinator;
      }
   }
   
   ms_client_request_result_free( &result );
   
   return rc;
}

// rename from src to dest, synchronously
// return 0 on success
// return -EXDEV if the volumes do not agree between src and dest 
// return -EINVAL if dest is NULL
// return negative on error 
int ms_client_rename( struct ms_client* client, int64_t* write_nonce, struct md_entry* src, struct md_entry* dest ) {
   
   // sanity check
   if( src->volume != dest->volume ) {
      return -EXDEV;
   }
   
   // sanity check
   if( dest == NULL ) {
      return -EINVAL;
   }
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // populate the request 
   req.ent = src;
   req.dest = dest;
   req.op = ms::ms_update::RENAME;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, &req, &result );
   if( rc != 0 ) {
      
      SG_error("ms_client_single_rpc(RENAME) rc = %d\n", rc );
   }
   else {
      if( result.reply_error != 0 ) {
         
         SG_error("ERR: MS reply error %d\n", result.reply_error );
         rc = result.reply_error;
      }
      else if( result.rc != 0 ) {
         
         SG_error("ERR: MS file_rename rc = %d\n", result.rc );
         rc = result.rc;
      }
      else {
         
         // got data too!
         *write_nonce = result.ent->write_nonce;
         
         SG_debug("New write_nonce of %" PRIX64 " is %" PRId64 "\n", src->file_id, *write_nonce );
      }
   }
   
   ms_client_request_result_free( &result );
   
   return rc;
}

// parse an MS reply
// NOTE: the MS client cannot be config-locked
// return 0 on success
// return negative on error
int ms_client_parse_reply( struct ms_client* client, ms::ms_reply* reply, char const* buf, size_t buf_len ) {

   int rc = md_parse< ms::ms_reply >( reply, buf, buf_len );
   if( rc != 0 ) {
      
      SG_error("md_parse ms_reply failed, rc = %d\n", rc );
      
      return rc;
   }
   
   ms_client_config_rlock( client );

   // verify integrity and authenticity
   rc = md_verify< ms::ms_reply >( client->volume->volume_public_key, reply );
   if( rc != 0 ) {
      
      ms_client_config_unlock( client );
      
      SG_error("md_verify ms_reply failed, rc = %d\n", rc );
      
      return rc;
   }
   
   ms_client_config_unlock( client );
   
   return 0;
}
