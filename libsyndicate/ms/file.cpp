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
#include "libsyndicate/ms/vacuum.h"

// convert a list of requests into a protobuf
// return 0 on success
// return -EINVAL if the an update in updates is invalid
// return -ENOMEM if we're out of memory
static int ms_client_requests_protobuf( ms_client_request_list* requests, ms::ms_request_multi* ms_requests ) {
   
   try {
      // populate the protobuf
      for( ms_client_request_list::iterator itr = requests->begin(); itr != requests->end(); itr++ ) {

         struct ms_client_request* request = *itr;
         
         // verify that we have a valid update type...
         if( request->op <= 0 || request->op >= ms::ms_request::NUM_UPDATE_TYPES ) {
            
            SG_error("Invalid update type %d\n", request->op );
            return -EINVAL;
         }
         
         ms::ms_request* ms_req = ms_requests->add_requests();
         ms::ms_entry* ms_ent = ms_req->mutable_entry();

         ms_req->set_type( request->op );
         md_entry_to_ms_entry( ms_ent, request->ent );
         
         // if this an UPDATE or a VACUUMAPPEND, then add the affected blocks 
         if( request->op == ms::ms_request::UPDATE || request->op == ms::ms_request::VACUUMAPPEND ) {
            
            // VACUUMAPPEND requires blocks 
            if( request->op == ms::ms_request::VACUUMAPPEND && request->affected_blocks == NULL ) {
               
               SG_error("%s", "VACUUMAPPEND requires block IDs\n");
               return -EINVAL;
            }
            
            // fill in blocks and signature
            if( request->affected_blocks != NULL ) {
               
               if( request->vacuum_signature == NULL ) {
                  
                  SG_error("%s", "Missing vacuum signature\n");
                  return -EINVAL;
               }
               
               for( size_t i = 0; i < request->num_affected_blocks; i++ ) {
                  ms_req->add_affected_blocks( request->affected_blocks[i] );
               }
               
               ms_req->set_vacuum_signature( string((char*)request->vacuum_signature, request->vacuum_signature_len) );
            }
         }
         
         // if this is a RENAME, then add the 'dest' argument
         else if( request->op == ms::ms_request::RENAME ) {
            ms::ms_entry* dest_ent = ms_req->mutable_dest();
            md_entry_to_ms_entry( dest_ent, request->dest );
         }
         
         // if this is a SETXATTR, then set the attr name and attr value
         else if( request->op == ms::ms_request::PUTXATTR ) {
            
            // sanity check...
            if( request->xattr_name == NULL || request->xattr_value == NULL ) {
               return -EINVAL;
            }
            
            // set name, value, signature
            ms_req->set_xattr_name( string(request->xattr_name) );
            ms_req->set_xattr_value( string(request->xattr_value, request->xattr_value_len) );
         }
         
         // if this is a REMOVEXATTR, then set the attr name
         else if( request->op == ms::ms_request::REMOVEXATTR ) {
            // sanity check ...
            if( request->xattr_name == NULL ) {
               return -EINVAL;
            }
            
            ms_req->set_xattr_name( string(request->xattr_name) );
         }
      }

      ms_requests->set_signature( string("") );
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
ssize_t ms_client_update_set_to_string( ms::ms_request_multi* ms_requests, char** update_text ) {
   
   string update_bits;
   bool valid;

   try {
      valid = ms_requests->SerializeToString( &update_bits );
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


// sign a sequence of requests, to show that they come from the same origin
// return 0 on success
// return -EINVAL if pkey or ms_requests is NULL
// return -EINVAL if we can't sign 
// return -ENOMEM if OOM
static int ms_client_sign_requests( EVP_PKEY* pkey, ms::ms_request_multi* ms_requests ) {
   if( pkey == NULL || ms_requests == NULL ) {
      return -EINVAL;
   }
   return md_sign<ms::ms_request_multi>( pkey, ms_requests );
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

// generate data to upload and HTTP forms wrapping it.
// return 0 on success, and set *serialized_text and *serialized_text_len to the serialized buffer holding all of the updates, and set *ret_post and *ret_last to a CURL form structure 
//   that maps the 'ms-metadata-updates' field to the serialized data.
// return positive error if we failed to generate forms (see curl error codes)
// return negative on error (result of either ms_client_update_set_serialize, ms_client_sign_requests, ms_client_update_set_to_string)
static int ms_client_requests_serialize( struct ms_client* client, ms_client_request_list* requests, char** serialized_text, size_t* serialized_text_len, struct curl_httppost** ret_post, struct curl_httppost** ret_last ) {
   
   int rc = 0;
   
   // pack the updates into a protobuf
   ms::ms_request_multi ms_requests;
   rc = ms_client_requests_protobuf( requests, &ms_requests );
   if( rc != 0 ) {
      
      SG_error("ms_client_update_set_serialize rc = %d\n", rc );
      return rc;
   }

   // sign it
   rc = ms_client_sign_requests( client->gateway_key, &ms_requests );
   if( rc != 0 ) {
      
      SG_error("ms_client_sign_requests rc = %d\n", rc );
      return rc;
   }

   // make it a string
   char* update_text = NULL;
   ssize_t update_text_len = ms_client_update_set_to_string( &ms_requests, &update_text );

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


// initialize a create request.
// NOTE: this shallow-copies the data; do not free ent
// always succeeds
int ms_client_create_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::CREATE;
   
   return 0;
}

// initialize a create-async request.
// NOTE: this shallow-copies the data; do not free ent
// always succeeds
int ms_client_create_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::CREATE_ASYNC;
   
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
   return ms_client_update_write_request( client, ent, NULL, 0, NULL, 0, request );
}

// initialize an update-async request (but not one for writes)
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_update_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::UPDATE_ASYNC;
   request->affected_blocks = NULL;
   request->num_affected_blocks = 0;
   
   return 0;
}

// initialize an update request for a write 
// NOTE: this shallow-copies the data; do not free ent, affected_blocks
// always succeeds
int ms_client_update_write_request( struct ms_client* client, struct md_entry* ent, uint64_t* affected_blocks, size_t num_affected_blocks,
                                    unsigned char* vacuum_signature, size_t vacuum_signature_len, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::UPDATE;
   request->affected_blocks = affected_blocks;
   request->num_affected_blocks = num_affected_blocks;
   request->vacuum_signature = vacuum_signature;
   request->vacuum_signature_len = vacuum_signature_len;
   
   return 0;
}


// initialize a coordinate request
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_coordinate_request( struct ms_client* client, struct md_entry* ent, unsigned char* xattr_hash, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::CHCOORD;
   request->xattr_hash = xattr_hash;
   
   return 0;
}


// initialize a rename request 
// NOTE: this shallow-copies the data; do not free src or dest 
// always succeeds
int ms_client_rename_request( struct ms_client* client, struct md_entry* src, struct md_entry* dest, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = src;
   request->dest = dest;
   request->op = ms::ms_request::RENAME;
   
   return 0;
}

// initialize a delete request 
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_delete_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::DELETE;
   
   return 0;
}

// initialize a delete-async request
// NOTE: this shallow-copies the data; do not free ent 
// always succeeds
int ms_client_delete_async_request( struct ms_client* client, struct md_entry* ent, struct ms_client_request* request ) {
   
   memset( request, 0, sizeof(struct ms_client_request) );
   
   request->ent = ent;
   request->op = ms::ms_request::DELETE_ASYNC;
   
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
// return -EBADMSG if the reply was improperly structured, or contained an entry whose authenticity could not be verified
// return negative on lower-level errors, like protocol, transport, marshalling problems. (TODO: better documentation)
// return -ENOMEM on OOM
int ms_client_single_rpc( struct ms_client* client, struct ms_client_request* request, struct ms_client_request_result* result ) {
   
   int rc = 0;
   ms_client_request_list requests;
   CURL* curl = NULL;
   struct curl_httppost *post = NULL, *last = NULL;
   struct ms_client_timing timing;
   char* url = NULL;
   char* auth_header = NULL;
   char* serialized_text = NULL;
   size_t serialized_text_len = 0;
   char* buf = NULL;
   off_t buflen = 0;
   ms::ms_reply reply;
   struct md_entry* ent = NULL;
   
   memset( &timing, 0, sizeof(struct ms_client_timing) );
   
   uint64_t volume_id = ms_client_get_volume_id( client );
   
   // generate our update
   requests.push_back( request );
   
   rc = ms_client_requests_serialize( client, &requests, &serialized_text, &serialized_text_len, &post, &last );
   if( rc != 0 ) {
      
      SG_error("ms_client_requests_serialize rc = %d\n", rc );
      return rc;
   }
   
   // connect (TODO: connection pool)
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      SG_safe_free( serialized_text );
      curl_formfree( post );
      return -ENOMEM;
   }
   
   url = ms_client_file_url( client->url, volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ) );
   if( url == NULL ) {
      
      curl_easy_cleanup( curl );
      SG_safe_free( serialized_text );
      curl_formfree( post );
      return -ENOMEM;
   }
   
   // generate auth header
   rc = ms_client_auth_header( client, url, &auth_header );
   if( rc != 0 ) {
      
      // failed!
      curl_easy_cleanup( curl );
      SG_safe_free( serialized_text );
      curl_formfree( post );
      SG_safe_free( url );
      return -ENOMEM;
   }
                             
   ms_client_init_curl_handle( client, curl, url, auth_header );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL );    // force POST on redirect
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, post );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_timing_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, &timing );
  
   // run 
   rc = md_download_run( curl, MS_MAX_MSG_SIZE, &buf, &buflen );
   
   curl_easy_cleanup( curl );
   curl_formfree( post );
   SG_safe_free( serialized_text );
   SG_safe_free( url );
   SG_safe_free( auth_header );
   
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
      SG_error("MS operation %d error %d\n", request->op, reply.errors(0) );
   }
   
   else if( MS_CLIENT_OP_RETURNS_ENTRY( request->op ) ) {
      
      if( !reply.has_listing() ) {
         SG_error("%s", "MS replied 0 entries (expected 1)\n" );
         
         ms_client_timing_free( &timing );
         return -EBADMSG;
      }
      
      if( reply.listing().entries_size() != 1 && request->op != ms::ms_request::RENAME ) {
         SG_error("MS replied %d entries (expected 1)\n", reply.listing().entries_size() );
         
         ms_client_timing_free( &timing );
         return -EBADMSG;
      }
      
      if( reply.listing().entries_size() == 1 ) {

         // verify authenticity
         ms::ms_entry* msent = reply.mutable_listing()->mutable_entries(0);
         rc = ms_entry_verify( client, msent );
         if( rc != 0 ) {
            SG_error("Invalid entry %" PRIX64 "\n", reply.listing().entries(0).file_id() );
               
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
   }
   
   result->rc = reply.errors(0);
   result->ent = ent;
   
   ms_client_timing_log( &timing );
   ms_client_timing_free( &timing );
   
   return 0;
}


// set the initial fields in an md_entry 
// always succeeds 
void ms_client_create_initial_fields( struct md_entry* ent ) {
   ent->version = 1;
   ent->write_nonce = 1;        // initial version; will be regenerated by the MS as needed
   ent->generation = 1;         // initial version 
   ent->num_children = 0;       // initial version 
   ent->capacity = 16;          // initial version
   ent->xattr_nonce = 0;        // initial version
}

// create a single file or directory record on the MS, synchronously
// Sign the entry if we haven't already.
// NOTE: ent will be modified internally, so don't call this method or access this ent while this method is running
// return 0 on success
// return negative on error
// TODO: better documentation
static int ms_client_create_or_mkdir( struct ms_client* client, struct md_entry* ent_out, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   
   int64_t write_nonce = ent->write_nonce;
   int64_t xattr_nonce = ent->xattr_nonce;
   int64_t version = ent->version;
   int64_t generation = ent->generation;
   int64_t num_children = ent->num_children;
   int64_t capacity = ent->capacity;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // remember the old file ID
   uint64_t old_file_id = ent->file_id;
   uint64_t new_file_id = ms_client_make_file_id();
   
   // request a particular file ID
   ent->file_id = new_file_id;
   ms_client_create_initial_fields( ent );
   
   SG_debug("desired file_id: %" PRIX64 "\n", ent->file_id );
   
   // sign the request
   rc = md_entry_sign( client->gateway_key, ent, &sig, &sig_len );
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   ent->ent_sig = sig;
   ent->ent_sig_len = sig_len;

   // populate the request 
   ms_client_create_request( client, ent, &req );
   
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
            
            rc = md_entry_dup2( &result.ent[0], ent_out );
         }
      }
   }
   
   // restore
   ent->ent_sig = NULL;
   ent->ent_sig_len = 0;
   ent->write_nonce = write_nonce;
   ent->version = version;
   ent->generation = generation;
   ent->num_children = num_children;
   ent->capacity = capacity;
   ent->xattr_nonce = xattr_nonce;
   
   SG_safe_free( sig );
   
   ms_client_request_result_free( &result );
   
   return rc;
}


// create a single file on the MS, synchronously.
// unlike ms_client_create_or_mkdir, this only works for files.
// Sign the entry if we haven't already.
// return 0 on success
// return negative on error
int ms_client_create( struct ms_client* client, struct md_entry* ent_out, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, ent_out, ent );
}

// create a single directory on the MS, synchronously 
// unlike ms_client_create_or_mkdir, this only works for diretories 
// Sign the entry if we haven't already.
// return 0 on success 
// return negative on error
int ms_client_mkdir( struct ms_client* client, struct md_entry* ent_out, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, ent_out, ent );
}


// delete a record from the MS, synchronously
// Sign the entry if we haven't already.
// Only ent's coordinator should call this.
// return 0 on success 
// return negative on error
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // sign the request 
   rc = md_entry_sign( client->gateway_key, ent, &sig, &sig_len );
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   ent->ent_sig = sig;
   ent->ent_sig_len = sig_len;
   
   // populate the request 
   ms_client_delete_request( client, ent, &req );
   
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
   
   ent->ent_sig = NULL;
   ent->ent_sig_len = 0;
   
   SG_safe_free( sig );
   
   ms_client_request_result_free( &result );
   
   return rc;
}


// update a record on the MS, synchronously.
// Sign the entry if we haven't already.
// only ent's coordinator should call this.
// return 0 on success 
// return negative on error
int ms_client_update( struct ms_client* client, struct md_entry* ent_out, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   
   int64_t write_nonce = ent->write_nonce;
      
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   if( ent->type == MD_ENTRY_DIR ) {
       // for directories, choose a random nonce 
       ent->write_nonce = md_random64();
   }
   else {
       
       // for files, writes only come from the coordinator, so we can sequentially increment.
       ent->write_nonce = write_nonce + 1;
   }
   
   // sign the request 
   rc = md_entry_sign( client->gateway_key, ent, &sig, &sig_len );
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   ent->ent_sig = sig;
   ent->ent_sig_len = sig_len;

   // build the request
   ms_client_update_request( client, ent, &req );
   
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
      else if( result.ent == NULL ) {
          
         SG_error("BUG: No entry given (%p)\n", result.ent );
         rc = -ENODATA;
      }
      else {
         
         rc = md_entry_dup2( result.ent, ent_out );
         if( rc == 0 ) {
            
            // advance write nonce 
            if( ent->type == MD_ENTRY_DIR ) {
                
                // MS controls directory consistency information 
                ent_out->write_nonce = result.ent->write_nonce;
            }
            else {
                
                // we're only calling this because we're the coordinator.
                ent_out->write_nonce = write_nonce + 1;
            }
         }
      }
   }
   
   ent->ent_sig = NULL;
   ent->ent_sig_len = 0;
   
   SG_safe_free( sig );
   
   ms_client_request_result_free( &result );
   
   return 0;
}


// change coordinator ownership of a file on the MS, synchronously
// Sign the entry if we haven't already.
// Populate *ent_out with the data on the MS.  The caller must free it.
// return 0 on success, and give back the write nonce and new coordinator ID of the file
// return -EINVAL if the xattr hash is missing from ent
// return negative on error
int ms_client_coordinate( struct ms_client* client, struct md_entry* ent_out, struct md_entry* ent, unsigned char* xattr_hash ) {
   
   int rc = 0;
   struct ms_client_request_result result;
   struct ms_client_request req;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   
   int64_t version = ent->version;
   int64_t write_nonce = ent->write_nonce;
   
   unsigned char* old_xattr_hash = NULL;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // signature must cover xattr hash...
   old_xattr_hash = ent->xattr_hash;
   ent->xattr_hash = xattr_hash;
   ent->version = version + 1;
   ent->write_nonce = write_nonce + 1;
   
   // sign the request 
   rc = md_entry_sign( client->gateway_key, ent, &sig, &sig_len );
   
   ent->xattr_hash = old_xattr_hash;
   
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   ent->ent_sig = sig;
   ent->ent_sig_len = sig_len;
   
   // populate...
   ms_client_coordinate_request( client, ent, xattr_hash, &req );
   
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
         
         // if we changed coordinators, then the version and write nonces will advance 
         if( result.ent->coordinator == client->gateway_id ) {
            
            rc = md_entry_dup2( ent, ent_out );
            if( rc == 0 ) {
                
                ent_out->coordinator = client->gateway_id;
                
                // advance locally
                ent_out->write_nonce = write_nonce + 1;
                ent_out->version = version + 1;
            }
         }
         else {
             
            // need to try again, with higher versions (i.e. the caller should refresh)
            rc = md_entry_dup2( &result.ent[0], ent_out );
         }
      }
   }
   
   ent->ent_sig = NULL;
   ent->ent_sig_len = 0;
   ent->version = version;
   ent->write_nonce = write_nonce;
   
   SG_safe_free( sig );
   
   ms_client_request_result_free( &result );
   
   return rc;
}

// rename from src to dest, synchronously
// Sign the src entry if we haven't already.
// return 0 on success, and populate *old_dest_out with the replaced file/directory metadata
// return -EXDEV if the volumes do not agree between src and dest 
// return -EINVAL if dest is NULL
// return negative on error 
int ms_client_rename( struct ms_client* client, struct md_entry* old_dest_out, struct md_entry* src, struct md_entry* dest ) {
   
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
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   
   int64_t write_nonce = src->write_nonce;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   src->write_nonce = write_nonce + 1;
   
   // sign the request 
   rc = md_entry_sign( client->gateway_key, src, &sig, &sig_len );
   if( rc != 0 ) {
      return -ENOMEM;
   }
   
   src->ent_sig = sig;
   src->ent_sig_len = sig_len;

   sig = NULL;
   sig_len = 0;

   // sign dest as well 
   rc = md_entry_sign( client->gateway_key, dest, &sig, &sig_len );
   if( rc != 0 ) {
      SG_safe_free( src->ent_sig );
      src->ent_sig_len = 0;
      return -ENOMEM;
   }

   dest->ent_sig = sig;
   dest->ent_sig_len = sig_len;

   sig = NULL;
   sig_len = 0;
   
   // populate the request 
   ms_client_rename_request( client, src, dest, &req );
   
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
        
         // got old dest, if we got one 
         if( result.ent != NULL ) { 
             rc = md_entry_dup2( result.ent, old_dest_out );
         }
         else {
            memset( old_dest_out, 0, sizeof(struct md_entry));
         }
      }
   }

   SG_safe_free( src->ent_sig );
   SG_safe_free( dest->ent_sig );
   
   src->ent_sig = NULL;
   src->ent_sig_len = 0;
   dest->ent_sig = NULL;
   dest->ent_sig_len = 0;

   src->write_nonce = write_nonce;
   
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
   rc = md_verify< ms::ms_reply >( client->syndicate_pubkey, reply );
   if( rc != 0 ) {
      
      ms_client_config_unlock( client );
      
      SG_error("md_verify ms_reply failed, rc = %d\n", rc );
      
      return rc;
   }
   
   ms_client_config_unlock( client );
   
   return 0;
}
