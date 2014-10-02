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


// begin posting file data
// return 0 on success, negative on error
static int ms_client_send_begin( struct ms_client* client, char const* url, char const* data, size_t len, struct ms_client_network_context* nctx ) {
   
   struct curl_httppost *post = NULL, *last = NULL;
   int rc = 0;
   
   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-metadata-updates", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, data, CURLFORM_BUFFERLENGTH, len, CURLFORM_END );

   // initialize the context 
   ms_client_network_context_upload_init( nctx, url, post );
   
   // start the upload
   rc = ms_client_network_context_begin( client, nctx );
   if( rc != 0 ) {
      errorf("ms_client_upload_begin(%s) rc = %d\n", url, rc );
      
      ms_client_network_context_free( nctx );
      return rc;
   }
   
   return rc;
}



// finish posting file data 
// return 0 on success, negative on parse error, positive HTTP response on HTTP error
// NOTE: does not check the error value in reply--a return of 0 only indicates that we got a reply structure back.
static int ms_client_send_end( struct ms_client* client, ms::ms_reply* reply, bool verify, struct ms_client_network_context* nctx ) {
                               
   int rc = 0;
   int http_response = 0;
   char* buf = NULL;
   size_t buflen = 0;
   
   // wait for it...
   http_response = ms_client_network_context_end( client, nctx, &buf, &buflen );
   
   if( http_response != 200 ) {
      errorf("ms_client_upload_end rc = %d\n", http_response );
      
      ms_client_network_context_free( nctx );
      return http_response;
   }
   
   
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
      }
      else {
         // no response...
         rc = -ENODATA;
      }
   }
   
   if( buf != NULL ) {
      free( buf );
   }
   
   // record benchmark information
   ms_client_timing_log( nctx->timing );
   
   ms_client_network_context_free( nctx );
   
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

// random 64-bit number
uint64_t ms_client_make_file_id() {
   return (uint64_t)md_random64();
}


// extract file metadata from reply, and perform sanity checks.
// * verify that the reply indcates that the expected number of requests were processed
// * verify that the reply contains the expected number of entries in its entry listing.
// * verify that the entries list does not contain duplicates.
// return 0 on success, -EBADMSG on invalid reply structure, -EINVAL on invalid input, -ENODATA we couldn't populate results with anything but an error message from the MS.
// This method always sets results->reply_error to the reply's error code on error.
static int ms_client_get_partial_results( ms::ms_reply* reply, struct ms_client_multi_result* results, int num_expected_processed, int num_expected_ents ) {
   
   int num_items_processed = 0;
   int error = 0;
   int rc = 0;
   struct md_entry* ents = NULL;
   size_t num_ents = 0;
   
   
   if( results == NULL ) {
      return -EINVAL;
   }
   
   // sanity check 
   if( !reply->has_num_processed() ) {
      errorf("%s", "MS reply is missing num_processed\n");
      return -EBADMSG;
   }
   
   num_items_processed = reply->num_processed();
   
   // sanity check--did anything get through at all?
   if( num_items_processed == 0 ) {
      results->reply_error = reply->error();
      return -ENODATA;
   }
   
   // sanity check--was everything processed?
   if( num_items_processed != num_expected_processed ) {
      errorf("Requested %d items, but %d were processed\n", num_expected_processed, num_items_processed );
      
      results->reply_error = reply->error();
      return -ENODATA;
   }
   
   // sanity check--do we have the expected number of entries in the listing?
   if( num_expected_ents > 0 ) {
      
      if( !reply->has_listing() ) {
         errorf("Expected %d entries, but no listing given\n", num_expected_ents);
         
         results->reply_error = reply->error();
         return -EBADMSG;
      }
      
      if( reply->listing().entries_size() != num_expected_ents ) {
         errorf("Expected %d entries, but listing contains %d\n", num_expected_ents, reply->listing().entries_size() );
         
         results->reply_error = reply->error();
         return -ENODATA;
      }
   }
   
   error = reply->error();
   results->reply_error = error;
   
   if( num_expected_ents > 0 ) {
      // get the entries.
      
      // First, sanity check: verify no duplicate names or IDs
      set<string> names;
      set<uint64_t> ids;
      
      for( int i = 0; i < num_expected_ents; i++ ) {
         
         string name = reply->listing().entries(i).name();
         uint64_t id = reply->listing().entries(i).file_id();
         
         if( names.count(string(name)) != 0 || ids.count(id) ) {
            errorf("ERR: Duplicate entry '%s' (%" PRIX64 ")\n", name.c_str(), id );
            rc = -EBADMSG;
         }
         
         names.insert( name );
         ids.insert( id );
      }
      
      if( rc != 0 ) {
         // invalid message
         return rc;
      }
      
      // store entries
      num_ents = reply->listing().entries_size();
      ents = CALLOC_LIST( struct md_entry, num_ents );
   
      for( int i = 0; i < num_expected_ents; i++ ) {
         
         ms_entry_to_md_entry( reply->listing().entries(i), &ents[i] );   
         dbprintf("%s: output file_id: %" PRIX64 ", write_nonce: %" PRId64 ", coordinator_id: %" PRIu64 "\n", ents[i].name, ents[i].file_id, ents[i].write_nonce, ents[i].coordinator );
      }
   }
   
   // fill in the rest of the structure
   results->num_processed = num_items_processed;
   results->ents = ents;
   results->num_ents = num_ents;
   
   return 0;
}


// free a request and all of its data (assumed to be dynamically allocated)
int ms_client_request_free( struct ms_client_request* req ) {
   
   if( req->ent != NULL ) {
      
      md_entry_free( req->ent );
      free( req->ent );
      req->ent = NULL;
   }
   
   if( req->dest != NULL ) {
   
      md_entry_free( req->ent );
      free( req->ent );
      req->ent = NULL;
   }
   
   if( req->affected_blocks != NULL ) {
      
      free( req->affected_blocks );
      req->affected_blocks = NULL;
   }
   
   memset( req, 0, sizeof(struct ms_client_request) );
   
   return 0;
}


// free a multi-result 
int ms_client_multi_result_free( struct ms_client_multi_result* result ) {
   
   if( result->ents != NULL ) {
      
      for( unsigned int i = 0; i < result->num_ents; i++ ) {
         md_entry_free( &result->ents[i] );
      }
      
      free( result->ents );
      
      result->ents = NULL;
      result->num_ents = 0;
   }
   
   return 0;
}


// merge two successful multi-results.
// NOTE: this will free the ents in src, and set its number of ents to 0
int ms_client_multi_result_merge( struct ms_client_multi_result* dest, struct ms_client_multi_result* src ) {
   
   if( src->num_ents > 0 && src->ents != NULL ) {
   
      size_t total_num_ents = dest->num_ents + src->num_ents;
      size_t total_size = sizeof(struct md_entry) * total_num_ents;
      
      size_t src_size = sizeof(struct md_entry) * src->num_ents;
   
      struct md_entry* new_ents = (struct md_entry*)realloc( dest->ents, total_size );
      
      if( new_ents == NULL ) {
         return -ENOMEM;
      }
      
      dbprintf("Copy over %zu bytes to %p (base: %p, original: %p) (total: %zu)\n", src_size, new_ents + dest->num_ents, new_ents, dest->ents, total_size );
      
      memcpy( new_ents + dest->num_ents, src->ents, src_size );
      
      dest->ents = new_ents;
      dest->num_ents = total_num_ents;
      
      // ensure this won't get used again
      free( src->ents );
      src->ents = NULL;
   }
   
   dest->num_processed += src->num_processed;
   
   // ensure we won't access src->ents again
   src->num_ents = 0;
   src->num_processed = 0;
   
   return 0;
}

// network context information for creating/updating/deleting entries 
struct ms_client_multi_cls {
   ms_client_update_set* updates;
   char* serialized_updates;
   int num_expected_replies;
};

// how many expected entries in a rely listing, given the operation?
int ms_client_num_expected_reply_ents( size_t num_reqs, int op ) {
   
   // list of all operations where we expect one reply for each request 
   static int expected_all_replies[] = {
      ms::ms_update::CREATE,
      ms::ms_update::UPDATE,
      ms::ms_update::CHCOORD,
      ms::ms_update::RENAME,
      -1
   };
   
   // for anything else, we expect zero replies 
   
   for( int i = 0; expected_all_replies[i] != -1; i++ ) {
      
      if( op == expected_all_replies[i] ) {
         return (int)num_reqs;
      }
   }
   
   // no reply expected
   return 0;
}

// start performing multiple instances of a single operation over a set of file and/or directory records on the MS 
// ms_op should be one of the ms::ms_update::* values
// ms_op_flags only really applies to ms::ms_update::SETXATTR
// return 0 on success, negative on failure
int ms_client_multi_begin( struct ms_client* client, int ms_op, int ms_op_flags, struct ms_client_request* reqs, size_t num_reqs, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   
   ms_client_update_set* updates = new ms_client_update_set();
   
   for( unsigned int i = 0; i < num_reqs; i++ ) {
      
      struct md_entry* ent = reqs[i].ent;
      
      
      // generate our update
      struct md_update up;
      memset( &up, 0, sizeof(struct md_update) );
      
      ms_client_populate_update( &up, ms_op, ms_op_flags, ent );
      
      // affected blocks? shallow-copy them over
      if( reqs[i].affected_blocks != NULL ) {
         
         up.affected_blocks = reqs[i].affected_blocks;
         up.num_affected_blocks = reqs[i].num_affected_blocks;
      }
      
      // destination?  i.e. due to a RENAME?
      // shallow-copy it over
      if( reqs[i].dest != NULL ) {
         memcpy( &up.dest, reqs[i].dest, sizeof(struct md_entry) );
      }
      
      ms_client_add_update( updates, &up );
   }
   
   char* serialized_updates = NULL;
   
   // start posting 
   rc = ms_client_send_updates_begin( client, updates, &serialized_updates, nctx );
   if( rc < 0 ) {
      errorf("ms_client_send_updates_begin rc = %d\n", rc );
      
      return rc;
   }
   
   struct ms_client_multi_cls* multi_cls = CALLOC_LIST( struct ms_client_multi_cls, 1 );
   multi_cls->updates = updates;
   multi_cls->serialized_updates = serialized_updates;
   multi_cls->num_expected_replies = ms_client_num_expected_reply_ents( num_reqs, ms_op );
   
   ms_client_network_context_set_cls( nctx, multi_cls );
   
   return rc;
}

// finish performing multiple instances of an operation over a set of file and director records on the MS 
// return 0 if we were able to extract useful information from the MS reply.  If the MS itself returned an error, it will be encoded in results->reply_error and this method will return 0.
// return -EINVAL if nctx was not properly set up.
// if there were no network or formatting errors, populate results with the results of successful operations.
int ms_client_multi_end( struct ms_client* client, struct ms_client_multi_result* results, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   ms::ms_reply reply;
   
   // restore context 
   ms_client_update_set* updates = NULL;
   char* serialized_updates = NULL;
   int num_expected_replies = 0;
   int num_expected_processed = 0;
   
   struct ms_client_multi_cls* multi_cls = (struct ms_client_multi_cls*)ms_client_network_context_get_cls( nctx );
   ms_client_network_context_set_cls( nctx, NULL );
   
   if( multi_cls == NULL ) {
      return -EINVAL;
   }
   
   updates = multi_cls->updates;
   serialized_updates = multi_cls->serialized_updates;
   num_expected_replies = multi_cls->num_expected_replies;
   num_expected_processed = updates->size();
   
   // done with this
   free( multi_cls );
   
   // finish sending 
   rc = ms_client_send_updates_end( client, &reply, true, nctx );
   
   // done with this
   if( serialized_updates != NULL ) {
      free( serialized_updates );
   }
   
   if( rc != 0 ) {
      
      errorf("ms_client_send_updates_end rc = %d\n", rc );
   }
   
   else {
      // if requested, get back at least partial data
      rc = ms_client_get_partial_results( &reply, results, num_expected_processed, num_expected_replies );
      if( rc != 0 ) {
         
         errorf("WARN: ms_client_get_partial_results rc = %d\n", rc );
         
         // error code should be encoded in results->reply_error 
         if( results->reply_error != 0 ) {
            rc = 0;
         }
      }
   }
   
   // free updates (shallow-copied, so no further action needed)
   delete updates;
   
   return rc;
}


// cancel a multi-upload 
// call this in place of ms_client_multi_end
int ms_client_multi_cancel( struct ms_client* client, struct ms_client_network_context* nctx ) {
   
   int rc = ms_client_network_context_cancel( client, nctx );
   if( rc != 0 ) {
      errorf("ms_client_network_context_cancel(%p) rc = %d\n", nctx->dlctx, rc );
      return rc;
   }
   
   // free the associated cls, if givn 
   struct ms_client_multi_cls* multi_cls = (struct ms_client_multi_cls*)ms_client_network_context_get_cls( nctx );
   
   if( multi_cls != NULL ) {
      
      if( multi_cls->updates != NULL ) {
         
         // NOTE: shallow-copied from the caller in ms_client_multi_begin, so don't free the contents of updates
         delete multi_cls->updates;
         multi_cls->updates = NULL;
      }
      
      if( multi_cls->serialized_updates ) {
         
         free( multi_cls->serialized_updates );
         multi_cls->serialized_updates = NULL;
      }
      
      memset( multi_cls, 0, sizeof(struct ms_client_multi_cls) );
      
      free( multi_cls );
   }
   
   ms_client_network_context_set_cls( nctx, NULL );
   
   return 0;
}


// perform a single operation on the MS, synchronously 
// return 0 on success, negative on error.  This method is considered failed if there is a protocol error, a message formatting error, or an RPC error
// allocate and populate the ms_client_multi_result field 
int ms_client_single_rpc( struct ms_client* client, int ms_op, int ms_op_flags, struct ms_client_request* request, struct ms_client_multi_result* result ) {
   
   int rc = 0;
   struct ms_client_network_context nctx;
   
   memset( &nctx, 0, sizeof(struct ms_client_network_context) );
   
   // start creating 
   rc = ms_client_multi_begin( client, ms_op, ms_op_flags, request, 1, &nctx );
   if( rc != 0 ) {
      errorf("ms_client_multi_begin rc = %d\n", rc );
      return rc;
   }
   
   // finish creating 
   rc = ms_client_multi_end( client, result, &nctx );
   if( rc != 0 ) {
      errorf("ms_client_multi_end rc = %d\n", rc );
      return rc;
   }
   
   // any errors?
   if( result->reply_error != 0 ) {
      errorf("MS reply error = %d\n", result->reply_error );
      rc = result->reply_error;
   }
   
   return rc;
}


// perform one update rpc with the MS, synchronously
// return 0 on success, negative on error (be it protocol, formatting, or RPC error)
int ms_client_update_rpc( struct ms_client* client, struct md_update* up ) {
   
   int rc = 0;
   ms_client_update_set updates;
   ms::ms_reply reply;
   
   ms_client_add_update( &updates, up );
   
   // perform the RPC 
   rc = ms_client_send_updates( client, &updates, &reply, true );
   if( rc != 0 ) {
      errorf("ms_client_send_updates rc = %d\n", rc );
      return rc;
   }
   else if( reply.error() != 0 ) {
      errorf("MS reply error = %d\n", reply.error() );
      return reply.error();
   }
   
   return 0;
}


// create a single file or directory record on the MS, synchronously
// assert that the entry's type (MD_ENTRY_FILE or MD_ENTRY_DIRECTORY) matches a given type (return -EINVAL if it doesn't)
// return 0 on success, negative on error
// ent will not be modified
static int ms_client_create_or_mkdir( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, int type, struct md_entry* ent ) {
   
   // sanity check 
   if( ent->type != type ) {
      errorf("Entry '%s' has type %d; expected type %d\n", ent->name, ent->type, type );
      return EINVAL;
   }
   
   int rc = 0;
   struct ms_client_multi_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   // remember the old file ID
   uint64_t old_file_id = ent->file_id;
   uint64_t new_file_id = ms_client_make_file_id();
   
   // request a particular file ID
   ent->file_id = new_file_id;
   
   dbprintf("desired file_id: %" PRIX64 "\n", ent->file_id );
   
   // populate the request 
   req.ent = ent;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, ms::ms_update::CREATE, 0, &req, &result );
   
   // restore 
   ent->file_id = old_file_id;
   
   if( rc != 0 ) {
      
      errorf("ms_client_single_rpc(CREATE) rc = %d\n", rc );
   }
   else {
      
      // get data
      if( result.num_processed != 1 ) {
         errorf("ERR: created %d entries\n", result.num_processed );
      }
      else {
         // got data!  make sure it matches (the MS should have went with our file ID)
         if( new_file_id != result.ents[0].file_id ) {
            errorf("MS returned invalid data: expected file ID %" PRIX64 ", but got %" PRIX64 "\n", new_file_id, result.ents[0].file_id );
            rc = -EBADMSG;
         }
         
         else {
            *file_id_ret = result.ents[0].file_id;
            *write_nonce_ret = result.ents[0].write_nonce;
         }
      }
   }
   
   ms_client_multi_result_free( &result );
   
   return rc;
}


// create a single file on the MS, synchronously.
// unlike ms_client_create_or_mkdir, this only works for files
int ms_client_create( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, file_id_ret, write_nonce_ret, MD_ENTRY_FILE, ent );
}

// create a single directory on the MS, synchronously 
// unlike ms_client_create_or_mkdir, this only works for diretories 
int ms_client_mkdir( struct ms_client* client, uint64_t* file_id_ret, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_create_or_mkdir( client, file_id_ret, write_nonce_ret, MD_ENTRY_DIR, ent );
}


// delete a record from the MS, synchronously
int ms_client_delete( struct ms_client* client, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_multi_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof( struct ms_client_request ) );
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   // populate the request 
   req.ent = ent;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, ms::ms_update::DELETE, 0, &req, &result );
   if( rc != 0 ) {
      
      errorf("ms_client_single_rpc(DELETE) rc = %d\n", rc );
   }
   
   ms_client_multi_result_free( &result );
   
   return 0;
}


// update a record on the MS, synchronously
int ms_client_update_write( struct ms_client* client, int64_t* write_nonce, struct md_entry* ent, uint64_t* in_affected_blocks, size_t num_affected_blocks ) {
   
   int rc = 0;
   struct ms_client_multi_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   // populate the request 
   req.ent = ent;
   req.affected_blocks = in_affected_blocks;
   req.num_affected_blocks = num_affected_blocks;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, ms::ms_update::UPDATE, 0, &req, &result );
   if( rc != 0 ) {
      
      errorf("ms_client_single_rpc(UPDATE) rc = %d\n", rc );
   }
   else {
      if( result.num_processed != 1 ) {
         errorf("ERR: updated %d entries\n", result.num_processed );
         rc = -ENODATA;
      }
      else {
         // got data too!
         *write_nonce = result.ents[0].write_nonce;
      }
   }
   
   ms_client_multi_result_free( &result );
   
   return 0;
}


// update a record on the MS, synchronously, NOT due to a write()
int ms_client_update( struct ms_client* client, int64_t* write_nonce_ret, struct md_entry* ent ) {
   return ms_client_update_write( client, write_nonce_ret, ent, NULL, 0 );
}

// change coordinator ownership of a file on the MS, synchronously
// return 0 on success, and give back the write nonce and new coordinator ID of the file
// return negative on error
int ms_client_coordinate( struct ms_client* client, uint64_t* new_coordinator, int64_t* write_nonce, struct md_entry* ent ) {
   
   int rc = 0;
   struct ms_client_multi_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   // populate the request 
   req.ent = ent;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, ms::ms_update::CHCOORD, 0, &req, &result );
   if( rc != 0 ) {
      
      errorf("ms_client_single_rpc(CHCOORD) rc = %d\n", rc );
   }
   else {
      if( result.num_processed != 1 ) {
         errorf("ERR: updated %d entries\n", result.num_processed );
      }
      else {
         // got data too!
         *write_nonce = result.ents[0].write_nonce;
         *new_coordinator = result.ents[0].coordinator;
      }
   }
   
   ms_client_multi_result_free( &result );
   
   return rc;
}

// rename from src to dest, synchronously
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
   struct ms_client_multi_result result;
   struct ms_client_request req;
   
   memset( &req, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_multi_result) );
   
   // populate the request 
   req.ent = src;
   req.dest = dest;
   
   // perform the operation 
   rc = ms_client_single_rpc( client, ms::ms_update::RENAME, 0, &req, &result );
   if( rc != 0 ) {
      
      errorf("ms_client_single_rpc(RENAME) rc = %d\n", rc );
   }
   else {
      if( result.num_processed != 1 ) {
         errorf("ERR: updated %d entries\n", result.num_processed );
      }
      else {
         // got data too!
         *write_nonce = result.ents[0].write_nonce;
         
         dbprintf("New write_nonce of %" PRIX64 " is %" PRId64 "\n", src->file_id, *write_nonce );
      }
   }
   
   ms_client_multi_result_free( &result );
   
   return rc;
}


// serialize and begin sending a batch of updates.
// put the allocated serialized updates bufer into serialized_updates, which the caller must free after the transfer finishes.
// client must not be locked
// return 0 on success, and allocate *serialized_updates to hold the serialized update set (which the caller must free once the transfer finishes)
// return 1 if we didn't have an error, but had nothing to send.
// return negative on error 
int ms_client_send_updates_begin( struct ms_client* client, ms_client_update_set* all_updates, char** serialized_updates, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   
   // don't do anything if we have nothing to do
   if( all_updates->size() == 0 ) {
      // nothing to do
      return 1;
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

   // start sending 
   rc = ms_client_send_begin( client, file_url, update_text, update_text_len, nctx );
   if( rc != 0 ) {
      errorf("ms_client_send_begin(%s) rc = %d\n", file_url, rc );
      free( update_text );
   }
   else {
      *serialized_updates = update_text;
   }
   
   free( file_url );
   
   return rc;
}


// finish sending a batch of updates
// client must not be locked 
// return 0 on success
// return negative on parse error, or positive HTTP status on HTTP error
int ms_client_send_updates_end( struct ms_client* client, ms::ms_reply* reply, bool verify_response, struct ms_client_network_context* nctx ) {
   
   int rc = 0;
   
   rc = ms_client_send_end( client, reply, verify_response, nctx );
   if( rc != 0 ) {
      errorf("ms_client_send_end rc = %d\n", rc );
   }
   
   return rc;
}

// send a batch of updates.
// client must NOT be locked in any way.
// return 0 on success (or if there are no updates)
// return negative on error
int ms_client_send_updates( struct ms_client* client, ms_client_update_set* all_updates, ms::ms_reply* reply, bool verify_response ) {

   int rc = 0;
   struct ms_client_network_context nctx;
   char* serialized_updates = NULL;
   
   memset( &nctx, 0, sizeof(struct ms_client_network_context) );
   
   rc = ms_client_send_updates_begin( client, all_updates, &serialized_updates, &nctx );
   if( rc < 0 ) {
      
      errorf("ms_client_send_updates_begin rc = %d\n", rc );
      
      return rc;
   }
   
   if( rc == 1 ) {
      // nothing to do 
      return 0;
   }
   
   // wait for it to finish 
   rc = ms_client_send_updates_end( client, reply, verify_response, &nctx );
   if( rc != 0 ) {
      
      errorf("ms_client_send_updates_end rc = %d\n", rc );
      
      return rc;
   }
   
   if( serialized_updates ) {
      free( serialized_updates );
   }
   
   return 0;
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
   
// parse an MS listing, initializing the given ms_listing structure
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
   
   dst->error = reply->error();

   return 0;
}


// free an MS listing
void ms_client_free_listing( struct ms_listing* listing ) {
   if( listing->entries != NULL ) {
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


// initialize an ms path download context 
// NOTE: path_ent will be referenced by pdlctx, so don't free it before freeing pdlctx.
static int ms_client_path_download_context_init( struct ms_client* client, struct ms_path_download_context* pdlctx, struct ms_path_ent* path_ent ) {

   memset( pdlctx, 0, sizeof(struct ms_path_download_context) );
   
   // TODO: connection pool 
   CURL* curl_handle = curl_easy_init();
   
   char* url = ms_client_file_read_url( client->url, path_ent->volume_id, path_ent->file_id, path_ent->version, path_ent->write_nonce, 0 );
   
   // NOTE: no cache driver for the MS, so we'll do this manually 
   md_init_curl_handle( client->conf, curl_handle, url, client->conf->connect_timeout );
   curl_easy_setopt( curl_handle, CURLOPT_USERPWD, client->userpass );
   curl_easy_setopt( curl_handle, CURLOPT_URL, url );
   
   pdlctx->dlctx = CALLOC_LIST( struct md_download_context, 1 );
   
   int rc = md_download_context_init( pdlctx->dlctx, curl_handle, NULL, NULL, -1 );
   if( rc != 0 ) {
      
      errorf("md_download_context_init( %s ) rc = %d\n", url, rc );
      free( url );
      free( pdlctx->dlctx );
      
      return rc;
   }
   
   free( url );
   
   pdlctx->page_id = 0;
   pdlctx->path_ent = path_ent;
   
   return rc;
}
   

// free an ms path download context 
static int ms_client_path_download_context_free( struct ms_path_download_context* pdlctx ) {

   if( pdlctx->dlctx ) {
      if( !md_download_context_finalized( pdlctx->dlctx ) ) {
         // wait for it...
         md_download_context_wait( pdlctx->dlctx, -1 );
      }
      
      // TODO: connection pool
      CURL* old_handle = NULL;
      md_download_context_free( pdlctx->dlctx, &old_handle );
      
      if( old_handle != NULL ) {
         curl_easy_cleanup( old_handle );
      }
      
      free( pdlctx->dlctx );
      pdlctx->dlctx = NULL;
   }
   
   memset( pdlctx, 0, sizeof(struct ms_path_download_context));
   
   return 0;
}


// free all downloads 
static int ms_client_free_path_downloads( struct ms_client* client, struct ms_path_download_context* path_downloads, unsigned int num_downloads ) {
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      ms_client_path_download_context_free( &path_downloads[i] );
   }
   
   return 0;
}


// cancel all running downloads
static int ms_client_cancel_path_downloads( struct ms_client* client, struct ms_path_download_context* path_downloads, unsigned int num_downloads ) {
   
   // cancel all downloads
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      if( !md_download_context_finalized( path_downloads[i].dlctx ) ) {
         // cancel this 
         md_download_context_cancel( &client->dl, path_downloads[i].dlctx );
      }
   }
   
   return 0;
}


// set up a path download 
static int ms_client_set_up_path_downloads( struct ms_client* client, ms_path_t* path, struct ms_path_download_context** ret_path_downloads ) {
   
   unsigned int num_downloads = path->size();
   
   // fetch all downloads concurrently 
   struct ms_path_download_context* path_downloads = CALLOC_LIST( struct ms_path_download_context, num_downloads );
   
   int rc = 0;
   
   // set up downloads
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      
      // TODO: use a connection pool for the MS
      struct ms_path_ent* path_ent = &path->at(i);
      
      rc = ms_client_path_download_context_init( client, &path_downloads[i], path_ent );
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


// start running path downloads 
int ms_client_start_path_downloads( struct ms_client* client, struct ms_path_download_context* path_downloads, int num_downloads ) {
   
   int rc = 0;
   
   // start downloads on the MS downloader
   for( int i = 0; i < num_downloads; i++ ) {
      
      rc = md_download_context_start( &client->dl, path_downloads[i].dlctx, NULL, NULL );
      if( rc != 0 ) {
         // shouldn't happen, but just in case 
         errorf("md_download_context_start(%p (%" PRIX64 ")) rc = %d\n", path_downloads[i].dlctx, path_downloads[i].path_ent->file_id, rc );
         break;
      }
   }
   
   return rc;
}


// extract a path listing from a successful download context.
// Return 0 on success, negative on error:
// * -EIO if we couldn't get the download buffer from the download context, or if we couldn't unserialize the buffer.
// * -ENODATA if there is no listing structure in the reply 
// * -EBADMSG if the reply had inconsistent/incoherent data
// NOTE: this method has the possible side-effect of waking up the Volume reload thread, since it looks at the versioning information 
// that piggybacks on MS reply messages to see if we have stale certificates.
static int ms_client_read_listing_from_path_download_context( struct ms_client* client, struct ms_path_download_context* pdlctx, struct ms_listing* listing ) {
   
   if( !md_download_context_finalized( pdlctx->dlctx ) ) {
      return -EINVAL;
   }
   
   // get the buffer 
   char* buf = NULL;
   off_t buf_len = 0;
   int rc = 0;
   
   struct md_download_context* dlctx = pdlctx->dlctx;
   
   rc = md_download_context_get_buffer( dlctx, &buf, &buf_len );
   if( rc != 0 ) {
      
      errorf("md_download_context_get_buffer rc = %d\n", rc );
      rc = -EIO;
      return rc;
   }
   
   // parse and verify
   ms::ms_reply reply;
   rc = ms_client_parse_reply( client, &reply, buf, buf_len, true );
   if( rc != 0 ) {
      errorf("ms_client_parse_reply rc = %d\n", rc );
      rc = -EIO;
      
      free( buf );
      return rc;
   }
   
   // verify that we have the listing 
   if( !reply.has_listing() ) {
      
      errorf("%s", "MS reply does not contain a listing\n" );
      rc = -ENODATA;
      
      free( buf );
      return rc;
   }
   
   // extract versioning information from the reply,
   // and possibly trigger a cert reload.
   uint64_t volume_id = ms_client_get_volume_id( client );
   ms_client_process_header( client, volume_id, reply.volume_version(), reply.cert_version() );
   
   rc = ms_client_parse_listing( listing, &reply );
   
   free( buf );
   
   if( rc != 0 ) {
      
      errorf("ms_client_parse_listing rc = %d\n", rc );
      rc = -EIO;
      
      return rc;
   }
   
   return 0;
}


// merge two listings' entries (i.e. from two pages of the same directory).
// Skip the first element of src if skip_first is true (i.e. each listing has the parent directory as the first entry, which we can ignore if we're accumulating multiple pages)
// free the src listing
static int ms_client_merge_listing_entries( struct ms_listing* dest, struct ms_listing* src, bool skip_first ) {
   
   unsigned int start = 0;
   if( skip_first ) {
      start = 1;
   }
   
   for( unsigned int i = start; i < src->entries->size(); i++ ) {
      
      dest->entries->push_back( src->entries->at(i) );
   }
   
   src->entries->erase( src->entries->begin() + start, src->entries->end() );
   
   return 0;
}


// try to download again
// NOTE:  dlctx must be finalized
static int ms_client_retry_path_download( struct ms_client* client, struct md_download_context* dlctx, int attempts ) {
   
   if( !md_download_context_finalized( dlctx ) ) {
      return -EINVAL;
   }
   
   int rc = 0;
   
   // try again?
   if( attempts < client->conf->max_metadata_read_retry ) {
      
      md_download_context_reset( dlctx, NULL );
      
      rc = md_download_context_start( &client->dl, dlctx, NULL, NULL );
      if( rc != 0 ) {
         // shouldn't happen, but just in case 
         errorf("md_download_context_start(%p) rc = %d\n", dlctx, rc );
         return rc;
      }
   }
   else {
      // download failed, and we tried as many times as we could
      rc = -ENODATA;
   }
   
   return rc;
}


// consume a downloaded listing.  If we have more, indicate it in *have_more
// if this is not page 0, then skip the first entry in the listing (since listing[0] is the directory)
// NOTE:  pdlctx->dlctx should be finalized
static int ms_client_consume_listing_page( struct ms_client* client, struct ms_path_download_context* pdlctx, bool* have_more ) {
   
   if( !md_download_context_finalized( pdlctx->dlctx ) ) {
      errorf("%p is already finalized\n", pdlctx->dlctx);
      return -EINVAL;
   }
   
   *have_more = false;
   
   struct ms_listing listing;
   memset( &listing, 0, sizeof(struct ms_listing) );
   
   struct md_download_context* dlctx = pdlctx->dlctx;
   
   // get the listing
   int rc = ms_client_read_listing_from_path_download_context( client, pdlctx, &listing );
   if( rc != 0 ) {
      
      errorf("ms_client_read_listing_from_path_download_context(%p) rc = %d\n", dlctx, rc );
      return rc;
   }
   
   // remember the listing, if we haven't alread
   bool have_listing = pdlctx->have_listing;
   if( !have_listing ) {
      
      // shallow-copy it in 
      pdlctx->listing_buf = listing;
      pdlctx->have_listing = true;
   }
      
   // do we have more entries?
   if( listing.status == MS_LISTING_NEW ) {
      
      if( listing.entries->size() > 1 ) {
         // NOTE: entries[0] is the parent directory 
         // ask for more
         *have_more = true;
      }
      
      if( have_listing ) {
         
         dbprintf("Consume page %d (entries: %zu)\n", pdlctx->page_id, listing.entries->size() );
         
         // merge them in to our accumulated listing (shallow-copy)
         ms_client_merge_listing_entries( &pdlctx->listing_buf, &listing, pdlctx->page_id > 0 );
         ms_client_free_listing( &listing );
      }
   }
   else if( listing.status == MS_LISTING_NOCHANGE ) {
      
      errorf("%s", "WARN: listing.status == NOCHANGE\n" );
      
      dbprintf("Ignore page %d (entries: %zu)\n", pdlctx->page_id, listing.entries->size() );
      
      if( listing.entries->size() > 1 ) {
         // NOTE: entries[0] is the parent directory 
         // ask for more
         *have_more = true;
      }
      
      if( have_listing ) {
         
         // don't need to remember this 
         ms_client_free_listing( &listing );
      }
   }
   else if( listing.status == MS_LISTING_NONE ) {
      
      errorf("%s", "ERR: listing.status == NONE\n");
      
      rc = listing.error;
      if( rc == 0 ) {
         rc = -ENOENT;
      }
      
      if( have_listing ) {
         
         // don't need to remember this 
         ms_client_free_listing( &listing );
      }
   }  
   else {
      
      errorf("ERR: listing.status == %d\n", listing.status );
      
      rc = -EBADMSG;
      if( have_listing ) {
         
         ms_client_free_listing( &listing );
      }
   }
   
   return rc;
}


// proceed to download the next page
static int ms_client_start_next_page( struct ms_client* client, struct ms_path_download_context* pdlctx ) {
      
   struct md_download_context* dlctx = pdlctx->dlctx;
   
   pdlctx->page_id ++;
   
   dbprintf("Download %p: download page %d\n", dlctx, pdlctx->page_id );
   
   // restart the download
   md_download_context_reset( dlctx, NULL );
   
   char* new_url = ms_client_file_read_url( client->url, pdlctx->path_ent->volume_id, pdlctx->path_ent->file_id, pdlctx->path_ent->version, pdlctx->path_ent->write_nonce, pdlctx->page_id );
   
   // set the new URL 
   CURL* curl = md_download_context_get_curl( dlctx );
   curl_easy_setopt( curl, CURLOPT_URL, new_url );
   
   free( new_url );
   
   // start downloading the next page
   int rc = md_download_context_start( &client->dl, dlctx, NULL, NULL );
   if( rc != 0 ) {
      // shouldn't happen, but just in case 
      errorf("md_download_context_start(%p) rc = %d\n", dlctx, rc );
   }
   
   return rc;
}


// run a set of downloads
// retry ones that time out, up to conf->max_metadata_read_retry times.
static int ms_client_run_path_downloads( struct ms_client* client, struct ms_path_download_context* path_downloads, unsigned int num_downloads ) {
   
   int rc = 0;
   int num_running_downloads = num_downloads;
   
   // associate an attempt counter with each download, to handle timeouts and retries
   map< struct md_download_context*, int > attempts;
   
   // associate with each download context its parent ms_path_download_context
   map< struct md_download_context*, struct ms_path_download_context* > path_contexts;
   
   for( unsigned int i = 0; i < num_downloads; i++ ) {
      attempts[ path_downloads[i].dlctx ] = 0;
   }
   
   
   // set up a download set to track these downloads 
   struct md_download_set path_download_set;
   
   md_download_set_init( &path_download_set );
   
   // add all downloads to the download set 
   for( unsigned int i = 0; i < num_downloads; i++ ) {
         
      rc = md_download_set_add( &path_download_set, path_downloads[i].dlctx );
      if( rc != 0 ) {
         errorf("md_download_set_add rc = %d\n", rc );
         
         md_download_set_free( &path_download_set );
         return rc;
      }
      
      // bind the context to its parent 
      path_contexts[ path_downloads[i].dlctx ] = &path_downloads[i];
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
      
      // list of downloads that succeeded
      vector<struct md_download_context*> succeeded;
      
      rc = 0;
      
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
         
         struct ms_path_download_context* pdlctx = path_contexts[ dlctx ];
         
         // serious MS error?
         if( http_status >= 500 ) {
            errorf("Download %p (%s) HTTP status %d\n", dlctx, final_url, http_status );
            
            rc = -EREMOTEIO;
            
            free( final_url );
            break;
         }
         
         // timed out?
         else if( curl_rc == CURLE_OPERATION_TIMEDOUT || os_err == -ETIMEDOUT ) {
            
            errorf("Download %p (%s) timed out (curl_rc = %d, errno = %d, attempt %d)\n", dlctx, final_url, curl_rc, os_err, attempts[dlctx] + 1);
            free( final_url );
            
            attempts[dlctx] ++;
            
            rc = ms_client_retry_path_download( client, dlctx, attempts[dlctx] );
            if( rc != 0 ) {
               errorf("ms_client_retry_path_download(%p) rc = %d\n", dlctx, rc );
               break;
            }
            
            // count this one as running
            num_running_downloads++;
         }
         
         // some other error?
         else if( http_status != 200 || curl_rc != 0 ) {
            
            errorf("Download %p (%s) failed, HTTP status = %d, cURL rc = %d, errno = %d\n", dlctx, final_url, http_status, curl_rc, os_err );
            
            if( os_err != 0 ) {
               rc = os_err;
            }
            else {
               rc = -EREMOTEIO;
            }
            
            free( final_url );
            break;
         }
         
         else {
            // succeeded!
            free( final_url );
            
            struct ms_listing listing;
            memset( &listing, 0, sizeof(struct ms_listing) );
            
            // more to download?
            bool have_more = false;
            bool restarted = false;
            
            // consume the listing
            int rc = ms_client_consume_listing_page( client, pdlctx, &have_more );
            
            if( rc != 0 ) {
               errorf("ms_client_consume_listing_page(%p, page=%d) rc = %d\n", dlctx, pdlctx->page_id, rc );
               
               if( rc == -EAGAIN ) {
                  // MS says to try this one again 
                  attempts[dlctx] ++;
                  
                  rc = ms_client_retry_path_download( client, dlctx, attempts[dlctx] );
                  if( rc != 0 ) {
                     errorf("ms_client_retry_path_download(%p) rc = %d\n", dlctx, rc );
                     break;
                  }
                  
                  // count this one as running
                  num_running_downloads++;
                  restarted = true;
                  
                  rc = 0;
               }
            }
            
            // more to download?
            if( have_more ) {
               
               // reset attempt count and get the next page 
               attempts[dlctx] = 0;
               
               rc = ms_client_start_next_page( client, pdlctx );
               
               if( rc != 0 ) {
                  errorf("ms_client_start_next_page(%p) rc = %d\n", dlctx, rc );
                  break;
               }
               
               // count this one as running
               num_running_downloads++;
            }
            
            else if( rc == 0 && !restarted ) {
               
               dbprintf("Download %p succeeded!\n", dlctx );
               
               // done with this download context
               succeeded.push_back( dlctx );
            }
         }
      }
      
      // clear the ones that succeeded from the download set 
      for( unsigned int i = 0; i < succeeded.size(); i++ ) {
         
         md_download_set_clear( &path_download_set, succeeded[i] );
      }
      
      if( rc != 0 ) {
         
         dbprintf("Cancel %d path downloads on error (rc = %d)\n", num_downloads, rc );
         
         vector<struct md_download_context*> downloads;
         
         // clear all 
         for( md_download_set_iterator itr = md_download_set_begin( &path_download_set ); itr != md_download_set_end( &path_download_set ); itr++ ) {
            
            struct md_download_context* dlctx = md_download_set_iterator_get_context( itr );
            downloads.push_back( dlctx );
         }
         
         for( unsigned int i = 0; i < downloads.size(); i++ ) {
            
            md_download_set_clear( &path_download_set, downloads[i] );
         }
         
         // cancel all
         int cancel_rc = ms_client_cancel_path_downloads( client, path_downloads, num_downloads );
         if( cancel_rc != 0 ) {
            errorf("ms_client_cancel_path_downloads rc = %d\n", cancel_rc );
         }
      }
   }
   
   md_download_set_free( &path_download_set );
   
   // all downloads finished 
   return rc;
}


// run the path downloads in the download set, retrying any that fail due to timeouts
// on success, put the finalized download contexts into ret_path_downloads
static int ms_client_download_path_listing( struct ms_client* client, ms_path_t* path, struct ms_path_download_context** ret_path_downloads ) {
   
   int rc = 0;
   unsigned int num_downloads = path->size();
   
   // initialize a download set to track these downloads
   struct ms_path_download_context* path_downloads = NULL;
   
   rc = ms_client_set_up_path_downloads( client, path, &path_downloads );
   if( rc != 0 ) {
      errorf("ms_client_set_up_path_downloads rc = %d\n", rc );
      return rc;
   }
   
   rc = ms_client_start_path_downloads( client, path_downloads, num_downloads );
   
   if( rc != 0 ) {
      
      errorf("ms_client_start_path_downloads rc = %d\n", rc );
      
      // stop everything 
      ms_client_cancel_path_downloads( client, path_downloads, num_downloads );
      ms_client_free_path_downloads( client, path_downloads, num_downloads );
      free( path_downloads );
      
      *ret_path_downloads = NULL;
   }
   else {
      // process all downloads 
      rc = ms_client_run_path_downloads( client, path_downloads, num_downloads );
   }
   
   if( rc != 0 ) {
      
      ms_client_free_path_downloads( client, path_downloads, num_downloads );
      free( path_downloads );
      
      *ret_path_downloads = NULL;
   }
   else {
      *ret_path_downloads = path_downloads;
   }
   return rc;
}


// get a set of metadata entries from an MS response.
// on succes, populate ms_response with ms_listing structures for each path entry that needed to be downloaded, as indicated by the stale flag.
// NOTE: if the request was for a page >= 1, then strip the first entry, since it will contain the parent directory's metadata (which was given to the caller in page 0)
int ms_client_get_listings( struct ms_client* client, ms_path_t* path, ms_response_t* ms_response ) {

   unsigned int num_downloads = path->size();

   if( num_downloads == 0 ) {
      // everything's fresh
      return 0;
   }
   
   struct ms_path_download_context* path_downloads = NULL;
   struct timespec ts, ts2;
   
   BEGIN_TIMING_DATA( ts );
   
   int rc = ms_client_download_path_listing( client, path, &path_downloads );
   
   END_TIMING_DATA( ts, ts2, "MS recv" );
   
   if( rc != 0 ) {
      errorf("ms_client_perform_multi_download rc = %d\n", rc );
      
      return rc;
   }

   // do a sanity check, and consume the listings
   for( unsigned int i = 0; i < path->size(); i++ ) {
      
      struct ms_listing* listing = &path_downloads[i].listing_buf;
      
      // sanity check: listing[0], if given, must match the ith path element's file ID 
      if( listing->entries != NULL && listing->entries->size() != 0 ) {
         
         if( listing->entries->at(0).file_id != path->at(i).file_id ) {
            
            errorf("Invalid MS listing: requested listing of %" PRIX64 ", got listing of %" PRIX64 "\n", path->at(i).file_id, listing->entries->at(0).file_id );
            rc = -EBADMSG;
            ms_client_free_response( ms_response );
            break;
         }
      }
      
      // save
      // NOTE: shallow-copy the listing; don't free its contents
      // the ms_response takes ownership.
      (*ms_response)[ path->at(i).file_id ] = *listing;
      path_downloads[i].have_listing = false;
   }

   ms_client_free_path_downloads( client, path_downloads, num_downloads );
   free( path_downloads );

   return rc;
}
