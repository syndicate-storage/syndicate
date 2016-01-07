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

#include "libsyndicate/ms/vacuum.h"
#include "libsyndicate/ms/url.h"
#include "libsyndicate/ms/file.h"


// make a vacuum entry.
// return 0 on success
// return -ENOMEM on OOM
int ms_client_vacuum_entry_init( struct ms_vacuum_entry* vreq, uint64_t volume_id, uint64_t gateway_id, uint64_t file_id, int64_t file_version,
                                 int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, uint64_t* affected_blocks, size_t num_affected_blocks ) {
   
  
   uint64_t* affected_blocks_dup = SG_CALLOC( uint64_t, num_affected_blocks );
   if( affected_blocks_dup == NULL ) {
      return -ENOMEM;
   }

   memcpy( affected_blocks_dup, affected_blocks, sizeof(affected_blocks[0]) * num_affected_blocks );

   memset( vreq, 0, sizeof(struct ms_vacuum_entry) );
   vreq->volume_id = volume_id;
   vreq->writer_id = gateway_id;
   vreq->file_id = file_id;
   vreq->file_version = file_version;
   vreq->manifest_mtime_sec = manifest_mtime_sec;
   vreq->manifest_mtime_nsec = manifest_mtime_nsec;
   vreq->affected_blocks = affected_blocks_dup;
   vreq->num_affected_blocks = num_affected_blocks;
   
   return 0;
}

// set a vacuum entry's affected blocks (i.e. if they weren't known at the time of initialization).
// the caller must allocate affected_blocks; the ms_vacuum_entry will own the array.
// return 0 on success
// return -EINVAL if the entry already has blocks 
int ms_client_vacuum_entry_set_blocks( struct ms_vacuum_entry* vreq, uint64_t* affected_blocks, size_t num_affected_blocks ) {
   
   if( vreq->affected_blocks != NULL || vreq->num_affected_blocks != 0 ) {
      return -EINVAL;
   }
   
   vreq->affected_blocks = affected_blocks;
   vreq->num_affected_blocks = num_affected_blocks;
   
   return 0;
}

// free a vacuum entry 
// always succeeds
int ms_client_vacuum_entry_free( struct ms_vacuum_entry* vreq ) {
   
   SG_safe_free( vreq->affected_blocks );
   
   memset( vreq, 0, sizeof(struct ms_vacuum_entry) );
   return 0;
}


// extract the affected blocks from an ms_vacuum_ticket 
// return 0 on success, and set *affected_blocs and *num_affected_blocks (the former is calloc'ed)
// return -ENOMEM on OOM
static int ms_client_vacuum_entry_get_affected_blocks( ms::ms_vacuum_ticket* vt, uint64_t** affected_blocks, size_t* num_affected_blocks ) {
   
   if( vt->affected_blocks_size() == 0 ) {
      
      *affected_blocks = NULL;
      *num_affected_blocks = 0;
      return 0;
   }
   
   uint64_t* ret = SG_CALLOC( uint64_t, vt->affected_blocks_size() );
   if( ret == NULL ) {
      
      return -ENOMEM;
   }
   
   for( int64_t i = 0; i < vt->affected_blocks_size(); i++ ) {
      
      ret[i] = vt->affected_blocks(i);
   }
   
   *affected_blocks = ret;
   *num_affected_blocks = vt->affected_blocks_size();
   
   return 0;
}


// sign the affected blocks in an update.
// return 0 on success, and populate *sig and *sig_len 
// return -ENOMEM on OOM 
int ms_client_sign_vacuum_ticket( struct ms_client* client, struct ms_vacuum_entry* ve, unsigned char** sig, size_t* sig_len ) {
   
   int rc = 0;
   ms::ms_vacuum_ticket vt;
   
   vt.set_volume_id( ve->volume_id );
   vt.set_writer_id( ve->writer_id );
   vt.set_file_id( ve->file_id );
   vt.set_file_version( ve->file_version );
   vt.set_manifest_mtime_sec( ve->manifest_mtime_sec );
   vt.set_manifest_mtime_nsec( ve->manifest_mtime_nsec );
   
   for( size_t i = 0; i < ve->num_affected_blocks; i++ ) {
      vt.add_affected_blocks( ve->affected_blocks[i] );
   }
   
   vt.set_signature( string("") );
   
   rc = md_sign< ms::ms_vacuum_ticket >( client->gateway_key, &vt );
   if( rc != 0 ) {
      
      return rc;
   }
   
   *sig_len = vt.signature().size();
   *sig = SG_CALLOC( unsigned char, vt.signature().size() );
   if( *sig == NULL ) {
      
      return -ENOMEM;
   }
   
   memcpy( *sig, vt.signature().data(), *sig_len );
   return 0;
}


// verify the authenticity of a vacuum ticket.
// return 0 on success
// return -EPERM if signature mismatch 
// return -EAGAIN if the gateway that signed is unknown to us 
// return -ENOMEM if OOM
int ms_client_verify_vacuum_ticket( struct ms_client* client, ms::ms_vacuum_ticket* vt ) {
   
   int rc = 0;
   struct ms_gateway_cert* cert = NULL;
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, vt->writer_id() );
   if( cert == NULL ) {
      
      // not known to us 
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   rc = md_verify< ms::ms_vacuum_ticket >( ms_client_gateway_pubkey( cert ), vt );
   
   ms_client_config_unlock( client );
   
   return rc;
}


// get a vacuum log entry for a file 
// return 0 on success
// return -ENOMEM if OOM
// return -ENODATA if there is no vacuum data to be had
// return -EACCES if we aren't allowed to vacuum
// return -EPERM if the vacuum ticket was not signed by a gateway we know
// return -EBADMSG if the vacuum ticket contained invalid data
// return -EREMOTEIO on remote server error
// return -EPROTO on HTTP 400-level error (i.e. no data, access denied, bad request, etc.)
// return negative if we couldn't download or parse the result
int ms_client_peek_vacuum_log( struct ms_client* client, uint64_t volume_id, uint64_t file_id, struct ms_vacuum_entry* ve ) {
   
   char* vacuum_url = ms_client_vacuum_url( client->url, volume_id, ms_client_volume_version( client ), ms_client_cert_version( client ), file_id );
   ms::ms_reply* reply = NULL;
   ms::ms_vacuum_ticket* vacuum_ticket = NULL;
   int rc = 0;
   
   reply = SG_safe_new( ms::ms_reply() );
   if( reply == NULL ) {
      
      return -ENOMEM;
   }
   
   rc = ms_client_read( client, vacuum_url, reply );
   
   SG_safe_free( vacuum_url );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_read(peek vacuum %" PRIX64 ") rc = %d\n", file_id, rc );
      SG_safe_delete( reply );
      
      return rc;
   }
   else {
      
      // check value
      if( !reply->has_vacuum_ticket() ) {
         
         SG_error("MS did not reply vacuum ticket for %" PRIX64 "\n", file_id );
         SG_safe_delete( reply );
         return -ENODATA;
      }
      
      vacuum_ticket = reply->mutable_vacuum_ticket();
      
      rc = ms_client_verify_vacuum_ticket( client, vacuum_ticket );
      if( rc != 0 ) {
         
         SG_error("Failed to verify vacuum ticket for %" PRIX64 "\n", file_id );
         SG_safe_delete( reply );
         return -EPERM;
      }
      
      // sanity check...
      if( file_id != vacuum_ticket->file_id() ) {
         
         SG_error("File ID mismatch: expected %" PRIX64 ", got %" PRIX64 "\n", file_id, vacuum_ticket->file_id());
         SG_safe_delete( reply );
         return -EINVAL;
      }
      
      // sanity check
      if( volume_id != vacuum_ticket->volume_id() ) {
         
         SG_error("Volume ID mismatch: expected %" PRIu64 ", got %" PRIu64 "\n", volume_id, vacuum_ticket->volume_id());
         SG_safe_delete( reply );
         return -EINVAL;
      }
      
      uint64_t* affected_blocks = NULL;
      size_t num_affected_blocks = 0;
      
      rc = ms_client_vacuum_entry_get_affected_blocks( vacuum_ticket, &affected_blocks, &num_affected_blocks );
      if( rc != 0 ) {
         
         SG_safe_delete( reply );
         return rc;
      }
      
      rc = ms_client_vacuum_entry_init( ve, volume_id, vacuum_ticket->writer_id(), file_id, vacuum_ticket->file_version(),
                                        vacuum_ticket->manifest_mtime_sec(), vacuum_ticket->manifest_mtime_nsec(), affected_blocks, num_affected_blocks );
      SG_safe_free( affected_blocks );
      if( rc != 0 ) {

         // OOM
         SG_safe_delete( reply );
         return rc;
      }
      
      SG_safe_delete( reply );
      
      return 0;
   }
}


// remove a vacuum log entry 
// NOTE: any gateway can send this, as long as it is the current coordinator of the file.
// writer_id identifies the gateway that performed the associated write; it can be obtained from the manifest or the vacuum log head.
// return 0 on success
// return -ENOMEM on OOM
// return -EPROTO on MS RPC protocol-level error or HTTP 400-level error
// return negative on RPC error (see ms_client_single_rpc)
int ms_client_remove_vacuum_log_entry( struct ms_client* client, uint64_t volume_id, uint64_t writer_id, uint64_t file_id, uint64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec ) {
   
   struct ms_client_request request;
   struct ms_client_request_result result;
   struct md_entry ent;
   int rc = 0;
   
   unsigned char* ent_sig = NULL;
   size_t ent_sig_len = 0;
   
   memset( &request, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   memset( &ent, 0, sizeof(ent) );
   
   // sentinel values
   ent.name = SG_strdup_or_null("");
   
   if( ent.name == NULL ) {
      SG_safe_free( ent.name );
      return -ENOMEM;
   }
   
   // sentinel md_entry with all of our given information
   ent.coordinator = writer_id;
   ent.volume = volume_id;
   ent.file_id = file_id;
   ent.version = file_version;
   ent.manifest_mtime_sec = manifest_mtime_sec;
   ent.manifest_mtime_nsec = manifest_mtime_nsec;
   
   // sign...
   rc = md_entry_sign( client->gateway_key, &ent, &ent_sig, &ent_sig_len );
   if( rc != 0 ) {
      
      md_entry_free( &ent );
      return -ENOMEM;
   }
   
   ent.ent_sig = ent_sig;
   ent.ent_sig_len = ent_sig_len;
   
   // fill in the request
   request.op = ms::ms_request::VACUUM;
   request.flags = 0;
   
   request.ent = &ent;
   
   rc = ms_client_single_rpc( client, &request, &result );
   
   md_entry_free( &ent );       // frees signature as well
   
   if( rc != 0 ) {
      return rc;
   }
   
   if( result.reply_error != 0 ) {
      // protocol-level error 
      return -EPROTO;
   }
   
   if( result.rc != 0 ) {
      return result.rc;
   }
   
   return 0;
}


// Append the vacuum log entry for a file.
// Do this before replicating the actual data.
// return 0 on success 
// return -ENOMEM on OOM 
// return negative on RPC error 
int ms_client_append_vacuum_log_entry( struct ms_client* client, struct ms_vacuum_entry* ve ) {
   
   // generate our update 
   struct md_entry ent;
   struct ms_client_request request;
   struct ms_client_request_result result;
   int rc = 0;
   
   unsigned char* ent_sig = NULL;
   size_t ent_sig_len = 0;
   
   unsigned char* vacuum_ticket_sig = NULL;
   size_t vacuum_ticket_sig_len = 0;
   
   memset( &ent, 0, sizeof(ent) );
   memset( &request, 0, sizeof(struct ms_client_request) );
   memset( &result, 0, sizeof(struct ms_client_request_result) );
   
   // get signature 
   rc = ms_client_sign_vacuum_ticket( client, ve, &vacuum_ticket_sig, &vacuum_ticket_sig_len );
   if( rc != 0 ) {
      
      return rc;
   }
   
   // sentinel md_entry with all of our given information
   ent.volume = ve->volume_id;
   ent.coordinator = ve->writer_id;     // 'coordinator' carries *this* gateway's ID to the vacuum log
   ent.file_id = ve->file_id;
   ent.version = ve->file_version;
   ent.manifest_mtime_sec = ve->manifest_mtime_sec;
   ent.manifest_mtime_nsec = ve->manifest_mtime_nsec;
           
   // sign...
   rc = md_entry_sign( client->gateway_key, &ent, &ent_sig, &ent_sig_len );
   if( rc != 0 ) {
      
      md_entry_free( &ent );
      SG_safe_free( vacuum_ticket_sig );
      return -ENOMEM;
   }
   
   ent.ent_sig = ent_sig;
   ent.ent_sig_len = ent_sig_len;
   
   // fill in the request
   request.op = ms::ms_request::VACUUMAPPEND;
   request.flags = 0;
   
   request.ent = &ent;
   request.affected_blocks = ve->affected_blocks;
   request.num_affected_blocks = ve->num_affected_blocks;
   request.vacuum_signature = vacuum_ticket_sig;
   request.vacuum_signature_len = vacuum_ticket_sig_len;
   
   rc = ms_client_single_rpc( client, &request, &result );
   
   ent.name = NULL;
   md_entry_free( &ent );       // frees signature as well

   SG_safe_free( vacuum_ticket_sig );
   request.vacuum_signature = NULL;
   
   if( rc != 0 ) {
      return rc;
   }
   
   if( result.reply_error != 0 ) {
      // protocol-level error 
      return -EPROTO;
   }
   
   if( result.rc != 0 ) {
      return result.rc;
   }
   
   return rc;
}


