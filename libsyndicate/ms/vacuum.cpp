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
// the resulting ms_vacuum_entry structure will own the affected_blocks array (which the caller must dynamically allocate)
// always succeeds
int ms_client_vacuum_entry_init( struct ms_vacuum_entry* vreq, uint64_t volume_id, uint64_t file_id, int64_t file_version,
                                 int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, uint64_t* affected_blocks, size_t num_affected_blocks ) {
   
   memset( vreq, 0, sizeof(struct ms_vacuum_entry) );
   
   vreq->volume_id = volume_id;
   vreq->file_id = file_id;
   vreq->file_version = file_version;
   vreq->manifest_mtime_sec = manifest_mtime_sec;
   vreq->manifest_mtime_nsec = manifest_mtime_nsec;
   vreq->affected_blocks = affected_blocks;
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
   SG_safe_delete( vreq->ticket );
   
   memset( vreq, 0, sizeof(struct ms_vacuum_entry) );
   return 0;
}


// extract the affected blocks from an ms_reply 
// return 0 on success, and set *affected_blocs and *num_affected_blocks (the former is calloc'ed)
// return -ENOMEM on OOM
static int ms_client_vacuum_entry_get_affected_blocks( ms::ms_reply* reply, uint64_t** affected_blocks, size_t* num_affected_blocks ) {
   
   if( reply->affected_blocks_size() == 0 ) {
      
      *affected_blocks = NULL;
      *num_affected_blocks = 0;
      return 0;
   }
   
   uint64_t* ret = SG_CALLOC( uint64_t, reply->affected_blocks_size() );
   if( ret == NULL ) {
      
      return -ENOMEM;
   }
   
   for( int64_t i = 0; i < reply->affected_blocks_size(); i++ ) {
      
      ret[i] = reply->affected_blocks(i);
   }
   
   *affected_blocks = ret;
   *num_affected_blocks = reply->affected_blocks_size();
   
   return 0;
}

// get the head of the vacuum log for a file 
// return 0 on success
// return -ENOMEM if OOM
// return -ENODATA if there is no manifest timestamp in the message
// return negative if we couldn't download or parse the result
int ms_client_peek_vacuum_log( struct ms_client* client, uint64_t volume_id, uint64_t file_id, struct ms_vacuum_entry* ve, bool keep_ticket ) {
   
   char* vacuum_url = ms_client_vacuum_url( client->url, volume_id, file_id );
   ms::ms_reply* reply = NULL;
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
      if( !reply->has_manifest_mtime_sec() || !reply->has_manifest_mtime_nsec() ) {
         
         SG_error("MS did not reply manifest timestamp for %" PRIX64 "\n", file_id );
         SG_safe_delete( reply );
         
         return -ENODATA;
      }
      
      uint64_t* affected_blocks = NULL;
      size_t num_affected_blocks = 0;
      
      rc = ms_client_vacuum_entry_get_affected_blocks( reply, &affected_blocks, &num_affected_blocks );
      if( rc != 0 ) {
         
         SG_safe_delete( reply );
         return rc;
      }
      
      ms_client_vacuum_entry_init( ve, volume_id, file_id, reply->file_version(), reply->manifest_mtime_sec(), reply->manifest_mtime_nsec(), affected_blocks, num_affected_blocks );
      
      if( keep_ticket ) {
         
         ve->ticket = reply;
      }
      else {
         
         SG_safe_delete( reply );
      }
      
      return 0;
   }
}


// remove a vacuum log entry 
// return 0 on success
// return -ENOMEM on OOM
// return negative on RPC error (see ms_client_update_rpc)
int ms_client_remove_vacuum_log_entry( struct ms_client* client, uint64_t volume_id, uint64_t file_id, uint64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec ) {
   
   // generate our update 
   struct md_update up;
   struct md_entry ent;
   int rc = 0;
   
   memset( &ent, 0, sizeof(ent) );
   
   // sentinel values
   ent.name = SG_strdup_or_null("");
   ent.parent_name = SG_strdup_or_null("");
   
   if( ent.name == NULL || ent.parent_name == NULL ) {
      SG_safe_free( ent.name );
      SG_safe_free( ent.parent_name );
      return -ENOMEM;
   }
   
   // sentinel md_entry with all of our given information
   ent.volume = volume_id;
   ent.file_id = file_id;
   ent.version = file_version;
   ent.manifest_mtime_sec = manifest_mtime_sec;
   ent.manifest_mtime_nsec = manifest_mtime_nsec;
   
   ms_client_populate_update( &up, ms::ms_update::VACUUM, 0, &ent );
   
   rc = ms_client_update_rpc( client, &up );
   
   md_entry_free( &ent );
   
   return rc;
}


// append the vacuum log entry for a file
// return 0 on success 
// return -ENOMEM on OOM 
// return negative on RPC error 
int ms_client_append_vacuum_log_entry( struct ms_client* client, struct ms_vacuum_entry* ve ) {
   
   // generate our update 
   struct md_update up;
   struct md_entry ent;
   int rc = 0;
   
   memset( &ent, 0, sizeof(ent) );
   
   // sentinel values
   ent.name = SG_strdup_or_null("");
   ent.parent_name = SG_strdup_or_null("");
   
   if( ent.name == NULL || ent.parent_name == NULL ) {
      SG_safe_free( ent.name );
      SG_safe_free( ent.parent_name );
      return -ENOMEM;
   }
   
   // sentinel md_entry with all of our given information
   ent.volume = ve->volume_id;
   ent.file_id = ve->file_id;
   ent.version = ve->file_version;
   ent.manifest_mtime_sec = ve->manifest_mtime_sec;
   ent.manifest_mtime_nsec = ve->manifest_mtime_nsec;
   
   ms_client_populate_update( &up, ms::ms_update::VACUUMAPPEND, 0, &ent );
   
   // include vacuum information 
   up.affected_blocks = ve->affected_blocks;
   up.num_affected_blocks = ve->num_affected_blocks;
   
   rc = ms_client_update_rpc( client, &up );
   
   md_entry_free( &ent );
   
   return rc;
}


