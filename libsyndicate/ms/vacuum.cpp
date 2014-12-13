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
// return -EINVAL if the entry already has blocks 
int ms_client_vacuum_entry_set_blocks( struct ms_vacuum_entry* vreq, uint64_t* affected_blocks, size_t num_affected_blocks ) {
   
   if( vreq->affected_blocks != NULL || vreq->num_affected_blocks != 0 ) 
      return -EINVAL;
 
   vreq->affected_blocks = affected_blocks;
   vreq->num_affected_blocks = num_affected_blocks;
   
   return 0;
}

// free a vacuum entry 
int ms_client_vacuum_entry_free( struct ms_vacuum_entry* vreq ) {
   if( vreq->affected_blocks ) {
      free( vreq->affected_blocks );
   }
   
   memset( vreq, 0, sizeof(struct ms_vacuum_entry) );
   return 0;
}


// extract the affected blocks from an ms_reply 
static int ms_client_vacuum_entry_get_affected_blocks( ms::ms_reply* reply, uint64_t** affected_blocks, size_t* num_affected_blocks ) {
   
   uint64_t* ret = CALLOC_LIST( uint64_t, reply->affected_blocks_size() );
   
   for( int64_t i = 0; i < reply->affected_blocks_size(); i++ ) {
      ret[i] = reply->affected_blocks(i);
   }
   
   *affected_blocks = ret;
   *num_affected_blocks = reply->affected_blocks_size();
   
   return 0;
}

// get the head of the vacuum log for a file 
int ms_client_peek_vacuum_log( struct ms_client* client, uint64_t volume_id, uint64_t file_id, struct ms_vacuum_entry* ve ) {
   
   char* vacuum_url = ms_client_vacuum_url( client->url, volume_id, file_id );
   ms::ms_reply reply;
   int rc = 0;
   
   rc = ms_client_read( client, vacuum_url, &reply );
   
   free( vacuum_url );
   
   if( rc != 0 ) {
      errorf("ms_client_read(peek vacuum %" PRIX64 ") rc = %d\n", file_id, rc );
      return rc;
   }
   else {
      
      // check value 
      if( !reply.has_manifest_mtime_sec() || !reply.has_manifest_mtime_nsec() ) {
         errorf("MS did not reply manifest timestamp for %" PRIX64 "\n", file_id );
         return -ENODATA;
      }
      
      uint64_t* affected_blocks = NULL;
      size_t num_affected_blocks = 0;
      
      ms_client_vacuum_entry_get_affected_blocks( &reply, &affected_blocks, &num_affected_blocks );
      
      ms_client_vacuum_entry_init( ve, volume_id, file_id, reply.file_version(), reply.manifest_mtime_sec(), reply.manifest_mtime_nsec(), affected_blocks, num_affected_blocks );
      
      return 0;
   }
}

// remove a vacuum log entry 
int ms_client_remove_vacuum_log_entry( struct ms_client* client, uint64_t volume_id, uint64_t file_id, uint64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec ) {
   
   // generate our update 
   struct md_update up;
   
   struct md_entry ent;
   memset( &ent, 0, sizeof(ent) );
   
   // sentinel values
   ent.name = strdup("");
   ent.parent_name = strdup("");
   
   // sentinel md_entry with all of our given information
   ent.volume = volume_id;
   ent.file_id = file_id;
   ent.version = file_version;
   ent.manifest_mtime_sec = manifest_mtime_sec;
   ent.manifest_mtime_nsec = manifest_mtime_nsec;
   
   ms_client_populate_update( &up, ms::ms_update::VACUUM, 0, &ent );
   
   int rc = ms_client_update_rpc( client, &up );
   
   md_entry_free( &ent );
   
   return rc;
}
