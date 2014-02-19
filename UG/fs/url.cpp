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

#include "url.h"
#include "manifest.h"

// split a uint64 into four uint16s.
// assume i is litte-endian; otherwise convert it
void fs_entry_split_uint64( uint64_t i, uint16_t* o ) {
   if( htonl( 1234 ) == 1234 ) {
      // i is big endian...
      i = htole64( i );
   }
   o[0] = (i & (uint64_t)0xFFFF000000000000LL) >> 48;
   o[1] = (i & (uint64_t)0x0000FFFF00000000LL) >> 32;
   o[2] = (i & (uint64_t)0x00000000FFFF0000LL) >> 16;
   o[3] = (i & (uint64_t)0x000000000000FFFFLL);
}

char* fs_entry_path_from_file_id( uint64_t file_id ) {
   uint16_t file_id_parts[4];
   fs_entry_split_uint64( file_id, file_id_parts );

   char* ret = CALLOC_LIST( char, 21 );
   sprintf(ret, "/%04X/%04X/%04X/%04X", file_id_parts[0], file_id_parts[1], file_id_parts[2], file_id_parts[3] );
   return ret;
}


char* fs_entry_block_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, bool local ) {

   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 1 + 25 + 1 + 25 + 1 + 25 + 1;
   char* ret = NULL;

   if( local ) {
      // local
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + base_len );
      sprintf(ret, "%s%s%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, volume_id, fs_path, file_version, block_id, block_version );
   }
   else {
      // remote data block
      ret = CALLOC_LIST( char, strlen(base_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_version, block_id, block_version );
   }

   return ret;  
}


char* fs_entry_local_block_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // file:// URL to a locally-hosted block in a locally-coordinated file
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_block_url( core, core->volume, NULL, fs_path, file_version, block_id, block_version, true );
   free( fs_path );
   return ret;
}


char* fs_entry_public_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a locally-hosted block in a locally-coordinated file
   return fs_entry_block_url( core, core->volume, core->conf->content_url, fs_path, file_version, block_id, block_version, false );
}


char* fs_entry_remote_block_url( struct fs_core* core, uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a remotely-hosted block
   char* content_url = ms_client_get_UG_content_url( core->ms, gateway_id );
   if( content_url == NULL )
      return NULL;

   char* ret = fs_entry_block_url( core, core->volume, content_url, fs_path, file_version, block_id, block_version, false );
   free( content_url );
   return ret;
}


char* fs_entry_replica_block_url( struct fs_core* core, char* RG_url, uint64_t volume_id, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a remotely-hosted block on an RG
   char* url = CALLOC_LIST( char, strlen(RG_url) + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 );
   sprintf( url, "%s%s/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64, RG_url, SYNDICATE_DATA_PREFIX, volume_id, file_id, file_version, block_id, block_version );
   return url;
   
}

char* fs_entry_block_url_path( struct fs_core* core, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* url_path = CALLOC_LIST( char, 105 + strlen(fs_path) );
   sprintf(url_path, "/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64, core->volume, fs_path, version, block_id, block_version );
   return url_path;
}

char* fs_entry_AG_block_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* base_url = ms_client_get_AG_content_url( core->ms, ag_id );
   if( base_url == NULL )
      return NULL;
   
   uint64_t volume_id = 0;
   int rc = ms_client_get_gateway_volume( core->ms, SYNDICATE_AG, ag_id, &volume_id );
   if( rc != 0 ) {
      errorf("ms_client_gat_gateway_volume(%" PRIu64 ") rc = %d\n", ag_id, rc );
      return NULL;
   }
   
   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 1 + 25 + 1 + 25 + 1 + 25 + 1;
   char* ret = CALLOC_LIST( char, strlen(base_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );

   sprintf(ret, "%s%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64, base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, version, block_id, block_version );

   free( base_url );
   return ret;
}

char* fs_entry_RG_block_url( struct fs_core* core, uint64_t rg_id, uint64_t volume_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* base_url = ms_client_get_RG_content_url( core->ms, rg_id );
   if( base_url == NULL )
      return NULL;
   
   char* ret = fs_entry_replica_block_url( core, base_url, volume_id, file_id, version, block_id, block_version );
   free( base_url );
   return ret;
}

char* fs_entry_file_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, bool local ) {
   
   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 25 + 1 + 1;
   char* ret = NULL;

   if( local ) {
      // local block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + base_len );
      sprintf(ret, "%s%s/%" PRIu64 "%s.%" PRId64,
              SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, volume_id, fs_path, file_version );
   }
   else {
      // remote data block
      ret = CALLOC_LIST( char, strlen(core->conf->content_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s%s/%" PRIu64 "%s.%" PRId64,
              base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_version );
   }
   
   return ret;
}


char* fs_entry_local_file_url( struct fs_core* core, uint64_t file_id, int64_t file_version) {
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_file_url( core, core->volume, NULL, fs_path, file_version, true );
   free( fs_path );
   return ret;
}

char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path, int64_t file_version ) {
   char* ret = fs_entry_file_url( core, core->volume, core->conf->content_url, fs_path, file_version, false );
   return ret;
}

char* fs_entry_manifest_url( struct fs_core* core, char const* gateway_base_url, uint64_t volume_id, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* ret = CALLOC_LIST( char, strlen(SYNDICATE_DATA_PREFIX) + 1 + strlen(gateway_base_url) + 1 + strlen(fs_path) + 1 + 82 );
   sprintf( ret, "%s%s/%" PRIu64 "%s.%" PRId64 "/manifest.%ld.%ld", gateway_base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, version, (long)ts->tv_sec, (long)ts->tv_nsec );
   return ret;
}

char* fs_entry_public_manifest_url( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* ret = fs_entry_manifest_url( core, core->conf->content_url, core->volume, fs_path, version, ts );
   return ret;
}


char* fs_entry_remote_manifest_url( struct fs_core* core, uint64_t UG_id, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* content_url = ms_client_get_UG_content_url( core->ms, UG_id );
   if( content_url == NULL )
      return NULL;
   
   char* ret = fs_entry_manifest_url( core, content_url, core->volume, fs_path, version, ts );
   free( content_url );
   return ret;
}

char* fs_entry_replica_manifest_url( struct fs_core* core, char const* base_url, uint64_t volume_id, uint64_t file_id, int64_t version, struct timespec* ts ) {
   char* url = CALLOC_LIST( char, strlen(base_url) + 1 + 21 + 1 + 21 + 1 + 21 + 21 + 1 + strlen("manifest") + 21 + 1 + 21 );
   sprintf( url, "%s%s/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld", base_url, SYNDICATE_DATA_PREFIX, volume_id, file_id, version, (long)ts->tv_sec, (long)ts->tv_nsec );
   return url;
}

char* fs_entry_RG_manifest_url( struct fs_core* core, uint64_t rg_id, uint64_t volume_id, uint64_t file_id, int64_t file_version, struct timespec* ts ) {
   char* base_url = ms_client_get_RG_content_url( core->ms, rg_id );
   if( base_url == NULL )
      return NULL;
   
   char* ret = fs_entry_replica_manifest_url( core, base_url, volume_id, file_id, file_version, ts );
   free( base_url );
   return ret;
}

char* fs_entry_AG_manifest_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, int64_t file_version, struct timespec* ts ) {
   char* base_url = ms_client_get_AG_content_url( core->ms, ag_id );
   if( base_url == NULL ) {
      return NULL;
   }
   
   uint64_t volume_id = 0;
   int rc = ms_client_get_gateway_volume( core->ms, SYNDICATE_AG, ag_id, &volume_id );
   if( rc != 0 ) {
      errorf("ms_client_gat_gateway_volume(%" PRIu64 ") rc = %d\n", ag_id, rc );
      return NULL;
   }
   
   char* ret = CALLOC_LIST( char, strlen(SYNDICATE_DATA_PREFIX) + 1 + strlen(base_url) + 1 + strlen(fs_path) + 1 + 82 );
   sprintf( ret, "%s%s/%" PRIu64 "%s.%" PRId64 "/manifest.%ld.%ld", base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_version, (long)ts->tv_sec, (long)ts->tv_nsec );
   
   free( base_url );
   return ret;
}

char* fs_entry_manifest_url_path( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* url_path = CALLOC_LIST( char, 105 + strlen(fs_path) );
   sprintf(url_path, "/%" PRIu64 "%s.%" PRId64 "/manifest.%ld.%ld", core->volume, fs_path, version, (long)ts->tv_sec, (long)ts->tv_nsec );
   return url_path;
}
