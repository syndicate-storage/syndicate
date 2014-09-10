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

#include "libsyndicate/url.h"
#include "libsyndicate/ms/ms-client.h"

// split a uint64 into four uint16s.
// assume i is litte-endian; otherwise convert it
static void md_url_split_uint64( uint64_t i, uint16_t* o ) {
   if( htonl( 1234 ) == 1234 ) {
      // i is big endian...
      i = htole64( i );
   }
   o[0] = (i & (uint64_t)0xFFFF000000000000LL) >> 48;
   o[1] = (i & (uint64_t)0x0000FFFF00000000LL) >> 32;
   o[2] = (i & (uint64_t)0x00000000FFFF0000LL) >> 16;
   o[3] = (i & (uint64_t)0x000000000000FFFFLL);
}

static char* md_url_path_from_file_id( uint64_t file_id ) {
   uint16_t file_id_parts[4];
   md_url_split_uint64( file_id, file_id_parts );

   char* ret = CALLOC_LIST( char, 21 );
   sprintf(ret, "/%04X/%04X/%04X/%04X", file_id_parts[0], file_id_parts[1], file_id_parts[2], file_id_parts[3] );
   return ret;
}

// generate a block url, based on whether or not it is locally coordinated
// if local is true, then prefix should be the path on disk
// if local is false, then prefix should be the content url
static char* md_url_block_url( char const* prefix, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, bool local ) {

   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 1 + 25 + 1 + 25 + 1 + 25 + 1 + 25 + 1;
   char* ret = NULL;

   if( local ) {
      // local
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(prefix) + 1 + base_len );
      sprintf(ret, "%s%s%" PRIu64 "%s.%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64,
            SYNDICATEFS_LOCAL_PROTO, prefix, volume_id, fs_path, file_id, file_version, block_id, block_version );
   }
   else {
      // remote data block
      ret = CALLOC_LIST( char, strlen(prefix) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64,
            prefix, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_id, file_version, block_id, block_version );
   }

   return ret;  
}


// generate a locally-resolvable URL to a block in this UG
char* md_url_local_block_url( char const* data_root, uint64_t volume_id, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // file:// URL to a locally-hosted block in a locally-coordinated file
   char* fs_path = md_url_path_from_file_id( file_id );
   char* ret = md_url_block_url( data_root, volume_id, fs_path, file_id, file_version, block_id, block_version, true );
   free( fs_path );
   return ret;
}


// generate a publicly-resolvable URL to a block in this UG
char* md_url_public_block_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a locally-hosted block in a locally-coordinated file
   return md_url_block_url( base_url, volume_id, fs_path, file_id, file_version, block_id, block_version, false );
}


// generate a publicly-resolvable URL to a block in a UG
char* md_url_UG_block_url( struct ms_client* ms, uint64_t UG_id, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a remotely-hosted block
   uint64_t volume_id = ms_client_get_volume_id( ms );
   char* base_url = ms_client_get_UG_content_url( ms, UG_id );
   if( base_url == NULL )
      return NULL;

   char* ret = md_url_block_url( base_url, volume_id, fs_path, file_id, file_version, block_id, block_version, false );
   free( base_url );
   return ret;
}


// generaate a publicly-resolvable URL to a block in an AG
char* md_url_AG_block_url( struct ms_client* ms, uint64_t ag_id, char const* fs_path, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version ) {
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   char* base_url = ms_client_get_AG_content_url( ms, ag_id );
   if( base_url == NULL )
      return NULL;
   
   char* ret = md_url_block_url( base_url, volume_id, fs_path, file_id, version, block_id, block_version, false );
   
   free( base_url );
   return ret;
}

// generate a publicly-resolvable URL to a block in an RG
char* md_url_RG_block_url( struct ms_client* ms, uint64_t rg_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version ) {
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   char* base_url = ms_client_get_RG_content_url( ms, rg_id );
   if( base_url == NULL )
      return NULL;
   
   char* url = CALLOC_LIST( char, strlen(base_url) + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 + 21 + 1 );
   sprintf( url, "%s%s/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64, base_url, SYNDICATE_DATA_PREFIX, volume_id, file_id, version, block_id, block_version );
   
   free( base_url );
   return url;
}


// generate a block URL, based on what gateway hosts it.
// return -ENOENT if the gateway is currently unknown.
int md_url_make_block_url( struct ms_client* ms, char const* fs_path, uint64_t coordinator_id, uint64_t file_id, int64_t version, uint64_t block_id, int64_t block_version, char** url ) {
   
   int gateway_type = ms_client_get_gateway_type( ms, coordinator_id );
   
   if( gateway_type < 0 ) {
      // unknown gateway---maybe try reloading the certs?
      errorf("Unknown gateway %" PRIu64 "\n", coordinator_id );
      return -ENOENT;
   }
   
   char* block_url = NULL;
   
   if( gateway_type == SYNDICATE_UG ) {
      block_url = md_url_UG_block_url( ms, coordinator_id, fs_path, file_id, version, block_id, block_version );
   }
   else if( gateway_type == SYNDICATE_RG ) {
      block_url = md_url_RG_block_url( ms, coordinator_id, file_id, version, block_id, block_version );
   }
   else if( gateway_type == SYNDICATE_AG ) {
      block_url = md_url_AG_block_url( ms, coordinator_id, fs_path, file_id, version, block_id, block_version );
   }
   
   if( block_url == NULL ) {
      errorf("Failed to compute block URL for Gateway %" PRIu64 " (type %d)\n", coordinator_id, gateway_type);
      return -ENOENT;
   }

   *url = block_url;
   return 0;
}


// generate a URL to a file, either locally available or remotely available
// if local is true, then prefix should be the data root path
// if local is false, then prefix should be the base URL
char* md_url_file_url( char const* prefix, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version, bool local ) {
   
   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 25 + 1 + 25 + 1 + 1;
   char* ret = NULL;

   if( local ) {
      // local block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(prefix) + 1 + base_len );
      sprintf(ret, "%s%s/%" PRIu64 "%s.%" PRIX64 ".%" PRId64,
              SYNDICATEFS_LOCAL_PROTO, prefix, volume_id, fs_path, file_id, file_version );
   }
   else {
      // remote data block
      ret = CALLOC_LIST( char, strlen(prefix) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s%s/%" PRIu64 "%s.%" PRIX64 ".%" PRId64,
              prefix, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_id, file_version );
   }
   
   return ret;
}


// generate a locally-resolvable URL to a file on this gateway
char* md_url_local_file_url( char const* data_root, uint64_t volume_id, uint64_t file_id, int64_t file_version ) {
   
   char* fs_path = md_url_path_from_file_id( file_id );
   
   char* ret = md_url_file_url( data_root, volume_id, fs_path, file_id, file_version, true );
   
   free( fs_path );
   return ret;
}

// generate a publicly-resolvable URL to a file on this gateway
char* md_url_public_file_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t file_version ) {
   
   char* ret = md_url_file_url( base_url, volume_id, fs_path, file_id, file_version, false );
   return ret;
}

// manifest URL generator
char* md_url_public_manifest_url( char const* base_url, uint64_t volume_id, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts ) {
   char* ret = CALLOC_LIST( char, strlen(SYNDICATE_DATA_PREFIX) + 1 + strlen(base_url) + 1 + strlen(fs_path) + 1 + 107 );
   sprintf( ret, "%s%s/%" PRIu64 "%s.%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld", base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_id, version, (long)ts->tv_sec, (long)ts->tv_nsec );
   return ret;
}

// generate a URL to a UG's manifest
char* md_url_UG_manifest_url( struct ms_client* ms, uint64_t UG_id, char const* fs_path, uint64_t file_id, int64_t version, struct timespec* ts ) {
   char* base_url = ms_client_get_UG_content_url( ms, UG_id );
   if( base_url == NULL ) {
      return NULL;
   }
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   char* ret = md_url_public_manifest_url( base_url, volume_id, fs_path, file_id, version, ts );
   free( base_url );
   return ret;
}

// generate a URL to an RG's manifest
char* md_url_RG_manifest_url( struct ms_client* ms, uint64_t rg_id, uint64_t file_id, int64_t file_version, struct timespec* ts ) {
   char* base_url = ms_client_get_RG_content_url( ms, rg_id );
   if( base_url == NULL ) {
      return NULL;
   }
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   char* url = CALLOC_LIST( char, strlen(base_url) + 1 + 25 + 1 + 25 + 1 + 25 + 25 + 1 + strlen("manifest") + 25 + 1 + 25 );
   sprintf( url, "%s%s/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/manifest.%ld.%ld", base_url, SYNDICATE_DATA_PREFIX, volume_id, file_id, file_version, (long)ts->tv_sec, (long)ts->tv_nsec );
   
   free( base_url );
   return url;
}

// generate a URL to an AG's manifest
char* md_url_AG_manifest_url( struct ms_client* ms, uint64_t ag_id, char const* fs_path, uint64_t file_id, int64_t file_version, struct timespec* ts ) {
   char* base_url = ms_client_get_AG_content_url( ms, ag_id );
   if( base_url == NULL ) {
      return NULL;
   }
   
   uint64_t volume_id = ms_client_get_volume_id( ms );
   
   char* ret = md_url_public_manifest_url( base_url, volume_id, fs_path, file_id, file_version, ts );
   
   free( base_url );
   return ret;
}


// generate a URL to an fs_entry's manifest, given its coordinator.  Automatically determine what kind of gateway hosts it.
int md_url_make_manifest_url( struct ms_client* ms, char const* fs_path, uint64_t coordinator_id, uint64_t file_id, int64_t file_version, struct timespec* ts, char** url ) {
   
   // what kind of gateway?
   int gateway_type = ms_client_get_gateway_type( ms, coordinator_id );

   if( gateway_type < 0 ) {
      // unknown gateway
      errorf("Unknown Gateway %" PRIu64 "\n", coordinator_id );
      return -ENOENT;
   }
   
   char* manifest_url = NULL;
   
   if( gateway_type == SYNDICATE_UG ) {
      manifest_url = md_url_UG_manifest_url( ms, coordinator_id, fs_path, file_id, file_version, ts );
   }
   else if( gateway_type == SYNDICATE_RG ) {
      manifest_url = md_url_RG_manifest_url( ms, coordinator_id, file_id, file_version, ts );
   }
   else if( gateway_type == SYNDICATE_AG ) {
      manifest_url = md_url_AG_manifest_url( ms, coordinator_id, fs_path, file_id, file_version, ts );
   }
   
   if( manifest_url == NULL ) {
      // gateway not found 
      errorf("Unknown Gateway %" PRIu64 " of type %d\n", coordinator_id, gateway_type );
      return -ENOENT;
   }
   
   *url = manifest_url;
   return 0;
}
