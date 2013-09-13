/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
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
   o[0] = (i & 0xFFFF000000000000) >> 48;
   o[1] = (i & 0x0000FFFF00000000) >> 32;
   o[2] = (i & 0x00000000FFFF0000) >> 16;
   o[3] = (i & 0x000000000000FFFF);
}

char* fs_entry_path_from_file_id( uint64_t file_id ) {
   uint16_t file_id_parts[4];
   fs_entry_split_uint64( file_id, file_id_parts );

   char* ret = CALLOC_LIST( char, 21 );
   sprintf(ret, "/%X/%X/%X/%X", file_id_parts[0], file_id_parts[1], file_id_parts[2], file_id_parts[3] );
   return ret;
}


char* fs_entry_block_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, bool local, bool staging ) {

   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 1 + 25 + 1 + 25 + 1 + 25 + 1;
   char* ret = NULL;

   if( local && staging ) {
      // local staging block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->staging_root) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            SYNDICATEFS_LOCAL_PROTO, core->conf->staging_root, volume_id, fs_path, file_version, block_id, block_version );
   }
   else if( local && !staging ) {
      // local, not-staging block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, volume_id, fs_path, file_version, block_id, block_version );
   }
   else if( !local && !staging ) {
      // remote data block
      ret = CALLOC_LIST( char, strlen(base_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_version, block_id, block_version );
   }
   else if( !local && staging ) {
      // remote staging block
      ret = CALLOC_LIST( char, strlen(base_url) + 1 + strlen(SYNDICATE_STAGING_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64,
            base_url, SYNDICATE_STAGING_PREFIX, volume_id, fs_path, file_version, block_id, block_version );
   }

   return ret;  
}


char* fs_entry_local_block_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // file:// URL to a locally-hosted block in a locally-coordinated file
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_block_url( core, core->volume, NULL, fs_path, file_version, block_id, block_version, true, false );
   free( fs_path );
   return ret;
}

char* fs_entry_local_staging_block_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // file:// URL to a locally-hosted block in a remotely-coordinated file
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_block_url( core, core->volume, NULL, fs_path, file_version, block_id, block_version, true, true );
   free( fs_path );
   return ret;
}

char* fs_entry_public_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a locally-hosted block in a locally-coordinated file
   return fs_entry_block_url( core, core->volume, core->conf->content_url, fs_path, file_version, block_id, block_version, false, false );
}

char* fs_entry_public_staging_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a locally-hosted block in a remotely-coordinated file
   return fs_entry_block_url( core, core->volume, core->conf->content_url, fs_path, file_version, block_id, block_version, false, true );
}

char* fs_entry_remote_block_url( struct fs_core* core, uint64_t gateway_id, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a remotely-hosted block
   char* content_url = ms_client_get_UG_content_url( core->ms, core->volume, gateway_id );
   if( content_url == NULL )
      return NULL;

   char* ret = fs_entry_block_url( core, core->volume, content_url, fs_path, file_version, block_id, block_version, false, false );
   free( content_url );
   return ret;
}


char* fs_entry_replica_block_url( struct fs_core* core, char* RG_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   // http:// URL to a remotely-hosted block on an RG
   return fs_entry_block_url( core, core->volume, RG_url, fs_path, file_version, block_id, block_version, false, false );
}

char* fs_entry_block_url_path( struct fs_core* core, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* url_path = CALLOC_LIST( char, 105 + strlen(fs_path) );
   sprintf(url_path, "/%" PRIu64 "%s.%" PRId64 "/%" PRIu64 ".%" PRId64, core->volume, fs_path, version, block_id, block_version );
   return url_path;
}

char* fs_entry_AG_block_url( struct fs_core* core, uint64_t ag_id, char const* fs_path, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* base_url = ms_client_get_AG_content_url( core->ms, core->volume, ag_id );
                                                  
   int base_len = 25 + 1 + strlen(fs_path) + 1 + 25 + 1 + 25 + 1 + 25 + 1;
   
   char* ret = CALLOC_LIST( char, strlen(base_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );

   sprintf(ret, "%s/%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, base_url, SYNDICATE_DATA_PREFIX, fs_path, version, block_id, block_version );

   free( base_url );
   return ret;
}



char* fs_entry_file_url( struct fs_core* core, uint64_t volume_id, char const* base_url, char const* fs_path, int64_t file_version, bool local, bool staging ) {
   
   int base_len = 25 + 1 + 25 + 1 + strlen(fs_path) + 25 + 1 + 1;
   char* ret = NULL;

   if( local && staging ) {
      // local staging block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->staging_root) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64,
              SYNDICATEFS_LOCAL_PROTO, core->conf->staging_root, volume_id, fs_path, file_version );
   }
   else if( local && !staging ) {
      // local, not-staging block
      ret = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64,
              SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, volume_id, fs_path, file_version );
   }
   else if( !local && !staging ) {
      // remote data block
      ret = CALLOC_LIST( char, strlen(core->conf->content_url) + 1 + strlen(SYNDICATE_DATA_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64,
              base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, file_version );
   }
   else if( !local && staging ) {
      // remote staging block
      ret = CALLOC_LIST( char, strlen(core->conf->content_url) + 1 + strlen(SYNDICATE_STAGING_PREFIX) + 1 + base_len );
      sprintf(ret, "%s/%s/%" PRIu64 "%s.%" PRId64,
              base_url, SYNDICATE_STAGING_PREFIX, volume_id, fs_path, file_version );
   }

   return ret;
}


char* fs_entry_local_file_url( struct fs_core* core, uint64_t file_id, int64_t file_version) {
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_file_url( core, core->volume, NULL, fs_path, file_version, true, false );
   free( fs_path );
   return ret;
}

char* fs_entry_local_staging_file_url( struct fs_core* core, uint64_t file_id, int64_t file_version ) {
   char* fs_path = fs_entry_path_from_file_id( file_id );
   char* ret = fs_entry_file_url( core, core->volume, NULL, fs_path, file_version, true, true );
   free( fs_path );
   return ret;
}

char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path, int64_t file_version ) {
   char* ret = fs_entry_file_url( core, core->volume, core->conf->content_url, fs_path, file_version, false, false );
   return ret;
}

char* fs_entry_public_staging_file_url( struct fs_core* core, char const* fs_path, int64_t file_version ) {
   char* ret = fs_entry_file_url( core, core->volume, core->conf->content_url, fs_path, file_version, false, true );
   return ret;
}



char* fs_entry_manifest_url( struct fs_core* core, char const* gateway_base_url, uint64_t volume_id, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* ret = CALLOC_LIST( char, strlen(SYNDICATE_DATA_PREFIX) + 1 + strlen(gateway_base_url) + 1 + strlen(fs_path) + 1 + 82 );
   sprintf( ret, "%s/%s/%" PRIu64 "%s.%" PRId64 "/manifest.%ld.%ld", gateway_base_url, SYNDICATE_DATA_PREFIX, volume_id, fs_path, version, ts->tv_sec, ts->tv_nsec );
   return ret;
}

char* fs_entry_public_manifest_url( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* ret = fs_entry_manifest_url( core, core->conf->content_url, core->volume, fs_path, version, ts );
   return ret;
}


char* fs_entry_remote_manifest_url( struct fs_core* core, uint64_t UG_id, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* content_url = ms_client_get_UG_content_url( core->ms, core->volume, UG_id );
   if( content_url == NULL )
      return NULL;
   
   char* ret = fs_entry_manifest_url( core, content_url, core->volume, fs_path, version, ts );
   free( content_url );
   return ret;
}

char* fs_entry_replica_manifest_url( struct fs_core* core, char const* RG_url, char const* fs_path, int64_t version, struct timespec* ts ) {
   return fs_entry_manifest_url( core, RG_url, core->volume, fs_path, version, ts );
}

char* fs_entry_manifest_url_path( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts ) {
   char* url_path = CALLOC_LIST( char, 105 + strlen(fs_path) );
   sprintf(url_path, "/%" PRIu64 "%s.%" PRId64 "/manifest.%ld.%ld", core->volume, fs_path, version, ts->tv_sec, ts->tv_nsec );
   return url_path;
}
