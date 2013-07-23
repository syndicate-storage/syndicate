/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "url.h"
#include "manifest.h"

char* fs_entry_generic_url( char const* content_url, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   size_t len = strlen(content_url) + 66;

   char* ret = CALLOC_LIST( char, len );

   sprintf(ret, "%s.%" PRId64 "/%" PRIu64 ".%" PRId64, content_url, file_version, block_id, block_version );

   return ret;
}


// given a string and a version, concatenate them, preserving delimiters
char* fs_entry_mkpath( char const* fs_path, int64_t version ) {
   size_t t = strlen(fs_path);
   size_t v = 2 + log(abs(version) + 1);
   char* ret = CALLOC_LIST( char, t + 1 + v );

   bool delim = false;
   if( fs_path[t-1] == '/' ) {
      strncpy( ret, fs_path, t-1 );
      delim = true;
   }
   else {
      strncpy( ret, fs_path, t );
   }

   char buf[50];

   if( delim )
      sprintf(buf, ".%" PRId64 "/", version );
   else
      sprintf(buf, ".%" PRId64, version );

   strcat( ret, buf );

   return ret;
}


// given a file handle, get the path that can be appended to a hostname to form a URL
char* fs_entry_url_path( struct fs_file_handle* fh ) {
   fs_entry_rlock( fh->fent );
   char* ret = fs_entry_mkpath( fh->path, fh->fent->version );
   fs_entry_unlock( fh->fent );
   return ret;
}

char* fs_entry_url_path( char const* fs_path, int64_t version ) {
   char* ret = fs_entry_mkpath( fs_path, version );
   return ret;
}


// convert a local file url to a public file url.
// if file_version is >= 0, then it will be appended to the URL.
char* fs_entry_local_to_public( struct fs_core* core, char const* file_url, int64_t file_version ) {
   char const* fs_path = file_url + strlen(SYNDICATEFS_LOCAL_PROTO) + strlen(core->conf->data_root);
   char* ret = CALLOC_LIST( char, strlen(core->conf->content_url) + strlen(SYNDICATE_DATA_PREFIX) + strlen(fs_path) + 32 );

   char* tmp = md_fullpath( core->conf->content_url, SYNDICATE_DATA_PREFIX, NULL );
   
   md_prepend( tmp, fs_path, ret );

   // add the file version, if needed
   if( file_version >= 0 ) {
      char buf[30];
      sprintf(buf, ".%" PRId64, file_version );
      strcat( ret, buf );
   }
   
   free( tmp );
   
   return ret;
}

// convert a local file url to a public file url.
// do not include the version.
char* fs_entry_local_to_public( struct fs_core* core, char const* file_url ) {
   return fs_entry_local_to_public( core, file_url, -1 );
}

// given an offset and a file URL or path, calculate the URL at which the block can/should be found
// NEED TO LOCK FILE HANDLE FIRST!
char* fs_entry_remote_block_url( char const* content_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {

   char* path = CALLOC_LIST( char, strlen(content_url) + strlen(SYNDICATE_DATA_PREFIX) + strlen(fs_path) + 65 );

   if( block_version >= 0 )
      sprintf( path, "%s%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, content_url, SYNDICATE_DATA_PREFIX, fs_path, file_version, block_id, block_version );
   else
      sprintf( path, "%s%s%s.%" PRId64 "/%" PRIu64, content_url, SYNDICATE_DATA_PREFIX, fs_path, file_version, block_id );

   return path;
}


// get the block's path
char* fs_entry_local_block_path( char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* path = CALLOC_LIST( char, strlen(data_root) + 1 + strlen(fs_path) + 65 );
   sprintf( path, "%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, data_root, fs_path, file_version, block_id, block_version );
   return path;
}

// given an offset and a file path, calculate the local URL at which the block can/should be found.
char* fs_entry_local_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* path = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + strlen(fs_path) + 50 );
   sprintf(path, "%s%s/%s.%" PRId64 "/%" PRIu64 ".%" PRId64, SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, fs_path, file_version, block_id, block_version );
   return path;
}

// given an offset and a file path, calculate the staging URL at which the block should be found
char* fs_entry_local_staging_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* path = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->staging_root) + 1 + strlen(fs_path) + 50 );
   sprintf(path, "%s%s/%s.%" PRId64 "/%" PRIu64 ".%" PRId64, SYNDICATEFS_LOCAL_PROTO, core->conf->staging_root, fs_path, file_version, block_id, block_version );
   return path;
}

// given a file path, calculate the local file URL
char* fs_entry_local_file_url( struct fs_core* core, char const* fs_path, int64_t file_version ) {
   char* local_url = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + strlen(fs_path) + 21 );
   sprintf( local_url, "%s%s/%s.%" PRId64, SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, fs_path, file_version );
   return local_url;
}

// given a file path, calculate the local file URL, without the version
char* fs_entry_local_file_url( struct fs_core* core, char const* fs_path ) {
   char* local_url = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->data_root) + 1 + strlen(fs_path) + 1 );
   sprintf( local_url, "%s%s/%s", SYNDICATEFS_LOCAL_PROTO, core->conf->data_root, fs_path );
   return local_url;
}

// given a file path, calculate the staging file URL
// NEED TO LOCK path HANDLE FIRST!
char* fs_entry_local_staging_file_url( struct fs_core* core, char const* fs_path ) {
   char* staging_url = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->staging_root) + 1 + strlen(fs_path) + 1 );
   sprintf( staging_url, "%s%s/%s", SYNDICATEFS_LOCAL_PROTO, core->conf->staging_root, fs_path );
   return staging_url;
}

// given a file path, calculate the staging file URL
// NEED TO LOCK path HANDLE FIRST!
char* fs_entry_local_staging_file_url( struct fs_core* core, char const* fs_path, int64_t file_version ) {
   char* staging_url = CALLOC_LIST( char, strlen(SYNDICATEFS_LOCAL_PROTO) + 1 + strlen(core->conf->staging_root) + 1 + strlen(fs_path) + 21 );
   sprintf( staging_url, "%s%s/%s.%" PRId64, SYNDICATEFS_LOCAL_PROTO, core->conf->staging_root, fs_path, file_version );
   return staging_url;
}

// given a file path and version stuff, calculate the local path on disk where a staging block can be found
char* fs_entry_staging_block_path( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* staging_path = CALLOC_LIST( char, strlen(core->conf->staging_root) + 1 + strlen(fs_path) + 1 + 47 );
   sprintf( staging_path, "%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, core->conf->staging_root, fs_path, file_version, block_id, block_version );
   return staging_path;
}

// staging block URL on a remoete host
char* fs_entry_public_staging_block_url( char const* host_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   char* staging_url = CALLOC_LIST( char, strlen(host_url) + 1 + strlen(SYNDICATE_STAGING_PREFIX) + 1 + strlen(fs_path) + 1 + 47 );
   sprintf( staging_url, "%s%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, host_url, SYNDICATE_STAGING_PREFIX, fs_path, file_version, block_id, block_version );
   return staging_url;
}

// given a content url, file path, and version, make a public URL for a file
char* fs_entry_public_file_url( char const* content_url, char const* fs_path, int64_t version ) {

   size_t len = strlen( content_url ) + strlen( SYNDICATE_DATA_PREFIX ) + strlen(fs_path) + 1;
   if( version >= 0 )
      len += 21;

   char* ret = CALLOC_LIST( char, len );

   if( version >= 0 )
      sprintf( ret, "%s%s%s.%" PRId64, content_url, SYNDICATE_DATA_PREFIX, fs_path, version );
   else
      sprintf( ret, "%s%s%s", content_url, SYNDICATE_DATA_PREFIX, fs_path );

   return ret;
}


// given a file path, calculate the public URL for a file (including its version)
char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path, int64_t version ) {
   return fs_entry_public_file_url( core->conf->content_url, fs_path, version );
}

char* fs_entry_public_file_url( struct fs_core* core, char const* fs_path ) {
   return fs_entry_public_file_url( core->conf->content_url, fs_path, -1 );
}

char* fs_entry_public_file_url( char const* content_url, char const* fs_path ) {
   return fs_entry_public_file_url( content_url, fs_path, -1 );
}

// given a dir path, calculate the public directory URL on the metadata server
char* fs_entry_public_dir_url( struct fs_core* core, char const* fs_path ) {
   return md_fullpath( core->conf->metadata_url, fs_path, NULL );
}

// given a public dir url, get back its path 
char* fs_entry_dir_path_from_public_url( struct fs_core* core, char const* dir_url ) {
   size_t md_read_url_len = strlen(core->conf->metadata_url);
   
   if( md_read_url_len > strlen(dir_url) )
      return NULL;
   
   char const* start = dir_url + md_read_url_len;
   if( start[0] != '/' )
      start--;

   return strdup(start);
}

// given a content url, file path, version, block ID, and block version, make a public URL for the block
char* fs_entry_public_block_url( char const* content_url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   size_t len = strlen( content_url ) + strlen(SYNDICATE_DATA_PREFIX) + strlen(fs_path) + 1 + 21;
   if( file_version >= 0 )
      len += 21;

   if( block_version >= 0 )
      len += 21;

   char* ret = CALLOC_LIST( char, len );

   if( file_version >= 0 && block_version >= 0 ) {
      sprintf( ret, "%s%s%s.%" PRId64 "/%" PRIu64 ".%" PRId64, content_url, SYNDICATE_DATA_PREFIX, fs_path, file_version, block_id, block_version );
   }
   else if( file_version >= 0 && block_version < 0 ) {
      sprintf( ret, "%s%s%s.%" PRId64 "/%" PRIu64, content_url, SYNDICATE_DATA_PREFIX, fs_path, file_version, block_id );
   }
   else if( file_version < 0 && block_version >= 0 ) {
      sprintf( ret, "%s%s%s/%" PRIu64 ".%" PRId64, content_url, SYNDICATE_DATA_PREFIX, fs_path, block_id, block_version );
   }
   else {
      sprintf( ret, "%s%s%s/%" PRIu64, content_url, SYNDICATE_DATA_PREFIX, fs_path, block_id );
   }

   return ret;
}

// given a file path, version, block ID and block version, calculate the public URL for that block
char* fs_entry_public_block_url( struct fs_core* core, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   return fs_entry_public_block_url( core->conf->content_url, fs_path, file_version, block_id, block_version );
}

// calculate the manifest url
char* fs_entry_manifest_url( char const* host_url, char const* fs_path, int64_t version, struct timespec* ts ) {
   size_t len = strlen( host_url ) + strlen(SYNDICATE_DATA_PREFIX) + strlen(fs_path) + 5 + strlen("manifest");
   if( version >= 0 )
      len += 21;
   if( ts->tv_sec > 0 )
      len += 43;

   char* ret = CALLOC_LIST( char, len );
   if( version >= 0 )
      sprintf( ret, "%s%s%s.%" PRId64 "/manifest.%ld.%ld", host_url, SYNDICATE_DATA_PREFIX, fs_path, version, ts->tv_sec, ts->tv_nsec );
   else
      sprintf( ret, "%s%s%s/manifest.%ld.%ld", host_url, SYNDICATE_DATA_PREFIX, fs_path, ts->tv_sec, ts->tv_nsec );

   return ret;
}

// given a file path and version, get a URL to its manifest
char* fs_entry_public_manifest_url( struct fs_core* core, char const* fs_path, int64_t version, struct timespec* ts ) {
   return fs_entry_manifest_url( core->conf->content_url, fs_path, version, ts );
}

// calcualte the url path of a manifest
char* fs_entry_manifest_path( char const* fs_path, char const* file_url, int64_t file_version, uint64_t mtime_sec, uint32_t mtime_nsec ) {
   char* fs_fullpath = CALLOC_LIST( char, strlen(fs_path) + strlen("manifest") + 50 );
   sprintf( fs_fullpath, "%s.%" PRId64 "/manifest.%" PRIu64 ".%u", fs_path, file_version, mtime_sec, mtime_nsec );
   return fs_fullpath;
}


// calculate the remote manifest URL
char* fs_entry_remote_manifest_url( char const* fs_path, char const* file_url, int64_t version, struct timespec* ts ) {
   char* hostname = md_url_hostname( file_url );
   int portnum = md_portnum_from_url( file_url );

   char* host_url = md_prepend( "http://", hostname, NULL );
   if( portnum > 0 ) {
      char buf[10];
      sprintf(buf, ":%d", portnum);

      char* tmp = host_url;
      host_url = md_prepend( host_url, buf, NULL );
      free( tmp );
   }
   free( hostname );

   char* ret = fs_entry_manifest_url( host_url, fs_path, version, ts );
   free( host_url );
   return ret;
}


// calculate a URL for a block
// fent must be read-locked at least
static char* fs_entry_calculate_block_url( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t offset ) {

   uint64_t block_id = fs_entry_block_id( offset, core->conf );
   
   if( URL_LOCAL( fent->url ) ) {
      // this file is locally-hosted
      return fs_entry_local_block_url( core, fs_path, fent->version, block_id, fent->manifest->get_block_version( block_id ) );
   }
   else {
      // this file is remotely-hosted
      fs_entry_rlock( fent );

      // strip the version number from the file URL
      char* content_url = strdup( fent->url );
      char* tmp = md_url_strip_path( content_url );
      free( content_url );
      content_url = tmp;

      // generate the remote URL
      char* ret = fs_entry_remote_block_url( content_url, fs_path, fent->version, block_id, fent->manifest->get_block_version( block_id ) );

      free( content_url );

      fs_entry_unlock( fent );
      return ret;
   }
}


// get a block url, without a file handle
char* fs_entry_get_block_url( struct fs_core* core, char const* fs_path, uint64_t block_id ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      return NULL;
   }

   off_t offset = block_id * core->conf->blocking_factor + 1;
   char* block_url = fent->manifest->get_block_url( fent->version, block_id );

   char* ret = NULL;
   if( block_url == NULL ) {
      ret = fs_entry_calculate_block_url( core, fs_path, fent, offset );
   }
   else {
      // we have a URL for this block
      ret = block_url;
   }

   fs_entry_unlock( fent );
   return ret;
}


// given an offset and a file handle, retrieve or generate the URL at which the block can/should be found.
// NEED TO LOCK FILE HANDLE FIRST!
char* fs_entry_get_block_url( struct fs_core* core, struct fs_file_handle* fh, off_t offset ) {

   // block number
   uint64_t block_id = fs_entry_block_id( offset, core->conf );
   char* block_url = fh->fent->manifest->get_block_url( fh->fent->version, block_id );

   if( block_url == NULL ) {
      return fs_entry_calculate_block_url( core, fh->path, fh->fent, offset );
   }
   else {
      // we have a URL for this block
      return block_url;
   }
}

// given an offset and a file handle, retrieve or generate the URL at which the block can/should be found.
// NEED TO LOCK FS ENTRY FIRST!
char* fs_entry_get_block_url( struct fs_core* core, char const* fs_path, struct fs_entry* fent, off_t offset ) {

   // block number
   uint64_t block_id = fs_entry_block_id( offset, core->conf );
   char* block_url = fent->manifest->get_block_url( fent->version, block_id );

   if( block_url == NULL ) {
      return fs_entry_calculate_block_url( core, fs_path, fent, offset );
   }
   else {
      // we have a URL for this block
      return block_url;
   }
}

char* fs_entry_replica_block_url( char const* url, int64_t version, uint64_t block_id, int64_t block_version ) {
   char* ret = CALLOC_LIST( char, strlen(url) + 70 );
   sprintf( ret, "%s.%" PRId64 "/%" PRIu64 ".%" PRId64, url, version, block_id, block_version );
   return ret;
}

char* fs_entry_replica_manifest_url( char const* url, int64_t version, struct timespec* ts ) {
   char* ret = CALLOC_LIST( char, strlen(url) + 80 );
   sprintf( ret, "%s.%" PRId64 "/manifest.%ld.%ld", url, version, ts->tv_sec, ts->tv_nsec );
   return ret;
}
