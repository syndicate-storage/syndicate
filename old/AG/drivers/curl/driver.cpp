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

#include "driver.h"

// start up the driver
int driver_init( void** driver_state ) {
   
   return 0;
}


// shut down the driver 
int driver_shutdown( void* driver_state ) {
   
   return 0;
}

// connect
int connect_dataset_block( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_connection_state ) {
   
   char* request_path = AG_driver_get_request_path( ag_ctx );
   char* url = AG_driver_get_query_string( ag_ctx );
   
   struct curl_connection_context* curl_ctx = SG_CALLOC( struct curl_connection_context, 1 );
   
   curl_ctx->request_path = request_path;
   curl_ctx->url = url;
   
   *driver_connection_state = curl_ctx;
   
   return 0;
}

// free a connection context 
int close_dataset_block( void* driver_connection_state ) {
   
   struct curl_connection_context* curl_ctx = (struct curl_connection_context*)driver_connection_state;
   
   if( curl_ctx != NULL ) {
      if( curl_ctx->request_path != NULL ) {
         free( curl_ctx->request_path );
         curl_ctx->request_path = NULL;
      }

      if( curl_ctx->url != NULL ) {
         free( curl_ctx->url );
         curl_ctx->url = NULL;
      }

      free( curl_ctx );
   }
   return 0;
}


// dummy header function; seems to be needed to stat files 
static size_t curl_null_header( void* ptr, size_t size, size_t nmemb, void* data ) {
   return size * nmemb;
}

// write function to fill our buffer, and abort if we get too much 
static size_t curl_write_block( void* ptr, size_t size, size_t nmemb, void* data ) {
   
   struct curl_write_context* write_ctx = (struct curl_write_context*)data;
   
   size_t total_available = size * nmemb;
   
   // have space?
   if( write_ctx->num_written + total_available <= write_ctx->buf_len ) {
      
      memcpy( write_ctx->buf + write_ctx->num_written, ptr, total_available );
      write_ctx->num_written += total_available;
      
      return total_available;
   }
   else {
      // not enough space
      SG_error("Not enough space (have %zu bytes, but need at least %zu)\n", write_ctx->buf_len, write_ctx->num_written + total_available );
      return 0;
   }
}


// find out how big a file is, and when it was modified 
// return 0 on success
// return negative errno on failure, or if errno was not set, return positive curl status code on failure
static int curl_stat_file( CURL* curl, char const* url, struct AG_driver_publish_info* pub_info ) {
   
   SG_debug("stat %s\n", url );
   
   // set up the curl handle
   curl_easy_setopt( curl, CURLOPT_URL, url );
   curl_easy_setopt( curl, CURLOPT_NOBODY, 1L );
   curl_easy_setopt( curl, CURLOPT_FILETIME, 1L );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, curl_null_header );
   curl_easy_setopt( curl, CURLOPT_HEADER, 0L );
   
   int rc = curl_easy_perform( curl );
   
   if( rc != 0 ) {
      // error of some sort 
      long oserr = 0;
      
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &oserr );
      
      if( oserr != 0 ) {
         SG_error("curl_easy_perform(%s) rc = %d, errno = %ld\n", url, rc, -oserr );
         return (int)(-oserr);
      }
      else {
         SG_error("curl_easy_perform(%s) rc = %d\n", url, rc );
         return rc;
      }
   }
   
   // success! get info 
   long filetime = -1;
   double filesize = -1.0;
   
   int filetime_rc = curl_easy_getinfo( curl, CURLINFO_FILETIME, &filetime );
   int filesize_rc = curl_easy_getinfo( curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &filesize );
   
   if( (filetime_rc != 0 || filetime < 0) || (filesize_rc != 0 || filesize <= 0.0 ) ) {
      
      // failed to get data
      rc = -ENODATA;
      
      if( filetime_rc != 0 ) {
         SG_error("curl_easy_getinfo( CURLINFO_FILETIME ) rc = %d\n", filetime_rc );
         rc = filetime_rc;
      }
      if( filesize_rc != 0 ) {
         SG_error("curl_easy_getinfo( CURLINFO_CONTENT_LENGTH_DOWNLOAD ) rc = %d\n", filesize_rc );
         rc = filesize_rc;
      }
      
      return rc;
   }
   
   // fill in our pubinfo 
   pub_info->size = (off_t)filesize;
   
   // set the time to now 
   pub_info->mtime_sec = (int64_t)filetime;
   pub_info->mtime_nsec = 0;
   
   return 0;
}


// get a block of data, and get its pubinfo while we're at it
// return the number of bytes received on success
// return negative errno on failure, or if errno was not set, return positive curl status code on failure
static int curl_download_block( CURL* curl, char const* url, uint64_t block_id, char* buf, uint64_t block_size, struct AG_driver_publish_info* pub_info ) {
   
   SG_debug("Get block %s\n", url );
   
   struct curl_write_context write_ctx;
   memset( &write_ctx, 0, sizeof(struct curl_write_context) );
   
   write_ctx.num_written = 0;
   write_ctx.buf = buf;
   write_ctx.buf_len = block_size;
   
   // set up the curl handle
   curl_easy_setopt( curl, CURLOPT_URL, url );
   curl_easy_setopt( curl, CURLOPT_FILETIME, 1L );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, curl_null_header );
   curl_easy_setopt( curl, CURLOPT_HEADER, 0L );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, curl_write_block );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, &write_ctx );
   
   // add range requirement
   // format: X-Y, where X and Y are 64-bit byte indices.  Y is inclusive
   size_t range_maxlen = 20 + 1 + 20 + 1;  // NOTE: 19 < log10( UINT64_MAX ) < 20
   char range_buf[20 + 1 + 20 + 1];     
   snprintf( range_buf, range_maxlen, "%" PRIu64 "-%" PRIu64, block_id * block_size, (block_id + 1) * block_size - 1 );
   
   curl_easy_setopt( curl, CURLOPT_RANGE, range_buf );
   
   int rc = curl_easy_perform( curl );
   
   if( rc != 0 ) {
      // error of some sort 
      long oserr = 0;
      
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &oserr );
      
      if( oserr != 0 ) {
         SG_error("curl_easy_perform(%s) rc = %d, errno = %ld\n", url, rc, -oserr );
         return (int)(-oserr);
      }
      else {
         if( rc == CURLE_BAD_DOWNLOAD_RESUME ) {
            // EOF 
            SG_error("WARN: Block %" PRIu64 " is off the end of the file\n", block_id );
            
            write_ctx.num_written = 0;
         }
         else {
            SG_error("curl_easy_perform(%s) rc = %d\n", url, rc );
            return -rc;
         }
      }
   }
   
   // success! opportunistically try to get metadata info 
   long filetime = -1;
   double filesize = -1.0;
   
   int filetime_rc = curl_easy_getinfo( curl, CURLINFO_FILETIME, &filetime );
   int filesize_rc = curl_easy_getinfo( curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &filesize );
   
   // opportunistically got metadata?
   if( filetime_rc == 0 && filetime > 0 && filesize_rc == 0 && filesize >= 1.0 ) {
      
      // fill in our pubinfo 
      pub_info->size = (off_t)filesize;
      pub_info->mtime_sec = (int64_t)filetime;
      pub_info->mtime_nsec = 0;
   }
   
   return write_ctx.num_written;
}


// get the publish information for a dataset 
int curl_get_pubinfo( char const* request_path, char const* url, struct AG_driver_publish_info* pubinfo ) {
   
   // maybe we've cached it?
   char* info_path = md_fullpath( request_path, "curl-info", NULL );
   
   char* pubinfo_buf = NULL;
   size_t pubinfo_buflen = 0;
   
   int rc = AG_driver_cache_get_chunk( info_path, &pubinfo_buf, &pubinfo_buflen );
   
   if( rc == 0 ) {
      
      if( pubinfo_buflen == sizeof(struct AG_driver_publish_info) ) {
         // success!
         memcpy( pubinfo, pubinfo_buf, sizeof(struct AG_driver_publish_info) );
         free( pubinfo_buf );
            
         AG_driver_cache_promote_chunk( info_path );
         free( info_path );
         return 0;
      }
      else {
         SG_error("WARN: got invalid data for %s\n", info_path );
         AG_driver_cache_evict_chunk( info_path );
         
         rc = 0;
         free( pubinfo_buf );
         pubinfo_buf = NULL;
      }
   }
   
   // miss
   CURL* curl = curl_easy_init();

   // not cached
   rc = curl_stat_file( curl, url, pubinfo );
   if( rc != 0 ) {
      SG_error("ERR: curl_stat_file(%s, %s) rc = %d\n", info_path, url, rc );
   }
   else {
      // success! cache it 
      
      char* cache_chunk = SG_CALLOC( char, sizeof(struct AG_driver_publish_info) );
      memcpy( cache_chunk, &pubinfo, sizeof(struct AG_driver_publish_info) );
      
      AG_driver_cache_put_chunk_async( info_path, cache_chunk, sizeof(struct AG_driver_publish_info) );
      
      SG_debug("Got pubinfo for %s: { size = %jd, mtime_sec = %" PRId64 ", mtime_nsec = %" PRId32 " }\n", request_path, pubinfo->size, pubinfo->mtime_sec, pubinfo->mtime_nsec );
   }
   
   curl_easy_cleanup( curl );
   
   free( info_path );
   
   return rc;
}


// get data for a block.
// get it from the cache, if possible.
// otherwise, download it and serve it back 
ssize_t get_dataset_block( struct AG_connection_context* ag_ctx, uint64_t block_id, char* block_buf, size_t buf_len, void* driver_connection_state ) {
   
   struct curl_connection_context* curl_ctx = (struct curl_connection_context*)driver_connection_state;
   
   // opportunistically get pubinfo
   struct AG_driver_publish_info pubinfo;
   memset( &pubinfo, 0, sizeof(struct AG_driver_publish_info) );
   
   pubinfo.size = -1;
   pubinfo.mtime_sec = -1;
   
   CURL* curl = curl_easy_init();
   
   // no data cached; go get it 
   int num_read = curl_download_block( curl, curl_ctx->url, block_id, block_buf, buf_len, &pubinfo );
   
   if( num_read < 0 ) {
      
      SG_error("curl_download_block(%s, %" PRIu64 ") rc = %d\n", curl_ctx->url, block_id, num_read );
   }
   
   curl_easy_cleanup( curl );
   
   return num_read;
}


// prepare to publish--fill in pub_info
int stat_dataset( char const* path, struct AG_map_info* map_info, struct AG_driver_publish_info* pub_info, void* driver_state ) {
   
   char* url = AG_driver_map_info_get_query_string( map_info );
   
   
   SG_debug("stat %s (url: %s)\n", path, url );
   
   if( url == NULL ) {
      
      // it's a directory.
      // timestamp is simply now 
      pub_info->size = 4096;
      
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );
      
      pub_info->mtime_sec = ts.tv_sec;
      pub_info->mtime_nsec = ts.tv_nsec;
      
      return 0;
   }
   
   int rc = 0;
   
   // it's a file.
   // get information from upstream?
   char* get_from_upstream = AG_driver_get_config_var( AG_CURL_DRIVER_CONFIG_CHECK_UPSTREAM );
   if( get_from_upstream != NULL ) {
      
      free( get_from_upstream );
      
      // get the info from upstream
      int rc = curl_get_pubinfo( path, url, pub_info );
      
      if( rc != 0 ) {
         SG_error("curl_get_pubinfo(%s, %s) rc = %d\n", path, url, rc );
      }
      
   }
   else {
      // get locally 
      pub_info->size = -1;
      
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );
      
      pub_info->mtime_sec = ts.tv_sec;
      pub_info->mtime_nsec = ts.tv_nsec;
   }
   
   free( url );
   return rc;
}


// handle dataset reversion 
int reversion_dataset( char const* path, struct AG_map_info* mi, void* driver_state ) {
   
   // evict the associated cached publish info 
   char* info_path = md_fullpath( path, "curl-info", NULL );
   
   AG_driver_cache_evict_chunk( info_path );
   
   free( info_path );
   return 0;
}

// query type: curl
char* get_query_type(void) {
   return strdup("curl");
}
