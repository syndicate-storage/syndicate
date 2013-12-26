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


#include "http-common.h"


// given the HTTP settings, the url, and the HTTP response, validate the path referred to by the URL.
// the response will be unaltered if the url contains a valid path (this path will be returned).
// if the path is invalid, then the response will be populated and NULL will be returned.
char* http_validate_url_path( struct md_HTTP* http, char* url, struct md_HTTP_response* resp ) {

   char* path = url;

   // sanity check--if '/../' appears anywhere in the string and it ISN'T escaped, then it's invalid
   if( strstr( path, "/../" ) != NULL ) {
      // bad request
      char* msg = (char*)"Cannot have '/../' in the path";
      if( resp )
         md_create_HTTP_response_ram( resp, "text/plain", 400, msg, strlen(msg) + 1 );
      return NULL;
   }

   return path;
}


// file I/O error handler
void http_io_error_resp( struct md_HTTP_response* resp, int err, char const* msg_txt ) {
   char const* msg = msg_txt;
   
   switch( err ) {
      case -ENOENT:
         if( msg == NULL )
            msg = MD_HTTP_404_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 404, msg, strlen(msg) + 1 );
         break;

      case -EPERM:
      case -EACCES:
         if( msg == NULL )
            msg = MD_HTTP_403_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 403, msg, strlen(msg) + 1 );
         break;

      case 413:
         if( msg == NULL )
            msg = MD_HTTP_413_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 413, msg, strlen(msg) + 1 );
         break;

      case -EEXIST:
         if( msg == NULL )
            msg = MD_HTTP_409_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 409, msg, strlen(msg) + 1 );
         break;

      case -EINVAL:
         if( msg == NULL )
            msg = MD_HTTP_400_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 400, msg, strlen(msg) + 1 );
         break;

      case -ENOTEMPTY:
         if( msg == NULL )
            msg = MD_HTTP_422_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 422, msg, strlen(msg) + 1 );
         break;

      case -EAGAIN:
         if( msg == NULL )
            msg = MD_HTTP_504_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 504, msg, strlen(msg) + 1 );
         break;
         
      default:
         if( msg == NULL )
            msg = MD_HTTP_500_MSG;
         
         md_create_HTTP_response_ram( resp, "text/plain", 500, msg, strlen(msg) + 1 );
         break;
   }
}


int http_make_redirect_response( struct md_HTTP_response* resp, char* new_url ) {
   // make an HTTP redirect response
   md_create_HTTP_response_ram( resp, "text/plain", 302, "Redirect\n", strlen("Redirect\n") + 1 );
   md_HTTP_add_header( resp, "Location", new_url );
   md_HTTP_add_header( resp, "Cache-Control", "no-store" );
   
   return 0;
}


int http_make_default_headers( struct md_HTTP_response* resp, time_t last_modified, size_t size, bool cacheable ) {
   // add last-modified time
   char buf[200];
   memset(buf, 0, 200);
   struct tm rawtime;
   memset( &rawtime, 0, sizeof(rawtime) );
   gmtime_r( &last_modified, &rawtime );

   strftime( buf, 200, "%a, %d %b %Y %H:%M:%S GMT", &rawtime );

   md_HTTP_add_header( resp, "Last-Modified", buf );
   
   // add content-length field
   memset( buf, 0, 200 );
   sprintf(buf, "%zu", size );
   
   md_HTTP_add_header( resp, "Content-Length", buf );
   
   // add no-cache field, if needed
   if( !cacheable ) {
      md_HTTP_add_header( resp, "Cache-Control", "no-store" );
   }

   return 0;
}


// generate the correct URL to be redirected to, if needed
// return 0 if a new URL was generated
// return HTTP_REDIRECT_NOT_HANDLED if a new URL was not generated, but there were no errors
// return HTTP_REDIRECT_REMOTE if the requested data is not locally hosted
// return negative if a new URL could not be generated
int http_process_redirect( struct syndicate_state* state, char** redirect_url, struct stat* sb, struct gateway_request_data* reqdat ) {

   memset( sb, 0, sizeof(struct stat) );
   
   int rc = 0;

   uint64_t volume_id = reqdat->volume_id;
   char* fs_path = reqdat->fs_path;
   int64_t file_version = reqdat->file_version;
   uint64_t block_id = reqdat->block_id;
   int64_t block_version = reqdat->block_version;
   struct timespec* manifest_timestamp = &reqdat->manifest_timestamp;

   // wrong Volume?
   if( state->core->volume != volume_id ) {
      errorf("Request for invalid Volume %" PRIu64 "\n", volume_id );
      return -ENODATA;
   }

   // look up the file version
   int64_t latest_file_version = fs_entry_get_version( state->core, fs_path );

   if( latest_file_version < 0 ) {
      // this file does not exist
      dbprintf(" fs_entry_get_version(%s) rc = %" PRId64 "\n", fs_path, latest_file_version );
      return (int)latest_file_version;
   }

   bool local = false;
   rc = fs_entry_stat_extended( state->core, fs_path, sb, &local, SYS_USER, 0, false );

   if( rc < 0 ) {
      // could not be found
      errorf("fs_entry_stat_extended(%s) rc = %d\n", fs_path, rc );
      return rc;
   }
   
   // what is this a request for?
   // was this a request for a block?
   if( block_id != INVALID_BLOCK_ID ) {

      bool block_local = fs_entry_is_block_local( state->core, fs_path, SYS_USER, 0, reqdat->block_id );
      if( !block_local ) {
         // block exists, and is remotely-hosted
         uint64_t gateway_id = fs_entry_get_block_host( state->core, fs_path, block_id );
         char* block_url = fs_entry_remote_block_url( state->core, gateway_id, fs_path, file_version, block_id, block_version );

         *redirect_url = block_url;

         return HTTP_REDIRECT_HANDLED;
      }
      else {
         // otherwise, block exists and is local.
         // was the latest version requested?
         int64_t latest_block_version = fs_entry_get_block_version( state->core, fs_path, block_id );

         if( latest_block_version < 0 ) {
            // this block does not exist
            dbprintf("fs_entry_get_block_version(%s[%" PRId64 "]) rc = %" PRId64 "\n", fs_path, block_id, latest_block_version );
            return (int)latest_block_version;
         }

         if( latest_file_version != file_version ) {
            // older version of the file (i.e. file has been unlinked/recreated, so it's not the same file)
            dbprintf("Request for stale file version (%" PRId64 " != %" PRId64 ")\n", latest_file_version, file_version );
            return -ESTALE;
         }

         // same file version but wrong block version?  redirect
         if( latest_block_version != block_version ) {
            // HTTP redirect to the latest
            char* txt = fs_entry_public_block_url( state->core, fs_path, latest_file_version, block_id, latest_block_version );

            *redirect_url = txt;

            return HTTP_REDIRECT_HANDLED;
         }

         // same file version and same block version.  Serve data!
         return HTTP_REDIRECT_NOT_HANDLED;      // no error, but not handled, meaning the remote host requested the latest version of a valid, local block
      }
   }

   // request for a file or directory or file manifest?
   else {
      
      if( !local ) {
         // remote
         return HTTP_REDIRECT_REMOTE;
      }

      // regular local file?
      else if( S_ISREG( sb->st_mode ) ) {
         if( manifest_timestamp->tv_sec >= 0 && manifest_timestamp->tv_nsec >= 0 ) {
            // request for a manifest

            // correct manifest timestamp?
            struct timespec lastmod;
            rc = fs_entry_get_mod_time( state->core, fs_path, &lastmod );
            if( rc != 0 ) {
               // deleted!
               dbprintf("fs_entry_get_mod_time rc = %d\n", rc );
               return rc;
            }

            if( latest_file_version != file_version || (manifest_timestamp->tv_sec != lastmod.tv_sec || manifest_timestamp->tv_nsec != lastmod.tv_nsec) ) {
               // wrong file version; redirect to latest manifest
               struct timespec lastmod;
               memset( &lastmod, 0, sizeof(lastmod) );
               fs_entry_get_mod_time( state->core, fs_path, &lastmod );

               // redirect to the appropriate manifest URL
               char* txt = fs_entry_public_manifest_url( state->core, fs_path, latest_file_version, &lastmod );

               *redirect_url = txt;

               return HTTP_REDIRECT_HANDLED;
            }
            else {
               return HTTP_REDIRECT_NOT_HANDLED;      // need to serve the manifest
            }
         }
         else if( latest_file_version != file_version ) {
            // request for an older version of a local file
            char* txt = fs_entry_public_file_url( state->core, fs_path, latest_file_version );

            *redirect_url = txt;
            return HTTP_REDIRECT_HANDLED;
         }
      }
      
      return HTTP_REDIRECT_NOT_HANDLED;         // no error, but not handled, meaning the remote host requested the latest version of a valid file or a directory
   }

   return rc;
}



// handle redirect requests
// return HTTP_REDIRECT_HANDLED for handled
// return (HTTP_REDIRECT_NOT_HANDLED, HTTP_REDIRECT_REMOTE) otherwise
int http_handle_redirect( struct syndicate_state* state, struct md_HTTP_response* resp, struct stat* sb, struct gateway_request_data* reqdat ) {
   char* redirect_url = NULL;
   
   int rc = http_process_redirect( state, &redirect_url, sb, reqdat );
   if( rc < 0 ) {
      char buf[200];
      snprintf( buf, 200, "http_handle_redirect: http_get_redirect_url rc = %d\n", rc );
      http_io_error_resp( resp, rc, buf );
      return HTTP_REDIRECT_HANDLED;
   }
   if( rc == HTTP_REDIRECT_HANDLED ) {
      http_make_redirect_response( resp, redirect_url );
      free( redirect_url );
      return HTTP_REDIRECT_HANDLED;
   }
   free( redirect_url );
   return rc;
}



// extract useful information on a request
// and handle redirect requests.
// populate the given arguments and return 0 or 1 on success (0 means that no redirect occurred; 1 means a redirect occurred)
// return negative on error
int http_parse_request( struct md_HTTP* http_ctx, struct md_HTTP_response* resp, struct gateway_request_data* reqdat, char* url ) {

   memset( reqdat, 0, sizeof(struct gateway_request_data) );
   
   char* url_path = http_validate_url_path( http_ctx, url, resp );
   if( url_path == NULL ) {
      char buf[200];
      snprintf(buf, 200, "http_GET_parse_request: http_validate_url_path returned NULL\n");
      md_create_HTTP_response_ram( resp, "text/plain", 400, buf, strlen(buf) + 1 );
      
      dbprintf("%s", buf);
      return -400;
   }

   // parse the url_path into its constituent components
   memset( &reqdat->manifest_timestamp, 0, sizeof(reqdat->manifest_timestamp) );

   int rc = md_HTTP_parse_url_path( url_path, &reqdat->volume_id, &reqdat->fs_path, &reqdat->file_version, &reqdat->block_id, &reqdat->block_version, &reqdat->manifest_timestamp, &reqdat->staging );
   if( rc != 0 && rc != -EISDIR ) {
      char buf[200];
      snprintf(buf, 200, "http_GET_parse_request: md_HTTP_parse_url_path rc = %d\n", rc );
      md_create_HTTP_response_ram( resp, "text/plain", 400, buf, strlen(buf) + 1 );
      
      dbprintf("%s", buf);
      return -400;
   }
   else if( rc == -EISDIR ) {
      reqdat->fs_path = strdup( url_path );
   }

   if( reqdat->block_id != INVALID_BLOCK_ID )
      dbprintf("volume_id = %" PRIu64 ", fs_path = '%s', file_version = %" PRId64 ", block_id = %" PRIu64 ", block_version = %" PRId64 ", manifest_timestamp = %ld.%ld, staging = %d\n",
               reqdat->volume_id, reqdat->fs_path, reqdat->file_version, reqdat->block_id, reqdat->block_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->staging );
   
   else
      dbprintf("volume_id = %" PRIu64 ", fs_path = '%s', file_version = %" PRId64 ", block_id = (none), block_version = %" PRId64 ", manifest_timestamp = %ld.%ld, staging = %d\n",
               reqdat->volume_id, reqdat->fs_path, reqdat->file_version, reqdat->block_version, reqdat->manifest_timestamp.tv_sec, reqdat->manifest_timestamp.tv_nsec, reqdat->staging );

   if( reqdat->fs_path == NULL) {
      // nothing to do
      char buf[200];
      snprintf(buf, 200, "http_GET_parse_request: fs_path == NULL\n");
      md_create_HTTP_response_ram( resp, "text/plain", 400, buf, strlen(buf) + 1 );
      
      dbprintf("%s", buf);
      return -400;
   }

   return 0;
}
