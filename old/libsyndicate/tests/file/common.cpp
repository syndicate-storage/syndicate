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


#include "libsyndicate/download.h"

int print_download( struct md_download_context* dlctx, char const* base_url ) {
   
   int rc = 0;
   
   // print information 
   char* download_buf = NULL;
   off_t download_buf_len = 0;
   
   rc = md_download_context_get_buffer( dlctx, &download_buf, &download_buf_len );
   if( rc != 0 ) {
      SG_error("md_download_context_get_buffer( %s ) rc = %d\n", rc, base_url );
      return rc;
   }
   
   // get status codes
   int http_status = md_download_context_get_http_status( dlctx );
   int err = md_download_context_get_errno( dlctx );
   int curl_rc = md_download_context_get_curl_rc( dlctx );
   
   printf("GET: %s\nHTTP status: %d\nCURL rc: %d\nerrno: %d\nlength: %jd\n", base_url, http_status, curl_rc, err, download_buf_len );
   
   if( download_buf_len > 0 ) {
      // null-terminated 
      char* null_terminated_buf = (char*)realloc( download_buf, download_buf_len + 1 );
      
      if( null_terminated_buf == NULL ) {
         SG_error("realloc rc = %p\n", null_terminated_buf );
         exit(1);
      }
      
      download_buf = null_terminated_buf;
      
      download_buf[ download_buf_len ] = '\0';
      
      printf("buffer:\n%s\n", download_buf);
   }
   
   free( download_buf );
   
   printf("\n\n\n");
   
   return 0;
}
