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

#include "common.h"

int main( int argc, char** argv ) {
   // usage: $NAME URL [URL...]
   if( argc <= 1 ) {
      errorf("Usage: %s URL [URL...]", argv[0] );
      exit(1);
   }
   
   int rc = 0;
   
   // activate debuggging and error logging
   md_set_debug_level( 1 );
   md_set_error_level( 1 );
   
   // initialize a downloader 
   struct md_downloader dl;
   rc = md_downloader_init( &dl, "test downloader" );
   if( rc != 0 ) {
      errorf("md_downloader_init rc = %d\n", rc );
      exit(1);
   }
   
   // start downloader 
   rc = md_downloader_start( &dl );
   if( rc != 0 ) {
      errorf("md_downloader_start rc = %d\n", rc );
      exit(1);
   }
   
   for( int i = 1; i < argc; i++ ) {
      char* url = argv[i];
      
      // CURL handle for this URL 
      CURL* curl_h = curl_easy_init();
      
      // initialize the curl handle 
      md_init_curl_handle2( curl_h, url, 30, true );
      
      // make a download context for this URL
      dbprintf("initializing download for %s\n", url );
      
      struct md_download_context dlctx;
      rc = md_download_context_init( &dlctx, curl_h, NULL, NULL, -1 );
      if( rc != 0 ) {
         errorf("md_download_context_init( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // start it
      dbprintf("Starting download for %s\n", url );
      rc = md_download_context_start( &dl, &dlctx, NULL, url );
      if( rc != 0 ) {
         errorf("md_download_context_start( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // wait for it 
      rc = md_download_context_wait( &dlctx, -1 );
      if( rc != 0 ) {
         errorf("md_download_context_wait( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // print results
      rc = print_download( &dlctx, url );
      if( rc != 0 ) {
         errorf("print_download( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // free memory 
      dbprintf("freeing download for %s\n", url);
      md_download_context_free( &dlctx, NULL );
      
      curl_easy_cleanup( curl_h );
   }
   
   // stop the downloader 
   rc = md_downloader_stop( &dl );
   if( rc != 0 ) {
      errorf("md_downloader_stop rc = %d\n", rc );
      exit(1);
   }
   
   // shut it down 
   rc = md_downloader_shutdown( &dl );
   if( rc != 0 ) {
      errorf("md_downloader_shutdown rc = %d\n", rc );
      exit(1);
   }
   
   return 0;
}
