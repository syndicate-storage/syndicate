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
   set_debug_level( 1 );
   set_error_level( 1 );
   
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
   
   // make a download set 
   struct md_download_set dlset;
   rc = md_download_set_init( &dlset );
   if( rc != 0 ) {
      errorf("md_download_set_init rc = %d\n", rc );
      exit(1);
   }
   
   // add each URL to it
   for( int i = 1; i < argc; i++ ) {
      char* url = argv[i];
      
      // CURL handle for this URL 
      CURL* curl_h = curl_easy_init();
      
      // initialize the curl handle 
      md_init_curl_handle2( curl_h, url, 30, true );
      
      // make a download context for this URL
      dbprintf("initializing download for %s\n", url );
      struct md_download_context* dlctx = CALLOC_LIST( struct md_download_context, 1 );
      
      rc = md_download_context_init( dlctx, curl_h, NULL, NULL, -1 );
      if( rc != 0 ) {
         errorf("md_download_context_init( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // add it to the download set 
      dbprintf("adding %s to the download set\n", url );
      rc = md_download_set_add( &dlset, dlctx );
      if( rc != 0 ) {
         errorf("md_download_set_add( %s ) rc = %d\n", url, rc );
         exit(1);
      }
      
      // start it
      dbprintf("Starting download for %s\n", url );
      rc = md_download_context_start( &dl, dlctx, NULL, url );
      if( rc != 0 ) {
         errorf("md_download_context_start( %s ) rc = %d\n", url, rc );
         exit(1);
      }
   }
   
   // wait for all downloads to finish
   while( md_download_set_size( &dlset ) > 0 ) {
      
      // wait for any of them to finish 
      dbprintf("Wait for %zu downloads to finish...\n", md_download_set_size( &dlset ) );
      
      rc = md_download_context_wait_any( &dlset, -1 );
      if( rc != 0 ) {
         errorf("md_download_wait_any rc = %d\n", rc );
      }
      
      // find the ones that are done 
      for( md_download_set_iterator itr = md_download_set_begin( &dlset ); itr != md_download_set_end( &dlset ); ) {
         
         struct md_download_context* dlctx = md_download_set_iterator_get_context( itr );
         
         md_download_set_iterator curr_itr = itr;
         itr++;
         
         if( dlctx == NULL )
            continue;
         
         // finalized?
         if( md_download_context_finalized( dlctx ) ) {
            
            char* url = NULL;
            rc = md_download_context_get_effective_url( dlctx, &url );
            if( rc != 0 ) {
               errorf("md_downlaod_context_get_effective_url rc = %d\n", rc );
               exit(1);
            }
            
            // print results
            rc = print_download( dlctx, url );
            if( rc != 0 ) {
               errorf("print_download( %s ) rc = %d\n", url, rc );
               exit(1);
            }
            
            // remove it
            rc = md_download_set_clear_itr( &dlset, curr_itr );
            if( rc != 0 ) {
               errorf("md_downlaod_set_clear rc = %d\n", url, rc );
               exit(1);
            }
            
            // free it 
            CURL* curl_h = NULL;
            md_download_context_free( dlctx, &curl_h );
            
            if( curl_h )
               curl_easy_cleanup( curl_h );
            
            free( dlctx );
            
            free( url );
         }
      }
      
      dbprintf("Download set now has %zu downloads remaining\n", md_download_set_size( &dlset ) );
   }
   
   // free the download set 
   dbprintf("%s", "freeing download set\n");
   rc = md_download_set_free( &dlset );
   if( rc != 0 ) {
      errorf("md_download_set_free rc = %d\n", rc );
      exit(1);
   }
   
   // stop the downloader 
   dbprintf("%s", "stopping downloader\n");
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