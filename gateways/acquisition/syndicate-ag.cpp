/*
   Copyright 2016 The Trustees of Princeton University

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

#include "syndicate-ag.h"
#include "core.h"
#include "crawl.h"

// global running flag
volatile bool g_running = true;

// toggle running flag for the crawler loop
void AG_set_running( bool running ) {
   g_running = running;
}


// AG main loop: crawl the dataset, using the crawler 
static void* AG_crawl_loop( void* cls ) {

   int rc = 0;
   bool have_more = true;
   struct AG_state* ag = (struct AG_state*)cls;

   while( g_running && have_more ) {

      // poll on the crawler
      if( have_more ) {
         rc = AG_crawl_next_entry( ag );
         if( rc < 0 ) {
            if( rc != -ENOTCONN ) {
               SG_error("AG_crawl_next_entry rc = %d\n", rc );
               sleep(1);
            }
            else {

               SG_warn("%s", "Crawler process is no longer running\n");
               have_more = false;
            }
         }
      }

      if( rc > 0 ) {
         // done crawling 
         have_more = false;
      }
   }

   SG_debug("%s", "Crawler thread exit\n");
   return NULL;
}


// entry point 
int main( int argc, char** argv ) {

   int rc = 0;
   int exit_code = 0;
   struct AG_state* ag = NULL;
   pthread_t crawl_thread;

   // setup...
   ag = AG_init( argc, argv );
   if( ag == NULL ) {
      
      SG_error("%s", "AG_init failed\n" );
      exit(1);
   }

   // start crawler 
   rc = md_start_thread( &crawl_thread, AG_crawl_loop, ag, false );
   if( rc != 0 ) {
      SG_error("md_start_thread rc = %d\n", rc );
      exit(1);
   }

   // run gateway 
   rc = AG_main( ag );
   if( rc != 0 ) {
      SG_error("AG_main rc = %d\n", rc );
      exit_code = 1;
   }

   // stop crawler 
   g_running = false;
   pthread_cancel( crawl_thread );
   pthread_join( crawl_thread, NULL );

   // stop gateway 
   rc = AG_shutdown( ag );
   if( rc != 0 ) {
      SG_error("AG_shutdown rc = %d\n", rc );
   }

   SG_safe_free( ag );
   exit(exit_code);
}

