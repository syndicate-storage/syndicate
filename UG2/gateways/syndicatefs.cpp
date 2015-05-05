/*
   Copyright 2015 The Trustees of Princeton University

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

#include "syndicatefs.h"

// gateway main thread 
void* UG_main( void* arg ) {

   struct UG_state* ug = (struct UG_state*)arg;
   int rc = 0;
   
   SG_debug("UG %p starting up\n", ug );
   
   rc = UG_main( ug );
   
   if( rc != 0 ) {
      SG_error("UG_main rc = %d\n", rc );
   }
   
   return NULL;
}


// entry point 
int main( int argc, char** argv ) {
   
   int rc = 0;
   struct UG_state ug;
   pthread_t ug_thread;
   
   // set up the UG
   rc = UG_init( &ug, false, argc, argv );
   if( rc != 0 ) {
      
      SG_error("UG_init rc = %d\n", rc );
      exit(1);
   }
   
   // start the UG 
   ug_thread = md_start_thread( UG_main, &ug, false );
   if( ug_thread == (pthread_t)(-1) ) {
      
      SG_error("md_start_thread rc = %d\n", (int)ug_thread );
      exit(2);
   }
   
   // run the filesystem!
   rc = fskit_fuse_main( UG_state_fs( ug ), argc, argv );
   
   if( rc != 0 ) {
      
      SG_error("fskit_fuse_main rc = %d\n", rc );
   }
   
   // shut down 
   SG_gateway_signal_main( UG_state_gateway( ug ) );
   
   SG_debug("joining with UG_main (thread %d)\n", (int)ug_thread);
   pthread_join( ug_thread, NULL );
   
   // success!
   return 0;
}
