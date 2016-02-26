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
   struct UG_state* ug = NULL;
   struct fskit_fuse_state* fs_fuse = NULL;
   int first_arg_optind = 0;

   char const* foreground = "-f";

   char* fuse_argv[] = {
      argv[0],
      NULL,     // for -f
      NULL,     // for mountpoint
      NULL
   };

   int fuse_argc = 1;

   // set up fskit-fuse
   fs_fuse = fskit_fuse_state_new();
   if( fs_fuse == NULL ) {

      exit(1);
   }

   // set up the UG
   ug = UG_init( argc, argv, false );
   if( ug == NULL ) {
      
      SG_error("%s", "UG failed to initialize\n" );
      exit(1);
   }

   // consume the UG's args, so we can feed them into fskit 
   first_arg_optind = SG_gateway_first_arg_optind( UG_state_gateway( ug ) );
   if( SG_gateway_foreground( UG_state_gateway(ug) ) ) {

      fuse_argv[fuse_argc] = (char*)foreground;
      fuse_argc++;
   }

   fuse_argv[fuse_argc] = argv[first_arg_optind];
   fuse_argc++;

   for( int i = 0; i < fuse_argc; i++ ) {

      SG_debug("FUSE argv[%d] = '%s'\n", i, fuse_argv[i]);
   }
  
   // bind the UG to fskit-fuse 
   rc = fskit_fuse_init_fs( fs_fuse, UG_state_fs( ug ) );
   if( rc != 0 ) {

      SG_error("fskit_fuse_init_fs rc = %d\n", rc );
      exit(1);
   }

   // disable permissions checks--we enforce them ourselves 
   fskit_fuse_setting_enable( fs_fuse, FSKIT_FUSE_NO_PERMISSIONS );

   // start the UG
   rc = UG_start( ug );
   if( rc != 0 ) {
      SG_error("UG_start rc = %d\n", rc );
      exit(2);
   }

   // run the filesystem!
   rc = fskit_fuse_main( fs_fuse, fuse_argc, fuse_argv );
   
   if( rc != 0 ) {
      
      SG_error("fskit_fuse_main rc = %d\n", rc );
   }
   
   // shut down
   SG_debug("%s", "Signaling gateway shutdown\n");
   SG_gateway_signal_main( UG_state_gateway( ug ) );
   
   UG_shutdown( ug );
   fskit_fuse_detach_core( fs_fuse );        // because UG_shutdown destroyed it
   fskit_fuse_shutdown( fs_fuse, NULL );

   // success!
   return 0;
}
