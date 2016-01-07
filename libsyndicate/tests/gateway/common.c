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

#include "common.h"

// find the optind at the end of the syndicate options 
int common_parse_opts( struct md_opts* opts, int argc, char** argv, int* new_optind ) {
   
   memset( opts, 0, sizeof(struct md_opts) );
   
   int rc = 0;
   rc = md_opts_parse( opts, argc, argv, new_optind, NULL, NULL );
   if( rc != 0 ) {
      
      SG_error("md_opts_parse rc = %d\n", rc );
      return rc;
   }
   
   return rc;
}

// print out a request from a gateway 
// always succeeds
int common_print_request( SG_messages::Request* request ) {
   
   printf("Request: type=%d coordinator_id=%" PRIu64 " target=/%" PRIu64 "/%" PRIX64 ".%" PRId64 " (%s)\n  Header: volume_version=%" PRIu64 ", cert_version=%" PRIu64 "\n  Message nonce: %" PRIX64 "\n  User=%" PRIu64 " Remote gateway=%" PRIu64 " Local gateway=%" PRIu64 "\n  Optional data:\n", 
      request->request_type(), request->coordinator_id(), request->volume_id(), request->file_id(), request->file_version(), request->fs_path().c_str(), request->volume_version(), request->cert_version(),
      request->message_nonce(), request->user_id(), request->dest_gateway_id(), request->src_gateway_id() );
   
   if( request->has_new_fs_path() ) {
      printf("    new_fs_path='%s'\n", request->new_fs_path().c_str() );
   }
   
   if( request->has_new_size() ) {
      printf("    new_size=%" PRIu64 "\n", request->new_size() );
   }
   
   if( request->has_new_manifest_mtime_sec() && request->has_new_manifest_mtime_nsec() ) {
      printf("    new_mtime=%" PRId64 ".%d\n", request->new_manifest_mtime_sec(), request->new_manifest_mtime_nsec() );
   }
   
   if( request->blocks_size() > 0 ) {
      printf("    num_blocks=%d\n", request->blocks_size() );
   }
   
   return 0;
}

// print out a reply from a gateway 
// always succeeds
int common_print_reply( SG_messages::Reply* reply ) {
   
   printf("Reply: error code = %d\n  Header: volume_version=%" PRIu64 ", cert_version=%" PRIu64 "\n  Message nonce: %" PRIX64 "\n  User=%" PRIu64 "\n  Remote gateway=%" PRIu64 " type=%" PRIu64 "\n",
          reply->error_code(), reply->volume_version(), reply->cert_version(), reply->message_nonce(), reply->user_id(), reply->gateway_id(), reply->gateway_type() );
   
   return 0;
}
