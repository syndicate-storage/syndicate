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

#include "coblitz.h"

// intialize our cache driver 
int closure_init( struct md_closure* closure, void** cls ) {
   // get the CDN prefix...
   char* cdn_prefix = NULL;
   size_t cdn_prefix_len = 0;
   
   int rc = md_closure_get_config( closure, "CDN_PREFIX", &cdn_prefix, &cdn_prefix_len );
   if( rc != 0 ) {
      SG_error("CDN_PREFIX not found (rc = %d)\n", rc );
      return -EINVAL;
   }
   
   SG_debug("CDN prefix is '%s'\n", cdn_prefix );
   
   struct coblitz_cls* ccls = SG_CALLOC( struct coblitz_cls, 1 );
   ccls->cdn_prefix = cdn_prefix;
   
   return 0;
}


// shut down our cache driver
int closure_shutdown( void* cls ) {
   
   struct coblitz_cls* ccls = (struct coblitz_cls*)(cls);
   
   if( ccls->cdn_prefix ) {
      free( ccls->cdn_prefix );
      ccls->cdn_prefix = NULL;
   }
   
   return 0;
}

// connect to the coblitz CDN 
int connect_cache( struct md_closure* closure, CURL* curl, char const* url, void* cls ) {
   SG_debug("Coblitz connect_cache on %s\n", url );
   
   // NO-OP for now...also, no SSL
   md_init_curl_handle2( curl, url, 5, false );
   
   return 0;
}
