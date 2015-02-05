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

#include "libsyndicate/ms/benchmark.h"
#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/volume.h"

// print out timing data
static void print_timings( uint64_t* timings, size_t num_timings, char const* hdr ) {
   if( num_timings > 0 ) {
      for( size_t i = 0; i < num_timings; i++ ) {
         SG_TIMING_DATA( hdr, (double)(timings[i]) / 1e9 );
      }
   }
}


// benchmark header parser, for libcurl
// returns size * nmemb on success
// returns 0 on error
size_t ms_client_timing_header_func( void *ptr, size_t size, size_t nmemb, void *userdata) {
   struct ms_client_timing* times = (struct ms_client_timing*)userdata;

   size_t len = size * nmemb;
   char* data = (char*)ptr;

   char* data_str = SG_CALLOC( char, len + 1 );
   
   if( data_str == NULL ) {
      // out of memory 
      return 0;
   }
   
   strncpy( data_str, data, len );

   //SG_debug("header: %s\n", data_str );

   // is this one of our headers?  Find each of them
   off_t off = md_header_value_offset( data_str, len, HTTP_VOLUME_TIME );
   if( off > 0 ) {
      times->volume_time = md_parse_header_uint64( data_str, off, len );
      free( data_str );
      return len;
   }
   
   off = md_header_value_offset( data_str, len, HTTP_GATEWAY_TIME );
   if( off > 0 ) {
      times->ug_time = md_parse_header_uint64( data_str, off, len );
      free( data_str );
      return len;
   }

   off = md_header_value_offset( data_str, len, HTTP_TOTAL_TIME );
   if( off > 0 ) {
      times->total_time = md_parse_header_uint64( data_str, off, len );
      free( data_str );
      return len;
   }

   off = md_header_value_offset( data_str, len, HTTP_RESOLVE_TIME );
   if( off > 0 ) {
      times->resolve_time = md_parse_header_uint64( data_str, off, len );
      free( data_str );
      return len;
   }

   off = md_header_value_offset( data_str, len, HTTP_CREATE_TIMES );
   if( off > 0 ) {
      
      if( times->create_times != NULL ) {
         free( times->create_times );
      }
      
      times->create_times = md_parse_header_uint64v( data_str, off, len, &times->num_create_times );
      free( data_str );
      return len;
   }

   off = md_header_value_offset( data_str, len, HTTP_UPDATE_TIMES );
   if( off > 0 ) {
      
      if( times->update_times != NULL ) {
         free( times->update_times );
      }
      
      times->update_times = md_parse_header_uint64v( data_str, off, len, &times->num_update_times );
      free( data_str );
      return len;
   }

   off = md_header_value_offset( data_str, len, HTTP_DELETE_TIMES );
   if( off > 0 ) {
      
      if( times->delete_times != NULL ) {
         free( times->delete_times );
      }
      
      times->delete_times = md_parse_header_uint64v( data_str, off, len, &times->num_delete_times );
      free( data_str );
      return len;
   }

   free( data_str );
   return len;
}


// extract and print out benchmark data after a write 
// ms_client must not be locked
int ms_client_timing_log( struct ms_client_timing* times ) {
   
   if( times->create_times != NULL ) {
      print_timings( times->create_times, times->num_create_times, HTTP_CREATE_TIMES );
   }
   
   if( times->update_times != NULL ) {
      print_timings( times->update_times, times->num_update_times, HTTP_UPDATE_TIMES );
   }
   
   if( times->delete_times != NULL ) {
      print_timings( times->delete_times, times->num_delete_times, HTTP_DELETE_TIMES );
   }
   
   return 0;
}


// free timing data 
int ms_client_timing_free( struct ms_client_timing* times ) {
   
   if( times->create_times != NULL ) {
      free( times->create_times );
   }
   
   if( times->update_times != NULL ) {
      free( times->update_times );
   }
   
   if( times->delete_times != NULL ) {
      free( times->delete_times );
   }
   
   memset( times, 0, sizeof(struct ms_client_timing) );
   
   return 0;
}