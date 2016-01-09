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

#include "server.h"

char* cwd = NULL;
bool running = true;
char** accepted_fields = NULL;

void die_handler( int param ) {
   running = false;
}


int HTTP_connect( struct md_HTTP_connection_data* con_data, void** cls ) {
   return 0;
}


int HTTP_get( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   char* path = con_data->url_path;
   int rc = 0;
   int fd = 0;
   struct stat sb;
   char* fullpath = NULL;
   
   fullpath = md_fullpath( cwd, path, NULL );
   if( fullpath == NULL ) {
      return -ENOMEM;
   }
   
   fd = open( fullpath, O_RDONLY );
   if( fd < 0 ) {
      
      rc = -errno;
      if( rc == -ENOENT ) {
         
         rc = md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   else {
   
      rc = fstat( fd, &sb );
      if( rc != 0 ) {
         
         close( fd );
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
      else {
         
         rc = md_HTTP_create_response_fd( resp, "application/octet-stream", 200, fd, 0, sb.st_size );
      }
   }
   
   SG_safe_free( fullpath );
   return rc;
}


int HTTP_head( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   char* path = con_data->url_path;
   struct stat sb;
   int rc = 0;
   char* fullpath = NULL;
   
   fullpath = md_fullpath( cwd, path, NULL );
   if( fullpath == NULL ) {
      return -ENOMEM;
   }
   
   rc = stat( path, &sb );
   if( rc != 0 ) {
      
      rc = -errno;
      
      if( rc == -ENOENT ) {
         rc = md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   
   else {
      
      rc = md_HTTP_create_response_builtin( resp, 200 );
   }
   
   SG_safe_free( fullpath );
   return rc;
}


int HTTP_upload_RAM_finish( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   char* path = con_data->url_path;
   int rc = 0;
   char* fullpath = NULL;
   char* fullpath_field = NULL;
   FILE* f = NULL;
   char* data = NULL;
   size_t data_len = 0;
   ssize_t nw = 0;
   
   fullpath = md_fullpath( cwd, path, NULL );
   if( fullpath == NULL ) {
      
      return -ENOMEM;
   }
   
   // write all accepted fields to disk 
   for( int i = 0; accepted_fields[i] != NULL; i++ ) {
   
      rc = md_HTTP_upload_get_field_buffer( con_data, accepted_fields[i], &data, &data_len );
      if( rc == -ENOENT ) {
         continue;
      }
      else if( rc != 0 ) {
         
         SG_safe_free( fullpath );
         
         SG_error("md_HTTP_upload_get_buffer rc = %d\n", rc );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      fullpath_field = SG_CALLOC( char, strlen(fullpath) + 1 + strlen(accepted_fields[i]) + 1 );
      if( fullpath_field == NULL ) {
         
         SG_safe_free( fullpath );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      sprintf( fullpath_field, "%s.%s", fullpath, accepted_fields[i] );
      
      
      f = fopen( fullpath_field, "w" );
      if( f == NULL ) {
         rc = md_HTTP_create_response_builtin( resp, 500 );
         
         SG_safe_free( fullpath );
         SG_safe_free( fullpath_field );
         SG_safe_free( data );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      nw = md_write_uninterrupted( fileno(f), data, data_len );
      if( nw < 0 || (size_t)nw != data_len ) {
         
         SG_error("md_write_uninterrupted('%s', %zu) rc = %d\n", fullpath_field, data_len, rc );
         
         SG_safe_free( fullpath );
         SG_safe_free( fullpath_field );
         SG_safe_free( data );
         
         fclose( f );
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      fsync( fileno(f) );
      fclose( f );
      
      SG_safe_free( data );
      SG_safe_free( fullpath_field );
   }
   
   SG_safe_free( fullpath );
   
   return md_HTTP_create_response_builtin( resp, 200 );
}


int HTTP_upload_disk_finish( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   char* path = con_data->url_path;
   int rc = 0;
   char* fullpath = NULL;
   char* fullpath_field = NULL;
   char* tmpfile_path = NULL;
   int tmpfd = -1;
   
   fullpath = md_fullpath( cwd, path, NULL );
   if( fullpath == NULL ) {
      
      return -ENOMEM;
   }
   
   // move all accepted fields into place 
   for( int i = 0; accepted_fields[i] != NULL; i++ ) {
      
      rc = md_HTTP_upload_get_field_tmpfile( con_data, accepted_fields[i], &tmpfile_path, &tmpfd );
      if( rc == -ENOENT ) {
         continue;
      }
         
      else if( rc != 0 ) {
         
         SG_error("md_HTTP_upload_get_tmpfile rc = %d\n", rc );
         
         SG_safe_free( fullpath );
         SG_safe_free( tmpfile_path );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      fullpath_field = SG_CALLOC( char, strlen(fullpath) + 1 + strlen(accepted_fields[i]) + 1 );
      if( fullpath_field == NULL ) {
         
         SG_safe_free( fullpath );
         SG_safe_free( tmpfile_path );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      sprintf(fullpath_field, "%s.%s", fullpath, accepted_fields[i] );
      
      // move into place 
      rc = rename( tmpfile_path, fullpath_field );
      if( rc != 0 ) {
         
         rc = -errno;
         SG_error("rename('%s', '%s') rc = %d\n", tmpfile_path, fullpath_field, rc );
         
         SG_safe_free( fullpath_field );
         SG_safe_free( fullpath );
         SG_safe_free( tmpfile_path );
         
         return md_HTTP_create_response_builtin( resp, 500 );
      }
      
      SG_safe_free( fullpath_field );
      SG_safe_free( tmpfile_path );
   }
   
   SG_safe_free( fullpath );
   
   return md_HTTP_create_response_builtin( resp, 200 );
}


int HTTP_delete( struct md_HTTP_connection_data* con_data, struct md_HTTP_response* resp ) {
   
   char* path = con_data->url_path;
   int rc = 0;
   char* fullpath = NULL;
   
   fullpath = md_fullpath( cwd, path, NULL );
   if( fullpath == NULL ) {
      
      return -ENOMEM;
   }
   
   rc = unlink( fullpath );
   if( rc != 0 ) {
      
      rc = -errno;
      if( rc == -ENOENT ) {
         rc = md_HTTP_create_response_builtin( resp, 404 );
      }
      else {
         rc = md_HTTP_create_response_builtin( resp, 500 );
      }
   }
   else {
      rc = md_HTTP_create_response_builtin( resp, 200 );  
   }
   
   SG_safe_free( fullpath );
   return rc;
}


int main( int argc, char** argv ) {
   
   struct md_HTTP http;
   int portnum = 0;
   int rc = 0;
   struct md_syndicate_conf conf;
   
   
   GOOGLE_PROTOBUF_VERIFY_VERSION;
   rc = curl_global_init( CURL_GLOBAL_ALL );
   
   if( rc != 0 ) {
      SG_error("curl_global_init rc = %d\n", rc );
      return rc;
   }
   
   memset( &conf, 0, sizeof(struct md_syndicate_conf) );
   
   if( argc < 3 ) {
      fprintf(stderr, "Usage: %s [disk|RAM] portnum [field...]\n", argv[0] );
      exit(1);
   }
   
   rc = sscanf( argv[2], "%d", &portnum );
   
   if( rc != 1 ) {
      fprintf(stderr, "Failed to parse '%s'\n", argv[2] );
      exit(1);
   }
   
   cwd = getcwd(NULL, 0);
   if( cwd == NULL ) {
      exit(2);
   }
   
   if( strcasecmp(argv[1], "ram") != 0 && strcasecmp(argv[1], "disk") != 0 ) {
      fprintf(stderr, "Usage: %s [disk|RAM] portnum [field...]\n", argv[0] );
      exit(1);
   }
   
   signal( SIGINT, die_handler );
   signal( SIGTERM, die_handler );
   signal( SIGQUIT, die_handler );
   
   accepted_fields = &argv[3];
   
   // this is the only thing we'll need for this test
   conf.num_http_threads = 1;
   
   md_set_debug_level( 3 );
   md_set_error_level( 3 );
   
   md_HTTP_init( &http, MD_HTTP_TYPE_STATEMACHINE | MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY | MHD_USE_DEBUG, NULL );
   
   md_HTTP_connect( http, HTTP_connect );
   md_HTTP_GET( http, HTTP_get );
   md_HTTP_HEAD( http, HTTP_head );
   md_HTTP_DELETE( http, HTTP_delete );
   
   if( strcasecmp(argv[1], "ram" ) == 0 ) {
      md_HTTP_POST_finish( http, HTTP_upload_RAM_finish );
      md_HTTP_PUT_finish( http, HTTP_upload_RAM_finish );
      
      for( int i = 0; accepted_fields[i] != NULL; i++ ) {
         md_HTTP_post_field_handler( http, accepted_fields[i], md_HTTP_post_field_handler_ram );
      }
   }
   else {
      md_HTTP_POST_finish( http, HTTP_upload_disk_finish );
      md_HTTP_PUT_finish( http, HTTP_upload_disk_finish );
      
      for( int i = 0; accepted_fields[i] != NULL; i++ ) {
         md_HTTP_post_field_handler( http, accepted_fields[i], md_HTTP_post_field_handler_disk );
      }
   }

   rc = md_HTTP_start( &http, portnum, &conf );
   if( rc != 0 ) {
      
      fprintf(stderr, "md_HTTP_start(%d) rc = %d\n", portnum, rc );
      exit(3);
   }
   
   while( running ) {
      sleep(1);
   }
   
   md_HTTP_stop( &http );
   md_HTTP_free( &http );
   
   curl_global_cleanup();
   exit(0);
}
