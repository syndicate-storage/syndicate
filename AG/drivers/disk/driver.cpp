/*
   Copyright 2013 The Trustees of Princeton University

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

#include "driver.h"

#define DRIVER_QUERY_TYPE "disk"

// translate the requested path into an absolute path on disk, using our driver config 
static char* get_request_abspath( char const* request_path ) {
   
   // where's our dataset root directory?
   char* dataset_root = AG_driver_get_config_var( AG_CONFIG_DISK_DATASET_ROOT );
   if( dataset_root == NULL ) {
      errorf("Configuration error: No config value for '%s'\n", AG_CONFIG_DISK_DATASET_ROOT );
      return NULL;
   }
   
   // path to the file 
   char* dataset_path = md_fullpath( dataset_root, request_path, NULL );
   free( dataset_root );
   
   return dataset_path;
}

// translate an -errno into an HTTP status 
// return 0 if supported 
// return -1 if there wasn't a suitable HTTP status
static int errno_to_HTTP_status( struct AG_connection_context* ag_ctx, int err ) {

   // not found? give a 404 
   if( err == -ENOENT ) {
      AG_driver_set_HTTP_status( ag_ctx, 404 );
      return 0;
   }
   // can't access? give a 403 
   else if( err == -EACCES ) {
      AG_driver_set_HTTP_status( ag_ctx, 403 );
      return 0;
   }
   // out of memory?
   else if( err == -ENOMEM ) {
      AG_driver_set_HTTP_status( ag_ctx, 503 );
      return 0;
   }
   // bad FD?
   else if( err == -EBADF ) {
      AG_driver_set_HTTP_status( ag_ctx, 500 );
      return 0;
   }
   
   return -1;
}

// initialize the driver 
int driver_init( void** driver_state ) {
   dbprintf("%s driver init\n", DRIVER_QUERY_TYPE );
   return 0;
}

// shut down the driver 
int driver_shutdown( void* driver_state ) {
   dbprintf("%s driver shutdown\n", DRIVER_QUERY_TYPE );
   return 0;
}

// set up an incoming connection.
// try to open the file.
int connect_dataset_block( struct AG_connection_context* ag_ctx, void* driver_state, void** driver_connection_state ) {
   
   dbprintf("%s connect dataset\n", DRIVER_QUERY_TYPE );
   
   char* request_path = AG_driver_get_request_path( ag_ctx );
   
   // get the absolute path 
   char* dataset_path = get_request_abspath( request_path );
   
   if( dataset_path == NULL ) {
      errorf("Could not translate %s to absolute path\n", request_path );
      
      free( request_path );
      return -EINVAL;
   }
   
   free( request_path );
   
   // open the file 
   int fd = open( dataset_path, O_RDONLY );
   if( fd < 0 ) {
      fd = -errno;
      errorf("Failed to open %s, errno = %d\n", dataset_path, fd );
      
      free( dataset_path );
      return fd;
   }
   
   free( dataset_path );
   
   // got it!
   // set up a connection context 
   struct AG_disk_context* disk_ctx = CALLOC_LIST( struct AG_disk_context, 1 );
   
   disk_ctx->fd = fd;
   
   *driver_connection_state = disk_ctx;
   
   return 0;
}

// clean up a handled connection for a block 
int close_dataset_block( void* driver_connection_state ) {
   
   dbprintf("%s close dataset block\n", DRIVER_QUERY_TYPE );
   
   
   struct AG_disk_context* disk_ctx = (struct AG_disk_context*)driver_connection_state;
   
   if( disk_ctx != NULL ) {
      close( disk_ctx->fd );
      free( disk_ctx );
   }
   
   return 0;
}

// get information for creating and sending a block.
// return the number of bytes read
ssize_t get_dataset_block( struct AG_connection_context* ag_ctx, uint64_t block_id, char* block_buf, size_t buf_len, void* driver_connection_state ) {
   
   dbprintf("%s get dataset block %" PRIu64 "\n", DRIVER_QUERY_TYPE, block_id );
   
   struct AG_disk_context* disk_ctx = (struct AG_disk_context*)driver_connection_state;
   
   // seek to the appropriate offset 
   uint64_t block_size = AG_driver_get_block_size();
   off_t block_offset = block_size * block_id;
   
   int rc = lseek( disk_ctx->fd, block_offset, SEEK_SET );
   if( rc < 0 ) {
      rc = -errno;
      errorf("lseek errno = %d\n", rc );
      
      errno_to_HTTP_status( ag_ctx, rc );
      
      return rc;
   }
   
   // read in the buffer 
   ssize_t num_read = md_read_uninterrupted( disk_ctx->fd, block_buf, buf_len );
   if( num_read < 0 ) {
      
      errorf("md_read_uninterrupted rc = %zd\n", num_read );
      errno_to_HTTP_status( ag_ctx, num_read );
   }
   
   return num_read;
}

// get information for publishing a particular file to the MS 
int stat_dataset( char const* path, struct AG_map_info* map_info, struct AG_driver_publish_info* pub_info, void* driver_state ) {
   
   dbprintf("%s stat dataset %s\n", DRIVER_QUERY_TYPE, path );
   
   // get the absolute path 
   char* dataset_path = get_request_abspath( path );
   
   if( dataset_path == NULL ) {
      errorf("%s", "Could not translate request to absolute path\n" );
      return -EINVAL;
   }
   
   struct stat sb;
   int rc = 0;
   
   // stat the path 
   rc = stat( dataset_path, &sb );
   if( rc != 0 ) {
      rc = -errno;
      errorf("stat(%s) errno = %d\n", dataset_path, rc);
      free( dataset_path );
      return rc;
   }
   
   free( dataset_path );
   
   // fill in the publish info
   pub_info->size = sb.st_size;
   pub_info->mtime_sec = sb.st_mtime;
   pub_info->mtime_nsec = 0;
   
   return 0;
}

// handle a driver-specfic event.
// there are none for this driver.
int handle_event( char* event_payload, size_t event_payload_len, void* driver_state ) {
   return 0;
}

// what kind of query type does this driver support?
char* get_query_type(void) {
   return strdup(DRIVER_QUERY_TYPE);
}

