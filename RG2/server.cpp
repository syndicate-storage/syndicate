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

#include <libsyndicate/proc.h>

#include "server.h"
#include "syndicate-rg.h"

// get a block on cache miss
// return 0 on success, and fill in *block
// return -ENOMEM on OOM 
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
static int RG_server_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   char* request_path = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   size_t len = ms_client_get_volume_blocksize( ms );
   
   char* block_data = SG_CALLOC( char, len );
   if( block_data == NULL ) {
      return -ENOMEM;
   }
   
   SG_chunk_init( block, block_data, len );
   
   RG_core_rlock( core );
   
   // find a reader 
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "read" );
   if( group != NULL ) {
      
      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         goto RG_server_block_get_finish;
      }
      
      // generate the request 
      request_path = SG_driver_reqdat_to_path( reqdat );
      if( request_path == NULL ) {
         
         rc = -ENOMEM;
         goto RG_server_block_get_finish;
      }
      
      // ask for the block 
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", fileno( SG_proc_stdout_f( proc ) ), rc );
         
         rc = -EIO;
         goto RG_server_block_get_finish;
      }
      
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto RG_server_block_get_finish;
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;
         
         goto RG_server_block_get_finish;
      }
      
      // bail if the gateway had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Worker %d: GET '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_block_get_finish;
      }
      
      // get the block 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), block );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );
         
         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto RG_server_block_get_finish;
      }
      
      // make sure the block is the right size 
      if( len != (size_t)block->len ) {
         
         // nope!
         SG_chunk_free( block );
         memset( block, 0, sizeof(struct SG_chunk) );
         
         SG_error("md_read_uninterrupted(%d) returned %zu of %zu expected bytes\n", fileno( SG_proc_stdout_f( proc ) ), len, block->len );
         
         rc = -EIO;
         goto RG_server_block_get_finish;
      }
   }
   else {
      
      // no way to do work--no process group 
      rc = -ENODATA;
   }
   
RG_server_block_get_finish:

   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   RG_core_unlock( core );
   return rc;
}


// get a manifest on cache miss 
// return 0 on success, and fill in *manifest 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if the driver is offline (i.e. gateway error)
static int RG_server_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_chunk chunk;
   char* request_path = NULL;
   SG_messages::Manifest manifest_message;
   
   memset( &chunk, 0, sizeof(struct SG_chunk) );

   // generate the request's path
   request_path = SG_driver_reqdat_to_path( reqdat );
   if( request_path == NULL ) {
      
      return -ENOMEM;
   }
   
   // find a reader 
   RG_core_rlock( core );
   
   group = SG_driver_get_proc_group( SG_gateway_driver( gateway ), "read" );
   if( group != NULL ) {
      
      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         goto RG_server_manifest_get_finish;
      }
      
      // ask for the serialized manifest 
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -EIO;
         goto RG_server_manifest_get_finish;
      }
     
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto RG_server_manifest_get_finish;
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;
         
         goto RG_server_manifest_get_finish;
      }
      
      // bail if the gateway had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Worker %d: GET '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_manifest_get_finish;
      }
      
      // get the serialized manifest 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), &chunk );
      if( rc < 0 ) {
         
         // OOM, EOF, or driver crash (error is -ENOMEM, -ENODATA, or -EIO, respectively)
         SG_error( "SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f( proc ) ), rc );
         goto RG_server_manifest_get_finish;
      }
      
      // deserialize 
      rc = md_parse< SG_messages::Manifest >( &manifest_message, chunk.data, chunk.len );
      SG_chunk_free( &chunk );
      
      if( rc < 0 ) {
         
         SG_error("md_parse(%zu) rc = %d\n", chunk.len, rc );
         
         rc = -EIO;
         goto RG_server_manifest_get_finish;
      }
      
      // propagate 
      rc = SG_manifest_load_from_protobuf( manifest, &manifest_message );
      if( rc < 0 ) {
         
         SG_error("SG_manifest_load_from_protobuf rc = %d\n", rc );
         
         if( rc != -ENOMEM ) {
            rc = -EIO;
         }
         
         goto RG_server_manifest_get_finish;
      }
   }
   else {
      
      // no way to do work--no process group 
      rc = -ENODATA;
   }
   
RG_server_manifest_get_finish:

   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   RG_core_unlock( core );
   return rc;
}


// put a block into the RG
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
static int RG_server_block_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   
   char* request_path = NULL;
   
   // generate the request's path
   request_path = SG_driver_reqdat_to_path( reqdat );
   if( request_path == NULL ) {
      
      return -ENOMEM;
   }
   
   RG_core_rlock( core );
   
   // find a worker 
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "write" );
   if( group != NULL ) {
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers
         SG_error("No free 'write' workers for %s\n", request_path );

         rc = -ENODATA;
         goto RG_server_block_put_finish;
      }
      
      // send path
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc);
         
         rc = -ENODATA;
         goto RG_server_block_put_finish;
      }
         
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto RG_server_block_put_finish;
      }

      // put the block 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), block );
      if( rc < 0 ) {
       
         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
         goto RG_server_block_put_finish;
      }
      
      // get the reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", fileno(SG_proc_stdout_f( proc )), rc );
         
         rc = -EIO;
         goto RG_server_block_put_finish;
      }
      
      if( worker_rc < 0 ) {
         
         SG_error("Worker %d: PUT '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_block_put_finish;
      }
   }
   else {
      
      // no writers????
      SG_error("BUG: no writers started.  Cannot handle %s\n", request_path );
      rc = -ENODATA;
   }
   
RG_server_block_put_finish:
   
   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   RG_core_unlock( core );
   return rc;
}


// put a manifest into the RG--basically, serialize it and treat it like a block
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
static int RG_server_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints, void* cls ) {
   
   int rc = 0;
   struct SG_chunk chunk;
   SG_messages::Manifest manifest_message;
   size_t len = 0;
   
   SG_chunk_init( &chunk, NULL, 0 );
   
   // convert to protobuf 
   rc = SG_manifest_serialize_to_protobuf( manifest, &manifest_message );
   if( rc < 0 ) {
      
      SG_error("SG_manifest_serialize_to_protobuf rc = %d\n", rc );
      
      if( rc != -ENOMEM ) {
         rc = -ENODATA;
      }
      
      goto RG_server_manifest_put_finish;
   }
   
   // serialize 
   rc = md_serialize< SG_messages::Manifest >( &manifest_message, &chunk.data, &len );
   chunk.len = (off_t)len;
   
   if( rc < 0 ) {
      
      SG_error("md_serialize rc = %d\n", rc );
      
      if( rc != -ENOMEM ) {
         rc = -ENODATA;
      }
      
      goto RG_server_manifest_put_finish;
   }
   
   // send it off, as a block 
   rc = RG_server_block_put( gateway, reqdat, &chunk, hints, cls );
   SG_chunk_free( &chunk );
   
   if( rc < 0 ) {
      
      SG_error("RG_server_block_put rc = %d\n", rc );
   }
      
RG_server_manifest_put_finish:

   return rc;
}


// delete a block from the RG 
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the drier (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
static int RG_server_block_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   char* request_path = NULL;
   struct SG_proc* proc = NULL;
   struct SG_proc_group* group = NULL;
   
   // generate the path 
   request_path = SG_driver_reqdat_to_path( reqdat );
   if( request_path == NULL ) {
      
      return -ENOMEM;
   }
   
   // find a worker...
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "delete" );
   if( group != NULL ) {
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers 
         rc = -ENODATA;
         goto RG_server_block_delete_finish;
      }
      
      // send the worker the path to delete 
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
         goto RG_server_block_delete_finish;
      }
      
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto RG_server_block_delete_finish;
      }

      // get a reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -EIO;
         goto RG_server_block_delete_finish;
      }
      
      // bail if the gateway had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Worker %d: GET '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_block_delete_finish;
      }
   }
   else {
      
      // no delete workers 
      rc = -ENODATA;
   }
   
RG_server_block_delete_finish:
   
   SG_safe_free( request_path );
   
   return rc;
}


// delete a manifest from the RG (in the same way that we might delete a block)
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the drier (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
static int RG_server_manifest_delete( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {

   return RG_server_block_delete( gateway, reqdat, cls );
}


// set up the gateway's method implementation 
// always succeeds
int RG_server_install_methods( struct SG_gateway* gateway, struct RG_core* core ) {
   
   SG_impl_get_block( gateway, RG_server_block_get );
   SG_impl_get_manifest( gateway, RG_server_manifest_get );
   
   SG_impl_put_block( gateway, RG_server_block_put );
   SG_impl_put_manifest( gateway, RG_server_manifest_put );
   
   SG_impl_delete_block( gateway, RG_server_block_delete );
   SG_impl_delete_manifest( gateway, RG_server_manifest_delete );
  
   SG_gateway_set_cls( gateway, core );
   return 0;
}
