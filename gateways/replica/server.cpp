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
// return -ENOENT if the block does not exist
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
static int RG_server_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;

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
      
      // ask for the block 
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;

         goto RG_server_block_get_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
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
      if( worker_rc < 0 ) {
        
         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );

         if( worker_rc == -ENOENT ) { 
             rc = -ENOENT;
         }
         else {
             rc = -EIO;
         }

         goto RG_server_block_get_finish;
      }
      
      // get the block 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), block );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );
         
         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto RG_server_block_get_finish;
      }
   }
   else {
      
      // no way to do work--no process group 
      rc = -ENODATA;
   }
   
RG_server_block_get_finish:

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }

   RG_core_unlock( core );
   return rc;
}


// get a manifest on cache miss 
// return 0 on success, and fill in *manifest 
// return -ENOMEM on OOM
// return -ENOENT if the manifest is not present 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if the driver is offline (i.e. gateway error)
static int RG_server_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_chunk chunk;
   size_t manifest_len = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;
   SG_messages::Manifest manifest_message;
   
   memset( &chunk, 0, sizeof(struct SG_chunk) );

   // find a reader 
   RG_core_rlock( core );
   
   group = SG_driver_get_proc_group( SG_gateway_driver( gateway ), "read" );
   if( group != NULL ) {
      
      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         SG_error("No free 'read' processes in group %p\n", group);
         goto RG_server_manifest_get_finish;
      }

      // ask for the serialized manifest 
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;

         goto RG_server_manifest_get_finish;
      }

      SG_debug("Request get %s\n", (driver_req.request_type() == SG_messages::DriverRequest::MANIFEST ? "manifest" : "block"));

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc < 0 ) {

         SG_error("SG_proc_write_request(%d) rc = %d\n", SG_proc_stdin(proc), rc );
         rc = -EIO;

         goto RG_server_manifest_get_finish;
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         
         goto RG_server_manifest_get_finish;
      }
      
      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );

      // bail if the gateway had a problem
      if( worker_rc < 0 ) {
         
         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );

         if( worker_rc == -ENOENT ) {
             rc = -ENOENT;
         }
         else {
             rc = -EIO;
         }
         
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
      manifest_len = chunk.len;
      SG_chunk_free( &chunk );
      
      if( rc < 0 ) {
         
         SG_error("md_parse(%zu) rc = %d\n", manifest_len, rc );
         
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
      SG_error("%s", "No such process group 'read'\n");
      rc = -ENODATA;
   }
   
RG_server_manifest_get_finish:

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
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req; 
   
   RG_core_rlock( core );
   
   // find a worker 
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "write" );
   if( group != NULL ) {
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers
         SG_error("%s", "No free 'write' workers\n" );

         rc = -ENODATA;
         goto RG_server_block_put_finish;
      }
      
      // send request 
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );

         rc = -ENODATA;
         goto RG_server_block_put_finish;
      }

      SG_debug("Request put %s\n", (driver_req.request_type() == SG_messages::DriverRequest::MANIFEST ? "manifest" : "block"));

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc < 0 ) {

         SG_error("SG_proc_write_request(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
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
      
      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );

      if( worker_rc < 0 ) {
         
         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_block_put_finish;
      }
   }
   else {
      
      // no writers????
      SG_error("%s", "BRG: no writers started.  Cannot handle!\n");
      rc = -ENODATA;
   }
   
RG_server_block_put_finish:
   
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
// return -ESTALE if the sender was not the coordinator (suggests that the sender does yet know that it is not the coordinator)
static int RG_server_manifest_put( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* manifest_chunk, uint64_t hints, void* cls ) {
   
   int rc = 0;

   // sanity check: must be a manifest
   SG_messages::Manifest mmsg;
   rc = md_parse< SG_messages::Manifest >( &mmsg, manifest_chunk->data, manifest_chunk->len );
   if( rc != 0 ) {

      SG_error("not a manifest: %s\n", reqdat->fs_path );
      return -EINVAL;
   }

   // sanity check: sender must be the coordinator 
   if( mmsg.coordinator_id() != reqdat->src_gateway_id ) {

      SG_error("Not the coordinator of %" PRIX64 ": %" PRIu64 " (expected %" PRIu64 ")\n", reqdat->file_id, reqdat->src_gateway_id, mmsg.coordinator_id() );
      return -ESTALE;
   }

   // send it off, as a block 
   rc = RG_server_block_put( gateway, reqdat, manifest_chunk, hints, cls );
   
   if( rc < 0 ) {
      
      SG_error("RG_server_block_put rc = %d\n", rc );
   }
    
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
   struct SG_proc* proc = NULL;
   struct SG_proc_group* group = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;
   struct SG_IO_hints io_hints;
   uint64_t block_size = ms_client_get_volume_blocksize(ms);

   SG_IO_hints_init( &io_hints, SG_IO_DELETE, block_size * reqdat->block_id, block_size );

   // find a worker...
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "delete" );

   if( group != NULL && SG_proc_group_size( group ) > 0 ) {   
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers 
         rc = -ENODATA;
         goto RG_server_block_delete_finish;
      }
      
      // send the worker the request 
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );

         rc = -ENODATA;
         goto RG_server_block_delete_finish;
      }

      SG_debug("Request delete %s\n", (driver_req.request_type() == SG_messages::DriverRequest::MANIFEST ? "manifest" : "block"));

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
         goto RG_server_block_delete_finish;
      }

      // get a reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -EIO;
         goto RG_server_block_delete_finish;
      }
      
      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );
      
      // bail if the gateway had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_block_delete_finish;
      }
   }
   else {
      
      // no delete workers 
      rc = -ENODATA;
   }
   
RG_server_block_delete_finish:
    
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   } 

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


// gateway callback to deserialize a chunk
// return 0 on success, and fill in *chunk
// return -ENOMEM on OOM 
// return -EIO if the driver did not fulfill the request (driver error)
// return -EAGAIN if we couldn't request the data, for whatever reason (i.e. no free processes)
int RG_server_chunk_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;
   struct SG_driver* driver = NULL;
  
   RG_core_rlock( core );
   
   // find a free deserializer
   driver = SG_gateway_driver( gateway );
   group = SG_driver_get_proc_group( driver, "deserialize" );
   
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {   

      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -EAGAIN;
         goto RG_server_chunk_deserialize_finish;
      }
      
      // feed in the metadata for this block
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto RG_server_chunk_deserialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto RG_server_chunk_deserialize_finish;
      }

      // feed in the block itself 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin(proc), rc );

         rc = -EIO;
         goto RG_server_chunk_deserialize_finish;
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;
         
         goto RG_server_chunk_deserialize_finish;
      }
      
      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );
      
      // bail if the driver had a problem
      if( worker_rc < 0 ) {
         
         SG_error("Worker %d: deserialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_chunk_deserialize_finish;
      }
      
      // get the serialized chunk 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );
         
         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto RG_server_chunk_deserialize_finish;
      }
   }
   else {
      
      // no-op deserializer 
      rc = SG_chunk_dup( out_chunk, in_chunk );
   }
  
RG_server_chunk_deserialize_finish: 

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   if( rc != 0 ) {
      SG_chunk_free( out_chunk );
   }

   RG_core_unlock( core );
   return rc;
}


// gateway callback to serialize a chunk
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we failed to communicate with the driver (i.e. driver error)
// return -EAGAIN if there were no free workers
int RG_server_chunk_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct RG_core* core = (struct RG_core*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_driver* driver = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;
   
   RG_core_rlock( core );
   
   // find a worker 
   driver = SG_gateway_driver( gateway );
   group = SG_driver_get_proc_group( driver, "serialize" );
   
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {   
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers
         SG_error("%s", "No free 'write' workers\n" );

         rc = -EAGAIN;
         goto RG_server_chunk_serialize_finish;
      }

      // feed in the metadata for this block
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto RG_server_chunk_serialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto RG_server_chunk_serialize_finish;
      }

      // put the block 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {
       
         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -EIO;
         goto RG_server_chunk_serialize_finish;
      }
      
      // get the reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", fileno(SG_proc_stdout_f( proc )), rc );
         
         rc = -EIO;
         goto RG_server_chunk_serialize_finish;
      }

      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );
      
      if( worker_rc < 0 ) {
         
         SG_error("Worker %d: serialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto RG_server_chunk_serialize_finish;
      }

      // get the deserialized chunk 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc != 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno(SG_proc_stdout_f(proc)), rc );
         goto RG_server_chunk_serialize_finish;
      }
   }
   else {
   
      // no-op serializer 
      rc = SG_chunk_dup( out_chunk, in_chunk );   
   }
   
RG_server_chunk_serialize_finish:
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   RG_core_unlock( core );
   return rc;
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
 
   SG_impl_serialize( gateway, RG_server_chunk_serialize );
   SG_impl_deserialize( gateway, RG_server_chunk_deserialize );

   SG_gateway_set_cls( gateway, core );
   return 0;
}
