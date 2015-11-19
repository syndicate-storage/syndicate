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

#include "core.h"
#include "driver.h"

// deserialize a chunk
// return 0 on success, and fill in *chunk
// return -ENOMEM on OOM 
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
int UG_driver_chunk_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* chunk, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct UG_state* core = (struct UG_state*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   char* request_path = NULL;
   struct SG_driver* driver = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   size_t len = ms_client_get_volume_blocksize( ms );
   
   char* chunk_data = SG_CALLOC( char, len );
   if( chunk_data == NULL ) {
      return -ENOMEM;
   }
   
   SG_chunk_init( chunk, chunk_data, len );
   
   UG_state_rlock( core );
   
   // find a free deserializer
   driver = UG_state_driver( core );
   group = SG_driver_get_proc_group( core, "deserialize" );
   if( group != NULL ) {
      
      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         goto UG_driver_chunk_deserialize_finish
      }
      
      // generate the request 
      request_path = SG_driver_reqdat_to_path( reqdat );
      if( request_path == NULL ) {
         
         rc = -ENOMEM;
         goto UG_driver_chunk_deserialize_finish
      }
      
      // ask for the block 
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", fileno( SG_proc_stdout_f( proc ) ), rc );
         
         rc = -EIO;
         goto UG_driver_chunk_deserialize_finish
      }
      
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto UG_driver_chunk_deserialize_finish
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;
         
         goto UG_driver_chunk_deserialize_finish
      }
      
      // bail if the driver had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Worker %d: GET '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto UG_driver_chunk_deserialize_finish
      }
      
      // get the serialized chunk 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), chunk );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );
         
         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto UG_driver_chunk_deserialize_finish
      }
   }
   else {
      
      // no way to do work--no process group 
      rc = -ENODATA;
   }
  
UG_driver_chunk_deserialize_finish: 

   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   UG_state_unlock( core );
   return rc;
}


// serialize a chunk
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
int UG_driver_chunk_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* chunk, uint64_t hints, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct UG_state* core = (struct UG_state*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_driver* driver = NULL;

   char* request_path = NULL;
   
   // generate the request's path
   request_path = SG_driver_reqdat_to_path( reqdat );
   if( request_path == NULL ) {
      
      return -ENOMEM;
   }
   
   SG_state_rlock( core );
   
   // find a worker 
   driver = UG_state_driver( core );
   group = SG_driver_get_proc_group( driver, "serialize" );
   if( group != NULL ) {
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers
         SG_error("No free 'write' workers for %s\n", request_path );

         rc = -ENODATA;
         goto UG_driver_chunk_serialize;
      }
      
      // send path
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), request_path, strlen(request_path) );
      if( rc < 0 ) {
         
         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc);
         
         rc = -ENODATA;
         goto UG_driver_chunk_serialize;
      }
         
      rc = md_write_uninterrupted( SG_proc_stdin( proc ), "\n", 1 );
      if( rc < 0 ) {

         SG_error("md_write_uninterrupted(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
          
         rc = -EIO;
         goto UG_driver_chunk_serialize;
      }

      // put the block 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), block );
      if( rc < 0 ) {
       
         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
         goto UG_driver_chunk_serialize;
      }
      
      // get the reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", fileno(SG_proc_stdout_f( proc )), rc );
         
         rc = -EIO;
         goto UG_driver_chunk_serialize;
      }
      
      if( worker_rc < 0 ) {
         
         SG_error("Worker %d: PUT '%s' rc = %d\n", SG_proc_pid( proc ), request_path, (int)worker_rc );
         rc = -EIO;
         
         goto UG_driver_chunk_serialize;
      }
   }
   else {
      
      // no writers????
      SG_error("BUG: no writers started.  Cannot handle %s\n", request_path );
      rc = -ENODATA;
   }
   
UG_driver_chunk_serialize:
   
   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   UG_state_unlock( core );
   return rc;
}



