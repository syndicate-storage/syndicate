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


// convert a URL into a CDN-ified URL 
// return 0 on success, and fill in *chunk with the URL 
// return -ENOMEM on OOM 
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
// NOTE: this method is called by the Syndicate "impl_connect_cache" callback implementation in the UG.
int UG_driver_cdn_url( struct UG_state* core, struct SG_request_data* reqdat, struct SG_chunk* out_url ) {

   int rc = 0;
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   char* request_path = NULL;
   struct SG_chunk request_path_chunk;
   struct SG_driver* driver = NULL;
   struct SG_chunk out_chunk;

   memset( &out_chunk, 0, sizeof(struct SG_chunk) );

   UG_state_rlock( core );

   // find a free cdn-url worker 
   driver = UG_state_driver( core );
   group = SG_driver_get_proc_group( driver, "cdn_url" );
   if( group != NULL ) {

      // get a free process 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {

         // got nothing 
         rc = -ENODATA;
         goto UG_driver_cdn_url_finish;
      }

      // feed in the metadata for this block
      request_path = SG_driver_reqdat_to_path( reqdat );
      if( request_path == NULL ) {
         
         rc = -ENOMEM;
         goto UG_driver_cdn_url_finish;
      }

      SG_chunk_init( &request_path_chunk, request_path, strlen(request_path) );
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), &request_path_chunk );
      if( rc < 0 ) {
         
         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin(proc), rc );
         
         rc = -EIO;
         goto UG_driver_cdn_url_finish;
      }

      // read back CDN-ified url 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), &out_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f( proc ) ), rc );

         rc = -EIO;
         goto UG_driver_cdn_url_finish;
      }

      // success!
      *out_url = out_chunk;
      memset( &out_chunk, 0, sizeof(struct SG_chunk) );
   }
   else {

      SG_error("%s", "BUG: no process group 'cdn_url'\n");
      exit(1);
   }

UG_driver_cdn_url_finish:

   SG_safe_free( request_path );
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   UG_state_unlock( core );
   return rc;
}


// gateway callback to deserialize a chunk
// return 0 on success, and fill in *chunk
// return -ENOMEM on OOM 
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
int UG_driver_chunk_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct UG_state* core = (struct UG_state*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   SG_messages::DriverRequest driver_req;
   struct SG_driver* driver = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   size_t len = ms_client_get_volume_blocksize( ms );
  
   // expect one block 
   char* chunk_data = SG_CALLOC( char, len );
   if( chunk_data == NULL ) {
      return -ENOMEM;
   }
   
   SG_chunk_init( out_chunk, chunk_data, len );
   
   UG_state_rlock( core );
   
   // find a free deserializer
   driver = UG_state_driver( core );
   group = SG_driver_get_proc_group( driver, "deserialize" );
   if( group != NULL ) {

      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
      
         // nothing running
         rc = -ENODATA;
         goto UG_driver_chunk_deserialize_finish;
      }
      
      // feed in the metadata for this block
      rc = SG_proc_request_init( &driver_req, reqdat );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto UG_driver_chunk_deserialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto UG_driver_chunk_deserialize_finish;
      }

      // feed in the block itself 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin(proc), rc );

         rc = -EIO;
         goto UG_driver_chunk_deserialize_finish;
      }

      // get error code 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;
         
         goto UG_driver_chunk_deserialize_finish;
      }
      
      // bail if the driver had a problem
      if( worker_rc != 0 ) {
         
         SG_error("Worker %d: deserialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto UG_driver_chunk_deserialize_finish;
      }
      
      // get the serialized chunk 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );
         
         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto UG_driver_chunk_deserialize_finish;
      }
   }
   else {
      
      // no way to do work--no process group 
      SG_error("%s", "BUG: no process group 'deserialize'\n");
      exit(1);
   }
  
UG_driver_chunk_deserialize_finish: 

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   if( rc != 0 ) {
      SG_chunk_free( out_chunk );
   }

   UG_state_unlock( core );
   return rc;
}


// gateway callback to serialize a chunk
// return 0 on success 
// return -ENOMEM on OOM 
// return -EIO if we get invalid data from the driver (i.e. driver error)
// return -ENODATA if we couldn't send data to the driver (i.e. gateway error)
int UG_driver_chunk_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {
   
   int rc = 0;
   int64_t worker_rc = 0;
   struct UG_state* core = (struct UG_state*)SG_gateway_cls( gateway );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_driver* driver = NULL;
   SG_messages::DriverRequest driver_req;
   
   UG_state_rlock( core );
   
   // find a worker 
   driver = UG_state_driver( core );
   group = SG_driver_get_proc_group( driver, "serialize" );
   if( group != NULL ) {
      
      // get a free worker 
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {
         
         // no free workers
         SG_error("%s", "No free 'write' workers\n" );

         rc = -ENODATA;
         goto UG_driver_chunk_serialize_finish;
      }

      // feed in the metadata for this block
      rc = SG_proc_request_init( &driver_req, reqdat );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto UG_driver_chunk_serialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto UG_driver_chunk_serialize_finish;
      }

      // put the block 
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {
       
         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin( proc ), rc );
         
         rc = -ENODATA;
         goto UG_driver_chunk_serialize_finish;
      }
      
      // get the reply 
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {
         
         SG_error("SG_proc_read_int64(%d) rc = %d\n", fileno(SG_proc_stdout_f( proc )), rc );
         
         rc = -EIO;
         goto UG_driver_chunk_serialize_finish;
      }
      
      if( worker_rc < 0 ) {
         
         SG_error("Worker %d: serialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;
         
         goto UG_driver_chunk_serialize_finish;
      }

      // get the deserialized chunk 
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc != 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno(SG_proc_stdout_f(proc)), rc );
         goto UG_driver_chunk_serialize_finish;
      }
   }
   else {
      
      // no writers????
      SG_error("%s", "BUG: no process group 'serialize'\n");
      exit(1);
   }
   
UG_driver_chunk_serialize_finish:
   
   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }
   
   UG_state_unlock( core );
   return rc;
}

