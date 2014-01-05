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


#include "collator.h"

// create a Collator
Collator::Collator( struct fs_core* core ) {

   pthread_mutex_init( &this->release_queue_lock, NULL );

   this->release_curl = curl_easy_init();
   md_init_curl_handle( core->conf, this->release_curl, "http://foo.com", core->conf->connect_timeout );

   curl_easy_setopt( this->release_curl, CURLOPT_POST, 1L );
   curl_easy_setopt( this->release_curl, CURLOPT_SSL_VERIFYPEER, (core->conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( this->release_curl, CURLOPT_SSL_VERIFYHOST, 2L );
   curl_easy_setopt( this->release_curl, CURLOPT_NOSIGNAL, 1L );
   
   sem_init( &this->release_sem, 0, 0 );

   this->core = core;
   this->running = false;
   this->stopped = true;
}

// destroy a Collator
Collator::~Collator() {
   dbprintf("%s", "stopping release messages...\n");
   this->stop();
   
   pthread_mutex_destroy( &this->release_queue_lock );
   curl_easy_cleanup( this->release_curl );
   sem_destroy( &this->release_sem );
}


// build up an AcceptMsg
static int build_AcceptMsg( Serialization::WriteMsg* acceptMsg, struct fs_core* core, char const* fs_path, uint64_t file_id, int64_t file_version, uint64_t start_id, uint64_t end_id, int64_t* block_versions ) {
   fs_entry_init_write_message( acceptMsg, core, Serialization::WriteMsg::ACCEPTED );

   Serialization::AcceptMsg* accept_data = acceptMsg->mutable_accepted();

   accept_data->set_fs_path( string(fs_path) );
   accept_data->set_file_id( file_id );
   accept_data->set_file_version( file_version );

   for( uint64_t i = 0; i < end_id - start_id; i++ ) {
      accept_data->add_block_id( i + start_id );
      accept_data->add_block_version( block_versions[i] );
   }

   return 0;
}


// send an accepted mssage off
static int send_accepted( struct fs_core* core, CURL* curl_h, char const* content_url, Serialization::WriteMsg* acceptMsg ) {
   string msg_data_str;
   acceptMsg->SerializeToString( &msg_data_str );

   md_init_curl_handle( core->conf, curl_h, content_url, core->conf->connect_timeout );

   curl_easy_setopt( curl_h, CURLOPT_POST, 1L );
   curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, (core->conf->verify_peer ? 1L : 0L) );
   curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYHOST, 2L );
   curl_easy_setopt( curl_h, CURLOPT_NOSIGNAL, 1L );

   struct curl_httppost *post = NULL, *last = NULL;
   curl_formadd( &post, &last, CURLFORM_PTRNAME, "WriteMsg", CURLFORM_PTRCONTENTS, msg_data_str.data(), CURLFORM_CONTENTSLENGTH, msg_data_str.size(), CURLFORM_END );

   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, post );

   int rc = curl_easy_perform( curl_h );

   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, NULL );
   curl_formfree( post );

   if( rc == 0 ) {
      curl_easy_getinfo( curl_h, CURLINFO_RESPONSE_CODE, &rc );

      if( rc != 200 ) {
         errorf( "curl_easy_perform(%s) HTTP %d\n", content_url, rc );
         rc = -abs(rc);
      }
      else {
         rc = 0;
      }
   }
   else {
      errorf("curl_easy_perform(%s) rc = %d\n", content_url, rc );
      rc = -abs(rc);
   }
   return rc;
}


// release a range of blocks
int Collator::release_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_block_id, uint64_t end_block_id ) {

   // build up a set of messages
   vector<struct release_entry>* released = new vector<struct release_entry>();

   uint64_t k = start_block_id;

   int rc = 0;

   while( k < end_block_id ) {

      uint64_t start = 0, end = 0;
      uint64_t writer_gateway = 0;

      rc = fent->manifest->get_range( k, &start, &end, &writer_gateway );
      if( rc != 0 )
         break;
      
      int64_t* blk_versions = fent->manifest->get_block_versions( start, end );

      Serialization::WriteMsg* msg = new Serialization::WriteMsg();
      build_AcceptMsg( msg, core, fs_path, fent->file_id, fent->version, start_block_id, end_block_id, blk_versions );

      struct release_entry rls;
      memset( &rls, 0, sizeof(rls) );
      
      rls.acceptMsg = msg;
      rls.gateway_id = writer_gateway;

      released->push_back( rls );

      k = end;
   }

   pthread_mutex_lock( &this->release_queue_lock );

   for( unsigned int i = 0; i < released->size(); i++ ) {
      this->release_queue.push_back( (*released)[i] );
   }

   pthread_mutex_unlock( &this->release_queue_lock );

   for( unsigned int i = 0; i < released->size(); i++ ) {
      sem_post( &this->release_sem );
   }

   delete released;

   return rc;
}

// release main loop
void* Collator::release_loop( void* arg ) {
   Collator* col = (Collator*)arg;
   col->stopped = false;
   
   while( col->running ) {
      struct release_entry next;

      sem_wait( &col->release_sem );
      if( !col->running )
         break;
      
      // post any pending releaselations
      pthread_mutex_lock( &col->release_queue_lock );
      if( !col->running ) {
         pthread_mutex_unlock( &col->release_queue_lock );
         break;
      }

      next = col->release_queue[0];
      col->release_queue.erase( col->release_queue.begin() );
      
      pthread_mutex_unlock( &col->release_queue_lock );

      char* gateway_url = ms_client_get_UG_content_url( col->core->ms, next.gateway_id );
      if( gateway_url == NULL ) {
         dbprintf("WARN: No such gateway %" PRIu64 "\n", next.gateway_id );
         delete next.acceptMsg;
         continue;
      }
      
      // tell the remote host that we've accepted
      dbprintf("send accept to %s\n", gateway_url );
      
      int rc = send_accepted( col->core, col->release_curl, gateway_url, next.acceptMsg );

      if( rc != 0 ) {
         errorf("send_accepted(%s) rc = %d\n", gateway_url, rc );
      }

      free( gateway_url );
      
      delete next.acceptMsg;

   }
   col->stopped = true;
   return NULL;
}

// Collator up the collator
int Collator::start() {
   int rc = 0;
   
   // start up the release thread
   this->release_thread = md_start_thread( Collator::release_loop, this, true );
   
   if( this->release_thread < 0 ) {
      return this->release_thread;
   }

   this->running = true;
   
   return rc;
}

// stop the collator
int Collator::stop() {
   this->running = false;

   // wake up the release loop
   sem_post( &this->release_sem );

   dbprintf("%s", "Collator: waiting for threads to die...\n");
   while( !this->stopped ) {
      sleep(1);
   }
   
   return 0;
}


// release a range of pending collations
// fent must be read-locked
int fs_entry_release_remote_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_block_id, uint64_t end_block_id ) {
   dbprintf("release %s.%" PRId64 "[%" PRIu64 "-%" PRIu64 "]\n", fs_path, fent->version, start_block_id, end_block_id);
   return core->col->release_blocks( core, fs_path, fent, start_block_id, end_block_id );
}
