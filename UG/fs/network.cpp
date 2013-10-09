/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "network.h"
#include "manifest.h"
#include "url.h"
#include "collator.h"

// download data via local proxy and CDN
// return 0 on success
// return negative on irrecoverable error
// return positive HTTP status code if the problem is with the proxy
int fs_entry_download( struct fs_core* core, CURL* curl, char const* proxy, char const* url, char** bits, ssize_t* ret_len, ssize_t max_len ) {
   return md_download( core->conf, curl, proxy, url, bits, ret_len, max_len );
}


// download data, trying in order:
// * both CDN and proxy
// * from proxy
// * from CDN
// * from gateway
int fs_entry_download_cached( struct fs_core* core, char const* url, char** bits, ssize_t* ret_len, ssize_t max_len ) {
   CURL* curl = curl_easy_init();
   
   int rc = md_download_cached( core->conf, curl, url, bits, ret_len, max_len );
   
   curl_easy_cleanup( curl );
   return rc;
}


// download a manifest
int fs_entry_download_manifest( struct fs_core* core, char const* manifest_url, Serialization::ManifestMsg* mmsg ) {
   CURL* curl = curl_easy_init();
   
   int rc = md_download_manifest( core->conf, curl, manifest_url, mmsg );
   
   curl_easy_cleanup( curl );
   return rc;
}


// download a block 
ssize_t fs_entry_download_block( struct fs_core* core, char const* block_url, char** block_bits, size_t block_len ) {

   CURL* curl = curl_easy_init();
   
   int rc = md_download_block( core->conf, curl, block_url, block_bits, block_len );
   
   curl_easy_cleanup( curl );
   return rc;
}



// set up a write message
int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type ) {
   struct ms_client* client = core->ms;
   
   writeMsg->set_type( type );
   writeMsg->set_volume_version( ms_client_volume_version( client, core->volume ) );
   writeMsg->set_cert_version( ms_client_cert_version( client, core->volume ) );
   writeMsg->set_closure_version( ms_client_closure_version( client, core->volume ) );
   writeMsg->set_user_id( core->conf->owner );
   writeMsg->set_volume_id( core->volume );
   writeMsg->set_gateway_id( core->conf->gateway );
   return 0;
}


// sign a write message
int fs_entry_sign_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core ) {
   return md_sign<Serialization::WriteMsg>( core->ms->my_key, writeMsg );
}

// set up a PREPARE message
// NEED TO AT LEAST READ-LOCK fent
int fs_entry_prepare_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_id, uint64_t end_id, int64_t* versions ) {
   
   fs_entry_init_write_message( writeMsg, core, Serialization::WriteMsg::PREPARE );
   
   Serialization::FileMetadata* file_md = writeMsg->mutable_metadata();
   Serialization::BlockList* block_list = writeMsg->mutable_blocks();

   file_md->set_fs_path( string(fs_path) );
   file_md->set_volume_id( core->volume );
   file_md->set_file_id( fent->file_id );
   file_md->set_file_version( fent->version );
   file_md->set_size( fent->size );
   file_md->set_mtime_sec( fent->mtime_sec );
   file_md->set_mtime_nsec( fent->mtime_nsec );
   file_md->set_gateway_id( fent->coordinator );
   
   block_list->set_start_id( start_id );
   block_list->set_end_id( end_id );

   for( uint64_t i = 0; i < end_id - start_id; i++ ) {
      block_list->add_version( versions[i] );
   }

   return 0;
}


// CURL method to quickly buffer up a response
static size_t fs_entry_response_buffer( char* ptr, size_t size, size_t nmemb, void* userdata ) {
   response_buffer_t* respBuf = (response_buffer_t*)userdata;

   size_t tot = size * nmemb;

   char* ptr_dup = CALLOC_LIST( char, tot );
   memcpy( ptr_dup, ptr, tot );

   respBuf->push_back( buffer_segment_t( ptr_dup, tot ) );

   return tot;
}


// send off a write message, and get back the received write message
int fs_entry_post_write( Serialization::WriteMsg* recvMsg, struct fs_core* core, uint64_t gateway_id, Serialization::WriteMsg* sendMsg ) {
   CURL* curl_h = curl_easy_init();
   response_buffer_t buf;

   char* content_url = ms_client_get_UG_content_url( core->ms, core->volume, gateway_id );
   if( content_url == NULL ) {
      errorf("No such Gateway %" PRIu64 "\n", gateway_id );
      curl_easy_cleanup( curl_h );
      return -EINVAL;
   }

   md_init_curl_handle( curl_h, content_url, core->conf->metadata_connect_timeout );
   curl_easy_setopt( curl_h, CURLOPT_POST, 1L );
   curl_easy_setopt( curl_h, CURLOPT_WRITEFUNCTION, fs_entry_response_buffer );
   curl_easy_setopt( curl_h, CURLOPT_WRITEDATA, &buf );
   curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, (core->conf->verify_peer ? 1L : 0L) );

   int rc = fs_entry_sign_write_message( sendMsg, core );
   if( rc != 0 ) {
      errorf("fs_entry_sign_write_message rc = %d\n", rc );
      curl_easy_cleanup( curl_h );
      return rc;
   }
   
   string msg_data_str;
   bool valid = false;
   
   try {
      valid = sendMsg->SerializeToString( &msg_data_str );
   }
   catch( exception e ) {
      valid = false;
   }
   
   if( !valid ) {
      errorf("%s", "Failed to serialize message\n");
      curl_easy_cleanup( curl_h );
      return -EINVAL;
   }
   
   struct curl_httppost *post = NULL, *last = NULL;
   curl_formadd( &post, &last, CURLFORM_PTRNAME, "WriteMsg", CURLFORM_PTRCONTENTS, msg_data_str.data(), CURLFORM_CONTENTSLENGTH, msg_data_str.size(), CURLFORM_END );

   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, post );

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   dbprintf( "send WriteMsg type %d length %zu\n", sendMsg->type(), msg_data_str.size() );
   rc = curl_easy_perform( curl_h );

   END_TIMING_DATA( ts, ts2, "Remote write" );
   
   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, NULL );
   curl_formfree( post );
   
   if( rc != 0 ) {
      // could not perform
      response_buffer_free( &buf );
      curl_easy_cleanup( curl_h );
      free( content_url );
      return rc;
   }
   else {
      long http_status = 0;
      curl_easy_getinfo( curl_h, CURLINFO_RESPONSE_CODE, &http_status );

      if( http_status != 200 ) {
         errorf( "remote HTTP response %ld\n", http_status );

         if( http_status == 202 ) {
            // got back an error code
            char* resp = response_buffer_to_string( &buf );
            char* tmp = NULL;
            long ret = strtol( resp, &tmp, 10 );
            if( tmp != NULL ) {
               rc = ret;
            }
            else {
               errorf("Incoherent error message '%s'\n", resp);
               rc = -EREMOTEIO;
            }
         }
         else {
            rc = -EREMOTEIO;
         }

         response_buffer_free( &buf );
         curl_easy_cleanup( curl_h );
         free( content_url );

         return rc;
      }

      // got back a message--parse it
      char* msg_buf = response_buffer_to_string( &buf );

      // extract the messsage
      string msg_buf_str( msg_buf, response_buffer_size( &buf ) );

      bool valid = false;

      try {
         valid = recvMsg->ParseFromString( msg_buf_str );
      }
      catch( exception e ) {
         errorf("failed to parse response from %s, caught exception\n", content_url);
      }

      free( msg_buf );
      response_buffer_free( &buf );
      curl_easy_cleanup( curl_h );

      if( !valid ) {
         // not a valid message
         rc = -EBADMSG;
         errorf("recv bad message from %s\n", content_url);
      }
      else {
         rc = 0;
         dbprintf( "recv WriteMsg type %d\n", recvMsg->type() );
         
         // send the MS-related header to our client
         ms_client_process_header( core->ms, core->volume, recvMsg->volume_version(), recvMsg->cert_version(), recvMsg->closure_version() );
      }
   }

   free( content_url );

   return rc;
}

