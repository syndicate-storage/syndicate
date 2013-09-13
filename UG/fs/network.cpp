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
   
   ssize_t len = 0;
   long status_code = 0;
   int rc = 0;

   md_init_curl_handle( curl, url, core->conf->metadata_connect_timeout );

   if( proxy ) {
      curl_easy_setopt( curl, CURLOPT_PROXY, proxy );
   }

   struct md_bound_response_buffer brb;
   brb.max_size = max_len;
   brb.size = 0;
   brb.rb = new response_buffer_t();
   
   char* tmpbuf = NULL;
   
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)&brb );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_bound_response_buffer );
   
   rc = curl_easy_perform( curl );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &status_code );

   if( rc < 0 ) {
      errorf("md_download_file6(%s, proxy=%s) rc = %d\n", url, proxy, rc );
      rc = -EIO;
   }
   else {
      tmpbuf = response_buffer_to_string( brb.rb );
      len = response_buffer_size( brb.rb );
      
      if( status_code != 200 ) {
         if( status_code == 202 ) {
            // error code from remote host
            char* tmp = NULL;
            long errcode = strtol( tmpbuf, &tmp, 10 );
            if( tmp == tmpbuf ) {
               // failed to parse
               char errbuf[101];
               strncpy( errbuf, tmpbuf, 100 );
               
               errorf("md_download_file5(%s, proxy=%s): Invalid error response (truncated): '%s'...\n", url, proxy, errbuf );

               rc = -EREMOTEIO;
            }
            else {
               errorf("md_download_file5(%s, proxy=%s): remote gateway error: %d\n", url, proxy, (int)errcode );
               rc = -abs((int)errcode);
            }
         }
         else {
            errorf("md_download_file5(%s, proxy=%s): HTTP status code %d\n", url, proxy, (int)status_code );
            rc = status_code;
         }
      }
   }
   
   response_buffer_free( brb.rb );
   delete brb.rb;

   if( rc == 0 ) {
      *bits = tmpbuf;
      *ret_len = len;
   }
   else {
      *bits = NULL;
      *ret_len = -1;
      
      if( tmpbuf ) {
         free( tmpbuf );
      }
   }

   return rc;
}


// download data, trying in order:
// * both CDN and proxy
// * from proxy
// * from CDN
// * from gateway
int fs_entry_download_cached( struct fs_core* core, char const* url, char** bits, ssize_t* ret_len, ssize_t max_len ) {
   CURL* curl = curl_easy_init();

   int rc = 0;

   char* cdn_url = md_cdn_url( url );
   char* proxy_url = core->conf->proxy_url;
   
   char const* proxy_urls[10];
   memset( proxy_urls, 0, sizeof(char const*) * 10 );

   if( core->conf->proxy_url ) {
      // try CDN and Proxy
      proxy_urls[0] = cdn_url;
      proxy_urls[1] = proxy_url;

      // try proxy only
      proxy_urls[2] = url;
      proxy_urls[3] = proxy_url;

      // try CDN only
      proxy_urls[4] = cdn_url;
      proxy_urls[5] = NULL;

      // try direct
      proxy_urls[6] = url;
      proxy_urls[7] = NULL;
   }
   else {
      // try CDN only
      proxy_urls[0] = cdn_url;
      proxy_urls[1] = NULL;

      // try direct
      proxy_urls[2] = url;
      proxy_urls[3] = NULL;
   }

   for( int i = 0; true; i++ ) {
      char const* target_url = proxy_urls[2*i];
      char const* target_proxy = proxy_urls[2*i + 1];

      if( target_url == NULL && target_proxy == NULL )
         break;

      rc = fs_entry_download( core, curl, target_proxy, target_url, bits, ret_len, max_len );
      if( rc == 0 ) {
         // success!
         break;
      }
      if( rc < 0 ) {
         // irrecoverable error
         errorf("fs_entry_download(%s, CDN_url=%s, proxy=%s) rc = %d\n", url, target_url, target_proxy, rc );
         break;
      }

      // try again--got an HTTP status code we didn't understand
      dbprintf("fs_entry_download(%s, CDN_url=%s, proxy=%s) HTTP status code = %d\n", url, target_url, target_proxy, rc );
   }

   free( cdn_url );
   curl_easy_cleanup( curl );
   
   return rc;
}


// download a manifest
int fs_entry_download_manifest( struct fs_core* core, char const* manifest_url, Serialization::ManifestMsg* mmsg ) {

   char* manifest_data = NULL;
   ssize_t manifest_data_len = 0;
   int rc = 0;

   rc = fs_entry_download_cached( core, manifest_url, &manifest_data, &manifest_data_len, 100000 );     // maximum manifest size is ~100KB
   
   if( rc != 0 ) {
      errorf( "fs_entry_download_cached(%s) rc = %d\n", manifest_url, rc );
      return rc;
   }

   else {
      // got data!  parse it
      bool valid = false;
      try {
         valid = mmsg->ParseFromString( string(manifest_data, manifest_data_len) );
      }
      catch( exception e ) {
         errorf("failed to parse manifest %s, caught exception\n", manifest_url);
         rc = -EIO;
      }
      
      if( !valid ) {
         errorf( "invalid manifest (%zd bytes)\n", manifest_data_len );
         rc = -EIO;
      }
   }

   if( manifest_data )
      free( manifest_data );

   return rc;
}


// repeatedly try to download a file's block, starting with its primary URL, and trying replicas subsequently
ssize_t fs_entry_download_block( struct fs_core* core, char const* block_url, char** block_bits, size_t block_len ) {

   ssize_t nr = 0;
   char* block_buf = NULL;
   
   dbprintf("fetch '%s'\n", block_url );
   
   int ret = fs_entry_download_cached( core, block_url, &block_buf, &nr, block_len );
   if( ret == 0 ) {
      // success
      *block_bits = block_buf;
      return nr;
   }
   
   if( ret == 204 ) {
      // signal from AG that it's not ready yet
      errorf("fs_entry_download_cached(%s) rc = %d\n", block_url, ret );
      *block_bits = NULL;
      return -EAGAIN;
   }

   if( ret > 0 ) {
      // bad HTTP code
      errorf("fs_entry_download_cached(%s) HTTP status code %d\n", block_url, ret );
      *block_bits = NULL;
   }
   return nr;
}



// set up a write message
int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type ) {
   struct ms_client* client = core->ms;
   
   writeMsg->set_type( type );
   writeMsg->set_volume_version( ms_client_volume_version( client, core->volume ) );
   writeMsg->set_ug_version( ms_client_UG_version( client, core->volume ) );
   writeMsg->set_rg_version( ms_client_RG_version( client, core->volume ) );
   writeMsg->set_ag_version( ms_client_AG_version( client, core->volume ) );
   writeMsg->set_user_id( core->conf->owner );
   writeMsg->set_volume_id( core->volume );
   writeMsg->set_gateway_id( core->conf->gateway );
   return 0;
}


// sign a write message
int fs_entry_sign_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core ) {
   return md_sign<Serialization::WriteMsg>( core->ms->my_key, writeMsg );
   /*
   writeMsg->set_signature( string("") );

   string data;
   bool valid = false;
   
   try {
      valid = writeMsg->SerializeToString( &data );
   }
   catch( exception e ) {
      valid = false;
   }
   
   if( !valid )
      return -EINVAL;

   struct ms_client* client = core->ms;

   char* sigb64 = NULL;
   size_t sigb64len = 0;
   
   int rc = md_sign_message( client->my_key, data.data(), data.size(), &sigb64, &sigb64len );
   if( rc != 0 ) {
      errorf("md_sign_message rc = %d\n", rc );
      return rc;
   }

   writeMsg->set_signature( string(sigb64, sigb64len) );

   free( sigb64 );
   return 0;
   */
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
         ms_client_process_header( core->ms, core->volume, recvMsg->volume_version(), recvMsg->ug_version(), recvMsg->rg_version(), recvMsg->ag_version() );
      }
   }

   free( content_url );

   return rc;
}

