/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "network.h"
#include "manifest.h"
#include "url.h"
#include "collator.h"

// download data, first from the local proxy, then from the CDN on failure, and then directly on failure.
// return 0 on success
// return negative on error 
int fs_entry_download_cached( struct fs_core* core, char const* url, char** bits, ssize_t* ret_len ) {
   char* cdn_url = md_cdn_url( url );
   
   ssize_t len = 0;
   int status_code = 0;
   
   if( core->conf->proxy_url ) {
      len = md_download_file_proxied( cdn_url, bits, core->conf->proxy_url, &status_code );
      if( len < 0 ) {
         errorf("md_download_file_proxied(%s) from %s rc = %zd\n", cdn_url, core->conf->proxy_url, len);
      }
      else if ( status_code != 200 ) {
         errorf("md_download_file_proxied(%s) from %s HTTP status %d\n", cdn_url, core->conf->proxy_url, status_code );
         len = -status_code;

         if( *bits ) {
            free( *bits );
            *bits = NULL;
         }
         *ret_len = -1;
      }
   }
   else {
      len = md_download_file( cdn_url, bits, &status_code );
      if( len < 0 ) {
         errorf("md_download_file(%s) rc = %zd\n", cdn_url, len );
      }
      if( status_code != 200 ) {
         errorf("md_download_file(%s) HTTP status %d\n", cdn_url, status_code );
         len = -status_code;

         if( *bits ) {
            free( *bits );
            *bits = NULL;
         }
         *ret_len = -1;
      }
   }

   free( cdn_url );

   if( len > 0 ) {
      *ret_len = len;
   }

   return (int)len;
}

// download a manifest
int fs_entry_download_manifest( struct fs_core* core, char const* manifest_url, Serialization::ManifestMsg* mmsg ) {

   char* manifest_data = NULL;
   ssize_t manifest_data_len = 0;
   int rc = 0;

   rc = fs_entry_download_cached( core, manifest_url, &manifest_data, &manifest_data_len );

   if( rc < 0 ) {
      errorf( "fs_entry_download_cached(%s) rc = %d\n", manifest_url, rc );

      // try it manually
      int status_code = 0;
      manifest_data_len = md_download_file( manifest_url, &manifest_data, &status_code );
      
      if( manifest_data_len < 0 ) {
         errorf( "md_download_file(%s) rc = %zd\n", manifest_url, manifest_data_len );

         // try replicas of this manifest 
         rc = (int)manifest_data_len;
      }
      else if( status_code != 200 ) {
         errorf("md_download_file(%s) HTTP status %d\n", manifest_url, status_code );
         rc = -status_code;
      }
      else {
         // success!
         rc = 0;
      }
   }

   if( rc >= 0 ) {
      // got data!  parse it
      bool valid = mmsg->ParseFromString( string(manifest_data, manifest_data_len) );
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
ssize_t fs_entry_download_block( struct fs_core* core, char const* block_url, char* block_bits ) {

   ssize_t nr = 0;

   char* tmpbuf = NULL;

   dbprintf("fetch '%s'\n", block_url );
   
   int ret = fs_entry_download_cached( core, block_url, &tmpbuf, &nr );
   
   if( ret >= 0 && nr <= (signed)core->conf->blocking_factor ) {
      memcpy( block_bits, tmpbuf, nr );
   }
   else if( ret < 0 && ret > -200 ) {
      // it is possible that this block was just collated back to the origin.
      // attempt to download from there
      errorf( "fs_entry_download_cached(%s) rc = %zd\n", block_url, nr );

      int status_code = 0;
      nr = md_download_file( block_url, &tmpbuf, &status_code );
      
      if( nr < 0 || status_code != 200 ) {

         if( nr < 0 ) {
            errorf( "md_download_file(%s) rc = %zd\n", block_url, nr );
         }
         else if( status_code != 200 ) {
            errorf( "md_download_file(%s) HTTP status %d\n", block_url, status_code );
            nr = -ENODATA;
         }
         
      }
      else if( nr >= 0 && nr <= (signed)core->conf->blocking_factor ) {
         memcpy( block_bits, tmpbuf, nr );
      }
      else {
         errorf( "md_download_file(%s) invalid size %zd\n", block_url, nr );
         nr = -EIO;
      }
   }
   else if( ret > -200 ) {
      errorf( "fs_entry_download_cached(%s) invalid size %zd\n", block_url, nr );
      nr = -ENODATA;
   }
   else {
      errorf( "fs_entry_download_cached(%s) HTTP status %d\n", block_url, -ret );
      nr = -ENODATA;
   }

   free( tmpbuf );

   return nr;
}



// set up a write message
int fs_entry_init_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, Serialization::WriteMsg_MsgType type ) {
   writeMsg->set_type( type );
   writeMsg->set_write_id( core->col->next_transaction_id() );
   writeMsg->set_session_id( core->col->get_session_id() );
   writeMsg->set_user_id( core->conf->owner );
   writeMsg->set_volume_id( core->conf->volume );

   return 0;
}

// set up a PREPARE message
// NEED TO AT LEAST READ-LOCK fent
int fs_entry_prepare_write_message( Serialization::WriteMsg* writeMsg, struct fs_core* core, char* fs_path, struct fs_entry* fent, uint64_t start_id, uint64_t end_id, int64_t* versions ) {
   if( fs_path == NULL ) {
      errorf("%s", "fs_path == NULL\n");
      return -EINVAL;
   }
   if( core->conf->content_url == NULL ) {
      errorf("%s", "core->conf->content_url == NULL\n");
      return -EINVAL;
   }
   
   fs_entry_init_write_message( writeMsg, core, Serialization::WriteMsg::PREPARE );
   
   Serialization::FileMetadata* file_md = writeMsg->mutable_metadata();
   Serialization::BlockList* block_list = writeMsg->mutable_blocks();

   file_md->set_fs_path( string(fs_path) );
   file_md->set_file_version( fent->version );
   file_md->set_size( fent->size );
   file_md->set_mtime_sec( fent->mtime_sec );
   file_md->set_mtime_nsec( fent->mtime_nsec );
   file_md->set_content_url( string( core->conf->content_url ) );
   
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
int fs_entry_post_write( Serialization::WriteMsg* recvMsg, struct fs_core* core, char* url, Serialization::WriteMsg* sendMsg ) {
   CURL* curl_h = curl_easy_init();
   response_buffer_t buf;

   md_init_curl_handle( curl_h, url, core->conf->metadata_connect_timeout );
   curl_easy_setopt( curl_h, CURLOPT_POST, 1L );
   curl_easy_setopt( curl_h, CURLOPT_WRITEFUNCTION, fs_entry_response_buffer );
   curl_easy_setopt( curl_h, CURLOPT_WRITEDATA, &buf );
   curl_easy_setopt( curl_h, CURLOPT_SSL_VERIFYPEER, (core->conf->verify_peer ? 1L : 0L) );

   string msg_data_str;
   sendMsg->SerializeToString( &msg_data_str );

   struct curl_httppost *post = NULL, *last = NULL;
   curl_formadd( &post, &last, CURLFORM_PTRNAME, "WriteMsg", CURLFORM_PTRCONTENTS, msg_data_str.data(), CURLFORM_CONTENTSLENGTH, msg_data_str.size(), CURLFORM_END );

   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, post );

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   dbprintf( "send WriteMsg type %d\n", sendMsg->type() );
   int rc = curl_easy_perform( curl_h );

   END_TIMING_DATA( ts, ts2, "MS update" );
   
   curl_easy_setopt( curl_h, CURLOPT_HTTPPOST, NULL );
   curl_formfree( post );
   
   if( rc != 0 ) {
      // could not perform
      response_buffer_free( &buf );
      curl_easy_cleanup( curl_h );
      return rc;
   }
   else {
      curl_easy_getinfo( curl_h, CURLINFO_RESPONSE_CODE, &rc );
      curl_easy_cleanup( curl_h );

      if( rc != 200 ) {
         errorf( "remote HTTP response %d\n", rc );
         rc = -EREMOTEIO;

         response_buffer_free( &buf );
         return rc;
      }

      // got back a message--parse it
      char* msg_buf = response_buffer_to_string( &buf );

      // extract the messsage
      string msg_buf_str( msg_buf, response_buffer_size( &buf ) );

      Serialization::WriteMsg promise_msg;
      bool valid = recvMsg->ParseFromString( msg_buf_str );

      free( msg_buf );
      response_buffer_free( &buf );

      if( !valid ) {
         // not a valid message
         rc = -EBADMSG;
         errorf("%s", " recv bad message\n");
      }
      else {
         rc = 0;
         dbprintf( "recv WriteMsg type %d\n", recvMsg->type() );
      }
   }

   return rc;
}

