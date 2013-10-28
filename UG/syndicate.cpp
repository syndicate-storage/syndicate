/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "syndicate.h"


struct syndicate_state *global_state = NULL;

// connection initialization handler for embedded HTTP server
void* syndicate_HTTP_connect( struct md_HTTP_connection_data* md_con_data ) {
   struct syndicate_connection* syncon = CALLOC_LIST( struct syndicate_connection, 1 );
   syncon->state = global_state;
   return syncon;
}

// HTTP authentication callback (does nothing, since we verify the message signature)
uint64_t syndicate_HTTP_authenticate( struct md_HTTP_connection_data* md_con_data, char* username, char* password ) {
   return 0;
}

// process the beginning of a HEAD or POST
static int syndicate_begin_read_request( struct md_HTTP_connection_data* md_con_data, struct md_HTTP_response* resp, struct gateway_request_data* reqdat, struct stat* sb ) {

   char* url = md_con_data->url_path;
   struct md_HTTP* http = md_con_data->http;
   struct syndicate_connection* syncon = (struct syndicate_connection*)md_con_data->cls;
   struct syndicate_state* state = syncon->state;

   dbprintf( "read on %s\n", url);
   
   int rc = http_parse_request( http, resp, reqdat, url );
   if( rc < 0 ) {
      dbprintf("http_parse_request(%s) rc = %d\n", url, rc );
      // error, but handled
      return 0;
   }

   // handle 302, 400, and 404
   rc = http_handle_redirect( state, resp, sb, reqdat );
   if( rc <= 0 ) {
      // handled!
      dbprintf("http_handle_redirect(%s) rc = %d\n", url, rc );
      gateway_request_data_free( reqdat );
      return 0;
   }

   if( rc == HTTP_REDIRECT_REMOTE ) {
      // requested object is not here.
      // will not redirect; that can lead to loops.
      errorf("ERR: Requested object %s is not local.  Will not redirect to avoid loops.\n", reqdat->fs_path );
      md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
      gateway_request_data_free( reqdat );
      return 0;
   }

   // check authorization
   if( !(sb->st_mode & 0044) ) {
      // not volume-readable or world readable
      errorf("ERR: Object %s is not volume-readable or world-readable (mode %o)\n", reqdat->fs_path, sb->st_mode );
      md_create_HTTP_response_ram_static( resp, "text/plain", 404, MD_HTTP_404_MSG, strlen(MD_HTTP_404_MSG) + 1 );
      gateway_request_data_free( reqdat );
      return 0;
   }

   // if this is a request for a directory, then bail
   if( S_ISDIR( sb->st_mode ) ) {
      errorf("ERR: Object %s is a directory\n", reqdat->fs_path );
      md_create_HTTP_response_ram_static( resp, "text/plain", 400, MD_HTTP_400_MSG, strlen(MD_HTTP_400_MSG) + 1 );
      gateway_request_data_free( reqdat );
      return 0;
   }

   // not handled
   return 1;
}


// HTTP HEAD handler
struct md_HTTP_response* syndicate_HTTP_HEAD_handler( struct md_HTTP_connection_data* md_con_data ) {

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   struct gateway_request_data reqdat;
   struct stat sb;
   
   int rc = syndicate_begin_read_request( md_con_data, resp, &reqdat, &sb );
   if( rc == 0 ) {
      // handled
      return 0;
   }

   // this object is local and is a file
   md_create_HTTP_response_ram_static( resp, "text/plain", 200, MD_HTTP_200_MSG, strlen(MD_HTTP_200_MSG) + 1 );
   http_make_default_headers( resp, sb.st_mtime, sb.st_size, true );

   gateway_request_data_free( &reqdat );
   
   return resp;
}


// HTTP GET handler
struct md_HTTP_response* syndicate_HTTP_GET_handler( struct md_HTTP_connection_data* md_con_data ) {

   struct syndicate_connection* syncon = (struct syndicate_connection*)md_con_data->cls;
   struct syndicate_state* state = syncon->state;

   struct md_HTTP_response* resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   // parse the url_path into its constituent components
   struct gateway_request_data reqdat;
   struct stat sb;

   int rc = syndicate_begin_read_request( md_con_data, resp, &reqdat, &sb );
   if( rc == 0 ) {
      // handled already
      return resp;
   }

   // not handled
   // what is this a request for?
   // a block?
   if( reqdat.block_id != INVALID_BLOCK_ID ) {
      // serve back the block
      char* block = CALLOC_LIST( char, state->core->blocking_factor );

      ssize_t size = fs_entry_read_block( state->core, reqdat.fs_path, reqdat.block_id, block, state->core->blocking_factor );
      if( size < 0 ) {
         errorf( "fs_entry_read_block(%s.%" PRId64 "/%" PRIu64 ".%" PRId64 ") rc = %zd\n", reqdat.fs_path, reqdat.file_version, reqdat.block_id, reqdat.block_version, size );
         md_create_HTTP_response_ram( resp, "text/plain", 500, "INTERNAL SERVER ERROR\n", strlen("INTERNAL SERVER ERROR\n") + 1 );

         gateway_request_data_free( &reqdat );
         free( block );
         return resp;
      }
      else {
         dbprintf( "served %zd bytes from %s.%" PRId64 "/%" PRIu64 ".%" PRId64 "\n", size, reqdat.fs_path, reqdat.file_version, reqdat.block_id, reqdat.block_version );
      }
      
      md_create_HTTP_response_ram_nocopy( resp, "application/octet-stream", 200, block, size );
      http_make_default_headers( resp, sb.st_mtime, size, true );

      gateway_request_data_free( &reqdat );
      return resp;
   }

   // request for a file or file manifest?
   else {
      if( reqdat.manifest_timestamp.tv_sec > 0 && reqdat.manifest_timestamp.tv_nsec > 0 ) {
         // request for a manifest
         // get the manifest and reply it
         char* manifest_txt = NULL;
         ssize_t manifest_txt_len = fs_entry_serialize_manifest( state->core, reqdat.fs_path, &manifest_txt, true );
         
         if( manifest_txt_len > 0 ) {
            md_create_HTTP_response_ram_nocopy( resp, "text/plain", 200, manifest_txt, manifest_txt_len );
            http_make_default_headers( resp, sb.st_mtime, manifest_txt_len, true );

            dbprintf( "served manifest %s.%" PRId64 "/manifest.%ld.%ld, %zd bytes\n", reqdat.fs_path, reqdat.file_version, reqdat.manifest_timestamp.tv_sec, reqdat.manifest_timestamp.tv_nsec, manifest_txt_len );
         }
         else {
            char buf[100];
            snprintf(buf, 100, "fs_entry_serialize_manifest rc = %zd\n", manifest_txt_len );
            http_io_error_resp( resp, 500, buf );
         }

         gateway_request_data_free( &reqdat );
         
         return resp;
      }
      else {

         // TODO: request for a file
         // redirect to its manifest for now
         md_create_HTTP_response_ram_static( resp, "text/plain", 400, "INVALID REQUEST", strlen("INVALID REQUEST") + 1 );
         gateway_request_data_free( &reqdat );

         return resp;
      }
   }

   // unreachable
   return NULL;
}


// extract some useful information from the message
bool syndicate_extract_file_info( Serialization::WriteMsg* msg, char const** fs_path, uint64_t* file_id, uint64_t* coordinator_id, int64_t* file_version ) {
   switch( msg->type() ) {
      case Serialization::WriteMsg::ACCEPTED: {
         dbprintf("%s", "Got ACCEPTED\n");
         if( !msg->has_accepted() ) {
            return false;
         }
         
         *file_id = msg->accepted().file_id();
         *coordinator_id = msg->accepted().coordinator_id();
         *fs_path = msg->accepted().fs_path().c_str();
         *file_version = msg->accepted().file_version();
         break;
      }
      case Serialization::WriteMsg::PREPARE: {
         dbprintf("%s", "Got PREPARE\n");
         if( !msg->has_metadata() || !msg->has_blocks() ) {
            return false;
         }

         *file_id = msg->metadata().file_id();
         *coordinator_id = msg->metadata().coordinator_id();
         *fs_path = msg->metadata().fs_path().c_str();
         *file_version = msg->metadata().file_version();

         break;
      }

      case Serialization::WriteMsg::TRUNCATE: {
         dbprintf("%s", "Got TRUNCATE\n");
         if( !msg->has_truncate() ) {
            errorf("%s", "msg has no truncate block\n");
            return false;
         }
         if( !msg->has_blocks() ) {
            errorf("%s", "msg has no blocks\n");
            return false;
         }

         *file_id = msg->truncate().file_id();
         *coordinator_id = msg->truncate().coordinator_id();
         *fs_path = msg->truncate().fs_path().c_str();
         *file_version = msg->truncate().file_version();

         break;
      }

      case Serialization::WriteMsg::DETACH: {
         dbprintf("%s", "Got DETACH\n");
         if( !msg->has_detach() ) {
            return false;
         }

         *file_id = msg->detach().file_id();
         *coordinator_id = msg->detach().coordinator_id();
         *fs_path = msg->detach().fs_path().c_str();
         *file_version = msg->detach().file_version();

         break;
      }

      default: {
         // unknown
         errorf( "unknown message type %d\n", msg->type() );
         return false;
      }
   }

   return true;
}


// make an HTTP response from a protobuf ack
void syndicate_make_msg_ack( struct md_HTTP_connection_data* md_con_data, Serialization::WriteMsg* ack ) {
   string ack_str;
   char const* ack_txt = NULL;
   size_t ack_txt_len = 0;

   struct syndicate_connection* syncon = (struct syndicate_connection*)md_con_data->cls;
   struct syndicate_state *state = syncon->state;
   struct ms_client* client = state->ms;

   ack->set_signature( string("") );

   ack->SerializeToString( &ack_str );

   md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );

   // sign this message
   ack_txt = ack_str.data();
   ack_txt_len = ack_str.size();

   char* sigb64 = NULL;
   size_t sigb64_len = 0;

   int rc = md_sign_message( client->my_key, ack_txt, ack_txt_len, &sigb64, &sigb64_len );
   if( rc != 0 ) {
      errorf("md_sign_message rc = %d\n", rc );
      md_create_HTTP_response_ram_static( md_con_data->resp, "text/plain", 500, MD_HTTP_500_MSG, strlen(MD_HTTP_500_MSG) + 1 );
      return;
   }

   ack->set_signature( string(sigb64, sigb64_len) );

   ack->SerializeToString( &ack_str );

   ack_txt = ack_str.data();
   ack_txt_len = ack_str.size();

   dbprintf( "ack message of length %zu\n", ack_txt_len );
   md_create_HTTP_response_ram( md_con_data->resp, "application/octet-stream", 200, ack_txt, ack_txt_len );

   free( sigb64 );
}


// fill a WriteMsg with a list of blocks
void syndicate_make_promise_msg( Serialization::WriteMsg* writeMsg ) {
   writeMsg->set_type( Serialization::WriteMsg::PROMISE );
}


// extract and verify a write message
int syndicate_parse_write_message( struct syndicate_state* state, Serialization::WriteMsg* msg, char* msg_buf, size_t msg_sz ) {
   // extract the actual message
   bool valid = false;

   try {
      valid = msg->ParseFromString( string(msg_buf, msg_sz) );
   }
   catch( exception e ) {
      errorf("%s", "failed to parse message, caught exception\n" );
      return -EINVAL;
   }

   if( !valid ) {
      errorf("%s", "Invalid message\n");
      return -EINVAL;
   }

   // verify message

   struct ms_client* client = state->ms;
   char* sigb64 = strdup( msg->signature().c_str() );
   size_t sigb64_len = msg->signature().size();

   msg->set_signature( string("") );

   string msg_data;
   valid = msg->SerializeToString( &msg_data );
   if( !valid ) {
      // something's seriously wrong
      errorf("%s", "failed to serialize message\n");
      free( sigb64 );
      return -EINVAL;
   }

   // which gateway sent this?  Find its public key
   int rc = ms_client_verify_gateway_message( client, state->core->volume, msg->gateway_id(), msg_data.c_str(), msg_data.size(), sigb64, sigb64_len );
   if( rc != 0 ) {
      // not valid
      errorf("ms_client_verify_gateway_message rc = %d\n", rc );
      free( sigb64 );
      return rc;
   }

   free( sigb64 );

   return 0;
}


// POST finish--extract the pending message and handle it
void syndicate_HTTP_POST_finish( struct md_HTTP_connection_data* md_con_data ) {

   struct syndicate_connection* syncon = (struct syndicate_connection*)md_con_data->cls;
   struct syndicate_state *state = syncon->state;
   response_buffer_t* rb = md_con_data->rb;
   Serialization::WriteMsg ack;

   // prepare an ACK
   char const* fs_path = NULL;
   uint64_t file_id = 0;
   uint64_t coordinator_id = 0;
   int64_t file_version = -1;
   int rc = 0;
   bool no_ack = false;

   char* msg_buf = response_buffer_to_string( rb );
   size_t msg_sz = response_buffer_size( rb );
   Serialization::WriteMsg *msg = new Serialization::WriteMsg();

   dbprintf("received message of length %zu\n", msg_sz);

   rc = syndicate_parse_write_message( state, msg, msg_buf, msg_sz );

   free( msg_buf );

   if( rc != 0 ) {
      // can't handle this
      errorf( "syndicate_parse_write_message rc = %d\n", rc );

      md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );
      
      if( rc == -EAGAIN ) {
         // tell the remote gateway to try again, while we refresh our UG listing with the MS
         char buf[10];
         sprintf(buf, "%d", rc);
         md_create_HTTP_response_ram( md_con_data->resp, "text/plain", 202, buf, strlen(buf) + 1);

         ms_client_sched_volume_reload( state->ms );
      }
      else {
         md_create_HTTP_response_ram( md_con_data->resp, "text/plain", 400, "INVALID REQUEST", strlen("INVALID REQUEST") + 1 );
      }

      delete msg;
      
      return;
   }
   
   fs_entry_init_write_message( &ack, state->core, Serialization::WriteMsg::ERROR );

   if( !syndicate_extract_file_info( msg, &fs_path, &file_id, &coordinator_id, &file_version ) ) {
      // missing information
      ack.set_errorcode( -EBADMSG );
      ack.set_errortxt( string("Missing message data") );

      errorf( "%s", "extract_file_info failed\n" );
      syndicate_make_msg_ack( md_con_data, &ack );

      delete msg;
      return;
   }

   int64_t current_version = fs_entry_get_version( state->core, fs_path );
   if( current_version < 0 ) {
      // couldn't find file
      ack.set_errorcode( current_version );
      ack.set_errortxt( string("Could not determine version") );

      errorf( "fs_entry_get_version(%s) rc = %" PRId64 "\n", fs_path, current_version);
      syndicate_make_msg_ack( md_con_data, &ack );

      delete msg;
      return;
   }

   dbprintf( "got WriteMsg(type=%d, path=%s)\n", msg->type(), fs_path );

   // handle the message
   switch( msg->type() ) {
      case Serialization::WriteMsg::PREPARE: {
         // got a PREPARE message; we'll need to respond with a PROMISE message
         // and update the appropriate file manifest to redirect to this client for the block data.
         // Also, begin downloading the affected blocks

         // update this file's manifest (republishing it in the process)
         rc = fs_entry_remote_write( state->core, fs_path, file_id, coordinator_id, msg );
         if( rc == 0 ) {
            // create a PROMISE request--ask the remote writer to hold on to the blocks so we can collate them later
            syndicate_make_promise_msg( &ack );
         }
         else {

            // error
            errorf( "fs_entry_handle_remote_write(%s) rc = %d\n", fs_path, rc );
            
            ack.set_errorcode( rc );
            ack.set_errortxt( string("failed to update manifest") );
         }

         break;
      }

      case Serialization::WriteMsg::TRUNCATE: {
         // received a truncate message.
         // Truncate the file, and if successful,
         // send back an ACCEPTED

         rc = fs_entry_versioned_truncate( state->core, fs_path, file_id, coordinator_id, msg->truncate().size(), file_version, msg->user_id(), msg->volume_id(), true );
         if( rc == 0 ) {
            ack.set_type( Serialization::WriteMsg::ACCEPTED );
         }
         else {
            ack.set_errorcode( rc );
            ack.set_errortxt( string("failed to truncate") );
         }

         break;
      }

      case Serialization::WriteMsg::DETACH: {
         // received DETACH request.
         // Unlink this file, and send back an ACCEPTED
         rc = fs_entry_versioned_unlink( state->core, fs_path, file_id, coordinator_id, file_version, msg->user_id(), msg->volume_id(), true );
         if( rc == 0 ) {
            ack.set_type( Serialization::WriteMsg::ACCEPTED );
         }
         else {
            ack.set_errorcode( rc );
            ack.set_errortxt( string("failed to unlink") );
         }

         break;
      }

      case Serialization::WriteMsg::ACCEPTED: {
         // received an ACCEPTED message directly.
         // We're allowed to forget about this block then,
         // if the remote client didn't encounter any errors
         // collating the data

         if( msg->has_errorcode() ) {
            errorf( "ACCEPTED error %d (%s)\n", msg->errorcode(), msg->errortxt().c_str() );
            break;
         }
         else {
            // we're allowed to withdraw all of these blocks from the staging area
            rc = fs_entry_release_staging( state->core, msg );
            if( rc != 0 ) {
               errorf( "fs_entry_release_staging(%s.%" PRId64 ") rc = %d\n", fs_path, file_version, rc );
            }
         }

         no_ack = true;
         break;
      }

      default: {
         //errorf( "unknown transaction message type %d (xid = %ld, session = %ld)\n", msg.type(), msg.transaction_id(), msg.session_id() );
         break;
      }
   }

   if( !no_ack ) {
      syndicate_make_msg_ack( md_con_data, &ack );
   }
   else {
      md_con_data->resp = CALLOC_LIST( struct md_HTTP_response, 1 );
      md_create_HTTP_response_ram( md_con_data->resp, "text/plain", 200, "OK\n", strlen("OK\n") + 1 );
   }

   delete msg;

   return;
}

// HTTP cleanup handler, called after the data has been sent
void syndicate_HTTP_cleanup(struct MHD_Connection *connection, void *con_cls, enum MHD_RequestTerminationCode term) {
   struct syndicate_connection* syncon = (struct syndicate_connection*)con_cls;
   free( syncon );
}


// initialize
int syndicate_init( char const* config_file,
                    struct md_HTTP* http_server,
                    int portnum,
                    char const* ms_url,
                    char const* volume_name,
                    char const* gateway_name,
                    char const* md_username,
                    char const* md_password,
                    char const* volume_pubkey_file,
                    char const* my_key_file,
                    char const* tls_key_file,
                    char const* tls_cert_file
                  ) {


   // initialize Syndicate state
   global_state = CALLOC_LIST( struct syndicate_state, 1 );

   struct syndicate_state* state = global_state;
   state->ms = CALLOC_LIST( struct ms_client, 1 );


   int rc = md_init( SYNDICATE_UG, config_file, &state->conf, state->ms, portnum, ms_url, volume_name, gateway_name, md_username, md_password, volume_pubkey_file, my_key_file, tls_key_file, tls_cert_file );
   if( rc != 0 ) {
      errorf("md_init rc = %d\n", rc );
      return rc;
   }

   // get the volume
   uint64_t volume_id = ms_client_get_volume_id( state->ms );
   uint64_t blocking_factor = ms_client_get_volume_blocksize( state->ms );

   if( volume_id == 0 ) {
      errorf("Volume '%s' not found\n", volume_name);
      return -ENOENT;
   }
   
   // make the logfile
   state->logfile = log_init( state->conf.logfile_path );
   if( state->logfile == NULL ) {
      return -ENOMEM;
   }

   // start up stats gathering
   state->stats = new Stats( NULL );
   state->stats->use_conf( &state->conf );

   // get root info
   struct md_entry root;
   memset( &root, 0, sizeof(root) );

   rc = ms_client_get_volume_root( state->ms, &root );
   if( rc != 0 ) {
      errorf("ms_client_get_volume_root rc = %d\n", rc );
      return -ENODATA;
   }

   // sanity check
   if( root.volume != volume_id ) {
      errorf("Invalid root Volume %" PRIu64 "\n", root.volume );
      md_entry_free( &root );
      return -EINVAL;
   }

   // initialize the filesystem core
   struct fs_core* core = CALLOC_LIST( struct fs_core, 1 );
   fs_core_init( core, &state->conf, root.owner, root.coordinator, root.volume, root.mode, blocking_factor );

   md_entry_free( &root );

   fs_entry_set_config( &state->conf );

   state->core = core;
   state->col = new Collator( core );
   
   fs_core_use_collator( core, state->col );
   fs_core_use_ms( core, state->ms );

   // restore local files
   rc = fs_entry_restore_files( core );
   if( rc != 0 ) {
      errorf("fs_entry_restore_files rc = %d\n", rc );
      exit(1);
   }

   state->col->start();
   
   state->uid = getuid();
   state->gid = getgid();
   
   state->mounttime = currentTimeSeconds();

   // start up replication
   replication_init( state->ms, volume_id );

   // start HTTP server
   memset( http_server, 0, sizeof( struct md_HTTP ) );
   md_HTTP_init( http_server, MD_HTTP_TYPE_STATEMACHINE, &state->conf, state->ms );
   
   http_server->HTTP_connect = syndicate_HTTP_connect;
   http_server->HTTP_GET_handler = syndicate_HTTP_GET_handler;
   http_server->HTTP_cleanup = syndicate_HTTP_cleanup;
   http_server->HTTP_HEAD_handler = syndicate_HTTP_HEAD_handler;
   http_server->HTTP_POST_iterator = md_response_buffer_upload_iterator;
   http_server->HTTP_POST_finish = syndicate_HTTP_POST_finish;
   http_server->HTTP_authenticate = syndicate_HTTP_authenticate;

   dbprintf( "Starting Syndicate HTTP server on port %d\n", state->conf.portnum );
   rc =  md_start_HTTP( http_server, state->conf.portnum );
   if( rc < 0 ) {
      errorf( "failed to start HTTP; rc = %d\n", rc );
      exit(1);
   }

   state->running = 1;

   return 0;
}


// shutdown
int syndicate_destroy() {

   struct syndicate_state* state = global_state;
   
   state->running = 0;

   dbprintf("%s", "stopping replication\n");
   replication_shutdown();
   
   dbprintf("%s", "destroy collation\n");
   delete state->col;

   dbprintf("%s", "destory MS client\n");
   ms_client_destroy( state->ms );
   free( state->ms );

   dbprintf("%s", "core filesystem shutdown\n");
   fs_destroy( state->core );
   free( state->core );
   
   string statistics_str = state->stats->dump();
   printf("Statistics: \n%s\n", statistics_str.c_str() );
   delete state->stats;

   dbprintf("%s", "log shutdown\n");

   log_shutdown( state->logfile );

   dbprintf("%s", "free configuration\n");
   md_free_conf( &state->conf );
   free( state );

   dbprintf("%s", "library shutdown\n");
   md_shutdown();
   
   return 0;
}


// get state
struct syndicate_state* syndicate_get_state() {
   return global_state;
}

// get config
struct md_syndicate_conf* syndicate_get_conf() {
   return &global_state->conf;
}

