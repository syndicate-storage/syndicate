/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "AG-disk.h"

// server config 
struct md_syndicate_conf CONF;

// set of files we're exposing
content_map DATA;

// generate a manifest for an existing file, putting it into the gateway context
int gateway_generate_manifest( struct gateway_context* replica_ctx, struct gateway_ctx* ctx, struct md_entry* ent ) {
   
   // populate a manifest
   Serialization::ManifestMsg* mmsg = new Serialization::ManifestMsg();
   mmsg->set_size( ent->size );
   mmsg->set_file_version( 1 );
   mmsg->set_mtime_sec( ent->mtime_sec );
   mmsg->set_mtime_nsec( 0 );
   mmsg->set_manifest_mtime_sec( ent->mtime_sec );
   mmsg->set_manifest_mtime_nsec( 0 );

   uint64_t num_blocks = ent->size / CONF.blocking_factor;
   if( ent->size % CONF.blocking_factor != 0 )
      num_blocks++;

   Serialization::BlockURLSetMsg *bbmsg = mmsg->add_block_url_set();
   bbmsg->set_start_id( 0 );
   bbmsg->set_end_id( num_blocks );
   bbmsg->set_file_url( string(ent->url) );

   for( uint64_t i = 0; i < num_blocks; i++ ) {
      bbmsg->add_block_versions( 0 );
   }
   
   // serialize
   string mmsg_str;
   bool src = mmsg->SerializeToString( &mmsg_str );
   if( !src ) {
      // failed
      errorf( "%s", "failed to serialize" );
      delete mmsg;
      return -EINVAL;
   }

   ctx->data_len = mmsg_str.size();
   ctx->data = CALLOC_LIST( char, mmsg_str.size() );
   replica_ctx->last_mod = ent->mtime_sec;
   memcpy( ctx->data, mmsg_str.data(), mmsg_str.size() );

   delete mmsg;
   
   return 0;
}


// read dataset or manifest 
ssize_t get_dataset( struct gateway_context* dat, char* buf, size_t len, void* user_cls ) {
   ssize_t ret = 0;
   struct gateway_ctx* ctx = (struct gateway_ctx*)user_cls;

   if( ctx->request_type == GATEWAY_REQUEST_TYPE_LOCAL_FILE ) {
      // read from disk
      ssize_t nr = 0;
      size_t num_read = 0;
      while( num_read < len ) {
         nr = read( ctx->fd, buf + num_read, len - num_read );
         if( nr == 0 ) {
            // EOF
            break;
         }
         if( nr < 0 ) {
            // error
            int errsv = -errno;
            errorf( "read(%d) errno = %d\n", ctx->fd, errsv );
            ret = errsv;
         }
         num_read += nr;
      }

      if( ret == 0 )
         ret = num_read;
   }
   else if( ctx->request_type == GATEWAY_REQUEST_TYPE_MANIFEST ) {
      // read from RAM
      memcpy( buf, ctx->data + ctx->data_offset, MIN( len, ctx->data_len - ctx->data_offset ) );
      ctx->data_offset += len;
      ret = (ssize_t)len;
   }
   else {
      // invalid structure
      ret = -EINVAL;
   }
   
   return ret;
}


// get metadata for a dataset
int metadata_dataset( struct gateway_context* dat, ms::ms_gateway_blockinfo* info, void* usercls ) {
   content_map::iterator itr = DATA.find( string(dat->url_path) );
   if( itr == DATA.end() ) {
      // not here
      return -ENOENT;
   }

   struct gateway_ctx* ctx = (struct gateway_ctx*)usercls;
   struct md_entry* ent = itr->second;
   
   info->set_progress( ms::ms_gateway_blockinfo::COMMITTED );     // ignored, but needs to be filled in
   info->set_blocking_factor( CONF.blocking_factor );
   
   info->set_file_version( 1 );
   info->set_block_id( ctx->block_id );
   info->set_block_version( 1 );
   info->set_fs_path( string(ctx->file_path) );
   info->set_file_mtime_sec( ent->mtime_sec );
   info->set_file_mtime_nsec( ent->mtime_nsec );
   info->set_write_time( ent->mtime_sec );
   
   return 0;
}


// interpret an inbound GET request
void* connect_dataset( struct gateway_context* replica_ctx ) {

   // is this a request for a file block, or a manifest?
   char* file_path = NULL;
   int64_t file_version = 0;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   struct timespec manifest_timestamp;
   manifest_timestamp.tv_sec = 0;
   manifest_timestamp.tv_nsec = 0;
   bool staging = false;

   int rc = md_HTTP_parse_url_path( (char*)replica_ctx->url_path, &file_path, &file_version, &block_id, &block_version, &manifest_timestamp, &staging );
   if( rc != 0 ) {
      errorf( "failed to parse '%s', rc = %d\n", replica_ctx->url_path, rc );
      free( file_path );
      return NULL;
   }

   if( staging ) {
      errorf("invalid URL path %s\n", replica_ctx->url_path );
      free( file_path );
      return NULL;
   }
   
   struct gateway_ctx* ctx = CALLOC_LIST( struct gateway_ctx, 1 );

   // is there metadata for this file?
   string fs_path( file_path );
   content_map::iterator itr = DATA.find( fs_path );
   if( itr == DATA.end() ) {
      // no entry; nothing to do
      free( file_path );
      return NULL;
   }

   struct md_entry* ent = DATA[ file_path ];

   // is this a request for a manifest?
   if( manifest_timestamp.tv_sec > 0 ) {
      // request for a manifest
      int rc = gateway_generate_manifest( replica_ctx, ctx, ent );
      if( rc != 0 ) {
         // failed
         errorf( "gateway_generate_manifest rc = %d\n", rc );

         // meaningful error code
         if( rc == -ENOENT )
            replica_ctx->err = -404;
         else if( rc == -EACCES )
            replica_ctx->err = -403;
         else
            replica_ctx->err = -500;

         free( ctx );
         free( file_path );

         return NULL;
      }

      ctx->request_type = GATEWAY_REQUEST_TYPE_MANIFEST;
      ctx->data_offset = 0;
      ctx->block_id = 0;
      ctx->num_read = 0;
      replica_ctx->size = ctx->data_len;
   }
   else {
      
      // request for local file
      char* fp = md_fullpath( CONF.data_root, GET_PATH( ent->url ), NULL );
      ctx->fd = open( fp, O_RDONLY );
      if( ctx->fd < 0 ) {
         rc = -errno;
         errorf( "open(%s) errno = %d\n", fp, rc );
         free( fp );
         free( ctx );
         free( file_path );
         return NULL;
      }
      else {
         free( fp );

         // set up for reading
         off_t offset = CONF.blocking_factor * block_id;
         rc = lseek( ctx->fd, offset, SEEK_SET );
         if( rc != 0 ) {
            rc = -errno;
            errorf( "lseek errno = %d\n", rc );

            free( ctx );
            free( file_path );
            return NULL;
         }
      }

      ctx->num_read = 0;
      ctx->block_id = block_id;
      ctx->request_type = GATEWAY_REQUEST_TYPE_LOCAL_FILE;
      replica_ctx->size = CONF.blocking_factor;
   }

   ctx->file_path = file_path;
   return ctx;
}


// clean up a transfer 
void cleanup_dataset( void* cls ) {
   struct gateway_ctx* ctx = (struct gateway_ctx*)cls;
   if (ctx) {
      close( ctx->fd );
      if( ctx->data )
        free( ctx->data );

      ctx->data = NULL;
      ctx->file_path = NULL;
   
      free( ctx );
   }
}

int main( int argc, char** argv ) {
   
   gateway_get_func( get_dataset );
   gateway_connect_func( connect_dataset );
   gateway_cleanup_func( cleanup_dataset );
   gateway_metadata_func( metadata_dataset );
   
   int rc = AG_main( argc, argv );

   return rc;
}
