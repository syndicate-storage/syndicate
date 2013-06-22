/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "AG-disk.h"
#include "libgateway.h"

// server config 
struct md_syndicate_conf CONF;
 
// set of files we're exposing
content_map DATA;

// Metadata service client of the AG
ms_client *mc = NULL;

// Location of local files we're exposing 
char *datapath = NULL;

// Length of datapath varaiable
size_t  datapath_len = 0;

// publish_func exit code
int pfunc_exit_code = 0;

// generate a manifest for an existing file, putting it into the gateway context
int gateway_generate_manifest( struct gateway_context* replica_ctx, struct gateway_ctx* ctx, struct md_entry* ent ) {
   errorf("%s", "INFO: gateway_generate_manifest\n"); 
   // populate a manifest
   Serialization::ManifestMsg* mmsg = new Serialization::ManifestMsg();
   mmsg->set_size( ent->size );
   mmsg->set_file_version( 1 );
   mmsg->set_mtime_sec( ent->mtime_sec );
   mmsg->set_mtime_nsec( 0 );
   mmsg->set_manifest_mtime_sec( ent->mtime_sec );
   mmsg->set_manifest_mtime_nsec( 0 );

   uint64_t num_blocks = ent->size / global_conf->blocking_factor;
   if( ent->size % global_conf->blocking_factor != 0 )
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
   errorf("%s", "INFO: get_dataset\n"); 
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
   errorf("%s","INFO: metadata_dataset\n"); 
   char* file_path = NULL;
   int64_t file_version = 0;
   uint64_t block_id = 0;
   int64_t block_version = 0;
   struct timespec manifest_timestamp;
   manifest_timestamp.tv_sec = 0;
   manifest_timestamp.tv_nsec = 0;
   bool staging = false;

   int rc = md_HTTP_parse_url_path( (char*)dat->url_path, &file_path, &file_version, &block_id, &block_version, &manifest_timestamp, &staging );
   if( rc != 0 ) {
      errorf( "failed to parse '%s', rc = %d\n", dat->url_path, rc );
      free( file_path );
      return -EINVAL;
   }

   content_map::iterator itr = DATA.find( string(file_path) );
   if( itr == DATA.end() ) {
      // not here
      return -ENOENT;
   }

   struct gateway_ctx* ctx = (struct gateway_ctx*)usercls;
   struct md_entry* ent = itr->second;
   
   info->set_progress( ms::ms_gateway_blockinfo::COMMITTED );     // ignored, but needs to be filled in
   info->set_blocking_factor( global_conf->blocking_factor );
   
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

   errorf("%s", "INFO: connect_dataset\n");  
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
      if ( !global_conf->data_root || !ent->url) {
         errorf( "Conf's data_root = %s and URL = %s\n", global_conf->data_root, ent->url );
         return NULL;
      }
      // request for local file
      char* fp = md_fullpath( global_conf->data_root, GET_PATH( ent->url ), NULL );
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
         off_t offset = global_conf->blocking_factor * block_id;
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
      replica_ctx->size = ent->size;
   }

   ctx->file_path = file_path;
   return ctx;
}


// clean up a transfer 
void cleanup_dataset( void* cls ) {
   
   errorf("%s", "INFO: cleanup_dataset\n"); 
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

int publish_func (struct gateway_context*, ms_client *client, 
	char* dataset ) {
    int flags = FTW_PHYS;
    mc = client;
    datapath = dataset;
    datapath_len = strlen(datapath); 
    if ( datapath[datapath_len - 1] == '/')
	   datapath_len--;	
    if (nftw(dataset, publish, 20, flags) == -1) {
	return pfunc_exit_code;
    }
    ms_client_destroy(mc);
    return 0;
}

static int publish(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf)
{
    int i = 0;
    struct md_entry* ment = new struct md_entry;
    size_t len = strlen(fpath);
    size_t local_proto_len = strlen( SYNDICATEFS_LOCAL_PROTO ); 
    size_t url_len = local_proto_len + len;
    if ( len < datapath_len ) { 
	pfunc_exit_code = -EINVAL;
	return -EINVAL;
    }
    if ( len == datapath_len ) {
	pfunc_exit_code = 0;
	return 0;
    }
    //Set volume path
    size_t path_len = ( len - datapath_len ) + 1; 
    ment->path = (char*)malloc( path_len );
    memset( ment->path, 0, path_len );
    strncpy( ment->path, fpath + datapath_len, path_len );

    //Set primary replica 
    ment->url = ( char* )malloc( url_len + 1);
    memset( ment->url, 0, url_len + 1 );
    strncat( ment->url, SYNDICATEFS_LOCAL_PROTO, local_proto_len );
    strncat( ment->url + local_proto_len, fpath, len );

    ment->url_replicas = mc->conf->replica_urls;
    ment->local_path = NULL;
    ment->ctime_sec = sb->st_ctime;
    ment->ctime_nsec = 0;
    ment->mtime_sec = sb->st_mtime;
    ment->mtime_nsec = 0;
    ment->mode = sb->st_mode;
    ment->version = 1;
    ment->max_read_freshness = 360000;
    ment->max_write_freshness = 1;
    ment->volume = mc->conf->volume;
    ment->size = sb->st_size;
    ment->owner = mc->conf->volume_owner;
    switch (tflag) {
	case FTW_D:
	    ment->type = MD_ENTRY_DIR;
	    if ( (i = ms_client_mkdir(mc, ment)) < 0 ) {
		cout<<"ms client mkdir "<<i<<endl;
	    }
	    break;
	case FTW_F:
	    ment->type = MD_ENTRY_FILE;
	    if ( (i = ms_client_create(mc, ment)) < 0 ) {
		cout<<"ms client mkdir "<<i<<endl;
	    }
	    break;
	case FTW_SL:
	    break;
	case FTW_DP:
	    break;
	case FTW_DNR:
	    break;
	default:
	    break;
    }
    DATA[ment->path] = ment;
    //delete ment;
    pfunc_exit_code = 0;
    return 0;  
}


int main( int argc, char** argv ) {
   
   gateway_get_func( get_dataset );
   gateway_connect_func( connect_dataset );
   gateway_cleanup_func( cleanup_dataset );
   gateway_metadata_func( metadata_dataset );
   gateway_publish_func( publish_func );   

   int rc = AG_main( argc, argv );

   return rc;
}
