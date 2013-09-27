/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include <driver.h>
#include <libgateway.h>

// server config 
extern struct md_syndicate_conf *global_conf;
 
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

// Is false if the driver is not initialized
bool initialized = false;

// Disk driver's data_root
char* data_root = NULL;

// generate a manifest for an existing file, putting it into the gateway context
extern "C" int gateway_generate_manifest( struct gateway_context* replica_ctx, struct gateway_ctx* ctx, struct md_entry* ent ) {
   errorf("%s", "INFO: gateway_generate_manifest\n"); 
   // populate a manifest
   Serialization::ManifestMsg* mmsg = new Serialization::ManifestMsg();
   mmsg->set_size( ent->size );
   mmsg->set_file_version( 1 );
   mmsg->set_mtime_sec( ent->mtime_sec );
   mmsg->set_mtime_nsec( 0 );
   
   uint64_t num_blocks = ent->size / ctx->blocking_factor;
   if( ent->size % ctx->blocking_factor != 0 )
      num_blocks++;

   Serialization::BlockURLSetMsg *bbmsg = mmsg->add_block_url_set();
   bbmsg->set_start_id( 0 );
   bbmsg->set_end_id( num_blocks );

   for( uint64_t i = 0; i < num_blocks; i++ ) {
      bbmsg->add_block_versions( 0 );
   }
   
   // sign the message
   int rc = gateway_sign_manifest( mc->my_key, mmsg );
   if( rc != 0 ) {
      errorf("gateway_sign_manifest rc = %d\n", rc );
      delete mmsg;
      return rc;
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
extern "C" ssize_t get_dataset( struct gateway_context* dat, char* buf, size_t len, void* user_cls ) {
   errorf("%s", "INFO: get_dataset\n"); 
   ssize_t ret = 0;
   struct gateway_ctx* ctx = (struct gateway_ctx*)user_cls;

   if (ctx == NULL && dat->size == 0)
       return 0;
   else
       return -EINVAL;

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
extern "C" int metadata_dataset( struct gateway_context* dat, ms::ms_gateway_blockinfo* info, void* usercls ) {
   errorf("%s","INFO: metadata_dataset\n"); 
   
   content_map::iterator itr = DATA.find( string( dat->reqdat.fs_path ) );
   if( itr == DATA.end() ) {
      // not here
      return -ENOENT;
   }
   
   // give back the file_id and last-mod, since that's all the disk has for now.
   // TODO: give back the block hash, maybe? 
   
   struct md_entry* ent = itr->second;
   
   info->set_file_id( ent->file_id );
   info->set_file_mtime_sec( ent->mtime_sec );
   info->set_file_mtime_nsec( ent->mtime_nsec );
   
   return 0;
}


// interpret an inbound GET request
extern "C" void* connect_dataset( struct gateway_context* replica_ctx ) {

   errorf("%s", "INFO: connect_dataset\n");  
   struct stat stat_buff;
   struct gateway_ctx* ctx = CALLOC_LIST( struct gateway_ctx, 1 );

   // is there metadata for this file?
   string fs_path( replica_ctx->reqdat.fs_path );
   content_map::iterator itr = DATA.find( fs_path );
   if( itr == DATA.end() ) {
      // no entry; nothing to do
       replica_ctx->err = -404;
       replica_ctx->http_status = 404;
       return NULL;
   }

   struct md_entry* ent = DATA[ fs_path ];

   // is this a request for a manifest?
   if( replica_ctx->reqdat.manifest_timestamp.tv_sec > 0 ) {
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

         return NULL;
      }

      ctx->request_type = GATEWAY_REQUEST_TYPE_MANIFEST;
      ctx->data_offset = 0;
      ctx->block_id = 0;
      ctx->num_read = 0;
      replica_ctx->size = ctx->data_len;
   }
   else {
      if ( !datapath) {
         errorf( "Driver datapath = %s\n", datapath );
         return NULL;
      }
      int rc = 0;
      
      // request for local file
      char* fp = md_fullpath( datapath, fs_path.c_str(), NULL );
      ctx->fd = open( fp, O_RDONLY );
      if( ctx->fd < 0 ) {
         rc = -errno;
         errorf( "open(%s) errno = %d\n", fp, rc );
         free( fp );
         free( ctx );
	 replica_ctx->err = -404;
	 replica_ctx->http_status = 404;
         return NULL;
      }
      else {
	 // Set blocking factor for this volume from replica_ctx
	 ctx->blocking_factor = global_conf->ag_block_size;
	  if ((rc = stat(fp, &stat_buff)) < 0) {
	      errorf( "stat errno = %d\n", rc );
	      perror("stat");
	      free( fp );
	      free( ctx );
	      replica_ctx->err = -404;
	      replica_ctx->http_status = 404;
	      return NULL;
	 }
         free( fp );
	 if ((size_t)stat_buff.st_size < ctx->blocking_factor * replica_ctx->reqdat.block_id) {
	     free( ctx );
	     replica_ctx->size = 0;
	     return NULL;
	 }
	 else if ((stat_buff.st_size - (ctx->blocking_factor * replica_ctx->reqdat.block_id))
		 <= (size_t)ctx->blocking_factor) {
	     replica_ctx->size = stat_buff.st_size - (ctx->blocking_factor * replica_ctx->reqdat.block_id);
	 }
	 else {
	     replica_ctx->size = ctx->blocking_factor;
	 }
         // set up for reading
         off_t offset = ctx->blocking_factor * replica_ctx->reqdat.block_id;
         rc = lseek( ctx->fd, offset, SEEK_SET );
	 if( rc < 0 ) {
            rc = -errno;
            errorf( "lseek errno = %d\n", rc );
            free( ctx );
	    replica_ctx->err = -404;
	    replica_ctx->http_status = 404;
            return NULL;
         }
      }
      ctx->num_read = 0;
      ctx->block_id = replica_ctx->reqdat.block_id;
      ctx->request_type = GATEWAY_REQUEST_TYPE_LOCAL_FILE;
   }

   return ctx;
}


// clean up a transfer 
extern "C" void cleanup_dataset( void* cls ) {
   
   errorf("%s", "INFO: cleanup_dataset\n"); 
   struct gateway_ctx* ctx = (struct gateway_ctx*)cls;
   if (ctx) {
      close( ctx->fd );
      if( ctx->data )
        free( ctx->data );

      ctx->data = NULL;
   
      free( ctx );
   }
}

extern "C" int publish_dataset (struct gateway_context*, ms_client *client, 
	char* dataset ) {
    if (!initialized)
	init();
    int flags = FTW_PHYS;
    mc = client;
    datapath = dataset;
    datapath_len = strlen(datapath); 
    if ( datapath[datapath_len - 1] == '/')
	   datapath_len--;	
    if (nftw(dataset, publish_to_volumes, 20, flags) == -1) {
	return pfunc_exit_code;
    }
    return 0;
}


static int publish_to_volumes(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf) {
    int nr_volumes = ms_client_get_num_volumes(mc);
    int vol_counter=0;
    for (vol_counter=0; vol_counter<nr_volumes; vol_counter++) {
	uint64_t volume_id = ms_client_get_volume_id(mc, vol_counter);
	publish(fpath, sb, tflag, ftwbuf, volume_id);
    }
    return 0;
}

static int publish(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf, uint64_t volume_id)
{
    int i = 0;
    struct md_entry* ment = new struct md_entry;
    memset( ment, 0, sizeof(struct md_entry) );
    
    size_t len = strlen(fpath);
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
    char* path = (char*)malloc( path_len );
    memset( path, 0, path_len );
    strncpy( path, fpath + datapath_len, path_len );

    char* parent_name_tmp = md_dirname( path, NULL );
    ment->parent_name = md_basename( parent_name_tmp, NULL );
    free( parent_name_tmp );
    
    ment->name = md_basename( path, NULL );
    
    ment->ctime_sec = sb->st_ctime;
    ment->ctime_nsec = 0;
    ment->mtime_sec = sb->st_mtime;
    ment->mtime_nsec = 0;
    ment->mode = sb->st_mode;
    ment->version = 1;
    ment->max_read_freshness = 360000;
    ment->max_write_freshness = 1;
    ment->volume = volume_id;
    ment->size = sb->st_size;
    switch (tflag) {
	case FTW_D:
	    ment->type = MD_ENTRY_DIR;
	    if ( (i = ms_client_mkdir(mc, &ment->file_id, ment)) < 0 ) {
		cout<<"ms client mkdir "<<i<<endl;
	    }
	    break;
	case FTW_F:
	    ment->type = MD_ENTRY_FILE;
	    if ( (i = ms_client_create(mc, &ment->file_id, ment)) < 0 ) {
		cout<<"ms client create "<<i<<endl;
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
    DATA[path] = ment;
    //delete ment;
    pfunc_exit_code = 0;
    return 0;  
}

extern "C" int controller(pid_t pid, int ctrl_flag) {
    return controller_signal_handler(pid, ctrl_flag);
}

void init() {
    if (!initialized)
	initialized = true;
    else
	return;
    add_driver_event_handler(DRIVER_TERMINATE, term_handler, NULL);
    driver_event_start();
}


void* term_handler(void *cls) {
    //Nothing to do here.
    exit(0);
}

