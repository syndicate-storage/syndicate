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

#include <driver.h>
#include <libgateway.h>
#include <pthread.h>

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

// timeout pulse gen thread
pthread_t timeout_pulse_gen_tid;

// generate a manifest for an existing file, putting it into the gateway context
extern "C" int gateway_generate_manifest( struct gateway_context* replica_ctx, struct gateway_ctx* ctx, struct md_entry* ent ) {
   errorf("%s", "INFO: gateway_generate_manifest\n"); 
   // populate a manifest
   Serialization::ManifestMsg* mmsg = new Serialization::ManifestMsg();
   mmsg->set_size( ent->size );
   mmsg->set_file_version( 1 );
   mmsg->set_mtime_sec( ent->mtime_sec );
   mmsg->set_mtime_nsec( 0 );

   //uint64_t blocking_factor = ms_client_get_AG_blocksize( mc, mc->gateway_id );
   uint64_t blocking_factor = global_conf->ag_block_size;
   uint64_t num_blocks = ent->size / blocking_factor;
   if( ent->size % blocking_factor != 0 )
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

   ctx->blocking_factor = blocking_factor;
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
extern "C" int metadata_dataset( struct gateway_context* dat, ms::ms_gateway_request_info* info, void* usercls ) {
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
      //ctx->blocking_factor = global_conf->ag_block_size;
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
      ctx->blocking_factor = global_conf->ag_block_size;
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
    
    dbprintf("publish %s\n", dataset );
    int flags = FTW_PHYS;
    mc = client;
    datapath = dataset;
    datapath_len = strlen(datapath); 
    if ( datapath[datapath_len - 1] == '/')
	   datapath_len--;

    if (check_modified(datapath, entry_modified_handler) < 0) {
        errorf("%s", "check_modified failed\n");
        return pfunc_exit_code;        
    }

    dbprintf("set timeout schedule - %dseconds\n", REFRESH_ENTRIES_TIMEOUT);
    if (set_timeout_event(REFRESH_ENTRIES_TIMEOUT, timeout_handler) < 0) {
        errorf("%s", "set_timeout_event error\n");
        return pfunc_exit_code;
    }

    //if (nftw(dataset, publish_to_volumes, 20, flags) == -1) {
    //    return pfunc_exit_code;
    //}
    return 0;
}


static int publish_to_volumes(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf, int mflag) {
   
    uint64_t volume_id = ms_client_get_volume_id(mc);
    publish(fpath, sb, tflag, ftwbuf, volume_id, mflag);
   
    return 0;
}

static int publish(const char *fpath, const struct stat *sb,
	int tflag, struct FTW *ftwbuf, uint64_t volume_id, int mflag)
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

    uint64_t parent_id = 0;
    if(strcmp(ment->parent_name, "/") == 0) {
        // root
    	parent_id = 0;    
    } else {
        char* parent_full_path = md_dirname( path, NULL );
        struct md_entry* ment_parent = DATA[parent_full_path];
        free(parent_full_path);
        if(ment_parent == NULL) {
            errorf("cannot find parent entry : %s\n", ment->parent_name);
            pfunc_exit_code = -EINVAL;
	    return -EINVAL;
        } else {
            cout << "found parent entry : " << ment->parent_name << " -> " << ment_parent->name << endl;
        }    
        
        parent_id = ment_parent->file_id;
    }
    
    ment->name = md_basename( path, NULL );
    ment->parent_id = parent_id;    
    
    ment->coordinator = mc->gateway_id;
    ment->owner = mc->owner_id;
    
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

    cout << "publish file entry (parent) : " << ment->parent_name << endl;
    cout << "publish file entry (parent id) : " << ment->parent_id << endl;
    cout << "publish file entry : " << ment->name << endl;

    //TODO: When AG is restarted, DIR_ENTRY_MODIFIED_FLAG_NEW events will be raised.
    // In this case, ms_client_mkdir/ms_client_create call will be failed.
    // We need to check presence of files and get file id if exists.
    // or create a new entry.
    switch (tflag) {
	case FTW_D:
	    ment->type = MD_ENTRY_DIR;
            if(mflag == DIR_ENTRY_MODIFIED_FLAG_NEW) {
                uint64_t new_file_id = 0;
	        if ( (i = ms_client_mkdir(mc, &new_file_id, ment)) < 0 ) {
		    cout<<"ms client mkdir "<<i<<endl;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
	        } else {
                    ment->file_id = new_file_id;
                }
	    } else if(mflag == DIR_ENTRY_MODIFIED_FLAG_MODIFIED) {
	        if ( (i = ms_client_update(mc, ment)) < 0 ) {
		    cout<<"ms client update "<<i<<endl;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
	        }
            } else if(mflag == DIR_ENTRY_MODIFIED_FLAG_REMOVED) {
	        if ( (i = ms_client_delete(mc, ment)) < 0 ) {
		    cout<<"ms client delete "<<i<<endl;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
	        }
            }
	    break;
	case FTW_F:
	    ment->type = MD_ENTRY_FILE;
            if(mflag == DIR_ENTRY_MODIFIED_FLAG_NEW) {
                uint64_t new_file_id = 0;
	        if ( (i = ms_client_create(mc, &new_file_id, ment)) < 0 ) {
		    cout<<"ms client create "<<i<<endl;
		} else {
                    ment->file_id = new_file_id;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
                }
	    } else if(mflag == DIR_ENTRY_MODIFIED_FLAG_MODIFIED) {
	        if ( (i = ms_client_update(mc, ment)) < 0 ) {
		    cout<<"ms client update "<<i<<endl;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
	        }
            } else if(mflag == DIR_ENTRY_MODIFIED_FLAG_REMOVED) {
	        if ( (i = ms_client_delete(mc, ment)) < 0 ) {
		    cout<<"ms client delete "<<i<<endl;
                    pfunc_exit_code = -EINVAL;
                    return -EINVAL;
	        }
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

    init_timeout();
    init_monitor();

    add_driver_event_handler(DRIVER_TERMINATE, term_handler, NULL);
    driver_event_start();
}

void timeout_handler(int sig_no, struct timeout_event* event) {
    cout << "waiting is over - start disk check" << endl;
    check_modified(datapath, entry_modified_handler);
    
    int rc = set_timeout_event(event->timeout, event->handler);
    if(rc < 0) {
        errorf("set timeout event error : %d", rc);
    }
}

void entry_modified_handler(int flag, string spath, struct filestat_cache *pcache) {
    cout << "found changes : " << spath << endl; 
    publish_to_volumes(pcache->fpath, pcache->sb, pcache->tflag, NULL, flag);

    //cout << "flag : " << flag << " path : " << spath << endl;
}

void* term_handler(void *cls) {
    //Nothing to do here.
    exit(0);
}

