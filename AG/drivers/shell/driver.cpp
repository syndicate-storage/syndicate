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

// server config 
extern struct md_syndicate_conf *global_conf;

// set of files we're exposing
content_map DATA;

// set of files we map to command
query_map* FS2CMD = NULL;

// volumes set
volume_set* VOLUMES = NULL;

// MapParser object...
MapParser* mp = NULL;

// Metadata service client of the AG
ms_client *mc = NULL;

// Location of local files we're exposing 
char *datapath = NULL;

// Length of datapath varaiable
size_t  datapath_len = 0;

// Block cache path string
unsigned char* cache_path = NULL;

// ReadWrite lock to protect DATA, FS2CMD etc.
pthread_rwlock_t driver_lock;

//extern struct md_syndicate_conf *global_conf;
// Reversion daemon
ReversionDaemon* revd = NULL;

// true if init() is called
bool initialized = false;

// generate a manifest for an existing file, putting it into the gateway context
extern "C" int gateway_generate_manifest( struct gateway_context* dataset_ctx, struct shell_ctx* ctx, struct md_entry* ent ) {
    // No need of a lock.
    errorf("%s", "INFO: gateway_generate_manifest\n"); 
    
    uint64_t volume_id = ms_client_get_volume_id( mc );
    
    // populate a manifest
    Serialization::ManifestMsg* mmsg = new Serialization::ManifestMsg();
    mmsg->set_volume_id( volume_id );
    mmsg->set_owner_id( global_conf->owner );
    mmsg->set_coordinator_id( global_conf->gateway );
    mmsg->set_file_id( ent->file_id );
    mmsg->set_size( ent->size );
    mmsg->set_file_version( 1 );
    mmsg->set_mtime_sec( ent->mtime_sec );
    mmsg->set_mtime_nsec( ent->mtime_nsec );
    mmsg->set_signature( string("") );
    
    //uint64_t num_blocks = ent->size / global_conf->blocking_factor;
    uint64_t num_blocks = ent->size / ctx->blocking_factor;
    if( ent->size % ctx->blocking_factor != 0 )
	num_blocks++;

    Serialization::BlockURLSetMsg *bbmsg = mmsg->add_block_url_set();
    bbmsg->set_start_id( 0 );
    bbmsg->set_end_id( num_blocks );
    bbmsg->set_gateway_id( global_conf->gateway );
    
    char* fake_hash = CALLOC_LIST( char, BLOCK_HASH_LEN() );
    
    for( uint64_t i = 0; i < num_blocks; i++ ) {
	bbmsg->add_block_versions( 0 );
        bbmsg->add_block_hashes( string(fake_hash, BLOCK_HASH_LEN()) );
    }
    
    free( fake_hash );

    // sign
    int rc = gateway_sign_manifest( mc->my_key, mmsg );
    if( rc != 0 ) {
       errorf("gateway_sign_manifest rc = %d\n", rc );
       delete mmsg;
       return -EINVAL;
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
    
    memcpy( ctx->data, mmsg_str.data(), mmsg_str.size() );
    
    dataset_ctx->last_mod = ent->mtime_sec;
    
    delete mmsg;

    return 0;
}


// read dataset or manifest 
extern "C" ssize_t get_dataset( struct gateway_context* dat, char* buf, size_t len, void* user_cls ) {
    DRIVER_RDONLY(&driver_lock);
    errorf("%s", "INFO: get_dataset\n"); 
    ssize_t ret = 0;
    ProcHandler& prch = ProcHandler::get_handle((char*)cache_path);
    struct shell_ctx* ctx = (struct shell_ctx*)user_cls;
    
    if( ctx != NULL ) {
    
      if (dat->http_status == 404 ) {
         // no data
         ret = 0;
      }
      else {
         if( ctx->request_type == GATEWAY_REQUEST_TYPE_LOCAL_FILE ) {
            if( dat->http_status == GATEWAY_HTTP_TRYAGAIN ) {
               // make sure the command is running 
               ret = 0;
            }
            else if( dat->http_status == GATEWAY_HTTP_EOF ) {
               // no data
               ret = 0;
            }
            else {
               // give back data (TODO: encrypt)
               dbprintf("try to read %zu bytes\n", len );
               ret = prch.read_command_results(ctx, buf, len);
               
               if( ret >= 0 && (unsigned)ret != len && ctx->need_padding ) {
                  // pad the results
                  memset( buf + ret, 0, len - ret );
                  ret = len;
               }
            }
         }
         else if( ctx->request_type == GATEWAY_REQUEST_TYPE_MANIFEST ) {
            // manifest data
            memcpy( buf, ctx->data + ctx->data_offset, MIN( (ssize_t)len, ctx->data_len - ctx->data_offset ) );
            ctx->data_offset += len;
            ret = (ssize_t)len;
         }
         else {
            // something's wrong
            ret = -EINVAL;
         }
      }
    }
    else {
       ret = -ENOENT;
    }
    DRIVER_RETURN(ret, &driver_lock);
}


// get metadata for a dataset
extern "C" int metadata_dataset( struct gateway_context* dat, ms::ms_gateway_request_info* info, void* usercls ) {
    DRIVER_RDONLY(&driver_lock);
    errorf("%s", "INFO: metadata_dataset\n"); 
    char* file_path = NULL;
    int64_t file_version = 0;
    int64_t block_version = 0;

    content_map::iterator itr = DATA.find( string(file_path) );
    if( itr == DATA.end() ) {
	// not found in this volume
	DRIVER_RETURN(-ENOENT, &driver_lock);
    }

    struct shell_ctx* ctx = (struct shell_ctx*)usercls;
    struct md_entry* ent = itr->second;

    info->set_file_version( file_version );
    info->set_block_id( ctx->block_id );
    info->set_block_version( block_version );
    info->set_file_mtime_sec( ent->mtime_sec );
    info->set_file_mtime_nsec( ent->mtime_nsec );

    DRIVER_RETURN(0, &driver_lock);
}


// interpret an inbound GET request
extern "C" void* connect_dataset( struct gateway_context* dataset_ctx ) {
   DRIVER_RDONLY(&driver_lock);
   errorf("%s", "INFO: connect_dataset\n"); 
   char* file_path = dataset_ctx->reqdat.fs_path;

   // is there metadata for this file?
   string fs_path( file_path );
   content_map::iterator itr = DATA.find( fs_path );
   if( itr == DATA.end() ) {
       // no entry; nothing to do
       errorf("No dataset %s found\n", file_path);
       free( file_path );
       dataset_ctx->err = -ENOENT;
       dataset_ctx->http_status = 404;
       DRIVER_RETURN(NULL, &driver_lock);
   }

   struct shell_ctx* ctx = CALLOC_LIST( struct shell_ctx, 1 );

   ctx->blocking_factor = global_conf->ag_block_size;
   
   struct md_entry* ent = itr->second;

   // is this a request for a manifest?
   if( AG_IS_MANIFEST_REQUEST( *dataset_ctx ) ) {
       
       // request for a manifest
       int rc = gateway_generate_manifest( dataset_ctx, ctx, ent );
       if( rc != 0 ) {
	   // failed
	   errorf( "gateway_generate_manifest rc = %d\n", rc );

           dataset_ctx->err = rc;
           
	   // meaningful error code
	   if( rc == -ENOENT )
	       dataset_ctx->http_status = 404;
	   else if( rc == -EACCES )
	       dataset_ctx->http_status = 403;
	   else
	       dataset_ctx->http_status = 500;

	   free( ctx );
	   free( file_path ); 
	   DRIVER_RETURN(NULL, &driver_lock);
       }

       ctx->request_type = GATEWAY_REQUEST_TYPE_MANIFEST;
       ctx->data_offset = 0;
       ctx->block_id = 0;
       ctx->file_path = file_path;
       ctx->fd = -1;
       dataset_ctx->size = ctx->data_len;
   }
   else {
       
       struct map_info* mi; 
       query_map::iterator itr = FS2CMD->find(string(file_path));
       if (itr != FS2CMD->end()) {
          
           // set up our driver's connection context
	   mi = itr->second;
	   char** cmd_array = str2array((char*)mi->shell_command);
	   ctx->proc_name = cmd_array[0];
	   ctx->argv = cmd_array;
	   ctx->envp = NULL;
	   ctx->data_offset = 0;
	   ctx->block_id = dataset_ctx->reqdat.block_id;
	   ctx->fd = -1;
	   ctx->request_type = GATEWAY_REQUEST_TYPE_LOCAL_FILE;
	   ctx->file_path = file_path;
	   ctx->id = mi->id;
	   ctx->mi = mi;
	   
           // will serve a block of data 
           dataset_ctx->size = global_conf->ag_block_size;
       
           
	   // Check the block status and set the http response appropriately
	   ProcHandler& prch = ProcHandler::get_handle((char*)cache_path);
	   block_status blk_stat = prch.get_block_status(ctx);
           
           // do we have data to serve?
           if( blk_stat.block_available ) {
              dbprintf("Block %" PRIu64 " is available with %zd bytes (padding: %d)\n", ctx->block_id, blk_stat.written_so_far, blk_stat.need_padding );
              dataset_ctx->err = 0;
              dataset_ctx->http_status = 200;
              ctx->need_padding = blk_stat.need_padding;
           }
           else {
              // no data
              // try again?
              if( blk_stat.in_progress ) {
                 dbprintf("Block %" PRIu64 " is in progress\n", ctx->block_id );
                 dataset_ctx->err = 0;
                 dataset_ctx->http_status = GATEWAY_HTTP_TRYAGAIN;
                 
                 prch.start_command_idempotent(ctx);
              }
              else {
                 // nothing :(
                 // no file?
                 if( blk_stat.no_file ) {
                    errorf("No such file %s\n", ctx->file_path );
                    dataset_ctx->err = -ENOENT;
                    dataset_ctx->http_status = 404;
                 }
                 // or EOF?
                 else {
                    errorf("EOF on file %s (no such block %" PRIu64 ")\n", ctx->file_path, ctx->block_id );
                    dataset_ctx->err = 0;
                    dataset_ctx->http_status = GATEWAY_HTTP_EOF;
                 }
              }
           }
       }
       else {
          errorf("Not found: %s\n", file_path );
          dataset_ctx->http_status = 404;
          free( ctx );
          ctx = NULL;
       }
   }
   DRIVER_RETURN(ctx, &driver_lock);
}


// clean up a transfer 
extern "C" void cleanup_dataset( void* cls ) {   
    //No need to lock
    errorf("%s", "INFO: cleanup_dataset\n"); 
    struct shell_ctx* ctx = (struct shell_ctx*)cls;
    if (ctx) {
	if (ctx->data != NULL) {
	    free(ctx->data);
	}
	if (ctx->argv != NULL) {
	    free(ctx->argv);
	}
	if (ctx->envp != NULL) {
	    free(ctx->envp);
	}
	if( ctx->fd >= 0 ) {
            close(ctx->fd);
        }
	free (ctx);
    }
}

extern "C" int publish_dataset (struct gateway_context*, ms_client *client, 
	char* dataset ) {
    if (mc == NULL)
	mc = client;
    if (mp == NULL)
	mp = new MapParser(dataset);
    map<string, struct map_info*>::iterator iter;
    set<char*, path_comp> dir_hierachy;
    mp->parse();
    unsigned char* cp = mp->get_dsn();
    init(cp);
    //DRIVER_RDWR should strictly be called after init.
    DRIVER_RDWR(&driver_lock);
    set<string> *volset = mp->get_volume_set();
    map<string, struct map_info*> *fs_map = mp->get_map();
    if (FS2CMD == NULL) {
	FS2CMD = new map<string, struct map_info*>(*fs_map);
    }
    else {
	update_fs_map(fs_map, FS2CMD, driver_special_inval_handler);
	//Delete map only, objects in the map are kept intact
	delete fs_map;
    }
    if (VOLUMES == NULL){
	VOLUMES = new set<string>(*volset);
    }
    else {
	//Delete set only, objects in the set are kept intact
	update_volume_set(volset, VOLUMES, NULL);
	delete volset;
    }
    for (iter = FS2CMD->begin(); iter != FS2CMD->end(); iter++) {
	const char* full_path = iter->first.c_str();
	char* path = strrchr((char*)full_path, '/');
	size_t dir_path_len = path - full_path;
	char* dir_path = (char*)malloc(dir_path_len + 1);
	dir_path[dir_path_len] = 0;
	strncpy(dir_path, full_path, dir_path_len);
	dir_hierachy.insert(dir_path);
    }
    set<char*, path_comp>::iterator it;
    //Publish to the volume
   uint64_t volume_id = ms_client_get_volume_id(mc);
   for( it = dir_hierachy.begin(); it != dir_hierachy.end(); it++ ) {
      struct map_info mi;
      publish (*it, MD_ENTRY_DIR, &mi, volume_id);
   }

   for (iter = FS2CMD->begin(); iter != FS2CMD->end(); iter++) {
      publish (iter->first.c_str(), MD_ENTRY_FILE, iter->second, 
                  volume_id);
   }
   
    //Add map_info objects to reversion daemon
    for (iter = FS2CMD->begin(); iter != FS2CMD->end(); iter++) {
	if (revd)
	    revd->add_map_info(iter->second);
    }
    DRIVER_RETURN(0, &driver_lock);
}


static int publish(const char *fpath, int type, struct map_info* mi, 
		    uint64_t volume_id)
{
    //Called by publish_dataset, therefore locking not needed.
    int i = 0;
    struct md_entry* ment = NULL;
    size_t len = strlen(fpath);
    char *path = NULL;
    uint64_t file_id = 0;
    
    if ( len < datapath_len ) { 
	return -EINVAL;
    }
    if ( len == datapath_len ) {
	return 0;
    }
    
    //Set volume path
    size_t path_len = ( len - datapath_len ) + 1; 
    path = (char*)malloc( path_len );
    memset( path, 0, path_len );
    strncpy(path, fpath + datapath_len, path_len );

    if ((ment = DATA[path]) == NULL) {
	ment = new struct md_entry;
        memset( ment, 0, sizeof(struct md_entry) );
	ment->version = 1;
        
        // insert the parent name
	char* parent_path = md_dirname( path, NULL );
	ment->parent_name = md_basename( parent_path, NULL );
        
        // insert name
	ment->name = md_basename( path, NULL );

        // insert the parent's ID, if it is known...
        content_map::iterator parent_itr = DATA.find( string(parent_path) );
        if( parent_itr != DATA.end() ) {
           ment->parent_id = parent_itr->second->file_id;
        }
        
        free( parent_path );
        
	DATA[path] = ment;
    }

    //Set time from the real time clock
    struct timespec rtime; 
    if ((i = clock_gettime(CLOCK_REALTIME, &rtime)) < 0) {
	errorf("Error: clock_gettime: %i\n", i); 
	return -1;
    }

    dbprintf("INFO: publish %s\n", path);
    ment->ctime_sec = rtime.tv_sec;
    ment->ctime_nsec = rtime.tv_nsec;
    ment->mtime_sec = rtime.tv_sec;
    ment->mtime_nsec = rtime.tv_nsec;
    ment->mode = mi->file_perm;
    ment->max_read_freshness = 1000;
    ment->max_write_freshness = 1;
    ment->volume = volume_id;
    ment->owner = mc->owner_id;
    ment->coordinator = mc->gateway_id;
    
    //ment->owner = volume_owner;
    switch (type) {
	case MD_ENTRY_DIR:
	    ment->size = 4096;
	    ment->type = MD_ENTRY_DIR;
	    ment->mode = DIR_PERMISSIONS_MASK;
	    ment->mode |= S_IFDIR;
	    if ( (i = ms_client_mkdir(mc, &file_id, ment)) < 0 ) {
		cerr<<"ms client mkdir "<<i<<endl;
	    }
	    break;
	case MD_ENTRY_FILE:
	    ment->size = -1;
	    ment->type = MD_ENTRY_FILE;
	    ment->mode &= FILE_PERMISSIONS_MASK;
	    ment->mode |= S_IFREG;
	    if ( (i = ms_client_create(mc, &file_id, ment)) < 0 ) {
		cerr<<"ms client create "<<i<<endl;
	    }
	    mi->mentry = ment;
	    mi->reversion_entry = reversion;
	    break;
	default:
	    break;
    }
    
    ment->file_id = file_id;
    
    DATA[path] = ment;
    if (path)
	free(path);
    //pfunc_exit_code = 0;
    return 0;  
}


void init(unsigned char* dsn) {
    if (!initialized)
	initialized = true;
    else
	return;
    if (cache_path == NULL) {
	size_t dsn_len = strlen((const char*)dsn);
	cache_path = (unsigned char*)malloc(dsn_len + 1);
	memset(cache_path, 0, dsn_len + 1);
	memcpy(cache_path, dsn, strlen((const char*)dsn));
    }
    if (revd == NULL) {
	revd = new ReversionDaemon();
	revd->run();
    }
    add_driver_event_handler(DRIVER_RECONF, reconf_handler, NULL);
    add_driver_event_handler(DRIVER_TERMINATE, term_handler, NULL);
    driver_event_start();
    if (pthread_rwlock_init(&driver_lock, NULL) < 0) {
	perror("pthread_rwlock_init");
	exit(-1);
    }
    
    // set up signals
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    block_all_signals();
    set_sigchld_handler(&act);
    
    int rc = init_event_receiver();
    if( rc != 0 ) {
       errorf("init_event_receiver rc = %d\n", rc );
       exit(-1);
    }
}

char** str2array(char *str) {
    char *tok = NULL, *saveptr = NULL;
    char **array = NULL;
    int array_size = 1;
    if (str == NULL)
	return NULL;
    while ((tok = strtok_r(str, " ", &saveptr)) != NULL ) {
	str = NULL;
	array = (char**)realloc(array, sizeof(char*) * array_size);
	array[array_size - 1] = tok;
	array_size++;
    }
    array = (char**)realloc(array, sizeof(char*) * array_size);
    array[array_size - 1] = NULL;
    return array;
}

void reversion(void *cls) {
    struct md_entry *ment = (struct md_entry*)cls;
    if (ment == NULL)
	return;
    ment->version++;
    ms_client_update(mc, ment);
}

void* reconf_handler(void *cls) {
    cout<<"calling publish_dataset"<<endl;
    publish_dataset (NULL, NULL, NULL );
    return NULL;
}

void* term_handler(void *cls) {
    clean_dir((char*)cache_path);
    exit(0);
}

void driver_special_inval_handler(string file_path) {
    //This will called from publish, therefore locking is not needed.
    ProcHandler& prch = ProcHandler::get_handle((char*)cache_path);
    prch.remove_proc_table_entry(file_path);
    struct md_entry *mde = DATA[file_path];
    if (mde != NULL) {
	//Delete this file from all the volumes we are attached to.
	ms_client_delete(mc, mde);
	DATA.erase(file_path);
	if (mde->name)
	    free(mde->name);
	if (mde->parent_name)
	    free(mde->parent_name);
	free(mde);
    }
    //Remove from reversion daemon
    struct map_info* mi = (*FS2CMD)[file_path];
    if (mi != NULL) {
	revd->remove_map_info(mi);
    }
}

extern "C" int controller(pid_t pid, int ctrl_flag) {
    return controller_signal_handler(pid, ctrl_flag);
}

