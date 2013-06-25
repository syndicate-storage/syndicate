/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "driver.h"
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
extern "C" int gateway_generate_manifest( struct gateway_context* replica_ctx, struct gateway_ctx* ctx, struct md_entry* ent ) {
   errorf("%s", "INFO: gateway_generate_manifest\n"); 
   return 0;
}


// read dataset or manifest 
extern "C" ssize_t get_dataset( struct gateway_context* dat, char* buf, size_t len, void* user_cls ) {
   errorf("%s", "INFO: get_dataset\n"); 
    return 0;
}


// get metadata for a dataset
extern "C" int metadata_dataset( struct gateway_context* dat, ms::ms_gateway_blockinfo* info, void* usercls ) {
   errorf("%s", "INFO: metadata_dataset\n"); 
   return 0;
}


// interpret an inbound GET request
extern "C" void* connect_dataset( struct gateway_context* replica_ctx ) {
   errorf("%s", "INFO: connect_dataset\n"); 
    return NULL;
}


// clean up a transfer 
extern "C" void cleanup_dataset( void* cls ) {   
   errorf("%s", "INFO: cleanup_dataset\n"); 
}

extern "C" int publish_dataset (struct gateway_context*, ms_client *client, 
	char* dataset ) {
    mc = client;
    MapParser mp = MapParser(dataset);
    map<string, string>::iterator iter;
    set<char*, path_comp> dir_hierachy;
    mp.parse();
    map<string, string> *fs2sql = mp.get_map();
    for (iter = fs2sql->begin(); iter != fs2sql->end(); iter++) {
	const char* full_path = iter->first.c_str();
	char* path = strrchr((char*)full_path, '/');
	size_t dir_path_len = path - full_path;
	char* dir_path = (char*)malloc(dir_path_len + 1);
	dir_path[dir_path_len] = 0;
	strncpy(dir_path, full_path, dir_path_len);
	dir_hierachy.insert(dir_path);
    }
    set<char*, path_comp>::iterator it;
    for( it = dir_hierachy.begin(); it != dir_hierachy.end(); it++ ) {
	publish (*it, MD_ENTRY_DIR, NULL);
    }

    for (iter = fs2sql->begin(); iter != fs2sql->end(); iter++) {
	publish (iter->first.c_str(), MD_ENTRY_FILE, iter->second.c_str());
    }
    ms_client_destroy(mc);
    return 0;
}

static int publish(const char *fpath, int type, const char* sql_query)
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

    //ment->url_replicas = mc->conf->replica_urls;
    ment->local_path = NULL;
    //ment->ctime_sec = sb->st_ctime;
    ment->ctime_nsec = 0;
    //ment->mtime_sec = sb->st_mtime;
    ment->mtime_nsec = 0;
    //ment->mode = sb->st_mode;
    ment->version = 1;
    ment->max_read_freshness = 360000;
    ment->max_write_freshness = 1;
    //ment->volume = mc->conf->volume;
    //ment->size = sb->st_size;
    //ment->owner = mc->conf->volume_owner;
    switch (type) {
	case MD_ENTRY_DIR:
	    ment->type = MD_ENTRY_DIR;
	    if ( (i = ms_client_mkdir(mc, ment)) < 0 ) {
		cout<<"ms client mkdir "<<i<<endl;
	    }
	    break;
	case MD_ENTRY_FILE:
	    ment->type = MD_ENTRY_FILE;
	    if ( (i = ms_client_create(mc, ment)) < 0 ) {
		cout<<"ms client create "<<i<<endl;
	    }
	    break;
	default:
	    break;
    }
    DATA[ment->path] = ment;
    //delete ment;
    pfunc_exit_code = 0;
    return 0;  
}

