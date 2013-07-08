/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _REPLICATION_H_
#define _REPLICATION_H_

#include <set>
#include <list>
#include <string>
#include <locale>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <curl/curl.h>
#include <openssl/sha.h>
#include <dirent.h>

#include "libsyndicate.h"
#include "log.h"
#include "fs.h"

using namespace std;

#define REPLICA_UPLOAD_OK     0
#define REPLICA_UPLOAD_BUSY   1


// chunk of data to upload
struct RG_upload {
   char* path;             // path of entity we're replicating
   struct curl_httppost* form_data;    // what we're posting
   int running;            // how many RGs are we still talking to?
   
   char* data;             // for the manifest
   FILE* file;             // for the block

   int error;              // error code

   bool sync;              // synchronous replication?  If so, then the following data will be set
   pthread_cond_t sync_cv;
   pthread_mutex_t sync_lock;
};


int RG_upload_init_block( struct RG_upload* rup, struct ms_client* ms, char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, int64_t mtime_sec, int32_t mtime_nsec, bool sync );
int RG_upload_init_manifest( struct RG_upload* rup, struct ms_client* ms, char* manifest_data, size_t manifest_data_len, char const* fs_path, int64_t file_version, int64_t mtime_sec, int32_t mtime_nsec, bool sync );
void RG_upload_destroy( struct RG_upload* rup );

typedef vector<struct RG_upload*> upload_list;

struct RG_channel {
   CURL* curl_h;
   upload_list* pending;
   pthread_mutex_t pending_lock;
   char* url;
};

// upload threadpool
class ReplicaUploader : public CURLTransfer {
public:
   
   ReplicaUploader( struct ms_client* ms );
   ~ReplicaUploader();

   void add_replica( struct RG_upload* rup );
   
   // start running
   int start();

   // stop running
   int cancel();

   // are we running?
   bool running;

   // are we stopped?
   bool stopped;

   // lock must be held when we're downloading stuff via CURL
   pthread_mutex_t download_lock;

   int get_num_RGs() { return this->num_RGs; }

private:
   
   // get a replica handle that finished downloading
   struct RG_channel* next_ready_RG( int* err );
   
   struct ms_client* ms;

   // upload thread
   pthread_t thread;
   
   static void* thread_main(void* arg);
   
   void finish_replica( struct RG_channel* rsc, int status );
   void enqueue_replica( struct RG_channel* rsc, struct RG_upload* rup );
   int start_next_replica( struct RG_channel* rsc );
   
   // list of connections to our replica gateways
   uint64_t volume_version;
   struct RG_channel* RGs;
   int num_RGs;

   struct curl_slist** headers;
};


int replication_init( struct ms_client* ms );
int replication_shutdown();

int fs_entry_replicate_write( struct fs_core* core, struct fs_file_handle* fh, modification_map* modified_blocks, bool sync );
int fs_entry_replicate_wait( struct fs_file_handle* fh );

#endif
