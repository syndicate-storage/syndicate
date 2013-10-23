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

#define REPLICA_CONTEXT_TYPE_BLOCK 1
#define REPLICA_CONTEXT_TYPE_MANIFEST 2

// chunk of data to upload
struct replica_context {
   vector<CURL*>* curls;
   struct curl_httppost* form_data;    // what we're uploading

   int type;               // block or manifest?
   char* data;             // for the manifest
   FILE* file;             // for the block
   off_t size;             // number of bytes to send
   
   struct timespec deadline;

   int error;              // error code
   
   sem_t processing_lock;
   
   bool sync;              // synchronous or asynchronous upload
   
   uint64_t file_id;
};

typedef map<CURL*, struct replica_context*> replica_upload_set;

struct syndicate_replication {
   CURLM* running;                      // CURL multi-upload interface
   replica_upload_set* uploads;         // pending uploads
   pthread_mutex_t running_lock;        // lock for the above information
   
   // hold requests until we have a chance to insert them into CURL (since we don't want to lock the upload process if we can help it)
   replica_upload_set* pending_uploads; // used for inserting updates
   bool has_pending;
   pthread_mutex_t pending_lock;        // lock for the above pending
   
   pthread_t upload_thread;     // thread to send data to Replica SGs
   
   bool active;                 // set to true when the syndciate_replication thread is running
   
   struct ms_client* ms;
   uint64_t volume_id;
};


int replication_init( struct ms_client* ms, uint64_t volume_id );
int replication_shutdown();

int replica_context_free( struct replica_context* rctx );

int fs_entry_replicate_manifest( struct fs_core* core, struct fs_entry* fent, bool sync, struct fs_file_handle* fh );
int fs_entry_replicate_blocks( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, bool sync, struct fs_file_handle* fh );

int fs_entry_replicate_wait( struct fs_file_handle* fh );
int fs_entry_replicate_wait_and_free( vector<struct replica_context*>* rctxs, struct timespec* timeout );

#endif
