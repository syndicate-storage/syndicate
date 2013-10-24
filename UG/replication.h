/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

/*
 * Interface for communicating with the replica managers in the Volume.
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

#define REPLICA_POST 1
#define REPLICA_DELETE 2

// chunk of data to upload
struct replica_context {
   vector<CURL*>* curls;        // connections to the replica managers in this Volume
   
   int type;               // block or manifest?
   int op;                 // put or delete?
   
   // data to upload
   struct curl_httppost* form_data;    // what we're uploading
   char* data;             // for POSTing the manifest
   FILE* file;             // for POSTing the block
   off_t size;             // number of bytes to send on POST
   
   struct timespec deadline;    // when we should abort this replication request

   int error;              // error code
   
   sem_t processing_lock;       // released when the context has been processed
   
   bool sync;              // synchronous or asynchronous operation
   
   uint64_t file_id;            // affected file
};

typedef map<CURL*, struct replica_context*> replica_upload_set;

struct syndicate_replication {
   CURLM* running;                      // CURL multi-upload interface
   replica_upload_set* uploads;         // contexts being processed
   pthread_mutex_t running_lock;        // lock for the above information
   
   // hold requests until we have a chance to insert them into CURL (since we don't want to lock the upload process if we can help it)
   replica_upload_set* pending_uploads; // contexts to begin processing on the next loop iteration
   bool has_pending;                    // do we have more work?
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

int fs_entry_delete_manifest_replicas( struct fs_core* core, struct fs_entry* fent, bool sync, struct fs_file_handle* fh );
int fs_entry_delete_block_replicas( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, bool sync, struct fs_file_handle* fh );

int fs_entry_replicate_wait( struct fs_file_handle* fh );

#endif
