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

/*
 * Interface for communicating with the RGs in the Volume.
 * This module has two related purposes: replicating new manifests and blocks,
 * and garbage-collecting old manifests and blocks.  Garbage-collecting is really
 * just the act of asking the RG to delete a particular block or manifest, but
 * processing garbage-collection requests is the same to CURL as processing
 * replication requests, so a lot of the infrastructure for replication 
 * gets reused.
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

#include "libsyndicate/libsyndicate.h"
#include "fs_entry.h"

using namespace std;

#define REPLICA_CONTEXT_TYPE_BLOCK 1
#define REPLICA_CONTEXT_TYPE_MANIFEST 2

#define REPLICA_POST 1
#define REPLICA_DELETE 2

// snapshot of vital fs_entry fields for replication and garbage collection
struct replica_snapshot {
   uint64_t file_id;
   int64_t file_version;
   uint64_t owner_id;
   uint64_t writer_id;          // our gateway ID
   uint64_t coordinator_id;     // file's coordinator ID
   uint64_t volume_id;
   uint32_t max_write_freshness;
   off_t size;
   int64_t fent_mtime_sec;
   int32_t fent_mtime_nsec;
   
   // for block replication 
   uint64_t block_id;
   int64_t block_version;
   
   // for manifest replication 
   int64_t manifest_mtime_sec;
   int32_t manifest_mtime_nsec;
   bool manifest_initialized;
};

// chunk of data to upload
struct replica_context {
   vector<CURL*>* curls;        // connections to the RGs in this Volume
   
   int type;               // block or manifest?
   int op;                 // put or delete?
   
   // data to upload
   struct curl_httppost* form_data;    // what we're uploading
   
   char* data;             // for POSTing the manifest
   FILE* file;             // for POSTing the block
   
   unsigned char* hash;         // hash of whatever we are POSTing
   size_t hash_len;
   
   off_t size;             // number of bytes to send on POST
   
   struct timespec deadline;    // when we should abort this replication request

   int error;              // error code
   
   sem_t processing_lock;       // released when the context has been processed
   
   bool free_on_processed;    // if true, destroy this structure when it is processed
   
   struct replica_snapshot snapshot;            // fent metadata
};

typedef map<CURL*, struct replica_context*> replica_upload_set;
typedef vector<struct replica_snapshot> replica_cancel_list;
typedef set<CURL*> replica_expire_set;
typedef vector<struct replica_context*> replica_list_t;

struct rg_client {
   char* process_name;            // used for logging
   
   CURLM* running;                      // CURL multi-upload interface
   replica_upload_set* uploads;         // contexts being processed
   pthread_mutex_t running_lock;        // lock for the above information
   
   // hold requests until we have a chance to insert them into CURL (since we don't want to lock the upload process if we can help it)
   replica_upload_set* pending_uploads; // contexts to begin processing on the next loop iteration
   bool has_pending;                    // do we have more work?
   pthread_mutex_t pending_lock;        // lock for the above pending
   
   // hold cancellations until we have a chance to remove them from CURL (since we don't want to lock the upload process if we can help it)
   replica_cancel_list* pending_cancels;   // contexts to remove on the next loop iteration
   bool has_cancels;
   pthread_mutex_t cancel_lock;
   
   // hold expirations until we have a chance to remove them from CURL (since we don't want to lock the upload process if we can help it)
   replica_expire_set* pending_expires;
   bool has_expires;
   pthread_mutex_t expire_lock;
   
   pthread_t upload_thread;     // thread to send data to Replica SGs
   
   bool active;                 // set to true when the syndciate_replication thread is running
   bool accepting;              // set to true if we're accepting new uploads
   
   int num_uploads;             // number of uploads running; safe for readers to access
   
   struct ms_client* ms;
   struct md_syndicate_conf* conf;
   uint64_t volume_id;
};

struct syndicate_state;

int replication_init( struct syndicate_state* state, uint64_t volume_id );
int replication_shutdown( struct syndicate_state* state, int wait_replicas );

int replica_context_free( struct replica_context* rctx );

int fs_entry_replicate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent );
struct replica_context* fs_entry_replicate_manifest_async( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int* ret );

struct replica_context* fs_entry_replicate_block_async( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, struct fs_entry_block_info* binfo, int* ret );

int fs_entry_garbage_collect_manifest( struct fs_core* core, struct replica_snapshot* snapshot );
int fs_entry_garbage_collect_blocks( struct fs_core* core, struct replica_snapshot* snapshot, modification_map* modified_blocks );
int fs_entry_garbage_collect_file( struct fs_core* core, struct fs_entry* fent );

int fs_entry_replica_snapshot( struct fs_core* core, struct fs_entry* snapshot_fent, uint64_t block_id, int64_t block_version, struct replica_snapshot* snapshot );
int fs_entry_replica_snapshot_restore( struct fs_core* core, struct fs_entry* fent, struct replica_snapshot* snapshot );

int fs_entry_replica_wait( struct fs_core* core, struct replica_context* rctx, uint64_t transfer_timeout_ms );
int fs_entry_replica_wait_all( struct fs_core* core, replica_list_t* rctxs, uint64_t transfer_timeout_ms );

int fs_entry_replica_context_free( struct replica_context* rctx );
int fs_entry_replica_list_free( replica_list_t* rctxs );

int fs_entry_replicate_blocks( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks );
int fs_entry_replicate_blocks_async( struct fs_core* core, struct fs_entry* fent, modification_map* modified_blocks, replica_list_t* replica_futures );

int fs_entry_extract_block_info_from_failed_block_replicas( replica_list_t* rctxs, modification_map* dirty_blocks );


#endif
