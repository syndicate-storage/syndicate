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

class ReplicaUploader;
struct replica_server_channel;

// upload state machine
class ReplicaUpload {
public:
   
   char* path;
   char* data;    // for the manifest
   bool verify_peer;
   struct curl_httppost* form_data;

   pthread_mutex_t lock;

   ReplicaUpload() {
      this->fh = NULL;
      this->verify_peer = false;
      this->path = NULL;
      this->form_data = NULL;
      this->status = NULL;
      this->ref_count = 0;
      this->wait_lock = NULL;
      this->wait_ref_count = 0;
      this->fh = NULL;
      pthread_mutex_init( &this->lock, NULL );
   }
   
   ReplicaUpload( struct md_syndicate_conf* conf );
   ~ReplicaUpload();
   
   int setup_block( struct fs_file_handle* fh, char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, int64_t mtime_sec, int32_t mtime_nsec, uint64_t blocking_factor, bool verify_peer );
   int setup_manifest( struct fs_file_handle* fh, char* manifest_data, size_t manifest_data_len, char const* fs_path, int64_t file_version, int64_t mtime_sec, int32_t mtime_nsec, bool verify_peer );

   int ref() {
      pthread_mutex_lock( &this->lock );
      this->ref_count++;
      int rc = this->ref_count;

      if( rc == this->wait_ref_count ) {
         pthread_mutex_unlock( this->wait_lock );
         this->wait_lock = NULL;
         this->wait_ref_count = 0;
      }
      
      pthread_mutex_unlock( &this->lock );
      return rc;
   }

   int unref() {
      pthread_mutex_lock( &this->lock );
      this->ref_count--;
      int rc = this->ref_count;

      if( rc == this->wait_ref_count ) {
         pthread_mutex_unlock( this->wait_lock );
         this->wait_lock = NULL;
         this->wait_ref_count = 0;
      }
      
      pthread_mutex_unlock( &this->lock );
      return rc;
   }
   
   void set_status( int id, int stat ) {
      this->status[id] = stat;
   }

   int get_status( int id ) {
      return this->status[id];
   }

   // wait for the reference count to reach a certain value, and unlock the given lock.
   // only one thread at a time can do this.
   int wait_ref( int ref_count, pthread_mutex_t* wait_lock ) {
      pthread_mutex_lock( &this->lock );

      if( this->wait_lock != NULL ) {
         pthread_mutex_unlock( &this->lock );
         return -EBUSY;
      }
      
      if( ref_count == this->ref_count ) {
         pthread_mutex_unlock( wait_lock );
      }
      else {
         this->wait_lock = wait_lock;
         this->wait_ref_count = ref_count;
      }

      pthread_mutex_unlock( &this->lock );
      return 0;
   }

   int get_ref() {
      pthread_mutex_lock( &this->lock );
      int rc = this->ref_count;
      pthread_mutex_unlock( &this->lock );
      return rc;
   }

   int* status;
   struct fs_file_handle* fh;
   
private:

   int ref_count;
   
   pthread_mutex_t* wait_lock;
   int wait_ref_count;

   FILE* file;
};


typedef vector<ReplicaUpload*> upload_list;
typedef map<struct fs_file_handle*, int> running_map;    // path hash --> how many replicas left

struct replica_server_channel {
   CURL* curl_h;
   upload_list* pending;
   pthread_mutex_t pending_lock;
   char* url;
   int id;
};

// upload threadpool
class ReplicaUploader : public CURLTransfer {
public:
   
   ReplicaUploader( struct md_syndicate_conf* conf );
   ~ReplicaUploader();
   
   // replicate a block
   ReplicaUpload* start_replica_block( struct fs_file_handle* fh, char const* data_root, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version, int64_t mtime_sec, int32_t mtime_nsec );

   // replicate a manifest 
   ReplicaUpload* start_replica_manifest( struct fs_file_handle* fh, char* manifest_str, size_t manifest_len, char const* fs_path, int64_t file_version, int64_t mtime_sec, int32_t mtime_nsec );
   
   // start running
   int start();

   // stop running
   int cancel();

   // how many replica servers do we talk to?
   int get_num_replica_servers() { return this->num_replica_servers; }
   
   // are we running?
   bool running;

   // are we stopped?
   bool stopped;

   pthread_mutex_t download_lock;

   int ref_replica_upload( ReplicaUpload* ru );
   int unref_replica_upload( ReplicaUpload* ru );

   int get_running( struct fs_file_handle* fh ) {
      pthread_mutex_lock( &this->running_lock );

      int rc = -1;
      running_map::iterator itr = this->running_refs.find( fh );
      if( itr != this->running_refs.end() ) {
         rc = itr->second;
      }

      pthread_mutex_unlock( &this->running_lock );
      return rc;
   }

private:
   
   // get a replica handle that finished downloading
   struct replica_server_channel* next_ready_server( int* err );
   
   struct md_syndicate_conf* conf;

   // upload thread
   pthread_t thread;
   
   static void* thread_main(void* arg);
   
   void finish_replica( struct replica_server_channel* rsc, int status );
   void enqueue_replica( struct replica_server_channel* rsc, ReplicaUpload* ru );
   int start_next_replica( struct replica_server_channel* rsc );

   pthread_mutex_t running_lock;
   running_map running_refs;

   // list of connections to our replica servers
   struct replica_server_channel* replica_servers;
   int num_replica_servers;

   struct curl_slist** headers;
};

int replication_init( struct md_syndicate_conf* conf );
int replication_shutdown();

int fs_entry_replicate_write( struct fs_core* core, struct fs_file_handle* fh, modification_map* modified_blocks, bool sync );
int fs_entry_replicate_wait( struct fs_file_handle* fh );

#endif
