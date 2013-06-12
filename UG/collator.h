/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _COLLATOR_H_
#define _COLLATOR_H_

#include "util.h"
#include "libsyndicate.h"
#include "serialization.pb.h"
#include "fs.h"

#include <map>
using namespace std;

struct release_entry {
   char* content_url;
   Serialization::WriteMsg* acceptMsg;
};

typedef vector< release_entry > release_list;

// send re-integration messages asynchronously
class Collator : public TransactionProcessor {
public:
   Collator( struct fs_core* core );
   ~Collator();
   
   // start collating
   int start();

   // stop collating
   int stop();
   
   // tell remote host to release blocks
   int release_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_block_id, uint64_t end_block_id );

private:
   
   // release thread
   pthread_t release_thread;
   release_list release_queue;
   sem_t release_sem;
   pthread_mutex_t release_queue_lock;
   CURL* release_curl;

   struct fs_core* core;
   
   // releaselation thread method
   static void* release_loop( void* );

   bool running;
   bool stopped;
   
   // insert entries to be released
   int queue_release( struct fs_core* core, vector<struct collator_entry*>* release );
   int queue_release( struct fs_core* core, struct collator_entry* ent );
   int insert_release( struct fs_core* core, char const* writer_url, Serialization::WriteMsg* acceptMsg );
};


// asynchronously release remote blocks
int fs_entry_release_remote_blocks( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t start_block_id, uint64_t end_block_id );

#endif
