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


#ifndef _COLLATOR_H_
#define _COLLATOR_H_

#include "libsyndicate/util.h"
#include "libsyndicate/libsyndicate.h"
#include "serialization.pb.h"
#include "fs.h"
#include "network.h"

#include <map>
using namespace std;

struct release_entry {
   uint64_t gateway_id;
   Serialization::WriteMsg* acceptMsg;
};

typedef vector< release_entry > release_list;

// send re-integration messages asynchronously
class Collator {
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
