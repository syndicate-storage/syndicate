/*
   Copyright 2014 The Trustees of Princeton University

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

#ifndef _LIBSYNDICATE_MS_LISTDIR_
#define _LIBSYNDICATE_MS_LISTDIR_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/path.h"
#include "libsyndicate/ms/url.h"
#include "libsyndicate/ms/volume.h"

typedef map<struct md_download_context*, int> ms_client_listdir_batch_set;
typedef map<int, int> ms_client_listdir_attempt_set;

// listdir context
struct ms_client_listdir_context {
   
   struct ms_client* client;
   
   uint64_t volume_id;   
   uint64_t parent_id;
   
   queue<int>* batches;                         // which batches to download next
   
   set<uint64_t>* children_ids;                 // file ids of downloaded children
   vector<struct md_entry>* children;           // downloaded children
   
   ms_client_listdir_batch_set* downloading;    // which batches are downloading
   ms_client_listdir_attempt_set* attempts;     // download attempts for a given batch
   
   int listing_error;
   
   pthread_mutex_t lock;
};

extern "C" {
   
int ms_client_listdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, struct ms_client_multi_result* results );
int ms_client_diffdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t least_unknown_generation, struct ms_client_multi_result* results );

}

#endif