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

typedef map< uint64_t, struct md_entry > ms_client_dir_listing;

// listdir context
struct ms_client_listdir_context {
   
   struct ms_client* client;
   
   uint64_t volume_id;   
   uint64_t parent_id;
   
   queue<int>* batches;                         // which batches of the index to download next
   
   set<uint64_t>* children_ids;                 // file ids of downloaded children
   vector<struct md_entry>* children;           // downloaded children
   
   int listing_error;
   int64_t num_children;
   int64_t capacity;
   
   bool finished;                              // set to true if we get all the children before we're done
   
   pthread_mutex_t lock;
};

extern "C" {
   
int ms_client_listdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t dir_capacity, struct ms_client_multi_result* results );
int ms_client_diffdir( struct ms_client* client, uint64_t parent_id, int64_t num_children, int64_t least_unknown_generation, struct ms_client_multi_result* results );

}

#endif