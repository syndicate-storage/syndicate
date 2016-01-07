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

#ifndef _LIBSYNDICATE_MS_PATH_
#define _LIBSYNDICATE_MS_PATH_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/file.h"

// status responses from the MS on a locally cached metadata record
#define MS_LISTING_NEW          ms::ms_listing::NEW             // new entry
#define MS_LISTING_NOCHANGE     ms::ms_listing::NOT_MODIFIED    // entry/listing not modified
#define MS_LISTING_NONE         ms::ms_listing::NONE            // entry doesn't exist


// path entry metadata for getting metadata listings
// not all entries need to be set; it depends on the context in which it is used
struct ms_path_ent {
   uint64_t volume_id;
   uint64_t file_id;
   uint64_t parent_id;
   int64_t version;
   int64_t write_nonce;
   int64_t num_children;
   int64_t generation;
   int64_t capacity;
   
   char* name;

   void* cls;
};

// directory listing
struct ms_listing {
   int status;       
   int type;         // file or directory?
   vector<struct md_entry>* entries;
   
   int error;
};

// list of path entries is a path 
typedef vector< struct ms_path_ent > ms_path_t;

extern "C" {
 
// structures
int ms_client_make_path_ent( struct ms_path_ent* path_ent, uint64_t volume_id, uint64_t parent_id, uint64_t file_id, int64_t version, int64_t write_nonce,
                             int64_t num_children, int64_t generation, int64_t capacity, char const* name, void* cls );

// downloads 
int ms_client_path_download( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* listings );
int ms_client_path_download_ent_head( struct ms_path_ent* path_ent, uint64_t volume_id, uint64_t parent_id, uint64_t file_id, char const* name, void* cls );
int ms_client_path_download_ent_tail( struct ms_path_ent* path_ent, uint64_t volume_id, char const* name, void* cls );

// list parsing
int ms_client_listing_read_entry( struct ms_client* client, struct md_download_context* dlctx, struct md_entry* ent, int* listing_error );
int ms_client_listing_read_entries( struct ms_client* client, struct md_download_context* dlctx, struct md_entry** ents, size_t* num_ents, int* listing_error );

// memory management
void ms_client_free_listing( struct ms_listing* listing );
void ms_client_free_path_ent( struct ms_path_ent* path_ent, void (*free_cls)(void*) );
void ms_client_free_path( ms_path_t* path, void (*free_cls)(void*) );

// serialization
char* ms_path_to_string( ms_path_t* ms_path, int max_index );

// getters 
void* ms_client_path_ent_get_cls( struct ms_path_ent* ent );

// setters 
void ms_client_path_ent_set_cls( struct ms_path_ent* ent, void* cls );

}
#endif