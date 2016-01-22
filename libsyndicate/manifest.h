/*
   Copyright 2015 The Trustees of Princeton University

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

#ifndef _LIBSYNDICATE_MANIFEST_H_
#define _LIBSYNDICATE_MANIFEST_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/httpd.h"

// prototype 
struct SG_chunk;

#define SG_MANIFEST_BLOCK_TYPE_MANIFEST SG_messages::ManifestBlock::MANIFEST
#define SG_MANIFEST_BLOCK_TYPE_BLOCK SG_messages::ManifestBlock::BLOCK

struct SG_manifest_block {
  
   int type;            // block represents an actual block, or a manifest? 
   uint64_t block_id;
   int64_t block_version;
   
   unsigned char* hash;
   size_t hash_len;
   
   bool dirty;          // if true, then this block represents locally-written data
};

// map block ID to block 
typedef map< uint64_t, struct SG_manifest_block > SG_manifest_block_map_t;

// syndicate manifest
// keeps track of a file's blocks
struct SG_manifest {
   
   uint64_t volume_id;
   uint64_t coordinator_id;
   uint64_t file_id;
   int64_t file_version;
   
   uint64_t size;       // total file size; filled in by the gateway implementation 
   uint64_t owner_id;   // ID of the user that owns the associated file; filled in by the gateway implementation
   
   bool stale;
   int64_t mtime_sec;   // time of last *replicated* write
   int32_t mtime_nsec;
   
   SG_manifest_block_map_t* blocks;
   
   pthread_rwlock_t lock;
};

// iterate over blocks 
#define SG_manifest_block_iterator SG_manifest_block_map_t::iterator 
#define SG_manifest_block_iterator_begin( manifest ) (*manifest).blocks->begin()
#define SG_manifest_block_iterator_end( manifest ) (*manifest).blocks->end()
#define SG_manifest_block_iterator_id( itr ) itr->first
#define SG_manifest_block_iterator_block( itr ) &itr->second

// manifest methods 
extern "C" {

// constructors
struct SG_manifest_block* SG_manifest_block_alloc( size_t count );
int SG_manifest_block_init( struct SG_manifest_block* dest, uint64_t block_id, int64_t block_version, unsigned char const* hash, size_t hash_len );
int SG_manifest_block_init_from_chunk( struct SG_manifest_block* dest, uint64_t block_id, int64_t block_version, struct SG_chunk* chunk );
int SG_manifest_block_dup( struct SG_manifest_block* dest, struct SG_manifest_block* src );
int SG_manifest_block_load_from_protobuf( struct SG_manifest_block* dest, const SG_messages::ManifestBlock* mblock );
int SG_manifest_block_set_dirty( struct SG_manifest_block* dest, bool dirty );
int SG_manifest_block_set_type( struct SG_manifest_block* dest, int type );

struct SG_manifest* SG_manifest_new();
int SG_manifest_init( struct SG_manifest* manifest, uint64_t volume_id, uint64_t coordinator_id, uint64_t file_id, int64_t file_version );
int SG_manifest_dup( struct SG_manifest* dest, struct SG_manifest* src );
int SG_manifest_load_from_protobuf( struct SG_manifest* manifest, const SG_messages::Manifest* mmsg );
int SG_manifest_load_from_chunk( struct SG_manifest* manifest, struct SG_chunk* chunk );

// destructors 
int SG_manifest_block_free( struct SG_manifest_block* block );
int SG_manifest_free( struct SG_manifest* manifest );
int SG_manifest_block_map_free( SG_manifest_block_map_t* blocks );

// setters   
int SG_manifest_set_file_version( struct SG_manifest* manifest, int64_t version );
int SG_manifest_set_block_dirty( struct SG_manifest* manifest, uint64_t block_id, bool dirty );
int SG_manifest_set_blocks_dirty( struct SG_manifest* manifest, bool dirty );
int SG_manifest_put_block( struct SG_manifest* manifest, struct SG_manifest_block* block, bool replace );
int SG_manifest_put_block_nocopy( struct SG_manifest* manifest, struct SG_manifest_block* block, bool replace );
int SG_manifest_delete_block( struct SG_manifest* manifest, uint64_t block_id );
int SG_manifest_truncate( struct SG_manifest* manifest, uint64_t max_block_id );
int SG_manifest_set_modtime( struct SG_manifest* manifest, int64_t mtime_sec, int32_t mtime_nsec );
int SG_manifest_set_owner_id( struct SG_manifest* manifest, uint64_t owner_id );
int SG_manifest_set_coordinator_id( struct SG_manifest* manifest, uint64_t coordinator_id );
int SG_manifest_set_size( struct SG_manifest* manifest, uint64_t size );
int SG_manifest_set_stale( struct SG_manifest* manifest, bool stale );
int SG_manifest_clear( struct SG_manifest* manifest );
int SG_manifest_clear_nofree( struct SG_manifest* manifest );

int SG_manifest_block_set_version( struct SG_manifest_block* block, int64_t version );
int SG_manifest_block_set_hash( struct SG_manifest_block* block, unsigned char* hash );

// getters 
uint64_t SG_manifest_block_id( struct SG_manifest_block* block );
int64_t SG_manifest_block_version( struct SG_manifest_block* block );
int SG_manifest_block_type( struct SG_manifest_block* block );
bool SG_manifest_block_is_dirty( struct SG_manifest_block* block );
unsigned char* SG_manifest_block_hash( struct SG_manifest_block* block );

uint64_t SG_manifest_get_volume_id( struct SG_manifest* manifest );
uint64_t SG_manifest_get_file_id( struct SG_manifest* manifest );
int64_t SG_manifest_get_file_version( struct SG_manifest* manifest );
uint64_t SG_manifest_get_block_range( struct SG_manifest* manifest );
uint64_t SG_manifest_get_block_count( struct SG_manifest* manifest );
uint64_t SG_manifest_get_file_size( struct SG_manifest* manifest );
int SG_manifest_get_block_hash( struct SG_manifest* manifest, uint64_t block_id, unsigned char** block_hash, size_t* hash_len );
bool SG_manifest_has_block_hash( struct SG_manifest* manifest, uint64_t block_id );
int SG_manifest_get_block_version( struct SG_manifest* manifest, uint64_t block_id, int64_t* block_version );
uint64_t SG_manifest_get_coordinator( struct SG_manifest* manifest );
bool SG_manifest_is_block_present( struct SG_manifest* manifest, uint64_t block_id );
int SG_manifest_get_modtime( struct SG_manifest* manifest, int64_t* mtime_sec, int32_t* mtime_nsec );
int64_t SG_manifest_get_modtime_sec( struct SG_manifest* manifest );
int32_t SG_manifest_get_modtime_nsec( struct SG_manifest* manifest );
bool SG_manifest_is_stale( struct SG_manifest* manifest );
struct SG_manifest_block* SG_manifest_block_lookup( struct SG_manifest*, uint64_t block_id );

// testers 
int SG_manifest_block_hash_eq( struct SG_manifest* manifest, uint64_t block_id, unsigned char* test_hash, size_t test_hash_len );

// serializers 
int SG_manifest_serialize_to_protobuf( struct SG_manifest* manifest, SG_messages::Manifest* mmsg );
int SG_manifest_serialize_blocks_to_request_protobuf( struct SG_manifest* manifest, SG_messages::Request* request );
int SG_manifest_block_serialize_to_protobuf( struct SG_manifest_block* block, SG_messages::ManifestBlock* mblock );

// debugging
int SG_manifest_print( struct SG_manifest* manifest );

// misc 
int SG_manifest_patch( struct SG_manifest* dest, struct SG_manifest* src, bool replace );
int SG_manifest_patch_nocopy( struct SG_manifest* dest, struct SG_manifest* src, bool replace );

}

#endif
