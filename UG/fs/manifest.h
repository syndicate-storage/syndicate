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

#ifndef _MANIFEST_H_
#define _MANIFEST_H_

#include "fs_entry.h"

// block URL set--a set of blocks for a particular file from a particular host
class block_url_set {
public:
   uint64_t volume_id;        // ID of the Volume this file is in
   uint64_t file_id;          // ID of the file
   uint64_t gateway_id;       // ID of the UG that wrote them last (0 if this is a write hole)
   uint64_t start_id;         // starting block ID
   uint64_t end_id;           // ending block ID
   int64_t file_version;      // version of this file
   int64_t* block_versions;   // versions of the blocks in this set
   unsigned char* block_hashes;       // hashes of the blocks in this set, interpreted as intervals of BLOCK_HASH_LEN()
   
   block_url_set();
   block_url_set( block_url_set& bus );
   block_url_set( uint64_t volume_id, uint64_t gateway_id, uint64_t file_id, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv, unsigned char* hashes );

   void init( uint64_t volume_id, uint64_t gateway_id, uint64_t file_id, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv, unsigned char* hashes );

   ~block_url_set();

   // get the version of a block, if we have this block ID.
   // return -1 if not found
   int64_t lookup_version( uint64_t block_id );
   
   // compare the hash of a block to a given hash.
   // return 0 if equal.
   // return 1 if not equal.
   // return -ENOENT if block not found
   int hash_cmp( uint64_t block_id, unsigned char* hash );
   
   // duplicate a hash
   unsigned char* hash_dup( uint64_t block_id );
   
   // get the gateway_id of a block, if we have this block ID.
   // return 0 if not found
   uint64_t lookup_gateway( uint64_t block_id );

   // is this block in range?
   bool in_range( uint64_t block_id );

   // is this block appendable?
   bool is_appendable( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id );

   // is this block prependable?
   bool is_prependable( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id );

   // append the block to end of this url set
   bool append( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, int64_t block_version, unsigned char* hash );

   // prepend the block to the beginning of this url set
   bool prepend( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, int64_t block_version, unsigned char* hash );

   // remove blocks from the end of this URL set.  return true if blocks were removed
   bool truncate( uint64_t new_end_id );

   // shrink left by one
   bool shrink_left();

   // shrink right by one
   bool shrink_right();

   // range size
   uint64_t size();

   // split left
   block_url_set* split_left( uint64_t block_id );

   // split right
   block_url_set* split_right( uint64_t block_id );

   // populate a protobuf structure with our data
   void as_protobuf( struct fs_core* core, Serialization::BlockURLSetMsg* busmsg );
   
   // is this a hole?
   bool is_hole() { return this->gateway_id == 0; }
};


typedef map<uint64_t, block_url_set*> block_map;

// SyndicateFS file manifest--provide a way to efficiently get and set the URL for a block
class file_manifest {
public:

   file_manifest();
   file_manifest( file_manifest& fm );
   file_manifest( file_manifest* fm );
   file_manifest( int64_t version );
   file_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

   ~file_manifest();

   // set the file version
   void set_file_version( struct fs_core* core, int64_t version );
   
   // get the number of blocks
   uint64_t get_num_blocks();

   // parse a manifest form a protobuf
   static int parse_protobuf( struct fs_core* core, struct fs_entry* fent, file_manifest* m, Serialization::ManifestMsg* mmsg );

   // serialize this manifest to protobuf data
   // read-lock the fent first
   void as_protobuf( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

   // reload a manifest from a protobuf
   void reload( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

   // serialize this manifest to a string
   char* serialize_str();
   
   // compare a block hash
   int hash_cmp( uint64_t block_id, unsigned char* hash );
   
   // get a block hash
   unsigned char* hash_dup( uint64_t block_id );

   // get a block's URL for I/O.
   char* get_block_url( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id );

   // get a block's host
   uint64_t get_block_host( struct fs_core* core, uint64_t block_id );

   // put a block URL.
   int put_block( struct fs_core* core, uint64_t gateway_id, struct fs_entry* fent, uint64_t block_id, int64_t block_version, unsigned char* block_hash );
   
   // put a block hole 
   int put_hole( struct fs_core* core, struct fs_entry* fent, uint64_t block_id );

   // truncate a manifest to a new (smaller) size.  Nothing happens if new_end_block is beyond the size of this manifest
   void truncate( uint64_t new_end_block );

   // look up a block version, given a block ID
   int64_t get_block_version( uint64_t block );

   // get all the block versions
   int64_t* get_block_versions( uint64_t start_id, uint64_t end_id );
   
   // get a block's hash
   unsigned char* get_block_hash( uint64_t block_id );
   
   // get the hashes of the blocks in a certain range 
   unsigned char** get_block_hashes( uint64_t start_id, uint64_t end_id );

   // is a block local?
   int is_block_local( struct fs_core* core, uint64_t block_id );
   
   // is this block part of a hole?  As in, is there data beyond this block id, but none for this one?
   bool is_hole( uint64_t block_id );
   
   // is a block present?
   bool is_block_present( uint64_t block_id );

   // mark the manifest as stale
   void mark_stale() {
      pthread_rwlock_wrlock( &this->manifest_lock );
      this->stale = true;
      pthread_rwlock_unlock( &this->manifest_lock );
   }

   // is the manifest stale?
   bool is_stale() {
      pthread_rwlock_rdlock( &this->manifest_lock );
      bool ret = this->stale;
      pthread_rwlock_unlock( &this->manifest_lock );
      return ret;
   }
   
   // is the manifest initialized?
   bool is_initialized() {
      pthread_rwlock_rdlock( &this->manifest_lock );
      bool ret = this->initialized;
      pthread_rwlock_unlock( &this->manifest_lock );
      return ret;
   }
   
   // mark the manifest as initialized
   void mark_initialized() { 
      pthread_rwlock_wrlock( &this->manifest_lock );
      this->initialized = true;
      pthread_rwlock_unlock( &this->manifest_lock );
   }

   int64_t get_file_version() {
      pthread_rwlock_rdlock( &this->manifest_lock );
      int64_t ret = this->file_version;
      pthread_rwlock_unlock( &this->manifest_lock );
      return ret;
   }


   // attempt to merge two adjacent block url sets, given the starting block ID to the first
   bool merge_adjacent( uint64_t block_id );

   // get the range of a block
   int get_range( uint64_t block_id, uint64_t* start_id, uint64_t* end_id, uint64_t* gateway_id );
   
   // get the modtime 
   int get_modtime( struct timespec* ts ) {
      pthread_rwlock_rdlock( &this->manifest_lock );
      ts->tv_sec = this->lastmod.tv_sec;
      ts->tv_nsec = this->lastmod.tv_nsec;
      pthread_rwlock_unlock( &this->manifest_lock );
      return 0;
   }
   
   // get the modtime 
   int get_modtime( int64_t* mtime_sec, int32_t* mtime_nsec ) {
      pthread_rwlock_rdlock( &this->manifest_lock );
      *mtime_sec = this->lastmod.tv_sec;
      *mtime_nsec = this->lastmod.tv_nsec;
      pthread_rwlock_unlock( &this->manifest_lock );
      return 0;
   }
   
   // set the modtime 
   int set_modtime( uint64_t mtime_sec, uint32_t mtime_nsec ) {
      pthread_rwlock_wrlock( &this->manifest_lock );
      this->lastmod.tv_sec = mtime_sec;
      this->lastmod.tv_nsec = mtime_nsec;
      pthread_rwlock_unlock( &this->manifest_lock );
      return 0;
   }
   
   
private:


   // find a block URL set
   block_map::iterator find_block_set( uint64_t block );

   int64_t file_version;                  // version of the file this manifest represents
   block_map block_urls;                  // map the start block ID to the url information
   struct timespec lastmod;               // which modification time this manifest refers to
   
   bool stale;                            // this manifest is stale, and should be refreshed
   bool initialized;                      // this manifest exists, but has not been initialized.

   pthread_rwlock_t manifest_lock;
};

int fs_entry_manifest_put_block( struct fs_core* core, uint64_t gateway_id, struct fs_entry* fent, uint64_t block_id, int64_t block_version, unsigned char* block_hash );

int fs_entry_manifest_error( Serialization::ManifestMsg* mmsg, int error, char const* errormsg );

#endif
