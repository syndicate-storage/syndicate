/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _MANIFEST_H_
#define _MANIFEST_H_

#include "fs_entry.h"

// block URL set--a set of blocks for a particular file from a particular host
class block_url_set {
public:
   //char* file_url;            // base URL (no version) to the host and file for the represented blocks
   uint64_t volume_id;        // ID of the Volume this file is in
   uint64_t file_id;          // ID of the file
   uint64_t gateway_id;       // ID of the gateway that hosts them
   uint64_t start_id;         // starting block ID
   uint64_t end_id;           // ending block ID
   int64_t file_version;      // version of this file
   int64_t* block_versions;   // versions of the blocks across the volume
   bool local;                // is this file locally-coordinated?
   bool staging;              // is this block set in staging?

   block_url_set();
   block_url_set( block_url_set& bus );
   block_url_set( uint64_t volume_id, uint64_t gateway_id, uint64_t file_id, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv, bool staging );

   void init( uint64_t volume_id, uint64_t gateway_id, uint64_t file_id, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv, bool staging );

   ~block_url_set();

   // get the version of a block, if we have this block ID.
   // return -1 if not found
   int64_t lookup_version( uint64_t block_id );
   
   // get the gateway_id of a block, if we have this block ID.
   // return 0 if not found
   uint64_t lookup_gateway( uint64_t block_id );

   // is this block in range?
   bool in_range( uint64_t block_id );

   // is this block appendable?
   bool is_appendable( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, bool staging );

   // is this block prependable?
   bool is_prependable( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, bool staging );

   // append the block to end of this url set
   bool append( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, int64_t block_version, bool staging );

   // prepend the block to the beginning of this url set
   bool prepend( uint64_t vid, uint64_t gid, uint64_t fid, uint64_t block_id, int64_t block_version, bool staging );

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
};


typedef map<uint64_t, block_url_set*> block_map;

// SyndicateFS file manifest--provide a way to efficiently get and set the URL for a block
class file_manifest {
public:

   file_manifest();
   file_manifest( file_manifest& fm );
   file_manifest( file_manifest* fm );
   file_manifest( int64_t version );
   file_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg& mmsg );

   ~file_manifest();

   // set the file version
   void set_file_version( struct fs_core* core, int64_t version );

   // parse a manifest form a protobuf
   static int parse_protobuf( struct fs_core* core, struct fs_entry* fent, file_manifest* m, Serialization::ManifestMsg& mmsg );

   // serialize this manifest to protobuf data
   // read-lock the fent first
   void as_protobuf( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg );

   // reload a manifest from a protobuf
   void reload( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg& mmsg );

   // serialize this manifest to a string
   char* serialize_str();

   // get a block's URL for I/O.
   char* get_block_url( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id );

   // get a block's host
   uint64_t get_block_host( struct fs_core* core, uint64_t block_id );

   // put a block URL.
   int put_block( struct fs_core* core, uint64_t gateway_id, struct fs_entry* fent, uint64_t block_id, int64_t block_version, bool staging );

   // directly put a url set
   void put_url_set( block_url_set* bus );

   // truncate a manifest to a new (smaller) size.  Nothing happens if new_end_block is beyond the size of this manifest
   void truncate( uint64_t new_end_block );

   // look up a block version, given a block ID
   int64_t get_block_version( uint64_t block );
   
   // look up a block's coordinator
   uint64_t get_block_gateway( uint64_t block );

   // get all the block versions
   int64_t* get_block_versions( uint64_t start_id, uint64_t end_id );

   // is a block local?
   int is_block_local( struct fs_core* core, uint64_t block_id );

   // is a block staging?
   int is_block_staging( uint64_t block_id );

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
   
private:


   // find a block URL set
   block_map::iterator find_block_set( uint64_t block );

   int64_t file_version;                  // version of the file this manifest represents
   block_map block_urls;                  // map the start block ID to the url information
   struct timespec lastmod;               // which modification time this manifest refers to
   
   bool stale;                            // this manifest is stale, and should be refreshed

   pthread_rwlock_t manifest_lock;
};

int fs_entry_manifest_put_block( struct fs_core* core, uint64_t gateway_id, struct fs_entry* fent, uint64_t block_id, int64_t block_version, bool staging );

#endif
