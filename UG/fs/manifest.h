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
   char* file_url;            // base URL (no version) to the host and file for the represented blocks
   uint64_t start_id;         // starting block ID
   uint64_t end_id;           // ending block ID
   int64_t file_version;      // version of this file
   int64_t* block_versions;   // versions of the blocks across the volume

   block_url_set();
   block_url_set( block_url_set& bus );
   block_url_set( char const* url, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv );
   block_url_set( Serialization::BlockURLSetMsg& busmsg );

   void init( char const* url, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv );

   ~block_url_set();

   // get the version of a block, if we have this block ID.
   // return -1 if not found
   int64_t lookup_version( uint64_t block_id );

   // generate a URL, depending on which versions we want
   char* make_url( int64_t file_version, uint64_t block_id );

   // compare a block url to another
   bool url_equals( char const* other_file_url );

   // is this block in range?
   bool in_range( uint64_t block_id );

   // is this block appendable?
   bool is_appendable( char const* file_url, uint64_t block_id );

   // is this block prependable?
   bool is_prependable( char const* file_url, uint64_t block_id );

   // append the block to end of this url set
   bool append( char const* file_url, uint64_t block_id, int64_t block_version );

   // prepend the block to the beginning of this url set
   bool prepend( char const* file_url, uint64_t block_id, int64_t block_version );

   // remove blocks from the end of this URL set.  return true if blocks were removed
   bool truncate( uint64_t new_end_id );

   // grow left by one, and put a version in
   bool grow_left( int64_t block_version );

   // grow right by one, and put a version in
   bool grow_right( int64_t block_version );

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

   file_manifest( struct fs_core* core );
   file_manifest( file_manifest& fm );
   file_manifest( file_manifest* fm );
   file_manifest( struct fs_core*, int64_t version );
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

   // get a block's URL for I/O.  Local block URLs will NOT be versioned.  Remote block URLs will be versioned.
   char* get_block_url( int64_t file_version, uint64_t block );

   // put a block URL.
   bool put_block_url( char const* file_url, int64_t file_version, uint64_t block_id, int64_t block_version );

   // directly put a url set
   void put_url_set( block_url_set* bus );

   // truncate a manifest to a new (smaller) size.  Nothing happens if new_end_block is beyond the size of this manifest
   void truncate( uint64_t new_end_block );

   // look up a block version, given a block ID
   int64_t get_block_version( uint64_t block );

   // get all the block versions
   int64_t* get_block_versions( uint64_t start_id, uint64_t end_id );

   // is a block local?
   bool is_block_local( uint64_t block_id );

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

   // get lastmod time
   void get_lastmod( struct timespec* ts ) {
      pthread_rwlock_rdlock( &this->manifest_lock );
      memset( ts, 0, sizeof(struct timespec) );
      ts->tv_sec = this->lastmod.tv_sec;
      ts->tv_nsec = this->lastmod.tv_nsec;
      pthread_rwlock_unlock( &this->manifest_lock );
   }

   void set_lastmod( struct timespec* ts ) {
      pthread_rwlock_rdlock( &this->manifest_lock );
      this->lastmod.tv_sec = ts->tv_sec;
      this->lastmod.tv_nsec = ts->tv_nsec;
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
   int get_range( uint64_t block_id, uint64_t* start_id, uint64_t* end_id, char** url );
   
private:


   // find a block URL set
   block_map::iterator find_block_set( uint64_t block );

   int64_t file_version;                  // version of the file this manifest represents
   block_map block_urls;                  // map the start block ID to the url information
   struct timespec lastmod;               // which modification time this manifest refers to
   
   bool stale;                            // this manifest is stale, and should be refreshed

   pthread_rwlock_t manifest_lock;
};

int fs_entry_put_block_url( struct fs_entry* fent, char const* file_url, int64_t file_version, uint64_t block_id, int64_t block_version );

#endif
