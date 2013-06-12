/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "manifest.h"
#include "url.h"

// default constructor
block_url_set::block_url_set() {
   this->file_url = NULL;
   this->start_id = -1;
   this->end_id = -1;
   this->block_versions = NULL;
   this->file_version = -1;
}

// value constructor
block_url_set::block_url_set( char const* url, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv ) {
   this->init( url, file_version, start, end, bv );
}


// copy constructor
block_url_set::block_url_set( block_url_set& bus ) {
   this->init( bus.file_url, bus.file_version, bus.start_id, bus.end_id, bus.block_versions );
}


// protobuf constructor
block_url_set::block_url_set( Serialization::BlockURLSetMsg& busmsg ) {
   this->file_url = strdup( busmsg.file_url().c_str() );
   this->start_id = busmsg.start_id();
   this->end_id = busmsg.end_id();
   this->block_versions = CALLOC_LIST( int64_t, this->end_id - this->start_id );
   
   for( int i = 0; i < busmsg.block_versions_size(); i++ ) {
      this->block_versions[i] = busmsg.block_versions(i);
   }
}


// initialization
void block_url_set::init( char const* url, int64_t file_version, uint64_t start, uint64_t end, int64_t* bv ) {
   this->file_url = strdup(url);
   this->start_id = start;
   this->end_id = end;
   this->file_version = file_version;
   this->block_versions = CALLOC_LIST( int64_t, end - start );
   memcpy( this->block_versions, bv, sizeof(int64_t) * (end - start) );
   
   dbprintf( "%s.%" PRId64 "[%" PRIu64 "-%" PRIu64 "]\n", url, file_version, start, end );
}


// destructor
block_url_set::~block_url_set() {
   if( this->file_url ) {
      free( this->file_url );
      this->file_url = NULL;
   }
   if( this->block_versions ) {
      free( this->block_versions );
      this->block_versions = NULL;
   }
   start_id = -1;
   end_id = -1;
}


// look up the version
int64_t block_url_set::lookup_version( uint64_t block_id ) {
   if( this->in_range( block_id ) )
      return this->block_versions[ block_id - this->start_id ];
   else
      return -1;
}


// make a URL from a block ID and a few flags
char* block_url_set::make_url( int64_t file_version, uint64_t block_id ) {
   return fs_entry_generic_url( this->file_url, file_version, block_id, this->lookup_version( block_id ) );
}


// compare two urls through normalization
bool block_url_set::url_equals( char const* other_file_url ) {
   int rc = 0;
   char* my_url = md_normalize_url( this->file_url, &rc );
   if( rc != 0 )
      return false;     // error

   char* other_url = md_normalize_url( other_file_url, &rc );
   if( rc != 0 ) {
      free( my_url );
      return false;     // error
   }

   //printf("'%s' =?= '%s'\n", my_url, other_url);
   bool equ = (strcmp( my_url, other_url ) == 0);

   free( my_url );
   free( other_url );

   return equ;
}


// is a block id in a range?
bool block_url_set::in_range( uint64_t block_id ) { return (block_id >= this->start_id && block_id < this->end_id); }

// is a block appendable?
bool block_url_set::is_appendable( char const* url, uint64_t block_id ) { return block_id == this->end_id && this->url_equals( url ); }

// is a block prependable?
bool block_url_set::is_prependable( char const* url, uint64_t block_id ) { return block_id + 1 == this->start_id && this->url_equals( url ); }

// size of a block range
uint64_t block_url_set::size() { return this->end_id - this->start_id; }

// append a block to a set
bool block_url_set::append( char const* url, uint64_t block_id, int64_t block_version ) {
   if( this->is_appendable( url, block_id ) ) {
      this->end_id++;
      int64_t* tmp = (int64_t*)realloc( this->block_versions, (this->end_id - this->start_id) * sizeof(int64_t) );
      tmp[ this->end_id - 1 - this->start_id ] = block_version;
      this->block_versions = tmp;

      return true;
   }
   else {
      return false;
   }
}


// prepend a block to a set
bool block_url_set::prepend( char const* url, uint64_t block_id, int64_t block_version ) {
   if( this->is_prependable( url, block_id ) ) {
      // shift everyone down
      this->start_id--;
      int64_t* tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );
      tmp[ 0 ] = block_version;
      memcpy( tmp + 1, this->block_versions, sizeof(int64_t) * (this->end_id - this->start_id - 1) );

      free( this->block_versions);
      this->block_versions = tmp;

      return true;
   }
   else {
      return false;
   }
}


// truncate a block set.  return true on success
bool block_url_set::truncate( uint64_t new_end_id ) {
   if( this->in_range( new_end_id ) ) {
      this->end_id = new_end_id;
      return true;
   }
   else {
      return false;
   }
}


// grow one unit to the left
bool block_url_set::grow_left( int64_t version ) {
   if( this->start_id == 0 )
      return false;

   this->start_id--;
   int64_t* tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );

   tmp[0] = version;
   memcpy( tmp + 1, this->block_versions, sizeof(int64_t) * (this->end_id - this->start_id - 1) );

   free( this->block_versions );
   this->block_versions = tmp;

   return true;
}


// grow one unit to the right
bool block_url_set::grow_right( int64_t version ) {
   this->end_id++;

   int64_t* tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );

   memcpy( tmp, this->block_versions, sizeof(int64_t) * (this->end_id - this->start_id - 1) );
   tmp[ this->end_id - 1 ] = version;

   free( this->block_versions );
   this->block_versions = tmp;

   if( URL_LOCAL( this->file_url ) ) {
      tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );

      memcpy( tmp, this->block_versions, sizeof(int64_t) * (this->end_id - this->start_id - 1) );
      tmp[ this->end_id - 1 ] = version;

      free( this->block_versions );
      this->block_versions = tmp;
   }

   return true;
}


// shrink one unit from the left
bool block_url_set::shrink_left() {
   if( this->start_id + 1 >= this->end_id )
      return false;

   this->start_id++;
   int64_t* tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );

   memcpy( tmp, this->block_versions + 1, sizeof(int64_t) * (this->end_id - this->start_id) );
   free( this->block_versions );
   this->block_versions = tmp;

   return true;
}


// shrink one unit from the right
bool block_url_set::shrink_right() {
   if( this->start_id + 1 >= this->end_id )
      return false;

   this->end_id--;
   int64_t* tmp = CALLOC_LIST( int64_t, this->end_id - this->start_id );

   memcpy( tmp, this->block_versions, sizeof(int64_t) * (this->end_id - this->start_id) );
   free( this->block_versions );
   this->block_versions = tmp;

   return true;
}


// split to the left
block_url_set* block_url_set::split_left( uint64_t block_id ) {
   block_url_set* ret = new block_url_set( this->file_url, this->file_version, this->start_id, block_id, this->block_versions );
   return ret;
}


// split to the right
block_url_set* block_url_set::split_right( uint64_t block_id ) {
   int64_t off = (int64_t)(block_id) - this->start_id + 1;
   block_url_set* ret = new block_url_set( this->file_url, this->file_version, block_id+1, this->end_id, this->block_versions + off );
   return ret;
}


// populate a protobuf representation of ourself
void block_url_set::as_protobuf( struct fs_core* core, Serialization::BlockURLSetMsg* busmsg ) {
   busmsg->set_start_id( this->start_id );
   busmsg->set_end_id( this->end_id );

   // if the URL is local, convert it to public
   if( URL_LOCAL( this->file_url ) ) {
      char* public_url = fs_entry_local_to_public( core, this->file_url );
      busmsg->set_file_url( public_url );
      free( public_url );
   }
   else {
      busmsg->set_file_url( this->file_url );
   }

   for( uint64_t id = this->start_id; id < this->end_id; id++ ) {
      busmsg->add_block_versions( this->block_versions[id - this->start_id] );
   }
}

// default constructor
file_manifest::file_manifest( struct fs_core* core ) {
   this->lastmod.tv_sec = 1;
   this->lastmod.tv_nsec = 1;
   this->file_version = -1;
   this->stale = true;
   pthread_rwlock_init( &this->manifest_lock, NULL );
}

// default constructor
file_manifest::file_manifest( struct fs_core* core, int64_t version ) {
   this->file_version = version;
   this->stale = true;
   this->lastmod.tv_sec = 1;
   this->lastmod.tv_nsec = 1;
   pthread_rwlock_init( &this->manifest_lock, NULL );
}

// destructor
file_manifest::~file_manifest() {
   pthread_rwlock_wrlock( &this->manifest_lock );
   for( block_map::iterator itr = this->block_urls.begin(); itr != this->block_urls.end(); itr++ ) {
      delete itr->second;
   }
   this->block_urls.clear();
   pthread_rwlock_unlock( &this->manifest_lock );
   pthread_rwlock_destroy( &this->manifest_lock );
}

// copy-construct a file manifest
file_manifest::file_manifest( file_manifest& fm ) {
   for( block_map::iterator itr = fm.block_urls.begin(); itr != block_urls.end(); itr++ ) {
      this->block_urls[ itr->first ] = itr->second;
   }
   
   this->lastmod = fm.lastmod;
   this->file_version = fm.file_version;
   this->stale = fm.stale;
   pthread_rwlock_init( &this->manifest_lock, NULL );
}

file_manifest::file_manifest( file_manifest* fm ) {
   for( block_map::iterator itr = fm->block_urls.begin(); itr != fm->block_urls.end(); itr++ ) {
      this->block_urls[ itr->first ] = itr->second;
   }
   
   this->lastmod = fm->lastmod;
   this->file_version = fm->file_version;
   this->stale = fm->stale;
   pthread_rwlock_init( &this->manifest_lock, NULL );
}

file_manifest::file_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg& mmsg ) {
   pthread_rwlock_init( &this->manifest_lock, NULL );
   this->file_version = fent->version;
   this->stale = true;
   file_manifest::parse_protobuf( core, fent, this, mmsg );
}


// set the version
void file_manifest::set_file_version(struct fs_core* core, int64_t version) {
   pthread_rwlock_wrlock( &this->manifest_lock );

   for( block_map::iterator itr = this->block_urls.begin(); itr != this->block_urls.end(); itr++ ) {
      // if local and not staging...
      if( URL_LOCAL( itr->second->file_url ) && strstr( itr->second->file_url, core->conf->staging_root ) == NULL ) {
         itr->second->file_version = version;
      }
   }

   this->file_version = version;
   
   pthread_rwlock_unlock( &this->manifest_lock );
   return;
}

// look up a block URL, given a block number
char* file_manifest::get_block_url( int64_t file_version, uint64_t block_id ) {
   pthread_rwlock_rdlock( &this->manifest_lock );
   block_map::iterator itr = this->find_block_set( block_id );
   if( itr == this->block_urls.end() ) {
      // not found
      pthread_rwlock_unlock( &this->manifest_lock );
      return NULL;
   }
   else {
      pthread_rwlock_unlock( &this->manifest_lock );
      return itr->second->make_url( file_version, block_id );
   }
}


// directly put a url set
void file_manifest::put_url_set( block_url_set* bus ) {
   pthread_rwlock_wrlock( &this->manifest_lock );
   this->block_urls[ bus->start_id ] = bus;
   pthread_rwlock_unlock( &this->manifest_lock );
}


// look up a block version, given a block ID
int64_t file_manifest::get_block_version( uint64_t block ) {
   pthread_rwlock_rdlock( &this->manifest_lock );
   block_map::iterator urlset = this->find_block_set( block );
   if( urlset == this->block_urls.end() ) {
      pthread_rwlock_unlock( &this->manifest_lock );
      return 0;     // not found
   }
   else {
      int64_t ret = urlset->second->lookup_version( block );
      pthread_rwlock_unlock( &this->manifest_lock );
      return ret;
   }
}

// get the block versions (a copy)
int64_t* file_manifest::get_block_versions( uint64_t start_id, uint64_t end_id ) {
   if( end_id <= start_id )
      return NULL;

   pthread_rwlock_rdlock( &this->manifest_lock );

   int64_t* ret = CALLOC_LIST( int64_t, end_id - start_id );

   int i = 0;
   uint64_t curr_id = start_id;

   while( curr_id < end_id ) {
      block_map::iterator itr = this->find_block_set( curr_id );
      if( itr == this->block_urls.end() ) {
         free( ret );
         return NULL;
      }

      for( uint64_t j = 0; j < itr->second->end_id - itr->second->start_id && curr_id < end_id; j++ ) {
         ret[i] = itr->second->block_versions[j];
         i++;
         curr_id++;
      }
   }

   pthread_rwlock_unlock( &this->manifest_lock );
   return ret;
}


// is a block local?
bool file_manifest::is_block_local( uint64_t block_id ) {
   bool ret = false;
   
   pthread_rwlock_rdlock( &this->manifest_lock );

   block_map::iterator itr = this->find_block_set( block_id );
   if( itr != this->block_urls.end() ) {
      ret = URL_LOCAL( itr->second->file_url );
   }
   
   pthread_rwlock_unlock( &this->manifest_lock );
   return ret;
}

// find a block set for a block URL
// NEED TO LOCK FIRST!
block_map::iterator file_manifest::find_block_set( uint64_t block ) {

   block_map::iterator itr = this->block_urls.begin();

   while( itr != this->block_urls.end() ) {
      // does this block fall into the range?
      if( itr->second->in_range( block ) ) {
         // found it!
         break;
      }
      itr++;
   }

   return itr;
}


// attempt to merge two adjacent block url sets, given the block_id that identiifes the block URL set (i.e. can be found)
// return true on success
bool file_manifest::merge_adjacent( uint64_t block_id ) {
   block_map::iterator itr = this->find_block_set( block_id );
   if( itr == this->block_urls.end() )
      // nothing to do
      return false;

   block_map::iterator itr2 = itr;
   itr2++;
   if( itr2 == this->block_urls.end() )
      // nothing to do
      return false;

   block_url_set* left = itr->second;
   block_url_set* right = itr2->second;

   //printf("// merge %s.%lld to %s.%lld?\n", left->file_url, left->file_version, right->file_url, right->file_version );
   if( left->url_equals( right->file_url ) && left->file_version == right->file_version ) {
      // these block URL sets refer to blocks on the same host.  merge them
      int64_t *bvec = CALLOC_LIST( int64_t, right->end_id - left->start_id );

      uint64_t i = 0;
      for( uint64_t j = 0; j < left->end_id - left->start_id; j++ ) {
         bvec[i] = left->block_versions[j];
         
         i++;
      }
      for( uint64_t j = 0; j < right->end_id - right->start_id; j++ ) {
         bvec[i] = right->block_versions[j];

         i++;
      }
      
      block_url_set* merged = new block_url_set( left->file_url, left->file_version, left->start_id, right->end_id, bvec );

      free( bvec );

      this->block_urls.erase( itr );
      this->block_urls.erase( itr2 );

      delete left;
      delete right;

      this->block_urls[ merged->start_id ] = merged;

      return true;
   }
   else {
      return false;
   }
}

// get the range information from a block ID
int file_manifest::get_range( uint64_t block_id, uint64_t* start_id, uint64_t* end_id, char** url ) {
   block_map::iterator itr = this->find_block_set( block_id );
   if( itr == this->block_urls.end() ) {
      return -ENOENT;
   }

   if( start_id )
      *start_id = itr->second->start_id;

   if( end_id )
      *end_id = itr->second->end_id;

   if( url )
      *url = strdup( itr->second->file_url );

   return 0;
}


// insert a URL into a file manifest
bool file_manifest::put_block_url( char const* file_url, int64_t fv, uint64_t block_id, int64_t block_version ) {
   pthread_rwlock_wrlock( &this->manifest_lock );

   if( fv != this->file_version ) {
      // version mismatch
      pthread_rwlock_unlock( &this->manifest_lock );
      return false;
   }

   //dbprintf( "put %s.%lld/%llu.%lld\n", file_url, fv, block_id, block_version);

   block_map::iterator itr = this->find_block_set( block_id );
   int64_t bvec[] = { block_version };

   if( itr == this->block_urls.end() ) {

      //printf("// no block set contains this block\n");

      if( this->block_urls.size() > 0 ) {
         //printf("// block occurs after the end of the file.  Can we append it?\n");
         block_map::reverse_iterator last_range_itr = this->block_urls.rbegin();
         block_url_set* last_range = last_range_itr->second;

         bool rc = last_range->append( file_url, block_id, block_version );
         if( !rc ) {
            //printf("// could not append to the last block range, so we'll need to make a new one\n");
            this->block_urls[ block_id ] = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
         }
         else {
            //printf("// successfully appended this block to the last range!\n");
         }
      }
      else {
         //printf("// we don't have any blocks yet.  put the first one\n");
         this->block_urls[ block_id ] = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
      }
   }

   else {
      
      block_url_set* existing = itr->second;
      block_map::iterator itr_existing = itr;
      //uint64_t existing_block_id = itr->first;

      //printf("// some block set (start_id = %lld) contains this block.\n", existing_block_id);

      if( existing->url_equals( file_url ) ) {
         //printf("// this block URL belongs to this url set.\n");
         //printf("// insert the version\n");
         existing->block_versions[ block_id - existing->start_id ] = block_version;
      }
      else {
         //printf("// this block URL does not belong to this block URL set.\n");
         //printf("// It is possible that it belongs in the previous or next URL sets, if the block ID is on the edge\n");

         if( existing->start_id == block_id ) {
            // need to clear the existing block url set? (i.e. will we need to modify it and re-insert it?)
            bool need_clear = true;

            if( itr != this->block_urls.begin() ) {

               //printf("// see if we can insert this block URL into the previous set.\n");
               //printf("// if not, then shift this set to the right and make a new set\n");
               itr--;
               block_url_set* prev_existing = itr->second;

               bool rc = prev_existing->append( file_url, block_id, block_version );
               if( !rc ) {
                  //printf("// could not append to the previous block URL set.\n");
                  //printf("// Make a new block URL set and insert it (replacing existing)\n");
                  
                  block_url_set* bus = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
                  this->block_urls[ block_id ] = bus;
               }
            }
            else {
               //printf("// need to insert this block URL set at the beginning (replacing existing)\n");
               
               block_url_set* bus = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
               this->block_urls[ block_id ] = bus;
               
               need_clear = false;
            }

            //printf("// will need to shift the existing block URL set down a slot\n");
            bool rc = existing->shrink_left();
            if( !rc ) {
               delete existing;
            }
            else {
               this->block_urls[ existing->start_id ] = existing;
            }

            if( need_clear )
               this->block_urls.erase( itr_existing );

            // attempt to merge
            this->merge_adjacent( block_id );
         }

         else if( existing->end_id - 1 == block_id ) {
            if( itr != this->block_urls.end() ) {

               //printf("// see if we can insert this block URL into the next set\n");
               //printf("// If not, then shift this set to the left and make a new set\n");
               itr++;

               bool rc = false;
               block_url_set* next_existing = itr->second;

               if( itr != this->block_urls.end() ) {
                  rc = next_existing->prepend( file_url, block_id, block_version );
               }

               if( !rc ) {
                  //printf("// could not prepend to the next block URL set.\n");
                  //printf("// Make a new block URL set and insert it.\n");
                  this->block_urls[ block_id ] = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
               }
               else {
                  //printf("// adjust and shift next_existing into its new place.\n");
                  //printf("// get rid of the old next_existing first.\n");
                  this->block_urls.erase( itr );
                  this->block_urls[ next_existing->start_id ] = next_existing;
               }
            }
            else {
               //printf("// need to insert this block URL at the end\n");
               this->block_urls[ block_id ] = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );
            }

            //printf("// will need to shrink existing\n");
            bool rc = existing->shrink_right();
            if( !rc ) {
               //printf("// NOTE: this shouldn't ever happen, since the only way for existing to hold only one block is for existing->start_id + 1 == existing->end_id\n");
               //printf("// (in which case block_id == existing->start_id, meaning this branch won't execute)\n");
               delete existing;
            }

            // attempt to merge
            this->merge_adjacent( block_id );
         }

         else {
            //printf("// split up this URL set\n");
            block_url_set* left = existing->split_left( block_id );
            block_url_set* right = existing->split_right( block_id );
            block_url_set* given = new block_url_set( file_url, fv, block_id, block_id + 1, bvec );

            delete existing;

            this->block_urls[ left->start_id ] = left;
            this->block_urls[ given->start_id ] = given;
            this->block_urls[ right->start_id ] = right;
         }
      }
   }

   pthread_rwlock_unlock( &this->manifest_lock );

   char* data = this->serialize_str();
   dbprintf( "Manifest is now:\n%s\n", data);
   free( data );

   return true;
}


// truncate a manifest
void file_manifest::truncate( uint64_t new_end_id ) {
   pthread_rwlock_wrlock( &this->manifest_lock );

   block_map::iterator itr = this->find_block_set( new_end_id );

   if( itr != this->block_urls.end() ) {
      // truncate this one
      itr->second->truncate( new_end_id );

      // remove the rest of the block URLs
      if( itr->second->size() > 0 )
         itr++;      // preserve this blocK-url-set

      if( itr != this->block_urls.end() ) {
         this->block_urls.erase( itr, this->block_urls.end() );
      }
   }

   pthread_rwlock_unlock( &this->manifest_lock );

   char* data = this->serialize_str();
   dbprintf( "Manifest is now:\n%s\n", data);
   free( data );
}



// serialize the manifest to a string
char* file_manifest::serialize_str() {
   stringstream sts;

   pthread_rwlock_rdlock( &this->manifest_lock );
   for( block_map::iterator itr = this->block_urls.begin(); itr != this->block_urls.end(); itr++ ) {
      sts << "[" << (long)itr->second->start_id << "-" << (long)itr->second->end_id << "] ";

      for( uint64_t i = 0; i < itr->second->end_id - itr->second->start_id; i++ ) {
         sts << itr->second->block_versions[i] << " ";
      }

      sts << string(itr->second->file_url) << "." << itr->second->file_version << "\n";
   }
   pthread_rwlock_unlock( &this->manifest_lock );
   return strdup( sts.str().c_str() );
}


// serialize the manifest to a protobuf
void file_manifest::as_protobuf( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg ) {
   pthread_rwlock_rdlock( &this->manifest_lock );

   for( block_map::iterator itr = this->block_urls.begin(); itr != this->block_urls.end(); itr++ ) {
      Serialization::BlockURLSetMsg* busmsg = mmsg->add_block_url_set();
      itr->second->as_protobuf( core, busmsg );
   }

   mmsg->set_file_version( fent->version );
   mmsg->set_size( fent->size );
   mmsg->set_mtime_sec( fent->mtime_sec );
   mmsg->set_mtime_nsec( fent->mtime_nsec );

   struct timespec ts;
   fent->manifest->get_lastmod( &ts );

   mmsg->set_manifest_mtime_sec( ts.tv_sec );
   mmsg->set_manifest_mtime_nsec( ts.tv_nsec );

   pthread_rwlock_unlock( &this->manifest_lock );
}

// reload a manifest
void file_manifest::reload( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg& mmsg ) {
   pthread_rwlock_wrlock( &this->manifest_lock );

   for( block_map::iterator itr = this->block_urls.begin(); itr != this->block_urls.end(); itr++ ) {
      delete itr->second;
   }
   this->block_urls.clear();

   file_manifest::parse_protobuf( core, fent, this, mmsg );

   this->stale = false;

   pthread_rwlock_unlock( &this->manifest_lock );
}


// populate a manifest from a protobuf
// must lock the manifest first,
// and must lock fent first
int file_manifest::parse_protobuf( struct fs_core* core, struct fs_entry* fent, file_manifest* m, Serialization::ManifestMsg& mmsg ) {

   for( int i = 0; i < mmsg.block_url_set_size(); i++ ) {
      Serialization::BlockURLSetMsg busmsg = mmsg.block_url_set( i );

      int64_t* block_versions = CALLOC_LIST( int64_t, busmsg.end_id() - busmsg.start_id() );

      for( int j = 0; j < busmsg.block_versions_size(); j++ ) {
         block_versions[j] = busmsg.block_versions(j);
      }

      char* file_url = (char*)busmsg.file_url().c_str();
      bool fr = false;

      // are these blocks hosted here?
      if( md_is_locally_hosted( core->conf, busmsg.file_url().c_str() ) ) {
         if( URL_LOCAL( fent->url ) ) {
            // this file is locally hosted
            file_url = fs_entry_local_file_url( core, GET_FS_PATH( core->conf->data_root, fent->url ) );
         }
         else {
            // this file is remotely-hosted, but there are blocks that are local.  They must be staging.
            char* fs_path = md_fs_path_from_url( busmsg.file_url().c_str() );
            file_url = fs_entry_local_staging_file_url( core, fs_path );
            free( fs_path );
         }

         fr = true;
      }

      m->block_urls[ busmsg.start_id() ] = new block_url_set( file_url, mmsg.file_version(), busmsg.start_id(), busmsg.end_id(), block_versions );
      free( block_versions );

      if( fr )
         free( file_url );
   }

   //m->lastmod_realtime = currentTimeSeconds();

   m->file_version = mmsg.file_version();

   return 0;
}

// put a single block URL into a manifest
int fs_entry_put_block_url( struct fs_entry* fent, char const* file_url, int64_t file_version, uint64_t block_id, int64_t block_version ) {
   fent->manifest->put_block_url( file_url, file_version, block_id, block_version );
   // update mtime and size
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );

   fent->mtime_sec = ts.tv_sec;
   fent->mtime_nsec = ts.tv_nsec;
   fent->manifest->set_lastmod( &ts );
   return 0;
}
