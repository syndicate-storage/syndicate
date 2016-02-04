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

#include "libsyndicate/manifest.h"
#include "libsyndicate/gateway.h"

// read-lock a manifest 
static int SG_manifest_rlock( struct SG_manifest* manifest ) {
   return pthread_rwlock_rdlock( &manifest->lock );
}

// write-lock a manifest 
static int SG_manifest_wlock( struct SG_manifest* manifest ) {
   return pthread_rwlock_wrlock( &manifest->lock );
}

// unlock a manifest 
static int SG_manifest_unlock( struct SG_manifest* manifest ) {
   return pthread_rwlock_unlock( &manifest->lock );
}

// allocate manifest blocks 
struct SG_manifest_block* SG_manifest_block_alloc( size_t num_blocks ) {
   return SG_CALLOC( struct SG_manifest_block, num_blocks );
}

// initialize a manifest block (for a block of data, instead of a serialized manifest)
// duplicate all information 
// return 0 on success
// return -ENOMEM on OOM 
// hash can be NULL
int SG_manifest_block_init( struct SG_manifest_block* dest, uint64_t block_id, int64_t block_version, unsigned char const* hash, size_t hash_len ) {
   
   memset( dest, 0, sizeof(struct SG_manifest_block) );
   
   if( hash_len > 0 ) {
      
      dest->hash = SG_CALLOC( unsigned char, hash_len );
      if( dest->hash == NULL ) {
         return -ENOMEM;
      }
      
      memcpy( dest->hash, hash, hash_len * sizeof(unsigned char) );
   }
  
   dest->type = SG_MANIFEST_BLOCK_TYPE_BLOCK;   // default type 
   dest->block_id = block_id;
   dest->block_version = block_version;
   dest->hash_len = hash_len;
   
   return 0;
}

// duplicate a manifest block 
// return 0 on success
// return -ENOMEM on OOM 
int SG_manifest_block_dup( struct SG_manifest_block* dest, struct SG_manifest_block* src ) {
   
   int rc = SG_manifest_block_init( dest, src->block_id, src->block_version, src->hash, src->hash_len );
   if( rc == 0 ) {
      
      // preserve dirty status and type
      dest->dirty = src->dirty;
      dest->type = src->type;
   }
   
   return rc;
}


// load a manifest block from a block protobuf 
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL on missing/invalid fields (i.e. the hash wasn't the right size)
int SG_manifest_block_load_from_protobuf( struct SG_manifest_block* dest, const SG_messages::ManifestBlock* mblock ) {
   
   unsigned char const* hash = NULL;
   size_t hash_len = 0;
   
   if( mblock->has_hash() ) {
      
      // expect SG_BLOCK_HASH_LEN 
      if( mblock->hash().size() != SG_BLOCK_HASH_LEN ) {
         
         return -EINVAL;
      }
      
      hash = (unsigned char*)mblock->hash().data();
      hash_len = mblock->hash().size();
   }
   
   
   int rc = SG_manifest_block_init( dest, mblock->block_id(), mblock->block_version(), hash, hash_len );
   if( rc != 0 ) {
      return rc;
   }
   
   if( mblock->has_chunk_type() ) {
      dest->type = mblock->chunk_type();
   }

   return rc;
}

// set the dirty status for a block 
int SG_manifest_block_set_dirty( struct SG_manifest_block* dest, bool dirty ) {
   
   dest->dirty = dirty;
   return 0;
}


// set the type of block
int SG_manifest_block_set_type( struct SG_manifest_block* dest, int type ) {
   dest->type = type;
   return 0;
}


// construct a manifest block from a chunk of data and versioning info 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_manifest_block_init_from_chunk( struct SG_manifest_block* dest, uint64_t block_id, int64_t block_version, struct SG_chunk* chunk ) {
   
   int rc = 0;
   unsigned char* hash = NULL;
   size_t hash_len = 0;
   
   hash = sha256_hash_data( chunk->data, chunk->len );
   if( hash == NULL ) {
      
      return -ENOMEM;
   }
   
   hash_len = SG_BLOCK_HASH_LEN;
   
   rc = SG_manifest_block_init( dest, block_id, block_version, hash, hash_len );
   
   SG_safe_free( hash );
   
   return rc;
}


// allocate a manifest 
struct SG_manifest* SG_manifest_new() {
   return SG_CALLOC( struct SG_manifest, 1 );
}

// initialize a fresh, empty manifest.
// it's modification time will be 0.
// return 0 on success
// return -ENOMEM on OOM 
int SG_manifest_init( struct SG_manifest* manifest, uint64_t volume_id, uint64_t coordinator_id, uint64_t file_id, int64_t file_version ) {
   
   memset( manifest, 0, sizeof(struct SG_manifest) );
   
   manifest->blocks = SG_safe_new( SG_manifest_block_map_t() );
   if( manifest->blocks == NULL ) {
      
      return -ENOMEM;
   }
   
   int rc = pthread_rwlock_init( &manifest->lock, NULL );
   if( rc != 0 ) {
      
      SG_safe_free( manifest->blocks );
      return rc;
   }
   
   manifest->volume_id = volume_id;
   manifest->coordinator_id = coordinator_id;
   manifest->file_id = file_id;
   manifest->file_version = file_version;
   
   manifest->mtime_sec = 0;
   manifest->mtime_nsec = 0;
   manifest->stale = false;
   
   return 0;
}

// duplicate a manifest, including its freshness status and modtime 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if src is malformed
// NOTE: src must be unlocked or readlocked
int SG_manifest_dup( struct SG_manifest* dest, struct SG_manifest* src ) {
   
   int rc = 0;
   rc = SG_manifest_init( dest, src->volume_id, src->coordinator_id, src->file_id, src->file_version );
   if( rc != 0 ) {
      return rc;
   }
   
   SG_manifest_rlock( src );
   
   for( SG_manifest_block_map_t::iterator itr = src->blocks->begin(); itr != src->blocks->end(); itr++ ) {
      
      struct SG_manifest_block new_block;
      
      rc = SG_manifest_block_dup( &new_block, &itr->second );
      if( rc != 0 ) {
         
         SG_manifest_unlock( src );
         
         // invalid or OOM 
         SG_manifest_free( dest );
         
         return rc;
      }
      
      try {
         (*dest->blocks)[ new_block.block_id ] = new_block;
      }
      catch( bad_alloc& ba ) {
         
         SG_manifest_unlock( src );
         SG_manifest_free( dest );
         
         return -ENOMEM;
      }
   }
   
   SG_manifest_unlock( src );
   
   // duplicate the remaining fields
   dest->mtime_sec = src->mtime_sec;
   dest->mtime_nsec = src->mtime_nsec;
   dest->stale = src->stale;
   
   return 0;
}


// clear a manifest's blocks 
int SG_manifest_clear( struct SG_manifest* manifest ) {
   
   for( SG_manifest_block_map_t::iterator itr = manifest->blocks->begin(); itr != manifest->blocks->end(); itr++ ) {
    
      SG_manifest_block_free( &itr->second );
   }
   
   manifest->blocks->clear();
   return 0;
}

// clear a manifest's blocks, but don't free them
int SG_manifest_clear_nofree( struct SG_manifest* manifest ) {
   
   manifest->blocks->clear();
   return 0;
}


// load a manifest from a protocol buffer 
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL if an invalid block is encountered
int SG_manifest_load_from_protobuf( struct SG_manifest* dest, const SG_messages::Manifest* mmsg ) {
   
   int rc = 0;
   
   pthread_rwlock_t lock;
   
   rc = pthread_rwlock_init( &lock, NULL );
   if( rc != 0 ) {
      return rc;
   }
   
   // load each block 
   SG_manifest_block_map_t* blocks = SG_safe_new( SG_manifest_block_map_t() );
   if( blocks == NULL ) {
      
      pthread_rwlock_destroy( &lock );
      return -ENOMEM;
   }
   
   for( int i = 0; i < mmsg->blocks_size(); i++ ) {
      
      const SG_messages::ManifestBlock& mblock = mmsg->blocks(i);
      struct SG_manifest_block block;
      
      rc = SG_manifest_block_load_from_protobuf( &block, &mblock );
      if( rc != 0 ) {
         
         // abort 
         SG_manifest_block_map_free( blocks );
         SG_safe_delete( blocks );
         
         pthread_rwlock_destroy( &lock );
         return rc;
      }
      
      try {
         (*blocks)[ block.block_id ] = block;
      }
      catch( bad_alloc& ba ) {
         
         SG_manifest_block_map_free( blocks );
         SG_safe_delete( blocks );
         pthread_rwlock_destroy( &lock );
         
         return -ENOMEM;
      }
   }
   
   // got all blocks; load the rest of the structure
   dest->volume_id = mmsg->volume_id();
   dest->coordinator_id = mmsg->coordinator_id();
   dest->file_id = mmsg->file_id();
   dest->file_version = mmsg->file_version();
   
   dest->size = mmsg->size();
   dest->owner_id = mmsg->owner_id();
   
   dest->mtime_sec = mmsg->mtime_sec();
   dest->mtime_nsec = mmsg->mtime_nsec();
   dest->stale = false;
   
   dest->blocks = blocks;
   dest->lock = lock;
   
   return 0;
}


// load a manifest from a serialized bytestring that encodes a protobuf
// return 0 on success, and populate *manifest
// return -EINVAL if it's not a valid protobuf
// return -ENOMEM on OOM
int SG_manifest_load_from_chunk( struct SG_manifest* manifest, struct SG_chunk* chunk ) {

   int rc = 0;
   SG_messages::Manifest proto_manifest;

   try {
      rc = md_parse< SG_messages::Manifest >( &proto_manifest, chunk->data, chunk->len );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   if( rc != 0 ) {
      SG_error("md_parse rc = %d\n", rc );
      return -EINVAL;
   }

   rc = SG_manifest_load_from_protobuf( manifest, &proto_manifest );
   if( rc != 0 ) {
      SG_error("SG_manifest_load_from_protobuf rc = %d\n", rc );
      return rc;
   }

   return rc;
}


// free a manifest block 
// always succeeds
int SG_manifest_block_free( struct SG_manifest_block* block ) {
   
   SG_safe_free( block->hash );
   
   memset( block, 0, sizeof(struct SG_manifest_block) );
   return 0;
}


// free a manifest 
// always succeeds
int SG_manifest_free( struct SG_manifest* manifest ) {
   
   if( manifest->blocks != NULL ) {
      
      SG_manifest_block_map_free( manifest->blocks );
      SG_safe_delete( manifest->blocks );
   }
   
   pthread_rwlock_destroy( &manifest->lock );
   memset( manifest, 0, sizeof(struct SG_manifest) );
   return 0;
}


// free a block map 
// always succeeds
int SG_manifest_block_map_free( SG_manifest_block_map_t* blocks ) {
   
   for( SG_manifest_block_map_t::iterator itr = blocks->begin(); itr != blocks->end(); itr++ ) {
      
      SG_manifest_block_free( &itr->second );
   }
   
   blocks->clear();
   return 0;
}


// set the manifest file version 
// always succeeds
// NOTE: manifest cannot be locked
int SG_manifest_set_file_version( struct SG_manifest* manifest, int64_t version ) {
   
   SG_manifest_wlock( manifest );
   
   manifest->file_version = version;
   
   SG_manifest_unlock( manifest );
   return 0;
}

// add a block to the manifest.
// duplicate the block if dup_block is true; otherwise the manifest takes ownership of all data within it (shallow-copied)
// if replace is true, then this block will be allowed to overwrite an existing block (which will then be freed)
// otherwise, this method will return with -EEXIST if the given block is already present.
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block is malformed
// return -EEXIST if replace is false, but a block with the given ID is already present in the manifest
// NOTE: manifest cannot be locked
// NOTE: this is a zero-alloc operation if replace is true, dup_block is false, and the block already exists in the manifest (i.e. the data is just copied over, and the old data is freed)
static int SG_manifest_put_block_ex( struct SG_manifest* manifest, struct SG_manifest_block* block, bool replace, bool dup_block ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   // does this block exist, and if so, can we bail?
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block->block_id );
   if( itr != manifest->blocks->end() ) {
      
      if( !replace ) {
         // can't replace 
         SG_manifest_unlock( manifest );
         return -EEXIST;
      }
      
      struct SG_manifest_block oldblock = itr->second;
      SG_manifest_block_free( &itr->second );
      
      // replace
      if( dup_block ) {
         
         rc = SG_manifest_block_dup( &itr->second, block );
         
         if( rc != 0 ) {
            
            // OOM 
            itr->second = oldblock;
            SG_manifest_unlock( manifest );
            return rc;
         }
      }
      else {
         
         // replace
         struct SG_manifest_block old_block = itr->second;
         
         itr->second = *block;
         
         SG_manifest_block_free( &old_block );
      }
   }
   else {
      
      // no such block.  make one 
      struct SG_manifest_block *to_put = NULL;
      struct SG_manifest_block block_dup;
      
      if( dup_block ) {
         
         // duplicate
         memset( &block_dup, 0, sizeof(struct SG_manifest_block) );
         
         rc = SG_manifest_block_dup( &block_dup, block );
         if( rc != 0 ) {
            
            // OOM 
            SG_manifest_unlock( manifest );
            return rc;
         }
         
         to_put = &block_dup;
      }
      else {
         
         // put directly
         to_put = block;
      }
      
      try {
         
         // put in place (shallow-copy) 
         (*manifest->blocks)[ to_put->block_id ] = *to_put;
      }
      catch( bad_alloc& ba ) {
         
         // OOM 
         if( dup_block ) {
            SG_manifest_block_free( &block_dup );
         }
         
         SG_manifest_unlock( manifest );
         return -ENOMEM;
      }
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// add a block to the manifest, duplicating it in the process.
// if replace is true, then this block will be allowed to overwrite an existing block (which will then be freed)
// otherwise, this method will return with -EEXIST if the given block is already present.
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block is malformed
// NOTE: manifest cannot be locked
int SG_manifest_put_block( struct SG_manifest* manifest, struct SG_manifest_block* block, bool replace ) {
   
   return SG_manifest_put_block_ex( manifest, block, replace, true );
}


// put a block into the manifest directly
// if replace is true, then this block will be allowed to overwrite an existing block (which will then be freed)
// otherwise, this method will return with -EEXIST if the given block is already present.
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block is malformed
// NOTE: manifest cannot be locked
int SG_manifest_put_block_nocopy( struct SG_manifest* manifest, struct SG_manifest_block* block, bool replace ) {
   
   return SG_manifest_put_block_ex( manifest, block, replace, false );
}


// delete a block from the manifest 
// return 0 on success
// return -ENOENT if not found.
int SG_manifest_delete_block( struct SG_manifest* manifest, uint64_t block_id ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr == manifest->blocks->end() ) {
      
      rc = -ENOENT;
   }
   else {
      
      SG_manifest_block_free( &itr->second ); 
      manifest->blocks->erase( itr );
   }
   
   SG_manifest_unlock( manifest );
   
   return rc;
}


// patch a manifest 
// go through the blocks of src, and put them into dest.
// if replace is true, then the blocks of dest will overwrite existing blocks in src (which will then be freed)
// if dup_block is true, then blocks of src will be duplicated and put into dest.  otherwise, they'll be placed in directly.
// otherwise, this method fails with -EEXIST 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block was malformed 
// NOTE: manifest cannot be locked 
static int SG_manifest_patch_ex( struct SG_manifest* dest, struct SG_manifest* src, bool replace, bool dup_block ) {

   int rc = 0;
   
   if( !replace ) {
      
      // verify that no blocks will be replaced 
      for( SG_manifest_block_map_t::iterator itr = src->blocks->begin(); itr != src->blocks->end(); itr++ ) {
         
         if( dest->blocks->find( itr->first ) != dest->blocks->end() ) {
            
            // will collide
            return -EEXIST;
         }
      }
   }
   
   for( SG_manifest_block_map_t::iterator itr = src->blocks->begin(); itr != src->blocks->end(); itr++ ) {
      
      rc = SG_manifest_put_block_ex( dest, &itr->second, replace, dup_block );
      if( rc != 0 ) {
         
         return rc;
      }
   }
   
   return rc;
}


// patch a manifest 
// go through the blocks of src, duplicate them, and put the duplicates into dest.
// if replace is true, then the blocks of dest will overwrite existing blocks in src (which will then be freed)
// otherwise, this method fails with -EEXIST 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block was malformed 
// NOTE: manifest cannot be locked 
int SG_manifest_patch( struct SG_manifest* dest, struct SG_manifest* src, bool replace ) {
   
   return SG_manifest_patch_ex( dest, src, replace, true );
}


// patch a manifest 
// go through the blocks of src, and put them directly into dest.  dest takes ownership of src's blocks.
// if replace is true, then the blocks of dest will overwrite existing blocks in src (which will then be freed)
// otherwise, this method fails with -EEXIST 
// return 0 on success
// return -ENOMEM on OOM 
// return -EINVAL if the block was malformed 
// NOTE: manifest cannot be locked 
int SG_manifest_patch_nocopy( struct SG_manifest* dest, struct SG_manifest* src, bool replace ) {
   
   return SG_manifest_patch_ex( dest, src, replace, false );
}


// truncate a manifest
// if there are any blocks with a block ID larger than max_block_id, then remove them 
// always succeeds; returns 0
int SG_manifest_truncate( struct SG_manifest* manifest, uint64_t max_block_id ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   // find all blocks with IDs greater than max_block_id 
   SG_manifest_block_map_t::iterator base = manifest->blocks->upper_bound( max_block_id );
   SG_manifest_block_map_t::iterator itr = base;
   
   if( itr != manifest->blocks->end() ) {
      
      // remove them all 
      while( itr != manifest->blocks->end() ) {
         
         SG_manifest_block_free( &itr->second );
         itr++;
      }
      
      manifest->blocks->erase( base, manifest->blocks->end() );
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// set the dirty bit for a block 
// return 0 on success
// return -ENOENT if there is no such block 
int SG_manifest_set_block_dirty( struct SG_manifest* manifest, uint64_t block_id, bool dirty ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
      
      struct SG_manifest_block* block_info = &itr->second;
      
      block_info->dirty = dirty;
   }
   else {
      
      rc = -ENOENT;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// set the dirty bit for all blocks in a manifest 
// return 0 on success
int SG_manifest_set_blocks_dirty( struct SG_manifest* manifest, bool dirty ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   for( SG_manifest_block_map_t::iterator itr = manifest->blocks->begin(); itr != manifest->blocks->end(); itr++ ) {
      
      struct SG_manifest_block* block_info = &itr->second;
      
      block_info->dirty = dirty;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// set the modification time for the manifest
// always succeeds
int SG_manifest_set_modtime( struct SG_manifest* manifest, int64_t mtime_sec, int32_t mtime_nsec ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   manifest->mtime_sec = mtime_sec;
   manifest->mtime_nsec = mtime_nsec;
   
   SG_manifest_unlock( manifest );
   return rc;
}

// set the owner ID of the manifest 
// always succeeds
int SG_manifest_set_owner_id( struct SG_manifest* manifest, uint64_t owner_id ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   manifest->owner_id = owner_id;
   
   SG_manifest_unlock( manifest );
   
   return rc;
}

// set the coordinator ID of the manifest 
// always succeeds
int SG_manifest_set_coordinator_id( struct SG_manifest* manifest, uint64_t coordinator_id ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   manifest->coordinator_id = coordinator_id;
   
   SG_manifest_unlock( manifest );
   
   return rc;
}

// set the size of the assocaited file 
// always succeds
int SG_manifest_set_size( struct SG_manifest* manifest, uint64_t size ) {
   
   int rc = 0;
   
   SG_manifest_wlock( manifest );
   
   manifest->size = size;
   
   SG_manifest_unlock( manifest );
   
   return rc;
}

// mark the manifest as stale 
// always succeeds 
int SG_manifest_set_stale( struct SG_manifest* manifest, bool stale ) {
   
   SG_manifest_wlock( manifest );
   
   manifest->stale = stale;
   
   SG_manifest_unlock( manifest );

   if( stale ) {
      SG_debug("%p: set stale\n", manifest);
   }

   return 0;
}

// get a manifest block's ID
uint64_t SG_manifest_block_id( struct SG_manifest_block* block ) {
   return block->block_id;
}

// get a manifest block's version 
int64_t SG_manifest_block_version( struct SG_manifest_block* block ) {
   return block->block_version;
}

// get manifest block's type 
int SG_manifest_block_type( struct SG_manifest_block* block ) {
   return block->type;
}

// get a manifest block's dirty status 
bool SG_manifest_block_is_dirty( struct SG_manifest_block* block ) {
   return block->dirty;
}

// get a manifest block's hash 
unsigned char* SG_manifest_block_hash( struct SG_manifest_block* block ) {
   return block->hash;
}

// set the block version 
int SG_manifest_block_set_version( struct SG_manifest_block* block, int64_t version ) {
   block->block_version = version;
   return 0;
}

// set a manifest's block hash (freeing the previous one, if present)
// the block takes ownership of the hash
int SG_manifest_block_set_hash( struct SG_manifest_block* block, unsigned char* hash ) {
   if( block->hash != NULL ) {
      SG_safe_free( block->hash );
   }
   block->hash = hash;
   return 0;
}

// get the manifest volume ID 
uint64_t SG_manifest_get_volume_id( struct SG_manifest* manifest ) {
   
   uint64_t volume_id = 0;
   
   SG_manifest_rlock( manifest );
   
   volume_id = manifest->volume_id;
   
   SG_manifest_unlock( manifest );
   
   return volume_id;
}

// get the manifest file ID 
uint64_t SG_manifest_get_file_id( struct SG_manifest* manifest ) {
   
   uint64_t file_id = 0;
   
   SG_manifest_rlock( manifest );
   
   file_id = manifest->file_id;
   
   SG_manifest_unlock( manifest );
   
   return file_id;
}


// get the manifest file version 
int64_t SG_manifest_get_file_version( struct SG_manifest* manifest ) {
   
   int64_t version = 0;
   
   SG_manifest_rlock( manifest );
   
   version = manifest->file_version;
   
   SG_manifest_unlock( manifest );
   
   return version;
}


// get the number of blocks *represented* by the manifest
// return the *maximum* block ID + 1
uint64_t SG_manifest_get_block_range( struct SG_manifest* manifest ) {
   
   uint64_t rc = 0;
   
   SG_manifest_rlock( manifest );
   
   if( manifest->blocks->size() > 0 ) {
      
      SG_manifest_block_map_t::reverse_iterator ritr = manifest->blocks->rbegin();
      rc = ritr->first + 1;
   }
   
   SG_manifest_unlock( manifest );
   
   return rc;
}

// get the actual number of blocks in the manifest 
uint64_t SG_manifest_get_block_count( struct SG_manifest* manifest ) {
   
   uint64_t ret = 0;
   
   SG_manifest_rlock( manifest );
   
   ret = manifest->blocks->size();
   
   SG_manifest_unlock( manifest );
   
   return ret;
}
   
// get the size of the file 
uint64_t SG_manifest_get_file_size( struct SG_manifest* manifest ) {
   
   uint64_t ret = 0;
   
   SG_manifest_rlock( manifest );
   
   ret = manifest->size;
   
   SG_manifest_unlock( manifest );
   
   return ret;
}


// get a malloc'ed copy of a block's hash 
// if block_hash is NULL, it will be alloced.  Otherwise, it will be used.
// return 0 on success, and set *block_hash and *hash_len
// return -ENOMEM on OOM 
// return -ENOENT if not found
// return -ERANGE if *block_hash is not NULL, but is not big enough to hold the block's hash (*hash_len will be set to the required length)
// return -ENODATA if there is no hash for this (existant) block 
int SG_manifest_get_block_hash( struct SG_manifest* manifest, uint64_t block_id, unsigned char** block_hash, size_t* hash_len ) {
   
   unsigned char* ret = NULL;
   int rc = 0;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
     
      if( itr->second.hash_len == 0 || itr->second.hash == NULL ) {
         // no hash 
         rc = -ENODATA;
      }
      else {
         if( *block_hash != NULL && itr->second.hash_len >= *hash_len * sizeof(unsigned char) ) {
            memcpy( *block_hash, itr->second.hash, itr->second.hash_len * sizeof(unsigned char) );
         }
         else if( *block_hash != NULL ) {

            rc = -ERANGE;
            *hash_len = itr->second.hash_len;
         }
         else {
             ret = SG_CALLOC( unsigned char, itr->second.hash_len );
         
             if( ret != NULL ) {
            
                memcpy( ret, itr->second.hash, sizeof(unsigned char) * itr->second.hash_len );
            
                *block_hash = ret;
                *hash_len = itr->second.hash_len;
             }
             else {
                rc = -ENOMEM;
             }
         }
      }
   }
   else {
      rc = -ENOENT;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// does a block have a hash?
// return true if so
// return false if not (including if it doesn't exist)
bool SG_manifest_has_block_hash( struct SG_manifest* manifest, uint64_t block_id ) {

   bool rc = true;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
     
      if( itr->second.hash_len == 0 || itr->second.hash == NULL ) {
         // no hash 
         rc = -false;
      }
   }
   else {
      rc = false;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// get a block's version
// return 0 on success 
// return -ENOENT if not found
int SG_manifest_get_block_version( struct SG_manifest* manifest, uint64_t block_id, int64_t* block_version ) {
   
   int rc = 0;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
      
      *block_version = itr->second.block_version;
   }
   else { 
      
      rc = -ENOENT;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// get the coordinator for this manifest 
// always succeeds
uint64_t SG_manifest_get_coordinator( struct SG_manifest* manifest ) {
   
   uint64_t ret = 0;
   
   SG_manifest_rlock( manifest );
   
   ret = manifest->coordinator_id;
   
   SG_manifest_unlock( manifest );
   return ret;
}

// determine if a block is represented in the manifest.
// if it is not, it's a "block hole"
// return true if it's present 
// return false if it's a hole
bool SG_manifest_is_block_present( struct SG_manifest* manifest, uint64_t block_id ) {
   
   bool ret = false;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   
   ret = (itr != manifest->blocks->end());
   
   SG_manifest_unlock( manifest );
   
   return ret;
}

// get a manifest's modtime, putting it into *mtime_sec and *mtime_nsec 
// always succeeds
int SG_manifest_get_modtime( struct SG_manifest* manifest, int64_t* mtime_sec, int32_t* mtime_nsec ) {
   
   SG_manifest_rlock( manifest );
   
   *mtime_sec = manifest->mtime_sec;
   *mtime_nsec = manifest->mtime_nsec;
   
   SG_manifest_unlock( manifest );
   return 0;
}


// get the manifest's modtime, second half
// always succeeds
int64_t SG_manifest_get_modtime_sec( struct SG_manifest* manifest ) {
   
   SG_manifest_rlock( manifest );
   
   int64_t mtime_sec = manifest->mtime_sec;
   
   SG_manifest_unlock( manifest );
   return mtime_sec;
}


// get the manifest's modtime, nanosecond half
// always succeeds
int32_t SG_manifest_get_modtime_nsec( struct SG_manifest* manifest ) {

   SG_manifest_rlock( manifest );
   
   int32_t mtime_nsec = manifest->mtime_nsec;
   
   SG_manifest_unlock( manifest );
   return mtime_nsec;
}

// is a manifest stale?
bool SG_manifest_is_stale( struct SG_manifest* manifest ) {
   
   bool ret = false;
   
   SG_manifest_rlock( manifest );
   
   ret = manifest->stale;
   
   SG_manifest_unlock( manifest );
   
   return ret;
}

// look up a block and return a pointer to it 
// return NULL if the block is not known.
// NOTE: this pointer is only good for as long as no blocks are added or removed from the manifest!
struct SG_manifest_block* SG_manifest_block_lookup( struct SG_manifest* manifest, uint64_t block_id ) {
   
   struct SG_manifest_block* ret = NULL;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
      
      ret = &itr->second;
   }
   
   SG_manifest_unlock( manifest );
   
   return ret;
}


// look up and compare a block's hash against a test hash 
// return 1 if they are equal 
// return 0 if they are not equal 
// return -ENOENT if there is no block in this manifest 
// return -ENODATA if the block in the manifest does not have a hash 
// return -EINVAL if the hash length does not match the block's hash length 
int SG_manifest_block_hash_eq( struct SG_manifest* manifest, uint64_t block_id, unsigned char* test_hash, size_t test_hash_len ) {
   
   int rc = 0;
   
   SG_manifest_rlock( manifest );
   
   SG_manifest_block_map_t::iterator itr = manifest->blocks->find( block_id );
   if( itr != manifest->blocks->end() ) {
      
      struct SG_manifest_block* block = &itr->second;
      
      if( block->hash == NULL ) {
         
         // no hash 
         rc = -ENODATA;
      }
      else if( block->hash_len != test_hash_len ) {
         
         // differring lengths 
         rc = -EINVAL;
      }
      else {
         
         // compare!
         rc = memcmp( block->hash, test_hash, test_hash_len );
         if( rc == 0 ) {
            
            // equal 
            rc = 1;
         }
         else {
            
            // not equal 
            rc = 0;
         }
      }
   }
   else {
      
      // no block 
      rc = -ENOENT;
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}

// put a manifest's data into its protobuf representation 
// the manifest will NOT be signed
// return 0 on success
// return -ENOMEM on OOM 
// the caller should free mmsg regardless of the return code
int SG_manifest_serialize_to_protobuf( struct SG_manifest* manifest, SG_messages::Manifest* mmsg ) {
   
   int rc = 0;
   
   SG_manifest_rlock( manifest );

   // serialize all blocks 
   for( SG_manifest_block_map_t::iterator itr = manifest->blocks->begin(); itr != manifest->blocks->end(); itr++ ) {
      
      SG_messages::ManifestBlock* next_block = NULL;
      
      try {
         
         next_block = mmsg->add_blocks();
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         break;
      }
      
      rc = SG_manifest_block_serialize_to_protobuf( &itr->second, next_block );
      if( rc != 0 ) {
         break;
      }
   }
   
   if( rc == 0 ) {
      
      // serialize the rest of the manifest 
      mmsg->set_volume_id( manifest->volume_id );
      mmsg->set_coordinator_id( manifest->coordinator_id );
      mmsg->set_file_id( manifest->file_id );
      mmsg->set_file_version( manifest->file_version );
      
      mmsg->set_mtime_sec( manifest->mtime_sec );
      mmsg->set_mtime_nsec( manifest->mtime_nsec );
      
      mmsg->set_size( manifest->size );
      mmsg->set_owner_id( manifest->owner_id );

      mmsg->set_signature( string("") );
   }
      
   SG_manifest_unlock( manifest );
   return rc;
}


// put a manifest's block data into a request 
// return 0 on success 
// return -ENOMEM on OOM 
int SG_manifest_serialize_blocks_to_request_protobuf( struct SG_manifest* manifest, SG_messages::Request* request ) {
   
   int rc = 0;
   
   SG_manifest_rlock( manifest );
   
   for( SG_manifest_block_map_t::iterator itr = manifest->blocks->begin(); itr != manifest->blocks->end(); itr++ ) {
      
      SG_messages::ManifestBlock* next_block = NULL;
      
      try {
         
         next_block = request->add_blocks();
      }
      catch( bad_alloc& ba ) {
         
         rc = -ENOMEM;
         break;
      }
      
      rc = SG_manifest_block_serialize_to_protobuf( &itr->second, next_block );
      if( rc != 0 ) {
         break;
      }
   }
   
   SG_manifest_unlock( manifest );
   return rc;
}


// serialize a block to a protobuf 
// return 0 on success
// return -ENOMEM on OOM 
int SG_manifest_block_serialize_to_protobuf( struct SG_manifest_block* block, SG_messages::ManifestBlock* mblock ) {
  
   // sanity check...
   if( block->hash == NULL && block->hash_len != 0 ) {
     SG_error("BUG: block [%" PRIu64 ".%" PRId64 "] hash is NULL\n", block->block_id, block->block_version);
     exit(1);
   }
   try {
     
      if( block->hash != NULL ) { 
          mblock->set_hash( string((char*)block->hash, block->hash_len) );
      }
       
      mblock->set_block_id( block->block_id );
      mblock->set_block_version( block->block_version );

      if( block->type != 0 ) {
         mblock->set_chunk_type( block->type );
      }
   }
   catch( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
   
   return 0;
}


// print out a manifest to stdout (i.e. for debugging)
// return -ENOMEM on OOM
int SG_manifest_print( struct SG_manifest* manifest ) {
   
   SG_manifest_rlock( manifest );
   
   printf("Manifest: /%" PRIu64 "/%" PRIX64 ".%" PRId64 ".%" PRId64 ".%d, coordinator=%" PRIu64 ", owner=%" PRIu64 ", size=%" PRIu64 "\n",
           manifest->volume_id, manifest->file_id, manifest->file_version, manifest->mtime_sec, manifest->mtime_nsec, manifest->coordinator_id, manifest->owner_id, manifest->size );
   
   for( SG_manifest_block_map_t::iterator itr = manifest->blocks->begin(); itr != manifest->blocks->end(); itr++ ) {
      
      char* hash_printable = NULL;
      char const* type_str = NULL;

      hash_printable = md_data_printable( itr->second.hash, itr->second.hash_len );
      if( hash_printable == NULL ) {
         return -ENOMEM;
      }
      
      if( itr->second.type == SG_MANIFEST_BLOCK_TYPE_MANIFEST ) {
         type_str = "manifest";
      }
      else if( itr->second.type == SG_MANIFEST_BLOCK_TYPE_BLOCK ) {
         type_str = "block";
      }
      else {
         type_str = "UNKNOWN";
      }

      printf("  Block (type=%s) %" PRIu64 ".%" PRId64 " hash=%s\n", type_str, itr->first, itr->second.block_version, hash_printable );
      
      SG_safe_free( hash_printable );
   }
   
   return 0;
}
