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

#include "fs_entry.h"
#include "manifest.h"
#include "url.h"
#include "ms-client.h"
#include "cache.h"
#include "consistency.h"
#include "replication.h"
#include "driver.h"
#include "sync.h"

int _debug_locks = 0;

int fs_entry_set_config( struct md_syndicate_conf* conf ) {
   _debug_locks = conf->debug_lock;
   return 0;
}

// insert a child entry into an fs_entry_set
void fs_entry_set_insert( fs_entry_set* set, char const* name, struct fs_entry* child ) {
   long nh = fs_entry_name_hash( name );
   return fs_entry_set_insert_hash( set, nh, child );
}

// insert a child entry into an fs_entry_set
void fs_entry_set_insert_hash( fs_entry_set* set, long hash, struct fs_entry* child ) {
   for( unsigned int i = 0; i < set->size(); i++ ) {
      if( set->at(i).second == NULL ) {
         set->at(i).second = child;
         set->at(i).first = hash;
         return;
      }
   }

   fs_dirent dent( hash, child );
   set->push_back( dent );
}


// find a child entry in a fs_entry_set
struct fs_entry* fs_entry_set_find_name( fs_entry_set* set, char const* name ) {
   long nh = fs_entry_name_hash( name );
   return fs_entry_set_find_hash( set, nh );
}


// find a child entry in an fs_entry set
struct fs_entry* fs_entry_set_find_hash( fs_entry_set* set, long nh ) {
   for( unsigned int i = 0; i < set->size(); i++ ) {
      if( set->at(i).first == nh )
         return set->at(i).second;
   }
   return NULL;
}


// remove a child entry from an fs_entry_set
bool fs_entry_set_remove( fs_entry_set* set, char const* name ) {
   long nh = fs_entry_name_hash( name );
   return fs_entry_set_remove_hash( set, nh );
}


// remove a child entry from an fs_entry_set
bool fs_entry_set_remove_hash( fs_entry_set* set, long nh ) {
   bool removed = false;
   for( unsigned int i = 0; i < set->size(); i++ ) {
      if( set->at(i).first == nh ) {
         // invalidate this
         set->at(i).second = NULL;
         set->at(i).first = 0;
         removed = true;
         break;

         // TODO: recompress
      }
   }

   return removed;
}



// replace an entry
bool fs_entry_set_replace( fs_entry_set* set, char const* name, struct fs_entry* replacement ) {
   long nh = fs_entry_name_hash( name );
   for( unsigned int i = 0; i < set->size(); i++ ) {
      if( set->at(i).first == nh ) {
         (*set)[i].second = replacement;
         return true;
      }
   }
   return false;
}


// count the number of entries in an fs_entry_set
unsigned int fs_entry_set_count( fs_entry_set* set ) {
   unsigned int ret = 0;
   for( unsigned int i = 0; i < set->size(); i++ ) {
      if( set->at(i).second != NULL )
         ret++;
   }
   return ret;
}

// dereference an iterator to an fs_entry_set member
struct fs_entry* fs_entry_set_get( fs_entry_set::iterator* itr ) {
   return (*itr)->second;
}

// dereference an iterator to an fs_entry_set member
long fs_entry_set_get_name_hash( fs_entry_set::iterator* itr ) {
   return (*itr)->first;
}

// calculate the block ID from an offset
uint64_t fs_entry_block_id( struct fs_core* core, off_t offset ) {
   return ((uint64_t)offset) / core->blocking_factor;
}

uint64_t fs_entry_block_id( size_t blocksize, off_t offset ) {
   return ((uint64_t)offset) / blocksize;
}

// set up the core of the FS
int fs_core_init( struct fs_core* core, struct syndicate_state* state, struct md_syndicate_conf* conf, struct ms_client* client, struct syndicate_cache* cache,
                  uint64_t owner_id, uint64_t gateway_id, uint64_t volume, mode_t mode, uint64_t blocking_factor ) {
   
   if( core == NULL ) {
      return -EINVAL;
   }

   memset( core, 0, sizeof(struct fs_core) );
   core->conf = conf;
   core->ms = client;
   core->state = state;
   core->cache = cache;
   core->volume = volume;
   core->gateway = conf->gateway;
   core->blocking_factor = blocking_factor;
   
   pthread_rwlock_init( &core->lock, NULL );
   pthread_rwlock_init( &core->fs_lock, NULL );

   // initialize the root, but make it searchable and mark it as stale 
   core->root = CALLOC_LIST( struct fs_entry, 1 );

   int rc = fs_entry_init_dir( core, core->root, "/", 1, owner_id, 0, volume, 0755, 0, 0 );
   if( rc != 0 ) {
      errorf("fs_entry_init_dir rc = %d\n", rc );
      
      pthread_rwlock_destroy( &core->lock );
      pthread_rwlock_destroy( &core->fs_lock );
      return rc;
   }

   core->root->link_count = 1;
   fs_entry_set_insert( core->root->children, ".", core->root );
   fs_entry_set_insert( core->root->children, "..", core->root );

   // we're stale; refresh on read
   fs_entry_mark_read_stale( core->root );
   
   // initialize the driver
   core->closure = CALLOC_LIST( struct md_closure, 1 );
   rc = driver_init( core, &core->closure );
   
   if( rc != 0 && rc != -ENOENT ) {
      errorf("driver_init rc = %d\n", rc );
      
      free( core->closure );
      
      fs_entry_destroy( core->root, true );
      free( core->root );
      
      pthread_rwlock_destroy( &core->lock );
      pthread_rwlock_destroy( &core->fs_lock );
      return rc;
   }

   // start watching for reloads 
   struct fs_entry_view_change_cls* cls = CALLOC_LIST( struct fs_entry_view_change_cls, 1 );
   
   cls->core = core;
   cls->cert_version = 0;
   
   ms_client_set_view_change_callback( core->ms, fs_entry_view_change_callback, cls );
   core->viewchange_cls = cls;
   
   return rc;
}

// destroy the core of the FS
int fs_core_destroy( struct fs_core* core ) {

   if( core->closure ) {
      int rc = driver_shutdown( core->closure );
      if( rc != 0 ) {
         errorf("WARN: driver_shutdown rc = %d\n", rc );
      }
      
      free( core->closure );
      core->closure = NULL;
   }
   
   pthread_rwlock_destroy( &core->lock );
   pthread_rwlock_destroy( &core->fs_lock );
   
   ms_client_set_view_change_callback( core->ms, NULL, NULL );
   
   if( core->viewchange_cls != NULL ) {
      free( core->viewchange_cls );
      core->viewchange_cls = NULL;
   }

   return 0;
}

// rlock the fs
int fs_core_fs_rlock( struct fs_core* core ) {
   return pthread_rwlock_rdlock( &core->fs_lock );
}

// wlock the fs
int fs_core_fs_wlock( struct fs_core* core ) {
   return pthread_rwlock_wrlock( &core->fs_lock );
}

// unlock the fs
int fs_core_fs_unlock( struct fs_core* core ) {
   return pthread_rwlock_unlock( &core->fs_lock );
}


// unlink a directory's immediate children and subsequent descendants
int fs_unlink_children( struct fs_core* core, fs_entry_set* dir_children, bool remove_data ) {
   
   queue<struct fs_entry*> destroy_queue;
   for( fs_entry_set::iterator itr = dir_children->begin(); itr != dir_children->end(); ) {
      struct fs_entry* child = fs_entry_set_get( &itr );

      if( child == NULL ) {
         itr = dir_children->erase( itr );
         continue;
      }

      long fent_name_hash = fs_entry_set_get_name_hash( &itr );

      if( fent_name_hash == fs_entry_name_hash( "." ) || fent_name_hash == fs_entry_name_hash( ".." ) ) {
         itr++;
         continue;
      }

      if( fs_entry_wlock( child ) != 0 ) {
         itr = dir_children->erase( itr );
         continue;
      }

      destroy_queue.push( child );
      
      itr = dir_children->erase( itr );
   }
   
   while( destroy_queue.size() > 0 ) {
      struct fs_entry* fent = destroy_queue.front();
      destroy_queue.pop();

      int old_type = fent->ftype;
      
      fent->ftype = FTYPE_DEAD;
      fent->link_count = 0;

      if( old_type == FTYPE_FILE || old_type == FTYPE_FIFO ) {
         if( fent->open_count == 0 ) {
            if( FS_ENTRY_LOCAL( core, fent ) && remove_data ) {
               fs_entry_cache_evict_file( core, core->cache, fent->file_id, fent->version );
            }
            
            fs_entry_destroy( fent, false );
            free( fent );
         }
      }

      else {
         fs_entry_set* children = fent->children;
         fent->children = NULL;

         fent->link_count = 0;

         if( fent->open_count == 0 ) {
            fs_entry_destroy( fent, false );
            free( fent );
         }

         for( fs_entry_set::iterator itr = children->begin(); itr != children->end(); itr++ ) {
            struct fs_entry* child = fs_entry_set_get( &itr );

            if( child == NULL )
               continue;

            long fent_name_hash = fs_entry_set_get_name_hash( &itr );

            if( fent_name_hash == fs_entry_name_hash( "." ) || fent_name_hash == fs_entry_name_hash( ".." ) )
               continue;

            if( fs_entry_wlock( child ) != 0 )
               continue;

            destroy_queue.push( child );
         }

         delete children;
      }
   }

   return 0;
}

// destroy a filesystem.
// this is NOT thread-safe!
int fs_destroy( struct fs_core* core ) {
   if( core == NULL )
      return 0;
   
   fs_entry_wlock( core->root );
   int rc = fs_unlink_children( core, core->root->children, false );
   if( rc != 0 ) {
      errorf("WARN: fs_unlink_children(/) rc = %d\n", rc );
   }

   fs_entry_destroy( core->root, false );

   free( core->root );
   
   return fs_core_destroy( core );
}



static int fs_entry_init_data( struct fs_core* core, struct fs_entry* fent, int type, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec ) {
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   
   if( mtime_sec <= 0 ) {
      mtime_sec = ts.tv_sec;
      mtime_nsec = ts.tv_nsec;
   }

   fent->name = strdup( name );

   fent->file_id = 0;
   fent->version = version;
   fent->owner = owner;
   fent->coordinator = coordinator;
   fent->volume = volume;
   fent->mode = mode;
   fent->size = size;
   fent->ctime_sec = ts.tv_sec;
   fent->ctime_nsec = ts.tv_nsec;
   fent->atime = fent->ctime_sec;
   fent->mtime_sec = mtime_sec;
   fent->mtime_nsec = mtime_nsec;
   fent->link_count = 0;
   fent->manifest = new file_manifest( fent->version );
   fent->max_read_freshness = core->conf->default_read_freshness;
   fent->max_write_freshness = core->conf->default_write_freshness;
   fent->read_stale = false;
   fent->xattr_cache = new xattr_cache_t();
   
   fent->manifest->set_modtime( 0, 0 );
   
   clock_gettime( CLOCK_REALTIME, &fent->refresh_time );
   
   return 0;
}

// common fs_entry initializion code
// a version of <= 0 will cause the FS to look at the underlying data to deduce the correct version
static int fs_entry_init_common( struct fs_core* core, struct fs_entry* fent, int type, char const* name, int64_t version,
                                 uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec) {

   memset( fent, 0, sizeof(struct fs_entry) );
   fs_entry_init_data( core, fent, type, name, version, owner, coordinator, volume, mode, size, mtime_sec, mtime_nsec );
   pthread_rwlock_init( &fent->lock, NULL );
   pthread_rwlock_init( &fent->xattr_lock, NULL );
   
   return 0;
}

// create an FS entry that is a file
int fs_entry_init_file( struct fs_core* core, struct fs_entry* fent, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec ) {
   fs_entry_init_common( core, fent, FTYPE_FILE, name, version, owner, coordinator, volume, mode, size, mtime_sec, mtime_nsec );
   fent->ftype = FTYPE_FILE;
   fent->sync_queue = new sync_context_list_t();
   fent->old_snapshot = CALLOC_LIST( struct replica_snapshot, 1 );
   return 0;
}


// create an FS entry that is a file
int fs_entry_init_fifo( struct fs_core* core, struct fs_entry* fent, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec, bool local ) {
   fs_entry_init_common( core, fent, FTYPE_FILE, name, version, owner, coordinator, volume, mode, size, mtime_sec, mtime_nsec );
   fent->ftype = FTYPE_FIFO;
   return 0;
}

// create an FS entry that is a directory
int fs_entry_init_dir( struct fs_core* core, struct fs_entry* fent, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, int64_t mtime_sec, int32_t mtime_nsec ) {
   fs_entry_init_common( core, fent, FTYPE_DIR, name, version, owner, coordinator, volume, mode, 4096, mtime_sec, mtime_nsec );
   fent->ftype = FTYPE_DIR;
   fent->children = new fs_entry_set();
   return 0;
}

// get the next block version number (unique with high probability)
int64_t fs_entry_next_random_version(void) {
   int64_t upper = CMWC4096() & 0x7fffffff;
   int64_t lower = CMWC4096();

   int64_t ret = (upper << 32) | lower;
   return ret;
}


// get the next file version number, as the milliseconds since the epoch
int64_t fs_entry_next_file_version(void) {
   return fs_entry_next_random_version();
}

int64_t fs_entry_next_block_version(void) {
   return fs_entry_next_random_version();
}

// create an FS entry from an md_entry.
int fs_entry_init_md( struct fs_core* core, struct fs_entry* fent, struct md_entry* ent ) {
   
   if( ent->type == MD_ENTRY_DIR ) {
      // this is a directory
      fs_entry_init_dir( core, fent, ent->name, ent->version, ent->owner, ent->coordinator, ent->volume, ent->mode, ent->mtime_sec, ent->mtime_nsec );
   }
   else if ( ent->type == MD_ENTRY_FILE ){
      // this is a file
      fs_entry_init_file( core, fent, ent->name, ent->version, ent->owner, ent->coordinator, ent->volume, ent->mode, ent->size, ent->mtime_sec, ent->mtime_nsec );
      fent->manifest->set_modtime( ent->manifest_mtime_sec, ent->manifest_mtime_nsec );
   }
   else if (S_ISFIFO(ent->mode)){
      // this is a FIFO 
      fs_entry_init_fifo( core, fent, ent->name, ent->version, ent->owner, ent->coordinator, ent->volume, ent->mode, ent->size, ent->mtime_sec, ent->mtime_nsec, fent->coordinator == core->gateway );
   }

   fent->file_id = ent->file_id;
   fent->write_nonce = ent->write_nonce;
   fent->xattr_nonce = ent->xattr_nonce;
   
   return 0;
}


// destroy an FS entry
int fs_entry_destroy( struct fs_entry* fent, bool needlock ) {

   dbprintf("destroy %" PRIX64 " (%s) (%p)\n", fent->file_id, fent->name, fent );

   // free common fields
   if( needlock )
      fs_entry_wlock( fent );

   if( fent->name ) {
      free( fent->name );
      fent->name = NULL;
   }

   if( fent->manifest ) {
      delete fent->manifest;
      fent->manifest = NULL;
   }

   if( fent->children ) {
      delete fent->children;
      fent->children = NULL;
   }
   
   if( fent->xattr_cache ) {
      delete fent->xattr_cache;
      fent->xattr_cache = NULL;
   }
   
   if( fent->sync_queue ) {
      delete fent->sync_queue;
      fent->sync_queue = NULL;
   }
   
   if( fent->old_snapshot ) {
      free( fent->old_snapshot );
      fent->old_snapshot = NULL;
   }
   
   fs_entry_free_working_data( fent );
   
   fent->ftype = FTYPE_DEAD;      // next thread to hold this lock knows this is a dead entry
   
   fs_entry_unlock( fent );
   pthread_rwlock_destroy( &fent->lock );
   pthread_rwlock_destroy( &fent->xattr_lock );
   return 0;
}

// destroy an fs_entry if it is no longer referenced.
// fent must be write-locked
// return 0 on success
// return 1 if the fent was completely unreferenced (and destroyed)
// return negative on error
// this method does NOT free fent if it is destroyed; the caller must check the return code to know when to do this
int fs_entry_try_destroy( struct fs_core* core, struct fs_entry* fent ) {
   // decrement reference count on the fs_entry itself
   int rc = 0;
   
   if( fent->link_count <= 0 && fent->open_count <= 0 ) {
      
      if( fent->ftype == FTYPE_FILE ) {
         // file is unlinked and no one is manipulating it--safe to destroy
         rc = fs_entry_cache_evict_file( core, core->cache, fent->file_id, fent->version );
         if( rc == -ENOENT ) {
            // not a problem
            rc = 0;
         }
         else {
            errorf("WARN: fs_entry_cache_evict_file(%" PRIX64 " (%s)) rc = %d\n", fent->file_id, fent->name, rc );
            rc = 0;
         }
      }
      
      fs_entry_destroy( fent, false );
      
      rc = 1;
   }
   
   return rc;
}

// free an fs_dir_entry
int fs_dir_entry_destroy( struct fs_dir_entry* dent ) {
   md_entry_free( &dent->data );
   return 0;
}


// free a list of fs_dir_entrys
int fs_dir_entry_destroy_all( struct fs_dir_entry** dents ) {
   for( unsigned int i = 0; dents[i] != NULL; i++ ) {
      fs_dir_entry_destroy( dents[i] );
      free( dents[i] );
   }
   return 0;
}

// calculate the hash of a name
long fs_entry_name_hash( char const* name ) {
   return md_hash( name );
}


// cache an xattr, but only if the caller knows the current xattr_nonce
// fent must be read-locked at least
int fs_entry_put_cached_xattr( struct fs_entry* fent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, int64_t last_known_xattr_nonce ) {
   string xattr_name_s( xattr_name );
   
   pthread_rwlock_wrlock( &fent->xattr_lock );
   
   if( fent->xattr_nonce != last_known_xattr_nonce ) {
      // stale 
      pthread_rwlock_unlock( &fent->xattr_lock );
      
      dbprintf("xattr_nonce = %" PRId64 ", but caller thought it was %" PRId64 "\n", fent->xattr_nonce, last_known_xattr_nonce );
      return -ESTALE;
   }
   
   xattr_cache_t::iterator itr = fent->xattr_cache->find( xattr_name_s );
   if( itr != fent->xattr_cache->end() ) {
      // erase this 
      fent->xattr_cache->erase( itr );
   }
   
   (*fent->xattr_cache)[ xattr_name_s ] = string( xattr_value, xattr_value_len );
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   
   return 0;
}

// get a cached xattr
// fent must be read-locked
int fs_entry_get_cached_xattr( struct fs_entry* fent, char const* xattr_name, char** xattr_value, size_t* xattr_value_len ) {
   string xattr_name_s( xattr_name );
   
   pthread_rwlock_rdlock( &fent->xattr_lock );
   
   xattr_cache_t::iterator itr = fent->xattr_cache->find( xattr_name_s );
   if( itr != fent->xattr_cache->end() ) {
      
      size_t retlen = itr->second.size();
      if( retlen > 0 ) {
         char* ret = CALLOC_LIST( char, retlen );
      
         memcpy( ret, itr->second.data(), retlen );
      
         *xattr_value = ret;
         *xattr_value_len = retlen;
         
         pthread_rwlock_unlock( &fent->xattr_lock );
      
         return 0;
      }
      else {
         pthread_rwlock_unlock( &fent->xattr_lock );
         return -ENODATA;
      }
   }
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   
   // not found 
   return -ENODATA;
}

// evict a cached xattr
// fent must be read-locked
int fs_entry_evict_cached_xattr( struct fs_entry* fent, char const* xattr_name ) {
   string xattr_name_s( xattr_name );
   
   pthread_rwlock_wrlock( &fent->xattr_lock );
   
   xattr_cache_t::iterator itr = fent->xattr_cache->find( xattr_name_s );
   if( itr != fent->xattr_cache->end() ) {
      // erase this 
      fent->xattr_cache->erase( itr );
      
      pthread_rwlock_unlock( &fent->xattr_lock );
      
      return 0;
   }
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   
   return -ENODATA;
}

// clear all cached xattrs, and update the xattr_nonce
// fent must be write-locked 
int fs_entry_clear_cached_xattrs( struct fs_entry* fent, int64_t new_xattr_nonce ) {
   
   int rc = 0;
   
   pthread_rwlock_wrlock( &fent->xattr_lock );

   fent->xattr_cache->clear();

   fent->xattr_nonce = new_xattr_nonce;
   
   dbprintf("xattr nonce for %" PRIX64 " (%s) is now %" PRId64 "\n", fent->file_id, fent->name, new_xattr_nonce );
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   
   return rc;
}

// get a list of cached xattrs, but only if the caller knows the current xattr_nonce for fent.
// fent must be read-locked 
int fs_entry_list_cached_xattrs( struct fs_entry* fent, char** xattr_list, size_t* xattr_list_len, int64_t last_known_xattr_nonce ) {
   
   pthread_rwlock_rdlock( &fent->xattr_lock );
   
   if( fent->xattr_nonce != last_known_xattr_nonce ) {
      // stale
      pthread_rwlock_unlock( &fent->xattr_lock );
      
      dbprintf("xattr_nonce = %" PRId64 ", but caller thought it was %" PRId64 "\n", fent->xattr_nonce, last_known_xattr_nonce );
      return -ESTALE;
   }
      
   size_t total_len = 0;
   
   for( xattr_cache_t::iterator itr = fent->xattr_cache->begin(); itr != fent->xattr_cache->end(); itr++ ) {
      total_len += itr->first.size() + 1;
   }
   
   char* ret = CALLOC_LIST( char, total_len );
   size_t offset = 0;
   
   for( xattr_cache_t::iterator itr = fent->xattr_cache->begin(); itr != fent->xattr_cache->end(); itr++ ) {
      strcpy( ret + offset, itr->first.c_str() );
      offset += itr->first.size() + 1;
   }
   
   *xattr_list = ret;
   *xattr_list_len = offset;
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   return 0;
}


// cache a list of xattr names, if the caller's last known xattr_nonce is fresh (i.e. CAS semantics)
int fs_entry_cache_xattr_list( struct fs_entry* fent, xattr_cache_t* new_listing, int64_t last_known_xattr_nonce ) {
   
   pthread_rwlock_wrlock( &fent->xattr_lock );
   
   if( fent->xattr_nonce != last_known_xattr_nonce ) {
      // can't swap--caller has stale information 
      pthread_rwlock_unlock( &fent->xattr_lock );
      
      dbprintf("xattr_nonce = %" PRId64 ", but caller thought it was %" PRId64 "\n", fent->xattr_nonce, last_known_xattr_nonce );
      return -ESTALE;
   }
   
   xattr_cache_t* old_listing = fent->xattr_cache;
   fent->xattr_cache = new_listing;
   
   pthread_rwlock_unlock( &fent->xattr_lock );
   
   delete old_listing;
   
   return 0;
}


// lock a file for reading
int fs_entry_rlock2( struct fs_entry* fent, char const* from_str, int line_no ) {
   int rc = pthread_rwlock_rdlock( &fent->lock );
   if( rc == 0 ) {
      if( _debug_locks ) {
         dbprintf( "%p: %s, from %s:%d\n", fent, fent->name, from_str, line_no );
      }
   }
   else {
      errorf("pthread_rwlock_rdlock(%p) rc = %d (from %s:%d)\n", fent, rc, from_str, line_no );
   }

   return rc;
}

// lock a file for writing
int fs_entry_wlock2( struct fs_entry* fent, char const* from_str, int line_no ) {
   int rc = pthread_rwlock_wrlock( &fent->lock );
   if( fent->ftype == FTYPE_DEAD )
      return -ENOENT;
   
   if( rc == 0 ) {
      if( _debug_locks ) {
         dbprintf( "%p: %s, from %s:%d\n", fent, fent->name, from_str, line_no );
      }
   }
   else {
      errorf("pthread_rwlock_wrlock(%p) rc = %d (from %s:%d)\n", fent, rc, from_str, line_no );
   }

   return rc;
}

// unlock a file
int fs_entry_unlock2( struct fs_entry* fent, char const* from_str, int line_no ) {
   int rc = pthread_rwlock_unlock( &fent->lock );
   if( rc == 0 ) {
      if( _debug_locks ) {
         dbprintf( "%p: %s, from %s:%d\n", fent, fent->name, from_str, line_no );
      }
   }
   else {
      errorf("pthread_rwlock_unlock(%p) rc = %d (from %s:%d)\n", fent, rc, from_str, line_no );
   }

   return rc;
}

// lock a file handle for reading
int fs_file_handle_rlock( struct fs_file_handle* fh ) {
   return pthread_rwlock_rdlock( &fh->lock );
}

// lock a file handle for writing
int fs_file_handle_wlock( struct fs_file_handle* fh ) {
   return pthread_rwlock_wrlock( &fh->lock );
}

// unlock a file handle
int fs_file_handle_unlock( struct fs_file_handle* fh ) {
   return pthread_rwlock_unlock( &fh->lock );
}

// lock a directory handle for reading
int fs_dir_handle_rlock( struct fs_dir_handle* dh ) {
   return pthread_rwlock_rdlock( &dh->lock );
}

// lock a directory handle for writing
int fs_dir_handle_wlock( struct fs_dir_handle* dh ) {
   return pthread_rwlock_wrlock( &dh->lock );
}

// unlock a directory handle
int fs_dir_handle_unlock( struct fs_dir_handle* dh ) {
   return pthread_rwlock_unlock( &dh->lock );
}

// read-lock a filesystem core
int fs_core_rlock( struct fs_core* core ) {
   return pthread_rwlock_rdlock( &core->lock );
}

// write-lock a filesystem core
int fs_core_wlock( struct fs_core* core ) {
   return pthread_rwlock_wrlock( &core->lock );
}

// unlock a filesystem core
int fs_core_unlock( struct fs_core* core ) {
   return pthread_rwlock_unlock( &core->lock );
}

// run the eval function on cur_ent.
// prev_ent must be write-locked, in case cur_ent gets deleted.
// return the eval function's return code.
// if th eval function fails, both cur_ent and prev_ent will be unlocked
static int fs_entry_ent_eval( struct fs_entry* prev_ent, struct fs_entry* cur_ent, int (*ent_eval)( struct fs_entry*, void* ), void* cls ) {
   long name_hash = md_hash( cur_ent->name );
   char* name_dup = strdup( cur_ent->name );
   
   int eval_rc = (*ent_eval)( cur_ent, cls );
   if( eval_rc != 0 ) {
      
      dbprintf("ent_eval(%" PRIX64 " (%s)) rc = %d\n", cur_ent->file_id, name_dup, eval_rc );
      
      // cur_ent might not even exist anymore....
      if( cur_ent->ftype != FTYPE_DEAD ) {
         fs_entry_unlock( cur_ent );
         if( prev_ent != cur_ent && prev_ent != NULL ) {
            fs_entry_unlock( prev_ent );
         }
      }
      else {
         free( cur_ent );
         
         if( prev_ent ) {
            dbprintf("Remove %s from %s\n", name_dup, prev_ent->name );
            fs_entry_set_remove_hash( prev_ent->children, name_hash );
            fs_entry_unlock( prev_ent );
         }
      }
   }
   
   free( name_dup );
   
   return eval_rc;
}

// resolve an absolute path, running a given function on each entry as the path is walked
// returns the locked fs_entry at the end of the path on success
struct fs_entry* fs_entry_resolve_path_cls( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err, int (*ent_eval)( struct fs_entry*, void* ), void* cls ) {

   if( vol != core->volume && user != SYS_USER ) {
      // wrong volume
      *err = -EXDEV;
      return NULL;
   }
   
   // if this path ends in '/', then append a '.'
   char* fpath = NULL;
   if( strlen(path) == 0 ) {
      *err = -EINVAL;
      return NULL;
   }

   if( path[strlen(path)-1] == '/' ) {
      fpath = md_fullpath( path, ".", NULL );
   }
   else {
      fpath = strdup( path );
   }

   char* tmp = NULL;

   char* name = strtok_r( fpath, "/", &tmp );
   while( name != NULL && strcmp(name, ".") == 0 ) {
      name = strtok_r( NULL, "/", &tmp );
   }

   if( name == NULL && writelock )
      fs_entry_wlock( core->root );
   else
      fs_entry_rlock( core->root );

   if( core->root->link_count == 0 ) {
      // filesystem was nuked
      free( fpath );
      fs_entry_unlock( core->root );
      *err = -ENOENT;
      return NULL;
   }

   struct fs_entry* cur_ent = core->root;
   struct fs_entry* prev_ent = NULL;

   // run our evaluator on the root entry (which is already locked)
   if( ent_eval ) {
      int eval_rc = fs_entry_ent_eval( prev_ent, cur_ent, ent_eval, cls );
      if( eval_rc != 0 ) {
         *err = eval_rc;
         free( fpath );
         return NULL;
      }
   }
   
   do {
       
       // if this isn't a directory, then invalid path
       if( name != NULL && cur_ent->ftype != FTYPE_DIR ) {
         if( cur_ent->ftype == FTYPE_FILE )
            *err = -ENOTDIR;
         else
            *err = -ENOENT;

         free( fpath );
         fs_entry_unlock( cur_ent );

         return NULL;
      }

      // do we have permission to search this directory?
      if( cur_ent->ftype == FTYPE_DIR && !IS_DIR_READABLE( cur_ent->mode, cur_ent->owner, cur_ent->volume, user, vol ) ) {

         // the appropriate read flag is not set
         *err = -EACCES;
         free( fpath );
         fs_entry_unlock( cur_ent );

         return NULL;
      }
      
      if( name == NULL )
         break;

      // resolve next name
      prev_ent = cur_ent;
      if( name != NULL ) {
         cur_ent = fs_entry_set_find_name( prev_ent->children, name );
      }
      else {
         // out of path
         break;
      }
      
      if( cur_ent == NULL ) {
         // not found
         *err = -ENOENT;
         free( fpath );
         fs_entry_unlock( prev_ent );

         return NULL;
      }
      else {
         // next path name
         name = strtok_r( NULL, "/", &tmp );
         while( name != NULL && strcmp(name, ".") == 0 ) {
            name = strtok_r( NULL, "/", &tmp );
         }
         
         fs_entry_wlock( cur_ent );
         
         // before unlocking the previous ent, run our evaluator (if we have one)
         if( ent_eval ) {
            int eval_rc = fs_entry_ent_eval( prev_ent, cur_ent, ent_eval, cls );
            if( eval_rc != 0 ) {
               *err = eval_rc;
               free( fpath );
               return NULL;
            }
         }
         
         
         // If this is the last step of the path,
         // downgrade to a read lock if requested
         if( name == NULL && !writelock ) {
            fs_entry_unlock( cur_ent );
            fs_entry_rlock( cur_ent );
         }
         
         fs_entry_unlock( prev_ent );

         if( cur_ent->link_count == 0 || cur_ent->ftype == FTYPE_DEAD ) {
           // just got removed
           *err = -ENOENT;
           free( fpath );
           fs_entry_unlock( cur_ent );

           return NULL;
         }
      }
   } while( true );
   
   free( fpath );
   if( name == NULL ) {
      // ran out of path
      *err = 0;
      
      // check readability
      if( !IS_READABLE( cur_ent->mode, cur_ent->owner, cur_ent->volume, user, vol ) ) {
         *err = -EACCES;
         fs_entry_unlock( cur_ent );
         return NULL;
      }
      
      return cur_ent;
   }
   else {
      // not a directory
      *err = -ENOTDIR;
      fs_entry_unlock( cur_ent );
      return NULL;
   }
}



// resolve an absolute path.
// returns the locked fs_entry at the end of the path on success
struct fs_entry* fs_entry_resolve_path( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err ) {
   return fs_entry_resolve_path_cls( core, path, user, vol, writelock, err, NULL, NULL );
}


struct fs_entry_resolve_parent_cls {
   uint64_t parent_id;
   char* parent_name;
   uint64_t file_id;
   char* file_name;
};

static int fs_entry_resolve_parent( struct fs_entry* fent, void* cls ) {
   struct fs_entry_resolve_parent_cls* parent_cls = (struct fs_entry_resolve_parent_cls*)cls;

   parent_cls->parent_id = parent_cls->file_id;
   parent_cls->file_id = fent->file_id;

   if( parent_cls->parent_name ) {
      free( parent_cls->parent_name );
   }
   parent_cls->parent_name = parent_cls->file_name;
   parent_cls->file_name = strdup( fent->name );

   return 0;
}

// resolve an absolute path, AND get the parent's file_id
// returns the locked fs_entry at the end of the path on success, and the file_id of the parent
struct fs_entry* fs_entry_resolve_path_and_parent_info( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, bool writelock, int* err, uint64_t* parent_id, char** parent_name ) {
   struct fs_entry_resolve_parent_cls parent_cls;

   memset( &parent_cls, 0, sizeof(parent_cls) );
   
   struct fs_entry* fent = fs_entry_resolve_path_cls( core, path, user, vol, writelock, err, fs_entry_resolve_parent, (void*)&parent_cls );

   if( fent != NULL ) {
      // did we resolve a top-level file/directory?
      if( parent_cls.file_name != NULL && parent_cls.parent_name == NULL ) {
         parent_cls.parent_name = parent_cls.file_name;
         parent_cls.file_name = NULL;
      }

      if( parent_cls.parent_name == NULL ) {
         // got root
         parent_cls.parent_name = strdup("/");
      }

      if( parent_id != NULL ) {
         *parent_id = parent_cls.parent_id;
      }
      if( parent_name != NULL ) {
         *parent_name = parent_cls.parent_name;
      }
      else {
         free( parent_cls.parent_name );
      }
   }
   else {
      free( parent_cls.parent_name );
   }
   
   if( parent_cls.file_name )
      free( parent_cls.file_name );

   return fent;
}


// convert an fs_entry to an md_entry.
// the URLs will all be public.
int fs_entry_to_md_entry( struct fs_core* core, struct md_entry* dest, char const* fs_path, uint64_t owner, uint64_t volume ) {
   int err = 0;
   char* parent_name = NULL;
   uint64_t parent_id = 0;
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, fs_path, owner, volume, false, &err, &parent_id, &parent_name );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   err = fs_entry_to_md_entry( core, dest, fent, parent_id, parent_name );
   free( parent_name );

   fs_entry_unlock( fent );
   return err;
}

// convert an fs_entry to an md_entry.
int fs_entry_to_md_entry( struct fs_core* core, struct md_entry* dest, struct fs_entry* fent, uint64_t parent_id, char const* parent_name ) {

   memset( dest, 0, sizeof(struct md_entry) );
   
   struct timespec manifest_ts;
   if( fent->manifest != NULL ) {
      fent->manifest->get_modtime( &manifest_ts );
   }
   else {
      manifest_ts.tv_sec = 0;
      manifest_ts.tv_nsec = 0;
   }
   
   dest->type = fent->ftype == FTYPE_FILE ? MD_ENTRY_FILE : MD_ENTRY_DIR;
   dest->name = strdup( fent->name );
   dest->file_id = fent->file_id;
   dest->ctime_sec = fent->ctime_sec;
   dest->ctime_nsec = fent->ctime_nsec;
   dest->mtime_sec = fent->mtime_sec;
   dest->mtime_nsec = fent->mtime_nsec;
   dest->manifest_mtime_sec = manifest_ts.tv_sec;
   dest->manifest_mtime_nsec = manifest_ts.tv_nsec;
   dest->owner = fent->owner;
   dest->coordinator = fent->coordinator;
   dest->volume = fent->volume;
   dest->mode = fent->mode;
   dest->size = fent->size;
   dest->version = fent->version;
   dest->max_read_freshness = fent->max_read_freshness;
   dest->max_write_freshness = fent->max_write_freshness;
   dest->parent_id = parent_id;
   dest->write_nonce = fent->write_nonce;
   dest->xattr_nonce = fent->xattr_nonce;

   if( parent_name )
      dest->parent_name = strdup( parent_name );
   else
      dest->parent_name = NULL;
   
   return 0;
}



// destroy a directory handle
void fs_dir_handle_destroy( struct fs_dir_handle* dh ) {
   dh->dent = NULL;
   if( dh->path ) {
      free( dh->path );
      dh->path = NULL;
   }
   if( dh->parent_name ) {
      free( dh->parent_name );
      dh->parent_name = NULL;
   }
   
   pthread_rwlock_destroy( &dh->lock );
}



// destroy a file handle
// NOTE: it must be wlocked first
int fs_file_handle_destroy( struct fs_file_handle* fh ) {
   fh->fent = NULL;
   if( fh->path ) {
      free( fh->path );
      fh->path = NULL;
   }
   if( fh->parent_name ) {
      free( fh->parent_name );
      fh->parent_name = NULL;
   }

   pthread_rwlock_unlock( &fh->lock );
   pthread_rwlock_destroy( &fh->lock );

   return 0;
}

// initialize an fs_entry_block_info_structure for replication
void fs_entry_block_info_replicate_init( struct fs_entry_block_info* binfo, int64_t version, unsigned char* hash, size_t hash_len, uint64_t gateway_id, int block_fd ) {
   memset( binfo, 0, sizeof(struct fs_entry_block_info) );
   binfo->version = version;
   binfo->hash = hash;
   binfo->hash_len = hash_len;
   binfo->gateway_id = gateway_id;
   binfo->block_fd = block_fd;
}

// initialize an fs_entry_block_info structure for garbage collection 
void fs_entry_block_info_garbage_init( struct fs_entry_block_info* binfo, int64_t version, unsigned char* hash, size_t hash_len, uint64_t gateway_id ) {
   memset( binfo, 0, sizeof(struct fs_entry_block_info) );
   binfo->version = version;
   binfo->gateway_id = gateway_id;
   binfo->hash = hash;
   binfo->hash_len = hash_len;
   binfo->block_fd = -1;
}

// initialize an fs_entry_block_info structure for bufferring blocks 
void fs_entry_block_info_buffer_init( struct fs_entry_block_info* binfo, size_t block_len ) {
   memset( binfo, 0, sizeof(struct fs_entry_block_info) );
   binfo->block_buf = CALLOC_LIST( char, block_len );
   binfo->block_len = block_len;
   binfo->block_fd = -1;
}


// free an fs_entry_block_info structure's memory.
int fs_entry_block_info_free_ex( struct fs_entry_block_info* binfo, bool close_fd ) {
   if( binfo->hash ) {
      free( binfo->hash );
      binfo->hash = NULL;
   }
   
   if( binfo->block_buf ) {
      free( binfo->block_buf );
      binfo->block_buf = NULL;  
   }
   
   if( close_fd && binfo->block_fd >= 0 ) {
      close( binfo->block_fd );  
   }
   
   memset(binfo, 0, sizeof(struct fs_entry_block_info) );
   return 0;
}

int fs_entry_block_info_free( struct fs_entry_block_info* binfo ) {
   return fs_entry_block_info_free_ex( binfo, false );
}

// free a modification_map.
int fs_entry_free_modification_map_ex( modification_map* m, bool close_fds ) {
   if( m->size() > 0 ) {
      for( modification_map::iterator itr = m->begin(); itr != m->end(); itr++ ) {
         fs_entry_block_info_free_ex( &itr->second, close_fds );
      }
      
      m->clear();
   }
   return 0;
}

int fs_entry_free_modification_map( modification_map* m ) {
   return fs_entry_free_modification_map_ex( m, false );
}


// view change callback: reload the driver
int fs_entry_view_change_callback( struct ms_client* ms, void* cls ) {
   struct fs_entry_view_change_cls* viewchange_cls = (struct fs_entry_view_change_cls*)cls;
   
   struct fs_core* core = viewchange_cls->core;
   uint64_t old_version = viewchange_cls->cert_version;
   
   uint64_t cert_version = ms_client_cert_version( ms );
   
   if( cert_version != old_version ) {
      dbprintf("cert version was %" PRIu64 ", now is %" PRIu64 ".  Reloading closure...\n", old_version, cert_version );
      
      int rc = driver_reload( core, core->closure );
      if( rc == 0 ) {
         viewchange_cls->cert_version = cert_version;
      }
   }
   else {
      dbprintf("%s", "cert version has not changed, so not reloading closure\n" );
   }
   
   return 0;
}

// extract and re-initialize the dirty block set for an fs_entry.
// the caller gains exclusive access to the dirty block set, and must free it.
// fent must be write-locked
int fs_entry_extract_dirty_blocks( struct fs_entry* fent, modification_map** dirty_blocks ) {
   *dirty_blocks = fent->dirty_blocks;
   fent->dirty_blocks = new modification_map();
   return 0;
}

// extract and re-initialize the garbage block set for an fs_entry.
// the caller gains exclusive access to the garbage block set, and must free it.
// fent must be write-locked
int fs_entry_extract_garbage_blocks( struct fs_entry* fent, modification_map** garbage_blocks ) {
   *garbage_blocks = fent->garbage_blocks;
   fent->garbage_blocks = new modification_map();
   return 0;
}


// replace dirty blocks (i.e. on replica failure)
// this overwrites existing dirty blocks 
// fent must be write-locked, and *should* be locked during the same interval as a previous call to extract_dirty_blocks that obtained the dirty_blocks argument.
int fs_entry_replace_dirty_blocks( struct fs_entry* fent, modification_map* dirty_blocks ) {
   if( fent->dirty_blocks ) {
      fs_entry_free_modification_map_ex( fent->dirty_blocks, true );
      delete fent->dirty_blocks;
   }
   
   fent->dirty_blocks = dirty_blocks;
   return 0;
}

// replace garbage blocks (i.e. on replica failure)
// this overwrites existing garbage blocks 
// fent must be write-locked, and *should* be locked during the same interval as a previous call to extract_garbage_blocks that obtained the garbage_blocks argument.
int fs_entry_replace_garbage_blocks( struct fs_entry* fent, modification_map* garbage_blocks ) {
   if( fent->garbage_blocks ) {
      fs_entry_free_modification_map_ex( fent->dirty_blocks, true );
      delete fent->garbage_blocks;
   }
   
   fent->garbage_blocks = garbage_blocks;
   return 0;
   
}
   

// allocate working data for when the file is opened at least once
// fent must be write-locked
int fs_entry_setup_working_data( struct fs_core* core, struct fs_entry* fent ) {
   fent->dirty_blocks = new modification_map();
   fent->garbage_blocks = new modification_map();
   fent->bufferred_blocks = new modification_map();
   
   if( fent->dirty_blocks == NULL || fent->garbage_blocks == NULL || fent->bufferred_blocks == NULL )
      return -ENOMEM;
   
   return 0;
}

// free working data 
// fent must be write-locked
int fs_entry_free_working_data( struct fs_entry* fent ) {
   if( fent->dirty_blocks ) {
      fs_entry_free_modification_map_ex( fent->dirty_blocks, true );
      delete fent->dirty_blocks;
   }

   if( fent->garbage_blocks ) {
      fs_entry_free_modification_map_ex( fent->garbage_blocks, false );
      delete fent->garbage_blocks;
   }
   
   if( fent->bufferred_blocks ) {
      fs_entry_free_modification_map_ex( fent->bufferred_blocks, false );
      delete fent->bufferred_blocks;
   }
   
   fent->dirty_blocks = NULL;
   fent->garbage_blocks = NULL;
   fent->bufferred_blocks = NULL;
   
   return 0;
}

// Merge new dirty blocks into an fs_entry's list of dirty blocks.
// This replaces versions of the same block.  It will close an existing dirty block's file descriptor, if it is open.
// fent must be write-locked
int fs_entry_merge_new_dirty_blocks( struct fs_entry* fent, modification_map* new_dirty_blocks ) {
   for( modification_map::iterator itr = new_dirty_blocks->begin(); itr != new_dirty_blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      
      // find the existing entry...
      modification_map::iterator existing_itr = fent->dirty_blocks->find( block_id );

      if( existing_itr != fent->dirty_blocks->end() ) {
         
         // clean up the existing entry, closing its file descriptor as well (so it can be unlinked and erased) 
         struct fs_entry_block_info* binfo = &itr->second;
         
         fs_entry_block_info_free_ex( binfo, true );
      }

      (*fent->dirty_blocks)[ block_id ] = itr->second;
   }
   
   return 0;
}


// Merge old dirty blocks into an fs_entry's list of dirty blocks, effectively "undo-ing" a block flush.
// However, the file could have been subsequently modified, truncated, or deleted/recreated.
// As such, does NOT replace new versions of the same block, and does NOT put dirty blocks into a new file.
// fent must be write-locked
int fs_entry_merge_old_dirty_blocks( struct fs_core* core, struct fs_entry* fent, uint64_t original_file_id, int64_t original_file_version, modification_map* old_dirty_blocks, modification_map* unmerged ) {
   
   uint64_t max_block = fs_entry_block_id( core, fent->size );
   
   for( modification_map::iterator itr = old_dirty_blocks->begin(); itr != old_dirty_blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* binfo = &itr->second;
      
      // if this is a different file, then these old dirty blocks shall not be merged (i.e. file was deleted and recreated, so they don't belong)
      if( fent->file_id != original_file_id || fent->version != original_file_version ) {
         (*unmerged)[ block_id ] = *binfo;
         continue;
      }
      
      // if the block is off the end of the file, then it won't be merged (a truncate superceded this block)
      if( block_id > max_block ) {
         (*unmerged)[ block_id ] = *binfo;
         continue;
      }
      
      // find the existing entry...
      modification_map::iterator existing_itr = fent->dirty_blocks->find( block_id );

      if( existing_itr == fent->dirty_blocks->end() ) {
         // no entry for this block...check the manifest
         
         if( fent->manifest->get_block_version( block_id ) == binfo->version ) {
            
            // block has not been modified since we extracted it.  Make it dirty again
            (*fent->dirty_blocks)[ block_id ] = *binfo;
         }
         else {
            // a new write superceded this block.  It will not be merged
            (*unmerged)[ block_id ] = *binfo;
         }
      }
      else {
         // a new write superceded this block.  It will not be merged 
         (*unmerged)[ block_id ] = *binfo;
      }
   }
   
   return 0;
}


// Merge garbage blocks into an existing list of garbage blocks.
// This only adds blocks into the garbage block list; if a block has an existing garbage entry,
// then any subsequent block written will only have been cached locally (so no need to garbage-collect it).
// fent must be write-locked 
int fs_entry_merge_garbage_blocks( struct fs_core* core, struct fs_entry* fent, uint64_t original_file_id, int64_t original_file_version, modification_map* new_garbage_blocks, modification_map* unmerged ) {
   
   uint64_t max_block = fs_entry_block_id( core, fent->size );
   
   for( modification_map::iterator itr = new_garbage_blocks->begin(); itr != new_garbage_blocks->end(); itr++ ) {
      
      uint64_t block_id = itr->first;
      struct fs_entry_block_info* binfo = &itr->second;
      
      // if this is a different file, then we shouldn't add this to be garbage-collected.  The caller should garbage-collect it
      if( fent->file_id != original_file_id || fent->version != original_file_version ) {
         (*unmerged)[ block_id ] = *binfo;
         continue;
      }
      
      // if the block is off the end of the file, then it shouldn't be merged (a truncate superceded this block, and we don't want to stop garbage-collection for subsequent writes or truncates that expand the file)
      if( block_id > max_block ) {
         (*unmerged)[ block_id ] = *binfo;
         continue;
      }
      
      // are we already going to garbage-collect this block?
      modification_map::iterator existing_itr = fent->garbage_blocks->find( block_id );

      if( existing_itr == fent->garbage_blocks->end() ) {
         // Nope--add the entry.
         (*fent->garbage_blocks)[ block_id ] = *binfo;
      }
      else {
         // going to garbage-collect a different version of this block, so caller should handle this
         (*unmerged)[ block_id ] = *binfo;
      }
   }
   
   return 0;
}


// is a block bufferred in RAM?
// return 1 if so.
// return -ENOENT if not.
// fent must be at least read-locked
int fs_entry_has_bufferred_block( struct fs_entry* fent, uint64_t block_id ) {
   
   if( fent->bufferred_blocks ) {
      modification_map::iterator itr = fent->bufferred_blocks->find( block_id );
      if( itr != fent->bufferred_blocks->end() ) {
         // have an entry...
         struct fs_entry_block_info* binfo = &itr->second;
         
         // block ID must match, and must have allocated
         if( binfo->block_buf == NULL ) {
            return -ENOENT;
         }
         else {
            // have an in-core block buffer
            return 1;
         }
      }
      else {
         return -ENOENT;
      }
   }
   else {
      return -ENOENT;
   }
}


// read part of a bufferred block 
// return 0 on success
// return -ENOENT if there is no block.
// return negative on error
// fent must be read-locked 
int fs_entry_read_bufferred_block( struct fs_entry* fent, uint64_t block_id, char* buf, off_t block_offset, size_t read_len ) {
   
   if( fent->bufferred_blocks ) {
      
      modification_map::iterator itr = fent->bufferred_blocks->find( block_id );
      if( itr != fent->bufferred_blocks->end() ) {
         // have an entry...
         struct fs_entry_block_info* binfo = &itr->second;
         
         if( binfo->block_buf != NULL ) {
            // range check 
            if( block_offset + read_len < 0 || block_offset + read_len >= binfo->block_len ) {
               return -ERANGE;
            }
            else {
               // have an in-core copy of this block.  Read it 
               memcpy( buf, binfo->block_buf + block_offset, read_len );
               return 0;
            }
         }
         else {
            return -ENOENT;
         }
      }
      else {
         return -ENOENT;
      }
   }
   else {
      // shouldn't get here--it's a bug 
      errorf("BUG: %" PRIX64 "'s bufferred_blocks is not allocated\n", fent->file_id);
      return -EIO;
   }
}


// write to a bufferred block, creating it on-the-fly if one does not exist.
// return 0 on success
// return -EEXIST if there is a buffer for a different block than the one indicated
// return negative on error
// fent must be write-locked 
int fs_entry_write_bufferred_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char const* buf, off_t block_offset, size_t write_len ) {
   
   if( fent->bufferred_blocks ) {
      
      modification_map::iterator itr = fent->bufferred_blocks->find( block_id );
      
      struct fs_entry_block_info* binfo = NULL;
      
      if( itr == fent->bufferred_blocks->end() ) {
         // no block; allocate it 
         struct fs_entry_block_info new_binfo;
         
         fs_entry_block_info_buffer_init( &new_binfo, core->blocking_factor );
         
         binfo = &new_binfo;
      }
      else {
         // have an entry...
         binfo = &itr->second;
      }
      
      if( binfo->block_buf != NULL ) {
         // range check 
         if( block_offset + write_len < 0 || block_offset + write_len >= binfo->block_len ) {
            return -ERANGE;
         }
         else {
            // have an in-core copy of this block.  Write to it 
            memcpy( binfo->block_buf + block_offset, buf, write_len );
            return 0;
         }
      }
      else {
         if( binfo->block_buf == NULL ) {
            // shouldn't get here
            errorf( "BUG: %" PRIX64 " has no buffer for %" PRIu64 "\n", fent->file_id, block_id );
            return -EIO;
         }
         else {
            // wrong block
            return -EEXIST;
         }
      }
   }
   else {
      errorf("BUG: %" PRIX64 "'s bufferred_blocks is not allocated\n", fent->file_id);
      return -ENODATA;
   }
}

// replace a bufferred block's contents
// if there is no block data, then allocate it.
// return 0 and fill in buf, buf_len on success with the block buffer
// return -ENOENT if the block ID doesn't match the bufferred block 
// return negative on error 
// fent must be write-locked 
int fs_entry_replace_bufferred_block( struct fs_entry* fent, uint64_t block_id, char* buf, size_t buf_len ) {
   if( fent->bufferred_blocks ) {
      
      modification_map::iterator itr = fent->bufferred_blocks->find( block_id );
      
      struct fs_entry_block_info* binfo = NULL;
      
      if( itr == fent->bufferred_blocks->end() ) {
         // no block; add it
         binfo = &((*fent->bufferred_blocks)[ block_id ]);
      }
      else {
         // have an entry 
         binfo = &itr->second;
      }
      
      if( binfo->block_buf == NULL ) {
         // allocate this 
         binfo->block_buf = CALLOC_LIST( char, buf_len );
         binfo->block_len = buf_len;
         
         if( binfo->block_buf == NULL )
            return -ENOMEM;
         
      }
      else if( binfo->block_len != buf_len ) {
         // realloc this to be bigger/smaller
         char* tmp = (char*)realloc( binfo->block_buf, buf_len );
         
         if( tmp == NULL )
            return -ENOMEM;
         
         binfo->block_buf = tmp;
         binfo->block_len = buf_len;
      }
      
      // copy the data over 
      memcpy( binfo->block_buf, buf, buf_len );
      
      return 0;
   }
   else {
      errorf("BUG: %" PRIX64 "'s bufferred_blocks is not allocated\n", fent->file_id );
      return -ENODATA;
   }
}

// clear a single bufferred block 
// fent must be write-locked
int fs_entry_clear_bufferred_block( struct fs_entry* fent, uint64_t block_id ) {
   
   if( fent->bufferred_blocks ) {
      
      modification_map::iterator itr = fent->bufferred_blocks->find( block_id );
      if( itr != fent->bufferred_blocks->end() ) {
         
         struct fs_entry_block_info* binfo = &itr->second;
      
         // clear it 
         fs_entry_block_info_free_ex( binfo, true );
      }
   }
   return 0;
}

// extract all bufferred blocks to a modification map.
// replace the bufferred blocks in fent with a new, empty set.
// fent must be write-locked 
int fs_entry_extract_bufferred_blocks( struct fs_entry* fent, modification_map* block_info ) {
   if( fent->bufferred_blocks ) {
      
      for( modification_map::iterator itr = fent->bufferred_blocks->begin(); itr != fent->bufferred_blocks->end(); itr++ ) {
         
         uint64_t block_id = itr->first;
         struct fs_entry_block_info* binfo = &itr->second;
         
         // NOTE: don't duplicate; copy directly
         (*block_info)[ block_id ] = *binfo;
      }
      
      fent->bufferred_blocks->clear();
   }
   
   return 0;
}

// how many items are queued in the sync queue?
// fent must be at least read-locked 
size_t fs_entry_sync_context_size( struct fs_entry* fent ) {
   return fent->sync_queue->size();
}

// add a sync context to the sync queue
// fent must be write-locked
int fs_entry_sync_context_enqueue( struct fs_entry* fent, struct sync_context* ctx ) {
   fent->sync_queue->push_back( ctx );
   
   return 0;
}

// remove a sync context from the head of the sync queue.
// return 0 on succes; -ENOENT if no entries
// fent must be write-locked
int fs_entry_sync_context_dequeue( struct fs_entry* fent, struct sync_context** ctx ) {
   int rc = 0;
   
   if( fent->sync_queue->size() > 0 ) {
      *ctx = fent->sync_queue->front();
      fent->sync_queue->pop_front();
   }
   else {
      rc = -ENOENT;
   }
   
   return rc;
}

// clear a sync context from the sync queue 
// fent must be write-locked
int fs_entry_sync_context_remove( struct fs_entry* fent, struct sync_context* ctx ) {
   int rc = 0;
   
   if( fent->sync_queue->size() > 0 ) {
      for( sync_context_list_t::iterator itr = fent->sync_queue->begin(); itr != fent->sync_queue->end(); itr++ ) {
         
         struct sync_context* curr_ctx = *itr;
         
         if( curr_ctx == ctx ) {
            sync_context_list_t::iterator old = itr;
            itr++;
            
            fent->sync_queue->erase( old );
         }
      }
   }
   
   return rc;
}


// advance an fs_entry's last-mod time to now.
// fent must be write-locked
int fs_entry_update_modtime( struct fs_entry* fent ) {
   
   // update our modtime
   struct timespec ts;
   int rc = clock_gettime( CLOCK_REALTIME, &ts );
   
   if( rc == 0 ) {
      fent->mtime_sec = ts.tv_sec;
      fent->mtime_nsec = ts.tv_nsec;
   }
   
   return rc;
}

// how many children (besides . and ..) are there in this fent?
unsigned int fs_entry_num_children( struct fs_entry* fent ) {
   if( fent->ftype != FTYPE_DIR || fent->children == NULL )
      return 0;
   
   return fent->children->size() - 2;
}

uint64_t fs_dir_entry_type( struct fs_dir_entry* dirent ) {
   return dirent->data.type;
}

char* fs_dir_entry_name( struct fs_dir_entry* dirent ) {
   return dirent->data.name;
}

uint64_t fs_dir_entry_file_id( struct fs_dir_entry* dirent ) {
   return dirent->data.file_id;
}

int64_t fs_dir_entry_mtime_sec( struct fs_dir_entry* dirent ) {
   return dirent->data.mtime_sec;
}

int32_t fs_dir_entry_mtime_nsec( struct fs_dir_entry* dirent ) {
   return dirent->data.mtime_nsec;
}

int64_t fs_dir_entry_ctime_sec( struct fs_dir_entry* dirent ) {
   return dirent->data.ctime_sec;
}

int32_t fs_dir_entry_ctime_nsec( struct fs_dir_entry* dirent ) {
   return dirent->data.ctime_nsec;
}

int64_t fs_dir_entry_write_nonce( struct fs_dir_entry* dirent ) {
   return dirent->data.write_nonce;
}

int64_t fs_dir_entry_xattr_nonce( struct fs_dir_entry* dirent ) {
   return dirent->data.xattr_nonce;
}

int64_t fs_dir_entry_version( struct fs_dir_entry* dirent ) {
   return dirent->data.version;
}

int32_t fs_dir_entry_max_read_freshness( struct fs_dir_entry* dirent ) {
   return dirent->data.max_read_freshness;
}

int32_t fs_dir_entry_max_write_freshness( struct fs_dir_entry* dirent ) {
   return dirent->data.max_write_freshness;
}

uint64_t fs_dir_entry_owner( struct fs_dir_entry* dirent ) {
   return dirent->data.owner;
}

uint64_t fs_dir_entry_coordinator( struct fs_dir_entry* dirent ) {
   return dirent->data.coordinator;
}

uint64_t fs_dir_entry_volume( struct fs_dir_entry* dirent ) {
   return dirent->data.volume;
}

int32_t fs_dir_entry_mode( struct fs_dir_entry* dirent ) {
   return dirent->data.mode;
}

uint64_t fs_dir_entry_size( struct fs_dir_entry* dirent ) {
   return dirent->data.size;
}

