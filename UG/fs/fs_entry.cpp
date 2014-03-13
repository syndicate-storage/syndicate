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
      return rc;
   }

   core->root->link_count = 1;
   fs_entry_set_insert( core->root->children, ".", core->root );
   fs_entry_set_insert( core->root->children, "..", core->root );

   // we're stale; refresh on read
   fs_entry_mark_read_stale( core->root );
   
   // initialize the driver
   core->driver = CALLOC_LIST( struct storage_driver, 1 );
   rc = driver_init( core, core->driver );
   
   if( rc != 0 && rc != -ENOENT ) {
      errorf("driver_init rc = %d\n", rc );
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

   if( core->driver ) {
      int rc = driver_shutdown( core, core->driver );
      if( rc != 0 ) {
         errorf("WARN: driver_shutdown rc = %d\n", rc );
      }
      
      free( core->driver );
      core->driver = NULL;
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
   
   clock_gettime( CLOCK_REALTIME, &fent->refresh_time );
   
   ts.tv_sec = mtime_sec;
   ts.tv_nsec = mtime_nsec;
   
   return 0;
}

// common fs_entry initializion code
// a version of <= 0 will cause the FS to look at the underlying data to deduce the correct version
static int fs_entry_init_common( struct fs_core* core, struct fs_entry* fent, int type, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec) {

   memset( fent, 0, sizeof(struct fs_entry) );
   fs_entry_init_data( core, fent, type, name, version, owner, coordinator, volume, mode, size, mtime_sec, mtime_nsec );
   pthread_rwlock_init( &fent->lock, NULL );
   
   return 0;
}

// create an FS entry that is a file
int fs_entry_init_file( struct fs_core* core, struct fs_entry* fent, char const* name, int64_t version, uint64_t owner, uint64_t coordinator, uint64_t volume, mode_t mode, off_t size, int64_t mtime_sec, int32_t mtime_nsec ) {
   fs_entry_init_common( core, fent, FTYPE_FILE, name, version, owner, coordinator, volume, mode, size, mtime_sec, mtime_nsec );
   fent->ftype = FTYPE_FILE;
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

// duplicate an FS entry
int fs_entry_dup( struct fs_core* core, struct fs_entry* fent, struct fs_entry* src ) {
   fs_entry_init_common( core, fent, src->ftype, src->name, src->version, src->owner, src->coordinator, src->volume, src->mode, src->size, src->mtime_sec, src->mtime_nsec );
   fent->ftype = src->ftype;
   fent->file_id = src->file_id;
   
   if( src->children ) {
      fent->children = new fs_entry_set();
      for( fs_entry_set::iterator itr = src->children->begin(); itr != src->children->end(); itr++ ) {
         fs_dirent d( itr->first, itr->second );
         fent->children->push_back( d );
      }
   }

   fent->manifest = new file_manifest( src->manifest );

   return 0;
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
   }
   else if (S_ISFIFO(ent->mode)){
      // this is a FIFO 
      fs_entry_init_fifo( core, fent, ent->name, ent->version, ent->owner, ent->coordinator, ent->volume, ent->mode, ent->size, ent->mtime_sec, ent->mtime_nsec, fent->coordinator == core->gateway );
   }

   fent->file_id = ent->file_id;
   fent->write_nonce = ent->write_nonce;
   
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
   
   fent->ftype = FTYPE_DEAD;      // next thread to hold this lock knows this is a dead entry
   
   fs_entry_unlock( fent );
   pthread_rwlock_destroy( &fent->lock );
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
// if th eval functio fails, both cur_ent and prev_ent will be unlocked
static int fs_entry_ent_eval( struct fs_entry* prev_ent, struct fs_entry* cur_ent, int (*ent_eval)( struct fs_entry*, void* ), void* cls ) {
   long name_hash = md_hash( cur_ent->name );
   char* name_dup = strdup( cur_ent->name );
   
   int eval_rc = (*ent_eval)( cur_ent, cls );
   if( eval_rc != 0 ) {
      
      dbprintf("ent_eval(%" PRIX64 " (%s)) rc = %d\n", cur_ent->file_id, name_dup, eval_rc );
      
      // cur_ent might not even exist anymore....
      if( cur_ent->ftype != FTYPE_DEAD ) {
         fs_entry_unlock( cur_ent );
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

   // run our evaluator on the root entry
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
         
         /*
         // attempt to lock.  If this is the last step of the path,
         // then write-lock it if needed
         if( name == NULL && writelock )
            fs_entry_wlock( cur_ent );
         else
            fs_entry_rlock( cur_ent );
         */
         
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
   
   dest->type = fent->ftype == FTYPE_FILE ? MD_ENTRY_FILE : MD_ENTRY_DIR;
   dest->name = strdup( fent->name );
   dest->file_id = fent->file_id;
   dest->ctime_sec = fent->ctime_sec;
   dest->ctime_nsec = fent->ctime_nsec;
   dest->mtime_sec = fent->mtime_sec;
   dest->mtime_nsec = fent->mtime_nsec;
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
   if( fh->rctxs ) {
      delete fh->rctxs;
   }
   pthread_rwlock_unlock( &fh->lock );
   pthread_rwlock_destroy( &fh->lock );

   return 0;
}


// reversion a file.  Only valid for local files 
// FENT MUST BE WRITE-LOCKED!
int fs_entry_reversion_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t new_version, uint64_t parent_id, char const* parent_name ) {

   if( !FS_ENTRY_LOCAL( core, fent ) ) {
      return -EINVAL;
   }
   
   // reversion the data locally
   int rc = fs_entry_cache_reversion_file( core, core->cache, fent->file_id, fent->version, new_version );
   if( rc != 0 ) {
      return rc;
   }

   // set the version on local data, since the local reversioning succeeded
   fent->version = new_version;
   fent->manifest->set_file_version( core, new_version );

   struct md_entry ent;
   fs_entry_to_md_entry( core, &ent, fent, parent_id, parent_name );

   // synchronously update
   rc = ms_client_update( core->ms, &ent );

   md_entry_free( &ent );

   if( rc != 0 ) {
      // failed to reversion remotely
      errorf("ms_client_update(%s.%" PRId64 " --> %" PRId64 ") rc = %d\n", fs_path, fent->version, new_version, rc );
   }
   
   return rc;
}


int fs_entry_block_info_free( struct fs_entry_block_info* binfo ) {
   if( binfo->hash ) {
      free( binfo->hash );
      binfo->hash = NULL;
   }
   
   memset(binfo, 0, sizeof(struct fs_entry_block_info) );
   return 0;
}


// view change callback: reload the driver
int fs_entry_view_change_callback( struct ms_client* ms, void* cls ) {
   struct fs_entry_view_change_cls* viewchange_cls = (struct fs_entry_view_change_cls*)cls;
   
   struct fs_core* core = viewchange_cls->core;
   uint64_t old_version = viewchange_cls->cert_version;
   
   uint64_t cert_version = ms_client_cert_version( ms );
   
   if( cert_version != old_version ) {
      dbprintf("cert version was %" PRIu64 ", now is %" PRIu64 ".  Reloading driver...\n", old_version, cert_version );
      
      int rc = driver_reload( core, core->driver );
      if( rc == 0 )
         viewchange_cls->cert_version = cert_version;
      
   }
   else {
      dbprintf("%s", "cert version has not changed, so not reloading driver\n" );
   }
   
   return 0;
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

