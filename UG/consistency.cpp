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

#include "consistency.h"
#include "manifest.h"
#include "url.h"
#include "link.h"
#include "unlink.h"
#include "network.h"
#include "replication.h"
#include "driver.h"


// is a fent stale for reads?
bool fs_entry_is_read_stale( struct fs_entry* fent ) {
   if( fent->read_stale ) {
      dbprintf("%s is read stale\n", fent->name);
      return true;
   }

   uint64_t now_ms = currentTimeMillis();
   uint64_t refresh_ms = (uint64_t)(fent->refresh_time.tv_sec) * 1000 + (uint64_t)(fent->refresh_time.tv_nsec) / 1000000;

   dbprintf("%s is %" PRIu64 " millis old, max is %d\n", fent->name, now_ms - refresh_ms, fent->max_read_freshness );
   if( now_ms - refresh_ms >= (uint64_t)fent->max_read_freshness )
      return true;
   else
      return false;
}


// determine whether or not an entry is stale, given the current entry's modtime and the time of the query.
// for files, the modtime is the manifest modtime (which increases monotonically)
// for directories, the modtime is the fs_entry modtime (which also increases monotonically)
// fent must be at least read-locked!
static bool fs_entry_should_reload( struct fs_core* core, struct fs_entry* fent, struct md_entry* ent, struct timespec* query_time ) {
   
   int64_t local_modtime_sec = 0;
   int32_t local_modtime_nsec = 0;
   
   int64_t remote_modtime_sec = 0;
   int32_t remote_modtime_nsec = 0;
   
   int64_t write_nonce = ent->write_nonce;
   
   // a directory is stale if the write nonce has changed
   if( fent->ftype == FTYPE_DIR ) {
      if( fent->read_stale ) {
         dbprintf("directory %s is stale\n", fent->name );
         return true;
      }
      if( fent->write_nonce != write_nonce ) {
         dbprintf("write nonce of directory %s has changed\n", fent->name);
         return true;
      }
      else {
         dbprintf("write nonce of directory %s has NOT changed\n", fent->name);
         return false;
      }
   }

   if( FS_ENTRY_LOCAL( core, fent ) ) {
      
      if( fent->ftype == FTYPE_DIR ) {
         local_modtime_sec = fent->mtime_sec;
         local_modtime_nsec = fent->mtime_nsec;
         
         remote_modtime_sec = ent->mtime_sec;
         remote_modtime_nsec = ent->mtime_sec;
      }
      else {
         if( fent->manifest == NULL || !fent->manifest->is_initialized() ) {
            // definitely stale 
            return true;
         }
         else {
            // check manifest modtime
            fent->manifest->get_modtime( &local_modtime_sec, &local_modtime_nsec );
            
            remote_modtime_sec = ent->manifest_mtime_sec;
            remote_modtime_nsec = ent->manifest_mtime_nsec;
         }
      }
      
      // local means that only this UG controls its ctime and mtime (which both increase monotonically)
      if( fent->ctime_sec > query_time->tv_sec || (fent->ctime_sec == query_time->tv_sec && fent->ctime_nsec > query_time->tv_nsec) ) {
         // fent is local and was created after the query time.  Don't reload; we have potentially uncommitted changes.
         return false;
      }
      if( local_modtime_sec > query_time->tv_sec || (local_modtime_sec == query_time->tv_sec && local_modtime_nsec > query_time->tv_nsec) ) {
         // fent is local and was modified after the query time.  Don't reload; we have potentially uncommitted changes
         return false;
      }
      if( local_modtime_sec == remote_modtime_sec && local_modtime_nsec == remote_modtime_nsec ) {
         // not modified
         return false;
      }
      if( fent->write_nonce == write_nonce ) {
         // not modified, but mtime was tweaked in utime
         return false;
      }
      
      // remotely modified
      return true;
   }
   else {
      // remote object--check write nonce only 
      return (fent->write_nonce != write_nonce);
   }
}


// determine whether or not we need to clear the cached xattrs 
static bool fs_entry_xattr_cache_stale( struct fs_entry* fent, int64_t xattr_nonce ) {
   return fent->xattr_nonce != xattr_nonce;
}

// make a fent read stale
int fs_entry_mark_read_stale( struct fs_entry* fent ) {
   fent->read_stale = true;
   fent->write_nonce = 0;       // make sure that when we read next, we get a listing
   return 0;
}


// is a manifest stale?
bool fs_entry_is_manifest_stale( struct fs_entry* fent ) {
   if( fent->manifest == NULL ) {
      return true;
   }
   
   if( !fent->manifest->is_initialized() ) {
      return true;
   }
   
   if( fent->manifest->is_stale() ) {
      return true;
   }
   return false;
}

static int fs_entry_mark_read_fresh( struct fs_entry* fent ) {   
   clock_gettime( CLOCK_REALTIME, &fent->refresh_time );
   fent->read_stale = false;

   return 0;
}

static int fs_entry_mark_read_fresh_path( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* fent ) {
   
   fs_entry_mark_read_fresh( fent );
   consistency_cls->reloaded.push_back( fent->file_id );
   
   return 0;
}

// reload a write-locked fs_entry with an md_entry's data, marking the manifest as stale if it is a file
int fs_entry_reload( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* fent, struct md_entry* ent ) {

   if( fent->manifest ) {
      // the manifest is only stale 
      if( !fent->manifest->is_initialized() ) {
         fent->manifest->mark_stale();
      }
      else {
         int64_t modtime_sec = 0;
         int32_t modtime_nsec = 0;
         
         fent->manifest->get_modtime( &modtime_sec, &modtime_nsec );
         
         if( modtime_sec != ent->manifest_mtime_sec || modtime_nsec != ent->manifest_mtime_nsec || fent->write_nonce != ent->write_nonce )
            fent->manifest->mark_stale();

         if( fent->version != fent->manifest->get_file_version() )
            fent->manifest->mark_stale();
      }
   }
   
   fent->owner = ent->owner;
   fent->coordinator = ent->coordinator;
   fent->mode = ent->mode;
   fent->size = ent->size;
   fent->mtime_sec = ent->mtime_sec;
   fent->mtime_nsec = ent->mtime_nsec;
   fent->ctime_sec = ent->ctime_sec;
   fent->ctime_nsec = ent->ctime_nsec;
   fent->volume = ent->volume;
   fent->max_read_freshness = ent->max_read_freshness;
   fent->max_write_freshness = ent->max_write_freshness;
   fent->file_id = ent->file_id;
   fent->version = ent->version;
   fent->write_nonce = ent->write_nonce;
   fent->xattr_nonce = ent->xattr_nonce;
   
   if( fent->name )
      free( fent->name );
      
   fent->name = strdup( ent->name );
   
   if( fent->manifest )
      fent->manifest->set_modtime( ent->manifest_mtime_sec, ent->manifest_mtime_nsec );
   
   fs_entry_mark_read_fresh_path( consistency_cls, fent );
   dbprintf("reloaded %s up to (%" PRIu64 ".%d)\n", ent->name, ent->manifest_mtime_sec, ent->manifest_mtime_nsec );
   return 0;
}
   

// given an MS directory record and a directory, attach it
static struct fs_entry* fs_entry_attach_ms_directory( struct fs_core* core, struct fs_entry* parent, struct md_entry* ms_record ) {
   struct fs_entry* new_dir = CALLOC_LIST( struct fs_entry, 1 );
   fs_entry_init_md( core, new_dir, ms_record );

   // Make sure this is a directory we're attaching
   if( new_dir->ftype != FTYPE_DIR ) {
      // invalid MS data
      errorf("not a directory: /%" PRIu64 "/%" PRIu64 "/%" PRIX64 "\n", ms_record->volume, ms_record->coordinator, ms_record->file_id );
      
      fs_entry_destroy( new_dir, true );
      
      free( new_dir );
      return NULL;
   }
   else {
      dbprintf("add dir %p\n", new_dir );
      // add the new directory; make a note to load up its children on the next opendir()
      fs_entry_set_insert( new_dir->children, ".", new_dir );
      fs_entry_set_insert( new_dir->children, "..", parent );
      
      fs_entry_attach_lowlevel( core, parent, new_dir );
      
      new_dir->read_stale = false;
      clock_gettime( CLOCK_REALTIME, &new_dir->refresh_time );
      
      return new_dir;
   }
}

// given an MS file record and a directory, attach it
static struct fs_entry* fs_entry_attach_ms_file( struct fs_core* core, struct fs_entry* parent, struct md_entry* ms_record ) {
   struct fs_entry* new_file = CALLOC_LIST( struct fs_entry, 1 );
   
   fs_entry_init_md( core, new_file, ms_record );

   // Make sure this is a directory we're attaching
   if( ( new_file->ftype != FTYPE_FILE ) &&
	  ( new_file->ftype != FTYPE_FIFO )) {
      // invalid MS data
      errorf("not a file: /%" PRIu64 "/%" PRIu64 "/%" PRIX64 " (type = %d)\n", ms_record->volume, ms_record->coordinator, ms_record->file_id, new_file->ftype );
      fs_entry_destroy( new_file, true );
      return NULL;
   }
   else {
      dbprintf("add file %" PRIX64 " (at %p)\n", new_file->file_id, new_file );
      
      fs_entry_attach_lowlevel( core, parent, new_file );

      clock_gettime( CLOCK_REALTIME, &new_file->refresh_time );
      new_file->read_stale = false;
      new_file->manifest->mark_stale();
      return new_file;
   }
}

// attach an MS record to a directory
static struct fs_entry* fs_entry_add_ms_record( struct fs_core* core, struct fs_entry* parent, struct md_entry* ms_record ) {
   // is this a file or directory?
   if( ms_record->type == MD_ENTRY_FILE ) {
      // file
      return fs_entry_attach_ms_file( core, parent, ms_record );
   }
   else {
      // directory
      return fs_entry_attach_ms_directory( core, parent, ms_record );
   }
}

// split up a path into its components.
static size_t fs_entry_split_path( char const* _path, vector<char*>* ret_vec ) {
   char* path = NULL;

   // if path ends in /, make it end in .
   if( _path[strlen(_path)-1] == '/' ) {
      path = CALLOC_LIST( char, strlen(_path) + 2 );
      strcpy( path, _path );
      strcat( path, "." );
   }
   else {
      path = strdup( _path );
   }

   // tokenize and count up
   char* tmp = NULL;
   char* path_tok = path;
   char* tok = NULL;
   
   ret_vec->push_back( strdup("/") );

   do {
      tok = strtok_r( path_tok, "/", &tmp );
      path_tok = NULL;

      if( tok == NULL )
         break;

      ret_vec->push_back( strdup(tok) );
   } while( tok != NULL );

   free( path );
   
   return ret_vec->size();
}

static int fs_entry_make_listing_cls( struct fs_entry_listing_cls* cls, char const* parent_path, char const* name, bool exists, bool stale ) {
   memset( cls, 0, sizeof(struct ms_listing) );
   cls->fs_path = md_fullpath( parent_path, name, NULL );
   cls->stale = stale;
   cls->exists = exists;

   return 0;
}

static void fs_entry_free_listing_cls( void* _cls ) {
   struct fs_entry_listing_cls* cls = (struct fs_entry_listing_cls*)_cls;
   
   if( cls->fs_path ) {
      free( cls->fs_path );
   }
   ms_client_free_listing( &cls->listing );
   free( cls );
}


static int fs_entry_ms_path_append( struct fs_entry* fent, void* ms_path_cls ) {
   // build up the ms_path as we traverse our cached path
   path_t* ms_path = (path_t*)ms_path_cls;

   struct fs_entry_listing_cls* cls = CALLOC_LIST( struct fs_entry_listing_cls, 1 );

   if( ms_path->size() == 0 ) {
      // root
      fs_entry_make_listing_cls( cls, "/", "", true, fs_entry_is_read_stale( fent ) );
   }
   else {
      // not root
      struct fs_entry_listing_cls* parent_cls = (struct fs_entry_listing_cls*)ms_path->at( ms_path->size() - 1 ).cls;
      
      fs_entry_make_listing_cls( cls, parent_cls->fs_path, fent->name, true, fs_entry_is_read_stale( fent ) );
   }
                              
   struct ms_path_ent path_ent;
   ms_client_make_path_ent( &path_ent, fent->volume, fent->file_id, fent->version, fent->write_nonce, fent->name, cls );
   
   ms_path->push_back( path_ent );
   
   //////////////////
   
   int64_t modtime_sec = 0;
   int32_t modtime_nsec = 0;
   bool manifest_inited = false;
   
   if( fent->manifest && fent->manifest->is_initialized() ) {
      fent->manifest->get_modtime( &modtime_sec, &modtime_nsec );
      manifest_inited = true;
   }
   else {
      modtime_sec = fent->mtime_sec;
      modtime_nsec = fent->mtime_nsec;
   }
   
   dbprintf("in path: %s.%" PRId64 " (mtime=%" PRId64 ".%d, inited=%d) (write_nonce=%" PRId64 ") (%s)\n", fent->name, fent->version, modtime_sec, modtime_nsec, manifest_inited, fent->write_nonce, cls->fs_path);
   return 0;
}



static int fs_entry_build_ms_path( struct fs_core* core, char const* path, path_t* ms_path ) {
   // build up an ms_path from the actual path
   vector<char*> path_parts;
   size_t path_len = fs_entry_split_path( path, &path_parts );
   int rc = 0;

   // populate ms_path with our cached entries
   struct fs_entry* fent = fs_entry_resolve_path_cls( core, path, core->ms->owner_id, core->volume, false, &rc, fs_entry_ms_path_append, ms_path );
   if( fent == NULL ) {
      if( rc == -ENOENT ) {
         rc = 0;
         
         // populate the remaining path elements with empties.
         // We're trying to read directory listings that we don't know about (yet).
         size_t ms_path_len = ms_path->size();
         for( unsigned int i = ms_path_len; i < path_len; i++ ) {

            struct fs_entry_listing_cls* cls = CALLOC_LIST( struct fs_entry_listing_cls, 1 );
            struct fs_entry_listing_cls* parent_cls = (struct fs_entry_listing_cls*)ms_path->at( ms_path->size() - 1 ).cls;

            dbprintf("add %s to %s (%s)\n", path_parts[i], parent_cls->fs_path, ms_path->at( ms_path->size() - 1 ).name );
            
            fs_entry_make_listing_cls( cls, parent_cls->fs_path, path_parts[i], false, false );

            struct ms_path_ent path_ent;

            ms_client_make_path_ent( &path_ent, 0, 0, -1, 0, path_parts[i], cls );

            ms_path->push_back( path_ent );
         }
      }
   }
   else {
      fs_entry_unlock( fent );
   }

   // free memory
   for( unsigned int i = 0; i < path_parts.size(); i++ ) {
      free( path_parts[i] );
   }
   
   dbprintf("ms_path size = %zu\n", ms_path->size() );
   for( unsigned int i = 0; i < ms_path->size(); i++ ) {
      struct fs_entry_listing_cls* cls = (struct fs_entry_listing_cls*)ms_path->at(i).cls;
      dbprintf("ms_path[%d] = %s, stale = %d, exists = %d\n", i, ms_path->at(i).name, cls->stale, cls->exists );
   }

   return rc;
}

static int fs_entry_zip_path_listing( path_t* ms_path, ms_response_t* ms_response ) {
   // merge an ms_response's into the path, via our path cls.
   for( unsigned int i = 0; i < ms_path->size(); i++ ) {

      struct fs_entry_listing_cls* listing_cls = (struct fs_entry_listing_cls*)ms_path->at(i).cls;
      
      // find the response for this path entry 
      ms_response_t::iterator itr = ms_response->find( ms_path->at(i).file_id );
      if( itr == ms_response->end() ) {
         // no response
         memset( &listing_cls->listing, 0, sizeof(struct ms_listing) );
      }
      else {
         listing_cls->listing = itr->second;
      }
   }

   return 0;
}


// fent should be write-locked
static int fs_entry_reload_file( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* fent, struct ms_listing* listing ) {
   // sanity check
   if( fent->ftype != FTYPE_FILE )
      return -EINVAL;

   if( listing->type != ms::ms_entry::MS_ENTRY_TYPE_FILE )
      return -EINVAL;

   if( listing->entries->size() != 1 ) {
      errorf("Got back %zu listings\n", listing->entries->size() );
      return -EINVAL;
   }

   // reload this file's metadata?
   struct md_entry* ent = &(*listing->entries)[0];
   if( ent->name == NULL ) {
      // nothing to load
      errorf("No data for '%s'\n", fent->name );
      return -ENODATA;
   }
   
   // xattr cache stale?
   if( fs_entry_xattr_cache_stale( fent, ent->xattr_nonce ) ) {
      fs_entry_clear_cached_xattrs( fent, ent->xattr_nonce );
   }
   
   if( !fs_entry_should_reload( consistency_cls->core, fent, ent, &consistency_cls->query_time ) ) {
      // nothing to do
      fs_entry_mark_read_fresh_path( consistency_cls, fent );
      return 0;
   }
   return fs_entry_reload( consistency_cls, fent, ent );
}


static int fs_entry_clear_child( fs_entry_set* children, unsigned int i ) {
   if( i >= children->size() )
      return -EINVAL;

   children->at(i).first = 0;
   children->at(i).second = NULL;
   return 0;
}


static struct fs_entry* fs_entry_remove_child( fs_entry_set* children, char const* name ) {
   struct fs_entry* ret = fs_entry_set_find_name( children, name );
   if( ret != NULL )
      fs_entry_set_remove( children, name );
   
   return ret;
}


static int fs_entry_populate_directory( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* dent, struct md_entry** ms_ents, size_t ms_ents_size ) {
   int rc = 0;
   
   // add the new entries (they will be the non-NULL entries)
   for( unsigned int i = 0; i < ms_ents_size; i++ ) {

      struct md_entry* ms_ent = ms_ents[i];
      if( ms_ent == NULL ) {
         // already processed
         continue;
      }

      if( ms_ent->file_id == dent->file_id ) {
         // skip
         continue;
      }
      
      dbprintf("Attach: %s --> %s\n", dent->name, ms_ent->name );
      struct fs_entry* child = fs_entry_add_ms_record( consistency_cls->core, dent, ms_ent );
      if( child == NULL ) {
         errorf("fs_entry_add_ms_record(%" PRIX64 " (%s) to %" PRIX64 " (%s)) returned NULL\n", ms_ent->file_id, ms_ent->name, dent->file_id, dent->name );
         rc = -EUCLEAN;
         break;
      }
      else {
         if( child->ftype == FTYPE_DIR ) {
            // directories are always stale on load, since we don't know if they have children yet
            fs_entry_mark_read_stale( child );
         }
      }
   }

   return rc;
}


// reload a directory.
// reload its base information, plus all of its immediate children that are stale and don't have pending updates.
// remove children no longer present in the listing.
// dent must be WRITE-LOCKED
static int fs_entry_reload_directory( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* dent, struct ms_listing* listing ) {
   // sanity check
   if( dent->ftype != FTYPE_DIR )
      return -EINVAL;

   if( listing->type != ms::ms_entry::MS_ENTRY_TYPE_DIR )
      return -EINVAL;
   
   dbprintf("Reload directory %" PRIX64 " (%s) with new data\n", dent->file_id, dent->name );
   
   vector<struct md_entry>* ms_ents_vec = listing->entries;

   // convert to list, so we don't have to modify the listing
   size_t ms_ents_size = ms_ents_vec->size();
   struct md_entry** ms_ents = CALLOC_LIST( struct md_entry*, ms_ents_size );
   
   for( unsigned int i = 0; i < ms_ents_size; i++ ) {
      // skip freed ones
      if( ms_ents_vec->at(i).name == NULL )
         continue;
      
      ms_ents[i] = &(*ms_ents_vec)[i];
      dbprintf("listing: %" PRIX64 " %s.%" PRId64 " (mtime=%" PRId64 ".%d) (write_nonce=%" PRId64 ")\n", ms_ents[i]->file_id, ms_ents[i]->name, ms_ents[i]->version, ms_ents[i]->mtime_sec, ms_ents[i]->mtime_nsec, ms_ents[i]->write_nonce);
   }

   // reload this entry
   bool reloaded_dent = false;
   for( unsigned int i = 0; i < ms_ents_size; i++ ) {
      struct md_entry* ms_ent = ms_ents[i];
      
      if( ms_ent == NULL )
         continue;

      if( ms_ent->file_id == dent->file_id ) {
         // check cached xattrs 
         if( fs_entry_xattr_cache_stale( dent, ms_ent->xattr_nonce ) ) {
            fs_entry_clear_cached_xattrs( dent, ms_ent->xattr_nonce );
         }
         
         if( fs_entry_should_reload( consistency_cls->core, dent, ms_ent, &consistency_cls->query_time ) ) {
            dbprintf("reload '%s' ('%s')\n", dent->name, ms_ent->name );
            fs_entry_reload( consistency_cls, dent, ms_ent );
         }
         else {
            dbprintf("do not reload '%s', since we don't have to.\n", dent->name );
            fs_entry_mark_read_fresh_path( consistency_cls, dent );
         }

         reloaded_dent = true;
         ms_ents[i] = NULL;
         break;
      }
   }

   if( !reloaded_dent ) {
      // this listing indicates that the entry does not exist.  Remove all children.
      errorf("Directory entry %" PRIX64 " (%s) not found in listing\n", dent->file_id, dent->name );

      int rc = fs_unlink_children( consistency_cls->core, dent->children, true );
      if( rc != 0 ) {
         // NOTE: this should never happen in practice, but here for defensive purposes
         errorf("fs_unlink_children(%" PRIX64 " (%s)) rc = %d\n", dent->file_id, dent->name, rc );
         rc = -EIO;
      }

      // delete this entry
      uint64_t file_id = dent->file_id;
      dent->link_count = 0;
      rc = fs_entry_try_destroy( consistency_cls->core, dent );
      if( rc > 0 ) {
         // entry was destroyed
         dbprintf("Destroyed %" PRIX64 "\n", file_id);
      }
      else {
         // last closedir() will destroy this
         fs_entry_unlock( dent );
      }
      
      consistency_cls->err = -EUNATCH;    // this is guaranteed not to be returned by fs_entry_resolve_path_cls, and indicates "detachment"

      free( ms_ents );
      
      return -ENOENT;
   }

   // build a new child list for this directory
   fs_entry_set* children_keep = new fs_entry_set();
   fs_entry_set* children = dent->children;
   
   // keep . and ..
   struct fs_entry* dot = fs_entry_remove_child( children, "." );
   struct fs_entry* dotdot = fs_entry_remove_child( children, ".." );

   fs_entry_set_insert( children_keep, ".", dot );
   fs_entry_set_insert( children_keep, "..", dotdot );
   

   // find the keepers listed in ms_ents
   for( unsigned int i = 0; i < ms_ents_size; i++ ) {
      
      struct md_entry* ms_ent = ms_ents[i];
      bool reloaded_child = false;
      
      if( ms_ent == NULL )
         continue;
      
      for( unsigned int j = 0; j < children->size(); j++ ) {
         struct fs_entry* child = children->at(j).second;

         if( child == NULL )
            continue;

         if( ms_ent->file_id == child->file_id ) {

            // check cached xattrs 
            if( fs_entry_xattr_cache_stale( child, ms_ent->xattr_nonce ) ) {
               fs_entry_clear_cached_xattrs( child, ms_ent->xattr_nonce );
            }
            
            // do the reload (if we need to)
            if( fs_entry_should_reload( consistency_cls->core, child, ms_ent, &consistency_cls->query_time ) ) {
               // keep directories marked as read-stale if they were before, since we want to later refresh their children if they were modified
               bool read_stale = false;

               if( child->ftype == FTYPE_DIR )
                  read_stale = fs_entry_is_read_stale( child );
               
               fs_entry_reload( consistency_cls, child, ms_ent );

               if( child->ftype == FTYPE_DIR )
                  child->read_stale = read_stale;
            }
            
            // keep this child
            fs_entry_set_insert( children_keep, child->name, child );
            fs_entry_clear_child( children, j );

            reloaded_child = true;
            break;
         }
      }

      if( reloaded_child ) {
         // clear this entry from ms_ents
         ms_ents[i] = NULL;
      }
   }
   
   // keep all locally-coordinated files
   for( unsigned int i = 0; i < children->size(); i++ ) {
      struct fs_entry* child = children->at(i).second;
      
      if( child == NULL )
         continue;
      
      if( child->coordinator == consistency_cls->core->gateway ) {
         // we own this, and would have unlinked it via some other process
         fs_entry_set_insert( children_keep, child->name, child );
         fs_entry_clear_child( children, i );
      }
   }
         
   
   // new child set, filled with the keepers
   dent->children = children_keep;

   // add the new entries from the MS to dent
   int rc = fs_entry_populate_directory( consistency_cls, dent, ms_ents, ms_ents_size );

   if( rc != 0 ) {
      // error processing
      errorf("fs_entry_populate_directory(%" PRIX64 " (%s)) rc = %d\n", dent->file_id, dent->name, rc );
   }
   
   // the old children now contains all fs_entry structures not found in the listing.
   // delete them.
   rc = fs_unlink_children( consistency_cls->core, children, true );
   if( rc != 0 ) {
      // NOTE: this should never happen in practice, but here for defensive purposes
      errorf("fs_unlink_children(%" PRIX64 " (%s)) rc = %d\n", dent->file_id, dent->name, rc );
   }

   delete children;
   free( ms_ents );
   
   dent->read_stale = false;

   return rc;
}


static int fs_entry_path_find( path_t* ms_path, uint64_t file_id ) {

   // find this directory along the path
   unsigned int i = 0;
   for( i = 0; i < ms_path->size(); i++ ) {
      if( ms_path->at(i).file_id == file_id ) {
         break;
      }
   }

   if( i == ms_path->size() ) {
      // not found, so nothing to do
      dbprintf("%" PRIX64 " not found in:\n", file_id );
      for( i = 0; i < ms_path->size(); i++ ) {
         dbprintf("   %" PRIX64 " %s\n", ms_path->at(i).file_id, ms_path->at(i).name );
      }
      
      return -ENOENT;
   }

   return (signed)i;
}


static int fs_entry_path_find_by_name( path_t* ms_path, char const* name ) {

   // find this directory along the path
   unsigned int i = 0;
   for( i = 0; i < ms_path->size(); i++ ) {
      if( strcmp(ms_path->at(i).name, name) == 0 ) {
         break;
      }
   }

   if( i == ms_path->size() ) {
      // not found, so nothing to do
      dbprintf("%s not found in:\n", name );
      for( i = 0; i < ms_path->size(); i++ ) {
         dbprintf("   %" PRIX64 " %s\n", ms_path->at(i).file_id, ms_path->at(i).name );
      }
      
      return -ENOENT;
   }

   return (signed)i;
}


static int fs_entry_load_listing( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* fent, struct ms_listing* listing ) {

   int rc = 0;
   
   if( listing->status == MS_LISTING_NOCHANGE ) {
      // nothing to do
      dbprintf("No change: %" PRIX64 " (%s)\n", fent->file_id, fent->name );
      fs_entry_mark_read_fresh( fent );
      return 0;
   }

   else if( listing->status == MS_LISTING_NONE ) {
      // nothing on the MS
      dbprintf("Removed: %" PRIX64 " (%s)\n", fent->file_id, fent->name );
      consistency_cls->err = MS_LISTING_NONE;
      return -ENOENT;
   }

   else if( listing->status == MS_LISTING_NEW ) {
      
      dbprintf("New data: %" PRIX64 " (%s)\n", fent->file_id, fent->name );
      
      // verify the types are still the same
      int rc = 0;
      if( fent->ftype == FTYPE_DIR && listing->type == ms::ms_entry::MS_ENTRY_TYPE_DIR ) {
         // reload this directory
         rc = fs_entry_reload_directory( consistency_cls, fent, listing );
      }
      else if( fent->ftype == FTYPE_FILE && listing->type == ms::ms_entry::MS_ENTRY_TYPE_FILE ) {
         // reload this file
         rc = fs_entry_reload_file( consistency_cls, fent, listing );
      }
      else {
         // something's wrong
         errorf("Incompatible types: fs_entry is %d, but ms_entry is %d\n", fent->ftype, listing->type );
         rc = -EINVAL;
      }

      consistency_cls->err = rc;
   }
   else {
      errorf("Unknown listing status %d\n", listing->status );
      rc = -EINVAL;
   }

   return rc;
}


// NOTE: fent must be write-locked
static int fs_entry_reload_entry( struct fs_entry* fent, void* cls ) {
   // reload this particular directory
   struct fs_entry_consistency_cls* consistency_cls = (struct fs_entry_consistency_cls*)cls;
   
   path_t* ms_path = consistency_cls->path;

   // find this directory along the path
   int i = fs_entry_path_find( ms_path, fent->file_id );
   
   if( i == -ENOENT ) {
      // not found in this portion of the path, so nothing to do
      return 0;
   }

   // this directory's listing from the MS....
   struct fs_entry_listing_cls* listing_cls = (struct fs_entry_listing_cls*)ms_path->at(i).cls;
   struct ms_listing* listing = &listing_cls->listing;

   int rc = fs_entry_load_listing( consistency_cls, fent, listing );
   if( rc != 0 ) {
      errorf("fs_entry_load_listing(%s) rc = %d\n", fent->name, rc );
      
      if( rc == -ENOENT && consistency_cls->err == MS_LISTING_NONE ) {
         // we found an entry that is no longer on the MS.  Destroy it locally.
         
         uint64_t file_id = fent->file_id;
         dbprintf("remove %" PRIX64 " (%s) locally, since it is no longer on the MS\n", fent->file_id, fent->name );
         
         if( fent->ftype == FTYPE_DIR ) {
            // destroy all children beneath us
            rc = fs_unlink_children( consistency_cls->core, fent->children, true );
            if( rc != 0 ) {
               // NOTE: this should never happen in practice, but here for defensive purposes
               errorf("fs_unlink_children(%" PRIX64 " (%s)) rc = %d\n", fent->file_id, fent->name, rc );
               rc = 0;
            }
         }
         
         fent->link_count = 0;
         rc = fs_entry_try_destroy( consistency_cls->core, fent );
         if( rc > 0 ) {
            // entry was destroyed
            // do NOT free it; fs_entry_resolve_path_cls() will do it for us.
            dbprintf("Destroyed %" PRIX64 "\n", file_id);
         }
         
         // stop searching
         rc = -ENOENT;
      }
      
      consistency_cls->err = rc;
   }
   
   return rc;
}


// get listings and merge them into a path
static int fs_entry_download_path_listings( struct fs_core* core, path_t* to_download ) {

   ms_response_t listings;
   
   // fetch the stale entries
   int rc = ms_client_get_listings( core->ms, to_download, &listings );
   if( rc != 0 ) {
      errorf("ms_client_get_listings() rc = %d\n", rc );
      return rc;
   }

   // pair the responses with the path
   rc = fs_entry_zip_path_listing( to_download, &listings );
   if( rc != 0 ) {
      errorf("fs_entry_zip_path_listing() rc = %d\n", rc );
      ms_client_free_response( &listings );
      return -EUCLEAN;
   }

   return 0;
}


// given the list of path entries that exist locally, reload them.
// if one of them no longer exists on the MS, then unlink all of the children beneath it.
static int fs_entry_reload_local_path_entries( struct fs_entry_consistency_cls* cls, path_t* ms_path ) {

   struct fs_core* core = cls->core;

   dbprintf("%s", "reload local entries\n");
   
   int rc = fs_entry_download_path_listings( core, ms_path );
   if( rc != 0 ) {
      errorf("fs_entry_download_path_listings() rc = %d\n", rc );
      return rc;
   }

   // what is the path to the deepest entry that is present on this UG?
   struct fs_entry_listing_cls* listing_cls = (struct fs_entry_listing_cls*)ms_path->at( ms_path->size() - 1 ).cls;
   char* deepest_path = listing_cls->fs_path;

   // re-integrate them with the FS
   cls->path = ms_path;

   struct fs_entry* fent = fs_entry_resolve_path_cls( core, deepest_path, core->ms->owner_id, core->volume, true, &rc, fs_entry_reload_entry, cls );
   if( fent == NULL ) {
      if( cls->err != 0 ) {
         errorf("fs_entry_reload_entry(%s) rc = %d\n", deepest_path, rc );
         return cls->err;
      }
      else {
         // some other problem
         errorf("fs_entry_resolve_path_cls(%s) rc = %d\n", deepest_path, rc );
         return rc;
      }
   }
   
   fs_entry_unlock( fent );

   if( cls->err != 0 ) {
      rc = cls->err;
      errorf("fs_entry_reload_entry(%s) rc = %d\n", deepest_path, rc);
   }
   

   return rc;
}


// callback to iteratively build up a path from non-locally-hosted entries
// fent must be write-locked
static int fs_entry_download_and_attach_entry( struct fs_entry* fent, void* cls ) {
   
   struct fs_entry_consistency_cls* consistency_cls = (struct fs_entry_consistency_cls*)cls;
   struct fs_core* core = consistency_cls->core;
   int rc = 0;
   path_t* ms_path = consistency_cls->path;

   // find this directory along the path
   int idx = fs_entry_path_find_by_name( ms_path, fent->name );

   if( idx == -ENOENT ) {
      // not found, so nothing to do
      dbprintf( "Not found: '%s'\n", fent->name);
      return 0;
   }

   if( (unsigned)idx == ms_path->size() - 1 ) {
      // last entry; nothing to do
      dbprintf("End of path: '%s'\n", fent->name);
      return 0;
   }

   // find the child along the path we're supposed to reload
   const struct ms_path_ent& child_path_ent = ms_path->at( idx + 1 );
   const struct ms_path_ent& fent_path_ent = ms_path->at( idx );
   
   struct fs_entry_listing_cls* fent_listing_cls = (struct fs_entry_listing_cls*)fent_path_ent.cls;
   struct fs_entry_listing_cls* child_listing_cls = (struct fs_entry_listing_cls*)child_path_ent.cls;
   
   // do we need to download the child ent?
   if( child_listing_cls != NULL && child_listing_cls->exists ) {
      // nothing to do
      dbprintf("Child %s exists\n", child_listing_cls->fs_path);
      return 0;
   }
   
   // if fent is not a directory, then we're done
   if( fent->ftype != FTYPE_DIR ) {
      // nothing to do
      dbprintf("%s is not a directory\n", fent->name );
      return 0;
   }
   
   // the child should exist in fent's children
   struct fs_entry* child_fent = fs_entry_set_find_name( fent->children, child_path_ent.name );
   if( child_fent == NULL ) {
      errorf("%s: no such child '%s'\n", fent->name, child_path_ent.name );
      consistency_cls->err = -ENOENT;
      return -ENOENT;
   }
   
   fs_entry_wlock( child_fent );
   
   // if the child is not a directory, then we're done
   if( child_fent->ftype != FTYPE_DIR ) {
      dbprintf("child %s is not a directory\n", child_fent->name );
      fs_entry_unlock( child_fent );
      return 0;
   }
   
   // get the child's children.  Populate child_path_ent with the child's data
   // (it will be unpopulated, since before the call to fs_entry_revalidate_path
   // it did not exist locally).
   path_t child_path;
   
   struct fs_entry_listing_cls* child_listing_cls2 = CALLOC_LIST( struct fs_entry_listing_cls, 1 );
   fs_entry_make_listing_cls( child_listing_cls2, fent_listing_cls->fs_path, child_fent->name, true, false );

   struct ms_path_ent child_path_ent2;
   ms_client_make_path_ent( &child_path_ent2, child_fent->volume, child_fent->file_id, child_fent->version, child_fent->write_nonce, child_fent->name, child_listing_cls2 );
   
   child_path.push_back( child_path_ent2 );

   // download the listing for the child
   dbprintf("download non-local entry %" PRIX64 " %s\n", child_fent->file_id, child_fent->name );
   rc = fs_entry_download_path_listings( core, &child_path );
   if( rc != 0 ) {
      fs_entry_unlock( child_fent );
      
      errorf("fs_entry_download_path_listings(%s) rc = %d\n", fent->name, rc );
      consistency_cls->err = -EREMOTEIO;
      
      ms_client_free_path_ent( &child_path_ent2, fs_entry_free_listing_cls );
      return -EREMOTEIO;
   }

   if( child_listing_cls2->listing.status == MS_LISTING_NOCHANGE ) {
      fs_entry_unlock( child_fent );
      
      // definitely shouldn't happen---this child does not exist locally
      errorf("Entry '%s' does not exist at '%s', but MS says 'Not Modified'\n", child_path_ent2.name, child_listing_cls2->fs_path );
      consistency_cls->err = -EUCLEAN;
      
      ms_client_free_path_ent( &child_path_ent2, fs_entry_free_listing_cls );
      return -EUCLEAN;
   }

   if( child_listing_cls2->listing.status == MS_LISTING_NONE ) {
      fs_entry_unlock( child_fent );
      
      // no data for this
      errorf("Entry '%s' does not exist at '%s'\n", child_path_ent2.name, child_listing_cls2->fs_path );
      consistency_cls->err = -ENOENT;
      
      ms_client_free_path_ent( &child_path_ent2, fs_entry_free_listing_cls );
      return -ENOENT;
   }
   
   rc = fs_entry_reload_directory( consistency_cls, child_fent, &child_listing_cls2->listing );
   if( rc != 0 ) {
      errorf("fs_entry_reload_directory(%" PRIX64 " (%s) at %s) rc = %d\n", child_fent->file_id, child_fent->name, child_listing_cls2->fs_path, rc );
   }
   
   fs_entry_unlock( child_fent );
   ms_client_free_path_ent( &child_path_ent2, fs_entry_free_listing_cls );
   
   return rc;
}


// make a pass through a path and download any listings that are not local.
// Path is populated with entries that are fresh if present, and is built with fs_entry_build_ms_path
static int fs_entry_reload_remote_path_entries( struct fs_entry_consistency_cls* consistency_cls, path_t* path ) {

   dbprintf("%s", "Begin downloading remote entries\n");
   
   // get the last path
   struct fs_entry_listing_cls* listing_cls = (struct fs_entry_listing_cls*)path->at( path->size() - 1 ).cls;
   char* deepest_path = listing_cls->fs_path;
   
   // re-integrate with the FS
   consistency_cls->path = path;
   
   int rc = 0;
   struct fs_entry* fent = fs_entry_resolve_path_cls( consistency_cls->core, deepest_path, consistency_cls->core->ms->owner_id, consistency_cls->core->volume, true, &rc, fs_entry_download_and_attach_entry, consistency_cls );
   if( fent == NULL ) {
      if( consistency_cls->err != 0 ) {
         errorf("fs_entry_download_and_attach_entry(%s) rc = %d\n", deepest_path, rc );
         return consistency_cls->err;
      }
      else {
         // no network problem, so path-related
         errorf("fs_entry_resolve_path_cls(%s) rc = %d\n", deepest_path, rc );
         rc = 0;
      }
   }
   
   else {
      fs_entry_unlock( fent );
      
      if( consistency_cls->err != 0 ) {
         rc = consistency_cls->err;
         errorf("fs_entry_download_and_attach_entry(%s) rc = %d\n", deepest_path, rc );
      }
   }

   return rc;
}

int fs_entry_revalidate_path( struct fs_core* core, uint64_t volume, char const* _path ) {
   // must be absolute
   if( _path[0] != '/' )
      return -EINVAL;
   
   // normalize the path first
   int rc = 0;
   //char* path = md_normalize_url( _path, &rc );
   char* path = md_flatten_path( _path );

   if( rc != 0 ) {
      errorf("Invalid path '%s'\n", _path );
      return -EINVAL;
   }

   dbprintf("Revalidate %s\n", path );
   
   // path entires to send to the MS for resolution
   path_t ms_path;
   path_t ms_path_stale;            // buffer to hold parts of ms_path that are stale
   ms_response_t stale_listings;    // listings for stale directories

   // consistency closure
   struct fs_entry_consistency_cls consistency_cls;
   memset( &consistency_cls, 0, sizeof(consistency_cls) );
   
   consistency_cls.core = core;
   clock_gettime( CLOCK_REALTIME, &consistency_cls.query_time );     // time of refresh start

   rc = fs_entry_build_ms_path( core, path, &ms_path );
   if( rc != 0 ) {
      errorf("fs_entry_build_ms_path(%s) rc = %d\n", path, rc );
      free( path );
      return -EINVAL;
   }

   bool missing_local = false;

   // isolate the stale components
   for( unsigned int i = 0; i < ms_path.size(); i++ ) {
      
      struct fs_entry_listing_cls* listing_cls = (struct fs_entry_listing_cls*)ms_path[i].cls;
      dbprintf("listing %s\n", listing_cls->fs_path );
      
      if( listing_cls->stale && listing_cls->exists ) {
         dbprintf("%s is local and stale\n", listing_cls->fs_path );
         ms_path_stale.push_back( ms_path[i] );
      }

      if( !listing_cls->exists ) {
         // need to download some parts
         dbprintf("%s is not local\n", listing_cls->fs_path );
         missing_local = true;
         break;
      }
   }

   if( ms_path_stale.size() == 0 && !missing_local ) {
      // no need to contact the MS--we have everything, and it's fresh
      dbprintf("%s is complete and fresh\n", path );
      free( path );
      ms_client_free_path( &ms_path, fs_entry_free_listing_cls );
      return 0;
   }

   if( ms_path_stale.size() > 0 ) {
      // reload the stale entries
      dbprintf("%zu stale entries\n", ms_path_stale.size() );
      rc = fs_entry_reload_local_path_entries( &consistency_cls, &ms_path_stale );
      
      if( rc != 0 ) {
         errorf("fs_entry_reload_local_path_entries(%s) rc = %d\n", path, rc );
         free( path );
         ms_client_free_path( &ms_path, fs_entry_free_listing_cls );
         return rc;
      }
   }

   if( missing_local ) {
      // get missing path components, if all stale path components were successfully reloaded
      rc = fs_entry_reload_remote_path_entries( &consistency_cls, &ms_path );
      if( rc != 0 ) {
         errorf("fs_entry_reload_remote_path_entries(%s) rc = %d\n", path, rc );
         free( path );
         ms_client_free_path( &ms_path, fs_entry_free_listing_cls );
         return rc;
      }
   }

   free( path );
   ms_client_free_path( &ms_path, fs_entry_free_listing_cls );
   return rc;
}


// reload an fs_entry's manifest-related data, initializing it
// fent must be write-locked first!
int fs_entry_reload_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg ) {
   fent->manifest->reload( core, fent, mmsg );
   fent->size = mmsg->size();
   
   fent->mtime_sec = mmsg->fent_mtime_sec();
   fent->mtime_nsec = mmsg->fent_mtime_nsec();
   fent->version = mmsg->file_version();
   
   fent->manifest->mark_initialized();
   
   return 0;
}


// ensure that the manifest is up to date.
// if check_coordinator is true, then ask the manifest-designated gateway before asking the RGs.
// if successful_gateway_id != NULL, then fill it with the ID of the gateway that served the manifest (if any). Otherwise set to 0 if given but the manifest was fresh.
// a manifest fetched from an AG will be marked as stale, since a subsequent read can fail with HTTP 204.  The caller should mark the manifest as fresh if it succeeds in reading data.
// fent must be write-locked
int fs_entry_revalidate_manifest_ex( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t version, int64_t mtime_sec, int32_t mtime_nsec, bool check_coordinator, uint64_t* successful_gateway_id, bool force_refresh ) {
   
   if( fent->manifest != NULL && fent->manifest->is_initialized() ) {
      if( FS_ENTRY_LOCAL( core, fent ) && !core->conf->is_client ) {
         dbprintf("%s is local, and not in client mode\n", fent->name);
         return 0;      // nothing to do--we automatically have the latest
      }
      
      // if we're in client mode, and we created this file in this session and we are the coordinator, then nothing to do
      if( core->conf->is_client && FS_ENTRY_LOCAL( core, fent ) && fent->created_in_session ) {
         dbprintf("%s is local, and was created in this client session\n", fent->name);
         return 0;         // we automatically have the latest
      }
   }
   
   // otherwise, it's remote or doesn't exist, or we are running as a client and should check the RGs anyway
   
   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   bool need_refresh = false;
   if( force_refresh ) {
      need_refresh = true;
   }
   else {  
      if( fent->manifest == NULL ) {
         fent->manifest = new file_manifest( version );
         need_refresh = true;
      }
      else {
         // does the manifest need refreshing?
         need_refresh = fs_entry_is_manifest_stale( fent );
      }
   }
   
   if( !need_refresh ) {
      // we're good to go
      END_TIMING_DATA( ts, ts2, "manifest refresh (fresh)" );
   
      if( successful_gateway_id )
         *successful_gateway_id = 0;
      
      dbprintf("Manifest for %s is fresh\n", fent->name);
      return 0;
   }

   // otherwise, we need to refresh.  GoGoGo!
   struct timespec modtime;
   modtime.tv_sec = mtime_sec;
   modtime.tv_nsec = mtime_sec;
   
   char* manifest_url = NULL;
   int rc = 0;
   Serialization::ManifestMsg manifest_msg;
   
   int gateway_type = ms_client_get_gateway_type( core->ms, fent->coordinator );
   
   if( check_coordinator && !FS_ENTRY_LOCAL( core, fent ) ) {
      // check the MS-listed coordinator first, instead of asking the RGs directly
      
      if( gateway_type < 0 ) {
         // unknown gateway...try refreshing the Volume
         errorf("Unknown Gateway %" PRIu64 "\n", fent->coordinator );
         ms_client_sched_volume_reload( core->ms );
         return -EAGAIN;
      }
      
      rc = fs_entry_make_manifest_url( core, fs_path, fent->coordinator, fent->file_id, version, &modtime, &manifest_url );
      
      if( rc != 0 ) {
         // failed to produce the url
         errorf("fs_entry_make_manifest_url rc = %d\n", rc );
         
         if( rc == -ENOENT ) {
            // gateway not found.  try refreshing our certs
            ms_client_sched_volume_reload( core->ms );
            return -EAGAIN;
         }
         else {
            return -ENODATA;
         }
      }
      
      dbprintf("Reload manifest from Gateway %" PRIu64 " at %s\n", fent->coordinator, manifest_url );
  
      rc = fs_entry_download_manifest( core, fs_path, fent, mtime_sec, mtime_nsec, manifest_url, &manifest_msg );
      
      if( rc == 0 && successful_gateway_id ) {
         // got it from the coordinator
         *successful_gateway_id = fent->coordinator;
      }
   }
   
   if( gateway_type != SYNDICATE_AG && (!check_coordinator || rc != 0 || FS_ENTRY_LOCAL( core, fent )) ) {
      // either we couldn't get it from the remote UG, or its local and we don't have a copy ourselves
      
      if( rc != 0 ) {
         errorf("fs_entry_download_manifest(%s) rc = %d\n", manifest_url, rc );
      }
      
      // try the RGs
      uint64_t rg_id = 0;
      
      rc = fs_entry_download_manifest_replica( core, fs_path, fent, mtime_sec, mtime_nsec, &manifest_msg, &rg_id );
      
      if( rc != 0 ) {
         // error
         errorf("Failed to read /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/manifest.%" PRId64 ".%d from RGs\n", fent->volume, fent->file_id, version, mtime_sec, mtime_nsec );
         rc = -ENODATA;
      }
      else {
         // success!
         dbprintf("Read /%" PRIu64 "/%" PRIX64 ".%" PRId64 "/manifest.%" PRId64 ".%d from RG %" PRIu64 "\n", fent->volume, fent->file_id, version, mtime_sec, mtime_nsec, rg_id );
         rc = 0;
         
         if( successful_gateway_id ) {
            // got it from this RG
            *successful_gateway_id = rg_id;
         }
      }
   }

   free( manifest_url );

   if( rc != 0 )
      return rc;

   // is this an error code?
   if( manifest_msg.has_errorcode() ) {
      // remote gateway indicates error
      errorf("manifest error %d\n", manifest_msg.errorcode() );
      return manifest_msg.errorcode();
   }
   
   // verify that the manifest matches the timestamp
   if( manifest_msg.mtime_sec() != mtime_sec || manifest_msg.mtime_nsec() != mtime_nsec ) {
      // invalid manifest
      errorf("timestamp mismatch: got %" PRId64 ".%d, expected %" PRId64 ".%d\n", manifest_msg.mtime_sec(), manifest_msg.mtime_nsec(), mtime_sec, mtime_nsec );
      return -EBADMSG;
   }
   
   // repopulate the manifest and update the relevant metadata
   fs_entry_reload_manifest( core, fent, &manifest_msg );
   
   char* dat = fent->manifest->serialize_str();
   dbprintf("Manifest:\n%s\n", dat);
   free( dat );

   END_TIMING_DATA( ts, ts2, "manifest refresh (stale)" );
   
   return 0;
}

// ensure the manifest is fresh
// fent should be write-locked
int fs_entry_revalidate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   struct timespec ts;
   
   bool force_refresh = false;
   
   if( fent->manifest == NULL ) {
      // no manifest on file...reload it
      fent->manifest = new file_manifest( fent->version );
      fent->manifest->set_modtime( 0, 0 );
      force_refresh = true;
   }
   
   fent->manifest->get_modtime( &ts );
   
   return fs_entry_revalidate_manifest_ex( core, fs_path, fent, fent->version, ts.tv_sec, ts.tv_nsec, true, NULL, force_refresh );   
}


// become the coordinator for a fent.
// if it succeeds, we become the coordinator (and return 0)
// if it fails, we learn the new coordinator, which gets written to fent->coordinator (this method returns -EAGAIN in this case)
// NOTE: you should revalidate the metadata and manifest prior to calling this, so it's not stale
// fent must be write-locked!
int fs_entry_coordinate( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   
   // sanity check...
   if( FS_ENTRY_LOCAL( core, fent ) )
      return 0;
   
   if( core->conf->is_client ) {
      // clients can't become coordinators 
      errorf("This gateway is in client mode; will not become coordinator for %" PRIu64 "\n", fent->file_id );
      return -EINVAL;
   }
   
   // run the pre-chcoord driver code...
   int rc = driver_chcoord_begin( core, core->closure, fs_path, fent, fent->version );
   if( rc != 0 ) {
      errorf("driver_chcoord_begin(%s %" PRIX64 ".%" PRId64 ") rc = %d\n", fs_path, fent->file_id, fent->version, rc );
      return rc;
   }
   
   struct md_entry ent;
   fs_entry_to_md_entry( core, &ent, fent, 0, NULL );
   
   // try to become the coordinator
   uint64_t current_coordinator = 0;
   rc = ms_client_coordinate( core->ms, &current_coordinator, &ent );
   
   if( rc != 0 ) {
      // failed to contact the MS
      errorf("ms_client_coordinate( /%" PRIu64 "/%" PRIX64 " (%s) ) rc = %d\n", core->volume, fent->file_id, fent->name, rc );
      rc = -EREMOTEIO;
   }
   
   else {
      // if the coordintaor is different than us, then retry the operation
      if( current_coordinator != core->gateway ) {
         dbprintf("/%" PRIu64 "/%" PRIX64 " now coordinated by UG %" PRIu64 "\n", core->volume, fent->file_id, current_coordinator );
         rc = -EAGAIN;
      }
      fent->coordinator = current_coordinator;
   }
   
   // run the post-chcoord driver code...
   int driver_rc = driver_chcoord_end( core, core->closure, fs_path, fent, fent->version, rc );
   if( driver_rc != 0 ) {
      errorf("driver_chcoord_end(%s %" PRIX64 ".%" PRId64 ") rc = %d\n", fs_path, fent->file_id, fent->version, driver_rc );
   }
   
   md_entry_free( &ent );
   
   return rc;
}


// revalidate a path and the manifest at the end of the path
// fent should NOT be locked
int fs_entry_revalidate_metadata( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t* rg_id_ret ) {
   
   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   // reload this path
   int rc = fs_entry_revalidate_path( core, core->volume, fs_path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate(%s) rc = %d\n", fs_path, rc );
      return rc;
   }
   
   fs_entry_wlock( fent );
   
   // reload this manifest.  If we get this manifest from an RG, remember which one.
   uint64_t rg_id = 0;
   
   struct timespec manifest_ts;
   bool force_refresh = false;
   
   if( fent->manifest == NULL ) {
      // no manifest; force refresh
      fent->manifest = new file_manifest( fent->version );
      fent->manifest->set_modtime( fent->mtime_sec, fent->mtime_nsec );
      force_refresh = true;
   }
   
   fent->manifest->get_modtime( &manifest_ts );
   
   rc = fs_entry_revalidate_manifest_ex( core, fs_path, fent, fent->version, manifest_ts.tv_sec, manifest_ts.tv_nsec, true, &rg_id, force_refresh );

   if( rc != 0 ) {
      errorf("fs_entry_revalidate_manifest(%s) rc = %d\n", fs_path, rc );
      fs_entry_unlock( fent );
      return rc;
   }

   if( rg_id_ret != NULL ) {
      *rg_id_ret = rg_id;
   }
   
   END_TIMING_DATA( ts, ts2, "metadata latency" );
 
   fs_entry_unlock( fent );
   
   return rc;
}


