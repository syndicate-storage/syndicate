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
#include "vacuumer.h"
#include "syndicate.h"

// is a fent stale for reads?
bool fs_entry_is_read_stale( struct fs_entry* fent ) {
   if( fent->read_stale ) {
      dbprintf("%s is read stale\n", fent->name);
      return true;
   }

   uint64_t now_ms = md_current_time_millis();
   uint64_t refresh_ms = (uint64_t)(fent->refresh_time.tv_sec) * 1000 + (uint64_t)(fent->refresh_time.tv_nsec) / 1000000;

   if( now_ms - refresh_ms >= (uint64_t)fent->max_read_freshness ) {
      dbprintf("STALE: %s is %" PRIu64 " millis old, max is %d\n", fent->name, now_ms - refresh_ms, fent->max_read_freshness );
      return true;
   }
   else {
      dbprintf("FRESH: %s is %" PRIu64 " millis old, max is %d\n", fent->name, now_ms - refresh_ms, fent->max_read_freshness );
      return false;
   }
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
         dbprintf("write nonce of directory %s has changed: %" PRId64 " --> %" PRId64 "\n", fent->name, fent->write_nonce, write_nonce );
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

// reload a write-locked fs_entry with an md_entry's data, marking the manifest as stale if it is a file
int fs_entry_reload( struct fs_entry* fent, struct md_entry* ent ) {
   
   // manifest modtimes
   int64_t manifest_modtime_sec = 0;
   int32_t manifest_modtime_nsec = 0;
               
   if( fent->ftype == FTYPE_FILE ) {
      
      if( fent->manifest == NULL ) {
         // should never happen--this is a bug
         errorf("BUG: manifest of %" PRIX64 " is NULL\n", fent->file_id );
         exit(1);
      }
      
      // initialized?
      if( !fent->manifest->is_initialized() ) {
         dbprintf("%" PRIX64 "(%s)'s manifest is not initialized\n", fent->file_id, fent->name );
         fent->manifest->mark_stale();
      }
      else {
         
         // get the modtime
         fent->manifest->get_modtime( &manifest_modtime_sec, &manifest_modtime_nsec );
         
         // only consider refreshing the manifest if it's not dirty 
         if( !fent->dirty ) {
            
            // do we need to refresh the manifest later?
            if( manifest_modtime_sec != ent->manifest_mtime_sec || manifest_modtime_nsec != ent->manifest_mtime_nsec || fent->write_nonce != ent->write_nonce ) {
               
               dbprintf("%" PRIX64 " (%s)'s manifest is stale\n", fent->file_id, fent->name );
               // manifest has changed remotely
               fent->manifest->mark_stale();
            }

            if( fent->version != fent->manifest->get_file_version() ) {
               
               // file was reversioned (i.e. truncated)
               dbprintf("%" PRIX64 " (%s)'s manifest was reversioned\n", fent->file_id, fent->name );
               fent->manifest->mark_stale();
            }
         }
         else {
            dbprintf("%" PRIX64 " (%s) is dirty; will not mark manifest as stale\n", fent->file_id, fent->name );
         }
      }
   }
   
   dbprintf("reload %s (dirty = %d), version %" PRId64 " --> %" PRId64 ", manifest mtime %" PRId64 ".%d --> %" PRId64 ".%d, write nonce %" PRId64 " --> %" PRId64 ", xattr nonce %" PRId64 " --> %" PRId64 "\n",
            ent->name, fent->dirty, fent->version, ent->version, manifest_modtime_sec, manifest_modtime_nsec, ent->manifest_mtime_sec, ent->manifest_mtime_nsec,
            fent->write_nonce, ent->write_nonce, fent->xattr_nonce, ent->xattr_nonce );
   
   fs_entry_ms_reload( fent, ent );
   
   fs_entry_mark_read_fresh( fent );
   
   return 0;
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
   char* ms_path_tok = path;
   char* tok = NULL;
   
   ret_vec->push_back( strdup("/") );

   do {
      tok = strtok_r( ms_path_tok, "/", &tmp );
      ms_path_tok = NULL;

      if( tok == NULL )
         break;

      ret_vec->push_back( strdup(tok) );
   } while( tok != NULL );

   free( path );
   
   return ret_vec->size();
}

static int fs_entry_getattr_cls_init( struct fs_entry_getattr_cls* cls, char const* parent_path, char const* name, bool exists, bool stale ) {
   memset( cls, 0, sizeof(struct ms_listing) );
   cls->fs_path = md_fullpath( parent_path, name, NULL );
   cls->stale = stale;
   cls->exists = exists;

   return 0;
}

static void fs_entry_getattr_cls_free( void* _cls ) {
   struct fs_entry_getattr_cls* cls = (struct fs_entry_getattr_cls*)_cls;
   
   if( cls->fs_path ) {
      free( cls->fs_path );
   }
   md_entry_free( &cls->ent );
   free( cls );
}


// visitor along a path for building up an ms_path with inode consistency status.
// return 0 on success (always succeds)
static int fs_entry_ms_path_append( struct fs_entry* fent, void* ms_path_cls ) {
   // build up the ms_path as we traverse our cached path
   ms_path_t* ms_path = (ms_path_t*)ms_path_cls;

   struct fs_entry_getattr_cls* cls = CALLOC_LIST( struct fs_entry_getattr_cls, 1 );

   if( ms_path->size() == 0 ) {
      // root
      fs_entry_getattr_cls_init( cls, "/", "", true, fs_entry_is_read_stale( fent ) );
   }
   else {
      // not root
      struct fs_entry_getattr_cls* parent_cls = (struct fs_entry_getattr_cls*)ms_path->at( ms_path->size() - 1 ).cls;
      
      fs_entry_getattr_cls_init( cls, parent_cls->fs_path, fent->name, true, fs_entry_is_read_stale( fent ) );
   }
                              
   struct ms_path_ent path_ent;
   memset( &path_ent, 0, sizeof(struct ms_path_ent) );
   
   ms_client_make_path_ent( &path_ent, fent->volume, 0, fent->file_id, fent->version, fent->write_nonce, fent->ms_num_children, fent->generation, fent->ms_capacity, fent->name, cls );
   
   ms_path->push_back( path_ent );
   
   ////////////////// Debugging info
   
   int64_t modtime_sec = 0;
   int32_t modtime_nsec = 0;
   bool manifest_inited = false;
   char const* mtime_type = NULL;
   
   if( fent->manifest && fent->manifest->is_initialized() ) {
      fent->manifest->get_modtime( &modtime_sec, &modtime_nsec );
      manifest_inited = true;
      mtime_type = "manifest ";
   }
   else {
      modtime_sec = fent->mtime_sec;
      modtime_nsec = fent->mtime_nsec;
      mtime_type = "fent ";
   }
   
   dbprintf("in path: %s.%" PRId64 " (%s mtime=%" PRId64 ".%d, inited=%d) (write_nonce=%" PRId64 ") (%s)\n", fent->name, fent->version, mtime_type, modtime_sec, modtime_nsec,
            manifest_inited, fent->write_nonce, cls->fs_path);
   
   return 0;
}


// build up an ms_path from a path and the filesystem core.
// mark path entries as existing or non-existing locally, and stale or fresh if local.
static int fs_entry_build_ms_path( struct fs_core* core, char const* path, ms_path_t* ms_path ) {
   // build up an ms_path from the actual path
   vector<char*> path_parts;
   size_t path_len = fs_entry_split_path( path, &path_parts );
   int rc = 0;
   
   // populate ms_path with our cached entries
   struct fs_entry* fent = fs_entry_resolve_path_cls( core, path, core->ms->owner_id, core->volume, false, &rc, fs_entry_ms_path_append, ms_path );
   if( fent == NULL ) {
      
      // end of path reached prematurely?
      if( rc == -ENOENT ) {
         rc = 0;
         
         // populate the remaining path elements with empties.
         // We're trying to read directory listings that we don't know about (yet).
         // we'll do GETCHILD to fill these in
         size_t ms_path_len = ms_path->size();
         for( unsigned int i = ms_path_len; i < path_len; i++ ) {

            struct fs_entry_getattr_cls* cls = CALLOC_LIST( struct fs_entry_getattr_cls, 1 );
            struct fs_entry_getattr_cls* parent_cls = (struct fs_entry_getattr_cls*)ms_path->at( ms_path->size() - 1 ).cls;

            dbprintf("add %s to %s (%s)\n", path_parts[i], parent_cls->fs_path, ms_path->at( ms_path->size() - 1 ).name );
            
            fs_entry_getattr_cls_init( cls, parent_cls->fs_path, path_parts[i], false, false );

            struct ms_path_ent path_ent;

            // only supply path name
            ms_client_make_path_ent( &path_ent, core->volume, 0, 0, 0, 0, 0, 0, 0, path_parts[i], cls );

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
   
   // debugging...
   dbprintf("ms_path size = %zu\n", ms_path->size() );
   for( unsigned int i = 0; i < ms_path->size(); i++ ) {
      struct fs_entry_getattr_cls* cls = (struct fs_entry_getattr_cls*)ms_path->at(i).cls;
      dbprintf("ms_path[%d] = %s, stale = %d, exists = %d\n", i, ms_path->at(i).name, cls->stale, cls->exists );
   }

   return rc;
}

// look up an ms_path_ent in an ms_path, by file_id
// return the (non-negative) index into the path if found.
// return -ENOENT if not found
static int fs_entry_path_find( ms_path_t* ms_path, uint64_t file_id ) {

   // find this directory along the path
   unsigned int i = 0;
   for( i = 0; i < ms_path->size(); i++ ) {
      if( ms_path->at(i).file_id == file_id ) {
         break;
      }
   }

   if( i == ms_path->size() ) {
      // not found, so nothing to do
      return -ENOENT;
   }

   return (signed)i;
}

// generate the path to fent, using the ms_path
// return NULL if not found
static char* fs_entry_ms_path_to_string( ms_path_t* ms_path, struct fs_entry* fent ) {
   
   // what's this entry's index in ms_path?
   int fent_index = -1;
   for( unsigned int i = 0; i < ms_path->size(); i++ ) {
      if( ms_path->at(i).file_id == fent->file_id ) {
         fent_index = i;
         break;
      }
   }
   
   if( fent_index == -1 ) {
      return NULL;
   }
   
   return ms_path_to_string( ms_path, fent_index );
}


// ensure that a fent has been or is in the process of being vacuumed 
// fent must be read-locked
static int fs_entry_ensure_vacuumed( struct fs_entry_consistency_cls* consistency_cls, struct fs_entry* fent ) {
   
   if( FS_ENTRY_LOCAL( consistency_cls->core, fent ) && fent->ftype == FTYPE_FILE && !fs_entry_vacuumer_is_vacuuming( fent ) && !fs_entry_vacuumer_is_vacuumed( fent ) ) {
      
      char* fs_path = fs_entry_ms_path_to_string( consistency_cls->path, fent );
      
      if( fs_path != NULL ) {
      
         fs_entry_vacuumer_write_bg_fent( &consistency_cls->core->state->vac, fs_path, fent );
      
         free( fs_path );
      }
   }
   
   return 0;
}

// reload an inode's metadata 
// fent should be write-locked
static int fs_entry_reload_inode( struct fs_core* core, struct timespec* query_time, struct fs_entry* fent, struct md_entry* ent ) {
   
   if( ent->name == NULL ) {
      
      // nothing to load
      errorf("No data for '%s'\n", fent->name );
      return -ENODATA;
   }
   
   // xattr cache stale?
   if( fs_entry_xattr_cache_stale( fent, ent->xattr_nonce ) ) {
      fs_entry_clear_cached_xattrs( fent, ent->xattr_nonce );
   }
   
   if( !fs_entry_should_reload( core, fent, ent, query_time ) ) {
      // nothing to do
      fs_entry_mark_read_fresh( fent );
      return 0;
   }
   
   dbprintf("reload %" PRIX64 " (%s)\n", ent->file_id, ent->name );
   return fs_entry_reload( fent, ent );
}


// path visitor for revalidating a stale inode.  cls is an fs_entry_consistency_cls.
// NOTE: fent must be write-locked
static int fs_entry_reload_entry( struct fs_entry* fent, void* cls ) {
   
   // reload this particular inode
   struct fs_entry_consistency_cls* consistency_cls = (struct fs_entry_consistency_cls*)cls;
   
   ms_path_t* ms_path = consistency_cls->path;
   struct ms_path_ent* ms_ent = NULL;
   struct fs_entry_getattr_cls* getattr_cls = NULL;
   int i = 0;
   int rc = 0;

   // find this inode along the path
   i = fs_entry_path_find( ms_path, fent->file_id );
   
   if( i == -ENOENT ) {
      // we haven't reached the download inodes yet 
      return 0;
   }

   // reload from this entry
   ms_ent = &ms_path->at(i);
   getattr_cls = (struct fs_entry_getattr_cls*)ms_ent->cls;
   
   // does this entry exist on the MS?
   if( !getattr_cls->exists ) {
      
      // nope.  unlink all descendents
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
   else {
      
      // still exists.  reload metadata and start vacuuming it, if we have new data 
      if( getattr_cls->modified ) {
         
         rc = fs_entry_reload_inode( consistency_cls->core, &consistency_cls->query_time, fent, &getattr_cls->ent );
         if( rc != 0 ) {
            
            errorf("fs_entry_reload_inode( %" PRIX64 " %s ) rc = %d\n", fent->file_id, fent->name, rc );
         }
      }
      else {
         // not modified.  mark fresh
         fs_entry_mark_read_fresh( fent );
      }
      
      // if the ent is local, is a file, and needs to be vacuumed, do so.
      if( rc == 0 && FS_ENTRY_LOCAL( consistency_cls->core, fent ) && fent->ftype == FTYPE_FILE ) {
         
         dbprintf("ensure vacuumed: %" PRIX64 ", vacuuming = %d, vacuumed = %d\n", fent->file_id, fent->vacuuming, fent->vacuumed );
         
         fs_entry_ensure_vacuumed( consistency_cls, fent );
      }
   }
   
   return rc;
}


// attach an entry to a parent, but resolve conflicts:
// * if the local node was created after the query time, go with the local node 
// * otherwise, reload it with the given data (but, I can't think of a reason why this case should happen)
// return 0 on success 
// return negative if we failed to merge the data (see fs_entry_reload_inode)
// NOTE: fent must be write-locked
static int fs_entry_merge_entry( struct fs_core* core, struct timespec* query_time, struct fs_entry* fent, char const* name, struct md_entry* ent ) {

   struct fs_entry* fent_child = NULL;
   int rc = 0;
   
   // is there already an entry?
   fent_child = fs_entry_set_find_name( fent->children, name );
   if( fent_child == NULL ) {
      
      // not here!  attach it 
      fent_child = CALLOC_LIST( struct fs_entry, 1 );
      fs_entry_init_md( core, fent_child, ent );
      
      fs_entry_mark_read_fresh( fent_child );
      
      fs_entry_attach_lowlevel( core, fent, fent_child );
   }
   else {
      
      // already here!
      fs_entry_wlock( fent_child );
      
      rc = fs_entry_reload_inode( core, query_time, fent_child, ent );
      if( rc != 0 ) {
         errorf("fs_entry_reload_inode( %" PRIX64 " %s ) rc = %d\n", fent->file_id, fent_child->name, rc );
      }
      
      fs_entry_unlock( fent_child );
   }
   
   return rc;
}

// path visitor for adding downloaded metadata to the filesystem.  cls is an fs_entry_consistency_cls structure.
// in the event of a conflict:
// * if the local node was created after the query time, go with the local node 
// * otherwise, reload it with the given data (but, I can't think of a reason why this case should happen)
// return 0 on success 
// NOTE: fent must be write-locked 
static int fs_entry_attach_entry( struct fs_entry* fent, void* cls ) {
   
   struct fs_entry_consistency_cls* consistency_cls = (struct fs_entry_consistency_cls*)cls;
   
   int rc = 0;
   ms_path_t* ms_path = consistency_cls->path;
   struct ms_path_ent* ms_child = NULL;
   struct fs_entry_getattr_cls* getattr_cls = NULL;

   // is this the parent of the next item in the path?
   if( fent->file_id == consistency_cls->file_id_remote_parent ) {
      
      // yup! next path item 
      ms_child = &ms_path->at( consistency_cls->remote_path_idx );
      getattr_cls = (struct fs_entry_getattr_cls*)ms_child->cls;
      
      rc = fs_entry_merge_entry( consistency_cls->core, &consistency_cls->query_time, fent, ms_child->name, &getattr_cls->ent );
      
      if( rc != 0 ) {
      
         errorf("fs_entry_merge_entry( %" PRIX64 ".%s ) rc = %d\n", fent->file_id, ms_child->name, rc );
         consistency_cls->err = rc;
         
         rc = -EUCLEAN;
      }
      
      if( rc == 0 ) {
         
         // success!  next child
         consistency_cls->remote_path_idx++;
         consistency_cls->file_id_remote_parent = getattr_cls->ent.file_id;
      }
   }
   
   return rc;
}


// combine downloaded metadata with the corresponding ms_path data.
// mark each path entry as existing and/or changed
// return 0 on success
static int fs_entry_zip_path_listing( ms_path_t* ms_path, struct ms_client_multi_result* results ) {
   
   int rc = 0;
   
   // sanity check 
   if( ms_path->size() != results->num_ents ) {
      errorf("requested %zu entries, got %zu\n", ms_path->size(), results->num_ents );
      return -EINVAL;
   }
   
   // merge results into the path.
   for( unsigned int i = 0; i < ms_path->size(); i++ ) {
      
      uint64_t file_id = ms_path->at(i).file_id;
      struct fs_entry_getattr_cls* getattr_cls = (struct fs_entry_getattr_cls*)ms_path->at(i).cls;
      
      if( results->ents[i].error == MS_LISTING_NONE ) {
         
         // no data for this entry 
         getattr_cls->exists = false;
         continue;
      }
      else if( results->ents[i].error == MS_LISTING_NOCHANGE ) {
         
         // the data for this entry has not changed 
         getattr_cls->exists = true;
         getattr_cls->modified = false;
         continue;
      }
      else {
         
         // new data!
         // sanity check 
         if( results->ents[i].file_id != file_id ) {
            errorf("requested entry %" PRIX64 " at %d; got %" PRIX64 "\n", file_id, i, results->ents[i].file_id );
            rc = -EINVAL;
            break;
         }
         
         getattr_cls->exists = true;
         getattr_cls->modified = true;
         getattr_cls->ent = results->ents[i];
         
         // NOTE: clear this out--we've transferred ownership
         memset( &results->ents[i], 0, sizeof(struct md_entry) );
      }
   }
   
   return rc;
}


// getattr on a set of cached but stale inodes and merge their data into a path
// return 0 on success
// return -EUCLEAN if we can't merge 
// return some other negative on transfer error
static int fs_entry_path_getattr_all( struct fs_core* core, ms_path_t* to_download ) {

   int rc = 0;
   struct ms_client_multi_result results;
   
   memset( &results, 0, sizeof(struct ms_client_multi_result) );
   
   // fetch all 
   rc = ms_client_getattr_multi( core->ms, to_download, &results );
   if( rc != 0 ) {
      errorf("ms_client_getattr_multi() rc = %d\n", rc );
      return rc;
   }
   
   // pair the responses with the path
   rc = fs_entry_zip_path_listing( to_download, &results );
   ms_client_multi_result_free( &results );
   
   if( rc != 0 ) {
      errorf("fs_entry_zip_path_listing() rc = %d\n", rc );
      
      return -EUCLEAN;
   }

   // NOTE: don't free results--it got shallow-copied into to_download
   return 0;
}


// download a path of entries that we do not have locally.
// return 0 on success
// return -EUCLEAN if we failed to merge the data into the path
// return negative on protocol-level error
static int fs_entry_path_download_all( struct fs_core* core, ms_path_t* to_download ) {
   
   int rc = 0;
   int download_rc = 0;
   int failed_idx = -1;
   struct ms_client_multi_result results;
   
   memset( &results, 0, sizeof(struct ms_client_multi_result) );
   
   // fetch all 
   rc = ms_client_path_download( core->ms, to_download, NULL, NULL, &download_rc, &failed_idx );
   
   if( rc != 0 ) {
      // protocol-level error 
      errorf("ms_client_path_download() rc = %d\n", rc );
      return rc;
   }
   
   if( download_rc != 0 || failed_idx >= 0 ) {
      dbprintf("WARN: ms_client_download_path() RPC failed with rc = %d, failed path node %d\n", download_rc, failed_idx );
   }
   
   // merge what we can
   rc = fs_entry_zip_path_listing( to_download, &results );
   ms_client_multi_result_free( &results );
   
   if( rc != 0 ) {
      errorf("fs_entry_zip_path_listing() rc = %d\n", rc );
      
      return -EUCLEAN;
   }
   
   return 0;
}


// given the list of path entries that are cached locally, reload them.
// if one of them no longer exists on the MS, then unlink all of the children beneath it.
// return 0 on success 
// return negative on error.  Specifically, -ENOENT can mean that the path no longer exists on the MS
static int fs_entry_reload_cached_path_entries( struct fs_entry_consistency_cls* cls, ms_path_t* ms_stale_path ) {

   struct fs_core* core = cls->core;

   dbprintf("%s", "reload local entries\n");
   
   int rc = fs_entry_path_getattr_all( core, ms_stale_path );
   if( rc != 0 ) {
      errorf("fs_entry_path_getattr_all() rc = %d\n", rc );
      return rc;
   }

   // recover the full path for this ms_stale_path (found at the deepest node, by construction)
   struct fs_entry_getattr_cls* getattr_cls = (struct fs_entry_getattr_cls*)ms_stale_path->at( ms_stale_path->size() - 1 ).cls;
   char* deepest_path = getattr_cls->fs_path;

   // re-integrate them with the FS
   cls->path = ms_stale_path;

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


// given the list of path entries that are NOT cached locally, download them and merge them into the filesystem.
// merge partially-downloaded paths, if the whole path cannot be obtained.
// return 0 on success 
// return negative on error.
static int fs_entry_download_remote_path_entries( struct fs_entry_consistency_cls* cls, ms_path_t* ms_path ) {

   struct fs_core* core = cls->core;

   // debugging information....
   struct fs_entry_getattr_cls* deepest_getattr_cls = (struct fs_entry_getattr_cls*)ms_path->at( ms_path->size() - 1 ).cls;
   char* deepest_path = deepest_getattr_cls->fs_path;

   dbprintf("download remote entries of %s, starting at %d\n", deepest_path, cls->remote_path_idx);
   
   int rc = fs_entry_path_download_all( core, ms_path );
   if( rc != 0 ) {
      errorf("fs_entry_path_download_all(%s) rc = %d\n", deepest_path, rc );
      return rc;
   }
   
   // re-integrate downloaded data with the FS
   cls->path = ms_path;
   
   // walk the path and attach all entries
   struct fs_entry* fent = fs_entry_resolve_path_cls( core, deepest_path, core->ms->owner_id, core->volume, true, &rc, fs_entry_attach_entry, cls );
   if( fent == NULL ) {
      if( cls->err != 0 ) {
         errorf("fs_entry_attach_entry(%s) rc = %d\n", deepest_path, rc );
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
      errorf("fs_entry_attach_entry(%s) rc = %d\n", deepest_path, rc);
   }

   return rc;
}


// initialize a consistency cls (NOTE: shallow copied)
int fs_entry_consistency_cls_init( struct fs_entry_consistency_cls* consistency_cls, struct fs_core* core, ms_path_t* path ) {
   
   memset( consistency_cls, 0, sizeof(struct fs_entry_consistency_cls) );
   
   consistency_cls->core = core;
   consistency_cls->path = path;
   
   clock_gettime( CLOCK_REALTIME, &consistency_cls->query_time );        // time of revalidate start
   
   return 0;
}

// revalidate a path's metadata.
// walk down an absolute path and check to see if the directories leading to the requested entries are fresh.
// for each stale entry, re-download metadata and merge it into their respective inodes.
// for each entry not found locally, try to download metadata and attach it to the metadata hierarchy.
int fs_entry_revalidate_path( struct fs_core* core, char const* fs_path ) {
   
   // must be absolute
   if( fs_path[0] != '/' ) {
      return -EINVAL;
   }
   
   // normalize the path first
   int rc = 0;
   char* path = md_flatten_path( fs_path );
   struct fs_entry_consistency_cls consistency_cls;
   bool missing_local = false;

   // path entires to send to the MS for resolution
   ms_path_t ms_path;
   ms_path_t ms_path_stale;            // buffer to hold parts of ms_path that are stale
   
   dbprintf("Revalidate %s\n", path );
   
   fs_entry_consistency_cls_init( &consistency_cls, core, &ms_path );
   
   // build the whole path 
   rc = fs_entry_build_ms_path( core, path, &ms_path );
   if( rc != 0 ) {
      errorf("fs_entry_build_ms_path(%s) rc = %d\n", path, rc );
      free( path );
      return -EINVAL;
   }

   // isolate the stale and remote components
   for( unsigned int i = 0; i < ms_path.size(); i++ ) {
      
      struct fs_entry_getattr_cls* getattr_cls = (struct fs_entry_getattr_cls*)ms_path[i].cls;
      dbprintf("getattr %s\n", getattr_cls->fs_path );
      
      if( getattr_cls->stale && getattr_cls->exists ) {
         
         // stale but cached
         dbprintf("%s is local and stale\n", getattr_cls->fs_path );
         ms_path_stale.push_back( ms_path[i] );
      }

      else if( !getattr_cls->exists ) {
         
         // need to download this and all subsequent inodes
         dbprintf("%s is not local\n", getattr_cls->fs_path );
         
         missing_local = true;
         
         // remember the parent of this initial uncached inode, so we can download it and attach it at the right place
         if( i > 0 ) {
            consistency_cls.file_id_remote_parent = ms_path[i-1].file_id;
            consistency_cls.remote_path_idx = i;
         }
         else {
            consistency_cls.file_id_remote_parent = 0;
            consistency_cls.remote_path_idx = 0;
         }
         
         break;
      }
   }
   
   if( ms_path_stale.size() == 0 && !missing_local ) {
      
      // no need to contact the MS--we have everything, and it's fresh
      dbprintf("%s is complete and fresh\n", path );
      
      free( path );
      ms_client_free_path( &ms_path, fs_entry_getattr_cls_free );
      
      return 0;
   }

   if( ms_path_stale.size() > 0 ) {
      
      // reload the stale entries
      dbprintf("%zu stale entries\n", ms_path_stale.size() );
      
      // reload them all 
      rc = fs_entry_reload_cached_path_entries( &consistency_cls, &ms_path_stale );
      
      if( rc != 0 ) {
         
         errorf("fs_entry_reload_cached_path_entries(%s) rc = %d\n", path, rc );
         
         free( path );
         ms_client_free_path( &ms_path, fs_entry_getattr_cls_free );
         return rc;
      }
   }

   if( missing_local ) {
      
      // get missing path components, if all stale path components were successfully reloaded
      rc = fs_entry_download_remote_path_entries( &consistency_cls, &ms_path );
      
      if( rc != 0 ) {
         
         errorf("fs_entry_download_remote_path_entries(%s) rc = %d\n", path, rc );
         
         free( path );
         ms_client_free_path( &ms_path, fs_entry_getattr_cls_free );
         
         return rc;
      }
   }

   free( path );
   ms_client_free_path( &ms_path, fs_entry_getattr_cls_free );
   
   return rc;
}


// resolve an entry, and make sure it's a directory 
struct fs_entry* fs_entry_resolve_directory( struct fs_core* core, char const* fs_path, uint64_t owner_id, uint64_t volume_id, bool writelock, int* rc ) {
   
   struct fs_entry* parent = NULL;
   
   parent = fs_entry_resolve_path( core, fs_path, owner_id, volume_id, writelock, rc );
   if( parent == NULL || *rc != 0 ) {
      if( *rc == 0 ) {
         *rc = -ENOMEM;
      }
      
      return NULL;
   }
   
   // must be a directory
   if( parent->ftype != FTYPE_DIR ) {
      fs_entry_unlock( parent );
      *rc = -ENOTDIR;
      return NULL;
   }
   
   return parent;
}


// convert a parent's stale children to an ms_path_t, for purposes of doing a getattr_multi
// parent must be at least read-locked
static int fs_entry_getattr_stale_children_to_path( char const* parent_path, struct fs_entry* parent, ms_path_t* children ) {

   for( fs_entry_set::iterator itr = parent->children->begin(); itr != parent->children->end(); itr++ ) {
      
      struct fs_entry* child = fs_entry_set_get( &itr );
      struct ms_path_ent ms_child;
      struct fs_entry_getattr_cls* getattr_cls = NULL;
      
      if( child == NULL ) {
         continue;
      }
      
      if( child == parent ) {
         // this is the case for /
         continue;
      }
      
      fs_entry_rlock( child );
      
      // skip . and .. 
      if( strcmp(child->name, "." ) == 0 || strcmp(child->name, "..") == 0 ) {
         fs_entry_unlock( child );
         continue;
      }
      
      // is this child stale?
      if( fs_entry_is_read_stale( child ) ) {
      
         // do a getattr on it 
         memset( &ms_child, 0, sizeof(struct ms_path_ent) );
         
         // keep state for getattr 
         getattr_cls = CALLOC_LIST( struct fs_entry_getattr_cls, 1 );
         fs_entry_getattr_cls_init( getattr_cls, parent_path, child->name, true, true );
         
         ms_client_make_path_ent( &ms_child, child->volume, parent->file_id, child->file_id, child->version, child->write_nonce, child->ms_num_children, child->generation, child->ms_capacity, child->name, getattr_cls );
         
         children->push_back( ms_child );
      }
      
      fs_entry_unlock( child );
   }
   
   return 0;
}


// revalidate a directory's children, using diffdir to find the latest children and getattr_multi to revalidate the cached children 
// return 0 on success 
// return negative if getattr_multi fails, or if diffdir fails
static int fs_entry_revalidate_children_diffdir( struct fs_core* core, char const* fs_path, ms_path_t* stale_children_list,
                                                 uint64_t parent_id, int64_t parent_ms_num_children, int64_t parent_max_generation, int64_t parent_capacity,
                                                 struct ms_client_multi_result* results ) {
   
   int rc = 0;
   
   // revalidate children, if there are any
   if( stale_children_list->size() > 0 ) {
      
      dbprintf("Revalidate %zu children of %s\n", stale_children_list->size(), fs_path );
      
      rc = fs_entry_path_getattr_all( core, stale_children_list );
      
      if( rc != 0 ) {
         errorf("fs_entry_path_getattr_all(%s) rc = %d\n", fs_path, rc );
         
         return rc;
      }
      
      // what's the latest generation number?
      // it's possible that a child has been created with a later generation number than the parent
      for( ms_path_t::iterator itr = stale_children_list->begin(); itr != stale_children_list->end(); itr++ ) {
         
         struct ms_path_ent* ms_child = &(*itr);
         struct fs_entry_getattr_cls* getattr_cls = (struct fs_entry_getattr_cls*)ms_child->cls;
         
         if( !getattr_cls->exists ) {
            continue;
         }
         
         if( getattr_cls->ent.generation > parent_max_generation ) {
            parent_max_generation = getattr_cls->ent.generation;
         }
      }
   }
   
   dbprintf("Diff dir %s (ms_num_children = %" PRId64 ", l.u.g = %" PRId64 ")\n", fs_path, parent_ms_num_children, parent_max_generation + 1 );
   
   // get the difference
   rc = ms_client_diffdir( core->ms, parent_id, parent_ms_num_children, parent_max_generation + 1, results );
   
   if( rc != 0 ) {
      errorf("ms_client_diffdir(%s) rc = %d\n", fs_path, rc );
      
      return rc;
   }
   
   return rc;
}



// ensure a directory's children list is up to date.
// this does not consider the consistency of the inodes of the elements leading up to it (use fs_entry_revalidate_path for that)
int fs_entry_revalidate_children( struct fs_core* core, char const* fs_path ) {
   
   // must be absolute
   if( fs_path[0] != '/' ) {
      return -EINVAL;
   }
   
   int rc = 0;
   int worst_rc = 0;
   char* path = md_flatten_path( fs_path );
   struct fs_entry* parent = NULL;
   uint64_t parent_file_id = 0;
   uint64_t parent_ms_num_children = 0;         // MS-given number of children 
   uint64_t parent_num_children = 0;            // number of cached children (only relevant if it's empty)
   uint64_t parent_max_generation = 0;          // largest known generation
   int64_t parent_capacity = 0;                 // largest index a child can have
   struct ms_client_multi_result results;
   struct timespec query_time;
   ms_path_t stale_children_list;
   
   clock_gettime( CLOCK_REALTIME, &query_time );
   
   // find the parent, so we can get its file_id 
   parent = fs_entry_resolve_directory( core, path, SYS_USER, core->volume, false, &rc );
   if( rc != 0 ) {
      
      free( path );
      return rc;
   }
   
   parent_file_id = parent->file_id;
   parent_ms_num_children = parent->ms_num_children;
   parent_num_children = parent->children->size();
   parent_max_generation = fs_entry_set_max_generation( parent->children );
   parent_capacity = parent->ms_capacity;
   
   // only do a diffdir (instead of a listdir) if we already have cached data
   bool do_diff_dir = (parent_num_children - 2 > 0);
   
   // if we're going to do a diffdir and a getattr_multi, then get the children's metadata 
   if( do_diff_dir ) {
      
      fs_entry_getattr_stale_children_to_path( path, parent, &stale_children_list );
   }
   
   fs_entry_unlock( parent );
   
   dbprintf("%s %" PRId64 " children of %s (max cached generation %" PRId64 ")\n", (do_diff_dir ? "DIFF" : "LIST"), parent_ms_num_children, fs_path, parent_max_generation );
   
   // fetch all children (listdir), or only the new ones (diffdir)?
   if( do_diff_dir ) {
      
      // have listed before; just get the difference
      rc = fs_entry_revalidate_children_diffdir( core, path, &stale_children_list, parent_file_id, parent_ms_num_children, parent_max_generation, parent_capacity, &results );
      
      if( rc != 0 ) {
         
         errorf("fs_entry_revalidate_children_diffdir(%s) rc = %d\n", path, rc );
         
         ms_client_free_path( &stale_children_list, fs_entry_getattr_cls_free );
         free( path );
         return rc;
      }  
   }
   else {
      
      // not listed yet
      rc = ms_client_listdir( core->ms, parent_file_id, parent_ms_num_children, parent_capacity, &results );

      if( rc != 0 ) {
         
         errorf("ms_client_listdir(%s) rc = %d\n", fs_path, rc );
         return rc;
      }
   }
   
   if( results.reply_error != 0 ) {
   
      errorf("MS replied error %d\n", results.reply_error );
      
      ms_client_multi_result_free( &results );
      ms_client_free_path( &stale_children_list, fs_entry_getattr_cls_free );
      free( path );
      
      return -abs(results.reply_error);
   }
   
   // re-acquire the parent, so we can merge the children in 
   parent = fs_entry_resolve_directory( core, path, SYS_USER, core->volume, false, &rc );
   if( rc != 0 ) {
      
      ms_client_multi_result_free( &results );
      ms_client_free_path( &stale_children_list, fs_entry_getattr_cls_free );
      free( path );
      
      return rc;
   }
   
   // merge path 
   for( unsigned int i = 0; i < stale_children_list.size(); i++ ) {
      
      struct ms_path_ent* ms_child = &(stale_children_list[i]);
      struct fs_entry_getattr_cls* getattr_cls = (struct fs_entry_getattr_cls*)ms_child->cls;
      
      // does this child still exist?  if not, don't include 
      if( !getattr_cls->exists ) {
         continue;
      }
      
      // was the child remotely modified?  if not, don't reload 
      if( !getattr_cls->modified ) {
         continue;
      }
      
      // attach/merge the child.  keep trying even on error
      rc = fs_entry_merge_entry( core, &query_time, parent, getattr_cls->ent.name, &getattr_cls->ent );
      if( rc != 0 ) {
         errorf("fs_entry_merge_entry( %" PRIX64 ".%s ) rc = %d\n", parent->file_id, getattr_cls->ent.name, rc );
         worst_rc = rc;
      }
   }
   
   // merge results
   for( unsigned int i = 0; i < results.num_ents; i++ ) {
      
      // was the entry modified?  if not, don't bother 
      if( results.ents[i].error == MS_LISTING_NOCHANGE || results.ents[i].error == MS_LISTING_NONE ) {
         continue;
      }
      
      // attach/merge the child.  keep trying even on error.
      rc = fs_entry_merge_entry( core, &query_time, parent, results.ents[i].name, &results.ents[i] );
      if( rc != 0 ) {
         errorf("fs_entry_merge_entry( %" PRIX64 ".%s ) rc = %d\n", parent->file_id, results.ents[i].name, rc );
         worst_rc = rc;
      }
   }
   
   fs_entry_unlock( parent );
   
   ms_client_multi_result_free( &results );
   ms_client_free_path( &stale_children_list, fs_entry_getattr_cls_free );
   free( path );
   
   return worst_rc;
}


// reload an fs_entry's manifest-related data, initializing it
// fent must be write-locked first!
int fs_entry_reload_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg* mmsg ) {
   fent->manifest->reload( core, fent, mmsg );
   fent->size = mmsg->size();
   
   fent->mtime_sec = mmsg->fent_mtime_sec();
   fent->mtime_nsec = mmsg->fent_mtime_nsec();
   fent->version = mmsg->file_version();
   
   return 0;
}


// ensure that the manifest is up to date.
// if successful_gateway_id != NULL, then fill it with the ID of the gateway that served the manifest (if any). Otherwise set to 0 if given but the manifest was fresh.
// a manifest fetched from an AG will be marked as stale, since a subsequent read can fail with HTTP 204.  The caller should mark the manifest as fresh if it succeeds in reading data.
// fent must be write-locked
int fs_entry_revalidate_manifest_ex( struct fs_core* core, char const* fs_path, struct fs_entry* fent, int64_t mtime_sec, int32_t mtime_nsec, uint64_t* successful_gateway_id ) {
   
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
   
   if( fent->manifest == NULL ) {
      errorf("BUG: %" PRIX64 " (%s)'s manifest is not initialized\n", fent->file_id, fent->name);
      exit(1);
   }
   else {
      // does the manifest need refreshing?
      need_refresh = fs_entry_is_manifest_stale( fent );
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
   int rc = 0;
   Serialization::ManifestMsg manifest_msg;
   
   rc = fs_entry_get_manifest( core, fs_path, fent, mtime_sec, mtime_nsec, &manifest_msg, successful_gateway_id );
   if( rc != 0 ) {
      errorf("fs_entry_get_manifest(%s.%" PRId64 ".%d) rc = %d\n", fs_path, mtime_sec, mtime_nsec, rc );
      
      END_TIMING_DATA( ts, ts2, "manifest refresh (error)" );
      
      if( rc == -ENOENT || rc == -EAGAIN ) {
         // not found or try again
         return rc;
      }
      else {
         return -ENODATA;
      }
   }
   
   // repopulate the manifest and update the relevant metadata
   fs_entry_reload_manifest( core, fent, &manifest_msg );
   
   ///////////////////////////////
   char* dat = fent->manifest->serialize_str();
   dbprintf("Manifest:\n%s\n", dat);
   free( dat );
   ///////////////////////////////

   END_TIMING_DATA( ts, ts2, "manifest refresh (stale)" );
   
   return 0;
}

// ensure the manifest is fresh (but fail-fast if we can't contact the remote gateway)
// fent should be write-locked
int fs_entry_revalidate_manifest_once( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {

   if( fent->manifest == NULL ) {
      errorf("BUG: %" PRIX64 " (%s)'s manifest is not initialized\n", fent->file_id, fent->name);
      exit(1);
   }
   
   return fs_entry_revalidate_manifest_ex( core, fs_path, fent, fent->ms_manifest_mtime_sec, fent->ms_manifest_mtime_nsec, NULL );   
}


// try to fetch a manifest up to core->conf->max_read_retry times 
// fent must be write-locked 
int fs_entry_revalidate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   
   int err = 0;
   int num_manifest_requests = 0;
   
   do {
      
      err = fs_entry_revalidate_manifest_once( core, fs_path, fent );
      
      if( err != -EAGAIN ) {
         // bad error
         break;
      }
      
      // try again 
      num_manifest_requests++;
      
      dbprintf("fs_entry_revalidate_manifest_once(%s) rc = %d, attempt %d\n", fs_path, err, num_manifest_requests );
      
      struct timespec ts;
      ts.tv_sec = core->conf->retry_delay_ms / 1000;
      ts.tv_nsec = (core->conf->retry_delay_ms % 1000) * 1000000;
      
      nanosleep( &ts, NULL );
      
   } while( num_manifest_requests < core->conf->max_read_retry );  
   
   return err;
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
   rc = ms_client_coordinate( core->ms, &current_coordinator, &fent->write_nonce, &ent );
   
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


// revalidate a path and the manifest at the end of the path.
// if anywhere along the process we are told to try again (i.e. by the MS, the remote gateway, etc,), do so up to conf->max_read_retry times.
// fent should NOT be locked, but it should be open as well
int fs_entry_revalidate_metadata( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t* rg_id_ret ) {
   
   struct timespec ts, ts2;
   uint64_t rg_id = 0;
   int rc = 0;
   
   BEGIN_TIMING_DATA( ts );
   
   int num_read_retries = 0;
   
   while( true ) {
      
      // reload this path
      rc = fs_entry_revalidate_path( core, fs_path );
      if( rc != 0 ) {
         errorf("fs_entry_revalidate(%s) rc = %d\n", fs_path, rc );
         return rc;
      }
      
      fs_entry_wlock( fent );
      
      // if we're not a file, we're done 
      if( fent->ftype != FTYPE_FILE ) {
         fs_entry_unlock( fent );
         break;
      }
      
      // reload this manifest.  If we get this manifest from an RG, remember which one.
      if( fent->manifest == NULL ) {
         errorf("BUG: %" PRIX64 " (%s)'s manifest is not initialized\n", fent->file_id, fent->name);
         exit(1);
      }
      
      rc = fs_entry_revalidate_manifest_ex( core, fs_path, fent, fent->ms_manifest_mtime_sec, fent->ms_manifest_mtime_nsec, &rg_id );

      if( rc == 0 ) {
         // got it!
         break;
      }
      
      else if( rc == -ESTALE || rc == -EAGAIN ) {
         // try again 
         fs_entry_unlock( fent );
         
         // should we try again?
         num_read_retries++;
         if( num_read_retries >= core->conf->max_read_retry ) {
            errorf("Maximum download retries exceeded for manifest of %s\n", fs_path );
            return -ENODATA;
         }
         else {
            errorf("WARN: failed to download manifest of %s; trying again in at most %d milliseconds.\n", fs_path, core->conf->retry_delay_ms);
            
            struct timespec ts;
            ts.tv_sec = core->conf->retry_delay_ms / 1000;
            ts.tv_nsec = (core->conf->retry_delay_ms % 1000) * 1000000;
            
            // NOTE: don't care if interrupted
            nanosleep( &ts, NULL );
         }
         continue;
      }
      else {
         errorf("fs_entry_revalidate_manifest_ex(%s) rc = %d\n", fs_path, rc );
         fs_entry_unlock( fent );
         return rc;
      }
   }

   if( rg_id_ret != NULL ) {
      *rg_id_ret = rg_id;
   }
   
   END_TIMING_DATA( ts, ts2, "metadata latency" );
 
   fs_entry_unlock( fent );
   
   return rc;
}


