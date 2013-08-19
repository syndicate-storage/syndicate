/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "consistency.h"
#include "manifest.h"
#include "url.h"
#include "link.h"
#include "unlink.h"
#include "network.h"
#include "replication.h"


// sync a file's metadata with the MS and flush replicas
int fs_entry_fsync( struct fs_core* core, struct fs_file_handle* fh ) {
   fs_file_handle_rlock( fh );
   if( fh->fent == NULL ) {
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   // flush replicas
   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   fs_entry_replicate_wait( fh );

   END_TIMING_DATA( ts, ts2, "replication" );
   
   int rc = ms_client_sync_update( core->ms, fh->volume, fh->path );
   if( rc != 0 ) {
      errorf("ms_client_sync_update(%s) rc = %d\n", fh->path, rc );

      // ENOENT allowed because the update thread could have preempted us
      if( rc == -ENOENT )
         rc = 0;
   }
   
   fs_file_handle_unlock( fh );
   return rc;
}

int fs_entry_fdatasync( struct fs_core* core, struct fs_file_handle* fh ) {
   
   // TODO:
   return -ENOSYS;
}


// is a fent stale for reads?
bool fs_entry_is_read_stale( struct fs_entry* fent ) {
   if( fent->read_stale ) {
      dbprintf("read_stale = %d\n", fent->read_stale);
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

// make a fent read stale
int fs_entry_mark_read_stale( struct fs_entry* fent ) {
   fent->read_stale = true;
   return 0;
}


// is a manifest stale?
bool fs_entry_is_manifest_stale( struct fs_entry* fent ) {

   if( fent->manifest->is_stale() ) {
      return true;
   }
   return false;
}


// reload a write-locked fs_entry with an md_entry's data, marking the manifest as stale if it is a file
int fs_entry_reload( struct fs_core* core, struct fs_entry* fent, struct md_entry* ent ) {

   if( fent->url ) {
      free( fent->url );
   }

   fent->url = strdup( ent->url );
   
   if( fent->manifest ) {
      // the manifest is only stale 
      if( fent->mtime_sec != ent->mtime_sec || fent->mtime_nsec != ent->mtime_nsec )
         fent->manifest->mark_stale();

      if( fent->version != fent->manifest->get_file_version() )
         fent->manifest->mark_stale();
   }
   
   fent->owner = ent->owner;
   fent->mode = ent->mode;
   fent->size = ent->size;
   fent->mtime_sec = ent->mtime_sec;
   fent->mtime_nsec = ent->mtime_nsec;
   fent->ctime_sec = ent->ctime_sec;
   fent->ctime_nsec = ent->ctime_nsec;
   fent->volume = ent->volume;
   fent->max_read_freshness = ent->max_read_freshness;
   fent->max_write_freshness = ent->max_write_freshness;

   // TODO: this version could have changed...need to reversion data and manifest as well
   fent->version = ent->version;
   
   clock_gettime( CLOCK_REALTIME, &fent->refresh_time );
   fent->read_stale = false;

   dbprintf("reloaded %s\n", ent->url );
   return 0;
}


// given an MS directory record and a directory, attach it
static struct fs_entry* fs_entry_attach_ms_directory( struct fs_core* core, struct fs_entry* parent, struct md_entry* ms_record ) {
   struct fs_entry* new_dir = CALLOC_LIST( struct fs_entry, 1 );
   fs_entry_init_md( core, new_dir, ms_record );

   // Make sure this is a directory we're attaching
   if( new_dir->ftype != FTYPE_DIR ) {
      // invalid MS data
      errorf("not a directory: %s\n", ms_record->path );
      fs_entry_destroy( new_dir, true );
      free( new_dir );
      return NULL;
   }
   else {
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
      errorf("not a file: %s\n", ms_record->path );
      fs_entry_destroy( new_file, true );
      return NULL;
   }
   else {
      // add the new directory; make a note to load up its children on the next opendir()
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

// given a parent fs_entry, a child fs_entry, and an MS record, reload it
static int fs_entry_replace( struct fs_core* core, struct fs_entry* parent, struct fs_entry** childptr, struct md_entry* ent ) {
   struct fs_entry* child = *childptr;

   // If the MS record is a directory but the existing entry is not, then remove it and replace it with one
   if( ent->type == MD_ENTRY_DIR && child->ftype != FTYPE_DIR ) {
      // preserve the parent's momdtimes (since unlinking the child will change them, but we want to keep them consistent with the MS since the parent was reloaded from its records).
      uint64_t parent_mtime_sec = parent->mtime_sec;
      uint32_t parent_mtime_nsec = parent->mtime_nsec;

      fs_entry_unlock( child );     // needed for fs_entry_detach_lowlevel

      int drc = fs_entry_detach_lowlevel( core, parent, child, true );
      if( drc != 0 ) {
         // can't proceed for some reason; we can't make the FS consistent since this entry could not be removed
         errorf("fs_entry_detach_lowlevel(%s) rc = %d\n", ent->path, drc );
         return -EUCLEAN;
      }
      else {
         // removed; add in a directory built from the MS record
         struct fs_entry* new_dir = fs_entry_attach_ms_directory( core, parent, ent );
         if( new_dir == NULL ) {
            // failed to attach
            // invalid MS data
            errorf("fs_entry_attach_ms_directory(%s): failed\n", ent->path );
            return -EREMOTEIO;
         }
         else {
            // successfully replaced!
            *childptr = new_dir;

            // restore the modtime
            parent->mtime_sec = parent_mtime_sec;
            parent->mtime_nsec = parent_mtime_nsec;

            // keep this locked, since the child was locked
            fs_entry_wlock( *childptr );
         }
      }
   }
   // If the MS record is a file but the existing entry is not, then remove it and its children and replace it with a file
   else if( ent->type == MD_ENTRY_FILE &&
	   child->ftype != FTYPE_FILE && 
	   child->ftype != FTYPE_FIFO) {
      int rc = fs_unlink_children( core, child->children, true );
      if( rc != 0 ) {
         // failed to remove children; FS will be inconsistent
         errorf("fs_destroy_children(%s) rc = %d\n", ent->path, rc );
         return -EUCLEAN;
      }
      else {
         // preserve the parent's momdtimes (since unlinking the child will change them, but we want to keep them consistent with the MS since the parent was reloaded from its records).
         uint64_t parent_mtime_sec = parent->mtime_sec;
         uint32_t parent_mtime_nsec = parent->mtime_nsec;

         // removed children; detach this directory
         rc = fs_entry_detach_lowlevel( core, parent, child, true );
         if( rc != 0 ) {
            // failed to detach; FS will be inconsistent
            errorf("fs_entry_detach_lowlevel(%s) rc = %d\n", ent->path, rc );
            return -EUCLEAN;
         }
         else {
            // removed; add the new file
            struct fs_entry* new_file = fs_entry_attach_ms_file( core, parent, ent );
            if( new_file == NULL ) {
               // failed to attach
               // invalid MS data
               errorf("fs_entry_attach_ms_file(%s): failed\n", ent->path );
               return -EREMOTEIO;
            }
            else {
               // successfully replaced!
               *childptr = new_file;

               // restore the modtime
               parent->mtime_sec = parent_mtime_sec;
               parent->mtime_nsec = parent_mtime_nsec;

               // keep this locked, since the child was locked
               fs_entry_wlock( *childptr );
            }
         }
      }
   }
   else {
      // types match.  Reload.
      fs_entry_reload( core, child, ent );
   }

   return 0;
}


// determine whether or not to reload an entry, given the current entry (can be NULL) and the time of the query
// fent must be at least read-locked!
static bool can_reload( struct fs_entry* fent, struct md_entry* next_ent, struct timespec* query_time ) {
   // reload a directory if the mtime has changed from what it was before (if the new mtime is known).  Reload if the mtime is not known.
   if( fent->ftype == FTYPE_DIR ) {
      if( next_ent != NULL ) {
         if(fent->mtime_sec != next_ent->mtime_sec || fent->mtime_nsec != next_ent->mtime_nsec) {
            return true;
         }
         else {
            return false;
         }
      }
      else {
         return true;
      }
   }
   
   if( URL_LOCAL( fent->url ) ) {
      // local means that only this UG controls its ctime and mtime (which both increase linearly)
      if( fent->ctime_sec > query_time->tv_sec || (fent->ctime_sec == query_time->tv_sec && fent->ctime_nsec > query_time->tv_nsec) ) {
         // fent is local and was created after the query time.  Don't reload; we have potentially uncommitted changes.
         return false;
      }
      if( fent->mtime_sec > query_time->tv_sec || (fent->mtime_sec == query_time->tv_sec && fent->mtime_nsec > query_time->tv_nsec) ) {
         // fent is local and was modified after the query time.  Don't reload; we have potentially uncommitted changes
         return false;
      }

      return true;
   }
   else {
      // remote means that some other UG controls its mtime and ctime.
      // trust the values on the MS over our own.
      return true;
   }
}


// Refresh zero or more metadata entries in the given path.
// If the entry at the end of the path exists and is fresh, then do nothing.
// If the entry does not exist, or is stale, then re-synchronize every directory along the path, as well as the entry.
// If a directory along the path does not exist on the MS, then all descendents are unlinked (including the entry).
// If the entry exists on the MS, then its metadata is resync'ed.
// If the entry exists on the MS and is a directory, then its children are resync'ed as well.
//
// path must be normalized and absolute.
// return 0 on success
// return -EINVAL if the path is not absolute or not normalized
// return -EREMOTEIO if the MS returned invalid data
// return -EUCLEAN if we could not make the FS consistent with the MS
int fs_entry_revalidate_path( struct fs_core* core, uint64_t volume, char const* _path ) {
   if( _path[0] != '/' )
      return -EINVAL;

   int rc = 0;
   
   int prc = 0;
   char* path = md_normalize_url( _path, &prc );

   if( prc != 0 ) {
      // could not process
      return -EINVAL;
   }

   struct timespec ts, ts2, lastmod;

   memset( &lastmod, 0, sizeof(lastmod) );

   BEGIN_TIMING_DATA( ts );
   
   bool needs_refresh = true;
   
   // if this entry exists locally, and it is not read stale, then don't refresh
   dbprintf("check '%s'\n", path );

   struct fs_entry* child = fs_entry_resolve_path( core, path, SYS_USER, volume, false, &rc );
   if( child != NULL ) {
      needs_refresh = fs_entry_is_read_stale( child );
      fs_entry_unlock( child );
   }

   if( !needs_refresh ) {
      // good for now
      dbprintf("fresh; no need to synchronize '%s'\n", path );
      free( path );

      END_TIMING_DATA( ts, ts2, "MS revalidate" );
      return 0;
   }


   dbprintf("begin revalidate '%s'\n", path );
   
   // see if there are any stale entries
   // make the strtok'ed path end in /, so we get the last token
   char* path_strtok = CALLOC_LIST( char, strlen(path) + 2 );
   strcpy( path_strtok, path );
   if( strcmp( path_strtok, "/" ) != 0 )
      strcat( path_strtok, "/" );
   
   char* tmp = NULL;
   char* next = NULL;
   char* path_ptr = path_strtok;

   struct fs_entry* cur_ent = core->root;
   struct fs_entry* next_ent = NULL;

   fs_entry_rlock( cur_ent );

   while( true ) {

      // if this entry is stale, we'll refresh
      if( fs_entry_is_read_stale( cur_ent ) ) {
         needs_refresh = true;
         dbprintf("stale: '%s'\n", cur_ent->name );
      }
      
      next = strtok_r( path_ptr, "/", &tmp );
      path_ptr = NULL;

      if( next == NULL ) {
         // out of path
         fs_entry_unlock( cur_ent );
         break;
      }
      
      // find the next entry
      next_ent = fs_entry_set_find_name( cur_ent->children, next );
      if( next_ent == NULL ) {
         // not found; check the MS 
         needs_refresh = true;
         dbprintf("not found locally: '%s'\n", next );
         fs_entry_unlock( cur_ent );
         break;
      }
      else {
         if( cur_ent->mtime_sec > lastmod.tv_sec || (cur_ent->mtime_sec == lastmod.tv_sec && cur_ent->mtime_nsec > lastmod.tv_nsec) ) {
            lastmod.tv_sec = cur_ent->mtime_sec;
            lastmod.tv_nsec = cur_ent->mtime_nsec;
         }
         
         fs_entry_rlock( next_ent );
         fs_entry_unlock( cur_ent );
         cur_ent = next_ent;
      }
   }
   
   free( path_strtok );

   char* dir_path = NULL;
   
   if( needs_refresh ) {
      // refresh every path entry in this prefix
      vector<struct md_entry> path_dirs;
      vector<struct md_entry> path_ents;

      // get the current time.  Any entries created after this will be said to be newer than the entries returned by the query
      struct timespec query_time;
      clock_gettime( CLOCK_REALTIME, &query_time );

      // perform the query
      int ms_error = 0;
      rc = ms_client_resolve_path( core->ms, volume, path, &path_dirs, &path_ents, &lastmod, &ms_error );

      if( rc != 0 ) {
         // failed to read
         errorf("ms_client_resolve_path(%s) rc = %d\n", path, rc );
         dbprintf("end revalidate %s\n", path );

         END_TIMING_DATA( ts, ts2, "MS revalidate failed" );
         
         free( path );
         return rc;
      }

      // got entries
      // refresh the directories, marking them as stale so a subsequent opendir() will cause their children to be refreshed

      bool valid = true;            // MS data is valid
      bool consistent = true;       // We can make the FS consistent with the MS

      
      dir_path = md_dirname( path, NULL );
      cur_ent = core->root;
      next_ent = NULL;

      // make the strtok'ed path end in /, so we get the last token
      path_strtok = CALLOC_LIST( char, strlen(dir_path) + 2 );
      strcpy( path_strtok, dir_path );
      if( strcmp(path_strtok, "/") != 0 )
         strcat( path_strtok, "/" );

      tmp = NULL;
      next = path_strtok + 1;
      path_ptr = path_strtok;

      // walk down the path entries and merge in changes 
      unsigned int i = 0;

      // the first entry from the MS will be the root entry.
      // see if it needs refreshing
      fs_entry_wlock( cur_ent );
      
      if( path_dirs.size() > 0 ) {
         // NOTE: only mtime inequality is required for directories for them to be considered different!
         if( fs_entry_is_read_stale( cur_ent ) ||
            (cur_ent->mtime_sec != path_dirs[i].mtime_sec || cur_ent->mtime_nsec != path_dirs[i].mtime_nsec) || 
             cur_ent->size != path_dirs[i].size ) {
            
            // MS's / directory is newer.  Merge in the changes
            dbprintf("reload %s\n", cur_ent->name );
            fs_entry_reload( core, cur_ent, &path_dirs[i] );
         }
         i++;
      }

      dbprintf("path_ptr = '%s'\n", path_ptr);
      
      // process the remaining directories in the given path
      while( i <= path_dirs.size() ) {
         next = strtok_r( path_ptr, "/", &tmp );
         path_ptr = NULL;

         if( next == NULL ) {
            dbprintf("%s", "out of path\n");
            // out of path; should only happen if i == path_dirs.size()
            if( i != path_dirs.size() ) {
               // MS gave invalid data
               errorf("ms_client_get_entries: invalid MS data for %s\n", dir_path );
               valid = false;
            }

            // leave cur_ent locked
            break;
         }

         // make sure this MS entry is a directory
         if( path_dirs[i].type != MD_ENTRY_DIR ) {
            errorf("not a directory: %s\n", path_dirs[i].path );
            valid = false;
            break;
         }
         
         dbprintf("find '%s' in %s\n", next, cur_ent->name);

         // find the next (directory) entry and merge in the changes
         next_ent = fs_entry_set_find_name( cur_ent->children, next );
         if( next_ent == NULL ) {
            dbprintf("attach %s to %s\n", path_dirs[i].path, cur_ent->name );
            struct fs_entry* new_dir = fs_entry_attach_ms_directory( core, cur_ent, &path_dirs[i] );
            if( new_dir == NULL ) {
               // failed to attach.
               // invalid MS data
               errorf("ms_client_get_entries: not a directory: %s\n", path_dirs[i].path );
               valid = false;

               fs_entry_unlock( cur_ent );
               break;
            }
            
            // next directory
            fs_entry_wlock( new_dir );
            fs_entry_unlock( cur_ent );
            cur_ent = new_dir;
         }
         else {
            // Found locally.
            fs_entry_wlock( next_ent );

            dbprintf("fresh-check '%s'\n", next_ent->name );
            
            // Only reload if it existed before our query
            if( can_reload( next_ent, &path_dirs[i], &query_time ) ) {
               dbprintf("reload/replace '%s'\n", next_ent->name );
               int rrc = fs_entry_replace( core, cur_ent, &next_ent, &path_dirs[i] );
               if( rrc == -EUCLEAN ) {
                  // could not replace and keep the FS consistent with the MS
                  consistent = false;
                  fs_entry_unlock( cur_ent );
                  break;
               }
               else if( rrc == -EREMOTEIO ) {
                  // MS data is unusable
                  valid = false;
                  fs_entry_unlock( cur_ent );
                  break;
               }
            }

            // move on to the next directory
            fs_entry_unlock( cur_ent );
            cur_ent = next_ent;
         }

         i++;     // next entry
      }

      // cur_ent refers to the deepest ancestor directory that exists in the MS on the path to the requested entry
      dbprintf("cur_ent->name = '%s'\n", cur_ent->name );
      
      rc = 0;

      if( valid && consistent ) {
         // did we process every directory from / to the parent?
         if( next != NULL && strlen(next) > 0 ) {
            dbprintf("did not process '%s'\n", next );
            // we did not.
            // The FS trees beneath the last processed directory in the parent path cannot be viewed.  Remove them all locally
            int drc = fs_unlink_children( core, cur_ent->children, true );
            if( drc != 0 ) {
               // problem--FS will not be consistent with the MS
               char* dp = fs_entry_dir_path_from_public_url( core, cur_ent->url );
               errorf("fs_unlink_children(%s) rc = %d\n", dp, drc );
               free( dp );

               consistent = false;
            }
         }
      }

      free( path_strtok );

      if( rc == 0 && valid && consistent ) {
         // resolve the child
         char* child_name = md_basename( path, NULL );
         dbprintf("child_name = '%s'\n", child_name );
         
         struct md_entry* child_md = NULL;
         struct fs_entry* child_fent = NULL;

         // is the child a directory?
         bool is_dir = false;

         // is this the root directory?
         bool is_root = false;
         if( strcmp(child_name, "/") == 0 )
            is_root = true;

         // does the child exist in the metadata?
         if( path_ents.size() > 0 ) {

            for( unsigned int i = 0; i < path_ents.size(); i++ ) {
               if( strcmp( path_ents[i].path, "." ) == 0 ) {
                  is_dir = true;
                  child_md = &path_ents[i];
                  break;
               }

               char name[NAME_MAX+1];
               memset(name, 0, NAME_MAX+1);
               md_basename( path_ents[i].path, name );
               
               if( strcmp( name, child_name ) == 0 ) {
                  // not a directory, but it exists
                  child_md = &path_ents[i];
                  break;
               }
            }
         }

         // find the entry in the local metadata, creating/reloading it if necessary, and then lock it.
         child_fent = fs_entry_set_find_name( cur_ent->children, child_name );
         free( child_name );
         
         if( child_fent == NULL ) {
            if( !is_root ) {
               // the child does not exist locally.
               if( child_md != NULL ) {
                  // but it does exist in the metadata.  Attach it

                  if( is_dir ) {
                     // if the child is the directory, the metadata path will be ".".
                     // change it to the actual path
                     char* old_path = child_md->path;
                     child_md->path = path;
                     dbprintf("fs_entry_add_ms_record(parent=%s, child=%s)\n", cur_ent->name, child_md->path);
                     child_fent = fs_entry_add_ms_record( core, cur_ent, child_md );
                     child_md->path = old_path;
                  }
                  else {
                     dbprintf("fs_entry_add_ms_record(parent=%s, child=%s)\n", cur_ent->name, child_md->path);
                     child_fent = fs_entry_add_ms_record( core, cur_ent, child_md );
                  }

                  if( child_fent == NULL ) {
                     // could not add
                     errorf("fs_entry_add_ms_record(%s) failed\n", child_md->path );
                     consistent = false;
                  }
                  else {
                     fs_entry_wlock( child_fent );
                  }
               }
               else {
                  // child does not exist in the metadata either.
                  rc = 0;
               }
            }
            else {
               // / is its own parent
               child_fent = cur_ent;
            }
         }
         else {
            // the child exists locally.
            fs_entry_wlock( child_fent );
            
            if( child_md != NULL ) {
               // the child exists in the metadata as well.  Replace it.
               int rrc = 0;
               if( is_dir ) {
                  // if the child is a directory, the metadata path will be "."
                  // change it to the actual path
                  char* old_path = child_md->path;
                  child_md->path = path;

                  dbprintf("fs_entry_replace(%s)\n", child_md->path );
                  rrc = fs_entry_replace( core, cur_ent, &child_fent, child_md );

                  child_md->path = old_path;
               }
               else {
                  dbprintf("fs_entry_replace(%s)\n", child_md->path );
                  rrc = fs_entry_replace( core, cur_ent, &child_fent, child_md );
               }
               
               if( rrc != 0 ) {
                  // failed
                  if( rrc == -EREMOTEIO ) {
                     valid = false;
                     rc = rrc;
                  }
                  if( rrc == -EUCLEAN ) {
                     consistent = false;
                     rc = rrc;
                  }
               }
               else {
                  // success!
                  rc = 0;
               }
            }
            else {
               // but no metadata on the MS.  Destroy this child and all of its children if it was created before the query
               if( can_reload( child_fent, NULL, &query_time ) ) {
                  
                  // if this is a directory, remove the children first
                  if( child_fent->ftype == FTYPE_DIR ) {
                     dbprintf("fs_unlink_children(%s)\n", path );
                     int drc = fs_unlink_children( core, child_fent->children, true );
                     if( drc != 0 ) {
                        // problem--FS will not be consistent with the MS
                        errorf("fs_unlink_children(%s) rc = %d\n", path, drc );
                        consistent = false;
                     }
                  }

                  fs_entry_unlock( child_fent );

                  dbprintf("fs_entry_detach_lowlevel(parent=%s, child=%s)\n", cur_ent->name, path );
                  int drc = fs_entry_detach_lowlevel( core, cur_ent, child_fent, true );
                  if( drc != 0 ) {
                     // problem--FS will not be consistent with the MS
                     errorf("fs_entry_detach_lowlevel(%s) rc = %d\n", path, drc );
                     consistent = false;
                  }

                  child_fent = NULL;
               }
               else {
                  rc = 0;
               }
            }
         }

         // child_fent should be NULL, or write-locked
         
         // if this child is a directory (and we've had no problems reloading it), reload the children
         if( rc == 0 && consistent && valid && child_fent != NULL && path_ents.size() > 0 && is_dir ) {
            
            // process the child entries given back by the MS--merge them into the FS.
            // IF this entry was a file, there will be only one entry in path_ents.
            // Otherwise, there will be at least one.
            for( unsigned int i = 0; i < path_ents.size(); i++ ) {
               
               if( is_dir && strcmp( path_ents[i].path, "." ) == 0 ) {
                  // already reloaded
                  continue;
               }

               char name[NAME_MAX+1];
               memset(name, 0, NAME_MAX+1 );
               md_basename( path_ents[i].path, name );
               
               // does this entry exist?
               struct fs_entry* fent = fs_entry_set_find_name( child_fent->children, name );
               if( fent == NULL ) {
                  // this entry does not exist locally.  Create it.
                  dbprintf("fs_entry_add_ms_record(parent=%s, child=%s)\n", child_fent->name, path_ents[i].path );
                  
                  fent = fs_entry_add_ms_record( core, child_fent, &path_ents[i] );
                  if( fent == NULL ) {
                     // could not add
                     errorf("fs_entry_add_ms_record(%s) failed\n", path_ents[i].path );
                     consistent = false;
                     break;
                  }
               }
               else {
                  fs_entry_wlock( fent );
                  
                  // this entry exists locally.
                  // Was it created before the query time?
                  if( fent->ctime_sec < query_time.tv_sec || (fent->ctime_sec == query_time.tv_sec && fent->ctime_sec < query_time.tv_nsec) ) {
                     // replace; the MS record is newer
                     dbprintf("reload/replace %s\n", path_ents[i].path );
                     int rrc = fs_entry_replace( core, child_fent, &fent, &path_ents[i] );
                     if( rrc != 0 ) {
                        // failed to replace
                        if( rrc == -EREMOTEIO ) {
                           valid = false;
                           rc = rrc;
                        }

                        if( rrc == -EUCLEAN ) {
                           consistent = false;
                           rc = rrc;
                        }
                     }
                     else {
                        rc = 0;
                     }
                  }
                  else {
                     // created after query; it's fresh
                     rc = 0;
                  }
                  
                  fs_entry_unlock( fent );
               }
            }

            // this entry is no longer stale
            if( child_fent != NULL ) {
               child_fent->read_stale = false;
               clock_gettime( CLOCK_REALTIME, &child_fent->refresh_time );
            }
         }
         // only unlock if this is NOT the root directory
         // (otherwise, child_ent and cur_ent are the same)
         if( child_fent != NULL && !is_root )
            fs_entry_unlock( child_fent );

      }

      fs_entry_unlock( cur_ent );
      
      // free memory
      for( unsigned int i = 0; i < path_dirs.size(); i++ ) {
         md_entry_free( &path_dirs[i] );
      }

      for( unsigned int i = 0; i < path_ents.size(); i++ ) {
         md_entry_free( &path_ents[i] );
      }

      if( rc == 0 && (!valid || !consistent) ) {
         // we had some problems
         if( !valid )
            rc = -EREMOTEIO;
         if( !consistent )
            rc = -EUCLEAN;
      }
   }

   dbprintf("end revalidate '%s'\n", path );
   
   free( path );
   free( dir_path );

   END_TIMING_DATA( ts, ts2, "MS revalidate" );
   
   return rc;
}



// reload an fs_entry's manifest-related data
// fent must be write-locked first!
int fs_entry_reload_manifest( struct fs_core* core, struct fs_entry* fent, Serialization::ManifestMsg& mmsg ) {
   fent->manifest->reload( core, fent, mmsg );
   fent->size = mmsg.size();
   
   fent->mtime_sec = mmsg.mtime_sec();
   fent->mtime_nsec = mmsg.mtime_nsec();
   fent->version = mmsg.file_version();
   
   struct timespec ts;
   ts.tv_sec = mmsg.manifest_mtime_sec();
   ts.tv_nsec = mmsg.manifest_mtime_nsec();
   fent->manifest->set_lastmod( &ts );
   
   return 0;
}


// ensure that the manifest is up to date
// FENT MUST BE WRITE-LOCKED FIRST!
int fs_entry_revalidate_manifest( struct fs_core* core, char const* fs_path, struct fs_entry* fent ) {
   
   if( URL_LOCAL( fent->url ) )
      return 0;      // nothing to do--we automatically have the latest

   struct timespec ts, ts2;

   BEGIN_TIMING_DATA( ts );
   
   bool need_refresh = false;
   
   if( fent->manifest == NULL ) {
      fent->manifest = new file_manifest( core );
      need_refresh = true;
   }
   else {
      // does the manifest need refreshing?
      need_refresh = fs_entry_is_manifest_stale( fent );
   }

   if( !need_refresh ) {
      // we're good to go
      clock_gettime( CLOCK_MONOTONIC, &ts );
      END_TIMING_DATA( ts, ts2, "manifest refresh (fresh)" );
   
      return 0;
   }

   struct timespec manifest_mtime;
   fent->manifest->get_lastmod( &manifest_mtime );
   
   // otherwise, we need to refresh.  GoGoGo!
   char* manifest_url = fs_entry_remote_manifest_url( core, fs_path, fent->url, fent->version, &manifest_mtime );
   
   // try the primary, then the replicas
   Serialization::ManifestMsg manifest_msg;
   int rc = fs_entry_download_manifest( core, manifest_url, &manifest_msg );
   if( rc < 0 ) {
      char** RG_urls = ms_client_RG_urls_copy( core->ms, core->volume );
      
      // try each replica
      if( RG_urls ) {

         for( int i = 0; RG_urls[i] != NULL; i++ ) {
            free( manifest_url );

            // next replica
            manifest_url = fs_entry_remote_manifest_url( core, fs_path, RG_urls[i], fent->version, &manifest_mtime );
            
            rc = fs_entry_download_manifest( core, manifest_url, &manifest_msg );

            // success?
            if( rc == 0 )
               break;
         }

         FREE_LIST( RG_urls );
      }
      
      if( rc < 0 ) {
         errorf("fs_entry_download_manifest(%s) rc = %d\n", manifest_url, rc );
      }
   }

   free( manifest_url );

   if( rc < 0 )
      return rc;

   
   // repopulate the manifest and update the relevant metadata
   fs_entry_reload_manifest( core, fent, manifest_msg );

   char* dat = fent->manifest->serialize_str();
   dbprintf("Manifest:\n%s\n", dat);
   free( dat );

   END_TIMING_DATA( ts, ts2, "manifest refresh (stale)" );
   
   return 0;
}
