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

#include "open.h"
#include "link.h"
#include "manifest.h"
#include "network.h"
#include "unlink.h"
#include "url.h"
#include "driver.h"
#include "trunc.h"

// create a file handle from an fs_entry
struct fs_file_handle* fs_file_handle_create( struct fs_core* core, struct fs_entry* ent, char const* opened_path, uint64_t parent_id, char const* parent_name ) {
   struct fs_file_handle* fh = CALLOC_LIST( struct fs_file_handle, 1 );
   fh->flags = 0;
   fh->open_count = 0;
   fh->fent = ent;
   fh->volume = ent->volume;
   fh->file_id = ent->file_id;
   fh->path = strdup( opened_path );
   fh->parent_name = strdup( parent_name );
   fh->parent_id = parent_id;
   fh->transfer_timeout_ms = (core->conf->transfer_timeout) * 1000L;
   fh->dirty = false;

   int gateway_type = ms_client_get_gateway_type( core->ms, ent->coordinator );
   if( gateway_type == SYNDICATE_AG ) {
      fh->is_AG = true;
   }
   
   pthread_rwlock_init( &fh->lock, NULL );
   
   return fh;
}


// open a file handle
// NOTE: make sure everything's locked first!
int fs_file_handle_open( struct fs_file_handle* fh, int flags, mode_t mode ) {
   // is this a local file?
   fh->flags = flags;
   fh->open_count++;
   return 0;
}

// create an entry, re-trying on -EAGAIN from fs_entry_create_once
struct fs_file_handle* fs_entry_create( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, mode_t mode, int* err ) {
   dbprintf( "create %s\n", path );
   return fs_entry_open( core, path, user, vol, O_CREAT|O_WRONLY|O_TRUNC, mode, err );
}

// make a node (regular files only at this time)
int fs_entry_mknod( struct fs_core* core, char const* path, mode_t mode, dev_t dev, uint64_t user, uint64_t vol ) {
   // only regular files at this time...
   if( ! ( S_ISREG( mode ) || S_ISFIFO( mode ) ) ) {
      return -ENOTSUP;
   }

   // revalidate this path
   int rc = fs_entry_revalidate_path( core, path );
   if( rc != 0 && rc != -ENOENT ) {
      // consistency cannot be guaranteed
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      return rc;
   }
   
   int err = 0;

   // get the parent directory and lock it
   char* path_dirname = md_dirname( path, NULL );
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, &err );
   free( path_dirname );

   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // not searchable
      fs_entry_unlock( parent );
      return -EACCES;
   }

   if( !IS_WRITEABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // not writeable
      fs_entry_unlock( parent );
      return -EACCES;
   }

   uint64_t parent_id = parent->file_id;
   char* parent_name = strdup( parent->name );

   char* path_basename = md_basename( path, NULL );

   // make sure it doesn't exist already (or isn't in the process of being deleted, since we might have to re-create it if deleting it fails)
   if( fs_entry_set_find_name( parent->children, path_basename ) != NULL ) {
      free( path_basename );
      fs_entry_unlock( parent );
      free( parent_name );
      return -EEXIST;
   }

   struct fs_entry* child = (struct fs_entry*)calloc( sizeof(struct fs_entry), 1 );

   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   int mmode = 0;
   if (S_ISFIFO(mode)) {
       mmode = ( mode & 0777 ) | S_IFIFO;
       err = fs_entry_init_fifo( core, child, path_basename, 0, fs_entry_next_file_version(), user, core->gateway, vol, mmode, 0, ts.tv_sec, ts.tv_nsec, 0, 0 );
   }
   if (S_ISREG(mode)) {
       mmode = ( mode & 0777 );
       err = fs_entry_init_file( core, child, path_basename, 0, fs_entry_next_file_version(), user, core->gateway, vol, mmode, 0, ts.tv_sec, ts.tv_nsec, 0, 0 );
   }

   if( err == 0 ) {
      
      // mark it as created in this session 
      child->created_in_session = true;
      
      // we're creating, so this manifest is initialized (to zero blocks)
      child->manifest->initialize_empty( child->version );
      
      fs_entry_wlock( child );
      
      // call the driver 
      err = driver_create_file( core, core->closure, path, child );
         
      if( err != 0 ) {
         // undo 
         errorf("driver_create_file(%s) rc = %d\n", path, err );
         
         child->open_count = 0;
         
         fs_entry_unlock( child );
         fs_entry_destroy( child, false );
         free( child );
      }
      
      else {
         
         // attach the file
         fs_entry_attach_lowlevel( core, parent, child );

         struct md_entry data;
         fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
         
         // create the child on the MS, obtaining its file ID and write nonce
         err = ms_client_create( core->ms, &child->file_id, &child->write_nonce, &data );

         md_entry_free( &data );
         
         if( err != 0 ) {
            errorf( "ms_client_create(%s) rc = %d\n", path, err );
            err = -EREMOTEIO;

            child->open_count = 0;
            fs_entry_unlock( child );
            fs_entry_detach_lowlevel( core, parent, child );
            free( child );
         }
         else {
            fs_entry_unlock( child );
         }
      }
   }
   
   fs_entry_unlock( parent );

   free( parent_name );
   free( path_basename );

   return err;
}


// get the parent and child nodes on create/open, checking permissions along the way 
// write-lock the parent. 
// do NOT touch the child
// if the child is not found, *child will be set to NULL
// return 0 on success
// return -ENOTDIR if a directory along the path wasn't a directory
// return -EACCES on permission error
int fs_entry_open_parent_and_child( struct fs_core* core, char const* path, uint64_t user, uint64_t vol, struct fs_entry** ret_parent, struct fs_entry** ret_child ) {
   
   // resolve the parent of this child (and write-lock it)
   int rc = 0;
   char* path_dirname = md_dirname( path, NULL );
   char* path_basename = md_basename( path, NULL );
   
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, &rc );

   if( parent == NULL ) {

      free( path_basename );
      free( path_dirname );
      
      return rc;
   }

   free( path_dirname );

   if( parent->ftype != FTYPE_DIR ) {
      // parent is not a directory
      fs_entry_unlock( parent );
      free( path_basename );
      
      return -ENOTDIR;
   }

   // can parent be searched?
   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // nope
      fs_entry_unlock( parent );
      free( path_basename );
      
      return -EACCES;
   }
   
   // resolve the child
   struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );
   
   free( path_basename );
   
   *ret_parent = parent;
   *ret_child = child;
   
   return 0;
}
   

// carry out the open (not for create)
// check permissions, increment open count, set up working data if need be
// return 0 on success
// return -EACCES on permission failure
// return -ENOENT if the child is in the process of being deleted
// return -EISDIR if the child was a directory
// child must be write-locked
int fs_entry_do_open( struct fs_core* core, char const* path, struct fs_entry* child, uint64_t user, uint64_t vol, int flags ) {

   int rc = 0;
   
   // existence
   if( child->link_count <= 0 || child->deletion_in_progress ) {
      // only possible if we didn't just create
      // someone unlinked this child at the last minute
      // can't open
      return -ENOENT;
   }

   // access control
   // check read/write status of flags, and bail on error
   if( (!(flags & O_RDWR) && !(flags & O_WRONLY))  && !IS_READABLE(child->mode, child->owner, child->volume, user, vol) ) {
      rc = -EACCES;  // not readable
   }
   else if( (flags & O_WRONLY) && !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol) ) {
      rc = -EACCES;  // not writable
   }
   else if( (flags & O_RDWR) && (!IS_READABLE(child->mode, child->owner, child->volume, user, vol) || !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol)) ) {
      rc = -EACCES;  // not readable or not writable
   }
   if( rc != 0 ) {
      // can't do I/O
      return rc;
   }
   
   // type
   if( child->ftype != FTYPE_FILE ) {
      // only possible if we didn't just create
      // not a file
      // can't open
      return -EISDIR;
   }
   
   // finish opening the child
   child->open_count++;
   
   if( child->open_count == 1 ) {
      // opened for the first time, so allocate working data
      fs_entry_setup_working_data( core, child );
   }
   
   return 0;
}


// carry out the create locally.
// check permissions, initialize the child, and add it as a child of parent.
// return the initialized child (which will NOT be locked) via *ret_child
// return 0 on success
// return -EACCES on permission failure 
// parent MUST be write locked
int fs_entry_do_create( struct fs_core* core, char const* path, struct fs_entry* parent, struct fs_entry** ret_child, uint64_t user, uint64_t vol, mode_t mode ) {
   
   int rc = 0;
   struct fs_entry* child = NULL;
   
   
   if( !IS_WRITEABLE(parent->mode, parent->owner, parent->volume, user, vol) ) {
      // can't create
      return -EACCES;
   }
   else {
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );

      // can create--initialize the child
      child = CALLOC_LIST( struct fs_entry, 1 );

      char* path_basename = md_basename( path, NULL );
      
      rc = fs_entry_init_file( core, child, path_basename, 0, fs_entry_next_file_version(), user, core->gateway, vol, mode, 0, ts.tv_sec, ts.tv_nsec, 0, 0 );
      
      free( path_basename );

      if( rc != 0 ) {
         errorf("fs_entry_init_file(%s) rc = %d\n", path, rc );

         fs_entry_destroy( child, false );
         free( child );
         
         return rc;
      }
      else {
         // mark it as created in this session 
         child->created_in_session = true;
         
         // we're creating, so this manifest is initialized (to zero blocks)
         child->manifest->initialize_empty( child->version );
         
         // run the driver 
         int driver_rc = driver_create_file( core, core->closure, path, child );
         if( driver_rc != 0 ) {
            errorf("driver_create_file(%s) rc = %d\n", path, driver_rc );
            
            fs_entry_destroy( child, false );
            free( child );
            
            return driver_rc;
         }
         
         // insert it into the filesystem
         fs_entry_wlock( child );
         
         // open it
         child->open_count++;
         fs_entry_setup_working_data( core, child );
         
         fs_entry_attach_lowlevel( core, parent, child );
         
         fs_entry_unlock( child );
         
         *ret_child = child;
      }
   }
   
   return 0;
}


// do the create remotely on the MS
// child must be write-locked
int fs_entry_do_MS_create( struct fs_core* core, char const* path, struct fs_entry* child, uint64_t parent_id, char const* parent_name ) {

   // create this file in the MS
   struct md_entry data;
   int rc = 0;
   
   fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
   
   // create synchronously, obtaining the child's file ID and write_nonce
   rc = ms_client_create( core->ms, &child->file_id, &child->write_nonce, &data );

   md_entry_free( &data );
   
   return rc;
}

// undo a local create 
// parent and child must be write-locked
// this will unlock and free the child
int fs_entry_undo_create( struct fs_core* core, char const* path, struct fs_entry* parent, struct fs_entry* child ) {

   // revert
   child->link_count--;
   child->open_count--;
   
   if( child->open_count == 0 ) {
      fs_entry_free_working_data( child );
   }
   
   fs_entry_unlock( child );
   
   // NOTE: parent will still exist--we can't remove a non-empty directory
   fs_entry_detach_lowlevel( core, parent, child );

   return 0;
}
   

// do truncate on open 
int fs_entry_open_truncate( struct fs_core* core, char const* path, struct fs_entry* child, uint64_t parent_id, char const* parent_name ) {

   char const* method = NULL;
   int rc = 0;
   
   if( FS_ENTRY_LOCAL( core, child ) ) {
      
      method = "fs_entry_truncate_local";
      rc = fs_entry_truncate_local( core, path, child, 0, parent_id, parent_name );
   }
   else {
      
      method = "fs_entry_truncate_remote";
      rc = fs_entry_truncate_remote( core, path, child, 0 );
      
   }
   
   if( rc < 0 ) {
      errorf("%s(%s) rc = %d\n", method, path, rc );
   }
   
   return rc;
}


// revalidate on create (not open)
int fs_entry_create_revalidate( struct fs_core* core, char const* path, uint64_t user, uint64_t vol ) {
  
   int rc = 0;
   char* parent_path = md_dirname( path, NULL );
   
   // see that the parent still exists
   rc = fs_entry_revalidate_path( core, parent_path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", parent_path, rc );
      
      free( parent_path );
      return rc;
   }
   
   free( parent_path );
   return rc;
}


// revalidate on open (not create)
int fs_entry_open_revalidate( struct fs_core* core, char const* path, uint64_t user, uint64_t vol ) {
   
   int rc = 0;
   struct fs_entry* fent = NULL;
   
   // see that the entry still exists
   rc = fs_entry_revalidate_path( core, path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      
      return rc;
   }
   
   // find the entry
   fent = fs_entry_resolve_path( core, path, user, vol, true, &rc );
   if( fent == NULL || rc != 0 ) {
      
      errorf("fs_entry_resolve_path(%s) rc = %d\n", path, rc );
      return rc;
   }
   
   // temporarily mark this entry as referenced, so it won't be unlinked while we revalidate
   fent->link_count++;
   
   fs_entry_unlock( fent );
   
   // revalidate the entry's path and manifest
   rc = fs_entry_revalidate_metadata( core, path, fent, NULL );
   
   fs_entry_wlock( fent );
   
   fent->link_count--;
   
   fs_entry_unlock( fent );
   
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_metadata(%s) rc = %d\n", path, rc );
   }
   
   return rc;
}

// Try to open a file, but fail-fast on error.  It behaves as close to POSIX-open as possible, with the following differences:
// * return -EREMOTEIO if the UG could not contact the MS, or if it could not obtain a fresh manifest.
// * return -EUCLEAN if the UG was unable to merge metadata from the MS into its metadata hierarchy (usually indicates a bug)
// * return a driver-specific, non-zero error code given by the driver's create_file() method
// Side-effects:
// * re-downloads and updates metadata for all entries along the path that are stale.
// * re-downloads the manifest for the i-node if it is stale.
struct fs_file_handle* fs_entry_open( struct fs_core* core, char const* _path, uint64_t user, uint64_t vol, int flags, mode_t mode, int* err ) {

   // sanity check 
   if( (flags & O_RDONLY) == 0 && (flags & O_RDWR) != 0 && (flags & O_WRONLY) != 0 ) {
      *err = -EINVAL;
      return NULL;
   }
   
   if( (flags & O_RDONLY) != 0 && (flags & O_WRONLY) != 0 ) {
      *err = -EINVAL;
      return NULL;
   }
   
   // sanity check: check open mode vs whether or not we're a client and/or have read-only caps 
   if( core->gateway == GATEWAY_ANON ) {
      // no authentication; we're read-only
      if( flags & (O_CREAT | O_RDWR | O_WRONLY | O_TRUNC | O_EXCL) ) {
         errorf("%s", "Opening to create, write, or truncate is forbidden for anonymous gateways\n");
         *err = -EPERM;
         return NULL;
      }
   }
   
   int rc = 0;
   char* parent_name = NULL;
   uint64_t parent_id = 0;
   struct fs_entry* child = NULL;
   struct fs_entry* parent = NULL;
   char const* reval_method = NULL;
   struct fs_file_handle* ret = NULL;
   
   // make sure path is sane
   char* path = strdup(_path);
   md_sanitize_path( path );
   
   // revalidate metadata 
   if( flags & O_CREAT ) {
      
      reval_method = "fs_entry_create_revalidate";
      rc = fs_entry_create_revalidate( core, path, user, vol );
   }
   else {
      
      reval_method = "fs_entry_open_revalidate";
      rc = fs_entry_open_revalidate( core, path, user, vol );
   }
   
   if( rc != 0 ) {
      
      errorf("%s(%s) rc = %d\n", reval_method, path, rc );
      
      *err = rc;
      free( path );
      return NULL;
   }
   
   // get the parent and child
   // NOTE: parent will be write-locked; child will not be
   rc = fs_entry_open_parent_and_child( core, path, user, vol, &parent, &child );
   if( rc != 0 ) {
      
      errorf("fs_entry_open_parent_and_child( %s ) rc = %d\n", path, rc );
      
      *err = rc;
      free( path );
      return NULL;
   }
   
   if( flags & O_CREAT ) {
      // creating...
      
      if( child != NULL ) {
         
         // can't create--child exists
         *err = -EEXIST;
         
         free( path );
         return NULL;
      }
      
      // carry out the local create 
      rc = fs_entry_do_create( core, path, parent, &child, user, vol, mode );
      if( rc != 0 ) {
         
         errorf("fs_entry_do_create( %s ) rc = %d\n", path, rc );
         *err = rc;
         
         free( path );
         return NULL;
      }
      
      // preserve these before unlocking, since we'll need them for the file handle
      parent_id = parent->file_id;
      parent_name = strdup( parent->name );
      
      fs_entry_wlock( child );
      fs_entry_unlock( parent );
      
      // carry out the remote create 
      rc = fs_entry_do_MS_create( core, path, child, parent_id, parent_name );
      
      if( rc != 0 ) {
         errorf("fs_entry_do_MS_create(%s) rc = %d\n", path, rc );
         
         if( rc == -EAGAIN ) {
            *err = rc;
         }
         else {
            *err = -EREMOTEIO;
         }
         
         // NOTE: parent is guaranteed to exist, since child is attached to it and is write-locked (so it can't be unlinked)
         fs_entry_wlock( parent );
         fs_entry_undo_create( core, path, parent, child );
         fs_entry_unlock( parent );
         
         child = NULL;
         
         free( path );
         free( parent_name );
         return NULL;
      }
   }
   else {
      // opening...
      
      if( child == NULL ) {
         fs_entry_unlock( parent );
         
         // can't open--child doesn't exist 
         *err = -ENOENT;
         
         free( path );
         return NULL;
      }
      
      // preserve these before unlocking, since we'll need them for the file handle
      parent_id = parent->file_id;
      parent_name = strdup( parent->name );
   
      fs_entry_wlock( child );
      fs_entry_unlock( parent );
      
      // carry out the open 
      rc = fs_entry_do_open( core, path, child, user, vol, flags );
      if( rc != 0 ) {
         
         fs_entry_unlock( child );
         
         errorf("fs_entry_do_open(%s) rc = %d\n", path, rc );
         
         *err = rc;
         
         free( path );
         free( parent_name );
         return NULL;
      }
      
      // if we're truncating, do so as well
      if( flags & O_TRUNC ) {
         rc = fs_entry_open_truncate( core, path, child, parent_id, parent_name );
         
         if( rc != 0 ) {
            
            fs_entry_unlock( child );
            
            errorf("fs_entry_open_truncate(%s) rc = %d\n", path, rc );
            
            *err = rc;
            free( path );
            free( parent_name );
            return NULL;
         }  
      }
   }
   
   // success!
   child->atime = md_current_time_seconds();
   
   // give back a file handle
   ret = fs_file_handle_create( core, child, path, parent_id, parent_name );
   fs_file_handle_open( ret, flags, mode );
   
   fs_entry_unlock( child );
   
   free( path );
   free( parent_name );
   return ret;
}

