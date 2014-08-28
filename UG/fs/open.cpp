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
#include "cache.h"
#include "driver.h"

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

// create an entry (equivalent to open with O_CREAT|O_WRONLY|O_TRUNC
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
   int rc = fs_entry_revalidate_path( core, vol, path );
   if( rc != 0 && rc != -ENOENT ) {
      // consistency cannot be guaranteed
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      return -EREMOTEIO;
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


// mark an fs_entry as having been opened, and/or create a file
struct fs_file_handle* fs_entry_open( struct fs_core* core, char const* _path, uint64_t user, uint64_t vol, int flags, mode_t mode, int* err ) {

   // first things first: check open mode vs whether or not we're a client and/or have read-only caps 
   if( core->gateway == GATEWAY_ANON ) {
      // no authentication; we're read-only
      if( flags & (O_CREAT | O_RDWR | O_WRONLY | O_TRUNC | O_EXCL) ) {
         errorf("%s", "Opening to create, write, or truncate is forbidden for anonymous gateways\n");
         *err = -EPERM;
         return NULL;
      }
   }
      
   int rc = 0;
   
   char* path = strdup(_path);
   md_sanitize_path( path );
   
   // revalidate this path
   rc = fs_entry_revalidate_path( core, vol, path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      
      if( rc == -ENOENT ) {
         if( !(flags & O_CREAT) ) {
            free( path );
            *err = rc;
            return NULL;
         }
         else {
            // otherwise we're good
            rc = 0;
         }
      }
      else {
         // consistency cannot be guaranteed
         errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
         free( path );
         *err = -EREMOTEIO;
         return NULL;
      }
   }
   else {
      if( flags & O_CREAT ) {
         errorf("%s already exists\n", path );
         free( path );
         *err = -EEXIST;
         return NULL;
      }
   }
         
   
   // resolve the parent of this child (and write-lock it)
   char* path_dirname = md_dirname( path, NULL );
   char* path_basename = md_basename( path, NULL );
   struct fs_file_handle* ret = NULL;

   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, err );

   if( parent == NULL ) {

      free( path_basename );
      free( path_dirname );
      free( path );

      // err is set appropriately
      return NULL;
   }

   free( path_dirname );

   if( parent->ftype != FTYPE_DIR ) {
      // parent is not a directory
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      *err = -ENOTDIR;
      return NULL;
   }

   *err = 0;

   // can parent be searched?
   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // nope
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      *err = -EACCES;
      return NULL;
   }

   uint64_t parent_id = parent->file_id;
   char* parent_name = strdup( parent->name );

   // resolve the child (which may be in the process of being deleted)
   struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );
   bool created = false;

   if( flags & O_CREAT ) {
      if( child != NULL ) {
         // can't create
         fs_entry_unlock( parent );
         free( path_basename );
         free( path );
         free( parent_name );
         *err = -ENOENT;
         return NULL;
      }
      else if( !IS_WRITEABLE(parent->mode, parent->owner, parent->volume, user, vol) ) {
         // can't create
         fs_entry_unlock( parent );
         free( path_basename );
         free( path );
         free( parent_name );
         *err = -EACCES;
         return NULL;
      }
      else {
         struct timespec ts;
         clock_gettime( CLOCK_REALTIME, &ts );

         // can create--initialize the child
         child = CALLOC_LIST( struct fs_entry, 1 );

         int rc = fs_entry_init_file( core, child, path_basename, 0, fs_entry_next_file_version(), user, core->gateway, vol, mode, 0, ts.tv_sec, ts.tv_nsec, 0, 0 );

         if( rc != 0 ) {
            errorf("fs_entry_init_file(%s) rc = %d\n", path, rc );
            *err = rc;

            fs_entry_unlock( parent );
            free( path_basename );
            free( path );
            free( parent_name );
            fs_entry_destroy( child, false );
            free( child );
            
            return NULL;
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
               
               fs_entry_unlock( parent );
               free( path_basename );
               free( path );
               free( parent_name );
               
               fs_entry_destroy( child, false );
               free( child );
               
               *err = driver_rc;
               return NULL;
            }
            
            // insert it into the filesystem
            fs_entry_wlock( child );
            
            // open it
            child->open_count++;
            fs_entry_setup_working_data( core, child );
            
            fs_entry_attach_lowlevel( core, parent, child );
            created = true;
            
            fs_entry_unlock( child );
            
         }
      }
   }
   else if( child == NULL || child->deletion_in_progress ) {
      // not found
      fs_entry_unlock( parent );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -ENOENT;
      return NULL;
   }

   // now child exists.
   
   // safe to lock it so we can release the parent
   fs_entry_wlock( child );
   fs_entry_unlock( parent );

   if( child->link_count <= 0 ) {
      // only possible if we didn't just create
      // someone unlinked this child at the last minute
      // can't open
      fs_entry_unlock( child );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -ENOENT;
      return NULL;
   }

   if( child->ftype != FTYPE_FILE ) {
      // only possible if we didn't just create
      // not a file
      // can't open
      fs_entry_unlock( child );
      free( path_basename );
      free( path );
      free( parent_name );
      *err = -EISDIR;
      return NULL;
   }

   if( !created ) {

      // access control
      // check read/write status of flags, and bail on error
      if( (!(flags & O_RDWR) && !(flags & O_WRONLY))  && !IS_READABLE(child->mode, child->owner, child->volume, user, vol) ) {
         *err = -EACCES;  // not readable
      }
      else if( (flags & O_WRONLY) && !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol) ) {
         *err = -EACCES;  // not writable
      }
      else if( (flags & O_RDWR) && (!IS_READABLE(child->mode, child->owner, child->volume, user, vol) || !IS_WRITEABLE(child->mode, child->owner, child->volume, user, vol)) ) {
         *err = -EACCES;  // not readable or not writable
      }
      if( *err != 0 ) {
         // can't do I/O
         fs_entry_unlock( child );
         free( path_basename );
         free( path );
         free( parent_name );
         return NULL;
      }

      // refresh the manifest
      int rc = fs_entry_revalidate_manifest( core, path, child );
      if( rc != 0 ) {
         errorf("fs_entry_revalidate_manifest(%s) rc = %d\n", path, rc );
         fs_entry_unlock( child );
         free( path_basename );
         free( path );
         free( parent_name );
         
         *err = rc;
         if( rc != -EAGAIN )    // i.e. not due to missing a cert
            *err = -EREMOTEIO;
         
         return NULL;
      }
      
      // finish opening the child
      child->open_count++;
      
      if( child->open_count == 1 ) {
         // opened for the first time, so allocate working data
         fs_entry_setup_working_data( core, child );
      }
      
      // truncate, if needed
      if( flags & O_TRUNC ) {
         
         char const* method = NULL;
         
         if( FS_ENTRY_LOCAL( core, child ) ) {
            
            method = "fs_entry_truncate_local";
            *err = fs_entry_truncate_local( core, path, child, 0, parent_id, parent_name );
         }
         else {
            
            method = "fs_entry_truncate_remote";
            *err = fs_entry_truncate_remote( core, path, child, 0 );
            
         }
         
         if( *err < 0 ) {
            errorf("%s(%s) rc = %d\n", method, path, *err );
            
            fs_entry_unlock( child );
            free( path_basename );
            free( path );
            free( parent_name );
            
            return NULL;
         }
      }
   }

   if( created && *err == 0 ) {

      // create this file in the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
      
      // create synchronously, obtaining the child's file ID and write_nonce
      *err = ms_client_create( core->ms, &child->file_id, &child->write_nonce, &data );

      if( *err != 0 ) {
         errorf("ms_client_create(%s) rc = %d\n", _path, *err );
         *err = -EREMOTEIO;

         // revert
         child->open_count = 0;
         fs_entry_free_working_data( child );
         
         fs_entry_unlock( child );

         // NOTE: parent will still exist--we can't remove a non-empty directory
         fs_entry_wlock( parent );
         fs_entry_detach_lowlevel( core, parent, child );
         fs_entry_unlock( parent );

         child = NULL;
      }
      
      md_entry_free( &data );
   }

   if( *err == 0 ) {
      // still here--we can open the file now!
      child->atime = currentTimeSeconds();
      ret = fs_file_handle_create( core, child, path, parent_id, parent_name );
      fs_file_handle_open( ret, flags, mode );
   }
   
   if( child ) {
      // merely opened
      fs_entry_unlock( child );
   }
   
   free( path_basename );
   free( path );
   free( parent_name );
   
   return ret;
}

