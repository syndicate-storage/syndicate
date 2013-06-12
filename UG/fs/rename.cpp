/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "rename.h"
#include "url.h"
#include "storage.h"

// rename a file
// TODO: make atomic
int fs_entry_rename( struct fs_core* core, char const* old_path, char const* new_path, uid_t user, gid_t volume ) {
   int err_old = 0, err_new = 0, err = 0;

   int rc = 0;

   // identify the parents of old_path and new_path
   char* old_path_dirname = md_dirname( old_path, NULL );
   char* new_path_dirname = md_dirname( new_path, NULL );

   struct fs_entry* fent_old_parent = NULL;
   struct fs_entry* fent_new_parent = NULL;
   struct fs_entry* fent_common_parent = NULL;

   // resolve the parent *lower* in the FS hierarchy first.  order matters!
   if( md_depth( old_path ) > md_depth( new_path ) ) {
      fent_old_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
      fent_new_parent = fs_entry_resolve_path( core, new_path_dirname, user, volume, true, &err_new );
   }
   else if( md_depth( old_path ) < md_depth( new_path ) ) {
      fent_new_parent = fs_entry_resolve_path( core, new_path_dirname, user, volume, true, &err_new );
      fent_old_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
   }
   else {
      // do these paths have the same parent?
      if( strcmp( old_path_dirname, new_path_dirname ) == 0 ) {
         // only resolve one path
         fent_common_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
      }
      else {
         // parents are different; safe to lock both
         fent_new_parent = fs_entry_resolve_path( core, new_path_dirname, user, volume, true, &err_new );
         fent_old_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
      }
   }

   free( old_path_dirname );
   free( new_path_dirname );

   // check path resolution errors...
   if( err_new || err_old ) {
      if( fent_old_parent ) {
         fs_entry_unlock( fent_old_parent );
         err = err_old;
      }
      if( fent_new_parent ) {
         fs_entry_unlock( fent_new_parent );
         err = err_new;
      }
      if( fent_common_parent ) {
         fs_entry_unlock( fent_common_parent );
         err = err_old;
      }
      
      return err;
   }

   // check permission errors...
   if( (fent_new_parent != NULL && (!IS_DIR_READABLE( fent_new_parent->mode, fent_new_parent->owner, fent_new_parent->volume, user, volume ) ||
                                   !IS_WRITEABLE( fent_new_parent->mode, fent_new_parent->owner, fent_new_parent->volume, user, volume ))) ||

       (fent_old_parent != NULL && (!IS_DIR_READABLE( fent_old_parent->mode, fent_old_parent->owner, fent_old_parent->volume, user, volume )
                                 || !IS_WRITEABLE( fent_old_parent->mode, fent_old_parent->owner, fent_old_parent->volume, user, volume ))) ||

       (fent_common_parent != NULL && (!IS_DIR_READABLE( fent_common_parent->mode, fent_common_parent->owner, fent_common_parent->volume, user, volume )
                                    || !IS_WRITEABLE( fent_common_parent->mode, fent_common_parent->owner, fent_common_parent->volume, user, volume ))) ) {

      if( fent_old_parent ) {
         fs_entry_unlock( fent_old_parent );
      }
      if( fent_new_parent ) {
         fs_entry_unlock( fent_new_parent );
      }
      if( fent_common_parent ) {
         fs_entry_unlock( fent_common_parent );
      }

      return -EACCES;
   }

   // now, look up the children
   char* new_path_basename = md_basename( new_path, NULL );
   char* old_path_basename = md_basename( old_path, NULL );

   struct fs_entry* fent_old = NULL;
   struct fs_entry* fent_new = NULL;

   if( fent_common_parent ) {
      fent_new = fs_entry_set_find_name( fent_common_parent->children, new_path_basename );
      fent_old = fs_entry_set_find_name( fent_common_parent->children, old_path_basename );
   }
   else {
      fent_new = fs_entry_set_find_name( fent_new_parent->children, new_path_basename );
      fent_old = fs_entry_set_find_name( fent_old_parent->children, old_path_basename );
   }

   free( old_path_basename );

   // old must exist...
   err = 0;
   if( fent_old == NULL )
      err = -ENOENT;

   // also, if we rename a file into itself, then it's okay
   if( err != 0 || fent_old == fent_new ) {
      if( fent_old_parent ) {
         fs_entry_unlock( fent_old_parent );
      }
      if( fent_new_parent ) {
         fs_entry_unlock( fent_new_parent );
      }
      if( fent_common_parent ) {
         fs_entry_unlock( fent_common_parent );
      }

      free( new_path_basename );

      return err;
   }

   // lock the chilren
   fs_entry_wlock( fent_old );
   if( fent_new )
      fs_entry_wlock( fent_new );

   // don't make a directory a subdirectory of itself
   if( fent_old->ftype == FTYPE_DIR && strlen(old_path) <= strlen(new_path) && strncmp( old_path, new_path, strlen(old_path) ) == 0 )
      err = -EINVAL;

   // don't proceed if one is a directory and the other is not
   if( fent_new ) {
      if( fent_new->ftype != fent_old->ftype ) {
         if( fent_new->ftype == FTYPE_DIR ) {
            err = -EISDIR;
         }
         else {
            err = -ENOTDIR;
         }
      }
   }

   // don't rename local to remote
   if( URL_LOCAL( fent_old->url ) && fent_new != NULL && !URL_LOCAL( fent_new->url ) ) {
      err = -EINVAL;
   }

   // TODO: for now, rename remote to local will fail
   if( !URL_LOCAL( fent_old->url ) && (fent_new == NULL || URL_LOCAL( fent_new->url ) ) ) {
      err = -EINVAL;
   }


   if( err ) {
      if( fent_old_parent ) {
         fs_entry_unlock( fent_old_parent );
      }
      if( fent_new_parent ) {
         fs_entry_unlock( fent_new_parent );
      }
      if( fent_common_parent ) {
         fs_entry_unlock( fent_common_parent );
      }
      fs_entry_unlock( fent_old );
      if( fent_new )
         fs_entry_unlock( fent_new );

      free( new_path_basename );

      return err;
   }

   // create the old entry into the new path
   struct md_entry new_ent;
   fs_entry_to_md_entry( core, new_path, fent_old, &new_ent );

   struct md_entry old_ent;
   fs_entry_to_md_entry( core, old_path, fent_old, &old_ent );

   // do the rename
   // create the new entry
   rc = ms_client_create( core->ms, &new_ent );

   md_entry_free( &new_ent );
   
   if( rc != 0 ) {
      errorf( "ms_entry_create(%s) rc = %d\n", new_path, rc );
      rc = -EREMOTEIO;
   }

   err = rc;

   // success?
   if( err == 0 ) {
      if( fent_common_parent ) {
         fs_entry_set_remove( fent_common_parent->children, fent_old->name );

         // rename this fs_entry
         free( fent_old->name );
         fent_old->name = new_path_basename;

         if( fent_new )
            fs_entry_set_remove( fent_common_parent->children, fent_new->name );

         fs_entry_set_insert( fent_common_parent->children, fent_old->name, fent_old );
      }
      else {
         fs_entry_set_remove( fent_old_parent->children, fent_old->name );

         // rename this fs_entry
         free( fent_old->name );
         fent_old->name = new_path_basename;

         if( fent_new )
            fs_entry_set_remove( fent_new_parent->children, fent_new->name );

         fs_entry_set_insert( fent_new_parent->children, fent_old->name, fent_old );
      }
      if( fent_new ) {
         // unlink
         fent_new->link_count--;
         if( fent_new->link_count <= 0 ) {
            if( fent_new->open_count <= 0 ) {
               fs_entry_destroy( fent_new, false );
               fent_new = NULL;

               fs_core_fs_wlock( core );
               core->num_files--;
               fs_core_fs_unlock( core );
            }
         }
      }

      // if this file was local, then we'll need to rename the local data
      if( URL_LOCAL( fent_old->url ) ) {
         char* old_path_localdata = md_fullpath( core->conf->data_root, old_path, NULL );
         char* new_path_localdata = md_fullpath( core->conf->data_root, new_path, NULL );

         char* old_path_localdata_versioned = fs_entry_mkpath( old_path_localdata, fent_old->version );
         char* new_path_localdata_versioned = fs_entry_mkpath( new_path_localdata, fent_old->version );

         free( old_path_localdata );
         free( new_path_localdata );

         int rc = md_mkdirs( new_path_localdata_versioned );
         if( rc != 0 ) {
            err = rc;
         }
         else {
            // rename everything over
            rc = fs_entry_move_local_data( old_path_localdata_versioned, new_path_localdata_versioned );
            if( rc != 0 ) {
               errorf( "failed to move bits from %s to %s\n", old_path_localdata_versioned, new_path_localdata_versioned );
               err = rc;
            }
         }

         free( old_path_localdata_versioned );
         free( new_path_localdata_versioned );
      }

      if( err == 0 ) {
         // withdraw old file
         rc = ms_client_delete( core->ms, &old_ent );
         
         if( rc != 0 ) {
            errorf( "ms_client_delete(%s) rc = %d\n", old_path, rc );
            rc = -EREMOTEIO;
         }

         err = rc;
      }
   }
   // unlock everything
   if( fent_old_parent ) {
      fs_entry_unlock( fent_old_parent );
   }
   if( fent_new_parent ) {
      fs_entry_unlock( fent_new_parent );
   }
   if( fent_common_parent ) {
      fs_entry_unlock( fent_common_parent );
   }
   fs_entry_unlock( fent_old );
   if( fent_new )
      fs_entry_unlock( fent_new );

   md_entry_free( &old_ent );
   md_entry_free( &new_ent );

   return err;
}
