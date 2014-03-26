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

#include "rename.h"
#include "url.h"
#include "network.h"
#include "replication.h"
#include "unlink.h"

// generate an md_entry for the destination that does not (yet) exist
int fs_entry_make_dest_entry( struct fs_core* core, char const* new_path, uint64_t parent_id, struct md_entry* src, struct md_entry* dest ) {
   md_entry_dup2( src, dest );
   
   // fix up name 
   char* dest_name = md_basename( new_path, NULL );
   free( dest->name );
   dest->name = dest_name;
   
   // fix up parent name
   char* dest_dir = md_dirname( new_path, NULL );
   char* parent_name = md_basename( dest_dir, NULL );
   free( dest_dir );
   free( dest->parent_name );
   dest->parent_name = parent_name;
   
   // fix up parent ID
   dest->parent_id = parent_id;
   
   // tell the MS that file ID for dest isn't known
   dest->file_id = 0;
   
   return 0;
}

int fs_entry_rename_cleanup( struct fs_entry* fent_common_parent, struct fs_entry* fent_old_parent, struct fs_entry* fent_new_parent ) {
   if( fent_old_parent ) {
      fs_entry_unlock( fent_old_parent );
   }
   if( fent_new_parent ) {
      fs_entry_unlock( fent_new_parent );
   }
   if( fent_common_parent ) {
      fs_entry_unlock( fent_common_parent );
   }

   return 0;
}


// handle a remote rename
int fs_entry_remote_rename( struct fs_core* core, Serialization::WriteMsg* renameMsg ) {
   // verify that this fent is local
   if( core->gateway != renameMsg->rename().coordinator_id() ) {
      errorf("File %" PRIX64 " (at %s) is not local\n", renameMsg->rename().file_id(), renameMsg->rename().old_fs_path().c_str() );
      return -EINVAL;
   }
   
   return fs_entry_versioned_rename( core, renameMsg->rename().old_fs_path().c_str(), renameMsg->rename().new_fs_path().c_str(), renameMsg->user_id(), renameMsg->volume_id(), renameMsg->rename().file_version() );
}


// check that we aren't trying to move a directory into itself
int fs_entry_verify_no_loop( struct fs_entry* fent, void* cls ) {
   set<uint64_t>* file_ids = (set<uint64_t>*)cls;
   
   if( file_ids->count( fent->file_id ) > 0 ) {
      // encountered this file ID before...
      errorf("File /%" PRIu64 "/%" PRIX64 " would occur twice\n", fent->volume, fent->file_id );
      return -EINVAL;
   }
   
   file_ids->insert( fent->file_id );
   
   return 0;
}

// rename a file
int fs_entry_versioned_rename( struct fs_core* core, char const* old_path, char const* new_path, uint64_t user, uint64_t volume, int64_t version ) {
   
   int err_old = 0, err_new = 0, err = 0;

   int rc = 0;
   
   // consistency check
   err = fs_entry_revalidate_path( core, volume, old_path );
   if( err != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", old_path, err );
      return err;
   }

   // consistency check
   err = fs_entry_revalidate_path( core, volume, new_path );
   if( err != 0 && err != -ENOENT ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", new_path, err );
      return err;
   }

   // identify the parents of old_path and new_path
   char* old_path_dirname = md_dirname( old_path, NULL );
   char* new_path_dirname = md_dirname( new_path, NULL );

   struct fs_entry* fent_old_parent = NULL;
   struct fs_entry* fent_new_parent = NULL;
   struct fs_entry* fent_common_parent = NULL;

   // resolve the parent *lower* in the FS hierarchy first.  order matters!
   if( md_depth( old_path ) > md_depth( new_path ) ) {
      fent_old_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
      if( fent_old_parent != NULL ) {
         set<uint64_t> file_ids;
         fent_new_parent = fs_entry_resolve_path_cls( core, new_path_dirname, user, volume, true, &err_new, fs_entry_verify_no_loop, &file_ids );
      }
   }
   else if( md_depth( old_path ) < md_depth( new_path ) ) {
      set<uint64_t> file_ids;
      fent_new_parent = fs_entry_resolve_path_cls( core, new_path_dirname, user, volume, true, &err_new, fs_entry_verify_no_loop, &file_ids );
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
         set<uint64_t> file_ids;
         fent_new_parent = fs_entry_resolve_path_cls( core, new_path_dirname, user, volume, true, &err_new, fs_entry_verify_no_loop, &file_ids );
         fent_old_parent = fs_entry_resolve_path( core, old_path_dirname, user, volume, true, &err_old );
      }
   }

   free( old_path_dirname );
   free( new_path_dirname );

   if( err_new ) {
      fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
      return err_new;
   }
   
   if( err_old ) {
      fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
      return err_old;
   }
   
   // check permission errors...
   if( (fent_new_parent != NULL && (!IS_DIR_READABLE( fent_new_parent->mode, fent_new_parent->owner, fent_new_parent->volume, user, volume ) ||
                                   !IS_WRITEABLE( fent_new_parent->mode, fent_new_parent->owner, fent_new_parent->volume, user, volume ))) ||

       (fent_old_parent != NULL && (!IS_DIR_READABLE( fent_old_parent->mode, fent_old_parent->owner, fent_old_parent->volume, user, volume )
                                 || !IS_WRITEABLE( fent_old_parent->mode, fent_old_parent->owner, fent_old_parent->volume, user, volume ))) ||

       (fent_common_parent != NULL && (!IS_DIR_READABLE( fent_common_parent->mode, fent_common_parent->owner, fent_common_parent->volume, user, volume )
                                    || !IS_WRITEABLE( fent_common_parent->mode, fent_common_parent->owner, fent_common_parent->volume, user, volume ))) ) {

      fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
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
   
   // old must be the right version
   if( fent_old != NULL && version > 0 && fent_old->version != version )
      err = -ENOENT;

   // also, if we rename a file into itself, then it's okay
   if( err != 0 || fent_old == fent_new ) {
      fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
      free( new_path_basename );

      return err;
   }

   // lock the chilren
   fs_entry_wlock( fent_old );
   if( fent_new )
      fs_entry_wlock( fent_new );

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
   
   if( err != 0 ) {
      fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
      free( new_path_basename );

      return err;
   }
   
   // snapshot old fent
   struct replica_snapshot fent_old_snapshot;
   fs_entry_replica_snapshot( core, fent_old, 0, 0, &fent_old_snapshot );
   
   // serialize...
   struct md_entry old_ent, new_ent;
   uint64_t new_parent_id = 0;
   uint64_t old_parent_id = 0;
   char const* new_parent_name = NULL;
   char const* old_parent_name = NULL;
   
   memset( &old_ent, 0, sizeof(old_ent) );
   memset( &new_ent, 0, sizeof(new_ent) );
   
   if( fent_new_parent && fent_old_parent ) {
      new_parent_id = fent_new_parent->file_id;
      old_parent_id = fent_old_parent->file_id;
      new_parent_name = fent_new_parent->name;
      old_parent_name = fent_old_parent->name;
   }
   else {
      new_parent_id = fent_common_parent->file_id;
      old_parent_id = new_parent_id;
      new_parent_name = fent_common_parent->name;
      old_parent_name = new_parent_name;
   }
   
   
   fs_entry_to_md_entry( core, &old_ent, fent_old, old_parent_id, old_parent_name );
   
   if( fent_new ) {
      fs_entry_to_md_entry( core, &new_ent, fent_new, new_parent_id, new_parent_name );
   }
   else {
      fs_entry_make_dest_entry( core, new_path, new_parent_id, &old_ent, &new_ent );
   }
   
   // dealing with files?
   if( fent_old->ftype == FTYPE_FILE && (fent_new == NULL || (fent_new != NULL && fent_new->ftype == FTYPE_FILE )) ) {
      
      bool local = FS_ENTRY_LOCAL( core, fent_old );
      
      if( !local ) {
         // tell remote coordinator to rename, or become the coordinator
         Serialization::WriteMsg* rename_request = new Serialization::WriteMsg();

         fs_entry_init_write_message( rename_request, core, Serialization::WriteMsg::RENAME );
         
         Serialization::RenameMsg* rename_info = rename_request->mutable_rename();
         rename_info->set_volume_id( fent_old->volume );
         rename_info->set_coordinator_id( fent_old->coordinator );
         rename_info->set_file_id( fent_old->file_id );
         rename_info->set_file_version( version );
         rename_info->set_old_fs_path( string(old_path) );
         rename_info->set_new_fs_path( string(new_path) );
         
         Serialization::WriteMsg* ack = new Serialization::WriteMsg();
         
         rc = fs_entry_send_write_or_coordinate( core, old_path, fent_old, &fent_old_snapshot, rename_request, ack );
         if( rc < 0 ) {
            errorf( "fs_entry_post_write(%s) rc = %d\n", old_path, rc );

            err = rc;
         }
         else if( rc == 0 ) {
            // successfully sent write!
            if( ack->type() != Serialization::WriteMsg::ACCEPTED ) {
               if( ack->type() == Serialization::WriteMsg::ERROR ) {
                  // could not rename on the remote end
                  errorf( "remote rename error = %d (%s)\n", ack->errorcode(), ack->errortxt().c_str() );
                  rc = ack->errorcode();
               }
               else {
                  // unknown message
                  errorf( "remote rename invalid message %d\n", ack->type() );
                  rc = -EIO;
               }
               
               err = rc;
            }
         }
         else if( rc > 0 ) {
            local = true;
         }
         
         delete ack;
         delete rename_request;
      }
      
      if( local ) {
         // rename on the MS 
         err = ms_client_rename( core->ms, &old_ent, &new_ent );
         if( err != 0 ) {
            errorf("ms_client_rename(%s --> %s) rc = %d\n", old_ent.name, new_ent.name, err );
         }
      }
   }
   else {
      // directories.  make sure the dest, if it exists, is empty 
      if( fent_new != NULL && fs_entry_num_children( fent_new ) > 0 ) {
         // can't rename
         err = -ENOTEMPTY;
         errorf("%s is not empty\n", fent_new->name );
      }
      else {
         // do the rename on the MS
         err = ms_client_rename( core->ms, &old_ent, &new_ent );
         if( err != 0 ) {
            errorf("ms_client_rename(%s --> %s) rc = %d\n", old_ent.name, new_ent.name, err );
         }
      }
   }
   
   // update our metadata
   if( err == 0 ) {
      
      struct fs_entry* dest_parent = NULL;
      
      if( fent_common_parent ) {
         dest_parent = fent_common_parent;
         
         fs_entry_set_remove( fent_common_parent->children, fent_old->name );

         // rename this fs_entry
         free( fent_old->name );
         fent_old->name = new_path_basename;

         if( fent_new )
            fs_entry_set_remove( fent_common_parent->children, fent_new->name );

         fs_entry_set_insert( fent_common_parent->children, fent_old->name, fent_old );
      }
      else {
         dest_parent = fent_new_parent;
         
         fs_entry_set_remove( fent_old_parent->children, fent_old->name );

         // rename this fs_entry
         free( fent_old->name );
         fent_old->name = new_path_basename;

         if( fent_new )
            fs_entry_set_remove( fent_new_parent->children, fent_new->name );

         fs_entry_set_insert( fent_new_parent->children, fent_old->name, fent_old );
      }
      if( fent_new ) {
         // unlink fent_new's data
         fs_entry_garbage_collect_file( core, fent_new );
         
         fs_entry_unlock( fent_new );
         err = fs_entry_detach_lowlevel( core, dest_parent, fent_new );
         
         if( err != 0 ) {
            // technically, it's still safe to access fent_new since dest_parent is write-locked
            errorf("fs_entry_detach_lowlevel(%s from %s) rc = %d\n", fent_new->name, dest_parent->name, err );
         }
      }
   }
   
   // unlock everything
   fs_entry_rename_cleanup( fent_common_parent, fent_old_parent, fent_new_parent );
   fs_entry_unlock( fent_old );
   if( fent_new )
      fs_entry_unlock( fent_new );

   
   if( err != 0 )
      free( new_path_basename );
   
   md_entry_free( &old_ent );
   md_entry_free( &new_ent );

   return err;
}

// rename (local caller)
int fs_entry_rename( struct fs_core* core, char const* old_path, char const* new_path, uint64_t user, uint64_t volume ) {
   return fs_entry_versioned_rename( core, old_path, new_path, user, volume, -1 );
}