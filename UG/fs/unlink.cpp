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

#include "unlink.h"
#include "storage.h"
#include "cache.h"
#include "replication.h"

// lowlevel unlink operation--given an fs_entry and the name of an entry
// PARENT MUST BE LOCKED FIRST!
// CHILD MUST NOT BE LOCKED!
int fs_entry_detach_lowlevel( struct fs_core* core, struct fs_entry* parent, struct fs_entry* child ) {

   if( parent == child ) {
      // tried to detach .
      return -ENOTEMPTY;
   }

   if( child == NULL ) {
      // no entry found
      return -ENOENT;
   }

   fs_entry_wlock( child );

   if( child->link_count == 0 ) {
      // child is invalid
      fs_entry_unlock( child );
      return -ENOENT;
   }

   // if the child is a directory, and it's not empty, then don't proceed
   if( child->ftype == FTYPE_DIR && fs_entry_set_count( child->children ) > 2 ) {
      // not empty
      fs_entry_unlock( child );
      return -ENOTEMPTY;
   }

   // unlink
   fs_entry_set_remove( parent->children, child->name );
   
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   parent->mtime_sec = ts.tv_sec;
   parent->mtime_nsec = ts.tv_nsec;


   int rc = 0;

   if( child->open_count == 0 ) {
      
      // evict blocks, if there is a file to begin with
      if( child->ftype == FTYPE_FILE && child->file_id != 0 ) {
         rc = fs_entry_cache_evict_file( core, core->cache, child->file_id, child->version );
         if( rc == -ENOENT ) {
            // not a problem
            rc = 0;
         }
      }
      
      if( rc == 0 || rc == -ENOENT ) {
         // ENOENT allowed--it just means there's not local data
         fs_entry_destroy( child, false );
         free( child );
         child = NULL;
      }
      else {
         fs_entry_unlock( child );
      }
   }
   else {
      fs_entry_unlock( child );
   }

   if( rc == 0 && child ) {
      child->link_count = 0;
   }

   return rc;
}


// detach a file from the filesystem
// Only remove a directory if it is empty.
int fs_entry_detach( struct fs_core* core, char const* path, uint64_t user, uint64_t vol ) {

   // resolve the parent of this child (and write-lock it)
   char* path_dirname = md_dirname( path, NULL );
   char* path_basename = md_basename( path, NULL );
   int err = 0;
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, &err );
   free( path_dirname );

   if( parent == NULL ) {
      free( path_basename );
      return err;
   }
   if( parent->ftype != FTYPE_DIR ) {
      // not a directory
      fs_entry_unlock( parent );
      free( path_basename );
      return -ENOTDIR;
   }


   if( !IS_DIR_READABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // directory not searchable
      fs_entry_unlock( parent );
      free( path_basename );
      return -EACCES;
   }
   
   // is parent writeable?
   if( !IS_WRITEABLE( parent->mode, parent->owner, parent->volume, user, vol ) ) {
      // nope
      fs_entry_unlock( parent );
      free( path_basename );
      return -EACCES;
   }

   struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );
   free( path_basename );

   if( child == NULL ) {
      // doesn't exist
      fs_entry_unlock( parent );
      return -ENOENT;
   }

   int rc = fs_entry_detach_lowlevel( core, parent, child );

   fs_entry_unlock( parent );

   return rc;
}


// unlink a file from the filesystem
// pass -1 if the version is not known, or pass the known version to be unlinked
int fs_entry_versioned_unlink( struct fs_core* core, char const* path, uint64_t file_id, uint64_t coordinator_id, int64_t known_version, uint64_t owner, uint64_t volume, uint64_t gateway_id, bool check_file_id_and_coordinator_id ) {
   
   // get some info about this file first
   int rc = 0;
   int err = 0;
   
   // consistency check
   err = fs_entry_revalidate_path( core, volume, path );
   if( err != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, err );
      return -EREMOTEIO;
   }

   // get the fent...
   struct fs_entry* fent = fs_entry_resolve_path( core, path, owner, volume, true, &err );
   if( !fent || err ) {
      return err;
   }

   bool local = FS_ENTRY_LOCAL( core, fent );
   int64_t version = fent->version;
   
   if( check_file_id_and_coordinator_id ) {
      if( fent->file_id != file_id ) {
         errorf("Remote unlink to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", path, file_id, fent->file_id );
         fs_entry_unlock( fent );
         return -ESTALE;
      }
      
      if( fent->coordinator != coordinator_id ) {
         errorf("Remote unlink to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", path, coordinator_id, fent->coordinator );
         fs_entry_unlock( fent );
         return -ESTALE;
      }
   }
   
   if( known_version > 0 && fent->version > 0 && fent->version != known_version ) {
      errorf("Remote unlink to file %s version %" PRId64 ", expected %" PRId64 "\n", path, known_version, fent->version );
      fs_entry_unlock( fent );
      return -ESTALE;
   }
   
   // make sure the manifest is fresh, so we delete every block
   err = fs_entry_revalidate_manifest( core, path, fent );
   if( err != 0 ) {
      errorf( "fs_entry_revalidate_manifest(%s) rc = %d\n", path, err );
      fs_entry_unlock( fent );
      return err;
   }

   char* path_dirname = md_dirname( path, NULL );
   
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, owner, volume, true, &err );

   free( path_dirname );

   if( !parent || err ) {
      fs_entry_unlock( fent );

      return err;
   }
   
   rc = 0;
   
   // before we do anything, snapshot
   struct replica_snapshot fent_snapshot;
   fs_entry_replica_snapshot( core, fent, 0, 0, &fent_snapshot );
  
   if( !local ) {
      // this is someone else's file; tell them to unlink
      Serialization::WriteMsg* detach_request = new Serialization::WriteMsg();

      fs_entry_init_write_message( detach_request, core, Serialization::WriteMsg::DETACH );
      
      Serialization::DetachRequest* detach = detach_request->mutable_detach();
      detach->set_volume_id( fent->volume );
      detach->set_coordinator_id( fent->coordinator );
      detach->set_file_id( fent->file_id );
      detach->set_fs_path( string(path) );
      detach->set_file_version( version );

      Serialization::WriteMsg* detach_ack = new Serialization::WriteMsg();
      
      // send the write message, or coordinate
      rc = fs_entry_send_write_or_coordinate( core, path, fent, &fent_snapshot, detach_request, detach_ack );
      
      if( rc < 0 ) {
         errorf( "fs_entry_send_write_or_coordinate(%s) rc = %d\n", path, rc );
      }
      else if( rc == 0 ) {
         // successfully sent
         if( detach_ack->type() != Serialization::WriteMsg::ACCEPTED ) {
            if( detach_ack->type() == Serialization::WriteMsg::ERROR ) {
               // could not detach on the remote end
               errorf( "remote unlink error = %d (%s)\n", detach_ack->errorcode(), detach_ack->errortxt().c_str() );
               rc = detach_ack->errorcode();
            }
            else {
               // unknown message
               errorf( "remote unlink invalid message %d\n", detach_ack->type() );
               rc = -EIO;
            }
         }
      }
      else {
         // we're now the coordinator.          
         local = true;
      }

      delete detach_ack;
      delete detach_request;
   }

   if( local ) {
      // we're responsible for this file; tell the metadata server we just unlinked
      // preserve the entry information so we can issue a withdraw
      struct md_entry ent;
      fs_entry_to_md_entry( core, &ent, fent, parent->file_id, parent->name );

      rc = ms_client_delete( core->ms, &ent );
      md_entry_free( &ent );
      
      if( rc != 0 ) {
         errorf( "ms_client_delete(%s) rc = %d\n", path, rc );
         rc = -EREMOTEIO;
         
         fs_entry_unlock( fent );
         fs_entry_unlock( parent );
         return rc;
      }
      else {
         fs_entry_garbage_collect_file( core, fent );
      }
   }
   
   // detach from our FS
   fs_entry_unlock( fent );

   if( rc == 0 ) {
      rc = fs_entry_detach_lowlevel( core, parent, fent );
      if( rc != 0 ) {
         errorf( "fs_entry_detach_lowlevel rc = %d\n", rc );
      }
   }

   fs_entry_unlock( parent );

   return rc;
}
