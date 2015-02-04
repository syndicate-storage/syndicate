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
#include "consistency.h"
#include "storage.h"
#include "replication.h"
#include "network.h"
#include "vacuumer.h"
#include "driver.h"

// lowlevel unlink operation--given an fs_entry and the name of an entry
// parent must be write-locked!
// child must NOT be locked!
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
         rc = md_cache_evict_file( core->cache, child->file_id, child->version );
         if( rc == -ENOENT ) {
            // not a problem
            rc = 0;
         }
      }
      
      if( rc == 0 ) {
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
// return -EUCLEAN if we failed to garbage-collect, but needed to (i.e. a manifest was missing)
// return -EREMOTEIO for failure to revalidate metadata 
// return -ESTALE if the given information is out of date
int fs_entry_versioned_unlink( struct fs_core* core, char const* path, uint64_t file_id, uint64_t coordinator_id, int64_t known_version, uint64_t owner, uint64_t volume, uint64_t gateway_id, 
                               bool check_file_id_and_coordinator_id ) {
   
   // can't modify state if anonymous
   if( core->gateway == GATEWAY_ANON ) {
      SG_error("%s", "Writing is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
   // get some info about this file first
   int rc = 0;
   int err = 0;
   bool no_manifest = false;
   
   // consistency check
   err = fs_entry_revalidate_path( core, path );
   if( err != 0 ) {
      SG_error("fs_entry_revalidate_path(%s) rc = %d\n", path, err );
      
      if( err == -ENOENT )
         return -ENOENT;
      
      return -EREMOTEIO;
   }
   
   // look up the parent
   char* path_dirname = md_dirname( path, NULL );
   char* path_basename = md_basename( path, NULL );
   
   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, owner, volume, true, &err );

   free( path_dirname );

   if( !parent || err ) {

      free( path_basename );
      return err;
   }
   
   if( parent->ftype != FTYPE_DIR ) {
      fs_entry_unlock( parent );
      free( path_basename );
      return err;
   }
   
   // get the child
   struct fs_entry* fent = fs_entry_set_find_name( parent->children, path_basename );
   
   free( path_basename );
   
   if( fent == NULL ) {
      fs_entry_unlock( fent );
      fs_entry_unlock( parent );
      return -ENOENT;
   }
   
   fs_entry_wlock( fent );
   
   bool local = FS_ENTRY_LOCAL( core, fent );
   int64_t version = fent->version;
   
   if( check_file_id_and_coordinator_id ) {
      if( fent->file_id != file_id ) {
         SG_error("Remote unlink to file %s ID %" PRIX64 ", expected %" PRIX64 "\n", path, file_id, fent->file_id );
         fs_entry_unlock( fent );
         fs_entry_unlock( parent );
         return -ESTALE;
      }
      
      if( fent->coordinator != coordinator_id ) {
         SG_error("Remote unlink to file %s coordinator %" PRIu64 ", expected %" PRIu64 "\n", path, coordinator_id, fent->coordinator );
         fs_entry_unlock( fent );
         fs_entry_unlock( parent );
         return -ESTALE;
      }
   }
   
   if( known_version > 0 && fent->version > 0 && fent->version != known_version ) {
      SG_error("Remote unlink to file %s version %" PRId64 ", expected %" PRId64 "\n", path, known_version, fent->version );
      fs_entry_unlock( fent );
      fs_entry_unlock( parent );
      return -ESTALE;
   }
   
   // make sure the manifest is fresh, so we delete every block
   // only need to worry about this if file has > 0 size
   if( fent->size > 0 ) {
      
      // try to get it
      err = fs_entry_revalidate_manifest( core, path, fent );
            
      if( err != 0 ) {
         SG_error( "fs_entry_revalidate_manifest(%s) rc = %d\n", path, err );
         
         if( err == -ENOENT ) {
            // continue without a manifest 
            no_manifest = true;
            SG_error("WARN: no manifest found for %s %" PRIX64 ".  Assuming data is already vacuumed.\n", path, fent->file_id );
         }
         else {
            // some other problem
            fs_entry_unlock( fent );
            fs_entry_unlock( parent );
            return err;
         }
      }
   }
   
   // tell the driver we're deleting 
   int driver_rc = driver_delete_file( core, core->closure, path, fent );
   if( driver_rc != 0 ) {
      SG_error("driver_delete_file(%s %" PRIX64 ") rc = %d\n", path, fent->file_id, driver_rc );
      
      fs_entry_unlock( fent );
      fs_entry_unlock( parent );
      return driver_rc;
   }
   
   rc = 0;
   
   if( !local ) {
      // this is someone else's file; tell them to unlink
      Serialization::WriteMsg* detach_request = new Serialization::WriteMsg();

      fs_entry_init_write_message( detach_request, core, Serialization::WriteMsg::DETACH );
      
      fs_entry_prepare_detach_message( detach_request, path, fent, version );

      Serialization::WriteMsg* detach_ack = new Serialization::WriteMsg();
      
      // send the write message, or become the coordinator
      rc = fs_entry_send_write_or_coordinate( core, path, fent, detach_request, detach_ack );
      
      if( rc < 0 ) {
         SG_error( "fs_entry_send_write_or_coordinate(%s) rc = %d\n", path, rc );
      }
      else if( rc == 0 ) {
         // successfully sent
         if( detach_ack->type() != Serialization::WriteMsg::ACCEPTED ) {
            if( detach_ack->type() == Serialization::WriteMsg::ERROR ) {
               // could not detach on the remote end
               SG_error( "remote unlink error = %d (%s)\n", detach_ack->errorcode(), detach_ack->errortxt().c_str() );
               rc = detach_ack->errorcode();
            }
            else {
               // unknown message
               SG_error( "remote unlink invalid message %d\n", detach_ack->type() );
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
      // we're responsible for this file
      // mark the file as deleted, so it won't show up again in any listing 
      fent->deletion_in_progress = true;
      
      // safe to unlock parent--it won't be empty (in a rmdir-able sense) until fent is fully garbage-collected, but fent won't be listed either
      fs_entry_unlock( parent );
      
      // garbage-collect, then unlink on the MS.  Loop this until we succeed in unlinking on the MS (which can only happen 
      // once all of fent's data has been garbage-collected).
      while( true ) {
         
         if( !no_manifest ) {
            // if we got the latest manifest, garbage-collect all writes on the file 
            rc = fs_entry_vacuumer_file( core, path, fent );
            
            if( rc != 0 ) {
               SG_error("fs_entry_vacuumer_vacuum_file( %s %" PRIX64 " ) rc = %d\n", path, fent->file_id, rc );
               
               // failed to garbage-collect...need to un-delete fent
               fent->deletion_in_progress = false;
               fs_entry_unlock( fent );
               
               return -EREMOTEIO;
            }
         }
         
         // tell the metadata server we just unlinked
         // preserve the entry information so we can issue a deletion
         struct md_entry ent;
         fs_entry_to_md_entry( core, &ent, fent, parent->file_id, parent->name );

         rc = ms_client_delete( core->ms, &ent );
         md_entry_free( &ent );
            
         if( rc != 0 ) {
            SG_error( "ms_client_delete(%s) rc = %d\n", path, rc );
            
            if( rc == -EAGAIN ) {
               if( !no_manifest ) {
                  // try vacuuming again--some write got added in between our garbage-collection and our unlink request
                  rc = 0;
                  continue;
               }
               else {
                  // there are un-garbage-collected writes, but we have no manifest, so we can't vacuum in order to proceed with the delete.
                  SG_error("MEMORY LEAK DETECTED: No manifest for %" PRIX64 " available; unable to vacuum!\n", fent->file_id );
                  
                  // failed to garbage-collect...need to un-delete fent
                  fent->deletion_in_progress = false;
                  fs_entry_unlock( fent );
                  
                  return -EUCLEAN;
               }
            }
            else {
               
               // something more serious 
               rc = -EREMOTEIO;
               
               fent->deletion_in_progress = false;
               fs_entry_unlock( fent );
               return rc;
            }
         }
         else {
            // success!
            break;
         }
      }
      
      // re-lock the parent--it's guaranteed to exist, since it's not empty 
      fs_entry_wlock( parent );
      
      // unlock fent--we're done with it 
      fs_entry_unlock( fent );
      
      // detatch fent from parent
      rc = fs_entry_detach_lowlevel( core, parent, fent );
      if( rc != 0 ) {
         SG_error("fs_entry_detach_lowlevel(%" PRIX64 ") rc = %d\n", fent->file_id, rc );
         fs_entry_unlock( parent );
         
         return rc;
      }
      
      fs_entry_unlock( parent );
   }
   
   return rc;
}


// unlink for client library consumption 
int fs_entry_unlink( struct fs_core* core, char const* path, uint64_t owner, uint64_t volume ) {
   return fs_entry_versioned_unlink( core, path, 0, 0, -1, owner, volume, core->gateway, false );
}
