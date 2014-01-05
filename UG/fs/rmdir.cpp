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

#include "rmdir.h"
#include "storage.h"
#include "unlink.h"

// remove a directory, if it is empty
int fs_entry_rmdir( struct fs_core* core, char const* path, uint64_t user, uint64_t volume ) {

   // get some info about this directory first
   int rc = 0;
   
   char* fpath = strdup( path );
   md_sanitize_path( fpath );
   
   // revalidate this path
   rc = fs_entry_revalidate_path( core, volume, fpath );
   if( rc != 0 && rc != -ENOENT ) {
      // consistency cannot be guaranteed
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fpath, rc );
      free( fpath );
      return rc;
   }
   
   free( fpath );

   int err = 0;
   struct fs_entry* dent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !dent || err ) {
      return err;
   }

   if( dent->ftype != FTYPE_DIR ) {
      fs_entry_unlock( dent );
      return -ENOTDIR;
   }

   char* path_dirname = md_dirname( path, NULL );

   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, volume, true, &err );

   free( path_dirname );

   if( !parent || err ) {
      fs_entry_unlock( dent );

      return err;
   }

   // IS THE PARENT EMPTY?
   if( fs_entry_set_count( dent->children ) > 2 ) {
      // nope
      fs_entry_unlock( dent );
      fs_entry_unlock( parent );

      return -ENOTEMPTY;
   }


   struct md_entry ent;
   fs_entry_to_md_entry( core, &ent, dent, parent->file_id, parent->name );

   // tell the MS that this directory should go away
   rc = ms_client_delete( core->ms, &ent );
   md_entry_free( &ent );

   if( rc != 0 ) {
      errorf( "ms_client_delete(%s) rc = %d\n", path, rc );
      rc = -EREMOTEIO;

      fs_entry_unlock( dent );
      fs_entry_unlock( parent );
   }
   else {

      fs_entry_unlock( dent );

      // detach from the filesystem 
      rc = fs_entry_detach_lowlevel( core, parent, dent, true );
      if( rc != 0 ) {
         errorf("fs_entry_detach_lowlevel(%s) rc = %d\n", path, rc );
      }
      
      fs_entry_unlock( parent );
   }
   
   return rc;
}

