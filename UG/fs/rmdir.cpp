/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "rmdir.h"
#include "storage.h"
#include "unlink.h"

// remove a directory, if it is empty
int fs_entry_rmdir( struct fs_core* core, char const* path, uid_t user, gid_t volume ) {

   // get some info about this directory first
   int rc = 0;

   int err = 0;
   struct fs_entry* dent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !dent || err ) {
      return err;
   }

   if( dent->ftype != FTYPE_DIR ) {
      fs_entry_unlock( dent );
      return -ENOTDIR;
   }

   struct md_entry ent;
   fs_entry_to_md_entry( core, path, dent, &ent );

   char* path_dirname = md_dirname( path, NULL );

   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, volume, true, &err );

   free( path_dirname );

   if( !parent || err ) {
      fs_entry_unlock( dent );

      md_entry_free( &ent );

      return err;
   }

   // IS THE PARENT EMPTY?
   if( fs_entry_set_count( dent->children ) > 2 ) {
      // nope
      fs_entry_unlock( dent );
      fs_entry_unlock( parent );

      md_entry_free( &ent );

      return -ENOTEMPTY;
   }

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
      
      // remove the directory
      fs_entry_remove_local_directory( core, path );

      fs_entry_unlock( parent );
   }
   
   return rc;
}

