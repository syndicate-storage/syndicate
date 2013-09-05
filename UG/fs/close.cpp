/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "close.h"
#include "consistency.h"

// close a file handle.
// NOTE: make sure everything's locked first!
int fs_file_handle_close( struct fs_file_handle* fh ) {
   fh->open_count--;
   return 0;
}


// mark an fs_entry as having been closed
int fs_entry_close( struct fs_core* core, struct fs_file_handle* fh ) {
   fs_file_handle_wlock( fh );

   int rc = 0;

   bool sync = false;

   fs_entry_wlock( fh->fent );

   fs_file_handle_close( fh );

   if( fh->open_count <= 0 ) {
      // all references to this handle have gone.
      // decrement reference count on the fs_entry itself
      fh->fent->open_count--;

      if( fh->fent->link_count <= 0 && fh->fent->open_count <= 0 ) {
         // file is unlinked and no one is manipulating it--safe to destroy
         fs_entry_destroy( fh->fent, false );
         free( fh->fent );
      }
      else {
         sync = true;
      }
   }
   
   else {
      sync = true;
   }

   if( sync ) {
      if( fh->dirty ) {

         char* path = fh->path;
         char* parent_name = fh->parent_name;
         
         fh->path = NULL;
         fh->parent_name = NULL;

         fs_entry_unlock( fh->fent );

         if( fh->open_count <= 0 )
            fs_file_handle_destroy( fh );
         else
            fs_file_handle_unlock( fh );

         // synchronize outstanding updates
         rc = ms_client_sync_update( core->ms, fh->volume, fh->file_id );

         if( path )
            free( path );

         if( parent_name )
            free( parent_name );

         if( rc != 0 && rc != -ENOENT ) {
            errorf("ms_client_sync_update rc = %d\n", rc );
            rc = -EIO;
         }
      }
      else {
         fs_entry_unlock( fh->fent );

         if( fh->open_count <= 0 )
            fs_file_handle_destroy( fh );
         else
            fs_file_handle_unlock( fh );
      }
   }

   return rc;
}

