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

#include "close.h"
#include "consistency.h"
#include "replication.h"
#include "network.h"
#include "sync.h"

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
   bool free_working_data = false;

   fs_entry_wlock( fh->fent );

   fs_file_handle_close( fh );

   if( fh->open_count <= 0 ) {
      
      // this descriptor is getting closed, so sync data 
      sync = true;
      
      fh->fent->open_count--;
      
      if( fh->fent->open_count <= 0 ) {
         // blow the working data away when we're done 
         free_working_data = true;
      }

      rc = fs_entry_try_destroy( core, fh->fent );
      if( rc > 0 ) {
         // fent was unlocked and destroyed
         free( fh->fent );
         sync = false;  // don't sync data--the file no longer exists
      }
   }
   
   if( sync ) {
      if( fh->dirty ) {
         
         struct sync_context sync_ctx;
         memset( &sync_ctx, 0, sizeof(struct sync_context) );
               
         // synchronize data and metadata
         rc = fs_entry_fsync_locked( core, fh, &sync_ctx );
         
         if( rc != 0 ) {
            
            fs_entry_sync_context_free( &sync_ctx );
            fs_entry_unlock( fh->fent );
            fs_file_handle_unlock( fh );
            
            return rc;
         }
         
         fs_entry_sync_context_free( &sync_ctx );
      }
   }
   
   if( free_working_data ) {
      fs_entry_free_working_data( fh->fent );
   }

   fs_entry_unlock( fh->fent );

   if( fh->open_count <= 0 )
      fs_file_handle_destroy( fh );
   else
      fs_file_handle_unlock( fh );

   return rc;
}

