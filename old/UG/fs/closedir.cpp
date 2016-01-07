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

#include "closedir.h"


// close a directory handle
int fs_entry_closedir( struct fs_core* core, struct fs_dir_handle* dirh ) {
   fs_dir_handle_wlock( dirh );

   fs_dir_handle_close( dirh );

   if( dirh->open_count <= 0 ) {
      // all references to this handle are gone.
      // decrement reference count
      fs_entry_wlock( dirh->dent );

      dirh->dent->open_count--;

      if( dirh->dent->open_count <= 0 && dirh->dent->link_count <= 0 ) {
         fs_entry_destroy( dirh->dent, false );
         free( dirh->dent );
         dirh->dent = NULL;
      }
      else {
         fs_entry_unlock( dirh->dent );
      }

      fs_dir_handle_destroy( dirh );
   }
   else {
      fs_dir_handle_unlock( dirh );
   }

   return 0;
}

// close a directory handle
// NOTE: make sure everything's locked first!
int fs_dir_handle_close( struct fs_dir_handle* dh ) {
   dh->open_count--;
   return 0;
}

