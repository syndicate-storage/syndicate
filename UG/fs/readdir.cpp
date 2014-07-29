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

#include "readdir.h"

// low-level read directory
// dent must be read-locked
struct fs_dir_entry** fs_entry_readdir_lowlevel( struct fs_core* core, char const* fs_path, struct fs_entry* dent, uint64_t parent_id, char const* parent_name ) {
   unsigned int num_ents = fs_entry_set_count( dent->children );

   struct fs_dir_entry** dents = (struct fs_dir_entry**)calloc( sizeof(struct fs_dir_entry*) * (num_ents + 1), 1 );
   unsigned int cnt = 0;

   
   for( fs_entry_set::iterator itr = dent->children->begin(); itr != dent->children->end(); itr++ ) {
      struct fs_entry* fent = fs_entry_set_get( &itr );
      long fent_name_hash = fs_entry_set_get_name_hash( &itr );

      if( fent == NULL )
         continue;

      struct fs_dir_entry* d_ent = NULL;

      // handle . and .. separately--we only want to lock children (not the current or parent directory)
      if( fent_name_hash == fs_entry_name_hash( "." ) ) {
         d_ent = CALLOC_LIST( struct fs_dir_entry, 1 );
         d_ent->ftype = FTYPE_DIR;
         fs_entry_to_md_entry( core, &d_ent->data, dent, parent_id, parent_name );

         free( d_ent->data.name );
         d_ent->data.name = strdup(".");
      }
      else if( fent_name_hash == fs_entry_name_hash( ".." ) ) {
         d_ent = CALLOC_LIST( struct fs_dir_entry, 1 );
         d_ent->ftype = FTYPE_DIR;
         char* parent_path = md_dirname( fs_path, NULL );

         if( fent != dent ) {
            fs_entry_to_md_entry( core, &d_ent->data, parent_path, SYS_USER, dent->volume );
         }
         else {
            fs_entry_to_md_entry( core, &d_ent->data, dent, parent_id, parent_name );     // this is /
         }

         free( parent_path );

         free( d_ent->data.name );
         d_ent->data.name = strdup("..");
      }
      else {
         if( fent != dent && fs_entry_rlock( fent ) != 0 ) {
            continue;
         }
         else if( fent->name != NULL && !fent->deletion_in_progress ) {    // only show entries that exist
            d_ent = CALLOC_LIST( struct fs_dir_entry, 1 );
            d_ent->ftype = fent->ftype;
            fs_entry_to_md_entry( core, &d_ent->data, fent, parent_id, parent_name );

         }
         
         if( fent != dent ) {
            fs_entry_unlock( fent );
         }
      }

      if( d_ent != NULL ) {
         dents[cnt] = d_ent;
         dbprintf( "in '%s': '%s'\n", dent->name, d_ent->data.name );
         cnt++;
      }
   }
   
   return dents;
}


// read data from a directory
struct fs_dir_entry** fs_entry_readdir( struct fs_core* core, struct fs_dir_handle* dirh, int* err ) {
   fs_dir_handle_rlock( dirh );
   if( dirh->dent == NULL || dirh->open_count <= 0 ) {
      // invalid
      fs_dir_handle_unlock( dirh );
      *err = -EBADF;
      return NULL;
   }

   fs_entry_rlock( dirh->dent );

   struct fs_dir_entry** dents = fs_entry_readdir_lowlevel( core, dirh->path, dirh->dent, dirh->parent_id, dirh->parent_name );

   fs_entry_unlock( dirh->dent );
   
   fs_dir_handle_unlock( dirh );

   return dents;
}
