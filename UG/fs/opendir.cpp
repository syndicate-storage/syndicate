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

#include "opendir.h"
#include "network.h"
#include "stat.h"
#include "url.h"
#include "consistency.h"

// create a directory handle from an fs_entry
struct fs_dir_handle* fs_dir_handle_create( struct fs_entry* dir, char const* path, uint64_t parent_id, char const* parent_name ) {
   struct fs_dir_handle* dirh = (struct fs_dir_handle*)calloc( sizeof(struct fs_dir_handle), 1 );
   dirh->dent = dir;
   dirh->path = strdup( path );
   dirh->open_count = 0;
   dirh->volume = dir->volume;
   dirh->file_id = dir->file_id;
   dirh->parent_id = parent_id;
   dirh->parent_name = strdup( parent_name );
   pthread_rwlock_init( &dirh->lock, NULL );
   return dirh;
}



// open a directory handle
// NOTE: make sure everything's locked first!
int fs_dir_handle_open( struct fs_dir_handle* dirh )  {
   dirh->open_count++;
   return 0;
}



// open a directory, but fail-fast if we can't get path metadata
struct fs_dir_handle* fs_entry_opendir( struct fs_core* core, char const* _path, uint64_t user, uint64_t vol, int* err ) {

   // ensure path ends in /
   char path[PATH_MAX];
   strcpy( path, _path );

   md_sanitize_path( path );
   
   int rc = fs_entry_revalidate_path( core, vol, path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      return NULL;
   }

   uint64_t parent_id = 0;
   char* parent_name = NULL;

   struct fs_entry* dir = fs_entry_resolve_path_and_parent_info( core, path, user, vol, true, err, &parent_id, &parent_name );
   if( dir == NULL ) {
      return NULL;
   }
   // make sure it's a directory
   if( dir->ftype != FTYPE_DIR ) {
      *err = -ENOTDIR;
      fs_entry_unlock( dir );
      return NULL;
   }

   // open this directory
   dir->open_count++;
   struct fs_dir_handle* dirh = fs_dir_handle_create( dir, path, parent_id, parent_name );
   rc = fs_dir_handle_open( dirh );
   if( rc != 0 ) {
      fs_dir_handle_destroy( dirh );
      free( dirh );
      dirh = NULL;
      *err = rc;
   }

   // release the directory
   fs_entry_unlock( dir );
   free( parent_name );
   
   return dirh;
}



