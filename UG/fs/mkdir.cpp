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

#include "mkdir.h"
#include "link.h"

// low-level mkdir
int fs_entry_mkdir_lowlevel( struct fs_core* core, char const* path, struct fs_entry* parent, char const* path_basename, mode_t mode, uint64_t user, uint64_t vol, int64_t mtime_sec, int32_t mtime_nsec ) {
   
   // resolve the child
   struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );
   int err = 0;
   if( child == NULL ) {

      // create an fs_entry and attach it
      child = (struct fs_entry*)calloc( sizeof(struct fs_entry), 1 );

      fs_entry_init_dir( core, child, path_basename, fs_entry_next_file_version(), user, 0, vol, mode, mtime_sec, mtime_nsec );
      
      // add . and ..
      fs_entry_set_insert( child->children, ".", child );
      fs_entry_set_insert( child->children, "..", parent );

      fs_entry_attach_lowlevel( core, parent, child );
   }
   else {
      // already exists
      err = -EEXIST;
   }

   return err;
}


// create a directory
int fs_entry_mkdir( struct fs_core* core, char const* path, mode_t mode, uint64_t user, uint64_t vol ) {
   
   if( core->gateway == GATEWAY_ANON ) {
      errorf("%s", "Making directories is forbidden for anonymous gateways\n");
      return -EPERM;
   }
   
   int err = 0;

   // resolve the parent of this child (and write-lock it)
   char* fpath = strdup( path );
   md_sanitize_path( fpath );
   
   // revalidate this path
   int rc = fs_entry_revalidate_path( core, vol, fpath );
   if( rc != 0 && rc != -ENOENT ) {
      // consistency cannot be guaranteed
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fpath, rc );
      free( fpath );
      return rc;
   }
   
   char* path_dirname = md_dirname( fpath, NULL );
   char* path_basename = md_basename( fpath, NULL );

   md_sanitize_path( path_dirname );

   free( fpath );

   struct fs_entry* parent = fs_entry_resolve_path( core, path_dirname, user, vol, true, &err );
   
   if( parent == NULL || err ) {
      // parent not found
      free( path_basename );
      free( path_dirname );
      // err is set appropriately
      return err;
   }
   if( parent->ftype != FTYPE_DIR ) {
      // parent is not a directory
      fs_entry_unlock( parent );
      free( path_basename );
      free( path_dirname );
      return -ENOTDIR;
   }
   if( !IS_WRITEABLE(parent->mode, parent->owner, parent->volume, user, vol) ) {
      // parent is not writeable
      errorf( "%s is not writable by %" PRIu64 " (%o, %" PRIu64 ", %" PRIu64 ", %" PRIu64 ", %" PRIu64 ")\n", path_dirname, user, parent->mode, parent->owner, parent->volume, user, vol );
      fs_entry_unlock( parent );
      free( path_basename );
      free( path_dirname );
      return -EACCES;
   }

   uint64_t parent_id = parent->file_id;
   char* parent_name = strdup( parent->name );
   
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );

   err = fs_entry_mkdir_lowlevel( core, path, parent, path_basename, mode, user, vol, ts.tv_sec, ts.tv_nsec );

   if( err != 0 ) {
      errorf( "fs_entry_mkdir_lowlevel(%s) rc = %d\n", path, err );
      fs_entry_unlock( parent );
      free( path_basename );
      free( path_dirname );
      free( parent_name );
      return err;
   }

   else {

      struct fs_entry* child = fs_entry_set_find_name( parent->children, path_basename );

      fs_entry_wlock( child );
      
      // create this directory in the MS
      struct md_entry data;
      fs_entry_to_md_entry( core, &data, child, parent_id, parent_name );
      free( parent_name );
      
      err = ms_client_mkdir( core->ms, &child->file_id, &data );
      
      if( err != 0 ) {
         errorf("ms_client_create(%s) rc = %d\n", path, err );
         
         fs_entry_unlock( child );
         rc = fs_entry_detach_lowlevel( core, parent, child );
         if( rc != 0 ) {
            errorf("fs_entry_detach_lowlevel(%s) rc = %d\n", path, rc );
         }
      }
      else {

         fs_entry_unlock( child );
      }
      
      md_entry_free( &data );
   }

   fs_entry_unlock( parent );
   free( path_basename );
   free( path_dirname );
   return err;
}
