/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "opendir.h"
#include "network.h"
#include "stat.h"
#include "url.h"
#include "consistency.h"

// create a directory handle from an fs_entry
struct fs_dir_handle* fs_dir_handle_create( struct fs_entry* dir, char const* path ) {
   struct fs_dir_handle* dirh = (struct fs_dir_handle*)calloc( sizeof(struct fs_dir_handle), 1 );
   dirh->dent = dir;
   dirh->path = strdup(path);
   dirh->open_count = 0;
   pthread_rwlock_init( &dirh->lock, NULL );
   return dirh;
}



// open a directory handle
// NOTE: make sure everything's locked first!
int fs_dir_handle_open( struct fs_dir_handle* dirh )  {
   dirh->open_count++;
   return 0;
}



// open a directory
struct fs_dir_handle* fs_entry_opendir( struct fs_core* core, char const* _path, uint64_t user, uint64_t vol, int* err ) {

   // ensure path ends in /
   char path[PATH_MAX];
   strcpy( path, _path );

   md_sanitize_path( path );
   
   int rc = fs_entry_revalidate_path( core, path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
      *err = -EREMOTEIO;
      return NULL;
   }

   struct fs_entry* dir = fs_entry_resolve_path( core, path, user, vol, true, err );
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
   struct fs_dir_handle* dirh = fs_dir_handle_create( dir, path );
   rc = fs_dir_handle_open( dirh );
   if( rc != 0 ) {
      fs_dir_handle_destroy( dirh );
      free( dirh );
      dirh = NULL;
      *err = rc;
   }

   // release the directory
   fs_entry_unlock( dir );

   return dirh;
}
