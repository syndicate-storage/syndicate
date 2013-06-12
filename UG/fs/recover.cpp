/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "recover.h"
#include "manifest.h"
#include "consistency.h"
#include "url.h"

// restore a single file's manifest from local data, given the latest data from the MS.
// fent must refer to a local file
int fs_entry_restore_file_from_disk( struct fs_core* core, struct fs_entry* fent ) {
   return 0;
   
   char* fs_path = GET_FS_PATH( core->conf->data_root, fent->url );
   char* publish_path = md_publish_path_file( core->conf->data_root, fent->url, 0 );
   md_clear_version( publish_path );

   // verify that the version of this file matches the local version
   int64_t* versions = md_versions( publish_path );
   int64_t max_version = -1;
   for( int i = 0; versions[i] >= 0; i++ ) {
      if( max_version < versions[i] ) {
         max_version = versions[i];
      }
   }
   free( versions );
   
   if( max_version != fent->version ) {
      // version from the metadata server conflicts with the version here
      errorf( "%s: stale version %lld, most recent is %lld\n", fs_path, max_version, fent->version );
      free( publish_path );
      return -ESTALE;
   }

   char* versioned_publish_path = fs_entry_mkpath( publish_path, fent->version );
   free( publish_path );

   Serialization::ManifestMsg mmsg;
   mmsg.set_size( fent->size );
   mmsg.set_file_version( fent->version );
   mmsg.set_mtime_sec( fent->mtime_sec );
   mmsg.set_mtime_nsec( fent->mtime_nsec );
   mmsg.set_manifest_mtime_sec( fent->mtime_sec );
   mmsg.set_manifest_mtime_nsec( fent->mtime_nsec );

   Serialization::BlockURLSetMsg* bmsg = mmsg.add_block_url_set();

   // add all of our local blocks
   DIR* dir = opendir( versioned_publish_path );
   if( dir == NULL ) {
      int rc = -errno;
      errorf( "could not open %s, errno = %d\n", versioned_publish_path, rc );
      free( versioned_publish_path );
      return rc;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(versioned_publish_path, _PC_NAME_MAX) + 1;

   struct dirent* dent = (struct dirent*)malloc( dirent_sz );;
   struct dirent* result = NULL;

   char name_buf[NAME_MAX+1];

   int rc = 0;
   uint64_t num_blocks = fent->size / core->conf->blocking_factor;
   if( fent->size % core->conf->blocking_factor != 0 )
      num_blocks++;

   int64_t* block_versions = CALLOC_LIST( int64_t, num_blocks + 1 );
   for( uint64_t i = 0; i < num_blocks; i++ ) {
      block_versions[i] = -1;
   }

   do {
      readdir_r( dir, dent, &result );
      if( result != NULL ) {
         if( strcmp(result->d_name, ".") == 0 || strcmp(result->d_name, "..") == 0 )
            continue;

         memset( name_buf, 0, NAME_MAX+1 );
         strcpy( name_buf, result->d_name );

         // find the version number
         char* version_ptr = strstr( name_buf, "." );
         if( version_ptr == NULL ) {
            // not valid
            errorf( "ignoring invalid fragment %s\n", result->d_name );
            continue;
         }

         *version_ptr = '\0';
         version_ptr++;

         // read block id
         char* tmp = NULL;
         uint64_t block_id = (uint64_t)strtol( name_buf, &tmp, 10 );

         if( tmp == NULL ) {
            errorf( "ignoring invalid fragment %s\n", result->d_name );
            continue;
         }

         if( block_id > num_blocks ) {
            errorf( "ignoring overflow fragment %s\n", result->d_name );
            continue;
         }

         // read version
         int64_t version = (int64_t)strtol( version_ptr, &tmp, 10 );
         if( tmp == NULL ) {
            errorf( "ignoring invalid fragment %s\n", result->d_name );
            continue;
         }

         // stash the name
         block_versions[ block_id ] = version;
      }
   } while( result != NULL );

   // verify that all the blocks are here
   uint64_t num_missing = 0;
   for( uint64_t i = 0; i < num_blocks; i++ ) {
      if( block_versions[i] == -1 ) {
         errorf( "%s (at %s): missing block %lld\n", fs_path, versioned_publish_path, i );
         num_missing++;
      }
   }

   if( num_missing == 0 ) {
      // build up the manifest
      bmsg->set_start_id( 0 );
      bmsg->set_end_id( num_blocks );
      bmsg->set_file_url( string( fent->url ) );
      
      // set the block versions
      for( uint64_t i = 0; i < num_blocks; i++ ) {
         bmsg->add_block_versions( block_versions[i] );
      }

      if( fent->manifest )
         delete fent->manifest;

      fent->manifest = new file_manifest( core, fent, mmsg );
      fent->manifest->mark_stale();

      struct timespec ts;
      ts.tv_sec = fent->mtime_sec;
      ts.tv_nsec = fent->mtime_nsec;
      fent->manifest->set_lastmod( &ts );

      rc = 0;
   }
   else {
      rc = -ENODATA;
   }

   closedir( dir );
   free( dent );
   free( versioned_publish_path );
   free( block_versions );

   return rc;
}

// once the filesystem has been re-built from the metadata server,
// recreate each fs_entry's manifest from local and remote data.
// NOTE: no locking is done here!  Perform this operation only when no changes can occur
int fs_entry_restore_files( struct fs_core* core ) {
   dbprintf("%s", "begin restoring\n");
   list<struct fs_entry*> dir_queue;

   dir_queue.push_back( core->root );
   int worst_rc = 0;

   string cur_path = "";
   
   while( dir_queue.size() > 0 ) {
      // restore each file here
      struct fs_entry* dir = dir_queue.front();
      dir_queue.pop_front();

      cur_path += string(dir->name);

      // get MS records
      fs_entry_mark_read_stale( dir );    // force a reload
      int rc = fs_entry_revalidate_path( core, cur_path.c_str() );
      if( rc != 0 ) {
         errorf("fs_entry_revalidate_path(%s) rc = %d\n", cur_path.c_str(), rc );
         break;
      }
      
      // find parent
      struct fs_entry* parent = fs_entry_set_find_name( dir->children, ".." );

      for( fs_entry_set::iterator itr = dir->children->begin(); itr != dir->children->end(); itr++ ) {

         struct fs_entry* fent = itr->second;

         // skip . and ..
         if( fent == dir || fent == parent )
            continue;

         // check corresponding directories on local disk to see if we need to re-add things
         if( fent->ftype == FTYPE_DIR ) {
            dir_queue.push_back( fent );
         }
         // restore files
         else {
            if( URL_LOCAL( fent->url ) ) {
               // file is local; restore its manifest
               rc = fs_entry_restore_file_from_disk( core, fent );
               if( rc != 0 ) {
                  errorf( "fs_entry_restore_file_from_disk(%s), rc = %d\n", GET_PATH( fent->url ), rc );

                  fent->size = 0;

                  if( fent->manifest )
                     delete fent->manifest;

                  fent->manifest = new file_manifest( core );

                  worst_rc = rc;
               }
               else {
                  dbprintf( "restored %s\n", GET_PATH( fent->url ) );
               }
            }
         }
      }
   }

   dbprintf("%s", "end restoring\n");
   return worst_rc;
}
