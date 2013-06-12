/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#include "storage.h"
#include "manifest.h"
#include "url.h"

// create a local file's block directory on disk.
// path must be locked somehow
int fs_entry_create_local_file( struct fs_core* core, char const* fs_path, int64_t version, mode_t mode ) {
   // it is possible for there to be a 0-sized non-directory here, to indicate the next version to be created.
   // if so, remove it

   char* path = md_publish_path_file( core->conf->data_root, fs_path, version );
   
   int rc = 0;

   dbprintf("create '%s.%'" PRId64 " mode %o\n", fs_path, version, mode);

   rc = md_mkdirs3( path, mode | 0700 );
   if( rc < 0 )
      rc = -errno;
   
   free( path );
   
   return rc;
}


// move a local file
// path is the fully-qualified path
int fs_entry_move_local_data( char* path, char* new_path ) {
   int rc = rename( path, new_path );
   if( rc != 0 ) {
      rc = -errno;
   }
   return rc;
}

// truncate a file on disk
int fs_entry_truncate_local_data( struct fs_core* core, char const* fs_path, int64_t version ) {
   // only remove if this directory represents an actual file

   char* path = md_publish_path_file( core->conf->data_root, fs_path, version );
   
   DIR* dir = opendir( path );
   if( dir == NULL ) {
      int rc = -errno;
      errorf( "opendir(%s) errno = %d\n", path, rc );
      free(path);
      return rc;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(path, _PC_NAME_MAX) + 1;

   struct dirent* dent = (struct dirent*)malloc( dirent_sz );;
   struct dirent* result = NULL;
   char block_path[PATH_MAX+1];
   int rc = 0;
   int worst_rc = 0;

   do {
      readdir_r( dir, dent, &result );
      if( result != NULL ) {
         if( strcmp(result->d_name, ".") == 0 || strcmp(result->d_name, "..") == 0 )
            continue;

         md_fullpath( path, result->d_name, block_path );
         rc = unlink( block_path );
         if( rc != 0 ) {
            // could not unlink
            rc = -errno;
            errorf( "unlink(%s) errno = %d\n", block_path, rc );
            worst_rc = rc;
         }
      }
   } while( result != NULL );

   closedir( dir );
   free( dent );
   free( path );
   return worst_rc;
}

// remove a local file from disk
// path is the fully-qualified path
// This path most somehow be locked first
int fs_entry_remove_local_data( struct fs_core* core, char const* fs_path, int64_t version ) {
   // only remove if this directory represents an actual file
   int worst_rc = fs_entry_truncate_local_data( core, fs_path, version );

   char* path = md_publish_path_file( core->conf->data_root, fs_path, version );
   
   if( worst_rc == 0 ) {
      
      int rc = rmdir( path );
      if( rc != 0 ) {
         worst_rc = -errno;
         errorf( "rmdir(%s) errno = %d\n", path, rc );
      }
   }
   else {
      errorf("fs_entry_truncate_local_data(%s, %" PRId64 ") rc = %d\n", fs_path, version, worst_rc );
   }

   free( path );

   return worst_rc;
}


// create a local directory
int fs_entry_create_local_directory( struct fs_core* core, char const* fs_path ) {
   
   // make this directory in the data directory
   char* data_path = md_fullpath( core->conf->data_root, fs_path, NULL );
   int rc = md_mkdirs( data_path );
   
   if( rc != 0 ) {
      errorf("md_mkdirs(%s) rc = %d\n", data_path, rc );
   }

   free( data_path );

   return rc;
}


// remove a local directory
int fs_entry_remove_local_directory( struct fs_core* core, char const* path ) {
   return md_withdraw_dir( core->conf->data_root, path );
}


// publish a file
int fs_entry_publish_file( struct fs_core* core, char const* fs_path, uint64_t version, mode_t mode ) {
   
   int rc = fs_entry_create_local_file( core, fs_path, version, mode );
   if( rc != 0 ) {
      errorf("fs_entry_create_local_file(%s.%" PRId64 ") rc = %d\n", fs_path, version, rc );
      return rc;
   }

   return rc;
}


// reversion a file, if the current verison exists
// fent must be write-locked
int fs_entry_reversion_local_file( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t new_version ) {
   char* cur_local_url = fs_entry_local_file_url( core, fs_path, fent->version );
   char* new_local_url = fs_entry_local_file_url( core, fs_path, new_version );

   char* cur_local_path = GET_PATH( cur_local_url );
   char* new_local_path = GET_PATH( new_local_url );

   int rc = rename( cur_local_path, new_local_path );
   if( rc != 0 ) {
      rc = -errno;
      errorf("rename(%s,%s) rc = %d\n", cur_local_path, new_local_path, rc );
   }

   free( cur_local_url );
   free( new_local_url );
   return rc;
}


// write a block's worth of content
ssize_t fs_entry_write_block( struct fs_core* core, int fd, char* buf ) {
   ssize_t ret = 0;
   while( ret < (signed)core->conf->blocking_factor ) {
      ssize_t nw = write( fd, buf + ret, core->conf->blocking_factor - ret );
      if( nw < 0 ) {
         ret = -errno;
         break;
      }
      else {
         ret += nw;
      }
   }
   return ret;
}


// read a block's worth of content
ssize_t fs_entry_get_block_local( struct fs_core* core, int fd, char* block ) {
   ssize_t nr = 0;
   while( nr < (signed)core->conf->blocking_factor ) {
      ssize_t tmp = read( fd, block + nr, core->conf->blocking_factor - nr );
      if( tmp < 0 ) {
         ssize_t rc = -errno;
         return rc;
      }

      if( tmp == 0 ) {
         break;
      }

      nr += tmp;
   }
   return nr;
}


// given a url and version, calculate either a data local URL or a staging local URL for a block, depending on the URL.
static char* fs_entry_get_block_storage_url( struct fs_core* core, char const* url, char const* fs_path, int64_t file_version, uint64_t block_id, int64_t block_version ) {

   char* local_block_url = NULL;
   if( URL_LOCAL( url ) ) {
      // file is locally hosted; put into our data directory
      local_block_url = fs_entry_local_block_url( core, fs_path, file_version, block_id, block_version );
   }
   else {
      // file is remotely hosted; put into our staging directory
      local_block_url = fs_entry_local_staging_block_url( core, fs_path, file_version, block_id, block_version );
   }
   return local_block_url;
}

// given a url and version, calculate either a data local URL or a staging local URL for a file, depending on the URL.
static char* fs_entry_get_file_storage_url( struct fs_core* core, char const* url, char const* fs_path, int64_t file_version ) {

   char* local_file_url = NULL;
   if( URL_LOCAL( url ) ) {
      // file is locally hosted; put into our data directory
      local_file_url = fs_entry_local_file_url( core, fs_path, file_version );
   }
   else {
      // file is remotely hosted; put into our staging directory
      local_file_url = fs_entry_local_staging_file_url( core, fs_path, file_version );
   }
   return local_file_url;
}


// put a block with the given version 
// return 0 on success
// FENT MUST BE WRITE-LOCKED, SO ANOTHER THREAD CAN'T ADD A BLOCK OF THE SAME VERSION
int fs_entry_commit_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* buf ) {
   int rc = 0;

   // get the location of this block
   char* local_block_url = fs_entry_get_block_storage_url( core, fent->url, fs_path, fent->version, block_id, block_version );
   
   // make sure the directories leading to this block exist
   char* storage_dir = md_dirname( GET_PATH( local_block_url ), NULL );
   rc = md_mkdirs( storage_dir );
   
   if( rc != 0 ) {
      errorf( "md_mkdirs(%s) rc = %d\n", storage_dir, rc );
      md_rmdirs( storage_dir );
      free( storage_dir );
      free( local_block_url );
      return rc;
   }
   free( storage_dir );
   
   char* block_path = GET_PATH( local_block_url );
   int fd = open( block_path, O_WRONLY | O_TRUNC | O_CREAT | O_EXCL, 0600 );
   if( fd < 0 ) {
      // this block doesn't exist, or some other error
      rc = -errno;
      errorf("open(%s) rc = %d\n", block_path, rc );
      free( local_block_url );
      return rc;
   }

   ssize_t num_written = fs_entry_write_block( core, fd, buf );
   if( num_written < 0 ) {
      errorf("fs_entry_write_block(%s) rc = %zd\n", block_path, num_written );
      close( fd );
      free( local_block_url );
      return num_written;
   }

   close( fd );
   free( local_block_url );

   return rc;
}


// clear out old versions of a block.
// preserve a block with current_block_version.
// fent must be at least read_locked.
int fs_entry_remove_old_block_versions( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t current_block_version ) {
   int rc = 0;

   // get the location of this block
   char* local_block_url_prefix = fs_entry_get_block_storage_url( core, fent->url, fs_path, fent->version, block_id, 0 );
   md_clear_version( local_block_url_prefix );

   char* block_path = GET_PATH( local_block_url_prefix );

   int64_t* versions = md_versions( block_path );

   for( int i = 0; versions != NULL && versions[i] >= 0; i++ ) {
      if( versions[i] == current_block_version ) {
         // ignore the current block
         continue;
      }

      char* block_versioned_path = fs_entry_mkpath( block_path, versions[i] );

      // remove the block
      rc = unlink( block_versioned_path );
      if( rc != 0 ) {
         rc = -errno;
         errorf("WARN: unlink(%s) rc = %d\n", block_versioned_path, rc );
         rc = 0;     // not really fatal, but now we have blocks left over 
      }

      free( block_versioned_path );
   }

   if( versions ) {
      free( versions );
   }

   free( local_block_url_prefix );
   
   return rc;
}


// write a block to a file, hosting it on underlying storage, and updating the filesystem entry's manifest to refer to it.
// if the URL refers to a local place on disk, then store it to the data directory.
// If it instead refers to a remote host, then store it to the staging directory.
// fent MUST BE WRITE LOCKED, SINCE WE MODIFY THE MANIFEST
int fs_entry_put_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, char* block_data ) {

   int64_t block_version = fs_entry_next_block_version();

   dbprintf("put %s.%" PRId64 "[%" PRIu64 ".%" PRId64 "]\n", fs_path, fent->version, block_id, block_version );
   
   // put the block into place
   int rc = fs_entry_commit_block( core, fs_path, fent, block_id, block_version, block_data );

   if( rc != 0 ) {
      // failed to write
      errorf( "fs_entry_commit_block(%s.%" PRId64 "[%" PRIu64 ".%" PRId64 "]) rc = %d\n", fs_path, fent->version, block_id, block_version, rc );
      return -EIO;
   }
   else {
      // clear out all older versions of this block
      rc = fs_entry_remove_old_block_versions( core, fs_path, fent, block_id, block_version );
      if( rc != 0 ) {
         // failed to remove
         errorf("WARN: fs_entry_remove_old_block_versions(%s.%" PRId64 ") rc = %d\n", fs_path, fent->version, rc );
         rc = 0;
      }
      
      // add this block's URL to the manifest
      char* local_url = fs_entry_get_file_storage_url( core, fent->url, fs_path, fent->version );

      // clear the version--local manifest URLs don't have versions
      md_clear_version( local_url );

      fs_entry_put_block_url( fent, local_url, fent->version, block_id, block_version );

      free( local_url );

      // update our modtime
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );

      fent->mtime_sec = ts.tv_sec;
      fent->mtime_nsec = ts.tv_nsec;
   }
   return 0;
}


// remove a locally-hosted block from a file, either from staging or local data.
// fent must be at least read-locked
int fs_entry_remove_block( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id ) {
   return fs_entry_remove_old_block_versions( core, fs_path, fent, block_id, -1 );
}


// collate a block back into a file, given the block data
// return negative on error
// return the next version of this block
// fent must be write-locked
int fs_entry_collate( struct fs_core* core, char const* fs_path, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* bits ) {

   char tmppath[PATH_MAX];
   strcpy( tmppath, SYNDICATE_COLLATE_TMPPATH );

   int fd = mkstemp( tmppath );
   if( fd < 0 ) {
      fd = -errno;
      errorf("mkstemp errno = %d\n", fd);
      return fd;
   }

   ssize_t wrc = fs_entry_write_block( core, fd, bits );
   if( wrc < 0 ) {
      errorf("fs_entry_write_block rc = %zd\n", wrc );
      return (int)wrc;
   }

   close( fd );
   
   // put the block in place
   char* block_path = fs_entry_local_block_path( core->conf->data_root, fs_path, fent->version, block_id, block_version );
   int rc = rename( tmppath, block_path );

   if( rc != 0 ) {
      rc = -errno;
      errorf( "rename(%s,%s) errno = %d\n", tmppath, block_path, rc );
      free( block_path );
      return -EIO;
   }
   
   free( block_path );

   // add this block's URL to the manifest
   char* local_url = fs_entry_local_file_url( core, fs_path, fent->version );

   // clear the version--local manifest URLs don't have versions
   md_clear_version( local_url );

   fs_entry_put_block_url( fent, local_url, fent->version, block_id, block_version );

   free( local_url );

   // write this back to the MS 
   struct md_entry data;
   fs_entry_to_md_entry( core, fs_path, fent, &data );

   ms_client_queue_update( core->ms, fs_path, &data, fent->max_write_freshness, 0 );

   md_entry_free( &data );
   
   return 0;
}


// release all the staging blocks for a particular file, since they have been reintegrated with the remote host
int fs_entry_release_staging( struct fs_core* core, Serialization::WriteMsg* accept_msg ) {
   // sanity check
   if( !accept_msg->has_accepted() )
      return -EINVAL;
   
   // sanity check
   if( accept_msg->accepted().block_id_size() != accept_msg->accepted().block_version_size() )
      return -EINVAL;
   
   char const* fs_path = accept_msg->accepted().fs_path().c_str();

   // get this fent, write-lock it since we need to manipulate its data
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOENT;

      else if( err == -ENOENT ) {
         // file got unlinked while it was being collated.
         // just remove the staging directory for this file's blocks.

         char const* fs_path = accept_msg->accepted().fs_path().c_str();
         int64_t file_version = accept_msg->accepted().file_version();

         // remove data
         int rc = md_withdraw_file( core->conf->staging_root, fs_path, file_version );
         if( rc != 0 ) {
            errorf( "md_withdraw_file(%s.%" PRId64 ") rc = %d\n", fs_path, file_version, rc );
         }
         
         err = rc;
      }

      return err;
   }

   if( URL_LOCAL( fent->url ) ) {
      // only remote files have staging information
      fs_entry_unlock( fent );
      return -EINVAL;
   }


   // mark the manifest as stale so we refresh it on the next I/O operation
   fent->manifest->mark_stale();

   int rc = 0;

   // remove all of the blocks we're holding for this file if they're the same version
   int64_t file_version = accept_msg->accepted().file_version();
   if( fent->version > file_version ) {
      errorf("ERR: %s: local file is a newer version (%" PRId64 ") than accepted blocks (%" PRId64 ")\n", fs_path, fent->version, file_version );
      rc = 0;
   }
   else {
      // remove staging
      for( int i = 0; i < accept_msg->accepted().block_id_size(); i++ ) {

         uint64_t block_id = accept_msg->accepted().block_id(i);

         int rc = fs_entry_remove_block( core, fs_path, fent, block_id );
         if( rc != 0 ) {
            errorf("fs_entry_remove_block(%s[%" PRId64 "]) rc = %d\n", fs_path, block_id, rc );
         }
      }

      // clean up the staging directory
      char* tmp = md_fullpath( core->conf->staging_root, fs_path, NULL );
      char* dir_fullpath = fs_entry_mkpath( tmp, fent->version );
      
      rmdir( dir_fullpath );

      free( dir_fullpath );
      free( tmp );
   }

   fs_entry_unlock( fent );
   return rc;
}


// get information about a specific block
int fs_entry_block_stat( struct fs_core* core, char const* path, uint64_t block_id, struct stat* sb ) {         // system use only
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // is this block local?  if not, then nothing we can do
   char* block_url = fent->manifest->get_block_url( fent->version, block_id );
   if( block_url == NULL ) {
      fs_entry_unlock( fent );
      return -ENODATA;
   }
   if( !URL_LOCAL( block_url ) ) {
      free( block_url );
      fs_entry_unlock( fent );
      return -EXDEV;
   }

   char* stat_path = GET_PATH( block_url );
   int rc = stat( stat_path, sb );

   fs_entry_unlock( fent );

   return rc;
}

