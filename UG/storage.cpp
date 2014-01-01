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


#include "storage.h"
#include "manifest.h"
#include "url.h"
#include "consistency.h"
#include "network.h"

// given a string and a version, concatenate them, preserving delimiters
char* fs_entry_add_version( char const* fs_path, int64_t version ) {
   size_t t = strlen(fs_path);
   size_t v = 2 + log(abs(version) + 1);
   char* ret = CALLOC_LIST( char, t + 1 + v );

   bool delim = false;
   if( fs_path[t-1] == '/' ) {
      strncpy( ret, fs_path, t-1 );
      delim = true;
   }
   else {
      strncpy( ret, fs_path, t );
   }

   char buf[50];

   if( delim )
      sprintf(buf, ".%" PRId64 "/", version );
   else
      sprintf(buf, ".%" PRId64, version );

   strcat( ret, buf );

   return ret;
}


// create a local file's block directory on disk.
// path must be locked somehow
int fs_entry_create_local_file( struct fs_core* core, uint64_t file_id, int64_t version, mode_t mode ) {
   // it is possible for there to be a 0-sized non-directory here, to indicate the next version to be created.
   // if so, remove it

   char* local_file_url = fs_entry_local_file_url( core, file_id, version );
   char* local_path = GET_PATH( local_file_url );

   dbprintf("create %s. mode %o\n", local_path, mode);

   int rc = md_mkdirs3( local_path, mode | 0700 );
   if( rc < 0 )
      rc = -errno;
   
   free( local_file_url );
   
   return rc;
}

// move a local file
// path is the fully-qualified path
int fs_entry_move_local_file( char* path, char* new_path ) {
   int rc = rename( path, new_path );
   if( rc != 0 ) {
      rc = -errno;
   }
   return rc;
}

// clear a directory out
static int fs_entry_clear_file_by_path( char const* local_path ) {
   
   DIR* dir = opendir( local_path );
   if( dir == NULL ) {
      int rc = -errno;
      errorf( "opendir(%s) errno = %d\n", local_path, rc );
      return rc;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(local_path, _PC_NAME_MAX) + 1;

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

         md_fullpath( local_path, result->d_name, block_path );
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
   
   return worst_rc;
}

// remove all blocks for a file
static int fs_entry_clear_file( struct fs_core* core, uint64_t file_id, int64_t version, bool staging ) {
   // only remove if this directory represents an actual file

   char* local_file_url = NULL;
   
   if( !staging )
      local_file_url = fs_entry_local_file_url( core, file_id, version );
   else
      local_file_url = fs_entry_local_staging_file_url( core, file_id, version );
   
   char* local_path = GET_PATH( local_file_url );
   
   int rc = fs_entry_clear_file_by_path( local_path );
   
   free( local_file_url );
   
   return rc;
}

int fs_entry_clear_local_file( struct fs_core* core, uint64_t file_id, int64_t version ) {
   return fs_entry_clear_file( core, file_id, version, false );
}

int fs_entry_clear_staging_file( struct fs_core* core, uint64_t file_id, int64_t version ) {
   return fs_entry_clear_file( core, file_id, version, true );
}


static int fs_entry_remove_file_by_path( char const* local_path ) {
   int rc = rmdir( local_path );
   if( rc != 0 ) {
      rc = -errno;
      errorf( "rmdir(%s) errno = %d\n", local_path, rc );
   }
   
   return rc;
}

// remove a local file from disk
// path is the fully-qualified path
// This path most somehow be locked first
static int fs_entry_remove_file( struct fs_core* core, uint64_t file_id, int64_t version, bool staging ) {
   // only remove if this directory represents an actual file
   int rc = fs_entry_clear_file( core, file_id, version, staging );
   if( rc != 0 ) {
      return rc;
   }
   
   char* local_file_url = NULL;
   
   if( !staging )
      local_file_url = fs_entry_local_file_url( core, file_id, version );
   else
      local_file_url = fs_entry_local_staging_file_url( core, file_id, version );
   
   char* local_path = GET_PATH( local_file_url );
   
   rc = fs_entry_remove_file_by_path( local_path );

   free( local_file_url );

   return rc;
}

int fs_entry_remove_local_file( struct fs_core* core, uint64_t file_id, int64_t version ) {
   return fs_entry_remove_file( core, file_id, version, false );
}

int fs_entry_remove_staging_file( struct fs_core* core, uint64_t file_id, int64_t version ) {
   return fs_entry_remove_file( core, file_id, version, true );
}


// reversion a local file, if the current verison exists
// fent must be write-locked
int fs_entry_reversion_local_file( struct fs_core* core, struct fs_entry* fent, uint64_t new_version ) {
   char* cur_local_url = fs_entry_local_file_url( core, fent->file_id, fent->version );
   char* new_local_url = fs_entry_local_file_url( core, fent->file_id, new_version );

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


// write ALL the block data
ssize_t fs_entry_write_block_data( struct fs_core* core, int fd, char* buf, size_t len ) {
   ssize_t ret = 0;
   while( (unsigned)ret < len ) {
      ssize_t nw = write( fd, buf + ret, len - ret );
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
ssize_t fs_entry_get_block_local( struct fs_core* core, int fd, char* block, size_t block_len ) {
   ssize_t nr = 0;
   while( nr < (signed)block_len ) {
      ssize_t tmp = read( fd, block + nr, block_len - nr );
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
static char* fs_entry_get_block_storage_url( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, bool staging ) {

   char* local_block_url = NULL;
   if( !staging ) {
      // file is locally hosted; put into our data directory
      local_block_url = fs_entry_local_block_url( core, file_id, file_version, block_id, block_version );
   }
   else {
      // file is remotely hosted; put into our staging directory
      local_block_url = fs_entry_local_staging_block_url( core, file_id, file_version, block_id, block_version );
   }
   return local_block_url;
}

// "open" a block, returning a file descriptor 
int fs_entry_open_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t block_version, bool staging, bool creat ) {
   int rc = 0;

   // get the location of this block
   char* local_block_url = fs_entry_get_block_storage_url( core, fent->file_id, fent->version, block_id, block_version, staging );
   
   if( creat ) {
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
   }
   
   char* block_path = GET_PATH( local_block_url );
   
   int flags = O_RDWR;
   if( creat )
      flags |= (O_CREAT | O_EXCL);
   
   int fd = open( block_path, flags, 0600 );
   if( fd < 0 )
      fd = -errno;
   
   free( local_block_url );
   
   return fd;
}


// put block data with the given version to the given offset
// return 0 on success
// the corresponding fent should be write-locked, to prevent another thread from writing the same block!
ssize_t fs_entry_commit_block_data( struct fs_core* core, uint64_t file_id, int64_t file_version, uint64_t block_id, int64_t block_version, char* buf, size_t len, bool staging ) {
   int rc = 0;

   // get the location of this block
   char* local_block_url = fs_entry_get_block_storage_url( core, file_id, file_version, block_id, block_version, staging );
   
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
   
   bool created = false;
   
   int fd = open( block_path, O_WRONLY | O_CREAT | O_EXCL, 0600 );
   if( fd < 0 && errno == EEXIST )
      fd = open( block_path, O_WRONLY );
   else
      created = true;
   
   if( fd < 0 ) {
      // this block doesn't exist, or some other error
      rc = -errno;
      errorf("open(%s) rc = %d\n", block_path, rc );
      free( local_block_url );
      return rc;
   }
   
   if( created ) {
      // make this block the block size
      rc = ftruncate( fd, core->blocking_factor );
      if( rc != 0 ) {
         rc = -errno;
         errorf("ftruncate(%s) rc = %d\n", block_path, rc );
         unlink( block_path );
         free( local_block_url );
         close( fd );
         return rc;
      }
   }
   
   ssize_t num_written = fs_entry_write_block_data( core, fd, buf, len );
   if( num_written < 0 ) {
      errorf("fs_entry_write_block_data(%s) rc = %zd\n", block_path, num_written );
      close( fd );
      free( local_block_url );
      return num_written;
   }

   close( fd );
   free( local_block_url );

   return num_written;
}


// given a base path, find the instances of the path locally that have version numbers. 
// return the version numbers as a -1 terminated list
int64_t* fs_entry_read_versions( char const* base_path ) {
   char* base_path_dir = md_dirname( base_path, NULL );
   char* base_path_basename = md_basename( base_path, NULL );

   DIR* dir = opendir( base_path_dir );
   if( dir == NULL ) {
      int errsv = errno;
      errorf( "could not open %s, errno = %d\n", base_path_dir, errsv );
      return NULL;
   }

   int dirent_sz = offsetof(struct dirent, d_name) + pathconf(base_path_dir, _PC_NAME_MAX) + 1;

   struct dirent* dent = (struct dirent*)malloc( dirent_sz );;
   struct dirent* result = NULL;

   vector<int64_t> versions;

   do {
      readdir_r( dir, dent, &result );
      if( result != NULL ) {
         if( md_is_versioned_form( base_path_basename, result->d_name ) ) {
            int64_t ver = md_path_version( result->d_name );
            if( ver >= 0 )
               versions.push_back( ver );
         }
      }
   } while( result != NULL );

   int64_t* ret = CALLOC_LIST( int64_t, versions.size() + 1 );
   for( unsigned int i = 0; i < versions.size(); i++ ) {
      ret[i] = versions[i];
   }
   ret[ versions.size() ] = (int64_t)(-1);

   closedir(dir);
   free( dent );
   free( base_path_dir );
   free( base_path_basename );

   return ret;
}


// clear out old versions of a block.
// preserve a block with current_block_version.
// fent must be at least read_locked.
int fs_entry_remove_old_block_versions( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t current_block_version, bool staging ) {
   int rc = 0;

   // get the location of this block
   char* local_block_url_prefix = fs_entry_get_block_storage_url( core, fent->file_id, fent->version, block_id, 0, staging );
   md_clear_version( local_block_url_prefix );

   char* block_path = GET_PATH( local_block_url_prefix );

   int64_t* versions = fs_entry_read_versions( block_path );

   for( int i = 0; versions != NULL && versions[i] >= 0; i++ ) {
      if( versions[i] == current_block_version ) {
         // ignore the current block
         continue;
      }

      char* block_versioned_path = fs_entry_add_version( block_path, versions[i] );

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

// reversion a modified block
// fent must be at least read_locked.
int fs_entry_reversion_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t old_block_version, int64_t new_block_version, bool staging ) {
   int rc = 0;

   // get the location of this block
   char* old_block_url_prefix = fs_entry_get_block_storage_url( core, fent->file_id, fent->version, block_id, old_block_version, staging );
   char* new_block_url_prefix = fs_entry_get_block_storage_url( core, fent->file_id, fent->version, block_id, new_block_version, staging );

   char* old_block_path = GET_PATH( old_block_url_prefix );
   char* new_block_path = GET_PATH( new_block_url_prefix );
   
   rc = rename( old_block_path, new_block_path );
   if( rc != 0 ) 
      rc = -errno;
   
   free( old_block_url_prefix );
   free( new_block_url_prefix );
   
   return rc;
}


// write a block to a file, hosting it on underlying storage, and updating the filesystem entry's manifest to refer to it.
// if the URL refers to a local place on disk, then store it to the data directory.
// If it instead refers to a remote host, then store it to the staging directory.
// fent MUST BE WRITE LOCKED, SINCE WE MODIFY THE MANIFEST
ssize_t fs_entry_put_block_data( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, char* block_data, size_t len, unsigned char* block_hash, bool staging ) {
   
   int64_t old_block_version = fent->manifest->get_block_version( block_id );
   int64_t new_block_version = fs_entry_next_block_version();

   dbprintf("put /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " --> .%" PRId64 "\n", core->gateway, core->volume, fent->file_id, fent->version, block_id, old_block_version, new_block_version );
   
   char prefix[21];
   memset( prefix, 0, 21 );
   memcpy( prefix, block_data, MIN( 20, core->blocking_factor ) );
   
   dbprintf("data: '%s'...\n", prefix );
   
   // put the block data into place
   ssize_t rc = fs_entry_commit_block_data( core, fent->file_id, fent->version, block_id, old_block_version, block_data, len, staging );
   if( (unsigned)rc != len ) {
      // failed to write
      errorf("fs_entry_commit_block( /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 ".%" PRId64 " ) rc = %zd\n", core->gateway, core->volume, fent->file_id, fent->version, block_id, old_block_version, rc );
      return -EIO;
   }
   else {
      rc = fs_entry_reversion_block( core, fent, block_id, old_block_version, new_block_version, staging );
      if( rc != 0 ) {
         // failed to reversion
         errorf("WARN: fs_entry_reversion_block( /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 "/%" PRId64 " --> .%" PRId64 " rc = %zd\n",
                core->gateway, core->volume, fent->file_id, fent->version, block_id, old_block_version, new_block_version, rc );
         
         return rc;
      }

      rc = fs_entry_manifest_put_block( core, core->gateway, fent, block_id, new_block_version, block_hash, staging );
      if( rc != 0 ) {
         errorf("fs_entry_manifest_put_block( /%" PRIu64 "/%" PRIu64 "/%" PRIX64 ".%" PRId64 "/%" PRIu64 "/%" PRId64 " --> .%" PRId64 " rc = %zd\n",
                core->gateway, core->volume, fent->file_id, fent->version, block_id, old_block_version, new_block_version, rc );
         return rc;
      }
      
      // update our modtime
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );

      fent->mtime_sec = ts.tv_sec;
      fent->mtime_nsec = ts.tv_nsec;
      
      rc = (ssize_t)len;
   }
   return rc;
}


// remove a locally-hosted block from a file, either from staging or local data.
// fent must be at least read-locked
int fs_entry_remove_block( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, bool staging ) {
   return fs_entry_remove_old_block_versions( core, fent, block_id, -1, staging);
}


// collate a block back into a file, given the block data
// return negative on error
// return the next version of this block
// fent must be write-locked
int fs_entry_collate( struct fs_core* core, struct fs_entry* fent, uint64_t block_id, int64_t block_version, char* bits, uint64_t block_len, uint64_t parent_id, char const* parent_name ) {

   char tmppath[PATH_MAX];
   strcpy( tmppath, SYNDICATE_COLLATE_TMPPATH );

   int fd = mkstemp( tmppath );
   if( fd < 0 ) {
      fd = -errno;
      errorf("mkstemp errno = %d\n", fd);
      return fd;
   }

   ssize_t wrc = fs_entry_write_block_data( core, fd, bits, block_len );
   if( wrc < 0 ) {
      errorf("fs_entry_write_block rc = %zd\n", wrc );
      close( fd );
      unlink( tmppath );
      return (int)wrc;
   }

   close( fd );
   
   // put the block in place
   char* block_url = fs_entry_local_block_url( core, fent->file_id, fent->version, block_id, block_version );
   char* block_path = GET_PATH( block_url );
   
   // make sure the directories exist
   char* block_dir = md_dirname( block_path, NULL );
   int rc = md_mkdirs3( block_dir, fent->mode | 0700 );
   if( rc != 0 && rc != -EEXIST ) {
      errorf("md_mkdirs3(%s) rc = %d\n", block_dir, rc );
      unlink( tmppath );
      free( block_dir );
      return rc;
   }
   
   free( block_dir );
   
   // move the data into place
   rc = rename( tmppath, block_path );

   if( rc != 0 ) {
      rc = -errno;
      errorf( "rename(%s,%s) errno = %d\n", tmppath, block_path, rc );
      free( block_url );
      return -EIO;
   }
   
   // hash the block
   fd = open( block_path, O_RDONLY );
   if( fd < 0 ) {
      rc = -errno;
      errorf("open(%s) errno = %d\n", block_path, rc );
      free( block_url );
      return -EIO;
   }
   
   unsigned char* block_hash = BLOCK_HASH_FD( fd );
   
   close( fd );
   
   if( block_hash == NULL ) {
      rc = -errno;
      errorf("Failed to hash block %s, errno = %d\n", block_path, rc );
      free( block_url );
      return -EIO;
   }
   
   free( block_url );
   
   // add the block 
   fs_entry_manifest_put_block( core, core->gateway, fent, block_id, block_version, block_hash, false );

   free( block_hash );
   
   // update timestamp on the MS
   struct md_entry data;
   fs_entry_to_md_entry( core, &data, fent, parent_id, parent_name );

   ms_client_queue_update( core->ms, &data, currentTimeMillis() + fent->max_write_freshness, 0 );

   md_entry_free( &data );
   
   
   dbprintf("Collated /%" PRIX64 "/%" PRId64 ".%" PRIu64 " (%s)\n", fent->file_id, block_id, block_version, fent->name );
   
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

      return err;
   }
   
   // check file_id match
   if( fent->file_id != accept_msg->accepted().file_id() ) {
      errorf("File ID mismatch: received %" PRIu64 ", expected %" PRIu64 "\n", accept_msg->accepted().file_id(), fent->file_id );
      fs_entry_unlock( fent );
      return -EINVAL;
   }


   if( FS_ENTRY_LOCAL( core, fent ) ) {
      // only remote files have staging information
      fs_entry_unlock( fent );
      return -EINVAL;
   }

   // mark the manifest as stale so we refresh it on the next I/O operation
   fent->manifest->mark_stale();

   int rc = 0;

   // remove all of the blocks we're holding for this file if they're the same version
   int64_t file_version = accept_msg->accepted().file_version();
   if( fent->version != file_version ) {
      errorf("ERR: %s: local file is a different version (%" PRId64 ") than accepted blocks (%" PRId64 ")\n", fs_path, fent->version, file_version );
      rc = 0;
   }
   else {
      // remove staging
      for( int i = 0; i < accept_msg->accepted().block_id_size(); i++ ) {

         uint64_t block_id = accept_msg->accepted().block_id(i);

         int rc = fs_entry_remove_block( core, fent, block_id, true );
         if( rc != 0 ) {
            errorf("fs_entry_remove_block(%s[%" PRId64 "]) rc = %d\n", fs_path, block_id, rc );
         }
      }

      // clean up the staging directory
      char* tmp = md_fullpath( core->conf->staging_root, fs_path, NULL );
      char* dir_fullpath = fs_entry_add_version( tmp, fent->version );

      // this will only succeed if the directory is empty, which is exactly what we want.
      // no need to worry about it if it's not empty
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
   if( !FS_ENTRY_LOCAL( core, fent ) ) {
      fs_entry_unlock( fent );
      return -EXDEV;
   }

   
   char* block_url = fent->manifest->get_block_url( core, path, fent, block_id );
   char* stat_path = GET_PATH( block_url );
   int rc = stat( stat_path, sb );

   fs_entry_unlock( fent );

   free( block_url );
   
   return rc;
}

