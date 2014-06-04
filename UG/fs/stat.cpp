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

#include "stat.h"
#include "manifest.h"
#include "consistency.h"

// get the in-memory version of a file
int64_t fs_entry_get_version( struct fs_core* core, char const* fs_path ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   int64_t ret = fent->version;

   fs_entry_unlock( fent );
   return ret;
}

// calculate the version of a block
int64_t fs_entry_get_block_version( struct fs_core* core, char* fs_path, uint64_t block_id ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   if( fent->manifest == NULL ) {
      fs_entry_unlock( fent );
      return -ENODATA;
   }

   int64_t ret = fent->manifest->get_block_version( block_id );

   fs_entry_unlock( fent );
   return ret;
}

// get the gateway coordinator of a file
uint64_t fs_entry_get_block_host( struct fs_core* core, char* fs_path, uint64_t block_id ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   if( fent->manifest == NULL ) {
      fs_entry_unlock( fent );
      return -ENODATA;
   }
   
   uint64_t ret = fent->manifest->get_block_host( core, block_id );

   fs_entry_unlock( fent );
   return ret;
}

// get a file manifest as a string
char* fs_entry_get_manifest_str( struct fs_core* core, char* fs_path ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      return NULL;
   }

   char* ret = fent->manifest->serialize_str();
   fs_entry_unlock( fent );

   return ret;
}


// serialize the manifest from a locked fent
ssize_t fs_entry_serialize_manifest( struct fs_core* core, struct fs_entry* fent, char** manifest_bits, bool sign ) {

   Serialization::ManifestMsg mmsg;
   fent->manifest->as_protobuf( core, fent, &mmsg );

   if( sign ) {
      int rc = md_sign< Serialization::ManifestMsg >( core->ms->my_key, &mmsg );
      if( rc != 0 ) {
         errorf("gateway_sign_manifest rc = %d\n", rc );
         *manifest_bits = NULL;
         return rc;
      }
   }
   else {
      mmsg.set_signature("");
   }
   
   size_t manifest_bits_len = 0;
   
   int rc = md_serialize< Serialization::ManifestMsg >( &mmsg, manifest_bits, &manifest_bits_len );
   if( rc != 0 ) {
      errorf("md_serialize rc = %d\n", rc );
      return rc;
   }
   
   return (ssize_t)manifest_bits_len;
}


// get a file manifest as a serialized protobuf
ssize_t fs_entry_serialize_manifest( struct fs_core* core, char* fs_path, char** manifest_bits, bool sign ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      *manifest_bits = NULL;
      return err;
   }

   ssize_t ret = fs_entry_serialize_manifest( core, fent, manifest_bits, sign );

   fs_entry_unlock( fent );

   return ret;
}

// get the actual creation time
// get the mod time in its entirety
int fs_entry_get_creation_time( struct fs_core* core, char const* fs_path, struct timespec* t ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   t->tv_sec = fent->ctime_sec;
   t->tv_nsec = fent->ctime_nsec;

   fs_entry_unlock( fent );
   return 0;
}

// get the mod time in its entirety
int fs_entry_get_mod_time( struct fs_core* core, char const* fs_path, struct timespec* t ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   t->tv_sec = fent->mtime_sec;
   t->tv_nsec = fent->mtime_nsec;

   fs_entry_unlock( fent );
   return 0;
}


// set the mod time (at the nanosecond resolution)
int fs_entry_set_mod_time( struct fs_core* core, char const* fs_path, struct timespec* t ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   fent->mtime_sec = t->tv_sec;
   fent->mtime_nsec = t->tv_nsec;

   fs_entry_unlock( fent );
   return 0;
}

// basic stat (called from fs_entry_stat or fs_entry_fstat)
// fent must be at least read-locked
static int fs_entry_do_stat( struct fs_core* core, struct fs_entry* fent, struct stat* sb ) {
   int rc = 0;

   memset( sb, 0, sizeof(struct stat) );
   
   sb->st_dev = 0;
   sb->st_ino = (ino_t)fent->file_id;   // NOTE: must support 64-bit inodes

   mode_t ftype = 0;
   if( fent->ftype == FTYPE_FILE )
      ftype = S_IFREG;
   else if( fent->ftype == FTYPE_DIR )
      ftype = S_IFDIR;
   else if( fent->ftype == FTYPE_FIFO )
      ftype = S_IFIFO;

   sb->st_mode = ftype | fent->mode;
   sb->st_nlink = fent->link_count;
   sb->st_uid = fent->owner;
   sb->st_gid = fent->volume;
   sb->st_rdev = 0;

   sb->st_blksize = core->blocking_factor;
   sb->st_blocks = (fent->size / core->blocking_factor);
   if( fent->size % core->blocking_factor != 0 )
      sb->st_blocks++;

   sb->st_atime = fent->atime;
   sb->st_ctime = fent->ctime_sec;
   sb->st_mtime = fent->mtime_sec;
   sb->st_size = fent->size;


   /*
   printf("\nfent %s\n", fent->name );
   printf( " mode = %o\n", sb->st_mode );
   printf( " uid  = %d\n", sb->st_uid );
   printf( " gid  = %d\n", sb->st_gid );
   printf( " size = %ld\n", sb->st_size );
   printf( " blks = %ld\n\n", sb->st_blocks );
   */
   return rc;
}



// stat
int fs_entry_stat_extended( struct fs_core* core, char const* path, struct stat* sb, bool* is_local, uint64_t user, uint64_t volume, bool revalidate ) {

   int rc = 0;
   
   if( revalidate ) {
      // revalidate
      rc = fs_entry_revalidate_path( core, volume, path );
      if( rc != 0 ) {
         errorf("fs_entry_revalidate_path(%s) rc = %d\n", path, rc );
         return rc;
      }
   }
   
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   memset( sb, 0, sizeof(struct stat) );

   // have entry read-locked
   fs_entry_do_stat( core, fent, sb );

   if( is_local ) {
      *is_local = FS_ENTRY_LOCAL( core, fent );
   }
   
   fs_entry_unlock( fent );

   return 0;
}

// stat
int fs_entry_stat( struct fs_core* core, char const* path, struct stat* sb, uint64_t user, uint64_t volume ) {
   return fs_entry_stat_extended( core, path, sb, NULL, user, volume, true );
}



// is this local?  That is, is the block hosted here?
bool fs_entry_is_block_local( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, uint64_t block_id ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   bool rc = fent->manifest->is_block_local( core, block_id );

   fs_entry_unlock( fent );

   return rc;
}

// is a file local?
bool fs_entry_is_local( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, int* err ) {
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, err );
   if( !fent || *err ) {
      if( !*err )
         *err = -ENOMEM;

      return false;
   }

   bool rc = FS_ENTRY_LOCAL( core, fent );
   fs_entry_unlock( fent );
   return rc;
}

// fstat
int fs_entry_fstat( struct fs_core* core, struct fs_file_handle* fh, struct stat* sb ) {
   int rc = fs_file_handle_rlock( fh );
   if( rc != 0 ) {
      errorf("fs_file_handle_rlock rc = %d\n", rc );
      return -EBADF;
   }

   // revalidate
   rc = fs_entry_revalidate_path( core, fh->volume, fh->path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      
      if( rc == -ENOENT ) {
         // file no longer exists
         return -EBADF;
      }
      
      return -EREMOTEIO;
   }
   
   rc = fs_entry_rlock( fh->fent );
   if( rc != 0 ) {
      errorf("fs_entry_rlock rc = %d\n", rc );
      fs_file_handle_unlock( fh );
      return -EBADF;
   }

   fs_entry_do_stat( core, fh->fent, sb );

   fs_entry_unlock( fh->fent );
   fs_file_handle_unlock( fh );
   return 0;
}

// fstat, with directory
int fs_entry_fstat_dir( struct fs_core* core, struct fs_dir_handle* dh, struct stat* sb ) {
   if( fs_dir_handle_rlock( dh ) != 0 ) {
      return -EBADF;
   }

   // revalidate
   int rc = fs_entry_revalidate_path( core, dh->volume, dh->path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", dh->path, rc );
      fs_dir_handle_unlock( dh );
      return -EREMOTEIO;
   }
   
   if( fs_entry_rlock( dh->dent ) != 0 ) {
      fs_dir_handle_unlock( dh );
      return -EBADF;
   }

   fs_entry_do_stat( core, dh->dent, sb );

   fs_entry_unlock( dh->dent );
   fs_dir_handle_unlock( dh );
   return 0;
}


// statfs
int fs_entry_statfs( struct fs_core* core, char const* path, struct statvfs *statv, uint64_t user, uint64_t vol ) {
   // make sure this path refers to a path in the FS
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, vol, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   uint64_t num_files = ms_client_get_num_files( core->ms );

   // populate the statv struct
   statv->f_bsize = core->blocking_factor;
   statv->f_blocks = 0;
   statv->f_bfree = 0;
   statv->f_bavail = 0;
   statv->f_files = num_files;
   statv->f_ffree = 0;
   statv->f_fsid = SYNDICATEFS_MAGIC;
   statv->f_namemax = 256;    // might as well keep it limited to what ext2/ext3/ext4 can handle
   statv->f_frsize = 0;
   statv->f_flag = ST_NODEV | ST_NOSUID;
   
   fs_entry_unlock( fent );

   return 0;
}

// access
int fs_entry_access( struct fs_core* core, char const* path, int mode, uint64_t user, uint64_t volume ) {
   // make sure this path exists
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // F_OK implicitly satisfied

   if( (mode & R_OK) && !IS_READABLE( fent->mode, fent->owner, fent->volume, user, volume ) ) {
      err = -EACCES;
   }
   else if( (mode & W_OK) && !IS_WRITEABLE( fent->mode, fent->owner, fent->volume, user, volume ) ) {
      err = -EACCES;
   }
   else if( (mode & X_OK) && !IS_EXECUTABLE( fent->mode, fent->owner, fent->volume, user, volume ) ) {
      err = -EACCES;
   }

   fs_entry_unlock( fent );
   return err;
}

// chown
int fs_entry_chown( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, uint64_t new_user ) {
   // TODO: ms_client_claim
   return -ENOSYS;
}


// chmod
int fs_entry_chmod( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, mode_t mode ) {
   int err = 0;
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, path, user, volume, true, &err, &parent_id, &parent_name );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // can't chmod unless we own the file
   if( fent->owner != user ) {
      fs_entry_unlock( fent );
      free( parent_name );
      return -EPERM;
   }

   fent->mode = mode;
   
   // post update
   struct md_entry up;
   fs_entry_to_md_entry( core, &up, fent, parent_id, parent_name );

   int rc = ms_client_queue_update( core->ms, &up, currentTimeMillis() + fent->max_write_freshness, 0 );
   if( rc != 0 ) {
      errorf("ms_client_queue_update(%s) rc = %d\n", path, rc );
   }

   md_entry_free( &up );
   fs_entry_unlock( fent );
   free( parent_name );

   return rc;
}

// utime
int fs_entry_utime( struct fs_core* core, char const* path, struct utimbuf* tb, uint64_t user, uint64_t volume ) {
   int err = 0;
   uint64_t parent_id = 0;
   char* parent_name = NULL;
   
   struct fs_entry* fent = fs_entry_resolve_path_and_parent_info( core, path, user, volume, true, &err, &parent_id, &parent_name );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // check permissions
   if( tb == NULL && !IS_WRITEABLE( fent->mode, fent->owner, fent->volume, user, volume ) ) {
      fs_entry_unlock( fent );
      return -EACCES;
   }
   if( tb != NULL && fent->owner != user ) {
      fs_entry_unlock( fent );
      return -EACCES;
   }

   if( tb != NULL ) {
      fent->mtime_sec = tb->modtime;
      fent->atime = tb->actime;
   }
   else {
      struct timespec ts;
      clock_gettime( CLOCK_REALTIME, &ts );

      fent->mtime_sec = ts.tv_sec;
      fent->mtime_nsec = ts.tv_nsec;
      fent->atime = fent->mtime_sec;
   }

   fent->atime = currentTimeSeconds();

   // post update
   struct md_entry up;
   fs_entry_to_md_entry( core, &up, fent, parent_id, parent_name );

   int rc = ms_client_queue_update( core->ms, &up, currentTimeSeconds() + fent->max_write_freshness, 0 );
   if( rc != 0 ) {
      errorf("ms_client_queue_update(%s) rc = %d\n", path, rc );
   }

   md_entry_free( &up );
   fs_entry_unlock( fent );
   return rc;
}

