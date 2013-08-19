/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "stat.h"
#include "manifest.h"
#include "consistency.h"

// get the lastmod of a manifest
int fs_entry_manifest_lastmod( struct fs_core* core, char const* fs_path, struct timespec* ts ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   fent->manifest->get_lastmod( ts );

   fs_entry_unlock( fent );
   return err;
}


/*
// calculate the version of a file, from disk
int64_t fs_entry_read_version( struct fs_core* core, char const* fs_path ) {
   char* fp = md_fullpath( core->conf->publish_dir, fs_path, NULL );

   char** versioned_paths = md_versioned_paths( fp );
   if( versioned_paths == NULL ) {
      errorf("fs_entry_read_version: failed to read %s\n", fs_path );
      return -1;
   }

   int64_t highest_version = md_next_version( versioned_paths ) - 1;

   FREE_LIST( versioned_paths );
   free( fp );

   return highest_version;
}
*/


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
ssize_t fs_entry_serialize_manifest( struct fs_core* core, struct fs_entry* fent, char** manifest_bits ) {

   Serialization::ManifestMsg mmsg;
   fent->manifest->as_protobuf( core, fent, &mmsg );

   string mb;
   bool valid = mmsg.SerializeToString( &mb );
   if( !valid ) {
      *manifest_bits = NULL;
      return -EINVAL;
   }

   *manifest_bits = CALLOC_LIST( char, mb.size() );
   memcpy( *manifest_bits, mb.data(), mb.size() );

   return (ssize_t)mb.size();
}


// get a file manifest as a serialized protobuf
ssize_t fs_entry_serialize_manifest( struct fs_core* core, char* fs_path, char** manifest_bits ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, fs_path, SYS_USER, 0, false, &err );
   if( !fent || err ) {
      *manifest_bits = NULL;
      return err;
   }

   ssize_t ret = fs_entry_serialize_manifest( core, fent, manifest_bits );

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
// NOTE: make sure the fs_entry is locked!
static int fs_entry_do_stat( struct fs_core* core, struct fs_entry* fent, struct stat* sb ) {
   int rc = 0;

   memset( sb, 0, sizeof(sb) );
   
   sb->st_dev = 0;
   //sb->st_ino = (ino_t)fent;

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
         return -EREMOTEIO;
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
      *is_local = URL_LOCAL( fent->url );
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

   bool rc;

   // search the entry for this block
   char* block_url = fent->manifest->get_block_url( fent->version, block_id );
   if( block_url == NULL ) {
      // not here
      rc = false;
   }
   else {
      rc = URL_LOCAL( block_url );
      free( block_url );
   }

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

   bool rc = URL_LOCAL( fent->url );
   fs_entry_unlock( fent );
   return rc;
}

// get a file's url
char* fs_entry_get_url( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, int* err ) {
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, err );
   if( !fent || *err ) {
      if( !*err )
         *err = -ENOMEM;

      return NULL;
   }

   char* url = NULL;
   if( fent->url )
      url = strdup( fent->url );

   fs_entry_unlock( fent );
   return url;
}

// get proto://file_host:file_portnum/
char* fs_entry_get_host_url( struct fs_core* core, char const* path, char const* proto, uint64_t user, uint64_t volume, int* err ) {
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, false, err );
   if( !fent || *err ) {
      if( !*err )
         *err = -ENOMEM;

      return NULL;
   }

   char* hostname = md_url_hostname( fent->url );
   int portnum = md_portnum_from_url( fent->url );

   char* host_url = md_prepend( proto, hostname, NULL );
   if( portnum > 0 ) {
      char buf[10];
      sprintf(buf, ":%d", portnum);

      char* tmp = host_url;
      host_url = md_prepend( host_url, buf, NULL );
      free( tmp );
   }
   free( hostname );

   fs_entry_unlock( fent );
   return host_url;
}

// fstat
int fs_entry_fstat( struct fs_core* core, struct fs_file_handle* fh, struct stat* sb ) {
   if( fs_file_handle_rlock( fh ) != 0 ) {
      return -EBADF;
   }

   // revalidate
   int rc = fs_entry_revalidate_path( core, fh->volume, fh->path );
   if( rc != 0 ) {
      errorf("fs_entry_revalidate_path(%s) rc = %d\n", fh->path, rc );
      fs_file_handle_unlock( fh );
      return -EREMOTEIO;
   }
   
   if( fs_entry_rlock( fh->fent ) != 0 ) {
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

   unsigned long int num_files = 0;
   fs_core_rlock( core );
   num_files = core->num_files;
   fs_core_unlock( core );

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
   return -ENOSYS;
}


// chmod
int fs_entry_chmod( struct fs_core* core, char const* path, uint64_t user, uint64_t volume, mode_t mode ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, true, &err );
   if( !fent || err ) {
      if( !err )
         err = -ENOMEM;

      return err;
   }

   // can't chmod remote files
   if( !URL_LOCAL( fent->url ) ) {
      fs_entry_unlock( fent );
      return -EINVAL;
   }

   // can't chmod unless we own the file
   if( fent->owner != user ) {
      fs_entry_unlock( fent );
      return -EPERM;
   }

   fent->mode = mode;

   fs_entry_unlock( fent );
   return 0;
}

// utime
int fs_entry_utime( struct fs_core* core, char const* path, struct utimbuf* tb, uint64_t user, uint64_t volume ) {
   int err = 0;
   struct fs_entry* fent = fs_entry_resolve_path( core, path, user, volume, true, &err );
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

      fent->manifest->set_lastmod( &ts );
   }

   fent->atime = currentTimeSeconds();

   fs_entry_unlock( fent );
   return 0;
}

