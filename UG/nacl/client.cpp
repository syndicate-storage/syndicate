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

#include "client.h"


/*
 * Get file attributes (lstat)
 */
int syndicate_getattr(const char *path, struct stat *statbuf) {
   
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_getattr( %s, %p )\n", pthread_self(), path, statbuf );
   
   SYNDICATE_DATA->stats->enter( STAT_GETATTR );
   
   int rc = fs_entry_stat( SYNDICATE_DATA->core, path, statbuf, conf->owner, SYNDICATE_DATA->core->volume );
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_getattr rc = %d\n", pthread_self(), rc );
   
   SYNDICATE_DATA->stats->leave( STAT_GETATTR, rc );
   
   return rc;
}

/** Create a directory (mkdir) */
int syndicate_mkdir(const char *path, mode_t mode) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_mkdir( %s, %o )\n", pthread_self(), path, mode );
   
   SYNDICATE_DATA->stats->enter( STAT_MKDIR );
   
   int rc = fs_entry_mkdir( SYNDICATE_DATA->core, path, mode, conf->owner, SYNDICATE_DATA->core->volume );
   
   SYNDICATE_DATA->stats->leave( STAT_MKDIR, rc );
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_mkdir rc = %d\n", pthread_self(), rc );
   return rc;
}

/** Remove a file (unlink) */
int syndicate_unlink(const char* path) {
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_unlink( %s )\n", pthread_self(), path );

   SYNDICATE_DATA->stats->enter( STAT_UNLINK );
   
   int rc = fs_entry_versioned_unlink( SYNDICATE_DATA->core, path, 0, 0, -1, SYNDICATE_DATA->conf.owner, SYNDICATE_DATA->core->volume, SYNDICATE_DATA->core->gateway, false );

   SYNDICATE_DATA->stats->leave( STAT_UNLINK, rc );
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_unlink rc = %d\n", pthread_self(), rc);
   return rc;
}

/** Remove a directory (rmdir) */
int syndicate_rmdir(const char *path) {
   
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_rmdir( %s )\n", pthread_self(), path );
   
   SYNDICATE_DATA->stats->enter( STAT_RMDIR );
   
   int rc = fs_entry_rmdir( SYNDICATE_DATA->core, path, conf->owner, SYNDICATE_DATA->core->volume );
   
   SYNDICATE_DATA->stats->leave( STAT_RMDIR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_rmdir rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Rename a file.  Paths are FS-relative! (rename) */
int syndicate_rename(const char *path, const char *newpath) {
   
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_rename( %s, %s )\n", pthread_self(), path, newpath );
   
   SYNDICATE_DATA->stats->enter( STAT_RENAME );

   //int rc = fs_entry_versioned_rename( SYNDICATE_DATA->core, path, newpath, conf->owner, SYNDICATE_DATA->core->volume, -1 );
   int rc = fs_entry_rename( SYNDICATE_DATA->core, path, newpath, conf->owner, SYNDICATE_DATA->core->volume );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_rename( %s, %s ) rc = %d\n", pthread_self(), path, newpath, rc );
   
   SYNDICATE_DATA->stats->leave( STAT_RENAME, rc );
   return rc;
}


/** Change the permission bits of a file (chmod) */
int syndicate_chmod(const char *path, mode_t mode) {
   
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_chmod( %s, %o )\n", pthread_self(), path, mode );
   
   SYNDICATE_DATA->stats->enter( STAT_CHMOD );
   
   int rc = fs_entry_chmod( SYNDICATE_DATA->core, path, conf->owner, SYNDICATE_DATA->core->volume, mode );
   if( rc == 0 ) {
      // TODO: update the modtime and metadata of this file
   }
   
   SYNDICATE_DATA->stats->leave( STAT_CHMOD, rc );
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_chmod rc = %d\n");
   return rc;
}


/** Change the coordinator of a file */
int syndicate_chown(const char *path, uint64_t new_coordinator ) {
   /*
    * TODO
   */
   return -ENOSYS;
}


/** Change the size of a file (truncate) */
/* only works on local files */
int syndicate_truncate(const char *path, off_t newsize) {
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_truncate( %s, %ld )\n", pthread_self(), path, newsize );

   SYNDICATE_DATA->stats->enter( STAT_TRUNCATE );

   int rc = fs_entry_truncate( SYNDICATE_DATA->core, path, newsize, conf->owner, SYNDICATE_DATA->core->volume );

   SYNDICATE_DATA->stats->leave( STAT_TRUNCATE, rc );
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_truncate rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Change the access and/or modification times of a file (utime) */
int syndicate_utime(const char *path, struct utimbuf *ubuf) {
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_utime( %s, {%d, %d} )\n", pthread_self(), path, ubuf->actime, ubuf->modtime );
   
   SYNDICATE_DATA->stats->enter( STAT_UTIME );
   
   int rc = fs_entry_utime( SYNDICATE_DATA->core, path, ubuf, conf->owner, SYNDICATE_DATA->core->volume );
   if( rc == 0 ) {
      // TODO: update the modtime of this file
   }
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_utime rc = %d\n", pthread_self(), rc);
   SYNDICATE_DATA->stats->leave( STAT_UTIME, rc );
   return rc;
}



/** File open operation (O_CREAT and O_EXCL will *not* be passed to this method, according to the documentation) */
syndicate_handle_t* syndicate_open(const char *path, int flags ) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_open( %s, %d )\n", pthread_self(), path, flags );
   
   SYNDICATE_DATA->stats->enter( STAT_OPEN );
   
   int err = 0;
   struct fs_file_handle* fh = fs_entry_open( SYNDICATE_DATA->core, path, conf->owner, SYNDICATE_DATA->core->volume, flags, ~conf->usermask, &err );
   
   // store th handle
   syndicate_handle_t* sh = CALLOC_LIST( syndicate_handle_t, 1 );
   sh->type = FTYPE_FILE;
   sh->fh = fh;
   
   SYNDICATE_DATA->stats->leave( STAT_OPEN, err );
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_open rc = %d\n", pthread_self(), err );
   
   return sh;
}


/** Read data from an open file.  Return number of bytes read. */
int syndicate_read(const char *path, char *buf, size_t size, off_t offset, syndicate_handle_t *fi) {
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_read( %s, %p, %ld, %ld, %p )\n", pthread_self(), path, buf, size, offset, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_READ );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      SYNDICATE_DATA->stats->leave( STAT_READ, -1 );
      logerr( SYNDICATE_DATA->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   ssize_t rc = fs_entry_read( SYNDICATE_DATA->core, fh, buf, size, offset );
   
   if( rc < 0 ) {
      SYNDICATE_DATA->stats->leave( STAT_READ, -1 );
      logerr( SYNDICATE_DATA->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
      return -1;
   }
   
   // fill the remainder of buf with 0's
   if( rc < (signed)size ) {
      memset( buf + rc, 0, size - rc );
   }
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
   
   SYNDICATE_DATA->stats->leave( STAT_READ, (rc >= 0 ? 0 : rc) );
   return rc;
}


/** Write data to an open file (pwrite) */
int syndicate_write(const char *path, const char *buf, size_t size, off_t offset, syndicate_handle_t *fi) {

   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_write( %s, %p, %ld, %ld, %p )\n", pthread_self(), path, buf, size, offset, fi->fh );
   
   SYNDICATE_DATA->stats->enter( STAT_WRITE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      SYNDICATE_DATA->stats->leave( STAT_WRITE, (rc >= 0 ? 0 : rc)  );
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_write rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   ssize_t rc = fs_entry_write( SYNDICATE_DATA->core, fh, buf, size, offset );
   
   SYNDICATE_DATA->stats->leave( STAT_WRITE, (rc >= 0 ? 0 : rc)  );
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_write rc = %d\n", pthread_self(), rc );
   return (int)rc;
}


/** Possibly flush cached data (No-op) */
int syndicate_flush(const char *path, syndicate_handle_t *fi) {
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_flush( %s, %p )\n", pthread_self(), path, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_FLUSH );

   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      SYNDICATE_DATA->stats->leave( STAT_FLUSH, rc );
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_flush rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   
   int rc = fs_entry_fsync( SYNDICATE_DATA->core, fh );
   
   SYNDICATE_DATA->stats->leave( STAT_FLUSH, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_flush rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Release an open file (close) */
int syndicate_close(const char *path, syndicate_handle_t *fi) {

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_close( %s, %p )\n", pthread_self(), path, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_RELEASE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_close rc = %d\n", pthread_self(), rc );
      SYNDICATE_DATA->stats->leave( STAT_RELEASE, rc );
   }
      
   struct fs_file_handle* fh = fi->fh;
   
   int rc = fs_entry_close( SYNDICATE_DATA->core, fh );
   if( rc != 0 ) {
      logerr( SYNDICATE_DATA->logfile, "%16lx: syndicate_close: fs_entry_close rc = %d\n", pthread_self(), rc );
   }
   
   fi->fh = NULL;
   free( fh );
   free( fi );
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_close rc = %d\n", pthread_self(), rc );
   
   SYNDICATE_DATA->stats->leave( STAT_RELEASE, rc );
   return rc;
}


/** Synchronize file contents (fdatasync, fsync)
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 */
int syndicate_fsync(const char *path, int datasync, syndicate_handle_t *fi) {
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fsync( %s, %d, %p )\n", pthread_self(), path, datasync, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_FSYNC );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      
      SYNDICATE_DATA->stats->leave( STAT_FSYNC, rc );
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fsync rc = %d\n", pthread_self(), rc );
      
      return rc;
   }
      
   struct fs_file_handle* fh = fi->fh;
   int rc = 0;
   if( datasync == 0 )
      rc = fs_entry_fdatasync( SYNDICATE_DATA->core, fh );
   
   if( rc == 0 )
      fs_entry_fsync( SYNDICATE_DATA->core, fh );
      
   SYNDICATE_DATA->stats->leave( STAT_FSYNC, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fsync rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Set extended attributes (lsetxattr) */
int syndicate_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   char* safe_value = (char*)calloc( size + 1, 1 );
   strncpy( safe_value, value, size );
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_setxattr( %s, %s, %s, %d, %x )\n", pthread_self(), path, name, safe_value, size, flags );
   free( safe_value );
   
   SYNDICATE_DATA->stats->enter( STAT_SETXATTR );
   
   int rc = fs_entry_setxattr( SYNDICATE_DATA->core, path, name, value, size, flags, conf->owner, SYNDICATE_DATA->core->volume );
   
   SYNDICATE_DATA->stats->leave( STAT_SETXATTR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_setxattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Get extended attributes (lgetxattr) */
int syndicate_getxattr(const char *path, const char *name, char *value, size_t size) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_getxattr( %s, %s, %p, %d )\n", pthread_self(), path, name, value, size );
   
   SYNDICATE_DATA->stats->enter( STAT_GETXATTR );
   
   int rc = fs_entry_getxattr( SYNDICATE_DATA->core, path, name, value, size, conf->owner, SYNDICATE_DATA->core->volume );
   
   SYNDICATE_DATA->stats->leave( STAT_GETXATTR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_getxattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** List extended attributes (llistxattr) */
int syndicate_listxattr(const char *path, char *list, size_t size) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_listxattr( %s, %p, %d )\n", pthread_self(), path, list, size );
   
   SYNDICATE_DATA->stats->enter( STAT_LISTXATTR );
   
   int rc = fs_entry_listxattr( SYNDICATE_DATA->core, path, list, size, conf->owner, SYNDICATE_DATA->core->volume );
   
   SYNDICATE_DATA->stats->leave( STAT_LISTXATTR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_listxattr rc = %d\n", pthread_self(), rc );
   
   return rc;
}


/** Remove extended attributes (lremovexattr) */
int syndicate_removexattr(const char *path, const char *name) {
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_removexattr( %s, %s )\n", pthread_self(), path, name );
   
   SYNDICATE_DATA->stats->enter( STAT_REMOVEXATTR );
   
   int rc = fs_entry_removexattr( SYNDICATE_DATA->core, path, name, conf->owner, SYNDICATE_DATA->core->volume );

   SYNDICATE_DATA->stats->leave( STAT_REMOVEXATTR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_removexattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Open directory (opendir) */
syndicate_handle_t* syndicate_opendir(const char *path) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_opendir( %s )\n", pthread_self(), path );
   
   SYNDICATE_DATA->stats->enter( STAT_OPENDIR );

   int rc = 0;
   struct fs_dir_handle* fdh = fs_entry_opendir( SYNDICATE_DATA->core, path, conf->owner, SYNDICATE_DATA->core->volume, &rc );
   syndicate_handle_t* ret = NULL;
   
   if( rc == 0 ) {
      ret = CALLOC_LIST( syndicate_handle_t, 1 );
      ret->fdh = fdh;
   }
   
   SYNDICATE_DATA->stats->leave( STAT_OPENDIR, rc );
   
   logmsg( SYNDICATE_DATA->logfile,  "%16lx: syndicate_opendir rc = %d\n", pthread_self(), rc );
   
   return ret;
}


/** Read directory (readdir)
 */
int syndicate_readdir(const char *path, syndicate_dir_listing_t* listing, syndicate_handle_t *fi) {

   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_readdir( %s, %p, %p )\n", pthread_self(), path, listing, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_READDIR );
   
   if( fi->type != FTYPE_DIR ) {
      int rc = -EINVAL;
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_readdir rc = %d\n", pthread_self(), rc );
      SYNDICATE_DATA->stats->leave( STAT_READDIR, rc );
      return rc;
   }

   struct fs_dir_handle* fdh = fi->fdh;     // get back our DIR instance
   
   int rc = 0;
   struct fs_dir_entry** dirents = fs_entry_readdir( SYNDICATE_DATA->core, fdh, &rc );
   
   if( rc == 0 && dirents ) {
      *listing = dirents;
   }
   
   fs_dir_entry_destroy_all( dirents );
   free( dirents );
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_readdir rc = %d\n", pthread_self(), rc );
   
   SYNDICATE_DATA->stats->leave( STAT_READDIR, rc );
   return rc;
}


/** Release directory (closedir) */
int syndicate_closedir(const char *path, syndicate_handle_t *fi) {
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_closedir( %s, %p )\n", pthread_self(), path, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_RELEASEDIR );
   
   if( fi->type != FTYPE_DIR ) {
      int rc = -EINVAL;
         
      SYNDICATE_DATA->stats->leave( STAT_RELEASEDIR, rc );
      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_closedir rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_dir_handle* fdh = fi->fdh;
   
   int rc = fs_entry_closedir( SYNDICATE_DATA->core, fdh );
   
   fi->fdh = NULL;
   free( fdh );
   free( fi );
   
   SYNDICATE_DATA->stats->leave( STAT_RELEASEDIR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_closedir rc = %d\n", pthread_self(), rc );
   return rc;
}

/** free a dir listing */
void syndicate_free_dir_listing( syndicate_dir_listing_t listing ) {
   fs_dir_entry_destroy_all( listing );
   free( listing );
}

/**
 * Check file access permissions (access)
 */
int syndicate_access(const char *path, int mask) {
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_access( %s, %x )\n", pthread_self(), path, mask );
   
   SYNDICATE_DATA->stats->enter( STAT_ACCESS );
   
   int rc = fs_entry_access( SYNDICATE_DATA->core, path, mask, conf->owner, SYNDICATE_DATA->core->volume );
      
   SYNDICATE_DATA->stats->leave( STAT_ACCESS, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_access rc = %d\n", pthread_self(), rc );
   return rc;
}


/**
 * Create and open a file (creat)
 */
syndicate_handle_t* syndicate_create(const char *path, mode_t mode) {
   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_create( %s, %o )\n", pthread_self(), path, mode );
   
   SYNDICATE_DATA->stats->enter( STAT_CREATE );
   
   int rc = 0;
   struct fs_file_handle* fh = fs_entry_create( SYNDICATE_DATA->core, path, conf->owner, SYNDICATE_DATA->core->volume, mode, &rc );
   syndicate_handle_t* ret = NULL;
   if( rc == 0 && fh != NULL ) {
      ret = CALLOC_LIST( syndicate_handle_t, 1 );
      ret->fh = fh;
   }
   
   SYNDICATE_DATA->stats->leave( STAT_CREATE, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_create rc = %d\n", pthread_self(), rc );
   return ret;
}


/**
 * Change the size of an file (ftruncate)
 */
int syndicate_ftruncate(const char *path, off_t length, syndicate_handle_t *fi) {

   struct md_syndicate_conf* conf = &SYNDICATE_DATA->conf;
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_ftruncate( %s, %ld, %p )\n", pthread_self(), path, length, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_FTRUNCATE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
         
      SYNDICATE_DATA->stats->leave( STAT_FTRUNCATE, rc );

      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_ftrunctate rc = %d\n", pthread_self(), rc );
      return rc;
   }

   struct fs_file_handle* fh = fi->fh;
   int rc = fs_entry_ftruncate( SYNDICATE_DATA->core, fh, length, conf->owner, SYNDICATE_DATA->core->volume );
   if( rc != 0 ) {
      errorf( "fs_entry_ftruncate rc = %d\n", rc );
   }
   
   SYNDICATE_DATA->stats->leave( STAT_FTRUNCATE, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_ftrunctate rc = %d\n", pthread_self(), rc );
   
   return rc;
}


/**
 * Get attributes from an open file (fstat)
 */
int syndicate_fgetattr(const char *path, struct stat *statbuf, syndicate_handle_t *fi) {
   
   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fgetattr( %s, %p, %p )\n", pthread_self(), path, statbuf, fi );
   
   SYNDICATE_DATA->stats->enter( STAT_FGETATTR );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
         
      SYNDICATE_DATA->stats->leave( STAT_FGETATTR, rc );

      logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fgetattr rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   int rc = fs_entry_fstat( SYNDICATE_DATA->core, fh, statbuf );
   
   SYNDICATE_DATA->stats->leave( STAT_FGETATTR, rc );

   logmsg( SYNDICATE_DATA->logfile, "%16lx: syndicate_fgetattr rc = %d\n", pthread_self(), rc );
   
   return rc;
}
