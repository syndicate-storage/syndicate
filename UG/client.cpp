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
int syndicate_getattr(struct syndicate_state* state, const char *path, struct stat *statbuf) {
   
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_getattr( %s, %p )\n", pthread_self(), path, statbuf );
   
   state->stats->enter( STAT_GETATTR );
   
   int rc = fs_entry_stat( state->core, path, statbuf, conf->owner, state->core->volume );
   logmsg( state->logfile, "%16lx: syndicate_getattr rc = %d\n", pthread_self(), rc );
   
   state->stats->leave( STAT_GETATTR, rc );
   
   return rc;
}

/** Create a directory (mkdir) */
int syndicate_mkdir(struct syndicate_state* state, const char *path, mode_t mode) {

   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_mkdir( %s, %o )\n", pthread_self(), path, mode );
   
   state->stats->enter( STAT_MKDIR );
   
   int rc = fs_entry_mkdir( state->core, path, mode, conf->owner, state->core->volume );
   
   state->stats->leave( STAT_MKDIR, rc );
   
   logmsg( state->logfile, "%16lx: syndicate_mkdir rc = %d\n", pthread_self(), rc );
   return rc;
}

/** Remove a file (unlink) */
int syndicate_unlink(struct syndicate_state* state, const char* path) {
   logmsg( state->logfile, "%16lx: syndicate_unlink( %s )\n", pthread_self(), path );

   state->stats->enter( STAT_UNLINK );
   
   int rc = fs_entry_versioned_unlink( state->core, path, 0, 0, -1, state->conf.owner, state->core->volume, state->core->gateway, false );

   state->stats->leave( STAT_UNLINK, rc );
   
   logmsg( state->logfile, "%16lx: syndicate_unlink rc = %d\n", pthread_self(), rc);
   return rc;
}

/** Remove a directory (rmdir) */
int syndicate_rmdir(struct syndicate_state* state, const char *path) {
   
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_rmdir( %s )\n", pthread_self(), path );
   
   state->stats->enter( STAT_RMDIR );
   
   int rc = fs_entry_rmdir( state->core, path, conf->owner, state->core->volume );
   
   state->stats->leave( STAT_RMDIR, rc );

   logmsg( state->logfile, "%16lx: syndicate_rmdir rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Rename a file.  Paths are FS-relative! (rename) */
int syndicate_rename(struct syndicate_state* state, const char *path, const char *newpath) {
   
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_rename( %s, %s )\n", pthread_self(), path, newpath );
   
   state->stats->enter( STAT_RENAME );

   //int rc = fs_entry_versioned_rename( state->core, path, newpath, conf->owner, state->core->volume, -1 );
   int rc = fs_entry_rename( state->core, path, newpath, conf->owner, state->core->volume );

   logmsg( state->logfile, "%16lx: syndicate_rename( %s, %s ) rc = %d\n", pthread_self(), path, newpath, rc );
   
   state->stats->leave( STAT_RENAME, rc );
   return rc;
}


/** Change the permission bits of a file (chmod) */
int syndicate_chmod(struct syndicate_state* state, const char *path, mode_t mode) {
   
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_chmod( %s, %o )\n", pthread_self(), path, mode );
   
   state->stats->enter( STAT_CHMOD );
   
   int rc = fs_entry_chmod( state->core, path, conf->owner, state->core->volume, mode );
   if( rc == 0 ) {
      // TODO: update the modtime and metadata of this file
   }
   
   state->stats->leave( STAT_CHMOD, rc );
   logmsg( state->logfile, "%16lx: syndicate_chmod rc = %d\n");
   return rc;
}


/** Change the coordinator of a file */
int syndicate_chown(struct syndicate_state* state, const char *path, uint64_t new_coordinator ) {
   /*
    * TODO
   */
   return -ENOSYS;
}


/** Change the size of a file (truncate) */
/* only works on local files */
int syndicate_truncate(struct syndicate_state* state, const char *path, off_t newsize) {
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_truncate( %s, %ld )\n", pthread_self(), path, newsize );

   state->stats->enter( STAT_TRUNCATE );

   int rc = fs_entry_truncate( state->core, path, newsize, conf->owner, state->core->volume );

   state->stats->leave( STAT_TRUNCATE, rc );
   logmsg( state->logfile, "%16lx: syndicate_truncate rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Change the access and/or modification times of a file (utime) */
int syndicate_utime(struct syndicate_state* state, const char *path, struct utimbuf *ubuf) {
   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_utime( %s, {%d, %d} )\n", pthread_self(), path, ubuf->actime, ubuf->modtime );
   
   state->stats->enter( STAT_UTIME );
   
   int rc = fs_entry_utime( state->core, path, ubuf, conf->owner, state->core->volume );
   if( rc == 0 ) {
      // TODO: update the modtime of this file
   }
   
   logmsg( state->logfile, "%16lx: syndicate_utime rc = %d\n", pthread_self(), rc);
   state->stats->leave( STAT_UTIME, rc );
   return rc;
}



/** File open operation (O_CREAT and O_EXCL will *not* be passed to this method, according to the documentation) */
syndicate_handle_t* syndicate_open(struct syndicate_state* state, const char *path, int flags, int* rc ) {

   struct md_syndicate_conf* conf = &state->conf;
   logmsg( state->logfile, "%16lx: syndicate_open( %s, %d )\n", pthread_self(), path, flags );
   
   state->stats->enter( STAT_OPEN );
   
   // client mode is always synchronous...
   struct fs_file_handle* fh = fs_entry_open( state->core, path, conf->owner, state->core->volume, flags | O_SYNC, ~conf->usermask, rc );
   
   // store the handle
   syndicate_handle_t* sh = CALLOC_LIST( syndicate_handle_t, 1 );
   sh->type = FTYPE_FILE;
   sh->fh = fh;
   
   state->stats->leave( STAT_OPEN, *rc );
   logmsg( state->logfile, "%16lx: syndicate_open rc = %d\n", pthread_self(), *rc );
   
   return sh;
}


/** Read data from an open file.  Return number of bytes read. */
int syndicate_read(struct syndicate_state* state, char *buf, size_t size, syndicate_handle_t *fi) {
   
   logmsg( state->logfile, "%16lx: syndicate_read( %p, %ld, %p )\n", pthread_self(), buf, size, fi );
   
   state->stats->enter( STAT_READ );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EBADF;
      state->stats->leave( STAT_READ, -1 );
      logerr( state->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   ssize_t rc = fs_entry_read( state->core, fh, buf, size, fi->offset );
   
   if( rc < 0 ) {
      state->stats->leave( STAT_READ, -1 );
      logerr( state->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
      return -1;
   }
   
   // fill the remainder of buf with 0's
   if( rc < (signed)size ) {
      memset( buf + rc, 0, size - rc );
   }
   
   if( rc >= 0 )
      fi->offset += rc;
   
   logmsg( state->logfile, "%16lx: syndicate_read rc = %ld\n", pthread_self(), rc );
   
   state->stats->leave( STAT_READ, (rc >= 0 ? 0 : rc) );
   return rc;
}


/** Write data to an open file (pwrite) */
int syndicate_write(struct syndicate_state* state, const char *buf, size_t size, syndicate_handle_t *fi) {

   logmsg( state->logfile, "%16lx: syndicate_write( %p, %ld, %p )\n", pthread_self(), buf, size, fi->fh );
   
   state->stats->enter( STAT_WRITE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EBADF;
      state->stats->leave( STAT_WRITE, (rc >= 0 ? 0 : rc)  );
      logmsg( state->logfile, "%16lx: syndicate_write rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   ssize_t rc = fs_entry_write( state->core, fh, buf, size, fi->offset );
   
   if( rc >= 0 )
      fi->offset += rc;
   
   state->stats->leave( STAT_WRITE, (rc >= 0 ? 0 : rc)  );
   
   logmsg( state->logfile, "%16lx: syndicate_write rc = %d\n", pthread_self(), rc );
   return (int)rc;
}

// seek a handle 
off_t syndicate_seek(syndicate_handle_t* fi, off_t pos, int whence) {
   if( fi->type != FTYPE_FILE )
      return -EBADF;
      
   if( whence == SEEK_SET ) {
      fi->offset = pos;
   }
   else if( whence == SEEK_CUR ) {
      fi->offset += pos;
   }
   else if( whence == SEEK_END ) {
      dbprintf("%s", "FIXME: implement SEEK_END");
      return -ENOSYS;
   }
   
   return fi->offset;
}


/** Possibly flush cached data */
int syndicate_flush(struct syndicate_state* state, syndicate_handle_t *fi) {
   
   logmsg( state->logfile, "%16lx: syndicate_flush( %p )\n", pthread_self(), fi );
   
   state->stats->enter( STAT_FLUSH );

   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      state->stats->leave( STAT_FLUSH, rc );
      logmsg( state->logfile, "%16lx: syndicate_flush rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   
   int rc = fs_entry_fsync( state->core, fh );
   
   state->stats->leave( STAT_FLUSH, rc );

   logmsg( state->logfile, "%16lx: syndicate_flush rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Release an open file (close) */
int syndicate_close(struct syndicate_state* state, syndicate_handle_t *fi) {

   logmsg( state->logfile, "%16lx: syndicate_close( %p )\n", pthread_self(), fi );
   
   state->stats->enter( STAT_RELEASE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      logmsg( state->logfile, "%16lx: syndicate_close rc = %d\n", pthread_self(), rc );
      state->stats->leave( STAT_RELEASE, rc );
   }
      
   struct fs_file_handle* fh = fi->fh;
   
   int rc = fs_entry_close( state->core, fh );
   if( rc != 0 ) {
      logerr( state->logfile, "%16lx: syndicate_close: fs_entry_close rc = %d\n", pthread_self(), rc );
   }
   
   fi->fh = NULL;
   free( fh );
   free( fi );
   
   logmsg( state->logfile, "%16lx: syndicate_close rc = %d\n", pthread_self(), rc );
   
   state->stats->leave( STAT_RELEASE, rc );
   return rc;
}


/** Synchronize file contents (fdatasync, fsync)
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 */
int syndicate_fsync(struct syndicate_state* state, int datasync, syndicate_handle_t *fi) {
   
   logmsg( state->logfile, "%16lx: syndicate_fsync( %d, %p )\n", pthread_self(), datasync, fi );
   
   state->stats->enter( STAT_FSYNC );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
      
      state->stats->leave( STAT_FSYNC, rc );
      logmsg( state->logfile, "%16lx: syndicate_fsync rc = %d\n", pthread_self(), rc );
      
      return rc;
   }
      
   struct fs_file_handle* fh = fi->fh;
   int rc = 0;
   if( datasync == 0 )
      rc = fs_entry_fdatasync( state->core, fh );
   
   if( rc == 0 )
      rc = fs_entry_fsync( state->core, fh );
      
   state->stats->leave( STAT_FSYNC, rc );

   logmsg( state->logfile, "%16lx: syndicate_fsync rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Set extended attributes (lsetxattr) */
int syndicate_setxattr(struct syndicate_state* state, const char *path, const char *name, const char *value, size_t size, int flags) {

   struct md_syndicate_conf* conf = &state->conf;
   
   char* safe_value = (char*)calloc( size + 1, 1 );
   strncpy( safe_value, value, size );
   logmsg( state->logfile, "%16lx: syndicate_setxattr( %s, %s, %s, %d, %x )\n", pthread_self(), path, name, safe_value, size, flags );
   free( safe_value );
   
   state->stats->enter( STAT_SETXATTR );
   
   int rc = fs_entry_setxattr( state->core, path, name, value, size, flags, conf->owner, state->core->volume );
   
   state->stats->leave( STAT_SETXATTR, rc );

   logmsg( state->logfile, "%16lx: syndicate_setxattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Get extended attributes (lgetxattr) */
int syndicate_getxattr(struct syndicate_state* state, const char *path, const char *name, char *value, size_t size) {

   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_getxattr( %s, %s, %p, %d )\n", pthread_self(), path, name, value, size );
   
   state->stats->enter( STAT_GETXATTR );
   
   int rc = fs_entry_getxattr( state->core, path, name, value, size, conf->owner, state->core->volume );
   
   state->stats->leave( STAT_GETXATTR, rc );

   logmsg( state->logfile, "%16lx: syndicate_getxattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** List extended attributes (llistxattr) */
int syndicate_listxattr(struct syndicate_state* state, const char *path, char *list, size_t size) {

   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_listxattr( %s, %p, %d )\n", pthread_self(), path, list, size );
   
   state->stats->enter( STAT_LISTXATTR );
   
   int rc = fs_entry_listxattr( state->core, path, list, size, conf->owner, state->core->volume );
   
   state->stats->leave( STAT_LISTXATTR, rc );

   logmsg( state->logfile, "%16lx: syndicate_listxattr rc = %d\n", pthread_self(), rc );
   
   return rc;
}


/** Remove extended attributes (lremovexattr) */
int syndicate_removexattr(struct syndicate_state* state, const char *path, const char *name) {
   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_removexattr( %s, %s )\n", pthread_self(), path, name );
   
   state->stats->enter( STAT_REMOVEXATTR );
   
   int rc = fs_entry_removexattr( state->core, path, name, conf->owner, state->core->volume );

   state->stats->leave( STAT_REMOVEXATTR, rc );

   logmsg( state->logfile, "%16lx: syndicate_removexattr rc = %d\n", pthread_self(), rc );
   return rc;
}


/** Open directory (opendir) */
syndicate_handle_t* syndicate_opendir(struct syndicate_state* state, const char *path, int* rc) {

   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_opendir( %s )\n", pthread_self(), path );
   
   state->stats->enter( STAT_OPENDIR );

   struct fs_dir_handle* fdh = fs_entry_opendir( state->core, path, conf->owner, state->core->volume, rc );
   syndicate_handle_t* ret = NULL;
   
   if( *rc == 0 ) {
      ret = CALLOC_LIST( syndicate_handle_t, 1 );
      ret->fdh = fdh;
      ret->type = FTYPE_DIR;
   }
   
   state->stats->leave( STAT_OPENDIR, *rc );
   
   logmsg( state->logfile,  "%16lx: syndicate_opendir rc = %d\n", pthread_self(), *rc );
   
   return ret;
}


/** Read directory (readdir)
 */
int syndicate_readdir(struct syndicate_state* state, syndicate_dir_listing_t* listing, syndicate_handle_t *fi) {

   
   logmsg( state->logfile, "%16lx: syndicate_readdir( %p, %p )\n", pthread_self(), listing, fi );
   
   state->stats->enter( STAT_READDIR );
   
   if( fi->type != FTYPE_DIR ) {
      int rc = -EINVAL;
      logmsg( state->logfile, "%16lx: syndicate_readdir rc = %d\n", pthread_self(), rc );
      state->stats->leave( STAT_READDIR, rc );
      return rc;
   }

   struct fs_dir_handle* fdh = fi->fdh;     // get back our DIR instance
   
   int rc = 0;
   struct fs_dir_entry** dirents = fs_entry_readdir( state->core, fdh, &rc );
   
   if( rc == 0 && dirents ) {
      *listing = dirents;
   }
   
   logmsg( state->logfile, "%16lx: syndicate_readdir rc = %d\n", pthread_self(), rc );
   
   state->stats->leave( STAT_READDIR, rc );
   return rc;
}


/** Release directory (closedir) */
int syndicate_closedir(struct syndicate_state* state, syndicate_handle_t *fi) {
   
   logmsg( state->logfile, "%16lx: syndicate_closedir( %p )\n", pthread_self(), fi );
   
   state->stats->enter( STAT_RELEASEDIR );
   
   if( fi->type != FTYPE_DIR ) {
      int rc = -EINVAL;
         
      state->stats->leave( STAT_RELEASEDIR, rc );
      logmsg( state->logfile, "%16lx: syndicate_closedir rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_dir_handle* fdh = fi->fdh;
   
   int rc = fs_entry_closedir( state->core, fdh );
   
   fi->fdh = NULL;
   free( fdh );
   free( fi );
   
   state->stats->leave( STAT_RELEASEDIR, rc );

   logmsg( state->logfile, "%16lx: syndicate_closedir rc = %d\n", pthread_self(), rc );
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
int syndicate_access(struct syndicate_state* state, const char *path, int mask) {
   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_access( %s, %x )\n", pthread_self(), path, mask );
   
   state->stats->enter( STAT_ACCESS );
   
   int rc = fs_entry_access( state->core, path, mask, conf->owner, state->core->volume );
      
   state->stats->leave( STAT_ACCESS, rc );

   logmsg( state->logfile, "%16lx: syndicate_access rc = %d\n", pthread_self(), rc );
   return rc;
}


/**
 * Create and open a file (creat)
 */
syndicate_handle_t* syndicate_create(struct syndicate_state* state, const char *path, mode_t mode, int* rc) {
   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_create( %s, %o )\n", pthread_self(), path, mode );
   
   state->stats->enter( STAT_CREATE );
   
   struct fs_file_handle* fh = fs_entry_create( state->core, path, conf->owner, state->core->volume, mode, rc );
   syndicate_handle_t* ret = NULL;
   if( *rc == 0 && fh != NULL ) {
      ret = CALLOC_LIST( syndicate_handle_t, 1 );
      ret->type = FTYPE_FILE;
      ret->fh = fh;
   }
   
   state->stats->leave( STAT_CREATE, *rc );

   logmsg( state->logfile, "%16lx: syndicate_create rc = %d\n", pthread_self(), *rc );
   return ret;
}


/**
 * Change the size of an file (ftruncate)
 */
int syndicate_ftruncate(struct syndicate_state* state, off_t length, syndicate_handle_t *fi) {

   struct md_syndicate_conf* conf = &state->conf;
   
   logmsg( state->logfile, "%16lx: syndicate_ftruncate( %ld, %p )\n", pthread_self(), length, fi );
   
   state->stats->enter( STAT_FTRUNCATE );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
         
      state->stats->leave( STAT_FTRUNCATE, rc );

      logmsg( state->logfile, "%16lx: syndicate_ftrunctate rc = %d\n", pthread_self(), rc );
      return rc;
   }

   struct fs_file_handle* fh = fi->fh;
   int rc = fs_entry_ftruncate( state->core, fh, length, conf->owner, state->core->volume );
   if( rc != 0 ) {
      errorf( "fs_entry_ftruncate rc = %d\n", rc );
   }
   
   state->stats->leave( STAT_FTRUNCATE, rc );

   logmsg( state->logfile, "%16lx: syndicate_ftrunctate rc = %d\n", pthread_self(), rc );
   
   return rc;
}


/**
 * Get attributes from an open file (fstat)
 */
int syndicate_fgetattr(struct syndicate_state* state, struct stat *statbuf, syndicate_handle_t *fi) {
   
   logmsg( state->logfile, "%16lx: syndicate_fgetattr( %p, %p )\n", pthread_self(), statbuf, fi );
   
   state->stats->enter( STAT_FGETATTR );
   
   if( fi->type != FTYPE_FILE ) {
      int rc = -EINVAL;
         
      state->stats->leave( STAT_FGETATTR, rc );

      logmsg( state->logfile, "%16lx: syndicate_fgetattr rc = %d\n", pthread_self(), rc );
      return rc;
   }
   
   struct fs_file_handle* fh = fi->fh;
   int rc = fs_entry_fstat( state->core, fh, statbuf );
   
   state->stats->leave( STAT_FGETATTR, rc );

   logmsg( state->logfile, "%16lx: syndicate_fgetattr rc = %d\n", pthread_self(), rc );
   
   return rc;
}

// initialize syndicate
int syndicate_client_init( struct syndicate_state* state, struct syndicate_opts* opts ) {
   
   struct ms_client* ms = CALLOC_LIST( struct ms_client, 1 );
   
   // load config file
   md_default_conf( &state->conf, SYNDICATE_UG );
   
   // read the config file
   if( opts->config_file != NULL ) {
      int rc = md_read_conf( opts->config_file, &state->conf );
      if( rc != 0 ) {
         dbprintf("ERR: failed to read %s, rc = %d\n", opts->config_file, rc );
         if( !(rc == -ENOENT || rc == -EACCES || rc == -EPERM) ) {
            // not just a simple "not found" or "permission denied"
            return rc;
         }  
         else {
            rc = 0;
         }
      }
   }
   
   // set debug level
   md_debug( &state->conf, opts->debug_level );
   
   // initialize library
   int rc = md_init_client( &state->conf, ms, opts->ms_url, opts->volume_name, opts->gateway_name, opts->username, (char*)opts->password.ptr, (char*)opts->user_pkey_pem.ptr,
                                              opts->volume_pubkey_pem, (char*)opts->gateway_pkey_pem.ptr, (char*)opts->gateway_pkey_decryption_password.ptr, opts->storage_root, opts->syndicate_pubkey_pem );
   if( rc != 0 ) {
      errorf("md_init_client rc = %d\n", rc );
      return rc;
   }
   
   // initialize state
   rc = syndicate_setup_state( state, ms );
   if( rc != 0 ) {
      errorf("syndicate_init_state rc = %d\n", rc );
      return rc;
   }
   
   syndicate_set_running_ex( state, 1 );
   return 0;
}

// destroy syndicate
int syndicate_client_shutdown( struct syndicate_state* state, int wait_replicas ) {  
   syndicate_destroy_ex( state, wait_replicas );
   return 0;
}
