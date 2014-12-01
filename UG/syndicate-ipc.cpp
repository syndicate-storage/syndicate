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


// Implementation of the SyndicateIPC methods.

#include "libsyndicate/libsyndicate.h"
#include "stats.h"
#include "log.h"
#include "fs.h"
#include "replication.h"
#include "syndicate.h"
#include "server.h"
#include "libsyndicate/opts.h"

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <signal.h>
#include <getopt.h>

using boost::asio::ip::tcp;

/*
 * Context
 */
struct syndicateipc_context {
    struct syndicate_state* syndicate_state_data;
    struct md_HTTP syndicate_http;
};

static syndicateipc_context native_context;

static syndicateipc_context* syndicateipc_get_context() {
    return &native_context;
}

#define SYNDICATEFS_DATA (syndicateipc_get_context()->syndicate_state_data)

/*
 * IPC Definitions and Structures
 */
const int MAX_PATH_SIZE = 1024;
const int MAX_XATTR_NAME_SIZE = 1024;

struct IPCFileInfo {
    long long int handle;
};
const int SIZE_IPCFILEINFO = 8;

struct IPCStat {
    int st_mode;
    int st_uid;
    int st_gid; // 12
    long long int st_size;
    long long int st_blksize;
    long long int st_blocks;
    long long int st_atim;
    long long int st_mtim; // 52
};
const int SIZE_IPCSTAT = 52;

enum IPCMessageOperations {
    OP_GET_STAT = 0,
    OP_DELETE = 1,
    OP_REMOVE_DIRECTORY = 2,
    OP_RENAME = 3,
    OP_MKDIR = 4,
    OP_READ_DIRECTORY = 5,
    OP_GET_FILE_HANDLE = 6,
    OP_CREATE_NEW_FILE = 7,
    OP_READ_FILEDATA = 8,
    OP_WRITE_FILE_DATA = 9,
    OP_FLUSH = 10,
    OP_CLOSE_FILE_HANDLE = 11,
    OP_TRUNCATE_FILE = 12,
    OP_GET_EXTENDED_ATTR = 13,
    OP_LIST_EXTENDED_ATTR = 14,
};

/*
 * Packet Building Utility Functions
 */
static int readIntFromNetworkBytes(const char* buf) {
    int value;
    char* bytePtr = (char*) &value;
    bytePtr[0] = buf[3];
    bytePtr[1] = buf[2];
    bytePtr[2] = buf[1];
    bytePtr[3] = buf[0];
    return value;
}

static void writeIntToNetworkBytes(char* bytes_ptr, int value) {
    char* bytePtr = (char*) &value;
    bytes_ptr[0] = bytePtr[3];
    bytes_ptr[1] = bytePtr[2];
    bytes_ptr[2] = bytePtr[1];
    bytes_ptr[3] = bytePtr[0];
}

static long long int readLongFromNetworkBytes(const char* buf) {
    long long int value;
    char* bytePtr = (char*) &value;
    bytePtr[0] = buf[7];
    bytePtr[1] = buf[6];
    bytePtr[2] = buf[5];
    bytePtr[3] = buf[4];
    bytePtr[4] = buf[3];
    bytePtr[5] = buf[2];
    bytePtr[6] = buf[1];
    bytePtr[7] = buf[0];
    return value;
}

static void writeLongToNetworkBytes(char* bytes_ptr, long long int value) {
    char* bytePtr = (char*) &value;
    bytes_ptr[0] = bytePtr[7];
    bytes_ptr[1] = bytePtr[6];
    bytes_ptr[2] = bytePtr[5];
    bytes_ptr[3] = bytePtr[4];
    bytes_ptr[4] = bytePtr[3];
    bytes_ptr[5] = bytePtr[2];
    bytes_ptr[6] = bytePtr[1];
    bytes_ptr[7] = bytePtr[0];
}

static void copyStatToIPCStat(struct IPCStat* ipcstat, const struct stat* stat) {
    ipcstat->st_mode = stat->st_mode;
    ipcstat->st_uid = stat->st_uid;
    ipcstat->st_gid = stat->st_gid;

    ipcstat->st_size = stat->st_size;
    ipcstat->st_blksize = stat->st_blksize;
    ipcstat->st_blocks = stat->st_blocks;
    ipcstat->st_atim = stat->st_atim.tv_sec;
    ipcstat->st_mtim = stat->st_mtim.tv_sec;
}

/////////////////////////////////////////////////////////////////
// syndicatefs base functions
/////////////////////////////////////////////////////////////////
/*
 * Get file attributes (lstat)
 */
int syndicatefs_getattr(const char *path, struct stat *statbuf) {
   
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_getattr( %s, %p )\n", path, statbuf );
   
   SYNDICATEFS_DATA->stats->enter( STAT_GETATTR );
   
   int rc = fs_entry_stat( SYNDICATEFS_DATA->core, path, statbuf, conf->owner, SYNDICATEFS_DATA->core->volume );
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_getattr rc = %d\n", rc );
   
   SYNDICATEFS_DATA->stats->leave( STAT_GETATTR, rc );
   
   return rc;
}

/*
 * Read the target of a symbolic link.
 * In practice, this is a no-op, since there aren't any symlinks (yet)
 */
/* -- not used in syndicate-ipc
int syndicatefs_readlink(const char *path, char *link, size_t size) {

   SYNDICATEFS_DATA->stats->enter( STAT_READLINK );
   logmsg( SYNDICATEFS_DATA->logfile, "syndicatefs_readlink on path %s, size %u\n", pthread_self(), path, size);
   logerr( SYNDICATEFS_DATA->logfile, "ERR: not implemented\n");
   
   SYNDICATEFS_DATA->stats->leave( STAT_READLINK, -1 );
   return -EINVAL;
}
*/


/*
 * Create a file node with open(), mkfifo(), or mknod(), depending on the mode.
 * Right now, only normal files are supported.
 */
/* -- not used in syndicate-ipc
int syndicatefs_mknod(const char *path, mode_t mode, dev_t dev) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_mknod( %s, %o, %d )\n", pthread_self(), path, mode, dev );
   
   SYNDICATEFS_DATA->stats->enter( STAT_MKNOD );
   
   int rc = fs_entry_mknod( SYNDICATEFS_DATA->core, path, mode, dev, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_MKNOD, rc );
   return rc;
}
*/

/** Create a directory (mkdir) */
int syndicatefs_mkdir(const char *path, mode_t mode) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_mkdir( %s, %o )\n", path, mode );
   
   SYNDICATEFS_DATA->stats->enter( STAT_MKDIR );
   
   int rc = fs_entry_mkdir( SYNDICATEFS_DATA->core, path, mode, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_MKDIR, rc );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_mkdir rc = %d\n", rc );
   return rc;
}

/** Remove a file (unlink) */
int syndicatefs_unlink(const char* path) {
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_unlink( %s )\n", path );

   SYNDICATEFS_DATA->stats->enter( STAT_UNLINK );
   
   int rc = fs_entry_versioned_unlink( SYNDICATEFS_DATA->core, path, 0, 0, -1, SYNDICATEFS_DATA->conf.owner, SYNDICATEFS_DATA->core->volume, SYNDICATEFS_DATA->core->gateway, false );

   SYNDICATEFS_DATA->stats->leave( STAT_UNLINK, rc );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_unlink rc = %d\n", rc);
   return rc;
}

/** Remove a directory (rmdir) */
int syndicatefs_rmdir(const char *path) {
   
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_rmdir( %s )\n", path );
   
   SYNDICATEFS_DATA->stats->enter( STAT_RMDIR );
   
   int rc = fs_entry_rmdir( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_RMDIR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_rmdir rc = %d\n", rc );
   return rc;
}

/** Create a symbolic link (symlink) */
/* -- not used in syndicate-ipc
int syndicatefs_symlink(const char *path, const char *link) {

   SYNDICATEFS_DATA->stats->enter( STAT_SYMLINK );
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_symlink on path %s, link %s\n", pthread_self(), path, link);
   SYNDICATEFS_DATA->stats->leave( STAT_SYMLINK, -1 );
   return -EPERM; // not supported
}
*/

/** Rename a file.  Paths are FS-relative! (rename) */
int syndicatefs_rename(const char *path, const char *newpath) {
   
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_rename( %s, %s )\n", path, newpath );
   
   SYNDICATEFS_DATA->stats->enter( STAT_RENAME );

   //int rc = fs_entry_versioned_rename( SYNDICATEFS_DATA->core, path, newpath, conf->owner, SYNDICATEFS_DATA->core->volume, -1 );
   int rc = fs_entry_rename( SYNDICATEFS_DATA->core, path, newpath, conf->owner, SYNDICATEFS_DATA->core->volume );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicatefs_rename( %s, %s ) rc = %d\n", path, newpath, rc );
   
   SYNDICATEFS_DATA->stats->leave( STAT_RENAME, rc );
   return rc;
}

/** Create a hard link to a file (link) */
/* -- not used in syndicate-ipc
int syndicatefs_link(const char *path, const char *newpath) {
   SYNDICATEFS_DATA->stats->enter( STAT_LINK );
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_link hard from %s to %s\n", pthread_self(), path, newpath);
   SYNDICATEFS_DATA->stats->leave( STAT_LINK, -1 );
   return -EXDEV;    // not supported
}
*/

/** Change the permission bits of a file (chmod) */
/* -- not used in syndicate-ipc
int syndicatefs_chmod(const char *path, mode_t mode) {
   
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_chmod( %s, %o )\n", pthread_self(), path, mode );
   
   SYNDICATEFS_DATA->stats->enter( STAT_CHMOD );
   
   int rc = fs_entry_chmod( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, mode );
   if( rc == 0 ) {
      // TODO: update the modtime and metadata of this file
   }
   
   SYNDICATEFS_DATA->stats->leave( STAT_CHMOD, rc );
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_chmod rc = %d\n");
   return rc;
}
*/

/** Change the owner and group of a file (chown) */
/* -- not used in syndicate-ipc
int syndicatefs_chown(const char *path, uid_t uid, gid_t gid) {
   return -ENOSYS;
}
*/

/** Change the size of a file (truncate) */
/* only works on local files */
/* -- not used in syndicate-ipc
int syndicatefs_truncate(const char *path, off_t newsize) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_truncate( %s, %ld )\n", pthread_self(), path, newsize );

   SYNDICATEFS_DATA->stats->enter( STAT_TRUNCATE );

   int rc = fs_entry_truncate( SYNDICATEFS_DATA->core, path, newsize, conf->owner, SYNDICATEFS_DATA->core->volume );

   SYNDICATEFS_DATA->stats->leave( STAT_TRUNCATE, rc );
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_truncate rc = %d\n", pthread_self(), rc );
   return rc;
}
*/

/** Change the access and/or modification times of a file (utime) */
/* -- not used in syndicate-ipc
int syndicatefs_utime(const char *path, struct utimbuf *ubuf) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_utime( %s, {%d, %d} )\n", pthread_self(), path, ubuf->actime, ubuf->modtime );
   
   SYNDICATEFS_DATA->stats->enter( STAT_UTIME );
   
   int rc = fs_entry_utime( SYNDICATEFS_DATA->core, path, ubuf, conf->owner, SYNDICATEFS_DATA->core->volume );
   if( rc == 0 ) {
      // TODO: update the modtime of this file
   }
   
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_utime rc = %d\n", pthread_self(), rc);
   SYNDICATEFS_DATA->stats->leave( STAT_UTIME, rc );
   return rc;
}
*/

/** File open operation (O_CREAT and O_EXCL will *not* be passed to this method, according to the documentation) */
//int syndicatefs_open(const char *path, struct fuse_file_info *fi) {
int syndicatefs_open(const char *path, struct IPCFileInfo *fi) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_open( %s )\n", path );
   
   SYNDICATEFS_DATA->stats->enter( STAT_OPEN );
   
   int err = 0;
   //struct fs_file_handle* fh = fs_entry_open( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, fi->flags, ~conf->usermask, &err );
   struct fs_file_handle* fh = fs_entry_open( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, 0, ~conf->usermask, &err );
   
   // store the read handle
   //fi->fh = (uint64_t) fh;
   fi->handle = (long long int) fh;

   // force direct I/O
   //fi->direct_io = 1;
   
   SYNDICATEFS_DATA->stats->leave( STAT_OPEN, err );
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_open rc = %d\n", err );
   
   return err;
}

/** Read data from an open file.  Return number of bytes read. */
//int syndicatefs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
int syndicatefs_read(char *buf, size_t size, off_t offset, struct IPCFileInfo *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_read( %ld, %ld )\n", size, offset );
   
   SYNDICATEFS_DATA->stats->enter( STAT_READ );
   
   //struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->handle;
   ssize_t rc = fs_entry_read( SYNDICATEFS_DATA->core, fh, buf, size, offset );
   
   if( rc < 0 ) {
      SYNDICATEFS_DATA->stats->leave( STAT_READ, -1 );
      logerr( SYNDICATEFS_DATA->logfile, "syndicateipc_read rc = %ld\n", rc );
      return -1;
   }
   
    // fill the remainder of buf with 0's
    //if (rc < (signed)size ) {
    //    memset( buf + rc, 0, size - rc );
    //}
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_read rc = %ld\n", rc );
   
   SYNDICATEFS_DATA->stats->leave( STAT_READ, (rc >= 0 ? 0 : rc) );
   return rc;
}

/** Write data to an open file (pwrite) */
//int syndicatefs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
int syndicatefs_write(const char *buf, size_t size, off_t offset, struct IPCFileInfo *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_write( %ld, %ld )\n", size, offset );
   
   SYNDICATEFS_DATA->stats->enter( STAT_WRITE );
   
   //struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->handle;
   
   ssize_t rc = fs_entry_write( SYNDICATEFS_DATA->core, fh, buf, size, offset );
   
   SYNDICATEFS_DATA->stats->leave( STAT_WRITE, (rc >= 0 ? 0 : rc)  );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_write rc = %d\n", rc );
   return (int)rc;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 */
/* -- not used in syndicate-ipc
int syndicatefs_statfs(const char *path, struct statvfs *statv) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_statfs( %s, %p )\n", pthread_self(), path, statv );
   
   SYNDICATEFS_DATA->stats->enter( STAT_STATFS );
   
   int rc = fs_entry_statfs( SYNDICATEFS_DATA->core, path, statv, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_STATFS, rc );
   return rc;
}
*/

/** Possibly flush cached data (No-op) */
//int syndicatefs_flush(const char *path, struct fuse_file_info *fi) {
int syndicatefs_flush(struct IPCFileInfo *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_flush( %p )\n", fi );
   
   SYNDICATEFS_DATA->stats->enter( STAT_FLUSH );

   //struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->handle;
   
   int rc = fs_entry_fsync( SYNDICATEFS_DATA->core, fh );
   
   SYNDICATEFS_DATA->stats->leave( STAT_FLUSH, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_flush rc = %d\n", rc );
   return rc;
}

/** Release an open file (close) */
//int syndicatefs_release(const char *path, struct fuse_file_info *fi) {
int syndicatefs_release(struct IPCFileInfo *fi) {

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_release\n" );
   
   SYNDICATEFS_DATA->stats->enter( STAT_RELEASE );
   
   //struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->handle;
   
   int rc = fs_entry_close( SYNDICATEFS_DATA->core, fh );
   if( rc != 0 ) {
      logerr( SYNDICATEFS_DATA->logfile, "syndicateipc_release: fs_entry_close rc = %d\n", rc );
   }
   
   free( fh );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_release rc = %d\n", rc );
   
   SYNDICATEFS_DATA->stats->leave( STAT_RELEASE, rc );
   return rc;
}

/** Synchronize file contents (fdatasync, fsync)
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 */
/* -- not used in syndicate-ipc
int syndicatefs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_fsync( %s, %d, %p )\n", pthread_self(), path, datasync, fi );
   
   SYNDICATEFS_DATA->stats->enter( STAT_FSYNC );
   
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   int rc = 0;
   if( datasync == 0 )
      rc = fs_entry_fdatasync( SYNDICATEFS_DATA->core, fh );
   
   if( rc == 0 )
      fs_entry_fsync( SYNDICATEFS_DATA->core, fh );
      
   SYNDICATEFS_DATA->stats->leave( STAT_FSYNC, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_fsync rc = %d\n", pthread_self(), rc );
   return rc;
}
*/

/** Set extended attributes (lsetxattr) */
/* -- not used in syndicate-ipc
int syndicatefs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   char* safe_value = (char*)calloc( size + 1, 1 );
   strncpy( safe_value, value, size );
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_setxattr( %s, %s, %s, %d, %x )\n", pthread_self(), path, name, safe_value, size, flags );
   free( safe_value );
   
   SYNDICATEFS_DATA->stats->enter( STAT_SETXATTR );
   
   int rc = fs_entry_setxattr( SYNDICATEFS_DATA->core, path, name, value, size, flags, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_SETXATTR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_setxattr rc = %d\n", pthread_self(), rc );
   return rc;
}
*/

/** Get extended attributes (lgetxattr) */
int syndicatefs_getxattr(const char *path, const char *name, char *value, size_t size) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_getxattr( %s, %s, %p, %d )\n", path, name, value, size );
   
   SYNDICATEFS_DATA->stats->enter( STAT_GETXATTR );
   
   int rc = fs_entry_getxattr( SYNDICATEFS_DATA->core, path, name, value, size, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_GETXATTR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_getxattr rc = %d\n", rc );
   return rc;
}

/** List extended attributes (llistxattr) */
int syndicatefs_listxattr(const char *path, char *list, size_t size) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_listxattr( %s, %p, %d )\n", path, list, size );
   
   SYNDICATEFS_DATA->stats->enter( STAT_LISTXATTR );
   
   int rc = fs_entry_listxattr( SYNDICATEFS_DATA->core, path, list, size, conf->owner, SYNDICATEFS_DATA->core->volume );
   
   SYNDICATEFS_DATA->stats->leave( STAT_LISTXATTR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_listxattr rc = %d\n", rc );
   
   return rc;
}

/** Remove extended attributes (lremovexattr) */
/* -- not used in syndicate-ipc
int syndicatefs_removexattr(const char *path, const char *name) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_removexattr( %s, %s )\n", pthread_self(), path, name );
   
   SYNDICATEFS_DATA->stats->enter( STAT_REMOVEXATTR );
   
   int rc = fs_entry_removexattr( SYNDICATEFS_DATA->core, path, name, conf->owner, SYNDICATEFS_DATA->core->volume );

   SYNDICATEFS_DATA->stats->leave( STAT_REMOVEXATTR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_removexattr rc = %d\n", pthread_self(), rc );
   return rc;
}
*/

/** Open directory (opendir) */
//int syndicatefs_opendir(const char *path, struct fuse_file_info *fi) {
int syndicatefs_opendir(const char *path, struct IPCFileInfo *fi) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_opendir( %s )\n", path );
   
   SYNDICATEFS_DATA->stats->enter( STAT_OPENDIR );

   int rc = 0;
   struct fs_dir_handle* fdh = fs_entry_opendir( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, &rc );
   
   if( rc == 0 ) {
      //fi->fh = (uint64_t)fdh;
      fi->handle = (long long int)fdh;
   }
   
   SYNDICATEFS_DATA->stats->leave( STAT_OPENDIR, rc );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_opendir rc = %d\n", rc );
   
   return rc;
}

/** Read directory (readdir)
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 */
//int syndicatefs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
int syndicatefs_readdir(std::vector<char*> &entryVector, struct IPCFileInfo *fi) {

   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_readdir\n" );
   
   SYNDICATEFS_DATA->stats->enter( STAT_READDIR );

   //struct fs_dir_handle* fdh = (struct fs_dir_handle *)fi->fh;     // get back our DIR instance
   struct fs_dir_handle* fdh = (struct fs_dir_handle*)fi->handle;
   
   int rc = 0;
   struct fs_dir_entry** dirents = fs_entry_readdir( SYNDICATEFS_DATA->core, fdh, &rc );
   
   if( rc == 0 && dirents ) {
      
      // fill in the directory data
      int i = 0;
      while( dirents[i] != NULL ) {
         //if( filler(buf, dirents[i]->data.name, NULL, 0) != 0 ) {
         //    logerr( SYNDICATEFS_DATA->logfile, "ERR: syndicatefs_readdir filler: buffer full\n" );
         //    rc = -ENOMEM;
         //    break;
         //}

         int entryPathLen = strlen(dirents[i]->data.name);
         char* entryPath = new char[entryPathLen + 1];
         memcpy(entryPath, dirents[i]->data.name, entryPathLen);
         entryPath[entryPathLen] = 0;
         entryVector.push_back(entryPath);
         i++;
      }
   }
   
   fs_dir_entry_destroy_all( dirents );
   free( dirents );
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_readdir rc = %d\n", rc );
   
   SYNDICATEFS_DATA->stats->leave( STAT_READDIR, rc );
   return rc;
}

/** Release directory (closedir) */
//int syndicatefs_releasedir(const char *path, struct fuse_file_info *fi) {
int syndicatefs_releasedir(struct IPCFileInfo *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_releasedir\n" );
   
   SYNDICATEFS_DATA->stats->enter( STAT_RELEASEDIR );
   
   //struct fs_dir_handle* fdh = (struct fs_dir_handle*)fi->fh;
   struct fs_dir_handle* fdh = (struct fs_dir_handle*)fi->handle;
   
   int rc = fs_entry_closedir( SYNDICATEFS_DATA->core, fdh );
   
   free( fdh );
   
   SYNDICATEFS_DATA->stats->leave( STAT_RELEASEDIR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_releasedir rc = %d\n", rc );
   return rc;
}

/** Synchronize directory contents (no-op) */
/* -- not used in syndicate-ipc
int syndicatefs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) {
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_fsyncdir( %s, %d, %p )\n", pthread_self(), path, datasync, fi );
   
   SYNDICATEFS_DATA->stats->enter( STAT_FSYNCDIR );
   
   SYNDICATEFS_DATA->stats->leave( STAT_FSYNCDIR, 0 );

   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_fsyncdir rc = %d\n", pthread_self(), 0 );
   return 0;
}
*/

/**
 * Check file access permissions (access)
 */
/* -- not used in syndicate-ipc
int syndicatefs_access(const char *path, int mask) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_access( %s, %x )\n", pthread_self(), path, mask );
   
   SYNDICATEFS_DATA->stats->enter( STAT_ACCESS );
   
   int rc = fs_entry_access( SYNDICATEFS_DATA->core, path, mask, conf->owner, SYNDICATEFS_DATA->core->volume );
      
   SYNDICATEFS_DATA->stats->leave( STAT_ACCESS, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "%16lx: syndicatefs_access rc = %d\n", pthread_self(), rc );
   return rc;
}
*/

/**
 * Create and open a file (creat)
 */
//int syndicatefs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
int syndicatefs_create(const char *path, mode_t mode, struct IPCFileInfo *fi) {
   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_create( %s, %o )\n", path, mode );
   
   SYNDICATEFS_DATA->stats->enter( STAT_CREATE );
   
   int rc = 0;
   struct fs_file_handle* fh = fs_entry_create( SYNDICATEFS_DATA->core, path, conf->owner, SYNDICATEFS_DATA->core->volume, mode, &rc );
   
   if( rc == 0 && fh != NULL ) {
      //fi->fh = (uint64_t)( fh );
      fi->handle = (long long int)fh;
   }
   
   SYNDICATEFS_DATA->stats->leave( STAT_CREATE, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_create rc = %d\n", rc );
   return rc;
}

/**
 * Change the size of an file (ftruncate)
 */
//int syndicatefs_ftruncate(const char *path, off_t length, struct fuse_file_info *fi) {
int syndicatefs_ftruncate(off_t length, struct IPCFileInfo *fi) {

   struct md_syndicate_conf* conf = &SYNDICATEFS_DATA->conf;
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_ftruncate( %ld, %p )\n", length, fi );
   
   SYNDICATEFS_DATA->stats->enter( STAT_FTRUNCATE );

   //struct fs_file_handle* fh = (struct fs_file_handle*)fi->fh;
   struct fs_file_handle* fh = (struct fs_file_handle*)fi->handle;
   int rc = fs_entry_ftruncate( SYNDICATEFS_DATA->core, fh, length, conf->owner, SYNDICATEFS_DATA->core->volume );
   if( rc != 0 ) {
      errorf( "fs_entry_ftruncate rc = %d\n", rc );
   }
   
   SYNDICATEFS_DATA->stats->leave( STAT_FTRUNCATE, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_ftrunctate rc = %d\n", rc );
   
   return rc;
}

/**
 * Get attributes from an open file (fstat)
 */
//int syndicatefs_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
int syndicatefs_fgetattr(struct stat *statbuf, struct IPCFileInfo *fi) {
   
   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_fgetattr\n" );
   
   SYNDICATEFS_DATA->stats->enter( STAT_FGETATTR );
   
   //struct fs_file_handle* fh = (struct fs_file_handle*)(fi->fh);
   struct fs_file_handle* fh = (struct fs_file_handle*) (fi->handle);
   int rc = fs_entry_fstat( SYNDICATEFS_DATA->core, fh, statbuf );
   
   SYNDICATEFS_DATA->stats->leave( STAT_FGETATTR, rc );

   logmsg( SYNDICATEFS_DATA->logfile, "syndicateipc_fgetattr rc = %d\n", rc );
   
   return rc;
}

/*
 * Networking Implementations
 */

class protocol {
/*
 * Incoming Packet Structure
    4 byte : OP code
    4 byte : Total Message Size
    4 byte : The number of inner messages
    [If have inner messages]
    {
        4 byte : length of message
        n byte : message body
    } repeats

 * Outgoing Packet Structure
    4 byte : OP code
    4 byte : Result of function call (0 : OK, other : Error Code)
    4 byte : Total Message Size
    4 byte : The number of inner messages
    [If have inner messages]
    {
        4 byte : length of message
        n byte : message body
    } repeats
 */

public:
    protocol() {
        preallocated_buffer_ = new char[PREALLOCATED_OUT_BUFFER_LENGTH];
    }

    ~protocol() {
        if(preallocated_buffer_ != NULL) {
            delete preallocated_buffer_;
            preallocated_buffer_ = NULL;
        }
    }

    void process_getStat(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - stat\n");
        char *bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        struct stat statbuf;
        int returncode = syndicatefs_getattr(path, &statbuf);

        IPCStat stat;
        copyStatToIPCStat(&stat, &statbuf);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCSTAT;
        }

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }

        char *outBuffer = *data_out;
        char *bufferNext;

        writeHeader(outBuffer, OP_GET_STAT, returncode, 4 + SIZE_IPCSTAT, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeStat(outBuffer, &stat, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_delete(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - delete\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        int returncode = syndicatefs_unlink(path);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_DELETE, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_removeDir(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - remove directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        int returncode = syndicatefs_rmdir(path);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_REMOVE_DIRECTORY, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_rename(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - rename\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char path1[MAX_PATH_SIZE];
        char path2[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path1, &bytes_ptr2);
        readPath(bytes_ptr2, path2, &bytes_ptr3);

        // call
        int returncode = syndicatefs_rename(path1, path2);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_RENAME, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_makeDir(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - make directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);
        mode_t mode = 509; // default

        // call
        int returncode = syndicatefs_mkdir(path, mode);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_MKDIR, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_readDir(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - read directory\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        struct IPCFileInfo fi;
        int returncode = syndicatefs_opendir(path, &fi);

        std::vector<char*> entryVector;
        if (returncode == 0) {
            returncode = syndicatefs_readdir(entryVector, &fi);
        }

        if (returncode == 0) {
            returncode = syndicatefs_releasedir(&fi);
        }

        int totalMessageSize = 0;
        int numOfEntries = entryVector.size();

        for (int i = 0; i < numOfEntries; i++) {
            totalMessageSize += strlen(entryVector[i]);
        }

        totalMessageSize += 4 * numOfEntries;

        int toWriteSize = 16 + totalMessageSize;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_READ_DIRECTORY, returncode, totalMessageSize, numOfEntries, &bufferNext);

        for (int j = 0; j < numOfEntries; j++) {
            outBuffer = bufferNext;
            writePath(outBuffer, entryVector[j], strlen(entryVector[j]), &bufferNext);
            delete entryVector[j];
        }

        entryVector.clear();

        *data_out_size = toWriteSize;
    }

    void process_getFileHandle(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - get file handle\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);

        // call
        IPCFileInfo fi;
        int returncode = syndicatefs_open(path, &fi);

        dbprintf("filehandle : %lld\n", fi.handle);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCFILEINFO;
        }

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_GET_FILE_HANDLE, returncode, 4 + SIZE_IPCFILEINFO, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeFileInfo(outBuffer, &fi, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_createNewFile(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - create new file\n");
        char* bytes_ptr;
        char path[MAX_PATH_SIZE];
        readPath(message, path, &bytes_ptr);
        mode_t mode = 33204; // default

        // call
        IPCFileInfo fi;
        int returncode = syndicatefs_create(path, mode, &fi);

        struct stat statbuf;
        if (returncode == 0) {
            returncode = syndicatefs_fgetattr(&statbuf, &fi);
        }

        if (returncode == 0) {
            returncode = syndicatefs_release(&fi);
        }

        IPCStat stat;
        copyStatToIPCStat(&stat, &statbuf);

        int toWriteSize = 16;
        if (returncode == 0) {
            toWriteSize += 4 + SIZE_IPCSTAT;
        }

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_CREATE_NEW_FILE, returncode, 4 + SIZE_IPCSTAT, 1, &bufferNext);
        if (returncode == 0) {
            outBuffer = bufferNext;
            writeStat(outBuffer, &stat, &bufferNext);
        }

        *data_out_size = toWriteSize;
    }

    void process_readFileData(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - read file data\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char* bytes_ptr4;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        dbprintf("filehandle : %lld\n", fi.handle);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);

        int size;
        readInt(bytes_ptr3, &size, &bytes_ptr4);

        dbprintf("offset : %lld, size : %d\n", fileoffset, size);

        // call
        char* buffer = new char[size];
        int returncode = syndicatefs_read(buffer, size, fileoffset, &fi);

        int toWriteSize = 16;
        if (returncode > 0) {
            toWriteSize += 4 + (int) returncode;
        }

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_READ_FILEDATA, returncode, 4 + (int) returncode, 1, &bufferNext);
        if (returncode >= 0) {
            outBuffer = bufferNext;
            writeBytes(outBuffer, buffer, (int) returncode, &bufferNext);
        }

        delete buffer;

        *data_out_size = toWriteSize;
    }

    void process_writeFileData(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - write file data\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char* bytes_ptr4;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        dbprintf("filehandle : %lld\n", fi.handle);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);

        char* rawData;
        int rawDataSize = readBytes(bytes_ptr3, &rawData, &bytes_ptr4);

        // call
        int returncode = syndicatefs_write(rawData, rawDataSize, fileoffset, &fi);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_WRITE_FILE_DATA, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_flush(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - flush file data\n");
        char* bytes_ptr;
        IPCFileInfo fi;
        readFileInfo(message, &fi, &bytes_ptr);

        // call
        int returncode = syndicatefs_flush(&fi);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_FLUSH, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }

    void process_closeFileHandle(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - close file handle\n");
        char* bytes_ptr;
        IPCFileInfo fi;
        readFileInfo(message, &fi, &bytes_ptr);

        // call
        int returncode = syndicatefs_release(&fi);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_CLOSE_FILE_HANDLE, returncode, 0, 0, &bufferNext);

        *data_out_size = toWriteSize;
    }
    
    void process_truncateFile(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - truncate file\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        IPCFileInfo fi;
        readFileInfo(bytes_ptr1, &fi, &bytes_ptr2);

        long long int fileoffset;
        readLong(bytes_ptr2, &fileoffset, &bytes_ptr3);
        
        // call
        int returncode = syndicatefs_ftruncate(fileoffset, &fi);

        int toWriteSize = 16;
        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char* outBuffer = *data_out;
        char* bufferNext;

        writeHeader(outBuffer, OP_TRUNCATE_FILE, returncode, 0, 0, &bufferNext);
        
        *data_out_size = toWriteSize;
    }
    
    void process_getXAttr(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - getxattr\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char* bytes_ptr3;
        char path[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path, &bytes_ptr2);
        
        char name[MAX_XATTR_NAME_SIZE];
        readString(bytes_ptr2, name, &bytes_ptr3);
        
        // call
        size_t xattr_size = syndicatefs_getxattr(path, name, NULL, 0);
        char* value = NULL;
        int returncode = 0;

        if(xattr_size > 0) {
            value = new char[xattr_size+1];
            memset(value, 0, xattr_size+1);
            returncode = syndicatefs_getxattr(path, name, value, xattr_size+1);
        } else {
            returncode = xattr_size;
        }


        int attrLen = 0;
        if (returncode >= 0) {
            attrLen = returncode;
        }

        int toWriteSize = 16;
        if (returncode >= 0) {
            toWriteSize += 4 + attrLen;
        }

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char *outBuffer = *data_out;
        char *bufferNext;

        if (returncode >= 0) {
            writeHeader(outBuffer, OP_GET_EXTENDED_ATTR, returncode, 4 + attrLen, 1, &bufferNext);
        } else {
            writeHeader(outBuffer, OP_GET_EXTENDED_ATTR, returncode, 0, 1, &bufferNext);
        }
        if (returncode >= 0) {
            outBuffer = bufferNext;
            writeString(outBuffer, value, attrLen, &bufferNext);
        }

        if (value != NULL) {
            delete value;
        }

        *data_out_size = toWriteSize;
    }
    
    void process_listXAttr(const char *message, char **data_out, int *data_out_size, bool *free_data_out) {
        dbprintf("%s", "process - listxattr\n");
        char* bytes_ptr1 = (char*)message;
        char* bytes_ptr2;
        char path[MAX_PATH_SIZE];
        readPath(bytes_ptr1, path, &bytes_ptr2);
        
        // call
        size_t xattrlist_size = syndicatefs_listxattr(path, NULL, 0);
        char* list = NULL;
        int returncode = 0;

        if(xattrlist_size > 0) {
            list = new char[xattrlist_size+1];
            memset(list, 0, xattrlist_size+1);
            returncode = syndicatefs_listxattr(path, list, xattrlist_size+1);
        } else {
            returncode = xattrlist_size;
        }

        
        std::vector<char*> entryVector;
        char* listptr = list;
        while(returncode > 0 && listptr < (list + returncode)) {
            int entryLen = strlen(listptr);
            if(entryLen > 0) {
                entryVector.push_back(listptr);
            }
            listptr += entryLen + 1;
        }
       
        int totalMessageSize = 0;
        int numOfEntries = entryVector.size();

        for (int i = 0; i < numOfEntries; i++) {
            totalMessageSize += strlen(entryVector[i]);
        }

        totalMessageSize += 4 * numOfEntries;

        int toWriteSize = 16 + totalMessageSize;

        if(toWriteSize < PREALLOCATED_OUT_BUFFER_LENGTH) {
            // use preallocated buffer
            *data_out = preallocated_buffer_;
            *free_data_out = false;
        } else {
            *data_out = new char[toWriteSize];
            *free_data_out = true;
        }
        char *outBuffer = *data_out;
        char *bufferNext;

        writeHeader(outBuffer, OP_LIST_EXTENDED_ATTR, returncode, totalMessageSize, numOfEntries, &bufferNext);
        
        for (int j = 0; j < numOfEntries; j++) {
            outBuffer = bufferNext;
            writeString(outBuffer, entryVector[j], strlen(entryVector[j]), &bufferNext);
        }
        
        entryVector.clear();
        if(list != NULL) {
            delete list;
        }

        *data_out_size = toWriteSize;
    }

private:
    enum {
        PREALLOCATED_OUT_BUFFER_LENGTH = 10*1024*1024, // 10MB
    };
    char* preallocated_buffer_;

private:
    int writeHeader(const char* buffer, int opcode, int returncode, int totalMsgSize, int totalNumOfMsg, char** bufferNext) {
        char* bytes_ptr = (char*)buffer;
        writeIntToNetworkBytes(bytes_ptr, opcode);
        bytes_ptr += 4;

        writeIntToNetworkBytes(bytes_ptr, returncode);
        bytes_ptr += 4;

        writeIntToNetworkBytes(bytes_ptr, totalMsgSize);
        bytes_ptr += 4;

        writeIntToNetworkBytes(bytes_ptr, totalNumOfMsg);
        bytes_ptr += 4;

        *bufferNext = bytes_ptr;
        return 16;
    }

    int readString(const char* msgFrom, char* outString, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        strncpy(outString, bytes_ptr, msgLen);
        outString[msgLen] = 0;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }
    
    int readPath(const char* msgFrom, char* outPath, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        strncpy(outPath, bytes_ptr, msgLen);
        outPath[msgLen] = 0;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeString(char* buffer, const char* inString, int strLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, strLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, inString, strLen);
        bytes_ptr += strLen;

        *bufferNext = bytes_ptr;
        return strLen + 4;
    }
    
    int writePath(char* buffer, const char* path, int pathLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, pathLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, path, pathLen);
        bytes_ptr += pathLen;

        *bufferNext = bytes_ptr;
        return pathLen + 4;
    }

    int readFileInfo(const char* msgFrom, IPCFileInfo* outFileInfo, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        outFileInfo->handle = readLongFromNetworkBytes(bytes_ptr);
        bytes_ptr += SIZE_IPCFILEINFO;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeFileInfo(char* buffer, const IPCFileInfo* inFileInfo, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, 8);
        bytes_ptr += 4;

        writeLongToNetworkBytes(bytes_ptr, inFileInfo->handle);
        bytes_ptr += SIZE_IPCFILEINFO;

        *bufferNext = bytes_ptr;
        return SIZE_IPCFILEINFO + 4;
    }

    int writeStat(char* buffer, const IPCStat* stat, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, SIZE_IPCSTAT);
        bytes_ptr += 4;

        writeIntToNetworkBytes(bytes_ptr, stat->st_mode);
        bytes_ptr += 4;
        writeIntToNetworkBytes(bytes_ptr, stat->st_uid);
        bytes_ptr += 4;
        writeIntToNetworkBytes(bytes_ptr, stat->st_gid);
        bytes_ptr += 4;
        writeLongToNetworkBytes(bytes_ptr, stat->st_size);
        bytes_ptr += 8;
        writeLongToNetworkBytes(bytes_ptr, stat->st_blksize);
        bytes_ptr += 8;
        writeLongToNetworkBytes(bytes_ptr, stat->st_blocks);
        bytes_ptr += 8;
        writeLongToNetworkBytes(bytes_ptr, stat->st_atim);
        bytes_ptr += 8;
        writeLongToNetworkBytes(bytes_ptr, stat->st_mtim);
        bytes_ptr += 8;

        *bufferNext = bytes_ptr;
        return 4 + SIZE_IPCSTAT;
    }

    int readLong(const char* msgFrom, long long int* outLong, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        *outLong = readLongFromNetworkBytes(bytes_ptr);
        bytes_ptr += 8;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeLong(char* buffer, long long int value, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, 8);
        bytes_ptr += 4;

        writeLongToNetworkBytes(bytes_ptr, value);
        bytes_ptr += 8;

        *bufferNext = bytes_ptr;
        return 4 + 8;
    }

    int readInt(const char* msgFrom, int* outInt, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        *outInt = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeInt(char* buffer, int value, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, 4);
        bytes_ptr += 4;

        writeIntToNetworkBytes(bytes_ptr, value);
        bytes_ptr += 4;

        *bufferNext = bytes_ptr;
        return 8;
    }

    int readBytes(const char* msgFrom, char** rawData, char** msgNext) {
        char* bytes_ptr = (char*) msgFrom;
        int msgLen = readIntFromNetworkBytes(bytes_ptr);
        bytes_ptr += 4;

        *rawData = bytes_ptr;
        bytes_ptr += msgLen;

        *msgNext = bytes_ptr;
        return msgLen;
    }

    int writeBytes(char* buffer, const char* bytes, int byteLen, char** bufferNext) {
        char* bytes_ptr = buffer;
        writeIntToNetworkBytes(bytes_ptr, byteLen);
        bytes_ptr += 4;

        memcpy(bytes_ptr, bytes, byteLen);
        bytes_ptr += byteLen;

        *bufferNext = bytes_ptr;
        return byteLen + 4;
    }
};

class session {
public:
    session(boost::asio::io_service& io_service)
    : socket_(io_service) {
    }

    ~session() {
        if(protocol_ != NULL) {
            delete protocol_;
            protocol_ = NULL;
        }

        if (message_ != NULL) {
            delete message_;
            message_ = NULL;
        }

        if(data_out_free_) {
            if (data_out_ != NULL) {
                delete data_out_;
                data_out_ = NULL;
            }
        }
    }

    tcp::socket& socket() {
        return socket_;
    }

    void start() {
        protocol_ = new protocol();
        stage_ = STAGE_READ_HEADER;
        header_offset_ = 0;
        message_ = NULL;
        message_offset_ = 0;
        data_out_ = NULL;
        data_out_free_ = false;
        memset(message_preallocated_buffer_, 0, PREALLOCATED_MESSAGE_BUFFER_LENGTH);

        socket_.async_read_some(boost::asio::buffer(data_in_, MAX_IN_BUFFER_LENGTH),
                boost::bind(&session::handle_read, this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
    }

    void handle_read(const boost::system::error_code& error, size_t bytes_transferred) {
        if (!error) {
            int bytes_remain = (int) bytes_transferred;
            char* bytes_ptr = data_in_;
            while (bytes_remain > 0) {
                if (stage_ == STAGE_READ_HEADER) {
                    if (bytes_remain >= PACKET_HEADER_LENGTH - header_offset_) {
                        int readSize = PACKET_HEADER_LENGTH - header_offset_;
                        memcpy(header_ + header_offset_, bytes_ptr, readSize);
                        header_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                        // move stage
                        stage_ = STAGE_READ_DATA;
                        //dbprintf("stage -> read_data\n");
                        // parse header
                        op_code_ = readIntFromNetworkBytes(header_);
                        //dbprintf("hdr opcode : %d\n", op_code_);
                        total_msg_size_ = readIntFromNetworkBytes(header_ + 4);
                        //dbprintf("hdr msg_size : %d\n", total_msg_size_);
                        num_messages_ = readIntFromNetworkBytes(header_ + 8);
                        //dbprintf("hdr num_messages : %d\n", num_messages_);
                        // allocate message if message size exceeds preallocated buffer size
                        if(total_msg_size_ > PREALLOCATED_MESSAGE_BUFFER_LENGTH) {
                            // allocate dynamic
                            message_ = new char[total_msg_size_];
                        }
                        message_offset_ = 0;
                    } else {
                        // chunked header
                        int readSize = bytes_remain;
                        memcpy(header_ + header_offset_, bytes_ptr, readSize);
                        header_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                    }
                } else if (stage_ == STAGE_READ_DATA) {
                    if (bytes_remain >= total_msg_size_ - message_offset_) {
                        int readSize = total_msg_size_ - message_offset_;
                        if(total_msg_size_ > PREALLOCATED_MESSAGE_BUFFER_LENGTH) {
                            memcpy(message_ + message_offset_, bytes_ptr, readSize);
                        } else {
                            memcpy(message_preallocated_buffer_ + message_offset_, bytes_ptr, readSize);
                        }
                        message_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;

                        if(bytes_remain > 0) {
                            dbprintf("%s", "cut-off noises!\n");
                            bytes_remain = 0;
                        }

                        // call processor
                        if(total_msg_size_ > PREALLOCATED_MESSAGE_BUFFER_LENGTH) {
                            handle_protocol(message_);
                        }
                        else {
                            handle_protocol(message_preallocated_buffer_);
                        }
                        // move stage
                        stage_ = STAGE_READ_HEADER;
                        header_offset_ = 0;
                        if(total_msg_size_ > PREALLOCATED_MESSAGE_BUFFER_LENGTH) {
                            if (message_ != NULL) {
                                delete message_;
                                message_ = NULL;
                            }
                        } else {
                            memset(message_preallocated_buffer_, 0, PREALLOCATED_MESSAGE_BUFFER_LENGTH);
                        }
                    } else {
                        // chunked data
                        int readSize = bytes_remain;
                        if(total_msg_size_ > PREALLOCATED_MESSAGE_BUFFER_LENGTH) {
                            memcpy(message_ + message_offset_, bytes_ptr, readSize);
                        } else {
                            memcpy(message_preallocated_buffer_ + message_offset_, bytes_ptr, readSize);
                        }
                        message_offset_ += readSize;
                        bytes_ptr += readSize;
                        bytes_remain -= readSize;
                    }
                }
            }

            // continue read
            socket_.async_read_some(boost::asio::buffer(data_in_, MAX_IN_BUFFER_LENGTH),
                    boost::bind(&session::handle_read, this,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
        } else {
            errorf("%s", "error\n");
            if (message_ != NULL) {
                delete message_;
                message_ = NULL;
            }

            if(data_out_free_) {
                if (data_out_ != NULL) {
                    delete data_out_;
                    data_out_ = NULL;
                }
            }
            delete this;
        }
    }

    void handle_write(const boost::system::error_code& error) {
        if (!error) {
            if (message_ != NULL) {
                delete message_;
                message_ = NULL;
            }

            if(data_out_free_) {
                if (data_out_ != NULL) {
                    delete data_out_;
                    data_out_ = NULL;
                }
            }
        } else {
            errorf("%s", "error\n");

            if (message_ != NULL) {
                delete message_;
                message_ = NULL;
            }

            if(data_out_free_) {
                if (data_out_ != NULL) {
                    delete data_out_;
                    data_out_ = NULL;
                }
            }
            delete this;
        }
    }

private:
    void handle_protocol(const char* message) {
        //dbprintf("%s", "read done!\n");
        dbprintf("op-code : %d\n", op_code_);
        //dbprintf("total message size : %d\n", total_msg_size_);
        //dbprintf("number of messages : %d\n", num_messages_);

        int data_out_size = 0;

        switch (op_code_) {
            case OP_GET_STAT:
                protocol_->process_getStat(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_DELETE:
                protocol_->process_delete(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_REMOVE_DIRECTORY:
                protocol_->process_removeDir(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_RENAME:
                protocol_->process_rename(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_MKDIR:
                protocol_->process_makeDir(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_READ_DIRECTORY:
                protocol_->process_readDir(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_GET_FILE_HANDLE:
                protocol_->process_getFileHandle(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_CREATE_NEW_FILE:
                protocol_->process_createNewFile(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_READ_FILEDATA:
                protocol_->process_readFileData(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_WRITE_FILE_DATA:
                protocol_->process_writeFileData(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_FLUSH:
                protocol_->process_flush(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_CLOSE_FILE_HANDLE:
                protocol_->process_closeFileHandle(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_TRUNCATE_FILE:
                protocol_->process_truncateFile(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_GET_EXTENDED_ATTR:
                protocol_->process_getXAttr(message, &data_out_, &data_out_size, &data_out_free_);
                break;
            case OP_LIST_EXTENDED_ATTR:
                protocol_->process_listXAttr(message, &data_out_, &data_out_size, &data_out_free_);
                break;
        }

        if(data_out_size > 0) {
            boost::asio::async_write(socket_,
                boost::asio::buffer(data_out_, data_out_size),
                boost::bind(&session::handle_write, this,
                boost::asio::placeholders::error));
        } else {
            errorf("%s", "protocol handler returned 0 output\n");
        }
    }

private:
    tcp::socket socket_;
    protocol* protocol_;
    enum {
        MAX_IN_BUFFER_LENGTH = 4096,
    };
    char data_in_[MAX_IN_BUFFER_LENGTH];
    char* data_out_;
    bool data_out_free_;

    int op_code_;
    int total_msg_size_;
    int num_messages_;

    enum {
        // int * 3
        PACKET_HEADER_LENGTH = 12,
    };
    char header_[PACKET_HEADER_LENGTH];
    int header_offset_;

    char* message_;
    enum {
        PREALLOCATED_MESSAGE_BUFFER_LENGTH = 4096,
    };
    char message_preallocated_buffer_[PREALLOCATED_MESSAGE_BUFFER_LENGTH];
    int message_offset_;

    enum {
        STAGE_READ_HEADER = 0,
        STAGE_READ_DATA = 1,
    };
    int stage_;
};

class server {
public:
    server(boost::asio::io_service& io_service, short port)
        : io_service_(io_service), acceptor_(io_service, tcp::endpoint(tcp::v4(), port)) {
        session* new_session = new session(io_service_);
        acceptor_.listen();
        acceptor_.async_accept(new_session->socket(),
                boost::bind(&server::handle_accept, this, new_session,
                boost::asio::placeholders::error));
    }

    void handle_accept(session* new_session, const boost::system::error_code& error) {
        if (!error) {
            new_session->start();
            new_session = new session(io_service_);
            acceptor_.async_accept(new_session->socket(),
                    boost::bind(&server::handle_accept, this, new_session,
                    boost::asio::placeholders::error));
        } else {
            delete new_session;
        }
    }

private:
    boost::asio::io_service& io_service_;
    tcp::acceptor acceptor_;
};


// handle extra options for IPC
static int ipcportnum = -1;

int grab_ipc_opts( int ipc_opt, char* ipc_arg ) {
   int rc = 0;

   switch( ipc_opt ) {
      case 'O': {
         // ipc service port number
         ipcportnum = strtol(ipc_arg, NULL, 10 );
         break;
      }
      default: {
         rc = UG_handle_opt( ipc_opt, ipc_arg );
         break;
      }
   }
   
   return rc;
}

void extra_usage(void) {
   fprintf(stderr, "\
Gateway-specific arguments:\n\
   -O PORTNUM\n\
            IPC port number\n\
\n");
}

// Program execution starts here!
int main(int argc, char** argv) {

    curl_global_init(CURL_GLOBAL_ALL);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int rc = 0;

    // prevent root from mounting this, since we don't really do much
    // in the way of checking access.
#ifndef _FIREWALL
    if( getuid() == 0 || geteuid() == 0 ) {
        perror("Running SyndicateIPC as root opens unnacceptable security holes\n");
       return 1;
    }
#else
    // skip
#endif
    
    struct md_opts opts;
    memset( &opts, 0, sizeof(struct md_opts) );
    UG_opts_init();
    
    rc = md_parse_opts( &opts, argc, argv, NULL, "O:", grab_ipc_opts );
    if( rc != 0 ) {
       md_common_usage( argv[0] );
       UG_usage();
       extra_usage();
       exit(1);
    }
    
    struct UG_opts ug_opts;
    UG_opts_get( &ug_opts );

    struct md_HTTP syndicate_http;

    // start core services
    rc = syndicate_init( &opts, &ug_opts );
    if (rc != 0) {
        exit(1);
    }

    struct syndicate_state* state = syndicate_get_state();
 
    // start back-end HTTP server
    rc = server_init( state, &syndicate_http );
    if( rc != 0 )
       exit(1);

    // finish initialization
    syndicate_set_running();
    
    syndicateipc_get_context()->syndicate_state_data = state;
    syndicateipc_get_context()->syndicate_http = syndicate_http;

    printf("\n\nSyndicateIPC starting up\n\n");

    try {
        boost::asio::io_service io_service;
        server s(io_service, ipcportnum);
        io_service.run();
   } catch (std::exception& e) {
       std::cerr << "Exception: " << e.what() << "\n";
   }

   printf( "\n\nSyndicateIPC shutting down\n\n");

   server_shutdown( &syndicate_http );

   int wait_replicas = -1;
   if( !ug_opts.flush_replicas ) {
       wait_replicas = 0;
   }
   
   syndicate_destroy( wait_replicas );
   
   curl_global_cleanup();
   google::protobuf::ShutdownProtobufLibrary();
   
   return 0;
}


