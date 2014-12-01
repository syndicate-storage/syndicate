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

// this is the FUSE interface to Syndicate

#ifndef _SYNDICATEFS_H_
#define _SYNDICATEFS_H_

#include "libsyndicate/libsyndicate.h"
#include "stats.h"
#include "log.h"
#include "fs.h"
#include "replication.h"
#include "syndicate.h"
#include "server.h"
#include "libsyndicate/opts.h"

#define FUSE_USE_VERSION 28

#include <signal.h>

#include <fuse.h>

//class IOProcessor;

#define SYNDICATEFS_DATA ((struct syndicate_state *) fuse_get_context()->private_data)

extern "C" {
   
// prototypes for FUSE methods
int syndicatefs_getattr(const char *path, struct stat *statbuf);
int syndicatefs_readlink(const char *path, char *link, size_t size);
int syndicatefs_mknod(const char *path, mode_t mode, dev_t dev);
int syndicatefs_mkdir(const char *path, mode_t mode);
int syndicatefs_unlink(const char *path);
int syndicatefs_rmdir(const char *path);
int syndicatefs_symlink(const char *path, const char *link);
int syndicatefs_rename(const char *path, const char *newpath);
int syndicatefs_link(const char *path, const char *newpath);
int syndicatefs_chmod(const char *path, mode_t mode);
int syndicatefs_chown(const char *path, uid_t uid, gid_t gid);
int syndicatefs_truncate(const char *path, off_t newsize);
int syndicatefs_utime(const char *path, struct utimbuf *ubuf);
int syndicatefs_open(const char *path, struct fuse_file_info *fi);
int syndicatefs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int syndicatefs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int syndicatefs_statfs(const char *path, struct statvfs *statv);
int syndicatefs_flush(const char *path, struct fuse_file_info *fi);
int syndicatefs_release(const char *path, struct fuse_file_info *fi);
int syndicatefs_fsync(const char *path, int datasync, struct fuse_file_info *fi);
int syndicatefs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
int syndicatefs_getxattr(const char *path, const char *name, char *value, size_t size);
int syndicatefs_listxattr(const char *path, char *list, size_t size);
int syndicatefs_removexattr(const char *path, const char *name);
int syndicatefs_opendir(const char *path, struct fuse_file_info *fi);
int syndicatefs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int syndicatefs_releasedir(const char *path, struct fuse_file_info *fi);
int syndicatefs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi);
int syndicatefs_access(const char *path, int mask);
int syndicatefs_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int syndicatefs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi);
int syndicatefs_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi);
void *syndicatefs_init(struct fuse_conn_info *conn);
void syndicatefs_destroy(void *userdata);


// get the above functions as a fuse_operations structure
struct fuse_operations get_syndicatefs_opers();

}

#endif
