/* 
 * File:   JSyndicateFS.h
 * Author: iychoi
 *
 * Created on June 5, 2013, 9:42 PM
 */

// this is the  implementation to SyndicateFS

#ifndef JSYNDICATEFS_H
#define	JSYNDICATEFS_H

#include "JSyndicateFSContext.h"
#include "JSyndicateFSFileInfo.h"
#include "JSyndicateFSFillDir.h"
#include "JSyndicateFSConfig.h"
#include "JSyndicateFSStat.h"
#include "JSyndicateFSUtimbuf.h"
#include "JSyndicateFSStatvfs.h"

#include "stats.h"
#include "log.h"
#include "fs.h"
#include "replication.h"
#include "collator.h"
#include "syndicate.h"

#include <libsyndicate.h>

#include <signal.h>
#include <getopt.h>
#include <sys/statvfs.h>

#ifdef	__cplusplus
extern "C" {
#endif

/*
 * Initialization & Destruction
 */
int jsyndicatefs_init(const JSyndicateFS_Config *cfg);
int jsyndicatefs_destroy();
    
/*
 * Filesystem Operations
 */
int jsyndicatefs_getattr(const char *path, struct stat *statbuf);
int jsyndicatefs_mknod(const char *path, mode_t mode, dev_t dev);
int jsyndicatefs_mkdir(const char *path, mode_t mode);
int jsyndicatefs_unlink(const char *path);
int jsyndicatefs_rmdir(const char *path);
int jsyndicatefs_rename(const char *path, const char *newpath);
int jsyndicatefs_chmod(const char *path, mode_t mode);
int jsyndicatefs_truncate(const char *path, off_t newsize);
int jsyndicatefs_utime(const char *path, struct utimbuf *ubuf);
int jsyndicatefs_open(const char *path, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_read(const char *path, char *buf, size_t size, off_t offset, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_write(const char *path, const char *buf, size_t size, off_t offset, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_statfs(const char *path, struct statvfs *statv);
int jsyndicatefs_flush(const char *path, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_release(const char *path, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_fsync(const char *path, int datasync, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
int jsyndicatefs_getxattr(const char *path, const char *name, char *value, size_t size);
int jsyndicatefs_listxattr(const char *path, char *list, size_t size);
int jsyndicatefs_removexattr(const char *path, const char *name);
int jsyndicatefs_opendir(const char *path, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_readdir(void *pjenv, void *pjobj, const char *path, JSyndicateFS_Fill_Dir_t filler, off_t offset, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_releasedir(const char *path, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_fsyncdir(const char *path, int datasync, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_access(const char *path, int mask);
int jsyndicatefs_create(const char *path, mode_t mode, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_ftruncate(const char *path, off_t offset, struct JSyndicateFS_FileInfo *fi);
int jsyndicatefs_fgetattr(const char *path, struct stat *statbuf, struct JSyndicateFS_FileInfo *fi);


#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFS_H */

