#ifndef _I_FUSELIB_TRACE_H_
#define _I_FUSELIB_TRACE_H_

// trace wrappers around the irods Fuse operations

#include <sys/statvfs.h>

// don't include if all we're doing is testing the trace functionality
#ifndef _TEST_TRACE
#include "rodsClient.h"
#include "rodsPath.h"
#include "iFuseLib.h"
#include "iFuseLib.Lock.h"
#endif

#include "iFuseLib.Logging.h"
#include "iFuseLib.Http.h"

#ifdef  __cplusplus
extern "C" {
#endif

// don't compile these if all we're doing is testing the trace functionality
#ifndef _TEST_TRACE
int traced_irodsGetattr(const char *path, struct stat *stbuf);
int traced_irodsReadlink(const char *path, char *buf, size_t size);
int traced_irodsMknod(const char *path, mode_t mode, dev_t rdev);
int traced_irodsMkdir(const char *path, mode_t mode);
int traced_irodsUnlink(const char *path);
int traced_irodsRmdir(const char *path);
int traced_irodsSymlink(const char *from, const char *to);
int traced_irodsRename(const char *from, const char *to);
int traced_irodsLink(const char *from, const char *to);
int traced_irodsChmod(const char *path, mode_t mode);
int traced_irodsChown(const char *path, uid_t uid, gid_t gid);
int traced_irodsTruncate(const char *path, off_t size);
int traced_irodsFlush(const char *path, struct fuse_file_info *fi);
int traced_irodsUtimens (const char *path, const struct timespec ts[]);
int traced_irodsOpen(const char *path, struct fuse_file_info *fi);
int traced_irodsRead(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int traced_irodsWrite(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int traced_irodsStatfs(const char *path, struct statvfs *stbuf);
int traced_irodsRelease(const char *path, struct fuse_file_info *fi);
int traced_irodsFsync (const char *path, int isdatasync, struct fuse_file_info *fi);
int traced_irodsReaddir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
#endif

int trace_get_environment_variables( char** http_host, int* portnum, int* sync_delay, char** log_path_salt );
int trace_begin( struct log_context** ctx );
int trace_end( struct log_context** ctx );

void trace_usage(void);

extern log_context* LOGCTX;

// helpful macros
#define strdup_or_default( s, d ) ((s) != NULL ? strdup(s) : ((d) != NULL ? strdup(d) : NULL))

#ifdef  __cplusplus
}
#endif


#endif