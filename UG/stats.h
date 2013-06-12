/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _STATS_H_
#define _STATS_H_

#include "libsyndicate.h"
#include "log.h"
#include <string>
#include <sys/time.h>

using namespace std;

enum stat_types {
   STAT_GETATTR = 1,
   STAT_READLINK,
   STAT_MKNOD,
   STAT_MKDIR,
   STAT_UNLINK,
   STAT_RMDIR,
   STAT_SYMLINK,
   STAT_RENAME,
   STAT_LINK,
   STAT_CHMOD,
   STAT_CHOWN,
   STAT_TRUNCATE,
   STAT_UTIME,
   STAT_OPEN,
   STAT_READ,
   STAT_WRITE,
   STAT_STATFS,
   STAT_FLUSH,
   STAT_RELEASE,
   STAT_FSYNC,
   STAT_SETXATTR,
   STAT_GETXATTR,
   STAT_LISTXATTR,
   STAT_REMOVEXATTR,
   STAT_OPENDIR,
   STAT_READDIR,
   STAT_RELEASEDIR,
   STAT_FSYNCDIR,
   STAT_ACCESS,
   STAT_CREATE,
   STAT_FTRUNCATE,
   STAT_FGETATTR,
   
   STAT_NUM_TYPES
};


// instrumentation module
class Stats {

public:
   Stats( char* output_path );
   ~Stats();
   
   void use_conf( struct md_syndicate_conf* conf );
   
   // log a call
   void enter( int stat_type );
   void leave( int stat_type, int rc );
   
   // dump results
   string dump();

private:
   
   // stat counters
   uint64_t call_counts[ STAT_NUM_TYPES ];        // how often each call was made
   uint64_t call_errors[ STAT_NUM_TYPES ];        // how often each call failed
   uint64_t begin_call_times[ STAT_NUM_TYPES ];    // when a call was last begun
   uint64_t elapsed_time[ STAT_NUM_TYPES ];       // total time spent in each function
   char* output_path;                        // where to dump stats (preferably on a RAM fs)
   bool gather_stats;
   
   void (*enter_func)(uint64_t* enter_times, int type );
   void (*leave_func)(uint64_t* count_times, uint64_t* elapsed_times, uint64_t* error_counts, uint64_t* begin_times, int type, int rc );
};

#endif
