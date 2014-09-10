/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/

/* irodsFs.c - The main program of the iRODS/Fuse server. It is to be run to
 * serve a single client 
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <sys/stat.h>
#include "irodsFs.h"
#include "iFuseOper.h"
#include "iFuseLib.h"
#include "iFuseLib.Trace.h"

/* some global variables */
extern rodsEnv MyRodsEnv;

#ifdef  __cplusplus
struct fuse_operations irodsOper; 
#else
static struct fuse_operations irodsOper =
{
  .getattr = traced_irodsGetattr,
  .readlink = traced_irodsReadlink,
  .readdir = traced_irodsReaddir,
  .mknod = traced_irodsMknod,
  .mkdir = traced_irodsMkdir,
  .symlink = traced_irodsSymlink,
  .unlink = traced_irodsUnlink,
  .rmdir = traced_irodsRmdir,
  .rename = traced_irodsRename,
  .link = traced_irodsLink,
  .chmod = traced_irodsChmod,
  .chown = traced_irodsChown,
  .truncate = traced_irodsTruncate,
  .utimens = traced_irodsUtimens,
  .open = traced_irodsOpen,
  .read = traced_irodsRead,
  .write = traced_irodsWrite,
  .statfs = traced_irodsStatfs,
  .release = traced_irodsRelease,
  .fsync = traced_irodsFsync,
  .flush = traced_irodsFlush,
};
#endif

void usage ();

/* Note - fuse_main parses command line options 
 * static const struct fuse_opt fuse_helper_opts[] = {
    FUSE_HELPER_OPT("-d",          foreground),
    FUSE_HELPER_OPT("debug",       foreground),
    FUSE_HELPER_OPT("-f",          foreground),
    FUSE_HELPER_OPT("-s",          singlethread),
    FUSE_HELPER_OPT("fsname=",     nodefault_subtype),
    FUSE_HELPER_OPT("subtype=",    nodefault_subtype),

    FUSE_OPT_KEY("-h",          KEY_HELP),
    FUSE_OPT_KEY("--help",      KEY_HELP),
    FUSE_OPT_KEY("-ho",         KEY_HELP_NOHEADER),
    FUSE_OPT_KEY("-V",          KEY_VERSION),
    FUSE_OPT_KEY("--version",   KEY_VERSION),
    FUSE_OPT_KEY("-d",          FUSE_OPT_KEY_KEEP),
    FUSE_OPT_KEY("debug",       FUSE_OPT_KEY_KEEP),
    FUSE_OPT_KEY("fsname=",     FUSE_OPT_KEY_KEEP),
    FUSE_OPT_KEY("subtype=",    FUSE_OPT_KEY_KEEP),
    FUSE_OPT_END
};

static void usage(const char *progname)
{
    fprintf(stderr,
            "usage: %s mountpoint [options]\n\n", progname);
    fprintf(stderr,
            "general options:\n"
            "    -o opt,[opt...]        mount options\n"
            "    -h   --help            print help\n"
            "    -V   --version         print version\n"
            "\n");
}

*/

int 
main (int argc, char **argv)
{

irodsOper.getattr = traced_irodsGetattr;
irodsOper.readlink = traced_irodsReadlink;
irodsOper.readdir = traced_irodsReaddir;
irodsOper.mknod = traced_irodsMknod;
irodsOper.mkdir = traced_irodsMkdir;
irodsOper.symlink = traced_irodsSymlink;
irodsOper.unlink = traced_irodsUnlink;
irodsOper.rmdir = traced_irodsRmdir;
irodsOper.rename = traced_irodsRename;
irodsOper.link = traced_irodsLink;
irodsOper.chmod = traced_irodsChmod;
irodsOper.chown = traced_irodsChown;
irodsOper.truncate = traced_irodsTruncate;
irodsOper.utimens = traced_irodsUtimens;
irodsOper.open = traced_irodsOpen;
irodsOper.read = traced_irodsRead;
irodsOper.write = traced_irodsWrite;
irodsOper.statfs = traced_irodsStatfs;
irodsOper.release = traced_irodsRelease;
irodsOper.fsync = traced_irodsFsync;
irodsOper.flush = traced_irodsFlush;


    int status;
    rodsArguments_t myRodsArgs;
    char *optStr;

#ifdef  __cplusplus
    bzero (&irodsOper, sizeof (irodsOper));
    irodsOper.getattr = traced_irodsGetattr;
    irodsOper.readlink = traced_irodsReadlink;
    irodsOper.readdir = traced_irodsReaddir;
    irodsOper.mknod = traced_irodsMknod;
    irodsOper.mkdir = traced_irodsMkdir;
    irodsOper.symlink = traced_irodsSymlink;
    irodsOper.unlink = traced_irodsUnlink;
    irodsOper.rmdir = traced_irodsRmdir;
    irodsOper.rename = traced_irodsRename;
    irodsOper.link = traced_irodsLink;
    irodsOper.chmod = traced_irodsChmod;
    irodsOper.chown = traced_irodsChown;
    irodsOper.truncate = traced_irodsTruncate;
    irodsOper.utimens = traced_irodsUtimens;
    irodsOper.open = traced_irodsOpen;
    irodsOper.read = traced_irodsRead;
    irodsOper.write = traced_irodsWrite;
    irodsOper.statfs = traced_irodsStatfs;
    irodsOper.release = traced_irodsRelease;
    irodsOper.fsync = traced_irodsFsync;
    irodsOper.flush = traced_irodsFlush;
#endif
    optStr = "hdo:";

    status = parseCmdLineOpt (argc, argv, optStr, 0, &myRodsArgs);

    if (status < 0) {
        printf("Use -h for help.\n");
        exit (1);
    }
    if (myRodsArgs.help==True) {
       usage();
       exit(0);
    }
    
    status = getRodsEnv (&MyRodsEnv);

    if (status < 0) {
        rodsLogError(LOG_ERROR, status, "main: getRodsEnv error. ");
        exit (1);
    }

    srandom((unsigned int) time(0) % getpid());

#ifdef CACHE_FILE_FOR_READ
    if (setAndMkFileCacheDir () < 0) exit (1);
#endif

    initPathCache ();
    initIFuseDesc ();
    initConn();
    initFileCache();
    
    status = trace_begin( NULL );
    if( status != 0 ) {
       rodsLogError(LOG_ERROR, status, "main: trace_begin failed. ");
       exit(1);
    }
    
    status = fuse_main (argc, argv, &irodsOper, NULL);

    disconnectAll ();
    
    trace_end( NULL );
     
    if (status < 0) {
        exit (3);
    } else {
        exit(0);
    }
}

void
usage ()
{
   
   char *msgs[]={
   "Usage : irodsFs [-hd] [-o opt,[opt...]]",
"Single user iRODS/Fuse server, with logging support",
"Options are:",
" -h  this help",
" -d  FUSE debug mode",
" -o  opt,[opt...]  FUSE mount options",
""};
    int i;
    for (i=0;;i++) {
        if (strlen(msgs[i])==0) return;
         printf("%s\n",msgs[i]);
    }
    
    trace_usage();
}


