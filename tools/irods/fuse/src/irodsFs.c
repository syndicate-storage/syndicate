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
#include "iFuseLib.Logging.h"
#include "iFuseLib.Http.h"

/* some global variables */
FILE* LOGFILE = NULL;

extern rodsEnv MyRodsEnv;

#ifdef  __cplusplus
struct fuse_operations irodsOper; 
#else
static struct fuse_operations irodsOper =
{
  .getattr = irodsGetattr,
  .readlink = irodsReadlink,
  .readdir = irodsReaddir,
  .mknod = irodsMknod,
  .mkdir = irodsMkdir,
  .symlink = irodsSymlink,
  .unlink = irodsUnlink,
  .rmdir = irodsRmdir,
  .rename = irodsRename,
  .link = irodsLink,
  .chmod = irodsChmod,
  .chown = irodsChown,
  .truncate = irodsTruncate,
  .utimens = irodsUtimens,
  .open = irodsOpen,
  .read = irodsRead,
  .write = irodsWrite,
  .statfs = irodsStatfs,
  .release = irodsRelease,
  .fsync = irodsFsync,
  .flush = irodsFlush,
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

irodsOper.getattr = irodsGetattr;
irodsOper.readlink = irodsReadlink;
irodsOper.readdir = irodsReaddir;
irodsOper.mknod = irodsMknod;
irodsOper.mkdir = irodsMkdir;
irodsOper.symlink = irodsSymlink;
irodsOper.unlink = irodsUnlink;
irodsOper.rmdir = irodsRmdir;
irodsOper.rename = irodsRename;
irodsOper.link = irodsLink;
irodsOper.chmod = irodsChmod;
irodsOper.chown = irodsChown;
irodsOper.truncate = irodsTruncate;
irodsOper.utimens = irodsUtimens;
irodsOper.open = irodsOpen;
irodsOper.read = irodsRead;
irodsOper.write = irodsWrite;
irodsOper.statfs = irodsStatfs;
irodsOper.release = irodsRelease;
irodsOper.fsync = irodsFsync;
irodsOper.flush = irodsFlush;


    int status;
    rodsArguments_t myRodsArgs;
    char *optStr;

#ifdef  __cplusplus
    bzero (&irodsOper, sizeof (irodsOper));
    irodsOper.getattr = irodsGetattr;
    irodsOper.readlink = irodsReadlink;
    irodsOper.readdir = irodsReaddir;
    irodsOper.mknod = irodsMknod;
    irodsOper.mkdir = irodsMkdir;
    irodsOper.symlink = irodsSymlink;
    irodsOper.unlink = irodsUnlink;
    irodsOper.rmdir = irodsRmdir;
    irodsOper.rename = irodsRename;
    irodsOper.link = irodsLink;
    irodsOper.chmod = irodsChmod;
    irodsOper.chown = irodsChown;
    irodsOper.truncate = irodsTruncate;
    irodsOper.utimens = irodsUtimens;
    irodsOper.open = irodsOpen;
    irodsOper.read = irodsRead;
    irodsOper.write = irodsWrite;
    irodsOper.statfs = irodsStatfs;
    irodsOper.release = irodsRelease;
    irodsOper.fsync = irodsFsync;
    irodsOper.flush = irodsFlush;
#endif
    optStr = "hdo:l:";

    status = parseCmdLineOpt (argc, argv, optStr, 0, &myRodsArgs);

    if (status < 0) {
        printf("Use -h for help.\n");
        exit (1);
    }
    if (myRodsArgs.help==True) {
       usage();
       exit(0);
    }
    
    char* logfile_path = log_make_path();
    
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
    
    LOGFILE = log_init( logfile_path );
    logmsg( LOGFILE, "%s", "fuse_main\n");

    // force foreground 
    char** fuse_argv = (char**)calloc( (argc + 2) * sizeof(char*), 1 );
    for( int i = 0; i < argc - 1; i++ ) {
       fuse_argv[i] = argv[i];
    }
    fuse_argv[argc-1] = "-f";
    fuse_argv[argc] = argv[argc-1];

    status = fuse_main (argc+1, fuse_argv, &irodsOper, NULL);

    logmsg( LOGFILE, "fuse_main rc = %d\n", status);

    disconnectAll ();
    
    log_shutdown( LOGFILE );

    char* compressed_logfile_path = log_compress( logfile_path );
    if( compressed_logfile_path == NULL ) {
       fprintf(stderr, "failed to compress %s\n", logfile_path );
    }
    else {
       // send the log off 
       int soc = http_connect( HTTP_LOG_SERVER_HOSTNAME, HTTP_LOG_SERVER_PORTNUM );
       if( soc < 0 ) {
          fprintf(stderr, "failed to connect to %s:%d, rc = %d\n", HTTP_LOG_SERVER_HOSTNAME, HTTP_LOG_SERVER_PORTNUM, soc );
       }
       else {
          FILE* compressed_logfile_f = fopen( compressed_logfile_path, "r" );
          if( compressed_logfile_f == NULL ) {
             fprintf(stderr, "failed to open %s\n", compressed_logfile_path );
          }
          else {
             int fd = fileno( compressed_logfile_f );
             struct stat sb;
             
             int rc = stat( compressed_logfile_path, &sb );
             if( rc != 0 ) {
                rc = -errno;
                fprintf(stderr, "failed to stat %s, rc = %d\n", compressed_logfile_path, rc );
             }
             else {
                rc = http_upload( soc, fd, sb.st_size );
                if( rc < 0 ) {
                   fprintf(stderr, "failed to upload, rc = %d\n", rc );
                }
                if( rc != 200 ) {
                   fprintf(stderr, "failed to upload, HTTP status = %d\n", rc );
                }
             }

             fclose( compressed_logfile_f );
          }
          close( soc );
       }
       free( compressed_logfile_path );
    }
    free( logfile_path );
     
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
   "Usage : irodsFs [-hd] [-l LOGFILE] [-o opt,[opt...]]",
"Single user iRODS/Fuse server",
"Options are:",
" -h  this help",
" -d  FUSE debug mode",
" -l  Log file path",
" -o  opt,[opt...]  FUSE mount options",
""};
    int i;
    for (i=0;;i++) {
        if (strlen(msgs[i])==0) return;
         printf("%s\n",msgs[i]);
    }
}


