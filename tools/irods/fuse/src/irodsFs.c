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

// helpful macros
#define strdup_or_default( s, d ) ((s) != NULL ? strdup(s) : ((d) != NULL ? strdup(d) : NULL))


/* some global variables */
struct log_context* LOGCTX = NULL;

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
    
    // get logging environment variables, or use defaults 
    char* http_host = strdup_or_default( getenv("IRODSFS_LOG_SERVER_HOSTNAME"), HTTP_LOG_SERVER_HOSTNAME );
    char* http_port_str = strdup_or_default( getenv("IRODSFS_LOG_SERVER_PORTNUM"), HTTP_LOG_SERVER_PORTNUM_STR );
    char* sync_delay_str = strdup_or_default( getenv("IRODSFS_LOG_SERVER_SYNC_DELAY"), HTTP_LOG_SYNC_TIMEOUT_STR );
    char* log_path_salt = strdup_or_default( getenv("IRODSFS_LOG_PATH_SALT"), LOG_FILENAME_SALT );
    
    // parse!
    char* tmp = NULL;
    int portnum = 0;
    int sync_delay = 0;
    
    portnum = (int)strtoll( http_port_str, &tmp, 10 );
    if( tmp == NULL || portnum < 0 || portnum >= 65535 ) {
       
       fprintf(stderr, "WARN: invalid port number %d.  Using default %d\n", portnum, HTTP_LOG_SERVER_PORTNUM );
       portnum = HTTP_LOG_SERVER_PORTNUM;
    }
    
    sync_delay = (int)strtoll( sync_delay_str, &tmp, 10 );
    if( tmp == NULL || sync_delay <= 0 ) {
       
       fprintf(stderr, "WARN: invalid sync delay of %d seconds.  Using default of %d seconds\n", sync_delay, HTTP_LOG_SYNC_TIMEOUT );
       sync_delay = HTTP_LOG_SYNC_TIMEOUT;
    }
    
    // set up logging 
    LOGCTX = log_init( http_host, portnum, sync_delay, log_path_salt );
    
    if( LOGCTX == NULL ) {
       // OOM
       fprintf(stderr, "FATAL: out of memory\n");
       exit(4);  
    }
    
    // start up logging 
    int rc = log_start_threads( LOGCTX );
    if( rc != 0 ) {
       fprintf(stderr, "FATAL: log_start_threads rc = %d\n", rc );
       exit(5);
    }
    
    logmsg( LOGCTX, "%s", "fuse_main\n");
    
    status = fuse_main (argc, argv, &irodsOper, NULL);

    logmsg( LOGCTX, "fuse_main rc = %d\n", status);

    // stop the threads 
    rc = log_stop_threads( LOGCTX );
    if( rc != 0 ) {
       fprintf(stderr, "log_stop_threads rc = %d\n", rc );
       exit(6);
    }
    
    // sync the last of the logs 
    rc = log_rollover( LOGCTX );
    if( rc != 0 ) {
       fprintf(stderr, "ERR: log_rollover rc = %d\n", rc );
       exit(7);
    }
    
    // upload the last of the logs 
    rc = http_sync_all_logs( LOGCTX );
    if( rc != 0 ) {
       fprintf(stderr, "WARN: http_sync_all_logs rc = %d\n", rc );
    }
    
    disconnectAll ();
     
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
" ",
"Special environment variables that override built-in defaults:",
" IRODSFS_LOG_PATH_SALT            A string to be used to salt path hashes when logging.",
"                                  It is best to make this at least 256 random characters.",
" ",
" IRODSFS_LOG_SERVER_HOSTNAME      The hostname of the log server that will receive access",
"                                  traces from this filesystem.  The built-in default is",
"                                  " HTTP_LOG_SERVER_HOSTNAME,
" ",
" IRODSFS_LOG_SERVER_PORTNUM       The port number of said log server.  The built-in",
"                                  default is " HTTP_LOG_SERVER_PORTNUM_STR,
" ",
" IRODSFS_LOG_SERVER_SYNC_DELAY    The number of seconds to wait between uploading snapshots",
"                                  of access traces to the log server.  The default is " HTTP_LOG_SYNC_TIMEOUT_STR,
""};
    int i;
    for (i=0;;i++) {
        if (strlen(msgs[i])==0) return;
         printf("%s\n",msgs[i]);
    }
}


