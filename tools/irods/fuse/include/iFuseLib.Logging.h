#ifndef _I_FUSELIB_LOGGING_H_
#define _I_FUSELIB_LOGGING_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <openssl/sha.h>

#define LOG_FILENAME_SALT "dasc46hbQWo8GZ2pI6Mw7Vknxdb9HIiUSaaPf9hh3QVgu4HrVrOnC3wMcQc2bxsDqDsJim1kXNx4qbb9eELYE8Jdzok3PZgiV3GRRBhPs0Zo49bBsmidJT4v50pJEOpo"
#define LOG_PATH_FMT "/tmp/irods.log.%d"
#define LOG_PATH_HASH_LEN (2 * SHA256_DIGEST_LENGTH + 1)

#define WHERESTR "%05d:%05d: [%16s:%04u] %s @%ld.%ld: "
#define WHEREARG (int)getpid(), (int)gettid(), __FILE__, __LINE__, __func__

#define logmsg( file, format, ... ) do { if( (file) != NULL ) { struct timespec _ts; clock_gettime( CLOCK_MONOTONIC, &_ts ); fprintf(file, WHERESTR "INFO: " format, WHEREARG, _ts.tv_sec, _ts.tv_nsec,__VA_ARGS__);} } while(0)
#define logerr( file, format, ... ) do { if( (file) != NULL ) { struct timespec _ts; clock_gettime( CLOCK_MONOTONIC, &_ts ); fprintf(file, WHERESTR "ERR : " format, WHEREARG, _ts.tv_sec, _ts.tv_nsec,__VA_ARGS__);} } while(0)

FILE* log_init(const char* logpath);
int log_shutdown( FILE* logfile );
char* log_make_path( void );
char* log_compress( char const* logpath );
void log_hash_path( char const* path, char hash_buf[LOG_PATH_HASH_LEN] );

pid_t gettid(void);

#endif
