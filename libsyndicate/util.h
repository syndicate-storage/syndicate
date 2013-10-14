/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/



#ifndef _UTIL_H_
#define _UTIL_H_

#include <limits.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <attr/xattr.h>
#include <semaphore.h>
#include <signal.h>
#include <openssl/sha.h>
#include <regex.h>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <curl/curl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <math.h>
#include <endian.h>

#define WHERESTR "%10lx: [%16s:%04u] %s: "
#define WHEREARG pthread_self(), __FILE__, __LINE__, __func__

extern int _DEBUG_MESSAGES;
extern int _ERROR_MESSAGES;

#ifdef dbprintf
#undef dbprintf
#endif

#ifdef errorf
#undef errorf
#endif

#define dbprintf( format, ... ) do { if( _DEBUG_MESSAGES ) { printf( WHERESTR format, WHEREARG, __VA_ARGS__ ); fflush(stdout); } } while(0)
#define errorf( format, ... ) do { if( _ERROR_MESSAGES ) { fprintf(stderr, WHERESTR format, WHEREARG, __VA_ARGS__); fflush(stderr); } } while(0)
#define QUOTE(x) #x

#define dbval(var, fmt) dbprintf(WHERESTR "%s = " fmt, WHEREARG, #var, var);
#define DIE 0xdeadbeef

#define NULLCHECK( var, ret ) \
  if( (var) == NULL ) {       \
      errorf("%s = NULL\n", #var);      \
      if( (ret) == DIE ) {       \
         exit(1);              \
      }                          \
      return ret;             \
  }

#define CALLOC_LIST(type, count) (type*)calloc( sizeof(type) * (count), 1 )
#define FREE_LIST(list) do { for(unsigned int __i = 0; (list)[__i] != NULL; ++ __i) { if( (list)[__i] != NULL ) { free( (list)[__i] ); (list)[__i] = NULL; }} free( (list) ); } while(0)
#define SIZE_LIST(sz, list) for( *(sz) = 0; (list)[*(sz)] != NULL; ++ *(sz) );
#define VECTOR_TO_LIST(ret, vec, type) do { ret = CALLOC_LIST(type, ((vec).size() + 1)); for( vector<type>::size_type __i = 0; __i < (vec).size(); ++ __i ) ret[__i] = (vec).at(__i); } while(0)
#define COPY_LIST(dst, src, duper) do { for( unsigned int __i = 0; (src)[__i] != NULL; ++ __i ) { (dst)[__i] = duper((src)[__i]); } } while(0)
#define DUP_LIST(type, dst, src, duper) do { unsigned int sz = 0; SIZE_LIST( &sz, src ); dst = CALLOC_LIST( type, sz + 1 ); COPY_LIST( dst, src, duper ); } while(0)

// for testing
#define BEGIN_TIMING_DATA(ts) clock_gettime( CLOCK_MONOTONIC, &ts )
#define END_TIMING_DATA(ts, ts2, key) clock_gettime( CLOCK_MONOTONIC, &ts2 ); printf("DATA %s %lf\n", key, ((double)(ts2.tv_nsec - ts.tv_nsec) + (double)(1e9 * (ts2.tv_sec - ts.tv_sec))) / 1e9 )
#define DATA(key, value) printf("DATA %s %lf\n", key, value)
#define DATA_S(str) printf("DATA %s\n", str)
#define DATA_BLOCK(name) printf("-------------------------------- %s\n", name)

#define BEGIN_EXTERN_C        extern "C" {
#define END_EXTERN_C          }

#define MAX(x,y) ((x) > (y) ? (x) : (y))

#define SCHED_SLEEP 50000
#define CURL_DEFAULT_SELECT_SEC 0
#define CURL_DEFAULT_SELECT_USEC SCHED_SLEEP

using namespace std;

// small message response buffers
typedef pair<char*, size_t> buffer_segment_t;
typedef vector< buffer_segment_t > response_buffer_t;



struct thread_args {
   void* context;
   int thread_no;
   struct sigaction act;
};

extern "C" {

void block_all_signals();
int install_signal_handler(int signo, struct sigaction *action, sighandler_t handler);
int uninstall_signal_handler(int signo);

// debug functions
void set_debug_level( int d );
void set_error_level( int e );
int get_debug_level();
int get_error_level();

// file functions
char* dir_path( const char* path );
char* fullpath( char* root, const char* path );
mode_t get_umask();

// time functions
int64_t currentTimeSeconds();
int64_t currentTimeMillis();
double currentTimeMono();
int64_t currentTimeMicros();

// misc functions
size_t sha256_len(void);
unsigned char* sha256_hash( char const* input );
unsigned char* sha256_hash_data( char const* input, size_t len );
char* sha256_printable( unsigned char const* sha256 );
char* sha256_hash_printable( char const* input, size_t len );
unsigned char* sha256_data( char const* sha256_print );
unsigned char* sha256_file( char const* path );
unsigned char* sha256_fd( int fd );
unsigned char* sha256_dup( unsigned char const* sha256 );
int sha256_cmp( unsigned char const* sha256_1, unsigned char const* sha256_2 );
int mkdir_sane( char* dirpath );
int rmdir_sane( char* dirpath );
int dir_exists( char* dirpath );
int make_lockfiles( char* path, char* lnk );
char* load_file( char const* path, size_t* size );
char* url_encode( char const* str, size_t len );
char* url_decode( char const* str, size_t* len );
int reg_match(const char *string, char const *pattern);
int timespec_cmp( struct timespec* t1, struct timespec* t2 );
uint32_t CMWC4096(void);

int Base64Decode(char* b64message, size_t len, char** buffer, size_t* buffer_len);
int Base64Encode(const char* message, size_t len, char** buffer);

int util_init(void);

}

#endif
