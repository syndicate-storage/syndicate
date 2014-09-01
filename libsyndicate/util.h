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

#ifndef _UTIL_H_
#define _UTIL_H_

#include <limits.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
//#include <memory.h>
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
#include <sys/mman.h>
#include <sys/syscall.h>        // for gettid()

#define WHERESTR "%05d:%05d: [%16s:%04u] %s: "
#define WHEREARG (int)getpid(), (int)gettid(), __FILE__, __LINE__, __func__

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
#define FREE_LIST(list) do { if( (list) != NULL ) { for(unsigned int __i = 0; (list)[__i] != NULL; ++ __i) { if( (list)[__i] != NULL ) { free( (list)[__i] ); (list)[__i] = NULL; }} free( (list) ); } } while(0)
#define SIZE_LIST(sz, list) for( *(sz) = 0; (list)[*(sz)] != NULL; ++ *(sz) );
#define VECTOR_TO_LIST(ret, vec, type) do { ret = CALLOC_LIST(type, ((vec).size() + 1)); for( vector<type>::size_type __i = 0; __i < (vec).size(); ++ __i ) ret[__i] = (vec).at(__i); } while(0)
#define COPY_LIST(dst, src, duper) do { for( unsigned int __i = 0; (src)[__i] != NULL; ++ __i ) { (dst)[__i] = duper((src)[__i]); } } while(0)
#define DUP_LIST(type, dst, src, duper) do { unsigned int sz = 0; SIZE_LIST( &sz, src ); dst = CALLOC_LIST( type, sz + 1 ); COPY_LIST( dst, src, duper ); } while(0)

#define strdup_or_null( str )  (str) != NULL ? strdup(str) : NULL

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


struct mlock_buf {
   void* ptr;
   size_t len;
};

extern "C" {

// debug functions
void set_debug_level( int d );
void set_error_level( int e );
int get_debug_level();
int get_error_level();

// file functions
char* dir_path( const char* path );
char* fullpath( char* root, const char* path );
mode_t get_umask();
int md_clear_dir( char const* dirname );
int md_unix_socket( char const* path, bool server );
int md_write_to_tmpfile( char const* tmpfile_fmt, char const* buf, size_t buflen, char** tmpfile_path );

// I/O functions 
ssize_t md_read_uninterrupted( int fd, char* buf, size_t len );
ssize_t md_recv_uninterrupted( int fd, char* buf, size_t len, int flags );
ssize_t md_write_uninterrupted( int fd, char const* buf, size_t len );
ssize_t md_send_uninterrupted( int fd, char const* buf, size_t len, int flags );

// time functions
int64_t currentTimeSeconds();
int64_t currentTimeMillis();
double currentTimeMono();
int64_t currentTimeMicros();
double timespec_to_double( struct timespec* ts );
double now_ns(void);

int md_sleep_uninterrupted( struct timespec* ts );

// sha256 functions
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

// system functions
char* load_file( char const* path, size_t* size );

// parser functions
char* md_url_encode( char const* str, size_t len );
char* md_url_decode( char const* str, size_t* len );
int timespec_cmp( struct timespec* t1, struct timespec* t2 );
int Base64Decode(const char* b64message, size_t b64len, char** buffer, size_t* buffer_len);
int Base64Encode(const char* message, size_t len, char** buffer);

// random number generator
uint32_t CMWC4096(void);
uint32_t md_random32(void);
uint64_t md_random64(void);

// library initialization
int util_init(void);

// mlock'ed memory allocators 
int mlock_calloc( struct mlock_buf* buf, size_t len );
int mlock_free( struct mlock_buf* buf );
int mlock_dup( struct mlock_buf* dest, char const* src, size_t src_len );
int mlock_buf_dup( struct mlock_buf* dest, struct mlock_buf* src );

char* response_buffer_to_string( response_buffer_t* rb );
void response_buffer_free( response_buffer_t* rb );
off_t response_buffer_size( response_buffer_t* rb );

// linux-specific...
pid_t gettid(void);
}


// extra macros
#define BLOCK_HASH_LEN sha256_len
#define BLOCK_HASH_DATA sha256_hash_data
#define BLOCK_HASH_FD sha256_fd
#define BLOCK_HASH_TO_STRING sha256_printable
#define BLOCK_HASH_DUP sha256_dup

#endif
