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
#include <zlib.h>

#define SG_WHERESTR "%05d:%05d: [%16s:%04u] %s: "
#define SG_WHEREARG (int)getpid(), (int)gettid(), __FILE__, __LINE__, __func__

extern int _SG_DEBUG_MESSAGES;
extern int _SG_INFO_MESSAGES;
extern int _SG_WARN_MESSAGES;
extern int _SG_ERROR_MESSAGES;

#define SG_MAX_VERBOSITY 2

#define SG_debug( format, ... ) do { if( _SG_DEBUG_MESSAGES ) { printf( SG_WHERESTR "DEBUG: " format, SG_WHEREARG, __VA_ARGS__ ); fflush(stdout); } } while(0)
#define SG_info( format, ... ) do { if( _SG_INFO_MESSAGES ) { printf( SG_WHERESTR "INFO: " format, SG_WHEREARG, __VA_ARGS__ ); fflush(stdout); } } while(0)
#define SG_warn( format, ... ) do { if( _SG_WARN_MESSAGES ) { fprintf(stderr, SG_WHERESTR "WARN: " format, SG_WHEREARG, __VA_ARGS__); fflush(stderr); } } while(0)
#define SG_error( format, ... ) do { if( _SG_ERROR_MESSAGES ) { fprintf(stderr, SG_WHERESTR "ERROR: " format, SG_WHEREARG, __VA_ARGS__); fflush(stderr); } } while(0)

#define SG_CALLOC(type, count) (type*)calloc( sizeof(type) * (count), 1 )
#define SG_FREE_LIST(list, freefunc) do { if( (list) != NULL ) { for(unsigned int __i = 0; (list)[__i] != NULL; ++ __i) { if( (list)[__i] != NULL ) { freefunc( (list)[__i] ); (list)[__i] = NULL; }} free( (list) ); } } while(0)
#define SG_FREE_LISTV(list, len, freefunc) do { if( (list) != NULL ) { for(unsigned int __i = 0; __i < (unsigned)len; ++ __i) { if( (list)[__i] != NULL ) { freefunc( (list)[__i] ); (list)[__i] = NULL; }} free( (list) ); } } while(0)

#define SG_strdup_or_null( str )  (str) != NULL ? strdup(str) : NULL
#define SG_strlen_or_zero( str )  (str) != NULL ? strlen(str) : 0

#define SG_safe_new( foo ) new (nothrow) foo
#define SG_safe_free( buf ) if( (buf) != NULL ) { free(buf); buf = NULL; }
#define SG_safe_delete( foo ) if( (foo) != NULL ) { delete foo; foo = NULL; }

// for testing
#define SG_BEGIN_TIMING_DATA(ts) clock_gettime( CLOCK_MONOTONIC, &ts )
#define SG_END_TIMING_DATA(ts, ts2, key) clock_gettime( CLOCK_MONOTONIC, &ts2 ); printf("DATA %s %lf\n", key, ((double)(ts2.tv_nsec - ts.tv_nsec) + (double)(1e9 * (ts2.tv_sec - ts.tv_sec))) / 1e9 )
#define SG_TIMING_DATA(key, value) printf("DATA %s %lf\n", key, value)


#ifndef MIN
#define MIN(x,y) ((x) < (y) ? (x) : (y))
#endif

#ifndef MAX
#define MAX(x,y) ((x) > (y) ? (x) : (y))
#endif

#define CURL_DEFAULT_SELECT_SEC 0
#define CURL_DEFAULT_SELECT_USEC 50000

using namespace std;

// small message response buffers
typedef pair<char*, size_t> md_buffer_segment_t;
typedef vector< md_buffer_segment_t > md_response_buffer_t;

struct mlock_buf {
   void* ptr;
   size_t len;
};

extern "C" {

// debug functions
void md_set_debug_level( int d );
void md_set_error_level( int e );
int md_get_debug_level();
int md_get_error_level();

// file functions
mode_t md_get_umask();
int md_unix_socket( char const* path, bool server );
int md_write_to_tmpfile( char const* tmpfile_fmt, char const* buf, size_t buflen, char** tmpfile_path );
char* md_load_file( char const* path, off_t* size );
int md_write_file( char const* path, char const* data, size_t len, mode_t mode );

// I/O functions 
ssize_t md_read_uninterrupted( int fd, char* buf, size_t len );
ssize_t md_recv_uninterrupted( int fd, char* buf, size_t len, int flags );
ssize_t md_write_uninterrupted( int fd, char const* buf, size_t len );
ssize_t md_send_uninterrupted( int fd, char const* buf, size_t len, int flags );
int md_transfer( int in_fd, int out_fd, size_t len );

// time functions
int64_t md_current_time_seconds();
int64_t md_current_time_millis();
int64_t md_timespec_diff_ms( struct timespec* t1, struct timespec* t2 );
int64_t md_timespec_diff( struct timespec* t1, struct timespec* t2 );

int md_sleep_uninterrupted( struct timespec* ts );

// (de)compression, with libz
int md_deflate( char* in, size_t in_len, char** out, size_t* out_len );
int md_inflate( char* in, size_t in_len, char** out, size_t* out_len );

// sha256 functions
size_t sha256_len(void);
unsigned char* sha256_hash( char const* input );
void sha256_hash_buf( char const* input, size_t len, unsigned char* output );
unsigned char* sha256_hash_data( char const* input, size_t len );
char* sha256_printable( unsigned char const* sha256 );
void sha256_printable_buf( unsigned char const* sha256, char* buf );
char* sha256_hash_printable( char const* input, size_t len );
unsigned char* sha256_data( char const* printable );
unsigned char* sha256_file( char const* path );
unsigned char* sha256_fd( int fd );
void sha256_fd_buf( int fd, size_t len, unsigned char* output );
unsigned char* sha256_dup( unsigned char const* sha256 );
int sha256_cmp( unsigned char const* hash1, unsigned char const* hash2 );

char* md_data_printable( unsigned char const* data, size_t len );
void md_sprintf_data( char* str, unsigned char const* data, size_t len );

// parser functions
char* md_url_encode( char const* str, size_t len );
char* md_url_decode( char const* str, size_t* len );
int md_base64_decode(const char* b64message, size_t b64len, char** buffer, size_t* buffer_len);
int md_base64_encode(const char* message, size_t len, char** buffer);
int md_strrstrip( char* str, char const* strip );

// pseudo-random number generator (not cryptographically secure)
uint32_t md_random32(void);
uint64_t md_random64(void);

// library initialization
int md_util_init(void);

// mlock'ed memory allocators 
int mlock_calloc( struct mlock_buf* buf, size_t len );
int mlock_free( struct mlock_buf* buf );
int mlock_dup( struct mlock_buf* dest, char const* src, size_t src_len );
int mlock_buf_dup( struct mlock_buf* dest, struct mlock_buf* src );

// memory functions
char* md_response_buffer_to_string( md_response_buffer_t* rb );
char* md_response_buffer_to_c_string( md_response_buffer_t* rb );
void md_response_buffer_free( md_response_buffer_t* rb );
off_t md_response_buffer_size( md_response_buffer_t* rb );
void* md_memdup( void* buf, size_t len );
char* SG_strdup_or_die( char const* str );

// linux-specific...
pid_t gettid(void);

}


// extra macros
#define SG_BLOCK_HASH_LEN SHA256_DIGEST_LENGTH
#define SG_BLOCK_HASH_DATA sha256_hash_data
#define SG_BLOCK_HASH_FD sha256_fd
#define SG_BLOCK_HASH_TO_STRING sha256_printable
#define SG_BLOCK_HASH_DUP sha256_dup

#endif
