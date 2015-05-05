/*
   Copyright 2014 The Trustees of Princeton University

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

#ifndef _LIBSYNDICATE_DOWNLOAD_H_
#define _LIBSYNDICATE_DOWNLOAD_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/closure.h"
#include "libsyndicate/util.h"

#include <set>

using namespace std;

// download buffer
struct md_download_buf {
   off_t len;         // amount of data
   off_t data_len;    // size of data (if data was preallocated)
   char* data;        // NOT null-terminated
};

// bounded response buffer
struct md_bound_response_buffer {
   off_t max_size;
   off_t size;
   md_response_buffer_t* rb;
};

struct md_download_set;

// download context
struct md_download_context { 
   
   struct md_bound_response_buffer brb;
   
   CURL* curl;
   
   int curl_rc;         // stores CURL error code
   int http_status;     // stores HTTP status 
   int transfer_errno;  // stores CURL-reported system errno, if an error occurred
   volatile bool cancelled;      // if true, this was cancelled
   char* effective_url; // stores final URL that resolved to data
   
   volatile bool initialized;    // if true, then this download context has been initialized
   volatile bool pending;        // if true, then this download context is in the process of being started
   volatile bool cancelling;     // if true, then this download context is in the process of being cancelled
   volatile bool running;        // if true, then this download is enqueued on the downloader
   volatile bool finalized;      // if true, then this download has finished
   int ref_count;                // number of threads referencing this download
   
   pthread_mutex_t finalize_lock;       // lock to serialize operations that change the above flags (primarily related to finalization)
   
   struct md_download_set* dlset;       // parent group containing this context
   
   sem_t sem;   // client holds this to be woken up when the download finishes 
   
   void* cls;   // associated download state
};

typedef map<CURL*, struct md_download_context*> md_downloading_map_t;
typedef set<struct md_download_context*> md_pending_set_t;
typedef md_pending_set_t::iterator md_download_set_iterator;

typedef void (*md_download_curl_release_func)( CURL*, void* );

// download set 
struct md_download_set {
   
   md_pending_set_t* waiting;           // pointers to download contexts for which we are waiting
   
   sem_t sem;                           // block on this until at least one of waiting has been finalized
};


// downloader 
struct md_downloader {
   
   char* name;
   pthread_t thread;    // CURL thread for downloading 
   
   md_downloading_map_t* downloading;   // currently-running downloads
   pthread_rwlock_t downloading_lock;   // guards downloading and curlm
   
   md_pending_set_t* pending;           // to be inserted into the downloading map
   pthread_rwlock_t pending_lock;       // guards pending
   volatile bool has_pending;
   
   md_pending_set_t* cancelling;        // to be removed from the downloading map
   pthread_rwlock_t cancelling_lock;    // guards cancelling_lock
   volatile bool has_cancelling;
   
   CURLM* curlm;        // multi-download
   
   bool running;        // if true, then this downloader is running
   bool inited;         // if true, then this downloader is fully initialized
};

// download loop state 
struct md_download_loop {
   
   struct md_downloader* dl;
   
   struct md_download_context* downloads;
   int num_downloads;
   
   struct md_download_set dlset;
   
   bool started;
};


#define MD_DOWNLOAD_DEFAULT_MAX_DOWNLOADS       10

#define MD_DOWNLOAD_FINISH                      0x1

extern "C" {
   
// initialization and tear-down 
int md_downloader_init( struct md_downloader* dl, char const* name );
int md_downloader_start( struct md_downloader* dl );
int md_downloader_stop( struct md_downloader* dl );
int md_downloader_shutdown( struct md_downloader* dl );

// initialize/tear down a download context.  Takes a CURL handle from the client, and gives it back when its done.
int md_download_context_init( struct md_download_context* dlctx, CURL* curl, off_t max_len, void* cls );
int md_download_context_reset( struct md_download_context* dlctx, CURL** old_curl );
int md_download_context_free2( struct md_download_context* dlctx, CURL** curl, char const* filename, int lineno );
#define md_download_context_free( dlctx, curl ) md_download_context_free2( dlctx, curl, __FILE__, __LINE__ )
int md_download_context_clear_set( struct md_download_context* dlctx );

// reference counting 
int md_download_context_ref( struct md_download_context* dlctx );
int md_download_context_unref( struct md_download_context* dlctx );

// download context sets (like an FDSET)
int md_download_set_init( struct md_download_set* dlset );
int md_download_set_free( struct md_download_set* dlset );
int md_download_set_add( struct md_download_set* dlset, struct md_download_context* dlctx );
int md_download_set_clear_itr( struct md_download_set* dlset, const md_download_set_iterator& itr );
int md_download_set_clear( struct md_download_set* dlset, struct md_download_context* dlctx );    // don't use inside a e.g. for() loop where you're iterating over a download set
size_t md_download_set_size( struct md_download_set* dlset );

// iterating through waiting
md_download_set_iterator md_download_set_begin( struct md_download_set* dlset );
md_download_set_iterator md_download_set_end( struct md_download_set* dlset );
struct md_download_context* md_download_set_iterator_get_context( const md_download_set_iterator& itr );

// begin downloading something, and wait for it to complete
int md_download_context_start( struct md_downloader* dl, struct md_download_context* dlctx );
int md_download_context_wait( struct md_download_context* dlctx, int64_t timeout_ms );
int md_download_context_wait_any( struct md_download_set* dlset, int64_t timeout_ms );
int md_download_context_cancel( struct md_downloader* dl, struct md_download_context* dlctx );

// run a download synchronously 
int md_download_context_run( struct md_download_context* dlctx );

// get back data from a download context
int md_download_context_get_buffer( struct md_download_context* dlctx, char** buf, off_t* buf_len );
int md_download_context_get_http_status( struct md_download_context* dlctx );
int md_download_context_get_errno( struct md_download_context* dlctx );
int md_download_context_get_curl_rc( struct md_download_context* dlctx );
int md_download_context_get_effective_url( struct md_download_context* dlctx, char** url );
void* md_download_context_get_cls( struct md_download_context* dlctx );
CURL* md_download_context_get_curl( struct md_download_context* dlctx );

// setters 
void md_download_context_set_cls( struct md_download_context* dlctx, void* new_cls );

// control
bool md_download_context_succeeded( struct md_download_context* dlctx, int desired_HTTP_status );
bool md_download_context_finalized( struct md_download_context* dlctx );
bool md_download_context_running( struct md_download_context* dlctx );
bool md_download_context_pending( struct md_download_context* dlctx );
bool md_download_context_cancelled( struct md_download_context* dlctx );

int md_HTTP_status_code_to_error_code( int status_code );

// low-level primitve for waiting on a semaphore (that tries again if interrupted)
int md_download_sem_wait( sem_t* sem, int64_t timeout_ms );

// helper functions to initialize curl handles for downloading 
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl, char const* url, time_t query_time );
void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer );

// download/upload callbacks
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data );
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data );

// simple one-shot download
int md_download_run( CURL* curl, off_t max_size, char** buf, off_t* buf_len );

// high-level download loop 
int md_download_loop_init( struct md_download_loop* dlloop, struct md_downloader* dl, int num_downloads );
int md_download_loop_free( struct md_download_loop* dlloop );
int md_download_loop_next( struct md_download_loop* dlloop, struct md_download_context** dlctx );
int md_download_loop_watch( struct md_download_loop* dlloop, struct md_download_context* dlctx );
int md_download_loop_run( struct md_download_loop* dlloop );
int md_download_loop_num_initialized( struct md_download_loop* dlloop );
int md_download_loop_num_running( struct md_download_loop* dlloop );
int md_download_loop_finished( struct md_download_loop* dlloop, struct md_download_context** dlctx );
bool md_download_loop_running( struct md_download_loop* dlloop );
int md_download_loop_abort( struct md_download_loop* dlloop );
int md_download_loop_cleanup( struct md_download_loop* dlloop, md_download_curl_release_func curl_release, void* release_cls );

// iteration 
struct md_download_context* md_download_loop_next_initialized( struct md_download_loop* dlloop, int *i );

// error-parsing 
int md_download_interpret_errors( int http_status, int curl_rc, int os_err );

// bound response buffers
int md_bound_response_buffer_init( struct md_bound_response_buffer* brb, off_t max_size );
int md_bound_response_buffer_free( struct md_bound_response_buffer* brb );

}

#endif
