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
   char* data;    // NOT null-terminated
};

// bounded response buffer
struct md_bound_response_buffer {
   off_t max_size;
   off_t size;
   response_buffer_t* rb;
};

typedef size_t (*md_download_read_func)(void*, size_t, size_t, void*);
typedef int (*md_cache_connector_func)(struct md_closure*, CURL*, char const*, void*);
typedef int (*md_manifest_processor_func)(struct md_closure*, char const*, size_t, char**, size_t*, void*);

struct md_download_set;

// download context
struct md_download_context { 
   
   struct md_bound_response_buffer brb;
   
   void* cache_func_cls;
   md_cache_connector_func cache_func;
   
   CURL* curl;
   sem_t sem;   // client holds this to be woken up when the download finishes 
   
   int curl_rc;         // stores CURL error code
   int http_status;     // stores HTTP status 
   int transfer_errno;  // stores CURL-reported system errno, if an error occurred
   bool cancelled;      // if true, this was cancelled
   char* effective_url; // stores final URL that resolved to data
   
   bool finalized;      // if true, then this download has finished
   
   struct md_download_set* dlset;       // parent group containing this context
};

typedef map<CURL*, struct md_download_context*> md_downloading_map_t;
typedef set<struct md_download_context*> md_pending_set_t;
typedef md_pending_set_t::iterator md_download_set_iterator;

// download set 
struct md_download_set {
   
   md_pending_set_t* waiting;           // download context for which we are waiting
   
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
   bool has_pending;
   
   md_pending_set_t* cancelling;        // to be removed from the downloading map
   pthread_rwlock_t cancelling_lock;    // guards cancelling_lock
   bool has_cancelling;
   
   CURLM* curlm;        // multi-download
   
   bool running;        // if true, then this downloader is running
};

extern "C" {
   
// initialization and tear-down 
int md_downloader_init( struct md_downloader* dl, char const* name );
int md_downloader_start( struct md_downloader* dl );
int md_downloader_stop( struct md_downloader* dl );
int md_downloader_shutdown( struct md_downloader* dl );

// initialize/tear down a download context.  Takes a CURL handle from the client, and gives it back when its done.
int md_download_context_init( struct md_download_context* dlctx, CURL* curl, md_cache_connector_func cache_func, void* cache_func_cls, off_t max_len );
int md_download_context_reset( struct md_download_context* dlctx, CURL* new_curl );
int md_download_context_free( struct md_download_context* dlctx, CURL** curl );

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
int md_download_context_start( struct md_downloader* dl, struct md_download_context* dlctx, struct md_closure* cache_closure, char const* base_url );
int md_download_context_wait( struct md_download_context* dlctx, int64_t timeout_ms );
int md_download_context_wait_any( struct md_download_set* dlset, int64_t timeout_ms );
int md_download_context_cancel( struct md_downloader* dl, struct md_download_context* dlctx );

// get back data from a download context
int md_download_context_get_buffer( struct md_download_context* dlctx, char** buf, off_t* buf_len );
int md_download_context_get_http_status( struct md_download_context* dlctx );
int md_download_context_get_errno( struct md_download_context* dlctx );
int md_download_context_get_curl_rc( struct md_download_context* dlctx );
int md_download_context_get_effective_url( struct md_download_context* dlctx, char** url );
void* md_download_context_get_cache_cls( struct md_download_context* dlctx );
bool md_download_context_succeeded( struct md_download_context* dlctx, int desired_HTTP_status );
bool md_download_context_finalized( struct md_download_context* dlctx );

// low-level primitve for waiting on a semaphore (that tries again if interrupted)
int md_download_sem_wait( sem_t* sem, int64_t timeout_ms );

// synchronously download
off_t md_download_file( CURL* curl_h, char** buf );
int md_download( struct md_syndicate_conf* conf, struct md_downloader* dl, struct md_closure* closure,
                 CURL* curl, char const* base_url, char** bits, off_t* ret_len, off_t max_len, int* status_code, md_cache_connector_func cache_func, void* cache_func_cls );

int md_download_manifest( struct md_syndicate_conf* conf, struct md_downloader* dl, struct md_closure* closure,
                          CURL* curl, char const* manifest_url, Serialization::ManifestMsg* mmsg,
                          md_cache_connector_func cache_func, void* cache_func_cls,
                          md_manifest_processor_func manifest_func, void* manifest_func_cls );

// helper functions to initialize curl handles for downloading 
void md_init_curl_handle( struct md_syndicate_conf* conf, CURL* curl, char const* url, time_t query_time );
void md_init_curl_handle2( CURL* curl_h, char const* url, time_t query_timeout, bool ssl_verify_peer );

// download/upload callbacks
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data );
size_t md_get_callback_response_buffer( void* stream, size_t size, size_t count, void* user_data );
size_t md_get_callback_bound_response_buffer( void* stream, size_t size, size_t count, void* user_data );

}

#endif
