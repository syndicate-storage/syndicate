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

#ifndef _MS_CLIENT_CORE_H_
#define _MS_CLIENT_CORE_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <sys/types.h>

#include <sstream>
#include <queue>
#include <set>
#include <locale>
#include <attr/xattr.h>

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/crypt.h"
#include "libsyndicate/closure.h"
#include "libsyndicate/download.h"

#include "libsyndicate/ms/benchmark.h"

// maximum cert size is 10MB
#define MS_MAX_CERT_SIZE 10240000

// flow control defaults 
#define MS_CLIENT_DEFAULT_MAX_REQUEST_BATCH 10
#define MS_CLIENT_DEFAULT_MAX_ASYNC_REQUEST_BATCH 100
#define MS_CLIENT_DEFAULT_MAX_CONNECTIONS 100
#define MS_CLIENT_DEFAULT_MS_TRANSFER_TIMEOUT 25

using namespace std;

// use STRONG crypto.
// Use ephemeral Diffie-Helman for symmetric key (with in RSA, DSA, or ECC for key exchange)
// Use at least 256-bit keys in the data encryption
// Use at least 256-bit MACs
#define MS_CIPHER_SUITES "ECDHE:EDH:SHA256:SHA384:SHA512:AES256:!DH:!eNULL:!aNULL:!MD5:!DES:!DES3:!LOW:!EXP:!SRP:!DSS:!RC4:!PSK:!SHA1:!SHA2:!AES128"

// prototypes 
struct ms_volume;

// callback to be alerted when a Volume's metadata changes
typedef int (*ms_client_view_change_callback)( struct ms_client*, void* );

/*
// context for performing network I/O with the MS
struct ms_client_network_context {
   
   bool upload;                                 // if true, then this is an upload 
   bool started;                                // if true, then a transfer has started
   bool ended;                                  // if true, then the transfer has ended (i.e. dlctx will have been freed)
   
   struct md_download_context* dlctx;           // upload/download context
   struct md_download_set* dlset;               // optional download set, which contains this dlctx.  If not null, then dlctx will get added to it.
   struct ms_client_timing* timing;             // benchmark information 
   
   struct curl_httppost* forms;                 // HTTP forms (optional)
   struct curl_slist* headers;                  // HTTP headers (optional)
   
   char* url;                                   // target URL
   
   void* cls;                                   // app-defined context data
};
*/

// MS client session
struct ms_client {
   
   //////////////////////////////////////////////////////////////////
   // core information
   pthread_rwlock_t lock;       // lock governing access to this whole structure
   
   char* url;                   // MS URL (read-only; never changes)
   
   struct md_downloader dl;     // downloader instance
   
   struct md_syndicate_conf* conf;      // reference to syndicate config (read-only, never changes)
   
   //////////////////////////////////////////////////////////////////
   // NOTE: the following fields are filled in at runtime
   bool inited;               // set to true if this structure was initialized
   uint64_t owner_id;         // ID of the User account running this ms_client
   uint64_t gateway_id;       // ID of the Gateway running this ms_client
   int gateway_type;          // what kind of gateway is this for?
   int portnum;               // port the gateway listens on

   bool running;              // set to true if threads are running for this ms_client
   sem_t uploader_sem;        // uploader thread waits on this until signaled to reload 
   
   //////////////////////////////////////////////////////////////////
   // flow-control information 
   int page_size;             // when listing files, how many are we allowed to ask for at once?
   int max_request_batch;     // maximum number of synchronous requests we can send in one multi_request
   int max_request_async_batch;     // maximum number of asynchronous requests we can send in one multi_request
   int max_connections;       // maximum number of open connections to make to the MS
   int ms_transfer_timeout;     // how long to wait for data transfer before failing with -EAGAIN
   
   //////////////////////////////////////////////////////////////////
   // gateway volume-change structures (represents a consistent view of the Volume control state)
   pthread_t view_thread;
   bool view_thread_running;        // set to true if the view thread is running
   
   struct ms_volume* volume;        // Volume we're bound to
   
   ms_client_view_change_callback view_change_callback;         // call this function when the Volume gets reloaded
   void* view_change_callback_cls;                              // user-supplied argument to the above callbck
   
   pthread_rwlock_t view_lock;          // lock governing the above

   //////////////////////////////////////////////////////////////////
   // session information
   int64_t session_expires;                 // when the session password expires
   char* session_password;                  // session password (used as HTTP Authentication: header)
   char* userpass;                          // combined HTTP username:password string.  Username is the gateway ID; password is the session password
   
   //////////////////////////////////////////////////////////////////
   // identity and authentication
   // key information (read-only, never changes)
   EVP_PKEY* my_key;
   EVP_PKEY* my_pubkey;
   
   // raw private key (read-only, never changes)
   char* my_key_pem;
   size_t my_key_pem_len;
   bool my_key_pem_mlocked;     // if true, then my_key_pem is mlock'ed and needs to be munlock'ed before being freed
   
   EVP_PKEY* syndicate_public_key;      // syndicate public key (read-only, never changes)
   char* syndicate_public_key_pem;      // raw data of the above
};

extern "C" {

// module control 
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf );
int ms_client_start_threads( struct ms_client* client );
int ms_client_destroy( struct ms_client* client );

// key management 
int ms_client_verify_key( EVP_PKEY* key );
int ms_client_try_load_key( struct md_syndicate_conf* conf, EVP_PKEY** key, char** key_pem_dup, char const* key_pem, bool is_public );

// lock a client context structure
int ms_client_rlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_wlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_unlock2( struct ms_client* client, char const* from_str, int lineno );

// lock a client context's view of the Volume
int ms_client_view_rlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_view_wlock2( struct ms_client* client, char const* from_str, int lineno  );
int ms_client_view_unlock2( struct ms_client* client, char const* from_str, int lineno );

#define ms_client_rlock( fent ) ms_client_rlock2( fent, __FILE__, __LINE__ )
#define ms_client_wlock( fent ) ms_client_wlock2( fent, __FILE__, __LINE__ )
#define ms_client_unlock( fent ) ms_client_unlock2( fent, __FILE__, __LINE__ )

#define ms_client_view_rlock( fent ) ms_client_view_rlock2( fent, __FILE__, __LINE__ )
#define ms_client_view_wlock( fent ) ms_client_view_wlock2( fent, __FILE__, __LINE__ )
#define ms_client_view_unlock( fent ) ms_client_view_unlock2( fent, __FILE__, __LINE__ )

// low-level network I/O
int ms_client_init_curl_handle( struct ms_client* client, CURL* curl, char const* url );

int ms_client_download( struct ms_client* client, char const* url, char** buf, off_t* buflen );
int ms_client_process_header( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version );
int ms_client_is_async_operation( int oper );

// higher-level network I/O 
int ms_client_read( struct ms_client* client, char const* url, ms::ms_reply* reply );

// CDN access closure
extern struct md_closure_callback_entry MS_CLIENT_CACHE_CLOSURE_PROTOTYPE[];
int ms_client_volume_connect_cache( struct ms_client* client, CURL* curl, char const* url );
int ms_client_connect_cache_impl( struct md_closure* closure, CURL* curl, char const* url, void* cls );

// control the thread that keeps a consistent view of volume metadata
int ms_client_set_view_change_callback( struct ms_client* client, ms_client_view_change_callback clb, void* cls );
void* ms_client_set_view_change_callback_cls( struct ms_client* client, void* cls );
int ms_client_sched_volume_reload( struct ms_client* client );
int ms_client_view_change_callback_default( struct ms_client* client, void* cls );

// misc getters
int ms_client_gateway_type_str( int gateway_type, char* gateway_type_str );
char* ms_client_get_hostname( struct ms_client* client );
int ms_client_get_portnum( struct ms_client* client );

}

#endif