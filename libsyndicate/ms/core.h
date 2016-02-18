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
#include "libsyndicate/download.h"

#include "libsyndicate/ms/benchmark.h"
#include "libsyndicate/ms/cert.h"

// maximum cert size is 10MB
#define MS_MAX_CERT_SIZE 10240000

// maximum message length is 1MB 
#define MS_MAX_MSG_SIZE 1024000

// flow control defaults 
#define MS_CLIENT_DEFAULT_RESOLVE_PAGE_SIZE 10
#define MS_CLIENT_DEFAULT_MAX_REQUEST_BATCH 10
#define MS_CLIENT_DEFAULT_MAX_ASYNC_REQUEST_BATCH 100
#define MS_CLIENT_DEFAULT_MAX_CONNECTIONS 100
#define MS_CLIENT_DEFAULT_MS_TRANSFER_TIMEOUT 25

using namespace std;

// use STRONG TLS crypto.
// Use ephemeral Diffie-Helman for symmetric key (with in RSA, DSA, or ECC for key exchange)
// Use at least 256-bit keys in the data encryption
// Use at least 256-bit MACs
#define MS_CIPHER_SUITES "ECDHE:EDH:SHA256:SHA384:SHA512:AES256:!DH:!eNULL:!aNULL:!MD5:!DES:!DES3:!LOW:!EXP:!SRP:!DSS:!RC4:!PSK:!SHA1:!SHA2:!AES128"

// prototypes 
struct ms_volume;

// callback to be alerted when a volume's gateway config changes
typedef int (*ms_client_config_change_callback)( struct ms_client*, void* );

// MS client session
struct ms_client {
   
   //////////////////////////////////////////////////////////////////
   // core information
   pthread_rwlock_t lock;       // lock governing access to this whole structure
   
   char* url;                   // MS URL (read-only; never changes)
   
   struct md_downloader* dl;    // downloader instance
   
   struct md_syndicate_conf* conf;      // reference to syndicate config (read-only, never changes)
   
   //////////////////////////////////////////////////////////////////
   // NOTE: the following fields are filled in at runtime, and do not change over the lifetime of the gateway 
   // (except for portnum, TODO: restart the http server on a new port if this ever changes)
   bool inited;               // set to true if this structure was initialized
   uint64_t owner_id;         // ID of the User account running this ms_client
   uint64_t gateway_id;       // ID of the Gateway running this ms_client (pass SG_GATEWAY_ANON for the anonymous gateway)
   uint64_t gateway_type;     // what kind of gateway is this for?
   
   int portnum;               // port the gateway listens on

   bool running;              // set to true if threads are running for this ms_client
   sem_t config_sem;          // uploader thread waits on this until signaled to reload 
   
   
   //////////////////////////////////////////////////////////////////
   // flow-control information 
   int page_size;             // when listing files, how many are we allowed to ask for at once?
   int max_request_batch;     // maximum number of synchronous requests we can send in one multi_request
   int max_request_async_batch;     // maximum number of asynchronous requests we can send in one multi_request
   int max_connections;       // maximum number of open connections to make to the MS
   int ms_transfer_timeout;     // how long to wait for data transfer before failing with -EAGAIN
   
   //////////////////////////////////////////////////////////////////
   // gateway volume-change structures (represents a consistent view of the Volume control state)
   struct ms_volume* volume;        // Volume we're bound to
   ms_cert_bundle* certs;           // certificates for all other gateways in the volume
   
   pthread_rwlock_t config_lock;          // lock governing the above
   
   //////////////////////////////////////////////////////////////////
   // identity and authentication
   // gateway key information (read-only, never changes)
   EVP_PKEY* gateway_key;
   EVP_PKEY* gateway_pubkey;
   
   // syndicate pubkey (never changes)
   EVP_PKEY* syndicate_pubkey;
   
   // raw private key (read-only, never changes)
   char* gateway_key_pem;
   size_t gateway_key_pem_len;
   bool gateway_key_pem_mlocked;     // if true, then gateway_key_pem is mlock'ed and needs to be munlock'ed before being freed
};

extern "C" {

// module control 
int ms_client_init( struct ms_client* client, struct md_syndicate_conf* conf, EVP_PKEY* syndicate_pubkey, ms::ms_volume_metadata* volume_cert );
int ms_client_destroy( struct ms_client* client );

// key management 
int ms_client_verify_key( EVP_PKEY* key );
int ms_client_try_load_key( struct md_syndicate_conf* conf, EVP_PKEY** key, char** key_pem_dup, char const* key_pem, bool is_public );

// lock a client context structure
int ms_client_rlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_wlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_unlock2( struct ms_client* client, char const* from_str, int lineno );

// lock a client context's configuration data
int ms_client_config_rlock2( struct ms_client* client, char const* from_str, int lineno );
int ms_client_config_wlock2( struct ms_client* client, char const* from_str, int lineno  );
int ms_client_config_unlock2( struct ms_client* client, char const* from_str, int lineno );

#define ms_client_rlock( fent ) ms_client_rlock2( fent, __FILE__, __LINE__ )
#define ms_client_wlock( fent ) ms_client_wlock2( fent, __FILE__, __LINE__ )
#define ms_client_unlock( fent ) ms_client_unlock2( fent, __FILE__, __LINE__ )

#define ms_client_config_rlock( fent ) ms_client_config_rlock2( fent, __FILE__, __LINE__ )
#define ms_client_config_wlock( fent ) ms_client_config_wlock2( fent, __FILE__, __LINE__ )
#define ms_client_config_unlock( fent ) ms_client_config_unlock2( fent, __FILE__, __LINE__ )

// low-level network I/O
int ms_client_init_curl_handle( struct ms_client* client, CURL* curl, char const* url, char const* auth_header );
int ms_client_auth_header( struct ms_client* client, char const* url, char** auth_header );
int ms_client_download( struct ms_client* client, char const* url, char** buf, off_t* buflen );
int ms_client_need_reload( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version );
int ms_client_is_async_operation( int oper );

// higher-level network I/O 
int ms_client_read( struct ms_client* client, char const* url, ms::ms_reply* reply );

// misc getters
int ms_client_gateway_type_str( uint64_t gateway_type, char* gateway_type_str );
int ms_client_get_portnum( struct ms_client* client );
uint64_t ms_client_volume_version( struct ms_client* client );
uint64_t ms_client_cert_version( struct ms_client* client );
uint64_t ms_client_get_volume_id( struct ms_client* client );
uint64_t ms_client_get_owner_id( struct ms_client* client );
uint64_t ms_client_get_gateway_id( struct ms_client* client );
char* ms_client_get_volume_name( struct ms_client* client );
uint64_t ms_client_get_volume_blocksize( struct ms_client* client );
int ms_client_get_volume_root( struct ms_client* client, int64_t version, int64_t write_nonce, struct md_entry* root );
EVP_PKEY* ms_client_my_pubkey( struct ms_client* client );
EVP_PKEY* ms_client_my_privkey( struct ms_client* client );

struct ms_gateway_cert* ms_client_get_gateway_cert( struct ms_client* client, uint64_t gateway_id );
uint64_t ms_client_get_num_gateways( struct ms_client* client );
int ms_client_get_gateway_ids( struct ms_client* client, uint64_t* id_buf, size_t id_buf_len );
uint64_t ms_client_get_gateway_caps( struct ms_client* client, uint64_t gateway_id );

int ms_client_get_gateways_by_type( struct ms_client* client, uint64_t gateway_type, uint64_t** gateway_ids, size_t* num_gateway_ids );

// certs
ms_cert_bundle* ms_client_swap_gateway_certs( struct ms_client* client, ms_cert_bundle* new_cert_bundle );
struct ms_volume* ms_client_swap_volume_cert( struct ms_client* client, ms::ms_volume_metadata* new_volume_cert );
EVP_PKEY* ms_client_swap_syndicate_pubkey( struct ms_client* client, EVP_PKEY* new_syndicate_pubkey );

}

#endif
