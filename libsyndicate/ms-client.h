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

#ifndef _MS_CLIENT_H_
#define _MS_CLIENT_H_

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
#include "libsyndicate/openid.h"
#include "libsyndicate/closure.h"
#include "libsyndicate/download.h"

// benchmarking HTTP headers
#define HTTP_VOLUME_TIME   "X-Volume-Time"
#define HTTP_GATEWAY_TIME  "X-Gateway-Time"
#define HTTP_TOTAL_TIME    "X-Total-Time"
#define HTTP_RESOLVE_TIME  "X-Resolve-Time"
#define HTTP_CREATE_TIMES  "X-Create-Times"
#define HTTP_UPDATE_TIMES  "X-Update-Times"
#define HTTP_DELETE_TIMES  "X-Delete-Times"
#define HTTP_GETXATTR_TIMES "X-Getxattr-Times"
#define HTTP_SETXATTR_TIMES "X-Setattr-Times"
#define HTTP_LISTXATTRS_TIMES "X-Listattrs-Times"
#define HTTP_REMOVEXATTRS_TIMES "X-Removexattrs-Times"
#define HTTP_MS_LASTMOD    "X-MS-LastMod"

// maximum cert size is 10MB
#define MS_MAX_CERT_SIZE 10240000

using namespace std;

typedef map<long, struct md_update> update_set;
typedef map<uint64_t, long> deadline_queue;

// status responses from the MS on a locally cached metadata record
#define MS_LISTING_NEW          ms::ms_listing::NEW             // new entry
#define MS_LISTING_NOCHANGE     ms::ms_listing::NOT_MODIFIED    // entry/listing not modified
#define MS_LISTING_NONE         ms::ms_listing::NONE            // entry doesn't exist

#define MS_NUM_CERT_BUNDLES 4

// use STRONG crypto.
// Use ephemeral Diffie-Helman for symmetric key (with in RSA, DSA, or ECC for key exchange)
// Use at least 256-bit keys in the data encryption
// Use at least 256-bit MACs
#define MS_CIPHER_SUITES "ECDHE:EDH:SHA256:SHA384:SHA512:AES256:!DH:!eNULL:!aNULL:!MD5:!DES:!DES3:!LOW:!EXP:!SRP:!DSS:!RC4:!PSK:!SHA1:!SHA2:!AES128"

// directory listing
struct ms_listing {
   int status;       
   int type;         // file or directory?
   vector<struct md_entry>* entries;
};

// Download information
struct ms_download_context {
   CURL* curl;
   char* url;
   response_buffer_t* rb;
};

// path entry metadata for getting metadata listings
struct ms_path_ent {
   uint64_t volume_id;
   uint64_t file_id;
   int64_t version;
   int64_t write_nonce;
   char* name;

   void* cls;
};

// vacuum entry 
struct ms_vacuum_entry {
   uint64_t volume_id;
   uint64_t file_id;
   int64_t file_version;
   int64_t manifest_mtime_sec;
   int32_t manifest_mtime_nsec;
   
   uint64_t* affected_blocks;
   size_t num_affected_blocks;
};

typedef vector< struct ms_path_ent > path_t;

typedef map< uint64_t, struct ms_listing > ms_response_t;

// version delta record between our Volume's cached cert bundle and the MS's
struct ms_cert_diff_entry {
   int gateway_type;
   uint64_t gateway_id;
   uint64_t cert_version;
};

typedef vector< ms_cert_diff_entry > ms_cert_diff_list;

// cert bundle delta
struct ms_cert_diff {
   ms_cert_diff_list* old_certs;
   ms_cert_diff_list* new_certs;
   
   ms_cert_diff() {
      this->old_certs = new ms_cert_diff_list();
      this->new_certs = new ms_cert_diff_list();
   }
   
   ~ms_cert_diff() {
      delete this->old_certs;
      delete this->new_certs;
   }
};


// benchmarking structure
struct ms_client_timing {
   uint64_t total_time;
   uint64_t volume_time;
   uint64_t ug_time;

   uint64_t* create_times;
   size_t num_create_times;

   uint64_t* update_times;
   size_t num_update_times;

   uint64_t* delete_times;
   size_t num_delete_times;

   uint64_t resolve_time;
};


struct ms_gateway_cert {
   uint64_t user_id;            // SyndicateUser ID
   uint64_t gateway_id;         // Gateway ID
   int gateway_type;            // what kind of gateway
   uint64_t volume_id;          // Volume ID
   char* name;                  // account (gateway) name
   char* hostname;              // what host this gateway runs on
   int portnum;                 // what port this gateway listens on
   char* closure_text;          // closure information (only retained for our gateway)
   uint64_t closure_text_len;   // length of the above
   EVP_PKEY* pubkey;            // gateway public key
   EVP_PKEY* privkey;           // decrypted from MS (only retained for our gateway)
   uint64_t caps;               // gateway capabilities
   uint64_t expires;            // when this certificate expires
   uint64_t version;            // version of this certificate (increases monotonically)
   uint64_t blocksize;          // used only by the AG--how big of a block will it deliver, irrespective of Volume
};

typedef map<uint64_t, struct ms_gateway_cert*> ms_cert_bundle;

// Volume data
struct ms_volume {
   uint64_t volume_id;           // ID of this Volume
   uint64_t volume_owner_id;     // UID of the User that owns this Volume
   uint64_t blocksize;           // blocksize of this Volume
   char* name;                   // name of the volume
   
   EVP_PKEY* volume_public_key;  // Volume public key 
   bool reload_volume_key;       // do we reload this public key if we get it from the MS?  Or do we trust the one given locally?
   
   ms_cert_bundle* UG_certs;    // UGs in this Volume
   ms_cert_bundle* RG_certs;    // RGs in this Volume
   ms_cert_bundle* AG_certs;    // AGs in this Volume
   
   int num_UG_certs;
   int num_RG_certs;
   int num_AG_certs;

   uint64_t volume_version;      // version of the above information
   uint64_t volume_cert_version;        // version of the cert bundle 
   
   struct md_entry* root;        // serialized root fs_entry
   
   uint64_t num_files;           // number of files in this Volume

   bool loading;                 // set to true if the Volume is in the process of being reloaded
   
   struct md_closure* cache_closure;    // closure for connecting to the cache providers
};

typedef int (*ms_client_view_change_callback)( struct ms_client*, void* );

struct ms_client {
   bool inited;         // set to true if this structure was initialized
   
   int gateway_type;    // what kind of gateway is this for?
   
   pthread_rwlock_t lock;       // structure lock
   
   CURL* ms_read;       // read file metadata
   CURL* ms_write;      // write file metadata
   CURL* ms_view;       // read/write Volume control plane state
   
   // benchmarking
   struct ms_client_timing read_times;
   struct ms_client_timing write_times;
   
   // asynchronous/batch RPCs to carry out
   update_set* updates;
   deadline_queue* deadlines;

   char* url;                 // MS URL
   char* userpass;            // HTTP username:password string.  Username is the gateway ID; password is the session password
   
   // NOTE: the following 3 fields are filled in at runtime
   uint64_t owner_id;         // ID of the User account running this ms_client
   uint64_t gateway_id;       // ID of the Gateway running this ms_client
   int portnum;               // port we listen on

   //pthread_t uploader_thread;
   bool running;        // set to true if the uploader thread is running
   bool downloading;    // set to true if we're downloading something on ms_read
   bool downloading_view;       // set to true if we're downloading something on ms_view
   bool downloading_certs;      // set to true if we're downloading something on ms_certs
   bool uploading;      // set to true if we're uploading something on ms_write
   bool more_work;      // set to true if more work arrives while we're working
   bool uploader_running;  // set to true if the uploader is running
   pthread_mutex_t uploader_lock;     // wake up the uploader thread when there is work to do
   pthread_cond_t uploader_cv;
   
   // gateway view-change structures (represents a consistent view of the Volume control state)
   pthread_t view_thread;
   bool view_thread_running;        // set to true if the view thread is running
   bool early_reload;               // check back to see if there is new Volume information
   struct ms_volume* volume;        // Volume we're bound to
   ms_client_view_change_callback view_change_callback;         // call this function when the Volume gets reloaded
   void* view_change_callback_cls;                              // user-supplied argument to the above callbck
   pthread_rwlock_t view_lock;

   // session information
   int64_t session_expires;                 // when the session password expires
   char* session_password;

   // key information
   // NOTE: this field does not change over the course of the ms_client structure's lifetime.
   // you can use it without locking, as long as you don't destroy it.
   EVP_PKEY* my_key;
   EVP_PKEY* my_pubkey;
   
   // raw private key
   char* my_key_pem;
   size_t my_key_pem_len;
   bool my_key_pem_mlocked;

   // reference to syndicate config 
   struct md_syndicate_conf* conf;
   
   // syndicate public key 
   EVP_PKEY* syndicate_public_key;
   char* syndicate_public_key_pem;
   
   // downloader instance 
   struct md_downloader dl;
};

extern "C" {

// module control 
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf );
int ms_client_destroy( struct ms_client* client );

// registration and certificate management
int ms_client_openid_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password, char const* volume_pubkey_pem, char const* key_password );
int ms_client_anonymous_gateway_register( struct ms_client* client, char const* volume_name, char const* volume_public_key_pem );
int ms_client_public_key_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* user_privkey_pem, char const* volume_pubkey_pem, char const* key_password );
int ms_client_load_cert( struct ms_client* client, uint64_t my_gateway_id, struct ms_gateway_cert* cert, const ms::ms_gateway_cert* ms_cert );
int ms_client_reload_certs( struct ms_client* client, uint64_t new_cert_bundle_version );
int ms_client_reload_volume( struct ms_client* client );

// peer verification
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len );

// generic access to MS RPC via OpenID
int ms_client_openid_auth_rpc( char const* ms_openid_url, char const* username, char const* password,
                               char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len,
                               char* syndicate_public_key_pem );

int ms_client_openid_rpc( char const* ms_openid_url, char const* username, char const* password,
                          char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len );

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

// file metadata API
uint64_t ms_client_make_file_id();
int ms_client_create( struct ms_client* client, uint64_t* file_id, struct md_entry* ent );
int ms_client_mkdir( struct ms_client* client, uint64_t* file_id, struct md_entry* ent );
int ms_client_delete( struct ms_client* client, struct md_entry* ent );
int ms_client_update_write( struct ms_client* client, struct md_entry* ent, uint64_t* affected_blocks, size_t num_affected_blocks );
int ms_client_update( struct ms_client* client, struct md_entry* ent );
int ms_client_coordinate( struct ms_client* client, uint64_t* new_coordinator, struct md_entry* ent );
int ms_client_rename( struct ms_client* client, struct md_entry* src, struct md_entry* dest );

// xattr API
int ms_client_getxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char const* xattr_name, char** xattr_value, size_t* xattr_value_len );
int ms_client_listxattr( struct ms_client* client, uint64_t volume_id, uint64_t file_id, char** xattr_names, size_t* xattr_names_len );
int ms_client_setxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, char const* xattr_value, size_t xattr_value_len, mode_t mode, int flags );
int ms_client_removexattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name );
int ms_client_chownxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, uint64_t new_owner );
int ms_client_chmodxattr( struct ms_client* client, struct md_entry* ent, char const* xattr_name, mode_t new_mode );

// vacuum API 
int ms_client_vacuum_entry_init( struct ms_vacuum_entry* vreq, uint64_t volume_id, uint64_t file_id, int64_t file_version,
                                 int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec, uint64_t* affected_blocks, size_t num_affected_blocks );
int ms_client_vacuum_entry_set_blocks( struct ms_vacuum_entry* vreq, uint64_t* affected_blocks, size_t num_affected_blocks );
int ms_client_vacuum_entry_free( struct ms_vacuum_entry* vreq );
int ms_client_peek_vacuum_log( struct ms_client* client, uint64_t volume_id, uint64_t file_id, struct ms_vacuum_entry* ve );
int ms_client_remove_vacuum_log_entry( struct ms_client* client, uint64_t volume_id, uint64_t file_id, uint64_t file_version, int64_t manifest_mtime_sec, int32_t manifest_mtime_nsec );

// path resolution 
int ms_client_get_listings( struct ms_client* client, path_t* path, ms_response_t* ms_response );
int ms_client_make_path_ent( struct ms_path_ent* path_ent, uint64_t volume_id, uint64_t file_id, int64_t version, int64_t write_nonce, char const* name, void* cls );

// asynchronous and batched API access
/*
int ms_client_queue_update( struct ms_client* client, struct md_entry* update, uint64_t deadline_ms, uint64_t deadline_delta_ms );
int ms_client_clear_update( struct ms_client* client, uint64_t volume_id, uint64_t file_id );

int ms_client_sync_update( struct ms_client* client, uint64_t volume_id, uint64_t file_id );
int ms_client_sync_updates( struct ms_client* client, uint64_t freshness_ms );
*/

// get information about the volume
uint64_t ms_client_volume_version( struct ms_client* client );
uint64_t ms_client_cert_version( struct ms_client* client );
uint64_t ms_client_get_volume_id( struct ms_client* client );
uint64_t ms_client_get_volume_blocksize( struct ms_client* client );
char* ms_client_get_volume_name( struct ms_client* client );
uint64_t ms_client_get_num_files( struct ms_client* client );
int ms_client_get_volume_root( struct ms_client* client, struct md_entry* root );

// control the thread that keeps a consistent view of volume metadata
int ms_client_set_view_change_callback( struct ms_client* client, ms_client_view_change_callback clb, void* cls );
void* ms_client_set_view_change_callback_cls( struct ms_client* client, void* cls );
int ms_client_sched_volume_reload( struct ms_client* client );
int ms_client_process_header( struct ms_client* client, uint64_t volume_id, uint64_t volume_version, uint64_t cert_version );

// get information about a specific gateway
int ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id );
char* ms_client_get_hostname( struct ms_client* client );
int ms_client_get_portnum( struct ms_client* client );
uint64_t* ms_client_RG_ids( struct ms_client* client );
bool ms_client_is_AG( struct ms_client* client, uint64_t ag_id );
uint64_t ms_client_get_AG_blocksize( struct ms_client* client, uint64_t gateway_id );
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t gateway_id );
char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t gateway_id );
char* ms_client_get_RG_content_url( struct ms_client* client, uint64_t gateway_id );
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id );
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* volume_id );
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, char** gateway_name );
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps );
int ms_client_my_key_pem_UNSAFE( struct ms_client* client, char** buf, size_t* len );
int ms_client_get_closure_text( struct ms_client* client, char** closure_text, uint64_t* closure_len );

// memory management
void ms_client_free_path_ent( struct ms_path_ent* path_ent, void (*free_cls)(void*) );
void ms_client_free_path( path_t* path, void (*free_cls)(void*) );
void ms_client_free_response( ms_response_t* ms_response );
void ms_client_free_listing( struct ms_listing* listing );

// closure
extern struct md_closure_callback_entry MS_CLIENT_CACHE_CLOSURE_PROTOTYPE[];
int ms_client_volume_connect_cache( struct ms_client* client, CURL* curl, char const* url );

}

// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// Verify the authenticity of a gateway message, encoded as a protobuf (class T)
template< class T > int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_type, uint64_t gateway_id, T* protobuf ) {
   ms_client_view_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      errorf("Message from outside Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // look up the certificate bundle
   ms_cert_bundle* bundle = NULL;
   
   if( gateway_type == SYNDICATE_UG ) {
      bundle = client->volume->UG_certs;
   }
   else if( gateway_type == SYNDICATE_RG ) {
      bundle = client->volume->RG_certs;
   }
   else if( gateway_type == SYNDICATE_AG ) {
      bundle = client->volume->AG_certs;
   }
   else {
      errorf("Invalid Gateway type %" PRIu64 "\n", gateway_type );
      ms_client_view_unlock( client );
      return -EINVAL;
   }
   
   // look up the cert
   ms_cert_bundle::iterator itr = bundle->find( gateway_id );
   if( itr == bundle->end() ) {
      // not found here--probably means we need to reload our certs
      
      dbprintf("WARN: No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      client->early_reload = true;
      ms_client_view_unlock( client );
      return -EAGAIN;
   }
   
   // verify the cert
   int rc = md_verify< T >( itr->second->pubkey, protobuf );
   
   ms_client_view_unlock( client );
   
   return rc;
}


#endif