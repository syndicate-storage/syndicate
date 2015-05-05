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

#ifndef _LIBSYNDICATE_H_
#define _LIBSYNDICATE_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <vector>
#include <map>
#include <dirent.h>
#include <utime.h>
#include <fstream>
#include <string>
#include <iostream>
#include <sys/socket.h>
#include <pthread.h>
#include <pwd.h>
#include <math.h>
#include <locale>
#include <signal.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <inttypes.h>
#include <sys/resource.h>
#include <stdexcept>
#include <execinfo.h>

using namespace std;

#include <curl/curl.h>

#include "ms.pb.h"
#include "sg.pb.h"

#define MD_ENTRY_FILE ms::ms_entry::MS_ENTRY_TYPE_FILE
#define MD_ENTRY_DIR  ms::ms_entry::MS_ENTRY_TYPE_DIR

// metadata entry (represents a file or a directory)
struct md_entry {
   int type;            // file or directory?
   char* name;          // name of this entry
   uint64_t file_id;    // id of this file 
   int64_t ctime_sec;   // creation time (seconds)
   int32_t ctime_nsec;  // creation time (nanoseconds)
   int64_t mtime_sec;   // last-modified time (seconds)
   int32_t mtime_nsec;  // last-modified time (nanoseconds)
   int64_t manifest_mtime_sec;  // manifest last-mod time (actual last-write time, regardless of utime) (seconds)
   int32_t manifest_mtime_nsec; // manifest last-mod time (actual last-write time, regardless of utime) (nanoseconds)
   int64_t write_nonce; // last-write nonce 
   int64_t xattr_nonce; // xattr write nonce
   int64_t version;     // file version
   int32_t max_read_freshness;      // how long is this entry fresh until it needs revalidation?
   int32_t max_write_freshness;     // how long can we delay publishing this entry?
   uint64_t owner;         // ID of the User that owns this File
   uint64_t coordinator;  // ID of the Gateway that coordinatates writes on this File
   uint64_t volume;        // ID of the Volume
   mode_t mode;         // file permission bits
   off_t size;          // size of the file
   int32_t error;       // error information with this md_entry
   int64_t generation;  // n, as in, the nth item to ever be created in the parent directory
   int64_t num_children; // number of children this entry has (if it's a directory)
   int64_t capacity;    // maximum index number a child can have (i.e. used by listdir())
   uint64_t parent_id;  // id of this file's parent directory
   char* parent_name;   // name of this file's parent directory
};

#define MD_ENTRY_INITIALIZED( ent ) ((ent).type != 0 && (ent).name != NULL)

typedef list<struct md_entry*> md_entry_list;


// metadata update
struct md_update {
   char op;               // update operation
   struct md_entry ent;
   struct md_entry dest;  // only used by RENAME
   int error;             // error information
   int flags;             // op-specific flags
   char* xattr_name;      // xattr name (setxattr, removexattr)
   char* xattr_value;     // xattr value (setxattr)
   size_t xattr_value_len;
   uint64_t xattr_owner;  // xattr owner (chownxattr)
   mode_t xattr_mode;     // xattr mode (chmodxattr)
   
   uint64_t* affected_blocks;   // blocks affected by a write 
   size_t num_affected_blocks;  
};

// metadata update operations
#define MD_OP_ADD    'A'      // add/replace an entry
#define MD_OP_RM     'R'      // remove an entry
#define MD_OP_UP     'U'      // update an existing entry
#define MD_OP_ERR    'E'      // (response only) error encountered in processing the entry
#define MD_OP_VER    'V'      // write verification request
#define MD_OP_USR    'S'      // special (context-specific) state
#define MD_OP_NEWBLK 'B'      // new block written
#define MD_OP_CHOWN  'C'

// upload buffer
struct md_upload_buf {
   char const* text;
   int offset;
   int len;
};


// merge command-line options with the config....
#define MD_SYNDICATE_CONF_OPT( conf, optname, value, result ) \
   do { \
     if( (value) ) { \
        if( (conf).optname ) { \
           SG_safe_free( (conf).optname ); \
        } \
        \
        (conf).optname = SG_strdup_or_null( value ); \
        if( (conf).optname == NULL ) { \
            result = -ENOMEM; \
        } \
      } \
   } while( 0 );


// server configuration
struct md_syndicate_conf {
   
   // gateway fields
   int64_t default_read_freshness;                    // default number of milliseconds a file can age before needing refresh for reads
   int64_t default_write_freshness;                   // default number of milliseconds a file can age before needing refresh for writes
   char* logfile_path;                                // path to the logfile
   bool gather_stats;                                 // gather statistics or not?
   char* content_url;                                 // what is the URL under which local data can be accessed publicly?.  Must end in /
   char* storage_root;                                // toplevel directory that stores local syndicate state (blocks, manifests, logs, etc).  Must end in /
   char* volume_name;                                 // name of the volume we're connected to
   char* volume_pubkey_path;                          // path on disk to find Volume metadata public key
   int max_read_retry;                                // maximum number of times to retry a read (i.e. fetching a block or manifest) before considering it failed 
   int max_write_retry;                               // maximum number of times to retry a write (i.e. replicating a block or manifest) before considering it failed
   int max_metadata_read_retry;                       // maximum number of times to retry a metadata read before considering it failed 
   int max_metadata_write_retry;                      // maximum number of times to retry a metadata write before considering it failed
   unsigned int num_http_threads;                     // how many HTTP threads to create
   char* server_key_path;                             // path to PEM-encoded TLS public/private key for this gateway server
   char* server_cert_path;                            // path to PEM-encoded TLS certificate for this gateway server
   char* local_sd_dir;                                // directory containing local storage drivers (AG only)
   char* gateway_name;                                // name of this gateway
   int portnum;                                       // Syndicate-side port number
   int connect_timeout;                               // number of seconds to wait to connect for data
   int transfer_timeout;                              // how long a manifest/block transfer is allowed to take (in seconds)
   bool verify_peer;                                  // whether or not to verify the gateway server's SSL certificate with peers (if using HTTPS to talk to them)
   char* gateway_key_path;                            // path to PEM-encoded user-given public/private key for this gateway
   uint64_t cache_soft_limit;                         // soft limit on the size in bytes of the cache 
   uint64_t cache_hard_limit;                         // hard limit on the size in bytes of the cache
   int num_iowqs;                                     // number of I/O work queues
      
   // debug
   int debug_lock;                                    // print verbose information on locks

   // MS-related fields
   char* metadata_url;                                // MS url
   char* ms_username;                                 // MS username for this SyndicateUser
   char* ms_password;                                 // MS password for this SyndicateUser
   uint64_t owner;                                    // what is our user ID in Syndicate?  Files created in this UG will assume this UID as their owner
   uint64_t gateway;                                  // what is our gateway ID?
   uint64_t volume;                                   // what is our volume ID?
   uint64_t config_reload_freq;                       // how often do we check for a new configuration from the MS?
   char* syndicate_pubkey_path;                       // location on disk where the MS's public key can be found.

   // security fields (loaded at runtime).
   // private keys are all mlock'ed
   char* gateway_key;                                 // gateway private key (PEM format)
   size_t gateway_key_len;
   char* server_key;                                  // TLS private key (PEM format)
   size_t server_key_len;
   char* server_cert;                                 // TLS certificate (PEM format)
   size_t server_cert_len;
   char* volume_pubkey;                               // volume metadata public key (PEM format)
   size_t volume_pubkey_len;
   char* syndicate_pubkey;                            // Syndicate-specific public key
   size_t syndicate_pubkey_len;
   char* user_pkey;                                   // Syndicate User private key 
   size_t user_pkey_len;

   // set at runtime
   char* data_root;                                   // root of the path where we store local file blocks (a subdirectory of storage_root)
   mode_t usermask;                                   // umask of the user running this program
   char* hostname;                                    // what's our hostname?
   
   // misc
   uint64_t gateway_type;                                  // type of gateway 
   bool is_client;                                    // if true for a UG, always fetch data from RGs
};

#define SG_USER_ANON               (uint64_t)0xFFFFFFFFFFFFFFFFLL
#define SG_GATEWAY_ANON            (uint64_t)0xFFFFFFFFFFFFFFFFLL

// config elements 
#define SG_CONFIG_DEFAULT_READ_FRESHNESS  "DEFAULT_READ_FRESHNESS"
#define SG_CONFIG_DEFAULT_WRITE_FRESHNESS "DEFAULT_WRITE_FRESHNESS"
#define SG_CONFIG_CONNECT_TIMEOUT         "CONNECT_TIMEOUT"
#define SG_CONFIG_MS_USERNAME             "USERNAME"
#define SG_CONFIG_MS_PASSWORD             "PASSWORD"
#define SG_CONFIG_RELOAD_FREQUENCY        "RELOAD_FREQUENCY"
#define SG_CONFIG_TLS_VERIFY_PEER         "TLS_VERIFY_PEER"
#define SG_CONFIG_MS_URL                  "MS_URL"
#define SG_CONFIG_LOGFILE_PATH            "LOGFILE"
#define SG_CONFIG_GATHER_STATS            "GATHER_STATISTICS"
#define SG_CONFIG_NUM_HTTP_THREADS        "HTTP_THREADPOOL_SIZE"
#define SG_CONFIG_STORAGE_ROOT            "STORAGE_ROOT"
#define SG_CONFIG_TLS_PKEY_PATH           "TLS_PKEY"
#define SG_CONFIG_TLS_CERT_PATH           "TLS_CERT"
#define SG_CONFIG_GATEWAY_NAME            "GATEWAY_NAME"
#define SG_CONFIG_GATEWAY_PKEY_PATH       "GATEWAY_PKEY"
#define SG_CONFIG_SYNDICATE_PUBKEY_PATH   "SYNDICATE_PUBKEY"
#define SG_CONFIG_VOLUME_NAME             "VOLUME_NAME"
#define SG_CONFIG_PORTNUM                 "PORTNUM"
#define SG_CONFIG_PUBLIC_URL              "PUBLIC_URL"
#define SG_CONFIG_DEBUG_LEVEL             "DEBUG_LEVEL"
#define SG_CONFIG_LOCAL_DRIVERS_DIR       "LOCAL_DRIVERS_DIR"
#define SG_CONFIG_TRANSFER_TIMEOUT        "TRANSFER_TIMEOUT"
#define SG_CONFIG_CACHE_SOFT_LIMIT        "CACHE_SOFT_LIMIT"
#define SG_CONFIG_CACHE_HARD_LIMIT        "CACHE_HARD_LIMIT"
#define SG_CONFIG_NUM_IOWQS               "NUM_IO_WQS"

// URL protocol prefix for local files
#define SG_LOCAL_PROTO     "file://"

#define SG_DATA_PREFIX "SYNDICATE-DATA"

// check to see if a URL refers to local data
#define SG_URL_LOCAL( url ) (strlen(url) > strlen(SG_LOCAL_PROTO) && strncmp( (url), SG_LOCAL_PROTO, strlen(SG_LOCAL_PROTO) ) == 0)

// extract the absolute, underlying path from a local url
#define SG_URL_LOCAL_PATH( url ) ((char*)(url) + strlen(SG_LOCAL_PROTO))

extern "C" {

// library config 
int md_debug( struct md_syndicate_conf* conf, int level );
int md_error( struct md_syndicate_conf* conf, int level );
int md_signals( int use_signals );

// configuration parser
int md_read_conf( char const* conf_path, struct md_syndicate_conf* conf );
int md_read_conf_line( char* line, char** key, char*** values );
int md_free_conf( struct md_syndicate_conf* conf );

// md_entry
struct md_entry* md_entry_dup( struct md_entry* src );
int md_entry_dup2( struct md_entry* src, struct md_entry* ret );
void md_entry_free( struct md_entry* ent );

// serialization
int md_path_version_offset( char const* path );
ssize_t md_metadata_update_text( struct md_syndicate_conf* conf, char **buf, struct md_update** updates );
ssize_t md_metadata_update_text3( struct md_syndicate_conf* conf, char **buf, struct md_update* (*iterator)( void* ), void* arg );
char* md_clear_version( char* path );
void md_update_free( struct md_update* update );
int md_update_dup2( struct md_update* src, struct md_update* dest );

// path manipulation
char* md_fullpath( char const* root, char const* path, char* dest );
char* md_dirname( char const* path, char* dest );
char* md_basename( char const* path, char* dest );
int md_depth( char const* path );
int md_basename_begin( char const* path );
int md_dirname_end( char const* path );
char* md_prepend( char const* prefix, char const* str, char* output );
long md_hash( char const* path );
int md_path_split( char const* path, vector<char*>* result );
void md_sanitize_path( char* path );

// serialization
int md_entry_to_ms_entry( ms::ms_entry* msent, struct md_entry* ent );
int ms_entry_to_md_entry( const ms::ms_entry& msent, struct md_entry* ent );

// threading
pthread_t md_start_thread( void* (*thread_func)(void*), void* args, bool detach );

// URL parsing
char** md_parse_cgi_args( char* query_string );
char* md_path_from_url( char const* url );
char* md_flatten_path( char const* path );

int md_split_url_qs( char const* url, char** url_and_path, char** qs );

// header parsing
off_t md_header_value_offset( char* header_buf, size_t header_len, char const* header_name );
int md_parse_header( char* header_buf, char const* header_name, char** header_value );
uint64_t md_parse_header_uint64( char* hdr, off_t offset, size_t size );
uint64_t* md_parse_header_uint64v( char* hdr, off_t offset, size_t size, size_t* ret_len );

// networking 
char* md_get_hostname( struct md_syndicate_conf* conf );
int md_set_hostname( struct md_syndicate_conf* conf, char const* hostname );

// top-level initialization
int md_init( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts );

// initialize syndicate as a client only
int md_init_client( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts );

int md_shutdown(void);
int md_default_conf( struct md_syndicate_conf* conf, uint64_t gateway_type );
int md_check_conf( struct md_syndicate_conf* conf, bool have_key_password );

}


// protobuf serializer
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
template <class T> int md_serialize( T* protobuf, char** bits, size_t* bits_len ) {
   string msgbits;
   try {
      protobuf->SerializeToString( &msgbits );
   }
   catch( exception& e ) {
      SG_error("SerializeToString exception: %s\n", e.what() );
      return -EINVAL;
   }
   
   char* ret = SG_CALLOC( char, msgbits.size() );
   if( ret == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( ret, msgbits.data(), msgbits.size() );
   
   *bits = ret;
   *bits_len = msgbits.size();
   
   return 0;
}


// protobuf parser
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
template <class T> int md_parse( T* protobuf, char const* bits, size_t bits_len ) {
   bool valid = false;
   try {
      valid = protobuf->ParseFromString( string(bits, bits_len) );
   }
   catch( exception& e ) {
      SG_error("ParseFromString exception: %s\n", e.what() );
      return -EINVAL;
   }
   
   if( !valid ) {
      SG_error("ParseFromString rc = %d (missing %s)\n", valid, protobuf->InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   return 0;
}


// system UID
#define SG_SYS_USER 0

#define SG_INVALID_BLOCK_ID (uint64_t)(-1)
#define SG_INVALID_GATEWAY_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_VOLUME_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_FILE_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_USER_ID SG_INVALID_BLOCK_ID

// gateway types for md_init
#define SYNDICATE_UG       ms::ms_gateway_cert::USER_GATEWAY
#define SYNDICATE_AG       ms::ms_gateway_cert::ACQUISITION_GATEWAY
#define SYNDICATE_RG       ms::ms_gateway_cert::REPLICA_GATEWAY
#define SG_VALID_GATEWAY_TYPE( type ) ((type) == SYNDICATE_UG || (type) == SYNDICATE_RG || (type) == SYNDICATE_AG)

// gateway HTTP error codes (used by the AG and UG)
#define SG_HTTP_TRYAGAIN    503

#define SG_CAP_READ_DATA  1
#define SG_CAP_WRITE_DATA  2
#define SG_CAP_READ_METADATA  4
#define SG_CAP_WRITE_METADATA  8
#define SG_CAP_COORDINATE  16

#define SG_RSA_KEY_SIZE 4096


// limits
#define SG_MAX_MANIFEST_LEN              100000000         // 100MB
#define SG_MAX_BLOCK_LEN_MULTIPLER       5                 // i.e. a block download can't be more than 5x the size of a block
                                                           // (there are some serious problems with the design of a closure that requires this, IMHO).

#endif
