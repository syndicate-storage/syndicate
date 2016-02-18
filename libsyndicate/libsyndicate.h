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
#include <wordexp.h>

using namespace std;

#include <curl/curl.h>

#include "ms.pb.h"
#include "sg.pb.h"

#define MD_ENTRY_FILE ms::ms_entry::MS_ENTRY_TYPE_FILE
#define MD_ENTRY_DIR  ms::ms_entry::MS_ENTRY_TYPE_DIR

#define SG_ENTRY_FILE MD_ENTRY_FILE
#define SG_ENTRY_DIR MD_ENTRY_DIR

#define SG_DEFAULT_CONFIG_DIR   "~/.syndicate"
#define SG_DEFAULT_CONFIG_PATH  "~/.syndicate/syndicate.conf"

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
   
   unsigned char* ent_sig;      // signature over this entry from the coordinator, as well as any ancillary data below
   size_t ent_sig_len;
   
   // ancillary data: not always filled in
   
   uint64_t parent_id;  // id of this file's parent directory
   
   // putxattr, removexattr only (and only from the coordinator)
   unsigned char* xattr_hash;   // hash over (volume ID, file ID, xattr_nonce, sorted(xattr name, xattr value))
};

#define MD_ENTRY_INITIALIZED( ent ) ((ent).type != 0 && (ent).name != NULL)

typedef list<struct md_entry*> md_entry_list;

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
   
   // paths 
   char* config_file_path;                            // *absolute* path to the config file.
   char* volumes_path;                                // path to the directory containing volume keys and certs
   char* gateways_path;                               // path to the directory containing gateway keys and certs 
   char* users_path;                                  // path to the directory containing user keys and certs 
   char* drivers_path;                                // path to the directory containing drivers
   char* syndicate_path;                              // path to the directory containing Syndicate public keys
   char* logs_path;                                   // path to the logfile directory to store gateway logs
   char* data_root;                                   // root of the path where we store local file blocks
   char* certs_root;                                  // path to the *root* directory containing cached certs from other users, volumes, and gateways.
   char* certs_path;                                  // path to the *gateway-specific* directory containing cached certs.  Derived from certs_root; set at runtime.
   
   // command-line options 
   char* volume_name;                                 // name of the volume we're connected to
   char* gateway_name;                                // name of this gateway
   char* ms_username;                                 // MS username for this user
   
   // gateway fields
   int64_t default_read_freshness;                    // default number of milliseconds a file can age before needing refresh for reads
   int64_t default_write_freshness;                   // default number of milliseconds a file can age before needing refresh for writes
   bool gather_stats;                                 // gather statistics or not?
   int max_read_retry;                                // maximum number of times to retry a read (i.e. fetching a block or manifest) before considering it failed 
   int max_write_retry;                               // maximum number of times to retry a write (i.e. replicating a block or manifest) before considering it failed
   int max_metadata_read_retry;                       // maximum number of times to retry a metadata read before considering it failed 
   int max_metadata_write_retry;                      // maximum number of times to retry a metadata write before considering it failed
   int connect_timeout;                               // number of seconds to wait to connect for data
   int transfer_timeout;                              // how long a manifest/block transfer is allowed to take (in seconds)
   bool verify_peer;                                  // whether or not to verify the gateway server's SSL certificate with peers (if using HTTPS to talk to them)
   uint64_t cache_soft_limit;                         // soft limit on the size in bytes of the cache 
   uint64_t cache_hard_limit;                         // hard limit on the size in bytes of the cache
   char* metadata_url;                                // MS url
   uint64_t config_reload_freq;                       // how often do we check for a new configuration from the MS?
   
   // cert and key processors 
   char* certs_reload_helper;                         // command to go reload and revalidate all certificates
   char* driver_reload_helper;                        // command to go reload and revalidate the driver
   char** helper_env;                                 // environment variables to pass 
   size_t num_helper_envs;
   size_t max_helper_envs;
   
   // debug
   int debug_lock;                                    // print verbose information on locks

   // security fields (loaded at runtime).
   // private keys are all mlock'ed
   char* gateway_key;                                 // gateway private key (PEM format)
   size_t gateway_key_len;

   // set at runtime
   mode_t usermask;                                   // umask of the user running this program
   char* hostname;                                    // what's our hostname?
   int portnum;                                       // Syndicate-side port number
   uint64_t owner;                                    // what is our user ID in Syndicate?  Files created in this UG will assume this UID as their owner
   uint64_t gateway;                                  // what is our gateway ID?
   uint64_t volume;                                   // what is our volume ID?
   uint64_t gateway_type;                             // type of gateway 
   int64_t cert_bundle_version;                       // the version of the cert bundle (obtained during initialization)
   int64_t gateway_version;                           // the version of this gateway's cert (obtained during initialization)
   int64_t volume_version;                            // the version of the volume (obtained during initialization)
   uint64_t blocksize;                                // the size in blocks for this volume.  Loaded at runtime.
   char* content_url;                                 // what is the URL under which local data can be accessed publicly?.  Must end in /

   char* driver_exec_path;                            // what is the path to the driver to execute?
   char** driver_roles;                               // what are the different role(s) this gateway's driver takes on?
   size_t num_driver_roles;
   
   char* user_pubkey_pem;                             // Syndicate User public key (PEM format) (if the private key is given, this will be generated from it)
   size_t user_pubkey_pem_len;
   EVP_PKEY* user_pubkey;
   char* volume_pubkey_pem;                           // volume metadata public key (PEM format).  Corresponds to the volume owner's public key
   size_t volume_pubkey_pem_len;
   EVP_PKEY* volume_pubkey;
   
   // misc
   bool is_client;                                    // if true for a UG, always fetch data from RGs
};

#define SG_USER_ANON               (uint64_t)0xFFFFFFFFFFFFFFFFLL
#define SG_GATEWAY_ANON            (uint64_t)0xFFFFFFFFFFFFFFFFLL
#define SG_GATEWAY_TOOL            (uint64_t)0xFFFFFFFFFFFFFFFELL       // gateway id used by messages from the administrative tool

// config elements 
#define SG_CONFIG_VOLUMES_PATH            "volumes"
#define SG_CONFIG_GATEWAYS_PATH           "gateways"
#define SG_CONFIG_USERS_PATH              "users"
#define SG_CONFIG_LOGS_PATH               "logs"
#define SG_CONFIG_DRIVERS_PATH            "drivers"
#define SG_CONFIG_DATA_ROOT               "data"
#define SG_CONFIG_SYNDICATE_PATH          "syndicate"
#define SG_CONFIG_CERTS_ROOT              "certs"

#define SG_CONFIG_CERTS_RELOAD_HELPER     "certs_reload"
#define SG_CONFIG_DRIVER_RELOAD_HELPER    "driver_reload"
#define SG_CONFIG_ENVAR                   "env"

#define SG_CONFIG_DEFAULT_READ_FRESHNESS  "default_read_freshness"
#define SG_CONFIG_DEFAULT_WRITE_FRESHNESS "default_write_freshness"
#define SG_CONFIG_GATHER_STATS            "gather_stats"
#define SG_CONFIG_CONNECT_TIMEOUT         "connect_timeout"
#define SG_CONFIG_MS_USERNAME             "username"
#define SG_CONFIG_RELOAD_FREQUENCY        "config_reload"
#define SG_CONFIG_TLS_VERIFY_PEER         "verify_peer"
#define SG_CONFIG_MS_URL                  "MS_url"
#define SG_CONFIG_PUBLIC_URL              "public_url"
#define SG_CONFIG_DEBUG_LEVEL             "debug_level"
#define SG_CONFIG_DEBUG_LOCK              "debug_lock"
#define SG_CONFIG_TRANSFER_TIMEOUT        "transfer_timeout"
#define SG_CONFIG_CACHE_SOFT_LIMIT        "cache_soft_limit"
#define SG_CONFIG_CACHE_HARD_LIMIT        "cache_hard_limit"
#define SG_CONFIG_MAX_READ_RETRY          "max_read_retry"
#define SG_CONFIG_MAX_WRITE_RETRY         "max_write_retry"
#define SG_CONFIG_MAX_METADATA_READ_RETRY "max_metadata_read_retry"
#define SG_CONFIG_MAX_METADATA_WRITE_RETRY "max_metadata_write_retry"


// some default values
#define SG_DEFAULT_VOLUMES_PATH     "~/.syndicate/volumes"
#define SG_DEFAULT_GATEWAYS_PATH    "~/.syndicate/gateways"
#define SG_DEFAULT_USERS_PATH       "~/.syndicate/users"
#define SG_DEFAULT_LOGS_PATH        "~/.syndicate/logs"
#define SG_DEFAULT_DRIVERS_PATH     "~/.syndicate/drivers"
#define SG_DEFAULT_DATA_ROOT        "~/.syndicate/data"
#define SG_DEFAULT_SYNDICATE_PATH   "~/.syndicate/syndicate"
#define SG_DEFAULT_CERTS_ROOT       "~/.syndicate/certs"

#define SG_DEFAULT_CERTS_RELOAD_HELPER     "/usr/local/lib/syndicate/certs_reload"
#define SG_DEFAULT_DRIVER_RELOAD_HELPER    "/usr/local/lib/syndicate/driver_reload"

// URL protocol prefix for local files
#define SG_LOCAL_PROTO     "file://"

#define SG_DATA_PREFIX "DATA"
#define SG_GETXATTR_PREFIX "GETXATTR"
#define SG_LISTXATTR_PREFIX "LISTXATTR"

// check to see if a URL refers to local data
#define SG_URL_LOCAL( url ) (strlen(url) > strlen(SG_LOCAL_PROTO) && strncmp( (url), SG_LOCAL_PROTO, strlen(SG_LOCAL_PROTO) ) == 0)

// extract the absolute, underlying path from a local url
#define SG_URL_LOCAL_PATH( url ) ((char*)(url) + strlen(SG_LOCAL_PROTO))

// gateway cert structure
struct ms_gateway_cert;

// map gateway IDs to certs
typedef map<uint64_t, struct ms_gateway_cert*> ms_cert_bundle;

extern "C" {

// library config 
int md_debug( struct md_syndicate_conf* conf, int level );
int md_error( struct md_syndicate_conf* conf, int level );
int md_signals( int use_signals );

// configuration parser
int md_read_conf( char const* conf_path, struct md_syndicate_conf* conf );
int md_read_conf_line( char* line, char** key, char*** values );
int md_free_conf( struct md_syndicate_conf* conf );
int md_default_conf( struct md_syndicate_conf* conf );
int md_check_conf( struct md_syndicate_conf* conf );

char* md_conf_get_data_root( struct md_syndicate_conf* conf );

// md_entry
struct md_entry* md_entry_dup( struct md_entry* src );
int md_entry_dup2( struct md_entry* src, struct md_entry* ret );
void md_entry_free( struct md_entry* ent );
#define md_entry_sign( privkey, ent, sig, siglen ) md_entry_sign2( privkey, ent, sig, siglen, __FILE__, __LINE__ )
int md_entry_sign2( EVP_PKEY* privkey, struct md_entry* ent, unsigned char** sig, size_t* sig_len, char const* file, int lineno );
int ms_entry_verify( struct ms_client* ms, ms::ms_entry* msent );

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
int md_expand_path( char const* path, char** expanded, size_t* expanded_len );

// serialization
int md_entry_to_ms_entry( ms::ms_entry* msent, struct md_entry* ent );
int ms_entry_to_md_entry( const ms::ms_entry& msent, struct md_entry* ent );
int md_entry_to_string( struct md_entry* ent, char** data );

// threading
int md_start_thread( pthread_t* th, void* (*thread_func)(void*), void* args, bool detach );

// URL parsing
char** md_parse_cgi_args( char* query_string );
char* md_path_from_url( char const* url );
char* md_flatten_path( char const* path );
char* md_rchomp( char* path, char delim );
int md_split_url_qs( char const* url, char** url_and_path, char** qs );
int md_parse_hostname_portnum( char const* url, char** hostname, int* portnum );

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

// initialize syndicate as an anonymous read-only client only
int md_init_client( struct md_syndicate_conf* conf, struct ms_client* client, struct md_opts* opts );

int md_shutdown(void);

// load certs 
int md_certs_reload( struct md_syndicate_conf* conf, EVP_PKEY** syndicate_pubkey, ms::ms_user_cert* user_cert, ms::ms_user_cert* volume_owner_cert, ms::ms_volume_metadata* volume_cert, ms_cert_bundle* gateway_certs );
int md_driver_reload( struct md_syndicate_conf* conf, struct ms_gateway_cert* cert );
struct ms_gateway_cert* md_gateway_cert_find( ms_cert_bundle* gateway_certs, uint64_t gateway_id );

// driver 
int md_conf_set_driver_params( struct md_syndicate_conf* conf, char const* driver_exec_path, char const** driver_roles, size_t num_roles );

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

#define SG_INVALID_BLOCK_ID (uint64_t)(0xffffffffffffffff)
#define SG_INVALID_GATEWAY_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_VOLUME_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_FILE_ID SG_INVALID_BLOCK_ID
#define SG_INVALID_USER_ID SG_INVALID_BLOCK_ID

// gateway HTTP error codes
#define SG_HTTP_TRYAGAIN    503

#define SG_CAP_READ_DATA  1
#define SG_CAP_WRITE_DATA  2
#define SG_CAP_READ_METADATA  4
#define SG_CAP_WRITE_METADATA  8
#define SG_CAP_COORDINATE  16

#define SG_RSA_KEY_SIZE 4096


// limits
#define SG_MAX_CERT_LEN                  10*1024           // 10kb--max certificate size
#define SG_MAX_MANIFEST_LEN              10*1024*1024L      // 10MB--max manifest size
#define SG_MAX_DRIVER_LEN                10*1024*1024L      // 10MB--max driver size
#define SG_MAX_XATTR_LEN                 10*1024*1024L     // 10MB--max xattr size
#define SG_MAX_BLOCK_LEN_MULTIPLIER      5                 // i.e. a serialized block can't be more than $SG_MAX_BLOCK_LEN_MULTIPLIER times the size of a block
                                                           // (there are some serious problems with the design of a driver that requires this, IMHO).
#endif
