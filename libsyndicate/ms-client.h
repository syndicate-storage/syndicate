/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _MS_CLIENT_H_
#define _MS_CLIENT_H_

#include <sstream>
#include <queue>
#include <locale>

#include "libsyndicate.h"

#define HTTP_VOLUME_SECRET "Syndicate-VolumeSecret"

#define HTTP_VOLUME_TIME   "X-Volume-Time"
#define HTTP_GATEWAY_TIME  "X-Gateway-Time"
#define HTTP_TOTAL_TIME    "X-Total-Time"
#define HTTP_RESOLVE_TIME  "X-Resolve-Time"
#define HTTP_CREATE_TIMES  "X-Create-Times"
#define HTTP_UPDATE_TIMES  "X-Update-Times"
#define HTTP_DELETE_TIMES  "X-Delete-Times"
#define HTTP_MS_LASTMOD    "X-MS-LastMod"

#define RSA_KEY_SIZE 4096

using namespace std;

typedef map<long, struct md_update> update_set;
typedef map<uint64_t, long> deadline_queue;


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


struct UG_cred {
   uint64_t user_id;
   uint64_t gateway_id;
   uint64_t volume_id;
   char* name;
   char* hostname;
   int portnum;
   EVP_PKEY* pubkey;
};


// Volume data
struct ms_volume {
   uint64_t volume_id;           // ID of this Volume
   uint64_t volume_owner_id;     // UID of the User that owns this Volume
   uint64_t blocksize;           // blocksize of this Volume
   char* name;
   
   EVP_PKEY* volume_public_key;  // Volume public key 
   bool reload_volume_key;       // do we reload this public key?
   
   bool early_reload;            // reload this Volume metadata now?

   uint64_t UG_version;          // version of UG metadata
   struct UG_cred** UG_creds;    // UGs in this Volume
   int num_UG_creds;

   uint64_t RG_version;          // version of the RG metadata
   char** RG_urls;               // URLs to RGs in this Volume
   int num_RG_urls;              // length of RG_urls

   uint64_t volume_version;      // version of the above information

   bool loading;                 // set to true if the Volume is in the process of being reloaded
};

struct ms_client {
   int gateway_type;
   
   pthread_rwlock_t lock;
   
   CURL* ms_read;
   CURL* ms_write;
   CURL* ms_view;

   struct ms_client_timing read_times;
   struct ms_client_timing write_times;
   
   update_set* updates;
   deadline_queue* deadlines;

   char* url;                 // MS URL
   char* userpass;            // HTTP username:password string.  Username is the gateway ID; password is the session password
   uint64_t owner_id;         // ID of the User account running this ms_client
   uint64_t gateway_id;       // ID of the Gateway running this ms_client

   pthread_t uploader_thread;
   bool running;        // set to true if the uploader thread is running
   bool downloading;    // set to true if we're downloading something on ms_read
   bool uploading;      // set to true if we're uploading something on ms_write
   bool more_work;      // set to true if more work arrives while we're working
   bool uploader_running;  // set to true if the uploader is running
   pthread_mutex_t uploader_lock;     // wake up the uploader thread when there is work to do
   pthread_cond_t uploader_cv;

   // gateway view-change structures
   pthread_t view_thread;
   bool view_thread_running;        // set to true if the view thread is running
   bool early_reload;               // check back to see if there are new Volumes
   struct ms_volume** volumes;      // Volumes we're bound to
   int num_volumes;                 // how many Volumes we're bound to
   pthread_rwlock_t view_lock;

   // session information
   int64_t session_timeout;                 // how long the session is valid
   char* session_password;

   // key information
   // NOTE: this field does not change over the course of the ms_client structure's lifetime.
   // you can use it without locking, as long as you don't destroy it.
   EVP_PKEY* my_key;

   // reference to syndicate config 
   struct md_syndicate_conf* conf;
};

extern "C" {

int ms_client_generate_key( EVP_PKEY** key );
int ms_client_init( struct ms_client* client, int gateway_type, struct md_syndicate_conf* conf );
int ms_client_destroy( struct ms_client* client );

int ms_client_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password );
int ms_client_load_cred( struct UG_cred* cred, const ms::ms_volume_gateway_cred* ms_cred );

int ms_client_reload_RGs( struct ms_client* client );
int ms_client_reload_UGs( struct ms_client* client );
int ms_client_reload_volume( struct ms_client* client, char const* volume_name, uint64_t volume_id );

int ms_client_verify_volume_metadata( EVP_PKEY* public_key, ms::ms_volume_metadata* volume_md );
int ms_client_verify_UGs( EVP_PKEY* public_key, ms::ms_volume_UGs* ugs );
int ms_client_verify_RGs( EVP_PKEY* public_key, ms::ms_volume_RGs* rgs );
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t user_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len );

int ms_client_load_pubkey( EVP_PKEY** key, char const* pubkey_str );
int ms_client_load_privkey( EVP_PKEY** key, char const* privkey_str );

int ms_client_rlock( struct ms_client* client );
int ms_client_wlock( struct ms_client* client );
int ms_client_unlock( struct ms_client* client );

int ms_client_view_rlock( struct ms_client* client );
int ms_client_view_wlock( struct ms_client* client );
int ms_client_view_unlock( struct ms_client* client );

int ms_client_queue_update( struct ms_client* client, struct md_entry* update, uint64_t deadline_ms, uint64_t deadline_delta_ms );
int ms_client_clear_update( struct ms_client* client, uint64_t volume_id, char const* path );

int ms_client_create( struct ms_client* client, struct md_entry* ent );
int ms_client_mkdir( struct ms_client* client, struct md_entry* ent );
int ms_client_delete( struct ms_client* client, struct md_entry* ent );
int ms_client_update( struct ms_client* client, struct md_entry* ent );

int ms_client_sync_update( struct ms_client* client, uint64_t volume_id, char const* path );
int ms_client_sync_updates( struct ms_client* client, uint64_t freshness_ms );

int ms_client_resolve_path( struct ms_client* client, uint64_t volume_id, char const* path, vector<struct md_entry>* result_dirs, vector<struct md_entry>* result_base, struct timespec* lastmod, int* md_rc );

int ms_client_claim( struct ms_client* client, char const* path );

char** ms_client_RG_urls_copy( struct ms_client* client, uint64_t volume_id );

uint64_t ms_client_volume_version( struct ms_client* client, uint64_t volume_id );
uint64_t ms_client_UG_version( struct ms_client* client, uint64_t volume_id );
uint64_t ms_client_RG_version( struct ms_client* client, uint64_t volume_id );
uint64_t ms_client_get_volume_id( struct ms_client* client, int i );
uint64_t ms_client_get_volume_blocksize( struct ms_client* client, uint64_t volume_id );
char* ms_client_get_volume_name( struct ms_client* client, uint64_t volume_id );

int ms_client_sched_volume_reload( struct ms_client* client, uint64_t volume_id );

}

#endif