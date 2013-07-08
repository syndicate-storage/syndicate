/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


#ifndef _MS_CLIENT_H_
#define _MS_CLIENT_H_

#include <sstream>
#include <queue>
#include "libsyndicate.h"

#define HTTP_VOLUME_SECRET "Syndicate-VolumeSecret"

#define HTTP_VOLUME_TIME   "X-Volume-Time"
#define HTTP_UG_TIME       "X-UG-Time"
#define HTTP_TOTAL_TIME    "X-Total-Time"
#define HTTP_RESOLVE_TIME  "X-Resolve-Time"
#define HTTP_CREATE_TIMES  "X-Create-Times"
#define HTTP_UPDATE_TIMES  "X-Update-Times"
#define HTTP_DELETE_TIMES  "X-Delete-Times"
#define HTTP_MS_LASTMOD    "X-MS-LastMod"

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

struct ms_client {
   pthread_rwlock_t lock;
   
   CURL* ms_read;
   CURL* ms_write;

   struct ms_client_timing read_times;
   struct ms_client_timing write_times;
   
   update_set* updates;
   deadline_queue* deadlines;

   char* url;
   char* userpass;
   char* file_url;

   pthread_t uploader_thread;
   bool running;        // set to true if the uploader thread is running
   bool downloading;    // set to true if we're downloading something on ms_read
   bool uploading;      // set to true if we're uploading something on ms_write
   bool more_work;      // set to true if more work arrives while we're working
   bool uploader_running;  // set to true if the uploader is running

   // gateway view-change structures
   pthread_t view_thread;
   struct md_user_entry** UG_creds;
   char** RG_urls;
   int num_RG_urls;
   uint64_t volume_version;      // version of the volume's metadata
   pthread_rwlock_t view_lock;

   char* volume_secret;          // secret for authenticating ourselves to the volume

   // wake up the uploader thread when there is work to do
   pthread_mutex_t uploader_lock;
   pthread_cond_t uploader_cv;
   
   struct md_syndicate_conf* conf;
};

extern "C" {
   
int ms_client_init( struct ms_client* client, struct md_syndicate_conf* conf, char const* volume_name, char const* username, char const* passwd );
int ms_client_get_volume_metadata( struct ms_client* client, char const* volume_name, char const* password, uint64_t* version, uid_t* my_owner_id, uid_t* volume_owner_id, gid_t* volume_id, uint64_t* blocksize );
int ms_client_destroy( struct ms_client* client );

int ms_client_rlock( struct ms_client* client );
int ms_client_wlock( struct ms_client* client );
int ms_client_unlock( struct ms_client* client );

int ms_client_view_rlock( struct ms_client* client );
int ms_client_view_wlock( struct ms_client* client );
int ms_client_view_unlock( struct ms_client* client );

int ms_client_queue_update( struct ms_client* client, char const* path, struct md_entry* update, uint64_t deadline_ms, uint64_t deadline_delta_ms );
int ms_client_clear_update( struct ms_client* client, char const* path );

int ms_client_create( struct ms_client* client, struct md_entry* ent );
int ms_client_mkdir( struct ms_client* client, struct md_entry* ent );
int ms_client_delete( struct ms_client* client, struct md_entry* ent );
int ms_client_update( struct ms_client* client, struct md_entry* ent );

int ms_client_sync_update( struct ms_client* client, char const* path );
int ms_client_sync_updates( struct ms_client* client, uint64_t freshness_ms );

int ms_client_resolve_path( struct ms_client* client, char const* path, vector<struct md_entry>* result_dirs, vector<struct md_entry>* result_base, struct timespec* lastmod, int* md_rc );

int ms_client_claim( struct ms_client* client, char const* path );

uid_t ms_client_authenticate( struct ms_client* client, struct md_HTTP_connection_data* data, char* username, char* password );

char** ms_client_RG_urls_copy( struct ms_client* client );

uint64_t ms_client_volume_version( struct ms_client* client );

}

#endif