/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/


// core state and thread control for Syndicate

#ifndef _SYNDICATE_H_
#define _SYNDICATE_H_

#include "libsyndicate.h"
#include "collator.h"
#include "stats.h"
#include "replication.h"
#include "http-common.h"
#include "fs.h"

class Collator;

struct syndicate_state {
   FILE* logfile;
   FILE* replica_logfile;
   struct ms_client* ms;   // metadata service client
   struct fs_core* core;   // core of the system
   Collator* col;          // collator

   // mounter info (since apparently FUSE doesn't do this right)
   int gid;
   int uid;

   // when was the filesystem started?
   time_t mounttime;

   // configuration
   struct md_syndicate_conf conf;

   // global running flag
   int running;

   // statistics
   Stats* stats;
};

struct metadata_poll_args {
   char* metadata_read_url;
   FILE* logfile;
   long pulltime;
   char* username;
   char* password;
   
   int* running;
};

// data for a single connection
struct syndicate_connection {
   struct syndicate_state* state;
};

int syndicate_init( char const* config_file, struct md_HTTP* http_server, int portnum, char const* ms_url, char const* volume_name, char const* volume_secret, char const* md_username, char const* md_password );
struct syndicate_state* syndicate_get_state();
struct md_syndicate_conf* syndicate_get_conf();
int syndicate_destroy();

#endif
