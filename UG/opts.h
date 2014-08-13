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

#ifndef _SYNDICATE_OPTS_H_
#define _SYNDICATE_OPTS_H_

#include "syndicate.h"
#include "cache.h"

#include <getopt.h>

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 
#endif 

#include <wordexp.h>

#define SYNDICATE_OPTS_STDIN_MAX 65536 

// command-line options
struct syndicate_opts {
   char* config_file;
   char* username;
   char* volume_name;
   char* ms_url;
   char* gateway_name;
   char* volume_pubkey_path;
   char* gateway_pkey_path;
   char* syndicate_pubkey_path;
   char* hostname;
   
   struct mlock_buf password;
   struct mlock_buf user_pkey_pem;
   struct mlock_buf gateway_pkey_pem;   // alternative to gateway_pkey_path
   struct mlock_buf gateway_pkey_decryption_password;
   
   char* volume_pubkey_pem;     // alternative to volume_pubkey_path
   char* syndicate_pubkey_pem;  // altenrative to syndicate_pubkey_path 
   char* tls_pkey_path;
   char* tls_cert_path;
   char* storage_root;
   char* mountpoint;             // UG only; first non-optarg
   bool flush_replicas;
   bool read_stdin;     // if true, get arguments from stdin (i.e. to avoid them showing up in /proc/self/cmdline)
   int debug_level;     // if 0, no debugging.  if 1, set global debug.  If 2, then set global and locking debug
   size_t cache_soft_limit;
   size_t cache_hard_limit;
   
   bool anonymous;
};

extern "C" {

int syndicate_default_opts( struct syndicate_opts* opts );
int syndicate_parse_opts( struct syndicate_opts* opts, int argc, char** argv, int* optind, char const* special_opts, int (*special_opt_handler)(int, char*) );
int syndicate_cleanup_opts( struct syndicate_opts* opts );
void syndicate_common_usage( char* progname );

}

#endif