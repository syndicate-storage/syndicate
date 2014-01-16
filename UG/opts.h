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
#include <getopt.h>

// command-line options
struct syndicate_opts {
   char* config_file;
   char* username;
   char* password;
   char* volume_name;
   char* ms_url;
   char* gateway_name;
   char* volume_pubkey_path;
   char* gateway_pkey_path;
   char* tls_pkey_path;
   char* tls_cert_path;
   char* CDN_prefix;
   char* proxy_url;
   char* storage_root;
   bool flush_replicas;
};

int syndicate_default_opts( struct syndicate_opts* opts );
int syndicate_parse_opts( struct syndicate_opts* opts, int argc, char** argv, int* optind, char const* special_opts, int (*special_opt_handler)(int, char*) );
void syndicate_common_usage( char* progname );

#endif