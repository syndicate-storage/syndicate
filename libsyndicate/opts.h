/*
   Copyright 2015 The Trustees of Princeton University

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

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/storage.h"

#include <getopt.h>

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 
#endif 

// command-line options
struct md_opts;

extern "C" {

struct md_opts* md_opts_new( int count );
int md_opts_default( struct md_opts* opts );
int md_opts_parse( struct md_opts* opts, int argc, char** argv, int* optind, char const* special_opts, int (*special_opt_handler)(int, char*) );
int md_opts_free( struct md_opts* opts );
void md_common_usage(void);

int md_opts_parse_long( int c, char* opt, long* result );

// getters
bool md_opts_get_client( struct md_opts* opts );
bool md_opts_get_ignore_driver( struct md_opts* opts );
uint64_t md_opts_get_gateway_type( struct md_opts* opts );
char const* md_opts_get_config_file( struct md_opts* opts );

// setters (e.g. for python)
void md_opts_set_client( struct md_opts* opts, bool client );
void md_opts_set_ignore_driver( struct md_opts* opts, bool ignore_driver );
void md_opts_set_gateway_type( struct md_opts* opts, uint64_t type );

void md_opts_set_config_file( struct md_opts* opts, char* config_filepath );
void md_opts_set_username( struct md_opts* opts, char* username );
void md_opts_set_volume_name( struct md_opts* opts, char* volume_name );
void md_opts_set_gateway_name( struct md_opts* opts, char* gateway_name );
void md_opts_set_ms_url( struct md_opts* opts, char* ms_url );
void md_opts_set_foreground( struct md_opts* opts, bool foreground );

void md_opts_set_driver_config( struct md_opts* opts, char const* driver_exec_str, char const** driver_roles, size_t num_driver_roles );

}

#endif
