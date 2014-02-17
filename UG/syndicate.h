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

// core state and control for Syndicate

#ifndef _SYNDICATE_H_
#define _SYNDICATE_H_

#include "libsyndicate/libsyndicate.h"
#include "cache.h"
#include "stats.h"
#include "replication.h"
#include "fs.h"
#include "state.h"

int syndicate_init( char const* config_file,
                    char const* ms_url,
                    char const* volume_name,
                    char const* gateway_name,
                    char const* md_username,
                    char const* md_password,
                    char const* volume_pubkey_file,
                    char const* my_key_file,
                    char const* my_key_password,
                    char const* tls_key_file,
                    char const* tls_cert_file );

void syndicate_finish_init( struct syndicate_state* state );

struct syndicate_state* syndicate_get_state();
struct md_syndicate_conf* syndicate_get_conf();

int syndicate_destroy( int wait_replicas );

#endif
