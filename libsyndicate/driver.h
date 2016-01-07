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

#ifndef _SG_DRIVER_H_
#define _SG_DRIVER_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/proc.h"
#include "libsyndicate/libjson-compat.h"

typedef map<string, string> SG_driver_conf_t;
typedef SG_driver_conf_t SG_driver_secrets_t;
typedef map<string, struct SG_proc_group*> SG_driver_proc_group_t;

struct SG_driver;

extern "C" {

// alloc 
struct SG_driver* SG_driver_alloc(void);

// locking...
int SG_driver_rlock( struct SG_driver* driver );
int SG_driver_wlock( struct SG_driver* driver );
int SG_driver_unlock( struct SG_driver* driver );

// initialization, reload, and shutdown 
int SG_driver_init( struct SG_driver* driver, struct md_syndicate_conf* conf,
                    EVP_PKEY* pubkey, EVP_PKEY* privkey,
                    char const* exec_str, char** const roles, size_t num_roles, int num_instances,
                    char const* driver_text, size_t driver_text_len);

int SG_driver_procs_start( struct SG_driver* driver );
int SG_driver_procs_stop( struct SG_driver* driver );
int SG_driver_reload( struct SG_driver* driver, EVP_PKEY* pubkey, EVP_PKEY* privkey, char const* driver_text, size_t driver_text_len );
int SG_driver_shutdown( struct SG_driver* driver );

// get fields
int SG_driver_load_binary_field( char* json_str, size_t json_str_len, char const* field_name, char** field_value, size_t* field_value_len );

// driver config API 
int SG_driver_get_config( struct SG_driver* driver, char const* key, char** value, size_t* len );
int SG_driver_get_secret( struct SG_driver* driver, char const* key, char** value, size_t* len );
int SG_driver_get_string( char const* driver_text, size_t driver_text_len, char const* key, char** value, size_t* value_len );
int SG_driver_get_chunk( char const* driver_text, size_t driver_text_len, char const* key, struct SG_chunk* chunk ); 
int SG_driver_decrypt_secrets( EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_pkey, char** ret_obj_json, size_t* ret_obj_json_len, char const* driver_secrets_b64, size_t driver_secrets_b64_len );

// communication 
char* SG_driver_reqdat_to_path( struct SG_request_data* reqdat );

// getters 
struct SG_proc_group* SG_driver_get_proc_group( struct SG_driver* driver, char const* proc_group_name );

}

#endif
