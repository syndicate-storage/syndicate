/*
   Copyright 2014 The Trustees of Princeton University

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

#ifndef _MS_CLIENT_VOLUME_H_
#define _MS_CLIENT_VOLUME_H_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/cert.h"

// Volume data
struct ms_volume {
   uint64_t volume_id;           // ID of this Volume
   uint64_t volume_owner_id;     // UID of the User that owns this Volume
   uint64_t blocksize;           // blocksize of this Volume
   char* name;                   // name of the volume
   
   EVP_PKEY* volume_public_key;  // Volume public key 
   
   uint64_t volume_version;      // version of the above information
   
   ms::ms_volume_metadata* volume_md;   // the signed cert for the above
};

extern "C" {
   
int ms_client_volume_init( struct ms_volume* vol, ms::ms_volume_metadata* volume_md );
void ms_client_volume_free( struct ms_volume* vol );

// download 
// int ms_client_download_volume_by_name( struct ms_client* client, char const* volume_name, struct ms_volume* vol, char const* volume_pubkey_pem );

// consistency 
// int ms_client_reload_volume( struct ms_client* client, uint64_t* ret_new_cert_version );

}

#endif