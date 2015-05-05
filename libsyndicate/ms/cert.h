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

#ifndef _MS_CLIENT_CERT_H_
#define _MS_CLIENT_CERT_H_

#include "libsyndicate/libsyndicate.h"

#define MS_CERT_BUNDLE_BEGIN    1
#define MS_NUM_CERT_BUNDLES     ms::ms_gateway_cert::NUM_CERT_TYPES

#define SG_MAX_CERT_LEN         10240000                // 10MB

// prototypes 
struct ms_volume;

struct ms_gateway_cert {
   uint64_t user_id;            // Syndicate User ID
   uint64_t gateway_id;         // Gateway ID
   uint64_t gateway_type;       // what kind of gateway
   uint64_t volume_id;          // Volume ID
   char* name;                  // gateway name
   char* hostname;              // what host this gateway runs on
   int portnum;                 // what port this gateway listens on
   char* closure_text;          // closure information (only retained for our gateway)
   uint64_t closure_text_len;   // length of the above
   EVP_PKEY* pubkey;            // gateway public key
   EVP_PKEY* privkey;           // decrypted from MS (only retained for our gateway)
   uint64_t caps;               // gateway capabilities
   uint64_t expires;            // when this certificate expires
   uint64_t version;            // version of this certificate (increases monotonically)
};

typedef map<uint64_t, struct ms_gateway_cert*> ms_cert_bundle;

extern "C" {

// init/free
int ms_client_gateway_cert_init( struct ms_gateway_cert* cert, uint64_t my_gateway_id, const ms::ms_gateway_cert* ms_cert );
void ms_client_gateway_cert_free( struct ms_gateway_cert* cert );
void ms_client_cert_bundle_free( ms_cert_bundle* bundle );

// helpers for synchronization 
int ms_client_revoke_certs( struct ms_client* client, struct SG_manifest* manifest );

// validation
int ms_client_cert_has_public_key( const ms::ms_gateway_cert* ms_cert );


}
#endif