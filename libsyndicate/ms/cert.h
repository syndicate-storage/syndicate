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

#include "libsyndicate/ms/core.h"

#define MS_NUM_CERT_BUNDLES     ms::ms_gateway_cert::NUM_CERT_TYPES

// prototypes 
struct ms_volume;

// version delta record between our Volume's cached cert bundle and the MS's
struct ms_cert_diff_entry {
   int gateway_type;
   uint64_t gateway_id;
   uint64_t cert_version;
};

typedef vector< ms_cert_diff_entry > ms_cert_diff_list;

// cert bundle delta
struct ms_cert_diff {
   ms_cert_diff_list* old_certs;
   ms_cert_diff_list* new_certs;
   
   ms_cert_diff() {
      this->old_certs = new ms_cert_diff_list();
      this->new_certs = new ms_cert_diff_list();
   }
   
   ~ms_cert_diff() {
      delete this->old_certs;
      delete this->new_certs;
   }
};

struct ms_gateway_cert {
   uint64_t user_id;            // SyndicateUser ID
   uint64_t gateway_id;         // Gateway ID
   int gateway_type;            // what kind of gateway
   uint64_t volume_id;          // Volume ID
   char* name;                  // account (gateway) name
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

// downloads 
int ms_client_gateway_cert_manifest_download( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, Serialization::ManifestMsg* mmsg );
int ms_client_gateway_cert_download( struct ms_client* client, CURL* curl, char const* url, ms::ms_gateway_cert* ms_cert );

// synchronization 
int ms_client_reload_certs( struct ms_client* client, uint64_t new_cert_bundle_version );

// helpers for synchronization 
int ms_client_make_cert_diff( struct ms_volume* vol, Serialization::ManifestMsg* mmsg, ms_cert_diff* certdiff );
int ms_client_find_expired_certs( struct ms_volume* vol, ms_cert_diff_list* expired );
int ms_client_revoke_certs( struct ms_volume* vol, ms_cert_diff_list* certdiff );

// validation
int ms_client_cert_has_public_key( const ms::ms_gateway_cert* ms_cert );

// misc 
void ms_client_cert_bundles( struct ms_volume* volume, ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1] );
int ms_client_cert_urls( char const* ms_url, uint64_t volume_id, uint64_t volume_cert_version, ms_cert_diff_list* new_certs, char*** cert_urls_buf );

}
#endif