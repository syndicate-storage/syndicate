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
   bool reload_volume_key;       // do we reload this public key if we get it from the MS?  Or do we trust the one given locally?
   
   ms_cert_bundle* UG_certs;    // UGs in this Volume
   ms_cert_bundle* RG_certs;    // RGs in this Volume
   ms_cert_bundle* AG_certs;    // AGs in this Volume
   
   int num_UG_certs;
   int num_RG_certs;
   int num_AG_certs;

   uint64_t volume_version;      // version of the above information
   uint64_t volume_cert_version;        // version of the cert bundle 
   
   struct md_entry* root;        // serialized root fs_entry
   
   uint64_t num_files;           // number of files in this Volume

   struct md_closure* cache_closure;    // closure for connecting to the cache providers
};

extern "C" {
   
int ms_client_volume_init( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem, struct md_syndicate_conf* conf, EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_privkey );
void ms_volume_free( struct ms_volume* vol );

// download 
int ms_client_download_volume_by_name( struct ms_client* client, char const* volume_name, struct ms_volume* vol, char const* volume_pubkey_pem );

// consistency 
int ms_client_reload_volume( struct ms_client* client );

// get information about the volume
uint64_t ms_client_volume_version( struct ms_client* client );
uint64_t ms_client_cert_version( struct ms_client* client );
uint64_t ms_client_get_volume_id( struct ms_client* client );
uint64_t ms_client_get_volume_blocksize( struct ms_client* client );
char* ms_client_get_volume_name( struct ms_client* client );
uint64_t ms_client_get_num_files( struct ms_client* client );
int ms_client_get_volume_root( struct ms_client* client, struct md_entry* root );

}


// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// Verify the authenticity of a gateway message, encoded as a protobuf (class T)
template< class T > int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_type, uint64_t gateway_id, T* protobuf ) {
   ms_client_view_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      SG_error("Message from outside Volume %" PRIu64 "\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // look up the certificate bundle
   ms_cert_bundle* bundle = NULL;
   
   if( gateway_type == SYNDICATE_UG ) {
      bundle = client->volume->UG_certs;
   }
   else if( gateway_type == SYNDICATE_RG ) {
      bundle = client->volume->RG_certs;
   }
   else if( gateway_type == SYNDICATE_AG ) {
      bundle = client->volume->AG_certs;
   }
   else {
      SG_error("Invalid Gateway type %" PRIu64 "\n", gateway_type );
      ms_client_view_unlock( client );
      return -EINVAL;
   }
   
   // look up the cert
   ms_cert_bundle::iterator itr = bundle->find( gateway_id );
   if( itr == bundle->end() ) {
      // not found here--probably means we need to reload our certs
      
      SG_debug("WARN: No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      // try reloading
      sem_post( &client->uploader_sem );
      ms_client_view_unlock( client );
      return -EAGAIN;
   }
   
   // verify the cert
   int rc = md_verify< T >( itr->second->pubkey, protobuf );
   
   ms_client_view_unlock( client );
   
   return rc;
}

#endif