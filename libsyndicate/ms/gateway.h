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

#ifndef _MS_CLIENT_GATEWAY_H_
#define _MS_CLIENT_GATEWAY_H_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/volume.h"

extern "C" {

// peer signatures and verification 
int ms_client_sign_gateway_message( struct ms_client* client, char const* data, size_t len, char** sigb64, size_t* sigb64_len );
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len );

// get information about a specific gateway
uint64_t ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id );
uint64_t ms_client_get_gateway_id( struct ms_client* client );
uint64_t ms_client_get_owner_id( struct ms_client* client );
uint64_t ms_client_get_gateway_cert_version( struct ms_client* client, uint64_t g_id );
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_id, uint64_t* user_id );
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_id, uint64_t* volume_id );
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_id, char** gateway_name );
int ms_client_get_gateway_driver_hash( struct ms_client* client, uint64_t gateway_id, unsigned char* hash_buf );
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_id, uint64_t caps );

// get information about *this* gateway
int ms_client_gateway_key_pem( struct ms_client* client, char** buf, size_t* len );
int ms_client_gateway_get_driver_text( struct ms_client* client, char** driver_text, size_t* driver_text_len );

char* ms_client_get_gateway_url( struct ms_client* client, uint64_t gateway_id );

}

// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// Verify the authenticity of a gateway message, encoded as a protobuf (class T)
// return 0 if successfully verified 
// return -EINVAL if message came from outside the volume
// return -EAGAIN if we have no certificate for this gateway_id 
template< class T > int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, T* protobuf ) {
   
   int rc = 0;
   
   ms_client_config_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      SG_error("Message from outside Volume %" PRIu64 "\n", volume_id );
      ms_client_config_unlock( client );
      return -EINVAL;
   }
   
   if( gateway_id != 0 ) {
      
      // came from a gateway
      uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
      if( gateway_type == SG_INVALID_GATEWAY_ID ) {
         
         ms_client_config_unlock( client );
         return -EINVAL;
      }
      
      // look up the certificate bundle
      ms_cert_bundle* bundle = client->certs;
      
      // look up the cert
      ms_cert_bundle::iterator itr = bundle->find( gateway_id );
      if( itr == bundle->end() ) {
         // not found here--probably means we need to reload our certs
         
         SG_debug("WARN: No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
         
         // try reloading
         return -EAGAIN;
      }
      
      // verify the cert
      rc = md_verify< T >( itr->second->pubkey, protobuf );
   }
   else {
      
      // verify that this came from the MS
      rc = md_verify< T >( client->volume->volume_public_key, protobuf );
   }
   
   ms_client_config_unlock( client );
   
   return rc;
}


#endif 
