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

#include "libsyndicate/ms/gateway.h"
#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/volume.h"

// sign an outbound message from us (needed by libsyndicate python wrapper)
// return 0 on success 
// return -ENOMEM on OOM
int ms_client_sign_gateway_message( struct ms_client* client, char const* data, size_t len, char** sigb64, size_t* sigb64_len ) {
    
    int rc = 0;
    ms_client_config_rlock( client );
    
    rc = md_sign_message( client->gateway_key, data, len, sigb64, sigb64_len );
    
    ms_client_config_unlock( client );
    
    return rc;
}

// verify that a message came from a peer with the given ID (needed by libsyndicate python wrapper)
// return 0 on success
// return -ENOENT if the volume_id does not match our volume_id
// return -EAGAIN if no certificate could be found for this gateway
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len ) {
   
   ms_client_config_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      SG_error("Message from outside the Volume (%" PRIu64 ")\n", volume_id );
      ms_client_config_unlock( client );
      return -ENOENT;
   }
   
   // only non-anonymous gateways can write
   ms_cert_bundle::iterator itr = client->certs->find( gateway_id );
   if( itr == client->certs->end() ) {
      
      // not found here--probably means we need to reload our certs
      SG_warn("No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      sem_post( &client->config_sem );
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   int rc = md_verify_signature( itr->second->pubkey, msg, msg_len, sigb64, sigb64_len );
   
   ms_client_config_unlock( client );
   
   return rc;
}

// get the type of gateway, given an id 
// return the type on success 
// return SG_INVALID_GATEWAY_ID on error
uint64_t ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id ) {
   
   ms_client_config_rlock( client );
   
   uint64_t ret = SG_INVALID_GATEWAY_ID;
   
   ms_cert_bundle::iterator itr = client->certs->find( g_id );
   if( itr != client->certs->end() ) {
      
      ret = itr->second->gateway_type;
   }
   
   ms_client_config_unlock( client );
   return ret;
}


// get the name of the gateway
// return 0 on success
// return -ENOTCONN if we aren't connected to a volume
// return -ENOMEM if we're out of memory
// return -EAGAIN if the gateway is not known to us, but could be if we reloaded the config
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_id, char** gateway_name ) {
   
   ms_client_config_rlock( client );
   
   int ret = 0;
   
   uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
   
   if( gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   // should return a non-null cert, since we know this gateway's type
   struct ms_gateway_cert* cert = ms_client_get_gateway_cert( client, gateway_id );
   
   if( cert != NULL ) {
      
      *gateway_name = SG_strdup_or_null( cert->name );
      if( *gateway_name == NULL ) {
         
         ret = -ENOMEM;
      }
   }
   else {
      
      ret = -ENOTCONN;
   }
   
   ms_client_config_unlock( client );
   return ret;
}

// get a gateway's host URL 
// return the calloc'ed URL on success 
// return NULL on error (i.e. gateway not known, is anonymous, or not found)
char* ms_client_get_gateway_url( struct ms_client* client, uint64_t gateway_id ) {
   
   char* ret = NULL;
   
   ms_client_config_rlock( client );
   
   uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
   
   if( gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      ms_client_config_unlock( client );
      return NULL;
   }
   
   struct ms_gateway_cert* cert = ms_client_get_gateway_cert( client, gateway_id );
   if( cert == NULL ) {
      
      ms_client_config_unlock( client );
      return NULL;
   }
   
   // found! 
   ret = SG_CALLOC( char, strlen("http://") + strlen(cert->hostname) + 1 + 7 + 1 );
   if( ret == NULL ) {
      
      ms_client_config_unlock( client );
      return NULL;
   }
   
   sprintf( ret, "http://%s:%d/", cert->hostname, cert->portnum );
   
   ms_client_config_unlock( client );
   
   return ret;
}

// check a gateway's capabilities (as a bit mask)
// return 0 if all the capabilites are allowed.
// return -EINVAL on bad arguments
// return -EPERM if at least one is not.
// return -EAGAIN if the gateway is not known, and the caller should reload
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_id, uint64_t caps ) {
   
   struct ms_gateway_cert* cert = NULL;
   int ret = 0;
   
   uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
   if( gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      return -EINVAL;
   }
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      return -EAGAIN;
   }
   
   ret = ((cert->caps & caps) == caps ? 0 : -EPERM);
   
   ms_client_config_unlock( client );
   
   return ret;
}


// get a gateway's user 
// return 0 on success, and set *user_id to the user ID 
// return -EAGAIN if the gateway is not known, and the caller should reload
// return -EINVAL on bad arguments
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_id, uint64_t* user_id ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   ms_client_config_rlock( client );
   
   uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
   if( gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   cert = ms_client_get_gateway_cert( client, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      return -EAGAIN;
   }
   
   *user_id = cert->user_id;
   
   ms_client_config_unlock( client );
   
   return 0;
}


// get a gateway's volume
// return 0 on success, and set *volume_id to the volume ID 
// return -EAGAIN if the gateway is not known, and the caller should reload
// return -EINVAL on bad arguments
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_id, uint64_t* volume_id ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   ms_client_config_rlock( client );
   
   uint64_t gateway_type = ms_client_get_gateway_type( client, gateway_id );
   if( gateway_type == SG_INVALID_GATEWAY_ID ) {
      
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   cert = ms_client_get_gateway_cert( client, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      return -EAGAIN;
   }
   
   *volume_id = cert->volume_id;
   
   ms_client_config_unlock( client );
   
   return 0;
}


// get the gateway's hash.  hash_buf should be at least SHA256_DIGEST_LEN bytes long
// return 0 on success
// return -EAGAIN if we have no certificate 
int ms_client_get_gateway_driver_hash( struct ms_client* client, uint64_t gateway_id, unsigned char* hash_buf ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, gateway_id );
   if( cert == NULL ) {
      
      // no cert 
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   memcpy( hash_buf, cert->driver_hash, cert->driver_hash_len );
   
   ms_client_config_unlock( client );
   
   return 0;
}


// get a copy of the gateway's driver text.
// return 0 on success 
// return -EAGAIN if cert is not on file, or there is (currently) no driver 
// return -ENOMEM on OOM
int ms_client_gateway_get_driver_text( struct ms_client* client, char** driver_text, size_t* driver_text_len ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, client->gateway_id );
   if( cert == NULL ) {
      
      // no cert for us
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   if( cert->driver_text == NULL ) {
      
      // no driver 
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   *driver_text = SG_CALLOC( char, cert->driver_text_len );
   if( *driver_text == NULL ) {
      
      // OOM 
      ms_client_config_unlock( client );
      return -ENOMEM;
   }
   
   memcpy( *driver_text, cert->driver_text, cert->driver_text_len );
   *driver_text_len = cert->driver_text_len; 
   ms_client_config_unlock( client );
   
   return 0;
}


// get my private key as a PEM-encoded string
// return 0 on success, and copy the PEM-encoded key into *buf and put its length in *len
// return -ENOMEM if we're out of memory 
// return -ENODATA if we have no public key
int ms_client_gateway_key_pem( struct ms_client* client, char** buf, size_t* len ) {
   
   int rc = 0;
   char* ret = NULL;
   
   ms_client_rlock( client );
   
   if( client->gateway_key_pem != NULL ) {
      
      ret = SG_strdup_or_null( client->gateway_key_pem );
      
      if( ret == NULL ) {
         rc = -ENOMEM;
      }
      else {
         *buf = ret;
         *len = strlen(ret);
      }
   }
   else {
      rc = -ENODATA;
   }
   
   ms_client_unlock( client );
   return rc;
}
