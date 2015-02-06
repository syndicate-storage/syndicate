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

// verify that a message came from a UG with the given ID (needed by libsyndicate python wrapper)
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
   
   // only UGs can send messages...
   ms_cert_bundle::iterator itr = client->volume->UG_certs->find( gateway_id );
   if( itr == client->volume->UG_certs->end() ) {
      // not found here--probably means we need to reload our certs
      
      SG_warn("No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      sem_post( &client->uploader_sem );
      ms_client_config_unlock( client );
      return -EAGAIN;
   }
   
   int rc = md_verify_signature( itr->second->pubkey, msg, msg_len, sigb64, sigb64_len );
   
   ms_client_config_unlock( client );
   
   return rc;
}


// get a copy of the RG URLs
// return a calloc'ed NULL-terminated list of null-terminated strings on success
// return NULL on error
char** ms_client_RG_urls( struct ms_client* client, char const* scheme ) {
   ms_client_config_rlock( client );
   
   int i = 0;
   char** urls = SG_CALLOC( char*, client->volume->RG_certs->size() + 1 );
   
   if( urls == NULL ) {
      
      ms_client_config_unlock( client );
      return NULL;
   }
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      urls[i] = SG_CALLOC( char, strlen(scheme) + 1 + strlen(rg_cert->hostname) + 7 + 1 + strlen(SG_DATA_PREFIX) + 2 );
      
      if( urls[i] == NULL ) {
         
         // out of memory
         SG_FREE_LIST( urls, SG_safe_free );
         ms_client_config_unlock( client );
         return NULL;
      }
      
      sprintf( urls[i], "%s%s:%d/%s/", scheme, rg_cert->hostname, rg_cert->portnum, SG_DATA_PREFIX );
      
      i++;
   }

   ms_client_config_unlock( client );

   return urls;
}

// get a list of RG ids
// return a calloc'ed list of IDs on success, terminated with 0
// return NULL on error
uint64_t* ms_client_RG_ids( struct ms_client* client ) {
   ms_client_config_rlock( client );
   
   int i = 0;
   uint64_t* ret = SG_CALLOC( uint64_t, client->volume->RG_certs->size() + 1 );
   
   if( ret == NULL ) {
      
      ms_client_config_unlock( client );
      return NULL;
   }
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      ret[i] = rg_cert->gateway_id;
      
      i++;
   }
   
   ms_client_config_unlock( client );
   return ret;
}


// get the type of gateway, given an id 
// return the type on success 
// return -ENOENT on error
int ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id ) {
   
   ms_client_config_rlock( client );
   
   int ret = -ENOENT;
   
   if( client->volume->UG_certs->count( g_id ) != 0 ) {
      ret = SYNDICATE_UG;
   }
   
   else if( client->volume->RG_certs->count( g_id ) != 0 ) {
      ret = SYNDICATE_RG;
   }
   
   else if( client->volume->AG_certs->count( g_id ) != 0 ) {
      ret = SYNDICATE_AG;
   }
   
   ms_client_config_unlock( client );
   return ret;
}

// get the name of the gateway
// return 0 on success
// return -ENOENT if no such gateway exists with the name 
// return -ENOTCONN if we aren't connected to a volume
// return -ENOMEM if we're out of memory
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, char** gateway_name ) {
   
   ms_client_config_rlock( client );
   
   int ret = 0;
   
   if( client->volume != NULL ) {
      
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
      if( itr != cert_bundles[ gateway_type ]->end() ) {
         
         *gateway_name = SG_strdup_or_null( itr->second->name );
         if( *gateway_name == NULL ) {
            ret = -ENOMEM;
         }
      }
      else {
         
         ret = -ENOENT;
      }
   }
   else {
      
      ret = -ENOTCONN;
   }
   
   ms_client_config_unlock( client );
   return ret;
}

// is this ID an AG ID?
// return True if so; False if not.
bool ms_client_is_AG( struct ms_client* client, uint64_t ag_id ) {
   
   ms_client_config_rlock( client );

   bool ret = false;
   
   if( client->volume->AG_certs->count( ag_id ) != 0 ) {
      ret = true;
   }

   ms_client_config_unlock( client );

   return ret;
}


// get a gateway's host URL 
// return the calloc'ed URL on success 
// return NULL on error 
char* ms_client_get_gateway_url( struct ms_client* client, int gateway_type, uint64_t gateway_id ) {
   
   char* ret = NULL;
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   
   if( !SG_VALID_GATEWAY_TYPE( gateway_type ) ) {
      return NULL;
   }
   
   ms_client_config_rlock( client );
   
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[gateway_type]->find( gateway_id );
   if( itr != cert_bundles[gateway_type]->end() ) {
      
      ret = SG_CALLOC( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
      if( ret == NULL ) {
         
         ms_client_config_unlock( client );
         return NULL;
      }
      
      sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   }

   ms_client_config_unlock( client );

   if( ret == NULL ) {
      SG_error("No such Gateway(type=%d) %" PRIu64 "\n", gateway_type, gateway_id );
   }

   return ret;
}

// get an AG's host URL 
// return the calloc'ed URL on success
// return NULL on error
char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t ag_id ) {
   return ms_client_get_gateway_url( client, SYNDICATE_AG, ag_id );
}


// get an RG's host URL 
// return the calloc'ed URL on success
// return NULL on error
char* ms_client_get_RG_content_url( struct ms_client* client, uint64_t rg_id ) {
   return ms_client_get_gateway_url( client, SYNDICATE_RG, rg_id );
}

// get a UG's host URL 
// return the calloc'ed URL on success
// return NULL on error
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t ug_id ) {
   return ms_client_get_gateway_url( client, SYNDICATE_UG, ug_id );
}


// get a gateway's certificate
// return a reference to the certificate on success
// return NULL otherwise.
// ms_client must be at least read-locked
static struct ms_gateway_cert* ms_client_get_gateway_cert( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id ) {
   
   struct ms_gateway_cert* cert = NULL;
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   
   if( !SG_VALID_GATEWAY_TYPE( gateway_type ) ) {
      return NULL;
   }
   
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      
      return NULL;
   }
   
   cert = itr->second;
   
   return cert;
}

// check a gateway's capabilities (as a bit mask)
// return 0 if all the capabilites are allowed.
// return -EINVAL on bad arguments
// return -EPERM if at least one is not.
// return -EAGAIN if the gateway is not known (this will start the reload process if so)
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps ) {
   
   struct ms_gateway_cert* cert = NULL;
   int ret = 0;
   
   if( !SG_VALID_GATEWAY_TYPE( gateway_type ) ) {
      return -EINVAL;
   }
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, gateway_type, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      ms_client_start_config_reload( client );
      
      return -EAGAIN;
   }
   
   ret = ((cert->caps & caps) == caps ? 0 : -EPERM);
   
   ms_client_config_unlock( client );
   
   return ret;
}


// get a gateway's user 
// return 0 on success, and set *user_id to the user ID 
// return -EAGAIN if the gateway is not known (this will start the reload process if so)
// return -EINVAL on bad arguments
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   if( !SG_VALID_GATEWAY_TYPE( gateway_type ) ) {
      return -EINVAL;
   }
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, gateway_type, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      ms_client_start_config_reload( client );
      
      return -EAGAIN;
   }
   
   *user_id = cert->user_id;
   
   ms_client_config_unlock( client );
   
   return 0;
}


// get a gateway's volume
// return 0 on success, and set *volume_id to the volume ID 
// return -EAGAIN if the gateway is not known (this wil start the reload process if so)
// return -EINVAL on bad arguments
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* volume_id ) {
   
   struct ms_gateway_cert* cert = NULL;
   
   if( !SG_VALID_GATEWAY_TYPE( gateway_type ) ) {
      return -EINVAL;
   }
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, gateway_type, gateway_id );
   if( cert == NULL ) {
      
      // not found--need to reload certs?
      ms_client_config_unlock( client );
      
      ms_client_start_config_reload( client );
      
      return -EAGAIN;
   }
   
   *volume_id = cert->volume_id;
   
   ms_client_config_unlock( client );
   
   return 0;
}


// get a copy of the closure text for this gateway
// return 0 on success, and set *closure_text and *closure_len accordingly 
// return -ENODATA if this is an anonymous gateway 
// return -ENOTCONN if we don't have the closure data yet 
// return -ENOENT if there is no closure for this gateway 
// return -ENOMEM if we're out of memory
int ms_client_get_closure_text( struct ms_client* client, char** closure_text, uint64_t* closure_len ) {
   
   struct ms_gateway_cert* cert = NULL;
   int rc = 0;
   
   ms_client_config_rlock( client );
   
   cert = ms_client_get_gateway_cert( client, client->gateway_type, client->gateway_id );
   if( cert == NULL ) {
      
      // no certificate on file for this gateway.  It might be anonymous
      if( client->conf->is_client || client->gateway_id == SG_GATEWAY_ANON ) {
         rc = -ENODATA;
      }
      else {
         rc = -ENOTCONN;
      }
      
      ms_client_config_unlock( client );
      return rc;
   }
   
   if( cert->closure_text != NULL ) {
      
      // duplicate it!
      *closure_text = SG_CALLOC( char, cert->closure_text_len );
      if( *closure_text == NULL ) {
         
         ms_client_config_unlock( client );
         return -ENOMEM;
      }
      
      memcpy( *closure_text, cert->closure_text, cert->closure_text_len );
      *closure_len = cert->closure_text_len;
   }
   else {
      rc = -ENOENT;
   }
   
   ms_client_config_unlock( client );
   
   return rc;
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
