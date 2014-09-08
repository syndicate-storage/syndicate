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
int ms_client_verify_gateway_message( struct ms_client* client, uint64_t volume_id, uint64_t gateway_id, char const* msg, size_t msg_len, char* sigb64, size_t sigb64_len ) {
   ms_client_view_rlock( client );

   if( client->volume->volume_id != volume_id ) {
      // not from this volume
      errorf("Message from outside the Volume (%" PRIu64 ")\n", volume_id );
      ms_client_view_unlock( client );
      return -ENOENT;
   }
   
   // only UGs can send messages...
   ms_cert_bundle::iterator itr = client->volume->UG_certs->find( gateway_id );
   if( itr == client->volume->UG_certs->end() ) {
      // not found here--probably means we need to reload our certs
      
      dbprintf("WARN: No cached certificate for Gateway %" PRIu64 "\n", gateway_id );
      
      sem_post( &client->uploader_sem );
      ms_client_view_unlock( client );
      return -EAGAIN;
   }
   
   int rc = md_verify_signature( itr->second->pubkey, msg, msg_len, sigb64, sigb64_len );
   
   ms_client_view_unlock( client );
   
   return rc;
}


// get a copy of the RG URLs
char** ms_client_RG_urls( struct ms_client* client, char const* scheme ) {
   ms_client_view_rlock( client );

   char** urls = CALLOC_LIST( char*, client->volume->RG_certs->size() + 1 );
   int i = 0;
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      urls[i] = CALLOC_LIST( char, strlen(scheme) + strlen(rg_cert->hostname) + 1 + 7 + 1 + strlen(SYNDICATE_DATA_PREFIX) + 2 );
      sprintf( urls[i], "%s%s:%d/%s/", scheme, rg_cert->hostname, rg_cert->portnum, SYNDICATE_DATA_PREFIX );
      
      i++;
   }

   ms_client_view_unlock( client );

   return urls;
}

// get a list of RG ids
uint64_t* ms_client_RG_ids( struct ms_client* client ) {
   ms_client_view_rlock( client );
   
   uint64_t* ret = CALLOC_LIST( uint64_t, client->volume->RG_certs->size() + 1 );
   int i = 0;
   
   for( ms_cert_bundle::iterator itr = client->volume->RG_certs->begin(); itr != client->volume->RG_certs->end(); itr++ ) {
      struct ms_gateway_cert* rg_cert = itr->second;
      
      ret[i] = rg_cert->gateway_id;
      
      i++;
   }
   
   ms_client_view_unlock( client );
   return ret;
}


// get the type of gateway
int ms_client_get_gateway_type( struct ms_client* client, uint64_t g_id ) {
   ms_client_view_rlock( client );
   
   int ret = -ENOENT;
   
   if( client->volume->UG_certs->count( g_id ) != 0 )
      ret = SYNDICATE_UG;
   
   else if( client->volume->RG_certs->count( g_id ) != 0 )
      ret = SYNDICATE_RG;
   
   else if( client->volume->AG_certs->count( g_id ) != 0 ) 
      ret = SYNDICATE_AG;
   
   ms_client_view_unlock( client );
   return ret;
}

// get the name of the gateway
int ms_client_get_gateway_name( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, char** gateway_name ) {
   ms_client_view_rlock( client );
   
   int ret = 0;
   
   if( client->volume != NULL ) {
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
      if( itr != cert_bundles[ gateway_type ]->end() ) {
         *gateway_name = strdup( itr->second->name );
      }
      else {
         ret = -ENOENT;
      }
   }
   else {
      ret = -ENOTCONN;
   }
   
   ms_client_view_unlock( client );
   return ret;
}

// is this ID an AG ID?
bool ms_client_is_AG( struct ms_client* client, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   bool ret = false;
   
   if( client->volume->AG_certs->count( ag_id ) != 0 )
      ret = true;

   ms_client_view_unlock( client );

   return ret;
}


char* ms_client_get_AG_content_url( struct ms_client* client, uint64_t ag_id ) {
   ms_client_view_rlock( client );

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->AG_certs->find( ag_id );
   if( itr != client->volume->AG_certs->end() ) {
      ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
      sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   }

   ms_client_view_unlock( client );

   if( ret == NULL ) {
      errorf("No such AG %" PRIu64 "\n", ag_id );
   }

   return ret;
}


char* ms_client_get_RG_content_url( struct ms_client* client, uint64_t rg_id ) {
   ms_client_view_rlock( client );

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->RG_certs->find( rg_id );
   if( itr != client->volume->RG_certs->end() ) {
      ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
      sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   }

   ms_client_view_unlock( client );

   if( ret == NULL ) {
      errorf("No such RG %" PRIu64 "\n", rg_id );
   }

   return ret;
}

uint64_t ms_client_get_num_files( struct ms_client* client ) {
   ms_client_view_rlock( client );

   uint64_t num_files = client->volume->num_files;

   ms_client_view_unlock( client );

   return num_files;
}


// get a UG url
char* ms_client_get_UG_content_url( struct ms_client* client, uint64_t gateway_id ) {
   ms_client_view_rlock( client );

   // is this us?
   if( gateway_id == client->gateway_id ) {
      char* ret = strdup( client->conf->content_url );
      ms_client_view_unlock( client );
      return ret;
   }

   char* ret = NULL;

   ms_cert_bundle::iterator itr = client->volume->UG_certs->find( gateway_id );
   if( itr == client->volume->UG_certs->end() ) {
      errorf("No such Gateway %" PRIu64 "\n", gateway_id );
      ms_client_view_unlock( client );
      return NULL;
   }
   
   ret = CALLOC_LIST( char, strlen("http://") + strlen(itr->second->hostname) + 1 + 7 + 1 );
   sprintf( ret, "http://%s:%d/", itr->second->hostname, itr->second->portnum );
   
   ms_client_view_unlock( client );
   
   return ret;
}



// check a gateway's capabilities (as a bit mask)
// return 0 if all the capabilites are allowed, or -EPERM if at least one is not.
// return -EAGAIN if the gateway is not known.
int ms_client_check_gateway_caps( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps ) {
   
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      sem_post( &client->uploader_sem );
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   int ret = ((cert->caps & caps) == caps ? 0 : -EPERM);
   
   ms_client_view_unlock( client );
   
   return ret;
}


// get a gateway's user 
int ms_client_get_gateway_user( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id ) {
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      sem_post( &client->uploader_sem );
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   *user_id = cert->user_id;
   
   ms_client_view_unlock( client );
   
   return 0;
}


// get a gateway's volume
int ms_client_get_gateway_volume( struct ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t* user_id ) {
   if( gateway_type <= 0 || gateway_type >= MS_NUM_CERT_BUNDLES )
      return -EINVAL;
   
   ms_client_view_rlock( client );
   
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( client->volume, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ gateway_type ]->find( gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      // not found--need to reload certs?
      sem_post( &client->uploader_sem );
      ms_client_view_unlock( client );
      
      return -EAGAIN;
   }
   
   struct ms_gateway_cert* cert = itr->second;
   
   *user_id = cert->volume_id;
   
   ms_client_view_unlock( client );
   
   return 0;
}


// get a copy of the closure text for this gateway
int ms_client_get_closure_text( struct ms_client* client, char** closure_text, uint64_t* closure_len ) {
   // find my cert
   ms_client_view_rlock( client );
   
   struct ms_volume* vol = client->volume;
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   ms_cert_bundle::iterator itr = cert_bundles[ client->gateway_type ]->find( client->gateway_id );
   if( itr == cert_bundles[ client->gateway_type ]->end() ) {
      int rc = 0;
      
      // no certificate on file for this gateway.  It might be anonymous
      if( client->conf->is_client || client->gateway_id == GATEWAY_ANON ) {
         rc = -ENODATA;
      }
      else {
         rc = -ENOTCONN;
      }
      
      ms_client_view_unlock( client );
      return rc;
   }
   
   struct ms_gateway_cert* my_cert = itr->second;
   
   int ret = 0;
   
   if( my_cert->closure_text != NULL ) {
      *closure_text = CALLOC_LIST( char, my_cert->closure_text_len );
      memcpy( *closure_text, my_cert->closure_text, my_cert->closure_text_len );
      *closure_len = my_cert->closure_text_len;
   }
   else {
      ret = -ENOENT;
   }
   
   ms_client_view_unlock( client );
   
   return ret;
}


// get my private key as a PEM-encoded string
int ms_client_my_key_pem( struct ms_client* client, char** buf, size_t* len ) {
   ms_client_rlock( client );
   
   int rc = 0;
   
   if( client->my_key_pem != NULL ) {
      char* ret = strdup( client->my_key_pem );
      *buf = ret;
      *len = strlen(ret);
   }
   else {
      rc = -ENODATA;
   }
   
   ms_client_unlock( client );
   return rc;
}
