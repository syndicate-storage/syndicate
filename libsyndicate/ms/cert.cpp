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

#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/url.h"

#include "libsyndicate/download.h"
#include "libsyndicate/client.h"

// free a cert
void ms_client_gateway_cert_free( struct ms_gateway_cert* cert ) {
   
   SG_safe_free( cert->hostname );
   SG_safe_free( cert->name );
   SG_safe_free( cert->closure_text );
   
   if( cert->pubkey != NULL ) {
      EVP_PKEY_free( cert->pubkey );
      cert->pubkey = NULL;
   }
}


// free a cert bundle 
void ms_client_cert_bundle_free( ms_cert_bundle* bundle ) {
   
   for( ms_cert_bundle::iterator itr = bundle->begin(); itr != bundle->end(); itr++ ) {
      
      ms_client_gateway_cert_free( itr->second );
      SG_safe_free( itr->second );
   }
   
   bundle->clear();
}


// go through the volume's certificates and revoke the ones that are *not* represneted by the certificate manifest, or have expired, or are stale
// return 0 on success (always succeeds)
int ms_client_revoke_certs( struct ms_client* client, struct SG_manifest* manifest ) {
   
   uint64_t now_s = md_current_time_seconds();
   int rc = 0;
   int64_t version = 0;
   
   ms_client_config_wlock( client );
   
   for( ms_cert_bundle::iterator itr = client->certs->begin(); itr != client->certs->end(); ) {
      
      // expired?
      if( itr->second->expires > 0 && itr->second->expires < now_s ) {
         
         SG_debug("Revoke certificate for %" PRIu64 ": expired at %" PRIu64 " (it is now %" PRIu64 ")\n", itr->second->gateway_id, itr->second->expires, now_s );
         
         ms_client_gateway_cert_free( itr->second );
         SG_safe_free( itr->second );
         
         // blow it away 
         ms_cert_bundle::iterator old_itr = itr;
         itr++;
         
         client->certs->erase( old_itr );
         continue;
      }
      
      // not present?
      rc = SG_manifest_get_block_version( manifest, itr->second->gateway_id, &version );
      if( rc != 0 ) {
         
         if( rc == -ENOENT ) {
            
            // nope 
            SG_debug("Revoke certificate for %" PRIu64 ": it was removed from the volume\n", itr->second->gateway_id );
            
            ms_client_gateway_cert_free( itr->second );
            SG_safe_free( itr->second );
            
            // blow it away 
            ms_cert_bundle::iterator old_itr = itr;
            itr++;
            
            client->certs->erase( old_itr );
            continue;
         }
         else {
            
            // some other error?
            SG_error("SG_manifest_get_block_version( %" PRIu64 " ) rc = %d\n", itr->second->gateway_id, rc );
            
            itr++;
            continue;
         }
      }
      
      // old version?
      if( itr->second->version < (uint64_t)version ) {
         
         // old version 
         SG_debug("Revoke certificate for %" PRIu64 ": it is stale (local=%" PRIu64 ", current=%" PRIu64 ")\n", itr->second->gateway_id, itr->second->version, (uint64_t)version );
         
         ms_client_gateway_cert_free( itr->second );
         SG_safe_free( itr->second );
         
         // blow it away
         ms_cert_bundle::iterator old_itr = itr;
         itr++;
         
         client->certs->erase( old_itr );
         continue;
      }
      
      itr++;
   }
   
   ms_client_config_unlock( client );
   return 0;
}


// does a certificate have a public key set?
int ms_client_cert_has_public_key( const ms::ms_gateway_cert* ms_cert ) {
   return (strcmp( ms_cert->public_key().c_str(), "NONE" ) != 0);
}


// (re)load a gateway certificate.
// If my_gateway_id matches the ID in the cert, then load the closure as well (since we'll need it)
// client cannot be write-locked! (but volume/view data can be)
int ms_client_gateway_cert_init( struct ms_gateway_cert* cert, uint64_t my_gateway_id, const ms::ms_gateway_cert* ms_cert ) {
   
   int rc = 0;
   
   cert->name = strdup( ms_cert->name().c_str() );
   cert->hostname = strdup( ms_cert->host().c_str() );
   
   if( cert->name == NULL || cert->hostname == NULL ) {
      // OOM
      SG_safe_free( cert->name );
      SG_safe_free( cert->hostname );
      return -ENOMEM;
   }
   
   cert->user_id = ms_cert->owner_id();
   cert->gateway_id = ms_cert->gateway_id();
   cert->gateway_type = ms_cert->gateway_type();
   cert->portnum = ms_cert->port();
   cert->version = ms_cert->version();
   cert->caps = ms_cert->caps();
   cert->volume_id = ms_cert->volume_id();
   
   // NOTE: closure information is base64-encoded
   // only store the closure if its for us
   if( my_gateway_id == cert->gateway_id && ms_cert->closure_text().size() > 0 ) {
      
      cert->closure_text_len = ms_cert->closure_text().size();
      cert->closure_text = SG_CALLOC( char, cert->closure_text_len + 1 );
      
      if( cert->closure_text == NULL ) {
         // OOM
         SG_safe_free( cert->name );
         SG_safe_free( cert->hostname );
         return -ENOMEM;
      }
      
      memcpy( cert->closure_text, ms_cert->closure_text().c_str(), cert->closure_text_len );
   }
   else {
      
      cert->closure_text = NULL;
      cert->closure_text_len = 0;
   }
   
   // validate... 
   if( !SG_VALID_GATEWAY_TYPE( cert->gateway_type ) ) {
      
      SG_error("Invalid gateway type %" PRIu64 "\n", cert->gateway_type );
      
      ms_client_gateway_cert_free( cert );
      return -EINVAL;
   }

   if( !ms_client_cert_has_public_key( ms_cert ) ) {
      
      // no public key for this gateway on the MS
      SG_warn("No public key for Gateway %s\n", cert->name );
      cert->pubkey = NULL;
   }
   else {
      
      rc = md_load_pubkey( &cert->pubkey, ms_cert->public_key().c_str() );
      if( rc != 0 ) {
         SG_error("md_load_pubkey(Gateway %s) rc = %d\n", cert->name, rc );
      }
   }
   
   if( rc == 0 ) {
      
      SG_debug("Loaded cert (user_id=%" PRIu64 ", gateway_type=%" PRIu64 ", gateway_id=%" PRIu64 ", gateway_name=%s, hostname=%s, portnum=%d, version=%" PRIu64 ", caps=%" PRIX64 ")\n",
               cert->user_id, cert->gateway_type, cert->gateway_id, cert->name, cert->hostname, cert->portnum, cert->version, cert->caps );
   }
   
   return rc;
}


