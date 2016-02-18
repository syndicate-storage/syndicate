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
   SG_safe_free( cert->driver_hash );
   SG_safe_free( cert->driver_text );
   SG_safe_delete( cert->pb );
   SG_safe_delete( cert->user_pb );
   
   if( cert->pubkey != NULL ) {
      EVP_PKEY_free( cert->pubkey );
      cert->pubkey = NULL;
   }
}


// free a cert bundle 
void ms_client_cert_bundle_free( ms_cert_bundle* bundle ) {
   
   for( ms_cert_bundle::iterator itr = bundle->begin(); itr != bundle->end(); itr++ ) {
      
      if( itr->second != NULL ) {
         ms_client_gateway_cert_free( itr->second );
         SG_safe_free( itr->second );
      }
   }
   
   bundle->clear();
}


// does a certificate have a public key set?
int ms_client_cert_has_public_key( ms::ms_gateway_cert* ms_cert ) {
   return (strcmp( ms_cert->public_key().c_str(), "NONE" ) != 0);
}


// initialize a gateway certificate.
// NOTE cert takes ownership of ms_cert 
// return 0 on success 
// return -ENOMEM on OOM 
// return -EINVAL on invalid
int ms_client_gateway_cert_init( struct ms_gateway_cert* cert, uint64_t my_gateway_id, ms::ms_gateway_cert* ms_cert ) {
   
   int rc = 0;
   
   // sanity check
   if( my_gateway_id == cert->gateway_id && ms_cert->driver_hash().size() > 0 ) {
       
      if( ms_cert->driver_hash().size() != SHA256_DIGEST_LENGTH ) {
         SG_error("Invalid driver hash length: expected %d, got %zu\n", SHA256_DIGEST_LENGTH, ms_cert->driver_hash().size() );
         return -EINVAL;
      }
   }
   
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
   cert->driver_text = NULL;
   cert->driver_text_len = 0;
   cert->pb = ms_cert;
   
   // store *our* driver hash 
   if( my_gateway_id == cert->gateway_id && ms_cert->driver_hash().size() > 0 ) {
      
      cert->driver_hash_len = ms_cert->driver_hash().size();
      cert->driver_hash = SG_CALLOC( unsigned char, cert->driver_hash_len );
      
      if( cert->driver_hash == NULL ) {
         // OOM
         SG_safe_free( cert->name );
         SG_safe_free( cert->hostname );
         return -ENOMEM;
      }
      
      memcpy( cert->driver_hash, ms_cert->driver_hash().data(), cert->driver_hash_len );
   }
   else {
      
      cert->driver_hash = NULL;
      cert->driver_hash_len = 0;
   }
   
   if( !ms_client_cert_has_public_key( ms_cert ) ) {
      
      // no public key for this gateway on the MS
      SG_warn("No public key for Gateway %s\n", cert->name );
      cert->pubkey = NULL;
   }
   else {
      
      rc = md_load_pubkey( &cert->pubkey, ms_cert->public_key().c_str(), ms_cert->public_key().size() );
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


// get the cert version 
uint64_t ms_client_gateway_cert_version( struct ms_gateway_cert* cert ) {
   return cert->version;
}


// get the user cert 
ms::ms_user_cert* ms_client_gateway_cert_user( struct ms_gateway_cert* cert ) {
   return cert->user_pb;
}


// get the gateway cert pb 
ms::ms_gateway_cert* ms_client_gateway_cert_gateway( struct ms_gateway_cert* cert ) {
   return cert->pb;
}

// get gateway name (ref)
char const* ms_client_gateway_cert_name( struct ms_gateway_cert* cert ) {
   return cert->name;
}

// get gateway pubkey (ref)
EVP_PKEY* ms_client_gateway_pubkey( struct ms_gateway_cert* cert ) {
   return cert->pubkey;
}

// copy out driver hash (should be at least SHA256_DIGEST_LEN bytes)
int ms_client_gateway_driver_hash_buf( struct ms_gateway_cert* cert, unsigned char* hash_buf ) {
   if( cert->driver_hash == NULL ) {
      return -ENOENT;
   }

   memcpy( hash_buf, cert->driver_hash, cert->driver_hash_len );
   return 0;
}


// get cert hostname
char const* ms_client_gateway_cert_hostname( struct ms_gateway_cert* cert ) {
   return cert->hostname;
}

// get cert portnum
int ms_client_gateway_cert_portnum( struct ms_gateway_cert* cert ) {
   return cert->portnum;
}


// add a user protobuf 
// NOTE: no authenticity check will be performed; this just sets the field.
// return 0 on success
int ms_client_gateway_cert_set_user( struct ms_gateway_cert* cert, ms::ms_user_cert* user_pb ) {
   
   cert->user_pb = user_pb;
   return 0;
}

// set the cert's driver 
// NOTE: no consistency check will be performed against the hash; this just sets the field.
// NOTE: the gateway cert takes ownership of the driver; the driver text must be malloc'ed or otherwise not go out of scope.
// return 0 on success 
int ms_client_gateway_cert_set_driver( struct ms_gateway_cert* cert, char* driver_text, uint64_t driver_text_len ) {
   
   if( cert->driver_text != NULL ) {
      SG_safe_free( cert->driver_text );
   }
   
   cert->driver_text = driver_text;
   cert->driver_text_len = driver_text_len;
   return 0;
}


// set the cert driver hash 
// NOTE: no consistency check will be performed against the driver text; this just sets the field
// NOTE: the gateway cert takes ownership of the hash; the driver hash must be malloc'ed or otherwise not go out of scope.
// return 0 on success 
int ms_client_gateway_cert_set_driver_hash( struct ms_gateway_cert* cert, unsigned char* driver_hash, size_t driver_hash_len ) {
   
   if( cert->driver_hash != NULL ) {
      SG_safe_free( cert->driver_hash );
   }
   
   cert->driver_hash = driver_hash;
   cert->driver_hash_len = driver_hash_len;
   return 0;
}

// put a cert into a cert bundle 
// return 0 on success 
// return -ENOMEM on OOM 
int ms_client_cert_bundle_put( ms_cert_bundle* bundle, struct ms_gateway_cert* cert ) {
   try {
      (*bundle)[ cert->gateway_id ] = cert;
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }
   
   return 0;
}

// get user id 
uint64_t ms_client_gateway_cert_user_id( struct ms_gateway_cert* cert ) {
   return cert->user_id;
}

// get gateway type 
uint64_t ms_client_gateway_cert_gateway_type( struct ms_gateway_cert* cert ) {
   return cert->gateway_type;
}

// get gateway id 
uint64_t ms_client_gateway_cert_gateway_id( struct ms_gateway_cert* cert ) {
   return cert->gateway_id;
}

// get volume id 
uint64_t ms_client_gateway_cert_volume_id( struct ms_gateway_cert* cert ) {
   return cert->volume_id;
}

