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

#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/url.h"

// free a volume
void ms_volume_free( struct ms_volume* vol ) {
   if( vol == NULL ) {
      return;
   }
   
   SG_debug("Destroy Volume '%s'\n", vol->name );
   
   if( vol->volume_public_key ) {
      EVP_PKEY_free( vol->volume_public_key );
      vol->volume_public_key = NULL;
   }
   
   ms_cert_bundle* all_certs[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, all_certs );

   for( int i = 1; all_certs[i] != NULL; i++ ) {
      ms_cert_bundle* certs = all_certs[i];
      
      for( ms_cert_bundle::iterator itr = certs->begin(); itr != certs->end(); itr++ ) {
         ms_client_gateway_cert_free( itr->second );
         free( itr->second );
      }
   }
   
   delete vol->UG_certs;
   delete vol->RG_certs;
   delete vol->AG_certs;
   
   vol->UG_certs = NULL;
   vol->RG_certs = NULL;
   vol->AG_certs = NULL;
   
   if( vol->name ) {
      free( vol->name );
      vol->name = NULL;
   }

   if( vol->root ) {
      md_entry_free( vol->root );
      free( vol->root );
   }
      
   if( vol->cache_closure ) {
      md_closure_shutdown( vol->cache_closure );
      free( vol->cache_closure );
   }

   memset( vol, 0, sizeof(struct ms_volume) );
}


// synchronously download a volume's metadata by name
// on success, populate the given ms_volume structure with the metadata
int ms_client_download_volume_by_name( struct ms_client* client, char const* volume_name, struct ms_volume* vol, char const* volume_pubkey_pem ) {
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   off_t len = 0;
   int rc = 0;

   char* volume_url = ms_client_volume_url_by_name( client->url, volume_name );

   rc = ms_client_download( client, volume_url, &buf, &len );

   free( volume_url );
   
   if( rc != 0 ) {
      SG_error("ms_client_download(%s) rc = %d\n", volume_url, rc );

      return rc;
   }

   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      SG_error("Invalid data for Volume %s (missing %s)\n", volume_name, volume_md.InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   rc = ms_client_volume_init( vol, &volume_md, volume_pubkey_pem, client->conf, client->my_pubkey, client->my_key );
   if( rc != 0 ) {
      SG_error("ms_client_volume_init rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// reload volume metadata
// * download new volume metadata, and load it in
// * if the cert version has changed, then reload the certs as well
// client must NOT be locked.
int ms_client_reload_volume( struct ms_client* client ) {
   
   int rc = 0;
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   off_t len = 0;
   uint64_t volume_id = 0;
   char* volume_url = NULL;
   
   ms_client_config_rlock( client );
 
   if( client->volume == NULL ) {
      
      SG_error("No volume attached to %p\n", client );
      
      ms_client_config_unlock( client );
      return -EINVAL;
   }

   // get the Volume ID for later
   volume_id = client->volume->volume_id;
   volume_url = ms_client_volume_url( client->url, volume_id );

   ms_client_config_unlock( client );

   rc = ms_client_download( client, volume_url, &buf, &len );

   if( rc != 0 ) {
      SG_error("ms_client_download(%s) rc = %d\n", volume_url, rc );

      free( volume_url );
   
      return rc;
   }

   free( volume_url );
   
   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      SG_error("Invalid data for Volume %" PRIu64 " (missing %s)\n", volume_id, volume_md.InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   ms_client_config_wlock( client );

   if( client->volume == NULL ) {
      ms_client_config_unlock( client );
      return -EINVAL;
   }

   uint64_t old_version = client->volume->volume_version;
   uint64_t old_cert_version = client->volume->volume_cert_version;
   
   // get the new versions, and make sure they've advanced.
   uint64_t new_version = volume_md.volume_version();
   uint64_t new_cert_version = volume_md.cert_version();
   
   if( new_version < old_version ) {
      SG_error("Invalid volume version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_version, new_version );
      ms_client_config_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_cert_version < old_cert_version ) {
      SG_error("Invalid certificate version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_cert_version, new_cert_version );
      ms_client_config_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_version > old_version ) {
      // have new data--load it in
      rc = ms_client_volume_init( client->volume, &volume_md, NULL, client->conf, client->my_pubkey, client->my_key );
   }
   else {
      rc = 0;
   }
   
   ms_client_config_unlock( client );
   
   if( rc != 0 ) {
      SG_error("ms_client_volume_init(%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // do we need to download the UGs and/or RGs as well?
   SG_debug("Volume  version %" PRIu64 " --> %" PRIu64 "\n", old_version, new_version );
   SG_debug("Cert    version %" PRIu64 " --> %" PRIu64 "\n", old_cert_version, new_cert_version );

   // load new certificate information, if we have any
   if( new_cert_version > old_cert_version ) {
      
      rc = ms_client_reload_certs( client, new_cert_version );
      if( rc != 0 ) {
         SG_error("ms_client_reload_certs rc = %d\n", rc );

         return rc;
      }
   }
   return 0;
}


// populate or reload a Volume structure with the volume metadata
// return 0 on success 
// return negative on error 
// if this fails, the volume should be unaffected
int ms_client_volume_init( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem, struct md_syndicate_conf* conf, EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_privkey ) {

   int rc = 0;
   struct md_entry* root = vol->root;
   char* new_name = vol->name;
   
   EVP_PKEY* volume_public_key = vol->volume_public_key;
   bool reload_volume_key = vol->reload_volume_key;
   
   ms_cert_bundle* UG_certs = vol->UG_certs;
   ms_cert_bundle* RG_certs = vol->RG_certs;
   ms_cert_bundle* AG_certs = vol->AG_certs;
   
   struct md_closure* cache_closure = vol->cache_closure;
   
   // get the new public key
   if( reload_volume_key || volume_public_key == NULL || volume_pubkey_pem != NULL ) {
      reload_volume_key = false;
      
      // trust it this time, but not in the future
      if( volume_pubkey_pem != NULL ) {
         rc = md_load_pubkey( &volume_public_key, volume_pubkey_pem );
      }
      else {
         rc = md_load_pubkey( &volume_public_key, volume_md->volume_public_key().c_str() );
      }
      
      if( rc != 0 ) {
         SG_error("md_load_pubkey rc = %d\n", rc );
         return -ENOTCONN;
      }
   }

   if( volume_public_key == NULL ) {
      SG_error("%s", "unable to verify integrity of metadata for Volume!  No public key given!\n");
      return -ENOTCONN;
   }

   // verify metadata
   rc = md_verify<ms::ms_volume_metadata>( volume_public_key, volume_md );
   if( rc != 0 ) {
      SG_error("Signature verification failed on Volume %s (%" PRIu64 "), rc = %d\n", volume_md->name().c_str(), volume_md->volume_id(), rc );
      
      if( volume_public_key != vol->volume_public_key ) {
         EVP_PKEY_free( volume_public_key );
      }
      
      return rc;
   }

   // set name
   new_name = SG_strdup_or_null( volume_md->name().c_str() );
   
   // set root
   if( volume_md->has_root() ) {
      
      root = SG_CALLOC( struct md_entry, 1 );
      
      if( root != NULL ) {
         
         rc = ms_entry_to_md_entry( volume_md->root(), root );
         if( rc != 0 ) {
            
            md_entry_free( root );
            SG_safe_free( root );
            
            root = NULL;
         }
      }
   }
   
   if( UG_certs == NULL ) {
      UG_certs = SG_safe_new( ms_cert_bundle() );
   }
   
   if( RG_certs == NULL ) {
      RG_certs = SG_safe_new( ms_cert_bundle() );
   }
   
   if( AG_certs == NULL ) {
      AG_certs = SG_safe_new( ms_cert_bundle() );
   }
   
   
   if( volume_md->has_cache_closure_text() ) {
      
      char const* method = NULL;
      
      if( cache_closure == NULL ) {
         method = "md_closure_init";
         
         cache_closure = SG_CALLOC( struct md_closure, 1 );
         if( cache_closure == NULL ) {
            
            // roll back 
            return -ENOMEM;
         }
         
         rc = md_closure_init( cache_closure, conf, gateway_pubkey, gateway_privkey,
                               MS_CLIENT_CACHE_CLOSURE_PROTOTYPE,
                               volume_md->cache_closure_text().data(), volume_md->cache_closure_text().size(),
                               false, false );
      }
      else {
         method = "md_closure_reload";
         
         rc = md_closure_reload( cache_closure, conf, gateway_pubkey, gateway_privkey, volume_md->cache_closure_text().data(), volume_md->cache_closure_text().size() );
      }
      
      if( rc != 0 ) {
         
         SG_error("%s rc = %d\n", method, rc );
      }
      else {
         
         SG_debug("(Re)initialized CDN closure %p for Volume %s\n", cache_closure, vol->name );
      }
   }
   else {
      
      SG_warn("no CDN closure for Volume %s\n", vol->name );
   }
   
   // did anything fail?
   if( rc != 0 || UG_certs == NULL || RG_certs == NULL || AG_certs == NULL || root == NULL || new_name == NULL ) {
      
      if( UG_certs != vol->UG_certs ) {
         SG_safe_delete( UG_certs );
      }
      
      if( RG_certs != vol->RG_certs ) {
         SG_safe_delete( RG_certs );
      }
      
      if( AG_certs != vol->AG_certs ) {
         SG_safe_free( AG_certs );
      }
      
      if( root != vol->root && root != NULL ) {
         md_entry_free( root );
         SG_safe_free( root );
      }
      
      if( new_name != vol->name ) {
         SG_safe_free( new_name );
      }
      
      if( volume_public_key != vol->volume_public_key ) {
         EVP_PKEY_free( volume_public_key );
      }
      
      if( rc == 0 ) {
         rc = -ENOMEM;
      }
      
      return rc;
   }
   
   else {
      // make all changes take effect
      vol->volume_cert_version = volume_md->cert_version();
      vol->volume_id = volume_md->volume_id();
      vol->volume_owner_id = volume_md->owner_id();
      vol->blocksize = volume_md->blocksize();
      vol->volume_version = volume_md->volume_version();
      vol->volume_public_key = volume_public_key;
      vol->reload_volume_key = reload_volume_key;
      vol->cache_closure = cache_closure;
      vol->UG_certs = UG_certs;
      vol->RG_certs = RG_certs;
      vol->AG_certs = AG_certs;
      
      if( root != vol->root && vol->root != NULL ) {
         md_entry_free( vol->root );
         SG_safe_free( vol->root );
      }
      vol->root = root;
      
      if( vol->name != new_name ) {
         SG_safe_free( vol->name );
      }
      vol->name = new_name;  
   }
   
   SG_debug("Reload volume '%s' (%" PRIu64 ")\n", vol->name, vol->volume_id );
   return 0;
}

