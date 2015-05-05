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
   
   SG_safe_free( vol->name );
   
   if( vol->root != NULL ) {
      md_entry_free( vol->root );
      SG_safe_free( vol->root );
   }

   memset( vol, 0, sizeof(struct ms_volume) );
}


// synchronously download a volume's metadata by name
// on success, populate the given ms_volume structure with the metadata
// return 0 on success
// return -EINVAL if we failed to unserialize 
// return -errno if we failed to download data, or failed to initialize the volume
int ms_client_download_volume_by_name( struct ms_client* client, char const* volume_name, struct ms_volume* vol, char const* volume_pubkey_pem ) {
   
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   off_t len = 0;
   int rc = 0;
   char* volume_url = ms_client_volume_url_by_name( client->url, volume_name );

   rc = ms_client_download( client, volume_url, &buf, &len );

   SG_safe_free( volume_url );
   
   if( rc != 0 ) {
      SG_error("ms_client_download(%s) rc = %d\n", volume_url, rc );

      return rc;
   }
   
   rc = md_parse< ms::ms_volume_metadata >( &volume_md, buf, len );
   SG_safe_free( buf );
   
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc );
      
      return rc;
   }
   
   rc = ms_client_volume_init( vol, &volume_md, volume_pubkey_pem, client->conf, client->gateway_pubkey, client->gateway_key );
   if( rc != 0 ) {
      
      SG_error("ms_client_volume_init rc = %d\n", rc );
      return rc;
   }
   
   return 0;
}


// reload volume metadata: download new volume metadata, and load it in
// return 0 on success, and set *new_cert_version to the volume's new certificate bundle version 
// return -EINVAL if there is no volume to reload in the client
// return -ENOMEM if OOM 
// return -EBADMSG if we can't parse the message, or if we get invalid versions
// client must NOT be locked.
int ms_client_reload_volume( struct ms_client* client, uint64_t* ret_new_cert_version ) {
   
   int rc = 0;
   ms::ms_volume_metadata volume_md;
   char* buf = NULL;
   off_t len = 0;
   uint64_t volume_id = 0;
   char* volume_url = NULL;
   
   uint64_t old_version = 0;
   uint64_t new_version = 0;
   
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
   
   if( volume_url == NULL ) {
      return -ENOMEM;
   }

   rc = ms_client_download( client, volume_url, &buf, &len );

   if( rc != 0 ) {
      SG_error("ms_client_download(%s) rc = %d\n", volume_url, rc );

      SG_safe_free( volume_url );
   
      return rc;
   }

   SG_safe_free( volume_url );
   
   // extract
   rc = md_parse< ms::ms_volume_metadata >( &volume_md, buf, len );
   
   SG_safe_free( buf );
   
   if( rc != 0 ) {
      
      SG_error("md_parse rc = %d\n", rc);
      
      if( rc == -EINVAL ) {
         // failed to parse 
         return -EBADMSG;
      }
      
      return rc;
   }
   
   ms_client_config_wlock( client );

   if( client->volume == NULL ) {
      // somehow this got freed out from under us?
      SG_error("No volume attached to %p\n", client );
      ms_client_config_unlock( client );
      return -EINVAL;
   }

   old_version = client->volume->volume_version;
   new_version = volume_md.volume_version();
   
   // version must advance
   if( new_version < old_version ) {
      
      SG_error("Invalid volume version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_version, new_version );
      ms_client_config_unlock( client );
      return -EBADMSG;
   }
   
   if( new_version > old_version ) {
      // have new data--load it in
      rc = ms_client_volume_init( client->volume, &volume_md, NULL, client->conf, client->gateway_pubkey, client->gateway_key );
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
   
   *ret_new_cert_version = volume_md.cert_version();
   
   return 0;
}


// populate or reload a Volume structure with the volume metadata
// return 0 on success 
// return -ENODATA if we can't load the volume public key 
// return -ENOMEM if OOM 
// return -EINVAL if we can't verify the volume metadata
// if this fails, the volume should be unaffected
int ms_client_volume_init( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem, struct md_syndicate_conf* conf, EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_privkey ) {

   int rc = 0;
   struct md_entry* root = vol->root;
   char* new_name = vol->name;
   
   EVP_PKEY* volume_public_key = vol->volume_public_key;
   bool reload_volume_key = vol->reload_volume_key;
   
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
         return -ENODATA;
      }
   }

   if( volume_public_key == NULL ) {
      SG_error("%s", "unable to verify integrity of metadata for Volume!  No public key given!\n");
      return -ENODATA;
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
   
   // did anything fail?
   if( rc != 0 || root == NULL || new_name == NULL ) {
      
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
      vol->volume_id = volume_md->volume_id();
      vol->volume_owner_id = volume_md->owner_id();
      vol->blocksize = volume_md->blocksize();
      vol->volume_version = volume_md->volume_version();
      vol->volume_public_key = volume_public_key;
      vol->reload_volume_key = reload_volume_key;
      
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
