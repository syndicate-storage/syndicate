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
   
   dbprintf("Destroy Volume '%s'\n", vol->name );
   
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
      errorf("ms_client_download(%s) rc = %d\n", volume_url, rc );

      return rc;
   }

   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      errorf("Invalid data for Volume %s (missing %s)\n", volume_name, volume_md.InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   rc = ms_client_volume_init( vol, &volume_md, volume_pubkey_pem, client->conf, client->my_pubkey, client->my_key );
   if( rc != 0 ) {
      errorf("ms_client_volume_init rc = %d\n", rc );
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
   
   ms_client_view_rlock( client );
 
   struct ms_volume* vol = client->volume;
   
   if( vol == NULL ) {
      errorf("%s", "ERR: unbound from Volume!\n" );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   // get the Volume ID for later
   uint64_t volume_id = vol->volume_id;
   
   char* volume_url = ms_client_volume_url( client->url, vol->volume_id );

   ms_client_view_unlock( client );

   rc = ms_client_download( client, volume_url, &buf, &len );

   if( rc != 0 ) {
      errorf("ms_client_download(%s) rc = %d\n", volume_url, rc );

      free( volume_url );
   
      return rc;
   }

   free( volume_url );
   
   // extract the message
   bool valid = volume_md.ParseFromString( string(buf, len) );
   free( buf );
   
   if( !valid ) {
      errorf("Invalid data for Volume %" PRIu64 " (missing %s)\n", volume_id, volume_md.InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   ms_client_view_wlock( client );

   // re-find the Volume
   vol = client->volume;
   if( vol == NULL ) {
      errorf("%s", "ERR: unbound from Volume!" );
      ms_client_view_unlock( client );
      return -ENOENT;
   }

   uint64_t old_version = vol->volume_version;
   uint64_t old_cert_version = vol->volume_cert_version;
   
   // get the new versions, and make sure they've advanced.
   uint64_t new_version = volume_md.volume_version();
   uint64_t new_cert_version = volume_md.cert_version();
   
   if( new_version < old_version ) {
      errorf("Invalid volume version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_version, new_version );
      ms_client_view_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_cert_version < old_cert_version ) {
      errorf("Invalid certificate version (expected greater than %" PRIu64 ", got %" PRIu64 ")\n", old_cert_version, new_cert_version );
      ms_client_view_unlock( client );
      return -ENOTCONN;
   }
   
   if( new_version > old_version ) {
      // have new data--load it in
      rc = ms_client_volume_init( vol, &volume_md, NULL, client->conf, client->my_pubkey, client->my_key );
   }
   else {
      rc = 0;
   }
   
   ms_client_view_unlock( client );
   
   if( rc != 0 ) {
      errorf("ms_client_volume_init(%" PRIu64 ") rc = %d\n", volume_id, rc );
      return rc;
   }
   
   // do we need to download the UGs and/or RGs as well?
   dbprintf("Volume  version %" PRIu64 " --> %" PRIu64 "\n", old_version, new_version );
   dbprintf("Cert    version %" PRIu64 " --> %" PRIu64 "\n", old_cert_version, new_cert_version );

   // load new certificate information, if we have any
   if( new_cert_version > old_cert_version ) {
      rc = ms_client_reload_certs( client, new_cert_version );
      if( rc != 0 ) {
         errorf("ms_client_reload_certs rc = %d\n", rc );

         return rc;
      }
   }
   return 0;
}


// populate a Volume structure with the volume metadata
int ms_client_volume_init( struct ms_volume* vol, ms::ms_volume_metadata* volume_md, char const* volume_pubkey_pem, struct md_syndicate_conf* conf, EVP_PKEY* gateway_pubkey, EVP_PKEY* gateway_privkey ) {

   int rc = 0;
   
   // get the new public key
   if( vol->reload_volume_key || vol->volume_public_key == NULL || volume_pubkey_pem != NULL ) {
      vol->reload_volume_key = false;
      
      // trust it this time, but not in the future
      if( volume_pubkey_pem != NULL ) {
         rc = md_load_pubkey( &vol->volume_public_key, volume_pubkey_pem );
      }
      else {
         rc = md_load_pubkey( &vol->volume_public_key, volume_md->volume_public_key().c_str() );
      }
      
      if( rc != 0 ) {
         errorf("md_load_pubkey rc = %d\n", rc );
         return -ENOTCONN;
      }
   }

   if( vol->volume_public_key == NULL ) {
      errorf("%s", "unable to verify integrity of metadata for Volume!  No public key given!\n");
      return -ENOTCONN;
   }

   // verify metadata
   rc = md_verify<ms::ms_volume_metadata>( vol->volume_public_key, volume_md );
   if( rc != 0 ) {
      errorf("Signature verification failed on Volume %s (%" PRIu64 "), rc = %d\n", volume_md->name().c_str(), volume_md->volume_id(), rc );
      return rc;
   }

   // sanity check 
   if( vol->name ) {
      char* new_name = strdup( volume_md->name().c_str() );
      if( strcmp( new_name, vol->name ) != 0 ) {
         errorf("Invalid Volume metadata: tried to change name from '%s' to '%s'\n", vol->name, new_name );
         free( new_name );
         return -EINVAL;
      }
      free( new_name );
   }
   
   struct md_entry* root = NULL;
   
   if( volume_md->has_root() ) {
      root = CALLOC_LIST( struct md_entry, 1 );
      ms_entry_to_md_entry( volume_md->root(), root );
   }
   
   if( vol->root ) {
      md_entry_free( vol->root );
      free( vol->root );
   }
   
   vol->root = root;
   
   if( vol->UG_certs == NULL ) {
      vol->UG_certs = new ms_cert_bundle();
   }
   
   if( vol->RG_certs == NULL ) {
      vol->RG_certs = new ms_cert_bundle();
   }
   
   if( vol->AG_certs == NULL ) {
      vol->AG_certs = new ms_cert_bundle();
   }
   
   vol->volume_cert_version = volume_md->cert_version();
   vol->volume_id = volume_md->volume_id();
   vol->volume_owner_id = volume_md->owner_id();
   vol->blocksize = volume_md->blocksize();
   vol->volume_version = volume_md->volume_version();
   
   if( vol->name == NULL ) {
      vol->name = strdup( volume_md->name().c_str() );
   }
   
   vol->cache_closure = NULL;
   
   if( volume_md->has_cache_closure_text() ) {
      char const* method = NULL;
      
      if( vol->cache_closure == NULL ) {
         method = "md_closure_init";
         
         vol->cache_closure = CALLOC_LIST( struct md_closure, 1 );
         rc = md_closure_init( vol->cache_closure, conf, gateway_pubkey, gateway_privkey,
                               MS_CLIENT_CACHE_CLOSURE_PROTOTYPE,
                               volume_md->cache_closure_text().data(), volume_md->cache_closure_text().size(),
                               false, false );
      }
      else {
         method = "md_closure_reload";
         
         rc = md_closure_reload( vol->cache_closure, conf, gateway_pubkey, gateway_privkey, volume_md->cache_closure_text().data(), volume_md->cache_closure_text().size() );
      }
      
      if( rc != 0 ) {
         errorf("%s rc = %d\n", method, rc );
         return rc;
      }
      else {
         dbprintf("(Re)initialized CDN closure %p for Volume %s\n", vol->cache_closure, vol->name );
      }
   }
   else {
      errorf("WARN: no CDN closure for Volume %s\n", vol->name );
   }
   return 0;
}

