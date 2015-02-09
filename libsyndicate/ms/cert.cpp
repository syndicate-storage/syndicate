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

// generate a cert bundle from a volume's certificates
void ms_client_cert_bundles( struct ms_volume* volume, ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1] ) {
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   memset( cert_bundles, 0, sizeof(cert_bundles[0]) * (MS_NUM_CERT_BUNDLES + 1) );
   cert_bundles[SYNDICATE_UG] = volume->UG_certs;
   cert_bundles[SYNDICATE_AG] = volume->AG_certs;
   cert_bundles[SYNDICATE_RG] = volume->RG_certs;
   return;
}  


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

// synchronously download a cert bundle manfest
// be sure to request our certificate
int ms_client_gateway_cert_manifest_download( struct ms_client* client, uint64_t volume_id, uint64_t volume_cert_version, Serialization::ManifestMsg* mmsg ) {
   
   char* url = ms_client_cert_manifest_url( client->url, volume_id, volume_cert_version, client->gateway_id );
   
   int rc = md_download_manifest( client->conf, &client->dl, url, client->volume->cache_closure, ms_client_connect_cache_impl, client->conf, mmsg, NULL, NULL );
   
   if( rc != 0 ) {
      SG_error("md_download_manifest(%s) rc = %d\n", url, rc );
      free( url );
      return rc;
   }
   
   free( url );
   return rc;
}


// initialize a cert diff structure 
// return 0 on success 
// return -ENOMEM if out of memory 
static int ms_client_cert_diff_init( struct ms_cert_diff* certdiff ) {
   
   memset( certdiff, 0, sizeof(struct ms_cert_diff) );
   
   certdiff->old_certs = SG_safe_new( ms_cert_diff_list() );
   certdiff->new_certs = SG_safe_new( ms_cert_diff_list() );
   
   if( certdiff->old_certs == NULL || certdiff->new_certs == NULL ) {
      
      SG_safe_delete( certdiff->old_certs );
      SG_safe_delete( certdiff->new_certs );
      
      return -ENOMEM;
   }
   
   return 0;
}

// free a cert diff structure 
// return 0 on success 
static int ms_client_cert_diff_free( struct ms_cert_diff* certdiff ) {
   
   SG_safe_delete( certdiff->old_certs );
   SG_safe_delete( certdiff->new_certs );
   
   memset( certdiff, 0, sizeof(struct ms_cert_diff) );
   
   return 0;
}


// calculate which certs are new, and which are stale, given a manifest of them.
// If we're a UG or RG, then only process certs for writer UGs and our own cert
// If we're an AG, only process our own cert
// client must be read-locked at least 
// return 0 on success
// return -EINVAL if the message contained invalid data
int ms_client_make_cert_diff( struct ms_volume* vol, Serialization::ManifestMsg* mmsg, ms_cert_diff* certdiff ) {
   
   set< uint64_t > present;
   uint64_t gateway_id = 0;
   uint64_t gateway_type = 0;
   uint64_t cert_version = 0;
   
   // sanity check 
   for( int64_t i = 0; i < mmsg->size(); i++ ) {
      
      const Serialization::BlockURLSetMsg& cert_block = mmsg->block_url_set(i);
      
      // start_id is the gateway type for the cert bundle manifest 
      if( !SG_VALID_GATEWAY_TYPE( cert_block.start_id() ) ) {
         SG_error("Invalid gateway type %" PRIu64 "\n", gateway_type );
         return -EINVAL;
      }
   }
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   // find new certs...
   for( int64_t i = 0; i < mmsg->size(); i++ ) {
      
      const Serialization::BlockURLSetMsg& cert_block = mmsg->block_url_set(i);
      
      // extract gateway metadata, according to serialization.proto
      gateway_id = cert_block.gateway_id();
      gateway_type = cert_block.start_id();
      cert_version = cert_block.block_versions(0);
      
      ms_cert_bundle* existing_bundle = cert_bundles[gateway_type];
      
      ms_cert_bundle::iterator itr = existing_bundle->find( gateway_id );
      if( itr != existing_bundle->end() ) {
         // found!
         // need to reload it?
         if( itr->second->version < cert_version ) {
            // new certificate for this gateway!
            struct ms_cert_diff_entry diffent;
            
            diffent.gateway_type = gateway_type;
            diffent.gateway_id = gateway_id;
            diffent.cert_version = cert_version;
            
            SG_debug("new cert: (gateway_type=%" PRIu64 ", gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type, gateway_id, cert_version );
            
            certdiff->new_certs->push_back( diffent );
         }
      }
      else {
         // certificate exists remotely but not locally
         struct ms_cert_diff_entry diffent;
         
         diffent.gateway_type = gateway_type;
         diffent.gateway_id = gateway_id;
         diffent.cert_version = cert_version;
         
         SG_debug("new cert: (gateway_type=%" PRId64 ", gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type, gateway_id, cert_version );
         
         certdiff->new_certs->push_back( diffent );
      }
      
      present.insert( gateway_id );
   }
   
   // find old certs...
   for( int i = 0; cert_bundles[i] != NULL; i++ ) {
      
      ms_cert_bundle* cert_bundle = cert_bundles[i];
      
      for( ms_cert_bundle::iterator itr = cert_bundle->begin(); itr != cert_bundle->end(); itr++ ) {
         if( present.count( itr->first ) == 0 ) {
            
            // absent
            struct ms_cert_diff_entry diffent;
         
            diffent.gateway_type = itr->second->gateway_type;
            diffent.gateway_id = itr->second->gateway_id;
            diffent.cert_version = itr->second->version;
            
            SG_debug("old cert: (gateway_type=%" PRId64 ", gateway_id=%" PRIu64 ", cert_version=%" PRIu64 ")\n", gateway_type, diffent.gateway_id, diffent.cert_version );
            
            certdiff->old_certs->push_back( diffent );
         }
      }
   }
   
   return 0;
}


// synchronously download a certificate, using our cache driver
// return 0 on success
// return -EINVAL if the cert was malformed 
// return negative on download error
int ms_client_gateway_cert_download( struct ms_client* client, char const* url, ms::ms_gateway_cert* ms_cert ) {
   
   char* buf = NULL;
   ssize_t buf_len = 0;
   int http_status = 0;
   bool valid = false;
   
   int rc = md_download( client->conf, &client->dl, url, MS_MAX_CERT_SIZE, client->volume->cache_closure, ms_client_connect_cache_impl, client->conf, &http_status, &buf, &buf_len );
   
   if( rc != 0 ) {
      SG_error("md_download('%s') rc = %d\n", url, rc );
      return rc;
   }
   
   // parse the cert...
   try {
      valid = ms_cert->ParseFromString( string(buf, buf_len) );
   }
   catch( exception e ) {
      SG_error("Invalid certificate '%s'\n", url );
      return -EINVAL;
   }
   
   rc = 0;
   
   if( !valid ) {
      SG_error("Invalid certificate '%s' (missing %s)\n", url, ms_cert->InitializationErrorString().c_str());
      rc = -EINVAL;
   }
   
   free( buf );
   
   return rc;
}


// given a cert diff list, revoke the contained certificates
int ms_client_revoke_certs( struct ms_volume* vol, ms_cert_diff_list* certdiff ) {
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   for( unsigned int i = 0; i < certdiff->size(); i++ ) {
      struct ms_cert_diff_entry* diffent = &(certdiff->at(i));
      
      ms_cert_bundle::iterator itr = cert_bundles[diffent->gateway_type]->find( diffent->gateway_type );
      if( itr != cert_bundles[diffent->gateway_type]->end() ) {
         
         // revoke!
         ms_client_gateway_cert_free( itr->second );
         SG_safe_free( itr->second );
         cert_bundles[diffent->gateway_type]->erase( itr );
      }
      else {
         SG_warn("No certificate for gateway %" PRIu64 " (type %d)\n", diffent->gateway_id, diffent->gateway_type );
      }
   }
   
   return 0;
}


// find all expired certs, and put them into the expired cert diff list.
// return 0 on success
int ms_client_find_expired_certs( struct ms_volume* vol, ms_cert_diff_list* expired ) {
   
   // NOTE: this is indexed to SYNDICATE_UG, SYNDICATE_AG, SYNDICATE_RG
   ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
   ms_client_cert_bundles( vol, cert_bundles );
   
   for( unsigned int i = 0; cert_bundles[i] != NULL; i++ ) {
      ms_cert_bundle* cert_bundle = cert_bundles[i];
      
      for( ms_cert_bundle::iterator itr = cert_bundle->begin(); itr != cert_bundle->end(); itr++ ) {
         
         struct ms_gateway_cert* cert = itr->second;
         int64_t now = md_current_time_seconds();
         
         if( now < 0 ) {
            // couldn't read the clock 
            return now;
         }
         
         if( cert->expires > 0 && cert->expires < (uint64_t)now ) {
            SG_debug("Certificate for Gateway %" PRIu64 " (type %d) expired at %" PRId64 "\n", cert->gateway_id, cert->gateway_type, cert->expires );
            
            struct ms_cert_diff_entry diffent;
            
            diffent.gateway_type = cert->gateway_type;
            diffent.gateway_id = cert->gateway_id;
            diffent.cert_version = cert->version;
            
            expired->push_back( diffent );
         }
      }
   }
   
   return 0;
}


// given a cert diff, calculate the set of certificate URLs
// return 0 on success
// return negative on error
int ms_client_cert_urls( char const* ms_url, uint64_t volume_id, uint64_t volume_cert_version, ms_cert_diff_list* new_certs, char*** cert_urls_buf ) {
   
   vector<char*> cert_urls;
   char** ret = NULL;
   struct ms_cert_diff_entry* diffent = NULL;
   char* url = NULL;
   
   for( unsigned int i = 0; i < new_certs->size(); i++ ) {
      
      diffent = &new_certs->at(i);
      
      url = ms_client_cert_url( ms_url, volume_id, volume_cert_version, diffent->gateway_type, diffent->gateway_id, diffent->cert_version );
      
      cert_urls.push_back( url );
   }
   
   ret = SG_CALLOC( char*, cert_urls.size() + 1 );
   if( ret == NULL ) {
      
      for( unsigned int i = 0; i < cert_urls.size(); i++ ) {
         SG_safe_free( cert_urls[i] );
      }
      
      return -ENOMEM;
   }
   
   for( unsigned int i = 0; i < cert_urls.size(); i++ ) {
      ret[i] = cert_urls[i];
   }
   
   *cert_urls_buf = ret;
   return 0;
}


// reload a client's certificates
// * download the manifest, then download the certificates.
// * calculate the difference between the client's current certificates and the new ones.
// * expire the old ones
// * trust the new ones
// TODO: fetch certs in parallel
int ms_client_reload_certs( struct ms_client* client, uint64_t new_cert_bundle_version ) {
   
   uint64_t volume_id = 0;
   uint64_t volume_cert_version = 0;
   int rc = 0;
   Serialization::ManifestMsg mmsg;
   struct ms_cert_diff certdiff;
   uint64_t my_gateway_id = 0;
   
   rc = ms_client_cert_diff_init( &certdiff );
   if( rc != 0 ) {
      return rc;
   }
   
   ms_client_config_rlock( client );
   
   volume_id = client->volume->volume_id;
  
   if( (signed)new_cert_bundle_version == -1 ) {
      // get from loaded volume; i.e. on initialization
      volume_cert_version = client->volume->volume_cert_version;
   }
   else {
      volume_cert_version = new_cert_bundle_version;
   }
   
   ms_client_config_unlock( client );
   
   // get the certificate manifest...
   rc = ms_client_gateway_cert_manifest_download( client, volume_id, volume_cert_version, &mmsg );
   if( rc != 0 ) {
      
      SG_error("ms_client_gateway_cert_manifest_download(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      
      ms_client_cert_diff_free( &certdiff );
      return rc;
   }
   
   SG_debug("Got cert manifest with %" PRIu64 " certificates\n", mmsg.size() );
   
   // lock Volume data to calculate the certs we need...
   ms_client_config_wlock( client );
   
   // get the old and new certs...
   rc = ms_client_make_cert_diff( client->volume, &mmsg, &certdiff );
   if( rc != 0 ) {
      
      ms_client_config_unlock( client );
      
      SG_error("ms_client_make_cert_diff(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      
      ms_client_cert_diff_free( &certdiff );
      return rc;
   }
   
   // revoke old certs
   rc = ms_client_revoke_certs( client->volume, certdiff.old_certs );
   if( rc != 0 ) { 
      
      ms_client_config_unlock( client );
      
      SG_error("ms_client_revoke_certs(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      
      ms_client_cert_diff_free( &certdiff );
      return rc;
   }
   
   // get the URLs for the new certs...
   char** cert_urls = NULL;
   rc = ms_client_cert_urls( client->url, volume_id, volume_cert_version, certdiff.new_certs, &cert_urls );
   if( rc != 0 ) {
      
      ms_client_config_unlock( client );
      
      SG_error("ms_client_cert_urls(volume=%" PRIu64 ") rc = %d\n", volume_id, rc );
      
      ms_client_cert_diff_free( &certdiff );
      return rc;
   }
   
   // unlock Volume data, so we can download without locking the view-change threads
   ms_client_config_unlock( client );
   
   // done with the cert diff 
   ms_client_cert_diff_free( &certdiff );
   
   // what's our gateway id?
   ms_client_rlock( client );
   my_gateway_id = client->gateway_id;
   ms_client_unlock( client );
   
   // go get each certificate
   for( int i = 0; cert_urls[i] != NULL; i++ ) {
      
      ms::ms_gateway_cert ms_cert;
      struct ms_gateway_cert* new_cert = NULL;
      
      SG_debug("Get certificate %s\n", cert_urls[i] );
      
      rc = ms_client_gateway_cert_download( client, cert_urls[i], &ms_cert );
      if( rc != 0 ) {
         
         SG_error("ms_client_gateway_cert_download(%s) rc = %d\n", cert_urls[i], rc );
         continue;
      }
      
      // lock Volume data...
      ms_client_config_wlock( client );
      
      if( client->volume->volume_cert_version > volume_cert_version ) {
         // moved on
         volume_cert_version = client->volume->volume_cert_version;
         
         ms_client_config_unlock( client );
         
         SG_error("Volume cert version %" PRIu64 " is too old (expected greater than %" PRIu64 ")\n", volume_cert_version, client->volume->volume_cert_version );
         
         SG_FREE_LIST( cert_urls, free );
         return 0;
      }
      
      // advance the volume cert version
      client->volume->volume_cert_version = volume_cert_version;
      
      // check signature with Volume public key
      rc = md_verify< ms::ms_gateway_cert >( client->volume->volume_public_key, &ms_cert );
      if( rc != 0 ) {
         ms_client_config_unlock( client );
         
         SG_error("Signature verification failed for certificate '%s'\n", cert_urls[i] );
         continue;
      }
      
      // next cert
      new_cert = SG_CALLOC( struct ms_gateway_cert, 1 );
      
      if( new_cert == NULL ) {
         // out of memory 
         ms_client_config_unlock( client );
         
         SG_FREE_LIST( cert_urls, free );
         return -ENOMEM;
      }
      
      // load!
      rc = ms_client_gateway_cert_init( new_cert, my_gateway_id, &ms_cert );
      if( rc != 0 ) {
         ms_client_config_unlock( client );
         
         SG_error("ms_client_gateway_cert_init(%s) rc = %d\n", cert_urls[i], rc );
         free( new_cert );
         continue;
      }
      
      // load this cert in, if it is newer
      // clear the old one, if needed.
      ms_cert_bundle* cert_bundles[MS_NUM_CERT_BUNDLES+1];
      ms_client_cert_bundles( client->volume, cert_bundles );
      
      ms_cert_bundle::iterator itr = cert_bundles[ new_cert->gateway_type ]->find( new_cert->gateway_id );
      if( itr != cert_bundles[ new_cert->gateway_type ]->end() ) {
         
         // verify that this certificate is newer (otherwise reject it)
         struct ms_gateway_cert* old_cert = itr->second;
         
         if( old_cert->version >= new_cert->version ) {
            
            if( old_cert->version > new_cert->version ) {
               // tried to load an old cert
               SG_warn("Downloaded certificate for Gateway %s (ID %" PRIu64 ") with old version %" PRIu64 "; expected greater than %" PRIu64 "\n", old_cert->name, old_cert->gateway_id, new_cert->version, old_cert->version);
            }
            
            ms_client_gateway_cert_free( new_cert );
            free( new_cert );
         }
         else {
            // old cert--revoke
            ms_client_gateway_cert_free( itr->second );
            free( itr->second );
            cert_bundles[ new_cert->gateway_type ]->erase( itr );
         }
      }
      
      SG_debug("Trusting new certificate for Gateway %s (ID %" PRIu64 ")\n", new_cert->name, new_cert->gateway_id);
      
      (*cert_bundles[ new_cert->gateway_type ])[ new_cert->gateway_id ] = new_cert;
      
      ms_client_config_unlock( client );
   }
   
   SG_FREE_LIST( cert_urls, free );
   
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
      
      SG_error("Invalid gateway type %d\n", cert->gateway_type );
      
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
      
      SG_debug("Loaded cert (user_id=%" PRIu64 ", gateway_type=%d, gateway_id=%" PRIu64 ", gateway_name=%s, hostname=%s, portnum=%d, version=%" PRIu64 ", caps=%" PRIX64 ")\n",
               cert->user_id, cert->gateway_type, cert->gateway_id, cert->name, cert->hostname, cert->portnum, cert->version, cert->caps );
   }
   
   return rc;
}
