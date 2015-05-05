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

#include "libsyndicate/ms/register.h"
#include "libsyndicate/ms/cert.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/url.h"
#include "libsyndicate/ms/openid.h"
#include "libsyndicate/download.h"

// unseal and load our private key from registration metadata
// return 0 on success, and set *ret_pkey, *ret_pubkey, *ret_pkey_pem, *ret_pkey_len, and *ret_mlocked
// return -ENOTCONN if no password is given, or if we failed to decode or decrypt the key
// return -ENODATA if we failed to load the decrypted key
// return -ENOMEM if we run out of memory
static int ms_client_unseal_and_load_keys( ms::ms_registration_metadata* registration_md, char const* key_password, EVP_PKEY** ret_pkey, EVP_PKEY** ret_pubkey, char** ret_pkey_pem, size_t* ret_pkey_len, bool* ret_mlocked ) {
   
   int rc = 0;
   int decode_rc = 0;
   
   char const* encrypted_gateway_private_key_b64 = NULL;
   size_t encrypted_gateway_private_key_b64_len = 0;
   
   char* encrypted_gateway_private_key = NULL;
   size_t encrypted_gateway_private_key_len = 0;
   
   char* gateway_private_key_str = NULL;
   size_t gateway_private_key_str_len = 0;
   
   EVP_PKEY* pkey = NULL;
   EVP_PKEY* pubkey = NULL;
   
   if( key_password == NULL ) {
      
      SG_error("%s\n", "No private key loaded, but no password to decrypt one with.");
      return -ENOTCONN;
   }
   
   // base64-encoded encrypted private key
   encrypted_gateway_private_key_b64 = registration_md->encrypted_gateway_private_key().c_str();
   encrypted_gateway_private_key_b64_len = registration_md->encrypted_gateway_private_key().size();
   
   decode_rc = md_base64_decode( encrypted_gateway_private_key_b64, encrypted_gateway_private_key_b64_len, &encrypted_gateway_private_key, &encrypted_gateway_private_key_len );
   if( decode_rc != 0 ) {
      
      SG_error("md_base64_decode() rc = %d\n", decode_rc );
      return decode_rc;
   }
   
   // NOTE: will be mlock'ed
   SG_debug("%s\n", "Unsealing gateway private key...");
   
   decode_rc = md_password_unseal_mlocked( encrypted_gateway_private_key, encrypted_gateway_private_key_len, key_password, strlen(key_password), &gateway_private_key_str, &gateway_private_key_str_len );
   
   memset( encrypted_gateway_private_key, 0, encrypted_gateway_private_key_len );
   SG_safe_free( encrypted_gateway_private_key );
   
   if( decode_rc != 0 ) {
      
      SG_error("md_password_unseal_mlocked() rc = %d\n", decode_rc );
      return -ENOTCONN;
   }

   // validate and import it
   decode_rc = md_load_public_and_private_keys( &pubkey, &pkey, gateway_private_key_str );
   if( decode_rc != 0 ) {
      
      SG_error("md_load_privkey rc = %d\n", decode_rc );
      rc = -ENODATA;
      
      memset( gateway_private_key_str, 0, gateway_private_key_str_len );
      munlock( gateway_private_key_str, gateway_private_key_str_len );
      SG_safe_free( gateway_private_key_str );
      
      return rc;
   }  
   
   // verify structure
   decode_rc = ms_client_verify_key( pkey );
   if( decode_rc != 0 ) {
      
      SG_error("ms_client_verify_key rc = %d\n", decode_rc );
      rc = -ENODATA;
      
      memset( gateway_private_key_str, 0, gateway_private_key_str_len );
      munlock( gateway_private_key_str, gateway_private_key_str_len );
      SG_safe_free( gateway_private_key_str );
      
      return rc;
   }
   
   // we're good!  install them
   *ret_pkey = pkey;
   *ret_pubkey = pubkey;
   *ret_pkey_pem = gateway_private_key_str;
   *ret_pkey_len = gateway_private_key_str_len;
   *ret_mlocked = true;
   
   return rc;
}
   

// load a registration message and populate the session-related and key-related fields in the client
// return 0 on success;
// return -ENOTCONN if the hostname does not match the MS's record (this check is ignored if compiled with -D_FIREWALL)
// return -EBADMSG if the message itself contains expired or invalid information 
// return -ENOMEM if we're out of memroy 
// return other negative if the volume or certificate fails to load
int ms_client_load_registration_metadata( struct ms_client* client, ms::ms_registration_metadata* registration_md, char const* volume_pubkey_pem, char const* key_password ) {

   int rc = 0;
   char gateway_type_str[5];
   char gateway_id_str[50];
   
   char const* new_session_password = NULL;
   char* new_userpass = NULL;
   
   EVP_PKEY* new_pkey = NULL;
   EVP_PKEY* new_pubkey = NULL;
   char* new_pkey_pem = NULL;
   size_t new_pkey_len = 0;
   bool new_pkey_mlocked = false;
   
   struct ms_volume* volume = NULL;
   ms::ms_volume_metadata* vol_md = NULL;
   const ms::ms_gateway_cert& my_cert = registration_md->cert();
   
   struct ms_gateway_cert* cert = SG_CALLOC( struct ms_gateway_cert, 1 );
   
   // sanity checks
   // flow control 
   if( registration_md->resolve_page_size() < 0 ) {
      
      SG_error("Invalid MS page size %d\n", registration_md->resolve_page_size() );
      
      rc = -EBADMSG;
      goto ms_client_load_registration_metadata_error;
   }
   
   // session expiry time
   if( registration_md->session_expires() > 0 && registration_md->session_expires() < md_current_time_seconds() ) {
      
      SG_error("Session expired at %" PRId64 "\n", registration_md->session_expires() );
      
      rc = -EBADMSG;
      goto ms_client_load_registration_metadata_error;
   }
   
   // load cert
   rc = ms_client_gateway_cert_init( cert, 0, &my_cert );
   if( rc != 0 ) {
      
      SG_error("ms_client_gateway_cert_init rc = %d\n", rc );
      
      goto ms_client_load_registration_metadata_error;
   }

   ms_client_rlock( client );

   // verify that our host and port match the MS's record.
   // The only time they don't have to match is when the gateway serves from localhost
   // (i.e. its intended to serve only local requests)
#ifndef _FIREWALL
   if( strcmp( cert->hostname, client->conf->hostname ) != 0 && strcasecmp( cert->hostname, "localhost" ) != 0 ) {
      
      // wrong host
      SG_error("ERR: This gateway is serving from %s, but the MS says it should be serving from %s:%d.  Please update the Gateway record on the MS.\n", client->conf->hostname, cert->hostname, cert->portnum );
      ms_client_unlock( client );

      rc = -ENOTCONN;
      goto ms_client_load_registration_metadata_error;
   }
#endif

   ms_client_unlock( client );

   SG_info("Registered as Gateway %s (%" PRIu64 ")\n", cert->name, cert->gateway_id );
   
   ms_client_wlock( client );
   
   // new session password
   new_session_password = registration_md->session_password().c_str();
   
   // new userpass
   // userpass format: ${gateway_type}_${gateway_id}:${session_password}
   sprintf(gateway_id_str, "%" PRIu64, cert->gateway_id );
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   new_userpass = SG_CALLOC( char, strlen(gateway_id_str) + 1 + strlen(gateway_type_str) + 1 + strlen(new_session_password) + 1 );
   if( new_userpass == NULL ) {
      
      rc = -ENOMEM;
      goto ms_client_load_registration_metadata_error;
   }
   
   sprintf( new_userpass, "%s_%s:%s", gateway_type_str, gateway_id_str, new_session_password );

   // get keys with password, if needed
   if( key_password != NULL ) {
      
      rc = ms_client_unseal_and_load_keys( registration_md, key_password, &new_pkey, &new_pubkey, &new_pkey_pem, &new_pkey_len, &new_pkey_mlocked );
      if( rc != 0 ) {
         
         SG_error("ms_client_unseal_and_load_keys rc = %d\n", rc );
         
         goto ms_client_load_registration_metadata_error;
      }
   }
   
   // new volume 
   volume = SG_CALLOC( struct ms_volume, 1 );
   if( volume == NULL ) {
      
      rc = -ENOMEM;
      goto ms_client_load_registration_metadata_error;
   }
   
   volume->reload_volume_key = true;         // get the public key
   vol_md = registration_md->mutable_volume();

   // load the Volume information, using the new client keys
   rc = ms_client_volume_init( volume, vol_md, volume_pubkey_pem, client->conf, new_pubkey, new_pkey );
   if( rc != 0 ) {
      
      SG_error("ms_client_volume_init('%s') rc = %d\n", vol_md->name().c_str(), rc );
      
      goto ms_client_load_registration_metadata_error;
   }

   SG_info("Register on Volume %" PRIu64 ": '%s', version: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version );

   // try cert insert 
   try {
      
      (*client->certs)[ cert->gateway_id ] = cert;
   }
   catch( bad_alloc& ba ) {
      
      rc = -ENOMEM;
      goto ms_client_load_registration_metadata_error;
   }
   
   // clear old fields
   if( client->userpass != NULL ) {
      SG_safe_free( client->userpass );
   }
   
   if( new_pkey != NULL && client->gateway_key != NULL ) {
      
      EVP_PKEY_free( client->gateway_key );
      
      client->gateway_key = new_pkey;
   }
   
   if( new_pubkey != NULL && client->gateway_pubkey != NULL ) {
      
      EVP_PKEY_free( client->gateway_pubkey );
      
      client->gateway_pubkey = new_pubkey;
   }
   
   if( new_pkey_pem != NULL && client->gateway_key_pem != NULL ) {
      
      if( client->gateway_key_pem_mlocked ) {
         munlock( client->gateway_key_pem, client->gateway_key_pem_len );
      }
      SG_safe_free( client->gateway_key_pem );
      
      client->gateway_key_pem = new_pkey_pem;
      client->gateway_key_pem_len = new_pkey_len;
      client->gateway_key_pem_mlocked = new_pkey_mlocked;
   }
   
   // set new fields
   client->owner_id = cert->user_id;
   client->gateway_id = cert->gateway_id;
   client->portnum = cert->portnum;
   client->userpass = new_userpass;
   client->session_expires = registration_md->session_expires();
   client->page_size = registration_md->resolve_page_size();
   client->volume = volume;
   client->cert_version = vol_md->cert_version();
   
   if( registration_md->has_max_batch_request_size() ) {
      client->max_request_batch = registration_md->max_batch_request_size();
   }
   else {
      client->max_request_batch = MS_CLIENT_DEFAULT_MAX_REQUEST_BATCH;
   }
   
   if( registration_md->has_max_batch_async_request_size() ) {
      client->max_request_async_batch = registration_md->max_batch_async_request_size();
   }
   else {
      client->max_request_async_batch = MS_CLIENT_DEFAULT_MAX_ASYNC_REQUEST_BATCH;
   }
   
   if( registration_md->has_max_connections() ) {
      client->max_connections = registration_md->max_connections();
   }
   else {
      client->max_connections = MS_CLIENT_DEFAULT_MAX_CONNECTIONS;
   }
   
   if( registration_md->has_max_transfer_time() ) {
      client->ms_transfer_timeout = registration_md->max_transfer_time();
   }
   else {
      client->ms_transfer_timeout = MS_CLIENT_DEFAULT_MS_TRANSFER_TIMEOUT;
   }
   
   ms_client_unlock( client );

   return rc;
   
ms_client_load_registration_metadata_error:
   
   if( volume != NULL ) {
      ms_volume_free( volume );
      SG_safe_free( volume );
   }
   
   if( new_pkey != NULL ) {
      EVP_PKEY_free( new_pkey );
   }
   
   if( new_pubkey != NULL ) {
      EVP_PKEY_free( new_pubkey );
   }
   
   if( new_pkey_mlocked && new_pkey_pem != NULL ) {
      munlock( new_pkey_pem, new_pkey_len );
   }
   
   SG_safe_free( new_pkey_pem );
   SG_safe_free( new_userpass );
   
   if( cert != NULL ) {
      ms_client_gateway_cert_free( cert );
      SG_safe_free( cert );
   }
   
   return rc;
}
   
   
// get the Syndicate public key from the MS
// return 0 on success 
// return -ENOMEM if out of memory 
// return negative on download error
static ssize_t ms_client_download_syndicate_public_key( struct ms_client* client, char** syndicate_public_key_pem ) {
   
   char* url = ms_client_syndicate_pubkey_url( client->url );
   if( url == NULL ) {
      return -ENOMEM;
   }
   
   char* bits = NULL;
   off_t len = 0;
   char* tmp = NULL;
   
   int rc = 0;
   
   rc = ms_client_download( client, url, &bits, &len );
   if( rc != 0 ) {
      
      SG_error("ms_client_download('%s') rc = %d\n", url, rc );
      
      SG_safe_free( url );
      return -rc;
   }
   
   // add a \0 at the end...
   tmp = (char*) realloc( bits, len + 1 );
   if( tmp == NULL ) {
      
      SG_safe_free( bits );
      SG_safe_free( url );
      return -ENOMEM;
   }

   bits = tmp;
   bits[len] = 0;
   *syndicate_public_key_pem = bits;
   
   SG_safe_free( url );
   
   return len;
}


// download and install the syndicate public key into the ms_client 
// return 0 on success 
// return -ENODATA if the key could not be loaded
// return negative on error
static int ms_client_reload_syndicate_public_key( struct ms_client* client ) {
   
   char* syndicate_public_key_pem = NULL;
   EVP_PKEY* new_public_key = NULL;
   
   int rc = 0;
   ssize_t pubkey_len = 0;
   
   pubkey_len = ms_client_download_syndicate_public_key( client, &syndicate_public_key_pem );
   
   if( pubkey_len < 0 ) {
      
      SG_error("ms_client_download_syndicate_public_key rc = %zd\n", pubkey_len );
      return (int)pubkey_len;
   }
   
   rc = ms_client_try_load_key( client->conf, &new_public_key, NULL, syndicate_public_key_pem, true );
   if( rc != 0 ) {
      
      SG_error("ms_client_try_load_key rc = %d\n", rc );
      
      SG_safe_free( syndicate_public_key_pem );
      return -ENODATA;
   }
   
   ms_client_wlock( client );
   
   if( client->syndicate_public_key != NULL ) {
      EVP_PKEY_free( client->syndicate_public_key );
      client->syndicate_public_key = NULL;
   }
   
   if( client->syndicate_public_key_pem != NULL ) {
      SG_safe_free( client->syndicate_public_key_pem );
   }
   
   client->syndicate_public_key = new_public_key;
   client->syndicate_public_key_pem = syndicate_public_key_pem;
   
   ms_client_unlock( client );
   
   SG_debug("Trusting new Syndicate public key:\n\n%s\n\n", syndicate_public_key_pem );
   
   return 0;
}


// register this gateway with the MS, using the user's OpenID username and password
// this will carry out the OpenID authentication
// return 0 on success
// return -ENOMEM if we run out of memory
// return -ENODATA if we do not have and were unable to obtain the Syndicate public key 
// return -EBADMSG if the data returned is invalid 
// return -ENOTCONN if we failed to register 
// return negative on OpenID error
int ms_client_openid_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password, char const* volume_pubkey_pem, char const* key_password ) {

   int rc = 0;

   CURL* curl = NULL;
   
   char* registration_md_buf = NULL;
   size_t registration_md_buf_len = 0;
   ms::ms_registration_metadata registration_md;
   
   char* register_url = NULL;
   
   bool valid = false;
   
   curl = curl_easy_init();
   if( curl == NULL ) {
      return -ENOMEM;
   }
   
   ms_client_rlock( client );

   // get registration url 
   register_url = ms_client_openid_register_url( client->url, client->gateway_type, gateway_name, username );
   if( register_url == NULL ) {
      
      ms_client_unlock( client );
      return -ENOMEM;
   }
   
   // setup curl handle
   md_init_curl_handle( client->conf, curl, NULL, client->conf->connect_timeout );

   ms_client_unlock( client );
   
   SG_debug("Register at MS: '%s'\n", register_url );
   
   // if we don't have the public key, grab it 
   if( client->syndicate_public_key == NULL ) {
      
      SG_warn("%s\n", "No Syndicate public key given.");
      
      rc = ms_client_reload_syndicate_public_key( client );
      if( rc != 0 ) {
         
         SG_error("ms_client_reload_syndicate_public_key rc = %d\n", rc );
         
         SG_safe_free( register_url );
         curl_easy_cleanup( curl );
         return -ENODATA;
      }
   }
   
   // open an OpenID-authenticated session, to get the registration data
   rc = ms_client_openid_session( curl, register_url, username, password, &registration_md_buf, &registration_md_buf_len, client->syndicate_public_key );
   
   // ... and close it, since we only needed to get the registration data
   curl_easy_cleanup( curl );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_openid_session('%s') rc = %d\n", register_url, rc );
      
      SG_safe_free( register_url );
      return -ENOTCONN;
   }
   
   SG_safe_free( register_url );
   
   // got the data.  parse it
   valid = registration_md.ParseFromArray( registration_md_buf, registration_md_buf_len );
   
   SG_safe_free( registration_md_buf );
  
   if( !valid ) {
      
      SG_error( "invalid registration metadata (missing %s)\n", registration_md.InitializationErrorString().c_str() );
      return -EBADMSG;
   }
   
   // load up the registration information, including our set of Volumes
   rc = ms_client_load_registration_metadata( client, &registration_md, volume_pubkey_pem, key_password );
   if( rc != 0 ) {
      
      SG_error("ms_client_load_registration_metadata rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   return rc;
}


// anonymously register with a (public) volume, in a read-only fashion
// return 0 on success 
// return -ENOMEM if we're out of memory
// return -ENODATA if we failed to download data 
// return -ENOTCONN if we failed to finish registration
// NOTE: this irreversably changes the state of the client; you should destroy this client if this method fails and try again
int ms_client_anonymous_gateway_register( struct ms_client* client, char const* volume_name, char const* volume_public_key_pem ) {
   
   int rc = 0;

   struct ms_gateway_cert cert;
   memset( &cert, 0, sizeof(cert) );
   
   struct ms_volume* volume = SG_CALLOC( struct ms_volume, 1 );
   if( volume == NULL ) {
      
      return -ENOMEM;
   }
   
   if( volume_public_key_pem != NULL ) {
      
      // attempt to load the public key 
      rc = md_load_pubkey( &volume->volume_public_key, volume_public_key_pem );
      if( rc != 0 ) {
         
         SG_error("md_load_pubkey rc = %d\n", rc );
         SG_safe_free( volume );
         return -EINVAL;
      }
   }
   else {  
      volume->reload_volume_key = true;         // get the public key
   }

   ms_client_wlock( client );
   
   if( client->userpass != NULL ) {
      SG_safe_free( client->userpass );
   }
   
   client->session_expires = -1;
   client->gateway_type = client->conf->gateway_type;
   client->owner_id = SG_USER_ANON;
   client->gateway_id = SG_GATEWAY_ANON;
   
   client->max_request_batch = MS_CLIENT_DEFAULT_MAX_REQUEST_BATCH;
   client->max_request_async_batch = MS_CLIENT_DEFAULT_MAX_ASYNC_REQUEST_BATCH;
   client->max_connections = MS_CLIENT_DEFAULT_MAX_CONNECTIONS;
   client->ms_transfer_timeout = MS_CLIENT_DEFAULT_MS_TRANSFER_TIMEOUT;
   
   ms_client_unlock( client );
   
   // load the volume information
   rc = ms_client_download_volume_by_name( client, volume_name, volume, volume_public_key_pem );
   if( rc != 0 ) {
      
      SG_error("ms_client_download_volume_by_name(%s) rc = %d\n", volume_name, rc );
      ms_volume_free( volume );
      SG_safe_free( volume );
      
      return -ENODATA;
   }
   
   SG_debug("Volume ID %" PRIu64 ": '%s', version: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version );

   ms_client_config_wlock( client );
   client->volume = volume;
   ms_client_config_unlock( client );
   
   return rc;
}


// populate a registration request for public key registration
// return 0 on success
// return -ENOMEM on error
// return -EINVAL if we failed to sign the request
static int ms_client_make_public_key_registration_request( struct ms_client* client, EVP_PKEY* user_pkey, char const* username, uint64_t gateway_type, char const* gateway_name, ms::ms_register_request* req ) {
   
   char nonce[4 * sizeof(uint64_t)];
   char const* tbl = "0123456789abcdef";
   uint64_t rnd = 0;
   char w1 = 0, w2 = 0, w3 = 0, w4 = 0, w5 = 0, w6 = 0, w7 = 0, w8 = 0;
   int rc = 0;
   
   try {
      req->set_username( string(username) );
      req->set_gateway_name( string(gateway_name) );
      req->set_gateway_type( gateway_type );
   }
   catch ( bad_alloc& ba ) {
      
      return -ENOMEM;
   }
   
   for( int i = 0; i < 4; i++ ) {
      
      rnd = md_random64();
      
      // unpack
      w1 = tbl[ (rnd >> 56) & 0xff ];
      w2 = tbl[ (rnd >> 48) & 0xff ];
      w3 = tbl[ (rnd >> 40) & 0xff ];
      w4 = tbl[ (rnd >> 32) & 0xff ];
      w5 = tbl[ (rnd >> 24) & 0xff ];
      w6 = tbl[ (rnd >> 16) & 0xff ];
      w7 = tbl[ (rnd >> 8) & 0xff ];
      w8 = tbl[ (rnd) & 0xff ];
      
      // copy over
      nonce[ 8*i ] = w1;
      nonce[ 8*i + 1 ] = w2;
      nonce[ 8*i + 2 ] = w3;
      nonce[ 8*i + 3 ] = w4;
      nonce[ 8*i + 4 ] = w5;
      nonce[ 8*i + 5 ] = w6;
      nonce[ 8*i + 6 ] = w7;
      nonce[ 8*i + 7 ] = w8;
   }
   
   // null-terminate 
   nonce[32] = 0;
      
   req->set_nonce( string(nonce) );
   
   rc = md_sign< ms::ms_register_request >( user_pkey, req );
   return rc;
}


// send the registration information via public-key authentication, and get back a reply 
// return 0 on success, and populate registration_md
// return -ENOMEM on oom
// return -ENODATA if we failed to download data
// return -EBADMSG if we failed to parse the resulting message 
static int ms_client_send_public_key_register_request( struct ms_client* client, char* url, ms::ms_register_request* reg_req, ms::ms_registration_metadata* registration_md ) {
   
   CURL* curl = NULL;
   
   // serialized request 
   char* serialized_registration_buf = NULL;
   size_t serialized_registration_buf_len = 0;
   char* tmp_registration_buf = NULL;
   
   // response 
   char* registration_md_buf = NULL;
   size_t registration_md_buf_len = 0;
   
   // POST the request
   struct curl_httppost *post = NULL, *last = NULL;
   long http_response = 0;
   long os_error = 0;
   md_response_buffer_t rb;

   bool valid = false;
   int rc = 0;
   int curl_rc = 0;
   
   curl = curl_easy_init();
   if( curl == NULL ) {
      return -ENOMEM;
   }
   
   ms_client_rlock( client );
   md_init_curl_handle( client->conf, curl, url, client->conf->connect_timeout );
   ms_client_unlock( client );
   
   rc = md_serialize< ms::ms_register_request >( reg_req, &serialized_registration_buf, &serialized_registration_buf_len );
   if( rc != 0 ) {
      
      SG_error("Failed to serialize, rc = %d\n", rc );
      
      curl_easy_cleanup( curl );
      return -EINVAL;
   }
   
   // zero-terminate the buffer, since older versions of libcurl will try to strlen() it and read past the end
   tmp_registration_buf = (char*)realloc( serialized_registration_buf, serialized_registration_buf_len + 1 );
   if( tmp_registration_buf == NULL ) {
      
      curl_easy_cleanup( curl );
      SG_safe_free( serialized_registration_buf );
      
      return -ENOMEM;
   }
   
   // null-terminated
   tmp_registration_buf[ serialized_registration_buf_len ] = 0;
   serialized_registration_buf = tmp_registration_buf;
   
   // POST the request 
   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-register-request", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, serialized_registration_buf, CURLFORM_BUFFERLENGTH, serialized_registration_buf_len, CURLFORM_END );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L);
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)&rb );
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, post );
   
   curl_rc = curl_easy_perform( curl );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );
   
   curl_easy_cleanup( curl );
   curl_formfree( post );
   SG_safe_free( serialized_registration_buf );
   
   
   if( curl_rc != 0 || http_response != 200 ) {
      
      md_response_buffer_free( &rb );
      
      rc = md_download_interpret_errors( http_response, curl_rc, os_error );
      
      SG_error("curl_easy_perform('%s') HTTP status = %ld, OS error = %ld, rc = %d\n", url, http_response, os_error, curl_rc );
      return rc;
   }

   // Parse to registration metadata
   registration_md_buf = md_response_buffer_to_string( &rb );
   registration_md_buf_len = md_response_buffer_size( &rb );
   
   // free memory
   md_response_buffer_free( &rb );
   
   if( registration_md_buf == NULL ) {
      
      return -ENOMEM;
   }
   
   try { 
      // got the data
      valid = registration_md->ParseFromString( string(registration_md_buf, registration_md_buf_len) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( registration_md_buf );
      return -ENOMEM;
   }
   
   // free memory
   SG_safe_free( registration_md_buf );

   if( !valid ) {
      
      SG_error( "%s", "invalid registration metadata\n" );
      return -EBADMSG;
   }
   
   return 0;
}
   
   
// register via public-key signatures.
// return 0 on success 
// return -EINVAL if a key could not be loaded
// return -ENOMEM if we run out of memory
// return -ENOTCONN if we failed to create a registration request
// return -ENODATA if we do not have the Syndicate public key and we could not get it, or if we couldn't download the registration data
// return -EBADMSG if we failed to load the metadata
int ms_client_public_key_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* user_privkey_pem, char const* volume_pubkey_pem, char const* key_password ) {
   
   ms::ms_registration_metadata registration_md;
   ms::ms_register_request registration_req;
   
   int rc = 0;
   
   EVP_PKEY* user_pkey = NULL;
   char* register_url = NULL;
   
   // load the private key
   rc = md_load_privkey( &user_pkey, user_privkey_pem );
   if( rc != 0 ) {
      SG_error("md_load_privkey rc = %d\n", rc );
      
      return -EINVAL;
   }

   ms_client_rlock( client );

   // make the request 
   rc = ms_client_make_public_key_registration_request( client, user_pkey, username, client->gateway_type, gateway_name, &registration_req );
   if( rc != 0 ) {
      ms_client_unlock( client );
      
      EVP_PKEY_free( user_pkey );
      
      SG_error("ms_client_make_public_key_registration_request rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   EVP_PKEY_free( user_pkey );
   
   register_url = ms_client_public_key_register_url( client->url );
   if( register_url == NULL ) {
      
      return -ENOMEM;
   }
   
   ms_client_unlock( client );
   
   SG_debug("register at %s\n", register_url );
   
   // if we don't have the public key, grab it 
   if( client->syndicate_public_key == NULL ) {
      
      SG_warn("%s\n", "WARN: no Syndicate public key given.");
      
      rc = ms_client_reload_syndicate_public_key( client );
      if( rc != 0 ) {
         
         SG_error("ms_client_reload_syndicate_public_key rc = %d\n", rc );
         
         SG_safe_free( register_url );
         return -ENODATA;
      }
   }

   // send our request; get our registration data 
   rc = ms_client_send_public_key_register_request( client, register_url, &registration_req, &registration_md );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_send_register_request('%s') rc = %d\n", register_url, rc );
      
      SG_safe_free( register_url );
   
      return -ENODATA;
   }
   
   
   // load up the registration information, including our set of Volumes
   rc = ms_client_load_registration_metadata( client, &registration_md, volume_pubkey_pem, key_password );
   if( rc != 0 ) {
      
      SG_error("ms_client_load_registration_metadata('%s') rc = %d\n", register_url, rc );
      SG_safe_free( register_url );
      
      return -ENOTCONN;
   }
   
   SG_safe_free( register_url );
   
   return rc;

}
