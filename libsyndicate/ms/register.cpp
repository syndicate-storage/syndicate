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
#include "libsyndicate/ms/volume.h"
#include "libsyndicate/ms/url.h"
#include "libsyndicate/ms/openid.h"

// unseal and load our private key from registration metadata, and load them into the client context
static int ms_client_unseal_and_load_keys( struct ms_client* client, ms::ms_registration_metadata* registration_md, char const* key_password ) {
   int rc = 0;
   if( key_password == NULL ) {
      errorf("%s\n", "No private key loaded, but no password to decrypt one with.");
      rc = -ENOTCONN;
   }
   else {
      // base64-encoded encrypted private key
      char const* encrypted_gateway_private_key_b64 = registration_md->encrypted_gateway_private_key().c_str();
      size_t encrypted_gateway_private_key_b64_len = registration_md->encrypted_gateway_private_key().size();
      
      size_t encrypted_gateway_private_key_len = 0;
      char* encrypted_gateway_private_key = NULL;
      
      int decode_rc = Base64Decode( encrypted_gateway_private_key_b64, encrypted_gateway_private_key_b64_len, &encrypted_gateway_private_key, &encrypted_gateway_private_key_len );
      if( decode_rc != 0 ) {
         errorf("%s\n", "Failed to decode private key.  No gateway private key given!" );
         rc = -ENOTCONN;
      }
      else {
         // NOTE: will be mlock'ed
         char* gateway_private_key_str = NULL;
         size_t gateway_private_key_str_len = 0;
         
         dbprintf("%s\n", "Unsealing gateway private key...");
         
         decode_rc = md_password_unseal_mlocked( encrypted_gateway_private_key, encrypted_gateway_private_key_len, key_password, strlen(key_password), &gateway_private_key_str, &gateway_private_key_str_len );
         if( decode_rc != 0 ) {
            errorf("Failed to unseal gateway private key, rc = %d\n", decode_rc );
            rc = -ENOTCONN;
         }
         else {
            // validate and import it
            EVP_PKEY* pkey = NULL;
            EVP_PKEY* pubkey = NULL;
            
            decode_rc = md_load_public_and_private_keys( &pubkey, &pkey, gateway_private_key_str );
            if( decode_rc != 0 ) {
               errorf("md_load_privkey rc = %d\n", decode_rc );
               rc = -ENODATA;
               
               memset( gateway_private_key_str, 0, gateway_private_key_str_len );
               munlock( gateway_private_key_str, gateway_private_key_str_len );
               free( gateway_private_key_str );
            }
            else {
               decode_rc = ms_client_verify_key( pkey );
               if( decode_rc != 0 ) {
                  errorf("ms_client_verify_key rc = %d\n", decode_rc );
                  rc = -ENODATA;
                  
                  memset( gateway_private_key_str, 0, gateway_private_key_str_len );
                  munlock( gateway_private_key_str, gateway_private_key_str_len );
                  free( gateway_private_key_str );
               }
               else {
                  // we're good!  install them
                  client->my_key = pkey;
                  client->my_pubkey = pubkey;
                  client->my_key_pem = gateway_private_key_str;
                  client->my_key_pem_len = gateway_private_key_str_len;
                  client->my_key_pem_mlocked = true;
               }
            }
         }
      }
      
      if( encrypted_gateway_private_key )
         free( encrypted_gateway_private_key );
   }
   
   return rc;
}
   

// load a registration message and populate the session-related and key-related fields in the client
int ms_client_load_registration_metadata( struct ms_client* client, ms::ms_registration_metadata* registration_md, char const* volume_pubkey_pem, char const* key_password ) {

   int rc = 0;

   struct ms_gateway_cert cert;
   memset( &cert, 0, sizeof(cert) );

   // load cert
   const ms::ms_gateway_cert& my_cert = registration_md->cert();
   rc = ms_client_gateway_cert_init( &cert, 0, &my_cert );
   if( rc != 0 ) {
      errorf("ms_client_gateway_cert_init rc = %d\n", rc );
      return rc;
   }

   ms_client_rlock( client );

   // verify that our host and port match the MS's record.
   // The only time they don't have to match is when the gateway serves from localhost
   // (i.e. its intended to serve only local requests)
#ifndef _FIREWALL
   if( strcmp( cert.hostname, client->conf->hostname ) != 0 && strcasecmp( cert.hostname, "localhost" ) != 0 ) {
      // wrong host
      errorf("ERR: This gateway is serving from %s, but the MS says it should be serving from %s:%d.  Please update the Gateway record on the MS.\n", client->conf->hostname, cert.hostname, cert.portnum );
      ms_client_unlock( client );

      ms_client_gateway_cert_free( &cert );
      return -ENOTCONN;
   }
#else
   // skip verifying -- virtual ip and real ip will be different
#endif

   ms_client_unlock( client );

   dbprintf("Registered as Gateway %s (%" PRIu64 ")\n", cert.name, cert.gateway_id );
   
   ms_client_wlock( client );
   
   // new session password
   if( client->userpass ) {
      free( client->userpass );
   }

   // userpass format:
   // ${gateway_type}_${gateway_id}:${password}
   char gateway_type_str[5];
   char gateway_id_str[50];

   if( client->session_password ) {
      free( client->session_password );
   }

   client->session_password = strdup( registration_md->session_password().c_str() );
   client->session_expires = registration_md->session_expires();
   
   sprintf(gateway_id_str, "%" PRIu64, cert.gateway_id );
   ms_client_gateway_type_str( client->gateway_type, gateway_type_str );

   client->userpass = CALLOC_LIST( char, strlen(gateway_id_str) + 1 + strlen(gateway_type_str) + 1 + strlen(client->session_password) + 1 );
   sprintf( client->userpass, "%s_%s:%s", gateway_type_str, gateway_id_str, client->session_password );

   client->owner_id = cert.user_id;
   client->gateway_id = cert.gateway_id;
   client->portnum = cert.portnum;
   
   client->page_size = registration_md->resolve_page_size();
   if( client->page_size < 0 ) {
      errorf("Invalid MS page size %d\n", client->page_size );
      rc = -EINVAL;
   }
   
   // sanity check...
   if( client->session_expires > 0 && client->session_expires < currentTimeSeconds() ) {
      errorf("Session password expired at %" PRId64 "\n", client->session_expires );
      rc = -EINVAL;
   }
   
   // possibly received our private key...
   if( rc == 0 && client->my_key == NULL && registration_md->has_encrypted_gateway_private_key() ) {
      rc = ms_client_unseal_and_load_keys( client, registration_md, key_password );
      if( rc != 0 ) {
         errorf("ms_client_unseal_and_load_keys rc = %d\n", rc );
      }
   }
   
   ms_client_unlock( client );
   
   if( rc != 0 ) {
      // failed to initialize 
      return rc;
   }
   
   // load the volume up
   struct ms_volume* volume = CALLOC_LIST( struct ms_volume, 1 );
   
   volume->reload_volume_key = true;         // get the public key

   ms::ms_volume_metadata* vol_md = registration_md->mutable_volume();

   // load the Volume information
   rc = ms_client_volume_init( volume, vol_md, volume_pubkey_pem, client->conf, client->my_pubkey, client->my_key );
   if( rc != 0 ) {
      errorf("ms_client_volume_init(%s) rc = %d\n", vol_md->name().c_str(), rc );
      
      ms_volume_free( volume );
      free( volume );
      ms_client_gateway_cert_free( &cert );
      return rc;
   }

   dbprintf("Volume ID %" PRIu64 ": '%s', version: %" PRIu64 ", certs: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version, volume->volume_cert_version );

   ms_client_view_wlock( client );
   client->volume = volume;
   ms_client_view_unlock( client );

   dbprintf("Registered with %s\n", client->url );

   ms_client_gateway_cert_free( &cert );

   return rc;
}
   
   
// get the Syndicate public key from the MS
static ssize_t ms_client_download_syndicate_public_key( struct ms_client* client, char** syndicate_public_key_pem ) {
   
   char* url = ms_client_syndicate_pubkey_url( client->url );
   
   char* bits = NULL;
   size_t len = 0;
   
   int http_status = ms_client_download( client, url, &bits, &len );
   if( http_status != 200 ) {
      
      errorf("ms_client_download(%s) rc = %d\n", url, http_status );
      return -abs(http_status);
   }
   
   // add a \0 at the end...
   char* tmp = (char*) realloc( bits, len + 1 );
   if( tmp == NULL ) {
      free( bits );
      free( url );
      return -ENOMEM;
   }

   bits = tmp;
   bits[len] = 0;
   *syndicate_public_key_pem = bits;
   
   free( url );
   return len;
}


// download and install the syndicate public key into the ms_client 
static int ms_client_reload_syndicate_public_key( struct ms_client* client ) {
   
   char* syndicate_public_key_pem = NULL;
   ssize_t pubkey_len = ms_client_download_syndicate_public_key( client, &syndicate_public_key_pem );
   
   if( pubkey_len < 0 ) {
      errorf("ms_client_download_syndicate_public_key rc = %zd\n", pubkey_len );
      return (int)pubkey_len;
   }
   
   EVP_PKEY* new_public_key = NULL;
   
   int rc = ms_client_try_load_key( client->conf, &new_public_key, NULL, syndicate_public_key_pem, true );
   if( rc != 0 ) {
      errorf("ms_client_try_load_key rc = %d\n", rc );
      free( syndicate_public_key_pem );
      return -ENODATA;
   }
   
   ms_client_wlock( client );
   
   if( client->syndicate_public_key ) {
      EVP_PKEY_free( client->syndicate_public_key );
   }
   
   if( client->syndicate_public_key_pem ) {
      free( client->syndicate_public_key_pem );
   }
   
   client->syndicate_public_key = new_public_key;
   client->syndicate_public_key_pem = syndicate_public_key_pem;
   
   dbprintf("Trusting new Syndicate public key:\n\n%s\n\n", syndicate_public_key_pem );
   
   ms_client_unlock( client );
   
   return 0;
}


// finish registration
static int ms_client_finish_registration( struct ms_client* client ) {
   int rc = 0;
   
   // load the certificate bundle   
   rc = ms_client_reload_certs( client, (uint64_t)(-1) );
   if( rc != 0 ) {
      errorf("ms_client_reload_certs rc = %d\n", rc );
      return -ENODATA;
   }
   
   // start the threads
   rc = ms_client_start_threads( client );
   if( rc != 0 && rc != -EALREADY ) {
      errorf("ms_client_start_threads rc = %d\n", rc );
   }
   else {
      rc = 0;
   }
   
   return rc;
}

   

// register this gateway with the MS, using the user's OpenID username and password
// this will carry out the OpenID 
int ms_client_openid_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* password, char const* volume_pubkey_pem, char const* key_password ) {

   int rc = 0;

   CURL* curl = curl_easy_init();
   
   char* registration_md_buf = NULL;
   size_t registration_md_buf_len = 0;
   ms::ms_registration_metadata registration_md;

   ms_client_rlock( client );

   char* register_url = ms_client_openid_register_url( client->url, client->gateway_type, gateway_name, username );
   md_init_curl_handle( client->conf, curl, NULL, client->conf->connect_timeout );

   ms_client_unlock( client );
   
   dbprintf("register at %s\n", register_url );
   
   // if we don't have the public key, grab it 
   if( client->syndicate_public_key == NULL ) {
      dbprintf("%s\n", "WARN: no Syndicate public key given.");
      rc = ms_client_reload_syndicate_public_key( client );
      if( rc != 0 ) {
         errorf("ms_client_reload_syndicate_public_key rc = %d\n", rc );
         
         free( register_url );
         return -ENODATA;
      }
   }
   
   // open an OpenID-authenticated session, to get the registration data
   rc = ms_client_openid_session( curl, register_url, username, password, &registration_md_buf, &registration_md_buf_len, client->syndicate_public_key );
   
   // ... and close it, since we only needed to get the registration data
   curl_easy_cleanup( curl );
   free( register_url );
   
   if( rc != 0 ) {
      errorf("ms_client_openid_session rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   // got the data
   bool valid = registration_md.ParseFromString( string(registration_md_buf, registration_md_buf_len) );
   
   free( registration_md_buf );
  
   if( !valid ) {
      errorf( "%s", "invalid registration metadata\n" );
      return -EINVAL;
   }
   
   // load up the registration information, including our set of Volumes
   rc = ms_client_load_registration_metadata( client, &registration_md, volume_pubkey_pem, key_password );
   if( rc != 0 ) {
      errorf("ms_client_load_registration_metadata rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   // finish up 
   rc = ms_client_finish_registration( client );
   if( rc != 0 ) {
      errorf("ms_client_finish_registration rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   return rc;
}


// anonymously register with a (public) volume, in a read-only fashion
int ms_client_anonymous_gateway_register( struct ms_client* client, char const* volume_name, char const* volume_public_key_pem ) {
   int rc = 0;

   struct ms_gateway_cert cert;
   memset( &cert, 0, sizeof(cert) );

   struct ms_volume* volume = CALLOC_LIST( struct ms_volume, 1 );
   
   if( volume_public_key_pem != NULL ) {
      // attempt to load the public key 
      rc = md_load_pubkey( &volume->volume_public_key, volume_public_key_pem );
      if( rc != 0 ) {
         errorf("md_load_pubkey rc = %d\n", rc );
         return -EINVAL;
      }
   }
   else {  
      volume->reload_volume_key = true;         // get the public key
   }

   ms_client_wlock( client );
   
   // fill in sane defaults
   if( client->session_password ) {
      free( client->session_password );
   }
   
   client->session_password = NULL;
   client->session_expires = -1;
   client->gateway_type = client->conf->gateway_type;
   client->owner_id = client->conf->owner;
   client->gateway_id = client->conf->gateway;
   
   ms_client_unlock( client );
   
   // load the volume information
   rc = ms_client_download_volume_by_name( client, volume_name, volume, volume_public_key_pem );
   if( rc != 0 ) {
      errorf("ms_client_download_volume_by_name(%s) rc = %d\n", volume_name, rc );
      ms_volume_free( volume );
      free( volume );
      return -ENODATA;
   }
   
   dbprintf("Volume ID %" PRIu64 ": '%s', version: %" PRIu64 ", certs: %" PRIu64 "\n", volume->volume_id, volume->name, volume->volume_version, volume->volume_cert_version );

   ms_client_view_wlock( client );
   client->volume = volume;
   ms_client_view_unlock( client );
   
   // finish registration 
   rc = ms_client_finish_registration( client );
   if( rc != 0 ) {
      errorf("ms_client_finish_registration rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   return rc;
}


// populate a registration request for public key registration
static int ms_client_make_public_key_registration_request( struct ms_client* client, EVP_PKEY* user_pkey, char const* username, int gateway_type, char const* gateway_name, ms::ms_register_request* req ) {
   req->set_username( string(username) );
   req->set_gateway_name( string(gateway_name) );
   req->set_gateway_type( gateway_type );
   
   char nonce[4 * sizeof(uint64_t)];
   char const* tbl = "0123456789abcdef";
   
   for( int i = 0; i < 4; i++ ) {
      
      uint64_t rnd = md_random64();
      
      // unpack
      char w1 = tbl[ (rnd >> 56) & 0xff ];
      char w2 = tbl[ (rnd >> 48) & 0xff ];
      char w3 = tbl[ (rnd >> 40) & 0xff ];
      char w4 = tbl[ (rnd >> 32) & 0xff ];
      char w5 = tbl[ (rnd >> 24) & 0xff ];
      char w6 = tbl[ (rnd >> 16) & 0xff ];
      char w7 = tbl[ (rnd >> 8) & 0xff ];
      char w8 = tbl[ (rnd) & 0xff ];
      
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
   
   int rc = md_sign< ms::ms_register_request >( user_pkey, req );
   return rc;
}


// send the registration information via public-key authentication, and get back a reply 
static int ms_client_send_public_key_register_request( struct ms_client* client, char* url, ms::ms_register_request* reg_req, ms::ms_registration_metadata* registration_md ) {
   
   CURL* curl = curl_easy_init();
   
   ms_client_rlock( client );
   md_init_curl_handle( client->conf, curl, url, client->conf->connect_timeout );
   ms_client_unlock( client );
   
   // serialize
   char* serialized_registration_buf = NULL;
   size_t serialized_registration_buf_len = 0;
   
   int rc = md_serialize< ms::ms_register_request >( reg_req, &serialized_registration_buf, &serialized_registration_buf_len );
   if( rc != 0 ) {
      errorf("Failed to serialize, rc = %d\n", rc );
      
      curl_easy_cleanup( curl );
      return -EINVAL;
   }
   
   // zero-terminate the buffer, since older versions of libcurl will try to strlen() it and read past the end
   char* tmp_registration_buf = (char*)realloc( serialized_registration_buf, serialized_registration_buf_len + 1 );
   if( tmp_registration_buf == NULL ) {
      curl_easy_cleanup( curl );
      free( serialized_registration_buf );
      
      return -ENOMEM;
   }
   
   tmp_registration_buf[ serialized_registration_buf_len ] = 0;
   serialized_registration_buf = tmp_registration_buf;
   
   // POST the request 
   struct curl_httppost *post = NULL, *last = NULL;
   long http_response = 0;
   response_buffer_t* rb = new response_buffer_t();

   // send as multipart/form-data file
   curl_formadd( &post, &last, CURLFORM_COPYNAME, "ms-register-request", CURLFORM_BUFFER, "data", CURLFORM_BUFFERPTR, serialized_registration_buf, CURLFORM_BUFFERLENGTH, serialized_registration_buf_len, CURLFORM_END );
   
   curl_easy_setopt( curl, CURLOPT_POST, 1L);
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)rb );
   curl_easy_setopt( curl, CURLOPT_HTTPPOST, post );
   
   rc = curl_easy_perform( curl );
   
   if( rc != 0 ) {
      errorf("curl_easy_perform(%s) rc = %d\n", url, rc );
         
      curl_easy_cleanup( curl );
      curl_formfree( post );
      free( serialized_registration_buf );
      
      response_buffer_free( rb );
      
      delete rb;
      return rc;
   }
   else {
      // get status 
      curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
      
      curl_easy_cleanup( curl );
      curl_formfree( post );
      free( serialized_registration_buf );
      
      if( http_response != 200 ) {
         errorf("curl_easy_perform(%s) HTTP status = %ld\n", url, http_response );
         response_buffer_free( rb );
         delete rb;
         return -ENODATA;
      }
      else {
         // got 200.  Parse to registration metadata
         char* registration_md_buf = response_buffer_to_string( rb );
         size_t registration_md_buf_len = response_buffer_size( rb );
         
         // free memory
         response_buffer_free( rb );
         delete rb;
         
         // got the data
         bool valid = registration_md->ParseFromString( string(registration_md_buf, registration_md_buf_len) );
         
         // free memory
         free( registration_md_buf );
      
         if( !valid ) {
            errorf( "%s", "invalid registration metadata\n" );
            return -EINVAL;
         }
      }
   }
   
   return 0;
}
   
   

// register via public-key signatures.
int ms_client_public_key_gateway_register( struct ms_client* client, char const* gateway_name, char const* username, char const* user_privkey_pem, char const* volume_pubkey_pem, char const* key_password ) {
   
   ms::ms_registration_metadata registration_md;
   ms::ms_register_request registration_req;
   
   EVP_PKEY* user_pkey = NULL;
   
   // load the private key
   int rc = md_load_privkey( &user_pkey, user_privkey_pem );
   if( rc != 0 ) {
      errorf("md_load_privkey rc = %d\n", rc );
      
      return -EINVAL;
   }

   ms_client_rlock( client );

   // make the request 
   rc = ms_client_make_public_key_registration_request( client, user_pkey, username, client->gateway_type, gateway_name, &registration_req );
   if( rc != 0 ) {
      ms_client_unlock( client );
      
      errorf("ms_client_make_public_key_registration_request rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   char* register_url = ms_client_public_key_register_url( client->url );
   
   ms_client_unlock( client );
   
   dbprintf("register at %s\n", register_url );
   
   // if we don't have the public key, grab it 
   if( client->syndicate_public_key == NULL ) {
      dbprintf("%s\n", "WARN: no Syndicate public key given.");
      rc = ms_client_reload_syndicate_public_key( client );
      if( rc != 0 ) {
         errorf("ms_client_reload_syndicate_public_key rc = %d\n", rc );
         
         free( register_url );
         return -ENODATA;
      }
   }

   // send our request; get our registration data 
   rc = ms_client_send_public_key_register_request( client, register_url, &registration_req, &registration_md );
   
   free( register_url );
   
   if( rc != 0 ) {
      errorf("ms_client_send_register_request rc = %d\n", rc );
      return -ENODATA;
   }
   
   // load up the registration information, including our set of Volumes
   rc = ms_client_load_registration_metadata( client, &registration_md, volume_pubkey_pem, key_password );
   if( rc != 0 ) {
      errorf("ms_client_load_registration_metadata rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   // finish up 
   rc = ms_client_finish_registration( client );
   if( rc != 0 ) {
      errorf("ms_client_finish_registration rc = %d\n", rc );
      return -ENOTCONN;
   }
   
   return rc;

}