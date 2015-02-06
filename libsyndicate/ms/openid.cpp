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

#include "libsyndicate/ms/openid.h"
#include "libsyndicate/crypt.h"
#include "libsyndicate/ms/file.h"

// set a CURL handle's HTTP method, URL and query string
// return 0 on success
// return -EINVAL if the method is invalid.
int ms_client_curl_http_setup( CURL* curl, char const* method, char const* url, char const* qs ) {

   curl_easy_setopt( curl, CURLOPT_URL, url );
   
   if( strcmp(method, "POST") == 0 ) {
      
      curl_easy_setopt( curl, CURLOPT_POST, 1L );

      if( qs != NULL ) {
         curl_easy_setopt( curl, CURLOPT_POSTFIELDS, qs );
      }
   }
   
   else if( strcmp(method, "GET") == 0 ) {
      
      curl_easy_setopt( curl, CURLOPT_HTTPGET, 1L );
   }
   else {
      
      SG_error("Invalid HTTP method '%s'\n", method );
      return -EINVAL;
   }
   
   return 0;
}


// read an OpenID reply from the MS, optionally verifying it with the given syndicate_public_key (if it is not NULL)
// return 0 on success, and put the data in 
// return -EINVAL if the message was malformed, or we couldn't verify it with the given public key 
int ms_client_load_openid_reply( ms::ms_openid_provider_reply* oid_reply, char* openid_redirect_reply_bits, size_t openid_redirect_reply_bits_len, EVP_PKEY* syndicate_public_key ) {
   
   int rc = 0;
   bool valid = false;
   
   // get back the OpenID provider reply
   string openid_redirect_reply_bits_str = string( openid_redirect_reply_bits, openid_redirect_reply_bits_len );

   valid = oid_reply->ParseFromString( openid_redirect_reply_bits_str );
   
   if( !valid ) {
      
      SG_error("Invalid MS OpenID provider reply (missing %s)\n", oid_reply->InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   if( syndicate_public_key != NULL ) {
      
      rc = md_verify< ms::ms_openid_provider_reply >( syndicate_public_key, oid_reply );
      if( rc != 0 ) {
         
         SG_error("%s", "Signature mismatch in OpenID provider reply\n");
         return -EINVAL;
      }
   }
   else {
      
      SG_warn("%s", "No Syndicate public key given.  Relying on TLS to guarantee authenticity of OpenID reply from MS\n" );
   }
   
   return 0;
}

// header accumulator function 
// userdata is a md_response_buffer_t
// return size * nmemb on success
// return 0 on failure (i.e. OOM)
static size_t ms_client_redirect_header_func( void *ptr, size_t size, size_t nmemb, void *userdata) {

   md_response_buffer_t* rb = (md_response_buffer_t*)userdata;
   size_t len = size * nmemb;
   char* data = (char*)ptr;
   
   char* data_str = SG_CALLOC( char, len );
   if( data_str == NULL ) {
      // OOM!
      return 0;
   }
   
   memcpy( data_str, data, len );
   rb->push_back( md_buffer_segment_t( data_str, len ) );
   
   return len;
}


// dummy CURL read
static size_t ms_client_dummy_callback( void *ptr, size_t size, size_t nmemb, void *userdata ) {
   memset( ptr, 0, size * nmemb );
   return size * nmemb;
}


// begin the authentication process.  Ask to be securely redirected from the MS to the OpenID provider.
// on success, return 0 and populate the ms_openid_provider_reply structure with the information needed to proceed with the OpenID authentication
// on failure, return nonzero
int ms_client_openid_begin( CURL* curl, char const* username, char const* begin_url, ms::ms_openid_provider_reply* oid_reply, EVP_PKEY* syndicate_public_key ) {

   md_response_buffer_t rb;      // will hold the OpenID provider reply
   char* username_encoded = NULL;
   char* post = NULL;
   int rc = 0;
   long http_response = 0;
   long os_error = 0;
   char* response = NULL;
   size_t response_len = 0;
   
   // url-encode the username
   username_encoded = md_url_encode( username, strlen(username) );
   if( username_encoded == NULL ) {
      return -ENOMEM;
   }
   
   // post arguments the MS expects
   post = SG_CALLOC( char, strlen(MS_OPENID_USERNAME_FIELD) + 1 + strlen(username_encoded) + 1 );
   if( post == NULL ) {
      
      SG_safe_free( username_encoded );
      return -ENOMEM;
   }
   
   sprintf( post, "%s=%s", MS_OPENID_USERNAME_FIELD, username_encoded );
   
   SG_safe_free( username_encoded );

   curl_easy_setopt( curl, CURLOPT_URL, begin_url );
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, post );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)&rb );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );

   rc = curl_easy_perform( curl );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );
   
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, ms_client_dummy_callback );
   
   SG_safe_free( post );

   if( rc != 0 || http_response != 200 ) {
      
      SG_error("curl_easy_perform rc = %d, err = %ld, HTTP status = %ld\n", rc, os_error, http_response );
      
      rc = ms_client_download_interpret_errors( begin_url, http_response, rc, -os_error );
      return rc;
   }

   if( rb.size() == 0 ) {
      SG_error("no data received from '%s'\n", begin_url);
      md_response_buffer_free( &rb );
      return -ENODATA;
   }

   response = md_response_buffer_to_string( &rb );
   
   if( response == NULL ) {
      
      md_response_buffer_free( &rb );
      return -ENOMEM;
   }
   
   response_len = md_response_buffer_size( &rb );

   rc = ms_client_load_openid_reply( oid_reply, response, response_len, syndicate_public_key );

   SG_safe_free( response );
   md_response_buffer_free( &rb );

   return rc;
}


// authenticate to the OpenID provider.
// populate the return_to_method (GET|POST) and return_to (URL) strings
int ms_client_openid_auth( CURL* curl, char const* username, char const* password, ms::ms_openid_provider_reply* oid_reply, char** return_to ) {

   char* post = NULL;
   char const* openid_redirect_url = oid_reply->redirect_url().c_str();
   long http_response = 0;
   long os_error = 0;
   
   char* url = NULL;
   char* qs = NULL;
   
   char* header_buf = NULL;
   char* location = NULL;
   
   // MS-given parameters about the OpenID provider fields 
   char const* extra_args = NULL;
   char const* username_field = NULL;
   char const* password_field = NULL;
   char const* auth_handler = NULL;
   
   char* username_urlencoded = NULL;
   char* password_urlencoded = NULL;
   
   // accumulate headers to find Location: 
   md_response_buffer_t header_rb;
   
   int rc = 0;
   
   // how we ask the OID provider to challenge us
   char const* challenge_method = oid_reply->challenge_method().c_str();

   // how we respond to the OID provider challenge
   char const* response_method = oid_reply->response_method().c_str();

   SG_debug("%s challenge to %s\n", challenge_method, openid_redirect_url );

   // inform the OpenID provider that we have been redirected by the RP by fetching the authentication page.
   // The OpenID provider may then redirect us back.
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );     // catch 302
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );

   rc = md_split_url_qs( openid_redirect_url, &url, &qs );
   if( rc != 0 ) {
      return rc;
   }
   
   rc = ms_client_curl_http_setup( curl, challenge_method, url, qs );
   if( rc != 0 ) {
      
      SG_error("ms_client_curl_http_setup(%s) rc = %d\n", challenge_method, rc );
      
      SG_safe_free( url );
      SG_safe_free( qs );
      
      return rc;
   }

   rc = curl_easy_perform( curl );

   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );
   
   if( rc != 0 ) {
      
      SG_error("curl_easy_perform rc = %d, err = %ld, HTTP status = %ld\n", rc, os_error, http_response );
      
      md_response_buffer_free( &header_rb );
      
      rc = ms_client_download_interpret_errors( url, http_response, rc, -os_error );
      
      SG_safe_free( url );
      SG_safe_free( qs );
      return rc;
   }
   
   SG_safe_free( url );
   SG_safe_free( qs );
   
   if( http_response != 200 && http_response != 302 ) {
      
      SG_error("curl_easy_perform HTTP status = %ld\n", http_response );
      md_response_buffer_free( &header_rb );
      return -ENODATA;
   }

   if( http_response == 302 ) {
      
      // authenticated already; we're being sent back
      header_buf = md_response_buffer_to_c_string( &header_rb );
      md_response_buffer_free( &header_rb );
      
      if( header_buf == NULL ) {
         
         // OOM
         return -ENOMEM;
      }
      
      rc = md_parse_header( header_buf, "Location", &location );
      
      if( rc != 0 ) {
         
         SG_error("No 'Location:' header found; full header is\n%s\n", header_buf );
         SG_safe_free( header_buf );
         return rc;
      }
      else {
         
         *return_to = location;
      }
      
      return 0;
   }

   md_response_buffer_free( &header_rb );

   // authenticate to the OpenID provider
   extra_args = oid_reply->extra_args().c_str();
   username_field = oid_reply->username_field().c_str();
   password_field = oid_reply->password_field().c_str();
   auth_handler = oid_reply->auth_handler().c_str();

   username_urlencoded = md_url_encode( username, strlen(username) );
   password_urlencoded = md_url_encode( password, strlen(password) );
   
   if( username_urlencoded == NULL || password_urlencoded == NULL ) {
      
      SG_safe_free( username_urlencoded );
      SG_safe_free( password_urlencoded );
      return -ENOMEM;
   }
   
   post = SG_CALLOC( char, strlen(username_field) + 1 + strlen(username_urlencoded) + 1 +
                           strlen(password_field) + 1 + strlen(password_urlencoded) + 1 +
                           strlen(extra_args) + 1 );
   
   if( post == NULL ) {
      
      SG_safe_free( username_urlencoded );
      SG_safe_free( password_urlencoded );
      return -ENOMEM;
   }
   
   sprintf( post, "%s=%s&%s=%s&%s", username_field, username_urlencoded, password_field, password_urlencoded, extra_args );
   
   SG_safe_free( username_urlencoded );
   SG_safe_free( password_urlencoded );

   SG_debug("%s authenticate to %s?%s\n", response_method, auth_handler, post );

   rc = ms_client_curl_http_setup( curl, response_method, auth_handler, post );
   if( rc != 0 ) {
      
      SG_error("ms_client_curl_http_setup(%s) rc = %d\n", response_method, rc );
      
      SG_safe_free( post );
      return rc;
   }

   // send the authentication request
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );

   rc = curl_easy_perform( curl );
   
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );

   SG_safe_free( post );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );
   
   if( rc != 0 ) {
      
      SG_error("curl_easy_perform rc = %d, err = %ld, HTTP status = %ld\n", rc, os_error, http_response );
      
      md_response_buffer_free( &header_rb );
      
      rc = ms_client_download_interpret_errors( url, http_response, rc, -os_error );
      
      return rc;
   }
   
   if( http_response != 302 ) {
      
      SG_error("curl_easy_perform HTTP status = %ld\n", http_response );
      md_response_buffer_free( &header_rb );
      return -ENODATA;
   }

   // authenticated! get the location 
   header_buf = md_response_buffer_to_c_string( &header_rb );
   md_response_buffer_free( &header_rb );
   
   if( header_buf == NULL ) {
      
      // OOM
      return -ENOMEM;
   }
   
   rc = md_parse_header( header_buf, "Location", &location );
   
   if( rc != 0 ) {
      
      SG_error("No 'Location:' header found; full header is\n%s\n", header_buf );
      SG_safe_free( header_buf );
      return rc;
   }
   else {
      
      *return_to = location;
      SG_safe_free( header_buf );
   }
   
   return 0;
}


// complete the openid authentication.
// if given, submit the request_body to the return_to URL.
// return 0 on success, and set *response_body and *response_len to the returned HTTP data (if they are not NULL)
// return -ENOMEM if out of memory 
// return -EINVAL if the return_to_method is unrecognized
// return -ENODATA if we didn't get an HTTP 200
int ms_client_openid_complete( CURL* curl, char const* return_to_method, char const* return_to, char** response_body, size_t* response_body_len ) {

   SG_debug("%s return to %s\n", return_to_method, return_to );

   char* return_to_url = NULL;
   char* return_to_qs = NULL;

   // replied data
   char* bits = NULL;
   ssize_t len = 0;
   long http_response = 0;
   long os_error = 0;
   
   int rc = 0;

   rc = md_split_url_qs( return_to, &return_to_url, &return_to_qs );
   if( rc != 0 ) {
      
      return rc;
   }
   
   rc = ms_client_curl_http_setup( curl, return_to_method, return_to_url, return_to_qs );
   if( rc != 0 ) {
      
      SG_error("ms_client_curl_http_setup(%s) rc = %d\n", return_to_method, rc );
      
      SG_safe_free( return_to_url );
      SG_safe_free( return_to_qs );
      
      return rc;
   }
   
   // complete the OpenID authentication
   rc = md_download_file( curl, &bits, &len );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );

   if( rc != 0 ) {
      
      SG_error("md_download_file('%s') rc = %d\n", return_to_url, rc );
      
      SG_safe_free( return_to_url );
      SG_safe_free( return_to_qs );
      
      rc = ms_client_download_interpret_errors( return_to_url, http_response, rc, -os_error );
      
      return rc;
   }

   if( http_response != 200 ) {
      
      SG_error("md_download_file('%s') HTTP status = %ld\n", return_to_url, http_response );
      
      SG_safe_free( return_to_url );
      SG_safe_free( return_to_qs );
      SG_safe_free( bits );
      
      return -ENODATA;
   }

   // success!
   if( response_body != NULL ) {
      *response_body = bits;
   }
   else {
      SG_safe_free( bits );
   }
   
   if( response_body_len != NULL ) {
      *response_body_len = len;
   }
   
   SG_safe_free( return_to_url );
   SG_safe_free( return_to_qs );
   
   return 0;
}


// open a session with the MS, authenticating via OpenID.
// return 0 on success, and set optional download buffer (response_buf, response_len) will hold the MS's initial response from the OpenID authentication (from the "complete" leg).
// return negative on error
int ms_client_openid_session( CURL* curl, char const* ms_openid_url, char const* username, char const* password, char** response_buf, size_t* response_len, EVP_PKEY* syndicate_public_key ) {
   
   int rc = 0;
   char* return_to = NULL;
   char const* return_to_method = NULL;
   
   ms::ms_openid_provider_reply oid_reply;
   
   // enable the cookie parser, so we can store session information
   curl_easy_setopt( curl, CURLOPT_COOKIEFILE, "/COOKIE" );
   
   // get info for the OpenID provider
   rc = ms_client_openid_begin( curl, username, ms_openid_url, &oid_reply, syndicate_public_key );
   
   if( rc != 0 ) {
      SG_error("ms_client_openid_begin('%s') rc = %d\n", ms_openid_url, rc);
      return rc;
   }

   // authenticate to the OpenID provider
   rc = ms_client_openid_auth( curl, username, password, &oid_reply, &return_to );
   if( rc != 0 ) {
      
      SG_error("ms_client_openid_auth('%s') rc = %d\n", ms_openid_url, rc);
      
      SG_safe_free( return_to );
      return rc;
   }

   return_to_method = oid_reply.redirect_method().c_str();
   
   // complete the authentication with the MS 
   rc = ms_client_openid_complete( curl, return_to_method, return_to, response_buf, response_len );
   SG_safe_free( return_to );
   
   if( rc != 0 ) {
      
      SG_error("ms_client_openid_complete('%s') rc = %d\n", ms_openid_url, rc);
      return rc;
   }
   
   return 0;
}



// do a one-off RPC call via OpenID.
// if syndicate_public_key_pem is non-NULL, use it to verify the authenticity of the response
// rpc_type can be "json" or "xml"
// return 0 on success 
// return -EINVAL if any arguments are invalid
int ms_client_openid_auth_rpc( char const* ms_openid_url, char const* username, char const* password,
                               char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len,
                               char* syndicate_public_key_pem ) {
   
   CURL* curl = NULL;
   
   EVP_PKEY* pubkey = NULL;
   int rc = 0;
   char* ms_openid_url_begin = NULL;
   
   // request
   struct md_upload_buf upload;
   struct curl_slist* headers = NULL;
   
   // reply 
   char* tmp_response = NULL;
   off_t tmp_response_len = 0;
   long http_response = 0;
   long os_error = 0;
   
   memset( &upload, 0, sizeof(upload) );
   
   // sanity check 
   if( strcasecmp( rpc_type, "json" ) != 0 && strcasecmp( rpc_type, "xml" ) != 0 ) {
      return -EINVAL;
   }
   
   if( syndicate_public_key_pem != NULL ) {
      
      // load the public key 
      rc = md_load_pubkey( &pubkey, syndicate_public_key_pem );
   
      if( rc != 0 ) {
         
         SG_error("Failed to load Syndicate public key, md_load_pubkey rc = %d\n", rc );
         return -EINVAL;
      }
   }
   
   ms_openid_url_begin = SG_CALLOC( char, strlen(ms_openid_url) + strlen("/begin") + 1 );
   if( ms_openid_url_begin == NULL ) {
      
      if( pubkey != NULL ) {
         EVP_PKEY_free( pubkey );
      }
      return -ENOMEM;
   }
   
   sprintf( ms_openid_url_begin, "%s/begin", ms_openid_url );
   
   // start curl 
   curl = curl_easy_init();
   if( curl == NULL ) {
      
      if( pubkey != NULL ) {
         EVP_PKEY_free( pubkey );
         pubkey = NULL;
      }
      
      SG_safe_free( ms_openid_url_begin );
      
      return -ENOMEM;
   }
   
   // TODO: elegant way to avoid hard constants?
   md_init_curl_handle2( curl, NULL, 30, true );
   
   rc = ms_client_openid_session( curl, ms_openid_url_begin, username, password, response_buf, response_len, pubkey );
   
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   
   SG_safe_free( ms_openid_url_begin );
   
   if( pubkey ) {
      EVP_PKEY_free( pubkey );
      pubkey = NULL;
   }
   
   if( rc != 0 ) {
      
      SG_error("ms_client_openid_session('%s') rc = %d\n", ms_openid_url, rc );
      curl_easy_cleanup( curl );
      return rc;
   }
   
   // set the body contents
   upload.text = request_buf;
   upload.len = request_len;
   upload.offset = 0;
   
   curl_easy_setopt(curl, CURLOPT_POST, 1L );
   curl_easy_setopt(curl, CURLOPT_URL, ms_openid_url);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_buf);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, request_len);
   
   // what kind of RPC?
   if( strcasecmp( rpc_type, "json" ) == 0 ) {
      // json rpc 
      headers = curl_slist_append( headers, "content-type: application/json" );
   }
   else if( strcasecmp( rpc_type, "xml" ) == 0 ) {
      // xml rpc
      headers = curl_slist_append( headers, "content-type: application/xml" );
   }
   
   curl_easy_setopt( curl, CURLOPT_HTTPHEADER, headers );
   
   rc = md_download_file( curl, &tmp_response, &tmp_response_len );
   
   curl_easy_setopt( curl, CURLOPT_HTTPHEADER, NULL );
   curl_slist_free_all( headers );
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &os_error );

   if( rc != 0 ) {
      
      SG_error("md_download_file(%s) rc = %d\n", ms_openid_url, rc );
      rc = ms_client_download_interpret_errors( ms_openid_url, http_response, rc, -os_error );
   }
   
   curl_easy_cleanup( curl );
   
   if( rc == 0 ) {
      *response_buf = tmp_response;
      *response_len = tmp_response_len;
   }
   
   return rc;
}

// OpenID RPC, but don't verify 
int ms_client_openid_rpc( char const* ms_openid_url, char const* username, char const* password,
                          char const* rpc_type, char const* request_buf, size_t request_len, char** response_buf, size_t* response_len ) {
   
   SG_warn("%s", "will not verify RPC result from Syndicate MS\n");
   return ms_client_openid_auth_rpc( ms_openid_url, username, password, rpc_type, request_buf, request_len, response_buf, response_len, NULL );
}
