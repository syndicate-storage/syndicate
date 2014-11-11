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

// set a CURL handle's HTTP method, as well as its URL and query string
int ms_client_set_method( CURL* curl, char const* method, char const* url, char const* qs ) {

   curl_easy_setopt( curl, CURLOPT_URL, url );
   
   if( strcmp(method, "POST") == 0 ) {
      curl_easy_setopt( curl, CURLOPT_POST, 1L );

      if( qs )
         curl_easy_setopt( curl, CURLOPT_POSTFIELDS, qs );
   }
   else if( strcmp(method, "GET") == 0 ) {
      curl_easy_setopt( curl, CURLOPT_HTTPGET, 1L );
   }
   else {
      errorf("Invalid HTTP method '%s'\n", method );
      return -EINVAL;
   }
   return 0;
}


// read an OpenID reply from the MS
int ms_client_load_openid_reply( ms::ms_openid_provider_reply* oid_reply, char* openid_redirect_reply_bits, size_t openid_redirect_reply_bits_len, EVP_PKEY* syndicate_public_key ) {
   // get back the OpenID provider reply
   string openid_redirect_reply_bits_str = string( openid_redirect_reply_bits, openid_redirect_reply_bits_len );

   bool valid = oid_reply->ParseFromString( openid_redirect_reply_bits_str );
   if( !valid ) {
      errorf("Invalid MS OpenID provider reply (missing %s)\n", oid_reply->InitializationErrorString().c_str() );
      return -EINVAL;
   }
   
   if( syndicate_public_key != NULL ) {
      int rc = md_verify< ms::ms_openid_provider_reply >( syndicate_public_key, oid_reply );
      if( rc != 0 ) {
         errorf("%s", "Signature mismatch in OpenID provider reply\n");
         return -EINVAL;
      }
   }
   else {
      errorf("%s", "WARN: No Syndicate public key given.  Relying on TLS to guarantee authenticity of OpenID reply from MS\n" );
   }
   
   return 0;
}


// redirect parser
static size_t ms_client_redirect_header_func( void *ptr, size_t size, size_t nmemb, void *userdata) {
   response_buffer_t* rb = (response_buffer_t*)userdata;

   size_t len = size * nmemb;

   char* data = (char*)ptr;

   // only get one Location header
   if( rb->size() > 0 ) {
      return len;
   }
   
   char* data_str = CALLOC_LIST( char, len + 1 );
   strncpy( data_str, data, len );

   off_t off = md_header_value_offset( data_str, len, "Location" );
   if( off > 0 ) {

      char* value = data_str + off;
      size_t value_len = len - off;
      
      char* value_str = CALLOC_LIST(char, value_len );
      strncpy( value_str, value, value_len - 2 );     // strip off '\n\r'

      rb->push_back( buffer_segment_t( value_str, value_len ) );
   }

   free( data_str );
   
   return len;
}


// dummy CURL read
size_t ms_client_dummy_read( void *ptr, size_t size, size_t nmemb, void *userdata ) {
   return size * nmemb;
}

// read from an md_post_buf
size_t ms_client_read_upload_buf( void* ptr, size_t size, size_t nmemb, void* userdata ) {
   struct md_upload_buf* buf = (struct md_upload_buf*)userdata;
   
   size_t len = size * nmemb;
   size_t to_copy = MAX( len, (size_t)(buf->len - buf->offset) );
   
   memcpy( ptr, buf->text + buf->offset, to_copy );
   
   return to_copy;
}

// dummy CURL write
size_t ms_client_dummy_write( char *ptr, size_t size, size_t nmemb, void *userdata) {
   return size * nmemb;
}


// begin the authentication process.  Ask to be securely redirected from the MS to the OpenID provider.
// on success, populate the ms_openid_provider_reply structure with the information needed to proceed with the OpenID authentication
int ms_client_openid_begin( CURL* curl, char const* username, char const* begin_url, ms::ms_openid_provider_reply* oid_reply, EVP_PKEY* syndicate_public_key ) {

   // url-encode the username
   char* username_encoded = md_url_encode( username, strlen(username) );
   
   // post arguments the MS expects
   char* post = CALLOC_LIST( char, strlen(MS_OPENID_USERNAME_FIELD) + 1 + strlen(username_encoded) + 1 );
   sprintf( post, "%s=%s", MS_OPENID_USERNAME_FIELD, username_encoded );
   
   free( username_encoded );

   response_buffer_t rb;      // will hold the OpenID provider reply
   response_buffer_t header_rb;
   
   curl_easy_setopt( curl, CURLOPT_URL, begin_url );
   curl_easy_setopt( curl, CURLOPT_POST, 1L );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, post );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void*)&rb );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, md_get_callback_response_buffer );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );

   int rc = curl_easy_perform( curl );

   long http_response = 0;
   
   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, NULL );

   free( post );

   if( rc != 0 ) {
      long err = 0;
   
      // get the errno
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      return err;
   }

   if( http_response != 200 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      return -http_response;
   }

   if( rb.size() == 0 ) {
      errorf("%s", "no response\n");
      response_buffer_free( &rb );
      return -ENODATA;
   }

   char* response = response_buffer_to_string( &rb );
   size_t len = response_buffer_size( &rb );

   rc = ms_client_load_openid_reply( oid_reply, response, len, syndicate_public_key );

   free( response );
   response_buffer_free( &rb );

   return rc;
}


// authenticate to the OpenID provider.
// populate the return_to_method (GET|POST) and return_to (URL) strings
int ms_client_openid_auth( CURL* curl, char const* username, char const* password, ms::ms_openid_provider_reply* oid_reply, char** return_to ) {

   char* post = NULL;
   char const* openid_redirect_url = oid_reply->redirect_url().c_str();
   long http_response = 0;
   
   // how we ask the OID provider to challenge us
   char const* challenge_method = oid_reply->challenge_method().c_str();

   // how we respond to the OID provider challenge
   char const* response_method = oid_reply->response_method().c_str();

   dbprintf("%s challenge to %s\n", challenge_method, openid_redirect_url );

   response_buffer_t header_rb;
   
   // inform the OpenID provider that we have been redirected by the RP by fetching the authentication page.
   // The OpenID provider may then redirect us back.
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );     // catch 302
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );
   curl_easy_setopt( curl, CURLOPT_READFUNCTION, ms_client_dummy_read );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, ms_client_dummy_write );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_READDATA, NULL );
   // curl_easy_setopt( curl, CURLOPT_VERBOSE, (get_debug_level() > 0 ? 1L: 0L) );

   char* url_and_path = NULL;
   char* url_qs = NULL;
   int rc = md_split_url_qs( openid_redirect_url, &url_and_path, &url_qs );
   if( rc != 0 ) {
      // no query string
      url_and_path = strdup( openid_redirect_url );
   }
   
   rc = ms_client_set_method( curl, challenge_method, url_and_path, url_qs );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", challenge_method, rc );
      return rc;
   }

   rc = curl_easy_perform( curl );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );

   free( url_and_path );
   if( url_qs )
      free( url_qs );

   if( rc != 0 ) {
      
      long err = 0;
      
      // get the errno
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      
      response_buffer_free( &header_rb );
      return -abs(rc);
   }

   if( http_response != 200 && http_response != 302 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      response_buffer_free( &header_rb );
      return -http_response;
   }

   if( http_response == 302 ) {
      // authenticated already; we're being sent back
      char* url = response_buffer_to_string( &header_rb );

      *return_to = url;
      
      response_buffer_free( &header_rb );

      return 0;
   }

   response_buffer_free( &header_rb );

   // authenticate to the OpenID provider
   char const* extra_args = oid_reply->extra_args().c_str();
   char const* username_field = oid_reply->username_field().c_str();
   char const* password_field = oid_reply->password_field().c_str();
   char const* auth_handler = oid_reply->auth_handler().c_str();

   char* username_urlencoded = md_url_encode( username, strlen(username) );
   char* password_urlencoded = md_url_encode( password, strlen(password) );
   post = CALLOC_LIST( char, strlen(username_field) + 1 + strlen(username_urlencoded) + 1 +
                             strlen(password_field) + 1 + strlen(password_urlencoded) + 1 +
                             strlen(extra_args) + 1);

   sprintf(post, "%s=%s&%s=%s&%s", username_field, username_urlencoded, password_field, password_urlencoded, extra_args );

   free( username_urlencoded );
   free( password_urlencoded );

   dbprintf("%s authenticate to %s?%s\n", response_method, auth_handler, post );

   rc = ms_client_set_method( curl, response_method, auth_handler, post );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", response_method, rc );
      return rc;
   }

   // send the authentication request
   curl_easy_setopt( curl, CURLOPT_FOLLOWLOCATION, 0L );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, ms_client_redirect_header_func );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, (void*)&header_rb );
   curl_easy_setopt( curl, CURLOPT_READFUNCTION, ms_client_dummy_read );
   curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, ms_client_dummy_write );
   curl_easy_setopt( curl, CURLOPT_WRITEDATA, NULL );
   curl_easy_setopt( curl, CURLOPT_READDATA, NULL );

   rc = curl_easy_perform( curl );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );
   curl_easy_setopt( curl, CURLOPT_URL, NULL );
   curl_easy_setopt( curl, CURLOPT_WRITEHEADER, NULL );
   curl_easy_setopt( curl, CURLOPT_HEADERFUNCTION, NULL );
   curl_easy_setopt( curl, CURLOPT_POSTFIELDS, NULL );

   free( post );

   if( rc != 0 ) {
      long err = 0;
      
      curl_easy_getinfo( curl, CURLINFO_OS_ERRNO, &err );
      err = -abs(err);
      
      errorf("curl_easy_perform rc = %d, err = %ld\n", rc, err );
      response_buffer_free( &header_rb );
      return -abs(rc);
   }

   if( http_response != 302 ) {
      errorf("curl_easy_perform HTTP status = %ld\n", http_response );
      response_buffer_free( &header_rb );
      return -http_response;
   }

   // authenticated! we're being sent back
   char* url = response_buffer_to_string( &header_rb );
   response_buffer_free( &header_rb );
   
   *return_to = url;
   return 0;
}


// complete the openid authentication.
// if given, submit the request_body to the return_to URL.
int ms_client_openid_complete( CURL* curl, char const* return_to_method, char const* return_to, char** response_body, size_t* response_body_len ) {

   dbprintf("%s return to %s\n", return_to_method, return_to );

   char* return_to_url_and_path = NULL;
   char* return_to_qs = NULL;

   char* bits = NULL;
   ssize_t len = 0;
   long http_response = 0;

   int rc = md_split_url_qs( return_to, &return_to_url_and_path, &return_to_qs );
   if( rc != 0 ) {
      // no qs
      return_to_url_and_path = strdup( return_to );
   }
   
   rc = ms_client_set_method( curl, return_to_method, return_to_url_and_path, return_to_qs );
   if( rc != 0 ) {
      errorf("ms_client_set_method(%s) rc = %d\n", return_to_method, rc );
      free( return_to_url_and_path );
      if( return_to_qs )
         free( return_to_qs );
      
      return rc;
   }
   
   // perform
   rc = md_download_file( curl, &bits, &len );

   curl_easy_getinfo( curl, CURLINFO_RESPONSE_CODE, &http_response );

   if( rc < 0 ) {
      errorf("md_download_file rc = %d\n", rc );
      free( return_to_url_and_path );
      if( return_to_qs ) {
         free( return_to_qs );
      }
      return rc;
   }

   if( http_response != 200 ) {
      errorf("md_download_file HTTP status = %ld\n", http_response );
      free( return_to_url_and_path );
      if( return_to_qs )
         free( return_to_qs );
      return -abs( (int)http_response );
   }

   // success!
   if( response_body != NULL ) {
      *response_body = bits;
   }
   
   if( response_body_len != NULL ) {
      *response_body_len = len;
   }
   
   free( return_to_url_and_path );
   if( return_to_qs )
      free( return_to_qs );

   return 0;
}


// open a session with the MS, authenticating via OpenID.
// an optional download buffer (response_buf, response_len) will hold the MS's initial response from the OpenID authentication (from the "complete" leg).
int ms_client_openid_session( CURL* curl, char const* ms_openid_url, char const* username, char const* password, char** response_buf, size_t* response_len, EVP_PKEY* syndicate_public_key ) {
   
   int rc = 0;
   
   ms::ms_openid_provider_reply oid_reply;
   
   // enable the cookie parser, so we can store session information
   curl_easy_setopt( curl, CURLOPT_COOKIEFILE, "/COOKIE" );
   
   // get info for the OpenID provider
   rc = ms_client_openid_begin( curl, username, ms_openid_url, &oid_reply, syndicate_public_key );
   
   if( rc != 0 ) {
      errorf("ms_client_openid_begin(%s) rc = %d\n", ms_openid_url, rc);
      return rc;
   }

   // authenticate to the OpenID provider
   char* return_to = NULL;
   char const* return_to_method = NULL;
   rc = ms_client_openid_auth( curl, username, password, &oid_reply, &return_to );
   if( rc != 0 ) {
      errorf("ms_client_openid_auth(%s) rc = %d\n", ms_openid_url, rc);
      return rc;
   }

   return_to_method = oid_reply.redirect_method().c_str();
   
   // complete the authentication with the MS 
   rc = ms_client_openid_complete( curl, return_to_method, return_to, response_buf, response_len );
   free( return_to );
   
   if( rc != 0 ) {
      errorf("ms_client_openid_complete(%s) rc = %d\n", ms_openid_url, rc);
      return rc;
   }
   
   return 0;
}
