/*
   Copyright 2013 The Trustees of Princeton University

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

#ifndef _LIBSYNDICATE_CRYPT_H_
#define _LIBSYNDICATE_CRYPT_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/util.h"

#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include <unistd.h>

/* _POSIX_THREADS is normally defined in unistd.h if pthreads are available
   on your platform. */
#define MD_MUTEX_TYPE pthread_mutex_t
#define MD_MUTEX_SETUP(x) pthread_mutex_init(&(x), NULL)
#define MD_MUTEX_CLEANUP(x) pthread_mutex_destroy(&(x))
#define MD_MUTEX_LOCK(x) pthread_mutex_lock(&(x))
#define MD_MUTEX_UNLOCK(x) pthread_mutex_unlock(&(x))
#define MD_THREAD_ID pthread_self( )

extern "C" {

int md_openssl_thread_setup(void);
int md_openssl_thread_cleanup(void);
void md_init_OpenSSL(void);
int md_openssl_error(void);
int md_load_pubkey( EVP_PKEY** key, char const* pubkey_str );
int md_load_privkey( EVP_PKEY** key, char const* privkey_str );
int md_generate_key( EVP_PKEY** key );
long md_dump_pubkey( EVP_PKEY* pkey, char** buf );
int md_sign_message( EVP_PKEY* pkey, char const* data, size_t len, char** sigb64, size_t* sigb64len );
int md_verify_signature( EVP_PKEY* public_key, char const* data, size_t len, char* sigb64, size_t sigb64len );
int md_encrypt( EVP_PKEY* pubkey, char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );
int md_encrypt_pem( char const* pubkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );     // for syntool
int md_decrypt( EVP_PKEY* privkey, char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );
int md_decrypt_pem( char const* privkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );     // for syntool
   
}


// signature verifier
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// NOTE:  class T should be a protobuf, and should have a string signature field
// TODO: verify the signature of the hash of the message, not the whole message?
template <class T> int md_verify( EVP_PKEY* pkey, T* protobuf ) {
   // get the signature
   size_t sigb64_len = protobuf->signature().size();
   
   if( sigb64_len == 0 ) {
      // malformed message
      errorf("%s\n", "invalid signature length");
      return -EINVAL;
   }
   
   char* sigb64 = CALLOC_LIST( char, sigb64_len + 1 );
   if( sigb64 == NULL )
      return -ENOMEM;
   
   memcpy( sigb64, protobuf->signature().data(), sigb64_len );
   
   protobuf->set_signature( "" );

   string bits;
   try {
      protobuf->SerializeToString( &bits );
   }
   catch( exception e ) {
      // revert
      protobuf->set_signature( string(sigb64) );
      free( sigb64 );
      return -EINVAL;
   }
   
   // verify the signature
   int rc = md_verify_signature( pkey, bits.data(), bits.size(), sigb64, sigb64_len );
   
   // revert
   protobuf->set_signature( string(sigb64) );
   free( sigb64 );

   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}


// signature generator
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// NOTE: class T should be a protobuf, and should have a string signature field 
// TODO: sign the hash of the message, not the whole message?
template <class T> int md_sign( EVP_PKEY* pkey, T* protobuf ) {
   protobuf->set_signature( "" );

   string bits;
   bool valid;
   
   try {
      valid = protobuf->SerializeToString( &bits );
   }
   catch( exception e ) {
      errorf("%s", "failed to serialize update set\n");
      return -EINVAL;
   }

   if( !valid ) {
      errorf("%s", "failed to serialize update set\n");
      return -EINVAL;
   }
   
   // sign this message
   char* sigb64 = NULL;
   size_t sigb64_len = 0;

   int rc = md_sign_message( pkey, bits.data(), bits.size(), &sigb64, &sigb64_len );
   if( rc != 0 ) {
      errorf("md_sign_message rc = %d\n", rc );
      return rc;
   }

   protobuf->set_signature( string(sigb64, sigb64_len) );
   
   free( sigb64 );
   return 0;
}

#endif
