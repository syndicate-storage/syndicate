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

#define MD_DEFAULT_CIPHER EVP_aes_256_cbc

extern "C" {

int md_crypt_init();
int md_crypt_shutdown();

int md_openssl_thread_setup(void);
int md_openssl_thread_cleanup(void);

int md_init_OpenSSL(void);
int md_openssl_error(void);

int md_read_urandom( char* buf, size_t len );

int md_load_pubkey( EVP_PKEY** key, char const* pubkey_str, size_t len );
int md_load_privkey( EVP_PKEY** key, char const* privkey_str, size_t len );
int md_load_public_and_private_keys( EVP_PKEY** _pubkey, EVP_PKEY** _privkey, char const* privkey_str );

int md_public_key_from_private_key( EVP_PKEY** ret_pubkey, EVP_PKEY* privkey );
int md_generate_key( EVP_PKEY** key );
long md_dump_pubkey( EVP_PKEY* pkey, char** buf );

int md_sign_message( EVP_PKEY* pkey, char const* data, size_t len, char** sigb64, size_t* sigb64len );
int md_sign_message_raw( EVP_PKEY* pkey, char const* data, size_t len, char** sig, size_t* siglen );

int md_verify_signature( EVP_PKEY* public_key, char const* data, size_t len, char* sigb64, size_t sigb64len );
int md_verify_signature_raw( EVP_PKEY* public_key, char const* data, size_t len, char* sig, size_t sig_len );

int md_encrypt( EVP_PKEY* sender_pkey, EVP_PKEY* receiver_pubkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );
int md_encrypt_pem( char const* sender_pkey_pem, char const* receiver_pubkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );     // for python

int md_decrypt( EVP_PKEY* sender_pubkey, EVP_PKEY* receiver_pkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );
int md_decrypt_pem( char const* sender_pubkey_pem, char const* receiver_pkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len );     // for python

int md_encrypt_symmetric( unsigned char const* key, size_t key_len, char* data, size_t data_len, char** ciphertext, size_t* ciphertext_len );
size_t md_encrypt_symmetric_ciphertext_len( size_t data_len );

// for python, which can't mlock
int md_decrypt_symmetric( unsigned char const* key, size_t key_len, char* ciphertext_data, size_t ciphertext_len, char** data, size_t* data_len );
size_t md_decrypt_symmetric_plaintext_len( size_t ciphertext_len );

int md_encrypt_symmetric_ex( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* data, size_t data_len, char** ciphertext, size_t* ciphertext_len );
size_t md_encrypt_symmetric_ex_ciphertext_len( size_t data_len );

// for python, which can't mlock
int md_decrypt_symmetric_ex( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* ciphertext_data, size_t ciphertext_len, char** data, size_t* data_len ); 
size_t md_decrypt_symmetric_ex_plaintext_len( size_t ciphertext_len );

}


// signature verifier
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// NOTE:  class T should be a protobuf, and should have a string signature field
// return 0 on successful verification 
// return -ENOMEM on OOM 
// return -EINVAL if the signature length is invalid
// return -EINVAL if the signature itself does not match
template <class T> int md_verify( EVP_PKEY* pkey, T* protobuf ) {
   // get the signature
   size_t sigb64_len = protobuf->signature().size();
   int rc = 0;
   
   if( sigb64_len == 0 ) {
      // malformed message
      SG_error("%s\n", "invalid signature length");
      return -EINVAL;
   }
   
   char* sigb64 = SG_CALLOC( char, sigb64_len + 1 );
   if( sigb64 == NULL ) {
      return -ENOMEM;
   }
   
   memcpy( sigb64, protobuf->signature().data(), sigb64_len );
   
   try {
      protobuf->set_signature( "" );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( sigb64 );
      return -ENOMEM;
   }
   
   string bits;
   try {
      protobuf->SerializeToString( &bits );
   }
   catch( exception e ) {
      try {
         // revert
         protobuf->set_signature( string(sigb64) );
         rc = -EINVAL;
      }
      catch( bad_alloc& ba ) {
         rc = -ENOMEM;
      }
      
      free( sigb64 );
      return rc;
   }
   
   // verify the signature
   rc = md_verify_signature( pkey, bits.data(), bits.size(), sigb64, sigb64_len );
   
   // revert
   try {
      protobuf->set_signature( string(sigb64) );
   }
   catch( bad_alloc& ba ) {
      SG_safe_free( sigb64 );
      return -ENOMEM;
   }
   
   SG_safe_free( sigb64 );

   if( rc != 0 ) {
      SG_error("md_verify_signature rc = %d\n", rc );
   }

   return rc;
}


// signature generator
// have to put this here, since C++ forbids separating the declaration and definition of template functions across multiple files???
// NOTE: class T should be a protobuf, and should have a string signature field 
template <class T> int md_sign( EVP_PKEY* pkey, T* protobuf ) {
   
   try {
      protobuf->set_signature( "" );
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   string bits;
   bool valid;
   
   try {
      valid = protobuf->SerializeToString( &bits );
   }
   catch( exception e ) {
      SG_error("%s", "failed to serialize update set\n");
      return -EINVAL;
   }

   if( !valid ) {
      SG_error("%s", "failed to serialize update set\n");
      return -EINVAL;
   }
   
   // sign this message
   char* sigb64 = NULL;
   size_t sigb64_len = 0;

   int rc = md_sign_message( pkey, bits.data(), bits.size(), &sigb64, &sigb64_len );
   if( rc != 0 ) {
      
      SG_error("md_sign_message rc = %d\n", rc );
      return rc;
   }

   try {
      protobuf->set_signature( string(sigb64, sigb64_len) );
   }
   catch( bad_alloc& ba ) {
      
      SG_safe_free( sigb64 );
      return -ENOMEM;
   }
   
   SG_safe_free( sigb64 );
   return 0;
}

#endif
