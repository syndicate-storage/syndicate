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

#include "libsyndicate/crypt.h"

// to help valgrind ignore OpenSSL uninitialized values during debugging
#ifndef _NO_VALGRIND_FIXES
#include "valgrind/memcheck.h"
#endif

////////////////////////////////////////////////////////////////////////////////
// derived from http://www.cs.odu.edu/~cs772/sourcecode/NSwO/compiled/common.c

// set up OpenSSL 
// return 1 on success
// return 0 on failure
int md_init_OpenSSL(void) {
   
   int rc = md_openssl_thread_setup();
   if( rc == 0 ) {
      
      SG_error("md_openssl_thread_setup rc = %d\n", rc );
      return 0;
   }
   
   rc = SSL_library_init();
   if( rc == 0 ) {
      
      SG_error("SSL_library_init() rc = %d\n", rc);
      return 0;
   }
   
   OpenSSL_add_all_digests();
   ERR_load_crypto_strings();
   
   return 1;
}


/* This array will store all of the mutexes available to OpenSSL. */
static MD_MUTEX_TYPE *md_openssl_mutex_buf = NULL ;

// callback to libssl for locking
static void md_ssl_locking_function(int mode, int n, const char * file, int line) {
   
   if (mode & CRYPTO_LOCK) {
      MD_MUTEX_LOCK( md_openssl_mutex_buf[n] );
   }
   
   else {
      MD_MUTEX_UNLOCK( md_openssl_mutex_buf[n] );
   }
}

// callback to libssl for thread id
static unsigned long md_ssl_thread_id_function(void) {
  return ((unsigned long)MD_THREAD_ID);
}

// set up libssl threads
// return 1 on success
// return 0 on failure
int md_openssl_thread_setup(void) {
   
  if( md_openssl_mutex_buf != NULL ) {
     // already initialized
     return 1;
  }
     
  int i;
  md_openssl_mutex_buf = (MD_MUTEX_TYPE *) malloc(CRYPTO_num_locks( ) * sizeof(MD_MUTEX_TYPE));
  if( md_openssl_mutex_buf == NULL ) {
    return 0;
  }
  
  for (i = 0; i < CRYPTO_num_locks(); i++) {
    MD_MUTEX_SETUP( md_openssl_mutex_buf[i] );
  }
  
  CRYPTO_set_id_callback(md_ssl_thread_id_function);
  CRYPTO_set_locking_callback(md_ssl_locking_function);
  
  return 1;
}

// clean up libssl threads 
// return 1 on success
// return 0 on failure 
int md_openssl_thread_cleanup(void) {

   int i;
   if ( md_openssl_mutex_buf == NULL ) {
      return 0;
   }
   
   CRYPTO_set_id_callback(NULL);
   CRYPTO_set_locking_callback(NULL);
   
   for (i = 0; i < CRYPTO_num_locks(); i++) {
      MD_MUTEX_CLEANUP( md_openssl_mutex_buf[i] );
   }
   
   free(md_openssl_mutex_buf);
   md_openssl_mutex_buf = NULL;
   return 1;
}

////////////////////////////////////////////////////////////////////////////////

static int urandom_fd = -1;     // /dev/urandom
static int inited = 0;

// initialize crypto libraries and set up state
// return 0 on success
// return -EPERM if we failed to set up OpenSSL
// return -errno if we failed to open /dev/urandom (see open(2))
// NOTE: this is not thread-safe!
int md_crypt_init() {
   SG_debug("%s\n", "starting up");
   
   int rc = md_init_OpenSSL();
   if( rc == 0 ) {
      
      SG_error("md_init_OpenSSL() rc = %d\n", rc );
      return -EPERM;
   }
   
   urandom_fd = open("/dev/urandom", O_RDONLY );
   if( urandom_fd < 0 ) {
      
      int errsv = -errno;
      SG_error("open('/dev/urandom') rc = %d\n", errsv);
      return errsv;
   }
   
   inited = 1;
   
   return 0;
}

// shut down crypto libraries and free state
// always succeeds
int md_crypt_shutdown() {
   
   SG_debug("%s\n", "shutting down");
   
   if( urandom_fd >= 0 ) {
      close( urandom_fd );
      urandom_fd = -1;
   }
   
   // shut down OpenSSL
   ERR_free_strings();
   CRYPTO_cleanup_all_ex_data();
   
   SG_debug("%s\n", "crypto thread shutdown" );
   md_openssl_thread_cleanup();
   
   return 0;
}


// check crypt initialization
// return 1 if initialized
// return 0 if not.
int md_crypt_check_init() {
   if( inited == 0 ) {
      return 0;
   }
   else {
      return 1;
   }
}

// read bytes from /dev/urandom
// return 0 on success (i.e. read len bytes)
// return -EINVAL if the crypto hasn't been initialized 
// return -errno if we failed to read
int md_read_urandom( char* buf, size_t len ) {
   if( urandom_fd < 0 ) {
      
      SG_error("%s", "crypto is not initialized\n");
      return -EINVAL;
   }
   
   ssize_t nr = 0;
   size_t num_read = 0;
   while( num_read < len ) {
      
      nr = read( urandom_fd, buf + num_read, len - num_read );
      if( nr < 0 ) {
         
         int errsv = -errno;
         SG_error("read(/dev/urandom) errno %d\n", errsv);
         return errsv;
      }
      
      num_read += nr;
   }
   
   return 0;
}


// print an OpenSSL error message
int md_openssl_error() {
   unsigned long err = ERR_get_error();
   char buf[4096];

   ERR_error_string_n( err, buf, 4096 );
   SG_error("OpenSSL error %ld: %s\n", err, buf );
   return 0;
}

// verify a message, given a base64-encoded signature
// return 0 on success
// return -EINVAL if we failed to parse the buffer
// return -EBADMSG if we failed to verify the digest
int md_verify_signature_raw( EVP_PKEY* public_key, char const* data, size_t len, char* sig_bin, size_t sig_bin_len ) {
      
   const EVP_MD* sha256 = EVP_sha256();

   EVP_MD_CTX *mdctx = EVP_MD_CTX_create();
   EVP_PKEY_CTX* pkey_ctx = NULL;
   int rc = 0;
   
   rc = EVP_DigestVerifyInit( mdctx, &pkey_ctx, sha256, NULL, public_key );
   if( rc <= 0 ) {
      
      SG_error("EVP_DigestVerifyInit_ex( %p ) rc = %d\n", public_key, rc);
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // set up padding
   
   // activate PSS
   rc = EVP_PKEY_CTX_set_rsa_padding( pkey_ctx, RSA_PKCS1_PSS_PADDING );
   if( rc <= 0 ) {
      
      SG_error( "EVP_PKEY_CTX_set_rsa_padding rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // use maximum possible salt length, as per http://wiki.openssl.org/index.php/Manual:EVP_PKEY_CTX_ctrl(3).
   // This is only because PyCrypto (used by the MS) does this in its PSS implementation.
   rc = EVP_PKEY_CTX_set_rsa_pss_saltlen( pkey_ctx, -1 );
   if( rc <= 0 ) {
      
      SG_error( "EVP_PKEY_CTX_set_rsa_pss_saltlen rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   rc = EVP_DigestVerifyUpdate( mdctx, (void*)data, len );
   if( rc <= 0 ) {
      
      SG_error("EVP_DigestVerifyUpdate rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   rc = EVP_DigestVerifyFinal( mdctx, (unsigned char*)sig_bin, sig_bin_len );
   if( rc <= 0 ) {
      
      SG_error("EVP_DigestVerifyFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EBADMSG;
   }

   EVP_MD_CTX_destroy( mdctx );
   
   return 0;
}

// verify a message with base64 signature
// if it hasn't been initialized yet, initialize the crypto subsystems
// return 0 on success
// return negative if we failed to set up the crypto subsystem (if it was not initialized)
// return -EINVAL if the message could not be decoded
// return -ENOMEM if we ran out of memory
// return -errno on failure to initialize the crypto subsystem, if it had not been initialized already
int md_verify_signature( EVP_PKEY* pubkey, char const* data, size_t len, char* sigb64, size_t sigb64_len ) {
   
   // safety for external clients
   if( !md_crypt_check_init() ) {
      int rc = md_crypt_init();
      if( rc != 0 ) {
         
         SG_error("md_crypt_init rc = %d\n", rc );
         return rc;
      }
   }
   
   char* sig_bin = NULL;
   size_t sig_bin_len = 0;

   // SG_debug("VERIFY: message len = %zu, strlen(sigb64) = %zu, sigb64 = %s\n", len, strlen(sigb64), sigb64 );

   int rc = md_base64_decode( sigb64, sigb64_len, &sig_bin, &sig_bin_len );
   if( rc != 0 ) {
      
      SG_error("md_base64_decode rc = %d\n", rc );
      return rc;
   }
   
   rc = md_verify_signature_raw( pubkey, data, len, sig_bin, sig_bin_len );
   
   SG_safe_free( sig_bin );
   return rc;
}


// sign a message
int md_sign_message_raw( EVP_PKEY* pkey, char const* data, size_t len, char** sig, size_t* siglen ) {

   // sign this with SHA256
   const EVP_MD* sha256 = EVP_sha256();

   EVP_MD_CTX *mdctx = EVP_MD_CTX_create();

   //int rc = EVP_SignInit( mdctx, sha256 );
   EVP_PKEY_CTX* pkey_ctx = NULL;
   int rc = EVP_DigestSignInit( mdctx, &pkey_ctx, sha256, NULL, pkey );
   
   if( rc <= 0 ) {
      SG_error("EVP_DigestSignInit rc = %d\n", rc);
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // set up padding
   
   // activate PSS
   rc = EVP_PKEY_CTX_set_rsa_padding( pkey_ctx, RSA_PKCS1_PSS_PADDING );
   if( rc <= 0 ) {
      SG_error( "EVP_PKEY_CTX_set_rsa_padding rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // use salt length == digest_length, as per http://wiki.openssl.org/index.php/Manual:EVP_PKEY_CTX_ctrl(3).
   // This is only because PyCrypto (used by the MS) does this in its PSS implementation.
   rc = EVP_PKEY_CTX_set_rsa_pss_saltlen( pkey_ctx, -1 );
   if( rc <= 0 ) {
      SG_error( "EVP_PKEY_CTX_set_rsa_pss_saltlen rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   rc = EVP_DigestSignUpdate( mdctx, (void*)data, len );
   if( rc <= 0 ) {
      SG_error("EVP_DigestSignUpdate rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // get signature size
   size_t sig_bin_len = 0;
   rc = EVP_DigestSignFinal( mdctx, NULL, &sig_bin_len );
   if( rc <= 0 ) {
      SG_error("EVP_DigestSignFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // allocate the signature, but leave some room just in case
   unsigned char* sig_bin = SG_CALLOC( unsigned char, sig_bin_len );
   if( sig_bin == NULL ) {
      EVP_MD_CTX_destroy( mdctx );
      return -ENOMEM;
   }

   rc = EVP_DigestSignFinal( mdctx, sig_bin, &sig_bin_len );
   if( rc <= 0 ) {
      SG_error("EVP_DigestSignFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   *sig = (char*)sig_bin;
   *siglen = sig_bin_len;
   
   EVP_MD_CTX_destroy( mdctx );
   return 0;
}

   
// sign a message, producing the base64 signature
// return 0 on success
// return -ENOMEM on OOM 
// return -1 on failure to sign 
// return -errno on failure to initialize the crypto subsystem, if it had not been initialized already
int md_sign_message( EVP_PKEY* pkey, char const* data, size_t len, char** sigb64, size_t* sigb64_len ) {
   
   int rc = 0;
   
   // safety for external clients
   if( !md_crypt_check_init() ) {
      rc = md_crypt_init();
      if( rc != 0 ) {
         
         SG_error("md_crypt_init rc = %d\n", rc );
         return rc;
      }
   }
   
   unsigned char* sig_bin = NULL;
   size_t sig_bin_len = 0;
   
   rc = md_sign_message_raw( pkey, data, len, (char**)&sig_bin, &sig_bin_len );
   if( rc != 0 ) {
      SG_error("md_sign_message_raw rc = %d\n", rc );
      return -1;
   }
   
   // convert to base64
   char* b64 = NULL;
   rc = md_base64_encode( (char*)sig_bin, sig_bin_len, &b64 );
   if( rc != 0 ) {
      
      SG_error("md_base64_encode rc = %d\n", rc );
      md_openssl_error();
      free( sig_bin );
      return rc;
   }

   *sigb64 = b64;
   *sigb64_len = strlen(b64);
   
   // SG_debug("SIGN: message len = %zu, sigb64_len = %zu, sigb64 = %s\n", len, strlen(b64), b64 );

   free( sig_bin );
   
   return 0;
}


// load a PEM-encoded (RSA) public key into an EVP key
// return 0 on success
// return -EINVAL if the public key could not be loaded
int md_load_pubkey( EVP_PKEY** key, char const* pubkey_str, size_t pubkey_len ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)pubkey_str, pubkey_len );

   EVP_PKEY* public_key = PEM_read_bio_PUBKEY( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( public_key == NULL ) {
      // invalid public key
      SG_error("%s", "failed to read public key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = public_key;
   
   return 0;
}


// load a PEM-encoded (RSA) private key into an EVP key
// return 0 on success 
// return -EINVAL onfailure to load the private key 
int md_load_privkey( EVP_PKEY** key, char const* privkey_str, size_t privkey_len ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)privkey_str, privkey_len );

   EVP_PKEY* privkey = PEM_read_bio_PrivateKey( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( privkey == NULL ) {
      // invalid private key
      SG_error("%s", "failed to read private key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = privkey;

   return 0;
}

// load both public and private keys from an RSA private key into EVP key structures
// return 0 on success 
// return -EINVAL on failure to load either key 
int md_load_public_and_private_keys( EVP_PKEY** _pubkey, EVP_PKEY** _privkey, char const* privkey_str ) {
   BIO* buf_priv_io = BIO_new_mem_buf( (void*)privkey_str, strlen(privkey_str) + 1 );
   
   EVP_PKEY* privkey = PEM_read_bio_PrivateKey( buf_priv_io, NULL, NULL, NULL );
   
   BIO_free_all( buf_priv_io );

   if( privkey == NULL ) {
      // invalid private key
      SG_error("%s", "ERR: failed to read private key\n");
      md_openssl_error();

      return -EINVAL;
   }
   
   // get the public part 
   char* pubkey_pem = NULL;
   long sz = md_dump_pubkey( privkey, &pubkey_pem );
   if( sz < 0 ) {
      SG_error("md_dump_pubkey rc = %ld\n", sz );
      
      return -EINVAL;
   }
   
   // load it 
   BIO* buf_pub_io = BIO_new_mem_buf( (void*)pubkey_pem, sz + 1 );
   EVP_PKEY* pubkey = PEM_read_bio_PUBKEY( buf_pub_io, NULL, NULL, NULL );
   
   BIO_free_all( buf_pub_io );
   free( pubkey_pem );
   
   if( pubkey == NULL ) {
      // invalid public key 
      SG_error("%s", "ERR: failed to read public key\n");
      md_openssl_error();
      
      return -EINVAL;
   }
   
   *_privkey = privkey;
   *_pubkey = pubkey;

   return 0;
}


// get the RSA public key from the RSA private key 
// return -EINVAL on failure to serialize the private key, or failure to load the public key from it
int md_public_key_from_private_key( EVP_PKEY** ret_pubkey, EVP_PKEY* privkey ) {
   
   // get the public part 
   char* pubkey_pem = NULL;
   long sz = md_dump_pubkey( privkey, &pubkey_pem );
   if( sz < 0 ) {
      SG_error("md_dump_pubkey rc = %ld\n", sz );
      
      return -EINVAL;
   }
   
   // load it 
   BIO* buf_pub_io = BIO_new_mem_buf( (void*)pubkey_pem, sz + 1 );
   EVP_PKEY* pubkey = PEM_read_bio_PUBKEY( buf_pub_io, NULL, NULL, NULL );
   
   BIO_free_all( buf_pub_io );
   free( pubkey_pem );
   
   if( pubkey == NULL ) {
      // invalid public key 
      SG_error("%s", "ERR: failed to read public key\n");
      md_openssl_error();
      
      return -EINVAL;
   }
   
   *ret_pubkey = pubkey;
   return 0;
}

// generate RSA public/private key pair
// return 0 on success
// return -1 on failure 
int md_generate_key( EVP_PKEY** key ) {

   SG_debug("%s", "Generating public/private key...\n");
   
   EVP_PKEY_CTX *ctx;
   EVP_PKEY *pkey = NULL;
   
   ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
   if( ctx == NULL ) {
      md_openssl_error();
      return -1;
   }

   int rc = EVP_PKEY_keygen_init( ctx );
   if( rc <= 0 ) {
      md_openssl_error();
      EVP_PKEY_CTX_free( ctx );
      return rc;
   }

   rc = EVP_PKEY_CTX_set_rsa_keygen_bits( ctx, SG_RSA_KEY_SIZE );
   if( rc <= 0 ) {
      md_openssl_error();
      EVP_PKEY_CTX_free( ctx );
      return rc;
   }

   rc = EVP_PKEY_keygen( ctx, &pkey );
   if( rc <= 0 ) {
      md_openssl_error();
      EVP_PKEY_CTX_free( ctx );
      return rc;
   }

   *key = pkey;
   EVP_PKEY_CTX_free( ctx );
   return 0;
}


// dump a key to memory as a PEM-encoded string
// return -EINVAL on failure to serialize the key 
// return -ENOMEM on OOM
long md_dump_pubkey( EVP_PKEY* pkey, char** buf ) {
   BIO* mbuf = BIO_new( BIO_s_mem() );
   
   int rc = PEM_write_bio_PUBKEY( mbuf, pkey );
   if( rc <= 0 ) {
      SG_error("PEM_write_bio_PUBKEY rc = %d\n", rc );
      md_openssl_error();
      return -EINVAL;
   }

   (void) BIO_flush( mbuf );
   
   char* tmp = NULL;
   long sz = BIO_get_mem_data( mbuf, &tmp );

   *buf = SG_CALLOC( char, sz );
   if( *buf == NULL ) {
      
      BIO_free( mbuf );
      return -ENOMEM;
   }
   
   memcpy( *buf, tmp, sz );

   BIO_free( mbuf );
   
   return sz;
}


// Seal data with a given public key, using AES256 in CBC mode.
// Sign the encrypted data with the given public key.
// return 0 on success 
// return -1 on failure to use an OpenSSL method 
// return -errno on failure to read random data
// return -ENOMEM on OOM 
// return -EOVERFLOW if the outputted message or any of its fields would be too long 
// return -ERANGE if the message is greater than 2**30 bytes long
int md_encrypt( EVP_PKEY* sender_pkey, EVP_PKEY* receiver_pubkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   
   // use AES256 in CBC mode
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   size_t block_size = EVP_CIPHER_block_size( cipher );
   
   // initialization vector
   int32_t iv_len = EVP_CIPHER_iv_length( cipher );
   unsigned char* iv = (unsigned char*)alloca( iv_len );
   
   if( iv == NULL ) {
      return -ENOMEM;
   }
   
   // size check 
   if( (uint64_t)in_data_len >= (1L << 30) ) {
      return -ERANGE;
   }
   
   // fill the iv with random data
   int rc = md_read_urandom( (char*)iv, iv_len );
   if( rc != 0 ) {
      SG_error("md_read_urandom rc = %d\n", rc);
      return rc;
   }
   
   // set up cipher 
   EVP_CIPHER_CTX ctx;
   EVP_CIPHER_CTX_init( &ctx );
   
   // encrypted symmetric key for sealing the ciphertext
   unsigned char* ek = SG_CALLOC( unsigned char, EVP_PKEY_size( receiver_pubkey ) );
   if( ek == NULL ) {
      return -ENOMEM;
   }
   
   int32_t ek_len = 0;
   
   // set up EVP Sealing
   rc = EVP_SealInit( &ctx, cipher, &ek, &ek_len, iv, &receiver_pubkey, 1 );
   if( rc == 0 ) {
      SG_error("EVP_SealInit rc = %d\n", rc );
      md_openssl_error();
      
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // allocate the output buffer
   // final output format:
   //    signature_len || iv_len || ek_len || ciphertext_len || iv || ek || ciphertext || signature
   // for now, the signature will be missing (but will be tacked on the end), since it covers all preceding fields
   
   // allocate all but the signature for now, since we don't know yet how big it will be.
   // allocate space for its length, however!
   int32_t output_len = sizeof(int32_t) + sizeof(iv_len) + sizeof(ek_len) + sizeof(int32_t) + iv_len + ek_len + (in_data_len + block_size);
   if( output_len < 0 ) {
      // overflow 
      return -EOVERFLOW;
   }
   
   unsigned char* output_buf = SG_CALLOC( unsigned char, output_len );
   if( output_buf == NULL ) {
      
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -ENOMEM;
   }
   
   size_t signature_len_offset  = 0;
   size_t iv_len_offset         = signature_len_offset + sizeof(int32_t);
   size_t ek_len_offset         = iv_len_offset + sizeof(int32_t);
   size_t ciphertext_len_offset = ek_len_offset + sizeof(int32_t);
   size_t iv_offset             = ciphertext_len_offset + sizeof(int32_t);
   size_t ek_offset             = iv_offset + iv_len;
   size_t ciphertext_offset     = ek_offset + ek_len;
   
   unsigned char* ciphertext = output_buf + ciphertext_offset;
   int32_t ciphertext_len = 0;
   
   // encrypt!
   rc = EVP_SealUpdate( &ctx, ciphertext, &ciphertext_len, (unsigned char const*)in_data, in_data_len );
   if( rc == 0 ) {
      SG_error("EVP_SealUpdate rc = %d\n", rc );
      md_openssl_error();
      
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // finalize
   int tmplen = 0;
   rc = EVP_SealFinal( &ctx, ciphertext + ciphertext_len, &tmplen );
   if( rc == 0 ) {
      SG_error("EVP_SealFinal rc = %d\n", rc );
      md_openssl_error();
      
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   ciphertext_len += tmplen;
   
   // clean up
   EVP_CIPHER_CTX_cleanup( &ctx );
   
   // populate the output buffer with the encrypted key, iv, and their sizes.  Then sign everything.
   int32_t iv_len_n = htonl( iv_len );
   int32_t ek_len_n = htonl( ek_len );
   int32_t ciphertext_len_n = htonl( ciphertext_len );
   
   memcpy( output_buf + iv_len_offset, &iv_len_n, sizeof(iv_len_n) );
   memcpy( output_buf + ek_len_offset, &ek_len_n, sizeof(ek_len_n) );
   memcpy( output_buf + ciphertext_len_offset, &ciphertext_len_n, sizeof(ciphertext_len_n) );
   
   memcpy( output_buf + iv_offset, iv, iv_len );
   memcpy( output_buf + ek_offset, ek, ek_len );
   // NOTE: ciphertext is already in place
   
   // sign:  iv_len || ek_len || ciphertext_len || iv || ek || ciphertext
   size_t sig_payload_len = sizeof(iv_len_n) + sizeof(ek_len_n) + sizeof(ciphertext_len_n) + iv_len + ek_len + ciphertext_len;
   if( (int32_t)(sig_payload_len) < 0 ) {
      // overflow 
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      return -EOVERFLOW;
   }
   
   size_t sig_payload_offset = iv_len_offset;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   rc = md_sign_message_raw( sender_pkey, (char*)(output_buf + sig_payload_offset), sig_payload_len, (char**)&sig, &sig_len );
   if( rc != 0 ) {
      SG_error("md_sign_message rc = %d\n", rc );
      
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      return -1;
   }
   
   // add space for the signature
   int32_t full_output_len = sizeof(int32_t) + sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + iv_len + ek_len + ciphertext_len + sig_len;
   if( full_output_len < 0 ) {
      // overflow
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      return -EOVERFLOW;
   }
   
   unsigned char* signed_output = (unsigned char*)realloc( output_buf, full_output_len );
   if( signed_output == NULL ) {
      // out of memory
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( output_buf );
      return -ENOMEM;
   }
   
   // add the signature and its length
   int32_t sig_len_n = htonl( sig_len );
   size_t signature_offset = ciphertext_offset + ciphertext_len;        // signature goes after ciphertext
   
   if( (int32_t)(signature_offset) < 0 ) {
      // overflow 
      memset( ek, 0, EVP_PKEY_size( receiver_pubkey ) );
      SG_safe_free( ek );
      
      free( signed_output );
      return -EOVERFLOW;
   }
   
   memcpy( signed_output + signature_len_offset, &sig_len_n, sizeof(sig_len_n) );
   memcpy( signed_output + signature_offset, sig, sig_len );
   
   // return data
   *out_data = (char*)signed_output;
   *out_data_len = full_output_len;
   
   // printf("Header: (iv_len = %d, ek_ken = %d, ciphertext_len = %d, signature_len = %zd); encrypted %zd bytes to %d bytes\n", iv_len, ek_len, ciphertext_len, sig_len, in_data_len, full_output_len );
   // fflush(stdout);
   
   free( ek );
   
   return 0;
}


// decrypt data with a given private key, generated from md_encrypt
// check the signature BEFORE decrypting!
// return 0 on success 
// return -EINVAL if the message is malformed (i.e. has nonsensical field sizes)
// return -EOVERFLOW on numeric overflow 
// return -1 if the signature is invalid
// NOTE: we will reject any message that reports to be 2**30 bytes long with -ERANGE
int md_decrypt( EVP_PKEY* sender_pubkey, EVP_PKEY* receiver_pkey, char const* in_data, size_t in_data_len, char** plaintext, size_t* plaintext_len ) {
   int32_t iv_len = 0;
   int32_t ek_len = 0;
   int32_t ciphertext_len = 0;
   int32_t signature_len = 0;
   
   // use AES256 in CBC mode
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   int32_t expected_iv_len = EVP_CIPHER_iv_length( cipher );
   
   // data must have these four values
   int32_t header_len = sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + sizeof(signature_len);
   if( (unsigned)header_len > in_data_len ) {
      
      SG_error("header_len (%d) > in_data_len (%zu)\n", header_len, in_data_len );
      return -EINVAL;
   }
   
   // read each of them
   size_t signature_len_offset  = 0;
   size_t iv_len_offset         = signature_len_offset + sizeof(int32_t);
   size_t ek_len_offset         = iv_len_offset + sizeof(int32_t);
   size_t ciphertext_len_offset = ek_len_offset + sizeof(int32_t);
   
   memcpy( &signature_len, in_data + signature_len_offset, sizeof(int32_t) );
   memcpy( &iv_len, in_data + iv_len_offset, sizeof(int32_t) );
   memcpy( &ek_len, in_data + ek_len_offset, sizeof(int32_t) );
   memcpy( &ciphertext_len, in_data + ciphertext_len_offset, sizeof(int32_t) );
   
   // convert to host byte order
   iv_len = ntohl( iv_len );
   ek_len = ntohl( ek_len );
   ciphertext_len = ntohl( ciphertext_len );
   signature_len = ntohl( signature_len );
   
   // correct iv len?
   if( iv_len != expected_iv_len ) {
      SG_error("iv_len = %d, expected %d\n", iv_len, expected_iv_len );
      return -EINVAL;
   }
   
   // correct 
   
   // remaining offsets
   size_t iv_offset             = ciphertext_len_offset + sizeof(int32_t);
   size_t ek_offset             = iv_offset + iv_len;
   size_t ciphertext_offset     = ek_offset + ek_len;
   
   // sanity check--too short? overflow?
   if( iv_len <= 0 || ek_len <= 0 || ciphertext_len <= 0 || signature_len <= 0 ) {
      SG_error("invalid header (iv_len = %d, ek_ken = %d, ciphertext_len = %d, signature_len = %d)\n", iv_len, ek_len, ciphertext_len, signature_len );
      return -EINVAL;
   }
   
   // sanity check--too long?
   int32_t total_len =  iv_len + ek_len + ciphertext_len + signature_len +
                        sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + sizeof(signature_len);
                        
   if( total_len < 0 ) {
      return -EOVERFLOW;
   }
   
   if( total_len >= (1L << 30) ) {
      
      return -ERANGE;
   }
   
   if( total_len > (signed)in_data_len ) {
      SG_debug("total_len (%d) > in_data_len (%zu)\n", total_len, in_data_len );
      return -EINVAL;
   }
   
   // seems okay...
   size_t signature_offset = ciphertext_offset + ciphertext_len;
   unsigned char* ek            = (unsigned char*)in_data + ek_offset;
   unsigned char* iv            = (unsigned char*)in_data + iv_offset;
   unsigned char* ciphertext    = (unsigned char*)in_data + ciphertext_offset;
   unsigned char* signature     = (unsigned char*)in_data + signature_offset;
   
   // verify: iv_len || ek_len || ciphertext_len || iv || ek || ciphertext
   
   size_t verify_offset = iv_len_offset;
   size_t verify_len = sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + iv_len + ek_len + ciphertext_len;
   
   if( (int32_t)(verify_len) < 0 ) {
      return -EOVERFLOW;
   }
   
   int rc = md_verify_signature_raw( sender_pubkey, in_data + verify_offset, verify_len, (char*)signature, signature_len );
   if( rc != 0 ) {
      SG_error("md_verify_signature rc = %d\n", rc );
      return -1;
   }
   
   // verified!  now we can decrypt
   
   // set up an encryption cipher
   EVP_CIPHER_CTX ctx;
   memset( &ctx, 0, sizeof(EVP_CIPHER_CTX) );
   
   EVP_CIPHER_CTX_init( &ctx );
   
   // initialize the cipher and start decrypting
   rc = EVP_OpenInit( &ctx, cipher, ek, ek_len, iv, receiver_pkey );
   if( rc == 0 ) {
      SG_error("EVP_OpenInit rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // output buffer
   unsigned char* output_buf = SG_CALLOC( unsigned char, ciphertext_len );
   int output_buf_written = 0;
   
   if( output_buf == NULL ) {
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -ENOMEM;
   }
   
   // decrypt everything
   rc = EVP_OpenUpdate( &ctx, output_buf, &output_buf_written, ciphertext, ciphertext_len );
   if( rc == 0 ) {
      SG_error("EVP_OpenUpdate rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      SG_safe_free( output_buf );
      return -1;
   }
      
   // finalize 
   int output_written_final = 0;
   
   rc = EVP_OpenFinal( &ctx, output_buf + output_buf_written, &output_written_final );
   if( rc == 0 ) {
      SG_error("EVP_OpenFinal rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      
      memset( output_buf, 0, ciphertext_len );
      SG_safe_free( output_buf );
      return -1;
   }
   
#ifndef _NO_VALGRIND_FIXES
   VALGRIND_MAKE_MEM_DEFINED( &output_buf_written, sizeof(output_buf_written) );
   VALGRIND_MAKE_MEM_DEFINED( &output_written_final, sizeof(output_written_final) );
#endif
   
   // reply decrypted data
   *plaintext = (char*)output_buf;
   *plaintext_len = (size_t)(output_buf_written + output_written_final);
   
   EVP_CIPHER_CTX_cleanup( &ctx );
   
   // printf("Header: (iv_len = %d, ek_ken = %d, ciphertext_len = %d, signature_len = %d); decrypted %zu bytes to %zu bytes at %p\n", iv_len, ek_len, ciphertext_len, signature_len, in_data_len, *plaintext_len, *plaintext );
   // fflush(stdout);
   
#ifndef _NO_VALGRIND_FIXES
   VALGRIND_MAKE_MEM_DEFINED( plaintext_len, sizeof(*plaintext_len) );
   VALGRIND_MAKE_MEM_DEFINED( *plaintext, *plaintext_len );
#endif
   
   return 0;
}


// helper function for md_encrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
// return 0 on success 
// return -EINVAL on failure to load either the public or private key 
// return -errno on failure to encrypt 
// return -errno on failure to initialize the crypto subsystem, if it had not been initialized already
int md_encrypt_pem( char const* sender_pkey_pem, char const* receiver_pubkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   
   int rc = 0;
   
   // safety for external clients
   if( !md_crypt_check_init() ) {
      rc = md_crypt_init();
      if( rc != 0 ) {
         
         SG_error("md_crypt_init rc = %d\n", rc );
         return rc;
      }
   }
   
   // load the keys
   EVP_PKEY* pkey = NULL;
   EVP_PKEY* pubkey = NULL;
   
   rc = md_load_pubkey( &pubkey, receiver_pubkey_pem, strlen(receiver_pubkey_pem) );
   if( rc != 0 ) {
      
      SG_error("md_load_pubkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_load_privkey( &pkey, sender_pkey_pem, strlen(sender_pkey_pem) );
   if( rc != 0 ) {
      
      SG_error("md_load_privkey rc = %d\n", rc );
      
      EVP_PKEY_free( pubkey );
      return -EINVAL;
   }
   
   rc = md_encrypt( pkey, pubkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( pkey );
   EVP_PKEY_free( pubkey );
   return rc;
}


// helper function for md_decrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
// return 0 on success 
// return -errno on failure to initialize the crypto subsystem, if it had not been initialized already
int md_decrypt_pem( char const* sender_pubkey_pem, char const* receiver_privkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   
   int rc = 0;
   
   // safety for external clients
   if( !md_crypt_check_init() ) {
      rc = md_crypt_init();
      if( rc != 0 ) {
         
         SG_error("md_crypt_init rc = %d\n", rc );
         return rc;
      }
   }
   
   // load the keys
   EVP_PKEY* pubkey = NULL;
   EVP_PKEY* privkey = NULL;
   
   rc = md_load_privkey( &privkey, receiver_privkey_pem, strlen(receiver_privkey_pem) );
   if( rc != 0 ) {
      
      SG_error("md_load_privkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_load_pubkey( &pubkey, sender_pubkey_pem, strlen(sender_pubkey_pem) );
   if( rc != 0 ) {
      
      SG_error("md_load_pubkey rc = %d\n", rc );
      
      EVP_PKEY_free( privkey );
      return -EINVAL;
   }
   
   rc = md_decrypt( pubkey, privkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( privkey );
   EVP_PKEY_free( pubkey );
   return rc;
}

// how big is the ciphertext buffer for md_encrypt_symmetric_ex?
// return the size 
size_t md_encrypt_symmetric_ex_ciphertext_len( size_t data_len ) {
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   return data_len + EVP_CIPHER_block_size( cipher );
}

// encrypt data using a symmetric key and an IV
// return 0 on success
// return -1 on OpenSSL initialization failure 
int md_encrypt_symmetric_ex( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* data, size_t data_len, char** ciphertext, size_t* ciphertext_len ) {
   
   if( key_len != 32 ) {
      // not 256-bit key 
      return -EINVAL;
   }
   
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   int rc = 0;
   
   /*
   if( EVP_CIPHER_iv_length( cipher ) != iv_len )
      // invalid IV length 
      return -EINVAL;
   */
   
   EVP_CIPHER_CTX e_ctx;
   
   // initialize
   EVP_CIPHER_CTX_init(&e_ctx);
   
   rc = EVP_EncryptInit_ex(&e_ctx, cipher, NULL, key, iv);
   if( rc == 0 ) {
      SG_error("EVP_EncryptInit_ex rc = %d\n", rc );
      md_openssl_error();
      
      return -1;
   }
   
   // allocate ciphertext 
   unsigned char* c_buf = NULL;
   int c_buf_len = 0;
   
   if( *ciphertext != NULL ) {
      c_buf = (unsigned char*)(*ciphertext);
   }
   else {
      c_buf = SG_CALLOC( unsigned char, md_encrypt_symmetric_ex_ciphertext_len( data_len ) );
   }
   
   if( c_buf == NULL ) {
      
      EVP_CIPHER_CTX_cleanup( &e_ctx );
      return -ENOMEM;
   }
   
   rc = EVP_EncryptUpdate( &e_ctx, c_buf, &c_buf_len, (unsigned char*)data, data_len );
   if( rc == 0 ) {
      
      SG_error("EVP_EncryptUpdate rc = %d\n", rc );
      md_openssl_error();
   
      memset( c_buf, 0, md_encrypt_symmetric_ex_ciphertext_len( data_len ) );
      SG_safe_free( c_buf );
      EVP_CIPHER_CTX_cleanup( &e_ctx );
      return -1;
   }
   
   // finalize 
   int final_len = 0;
   rc = EVP_EncryptFinal_ex( &e_ctx, c_buf + c_buf_len, &final_len );
   if( rc == 0 ) {
      SG_error("EVP_EncryptFinal_ex rc = %d\n", rc );
      md_openssl_error();
      
      memset( c_buf, 0, md_encrypt_symmetric_ex_ciphertext_len( data_len ) );
      SG_safe_free( c_buf );
      EVP_CIPHER_CTX_cleanup( &e_ctx );
      return -1;
   }
   
   c_buf_len += final_len;
   
   EVP_CIPHER_CTX_cleanup( &e_ctx );
   
   if( (unsigned char*)(*ciphertext) != c_buf ) {
      // was allocated
      *ciphertext = (char*)c_buf;
   }
   
   *ciphertext_len = c_buf_len;
   
   return 0;
}


// how big is the ciphertext buffer for md_encrypt_symmetric_ex?
size_t md_decrypt_symmetric_ex_ciphertext_len( size_t ciphertext_len ) {
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   return ciphertext_len + EVP_CIPHER_block_size( cipher );
}

// decrypt data using a symmetric key and an IV
// return 0 on success 
// return -EINVAL for invalid key length 
// return -1 on OpenSSL error 
// return -ENOMEM on OOM 
int md_decrypt_symmetric_ex( unsigned char const* key, size_t key_len, unsigned char const* iv, size_t iv_len, char* ciphertext_data, size_t ciphertext_len, char** data, size_t* data_len ) {
   
   if( key_len != 32 ) {
      // not a 256-bit key 
      return -EINVAL;
   }
   
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   int rc = 0;
   
   
   /*
   if( EVP_CIPHER_iv_length( cipher ) != iv_len )
      // invalid IV length 
      return -EINVAL;
   */
   
   EVP_CIPHER_CTX d_ctx;
   
   // initialize
   EVP_CIPHER_CTX_init(&d_ctx);
   
   rc = EVP_DecryptInit_ex(&d_ctx, cipher, NULL, key, iv);
   if( rc == 0 ) {
      SG_error("EVP_EncryptInit_ex rc = %d\n", rc );
      md_openssl_error();
      
      return -1;
   }
   
   // allocate plaintext, if needed
   unsigned char* p_buf = NULL;
   int p_buf_len = 0;
   
   if( *data != NULL ) {
      p_buf = (unsigned char*)(*data);
   }
   else {
      p_buf = SG_CALLOC( unsigned char, md_decrypt_symmetric_ex_ciphertext_len( ciphertext_len ) );
   }
   
   if( p_buf == NULL ) {
      
      return -ENOMEM;
   }
   
   rc = EVP_DecryptUpdate( &d_ctx, p_buf, &p_buf_len, (unsigned char*)ciphertext_data, ciphertext_len );
   if( rc == 0 ) {
      SG_error("EVP_EncryptUpdate rc = %d\n", rc );
      md_openssl_error();
   
      memset( p_buf, 0, md_decrypt_symmetric_ex_ciphertext_len( ciphertext_len ) );
      SG_safe_free( p_buf );
      EVP_CIPHER_CTX_cleanup( &d_ctx );
      return -1;
   }
   
   // finalize 
   int final_len = 0;
   rc = EVP_DecryptFinal_ex( &d_ctx, p_buf + p_buf_len, &final_len );
   if( rc == 0 ) {
      SG_error("EVP_EncryptFinal_ex rc = %d\n", rc );
      md_openssl_error();
      
      memset( p_buf, 0, md_decrypt_symmetric_ex_ciphertext_len( ciphertext_len ) );
      SG_safe_free( p_buf );
      EVP_CIPHER_CTX_cleanup( &d_ctx );
      return -1;
   }
   
   p_buf_len += final_len;
   
   EVP_CIPHER_CTX_cleanup( &d_ctx );
   
   if( (unsigned char*)(*data) != p_buf ) {
      *data = (char*)p_buf;
   }
   
   *data_len = p_buf_len;
   
   return 0;
}


// how big should the ciphertext buffer be for md_encrypt_symmetric?
size_t md_encrypt_symmetric_ciphertext_len( size_t data_len ) {
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   return data_len + EVP_CIPHER_iv_length( cipher ) + EVP_CIPHER_block_size( cipher );
}

// encrypt data using a symmetric key.  Generate an IV and keep it with the ciphertext
// return 0 on success 
// return -EINVAL on invalid key length 
// return -ENOMEM on OOM 
// return non-zero on failure to encrypt
// return -errno on failure to read random data
int md_encrypt_symmetric( unsigned char const* key, size_t key_len, char* data, size_t data_len, char** ret_ciphertext, size_t* ret_ciphertext_len ) {
   
   if( key_len != 32 ) {
      // not a 256-bit key 
      return -EINVAL;
   }
   
   // initialization vector
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   size_t iv_len = EVP_CIPHER_iv_length( cipher );
   unsigned char* iv = (unsigned char*)alloca( iv_len );
   
   if( iv == NULL ) {
      
      return -ENOMEM;
   }
   
   // fill the iv with random data
   int rc = md_read_urandom( (char*)iv, iv_len );
   if( rc != 0 ) {
      
      SG_error("md_read_urandom rc = %d\n", rc);
      return rc;
   }
   
   char* ciphertext_buffer = SG_CALLOC( char, md_encrypt_symmetric_ciphertext_len( data_len ) );
   size_t ciphertext_buffer_len = 0;
   
   // where's the ciphertext going to go?
   char* ciphertext = ciphertext_buffer + iv_len;
   size_t ciphertext_len = 0;
   
   rc = md_encrypt_symmetric_ex( key, key_len, iv, iv_len, data, data_len, &ciphertext, &ciphertext_len );
   
   if( rc != 0 ) {
      
      SG_error("md_encrypt_symmetric_ex rc = %d\n", rc );
      return rc;
   }
   
   // store the IV and calculate the ciphertext buffer's total length
   memcpy( ciphertext_buffer, iv, iv_len );
   ciphertext_buffer_len = iv_len + ciphertext_len;
   
   *ret_ciphertext = ciphertext_buffer;
   *ret_ciphertext_len = ciphertext_buffer_len;
   
   return 0;
}

// how big should the plaintext bufer be for md_decrypt_symmetric?
size_t md_decrypt_symmetric_plaintext_len( size_t ciphertext_buffer_len ) {
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   return ciphertext_buffer_len - EVP_CIPHER_iv_length( cipher ) + EVP_CIPHER_block_size( cipher );
}

// unseal data with a symmetric key.  the ciphertext buffer will have had to have been generated by md_encrypt_symmetric
// return 0 on success 
// return -EINVAL on invalid key length, or invalid buffer length 
// return non-zero on failure to decrypt 
int md_decrypt_symmetric( unsigned char const* key, size_t key_len, char* ciphertext_buffer, size_t ciphertext_buffer_len, char** data, size_t* data_len ) {
   
   if( key_len != 32 ) {
      // not a 256-bit key 
      return -EINVAL;
   }
   
   // extract the initialization vector 
   const EVP_CIPHER* cipher = MD_DEFAULT_CIPHER();
   size_t iv_len = EVP_CIPHER_iv_length( cipher );
   unsigned char* iv = (unsigned char*)ciphertext_buffer;
   
   // sanity check 
   if( ciphertext_buffer_len <= iv_len ) {
      // can't possibly be long enough
      return -EINVAL;
   }
   
   // the actual ciphertext is at...
   char* ciphertext_data = ciphertext_buffer + iv_len;
   size_t ciphertext_data_len = ciphertext_buffer_len - iv_len;
   
   int rc = md_decrypt_symmetric_ex( key, key_len, iv, iv_len, ciphertext_data, ciphertext_data_len, data, data_len );
   
   if( rc != 0 ) {
      SG_error("md_decrypt_symmetric_ex rc = %d\n", rc );
      return rc;
   }
   
   // success!
   return 0;
}
