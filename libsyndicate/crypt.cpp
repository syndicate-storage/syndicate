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


////////////////////////////////////////////////////////////////////////////////
// derived from http://www.cs.odu.edu/~cs772/sourcecode/NSwO/compiled/common.c

void md_init_OpenSSL(void) {
    if (!md_openssl_thread_setup() || !SSL_library_init())
    {
        errorf("%s", "OpenSSL initialization failed!\n");
        exit(-1);
    }
    
    OpenSSL_add_all_digests();
    ERR_load_crypto_strings();
}


/* This array will store all of the mutexes available to OpenSSL. */
static MD_MUTEX_TYPE *md_openssl_mutex_buf = NULL ;

static void locking_function(int mode, int n, const char * file, int line)
{
  if (mode & CRYPTO_LOCK)
    MD_MUTEX_LOCK(md_openssl_mutex_buf[n]);
  else
    MD_MUTEX_UNLOCK(md_openssl_mutex_buf[n]);
}

static unsigned long id_function(void)
{
  return ((unsigned long)MD_THREAD_ID);
}

int md_openssl_thread_setup(void)
{
  if( md_openssl_mutex_buf != NULL )
     // already initialized
     return 1;
     
  int i;
  md_openssl_mutex_buf = (MD_MUTEX_TYPE *) malloc(CRYPTO_num_locks( ) * sizeof(MD_MUTEX_TYPE));
  if(!md_openssl_mutex_buf)
    return 0;
  for (i = 0; i < CRYPTO_num_locks( ); i++)
    MD_MUTEX_SETUP(md_openssl_mutex_buf[i]);
  CRYPTO_set_id_callback(id_function);
  CRYPTO_set_locking_callback(locking_function);
  return 1;
}

int md_openssl_thread_cleanup(void)
{
  int i;
  if (!md_openssl_mutex_buf)
    return 0;
  CRYPTO_set_id_callback(NULL);
  CRYPTO_set_locking_callback(NULL);
  for (i = 0; i < CRYPTO_num_locks( ); i++)
    MD_MUTEX_CLEANUP(md_openssl_mutex_buf[i]);
  free(md_openssl_mutex_buf);
  md_openssl_mutex_buf = NULL;
  return 1;
}

////////////////////////////////////////////////////////////////////////////////


// print a crypto error message
int md_openssl_error() {
   unsigned long err = ERR_get_error();
   char buf[4096];

   ERR_error_string_n( err, buf, 4096 );
   errorf("OpenSSL error %ld: %s\n", err, buf );
   return 0;
}

// verify a message, given a base64-encoded signature
int md_verify_signature( EVP_PKEY* public_key, char const* data, size_t len, char* sigb64, size_t sigb64len ) {
   char* sig_bin = NULL;
   size_t sig_bin_len = 0;

   int rc = Base64Decode( sigb64, sigb64len, &sig_bin, &sig_bin_len );
   if( rc != 0 ) {
      errorf("Base64Decode rc = %d\n", rc );
      return -EINVAL;
   }

   dbprintf("VERIFY: message len = %zu, strlen(sigb64) = %zu, sigb64 = %s\n", len, strlen(sigb64), sigb64 );
   
   const EVP_MD* sha256 = EVP_sha256();

   EVP_MD_CTX *mdctx = EVP_MD_CTX_create();
   EVP_PKEY_CTX* pkey_ctx = NULL;

   rc = EVP_DigestVerifyInit( mdctx, &pkey_ctx, sha256, NULL, public_key );
   if( rc <= 0 ) {
      errorf("EVP_DigestVerifyInit_ex rc = %d\n", rc);
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // set up padding
   
   // activate PSS
   rc = EVP_PKEY_CTX_set_rsa_padding( pkey_ctx, RSA_PKCS1_PSS_PADDING );
   if( rc <= 0 ) {
      errorf( "EVP_PKEY_CTX_set_rsa_padding rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // use maximum possible salt length, as per http://wiki.openssl.org/index.php/Manual:EVP_PKEY_CTX_ctrl(3).
   // This is only because PyCrypto (used by the MS) does this in its PSS implementation.
   rc = EVP_PKEY_CTX_set_rsa_pss_saltlen( pkey_ctx, -1 );
   if( rc <= 0 ) {
      errorf( "EVP_PKEY_CTX_set_rsa_pss_saltlen rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   rc = EVP_DigestVerifyUpdate( mdctx, (void*)data, len );
   if( rc <= 0 ) {
      errorf("EVP_DigestVerifyUpdate rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   rc = EVP_DigestVerifyFinal( mdctx, (unsigned char*)sig_bin, sig_bin_len );
   if( rc <= 0 ) {
      errorf("EVP_DigestVerifyFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EBADMSG;
   }

   EVP_MD_CTX_destroy( mdctx );
   
   free( sig_bin );

   return 0;
}


// sign a message
// on success, populate sigb64 and sigb64len with the base64-encoded signature and length, respectively
int md_sign_message( EVP_PKEY* pkey, char const* data, size_t len, char** sigb64, size_t* sigb64len ) {

   // sign this with SHA256
   const EVP_MD* sha256 = EVP_sha256();

   EVP_MD_CTX *mdctx = EVP_MD_CTX_create();

   //int rc = EVP_SignInit( mdctx, sha256 );
   EVP_PKEY_CTX* pkey_ctx = NULL;
   int rc = EVP_DigestSignInit( mdctx, &pkey_ctx, sha256, NULL, pkey );
   
   if( rc <= 0 ) {
      errorf("EVP_DigestSignInit rc = %d\n", rc);
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // set up padding
   
   // activate PSS
   rc = EVP_PKEY_CTX_set_rsa_padding( pkey_ctx, RSA_PKCS1_PSS_PADDING );
   if( rc <= 0 ) {
      errorf( "EVP_PKEY_CTX_set_rsa_padding rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // use salt length == digest_length, as per http://wiki.openssl.org/index.php/Manual:EVP_PKEY_CTX_ctrl(3).
   // This is only because PyCrypto (used by the MS) does this in its PSS implementation.
   rc = EVP_PKEY_CTX_set_rsa_pss_saltlen( pkey_ctx, -1 );
   if( rc <= 0 ) {
      errorf( "EVP_PKEY_CTX_set_rsa_pss_saltlen rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   rc = EVP_DigestSignUpdate( mdctx, (void*)data, len );
   if( rc <= 0 ) {
      errorf("EVP_DigestSignUpdate rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   // get signature size
   size_t sig_bin_len = 0;
   rc = EVP_DigestSignFinal( mdctx, NULL, &sig_bin_len );
   if( rc <= 0 ) {
      errorf("EVP_DigestSignFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   // allocate the signature
   unsigned char* sig_bin = CALLOC_LIST( unsigned char, sig_bin_len );

   rc = EVP_DigestSignFinal( mdctx, sig_bin, &sig_bin_len );
   if( rc <= 0 ) {
      errorf("EVP_DigestSignFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }

   // convert to base64
   char* b64 = NULL;
   rc = Base64Encode( (char*)sig_bin, sig_bin_len, &b64 );
   if( rc != 0 ) {
      errorf("Base64Encode rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      free( sig_bin );
      return rc;
   }

   EVP_MD_CTX_destroy( mdctx );

   *sigb64 = b64;
   *sigb64len = strlen(b64);
   
   dbprintf("SIGN: message len = %zu, sigb64_len = %zu, sigb64 = %s\n", len, strlen(b64), b64 );

   free( sig_bin );
   
   return 0;
}


// load a PEM-encoded (RSA) public key into an EVP key
int md_load_pubkey( EVP_PKEY** key, char const* pubkey_str ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)pubkey_str, strlen(pubkey_str) );

   EVP_PKEY* public_key = PEM_read_bio_PUBKEY( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( public_key == NULL ) {
      // invalid public key
      errorf("%s", "ERR: failed to read public key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = public_key;
   
   return 0;
}


// load a PEM-encoded (RSA) private key into an EVP key
int md_load_privkey( EVP_PKEY** key, char const* privkey_str ) {
   BIO* buf_io = BIO_new_mem_buf( (void*)privkey_str, strlen(privkey_str) );

   EVP_PKEY* privkey = PEM_read_bio_PrivateKey( buf_io, NULL, NULL, NULL );

   BIO_free_all( buf_io );

   if( privkey == NULL ) {
      // invalid public key
      errorf("%s", "ERR: failed to read private key\n");
      md_openssl_error();
      return -EINVAL;
   }

   *key = privkey;

   return 0;
}

// generate RSA public/private key pair
int md_generate_key( EVP_PKEY** key ) {

   dbprintf("%s", "Generating public/private key...\n");
   
   EVP_PKEY_CTX *ctx;
   EVP_PKEY *pkey = NULL;
   ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
   if (!ctx) {
      md_openssl_error();
      return -1;
   }

   int rc = EVP_PKEY_keygen_init( ctx );
   if( rc <= 0 ) {
      md_openssl_error();
      EVP_PKEY_CTX_free( ctx );
      return rc;
   }

   rc = EVP_PKEY_CTX_set_rsa_keygen_bits( ctx, RSA_KEY_SIZE );
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


// dump a key to memory
long md_dump_pubkey( EVP_PKEY* pkey, char** buf ) {
   BIO* mbuf = BIO_new( BIO_s_mem() );
   
   int rc = PEM_write_bio_PUBKEY( mbuf, pkey );
   if( rc <= 0 ) {
      errorf("PEM_write_bio_PUBKEY rc = %d\n", rc );
      md_openssl_error();
      return -EINVAL;
   }

   (void) BIO_flush( mbuf );
   
   char* tmp = NULL;
   long sz = BIO_get_mem_data( mbuf, &tmp );

   *buf = CALLOC_LIST( char, sz );
   memcpy( *buf, tmp, sz );

   BIO_free( mbuf );
   
   return sz;
}


// encrypt data with a public key, using the EVP_Seal API.
// use AES256 with Galois/Counter Mode, and a 256-bit IV
int md_encrypt( EVP_PKEY* pubkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   const EVP_CIPHER* cipher = EVP_aes_256_gcm();
   
   // set up an encryption cipher
   EVP_CIPHER_CTX ctx;
   EVP_CIPHER_CTX_init( &ctx );
   
   // choose the cipher
   int rc = EVP_SealInit( &ctx, cipher, NULL, 0, NULL, NULL, 0 );
   if( rc == 0 ) {
      errorf("EVP_SealInit rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // set IV length 
   int iv_len = 256 / 8;
   unsigned char* iv = (unsigned char*)alloca( iv_len );
   
   rc = EVP_CIPHER_CTX_ctrl( &ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, NULL );
   if( rc == 0 ) {
      errorf("EVP_CIPHER_CTX_ctrl rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // get block size 
   int block_size = EVP_CIPHER_block_size( cipher );
   
   // allocate encrypted key and IV
   unsigned char* ek = CALLOC_LIST( unsigned char, EVP_PKEY_size( pubkey ) );
   int ek_len = 0;
   
   // allocate the tag
   int tag_len = 16;
   unsigned char* tag = (unsigned char*)alloca( tag_len );
   
   // begin
   rc = EVP_SealInit( &ctx, NULL, &ek, &ek_len, iv, &pubkey, 1 );
   if( rc == 0 ) {
      errorf("EVP_SealInit rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // copy in the key length, the (encrypted) key, the IV length, and the IV
   int ek_len_n = htonl( ek_len );
   int iv_len_n = htonl( iv_len );
   int tag_len_n = htonl( tag_len );
   int ciphertext_size = 0;
   
   // allocate the output buffer
   size_t output_len = sizeof(ek_len_n) + ek_len + sizeof(iv_len_n) + iv_len + sizeof(tag_len_n) + tag_len + sizeof(ciphertext_size) + (in_data_len + 2 * block_size);
   unsigned char* output_data = CALLOC_LIST( unsigned char, output_len );
   int output_begin = 0;
   
   // key length
   memcpy( output_data + output_begin, &ek_len_n, sizeof(ek_len_n) );
   output_begin += sizeof(ek_len_n);
   
   // encrypted key 
   memcpy( output_data + output_begin, ek, ek_len );
   output_begin += ek_len;
   
   // IV length
   memcpy( output_data + output_begin, &iv_len_n, sizeof(iv_len_n) );
   output_begin += sizeof(iv_len_n);
   
   // IV
   memcpy( output_data + output_begin, iv, iv_len );
   output_begin += iv_len;
   
   // leave room for the tag size
   unsigned char* tag_size_p = output_data + output_begin;
   output_begin += sizeof(tag_len_n);
   
   // leave room for the tag
   unsigned char* tag_p = output_data + output_begin;
   output_begin += tag_len;
   
   // leave room for a payload size
   unsigned char* ciphertext_size_p = output_data + output_begin;
   output_begin += sizeof(ciphertext_size);
   
   // encrypt the data 
   int output_written = 0;
   rc = EVP_SealUpdate( &ctx, output_data + output_begin, &output_written, (unsigned char*)in_data, in_data_len );
   if( rc == 0 ) {
      errorf("EVP_SealUpdate rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      free( output_data );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   output_begin += output_written;
   
   // finalize...
   int final_len = 0;
   rc = EVP_SealFinal( &ctx, output_data + output_begin, &final_len );
   if( rc == 0 ) {
      errorf("EVP_SealFinal rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      free( output_data );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   output_begin += final_len;
   
   // get the tag
   rc = EVP_CIPHER_CTX_ctrl( &ctx, EVP_CTRL_GCM_GET_TAG, tag_len, tag );
   if( rc == 0 ) {
      errorf("EVP_CIPHER_CTX_ctrl rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      free( output_data );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // set the tag length
   memcpy( tag_size_p, &tag_len_n, sizeof(tag_len_n) );
   
   // set the tag
   memcpy( tag_p, tag, tag_len );
   
   // set the ciphertext size
   ciphertext_size = htonl( output_written + final_len );
   memcpy( ciphertext_size_p, &ciphertext_size, sizeof(ciphertext_size));
   
   // reply data
   *out_data = (char*)output_data;
   *out_data_len = (size_t)output_begin;
   
   // clean up 
   free( ek );
   EVP_CIPHER_CTX_cleanup( &ctx );
   
   return 0;
}

// helper function for md_encrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
int md_encrypt_pem( char const* pubkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   // load the key
   EVP_PKEY* pubkey = NULL;
   int rc = md_load_pubkey( &pubkey, pubkey_pem );
   if( rc != 0 ) {
      errorf("md_load_pubkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_encrypt( pubkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( pubkey );
   return rc;
}

// decrypt data, given a private key
// use AES256 with Galois/Counter mode
int md_decrypt( EVP_PKEY* privkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   int ek_len = 0, ek_len_n = 0;
   int iv_len = 0, iv_len_n = 0;
   int tag_len = 0, tag_len_n = 0;
   int ciphertext_len = 0, ciphertext_len_n = 0;
   unsigned char* ek = NULL;
   unsigned char* iv = NULL;
   unsigned char* tag = NULL;
   unsigned char* ciphertext = NULL;
   int input_begin = 0;
   
   // sanity check: need a key length
   if( in_data_len < (size_t)(input_begin + sizeof(ek_len_n)) )
      return -EINVAL;

   // get the key length
   memcpy( &ek_len_n, in_data + input_begin, sizeof(ek_len_n) );
   input_begin += sizeof(ek_len_n);
   
   ek_len = ntohl( ek_len_n );
   
   // sanity check: need a key
   if( in_data_len < (size_t)(input_begin + ek_len) )
      return -EINVAL;
   
   // get the encrypted key
   ek = (unsigned char*)(in_data + input_begin);
   input_begin += ek_len;
   
   // sanity check: need an IV size 
   if( in_data_len < input_begin + sizeof(iv_len_n) )
      return -EINVAL;
   
   // get the IV length
   memcpy( &iv_len_n, in_data + input_begin, sizeof(iv_len_n) );
   input_begin += sizeof(iv_len_n);
   
   iv_len = ntohl( iv_len_n );
   
   // sanity check: need the IV
   if( in_data_len < (size_t)(input_begin + iv_len) )
      return -EINVAL;
   
   // get the IV
   iv = (unsigned char*)(in_data + input_begin);
   input_begin += iv_len;
   
   // sanity check: need the tag length
   if( in_data_len < input_begin + sizeof(tag_len_n) )
      return -EINVAL;
   
   // get the tag length
   memcpy( &tag_len_n, in_data + input_begin, sizeof(tag_len_n) );
   input_begin += sizeof(tag_len_n);
   
   tag_len = ntohl( tag_len_n );
   
   // sanity check: need the tag
   if( in_data_len < (size_t)(input_begin + tag_len) )
      return -EINVAL;
   
   // get the tag
   tag = (unsigned char*)(in_data + input_begin);
   input_begin += tag_len;
   
   // sanity check: need the ciphertext size
   if( in_data_len < input_begin + sizeof(ciphertext_len_n) )
      return -EINVAL;
   
   // get the ciphertext size
   memcpy( &ciphertext_len_n, in_data + input_begin, sizeof(ciphertext_len_n) );
   input_begin += sizeof(ciphertext_len_n);
   
   ciphertext_len = ntohl( ciphertext_len_n );
   
   // sanity check: need the ciphertext
   if( in_data_len < (size_t)(input_begin + ciphertext_len) )
      return -EINVAL;
   
   // get the ciphertext
   ciphertext = (unsigned char*)(in_data + input_begin);
   input_begin += ciphertext_len;
   
   // begin to decrypt it
   const EVP_CIPHER* cipher = EVP_aes_256_gcm();
   
   // set up an encryption cipher
   EVP_CIPHER_CTX ctx;
   EVP_CIPHER_CTX_init( &ctx );
   
   // initialize the context
   int rc = EVP_OpenInit( &ctx, cipher, NULL, 0, NULL, NULL );
   if( rc == 0 ) {
      errorf("EVP_OpenInit rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // set the IV
   rc = EVP_CIPHER_CTX_ctrl( &ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, NULL );
   if( rc == 0 ) {
      errorf("EVP_CIPHER_CTX_ctrl rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // initialize the cipher and start decrypting
   rc = EVP_OpenInit( &ctx, NULL, ek, ek_len, iv, privkey );
   if( rc == 0 ) {
      errorf("EVP_OpenInit rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // output buffer
   unsigned char* output_buf = CALLOC_LIST( unsigned char, ciphertext_len );
   int output_buf_written = 0;
   
   // decrypt everything
   rc = EVP_OpenUpdate( &ctx, output_buf, &output_buf_written, ciphertext, ciphertext_len );
   if( rc == 0 ) {
      errorf("EVP_OpenUpdate rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      free( output_buf );
      return -1;
   }
   
   // set the expected tag
   rc = EVP_CIPHER_CTX_ctrl( &ctx, EVP_CTRL_GCM_SET_TAG, tag_len, tag );
   if( rc == 0 ) {
      errorf("EVP_CIPHER_CTX_ctrl rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      free( output_buf );
      return -1;
   }
      
   // finalize 
   int output_written_final = 0;
   rc = EVP_OpenFinal( &ctx, output_buf + output_buf_written, &output_written_final );
   if( rc == 0 ) {
      errorf("EVP_OpenFinal rc = %d\n", rc );
      md_openssl_error();
      
      EVP_CIPHER_CTX_cleanup( &ctx );
      free( output_buf );
      return -1;
   }
   
   // reply decrypted data
   *out_data = (char*)output_buf;
   *out_data_len = output_buf_written + output_written_final;
   
   EVP_CIPHER_CTX_cleanup( &ctx );
   return 0;
}


// helper function for md_decrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
int md_decrypt_pem( char const* privkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   // load the key
   EVP_PKEY* privkey = NULL;
   int rc = md_load_privkey( &privkey, privkey_pem );
   if( rc != 0 ) {
      errorf("md_load_privkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_decrypt( privkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( privkey );
   return rc;
}
