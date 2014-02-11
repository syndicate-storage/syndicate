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

static int urandom_fd = -1;     // /dev/urandom
static int inited = 0;

// initialize crypto libraries and set up state
int md_crypt_init() {
   dbprintf("%s\n", "starting up");
   
   md_init_OpenSSL();
   
   urandom_fd = open("/dev/urandom", O_RDONLY );
   if( urandom_fd < 0 ) {
      int errsv = -errno;
      errorf("open(/dev/urandom) rc = %d\n", errsv);
      return errsv;
   }
   
   inited = 1;
   
   return 0;
}

// shut down crypto libraries and free state
int md_crypt_shutdown() {
   dbprintf("%s\n", "shutting down");
   
   if( urandom_fd >= 0 ) {
      close( urandom_fd );
      urandom_fd = -1;
   }
   
   // shut down OpenSSL
   ERR_free_strings();
   
   dbprintf("%s\n", "crypto thread shutdown" );
   md_openssl_thread_cleanup();
   
   return 0;
}


// check initialization
int md_crypt_check_init() {
   if( inited == 0 )
      return 0;
   else
      return 1;
}

// read bytes from /dev/urandom
int md_read_urandom( char* buf, size_t len ) {
   if( urandom_fd < 0 ) {
      errorf("%s", "crypto is not initialized\n");
      return -EINVAL;
   }
   
   ssize_t nr = 0;
   size_t num_read = 0;
   while( num_read < len ) {
      nr = read( urandom_fd, buf + num_read, len - num_read );
      if( nr < 0 ) {
         int errsv = -errno;
         errorf("read(/dev/urandom) errno %d\n", errsv);
         return errsv;
      }
      num_read += nr;
   }
   
   return 0;
}


// print a crypto error message
int md_openssl_error() {
   unsigned long err = ERR_get_error();
   char buf[4096];

   ERR_error_string_n( err, buf, 4096 );
   errorf("OpenSSL error %ld: %s\n", err, buf );
   return 0;
}

// verify a message, given a base64-encoded signature
int md_verify_signature_raw( EVP_PKEY* public_key, char const* data, size_t len, char* sig_bin, size_t sig_bin_len ) {
      
   const EVP_MD* sha256 = EVP_sha256();

   EVP_MD_CTX *mdctx = EVP_MD_CTX_create();
   EVP_PKEY_CTX* pkey_ctx = NULL;
   int rc = 0;
   
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
   
   return 0;
}

// verify a message with base64 signature
int md_verify_signature( EVP_PKEY* pubkey, char const* data, size_t len, char* sigb64, size_t sigb64_len ) {
   // safety for external clients
   if( !md_crypt_check_init() )
      md_crypt_init();
      
   char* sig_bin = NULL;
   size_t sig_bin_len = 0;

   dbprintf("VERIFY: message len = %zu, strlen(sigb64) = %zu, sigb64 = %s\n", len, strlen(sigb64), sigb64 );

   int rc = Base64Decode( sigb64, sigb64_len, &sig_bin, &sig_bin_len );
   if( rc != 0 ) {
      errorf("Base64Decode rc = %d\n", rc );
      return -EINVAL;
   }
   
   return md_verify_signature_raw( pubkey, data, len, sig_bin, sig_bin_len );
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
   if( sig_bin == NULL ) {
      EVP_MD_CTX_destroy( mdctx );
      return -ENOMEM;
   }

   rc = EVP_DigestSignFinal( mdctx, sig_bin, &sig_bin_len );
   if( rc <= 0 ) {
      errorf("EVP_DigestSignFinal rc = %d\n", rc );
      md_openssl_error();
      EVP_MD_CTX_destroy( mdctx );
      return -EINVAL;
   }
   
   *sig = (char*)sig_bin;
   *siglen = sig_bin_len;
   return 0;
}

   
// sign a message, producing the base64 signature
int md_sign_message( EVP_PKEY* pkey, char const* data, size_t len, char** sigb64, size_t* sigb64_len ) {
   // safety for external clients
   if( !md_crypt_check_init() )
      md_crypt_init();
      
   unsigned char* sig_bin = NULL;
   size_t sig_bin_len = 0;
   
   int rc = md_sign_message_raw( pkey, data, len, (char**)&sig_bin, &sig_bin_len );
   if( rc != 0 ) {
      errorf("md_sign_message_raw rc = %d\n", rc );
      return -1;
   }
   
   // convert to base64
   char* b64 = NULL;
   rc = Base64Encode( (char*)sig_bin, sig_bin_len, &b64 );
   if( rc != 0 ) {
      errorf("Base64Encode rc = %d\n", rc );
      md_openssl_error();
      free( sig_bin );
      return rc;
   }

   *sigb64 = b64;
   *sigb64_len = strlen(b64);
   
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


// Seal data with a given public key, using AES256 in CBC mode.
// Sign the encrypted data with the given public key.
int md_encrypt( EVP_PKEY* sender_pkey, EVP_PKEY* receiver_pubkey, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   
   // use AES256 in CBC mode
   const EVP_CIPHER* cipher = EVP_aes_256_cbc();
   size_t block_size = EVP_CIPHER_block_size( cipher );
   
   
   // initialization vector
   int iv_len = EVP_CIPHER_iv_length( cipher );
   unsigned char* iv = (unsigned char*)alloca( iv_len );
   
   if( iv == NULL ) {
      return -ENOMEM;
   }
   
   // fill the iv with random data
   int rc = md_read_urandom( (char*)iv, iv_len );
   if( rc != 0 ) {
      errorf("md_read_urandom rc = %d\n", rc);
      return rc;
   }
   
   // set up cipher 
   EVP_CIPHER_CTX ctx;
   EVP_CIPHER_CTX_init( &ctx );
   
   // encrypted symmetric key for sealing the ciphertext
   unsigned char* ek = CALLOC_LIST( unsigned char, EVP_PKEY_size( receiver_pubkey ) );
   int ek_len = 0;
   
   // set up EVP Sealing
   rc = EVP_SealInit( &ctx, cipher, &ek, &ek_len, iv, &receiver_pubkey, 1 );
   if( rc == 0 ) {
      errorf("EVP_SealInit rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // allocate the output buffer
   // final output format:
   //    signature_len || iv_len || ek_len || ciphertext_len || iv || ek || ciphertext || signature
   
   // allocate all but the signature for now, since we don't know yet how big it will be.
   // allocate space for its length, however!
   int output_len = sizeof(int) + sizeof(iv_len) + sizeof(ek_len) + sizeof(int) + iv_len + ek_len + (in_data_len + block_size);
   unsigned char* output_buf = CALLOC_LIST( unsigned char, output_len );
   if( output_buf == NULL ) {
      free( ek );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -ENOMEM;
   }
   
   size_t signature_len_offset  = 0;
   size_t iv_len_offset         = signature_len_offset + sizeof(int);
   size_t ek_len_offset         = iv_len_offset + sizeof(int);
   size_t ciphertext_len_offset = ek_len_offset + sizeof(int);
   size_t iv_offset             = ciphertext_len_offset + sizeof(int);
   size_t ek_offset             = iv_offset + iv_len;
   size_t ciphertext_offset     = ek_offset + ek_len;
   
   unsigned char* ciphertext = output_buf + ciphertext_offset;
   int ciphertext_len = 0;
   
   // encrypt!
   rc = EVP_SealUpdate( &ctx, ciphertext, &ciphertext_len, (unsigned char const*)in_data, in_data_len );
   if( rc == 0 ) {
      errorf("EVP_SealUpdate rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      free( output_buf );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   // finalize
   int tmplen = 0;
   rc = EVP_SealFinal( &ctx, ciphertext + ciphertext_len, &tmplen );
   if( rc == 0 ) {
      errorf("EVP_SealFinal rc = %d\n", rc );
      md_openssl_error();
      
      free( ek );
      free( output_buf );
      EVP_CIPHER_CTX_cleanup( &ctx );
      return -1;
   }
   
   ciphertext_len += tmplen;
   
   // clean up
   EVP_CIPHER_CTX_cleanup( &ctx );
   
   // populate the output buffer with the encrypted key, iv, and their sizes.  Then sign everything.
   int iv_len_n = htonl( iv_len );
   int ek_len_n = htonl( ek_len );
   int ciphertext_len_n = htonl( ciphertext_len );
   
   memcpy( output_buf + iv_len_offset, &iv_len_n, sizeof(iv_len_n) );
   memcpy( output_buf + ek_len_offset, &ek_len_n, sizeof(ek_len_n) );
   memcpy( output_buf + ciphertext_len_offset, &ciphertext_len_n, sizeof(ciphertext_len_n) );
   
   memcpy( output_buf + iv_offset, iv, iv_len );
   memcpy( output_buf + ek_offset, ek, ek_len );
   // NOTE: ciphertext is already in place
   
   // sign:  iv_len || ek_len || ciphertext_len || iv || ek || ciphertext
   size_t sig_payload_len = sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + iv_len + ek_len + ciphertext_len;
   size_t sig_payload_offset = iv_len_offset;
   
   unsigned char* sig = NULL;
   size_t sig_len = 0;
   rc = md_sign_message_raw( sender_pkey, (char*)(output_buf + sig_payload_offset), sig_payload_len, (char**)&sig, &sig_len );
   if( rc != 0 ) {
      errorf("md_sign_message rc = %d\n", rc );
      
      free( ek );
      free( output_buf );
      return -1;
   }
   
   // add space for the signature
   int full_output_len = sizeof(int) + sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + iv_len + ek_len + ciphertext_len + sig_len;
   unsigned char* signed_output = (unsigned char*)realloc( output_buf, full_output_len );
   if( signed_output == NULL ) {
      // out of memory
      free( ek );
      free( output_buf );
      return -ENOMEM;
   }
   
   // add the signature and its length
   int sig_len_n = htonl( sig_len );
   size_t signature_offset = ciphertext_offset + ciphertext_len;        // signature goes after ciphertext
   
   memcpy( signed_output + signature_len_offset, &sig_len_n, sizeof(sig_len_n) );
   memcpy( signed_output + signature_offset, sig, sig_len );
   
   // return data
   *out_data = (char*)signed_output;
   *out_data_len = full_output_len;
   
   return 0;
}


// decrypt data with a given private key, generated from md_encrypt
// check the signature BEFORE decrypting!
int md_decrypt( EVP_PKEY* sender_pubkey, EVP_PKEY* receiver_pkey, char const* in_data, size_t in_data_len, char** plaintext, size_t* plaintext_len ) {
   int iv_len = 0;
   int ek_len = 0;
   int ciphertext_len = 0;
   int signature_len = 0;
   
   // use AES256 in CBC mode
   const EVP_CIPHER* cipher = EVP_aes_256_cbc();
   
   // data must have these four values
   size_t header_len = sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + sizeof(signature_len);
   if( header_len > in_data_len ) {
      return -EINVAL;
   }
   
   // read each of them
   size_t signature_len_offset  = 0;
   size_t iv_len_offset         = signature_len_offset + sizeof(int);
   size_t ek_len_offset         = iv_len_offset + sizeof(int);
   size_t ciphertext_len_offset = ek_len_offset + sizeof(int);
   
   memcpy( &signature_len, in_data + signature_len_offset, sizeof(int) );
   memcpy( &iv_len, in_data + iv_len_offset, sizeof(int) );
   memcpy( &ek_len, in_data + ek_len_offset, sizeof(int) );
   memcpy( &ciphertext_len, in_data + ciphertext_len_offset, sizeof(int) );
   
   // convert to host byte order
   iv_len = ntohl( iv_len );
   ek_len = ntohl( ek_len );
   ciphertext_len = ntohl( ciphertext_len );
   signature_len = ntohl( signature_len );
   
   // remaining offsets
   size_t iv_offset             = ciphertext_len_offset + sizeof(int);
   size_t ek_offset             = iv_offset + iv_len;
   size_t ciphertext_offset     = ek_offset + ek_len;
   
   // sanity check--too short?
   if( iv_len <= 0 || ek_len <= 0 || ciphertext_len <= 0 || signature_len <= 0 )
      return -EINVAL;
   
   // sanity check--too long?
   uint64_t total_len = (uint64_t)iv_len + (uint64_t)ek_len + (uint64_t)ciphertext_len + (uint64_t)signature_len +
                        sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + sizeof(signature_len);
                        
   if( total_len > in_data_len )
      return -EINVAL;
   
   // seems okay...
   size_t signature_offset = ciphertext_offset + ciphertext_len;
   unsigned char* ek            = (unsigned char*)in_data + ek_offset;
   unsigned char* iv            = (unsigned char*)in_data + iv_offset;
   unsigned char* ciphertext    = (unsigned char*)in_data + ciphertext_offset;
   unsigned char* signature     = (unsigned char*)in_data + signature_offset;
   
   // verify: iv_len || ek_len || ciphertext_len || iv || ek || ciphertext
   
   size_t verify_offset = iv_len_offset;
   size_t verify_len = sizeof(iv_len) + sizeof(ek_len) + sizeof(ciphertext_len) + iv_len + ek_len + ciphertext_len;
   
   int rc = md_verify_signature_raw( sender_pubkey, in_data + verify_offset, verify_len, (char*)signature, signature_len );
   if( rc != 0 ) {
      errorf("md_verify_signature rc = %d\n", rc );
      return -1;
   }
   
   // verified!  now we can decrypt
   
   // set up an encryption cipher
   EVP_CIPHER_CTX ctx;
   EVP_CIPHER_CTX_init( &ctx );
   
   // initialize the cipher and start decrypting
   rc = EVP_OpenInit( &ctx, cipher, ek, ek_len, iv, receiver_pkey );
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
   *plaintext = (char*)output_buf;
   *plaintext_len = output_buf_written + output_written_final;
   
   EVP_CIPHER_CTX_cleanup( &ctx );
   
   return 0;
}


// helper function for md_encrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
int md_encrypt_pem( char const* sender_pkey_pem, char const* receiver_pubkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   // safety for external clients
   if( !md_crypt_check_init() )
      md_crypt_init();
      
   // load the keys
   EVP_PKEY* pkey = NULL;
   EVP_PKEY* pubkey = NULL;
   
   int rc = md_load_pubkey( &pubkey, receiver_pubkey_pem );
   if( rc != 0 ) {
      errorf("md_load_pubkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_load_privkey( &pkey, sender_pkey_pem );
   if( rc != 0 ) {
      errorf("md_load_privkey rc = %d\n", rc );
      
      EVP_PKEY_free( pubkey );
      return -EINVAL;
   }
   
   rc = md_encrypt( pkey, pubkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( pkey );
   EVP_PKEY_free( pubkey );
   return rc;
}


// helper function for md_decrypt, when we can't deal with EVP_PKEY (i.e. from a python script)
int md_decrypt_pem( char const* sender_pubkey_pem, char const* receiver_privkey_pem, char const* in_data, size_t in_data_len, char** out_data, size_t* out_data_len ) {
   // safety for external clients
   if( !md_crypt_check_init() )
      md_crypt_init();
      
   // load the keys
   EVP_PKEY* pubkey = NULL;
   EVP_PKEY* privkey = NULL;
   
   int rc = md_load_privkey( &privkey, receiver_privkey_pem );
   if( rc != 0 ) {
      errorf("md_load_privkey rc = %d\n", rc );
      return -EINVAL;
   }
   
   rc = md_load_pubkey( &pubkey, sender_pubkey_pem );
   if( rc != 0 ) {
      errorf("md_load_pubkey rc = %d\n", rc );
      
      EVP_PKEY_free( privkey );
      return -EINVAL;
   }
   
   rc = md_decrypt( pubkey, privkey, in_data, in_data_len, out_data, out_data_len );
   
   EVP_PKEY_free( privkey );
   EVP_PKEY_free( pubkey );
   return rc;
}
