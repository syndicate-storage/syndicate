
"""
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
"""

from libc.stdint cimport int32_t, uint32_t, int64_t, uint64_t

# ------------------------------------------
cdef extern from "sys/types.h":
   ctypedef int bool
   ctypedef int mode_t
   

# ------------------------------------------
cdef extern from "openssl/ssl.h":
   cdef struct EVP_PKEY_TAG:
      pass
   
   ctypedef EVP_PKEY_TAG EVP_PKEY

# ------------------------------------------
cdef extern from "string.h":
   void* memset( void*, int, size_t )

# ------------------------------------------
cdef extern from "libsyndicate/ms/ms-client.h":
   cdef struct ms_client:
      EVP_PKEY* my_key
      uint64_t gateway_id
      uint64_t owner_id
      pass

   cdef struct md_syndicate_conf

   int ms_client_init( ms_client* client, int gateway_type, md_syndicate_conf* conf )
   int ms_client_destroy( ms_client* client )
   char* ms_client_get_hostname( ms_client* client )
   int ms_client_get_portnum( ms_client* client )

# ------------------------------------------
cdef extern from "libsyndicate/util.h":

   cdef struct mlock_buf:
      void* ptr
      size_t len
      
# ------------------------------------------   
cdef extern from "libsyndicate/opts.h":
   cdef struct md_opts:
      char* config_file
      char* username
      mlock_buf password
      char* volume_name
      char* ms_url
      char* gateway_name
      char* volume_pubkey_path
      char* gateway_pkey_path
      mlock_buf gateway_pkey_decryption_password
      char* volume_pubkey_pem
      mlock_buf gateway_pkey_pem
      char* syndicate_pubkey_path
      char* syndicate_pubkey_pem
      mlock_buf user_pkey_pem
      char* tls_pkey_path
      char* tls_cert_path
      char* storage_root
      bool flush_replicas
      size_t cache_soft_limit
      size_t cache_hard_limit

# ------------------------------------------
cdef extern from "libsyndicate/libsyndicate.h":

   cdef struct md_syndicate_conf:
      char* local_sd_dir
      pass

   cdef int SYNDICATE_UG
   cdef int SYNDICATE_RG
   cdef int SYNDICATE_AG

   cdef int GATEWAY_CAP_READ_DATA
   cdef int GATEWAY_CAP_WRITE_DATA
   cdef int GATEWAY_CAP_READ_METADATA
   cdef int GATEWAY_CAP_WRITE_METADATA
   cdef int GATEWAY_CAP_COORDINATE

   
   ctypedef int (*ms_client_view_change_callback)( ms_client* client, void* cls )

   # ------------------------------------------
   # init and shutdown
   int md_default_conf( md_syndicate_conf* conf, int gateway_type )

   int md_free_conf( md_syndicate_conf* conf )
   
   int md_init(md_syndicate_conf* conf, ms_client* client, md_opts* opts) 

   int md_shutdown()
   
   # ------------------------------------------
   # networking 

   int md_set_hostname( md_syndicate_conf* conf, const char* hostname )

   # ------------------------------------------
   # crypto
   

   int md_crypt_init()
   int md_crypt_shutdown()
   int md_sign_message( EVP_PKEY* pkey, const char* data, size_t len, char** sigb64, size_t* sigb64len )
   int ms_client_verify_gateway_message( ms_client* client, uint64_t volume_id, uint64_t gateway_id, const char* msg, size_t msg_len, char* sigb64, size_t sigb64_len )
   int md_encrypt_pem( const char* sender_privkey_pem, const char* receiver_pubkey_pem, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   int md_decrypt_pem( const char* sender_pubkey_pem, const char* receiver_privkey_pepm, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   int md_password_seal( const char* data, size_t data_len, const char* password, size_t password_len, char** output, size_t* output_len )
   int md_password_unseal( const char* encrypted_data, size_t encrypted_data_len, const char* password, size_t password_len, char** output, size_t* output_len )
   int md_encrypt_symmetric( const unsigned char* key, size_t key_len, char* data, size_t data_len, char** ciphertext, size_t* ciphertext_len )
   int md_decrypt_symmetric( const unsigned char* key, size_t key_len, char* ciphertext_data, size_t ciphertext_len, char** data, size_t* data_len )
   
   # ------------------------------------------
   # config
   
   int ms_client_get_closure_text( ms_client* client, char** closure_text, uint64_t* closure_text_len )
   int ms_client_set_view_change_callback( ms_client* client, ms_client_view_change_callback clb, void* cls )
   int ms_client_sched_volume_reload( ms_client* client )
   int ms_client_my_key_pem( ms_client* client, char** my_key_pem, size_t* my_key_len )

   # ------------------------------------------
   # debugging 

   int md_debug( md_syndicate_conf* conf, int level )

   # ------------------------------------------
   # queries 
   
   int ms_client_check_gateway_caps( ms_client* client, uint64_t gateway_type, uint64_t gateway_id, uint64_t caps )
   int ms_client_get_gateway_type( ms_client* client, uint64_t g_id )

   # ------------------------------------------
   # OpenID RPC
   
   int ms_client_openid_rpc( const char* ms_openid_url, const char* username, const char* password, const char* request_type, const char* request_buf, size_t request_len, char** response_buf, size_t* response_len )
   int ms_client_openid_auth_rpc( const char* ms_openid_url, const char* username, const char* password,
                                  const char* request_type, const char* request_buf, size_t request_len, char** response_buf, size_t* response_len,
                                  char* syndicate_pubkey_pem )
