
"""
   Copyright 2015 The Trustees of Princeton University

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
      EVP_PKEY* gateway_key
      uint64_t gateway_id
      uint64_t owner_id
      pass

   cdef struct md_syndicate_conf
   
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
      pass

   md_opts* md_opts_new( int count )
   void md_opts_set_client( md_opts* opts, bool client )
   void md_opts_set_ignore_driver( md_opts* opts, bool ignore_driver )
   void md_opts_set_gateway_type( md_opts* opts, uint64_t type )

   void md_opts_set_config_file( md_opts* opts, char* config_filepath )
   void md_opts_set_username( md_opts* opts, char* username )
   void md_opts_set_volume_name( md_opts* opts, char* volume_name )
   void md_opts_set_gateway_name( md_opts* opts, char* gateway_name )
   void md_opts_set_ms_url( md_opts* opts, char* ms_url )
   void md_opts_set_foreground( md_opts* opts, bool foreground )

# ------------------------------------------   
cdef extern from "libsyndicate/gateway.h":

   cdef struct SG_gateway:
      pass 

   int SG_gateway_init( SG_gateway* gateway, uint64_t gateway_type, int argc, char** argv, md_opts* overrides )
   int SG_gateway_init_opts( SG_gateway* gateway, md_opts* opts )
   int SG_gateway_shutdown( SG_gateway* gateway )

   int SG_gateway_id( SG_gateway* gateway )
   int SG_gateway_user_id( SG_gateway* gateway )
   ms_client* SG_gateway_ms( SG_gateway* gateway )
   md_syndicate_conf* SG_gateway_conf( SG_gateway* gateway )

# ------------------------------------------
cdef extern from "libsyndicate/libsyndicate.h":

   cdef struct md_syndicate_conf:
      char* local_sd_dir
      pass

   cdef int SG_CAP_READ_DATA
   cdef int SG_CAP_WRITE_DATA
   cdef int SG_CAP_READ_METADATA
   cdef int SG_CAP_WRITE_METADATA
   cdef int SG_CAP_COORDINATE
   
   # ------------------------------------------
   # init and shutdown
   
   int md_default_conf( md_syndicate_conf* conf, int gateway_type )

   int md_free_conf( md_syndicate_conf* conf )
   
   int md_init(md_syndicate_conf* conf, ms_client* client, md_opts* opts) 

   int md_shutdown()
   
   # ------------------------------------------
   # networking 

   char* md_get_hostname( md_syndicate_conf* conf )
   int md_set_hostname( md_syndicate_conf* conf, const char* hostname )

   # ------------------------------------------
   # crypto

   int md_crypt_init()
   int md_crypt_shutdown()
   int md_sign_message( EVP_PKEY* pkey, const char* data, size_t len, char** sigb64, size_t* sigb64len )
   int ms_client_sign_gateway_message( ms_client* client, const char* data, size_t len, char** sigb64, size_t* sigb64_len )
   int ms_client_verify_gateway_message( ms_client* client, uint64_t volume_id, uint64_t gateway_id, const char* msg, size_t msg_len, char* sigb64, size_t sigb64_len )
   int md_encrypt_pem( const char* sender_privkey_pem, const char* receiver_pubkey_pem, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   int md_decrypt_pem( const char* sender_pubkey_pem, const char* receiver_privkey_pepm, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   int md_encrypt_symmetric( const unsigned char* key, size_t key_len, char* data, size_t data_len, char** ciphertext, size_t* ciphertext_len )
   int md_decrypt_symmetric( const unsigned char* key, size_t key_len, char* ciphertext_data, size_t ciphertext_len, char** data, size_t* data_len )
   
   # ------------------------------------------
   # config
   
   int ms_client_gateway_get_driver_text( ms_client* client, char** driver_text, uint64_t* driver_text_len )
   int ms_client_gateway_key_pem( ms_client* client, char** gateway_key_pem, size_t* gateway_key_len )

   # ------------------------------------------
   # debugging 

   int md_debug( md_syndicate_conf* conf, int level )

   # ------------------------------------------
   # queries 
   
   int ms_client_check_gateway_caps( ms_client* client, uint64_t gateway_id, uint64_t caps )
   uint64_t ms_client_get_gateway_type( ms_client* client, uint64_t g_id )
   uint64_t ms_client_get_gateway_id( ms_client* client )
   uint64_t ms_client_get_owner_id( ms_client* client )

  
# ------------------------------------------
cdef class Syndicate:
    
   cdef SG_gateway gateway_inst

   cdef md_opts* opts_to_syndicate( cls, opts )
 
   cpdef sign_message( self, data )
   cpdef verify_gateway_message( self, gateway_id, volume_id, message_bits, sigb64 )

   cpdef get_driver_text( self )
   cpdef get_gateway_private_key_pem( self )
   cpdef get_gateway_type( self, gw_id )
   cpdef check_gateway_caps( self, gw_id, caps )
