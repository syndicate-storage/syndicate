
"""
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
cdef extern from "ms-client.h":
   cdef struct ms_client:
      EVP_PKEY* my_key
      uint64_t gateway_id
      uint64_t owner_id
      pass

   cdef struct md_syndicate_conf

   int ms_client_init( ms_client* client, int gateway_type, md_syndicate_conf* conf )
   int ms_client_destroy( ms_client* client )

# ------------------------------------------
cdef extern from "libsyndicate.h":

   cdef struct md_syndicate_conf:
      char* local_sd_dir
      pass

   cdef int SYNDICATE_UG
   cdef int SYNDICATE_RG
   cdef int SYNDICATE_AG
   
   ctypedef int (*ms_client_view_change_callback)( ms_client* client, void* cls )

   # ------------------------------------------
   # init and shutdown
   int md_default_conf( md_syndicate_conf* conf, int gateway_type )

   int md_free_conf( md_syndicate_conf* conf )

   int md_init(md_syndicate_conf* conf,
               ms_client* client,
               char* ms_url,
               char* volume_name,
               char* gateway_name,
               char* md_username,
               char* md_password,
               char* volume_key_file,
               char* my_key_file,
               char* tls_key_file,
               char* tls_cert_file,
               char* storage_root
            ) 

   int md_shutdown()
   
   # ------------------------------------------
   # crypto
   
   int md_sign_message( EVP_PKEY* pkey, const char* data, size_t len, char** sigb64, size_t* sigb64len )
   int ms_client_verify_gateway_message( ms_client* client, uint64_t volume_id, uint64_t gateway_id, const char* msg, size_t msg_len, char* sigb64, size_t sigb64_len )
   int md_encrypt_pem( const char* pubkey_pem, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   int md_decrypt_pem( const char* privkey_pem, const char* in_data, size_t in_data_len, char** out_data, size_t* out_data_len )
   
   # ------------------------------------------
   # config
   
   int ms_client_get_closure_text( ms_client* client, char** closure_text, uint64_t* closure_text_len )
   int ms_client_set_view_change_callback( ms_client* client, ms_client_view_change_callback clb, void* cls )
   int ms_client_sched_volume_reload( ms_client* client )
   
