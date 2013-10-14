cdef extern from "sys/types.h":
   ctypedef int int64_t
   ctypedef unsigned int uint64_t
   ctypedef int int32_t
   ctypedef unsigned int uint32_t
   ctypedef int bool
   ctypedef int mode_t

cdef extern from "openssl/ssl.h":
   cdef struct EVP_PKEY_TAG:
      pass
   
   ctypedef EVP_PKEY_TAG EVP_PKEY

cdef extern from "ms-client.h":
   cdef struct ms_client_TAG:
      EVP_PKEY* my_key
      uint64_t gateway_id
      uint64_t owner_id
      pass

   ctypedef ms_client_TAG ms_client

   cdef struct md_syndicate_conf

   int ms_client_init( ms_client* client, int gateway_type, md_syndicate_conf* conf )
   int ms_client_destroy( ms_client* client )



cdef extern from "libsyndicate.h":

   cdef struct md_syndicate_conf_TAG:
      pass

   ctypedef md_syndicate_conf_TAG md_syndicate_conf

   cdef int SYNDICATE_UG
   cdef int SYNDICATE_RG
   cdef int SYNDICATE_AG

   # ------------------------------------------
   # init and shutdown
   int md_default_conf( md_syndicate_conf* conf )

   int md_free_conf( md_syndicate_conf* conf )

   int md_init(int gateway_type,
               char* config_file,
               md_syndicate_conf* conf,
               ms_client* client,
               int portnum,
               char* ms_url,
               char* volume_name,
               char* gateway_name,
               char* md_username,
               char* md_password,
               char* volume_key_file,
               char* my_key_file,
               char* tls_key_file,
               char* tls_cert_file
            ) 

   int md_shutdown()
   
   # ------------------------------------------
   # crypto
   
   int md_sign_message( EVP_PKEY* pkey, const char* data, size_t len, char** sigb64, size_t* sigb64len )
   int ms_client_verify_gateway_message( ms_client* client, uint64_t volume_id, uint64_t gateway_id, const char* msg, size_t msg_len, char* sigb64, size_t sigb64_len )