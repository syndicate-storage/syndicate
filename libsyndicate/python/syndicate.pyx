from syndicate cimport *
cimport libc.stdlib as stdlib

import errno

cdef class Syndicate:
   '''
      Python interface to libsyndicate.
   '''
   
   GATEWAY_TYPE_UG = SYNDICATE_UG
   GATEWAY_TYPE_RG = SYNDICATE_RG
   GATEWAY_TYPE_AG = SYNDICATE_AG
   
   cdef md_syndicate_conf conf_inst
   cdef ms_client client_inst

   def __cinit__(self):
      pass

   def __dealloc__(self):
      ms_client_destroy( &self.client_inst )
      md_free_conf( &self.conf_inst )
      md_shutdown()

   def __init__( self, gateway_type=0,
                       gateway_name=None,
                       portnum=0,
                       ms_url=None,
                       gateway_pass=None,
                       gateway_cred=None,
                       volume_name=None,
                       volume_key_filename=None,
                       conf_filename=None,
                       my_key_filename=None,
                       tls_pkey_filename=None,
                       tls_cert_filename=None ):

      '''
         Initialize libsyndicate.
      '''

      cdef:
         char *c_gateway_name = NULL
         char *c_ms_url = NULL
         char *c_gateway_cred = NULL
         char *c_gateway_pass = NULL
         char *c_volume_name = NULL
         char *c_volume_key_filename = NULL
         char *c_conf_filename = NULL
         char *c_my_key_filename = NULL
         char *c_tls_pkey_filename = NULL
         char *c_tls_cert_filename = NULL

      if gateway_name != None:
         c_gateway_name = gateway_name
      
      if ms_url != None:
         c_ms_url = ms_url

      if gateway_cred != None:
         c_gateway_cred = gateway_cred 
         
      if gateway_pass != None:
         c_gateway_pass = gateway_pass
         
      if volume_name != None:
         c_volume_name = volume_name 
         
      if conf_filename != None:
         c_conf_filename = conf_filename 
      
      if my_key_filename != None:
         c_my_key_filename = my_key_filename
         
      if tls_pkey_filename != None:
         c_tls_pkey_filename = tls_pkey_filename
      
      if tls_cert_filename != None:
         c_tls_cert_filename = tls_cert_filename

      rc = md_init(  gateway_type,
                     c_gateway_name,
                     &self.conf_inst,
                     &self.client_inst,
                     portnum,
                     c_ms_url,
                     c_volume_name,
                     c_gateway_name,
                     c_gateway_cred,
                     c_gateway_pass,
                     c_volume_key_filename,
                     c_my_key_filename,
                     c_tls_pkey_filename,
                     c_tls_cert_filename )
      
      if rc != 0:
         raise Exception( "md_init rc = %d" % rc )
      
   
   def gateway_id( self ):
      '''
         Get our gateway ID
      '''
      return self.client_inst.gateway_id
   
   def owner_id( self ):
      '''
         Get our user ID
      '''
      return self.client_inst.owner_id
   
   cdef sign_message( self, data ):
      '''
         Sign a message with our private key.
         Return a base64-encoded string containing the signature.
         Raises an exception on error.
      '''
      
      cdef char* c_data = data
      cdef size_t c_data_len = len(data)
      
      cdef char* sigb64 = NULL
      cdef size_t sigb64_len = 0
      
      rc = md_sign_message( self.client_inst.my_key, c_data, c_data_len, &sigb64, &sigb64_len )
      
      if rc != 0:
         raise Exception("md_sign_message rc = %d" % rc )
      
      py_sigb64 = sigb64[:sigb64_len]
      stdlib.free( sigb64 )
      
      return py_sigb64
   
   cdef verify_gateway_message( self, gateway_id, volume_id, message_bits, sigb64 ):
      '''
         Verify a User SG message's authenticity, given the ID of the sender User SG,
         the ID of the Volume to which it claims to belong, the base64-encoded
         message signature, and the serialized (protobuf'ed) message (with the protobuf's
         signature field set to "")
         
         This method checks libsyndicate's internal cached certificate bundle.
         If there is no valid certificate on file for this gateway in this volume,
         libsyndicate will re-request the certificate bundle and return -errno.EAGAIN
      '''
      
      cdef char* c_message_bits = message_bits
      cdef size_t c_message_len = len(message_bits)
      
      cdef char* c_sigb64 = sigb64 
      cdef size_t c_sigb64_len = len(sigb64)
      
      rc = ms_client_verify_gateway_message( &self.client_inst, volume_id, gateway_id, c_message_bits, c_message_len, c_sigb64, c_sigb64_len )
      
      if rc == 0:
         return True
      
      if rc == -errno.EAGAIN:
         return rc
      
      return False
   
   
                   