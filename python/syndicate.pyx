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

from syndicate cimport *
cimport libc.stdlib as stdlib

import types
import errno

syndicate_inited = False
syndicate_ref = None
runtime_privkey_path = None 


# ------------------------------------------
cpdef encrypt_data( sender_privkey_str, receiver_pubkey_str, data_str ):

   cdef char* c_data_str = data_str
   cdef size_t c_data_str_len = len(data_str)
   
   cdef char* c_encrypted_data = NULL
   cdef size_t c_encrypted_data_len = 0
   
   rc = md_encrypt_pem( sender_privkey_str, receiver_pubkey_str, c_data_str, c_data_str_len, &c_encrypted_data, &c_encrypted_data_len )
   if rc != 0:
      return (rc, None)
   
   else:
      py_encrypted_data = None
      try:
         py_encrypted_data = c_encrypted_data[:c_encrypted_data_len]
      except MemoryError:
         py_encrypted_data = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_encrypted_data )
      
      return (rc, py_encrypted_data)


# ------------------------------------------
cpdef decrypt_data( sender_pubkey_str, receiver_privkey_str, encrypted_data_str ):
   
   cdef char* c_encrypted_data_str = encrypted_data_str
   cdef size_t c_encrypted_data_str_len = len(encrypted_data_str)
   
   cdef char* c_data_str = NULL
   cdef size_t c_data_str_len = 0
   
   rc = md_decrypt_pem( sender_pubkey_str, receiver_privkey_str, c_encrypted_data_str, c_encrypted_data_str_len, &c_data_str, &c_data_str_len )
   if rc != 0:
      return (rc, None)
   
   else:
      py_data_str = None
      try:
         py_data_str = c_data_str[:c_data_str_len]
      except MemoryError:
         py_data_str = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_data_str )

      return (rc, py_data_str)
      

# ------------------------------------------
cpdef encrypt_closure_secrets( sender_privkey_str, gateway_pubkey_str, closure_secrets ):
   '''
      Encrypt a string with a gateway's public key.
   '''

   return encrypt_data( sender_privkey_str, gateway_pubkey_str, closure_secrets )


# ------------------------------------------
cpdef decrypt_closure_secrets( sender_pubkey_str, gateway_privkey_str, closure_secrets ):
   '''
      Decrypt a string with a gateway's public key.
   '''
   
   return decrypt_data( sender_pubkey_str, gateway_privkey_str, closure_secrets )
      

# ------------------------------------------
cpdef openid_rpc( ms_openid_url, username, password, rpc_type, request_buf ):
   '''
      Perform an RPC call to the MS, via OpenID
   '''

   cdef char* c_request_buf = request_buf
   cdef size_t c_request_buf_len = len(request_buf)

   cdef char* c_response_buf = NULL
   cdef size_t c_response_buf_len = 0

   rc = ms_client_openid_rpc( ms_openid_url, username, password, rpc_type, c_request_buf, c_request_buf_len, &c_response_buf, &c_response_buf_len )
   if rc != 0:
      return (rc, None)

   else:
      py_response_buf = None
      try:
         py_response_buf = c_response_buf[:c_response_buf_len]
      except MemoryError:
         py_response_buf = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_response_buf )

      return (rc, py_response_buf)


# ------------------------------------------
cpdef openid_auth_rpc( ms_openid_url, username, password, rpc_type, request_buf, syndicate_pubkey_pem ):
   '''
      Perform an RPC call to the MS, via OpenID.  Verify the call with the Syndicate public key.
   '''

   cdef char* c_request_buf = request_buf
   cdef size_t c_request_buf_len = len(request_buf)

   cdef char* c_response_buf = NULL
   cdef size_t c_response_buf_len = 0

   cdef char* c_syndicate_pubkey_pem = syndicate_pubkey_pem

   rc = ms_client_openid_auth_rpc( ms_openid_url, username, password, rpc_type, c_request_buf, c_request_buf_len, &c_response_buf, &c_response_buf_len, c_syndicate_pubkey_pem )
   if rc != 0:
      return (rc, None)

   else:
      py_response_buf = None
      try:
         py_response_buf = c_response_buf[:c_response_buf_len]
      except MemoryError:
         py_response_buf = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_response_buf )

      return (rc, py_response_buf)

# ------------------------------------------
cpdef password_seal( input_buf, password ):
   '''
      Seal data with a password
   '''
   cdef char* c_input_buf = input_buf 
   cdef size_t c_input_buf_len = len(input_buf)

   cdef char* c_password = password 
   cdef size_t c_password_len = len(password)

   cdef char* c_output_buf = NULL
   cdef size_t c_output_buf_len = 0

   rc = md_password_seal( c_input_buf, c_input_buf_len, c_password, c_password_len, &c_output_buf, &c_output_buf_len)
   if rc != 0:
      return (rc, None)

   else:
      py_sealed_data = None
      try:
         py_sealed_data = c_output_buf[:c_output_buf_len]
      except MemoryError:
         py_sealed_data = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_output_buf )

      return (rc, py_sealed_data)
   

# ------------------------------------------
cpdef password_unseal( input_buf, password ):
   '''
      Unseal data with a password 
   '''

   cdef char* c_input_buf = input_buf 
   cdef size_t c_input_buf_len = len(input_buf)

   cdef char* c_password = password 
   cdef size_t c_password_len = len(password)

   cdef char* c_output_buf = NULL
   cdef size_t c_output_buf_len = 0
   
   # NOTE: we have to use the unsafe version here (which does NOT mlock the output'ed data),
   # since Python doesn't seem to have a way to munlock the memory when it gets garbage-collected :(

   rc = md_password_unseal( c_input_buf, c_input_buf_len, c_password, c_password_len, &c_output_buf, &c_output_buf_len )
   if rc != 0:
      return (rc, None)

   else:
      py_output = None
      try:
         py_output = c_output_buf[:c_output_buf_len]
      except MemoryError:
         py_output = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_output_buf )

      return (rc, py_output)


# ------------------------------------------
cpdef symmetric_seal( input_buf, key ):
   '''
      Seal data with a (256-bit) key 
   '''

   cdef char* c_input_buf = input_buf 
   cdef size_t c_input_buf_len = len(input_buf)
   
   cdef unsigned char* c_key = key 
   cdef size_t c_key_len = len(key)

   cdef char* c_output_buf = NULL
   cdef size_t c_output_buf_len = 0

   rc = md_encrypt_symmetric( c_key, c_key_len, c_input_buf, c_input_buf_len, &c_output_buf, &c_output_buf_len );
   if rc != 0:
      return (rc, None)

   else:
      py_output = None
      try:
         py_output = c_output_buf[:c_output_buf_len]
      except MemoryError:
         py_output = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_output_buf )

      return (rc, py_output)


# ------------------------------------------
cpdef symmetric_unseal( input_buf, key ):
   '''
      Unseal data with a (256-bit) key generated by symmetric_seal()
   '''

   cdef char* c_input_buf = input_buf 
   cdef size_t c_input_buf_len = len(input_buf)
   
   cdef unsigned char* c_key = key 
   cdef size_t c_key_len = len(key)

   cdef char* c_output_buf = NULL
   cdef size_t c_output_buf_len = 0

   # NOTE: we have to use the unsafe version here (which does NOT mlock the output'ed data),
   # since Python doesn't seem to have a way to munlock the memory when it gets garbage-collected :(
   
   rc = md_decrypt_symmetric( c_key, c_key_len, c_input_buf, c_input_buf_len, &c_output_buf, &c_output_buf_len );
   if rc != 0:
      return (rc, None)

   else:
      py_output = None
      try:
         py_output = c_output_buf[:c_output_buf_len]
      except MemoryError:
         py_output = None
         rc = -errno.ENOMEM
      finally:
         stdlib.free( c_output_buf )

      return (rc, py_output)


# ------------------------------------------
cpdef crypto_init():
   return md_crypt_init()

# ------------------------------------------
cpdef crypto_shutdown():
   return md_crypt_shutdown()

# ------------------------------------------
cdef class Syndicate:
   '''
      Python interface to libsyndicate.
   '''
   
   GATEWAY_TYPE_UG = SYNDICATE_UG
   GATEWAY_TYPE_RG = SYNDICATE_RG
   GATEWAY_TYPE_AG = SYNDICATE_AG

   CAP_READ_DATA        = SG_CAP_READ_DATA
   CAP_WRITE_DATA       = SG_CAP_WRITE_DATA
   CAP_READ_METADATA    = SG_CAP_READ_METADATA
   CAP_WRITE_METADATA   = SG_CAP_WRITE_METADATA
   CAP_COORDINATE       = SG_CAP_COORDINATE

   
   cdef md_syndicate_conf conf_inst
   cdef ms_client client_inst
   
   def __cinit__(self):
      pass

   def __dealloc__(self):
      ms_client_destroy( &self.client_inst )
      md_free_conf( &self.conf_inst )
      md_shutdown()

   cdef char* string_or_null( self, s ):
      if s is None:
         return NULL
      else:
         return str(s)
   
   cdef size_t strlen_or_zero( self, s ):
      if s is not None:
         return len(s)
      else:
         return 0

   def __init__( self, gateway_type=0,
                       gateway_name=None,
                       ms_url=None,
                       username=None,
                       password=None,
                       user_pkey_pem=None,
                       gateway_pkey_decryption_password=None,
                       volume_name=None,
                       volume_pubkey_path=None,
                       volume_pubkey_pem=None,
                       config_file=None,
                       gateway_pkey_path=None,
                       gateway_pkey_pem=None,
                       tls_pkey_path=None,
                       tls_cert_path=None,
                       storage_root=None,
                       debug_level=0,
                       syndicate_pubkey_path=None,
                       syndicate_pubkey_pem=None,
                       hostname=None):

      '''
         Initialize libsyndicate.
         TODO: this method might not always be called, or called twice.  Need to be robust against it.
      '''
      
      global syndicate_inited
      global runtime_privkey_path
      
      if syndicate_inited:
         raise Exception("Syndicate already initialized!  Use Syndicate.getinstance() to get a reference to the API.")
      
      # initialize configuration first
      md_default_conf( &self.conf_inst, gateway_type )

      # set the hostname, if needed
      if hostname is not None:
         md_set_hostname( &self.conf_inst, hostname )

      # initialize debugging
      md_debug( &self.conf_inst, debug_level )

      cdef md_opts opts
      memset( &opts, 0, sizeof(opts) )
      
      opts.gateway_name = self.string_or_null( gateway_name )
      opts.ms_url = self.string_or_null( ms_url )
      opts.username = self.string_or_null( username )
      opts.volume_name = self.string_or_null( volume_name )
      opts.volume_pubkey_path = self.string_or_null( volume_pubkey_path )
      opts.volume_pubkey_pem = self.string_or_null( volume_pubkey_pem )
      opts.config_file = self.string_or_null( config_file )
      opts.storage_root = self.string_or_null( storage_root )
      opts.gateway_pkey_path = self.string_or_null( gateway_pkey_path )
      opts.tls_pkey_path = self.string_or_null( tls_pkey_path )
      opts.tls_cert_path = self.string_or_null( tls_cert_path )
      opts.syndicate_pubkey_path = self.string_or_null( syndicate_pubkey_path )
      opts.syndicate_pubkey_pem = self.string_or_null( syndicate_pubkey_pem )

      # NOTE: not mlock'ed!
      opts.gateway_pkey_pem.ptr = self.string_or_null( gateway_pkey_pem )
      opts.gateway_pkey_pem.len = self.strlen_or_zero(gateway_pkey_pem)
      
      # NOTE: not mlock'ed!
      opts.gateway_pkey_decryption_password.ptr = self.string_or_null( gateway_pkey_decryption_password )
      opts.gateway_pkey_decryption_password.len = self.strlen_or_zero( gateway_pkey_decryption_password )

      # NOTE: not mlock'ed!
      opts.password.ptr = self.string_or_null( password )
      opts.password.len = self.strlen_or_zero(password)
      
      # NOTE: not mlock'ed
      opts.user_pkey_pem.ptr = self.string_or_null( user_pkey_pem )
      opts.user_pkey_pem.len = self.strlen_or_zero(user_pkey_pem)

      rc = md_init( &self.conf_inst, &self.client_inst, &opts );

      if rc != 0:
         raise Exception( "md_init rc = %d" % rc )
      
      syndicate_inited = True
      
      
   @classmethod
   def getinstance( self,  gateway_type=0,
                           gateway_name=None,
                           ms_url=None,
                           username=None,
                           password=None,
                           user_pkey_pem=None,
                           gateway_pkey_decryption_password=None,
                           volume_name=None,
                           volume_pubkey_path=None,
                           volume_pubkey_pem=None,
                           config_file=None,
                           gateway_pkey_path=None,
                           gateway_pkey_pem=None,
                           tls_pkey_path=None,
                           tls_cert_path=None,
                           storage_root=None,
                           debug_level=0,
                           syndicate_pubkey_path=None,
                           syndicate_pubkey_pem=None,
                           hostname=None):
      
      '''
         Get the current Syndicate instance,
         instantiating it if needed.
         Call this method instead of the constructor.
      '''
      
      global syndicate_ref 
      
      if syndicate_ref == None:
         syndicate_ref = Syndicate( gateway_type=gateway_type,
                                    gateway_name=gateway_name,
                                    ms_url=ms_url,
                                    username=username,
                                    password=password,
                                    user_pkey_pem=user_pkey_pem,
                                    volume_name=volume_name,
                                    volume_pubkey_path=volume_pubkey_path,
                                    volume_pubkey_pem=volume_pubkey_pem,
                                    config_file=config_file,
                                    gateway_pkey_path=gateway_pkey_path,
                                    gateway_pkey_pem=gateway_pkey_pem,
                                    gateway_pkey_decryption_password=gateway_pkey_decryption_password,
                                    tls_pkey_path=tls_pkey_path,
                                    tls_cert_path=tls_cert_path,
                                    storage_root=storage_root,
                                    debug_level=debug_level,
                                    syndicate_pubkey_path=syndicate_pubkey_path,
                                    syndicate_pubkey_pem=syndicate_pubkey_pem,
                                    hostname=hostname)
         
      return syndicate_ref
   
   
   def gateway_id( self ):
      '''
         Get the gateway ID
      '''
      return self.client_inst.gateway_id
   
   
   def owner_id( self ):
      '''
         Get the user ID
      '''
      return self.client_inst.owner_id
   
   
   def portnum( self ):
      '''
         Get the portnum this gateway should listen on.
      '''
      return ms_client_get_portnum( &self.client_inst )


   def hostname( self ):
      '''
         Get the hostname the cert says we're supposed to listen on.
      '''
      cdef char* hostname = NULL;
      hostname = md_get_hostname( &self.conf_inst )
      if hostname != NULL:
         ret = hostname[:]
         stdlib.free( hostname )
         return ret 

      else:
         return None


   cpdef sign_message( self, data ):
      '''
         Sign a message with the gateway's private key.
         Return a base64-encoded string containing the signature.
         Raises an exception on error.
      '''
      
      cdef char* c_data = data
      cdef size_t c_data_len = len(data)
      
      cdef char* sigb64 = NULL
      cdef size_t sigb64_len = 0
      
      rc = md_sign_message( self.client_inst.gateway_key, c_data, c_data_len, &sigb64, &sigb64_len )
      
      if rc != 0:
         raise Exception("md_sign_message rc = %d" % rc )
      
      py_sigb64 = sigb64[:sigb64_len]
      stdlib.free( sigb64 )
      
      return py_sigb64
   
   
   cpdef verify_gateway_message( self, gateway_id, volume_id, message_bits, sigb64 ):
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
   
   
   cpdef get_closure_text( self ):
      '''
         Get a copy of the closure text.
      '''
      
      cdef char* c_closure_text = NULL
      cdef uint64_t c_closure_len = 0
      
      rc = ms_client_get_closure_text( &self.client_inst, &c_closure_text, &c_closure_len )
      
      if rc == 0:
         py_closure_text = c_closure_text[:c_closure_len]
         stdlib.free( c_closure_text )
         
         return py_closure_text
      
      elif rc == -errno.ENOTCONN:
         # something's seriously wrong
         raise Exception( "No certificate for this gateway on file!" )
      
      else:
         return None
   
   
   cpdef get_sd_path( self ):
      '''
         Get the location of our local storage drivers.
      '''
      cdef char* c_sd_path = NULL
      
      c_sd_path = self.conf_inst.local_sd_dir
      
      if c_sd_path != NULL:
         py_sd_path = <bytes> c_sd_path
         return py_sd_path
      
      return None

   
   cpdef get_gateway_private_key_pem( self ):
      '''
         Get the gateway private key (PEM-encoded).
      '''

      cdef char* c_privkey_pem = NULL
      cdef size_t c_privkey_len = 0

      rc = ms_client_gateway_key_pem( &self.client_inst, &c_privkey_pem, &c_privkey_len );
      if rc == 0:
         py_privkey_pem = None
         try:
            py_privkey_pem = c_privkey_pem[:c_privkey_len]
         except MemoryError, e:
            rc = -errno.ENOMEM
         finally:
            stdlib.free( c_privkey_pem )

         return (rc, py_privkey_pem)
      
      else:
         return (rc, None)


   cpdef get_gateway_type( self, gw_id ):
      '''
         Get the type of gateway, given its ID.
      '''

      return ms_client_get_gateway_type( &self.client_inst, gw_id )


   cpdef check_gateway_caps( self, gw_id, caps ):
      '''
         Check a gateway's capabilities.
         Return 0 if the capabilities (caps, a bit field) match those in the cert.
      '''

      return ms_client_check_gateway_caps( &self.client_inst, gw_id, caps )
