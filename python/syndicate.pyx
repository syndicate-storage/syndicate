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
import pickle

syndicate_inited = False
syndicate_ref = None
syndicate_view_change_callback = None
runtime_privkey_path = None 

# ------------------------------------------
cdef int py_view_change_callback_springboard( ms_client* client, void* cls ):
   global syndicate_view_change_callback
   
   if syndicate_view_change_callback is not None:
      return syndicate_view_change_callback()
   
   return 0

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
   
   try:
      closure_secrets_serialized = pickle.dumps( closure_secrets )
   except Exception, e:
      return (-errno.EINVAL, None)

   return encrypt_data( sender_privkey_str, gateway_pubkey_str, closure_secrets_serialized )


# ------------------------------------------
cpdef decrypt_closure_secrets( sender_pubkey_str, gateway_privkey_str, closure_secrets ):
   '''
      Decrypt a string with a gateway's public key.
   '''
   
   rc, py_serialized_secrets = decrypt_data( sender_pubkey_str, gateway_privkey_str, closure_secrets )

   if rc != 0:
      return (rc, None)

   else:      
      # try to deserialize
      try:
         secrets_dict = pickle.loads( py_serialized_secrets )
         return (0, secrets_dict)
      except Exception, e:
         return (-errno.ENODATA, None)


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

   rc = md_password_unseal_UNSAFE( c_input_buf, c_input_buf_len, c_password, c_password_len, &c_output_buf, &c_output_buf_len )
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
cdef class Syndicate:
   '''
      Python interface to libsyndicate.
   '''
   
   GATEWAY_TYPE_UG = SYNDICATE_UG
   GATEWAY_TYPE_RG = SYNDICATE_RG
   GATEWAY_TYPE_AG = SYNDICATE_AG

   CAP_READ_DATA        = GATEWAY_CAP_READ_DATA
   CAP_WRITE_DATA       = GATEWAY_CAP_WRITE_DATA
   CAP_READ_METADATA    = GATEWAY_CAP_READ_METADATA
   CAP_WRITE_METADATA   = GATEWAY_CAP_WRITE_METADATA
   CAP_COORDINATE       = GATEWAY_CAP_COORDINATE

   
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
                       ms_url=None,
                       username=None,
                       password=None,
                       user_pkey_pem=None,
                       gateway_pkey_decryption_password=None,
                       volume_name=None,
                       volume_pubkey_path=None,
                       config_file=None,
                       gateway_pkey_path=None,
                       tls_pkey_path=None,
                       tls_cert_path=None,
                       storage_root=None,
                       debug_level=0,
                       syndicate_pubkey_path=None):

      '''
         Initialize libsyndicate.
         TODO: this method might not always be called, or called twice.  Need to be robust against it.
      '''
      
      global syndicate_inited
      global runtime_privkey_path
      
      if syndicate_inited:
         raise Exception("Syndicate already initialized!  Use Syndicate.getinstance() to get a reference to the API.")
      
      cdef:
         char *c_gateway_name = NULL
         char *c_ms_url = NULL
         char *c_username = NULL
         char *c_password = NULL
         char *c_user_pkey_pem = NULL
         char* c_gateway_pkey_decryption_password = NULL
         char *c_volume_name = NULL
         char *c_volume_pubkey_path = NULL
         char *c_config_file = NULL
         char *c_gateway_pkey_path = NULL
         char *c_tls_pkey_path = NULL
         char *c_tls_cert_path = NULL
         char* c_storage_root = NULL
         char* c_syndicate_pubkey_path = NULL

      if gateway_name is not None:
         c_gateway_name = gateway_name
      
      if ms_url is not None:
         c_ms_url = ms_url

      if username is not None:
         c_username = username 
         
      if password is not None:
         c_password = password

      if user_pkey_pem is not None:
         c_user_pkey_pem = user_pkey_pem

      if gateway_pkey_decryption_password is not None:
         c_gateway_pkey_decryption_password = gateway_pkey_decryption_password
         
      if volume_name is not None:
         c_volume_name = volume_name 
         
      if config_file is not None:
         c_config_file = config_file 

      if volume_pubkey_path is not None:
         c_volume_pubkey_path = volume_pubkey_path
      
      if gateway_pkey_path is not None:
         c_gateway_pkey_path = gateway_pkey_path
         runtime_privkey_path = gateway_pkey_path
         
      if tls_pkey_path is not None:
         c_tls_pkey_path = tls_pkey_path
      
      if tls_cert_path is not None:
         c_tls_cert_path = tls_cert_path

      if storage_root is not None:
         c_storage_root = storage_root

      if syndicate_pubkey_path is not None:
         c_syndicate_pubkey_path = syndicate_pubkey_path

      # initialize configuration first
      md_default_conf( &self.conf_inst, gateway_type )

      # initialize debugging
      md_debug( &self.conf_inst, debug_level )

      rc = md_init(  &self.conf_inst,
                     &self.client_inst,
                     c_ms_url,
                     c_volume_name,
                     c_gateway_name,
                     c_username,
                     c_password,
                     c_user_pkey_pem,
                     c_volume_pubkey_path,
                     c_gateway_pkey_path,
                     c_gateway_pkey_decryption_password,
                     c_tls_pkey_path,
                     c_tls_cert_path,
                     c_storage_root,
                     c_syndicate_pubkey_path)
      
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
                           config_file=None,
                           gateway_pkey_path=None,
                           tls_pkey_path=None,
                           tls_cert_path=None,
                           storage_root=None,
                           debug_level=0,
                           syndicate_pubkey_path=None):
      
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
                                    config_file=config_file,
                                    gateway_pkey_path=gateway_pkey_path,
                                    gateway_pkey_decryption_password=gateway_pkey_decryption_password,
                                    tls_pkey_path=tls_pkey_path,
                                    tls_cert_path=tls_cert_path,
                                    storage_root=storage_root,
                                    debug_level=debug_level,
                                    syndicate_pubkey_path=syndicate_pubkey_path)
         
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
      hostname = ms_client_get_hostname( &self.client_inst )
      if hostname != None:
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
      
      rc = md_sign_message( self.client_inst.my_key, c_data, c_data_len, &sigb64, &sigb64_len )
      
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
         
      
   cpdef set_view_change_callback( self, callback_func ):
      '''
         Set the callback to be called when the Volume information gets changed.
      '''
      global syndicate_view_change_callback
      
      syndicate_view_change_callback = callback_func
      
      ms_client_set_view_change_callback( &self.client_inst, py_view_change_callback_springboard, NULL )
      
      return
   
   
   cpdef sched_reload( self ):
      '''
         Schedule a reload of our configuration.
      '''
      ms_client_sched_volume_reload( &self.client_inst )
      
      return
   
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

      rc = ms_client_my_key_pem_UNSAFE( &self.client_inst, &c_privkey_pem, &c_privkey_len );
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

   cpdef check_gateway_caps( self, gw_type, gw_id, caps ):
      '''
         Check a gateway's capabilities.
         Return 0 if the capabilities (caps, a bit field) match those in the cert.
      '''

      return ms_client_check_gateway_caps( &self.client_inst, gw_type, gw_id, caps )
