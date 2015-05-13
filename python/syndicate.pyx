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
from cpython.string cimport PyString_AsString

import types
import errno

syndicate_inited = False
syndicate_ref = None
runtime_privkey_path = None 
crypt_inited = False

class SyndicateException( Exception ):
   pass

# ------------------------------------------
cpdef crypto_init():
   return md_crypt_init()

# ------------------------------------------
cpdef crypto_shutdown():
   return md_crypt_shutdown()

# ------------------------------------------
cpdef crypt_ensure_inited():

   global crypt_inited

   if crypt_inited:
      return 0
   else:
      rc = md_crypt_init()
      if rc == 0:
         crypt_inited = True
      return rc

# ------------------------------------------
cpdef crypt_ensure_shutdown():

   global crypt_inited

   if not crypt_inited:
      return 0
   else:
      rc = md_crypt_shutdown()
      if rc == 0:
         crypt_inited = False 
      return rc

# ------------------------------------------
cpdef encrypt_data( sender_privkey_str, receiver_pubkey_str, data_str ):

   crypt_ensure_inited()

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
   
   crypt_ensure_inited()

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

   crypt_ensure_inited()
   
   return encrypt_data( sender_privkey_str, gateway_pubkey_str, closure_secrets )


# ------------------------------------------
cpdef decrypt_closure_secrets( sender_pubkey_str, gateway_privkey_str, closure_secrets ):
   '''
      Decrypt a string with a gateway's public key.
   '''
   
   crypt_ensure_inited()
   
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

   crypt_ensure_inited()
   
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

   crypt_ensure_inited()
   
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

   crypt_ensure_inited()
   
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

   crypt_ensure_inited()
   
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
cdef char* string_or_null( s ):
   if s is None:
      return NULL
   else:
      return str(s)
   
# ------------------------------------------
cdef size_t strlen_or_zero( s ):
   if s is not None:
      return len(s)
   else:
      return 0

# ------------------------------------------
cdef int int_or_zero( i ):
   if i is not None:
      return int(i)
   else:
      return 0

# ------------------------------------------
cdef int bool_or_false( b ):
   if b is not None:
      if b:
         return 1
      else:
         return 0
   else:
      return 0

# ------------------------------------------
cdef class Syndicate:
   '''
      Python interface to libsyndicate.
      Used to create Pythonic gateways
   '''
   
   GATEWAY_TYPE_UG = SYNDICATE_UG
   GATEWAY_TYPE_RG = SYNDICATE_RG
   GATEWAY_TYPE_AG = SYNDICATE_AG
   
   CAP_READ_DATA        = SG_CAP_READ_DATA
   CAP_WRITE_DATA       = SG_CAP_WRITE_DATA
   CAP_READ_METADATA    = SG_CAP_READ_METADATA
   CAP_WRITE_METADATA   = SG_CAP_WRITE_METADATA
   CAP_COORDINATE       = SG_CAP_COORDINATE

   mlock_buf_type = 1
   
   # internal gateway instance
   cdef SG_gateway gateway_inst
   
   def __cinit__(self):
      pass

   def __dealloc__(self):
      global syndicate_inited 
      global syndicate_ref 
      
      SG_gateway_shutdown( &self.gateway_inst )
      md_shutdown()

      syndicate_inited = False
      syndicate_ref = None
      
   cdef md_opts opts_to_syndicate( self, opts ):
      '''
      Convert a dictionary of options into a struct md_opts.
      '''
      
      cdef md_opts syn_opts
      
      syn_opts.config_file = string_or_null( opts.get("config_file") )
      syn_opts.username = string_or_null( opts.get("username") )
      syn_opts.volume_name = string_or_null( opts.get("volume_name") )
      syn_opts.gateway_name = string_or_null( opts.get("ms_url") )
      syn_opts.volume_pubkey_path = string_or_null( opts.get("volume_pubkey_path") )
      syn_opts.gateway_pkey_path = string_or_null( opts.get("gateway_pkey_path") )
      syn_opts.syndicate_pubkey_path = string_or_null( opts.get("syndicate_pubkey_path") )
      syn_opts.hostname = string_or_null( opts.get("hostname") )
      syn_opts.storage_root = string_or_null( opts.get("storage_root") )
      syn_opts.first_nonopt_arg = string_or_null( opts.get("first_nonopt_arg") )
      
      syn_opts.password.ptr = string_or_null( opts.get("password") )
      syn_opts.password.len = strlen_or_zero( opts.get("password") )

      syn_opts.user_pkey_pem.ptr = string_or_null( opts.get("user_pkey_pem") )
      syn_opts.user_pkey_pem.len = strlen_or_zero( opts.get("user_pkey_pem") )
      
      syn_opts.gateway_pkey_pem.ptr = string_or_null( opts.get("gateway_pkey_pem") )
      syn_opts.gateway_pkey_pem.len = strlen_or_zero( opts.get("gateway_pkey_pem") )

      syn_opts.gateway_pkey_decryption_password.ptr = string_or_null( opts.get("gateway_pkey_decryption_password") )
      syn_opts.gateway_pkey_decryption_password.len = strlen_or_zero( opts.get("gateway_pkey_decryption_password") )

      syn_opts.volume_pubkey_pem = string_or_null( opts.get("volume_pubkey_pem") )
      syn_opts.syndicate_pubkey_pem = string_or_null( opts.get("syndicate_pubkey_pem") )

      syn_opts.tls_pkey_path = string_or_null( opts.get("tls_pkey_path") )
      syn_opts.tls_cert_path = string_or_null( opts.get("tls_cert_path") )

      syn_opts.read_stdin = bool_or_false( opts.get("read_stdin") )
      syn_opts.debug_level = int_or_zero( opts.get("debug_level") )
      syn_opts.cache_hard_limit = int_or_zero( opts.get("cache_hard_limit") )
      syn_opts.cache_soft_limit = int_or_zero( opts.get("cache_soft_limit") )
      syn_opts.foreground = bool_or_false( opts.get("foreground") )
      syn_opts.gateway_type = int_or_zero( opts.get("gateway_type") )
      syn_opts.client = bool_or_false( opts.get("client") )

      return syn_opts
      
      
   def __init__( self, gateway_type=None, anonymous_client=None, args=None, opts=None ):
      '''
      Initialize a Syndicate gateway.  Pass either a dict of options, or the gateway type, 
      whether or not it is an anonymous client, and a list of command-line options.
      
      NOTE: this should only be called once--subsequent calls will raise a SyndicateException.
      Clients should use either new_from_args() or new_from_opts()
      '''

      global syndicate_inited
      global syndicate_ref
      
      if syndicate_inited:
         raise SyndicateException("Syndicate already initialized.  Use Syndicate.new_from_args() or Syndicate.new_from_opts() instead.")

      rc = 0
      method = None
      cdef md_opts syndicate_opts
      cdef char** c_args = NULL
      
      if opts is not None:

         # new from opts
         method = "SG_gateway_init_opts"
         syndicate_opts = self.opts_to_syndicate( opts )
         
         rc = SG_gateway_init_opts( &self.gateway_inst, &syndicate_opts )

      else:
         
         # new from argv 
         if gateway_type is None or anonymous_client is None or args is None:
            raise SyndicateException("Missing gateway_type, anonymous_client, and/or args")
         
         c_args = <char**>stdlib.malloc( (len(args) + 1) * sizeof(char*) )
         if c_args == NULL:
            raise MemoryError()
         
         for i in xrange(0,len(args)):
            c_args[i] = PyString_AsString( args[i] )
         
         method = "SG_gateway_init"
         rc = SG_gateway_init( &self.gateway_inst, gateway_type, anonymous_client, len(args), c_args )

         stdlib.free( c_args )

      if rc != 0:
         raise SyndicateException( "%s rc = %d" % (method, rc) )

      syndicate_ref = self
      syndicate_inited = True 
      

   @classmethod
   def new_from_opts( cls, gateway_opts ):
      '''
      Initialize a Syndicate gateway from an md_opts structure.
      Return a reference to the Syndicate gateway instance on success.
      Raise a SyndicateException on error.
      '''

      global syndicate_ref 
      global syndicate_inited 

      if syndicate_inited:
         return syndicate_ref 
      
      return Syndicate( opts=gateway_opts )
      

   @classmethod 
   def new_from_args( cls, gateway_type, anonymous_client, args ):
      '''
      Initialize a Syndicate gateway from argv.
      Return a reference to the Syndicate gateway instance on success.
      Return a SyndicatException on error.
      '''
      
      global syndicate_ref 
      global syndicate_inited 

      if syndicate_inited:
         return syndicate_ref 

      return Syndicate( gateway_type=gateway_type, anonymous_client=anonymous_client, args=args )


   def gateway_id( self ):
      '''
         Get the gateway ID
      '''
      return SG_gateway_id( &self.gateway_inst )
   
   
   def owner_id( self ):
      '''
         Get the user ID
      '''
      return SG_gateway_user_id( &self.gateway_inst )
   
   
   def portnum( self ):
      '''
         Get the portnum this gateway should listen on.
      '''
      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      return ms_client_get_portnum( ms )


   def hostname( self ):
      '''
         Get the hostname the cert says we're supposed to listen on.
      '''
      cdef char* hostname = NULL;
      cdef md_syndicate_conf* conf = SG_gateway_conf( &self.gateway_inst )
      
      hostname = md_get_hostname( conf )
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
      
      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      
      rc = ms_client_sign_gateway_message( ms, c_data, c_data_len, &sigb64, &sigb64_len )
      
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

      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      
      rc = ms_client_verify_gateway_message( ms, volume_id, gateway_id, c_message_bits, c_message_len, c_sigb64, c_sigb64_len )
      
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

      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      
      rc = ms_client_get_closure_text( ms, &c_closure_text, &c_closure_len )
      
      if rc == 0:
         py_closure_text = c_closure_text[:c_closure_len]
         stdlib.free( c_closure_text )
         
         return py_closure_text
      
      elif rc == -errno.ENOTCONN:
         # something's seriously wrong
         raise SyndicateException( "No certificate for this gateway on file!" )
      
      else:
         return None
   
   
   cpdef get_gateway_private_key_pem( self ):
      '''
         Get the gateway private key (PEM-encoded).
      '''

      cdef char* c_privkey_pem = NULL
      cdef size_t c_privkey_len = 0

      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )

      rc = ms_client_gateway_key_pem( ms, &c_privkey_pem, &c_privkey_len );
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
      
      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      return ms_client_get_gateway_type( ms, gw_id )


   cpdef check_gateway_caps( self, gw_id, caps ):
      '''
         Check a gateway's capabilities.
         Return 0 if the capabilities (caps, a bit field) match those in the cert.
      '''

      cdef ms_client* ms = SG_gateway_ms( &self.gateway_inst )
      return ms_client_check_gateway_caps( ms, gw_id, caps )
