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
   
   if syndicate_view_change_callback != None:
      return syndicate_view_change_callback()
   
   return 0

# ------------------------------------------
cpdef encrypt_data( pubkey_str, data_str ):

   cdef char* c_data_str = data_str
   cdef size_t c_data_str_len = len(data_str)
   
   cdef char* c_encrypted_data = NULL
   cdef size_t c_encrypted_data_len = 0
   
   rc = md_encrypt_pem( pubkey_str, c_data_str, c_data_str_len, &c_encrypted_data, &c_encrypted_data_len )
   if rc != 0:
      return (rc, None)
   
   else:
      py_encrypted_data = c_encrypted_data[:c_encrypted_data_len]
      stdlib.free( c_encrypted_data )
      
      return (0, py_encrypted_data)


# ------------------------------------------
cpdef decrypt_data( privkey_str, encrypted_data_str ):
   
   cdef char* c_encrypted_data_str = encrypted_data_str
   cdef size_t c_encrypted_data_str_len = len(encrypted_data_str)
   
   cdef char* c_data_str = NULL
   cdef size_t c_data_str_len = 0
   
   rc = md_decrypt_pem( privkey_str, c_encrypted_data_str, c_encrypted_data_str_len, &c_data_str, &c_data_str_len )
   if rc != 0:
      return (rc, None)
   
   else:
      py_data_str = c_data_str[:c_data_str_len]
      stdlib.free( c_data_str )

      return (0, py_data_str)
      

# ------------------------------------------
cpdef encrypt_closure_secrets( gateway_pubkey_str, closure_secrets ):
   '''
      Encrypt a string with a gateway's public key.
   '''
   
   try:
      closure_secrets_serialized = pickle.dumps( closure_secrets )
   except Exception, e:
      return (-errno.EINVAL, None)

   return encrypt_data( gateway_pubkey_str, closure_secrets_serialized )


# ------------------------------------------
cpdef decrypt_closure_secrets( gateway_privkey_str, closure_secrets ):
   '''
      Decrypt a string with a gateway's public key.
   '''
   
   rc, py_serialized_secrets = decrypt_data( gateway_privkey_str, closure_secrets )

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
                       oid_username=None,
                       oid_password=None,
                       volume_name=None,
                       volume_key_filename=None,
                       conf_filename=None,
                       my_key_filename=None,
                       tls_pkey_filename=None,
                       tls_cert_filename=None,
                       storage_root=None ):

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
         char *c_oid_username = NULL
         char *c_oid_password = NULL
         char *c_volume_name = NULL
         char *c_volume_key_filename = NULL
         char *c_conf_filename = NULL
         char *c_my_key_filename = NULL
         char *c_tls_pkey_filename = NULL
         char *c_tls_cert_filename = NULL
         char* c_storage_root = NULL

      if gateway_name != None:
         c_gateway_name = gateway_name
      
      if ms_url != None:
         c_ms_url = ms_url

      if oid_username != None:
         c_oid_username = oid_username 
         
      if oid_password != None:
         c_oid_password = oid_password
         
      if volume_name != None:
         c_volume_name = volume_name 
         
      if conf_filename != None:
         c_conf_filename = conf_filename 

      if volume_key_filename != None:
         c_volume_key_filename = volume_key_filename
      
      if my_key_filename != None:
         c_my_key_filename = my_key_filename
         runtime_privkey_path = my_key_filename
         
      if tls_pkey_filename != None:
         c_tls_pkey_filename = tls_pkey_filename
      
      if tls_cert_filename != None:
         c_tls_cert_filename = tls_cert_filename

      if storage_root != None:
         c_storage_root = storage_root

      rc = md_init(  gateway_type,
                     c_gateway_name,
                     &self.conf_inst,
                     &self.client_inst,
                     portnum,
                     c_ms_url,
                     c_volume_name,
                     c_gateway_name,
                     c_oid_username,
                     c_oid_password,
                     c_volume_key_filename,
                     c_my_key_filename,
                     c_tls_pkey_filename,
                     c_tls_cert_filename,
                     c_storage_root )
      
      if rc != 0:
         raise Exception( "md_init rc = %d" % rc )
      
      syndicate_inited = True
      
      
   @classmethod
   def getinstance( self,  gateway_type=0,
                           gateway_name=None,
                           portnum=0,
                           ms_url=None,
                           oid_username=None,
                           oid_password=None,
                           volume_name=None,
                           volume_key_filename=None,
                           conf_filename=None,
                           my_key_filename=None,
                           tls_pkey_filename=None,
                           tls_cert_filename=None,
                           storage_root=None):
      
      '''
         Get the current Syndicate instance,
         instantiating it if needed.
         Call this method instead of the constructor.
      '''
      
      global syndicate_ref 
      
      if syndicate_ref == None:
         syndicate_ref = Syndicate( gateway_type=gateway_type,
                                    gateway_name=gateway_name,
                                    portnum=portnum,
                                    ms_url=ms_url,
                                    oid_username=oid_username,
                                    oid_password=oid_password,
                                    volume_name=volume_name,
                                    volume_key_filename=volume_key_filename,
                                    conf_filename=conf_filename,
                                    my_key_filename=my_key_filename,
                                    tls_pkey_filename=tls_pkey_filename,
                                    tls_cert_filename=tls_cert_filename,
                                    storage_root=storage_root)
         
      return syndicate_ref
   
   
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
   
   
   cpdef sign_message( self, data ):
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
   
