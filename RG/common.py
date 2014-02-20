#!/usr/bin/env python

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

import os
import sys
import collections
import errno
import logging

import syndicate.protobufs.serialization_pb2 as serialization_proto
import syndicate.protobufs.ms_pb2 as ms_proto

from syndicate.syndicate import Syndicate

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")
LOG_PATH = os.path.join(FILE_ROOT, "log/")

my_logger = None

# libsyndicate instance 
libsyndicate = None

#-------------------------
def get_libsyndicate():
   global libsyndicate
   return libsyndicate

#-------------------------
def syndicate_lib_path( path ):
   sys.path.append( path )
   
   
#-------------------------
def validate_fields( data, required_fields, optional_fields=None ):
   '''
      Given a dictionary (data), a dict of required fields (dict: name --> type), and 
      a dict of optional fields (dict: name ---> type ), verify that all required fields
      are present in the dict and have the apporpriate types.  If optional fields are given,
      verify that any optional fields listed are present, and also verify that no other fields 
      are given.
      
      Raise an exception if the data does not conform to the required or optional fields.
      Return otherwise.
   '''
   
   missing = []
   invalid = []
   for req_field in required_fields.keys():
      if not data.has_key( req_field ):
         missing.append( req_field )
      elif type(data[req_field]) != required_fields[req_field]:
         print "invalid type for %s: got %s, expected %s" % (req_field, type(data[req_field]), required_fields[req_field])
         invalid.append( req_field )
   
   if optional_fields != None:
      for field in data.keys():
         if field not in required_fields.keys() and field not in optional_fields.keys():
            invalid.append( field )
            
   if len(missing) != 0 or len(invalid) != 0:
      missing_txt = ",".join( missing )
      if len(missing_txt) == 0:
         missing_txt = "None"
         
      invalid_txt = ",".join( invalid )
      if len(invalid_txt) == 0:
         invalid_txt = "None"
         
      raise Exception("Missing fields: %s; Invalid fields: %s" % (missing_txt, invalid_txt) )
   
   return


#-------------------------
def syndicate_init( gateway_name=None,
                    ms_url=None, 
                    volume_name=None,
                    username=None,
                    password=None,
                    config_file=None,
                    gateway_pkey_path=None,
                    gateway_pkey_decryption_password=None,
                    volume_pubkey_path=None,
                    tls_pkey_path=None,
                    tls_cert_path=None):
   
   '''
      Initialize libsyndicate library, but only once
   '''
   
   global libsyndicate
   
   if libsyndicate == None:
      libsyndicate = Syndicate.getinstance(  gateway_type=Syndicate.GATEWAY_TYPE_RG,
                                             gateway_name=gateway_name,
                                             volume_name=volume_name,
                                             ms_url=ms_url,
                                             username=username,
                                             password=password,
                                             config_file=config_file,
                                             gateway_pkey_path=gateway_pkey_path,
                                             gateway_pkey_decryption_password=gateway_pkey_decryption_password,
                                             tls_pkey_path=tls_pkey_path,
                                             tls_cert_path=tls_cert_path,
                                             volume_pubkey_path=volume_pubkey_path )
         
   else:
      raise Exception("libsyndicate already initialized!")
   
   return libsyndicate


#-------------------------
def get_logger():
   
    global my_logger
    
    if(DEBUG and my_logger == None):
        
        my_logger = logging.getLogger()
        my_logger.setLevel(logging.DEBUG)
        my_logger.propagate = False

        formatter = logging.Formatter('[%(levelname)s] [%(module)s:%(lineno)d] %(message)s')
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        my_logger.addHandler(handler_stream)

       
    return my_logger
