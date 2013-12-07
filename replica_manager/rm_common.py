#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import sys
import collections
import errno
import protobufs.serialization_pb2 as serialization_proto
import protobufs.ms_pb2 as ms_proto

from syndicate.syndicate import Syndicate

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")
LOG_PATH = os.path.join(FILE_ROOT, "log/")

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
                    portnum=None,
                    ms_url=None, 
                    volume_name=None,
                    gateway_cred=None,
                    gateway_pass=None,
                    conf_filename=None,
                    my_key_filename=None,
                    volume_key_filename=None,
                    tls_pkey_filename=None,
                    tls_cert_filename=None):
   
   '''
      Initialize libsyndicate library, but only once
   '''
   
   global libsyndicate
   
   if libsyndicate == None:
      libsyndicate = Syndicate.getinstance(  gateway_type=Syndicate.GATEWAY_TYPE_RG,
                                             gateway_name=gateway_name,
                                             volume_name=volume_name,
                                             portnum=portnum,
                                             ms_url=ms_url,
                                             gateway_cred=gateway_cred,
                                             gateway_pass=gateway_pass,
                                             conf_filename=conf_filename,
                                             my_key_filename=my_key_filename,
                                             tls_pkey_filename=tls_pkey_filename,
                                             tls_cert_filename=tls_cert_filename,
                                             volume_key_filename=volume_key_filename )
         
   else:
      raise Exception("libsyndicate already initialized!")
   
   return libsyndicate

#-------------------------
def get_logger():

    import logging

    if(DEBUG):
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)

        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        log.addHandler(handler_stream)

    else:
        log = None

    return log

#-------------------------
log = get_logger()
