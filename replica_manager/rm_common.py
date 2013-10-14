#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import sys
import collections
import errno
import protobufs.serialization_pb2 as serialization_proto
import protobufs.ms_pb2 as ms_proto

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
   
   from syndicate import Syndicate
   
   if libsyndicate == None:
      libsyndicate = Syndicate(  gateway_type=Syndicate.GATEWAY_TYPE_RG,
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
def syndicate_shutdown():
   global libsyndicate
   del libsyndicate
   libsyndicate=None

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


from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO

#-------------------------
class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = StringIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

        try:
            self.content_len = int(self.headers.getheader('content-length'))
            self.post_body = self.rfile.read(self.content_len)
        except:
            self.post_body = None

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message

#-------------------------
class UsageException(Exception):

    '''
        For printing custom Usage messages for using the RGs
    '''

    #-------------------------
    def __init__(self, code):
        self.code = code

        if(self.code == 100):
            self.message = '[USAGE] {prog} [-w -r -d <file_name>] [--get_key] [--run_server <port>]'.format(prog=sys.argv[0])
        elif(self.code == 200):
            self.message = ""

    #-------------------------
    def __str__(self):
        return repr(self.message)

#-------------------------
def print_exception(log):

    '''
        For printing a better error message than just the default traceback 
    '''

    import traceback 

    #Get exception_type, exception_object, and exception_traceback
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    
    print '-' * 25
    traceback.print_tb(exc_tb)
    log.debug('----- ERROR -----')
    log.debug('File Name: %s', fname) 
    log.debug('Line Number: %s', exc_tb.tb_lineno)  
    log.debug('Exception Type: %s', exc_type)
    log.debug('Message: %s', exc_obj.message)
    log.debug('-' * 12)
