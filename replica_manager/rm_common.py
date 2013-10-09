#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import sys

DEBUG = True

FILE_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(FILE_ROOT, "config/")
LOG_PATH = os.path.join(FILE_ROOT, "log/")

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

#-------------------------
def parse_block_request(data):

    '''
        function to parse incoming requests of the format
        GET /SYNDICATE-DATA/$path/$file_name.$file_version/$block_id.$block_version e.g., 
        GET /SYNDICATE-DATA/tmp/hello.1375782135401/0.8649607004776574730
    '''

    log.debug(u'Parsing request: %s', data)

    data = data.rsplit(' ')

    action = data[0]
    data = data[1].rsplit('/', 2)
    path = data[0]
    path = '/' + path + '/'
    file_info = data[1]
    block_info = data[2]

    file_info = file_info.rsplit('.')
    file_name = file_info[0]
    file_version = file_info[1]

    block_info = block_info.rsplit('.')
    block_id = block_info[0]
    block_version = block_info[1]


    log.debug(u'action: %s', action)
    log.debug(u'path: %s', path)
    log.debug(u'file_name: %s', file_name)    
    log.debug(u'file_version: %s', file_version)
    log.debug(u'block_id: %s', block_id)
    log.debug(u'block_version: %s', block_version)
    
    return 

#-------------------------
def parse_manifest_request(input):

    return 

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
