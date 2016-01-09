#!/usr/bin/pyhon

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

# NOTE: shared between the MS and the Syndicate python package

import os

try:
   import syndicate.protobufs.ms_pb2 as ms_pb2
except:
   import protobufs.ms_pb2 as ms_pb2

'''
try:
   from admin_info import OPENID_LOCAL
except:
   # imported by a client program, not the MS, since admin_info isn't available
   OPENID_LOCAL = False
'''

try:
   import syndicate.client.common.log as Log
   log = Log.get_logger()
except:
   import logging as log

# configuration parameters

# MS
MS_HOSTNAME = ""
MS_PROTO = ""
MS_URL = ""

if os.environ.get('SERVER_SOFTWARE','').startswith('Development'):
   
   # running locally in a development server
   MS_HOSTNAME = "localhost:%s" % str(os.environ.get("SERVER_PORT", 8080))
   MS_PROTO = "http://"
   
else:
   # running publicly.
   try:
      from google.appengine.api import app_identity
      MS_HOSTNAME = app_identity.get_default_version_hostname()
   except:
      # probably running with syntool, which doesn't use this 
      MS_HOSTNAME = "(none)"
      
   MS_PROTO = "https://"
   
MS_URL = MS_PROTO + MS_HOSTNAME

# security
OBJECT_KEY_SIZE = 4096

# gateways
GATEWAY_SESSION_SALT_LENGTH = 256
GATEWAY_SESSION_PASSWORD_LENGTH = 16
GATEWAY_RSA_KEYSIZE = OBJECT_KEY_SIZE

# volumes
VOLUME_RSA_KEYSIZE = OBJECT_KEY_SIZE

# users
USER_RSA_KEYSIZE = OBJECT_KEY_SIZE

USER_VOLUME_OWN = 1
USER_VOLUME_READONLY = 2
USER_VOLUME_READWRITE = 4
USER_VOLUME_HOST = 8

# caps
GATEWAY_CAP_READ_DATA = ms_pb2.ms_gateway_cert.CAP_READ_DATA
GATEWAY_CAP_WRITE_DATA = ms_pb2.ms_gateway_cert.CAP_WRITE_DATA
GATEWAY_CAP_READ_METADATA = ms_pb2.ms_gateway_cert.CAP_READ_METADATA
GATEWAY_CAP_WRITE_METADATA = ms_pb2.ms_gateway_cert.CAP_WRITE_METADATA
GATEWAY_CAP_COORDINATE = ms_pb2.ms_gateway_cert.CAP_COORDINATE

GATEWAY_ID_ANON = 0xFFFFFFFFFFFFFFFF    # taken from libsyndicate.h

# JSON
JSON_AUTH_COOKIE_NAME = "SynAuth"
JSON_SYNDICATE_CALLING_CONVENTION_FLAG = "__syndicate_json_rpc_calling_convention"

# key constants
KEY_UNSET = "unset"
KEY_UNUSED = "unused"

# verification methods
AUTH_METHOD_PUBKEY = "VERIFY_PUBKEY"
# AUTH_METHOD_PASSWORD = "VERIFY_PASSWORD"
AUTH_METHOD_NONE = "VERIFY_NONE"

# activation properties
PASSWORD_HASH_ITERS = 10000
PASSWORD_SALT_LENGTH = 32

# rate-limiting 
RESOLVE_MAX_PAGE_SIZE = 10 
MAX_NUM_CONNECTIONS = 50 
MAX_BATCH_REQUEST_SIZE = 6
MAX_BATCH_ASYNC_REQUEST_SIZE = 100
MAX_TRANSFER_TIME = 300

# ports 
GATEWAY_DEFAULT_PORT = 31111

# RESOLVE_MAX_PAGE_SIZE = 3       # for testing

