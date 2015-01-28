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

import os

try:
   import syndicate.protobufs.ms_pb2 as ms_pb2
except:
   import protobufs.ms_pb2 as ms_pb2

try:
   from admin_info import OPENID_LOCAL
except:
   # imported by a client program, not the MS, since admin_info isn't available
   OPENID_LOCAL = False

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

# OpenID
OPENID_SESSION_SSL_ONLY = True
OPENID_TRUST_ROOT_HOST = ""
OPENID_HOST_URL = ""

OPENID_POST_USERNAME = "openid_username"

if os.environ.get('SERVER_SOFTWARE','').startswith('Development'):
   
   # running locally in a development server
   MS_HOSTNAME = "localhost:%s" % str(os.environ.get("SERVER_PORT", 8080))
   MS_PROTO = "http://"
   
   OPENID_TRUST_ROOT_HOST = MS_HOSTNAME
   OPENID_HOST_URL = "http://" + OPENID_TRUST_ROOT_HOST
   OPENID_SESSION_SSL_ONLY = False
   
else:
   # running publicly.
   try:
      from google.appengine.api import app_identity
      MS_HOSTNAME = app_identity.get_default_version_hostname()
   except:
      # probably running with syntool, which doesn't use this 
      MS_HOSTNAME = "(none)"
      
   MS_PROTO = "https://"
   
   OPENID_TRUST_ROOT_HOST = MS_HOSTNAME
   OPENID_HOST_URL = "https://" + OPENID_TRUST_ROOT_HOST
   OPENID_SESSION_SSL_ONLY=True
   OPENID_LOCAL = False

if OPENID_LOCAL:
   # OpenID debug on local server
   OPENID_PROVIDER_NAME = "localhost"
   OPENID_PROVIDER_URL = "http://localhost:8081/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "http://localhost:8081/allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"
else:
   # use existing OpenID provider
   OPENID_PROVIDER_NAME = "VICCI"
   OPENID_PROVIDER_URL = "https://www.vicci.org/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "https://www.vicci.org/id-allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"

MS_URL = MS_PROTO + MS_HOSTNAME

log.info("OpenID provider: %s" % OPENID_PROVIDER_URL )

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

# website
MS_HOST = OPENID_TRUST_ROOT_HOST

# Gateway static constants
GATEWAY_TYPE_UG = ms_pb2.ms_gateway_cert.USER_GATEWAY
GATEWAY_TYPE_AG = ms_pb2.ms_gateway_cert.ACQUISITION_GATEWAY
GATEWAY_TYPE_RG = ms_pb2.ms_gateway_cert.REPLICA_GATEWAY

GATEWAY_CAP_READ_DATA = ms_pb2.ms_gateway_cert.CAP_READ_DATA
GATEWAY_CAP_WRITE_DATA = ms_pb2.ms_gateway_cert.CAP_WRITE_DATA
GATEWAY_CAP_READ_METADATA = ms_pb2.ms_gateway_cert.CAP_READ_METADATA
GATEWAY_CAP_WRITE_METADATA = ms_pb2.ms_gateway_cert.CAP_WRITE_METADATA
GATEWAY_CAP_COORDINATE = ms_pb2.ms_gateway_cert.CAP_COORDINATE

GATEWAY_ID_ANON = 0xFFFFFFFFFFFFFFFF    # taken from libsyndicate.h

# JSON
JSON_AUTH_COOKIE_NAME = "SynAuth"
JSON_MS_API_VERSION = "1.0"
JSON_SYNDICATE_CALLING_CONVENTION_FLAG = "__syndicate_json_rpc_calling_convention"

# alternative names
GATEWAY_TYPE_TO_STR = {
   GATEWAY_TYPE_UG: "UG",
   GATEWAY_TYPE_RG: "RG",
   GATEWAY_TYPE_AG: "AG"
}

# key constants
KEY_UNSET = "unset"
KEY_UNUSED = "unused"

# verification methods
AUTH_METHOD_PUBKEY = "VERIFY_PUBKEY"
AUTH_METHOD_PASSWORD = "VERIFY_PASSWORD"
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

# RESOLVE_MAX_PAGE_SIZE = 3       # for testing

