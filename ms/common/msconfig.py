#!/usr/bin/pyhon

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import os

try:
   import syndicate.client.protobufs.ms_pb2 as ms_pb2
except:
   import protobufs.ms_pb2 as ms_pb2

import admin_info

# configuration parameters

# debug
OPENID_DEBUG = True

# MS
MS_HOSTNAME = ""
MS_PROTO = ""
MS_URL = ""

# OpenID
OPENID_SESSION_SSL_ONLY=True
OPENID_LOCAL_TEST=True
OPENID_TRUST_ROOT_HOST = ""
OPENID_HOST_URL = ""

if os.environ.get('SERVER_SOFTWARE','').startswith('Development'):
   MS_HOSTNAME = "localhost:8080"
   MS_PROTO = "http://"
   
   OPENID_TRUST_ROOT_HOST = MS_HOSTNAME
   OPENID_HOST_URL = "http://" + OPENID_TRUST_ROOT_HOST
   OPENID_SESSION_SSL_ONLY=False
   OPENID_LOCAL_TEST=True
   
else:
   MS_HOSTNAME = "syndicate-metadata.appspot.com"
   MS_PROTO = "https://"
   
   OPENID_TRUST_ROOT_HOST = MS_HOSTNAME
   OPENID_HOST_URL = "https://" + OPENID_TRUST_ROOT_HOST
   OPENID_SESSION_SSL_ONLY=True
   OPENID_LOCAL_TEST=False

if not OPENID_DEBUG:
   OPENID_PROVIDER_NAME = "VICCI"
   OPENID_PROVIDER_URL = "https://www.vicci.org/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "https://www.vicci.org/id-allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"

else:
   OPENID_PROVIDER_NAME = "localhost"
   OPENID_PROVIDER_URL = "http://localhost:8081/id/"
   OPENID_PROVIDER_AUTH_HANDLER = "http://localhost:8081/allow"
   OPENID_PROVIDER_EXTRA_ARGS = {"yes": "yes"}
   OPENID_PROVIDER_USERNAME_FIELD = "login_as"
   OPENID_PROVIDER_PASSWORD_FIELD = "password"
   OPENID_PROVIDER_CHALLENGE_METHOD = "POST"
   OPENID_PROVIDER_RESPONSE_METHOD = "POST"


MS_URL = MS_PROTO + MS_HOSTNAME

# security
OBJECT_KEY_SIZE = 4096

# gateways
GATEWAY_PASSWORD_LENGTH = 256
GATEWAY_SALT_LENGTH = 256
GATEWAY_SESSION_PASSWORD_LENGTH = 16
GATEWAY_RSA_KEYSIZE = OBJECT_KEY_SIZE

# volumes
VOLUME_SECRET_LENGTH = 256
VOLUME_SECRET_SALT_LENGTH = 256

VOLUME_RSA_KEYSIZE = OBJECT_KEY_SIZE

# users
USER_RSA_KEYSIZE = OBJECT_KEY_SIZE

USER_VOLUME_OWN = 1
USER_VOLUME_READONLY = 2
USER_VOLUME_READWRITE = 4
USER_VOLUME_HOST = 8

# website
MS_HOST = OPENID_TRUST_ROOT_HOST
MS_URL = OPENID_HOST_URL

# Gateway static constants
GATEWAY_TYPE_UG = ms_pb2.ms_gateway_cert.USER_GATEWAY
GATEWAY_TYPE_AG = ms_pb2.ms_gateway_cert.ACQUISITION_GATEWAY
GATEWAY_TYPE_RG = ms_pb2.ms_gateway_cert.REPLICA_GATEWAY

GATEWAY_CAP_READ_DATA = ms_pb2.ms_gateway_cert.CAP_READ_DATA
GATEWAY_CAP_WRITE_DATA = ms_pb2.ms_gateway_cert.CAP_WRITE_DATA
GATEWAY_CAP_READ_METADATA = ms_pb2.ms_gateway_cert.CAP_READ_METADATA
GATEWAY_CAP_WRITE_METADATA = ms_pb2.ms_gateway_cert.CAP_WRITE_METADATA
GATEWAY_CAP_COORDINATE = ms_pb2.ms_gateway_cert.CAP_COORDINATE

# import admin stuff
ADMIN_PUBLIC_KEY = admin_info.ADMIN_PUBLIC_KEY
ADMIN_EMAIL = admin_info.ADMIN_EMAIL 
ADMIN_OPENID_URL = admin_info.ADMIN_OPENID_URL 

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
