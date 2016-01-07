#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

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

# DEPRECATED

import protobufs.ms_pb2 as ms_pb2

from storage import storage
import storage.storagetypes as storagetypes

from common.msconfig import *
from common.admin_info import *
import common.api as api

from MS.user import SyndicateUser 

import logging
import urllib
import base64

'''
from openid.gaeopenid import GAEOpenIDRequestHandler

# ----------------------------------
def register_make_openid_reply( oid_request, return_method, return_to, query ):
   """
   Generate a serialized ms_openid_provider_reply protobuf.  This structure 
   will contain everything the client needs to know to authenticate to the MS's
   OpenID provider.  This includes (by argument) the way to redirect the client,
   the URL to return to on authentication, and the query string.
   
   Other information is supplied via the OPENID_* fields in MS.common.msconfig.
   """
   
   # reply with the redirect URL
   trust_root = OPENID_HOST_URL
   immediate = GAEOpenIDRequestHandler.IMMEDIATE_MODE in query

   redirect_url = oid_request.redirectURL( trust_root, return_to, immediate=immediate )

   openid_reply = ms_pb2.ms_openid_provider_reply()
   openid_reply.redirect_url = redirect_url
   openid_reply.auth_handler = OPENID_PROVIDER_AUTH_HANDLER
   openid_reply.username_field = OPENID_PROVIDER_USERNAME_FIELD
   openid_reply.password_field = OPENID_PROVIDER_PASSWORD_FIELD
   openid_reply.extra_args = urllib.urlencode( OPENID_PROVIDER_EXTRA_ARGS )
   openid_reply.challenge_method = OPENID_PROVIDER_CHALLENGE_METHOD
   openid_reply.response_method = OPENID_PROVIDER_RESPONSE_METHOD
   openid_reply.redirect_method = return_method
   openid_reply.signature = ""
   
   data = openid_reply.SerializeToString()
   
   #signature = storagetypes.Object.auth_sign( SYNDICATE_PRIVKEY, data );
   signature = api.sign_data( SYNDICATE_PRIVKEY, data )
   
   openid_reply.signature = base64.b64encode( signature )
   
   data = openid_reply.SerializeToString()

   return data


# ----------------------------------
def register_load_gateway( gateway_name ):
   """
   Load up a Gateway from the datastore.
   Return the status to be sent back, and the gateway
   """
   
   gateway = storage.read_gateway( gateway_name )
   if gateway == None:
      logging.error("No such Gateway named %s" % (gateway_name))
      return (404, None)
   
   return (200, gateway)


# ----------------------------------
def register_load_objects( gateway_name, username ):
   """
   Load up a gateway and user object from the datastore, given the type of gateway,
   the name of the gateway, and the username.  Return an HTTP status code, as well,
   to be sent back to the caller in case of error.
   """

   # get the gateway
   status, gateway = register_load_gateway( gateway_name )
   if status != 200:
      return (status, None, None)
   
   user = storage.read_user( username )
   if user == None:
      logging.error("No such user '%s'" % username)
      return (401, None, None)

   return (200, gateway, user)

# ----------------------------------
def protobuf_volume( volume_metadata, volume, root=None ):
   """
   Given an ms_volume_metadata protobuf and a volume, populate the protobuf structure.
   """
   
   if root is not None:
      root.protobuf( volume_metadata.root )
      
   volume.protobuf( volume_metadata )

   return
   
   
# ----------------------------------
def register_complete( gateway ):
   """
   Generate the mount information for a Volume: give back the volume cert, the root inode, and extra flow-control info
   Only call this method once the given gateway has authenticated!
   Generate and return a serialized ms_registration_metadata protobuf.
   """
   
   registration_metadata = ms_pb2.ms_registration_metadata()

   # send back gateway cert and driver
   gateway.protobuf_cert( registration_metadata.cert )
   
   # find all Volumes
   volume = storage.read_volume( gateway.volume_id )
   
   if volume == None:
      logging.error("No such volume %s" % gateway.volume_id )
      return (404, None)
   
   root = storage.get_volume_root( volume )
   
   if root is None:
      logging.error("BUG: no root for volume %s" % volume.name)
      return (500, None)

   # add volume and contents
   protobuf_volume( registration_metadata.volume, volume, root )
   
   # add flow control data
   registration_metadata.resolve_page_size = RESOLVE_MAX_PAGE_SIZE
   registration_metadata.max_connections = MAX_NUM_CONNECTIONS
   registration_metadata.max_batch_request_size = MAX_BATCH_REQUEST_SIZE
   registration_metadata.max_batch_async_request_size = MAX_BATCH_ASYNC_REQUEST_SIZE
   registration_metadata.max_transfer_time = MAX_TRANSFER_TIME
   
   # sign and serialize!
   registration_metadata.signature = ""
   
   data = registration_metadata.SerializeToString()
   
   registration_metadata.signature = volume.sign_message( data )
   
   data = registration_metadata.SerializeToString()
   
   return (200, data)
'''
   