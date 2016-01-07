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

from storage import storage
import storage.storagetypes as storagetypes

from common.msconfig import *
from common.admin_info import *

import logging
import base64

from MS.volume import Volume, VolumeCertBundle
from MS.gateway import Gateway

# ----------------------------------
def response_read_gateway_basic_auth( headers ):
   """
   Given a dict of HTTP headers, extract the gateway's type, id, and base64 signature over both the URL and Authorization header
   """
   
   basic_auth = headers.get("Authorization")
   if basic_auth == None:
      logging.info("no authorization header")
      return (None, None, None)

   # basic auth format:
   # ${gateway_type}_${gateway_id}:${signatureb64}
   # example:
   # UG_3:01234567890abcdef

   gateway_type, gateway_id, signatureb64 = '', '', ''
   try:
      user_info = base64.decodestring( basic_auth[6:] )
      gateway, signatureb64 = user_info.split(":")
      gateway_type, gateway_id = gateway.split("_")
      gateway_type = int(gateway_type)
      gateway_id = int(gateway_id)
   except:
      logging.info("incomprehensible Authorization header: '%s'" % basic_auth )
      return (None, None, None)

   return gateway_type, gateway_id, signatureb64


# ----------------------------------
def response_load_volume_and_gateway( request_handler, volume_id, gateway_id=None ):
   """
   Load a volume and the gateway from the request handler.
   
   Return (volume, gateway, volume_cert_bundle, status, time)
   """
   
   read_start = storagetypes.get_time()
   
   # get the gateway's ID and credentials
   g_id = None 
   
   if gateway_id is None:
      gateway_type, g_id, signature_b64 = response_read_gateway_basic_auth( request_handler.request.headers )
   else:
      g_id = gateway_id
   
   volume = None 
   gateway = None
   cert_bundle = None 
   
   volume_fut = Volume.Read( volume_id, async=True )
   cert_bundle_fut = VolumeCertBundle.Get( volume_id, async=True )
   gateway_fut = None 
   
   if g_id is not None:
      gateway_fut = Gateway.Read( g_id, async=True )
      
   storagetypes.wait_futures( [volume_fut, gateway_fut, cert_bundle_fut] )
   
   volume = volume_fut.get_result()
   cert_bundle = cert_bundle_fut.get_result()
   if gateway_fut is not None:
       gateway = gateway_fut.get_result()
   
   if volume is None or cert_bundle is None:
      logging.error("No volume, gateway, or cert bundle")
      response_user_error( request_handler, 404 )
      return (None, None, None, 404, None)
   
   Volume.SetCache( volume.volume_id, volume )
   VolumeCertBundle.SetCache( volume.volume_id, cert_bundle )
   
   if gateway is not None:
      Gateway.SetCache( gateway.g_id, gateway )
      
   # sanity checks
   if (volume.need_gateway_auth()) and (gateway is None or gateway_type is None or signature_b64 is None):
      # required authentication, but we don't have an Authentication header
      logging.error("Unable to authenticate gateway")
      return (None, None, None, 403, None)

   # need auth?
   if volume.need_gateway_auth() and gateway is None:
      logging.error("Unable to authenticate gateway")
      return (None, None, None, 403, None)
   
   # gateway validity
   if gateway is not None:
      
      # type match?
      if gateway_type is not None and gateway.gateway_type != gateway_type:
         logging.error("Type mismatch on %s:%s" % (gateway_type, g_id))
         response_user_error( request_handler, 403 )
         return (None, None, None, 403, None )
      
      # is the gateway in this volume?
      if not volume.is_gateway_in_volume( gateway ):
         logging.error("Gateway '%s' is not in volume '%s'" % (gateway.name, volume.name))
         response_user_error( request_handler, 403 )
         return (None, None, None, 403, None)
      
      # make sure this gateway's cert is registered
      valid_gateway = gateway.authenticate_session( gateway_type, g_id, request_handler.request.url, signature_b64 )

      if not valid_gateway and volume.need_gateway_auth():
         # invalid credentials
         logging.error("Invalid authentication credentials")
         response_user_error( request_handler, 403 )
         return (None, None, None, 403, None)
   
   read_time = storagetypes.get_time() - read_start
   
   return (volume, gateway, cert_bundle, 200, read_time)

# ----------------------------------
def response_user_error( request_handler, status, message=None ):
   """
   Reply with a user-triggerred error (400-404, 501), with an 
   optional message.  Translate the error code into a human-readable
   message as well.
   """
   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"

   if status == 400:
      if message == None:
         messsage = "Invalid Request\n"
      request_handler.response.write(message)
      
   elif status == 404:
      if message == None:
         messsage = "No such object\n"
      request_handler.response.write(message)
      
   elif status == 401:
      if message == None:
         message = "Authentication required\n"
      request_handler.response.write(message)

   elif status == 403:
      if message == None:
         message = "Authorization Failed\n"
      request_handler.response.write(message)

   elif status == 501:
      if message == None:
         message = "Method not supported\n"
      
      request_handler.response.write(message)
   return


# ----------------------------------
def response_begin( request_handler, volume_id ):
   """
   Begin a response to a calling gateway, given the request handler and either the volume name or ID.
   Load up the calling gateway and the volume it's trying to access, and return both along with 
   some benchmark information.
   
   Return (volume, gateway, status, benchmark dict) on success
   Return Nones on failure.
   """
   
   timing = {}
   
   volume, gateway, volume_cert_bundle, status, read_time = response_load_volume_and_gateway( request_handler, volume_id )
   
   # transfer over cert bundle to volume
   if volume is not None and volume_cert_bundle is not None:
      cert_bundle = VolumeCertBundle.Load( volume_cert_bundle )
      volume.cert_bundle = cert_bundle
   
   timing['request_start'] = read_time 
   
   return (volume, gateway, status, timing)
   
   
# ----------------------------------
def response_end( request_handler, status, data, content_type=None, timing=None ):
   """
   Finish a response to a calling gateway, optionally including some benchmarking
   information in the HTTP headers.  Reply with the given status and the raw data string 
   (response MIME type is application/octet-stream unless otherwise specified).
   """
   
   if content_type == None:
      content_type = "application/octet-stream"

   if timing != None:
      request_total = storagetypes.get_time() - timing['request_start']
      timing['X-Total-Time'] = str(request_total)
      
      del timing['request_start']
      
      for (time_header, time) in timing.items():
         request_handler.response.headers[time_header] = time

   request_handler.response.headers['Content-Type'] = content_type
   request_handler.response.status = status
   request_handler.response.write( data )
   return
   
