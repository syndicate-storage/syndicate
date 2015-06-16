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


# ----------------------------------
def response_read_gateway_basic_auth( headers ):
   """
   Given a dict of HTTP headers, extract the gateway's type, id, and signature over both the URL and Authorization header
   """
   
   basic_auth = headers.get("Authorization")
   if basic_auth == None:
      logging.info("no authorization header")
      return (None, None, None)

   # basic auth format:
   # ${gateway_type}_${gateway_id}:${password}
   # example:
   # UG_3:01234567890abcdef

   gateway_type, gateway_id, password = '', '', ''
   try:
      user_info = base64.decodestring( basic_auth[6:] )
      gateway, password = user_info.split(":")
      gateway_type, gateway_id = gateway.split("_")
      gateway_id = int(gateway_id)
   except:
      logging.info("incomprehensible Authorization header: '%s'" % basic_auth )
      return (None, None, None)

   return gateway_type, gateway_id, password


# ----------------------------------
def response_load_gateway_by_type_and_id( gateway_type, gateway_id ):
   """
   Given a gateway's numeric type and ID, load it from the datastore.
   """
   
   if gateway_id == None:
      # invalid header
      return (None, 403, None)

   gateway_read_start = storagetypes.get_time()
   gateway = storage.read_gateway( gateway_id )
   
   if gateway is None:
      # not present
      return (None, 404, None)
   
   if GATEWAY_TYPE_TO_STR.get( gateway.gateway_type ) == None:
      # bad type (shouldn't happen)
      return (None, 400, None)
   
   if GATEWAY_TYPE_TO_STR[ gateway.gateway_type ] != gateway_type:
      # caller has the wrong type
      return (None, 401, None)
      
   gateway_read_time = storagetypes.get_time() - gateway_read_start
   return (gateway, 200, gateway_read_time)


# ----------------------------------
def response_load_volume( request_handler, volume_name_or_id ):
   """
   Load a volume from the data store, given either its name or ID.
   Automatically reply with an error message via the given 
   request handler.
   """
   
   volume_read_start = storagetypes.get_time()

   volume = storage.read_volume( volume_name_or_id )

   volume_read_time = storagetypes.get_time() - volume_read_start

   if volume == None:
      # no volume
      response_volume_error( request_handler, 404 )
      return (None, 404, None)

   if not volume.active:
      # inactive volume
      response_volume_error( request_handler, 503 )
      return (None, 503, None)

   return (volume, 200, volume_read_time)


# ----------------------------------
def response_volume_error( request_handler, status ):
   """
   Reply with a failure to load a volume.
   Optionally translate the status (if 404 or 503)
   into a message as well.
   """
   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"
   
   if status == 404:
      # no volume
      request_handler.response.write("No such volume\n")

   elif status == 503:
      # inactive volume
      request_handler.response.write("Service Not Available\n")

   return
   

# ----------------------------------
def response_server_error( request_handler, status, msg=None ):
   """
   Reply with a server failure (500 and above), with an optional message.
   """
   request_handler.response.status = status
   request_handler.response.headers['Content-Type'] = "text/plain"

   if status == 500:
      # server error
      if msg == None:
         msg = "Internal Server Error"
      request_handler.response.write( msg )

   return
   

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
def response_load_gateway( request_handler, vol ):
   """
   Given a loaded Volume and a request handler, load the calling gateway record.
   Return the gateway and an HTTP status, as well as some benchmark information.
   """
   
   # get the gateway's credentials
   gateway_type_str, g_id, signature_b64 = response_read_gateway_basic_auth( request_handler.request.headers )

   if (gateway_type_str is None or g_id is None or signature_b64 is None) and vol.need_gateway_auth():
      response_user_error( request_handler, 401 )
      return (None, 401, None)

   # look up the requesting gateway
   gateway, status, gateway_read_time = response_load_gateway_by_type_and_id( gateway_type_str, g_id )

   if vol.need_gateway_auth():
      if status != 200:
         response_user_error( request_handler, status )
         return (None, status, None)

      # make sure this gateway is legit
      valid_gateway = gateway.authenticate_session( signature_b64 )

      if not valid_gateway and vol.need_gateway_auth():
         # invalid credentials
         logging.error("Invalid session credentials")
         response_user_error( request_handler, 403 )
         return (None, 403, None)

   else:
      status = 200
      
   return (gateway, status, gateway_read_time)
   

# ----------------------------------
def response_begin( request_handler, volume_name_or_id ):
   """
   Begin a response to a calling gateway, given the request handler and either the volume name or ID.
   Load up the calling gateway and the volume it's trying to access, and return both along with 
   some benchmark information.  Return Nones on failure.
   
   TODO: load volume and gateway in parallel
   """
   
   timing = {}
   
   timing['request_start'] = storagetypes.get_time()

   # get the Volume
   volume, status, volume_read_time = response_load_volume( request_handler, volume_name_or_id )

   if status != 200:
      return (None, None, None)

   gateway_read_time = 0
   gateway = None

   # try to authenticate the gateway
   gateway, status, gateway_read_time = response_load_gateway( request_handler, volume )

   if (status != 200 or gateway == None):
      return (None, None, None)

   # make sure this gateway is allowed to access this Volume
   if volume.need_gateway_auth():
      
      if gateway is not None:
         
         valid_gateway = volume.is_gateway_in_volume( gateway )
         if not valid_gateway:
            
            # gateway does not belong to this Volume
            logging.error("Not in this Volume")
            response_user_error( request_handler, 403 )
            return (None, None, None)
      
      else:
         logging.error("No gateway, but we required authentication")
         response_user_error( request_handler, 403 )
         return (None, None, None)

   # if we're still here, we're good to go

   timing['X-Volume-Time'] = str(volume_read_time)
   timing['X-Gateway-Time'] = str(gateway_read_time)
   
   return (gateway, volume, timing)

   
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
   
