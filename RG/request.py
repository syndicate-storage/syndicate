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

from syndicate.rg.common import *

import re
import base64 
import binascii
import errno
import syndicate.protobufs.serialization_pb2 as serialization_proto
import syndicate.protobufs.ms_pb2 as ms_proto

import syndicate.rg.common as rg_common 
log = rg_common.get_logger()

#-------------------------
RE_BLOCK_PATH = re.compile( "^[/]+SYNDICATE-DATA[/]+([0123456789]+)[/]+([0123456789ABCDEF]+)\.([0123456789]+)[/]+([0123456789]+)\.([0123456789]+)[/]*$" )
RE_MANIFEST_PATH = re.compile( "^[/]+SYNDICATE-DATA[/]+([0123456789]+)[/]+([0123456789ABCDEF]+)\.([0123456789]+)[/]+manifest\.([0123456789]+)\.([0123456789]+)[/]*$" )

#-------------------------
# vetted block and manifest requests
RequestInfo = collections.namedtuple( "RequestInfo", ["type",
                                                      "volume_id",
                                                      "gateway_id",
                                                      "user_id",
                                                      "file_id",
                                                      "version",
                                                      "block_id",
                                                      "block_version",
                                                      "mtime_sec",
                                                      "mtime_nsec",
                                                      "data_hash",
                                                      "size",
                                                      "kwargs"] )

# TODO: write_nonce, delete list 

RequestInfo.MANIFEST = 1
RequestInfo.BLOCK = 2


#-------------------------
def verify_protobuf( gateway_id, volume_id, pb ):
   '''
      Verify the signature of a protobuf (pb)
      The protobuf must have a signature field,
      which stores its base64-encoded signature.
   '''
   
   libsyndicate = get_libsyndicate()
   
   # get the bits without the signature
   sigb64 = pb.signature
   pb.signature = ""
   
   toverify = pb.SerializeToString()
   
   pb.signature = sigb64
   
   # verify it
   valid = False
   try:
      valid = libsyndicate.verify_gateway_message( gateway_id, volume_id, toverify, sigb64 )
   except Exception, e:
      log.exception( e )
   
   return valid
   

#-------------------------
def parse_request_info_from_pb( req_info_str ):

    '''
        Parse and verify a serialized ms_client_request_info protobuf.
        Return a RequestInfo structure with the same information.
        Raises an exception if the authenticity of the sender could not be verified.
    '''
    
    libsyndicate = get_libsyndicate()
    
    # parse it
    req_info = ms_proto.ms_gateway_request_info()
    
    try:
       req_info.ParseFromString( req_info_str )
    except:
       log.debug("Failed to parse metadata string")
       return None
    
    valid = verify_protobuf( req_info.writer, req_info.volume, req_info )
    if not valid:
       raise Exception("Could not verify request info message authenticity")
      
    # construct a RequestInfo
    req_type = 0
    if req_info.type == ms_proto.ms_gateway_request_info.MANIFEST:
       req_type = RequestInfo.MANIFEST
    elif req_info.type == ms_proto.ms_gateway_request_info.BLOCK:
       req_type = RequestInfo.BLOCK
    else:
       # invalid
       return None
    
    replica_info = RequestInfo( type=req_type,
                                volume_id=req_info.volume,
                                gateway_id=req_info.writer,
                                user_id=req_info.owner,
                                file_id=req_info.file_id,
                                version=req_info.file_version,
                                block_id=req_info.block_id,
                                block_version=req_info.block_version,
                                mtime_sec=req_info.file_mtime_sec,
                                mtime_nsec=req_info.file_mtime_nsec,
                                data_hash=binascii.b2a_hex( base64.b64decode( req_info.hash ) ),
                                size=req_info.size,
                                kwargs=dict( [(a.key, a.value) for a in req_info.args] )
                             )
 
    return replica_info


#-------------------------
def parse_request_info_from_url_path( url_path ):
   '''
      Convert a URL path (i.e. from a GET request) into a RequestInfo structure.
   '''
   
   global RE_BLOCK_PATH
   global RE_MANIFEST_PATH
   
   libsyndicate = get_libsyndicate()
   
   # block, manifest, or neither?
   manifest_match = RE_MANIFEST_PATH.match( url_path )
   block_match = None
   if manifest_match == None:
      block_match = RE_BLOCK_PATH.match( url_path )
   
   if block_match == None and manifest_match == None:
      # neither
      log.info("bad match")
      return None
   
   elif block_match != None:
      # block
      try:
         volume_id = int( block_match.groups()[0] )
         file_id = int( block_match.groups()[1], 16 )
         version = int( block_match.groups()[2] )
         block_id = int( block_match.groups()[3] )
         block_version = int( block_match.groups()[4] )
      except:
         log.info("bad block request")
         return None
      
      replica_info = RequestInfo( type=RequestInfo.BLOCK,
                                  volume_id=volume_id,
                                  gateway_id=None,
                                  user_id=None,
                                  file_id=file_id,
                                  version=version,
                                  block_id=block_id,
                                  block_version=block_version,
                                  mtime_sec=None,
                                  mtime_nsec=None,
                                  data_hash=None,
                                  size=None,
                                  kwargs={})
      
      return replica_info
   
   else:
      # manifest
      try:
         volume_id = int( manifest_match.groups()[0] )
         file_id = int( manifest_match.groups()[1], 16 )
         version = int( manifest_match.groups()[2] )
         mtime_sec = int( manifest_match.groups()[3] )
         mtime_nsec = int( manifest_match.groups()[4] )
      except:
         log.info("bad manifest request")
         return None
      
      replica_info = RequestInfo( type=RequestInfo.MANIFEST,
                                  volume_id=volume_id,
                                  gateway_id=None,
                                  user_id=None,
                                  file_id=file_id,
                                  version=version,
                                  block_id=None,
                                  block_version=None,
                                  mtime_sec=mtime_sec,
                                  mtime_nsec=mtime_nsec,
                                  data_hash=None,
                                  size=None,
                                  kwargs={})
      
      return replica_info
   
   
if __name__ == "__main__":
   
   manifest_req_info = parse_request_info_from_url_path( "/SYNDICATE-DATA/1/12345.67890/manifest.12345.67890" )
   block_req_info = parse_request_info_from_url_path( "/SYNDICATE-DATA/1/12345.67890/12.34" )
   
   print "GET"
   print "manifest_req_info = %s" % str(manifest_req_info)
   print ""
   print "block_req_info = %s" % str(block_req_info)
   print ""
   
   