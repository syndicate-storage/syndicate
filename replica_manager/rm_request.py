#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

from rm_common import *

import base64 
import binascii
import errno
import protobufs.serialization_pb2 as serialization_proto
import protobufs.ms_pb2 as ms_proto

log = get_logger()


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
                                                      "size"] )
RequestInfo.MANIFEST = 1
RequestInfo.BLOCK = 2


#-------------------------
def verify_protobuf( gateway_id, volume_id, pb ):
   '''
      Verify the signature of a protobuf (pb)
      The protobuf must have a signature field,
      which stores its base64-encoded signature.
   '''
   
   # get the bits without the signature
   sigb64 = pb.signature
   pb.signature = ""
   
   toverify = pb.SerializeToString()
   
   pb.signature = sigb64
   
   # verify it
   valid = False
   try:
      valid = libsyndicate.ms_client_verify_gateway_message( gateway_id, volume_id, toverify, sigb64 )
   except Exception, e:
      log.exception( e )
   
   return valid
   

#-------------------------
def parse_request_info( req_info_str ):

    '''
        Parse and verify a serialized ms_client_request_info protobuf.
        Return a RequestInfo structure with the same information
    '''
    
    libsyndicate = get_libsyndicate()
    log = get_logger()
    
    # parse it
    req_info = ms_proto.ms_gateway_request_info()
    
    try:
       req_info.ParseFromString( req_info_str )
    except:
       log.debug("Failed to parse metadata string")
       return None
    
    valid = verify_protobuf( req_info.writer, req_info.volume, req_info )
    
    # construct a ReplicaInfo
    req_type = 0
    if req_info.type == ms_proto.ms_gateway_request_info.MANIFEST:
       req_type = ReplicaInfo.MANIFEST
    elif req_info.type == ms_proto.ms_gateway_request_info.BLOCK:
       req_type = ReplicaInfo.BLOCK
    else:
       # invalid
       return None
    
    replica_info = ReplicaInfo( type=req_type,
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
                             )
 
    return replica_info
   