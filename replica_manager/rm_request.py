#!/usr/bin/python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

from rm_common import *
from rm_certificates import *

import base64 
import errno
import protobufs.serialization_pb2 as serialization_proto
import protobufs.ms_pb2 as ms_proto

log = get_logger()


# vetted block and manifest requests
ReplicaInfo = collections.namedtuple( "ReplicaInfo", ["type",
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
ReplicaInfo.MANIFEST = 1
ReplicaInfo.BLOCK = 2


#-------------------------
def request_parse_replica_info( req_info_str ):

    '''
        Parse the "metadata" field of an incoming POST request.
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
    
    # get the bits without the signature
    req_info_sigb64 = req_info.signature
    req_info.signature = ""
    
    req_info_str_toverify = req_info.SerializeToString()
    
    # verify it
    valid = False
    try:
       valid = libsyndicate.ms_client_verify_gateway_message( req_info.writer, req_info.volume, req_info_str_toverify, req_info_sigb64 )
    except Exception, e:
       log.exception( e )
       
    if not valid:
       return None
    
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
                                data_hash=base64.b64decode( req_info.hash ),
                                size=req_info.size,
                             )
 
   return replica_info
   