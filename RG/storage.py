#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import syndicate.rg.common as rg_common
import types
import inspect
import collections

import syndicate.rg.closure as rg_closure
from syndicate.rg.request import *

#-------------------------
def filename_from_req_info( req_info ):
   '''
      Generate a filename from a RequestInfo structure.
   '''
   if req_info.type == RequestInfo.MANIFEST:
      return "%016d.%016X.%d.%d.man" % (req_info.volume_id, req_info.file_id, req_info.mtime_sec, req_info.mtime_nsec)
   elif req_info.type == RequestInfo.BLOCK:
      return "%016d.%016X.%d.%d.blk" % (req_info.volume_id, req_info.file_id, req_info.block_id, req_info.block_version)

   raise Exception("Invalid RequestInfo structure '%s'" % req_info)


#-------------------------
def read_data( req_info, outfile ):
   '''
      Call the replica_read() method in the replica manager's closure.
   '''
   filename = filename_from_req_info( req_info )
   return rg_closure.call_closure_read( req_info, filename, outfile )

#-------------------------
def write_data( req_info, infile ):
   '''
      Call the replica_write() method in the replica manager's closure.
   '''
   filename = filename_from_req_info( req_info )
   return rg_closure.call_closure_write( req_info, filename, infile )

#-------------------------
def delete_data( req_info ):
   '''
      Call the replica_delete() method in the replica manager's closure.
   '''
   filename = filename_from_req_info( req_info )
   return rg_closure.call_closure_delete( req_info, filename )
