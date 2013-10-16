#!/usr/bin/env python
# Copyright 2013 The Trustees of Princeton University
# All Rights Reserved

import os
import rm_common
import types
import inspect
import collections


#-------------------------
def file_name_from_req_info( req_info ):
   '''
      Generate a filename from a RequestInfo structure.
   '''
   if req_info.type == RequestInfo.MANIFEST:
      return "%016d.%016X.%d.%d.man" % (req_info.volume_id, req_info.file_id, req_info.mtime_sec, req_info.mtime_nsec)
   elif req_info.type == RequestInfo.BLOCK:
      return "%016d.%016X.%d.%d.blk" % (req_info.volume_id, req_info.file_id, req_info.block_id, req_info.block_version)

   raise Exception("Invalid RequestInfo structure '%s'" % req_info)


#-------------------------
def read_data( storage_config, req_info, outfile ):
   '''
      Call the read() method in the replica manager's closure.
   '''
   pass
   