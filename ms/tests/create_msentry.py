#!/usr/bin/python
"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storage as storage
import MS
from MS.entry import *

import time

def test( path, args ):
   
   username = args['username']
   volume_name = args['volume_name']

   volume = storage.read_volume( volume_name )
   if volume == None:
      raise Exception( "volume %s' does not exist" % volume_name )

   user = storage.read_user( username )
   if user == None:
      raise Exception( "user '%s' does not exist" % username )
   
   # create the entry
   ftype = MSENTRY_TYPE_FILE
   size = 0
   delim = ""
   if path.endswith("/"):
      ftype = MSENTRY_TYPE_DIR
      size = 4096
      delim = "/"

   now_sec, now_nsec = storage.clock_gettime()
   
   rc = storage.create_msentry( user,
                                volume,
                                ftype=ftype,
                                fs_path=path,
                                url=os.path.join( "http://localhost:32780/", path.strip('/') ) + delim,
                                version=1,
                                ctime_sec=now_sec,
                                ctime_nsec=now_nsec,
                                mtime_sec=now_sec,
                                mtime_nsec=now_nsec,
                                owner_id=user.owner_id,
                                volume_id=volume.volume_id,
                                mode=0755,
                                size=size
                             )
                             
                                
   if rc != 0:
      raise Exception("storage.create_msentry rc = %s" % rc)

   return (200, "OK")

