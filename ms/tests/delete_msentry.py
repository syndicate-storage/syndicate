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
   volume_name = args['volume_name']
   username = args['username']

   user = storage.read_user( username )
   volume = storage.read_volume( volume_name )

   if user == None:
      raise Exception("No such user '%s'" % username)

   if volume == None:
      raise Exception("No such volume '%s'" % volume_name)
   
   # delete the entry
   rc = storage.delete_msentry( user, volume, path )
   if rc != 0:
      raise Exception("storage.delete_msentry rc = %s" % rc )

   return (200, "OK")
   

