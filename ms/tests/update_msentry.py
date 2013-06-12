#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storage as storage
import MS
from MS.entry import *

import types
import time

def test( path, args ):
   username = args['username']
   
   # enforce types
   if args.has_key('version'):
      args['version'] = int(args['version'])

   if args.has_key( 'mtime_sec' ):
      args['mtime_sec'] = int(args['mtime_sec'])

   if args.has_key( 'mtime_nsec' ):
      args['mtime_nsec'] = int(args['mtime_nsec'])

   if args.has_key( 'mode' ):
      args['mode'] = int(args['mode'])

   if args.has_key( 'size' ):
      args['size'] = int(args['size'])

   volume_name = args['volume_name']
   volume = storage.read_volume( volume_name )
   if volume == None:
      raise Exception("No such volume '%s'" % volume_name )

   user = storage.read_user( username )
   if user == None:
      raise Exception("No such user '%s'" % username )
   
   # update the entry
   rc = storage.update_msentry( user, volume, path, **args )
   
   if rc != 0:
      raise Exception("storage.update_msentry rc = %s" % rc)

   return (200, "OK")

