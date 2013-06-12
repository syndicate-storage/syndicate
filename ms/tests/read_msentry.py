#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storage as storage
import MS
from MS.entry import *

import time
import types

def test( path, args ):
   username = args['username']
   volume_name = args['volume_name']

   user = storage.read_user( username )
   if user == None:
      raise Exception("user '%s' does not exist" % username )

   volume = storage.read_volume( volume_name )
   if volume == None:
      raise Exception("volume '%s' does not exist" % volume_name )
   
   ents = []
   if not path.endswith("/"):
      # read file
      ent = storage.read_msentry( user, volume, path )

      if ent == None:
         raise Exception("No such entry: %s" % path )
      
      if type(ent) == types.IntType:
         raise Exception("storage.read_msentry rc = %d" % ent )

      ents.append( ent )
      
   else:
      # read dir
      parent = storage.read_msentry( user, volume, path )

      if parent == None:
         raise Exception("No such entry: %s" % path)

      if type(parent) == types.IntType:
         raise Exception("storage.read_msentry rc = %d" % parent)
      
      ents = storage.read_msentry_children( volume, path, parent.num_children )
      ents = [parent] + ents


   msg = "<table border=\"1\"><tr>"
   for attr in MSEntry.required_attrs:
      msg += "<td><b>" + attr + "</b></td>"

   msg += "</tr>"

   for ent in ents:
      msg += "<tr>"
      for attr in MSEntry.required_attrs:
         msg += "<td>" + str( getattr(ent, attr) ) + "</td>"

      msg += "</tr>"

   msg += "</table>"
   
   return (200, msg)
