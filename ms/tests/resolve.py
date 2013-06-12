#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import storage.storage as storage
import MS
from MS.entry import *

from MS.user import SyndicateUser

import time
import logging

import protobufs.ms_pb2 as ms_pb2

import MS.methods
from MS.methods.resolve import Resolve

def test( path, args ):
   username = args['username']
   volume_name = args['volume_name']

   user = storage.read_user( username )
   volume = storage.read_volume( volume_name )

   if user == None:
      raise Exception("No such user '%s'" % username )

   if volume == None:
      raise Exception("No such volume '%s'" % volume_name)

   # resolve a path
   reply = Resolve( user, volume, path )

   # parse the reply
   reply_struct = ms_pb2.ms_reply()

   try:
      reply_struct.ParseFromString( reply )
   except Exception, e:
      raise Exception("Invalid Protobuf string")

   path_metadata = reply_struct.entries_dir
   children_metadata = reply_struct.entries_base

   msg = ""
   for name, ms_ents in [('Path Metadata', path_metadata), ('Children Metadata', children_metadata)]:
      msg += "<font size=+3><b>" + name + "</b></font><br>"
      msg += "<table border=\"1\"><tr>"
      
      for attr in MSEntry.required_attrs:
         msg += "<td><b>" + attr + "</b></td>"

      msg += "</tr>"

      for ms_ent in ms_ents:
         ent = MSEntry.unprotobuf( ms_ent )
         msg += "<tr>"
         for attr in MSEntry.required_attrs:
            msg += "<td>" + str( getattr(ent, attr) ) + "</td>"

         msg += "</tr>"

      msg += "</table><br><br>"
   
   
   return (200, msg)
