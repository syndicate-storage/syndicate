"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import MS
from MS.volume import Volume
from MS.entry import *

import common

import random
import os
import errno

import logging



def __read_path_metadata( owner_id, volume, fs_path ):
   """
   Read a path's worth of metadata from the volume.
   Return a list of path entries, and a list of children path entries (or None of the path refers to a file)
   """

   ent_metadata = MSEntry.Read( volume, fs_path )

   valid = True
   error = 0

   # validate consistency and permissions on the directories leading to the base
   # (avoid querying children if we can)
   for i in xrange(0,len(ent_metadata) - 1):
      ent = ent_metadata[i]

      if ent == None:
         # path refers to some nonexistant data
         ent_metadata = ent_metadata[:i]
         error = -errno.ENOENT;
         break

      # this must be a directory
      if ent.ftype != MSENTRY_TYPE_DIR:
         # not consistent
         valid = False
         ent_metadata = ent_metadata[:i]
         error = -errno.ENOTDIR;
         break

      # this must be searchable
      if ent.owner_id != owner_id and (ent.mode & 0011) == 0:
         # not searchable
         valid = False
         ent_metadata = ent_metadata[:i]
         error = -errno.EACCES;
         break

   children_metadata = []

   if valid:
      # got back valid, readable metadata.
      # did we get back the entry at the base?
      parts = fs_path.split("/")
      if parts[-1] == "":
         parts.pop()
      
      path_len = len( parts )
      if len( ent_metadata ) == path_len:
         # yup
         base_ent = ent_metadata.pop()

         # did we read the base?
         if base_ent != None:
            
            # if it's a directory, and we can read it, read its children.
            if base_ent.ftype == MSENTRY_TYPE_DIR and (base_ent.owner_id == owner_id or (base_ent.mode & 0044) != 0):
               children_metadata = MSEntry.ListAll( volume, fs_path )

            # force base_ent to be called "." if it's a directory
            if base_ent.ftype == MSENTRY_TYPE_DIR:
               base_ent.fs_path = "."
               
            children_metadata = [base_ent] + children_metadata
         else:
            error = -errno.ENOENT

   if ent_metadata == None:
      ent_metadata = []

   return (ent_metadata, children_metadata, error)


def Resolve( owner_id, volume, fs_path ):
   """
   Given an owner_id, volume, and an absolute filesystem path, resolve the path into an ms_reply message.
   Return a serialized response
   """

   # get the metadata
   path_metadata, children_metadata, error = __read_path_metadata( owner_id, volume, fs_path )

   # serialize
   reply = common.make_ms_reply( volume, error );
   
   found = True

   for ent_dir in path_metadata:
      if ent_dir != None:
         ent_dir_msg = reply.entries_dir.add()
         ent_dir.protobuf( ent_dir_msg )
      else:
         found = False
         break

   if found:
      for ent_base in children_metadata:
         ent_base_msg = reply.entries_base.add()
         ent_base.protobuf( ent_base_msg )

   return reply
   
